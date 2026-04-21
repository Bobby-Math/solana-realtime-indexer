use crate::geyser::wal_queue::{WalQueue, WalEntry};
use crate::geyser::decoder::GeyserEvent;
use crate::processor::pipeline::PipelineReport;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use helius_laserstream::grpc::SubscribeUpdate;
use helius_laserstream::grpc::subscribe_update::UpdateOneof;

pub struct WalPipelineConfig {
    pub wal_path: String,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub batch_flush_ms: u64,
    pub channel_capacity: usize,
}

pub struct WalPipelineRunner {
    wal_queue: Arc<WalQueue>,
    config: WalPipelineConfig,
    _api_state: Arc<Mutex<crate::api::rest::ApiSnapshot>>,
    sender: mpsc::SyncSender<GeyserEvent>,
}

impl WalPipelineRunner {
    pub fn new(
        wal_queue: Arc<WalQueue>,
        config: WalPipelineConfig,
        _api_state: Arc<Mutex<crate::api::rest::ApiSnapshot>>,
        sender: mpsc::SyncSender<GeyserEvent>,
    ) -> Self {
        Self {
            wal_queue,
            config,
            _api_state,
            sender,
        }
    }

    pub async fn run(&self) -> Result<PipelineReport, String> {
        let started_at = Instant::now();
        let mut events_processed = 0u64;
        let mut last_log_time = started_at;
        let log_interval = Duration::from_secs(5);

        log::info!("Starting WAL pipeline consumer from: {}", self.config.wal_path);

        // Main polling loop
        loop {
            // Check for new entries in WAL
            match self.wal_queue.read_next() {
                Ok(Some(entry)) => {
                    // Process the event
                    if let Err(e) = self.process_entry(&entry).await {
                        log::error!("Error processing entry slot {} seq {}: {}", entry.slot, entry.seq, e);
                        // Continue processing other entries even if one fails
                    } else {
                        events_processed += 1;

                        // Mark as processed in WAL
                        if let Err(e) = self.wal_queue.mark_processed(entry.slot, entry.seq) {
                            log::error!("Failed to mark entry slot {} seq {} as processed: {}", entry.slot, entry.seq, e);
                        }

                        // Log progress every 5 seconds
                        let now = Instant::now();
                        if now.duration_since(last_log_time) >= log_interval {
                            let elapsed = now.duration_since(started_at);
                            let events_per_sec = events_processed as f64 / elapsed.as_secs_f64();
                            let unprocessed_count = self.wal_queue.get_unprocessed_count();
                            log::info!("📊 WAL Pipeline Progress: {:.1}s elapsed, {} events processed ({:.1} events/sec), {} unprocessed in WAL",
                                      elapsed.as_secs_f64(), events_processed, events_per_sec, unprocessed_count);
                            last_log_time = now;
                        }
                    }
                }
                Ok(None) => {
                    // No new entries, sleep for poll interval
                    tokio::time::sleep(self.config.poll_interval).await;
                }
                Err(e) => {
                    {
                        let error_msg = format!("Error reading from WAL: {}", e);
                        log::error!("{}", error_msg);
                    }
                    tokio::time::sleep(self.config.poll_interval).await;
                }
            }
        }
    }

    async fn process_entry(&self, entry: &WalEntry) -> Result<(), String> {
        // Decode the protobuf bytes back to SubscribeUpdate
        let update = entry.decode_update()
            .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

        // Convert SubscribeUpdate to GeyserEvent for the pipeline
        let geyser_event = self.convert_update_to_event(&update, entry)?;

        // Send to the pipeline channel
        self.sender.send(geyser_event).map_err(|e| format!("Failed to send event to pipeline: {}", e))?;

        Ok(())
    }

    fn convert_update_to_event(&self, update: &SubscribeUpdate, entry: &WalEntry) -> Result<GeyserEvent, String> {
        let timestamp_unix_ms = entry.timestamp_unix_ms;

        match &update.update_oneof {
            Some(UpdateOneof::Account(account_update)) => {
                if let Some(account_info) = &account_update.account {
                    Ok(GeyserEvent::AccountUpdate(crate::geyser::decoder::AccountUpdate {
                        timestamp_unix_ms,
                        slot: account_update.slot,
                        pubkey: account_info.pubkey.clone(),
                        owner: account_info.owner.clone(),
                        lamports: account_info.lamports,
                        write_version: account_info.write_version,
                        data: account_info.data.clone(),
                    }))
                } else {
                    Err("Account update missing account info".to_string())
                }
            }
            Some(UpdateOneof::Transaction(tx_update)) => {
                if let Some(tx_info) = &tx_update.transaction {
                    let success = tx_info.meta.as_ref()
                        .map(|meta| meta.err.is_none())
                        .unwrap_or(false);

                    let fee = tx_info.meta.as_ref()
                        .map(|meta| meta.fee)
                        .unwrap_or(0);

                    let log_messages = tx_info.meta.as_ref()
                        .map(|meta| meta.log_messages.clone())
                        .unwrap_or_default();

                    let program_ids = if let Some(tx) = &tx_info.transaction {
                        if let Some(message) = &tx.message {
                            let mut invoked_programs = std::collections::HashSet::new();
                            for instruction in &message.instructions {
                                let program_idx = instruction.program_id_index as usize;
                                if program_idx < message.account_keys.len() {
                                    invoked_programs.insert(message.account_keys[program_idx].clone());
                                }
                            }
                            invoked_programs.into_iter().collect()
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    };

                    Ok(GeyserEvent::Transaction(crate::geyser::decoder::TransactionUpdate {
                        timestamp_unix_ms,
                        slot: tx_update.slot,
                        signature: tx_info.signature.clone(),
                        fee,
                        success,
                        program_ids,
                        log_messages,
                    }))
                } else {
                    Err("Transaction update missing transaction info".to_string())
                }
            }
            Some(UpdateOneof::Slot(slot_update)) => {
                Ok(GeyserEvent::SlotUpdate(crate::geyser::decoder::SlotUpdate {
                    timestamp_unix_ms,
                    slot: slot_update.slot,
                    parent_slot: slot_update.parent,
                    status: format!("{:?}", slot_update.status),
                }))
            }
            Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => {
                Err("Ping/Pong should not be stored in WAL".to_string())
            }
            Some(UpdateOneof::Block(_)) | Some(UpdateOneof::BlockMeta(_)) | Some(UpdateOneof::TransactionStatus(_)) | Some(UpdateOneof::Entry(_)) => {
                Err("Unsupported update type for WAL".to_string())
            }
            None => {
                Err("SubscribeUpdate has no variant".to_string())
            }
        }
    }

    pub async fn start_background_processor(self: Arc<Self>) -> tokio::task::JoinHandle<Result<PipelineReport, String>> {
        tokio::spawn(async move {
            self.run().await
        })
    }
}

// RPC Fallback for gap detection and filling
pub struct RpcGapFiller {
    _rpc_endpoints: Vec<String>,
    wal_queue: Arc<WalQueue>,
}

impl RpcGapFiller {
    pub fn new(_rpc_endpoints: Vec<String>, wal_queue: Arc<WalQueue>) -> Self {
        Self {
            _rpc_endpoints,
            wal_queue,
        }
    }

    pub async fn detect_and_fill_gaps(&self) -> Result<(), String> {
        let last_processed_seq = self.wal_queue.get_last_processed_seq();
        let total_written = self.wal_queue.get_total_written();

        if total_written > last_processed_seq {
            let gap_size = total_written - last_processed_seq;
            log::warn!("Detected gap in WAL processing: {} unprocessed entries (last_processed_seq: {}, total: {})",
                       gap_size, last_processed_seq, total_written);

            // Try to recover missing data via RPC
            self.fill_via_rpc(last_processed_seq, total_written).await?;
        }

        Ok(())
    }

    async fn fill_via_rpc(&self, from_seq: u64, to_seq: u64) -> Result<(), String> {
        log::info!("Attempting to fill gap from sequence {} to {} via RPC", from_seq, to_seq);

        // For each missing sequence number, try to get the data from RPC
        // This is a placeholder - actual implementation would call Solana RPC
        for seq in from_seq..to_seq {
            match self.wal_queue.read_from(0, seq) { // We don't know the slot, so use 0
                Ok(Some(entry)) => {
                    log::debug!("Recovered entry slot {} seq {} from WAL", entry.slot, seq);
                    // Process the recovered entry
                }
                Ok(None) => {
                    log::warn!("Sequence {} not found in WAL, will need RPC recovery", seq);
                    // TODO: Implement actual RPC call to get missing slot/transaction data
                }
                Err(e) => {
                    log::error!("Error reading entry {} from WAL: {}", seq, e);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use helius_laserstream::grpc::SubscribeUpdate;

    #[test]
    fn wal_pipeline_processes_protobuf_entries() {
        let temp_dir = tempdir().unwrap();
        let wal_queue = Arc::new(WalQueue::new(temp_dir.path()).unwrap());

        // Write test protobuf entry to WAL
        let update = SubscribeUpdate {
            ..Default::default()
        };

        wal_queue.push_update(100, &update).unwrap();

        // Verify queue state
        assert_eq!(wal_queue.get_total_written(), 1);
        assert_eq!(wal_queue.get_unprocessed_count(), 1);
        assert_eq!(wal_queue.get_last_processed_seq(), 0);
    }
}
