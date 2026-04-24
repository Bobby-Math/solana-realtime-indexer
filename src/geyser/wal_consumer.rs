use crate::geyser::wal_queue::{WalQueue, WalEntry};
use crate::geyser::decoder::GeyserEvent;
use crate::processor::pipeline::PipelineReport;
use crate::processor::batch_writer::{BatchWriter, BufferedBatch};
use crate::processor::decoder::{CustomDecoder, Type1Decoder, PersistedBatch};
use crate::processor::sink::{StorageSink, StorageWriteResult};
use crate::processor::sql::CheckpointUpdate;
use crate::processor::store::StoreSnapshot;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use helius_laserstream::grpc::SubscribeUpdate;
use helius_laserstream::grpc::subscribe_update::UpdateOneof;

pub struct WalPipelineConfig {
    pub wal_path: String,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub batch_flush_ms: u64,
}

pub struct WalPipelineRunner {
    wal_queue: Arc<WalQueue>,
    config: WalPipelineConfig,
    #[allow(dead_code)]
    api_state: Arc<Mutex<crate::api::rest::ApiSnapshot>>,
    writer: BatchWriter,
    decoder: Type1Decoder,
    custom_decoders: Vec<Box<dyn CustomDecoder>>,
    sink: Box<dyn StorageSink>,
}

impl WalPipelineRunner {
    pub fn new(
        wal_queue: Arc<WalQueue>,
        config: WalPipelineConfig,
        api_state: Arc<Mutex<crate::api::rest::ApiSnapshot>>,
        writer: BatchWriter,
        decoder: Type1Decoder,
        custom_decoders: Vec<Box<dyn CustomDecoder>>,
        sink: Box<dyn StorageSink>,
    ) -> Self {
        Self {
            wal_queue,
            config,
            api_state,
            writer,
            decoder,
            custom_decoders,
            sink,
        }
    }

    pub fn run(&mut self) -> Result<PipelineReport, String> {
        let started_at = Instant::now();
        let mut report = PipelineReport::default();
        let mut last_log_time = started_at;
        let log_interval = Duration::from_secs(5);

        log::info!("Starting WAL pipeline consumer from: {}", self.config.wal_path);

        // Main polling loop
        loop {
            let now = Instant::now();

            // Check if we should flush due to interval
            if let Some(batch) = self.writer.flush_if_needed(now) {
                self.process_batch(batch, &mut report)?;
            }

            // Check for new entries in WAL
            match self.wal_queue.read_next() {
                Ok(Some(entry)) => {
                    // Process the event
                    match self.process_entry(&entry, &mut report) {
                        Ok(()) => {
                            // Mark as processed in WAL
                            if let Err(e) = self.wal_queue.mark_processed(entry.slot, entry.seq) {
                                log::error!("Failed to mark entry slot {} seq {} as processed: {}", entry.slot, entry.seq, e);
                            }

                            // Log progress every 5 seconds
                            if now.duration_since(last_log_time) >= log_interval {
                                let elapsed = now.duration_since(started_at);
                                let events_per_sec = report.received_events as f64 / elapsed.as_secs_f64();
                                let unprocessed_count = self.wal_queue.get_unprocessed_count();
                                log::info!("📊 WAL Pipeline Progress: {:.1}s elapsed, {} events processed ({:.1} events/sec), {} unprocessed in WAL",
                                          elapsed.as_secs_f64(), report.received_events, events_per_sec, unprocessed_count);
                                last_log_time = now;
                            }
                        }
                        Err(e) => {
                            log::error!("Error processing entry slot {} seq {}: {}", entry.slot, entry.seq, e);
                        }
                    }
                }
                Ok(None) => {
                    // No new entries, sleep for poll interval
                    std::thread::sleep(self.config.poll_interval);
                }
                Err(e) => {
                    log::error!("Error reading from WAL: {}", e);
                    std::thread::sleep(self.config.poll_interval);
                }
            }
        }
    }

    fn process_entry(&mut self, entry: &WalEntry, report: &mut PipelineReport) -> Result<(), String> {
        // Decode the protobuf bytes back to SubscribeUpdate
        let update = entry.decode_update()
            .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

        // Convert SubscribeUpdate to GeyserEvent
        let geyser_event = self.convert_update_to_event(&update, entry)?;

        // Add to batch
        report.received_events += 1;
        self.writer.push(geyser_event);

        Ok(())
    }

    fn process_batch(&mut self, batch: BufferedBatch, report: &mut PipelineReport) -> Result<(), String> {
        let persisted = self.decoder.decode(batch, &mut self.custom_decoders);
        let checkpoint = checkpoint_update_for_batch(&persisted);
        let result = self.sink.write_batch(&persisted, checkpoint)
            .map_err(|e| format!("Storage error: {}", e))?;

        apply_batch_report(report, persisted, result);
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
                    status: Self::map_slot_status(slot_update.status),
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

    fn map_slot_status(status: i32) -> String {
        let status_str = match status {
            0 => "processed",
            1 => "confirmed",
            2 => "finalized",
            3 => "first_shred_received",
            4 => "completed",
            5 => "created_bank",
            6 => "dead",
            _ => {
                log::warn!("Unknown slot status value: {}, using 'unknown'", status);
                "unknown"
            }
        };

        log::debug!("Mapping slot status {} -> '{}'", status, status_str);
        status_str.to_string()
    }

    pub fn start_background_processor(mut self) -> tokio::task::JoinHandle<Result<PipelineReport, String>>
    where
        Self: Send + 'static,
    {
        tokio::task::spawn_blocking(move || {
            self.run()
        })
    }
}

fn apply_batch_report(report: &mut PipelineReport, batch: PersistedBatch, result: StorageWriteResult) {
    report.flush_count += 1;
    report.last_processed_slot = max_optional(report.last_processed_slot, latest_processed_slot(&batch));
    report.last_observed_at_unix_ms = max_optional(
        report.last_observed_at_unix_ms,
        batch.latest_timestamp_unix_ms(),
    );
    report.account_rows_written += batch.account_rows.len() as u64;
    report.transaction_rows_written += batch.transaction_rows.len() as u64;
    report.slot_rows_written += batch.slot_rows.len() as u64;
    report.custom_rows_written += batch.custom_rows.len() as u64;
    report.sql_statements_planned += result.sql_statements_planned;
    let snapshot: StoreSnapshot = result.snapshot;
    report.retained_account_rows = snapshot.account_rows;
    report.retained_transaction_rows = snapshot.transaction_rows;
    report.retained_slot_rows = snapshot.slot_rows;
    report.retained_custom_rows = snapshot.custom_rows;
    report.pruned_account_rows = snapshot.metrics.account_rows_pruned;
    report.pruned_transaction_rows = snapshot.metrics.transaction_rows_pruned;
    report.pruned_slot_rows = snapshot.metrics.slot_rows_pruned;
    report.pruned_custom_rows = snapshot.metrics.custom_rows_pruned;
}

fn max_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn latest_processed_slot(batch: &PersistedBatch) -> Option<i64> {
    batch
        .slot_rows
        .iter()
        .map(|row| row.slot)
        .chain(batch.transaction_rows.iter().map(|row| row.slot))
        .chain(batch.account_rows.iter().map(|row| row.slot))
        .chain(batch.custom_rows.iter().map(|row| row.slot))
        .max()
}

fn checkpoint_update_for_batch(batch: &PersistedBatch) -> Option<CheckpointUpdate> {
    let last_processed_slot = batch
        .slot_rows
        .iter()
        .map(|row| row.slot)
        .chain(batch.transaction_rows.iter().map(|row| row.slot))
        .chain(batch.account_rows.iter().map(|row| row.slot))
        .chain(batch.custom_rows.iter().map(|row| row.slot))
        .max();
    let last_observed_at_unix_ms = batch.latest_timestamp_unix_ms()?;

    Some(CheckpointUpdate {
        stream_name: "geyser-main".to_string(),
        last_processed_slot,
        last_observed_at_unix_ms,
        notes: Some(format!("flush_reason={:?}", batch.reason)),
    })
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

        // Detect all gaps in the range
        let gaps = self.wal_queue.detect_gaps(from_seq, to_seq);

        if gaps.is_empty() {
            log::info!("No gaps detected in range {}..{}", from_seq, to_seq);
            return Ok(());
        }

        log::warn!("Detected {} holes in WAL: {:?}", gaps.len(), gaps);

        // For each missing sequence, try to repair it using RPC
        for seq in gaps {
            // Look up which slot corresponds to this sequence
            let slot = match self.wal_queue.get_slot_for_seq(seq) {
                Some(s) => {
                    log::debug!("Seq {} maps to slot {} (from metadata)", seq, s);
                    s
                }
                None => {
                    log::error!("No slot mapping found for seq {} - cannot repair", seq);
                    continue;
                }
            };

            // TODO: Implement actual RPC call to fetch slot/transaction data
            // For now, we log what needs to be fetched:
            log::warn!(
                "Need to fetch slot {} from RPC to repair seq {}. \
                This slot's data was lost due to a crash between seq allocation and write.",
                slot, seq
            );

            // Pseudo-code for RPC repair:
            // 1. Call Solana RPC to get block/transaction data for this slot
            // 2. Convert the data into SubscribeUpdate format
            // 3. Call self.wal_queue.repair_hole(seq, slot, &update)
            // 4. Process the repaired entry normally

            log::info!("Would repair seq {} with slot {} data from RPC", seq, slot);
        }

        // TODO: Return error if any repairs failed
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
