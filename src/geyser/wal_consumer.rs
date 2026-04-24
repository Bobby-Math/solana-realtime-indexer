use crate::geyser::wal_queue::{WalQueue, WalEntry};
use crate::geyser::decoder::decode_subscribe_update;
use crate::geyser::BlockTimeCache;
use crate::processor::pipeline::PipelineReport;
use crate::processor::batch_writer::{BatchWriter, BufferedBatch};
use crate::processor::decoder::{CustomDecoder, Type1Decoder, PersistedBatch};
use crate::processor::sink::{StorageSink, StorageWriteResult};
use crate::processor::sql::CheckpointUpdate;
use crate::processor::store::StoreSnapshot;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

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
    pending_checkpoint_seqs: Vec<u64>, // Track seqs to checkpoint after DB commit
    gap_filler: Option<RpcGapFiller>, // Event-driven gap repair
    block_time_cache: Arc<BlockTimeCache>, // Cache slot → block_time mapping
    next_read_seq: u64, // In-memory read cursor, advances after each process_entry
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
        // Cache last 1000 slots (~8KB of memory) - Solana produces ~216k slots/day
        let block_time_cache = BlockTimeCache::new(1000);

        // Initialize in-memory read cursor from persisted checkpoint
        let next_read_seq = wal_queue.get_last_processed_seq();

        Self {
            wal_queue,
            config,
            api_state,
            writer,
            decoder,
            custom_decoders,
            sink,
            pending_checkpoint_seqs: Vec::new(),
            gap_filler: None,
            block_time_cache,
            next_read_seq,
        }
    }

    pub fn with_gap_filler(mut self, gap_filler: RpcGapFiller) -> Self {
        self.gap_filler = Some(gap_filler);
        self
    }

    pub fn with_block_time_cache(mut self, cache: Arc<BlockTimeCache>) -> Self {
        self.block_time_cache = cache;
        self
    }

    pub async fn run(&mut self) -> Result<PipelineReport, String> {
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
                self.process_batch(batch, &mut report).await?;
            }

            // Check for new entries in WAL using in-memory read cursor
            match self.wal_queue.read_from(0, self.next_read_seq) {
                Ok(Some(entry)) => {
                    // Process the event (just add to buffer, DON'T mark yet)
                    match self.process_entry(&entry, &mut report).await {
                        Ok(()) => {
                            // Only advance read cursor after successful processing
                            self.next_read_seq = entry.seq + 1;
                        }
                        Err(e) => {
                            // Check if this is an unrecoverable decode error
                            let is_decode_error = e.contains("Failed to decode protobuf") ||
                                                 e.contains("Failed to decode SubscribeUpdate");

                            if is_decode_error {
                                // Corrupted WAL entry - skip it to prevent permanent block
                                log::error!("🔴 CORRUPT WAL ENTRY at slot {} seq {}: {}. Skipping to prevent consumer block.",
                                          entry.slot, entry.seq, e);

                                // Mark as processed to advance past corrupted entry
                                if let Err(mark_err) = self.wal_queue.mark_processed(entry.slot, entry.seq) {
                                    log::error!("Failed to mark corrupted entry slot {} seq {} as processed: {}",
                                              entry.slot, entry.seq, mark_err);
                                }

                                // Advance in-memory cursor past this entry
                                self.next_read_seq = entry.seq + 1;
                            } else {
                                // Other processing errors - log but don't skip (may be transient)
                                log::error!("Error processing entry slot {} seq {}: {}", entry.slot, entry.seq, e);
                                // Don't advance cursor - will retry on next iteration
                            }
                        }
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
                Ok(None) => {
                    // No new entries, sleep for poll interval
                    tokio::time::sleep(self.config.poll_interval).await;
                }
                Err(e) => {
                    // Check if this is a gap error
                    if e.contains("gap detected") {
                        log::warn!("⚠️ Gap detected in WAL: {}", e);

                        // Extract the sequence number from the error message
                        if let Some(seq_str) = e.split(" seq ").nth(1) {
                            if let Some(seq_end) = seq_str.find(' ') {
                                if let Ok(seq) = seq_str[..seq_end].parse::<u64>() {
                                    // Trigger immediate repair if gap filler is available
                                    if let Some(gap_filler) = &self.gap_filler {
                                        log::info!("🔧 Triggering immediate RPC repair for seq {}", seq);
                                        match gap_filler.repair_gap(seq).await {
                                            Ok(true) => {
                                                log::info!("✅ Gap repaired successfully, will retry read");
                                                // Retry immediately after repair
                                                continue;
                                            }
                                            Ok(false) => {
                                                log::error!("❌ Gap repair failed for seq {}, skipping", seq);
                                                // Mark as processed to skip this gap - use actual slot not 0
                                                let last_flushed = self.wal_queue.get_last_processed_seq();
                                                let next_seq = last_flushed + 1;
                                                if let Some(slot) = self.wal_queue.get_slot_for_seq(next_seq) {
                                                    let _ = self.wal_queue.mark_processed(slot, next_seq);
                                                } else {
                                                    log::warn!("No slot mapping for seq {}, using fallback", next_seq);
                                                    let _ = self.wal_queue.mark_processed(0, next_seq);
                                                }
                                            }
                                            Err(repair_err) => {
                                                log::error!("❌ Gap repair error for seq {}: {}, skipping", seq, repair_err);
                                                // Mark as processed to skip this gap - use actual slot not 0
                                                let last_flushed = self.wal_queue.get_last_processed_seq();
                                                let next_seq = last_flushed + 1;
                                                if let Some(slot) = self.wal_queue.get_slot_for_seq(next_seq) {
                                                    let _ = self.wal_queue.mark_processed(slot, next_seq);
                                                } else {
                                                    log::warn!("No slot mapping for seq {}, using fallback", next_seq);
                                                    let _ = self.wal_queue.mark_processed(0, next_seq);
                                                }
                                            }
                                        }
                                    } else {
                                        log::warn!("No gap filler available, skipping gap");
                                        // Mark as processed to skip this gap - use actual slot not 0
                                        let last_flushed = self.wal_queue.get_last_processed_seq();
                                        let next_seq = last_flushed + 1;
                                        if let Some(slot) = self.wal_queue.get_slot_for_seq(next_seq) {
                                            let _ = self.wal_queue.mark_processed(slot, next_seq);
                                        } else {
                                            log::warn!("No slot mapping for seq {}, using fallback", next_seq);
                                            let _ = self.wal_queue.mark_processed(0, next_seq);
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        log::error!("Error reading from WAL: {}", e);
                    }

                    // Sleep before retrying
                    tokio::time::sleep(self.config.poll_interval).await;
                }
            }
        }
    }

    async fn process_entry(&mut self, entry: &WalEntry, report: &mut PipelineReport) -> Result<(), String> {
        // Decode the protobuf bytes back to SubscribeUpdate
        let update = entry.decode_update()
            .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

        // Convert SubscribeUpdate to GeyserEvent using shared decoder
        let geyser_event = decode_subscribe_update(&update, entry.timestamp_unix_ms)
            .ok_or_else(|| "Failed to decode SubscribeUpdate to GeyserEvent".to_string())?;

        // BlockMeta events populate the cache, not the batch
        if let crate::geyser::decoder::GeyserEvent::BlockMeta(bm) = &geyser_event {
            self.block_time_cache.insert(bm.slot, bm.block_time_ms);
            log::debug!("Cached block_time {}ms for slot {}", bm.block_time_ms, bm.slot);

            // CRITICAL FIX: Add BlockMeta to pending_checkpoint_seqs instead of marking processed immediately
            // This prevents checkpoint from advancing before DB commit, avoiding data loss on crash
            self.pending_checkpoint_seqs.push(entry.seq);
            return Ok(());
        }

        // Add to batch
        report.received_events += 1;
        self.writer.push(geyser_event);

        // Track this sequence for later checkpointing (after DB commit)
        self.pending_checkpoint_seqs.push(entry.seq);

        Ok(())
    }

    async fn process_batch(&mut self, batch: BufferedBatch, report: &mut PipelineReport) -> Result<(), String> {
        let persisted = self.decoder.decode(batch, &mut self.custom_decoders, &self.block_time_cache);
        let checkpoint = checkpoint_update_for_batch(&persisted);
        let result = self.sink.write_batch(&persisted, checkpoint).await
            .map_err(|e| format!("Storage error: {}", e))?;

        apply_batch_report(report, persisted, result);

        // CRITICAL FIX: Mark sequences as processed ONLY AFTER DB commit succeeds
        // This prevents data loss if crash occurs between buffering and DB flush
        let checkpointed_seqs: Vec<_> = self.pending_checkpoint_seqs.drain(..).collect();

        if !checkpointed_seqs.is_empty() {
            // Find the maximum seq in this batch
            let max_seq = *checkpointed_seqs.iter().max().unwrap_or(&0);

            // Look up which slot corresponds to this seq
            let slot = match self.wal_queue.get_slot_for_seq(max_seq) {
                Some(s) => s,
                None => {
                    log::error!("Failed to get slot for seq {} - skipping checkpoint", max_seq);
                    return Ok(());
                }
            };

            // CRITICAL: Mark processed ONCE with the correct slot and max seq
            // This prevents corrupting last_flushed_slot and avoids N batch commits
            if let Err(e) = self.wal_queue.mark_processed(slot, max_seq) {
                log::error!("Failed to mark seq {} as processed after DB commit: {}", max_seq, e);
                // Note: We don't return error here because the DB commit succeeded,
                // and the seq will be retried on next startup
            }

            log::debug!("Checkpointed {} sequences after DB commit (slot: {}, seq: {})",
                       checkpointed_seqs.len(), slot, max_seq);
        }

        Ok(())
    }

    pub fn start_background_processor(mut self) -> tokio::task::JoinHandle<Result<PipelineReport, String>>
    where
        Self: Send + 'static,
    {
        tokio::spawn(async move {
            self.run().await
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
    rpc_endpoints: Vec<String>,
    wal_queue: Arc<WalQueue>,
    client: reqwest::Client,
}

impl RpcGapFiller {
    pub fn new(rpc_endpoints: Vec<String>, wal_queue: Arc<WalQueue>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|e| {
                panic!("Failed to create HTTP client: {}", e);
            });

        Self {
            rpc_endpoints,
            wal_queue,
            client,
        }
    }

    /// Repair a single gap immediately upon detection
    /// Returns true if repair succeeded, false if it failed
    pub async fn repair_gap(&self, seq: u64) -> Result<bool, String> {
        log::warn!("🔧 Immediate gap repair triggered for seq {}", seq);

        // Look up which slot corresponds to this sequence
        let slot = match self.wal_queue.get_slot_for_seq(seq) {
            Some(s) => {
                log::debug!("Seq {} maps to slot {} (from metadata)", seq, s);
                s
            }
            None => {
                log::error!("No slot mapping found for seq {} - cannot repair", seq);
                return Ok(false);
            }
        };

        // Try each RPC endpoint until one succeeds
        for endpoint in &self.rpc_endpoints {
            match self.fetch_and_repair_slot(endpoint, slot, seq).await {
                Ok(true) => {
                    log::info!("✅ Successfully repaired seq {} (slot {})", seq, slot);
                    return Ok(true);
                }
                Ok(false) => {
                    log::warn!("Slot {} not found or unavailable, trying next endpoint", slot);
                    continue;
                }
                Err(e) => {
                    log::warn!("Failed to fetch slot {} from {}: {}", slot, endpoint, e);
                    continue;
                }
            }
        }

        log::error!("❌ Failed to repair seq {} (slot {}) from all endpoints", seq, slot);
        Ok(false)
    }

    async fn fetch_and_repair_slot(&self, endpoint: &str, slot: u64, seq: u64) -> Result<bool, String> {
        // Fetch block from Solana RPC
        let url = format!("{}/", endpoint.trim_end_matches('/'));
        let response = self.client
            .post(&url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBlock",
                "params": [
                    slot,
                    {
                        "encoding": "base64",
                        "transactionDetails": "full",
                        "rewards": false,
                        "maxSupportedTransactionVersion": 0
                    }
                ]
            }))
            .send()
            .await
            .map_err(|e| format!("RPC request failed: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("RPC returned status {}", response.status()));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| format!("Failed to parse JSON response: {}", e))?;

        // Check if slot was found
        if let Some(error) = json.get("error") {
            if error["code"] == -32007 || error["message"].as_str().map_or(false, |m| m.contains("skipped")) {
                // Slot was skipped or not finalized
                return Ok(false);
            }
            return Err(format!("RPC error: {}", error));
        }

        let block_data = json.get("result")
            .and_then(|v| v.as_object())
            .ok_or_else(|| "No result in RPC response".to_string())?;

        // Convert block data to SubscribeUpdate
        let update = self.block_to_subscribe_update(block_data, slot)
            .map_err(|e| format!("Failed to convert block data: {}", e))?;

        // Repair the hole
        self.wal_queue.repair_hole(seq, slot, &update)?;

        Ok(true)
    }

    fn block_to_subscribe_update(&self, _block_data: &serde_json::Map<String, serde_json::Value>, _slot: u64) -> Result<helius_laserstream::grpc::SubscribeUpdate, String> {
        // TODO: Convert block data to SubscribeUpdate protobuf format
        // For now, return a placeholder to satisfy the type system
        // This needs actual implementation to convert:
        // - Block metadata -> SlotUpdate
        // - Transactions -> Transaction messages
        // - Account changes -> Account messages

        Err("Block to SubscribeUpdate conversion not yet implemented".to_string())
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

    #[test]
    fn test_in_memory_read_cursor_prevents_infinite_reread() {
        let temp_dir = tempdir().unwrap();
        let wal_queue = Arc::new(WalQueue::new(temp_dir.path()).unwrap());

        // Write 3 distinct entries to WAL
        for i in 0..3 {
            let update = SubscribeUpdate {
                ..Default::default()
            };
            wal_queue.push_update(100 + i, &update).unwrap();
        }

        // Simulate the consumer loop using in-memory cursor
        let mut next_read_seq = wal_queue.get_last_processed_seq(); // Starts at 0

        // First iteration: should read seq=0
        let entry1 = wal_queue.read_from(0, next_read_seq).unwrap().unwrap();
        assert_eq!(entry1.seq, 0, "First read should return seq=0");
        next_read_seq = entry1.seq + 1; // Advance cursor to 1

        // Second iteration: should read seq=1, NOT seq=0 again
        let entry2 = wal_queue.read_from(0, next_read_seq).unwrap().unwrap();
        assert_eq!(entry2.seq, 1, "Second read should return seq=1, not seq=0");
        next_read_seq = entry2.seq + 1; // Advance cursor to 2

        // Third iteration: should read seq=2, NOT seq=0 or seq=1 again
        let entry3 = wal_queue.read_from(0, next_read_seq).unwrap().unwrap();
        assert_eq!(entry3.seq, 2, "Third read should return seq=2, not seq=0 or seq=1");
        next_read_seq = entry3.seq + 1; // Advance cursor to 3

        // Fourth iteration: should return None (no more entries)
        let entry4 = wal_queue.read_from(0, next_read_seq).unwrap();
        assert!(entry4.is_none(), "Fourth read should return None");

        // Verify persisted checkpoint hasn't advanced (no flush yet)
        assert_eq!(wal_queue.get_last_processed_seq(), 0,
                   "Persisted checkpoint should still be 0 since no flush occurred");
    }
}
