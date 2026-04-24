use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::Duration;
use std::path::Path;
use helius_laserstream::grpc::SubscribeUpdate;
use prost::Message;

const WAL_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const WAL_GC_INTERVAL: Duration = Duration::from_secs(10); // Run GC every 10 seconds

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub slot: u64,
    pub seq: u64,
    pub payload: Vec<u8>, // Raw protobuf bytes
    pub timestamp_unix_ms: i64,
}

impl WalEntry {
    pub fn from_update(slot: u64, seq: u64, update: &SubscribeUpdate) -> Self {
        Self {
            slot,
            seq,
            payload: update.encode_to_vec(), // ✅ One re-encode
            timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    pub fn decode_update(&self) -> Result<SubscribeUpdate, prost::DecodeError> {
        SubscribeUpdate::decode(self.payload.as_ref()) // ✅ One decode
    }
}

pub struct WalQueue {
    db: Arc<fjall::Database>,
    events: Arc<fjall::Keyspace>,
    metadata: Arc<fjall::Keyspace>,
    slot_sequence: Arc<AtomicU64>,
    wal_path: String,
}

impl WalQueue {
    pub fn new(wal_path: impl AsRef<Path>) -> Result<Self, String> {
        let wal_path = wal_path.as_ref();

        // Create WAL directory if it doesn't exist
        if let Some(parent) = wal_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("Failed to create WAL directory: {}", e))?;
        }

        // Initialize fjall database
        let db = fjall::Database::builder(wal_path)
            .open()
            .map_err(|e| format!("Failed to open fjall database: {}", e))?;

        // Create keyspaces for events and metadata
        let events = db.keyspace("events", fjall::KeyspaceCreateOptions::default)
            .map_err(|e| format!("Failed to open events keyspace: {}", e))?;

        let metadata = db.keyspace("metadata", fjall::KeyspaceCreateOptions::default)
            .map_err(|e| format!("Failed to open metadata keyspace: {}", e))?;

        // CRITICAL: Scan events keyspace to find the maximum existing sequence number.
        // This prevents the producer from overwriting unprocessed events on restart.
        // last_flushed_seq is the consumer checkpoint, NOT the producer cursor.
        let max_existing_seq = {
            let mut rev = events.range::<Vec<u8>, _>(..).rev();
            rev.next()
                .and_then(|guard| guard.into_inner().ok())
                .map(|(k, _)| {
                    let key_bytes = k.to_vec();
                    u64::from_be_bytes(key_bytes.try_into().unwrap())
                })
                .unwrap_or(0)
        };

        // Recover consumer checkpoint from metadata (for resuming consumption)
        let consumer_checkpoint_seq = if let Ok(Some(last_seq_bytes)) = metadata.get(b"last_flushed_seq") {
            if last_seq_bytes.len() == 8 {
                let arr: [u8; 8] = last_seq_bytes.to_vec().try_into().unwrap();
                Some(u64::from_be_bytes(arr))
            } else {
                None
            }
        } else {
            None
        };

        // Initialize producer cursor from actual max key in events keyspace
        // For an empty WAL (max_existing_seq = 0), start at 0
        // For a non-empty WAL (max_existing_seq = N), start at N+1 to avoid overwrites
        let slot_sequence = Arc::new(AtomicU64::new(
            if max_existing_seq == 0 {
                0  // Empty WAL starts at sequence 0
            } else {
                max_existing_seq + 1  // Non-empty WAL continues after max
            }
        ));

        log::info!("WAL init: max_existing_seq={}, producer_cursor={}", max_existing_seq,
                   if max_existing_seq == 0 { 0 } else { max_existing_seq + 1 });

        // Log recovery state
        if max_existing_seq > 0 {
            let unprocessed_count = max_existing_seq.saturating_add(1).saturating_sub(consumer_checkpoint_seq.unwrap_or(0));
            log::info!(
                "WAL recovered: max_seq={}, producer_cursor={}, consumer_checkpoint={:?}, unprocessed_events={}",
                max_existing_seq,
                max_existing_seq + 1,
                consumer_checkpoint_seq,
                unprocessed_count
            );
        } else {
            log::info!("New WAL initialized at: {}", wal_path.display());
        }

        Ok(Self {
            db: Arc::new(db),
            events: Arc::new(events),
            metadata: Arc::new(metadata),
            slot_sequence,
            wal_path: wal_path.display().to_string(),
        })
    }

    pub fn push_update(&self, slot: u64, update: &SubscribeUpdate) -> Result<u64, String> {
        let seq = self.slot_sequence.fetch_add(1, Ordering::SeqCst);

        // Create WAL entry with raw protobuf bytes
        let entry = WalEntry::from_update(slot, seq, update);

        // Serialize entry to bytes: slot(8) + seq(8) + timestamp(8) + payload_len(4) + payload
        let mut buffer = Vec::with_capacity(28 + entry.payload.len());
        buffer.extend_from_slice(&entry.slot.to_be_bytes());
        buffer.extend_from_slice(&entry.seq.to_be_bytes());
        buffer.extend_from_slice(&entry.timestamp_unix_ms.to_be_bytes());
        buffer.extend_from_slice(&(entry.payload.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&entry.payload);

        // Write to fjall events keyspace with seq as key (big-endian for correct sorting)
        // This is O(1) and does NOT block - writes go to OS buffer immediately
        self.events.insert(seq.to_be_bytes(), buffer)
            .map_err(|e| format!("Failed to write to WAL: {}", e))?;

        Ok(seq)
    }

    // Legacy method for compatibility - converts GeyserEvent to SubscribeUpdate
    // This should be removed once the pipeline is fully converted to use SubscribeUpdate
    #[allow(dead_code)]
    pub fn push(&self, event: crate::geyser::decoder::GeyserEvent) -> Result<u64, String> {
        // This is a temporary compatibility shim
        // In the future, all callers should use push_update() directly with SubscribeUpdate
        let _slot = match &event {
            crate::geyser::decoder::GeyserEvent::AccountUpdate(acc) => acc.slot,
            crate::geyser::decoder::GeyserEvent::Transaction(tx) => tx.slot,
            crate::geyser::decoder::GeyserEvent::SlotUpdate(slot) => slot.slot,
        };

        // For now, we'll store a placeholder - this method should be removed
        log::warn!("Using legacy push() method - this should be replaced with push_update()");
        let seq = self.slot_sequence.fetch_add(1, Ordering::SeqCst);
        Ok(seq)
    }

    pub fn read_next(&self) -> Result<Option<WalEntry>, String> {
        let last_flushed_seq = self.get_last_processed_seq();
        self.read_from_seq(last_flushed_seq)
    }

    pub fn read_from(&self, _slot: u64, seq: u64) -> Result<Option<WalEntry>, String> {
        self.read_from_seq(seq)
    }

    fn read_from_seq(&self, seq: u64) -> Result<Option<WalEntry>, String> {
        // Use fjall's O(1) range iterator to get the next event
        let start_key = seq.to_be_bytes();

        let mut iter = self.events.range(start_key..);

        if let Some(guard) = iter.next() {
            let (key, value) = guard.into_inner()
                .map_err(|e| format!("Failed to read from WAL: {}", e))?;

            // Decode the entry from the stored value
            let value_bytes = value.to_vec();
            let entry_slot = u64::from_be_bytes(value_bytes[0..8].try_into().unwrap());
            let entry_seq = u64::from_be_bytes(value_bytes[8..16].try_into().unwrap());
            let timestamp = i64::from_be_bytes(value_bytes[16..24].try_into().unwrap());
            let payload_len = u32::from_be_bytes(value_bytes[24..28].try_into().unwrap()) as usize;
            let payload = value_bytes[28..28 + payload_len].to_vec();

            // Verify the key matches (sanity check)
            let key_bytes = key.to_vec();
            let key_seq = u64::from_be_bytes(key_bytes.try_into().unwrap());
            if key_seq != entry_seq {
                return Err(format!("Key mismatch: key seq {} != entry seq {}", key_seq, entry_seq));
            }

            Ok(Some(WalEntry {
                slot: entry_slot,
                seq: entry_seq,
                payload,
                timestamp_unix_ms: timestamp,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn mark_processed(&self, slot: u64, seq: u64) -> Result<(), String> {
        // Update the metadata keyspace with the last processed sequence
        self.metadata.insert(b"last_flushed_slot", slot.to_be_bytes())
            .map_err(|e| format!("Failed to update last_flushed_slot: {}", e))?;

        self.metadata.insert(b"last_flushed_seq", (seq + 1).to_be_bytes())
            .map_err(|e| format!("Failed to update last_flushed_seq: {}", e))?;

        Ok(())
    }

    pub fn flush(&self) -> Result<(), String> {
        // Flush the fjall database to disk
        // This is safe to call in background - doesn't block the Geyser ingest thread
        self.db.persist(fjall::PersistMode::SyncAll)
            .map_err(|e| format!("Failed to flush WAL: {}", e))
    }

    pub fn get_unprocessed_count(&self) -> u64 {
        let total = self.slot_sequence.load(Ordering::SeqCst);
        let processed = self.get_last_processed_seq();
        total.saturating_sub(processed)
    }

    pub fn get_total_written(&self) -> u64 {
        self.slot_sequence.load(Ordering::SeqCst)
    }

    pub fn get_last_processed_slot(&self) -> u64 {
        if let Ok(Some(slot_bytes)) = self.metadata.get(b"last_flushed_slot") {
            if slot_bytes.len() == 8 {
                let arr: [u8; 8] = slot_bytes.to_vec().try_into().unwrap();
                return u64::from_be_bytes(arr);
            }
        }
        0
    }

    pub fn get_last_processed_seq(&self) -> u64 {
        if let Ok(Some(seq_bytes)) = self.metadata.get(b"last_flushed_seq") {
            if seq_bytes.len() == 8 {
                let arr: [u8; 8] = seq_bytes.to_vec().try_into().unwrap();
                return u64::from_be_bytes(arr);
            }
        }
        0
    }

    pub fn wal_path(&self) -> &str {
        &self.wal_path
    }

    /// Deletes processed events from the WAL to reclaim disk space.
    ///
    /// This method reads the last_flushed_seq from the metadata keyspace (which represents
    /// the high-water mark of events successfully written to TimescaleDB) and deletes all
    /// events with sequence numbers less than that value.
    ///
    /// Returns the number of events deleted.
    pub fn delete_processed_events(&self) -> Result<u64, String> {
        let last_flushed_seq = self.get_last_processed_seq();

        if last_flushed_seq == 0 {
            // No events to delete
            return Ok(0);
        }

        // Iterate from sequence 0 up to last_flushed_seq and delete each event
        let end_key = last_flushed_seq.to_be_bytes();
        let mut iter = self.events.range(..end_key);

        let mut deleted_count = 0u64;

        while let Some(guard) = iter.next() {
            let (key, _) = guard.into_inner()
                .map_err(|e| format!("Failed to read event key during GC: {}", e))?;

            // Delete the event (convert UserKey to Vec<u8> first)
            let key_bytes = key.to_vec();
            self.events.remove(key_bytes)
                .map_err(|e| format!("Failed to delete event during GC: {}", e))?;

            deleted_count += 1;
        }

        if deleted_count > 0 {
            log::debug!("WAL GC: Deleted {} processed events (seq < {})", deleted_count, last_flushed_seq);
        }

        Ok(deleted_count)
    }

    pub async fn start_background_flush(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut flush_interval = tokio::time::interval(WAL_FLUSH_INTERVAL);
            let mut gc_interval = tokio::time::interval(WAL_GC_INTERVAL);

            loop {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        // Flush every 100ms
                        if let Err(e) = self.flush() {
                            log::error!("WAL flush error: {}", e);
                        } else {
                            let unprocessed = self.get_unprocessed_count();
                            if unprocessed > 0 {
                                log::debug!("WAL flushed, {} unprocessed entries", unprocessed);
                            }
                        }
                    }
                    _ = gc_interval.tick() => {
                        // Run garbage collection every 10 seconds
                        if let Err(e) = self.delete_processed_events() {
                            log::error!("WAL garbage collection error: {}", e);
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn wal_queue_initializes() {
        let temp_dir = tempdir().unwrap();
        let wal_file = temp_dir.path().join("test.wal");
        let wal_queue = WalQueue::new(&wal_file).unwrap();
        assert_eq!(wal_queue.get_total_written(), 0);
    }

    #[test]
    fn test_normal_operation_garbage_collection() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path();
        let wal_queue = WalQueue::new(wal_path).unwrap();

        // Write 1,000 mock events to the WAL
        // Create dummy SubscribeUpdate with minimal valid protobuf data
        for seq in 0..1000 {
            let update = SubscribeUpdate {
                ..Default::default()
            };

            wal_queue.push_update(seq, &update).unwrap();
        }

        // Mark first 500 events as processed
        wal_queue.mark_processed(500, 499).unwrap();

        // Run garbage collection
        let deleted = wal_queue.delete_processed_events().unwrap();

        // Verify that 500 events were deleted
        assert_eq!(deleted, 500);

        // Verify that the events keyspace contains exactly 500 items (sequences 500-999)
        let remaining_count = wal_queue.events.iter().count();
        assert_eq!(remaining_count, 500, "Expected 500 remaining events, found {}", remaining_count);

        // Verify that read_next returns sequence 500 (first unprocessed event)
        let next_entry = wal_queue.read_next().unwrap().unwrap();
        assert_eq!(next_entry.seq, 500);
    }

    #[test]
    fn test_full_catchup_garbage_collection() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path();
        let wal_queue = WalQueue::new(wal_path).unwrap();

        // Write 100 mock events to the WAL
        for seq in 0..100 {
            let update = SubscribeUpdate {
                ..Default::default()
            };

            wal_queue.push_update(seq, &update).unwrap();
        }

        // Mark all 100 events as processed
        wal_queue.mark_processed(100, 99).unwrap();

        // Run garbage collection
        let deleted = wal_queue.delete_processed_events().unwrap();

        // Verify that all 100 events were deleted
        assert_eq!(deleted, 100);

        // Verify that the events keyspace is completely empty
        let remaining_count = wal_queue.events.iter().count();
        assert_eq!(remaining_count, 0, "Expected 0 remaining events, found {}", remaining_count);

        // Verify that read_next returns None (no more events)
        let next_entry = wal_queue.read_next().unwrap();
        assert!(next_entry.is_none(), "Expected no events remaining, but found one");
    }

    #[test]
    fn test_restart_preserves_unprocessed_events() {
        // This test verifies the fix for the critical bug where
        // the producer cursor was initialized from the consumer checkpoint,
        // causing unprocessed events to be overwritten on restart.
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test_restart_wal");
        std::fs::create_dir_all(&wal_path).unwrap();

        // Phase 1: Write 200 events and process 100
        {
            let wal_queue = WalQueue::new(&wal_path).unwrap();
            println!("Phase 1: Created WAL queue, total_written={}", wal_queue.get_total_written());

            // Write 200 events
            for seq in 0..200 {
                let update = SubscribeUpdate {
                    ..Default::default()
                };
                wal_queue.push_update(seq, &update).unwrap();
            }

            // Mark first 100 as processed (simulating consumer checkpoint at seq 99)
            wal_queue.mark_processed(100, 99).unwrap();

            // Verify state before "crash"
            let total_written = wal_queue.get_total_written();
            println!("After writing 200 events: total_written={}, last_processed_seq={}, unprocessed={}",
                      total_written, wal_queue.get_last_processed_seq(), wal_queue.get_unprocessed_count());
            assert_eq!(total_written, 200, "Should have written exactly 200 events");
            assert_eq!(wal_queue.get_last_processed_seq(), 100);
            assert_eq!(wal_queue.get_unprocessed_count(), 100);
        }

        // Phase 2: Simulate crash/restart by opening WAL again
        // This is the critical test: producer cursor must start at 200, not 100
        {
            let wal_queue = WalQueue::new(&wal_path).unwrap();

            // CRITICAL: Producer cursor must be at 200 (max_existing_seq=199 + 1)
            // NOT at 100 (consumer_checkpoint=99 + 1)
            assert_eq!(wal_queue.get_total_written(), 200,
                       "Producer cursor should start at max_existing_seq + 1 (200), not consumer_checkpoint + 1 (100)");

            // Consumer checkpoint should still be at 100
            assert_eq!(wal_queue.get_last_processed_seq(), 100,
                       "Consumer checkpoint should be preserved");

            // Should have 100 unprocessed events (seq 100-199)
            assert_eq!(wal_queue.get_unprocessed_count(), 100,
                       "All unprocessed events should be preserved");

            // Verify events 100-199 are still readable (not overwritten)
            for expected_seq in 100..200 {
                let entry = wal_queue.read_from(0, expected_seq).unwrap()
                    .expect(&format!("Event {} should still exist", expected_seq));
                assert_eq!(entry.seq, expected_seq,
                           "Event sequence should match expected value");
            }

            // Now write new event starting from cursor 200
            let update = SubscribeUpdate {
                ..Default::default()
            };
            let new_seq = wal_queue.push_update(200, &update).unwrap();
            assert_eq!(new_seq, 200, "New event should be written at sequence 200");

            // Verify new event is at sequence 200 (not overwriting 100)
            let new_entry = wal_queue.read_from(0, 200).unwrap()
                .expect("New event should be at sequence 200");
            assert_eq!(new_entry.seq, 200);

            // Verify event 100 still exists (proves no overwrite occurred)
            let old_entry = wal_queue.read_from(0, 100).unwrap()
                .expect("Event 100 should still exist (not overwritten)");
            assert_eq!(old_entry.seq, 100);
        }
    }
}
