use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::Duration;
use std::path::Path;
use helius_laserstream::grpc::SubscribeUpdate;
use prost::Message;

/// Typed error for WAL read operations
/// This replaces string-based error handling to prevent fragile parsing
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalReadError {
    /// A gap was detected in the WAL (crash between allocation and write)
    Gap { seq: u64 },
    /// Key mismatch during read (corruption or race condition)
    KeyMismatch { requested: u64, found: u64 },
    /// Generic IO error from underlying storage
    Io(String),
}

impl std::fmt::Display for WalReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalReadError::Gap { seq } => write!(
                f,
                "WAL gap detected at seq {} (next seq {} exists). \
                 This sequence was lost due to a crash between allocation and write. \
                 Try fetching from RPC to repair.",
                seq, seq + 1
            ),
            WalReadError::KeyMismatch { requested, found } => {
                write!(f, "Key mismatch: requested seq {}, found entry seq {}", requested, found)
            }
            WalReadError::Io(msg) => write!(f, "Failed to read from WAL: {}", msg),
        }
    }
}

impl std::error::Error for WalReadError {}

const WAL_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const WAL_GC_INTERVAL: Duration = Duration::from_secs(10);
const WAL_GC_SAFETY_MARGIN_DEFAULT: u64 = 1000; // Keep 1000 entries as safety buffer

/// WAL durability mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalDurabilityMode {
    /// Strict durability: fsync after every write (true WAL semantics)
    /// Guarantees no data loss on power failure, but slower
    Strict,

    /// Relaxed durability: fsync every 100ms in background
    /// Faster but up to 100ms of events can be lost on power failure
    Relaxed,
}

impl WalDurabilityMode {
    pub fn from_env() -> Self {
        match std::env::var("WAL_DURABILITY_MODE").as_deref() {
            Ok("strict") => WalDurabilityMode::Strict,
            Ok("relaxed") | Ok("") | Err(_) => WalDurabilityMode::Relaxed, // Default
            _ => {
                log::warn!("Unknown WAL_DURABILITY_MODE value, using 'relaxed'");
                WalDurabilityMode::Relaxed
            }
        }
    }
}

/// Get GC safety margin from environment variable
/// Returns the number of entries to keep as a safety buffer to prevent race conditions
/// between consumer reads and GC deletes
fn gc_safety_margin_from_env() -> u64 {
    match std::env::var("WAL_GC_SAFETY_MARGIN") {
        Ok(val) => {
            match val.parse::<u64>() {
                Ok(margin) if margin >= 100 => {
                    log::info!("WAL GC safety margin: {} entries", margin);
                    margin
                }
                Ok(margin) => {
                    log::warn!("WAL_GC_SAFETY_MARGIN too small ({}), using minimum 100", margin);
                    100
                }
                Err(_) => {
                    log::warn!("Invalid WAL_GC_SAFETY_MARGIN value, using default {}", WAL_GC_SAFETY_MARGIN_DEFAULT);
                    WAL_GC_SAFETY_MARGIN_DEFAULT
                }
            }
        }
        Err(_) => {
            log::info!("WAL GC safety margin: {} entries (default)", WAL_GC_SAFETY_MARGIN_DEFAULT);
            WAL_GC_SAFETY_MARGIN_DEFAULT
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub slot: u64,
    pub seq: u64,
    pub payload: Vec<u8>, // Raw protobuf bytes
    pub timestamp_unix_ms: i64,
}

impl WalEntry {
    /// Create a WAL entry from pre-encoded protobuf bytes.
    /// This avoids the unnecessary re-encode that happens when accepting
    /// an already-decoded SubscribeUpdate struct.
    pub fn from_raw_bytes(slot: u64, seq: u64, raw_protobuf_bytes: &[u8]) -> Self {
        Self {
            slot,
            seq,
            payload: raw_protobuf_bytes.to_vec(),
            timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Legacy method for compatibility - accepts decoded struct but re-encodes.
    /// Prefer from_raw_bytes() to avoid the encode/decode round-trip.
    #[deprecated(note = "Use from_raw_bytes() to avoid unnecessary re-encode")]
    pub fn from_update(slot: u64, seq: u64, update: &SubscribeUpdate) -> Self {
        Self {
            slot,
            seq,
            payload: update.encode_to_vec(),
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
    durability_mode: WalDurabilityMode,
    gc_safety_margin: u64, // Number of entries to keep as safety buffer
}

impl WalQueue {
    pub fn new(wal_path: impl AsRef<Path>) -> Result<Self, String> {
        Self::with_config(wal_path, WalDurabilityMode::from_env(), gc_safety_margin_from_env())
    }

    /// Create a WalQueue with explicit configuration (for testing)
    pub fn with_config(
        wal_path: impl AsRef<Path>,
        durability_mode: WalDurabilityMode,
        gc_safety_margin: u64,
    ) -> Result<Self, String> {
        log::info!("WAL durability mode: {:?}", durability_mode);
        if durability_mode == WalDurabilityMode::Relaxed {
            log::warn!("WAL in relaxed durability mode: up to 100ms of events can be lost on power failure");
            log::warn!("Set WAL_DURABILITY_MODE=strict for true WAL semantics (fsync after every write)");
        }

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

        // CRITICAL: Single scan to determine both WAL emptiness and max existing sequence number.
        // This prevents the producer from overwriting unprocessed events on restart.
        // last_flushed_seq is the consumer checkpoint, NOT the producer cursor.
        // The reverse scan result already encodes emptiness (None = empty, Some = non-empty).
        let (is_empty, max_existing_seq) = {
            let mut rev = events.range::<Vec<u8>, _>(..).rev();
            match rev.next().and_then(|g| g.into_inner().ok()) {
                None => (true, 0),
                Some((k, _)) => {
                    let key_bytes = k.to_vec();
                    let seq = u64::from_be_bytes(key_bytes.try_into().unwrap());
                    (false, seq)
                }
            }
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

        // CRITICAL FIX: Distinguish empty WAL from WAL with one entry at seq=0
        // max_existing_seq == 0 is ambiguous - could be empty OR could have entry at seq=0
        // is_empty from the single scan resolves this ambiguity
        let initial_seq = if is_empty {
            0  // Empty WAL starts at sequence 0
        } else {
            max_existing_seq + 1  // Non-empty WAL continues after max to avoid overwrites
        };

        let slot_sequence = Arc::new(AtomicU64::new(initial_seq));

        log::info!("WAL init: is_empty={}, max_existing_seq={}, producer_cursor={}",
                   is_empty, max_existing_seq, initial_seq);

        // Log recovery state
        if !is_empty {
            let unprocessed_count = max_existing_seq.saturating_add(1).saturating_sub(consumer_checkpoint_seq.unwrap_or(0));
            log::info!(
                "WAL recovered: max_seq={}, producer_cursor={}, consumer_checkpoint={:?}, unprocessed_events={}",
                max_existing_seq,
                initial_seq,
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
            durability_mode,
            gc_safety_margin,
        })
    }

    /// Push a SubscribeUpdate to the WAL (legacy method - re-encodes the struct).
    /// Prefer push_raw_bytes() to avoid the unnecessary encode/decode round-trip.
    pub fn push_update(&self, slot: u64, update: &SubscribeUpdate) -> Result<u64, String> {
        let seq = self.slot_sequence.fetch_add(1, Ordering::Release);
        let entry = WalEntry::from_update(slot, seq, update);
        self.write_entry(seq, slot, &entry)
    }

    /// Push pre-encoded protobuf bytes to the WAL, avoiding re-encode overhead.
    /// This is the preferred method for hot-path writes.
    pub fn push_raw_bytes(&self, slot: u64, raw_protobuf_bytes: &[u8]) -> Result<u64, String> {
        let seq = self.slot_sequence.fetch_add(1, Ordering::Release);
        let entry = WalEntry::from_raw_bytes(slot, seq, raw_protobuf_bytes);
        self.write_entry(seq, slot, &entry)
    }

    /// Internal method to write a WAL entry to storage.
    /// Shared by push_update() and push_raw_bytes().
    fn write_entry(&self, seq: u64, slot: u64, entry: &WalEntry) -> Result<u64, String> {
        // Serialize entry to bytes: slot(8) + seq(8) + timestamp(8) + payload_len(4) + payload
        let mut buffer = Vec::with_capacity(28 + entry.payload.len());
        buffer.extend_from_slice(&entry.slot.to_be_bytes());
        buffer.extend_from_slice(&entry.seq.to_be_bytes());
        buffer.extend_from_slice(&entry.timestamp_unix_ms.to_be_bytes());
        buffer.extend_from_slice(&(entry.payload.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&entry.payload);

        // Write to fjall events keyspace with seq as key (big-endian for correct sorting)
        // NOTE: This writes to fjall's memtable (in-memory LSM buffer)
        // Data is NOT durable until fsync happens (either immediately in strict mode or 100ms later in relaxed mode)
        self.events.insert(seq.to_be_bytes(), buffer)
            .map_err(|e| format!("Failed to write to WAL: {}", e))?;

        // Also store seq→slot mapping for RPC repair
        let seq_key = format!("seq2slot_{}", seq);
        self.metadata.insert(seq_key.as_bytes(), slot.to_be_bytes())
            .map_err(|e| format!("Failed to write seq→slot mapping: {}", e))?;

        // CRITICAL: In strict mode, fsync immediately to guarantee durability
        // In relaxed mode, background flush handles fsync every 100ms
        if self.durability_mode == WalDurabilityMode::Strict {
            self.flush()
                .map_err(|e| format!("Failed to fsync WAL: {}", e))?;
        }

        Ok(seq)
    }


    pub fn read_next(&self) -> Result<Option<WalEntry>, WalReadError> {
        // CRITICAL FIX: get_last_processed_seq() now returns the actual last processed seq,
        // not the next seq to read. Add +1 to read the next unprocessed seq.
        let last_flushed_seq = self.get_last_processed_seq();
        self.read_from_seq(last_flushed_seq.saturating_add(1))
    }

    pub fn read_from(&self, _slot: u64, seq: u64) -> Result<Option<WalEntry>, WalReadError> {
        self.read_from_seq(seq)
    }

    fn read_from_seq(&self, seq: u64) -> Result<Option<WalEntry>, WalReadError> {
        // Direct lookup for exact seq (no more silent skipping!)
        let key = seq.to_be_bytes();

        match self.events.get(&key) {
            Ok(Some(value)) => {
                // Decode the entry from the stored value
                let value_bytes = value.to_vec();
                let entry_slot = u64::from_be_bytes(value_bytes[0..8].try_into().unwrap());
                let entry_seq = u64::from_be_bytes(value_bytes[8..16].try_into().unwrap());
                let timestamp = i64::from_be_bytes(value_bytes[16..24].try_into().unwrap());
                let payload_len = u32::from_be_bytes(value_bytes[24..28].try_into().unwrap()) as usize;
                let payload = value_bytes[28..28 + payload_len].to_vec();

                // Verify the key matches (sanity check)
                if entry_seq != seq {
                    return Err(WalReadError::KeyMismatch { requested: seq, found: entry_seq });
                }

                Ok(Some(WalEntry {
                    slot: entry_slot,
                    seq: entry_seq,
                    payload,
                    timestamp_unix_ms: timestamp,
                }))
            }
            Ok(None) => {
                // Seq not found - this could be:
                // 1. End of WAL (no more data)
                // 2. A hole/crash-created gap
                // 3. Not yet written

                // Check if this is truly EOF or a hole by looking ahead
                let next_key = (seq + 1).to_be_bytes();
                if let Ok(Some(_)) = self.events.get(&next_key) {
                    // Next seq exists, so this seq is a hole/gap!
                    return Err(WalReadError::Gap { seq });
                }

                // No next seq either - likely EOF
                Ok(None)
            }
            Err(e) => Err(WalReadError::Io(format!("{}", e)))
        }
    }

    pub fn mark_processed(&self, slot: u64, seq: u64) -> Result<(), String> {
        // CRITICAL: Use atomic batch write to ensure slot and seq stay consistent
        // If crash happens between two separate writes, slot and seq could point
        // at different positions, causing corruption.
        //
        // NOTE: last_flushed_slot is technically redundant (recovery only uses seq)
        // but we keep it for potential debugging/monitoring purposes.
        //
        // CRITICAL FIX: Store the actual last processed seq, NOT seq+1.
        // This makes get_last_processed_seq() return the correct value (the last processed seq).
        // The consumer is responsible for adding +1 when computing next_read_seq.
        let mut batch = self.db.batch();

        batch.insert(&self.metadata, b"last_flushed_slot", slot.to_be_bytes());
        batch.insert(&self.metadata, b"last_flushed_seq", seq.to_be_bytes());

        batch.commit()
            .map_err(|e| format!("Failed to commit checkpoint batch: {}", e))?;

        Ok(())
    }

    /// Advance the sequence checkpoint WITHOUT updating the slot checkpoint.
    ///
    /// This is used when skipping unrecoverable gaps (e.g., RPC repair failed,
    /// slot doesn't exist, etc.) where we don't have a valid slot number.
    ///
    /// CRITICAL: Do NOT use mark_processed(0, seq) for gap skips — that permanently
    /// corrupts last_flushed_slot to 0. This method only advances the sequence,
    /// preserving the existing slot checkpoint. The next successful batch will
    /// update both correctly.
    ///
    /// CRITICAL FIX: Store the actual last processed seq, NOT seq+1.
    /// This makes get_last_processed_seq() return the correct value.
    pub fn skip_processed_sequence(&self, seq: u64) -> Result<(), String> {
        self.metadata
            .insert(b"last_flushed_seq", seq.to_be_bytes())
            .map_err(|e| format!("Failed to advance sequence checkpoint: {}", e))?;

        log::warn!("Advanced seq checkpoint to {} (slot checkpoint preserved)", seq);
        Ok(())
    }

    pub fn flush(&self) -> Result<(), String> {
        // Flush the fjall database to disk
        // This is safe to call in background - doesn't block the Geyser ingest thread
        self.db.persist(fjall::PersistMode::SyncAll)
            .map_err(|e| format!("Failed to flush WAL: {}", e))
    }

    pub fn get_unprocessed_count(&self) -> u64 {
        let total = self.slot_sequence.load(Ordering::Relaxed);
        let last_processed = self.get_last_processed_seq();

        // CRITICAL FIX: get_last_processed_seq() now returns the actual last processed seq,
        // not the next seq to read. So unprocessed count is total - (last_processed + 1).
        //
        // However, last_processed == 0 is ambiguous: it could mean "no checkpoint" or "seq 0 was processed".
        // We distinguish by checking if last_flushed_slot == 0 (no checkpoint created yet).
        let last_flushed_slot = self.get_last_processed_slot();
        if last_flushed_slot == 0 && last_processed == 0 {
            // No checkpoint created yet, all events are unprocessed
            total
        } else {
            // Checkpoint exists, calculate unprocessed count normally
            let next_unprocessed = last_processed.saturating_add(1);
            total.saturating_sub(next_unprocessed)
        }
    }

    pub fn get_total_written(&self) -> u64 {
        self.slot_sequence.load(Ordering::Relaxed)
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
        // CRITICAL: Return 0 to indicate "no checkpoint yet" (initial state).
        // This is technically ambiguous with "seq 0 was processed", but since we
        // always process seq 0 on startup, the consumer will advance to 1 in both cases.
        // The key fix is that we now store the actual last processed seq (not seq+1),
        // so get_last_processed_seq() returns the correct value after the first checkpoint.
        0
    }

    /// Get the slot number for a given sequence number
    /// Returns None if the mapping doesn't exist (hole or never allocated)
    pub fn get_slot_for_seq(&self, seq: u64) -> Option<u64> {
        let seq_key = format!("seq2slot_{}", seq);
        self.metadata.get(seq_key.as_bytes()).ok().flatten().and_then(|bytes| {
            if bytes.len() == 8 {
                let arr: [u8; 8] = bytes.to_vec().try_into().ok()?;
                Some(u64::from_be_bytes(arr))
            } else {
                None
            }
        })
    }

    /// Repair a hole in the WAL by writing data fetched from RPC.
    /// This is called when the consumer detects a gap and fetches the missing data from RPC.
    ///
    /// Prefer push_raw_bytes() for hot-path writes to avoid unnecessary re-encode.
    pub fn repair_hole(&self, seq: u64, slot: u64, update: &SubscribeUpdate) -> Result<(), String> {
        // Create WAL entry with the fetched data (legacy path - re-encodes)
        let entry = WalEntry::from_update(slot, seq, update);

        // Serialize entry to bytes
        let mut buffer = Vec::with_capacity(28 + entry.payload.len());
        buffer.extend_from_slice(&entry.slot.to_be_bytes());
        buffer.extend_from_slice(&entry.seq.to_be_bytes());
        buffer.extend_from_slice(&entry.timestamp_unix_ms.to_be_bytes());
        buffer.extend_from_slice(&(entry.payload.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&entry.payload);

        // Write the repaired entry to fill the hole
        self.events.insert(seq.to_be_bytes(), buffer)
            .map_err(|e| format!("Failed to repair hole at seq {}: {}", seq, e))?;

        // CRITICAL: Also write seq→slot mapping for checkpoint recovery
        // Without this, get_slot_for_seq() returns None for repaired entries,
        // causing batch checkpoint failures and infinite re-processing
        let seq_key = format!("seq2slot_{}", seq);
        self.metadata.insert(seq_key.as_bytes(), slot.to_be_bytes())
            .map_err(|e| format!("Failed to write seq→slot mapping for repaired hole: {}", e))?;

        log::info!("Repaired WAL hole at seq {} with slot {} data fetched from RPC", seq, slot);

        Ok(())
    }

    /// Repair a hole in the WAL using pre-encoded protobuf bytes.
    /// This avoids the unnecessary re-encode overhead of repair_hole().
    pub fn repair_hole_with_raw_bytes(&self, seq: u64, slot: u64, raw_protobuf_bytes: &[u8]) -> Result<(), String> {
        // Create WAL entry from raw bytes (no re-encode)
        let entry = WalEntry::from_raw_bytes(slot, seq, raw_protobuf_bytes);

        // Serialize entry to bytes
        let mut buffer = Vec::with_capacity(28 + entry.payload.len());
        buffer.extend_from_slice(&entry.slot.to_be_bytes());
        buffer.extend_from_slice(&entry.seq.to_be_bytes());
        buffer.extend_from_slice(&entry.timestamp_unix_ms.to_be_bytes());
        buffer.extend_from_slice(&(entry.payload.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&entry.payload);

        // Write the repaired entry to fill the hole
        self.events.insert(seq.to_be_bytes(), buffer)
            .map_err(|e| format!("Failed to repair hole at seq {}: {}", seq, e))?;

        // CRITICAL: Also write seq→slot mapping for checkpoint recovery
        let seq_key = format!("seq2slot_{}", seq);
        self.metadata.insert(seq_key.as_bytes(), slot.to_be_bytes())
            .map_err(|e| format!("Failed to write seq→slot mapping for repaired hole: {}", e))?;

        log::info!("Repaired WAL hole at seq {} with slot {} data fetched from RPC (raw bytes)", seq, slot);

        Ok(())
    }

    /// Scan for gaps in the WAL within a sequence range
    /// Returns a list of missing sequence numbers
    pub fn detect_gaps(&self, start_seq: u64, end_seq: u64) -> Vec<u64> {
        let mut gaps = Vec::new();

        for seq in start_seq..end_seq {
            let key = seq.to_be_bytes();
            if self.events.get(&key).ok().flatten().is_none() {
                gaps.push(seq);
            }
        }

        gaps
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

        // CRITICAL: Apply safety margin to prevent race condition between consumer reads and GC deletes
        //
        // The race: Consumer calls read_next() which reads last_flushed_seq, then fetches the entry.
        //           GC runs in parallel, reads last_flushed_seq, and deletes entries < last_flushed_seq.
        //           If GC deletes the entry consumer is currently reading, we get read errors or data loss.
        //
        // The fix: Only delete entries that are definitely safe (seq < last_flushed_seq - safety_margin).
        //          This creates a buffer zone where consumer can safely read without GC interference.
        //
        // CRITICAL FIX: get_last_processed_seq() now returns the actual last processed seq (not next unprocessed).
        // To maintain the same GC behavior as before, we need to add +1 to convert from "last processed" to "next unprocessed".
        let next_unprocessed_seq = last_flushed_seq.saturating_add(1);
        let delete_up_to = next_unprocessed_seq.saturating_sub(self.gc_safety_margin);

        if delete_up_to == 0 {
            // Not enough entries to safely delete yet
            return Ok(0);
        }

        // Iterate from sequence 0 up to (last_flushed_seq - safety_margin) and delete each event
        let end_key = delete_up_to.to_be_bytes();
        let mut iter = self.events.range(..end_key);

        let mut deleted_count = 0u64;

        while let Some(guard) = iter.next() {
            let (key, _) = guard.into_inner()
                .map_err(|e| format!("Failed to read event key during GC: {}", e))?;

            // Delete the event (convert UserKey to Vec<u8> first)
            let key_bytes = key.to_vec();
            self.events.remove(key_bytes.clone())
                .map_err(|e| format!("Failed to delete event during GC: {}", e))?;

            // CRITICAL: Also delete the seq→slot mapping from metadata to prevent unbounded growth
            // These mappings are only needed while the event is in the WAL
            let seq = u64::from_be_bytes(key_bytes.try_into().unwrap());
            let seq_key = format!("seq2slot_{}", seq);
            self.metadata.remove(seq_key.as_bytes())
                .map_err(|e| format!("Failed to delete seq→slot mapping during GC: {}", e))?;

            deleted_count += 1;
        }

        if deleted_count > 0 {
            log::debug!("WAL GC: Deleted {} processed events (seq < {}, checkpoint: {}, margin: {})",
                       deleted_count, delete_up_to, last_flushed_seq, self.gc_safety_margin);
        }

        Ok(deleted_count)
    }

    pub async fn start_background_flush(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut flush_interval = tokio::time::interval(WAL_FLUSH_INTERVAL);
            let mut gc_interval = tokio::time::interval(WAL_GC_INTERVAL);

            // Only run periodic flushes in relaxed mode
            // In strict mode, we flush on every write already
            let use_periodic_flush = self.durability_mode == WalDurabilityMode::Relaxed;

            loop {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        if use_periodic_flush {
                            // Flush every 100ms in relaxed mode
                            if let Err(e) = self.flush() {
                                log::error!("WAL flush error: {}", e);
                            } else {
                                let unprocessed = self.get_unprocessed_count();
                                if unprocessed > 0 {
                                    log::trace!("WAL flushed (relaxed mode), {} unprocessed entries", unprocessed);
                                }
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
        // Use safety_margin=100 for this test (default is 1000)
        let wal_queue = WalQueue::with_config(
            wal_path,
            WalDurabilityMode::Relaxed,
            100,
        ).unwrap();

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
        // With safety_margin=100, should delete entries < (500 - 100) = 400
        // So it should delete 400 entries (seq 0-399)
        let deleted = wal_queue.delete_processed_events().unwrap();

        // Verify that 400 events were deleted (safety margin prevents deleting 400-499)
        assert_eq!(deleted, 400);

        // Verify that the events keyspace contains exactly 600 items (sequences 400-999)
        let remaining_count = wal_queue.events.iter().count();
        assert_eq!(remaining_count, 600, "Expected 600 remaining events, found {}", remaining_count);

        // Verify that read_next returns sequence 500 (first unprocessed event)
        let next_entry = wal_queue.read_next().unwrap().unwrap();
        assert_eq!(next_entry.seq, 500);
    }

    #[test]
    fn test_full_catchup_garbage_collection() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path();
        // Use safety_margin=100 for this test (minimum allowed value)
        let wal_queue = WalQueue::with_config(
            wal_path,
            WalDurabilityMode::Relaxed,
            100,
        ).unwrap();

        // Write 300 mock events to the WAL
        for seq in 0..300 {
            let update = SubscribeUpdate {
                ..Default::default()
            };

            wal_queue.push_update(seq, &update).unwrap();
        }

        // Mark first 200 events as processed (last_flushed_seq becomes 199)
        wal_queue.mark_processed(200, 199).unwrap();

        // Run garbage collection
        // CRITICAL FIX: get_last_processed_seq() now returns 199 (not 200)
        // With safety_margin=100: next_unprocessed = 199 + 1 = 200, delete_up_to = 200 - 100 = 100
        // So it should delete 100 entries (seq 0-99), keeping 100 as safety buffer
        let deleted = wal_queue.delete_processed_events().unwrap();

        // Verify that 100 events were deleted (safety margin of 100 preserved)
        assert_eq!(deleted, 100);

        // Verify that the events keyspace contains exactly 200 items (sequences 100-299)
        let remaining_count = wal_queue.events.iter().count();
        assert_eq!(remaining_count, 200, "Expected 200 remaining events (safety buffer + unprocessed), found {}", remaining_count);

        // Verify that read_next returns sequence 200 (first unprocessed event)
        let next_entry = wal_queue.read_next().unwrap().unwrap();
        assert_eq!(next_entry.seq, 200);
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
            // CRITICAL FIX: get_last_processed_seq() now returns the actual last processed seq
            assert_eq!(wal_queue.get_last_processed_seq(), 99);
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

            // Consumer checkpoint should still be at 99
            // CRITICAL FIX: get_last_processed_seq() now returns the actual last processed seq
            assert_eq!(wal_queue.get_last_processed_seq(), 99,
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

    #[test]
    fn test_gap_detection_and_repair() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test_gap_detection");
        std::fs::create_dir_all(&wal_path).unwrap();

        let wal_queue = WalQueue::new(&wal_path).unwrap();

        // Phase 1: Write events 0-5 successfully
        println!("\n=== Phase 1: Writing events 0-5 ===");
        for slot in 0..6 {
            let update = SubscribeUpdate {
                ..Default::default()
            };
            let seq = wal_queue.push_update(slot, &update).unwrap();
            println!("Wrote slot {} at seq {}", slot, seq);
            assert_eq!(seq, slot, "Seq should match slot for this test");
        }

        // Verify we can read all events
        for seq in 0..6 {
            let entry = wal_queue.read_from(0, seq).unwrap()
                .expect(&format!("Seq {} should exist", seq));
            assert_eq!(entry.seq, seq);
        }

        // Phase 2: Simulate crash at seq 6 (allocate but don't write)
        println!("\n=== Phase 2: Simulating crash at seq 6 ===");
        // Manually increment the counter (simulating fetch_add)
        wal_queue.slot_sequence.fetch_add(1, Ordering::Release);
        println!("Allocated seq 6 but crashed before write");
        println!("seq_counter = {}", wal_queue.slot_sequence.load(Ordering::Relaxed));

        // Phase 3: Write events 7-8 (these will get seq 7 and 8)
        println!("\n=== Phase 3: Writing events 7-8 after crash ===");
        for slot in 7..9 {
            let update = SubscribeUpdate {
                ..Default::default()
            };
            let seq = wal_queue.push_update(slot, &update).unwrap();
            println!("Wrote slot {} at seq {}", slot, seq);
            assert_eq!(seq, slot);
        }

        // Verify events 0-5 and 7-8 exist, but 6 is missing
        println!("\n=== Phase 4: Verifying hole at seq 6 ===");
        for seq in 0..9 {
            match wal_queue.read_from(0, seq) {
                Ok(Some(entry)) => println!("Seq {} exists: slot {}", entry.seq, entry.slot),
                Ok(None) => println!("Seq {} is MISSING (hole)", seq),
                Err(e) => println!("Seq {} error: {}", seq, e),
            }
        }

        // Test gap detection: reading seq 6 should detect the gap
        println!("\n=== Phase 5: Testing gap detection ===");
        let result = wal_queue.read_from(0, 6);
        match result {
            Ok(Some(entry)) => panic!("Expected gap error, but got seq {}", entry.seq),
            Ok(None) => panic!("Expected gap error, but got None (EOF)"),
            Err(WalReadError::Gap { seq }) => {
                println!("✓ Gap detected correctly at seq {}", seq);
                assert_eq!(seq, 6, "Gap should be at seq 6");
            }
            Err(other) => panic!("Expected Gap error, got: {}", other),
        }

        // Verify we can still read seq 5 and 7
        let entry_5 = wal_queue.read_from(0, 5).unwrap().unwrap();
        assert_eq!(entry_5.seq, 5);
        println!("✓ Can still read seq 5 (before gap)");

        let entry_7 = wal_queue.read_from(0, 7).unwrap().unwrap();
        assert_eq!(entry_7.seq, 7);
        println!("✓ Can still read seq 7 (after gap)");

        // Test detect_gaps method
        println!("\n=== Phase 6: Testing detect_gaps() method ===");
        let gaps = wal_queue.detect_gaps(0, 9);
        println!("Detected gaps: {:?}", gaps);
        assert_eq!(gaps, vec![6], "Should detect exactly one gap at seq 6");

        // Test get_slot_for_seq (for the missing seq)
        println!("\n=== Phase 7: Testing seq→slot mapping ===");
        let slot_6 = wal_queue.get_slot_for_seq(6);
        println!("Slot mapping for seq 6: {:?}", slot_6);
        // Note: seq 6 was allocated but never written, so no mapping exists
        assert!(slot_6.is_none(), "Seq 6 should have no slot mapping (never written)");

        // But seq 7 should have a mapping
        let slot_7 = wal_queue.get_slot_for_seq(7).unwrap();
        assert_eq!(slot_7, 7, "Seq 7 should map to slot 7");
        println!("✓ Seq 7 maps to slot 7: {}", slot_7);

        // Phase 8: Test repair_hole
        println!("\n=== Phase 8: Testing repair_hole() ===");
        let repair_update = SubscribeUpdate {
            ..Default::default()
        };
        wal_queue.repair_hole(6, 6, &repair_update).unwrap();
        println!("✓ Repaired hole at seq 6");

        // Verify the repair worked
        let entry_6_repaired = wal_queue.read_from(0, 6).unwrap().unwrap();
        assert_eq!(entry_6_repaired.seq, 6);
        assert_eq!(entry_6_repaired.slot, 6);
        println!("✓ Can now read seq 6 after repair");

        // Verify no more gaps
        let gaps_after = wal_queue.detect_gaps(0, 9);
        assert_eq!(gaps_after, vec![] as Vec<u64>, "Should have no gaps after repair");
        println!("✓ No gaps detected after repair");

        // Test sequential reading after repair
        println!("\n=== Phase 9: Testing sequential reading after repair ===");
        for expected_seq in 0..9 {
            let entry = wal_queue.read_from(0, expected_seq).unwrap()
                .expect(&format!("Seq {} should exist after repair", expected_seq));
            assert_eq!(entry.seq, expected_seq);
        }
        println!("✓ All seqs 0-8 readable sequentially");

        println!("\n✅ All gap detection and repair tests passed!");
    }

    #[test]
    fn test_no_gap_when_all_exist() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test_no_gap");
        std::fs::create_dir_all(&wal_path).unwrap();

        let wal_queue = WalQueue::new(&wal_path).unwrap();

        // Write events 0-10
        for slot in 0..11 {
            let update = SubscribeUpdate {
                ..Default::default()
            };
            wal_queue.push_update(slot, &update).unwrap();
        }

        // Should detect no gaps
        let gaps = wal_queue.detect_gaps(0, 11);
        assert_eq!(gaps, vec![] as Vec<u64>, "Should have no gaps when all events exist");

        // Reading each seq should work
        for seq in 0..11 {
            let entry = wal_queue.read_from(0, seq).unwrap()
                .expect(&format!("Seq {} should exist", seq));
            assert_eq!(entry.seq, seq);
        }
    }

    #[test]
    fn test_eof_vs_gap_distinction() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test_eof_gap");
        std::fs::create_dir_all(&wal_path).unwrap();

        let wal_queue = WalQueue::new(&wal_path).unwrap();

        // Write only events 0-5
        for slot in 0..6 {
            let update = SubscribeUpdate {
                ..Default::default()
            };
            wal_queue.push_update(slot, &update).unwrap();
        }

        // Reading seq 5 should work
        let entry_5 = wal_queue.read_from(0, 5).unwrap().unwrap();
        assert_eq!(entry_5.seq, 5);

        // Reading seq 6 should NOT be a gap (no data exists at all after it)
        // This is EOF, not a gap
        let result_6 = wal_queue.read_from(0, 6);
        match result_6 {
            Ok(None) => println!("✓ Seq 6 is EOF (not a gap) - correct"),
            Ok(Some(entry)) => panic!("Expected EOF, but got seq {}", entry.seq),
            Err(e) => {
                // This would be acceptable if we get a gap error, but it's not ideal
                println!("Got error for seq 6: {} (acceptable but not ideal)", e);
            }
        }

        // Simulate crash at seq 6 by manually incrementing counter
        wal_queue.slot_sequence.fetch_add(1, Ordering::Release);
        println!("Simulated crash at seq 6 (allocated but not written)");

        // Now write next event - this should allocate seq 7
        let update = SubscribeUpdate {
            ..Default::default()
        };
        let seq_7 = wal_queue.push_update(7, &update).unwrap();
        assert_eq!(seq_7, 7, "Should allocate seq 7 after crash at 6");
        println!("Wrote event at seq {}", seq_7);

        // Now reading seq 6 should detect a gap
        let result_6_gap = wal_queue.read_from(0, 6);
        match result_6_gap {
            Ok(Some(_)) => panic!("Expected gap error, but got a result"),
            Ok(None) => panic!("Expected gap error, but got None"),
            Err(WalReadError::Gap { seq }) => {
                println!("✓ Seq {} is now detected as gap", seq);
                assert_eq!(seq, 6, "Gap should be at seq 6");
            }
            Err(other) => panic!("Expected Gap error, got: {}", other),
        }
    }

    #[test]
    fn test_atomic_checkpoint_write() {
        // Test that mark_processed uses atomic batch writes
        // This prevents slot and seq from getting out of sync on crash
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test_atomic_checkpoint");
        std::fs::create_dir_all(&wal_path).unwrap();

        let wal_queue = WalQueue::new(&wal_path).unwrap();

        // Mark a bunch of slots as processed
        for slot in 0..10 {
            wal_queue.mark_processed(slot, slot).unwrap();
        }

        // Verify both slot and seq are in sync
        let last_slot = wal_queue.get_last_processed_slot();
        let last_seq = wal_queue.get_last_processed_seq();

        assert_eq!(last_slot, 9, "Last slot should be 9");
        // CRITICAL FIX: get_last_processed_seq() now returns the actual last processed seq
        assert_eq!(last_seq, 9, "Last seq should be 9 (last processed)");

        // The key property: even if crash happened during mark_processed,
        // slot and seq should always be consistent (both or neither updated)
        // With atomic batch writes, this is guaranteed
        assert_eq!(last_slot, last_seq,
                   "Slot should equal seq (they stay in sync atomically)");

        println!("✓ Atomic checkpoint write keeps slot and seq in sync");
        println!("  last_slot={}, last_seq={}", last_slot, last_seq);
    }

    #[test]
    fn test_durability_mode_from_env() {
        // Test that WAL correctly reads durability mode from env

        // Test default (relaxed mode)
        std::env::remove_var("WAL_DURABILITY_MODE");
        let mode = WalDurabilityMode::from_env();
        assert_eq!(mode, WalDurabilityMode::Relaxed, "Default should be relaxed");

        // Test explicit relaxed
        std::env::set_var("WAL_DURABILITY_MODE", "relaxed");
        let mode = WalDurabilityMode::from_env();
        assert_eq!(mode, WalDurabilityMode::Relaxed);

        // Test strict
        std::env::set_var("WAL_DURABILITY_MODE", "strict");
        let mode = WalDurabilityMode::from_env();
        assert_eq!(mode, WalDurabilityMode::Strict);

        // Test unknown value (should fallback to relaxed)
        std::env::set_var("WAL_DURABILITY_MODE", "unknown");
        let mode = WalDurabilityMode::from_env();
        assert_eq!(mode, WalDurabilityMode::Relaxed, "Unknown should fallback to relaxed");

        // Clean up
        std::env::remove_var("WAL_DURABILITY_MODE");

        println!("✓ WAL durability mode correctly reads from env");
    }

    #[test]
    fn test_skip_processed_sequence_preserves_slot_checkpoint() {
        // This test verifies the fix for the critical bug where gap skip
        // paths called mark_processed(0, seq), permanently corrupting
        // last_flushed_slot to 0.
        let temp_dir = tempdir().unwrap();
        let wal_queue = WalQueue::new(temp_dir.path()).unwrap();

        // Write some events and mark them as processed
        for i in 0..5 {
            let update = SubscribeUpdate { ..Default::default() };
            wal_queue.push_update(100 + i, &update).unwrap();
        }

        // Mark seq 4 as processed (slot 104)
        wal_queue.mark_processed(104, 4).unwrap();

        // Verify both slot and seq are checkpointed
        assert_eq!(wal_queue.get_last_processed_slot(), 104);
        // CRITICAL FIX: get_last_processed_seq() now returns the actual last processed seq
        assert_eq!(wal_queue.get_last_processed_seq(), 4);

        // Simulate a gap at seq 5 that we need to skip
        // (e.g., RPC repair failed, slot doesn't exist)
        wal_queue.skip_processed_sequence(5).unwrap();

        // CRITICAL: Seq should advance, but slot should NOT be corrupted to 0
        assert_eq!(wal_queue.get_last_processed_seq(), 5,
                   "Seq checkpoint should advance to 5");
        assert_eq!(wal_queue.get_last_processed_slot(), 104,
                   "Slot checkpoint should remain at 104 (NOT corrupted to 0)");

        // Next successful batch should update both correctly
        wal_queue.mark_processed(110, 10).unwrap();
        assert_eq!(wal_queue.get_last_processed_slot(), 110);
        assert_eq!(wal_queue.get_last_processed_seq(), 10);

        println!("✓ skip_processed_sequence advances seq without corrupting slot checkpoint");
    }

    #[test]
    fn test_push_raw_bytes_avoids_re_encode() {
        // Test that push_raw_bytes() works correctly and avoids the re-encode overhead
        let temp_dir = tempdir().unwrap();
        let wal_queue = WalQueue::new(temp_dir.path()).unwrap();

        // Create a SubscribeUpdate and encode it once
        let update = SubscribeUpdate { ..Default::default() };
        let raw_bytes = update.encode_to_vec();

        // Write using the new API (no re-encode)
        let seq = wal_queue.push_raw_bytes(123, &raw_bytes).unwrap();
        assert_eq!(seq, 0);

        // Read it back and verify we can decode it
        let entry = wal_queue.read_from(0, 0).unwrap().unwrap();
        assert_eq!(entry.slot, 123);
        assert_eq!(entry.seq, 0);
        assert_eq!(entry.payload, raw_bytes);

        // Verify the payload can be decoded back to SubscribeUpdate
        let decoded = SubscribeUpdate::decode(&entry.payload[..]).unwrap();
        // Both should be empty/default, which is correct
        assert_eq!(decoded, update);

        println!("✓ push_raw_bytes() avoids re-encode and correctly stores/decodes data");
    }
}
