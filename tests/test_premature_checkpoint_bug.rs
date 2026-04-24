//! Test to demonstrate WAL premature checkpoint bug
//!
//! THE BUG:
//!   mark_processed() is called BEFORE batch is flushed to database.
//!   This causes permanent data loss if crash occurs between marking and flushing.
//!
//! Run with: cargo test --test test_premature_checkpoint_bug -- --nocapture

use std::sync::Arc;

// Mock storage sink that simulates a database
struct MockStorageSink {
    committed_seqs: std::sync::Mutex<Vec<u64>>,
    fail_commit: std::sync::atomic::AtomicBool,
}

impl MockStorageSink {
    fn new() -> Self {
        Self {
            committed_seqs: std::sync::Mutex::new(Vec::new()),
            fail_commit: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn simulate_failure(&self) {
        self.fail_commit.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn clear_failure(&self) {
        self.fail_commit.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn is_committed(&self, seq: u64) -> bool {
        self.committed_seqs.lock().unwrap().contains(&seq)
    }

    fn get_last_committed_seq(&self) -> u64 {
        let seqs = self.committed_seqs.lock().unwrap();
        seqs.iter().copied().max().unwrap_or(0)
    }
}

// Mock batch writer (simplified version of BatchWriter)
struct MockBatchWriter {
    buffer: Vec<u64>, // Store seqs for simplicity
    batch_size: usize,
}

impl MockBatchWriter {
    fn new(batch_size: usize) -> Self {
        Self {
            buffer: Vec::new(),
            batch_size,
        }
    }

    fn push(&mut self, seq: u64) {
        self.buffer.push(seq);
    }

    fn should_flush(&self) -> bool {
        self.buffer.len() >= self.batch_size
    }

    fn flush(&mut self) -> Vec<u64> {
        let batch = std::mem::replace(&mut self.buffer, Vec::new());
        batch
    }

    fn peek(&self) -> Vec<u64> {
        self.buffer.clone()
    }
}

// Simplified WAL implementation (just tracks checkpoint)
struct MockWalQueue {
    checkpoint: std::sync::Mutex<u64>,
}

impl MockWalQueue {
    fn new() -> Self {
        Self {
            checkpoint: std::sync::Mutex::new(0),
        }
    }

    fn mark_processed(&self, _slot: u64, seq: u64) -> Result<(), String> {
        *self.checkpoint.lock().unwrap() = seq + 1; // Next unprocessed seq
        Ok(())
    }

    fn get_last_processed_seq(&self) -> u64 {
        *self.checkpoint.lock().unwrap()
    }
}

// Simulates the BUGGY pipeline from wal_consumer.rs
struct BuggyPipeline {
    wal: Arc<MockWalQueue>,
    writer: MockBatchWriter,
    sink: Arc<MockStorageSink>,
}

impl BuggyPipeline {
    fn new(wal: Arc<MockWalQueue>, writer: MockBatchWriter, sink: Arc<MockStorageSink>) -> Self {
        Self { wal, writer, sink }
    }

    /// THIS IS THE BUGGY CODE from wal_consumer.rs lines 72-80
    fn process_entry(&mut self, seq: u64) -> Result<(), String> {
        // Step 1: Add to in-memory buffer
        self.writer.push(seq);
        println!("  ✓ Seq {} added to buffer", seq);

        // Step 2: BUG - Mark as processed BEFORE DB commit
        // This is what happens in wal_consumer.rs line 78
        self.wal.mark_processed(seq, seq)?;
        println!("  ✗ Seq {} marked as processed in WAL (CHECKPOINT ADVANCED)", seq);

        Ok(())
    }

    fn flush_if_needed(&mut self) -> Result<(), String> {
        if self.writer.should_flush() {
            self.process_batch()?;
        }
        Ok(())
    }

    fn process_batch(&mut self) -> Result<(), String> {
        let batch = self.writer.flush();

        if self.sink.fail_commit.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("Database commit failed".to_string());
        }

        // Simulate DB commit
        let mut committed = self.sink.committed_seqs.lock().unwrap();
        for seq in batch {
            committed.push(seq);
            println!("  ✓ Seq {} committed to database", seq);
        }

        Ok(())
    }
}

// Simulates the FIXED pipeline
struct FixedPipeline {
    wal: Arc<MockWalQueue>,
    writer: MockBatchWriter,
    sink: Arc<MockStorageSink>,
}

impl FixedPipeline {
    fn new(wal: Arc<MockWalQueue>, writer: MockBatchWriter, sink: Arc<MockStorageSink>) -> Self {
        Self { wal, writer, sink }
    }

    /// FIXED CODE - mark_processed called AFTER DB commit
    fn process_entry(&mut self, seq: u64) -> Result<(), String> {
        // Step 1: Add to in-memory buffer
        self.writer.push(seq);
        println!("  ✓ Seq {} added to buffer (NOT marked yet)", seq);

        // Step 2: DON'T mark yet - wait for DB commit
        Ok(())
    }

    fn flush_if_needed(&mut self) -> Result<(), String> {
        if self.writer.should_flush() {
            self.process_batch()?;
        }
        Ok(())
    }

    fn process_batch(&mut self) -> Result<(), String> {
        // Peek at buffer without draining (for potential retry)
        let batch = self.writer.peek();

        if batch.is_empty() {
            return Ok(());
        }

        // Step 1: Check if commit will fail BEFORE draining buffer
        if self.sink.fail_commit.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("Database commit failed".to_string());
        }

        // Step 2: Commit to database FIRST
        let mut committed = self.sink.committed_seqs.lock().unwrap();
        for seq in &batch {
            committed.push(*seq);
            println!("  ✓ Seq {} committed to database", seq);
        }
        drop(committed);

        // Step 3: Only NOW drain buffer and checkpoint
        let batch = self.writer.flush();
        for seq in batch {
            self.wal.mark_processed(seq, seq)?;
            println!("  ✓ Seq {} marked as processed in WAL (AFTER DB commit)", seq);
        }

        Ok(())
    }
}

#[test]
fn test_bug_permanent_data_loss() {
    println!("\n{:=<70}", '=');
    println!("BUG TEST: Permanent Data Loss from Premature Checkpoint");
    println!("{:=<70}\n", '=');

    let wal = Arc::new(MockWalQueue::new());
    let sink = Arc::new(MockStorageSink::new());
    let writer = MockBatchWriter::new(10); // Large batch, won't flush yet
    let mut pipeline = BuggyPipeline::new(wal.clone(), writer, sink.clone());

    println!("Phase 1: Process 3 events (add to buffer, mark as done)\n");

    for seq in 0..3 {
        pipeline.process_entry(seq).unwrap();
    }

    println!("\nPhase 2: Simulate crash (before DB flush)\n");
    println!("💥 System crashes (power loss, OOM, etc.)");

    let wal_checkpoint = wal.get_last_processed_seq();
    let db_last_committed = sink.get_last_committed_seq();

    println!("\nPhase 3: After restart\n");
    println!("  WAL checkpoint: {}", wal_checkpoint);
    println!("  DB last committed: {}", db_last_committed);
    println!();

    // THE BUG: Checkpoint advanced but DB has nothing
    assert_eq!(wal_checkpoint, 3, "WAL checkpoint should be at seq 3");
    assert_eq!(db_last_committed, 0, "DB should have no commits");

    // Verify data loss
    assert!(!sink.is_committed(0), "Seq 0 should NOT be committed (LOST)");
    assert!(!sink.is_committed(1), "Seq 1 should NOT be committed (LOST)");
    assert!(!sink.is_committed(2), "Seq 2 should NOT be committed (LOST)");

    println!("  ❌ PERMANENT DATA LOSS: seqs [0, 1, 2] lost forever");
    println!("  ❌ BUG: WAL checkpoint advanced BEFORE DB commit");
    println!("  ❌ On restart, WAL resumes from seq 3, skipping seqs 0-2\n");

    println!("{:=<70}\n", '=');
}

#[test]
fn test_fix_no_data_loss() {
    println!("\n{:=<70}", '=');
    println!("FIX TEST: No Data Loss with Correct Checkpoint Ordering");
    println!("{:=<70}\n", '=');

    let wal = Arc::new(MockWalQueue::new());
    let sink = Arc::new(MockStorageSink::new());
    let writer = MockBatchWriter::new(10);
    let mut pipeline = FixedPipeline::new(wal.clone(), writer, sink.clone());

    println!("Phase 1: Process 3 events (add to buffer, DON'T mark yet)\n");

    for seq in 0..3 {
        pipeline.process_entry(seq).unwrap();
    }

    println!("\nPhase 2: Flush batch to database\n");
    pipeline.process_batch().unwrap();

    println!("\nPhase 3: Simulate crash (AFTER DB commit and checkpoint)\n");
    println!("💥 System crashes");

    let wal_checkpoint = wal.get_last_processed_seq();
    let db_last_committed = sink.get_last_committed_seq();

    println!("\nPhase 4: After restart\n");
    println!("  WAL checkpoint: {}", wal_checkpoint);
    println!("  DB last committed: {}", db_last_committed);
    println!();

    // THE FIX: Both checkpoint and DB are in sync
    assert_eq!(wal_checkpoint, 3, "WAL checkpoint should be at seq 3");
    assert_eq!(db_last_committed, 2, "DB should have committed up to seq 2");

    // Verify no data loss
    assert!(sink.is_committed(0), "Seq 0 should be committed");
    assert!(sink.is_committed(1), "Seq 1 should be committed");
    assert!(sink.is_committed(2), "Seq 2 should be committed");

    println!("  ✅ NO DATA LOSS: seqs [0, 1, 2] are safe in database");
    println!("  ✅ FIX: WAL checkpoint advanced AFTER DB commit");
    println!("  ✅ On restart, WAL resumes from seq 3, all data safe\n");

    println!("{:=<70}\n", '=');
}

#[test]
fn test_db_failure_preserves_checkpoint() {
    println!("\n{:=<70}", '=');
    println!("FAILURE TEST: DB Failure Preserves WAL Checkpoint");
    println!("{:=<70}\n", '=');

    let wal = Arc::new(MockWalQueue::new());
    let sink = Arc::new(MockStorageSink::new());
    let writer = MockBatchWriter::new(3);
    let mut pipeline = FixedPipeline::new(wal.clone(), writer, sink.clone());

    println!("Phase 1: Process 3 events (add to buffer)\n");

    for seq in 0..3 {
        pipeline.process_entry(seq).unwrap();
    }

    println!("\nPhase 2: Simulate DB failure\n");
    sink.simulate_failure();

    println!("Phase 3: Try to flush (should fail)\n");
    let flush_result = pipeline.process_batch();

    assert!(flush_result.is_err(), "Flush should fail");

    println!("  ✗ Flush failed (as expected)");

    // CRITICAL: Checkpoint should NOT advance
    let wal_checkpoint = wal.get_last_processed_seq();
    assert_eq!(wal_checkpoint, 0, "WAL checkpoint should NOT advance on DB failure");
    println!("  ✅ WAL checkpoint still at 0 (correct!)");

    println!("\nPhase 4: Clear failure and retry\n");
    sink.clear_failure();
    pipeline.process_batch().unwrap();

    let wal_checkpoint_after = wal.get_last_processed_seq();
    assert_eq!(wal_checkpoint_after, 3, "WAL checkpoint should advance after successful commit");
    println!("  ✅ WAL checkpoint advanced to 3 after successful commit");

    println!("\nPhase 5: Verify all data committed\n");
    assert!(sink.is_committed(0));
    assert!(sink.is_committed(1));
    assert!(sink.is_committed(2));
    println!("  ✅ All seqs [0, 1, 2] committed to database");

    println!("\n  ✅ FIX: Checkpoint only advances after successful DB commit");
    println!("  ✅ Events can be retried on failure without data loss\n");

    println!("{:=<70}\n", '=');
}
