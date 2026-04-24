# Chaos Engineering Tests

This directory contains tests to verify the indexer's resilience to failures and crashes.

## Crash-Restart Tests

These tests verify that the WAL (Write-Ahead Log) correctly preserves unprocessed events during crashes and restarts.

### Critical Bug Fixed

**Problem**: The original code initialized the producer cursor from the consumer checkpoint (`last_flushed_seq + 1`), which would overwrite unprocessed events on restart.

**Scenario that caused data loss**:
1. Write 200 events (sequences 0-199)
2. Process 100 events (checkpoint at seq 99)
3. Crash/restart
4. Producer cursor incorrectly starts at 100 (from checkpoint)
5. Events 100-199 get overwritten by new events ❌

**Fix**: Now scans the actual keyspace for `max_existing_seq` and initializes producer cursor from `max_existing_seq + 1` (not from checkpoint).

### Test Scripts

#### `test_crash_restart_v2.sh`
Manual test script that:
1. Starts the indexer for 20 seconds
2. Kills it (simulating a crash)
3. Restarts and verifies proper WAL recovery

**Usage**:
```bash
./scripts/chaos/test_crash_restart_v2.sh
```

**What to look for**:
- `WAL recovered` message on restart (indicates recovery from existing WAL)
- `max_existing_seq` value (should be > 0 on restart)
- `producer_cursor` value (should be `max_existing_seq + 1`)
- No data loss indicators in logs

#### `test_crash_restart.sh`
Original crash test script (legacy, kept for reference).

### Running Tests

Make sure you have:
1. TimescaleDB running: `docker compose up -d`
2. Database migrations applied: `./scripts/run-migrations.sh`
3. Geyser endpoint configured in `.env`

Then run:
```bash
cd scripts/chaos
./test_crash_restart_v2.sh
```

### Expected Behavior

**Correct recovery**:
```
# Phase 1: Initial run
WAL init: max_existing_seq=0, producer_cursor=0
Events processed: 200

# Crash
Killed after 20 seconds

# Phase 2: Restart
WAL init: max_existing_seq=199, producer_cursor=200  ← Correct!
WAL recovered: max_seq=199, producer_cursor=200, consumer_checkpoint=100, unprocessed_events=100
```

**Incorrect recovery (old bug)**:
```
# Phase 2: Restart (buggy behavior)
WAL init: producer_cursor=100  ← WRONG! Should be 200
Events 100-199 overwritten ❌
```

### Related Code

- WAL queue: `src/geyser/wal_queue.rs`
- Unit test: `test_restart_preserves_unprocessed_events` in `src/geyser/wal_queue.rs`
- Main entry point: `src/main.rs`

### Safety

These tests are safe to run on devnet/staging. They simulate crashes but don't modify production data.
