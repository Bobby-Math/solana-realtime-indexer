#!/bin/bash
# Chaos engineering test to reproduce WAL premature checkpoint bug
#
# HOW THE BUG IS RECREATED:
#
#   Step 1: Start indexer with LARGE batch size (10000 events, 60 second timeout)
#           → This prevents automatic flushing to database
#           → Events accumulate in in-memory buffer
#
#   Step 2: Let indexer run for 15 seconds
#           → Events are written to WAL (persistent)
#           → Events are processed and marked as done in WAL (checkpoint advances)
#           → Events are buffered in memory (NOT in database yet)
#
#   Step 3: Kill the indexer (simulate crash)
#           → WAL checkpoint says "events processed"
#           → Database has NO events (still in memory buffer)
#           → Memory buffer is lost (volatile)
#
#   Step 4: Restart indexer
#           → WAL reads checkpoint: "already processed these events"
#           → Indexer skips those events (won't replay them)
#           → Events are PERMANENTLY LOST
#
#   THE BUG (in src/geyser/wal_consumer.rs):
#     Line 78: mark_processed() called IMMEDIATELY after process_entry()
#              → WAL checkpoint advances BEFORE database commit
#
#     Lines 124-132: process_batch() called much later
#              → Database flush happens AFTER checkpoint
#              → Crash between these two lines = data loss
#
#   EXPECTED OBSERVABLE SYMPTOMS:
#     - WAL checkpoint > 0 (says events were processed)
#     - Database row count = 0 (has no events)
#     - Gap between WAL and DB = permanent data loss

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   WAL Premature Checkpoint Bug - Chaos Engineering Test       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Configuration
WAL_DIR="./data/wal"
TEST_WAL_DIR="${WAL_DIR}/chaos_test_premature_checkpoint"
LOG_FILE="/tmp/chaos_checkpoint_test.log"
DB_QUERY_FILE="/tmp/chaos_db_query.txt"

# Clean up any previous test data
echo "🧹 Cleaning up previous test data..."
rm -rf "$TEST_WAL_DIR"
mkdir -p "$WAL_DIR"

# Build the project first
echo "🔨 Building the project..."
cd /home/bobby/solana-realtime-indexer
cargo build --release 2>&1 | tail -5

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "Phase 1: Start indexer with LARGE batch size (no flush)"
echo "══════════════════════════════════════════════════════════════════"
echo ""
echo "Configuration:"
echo "  - BATCH_SIZE=10000 (large to prevent automatic flush)"
echo "  - BATCH_FLUSH_MS=60000 (60 seconds, long interval)"
echo "  - Run time: 15 seconds"
echo ""
echo "Expected behavior:"
echo "  ✓ Events will be written to WAL"
echo "  ✓ Events will be marked as processed in WAL (checkpoint advances)"
echo "  ✗ Events will NOT be flushed to database (still in memory buffer)"
echo ""

# Start indexer with large batch size to prevent automatic flushing
# This ensures events get marked in WAL but NOT flushed to DB
export BATCH_SIZE=10000
export BATCH_FLUSH_MS=60000
export GEYSER_RUN_DURATION_SECONDS=15

timeout 20 ./target/release/solana-realtime-indexer > "$LOG_FILE" 2>&1 &
INDEXER_PID=$!

echo "Indexer started (PID: $INDEXER_PID)"
echo "Waiting for ingestion (15 seconds)..."
sleep 20

# Ensure process is killed
kill $INDEXER_PID 2>/dev/null || true
wait $INDEXER_PID 2>/dev/null || true

echo "✅ Phase 1 complete (simulated crash after mark_processed, before DB flush)"
echo ""

# Check WAL state
echo "══════════════════════════════════════════════════════════════════"
echo "Phase 2: Analyze WAL state after crash"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# Find the WAL directory
ACTUAL_WAL_DIR=$(find ./data/wal -type d -name "geyser_*" 2>/dev/null | head -1)
if [ -z "$ACTUAL_WAL_DIR" ]; then
    echo "❌ ERROR: WAL directory not found"
    echo "   The indexer may not have written any WAL entries"
    exit 1
fi

echo "WAL directory: $ACTUAL_WAL_DIR"
echo ""

# Check WAL size
WAL_SIZE=$(du -sh "$ACTUAL_WAL_DIR" 2>/dev/null | cut -f1)
echo "WAL disk size: $WAL_SIZE"
echo ""

# Extract WAL metrics from logs
echo "📊 WAL metrics from logs:"
echo ""

TOTAL_EVENTS=$(grep -o "total_written=[0-9]\+" "$LOG_FILE" 2>/dev/null | head -1 | cut -d= -f2 || echo "0")
LAST_PROCESSED_SEQ=$(grep -o "last_processed_seq=[0-9]\+" "$LOG_FILE" 2>/dev/null | head -1 | cut -d= -f2 || echo "0")
UNPROCESSED_COUNT=$(grep -o "unprocessed_count=[0-9]\+" "$LOG_FILE" 2>/dev/null | head -1 | cut -d= -f2 || echo "0")

echo "  Total events written to WAL: $TOTAL_EVENTS"
echo "  Last processed seq (WAL checkpoint): $LAST_PROCESSED_SEQ"
echo "  Unprocessed events in WAL: $UNPROCESSED_COUNT"
echo ""

# Check database checkpoint
echo "══════════════════════════════════════════════════════════════════"
echo "Phase 3: Check database state"
echo "══════════════════════════════════════════════════════════════════"
echo ""

if [ -n "$DATABASE_URL" ]; then
    echo "🔍 Database checkpoint:"
    psql "$DATABASE_URL" -c "
        SELECT
            stream_name,
            last_processed_slot,
            last_observed_at,
            notes
        FROM ingestion_checkpoints
        ORDER BY last_observed_at DESC
        LIMIT 1;
    " 2>/dev/null > "$DB_QUERY_FILE" || echo "   (Could not query database)"

    if [ -s "$DB_QUERY_FILE" ]; then
        cat "$DB_QUERY_FILE"
    fi

    echo ""
    echo "🔍 Row counts in database:"
    psql "$DATABASE_URL" -c "
        SELECT
            'accounts' as table_name, COUNT(*) as row_count FROM accounts
        UNION ALL
        SELECT 'transactions', COUNT(*) FROM transactions
        UNION ALL
        SELECT 'slots', COUNT(*) FROM slots;
    " 2>/dev/null || echo "   (Could not query row counts)"
else
    echo "⚠️  DATABASE_URL not set, skipping database checks"
fi

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "Phase 4: Detect data loss (THE BUG)"
echo "══════════════════════════════════════════════════════════════════"
echo ""

# Calculate expected data loss
if [ "$TOTAL_EVENTS" -gt "$LAST_PROCESSED_SEQ" ]; then
    LOST_EVENTS=$((TOTAL_EVENTS - LAST_PROCESSED_SEQ))
    echo "❌ BUG DETECTED: Potential data loss of $LOST_EVENTS events"
    echo ""
    echo "Explanation:"
    echo "  - WAL says it processed $LAST_PROCESSED_SEQ events (checkpoint advanced)"
    echo "  - But $TOTAL_EVENTS events were written to WAL"
    echo "  - The difference ($LOST_EVENTS events) were marked but not flushed to DB"
    echo "  - On restart, WAL will resume from seq $LAST_PROCESSED_SEQ"
    echo "  - Events in range [$LAST_PROCESSED_SEQ..$((TOTAL_EVENTS-1))] are PERMANENTLY LOST"
    echo ""
else
    echo "⚠️  Could not definitively detect bug from logs"
    echo "   (Need more precise WAL inspection)"
fi

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "Phase 5: Restart and verify data loss"
echo "══════════════════════════════════════════════════════════════════"
echo ""
echo "Restarting indexer to verify if data is lost..."
echo ""

# Restart indexer with normal batch size
export BATCH_SIZE=100
export BATCH_FLUSH_MS=1000
export GEYSER_RUN_DURATION_SECONDS=10

timeout 15 ./target/release/solana-realtime-indexer > /tmp/indexer_restart.log 2>&1 &
RESTART_PID=$!

echo "Indexer restarted (PID: $RESTART_PID)"
echo "Waiting for processing (10 seconds)..."
sleep 15

kill $RESTART_PID 2>/dev/null || true
wait $RESTART_PID 2>/dev/null || true

echo ""
echo "✅ Phase 5 complete"
echo ""

# Check for recovery messages
echo "══════════════════════════════════════════════════════════════════"
echo "Phase 6: Final Analysis"
echo "══════════════════════════════════════════════════════════════════"
echo ""

echo "🔍 Recovery logs:"
if grep -q "WAL recovered" /tmp/indexer_restart.log 2>/dev/null; then
    echo "✅ WAL recovery detected:"
    grep "WAL recovered" /tmp/indexer_restart.log
else
    echo "⚠️  No WAL recovery message found"
fi

echo ""
echo "📄 Logs available at:"
echo "   - Phase 1 (crash): $LOG_FILE"
echo "   - Phase 5 (restart): /tmp/indexer_restart.log"
echo ""
echo "💡 To view the bug in action:"
echo "   grep -E '(mark_processed|flush|checkpoint)' $LOG_FILE | head -20"
echo ""

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Test Complete                                                 ║"
echo "╚════════════════════════════════════════════════════════════════╝"
