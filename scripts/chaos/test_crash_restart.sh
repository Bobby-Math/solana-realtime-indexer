#!/bin/bash
# Test script to verify WAL recovery fix with real Geyser stream

set -e

echo "=== Testing WAL Recovery Fix with Real Geyser Stream ==="
echo ""

# Clean up any previous WAL data
echo "🧹 Cleaning up previous test data..."
rm -rf ./data/wal/test_restart*
mkdir -p ./data/wal

# Modify environment to use a test-specific WAL path
export GEYSER_RUN_DURATION_SECONDS=30  # Run for 30 seconds first
echo "⏱️  Phase 1: Starting indexer (will run for 30 seconds)..."
echo ""

# Start the indexer in background, capture PID
timeout 35 ./target/release/solana-realtime-indexer > /tmp/indexer_phase1.log 2>&1 &
INDEXER_PID=$!

echo "Indexer PID: $INDEXER_PID"
echo "Waiting for 30 seconds of ingestion..."
sleep 35

# Check if indexer is still running, kill if necessary
kill $INDEXER_PID 2>/dev/null || true
wait $INDEXER_PID 2>/dev/null || true

echo ""
echo "✅ Phase 1 complete (simulated crash)"
echo ""

# Check WAL state before restart
echo "📊 WAL state after phase 1:"
if [ -d "./data/wal" ]; then
    WAL_DIR=$(find ./data/wal -type d -name "geyser_*" | head -1)
    if [ -n "$WAL_DIR" ]; then
        echo "   WAL directory: $WAL_DIR"
        WAL_SIZE=$(du -sh "$WAL_DIR" | cut -f1)
        echo "   WAL size: $WAL_SIZE"
    fi
fi
echo ""

# Query database for checkpoint info
echo "🔍 Database checkpoint after phase 1:"
psql "$DATABASE_URL" -c "SELECT stream_name, last_processed_slot, last_observed_at, notes FROM ingestion_checkpoints;" 2>/dev/null || echo "   (Could not query database)"
echo ""

# Restart the indexer for 30 more seconds
echo "⏱️  Phase 2: Restarting indexer (recovery test)..."
echo "   This should:"
echo "   - Scan WAL for max existing sequence"
echo "   - Continue producer cursor from max+1 (NOT from checkpoint+1)"
echo "   - Process all unprocessed events from phase 1"
echo ""

timeout 35 ./target/release/solana-realtime-indexer > /tmp/indexer_phase2.log 2>&1 &
INDEXER_PID2=$!

echo "Indexer PID: $INDEXER_PID2"
echo "Waiting for 30 seconds of processing..."
sleep 35

# Check if indexer is still running, kill if necessary
kill $INDEXER_PID2 2>/dev/null || true
wait $INDEXER_PID2 2>/dev/null || true

echo ""
echo "✅ Phase 2 complete"
echo ""

# Final database state
echo "🔍 Final database checkpoint:"
psql "$DATABASE_URL" -c "SELECT stream_name, last_processed_slot, last_observed_at, notes FROM ingestion_checkpoints;" 2>/dev/null || echo "   (Could not query database)"
echo ""

# Check for any data loss indicators
echo "🔍 Checking for data loss indicators..."
if grep -q "Failed to mark entry.*as processed" /tmp/indexer_phase2.log; then
    echo "⚠️  WARNING: Found processing errors in phase 2"
else
    echo "✅ No processing errors detected"
fi

if grep -q "WAL recovered" /tmp/indexer_phase2.log; then
    echo "✅ WAL recovery message found (good!)"
    grep "WAL recovered" /tmp/indexer_phase2.log
else
    echo "⚠️  No WAL recovery message found"
fi

echo ""
echo "=== Test Complete ==="
echo ""
echo "📄 Logs available at:"
echo "   /tmp/indexer_phase1.log"
echo "   /tmp/indexer_phase2.log"
echo ""
echo "💡 To view logs:"
echo "   less /tmp/indexer_phase1.log | grep -E '(WAL|recovered|unprocessed|events)'"
echo "   less /tmp/indexer_phase2.log | grep -E '(WAL|recovered|unprocessed|events)'"
