#!/bin/bash
# Manual crash-restart test for WAL recovery fix

echo "=== Manual WAL Recovery Test ==="
echo ""
echo "This test will:"
echo "1. Start the indexer and let it ingest events for 20 seconds"
echo "2. Kill it (simulating a crash)"
echo "3. Restart it and verify proper WAL recovery"
echo ""

# Find the WAL directory that will be created
WAL_BASE="./data/wal"

echo "Step 1: Starting indexer for 20 seconds..."
timeout 25 ./target/release/solana-realtime-indexer > /tmp/phase1.log 2>&1 &
PID1=$!
echo "Indexer PID: $PID1"
sleep 20
kill -9 $PID1 2>/dev/null || true
wait $PID1 2>/dev/null || true
echo "✅ Killed after 20 seconds"
echo ""

# Find the WAL directory that was created
WAL_DIR=$(find $WAL_BASE -type d -name "geyser_*" 2>/dev/null | head -1)
if [ -z "$WAL_DIR" ]; then
    echo "❌ ERROR: No WAL directory found!"
    exit 1
fi

echo "Step 2: Checking WAL state..."
echo "   WAL directory: $WAL_DIR"
ls -lh "$WAL_DIR" 2>/dev/null || echo "   (Could not list WAL contents)"
echo ""

# Extract key metrics from phase 1
echo "   Events processed in phase 1:"
grep "events processed" /tmp/phase1.log | tail -1 || echo "   (No event logs found)"
echo ""

echo "Step 3: Restarting indexer (recovery test)..."
timeout 25 ./target/release/solana-realtime-indexer > /tmp/phase2.log 2>&1 &
PID2=$!
echo "Indexer PID: $PID2"
sleep 20
kill -9 $PID2 2>/dev/null || true
wait $PID2 2>/dev/null || true
echo "✅ Killed after 20 seconds"
echo ""

echo "Step 4: Analyzing recovery..."
echo ""

# Check for recovery message
if grep -q "WAL recovered" /tmp/phase2.log; then
    echo "✅ SUCCESS: WAL recovery message found!"
    grep "WAL recovered" /tmp/phase2.log
    echo ""
else
    echo "ℹ️  No WAL recovery message (WAL may have been empty)"
fi

# Check producer cursor initialization
echo "Producer cursor in phase 2:"
grep "WAL init" /tmp/phase2.log || echo "   (No WAL init message)"
echo ""

# Check for any errors
if grep -i "error\|failed\|panic" /tmp/phase2.log | grep -v "Failed to mark" | grep -v "test" > /dev/null; then
    echo "⚠️  Errors found in phase 2:"
    grep -i "error\|failed\|panic" /tmp/phase2.log | grep -v "Failed to mark" | grep -v "test" | head -10
    echo ""
else
    echo "✅ No errors in phase 2"
fi

# Compare event counts
EVENTS_PHASE1=$(grep "events processed" /tmp/phase1.log | tail -1 | grep -oP '\d+(?= events processed)' || echo "0")
EVENTS_PHASE2=$(grep "events processed" /tmp/phase2.log | tail -1 | grep -oP '\d+(?= events processed)' || echo "0")

echo "Summary:"
echo "   Phase 1 events: $EVENTS_PHASE1"
echo "   Phase 2 events: $EVENTS_PHASE2 (cumulative)"
echo ""
echo "Logs:"
echo "   Phase 1: /tmp/phase1.log"
echo "   Phase 2: /tmp/phase2.log"
echo ""
echo "To view WAL logs:"
echo "   grep 'WAL' /tmp/phase1.log"
echo "   grep 'WAL' /tmp/phase2.log"
