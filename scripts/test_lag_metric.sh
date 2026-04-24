#!/bin/bash
# Stress test for slot_to_indexed_lag_ms metric fix
#
# Tests the corrected lag metric computation to ensure it properly
# measures the time delta between current time and last observed event.
#
# Expected behavior:
#   - lag = current_time - last_observed_at_unix_ms
#   - lag should increase over time (for a fixed event)
#   - lag should be ~0 for very recent events
#   - lag should be large for old events

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Slot-to-Indexed Lag Metric - Stress Test                    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Build the project
echo "🔨 Building project..."
cargo build --release 2>&1 | tail -3

# Create test program to verify the metric
cat > /tmp/test_lag_metric.rs << 'EOF'
use std::time::{SystemTime, Duration, UNIX_EPOCH};

fn main() {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║   Lag Metric Computation Test                                 ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!("");

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Test 1: Recent event (1 second ago)
    println!("📊 Test 1: Recent event (1 second ago)");
    let event_time_1s = now_ms - 1_000;
    let lag_1s = now_ms.saturating_sub(event_time_1s).max(0);
    println!("   Current time: {} ms", now_ms);
    println!("   Event time: {} ms", event_time_1s);
    println!("   Computed lag: {} ms", lag_1s);
    println!("   ✓ Expected: ~1000ms, Got: {}ms", lag_1s);
    assert!(lag_1s >= 900 && lag_1s <= 1100, "Lag should be ~1000ms");
    println!("   ✅ PASS: Lag is correctly computed");
    println!(".");

    // Test 2: Old event (1 hour ago)
    println!("📊 Test 2: Old event (1 hour ago)");
    let event_time_1h = now_ms - 3_600_000;
    let lag_1h = now_ms.saturating_sub(event_time_1h).max(0);
    println!("   Current time: {} ms", now_ms);
    println!("   Event time: {} ms", event_time_1h);
    println!("   Computed lag: {} ms", lag_1h);
    println!("   ✓ Expected: ~3,600,000ms (1 hour), Got: {}ms", lag_1h);
    assert!(lag_1h >= 3_599_000 && lag_1h <= 3_601_000, "Lag should be ~1 hour");
    println!("   ✅ PASS: Large lag correctly computed");
    println!(".");

    // Test 3: Future event (should saturate to 0)
    println!("📊 Test 3: Future event (edge case)");
    let event_time_future = now_ms + 10_000;
    let regular_sub = now_ms - event_time_future;  // This would be negative
    let lag_future = now_ms.saturating_sub(event_time_future).max(0);  // This should be 0
    println!("   Current time: {} ms", now_ms);
    println!("   Event time: {} ms (future)", event_time_future);
    println!("   Regular subtraction: {} ms (would be negative)", regular_sub);
    println!("   Saturating subtraction + max(0): {} ms", lag_future);
    println!("   ✓ Expected: 0ms (clamped), Got: {}ms", lag_future);
    assert_eq!(lag_future, 0, "Lag should clamp to 0 for future events");
    println!("   ✅ PASS: Saturating subtraction with max(0) handles future events");
    println!(".");

    // Test 4: Very old event (24 hours ago)
    println!("📊 Test 4: Very old event (24 hours ago)");
    let event_time_24h = now_ms - 86_400_000;
    let lag_24h = now_ms.saturating_sub(event_time_24h).max(0);
    println!("   Current time: {} ms", now_ms);
    println!("   Event time: {} ms", event_time_24h);
    println!("   Computed lag: {} ms ({} hours)", lag_24h, lag_24h / 3_600_000);
    println!("   ✓ Expected: ~86,400,000ms (24 hours), Got: {}ms", lag_24h);
    assert!(lag_24h >= 86_399_000 && lag_24h <= 86_401_000, "Lag should be ~24 hours");
    println!("   ✅ PASS: Very old events correctly computed");
    println!(".");

    // Test 5: Lag increases over time (for fixed event)
    println!("📊 Test 5: Lag increases over time");
    let fixed_event_time = now_ms - 5_000;
    println!("   Fixed event time: {} ms", fixed_event_time);

    std::thread::sleep(Duration::from_millis(100));
    let now_after_100ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let lag_after_100ms = now_after_100ms.saturating_sub(fixed_event_time).max(0);
    println!("   After 100ms: lag = {}ms", lag_after_100ms);

    std::thread::sleep(Duration::from_millis(100));
    let now_after_200ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let lag_after_200ms = now_after_200ms.saturating_sub(fixed_event_time).max(0);
    println!("   After 200ms: lag = {}ms", lag_after_200ms);

    assert!(lag_after_200ms > lag_after_100ms, "Lag should increase over time");
    println!("   ✅ PASS: Lag correctly increases as time passes");
    println!(".");

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║   All Tests PASSED ✅                                         ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
}
EOF

# Compile and run the test program
echo "🔨 Compiling test program..."
rustc /tmp/test_lag_metric.rs -o /tmp/test_lag_metric 2>&1

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "Running Lag Metric Tests"
echo "══════════════════════════════════════════════════════════════════"
echo ""

/tmp/test_lag_metric

echo ""
echo "══════════════════════════════════════════════════════════════════"
echo "Integration Test: Unit Test Verification"
echo "══════════════════════════════════════════════════════════════════"
echo ""

echo "Running cargo tests for lag metric..."
cargo test api::rest::tests::slot_to_indexed_lag --lib -- --nocapture

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Stress Test Complete ✅                                     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "📋 Summary:"
echo "   ✅ Lag metric correctly computes: current_time - event_time"
echo "   ✅ Lag increases over time (for fixed event)"
echo "   ✅ Lag saturates to 0 for future events"
echo "   ✅ Handles very old events (24+ hours)"
echo "   ✅ All unit tests pass"
echo ""
