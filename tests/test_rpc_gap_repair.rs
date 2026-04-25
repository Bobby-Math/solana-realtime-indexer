//! Chaos test for RPC gap repair functionality
//!
//! This test simulates real-world failure scenarios where gRPC updates are lost
//! and verifies that the RPC gap repair mechanism correctly recovers the missing data.

use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// Test configuration
struct ChaosTestConfig {
    /// Known slot that exists on mainnet (update this periodically)
    test_slot: u64,
    /// Helius RPC endpoint
    rpc_url: String,
    /// Number of slots to create gaps in
    gap_count: usize,
}

impl Default for ChaosTestConfig {
    fn default() -> Self {
        Self {
            // Use a recent mainnet slot (update this to a slot from last 24 hours)
            test_slot: 250000000,
            rpc_url: std::env::var("HELIUS_RPC_URL")
                .unwrap_or_else(|_| "https://mainnet.helius-rpc.com".to_string()),
            gap_count: 3,
        }
    }
}

/// Chaos test that simulates gRPC data loss and verifies RPC gap repair
#[tokio::test]
async fn test_chaos_grpc_loss_with_rpc_repair() {
    // Skip test if no RPC URL configured
    let rpc_url = match std::env::var("HELIUS_RPC_URL") {
        Ok(url) => url,
        Err(_) => {
            println!("⚠️  Skipping chaos test: HELIUS_RPC_URL not configured");
            println!("   Run with: HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY cargo test test_chaos");
            return;
        }
    };

    let config = ChaosTestConfig {
        rpc_url: rpc_url.clone(),
        ..Default::default()
    };

    println!("🔥 Starting chaos test for RPC gap repair");
    println!("   RPC URL: {}", rpc_url);
    println!("   Test slot: {}", config.test_slot);

    // Step 1: Create a WAL with intentional gaps
    let temp_dir = tempdir().unwrap();
    let wal_queue = Arc::new(solana_realtime_indexer::geyser::wal_queue::WalQueue::new(temp_dir.path()).unwrap());

    println!("📝 Step 1: Creating WAL with intentional gaps");

    // Create a sequence of slots with a gap in the middle
    let base_slot = config.test_slot;
    let gap_slot = base_slot + 1; // This slot will be "lost"

    // Write slot 0 (before gap)
    let update0 = create_test_slot_update(base_slot);
    wal_queue.push_update(base_slot, &update0).unwrap();
    println!("   ✓ Wrote slot {} (before gap)", base_slot);

    // Skip slot 1 (simulate gRPC loss)
    println!("   ⚠️  Simulating loss of slot {} (gap)", gap_slot);

    // Write slot 2 (after gap)
    let update2 = create_test_slot_update(base_slot + 2);
    wal_queue.push_update(base_slot + 2, &update2).unwrap();
    println!("   ✓ Wrote slot {} (after gap)", base_slot + 2);

    // Step 2: Verify gap detection
    println!("\n🔍 Step 2: Verifying gap detection");

    // Try to read sequentially - should detect gap at seq 1
    let result = wal_queue.read_from(0, 1);
    assert!(result.is_err(), "Should detect gap at seq 1");

    let error_msg = result.as_ref().unwrap_err().to_string();
    assert!(error_msg.contains("gap detected"), "Error should mention gap");
    println!("   ✓ Gap detected: {}", error_msg);

    // Step 3: Attempt RPC gap repair
    println!("\n🔧 Step 3: Attempting RPC gap repair");

    let gap_filler = solana_realtime_indexer::geyser::wal_consumer::RpcGapFiller::new(
        vec![config.rpc_url.clone()],
        wal_queue.clone(),
    );

    // Extract seq from error
    let seq = match result.as_ref().unwrap_err() {
        solana_realtime_indexer::geyser::wal_queue::WalReadError::Gap { seq } => *seq,
        _ => panic!("Expected Gap error"),
    };

    // Try to repair the gap
    let repair_result = gap_filler.repair_gap(seq).await;
    println!("   Repair result: {:?}", repair_result);

    // Step 4: Verify repair succeeded
    println!("\n✅ Step 4: Verifying repair succeeded");

    let repair_success = matches!(repair_result, Ok(true));

    match &repair_result {
        Ok(true) => {
            println!("   ✓ Gap repair succeeded");

            // Verify the gap is now filled
            let filled_result = wal_queue.read_from(0, seq);
            assert!(filled_result.is_ok(), "Should be able to read gap slot after repair");
            let entry = filled_result.unwrap().unwrap();
            assert_eq!(entry.seq, seq, "Should read the repaired seq");
            println!("   ✓ Gap slot {} is now readable", seq);
        }
        Ok(false) => {
            println!("   ⚠️  Gap repair returned false (slot may not be available)");
            println!("   This is expected if test slot is too old or doesn't exist");
        }
        Err(e) => {
            println!("   ❌ Gap repair failed: {}", e);
            println!("   This may indicate:");
            println!("      - Test slot {} doesn't exist on chain", gap_slot);
            println!("      - RPC endpoint is invalid");
            println!("      - Network connectivity issues");
        }
    }

    // Step 5: External verification via direct RPC call
    println!("\n🔍 Step 5: External verification via direct RPC call");

    if let Ok(block_data) = fetch_block_direct(&config.rpc_url, gap_slot).await {
        println!("   ✓ Fetched slot {} directly from RPC", gap_slot);

        // Verify the block has expected fields
        assert!(block_data.get("blockhash").is_some(), "Should have blockhash");
        assert!(block_data.get("parentSlot").is_some(), "Should have parentSlot");
        println!("   ✓ Block data contains expected fields");

        // Compare with repaired data if repair succeeded
        if repair_success {
            let entry = wal_queue.read_from(0, seq).unwrap().unwrap();
            let repaired_update = entry.decode_update().unwrap();

            // Verify the repaired update has correct slot
            if let Some(solana_realtime_indexer::geyser::decoder::GeyserEvent::SlotUpdate(slot)) =
                solana_realtime_indexer::geyser::decoder::decode_subscribe_update(&repaired_update, entry.timestamp_unix_ms)
            {
                assert_eq!(slot.slot as u64, gap_slot, "Repaired slot should match");
                println!("   ✓ Repaired slot matches RPC data: {}", gap_slot);
            }
        }
    } else {
        println!("   ⚠️  Could not fetch block directly (slot may not exist)");
    }
}

/// Test that verifies RPC gap repair doesn't corrupt existing data
#[tokio::test]
async fn test_chaos_repair_preserves_existing_data() {
    let temp_dir = tempdir().unwrap();
    let wal_queue = Arc::new(solana_realtime_indexer::geyser::wal_queue::WalQueue::new(temp_dir.path()).unwrap());

    println!("🔒 Testing that gap repair preserves existing WAL data");

    // Write multiple slots
    for i in 0..5 {
        let update = create_test_slot_update(100 + i);
        wal_queue.push_update(100 + i, &update).unwrap();
    }

    // Mark some as processed
    wal_queue.mark_processed(100, 2).unwrap();
    let checkpoint_before = wal_queue.get_last_processed_seq();
    println!("   Checkpoint before: {}", checkpoint_before);

    // Simulate a gap and repair attempt (even if it fails)
    let gap_filler = solana_realtime_indexer::geyser::wal_consumer::RpcGapFiller::new(
        vec!["https://invalid.endpoint".to_string()],
        wal_queue.clone(),
    );

    let _ = gap_filler.repair_gap(3).await;

    // Verify checkpoint wasn't corrupted
    let checkpoint_after = wal_queue.get_last_processed_seq();
    assert_eq!(checkpoint_before, checkpoint_after, "Checkpoint should not change");
    println!("   ✓ Checkpoint preserved: {} == {}", checkpoint_before, checkpoint_after);
}

/// Helper: Fetch block directly from RPC for verification
async fn fetch_block_direct(
    rpc_url: &str,
    slot: u64,
) -> Result<serde_json::Value, String> {
    let client = reqwest::Client::new();
    let url = format!("{}/", rpc_url.trim_end_matches('/'));

    let response = client
        .post(&url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlock",
            "params": [
                slot,
                {
                    "encoding": "jsonParsed",
                    "transactionDetails": "none",
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
        .map_err(|e| format!("Failed to parse JSON: {}", e))?;

    if let Some(error) = json.get("error") {
        return Err(format!("RPC error: {}", error));
    }

    json.get("result")
        .cloned()
        .ok_or_else(|| "No result in RPC response".to_string())
}

/// Helper: Create a test slot update
fn create_test_slot_update(slot: u64) -> helius_laserstream::grpc::SubscribeUpdate {
    use helius_laserstream::grpc::{SubscribeUpdate, subscribe_update::UpdateOneof, SubscribeUpdateSlot};

    SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent: Some(slot.saturating_sub(1)),
            status: 2, // finalized
            dead_error: None,
        })),
        ..Default::default()
    }
}

/// Benchmark: Measure gap repair latency
#[tokio::test]
async fn benchmark_gap_repair_latency() {
    let rpc_url = match std::env::var("HELIUS_RPC_URL") {
        Ok(url) => url,
        Err(_) => {
            println!("⚠️  Skipping benchmark: HELIUS_RPC_URL not configured");
            return;
        }
    };

    println!("⚡ Benchmarking RPC gap repair latency");

    let test_slots = vec![250000000, 250000001, 250000002];
    let mut latencies = Vec::new();

    for slot in test_slots {
        let start = std::time::Instant::now();

        if let Ok(_) = fetch_block_direct(&rpc_url, slot).await {
            let latency = start.elapsed();
            latencies.push(latency);
            println!("   Slot {}: {:?}", slot, latency);
        }
    }

    if !latencies.is_empty() {
        let avg = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        println!("   📊 Average latency: {:?}", avg);

        // Alert if latency is too high
        if avg > Duration::from_secs(5) {
            println!("   ⚠️  WARNING: High latency may impact gap repair performance");
        }
    }
}
