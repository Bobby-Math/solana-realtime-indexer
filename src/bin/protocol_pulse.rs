// Protocol Pulse - Immediate Value Demo
// This creates an impressive live dashboard for prospects

use std::time::{Duration, Instant};
use std::collections::HashMap;
use solana_realtime_indexer::geyser::decoder::{GeyserEvent, TransactionUpdate};

#[derive(Debug, Clone)]
struct ProtocolMetrics {
    total_value_locked: f64,
    active_users: u64,
    transaction_volume_24h: f64,
    revenue_today: f64,
    health_score: f64,
    alerts: Vec<String>,
}

struct ProtocolPulse {
    metrics: ProtocolMetrics,
    start_time: Instant,
    large_transactions: Vec<LargeTransaction>,
    price_movements: Vec<PriceMovement>,
}

#[derive(Debug, Clone)]
struct LargeTransaction {
    timestamp: i64,
    amount: f64,
    wallet: String,
    description: String,
}

#[derive(Debug, Clone)]
struct PriceMovement {
    token_pair: String,
    price_before: f64,
    price_after: f64,
    change_percent: f64,
    timestamp: i64,
}

impl ProtocolPulse {
    fn new() -> Self {
        Self {
            metrics: ProtocolMetrics {
                total_value_locked: 0.0,
                active_users: 0,
                transaction_volume_24h: 0.0,
                revenue_today: 0.0,
                health_score: 100.0,
                alerts: Vec::new(),
            },
            start_time: Instant::now(),
            large_transactions: Vec::new(),
            price_movements: Vec::new(),
        }
    }

    fn process_event(&mut self, event: &GeyserEvent) {
        match event {
            GeyserEvent::Transaction(tx) => {
                // Track large transactions
                if tx.fee > 10000 {
                    self.large_transactions.push(LargeTransaction {
                        timestamp: tx.timestamp_unix_ms,
                        amount: tx.fee as f64 / 1_000_000.0, // Convert to lamports to SOL
                        wallet: tx.signature.clone(),
                        description: format!("Large transaction: {} SOL", tx.fee as f64 / 1_000_000_000.0),
                    });

                    // Add alert
                    self.metrics.alerts.push(format!(
                        "🚨 Large transaction detected: {} SOL - {}",
                        tx.fee as f64 / 1_000_000_000.0,
                        tx.signature
                    ));
                }

                // Update metrics
                self.metrics.transaction_volume_24h += tx.fee as f64;
                self.metrics.active_users += 1;
            }
            GeyserEvent::AccountUpdate(acc) => {
                // Track TVL changes
                self.metrics.total_value_locked += acc.lamports as f64 / 1_000_000_000.0;
            }
            _ => {}
        }
    }

    fn generate_live_report(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let tps = self.metrics.active_users as f64 / elapsed;

        format!(
            r#"
═══════════════════════════════════════════════════════════════
                    🔴 PROTOCOL PULSE - LIVE
═══════════════════════════════════════════════════════════════

⏱️  LIVE MONITORING: {:.1}s active
💰 TVL: ${:.2}M
👥 Active Users: {} ({} events/sec)
📊 24h Volume: ${:.2}M
💵 Revenue Today: ${:.2}
🏥 Health Score: {:.1}/100

═══════════════════════════════════════════════════════════════
🚨 ALERTS:
{}
═══════════════════════════════════════════════════════════════
🐋 RECENT LARGE TRANSACTIONS:
{}
═══════════════════════════════════════════════════════════════
"#,
            elapsed,
            self.metrics.total_value_locked,
            self.metrics.active_users,
            tps,
            self.metrics.transaction_volume_24h / 1_000_000.0,
            self.metrics.revenue_today,
            self.metrics.health_score,
            if self.metrics.alerts.is_empty() {
                "✅ No alerts - All systems normal!".to_string()
            } else {
                self.metrics.alerts.iter()
                    .map(|a| format!("  • {}", a))
                    .collect::<Vec<_>>()
                    .join("\n")
            },
            if self.large_transactions.is_empty() {
                "  No large transactions yet...".to_string()
            } else {
                self.large_transactions.iter()
                    .take(5)
                    .map(|tx| format!("  • {} SOL - {}", tx.amount, tx.description))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        )
    }
}

// This would be called from your main processing loop
fn demo_protocol_pulse() {
    let mut pulse = ProtocolPulse::new();

    // Simulate some events for demo
    println!("🚀 Starting Protocol Pulse Demo...\n");

    // Show immediate live updates
    for i in 1..=5 {
        let simulated_event = create_demo_event(i);
        pulse.process_event(&simulated_event);

        println!("{}", pulse.generate_live_report());
        std::thread::sleep(Duration::from_secs(2));
    }

    println!("\n✅ Demo complete - Imagine this with YOUR real protocol data!");
}

fn create_demo_event(num: i32) -> GeyserEvent {
    GeyserEvent::Transaction(solana_realtime_indexer::geyser::decoder::TransactionUpdate {
        timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
        slot: 123456 + num as u64,
        signature: format!("demo_signature_{}", num),
        fee: 50000 * num as u64, // Increasing fees
        success: true,
        program_ids: vec!["token-program".to_string()],
        log_messages: vec![],
    })
}

fn main() {
    demo_protocol_pulse();
}