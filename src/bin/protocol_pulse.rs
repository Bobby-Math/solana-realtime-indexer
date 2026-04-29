// Protocol Pulse - Immediate Value Demo
// This creates an impressive live dashboard for prospects
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use solana_realtime_indexer::geyser::decoder::{GeyserEvent, TransactionUpdate};

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const HIGH_FEE_THRESHOLD_LAMPORTS: u64 = 10_000;
const MAX_RECENT_ACTIVITY: usize = 100;

#[derive(Debug, Clone)]
struct ProtocolMetrics {
    total_value_locked: f64,
    active_users: u64,
    transaction_count: u64,
    observed_fee_volume_sol: f64,
    revenue_today: f64,
    health_score: f64,
}

struct ProtocolPulse {
    metrics: ProtocolMetrics,
    start_time: Instant,
    alerts: VecDeque<String>,
    high_fee_transactions: VecDeque<HighFeeTransaction>,
    account_balances: HashMap<Vec<u8>, u64>,
    active_user_pubkeys: HashSet<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct HighFeeTransaction {
    timestamp: i64,
    amount_sol: f64,
    signature: String,
    description: String,
}

impl ProtocolPulse {
    fn new() -> Self {
        Self {
            metrics: ProtocolMetrics {
                total_value_locked: 0.0,
                active_users: 0,
                transaction_count: 0,
                observed_fee_volume_sol: 0.0,
                revenue_today: 0.0,
                health_score: 100.0,
            },
            start_time: Instant::now(),
            alerts: VecDeque::new(),
            high_fee_transactions: VecDeque::new(),
            account_balances: HashMap::new(),
            active_user_pubkeys: HashSet::new(),
        }
    }

    fn process_event(&mut self, event: &GeyserEvent) {
        match event {
            GeyserEvent::Transaction(tx) => {
                let fee_sol = tx.fee as f64 / LAMPORTS_PER_SOL;

                if tx.fee > HIGH_FEE_THRESHOLD_LAMPORTS {
                    let signature = bs58::encode(&tx.signature).into_string();

                    push_bounded(
                        &mut self.high_fee_transactions,
                        HighFeeTransaction {
                            timestamp: tx.timestamp_unix_ms,
                            amount_sol: fee_sol,
                            signature: signature.clone(),
                            description: format!("High-fee transaction: {fee_sol:.9} SOL"),
                        },
                    );

                    push_bounded(
                        &mut self.alerts,
                        format!("🚨 High-fee transaction detected: {fee_sol:.9} SOL - {signature}"),
                    );
                }

                self.metrics.transaction_count += 1;
                self.metrics.observed_fee_volume_sol += fee_sol;

                if let Some(user) = tx.accounts.first() {
                    self.active_user_pubkeys.insert(user.clone());
                    self.metrics.active_users = self.active_user_pubkeys.len() as u64;
                }
            }
            GeyserEvent::AccountUpdate(acc) => {
                self.account_balances
                    .insert(acc.pubkey.clone(), acc.lamports);
                self.metrics.total_value_locked = self
                    .account_balances
                    .values()
                    .map(|lamports| *lamports as f64 / LAMPORTS_PER_SOL)
                    .sum();
            }
            _ => {}
        }
    }

    fn generate_live_report(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let tps = if elapsed > 0.0 {
            self.metrics.transaction_count as f64 / elapsed
        } else {
            0.0
        };

        format!(
            r#"
═══════════════════════════════════════════════════════════════
                    🔴 PROTOCOL PULSE - LIVE
═══════════════════════════════════════════════════════════════

⏱️  LIVE MONITORING: {:.1}s active
💰 TVL: {:.2} SOL
👥 Active Users: {}
🧾 Transactions Seen: {} ({:.1} TPS)
💸 Observed Fees: {:.6} SOL
💵 Revenue Today: ${:.2}
🏥 Health Score: {:.1}/100

═══════════════════════════════════════════════════════════════
🚨 ALERTS:
{}
═══════════════════════════════════════════════════════════════
🐋 RECENT HIGH-FEE TRANSACTIONS:
{}
═══════════════════════════════════════════════════════════════
"#,
            elapsed,
            self.metrics.total_value_locked,
            self.metrics.active_users,
            self.metrics.transaction_count,
            tps,
            self.metrics.observed_fee_volume_sol,
            self.metrics.revenue_today,
            self.metrics.health_score,
            if self.alerts.is_empty() {
                "✅ No alerts - All systems normal!".to_string()
            } else {
                self.alerts
                    .iter()
                    .rev()
                    .map(|alert| format!("  • {}", alert))
                    .collect::<Vec<_>>()
                    .join("\n")
            },
            if self.high_fee_transactions.is_empty() {
                "  No high-fee transactions yet...".to_string()
            } else {
                self.high_fee_transactions
                    .iter()
                    .rev()
                    .take(5)
                    .map(|tx| {
                        format!(
                            "  • {:.9} SOL - {} ({})",
                            tx.amount_sol, tx.description, tx.signature
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        )
    }
}

fn push_bounded<T>(entries: &mut VecDeque<T>, entry: T) {
    if entries.len() >= MAX_RECENT_ACTIVITY {
        entries.pop_front();
    }
    entries.push_back(entry);
}

// This would be called from your main processing loop
fn demo_protocol_pulse() {
    let mut pulse = ProtocolPulse::new();

    println!("🚀 Starting Protocol Pulse Demo...\n");

    for i in 1..=5 {
        let simulated_event = create_demo_event(i);
        pulse.process_event(&simulated_event);

        println!("{}", pulse.generate_live_report());
        std::thread::sleep(Duration::from_secs(2));
    }

    println!("\n✅ Demo complete - Imagine this with YOUR real protocol data!");
}

fn create_demo_event(num: i32) -> GeyserEvent {
    GeyserEvent::Transaction(TransactionUpdate {
        timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
        slot: 123456 + num as u64,
        signature: format!("demo_signature_{}", num).as_bytes().to_vec(),
        fee: 50_000 * num as u64,
        success: true,
        accounts: vec![format!("account_{}", num).as_bytes().to_vec()],
        program_ids: vec![b"token-program".to_vec()],
        log_messages: vec![],
    })
}

fn main() {
    demo_protocol_pulse();
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_realtime_indexer::geyser::decoder::AccountUpdate;

    fn make_transaction_event(signature_seed: u8, fee: u64, user_seed: u8) -> GeyserEvent {
        GeyserEvent::Transaction(TransactionUpdate {
            timestamp_unix_ms: 1_710_000_000_000 + signature_seed as i64,
            slot: 100 + signature_seed as u64,
            signature: vec![signature_seed; 64],
            fee,
            success: true,
            accounts: vec![vec![user_seed; 32]],
            program_ids: vec![],
            log_messages: vec![],
        })
    }

    fn make_account_event(pubkey_seed: u8, lamports: u64) -> GeyserEvent {
        GeyserEvent::AccountUpdate(AccountUpdate {
            timestamp_unix_ms: 1_710_000_000_000 + pubkey_seed as i64,
            slot: 200 + pubkey_seed as u64,
            pubkey: vec![pubkey_seed; 32],
            owner: vec![9; 32],
            lamports,
            write_version: 1,
            data: vec![],
        })
    }

    #[test]
    fn high_fee_transaction_amount_uses_lamports_per_sol() {
        let mut pulse = ProtocolPulse::new();
        pulse.process_event(&make_transaction_event(1, 1_500_000_000, 7));

        let tx = pulse
            .high_fee_transactions
            .back()
            .expect("expected tracked high-fee transaction");
        assert!((tx.amount_sol - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn account_updates_maintain_tvl_snapshot_per_account() {
        let mut pulse = ProtocolPulse::new();

        pulse.process_event(&make_account_event(1, 5_000_000_000));
        pulse.process_event(&make_account_event(1, 3_000_000_000));
        pulse.process_event(&make_account_event(2, 2_000_000_000));

        assert!((pulse.metrics.total_value_locked - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn active_users_are_unique_while_tps_uses_transaction_count() {
        let mut pulse = ProtocolPulse::new();

        pulse.process_event(&make_transaction_event(1, 20_000, 9));
        pulse.process_event(&make_transaction_event(2, 25_000, 9));
        pulse.process_event(&make_transaction_event(3, 30_000, 8));

        assert_eq!(pulse.metrics.active_users, 2);
        assert_eq!(pulse.metrics.transaction_count, 3);
    }

    #[test]
    fn recent_activity_buffers_are_bounded() {
        let mut pulse = ProtocolPulse::new();

        for seed in 0..(MAX_RECENT_ACTIVITY as u8 + 10) {
            pulse.process_event(&make_transaction_event(
                seed,
                HIGH_FEE_THRESHOLD_LAMPORTS + 1,
                seed,
            ));
        }

        assert_eq!(pulse.high_fee_transactions.len(), MAX_RECENT_ACTIVITY);
        assert_eq!(pulse.alerts.len(), MAX_RECENT_ACTIVITY);
    }
}
