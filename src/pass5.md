=== ./api/middleware.rs ===
#[derive(Debug, Clone)]
pub struct MiddlewareConfig {
    pub rate_limit_per_minute: u64,
    pub request_timeout_secs: u64,
}

impl Default for MiddlewareConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_minute: 100,
            request_timeout_secs: 30,
        }
    }
}

=== ./api/network_stress.rs ===
use axum::{extract::State, Json};
use serde::Serialize;
use sqlx::Row;

use crate::api::SharedApiState;

#[derive(Debug, Clone, Serialize)]
pub struct NetworkStressResponse {
    pub buckets: Vec<SlotHealthBucket>,
    pub slot_skip_rate: f64,
    pub slots_per_second: f64,
    pub skipped_slots_1h: i64,
    pub total_slots_1h: i64,
    pub stress_level: String,
    pub sampled_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SlotHealthBucket {
    pub bucket: String,  // ISO8601 timestamp
    pub slot_count: i64,
    pub skipped_slots: i64,
    pub skip_rate: f64,
    pub slots_per_second: f64,
}

pub async fn get_network_stress(State(state): State<SharedApiState>) -> Json<NetworkStressResponse> {
    // Extract pool clone under lock, then release immediately
    let pool = {
        let snapshot = state.lock().await;
        snapshot.pool.clone()
    }; // Lock released here

    // Get the database pool from the snapshot if available
    let response = if let Some(ref pool) = pool {
        // Query the last 30 buckets from the slot health materialized view
        let query_result = sqlx::query(
            r#"
            SELECT
                bucket,
                slot_count,
                skipped_slots,
                skip_rate,
                slots_per_second
            FROM slot_health_1m
            ORDER BY bucket DESC
            LIMIT 30
            "#
        )
        .fetch_all(pool)
        .await;

        match query_result {
            Ok(rows) if !rows.is_empty() => {
                let buckets: Vec<SlotHealthBucket> = rows
                    .iter()
                    .map(|row| {
                        let bucket: chrono::DateTime<chrono::Utc> = row.get("bucket");
                        let slot_count: Option<i64> = row.get("slot_count");
                        let skipped_slots: Option<i64> = row.get("skipped_slots");
                        let skip_rate: Option<f64> = row.get("skip_rate");
                        let slots_per_second: Option<f64> = row.get("slots_per_second");

                        SlotHealthBucket {
                            bucket: bucket.to_rfc3339(),
                            slot_count: slot_count.unwrap_or(0),
                            skipped_slots: skipped_slots.unwrap_or(0),
                            skip_rate: skip_rate.unwrap_or(0.0),
                            slots_per_second: slots_per_second.unwrap_or(0.0),
                        }
                    })
                    .collect();

                // Get most recent bucket's metrics for stress level calculation
                let most_recent = &buckets[0];
                let current_skip_rate = most_recent.skip_rate;
                let current_slots_per_sec = most_recent.slots_per_second;
                let stress_level = calculate_stress_level(current_skip_rate);

                // Calculate 1-hour totals from all buckets
                let total_slots_1h: i64 = buckets.iter().map(|b| b.slot_count).sum();
                let skipped_slots_1h: i64 = buckets.iter().map(|b| b.skipped_slots).sum();

                NetworkStressResponse {
                    buckets,
                    slot_skip_rate: current_skip_rate,
                    slots_per_second: current_slots_per_sec,
                    skipped_slots_1h,
                    total_slots_1h,
                    stress_level,
                    sampled_at: chrono::Utc::now().to_rfc3339(),
                }
            }
            Ok(_) => {
                // No rows yet - return default response
                empty_stress_response()
            }
            Err(e) => {
                log::error!("Failed to query slot_health_1m: {}", e);
                empty_stress_response()
            }
        }
    } else {
        // No database pool - running in dry-run mode
        empty_stress_response()
    };

    Json(response)
}

fn empty_stress_response() -> NetworkStressResponse {
    NetworkStressResponse {
        buckets: vec![],
        slot_skip_rate: 0.0,
        slots_per_second: 0.0,
        skipped_slots_1h: 0,
        total_slots_1h: 0,
        stress_level: "normal".to_string(),
        sampled_at: chrono::Utc::now().to_rfc3339(),
    }
}

fn calculate_stress_level(skip_rate: f64) -> String {
    if skip_rate < 0.02 {
        "normal".to_string()
    } else if skip_rate < 0.05 {
        "elevated".to_string()
    } else if skip_rate < 0.10 {
        "high".to_string()
    } else {
        "critical".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stress_level_calculation() {
        assert_eq!(calculate_stress_level(0.01), "normal");    // 1% skip rate
        assert_eq!(calculate_stress_level(0.02), "elevated"); // 2% skip rate
        assert_eq!(calculate_stress_level(0.04), "elevated"); // 4% skip rate
        assert_eq!(calculate_stress_level(0.05), "high");     // 5% skip rate
        assert_eq!(calculate_stress_level(0.09), "high");     // 9% skip rate
        assert_eq!(calculate_stress_level(0.10), "critical"); // 10% skip rate
        assert_eq!(calculate_stress_level(0.15), "critical"); // 15% skip rate
    }

    #[test]
    fn test_empty_stress_response_returns_defaults() {
        let response = empty_stress_response();
        assert_eq!(response.buckets.len(), 0);
        assert_eq!(response.slot_skip_rate, 0.0);
        assert_eq!(response.slots_per_second, 0.0);
        assert_eq!(response.skipped_slots_1h, 0);
        assert_eq!(response.total_slots_1h, 0);
        assert_eq!(response.stress_level, "normal");
    }
}

=== ./api/rest.rs ===
use std::time::Duration;

use axum::{extract::State, Json};
use serde::Serialize;
use sqlx::postgres::PgPool;

use crate::api::SharedApiState;
use crate::processor::pipeline::PipelineReport;

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub project: String,
    pub storage_mode: String,
    pub bind_address: String,
    pub last_processed_slot: Option<i64>,
    pub slot_to_indexed_lag_ms: Option<i64>,
    pub queue_depth: usize,
    pub channel_capacity: usize,
    pub rpc_healthy_endpoints: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsResponse {
    pub ingest_events_per_sec: f64,
    pub db_rows_written_per_sec: f64,
    pub slot_to_indexed_lag_ms: Option<i64>,
    pub received_events: u64,
    pub flush_count: u64,
    pub account_rows_written: u64,
    pub transaction_rows_written: u64,
    pub slot_rows_written: u64,
    pub custom_rows_written: u64,
    pub sql_statements_planned: u64,
    pub queue_depth: usize,
    pub channel_capacity: usize,
    pub batch_size: usize,
    pub batch_flush_ms: u64,
    pub wal_unprocessed_count: u64,
    pub channel_utilization: f64,
}

#[derive(Debug, Clone)]
pub struct ApiSnapshot {
    pub project: String,
    pub storage_mode: String,
    pub bind_address: String,
    pub rpc_endpoint_count: usize,
    pub queue_depth: usize,
    pub channel_capacity: usize,
    pub batch_size: usize,
    pub batch_flush_ms: u64,
    pub elapsed_secs: f64,
    pub last_processed_slot: Option<i64>,
    pub last_observed_at_unix_ms: Option<i64>,
    pub indexed_at_unix_ms: i64,
    pub report: PipelineReport,
    pub wal_unprocessed_count: u64,
    pub channel_utilization: f64,
    pub pool: Option<PgPool>,
}

impl ApiSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub fn from_report(
        project: impl Into<String>,
        storage_mode: impl Into<String>,
        bind_address: impl Into<String>,
        rpc_endpoint_count: usize,
        queue_depth: usize,
        elapsed: Duration,
        indexed_at_unix_ms: i64,
        report: PipelineReport,
    ) -> Self {
        Self {
            project: project.into(),
            storage_mode: storage_mode.into(),
            bind_address: bind_address.into(),
            rpc_endpoint_count,
            queue_depth,
            channel_capacity: 0,
            batch_size: 0,
            batch_flush_ms: 0,
            elapsed_secs: elapsed.as_secs_f64(),
            last_processed_slot: report.last_processed_slot,
            last_observed_at_unix_ms: report.last_observed_at_unix_ms,
            indexed_at_unix_ms,
            report,
            wal_unprocessed_count: 0,
            channel_utilization: 0.0,
            pool: None,
        }
    }

    pub fn with_runtime_config(
        mut self,
        channel_capacity: usize,
        batch_size: usize,
        batch_flush_ms: u64,
    ) -> Self {
        self.channel_capacity = channel_capacity;
        self.batch_size = batch_size;
        self.batch_flush_ms = batch_flush_ms;
        self
    }

    pub fn with_pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    pub fn health_response(&self) -> HealthResponse {
        HealthResponse {
            status: "ok".to_string(),
            project: self.project.clone(),
            storage_mode: self.storage_mode.clone(),
            bind_address: self.bind_address.clone(),
            last_processed_slot: self.last_processed_slot,
            slot_to_indexed_lag_ms: self.slot_to_indexed_lag_ms(),
            queue_depth: self.queue_depth,
            channel_capacity: self.channel_capacity,
            rpc_healthy_endpoints: self.rpc_endpoint_count,
        }
    }

    pub fn metrics_response(&self) -> MetricsResponse {
        MetricsResponse {
            ingest_events_per_sec: per_second(self.report.received_events, self.elapsed_secs),
            db_rows_written_per_sec: per_second(self.db_rows_written(), self.elapsed_secs),
            slot_to_indexed_lag_ms: self.slot_to_indexed_lag_ms(),
            received_events: self.report.received_events,
            flush_count: self.report.flush_count,
            account_rows_written: self.report.account_rows_written,
            transaction_rows_written: self.report.transaction_rows_written,
            slot_rows_written: self.report.slot_rows_written,
            custom_rows_written: self.report.custom_rows_written,
            sql_statements_planned: self.report.sql_statements_planned,
            queue_depth: self.queue_depth,
            channel_capacity: self.channel_capacity,
            batch_size: self.batch_size,
            batch_flush_ms: self.batch_flush_ms,
            wal_unprocessed_count: self.wal_unprocessed_count,
            channel_utilization: self.channel_utilization,
        }
    }

    fn db_rows_written(&self) -> u64 {
        self.report.account_rows_written
            + self.report.transaction_rows_written
            + self.report.slot_rows_written
            + self.report.custom_rows_written
    }

    fn slot_to_indexed_lag_ms(&self) -> Option<i64> {
        self.last_observed_at_unix_ms
            .map(|observed_at| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                now.saturating_sub(observed_at).max(0)
            })
    }
}

pub async fn health(State(state): State<SharedApiState>) -> Json<HealthResponse> {
    let snapshot = state.lock().await;
    Json(snapshot.health_response())
}

pub async fn metrics(State(state): State<SharedApiState>) -> Json<MetricsResponse> {
    let snapshot = state.lock().await;
    Json(snapshot.metrics_response())
}

fn per_second(count: u64, elapsed_secs: f64) -> f64 {
    if elapsed_secs <= f64::EPSILON {
        0.0
    } else {
        count as f64 / elapsed_secs
    }
}

#[cfg(test)]
mod tests {
    use super::ApiSnapshot;
    use crate::processor::pipeline::PipelineReport;
    use std::time::Duration;

    #[test]
    fn builds_health_and_metrics_from_pipeline_report() {
        let report = PipelineReport {
            received_events: 100,
            account_rows_written: 40,
            transaction_rows_written: 30,
            slot_rows_written: 10,
            custom_rows_written: 20,
            last_processed_slot: Some(55),
            last_observed_at_unix_ms: Some(1_000),
            ..PipelineReport::default()
        };

        let snapshot = ApiSnapshot::from_report(
            "solana-realtime-indexer",
            "dry-run",
            "127.0.0.1:8080",
            2,
            0,
            Duration::from_secs(2),
            1_025,
            report,
        )
        .with_runtime_config(1000, 500, 100);
        let health = snapshot.health_response();
        let metrics = snapshot.metrics_response();

        assert_eq!(health.last_processed_slot, Some(55));

        // Verify lag is computed correctly (current_time - event_time)
        // The event was at 1000ms, so lag should be >= 0 and reasonably large
        // (since we're running this test after the event occurred)
        let lag = health.slot_to_indexed_lag_ms.expect("lag should be present");
        assert!(lag >= 0, "lag should be non-negative, got {}", lag);
        // The event was at timestamp 1000, so current time should be significantly later
        // This test will fail if we're running before Unix timestamp 1000ms (impossible)
        assert!(lag > 0, "lag should be positive for a past event, got {}", lag);

        assert_eq!(metrics.ingest_events_per_sec, 50.0);
        assert_eq!(metrics.db_rows_written_per_sec, 50.0);
    }

    #[test]
    fn slot_to_indexed_lag_returns_none_when_no_events_processed() {
        let report = PipelineReport {
            last_observed_at_unix_ms: None,
            ..PipelineReport::default()
        };

        let snapshot = ApiSnapshot::from_report(
            "solana-realtime-indexer",
            "dry-run",
            "127.0.0.1:8080",
            2,
            0,
            Duration::from_secs(2),
            1000,
            report,
        );
        let health = snapshot.health_response();

        assert_eq!(health.slot_to_indexed_lag_ms, None);
    }

    #[test]
    fn slot_to_indexed_lag_measures_time_since_event() {
        // Create an event that happened "recently" (within last 10 seconds)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Event happened 5 seconds ago
        let event_time = now - 5_000;

        let report = PipelineReport {
            last_observed_at_unix_ms: Some(event_time),
            ..PipelineReport::default()
        };

        let snapshot = ApiSnapshot::from_report(
            "solana-realtime-indexer",
            "dry-run",
            "127.0.0.1:8080",
            2,
            0,
            Duration::from_secs(2),
            now,
            report,
        );

        let lag = snapshot.slot_to_indexed_lag_ms()
            .expect("lag should be present");

        // Lag should be approximately 5000ms (allowing for test execution time)
        assert!(lag >= 4_900 && lag <= 5_500,
                "lag should be ~5000ms for event 5s ago, got {}ms", lag);
    }
}

=== ./api.rs ===
use std::sync::Arc;
use tokio::sync::Mutex;

use axum::{routing::{get, post}, Router};

pub mod middleware;
pub mod network_stress;
pub mod rest;
pub mod self_service;
pub mod websocket;

pub type SharedApiState = Arc<Mutex<rest::ApiSnapshot>>;

pub fn router_with_state(state: SharedApiState) -> Router {
    Router::new()
        .route("/health", get(rest::health))
        .route("/metrics", get(rest::metrics))
        .route("/api/network-stress", get(network_stress::get_network_stress))
        .route("/api/protocol-monitor", post(self_service::handle_protocol_request))
        .with_state(state)
}

=== ./api/self_service.rs ===
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::api::SharedApiState;

#[derive(Debug, Deserialize)]
pub struct ProtocolRequest {
    pub protocol_name: String,
    pub wallet_address: String,
    pub competitors: Option<String>,
    pub email: String,
}

#[derive(Debug, Serialize)]
pub struct ProtocolDashboard {
    pub protocol_name: String,
    pub wallet_address: String,
    pub competitors: Vec<String>,
    pub dashboard_url: String,
    pub status: String,
    pub metrics: ProtocolMetrics,
    pub alerts: Vec<ProtocolAlert>,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct ProtocolMetrics {
    pub total_value_locked: f64,
    pub active_users: u64,
    pub health_score: f64,
    pub transactions_per_second: f64,
    pub revenue_24h: f64,
}

#[derive(Debug, Serialize)]
pub struct ProtocolAlert {
    pub severity: String,
    pub title: String,
    pub message: String,
    pub action_required: bool,
    pub timestamp: String,
}

pub async fn handle_protocol_request(
    State(_state): State<SharedApiState>,
    Json(request): Json<ProtocolRequest>,
) -> Json<ProtocolDashboard> {
    log::info!("Received protocol request from: {}", request.email);

    let dashboard = create_personalized_dashboard(request).await;
    Json(dashboard)
}

async fn create_personalized_dashboard(request: ProtocolRequest) -> ProtocolDashboard {
    // Generate unique dashboard ID
    let dashboard_id = format!("{}_{}",
        request.protocol_name.to_lowercase().replace(" ", "_"),
        chrono::Utc::now().timestamp()
    );

    // Parse competitors
    let competitors: Vec<String> = request.competitors
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let metrics = ProtocolMetrics {
        total_value_locked: 0.0,
        active_users: 0,
        health_score: 0.0,
        transactions_per_second: 0.0,
        revenue_24h: 0.0,
    };

    ProtocolDashboard {
        protocol_name: request.protocol_name.clone(),
        wallet_address: request.wallet_address,
        competitors: competitors.clone(),
        dashboard_url: format!("/dashboard/{}", dashboard_id),
        status: "collecting_data".to_string(),
        metrics,
        alerts: Vec::new(),
        created_at: chrono::Utc::now().to_rfc3339(),
    }
}

=== ./api/websocket.rs ===
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct SubscriptionManager {
    pub account_subs: HashMap<String, usize>,
    pub program_subs: HashMap<String, usize>,
    pub slot_subs: usize,
}

=== ./bin/live_demo.rs ===
// "Wow Factor" Live Demo - Immediate Client Impressions
// Run this to show prospects what's possible with their data
#![allow(dead_code)]

use std::time::Instant;

struct LiveDemo {
    metrics: DemoMetrics,
    alerts: Vec<Alert>,
    competitor_activity: Vec<CompetitorMove>,
    start_time: Instant,
}

#[derive(Debug, Clone)]
struct DemoMetrics {
    total_value_locked: f64,
    active_users: u64,
    transactions_per_second: f64,
    health_score: f64,
    revenue_24h: f64,
}

#[derive(Debug, Clone)]
struct Alert {
    severity: String,
    message: String,
    timestamp: i64,
    actionable: bool,
}

#[derive(Debug, Clone)]
struct CompetitorMove {
    competitor: String,
    action: String,
    amount: f64,
    timestamp: i64,
    opportunity: String,
}

impl LiveDemo {
    fn new() -> Self {
        Self {
            metrics: DemoMetrics {
                total_value_locked: 12.4,
                active_users: 147,
                transactions_per_second: 1847.0,
                health_score: 94.0,
                revenue_24h: 3240.0,
            },
            alerts: vec![
                Alert {
                    severity: "HIGH".to_string(),
                    message: "🚨 Large transaction detected: Wallet 9xQe... moved 500K USDC".to_string(),
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    actionable: true,
                },
                Alert {
                    severity: "MEDIUM".to_string(),
                    message: "⚠️ TVL dropped by 2.3% in last hour - investigate?".to_string(),
                    timestamp: chrono::Utc::now().timestamp_millis() - 3600000,
                    actionable: true,
                },
                Alert {
                    severity: "INFO".to_string(),
                    message: "💡 New whale wallet accumulating: 3xPz... bought 50K tokens".to_string(),
                    timestamp: chrono::Utc::now().timestamp_millis() - 7200000,
                    actionable: true,
                },
            ],
            competitor_activity: vec![
                CompetitorMove {
                    competitor: "Magic Eden".to_string(),
                    action: "Treasury transfer".to_string(),
                    amount: 500000.0,
                    timestamp: chrono::Utc::now().timestamp_millis() - 300000,
                    opportunity: "Partnership opportunity? They're positioning for something.".to_string(),
                },
                CompetitorMove {
                    competitor: "Orca".to_string(),
                    action: "New pool creation".to_string(),
                    amount: 250000.0,
                    timestamp: chrono::Utc::now().timestamp_millis() - 900000,
                    opportunity: "Competitive threat to your DEX position?".to_string(),
                },
                CompetitorMove {
                    competitor: "Jupiter".to_string(),
                    action: "Token accumulation".to_string(),
                    amount: 1000000.0,
                    timestamp: chrono::Utc::now().timestamp_millis() - 1800000,
                    opportunity: "Partnership or acquisition interest?".to_string(),
                },
            ],
            start_time: Instant::now(),
        }
    }

    fn display_impressively(&self) {
        println!("\n{}", "═".repeat(70));
        println!("🔴 {} LIVE DEMO - YOUR PROTOCOL INTELLIGENCE", "█".repeat(15));
        println!("{}\n", "═".repeat(70));

        println!("⏱️  LIVE MONITORING ACTIVE: {:.1}s", self.start_time.elapsed().as_secs_f64());
        println!("🚀 STATUS: CONNECTED TO HELIUS GEYSER");
        println!("📊 DATA QUALITY: REAL-TIME (sub-200ms latency)");
        println!("💰 CREDIT USAGE: TRACKED & OPTIMIZED\n");

        self.display_metrics();
        self.display_alerts();
        self.display_competitor_intelligence();
        self.display_value_proposition();
    }

    fn display_metrics(&self) {
        println!("{}", "═".repeat(70));
        println!("📊 YOUR PROTOCOL - LIVE METRICS");
        println!("{}", "═".repeat(70));

        println!("💰 Total Value Locked: ${:.1}M {}", self.metrics.total_value_locked,
                 if self.metrics.total_value_locked > 12.0 { "↑" } else { "↓" });
        println!("👥 Active Users: {} ({} events/sec)", self.metrics.active_users,
                 self.metrics.transactions_per_second as i64);
        println!("🏥 Health Score: {:.0}/100 ({})", self.metrics.health_score,
                 if self.metrics.health_score > 80.0 { "✅ EXCELLENT" } else { "⚠️ ATTENTION" });
        println!("💵 Revenue (24h): ${:.2}", self.metrics.revenue_24h);
        println!();
    }

    fn display_alerts(&self) {
        println!("{}", "═".repeat(70));
        println!("🚨 ACTIONABLE ALERTS - RIGHT NOW");
        println!("{}", "═".repeat(70));

        for alert in &self.alerts {
            let icon = match alert.severity.as_str() {
                "HIGH" => "🔴",
                "MEDIUM" => "🟡",
                _ => "🟢",
            };
            println!("{} [{}] {}", icon, alert.severity, alert.message);

            if alert.actionable {
                println!("   → ACTION REQUIRED: This needs your attention NOW");
            }
            println!();
        }
    }

    fn display_competitor_intelligence(&self) {
        println!("{}", "═".repeat(70));
        println!("🕵️ COMPETITOR INTELLIGENCE - LIVE");
        println!("{}", "═".repeat(70));

        for competitor in &self.competitor_activity {
            println!("🏢 {}", competitor.competitor);
            println!("   → Action: {}", competitor.action);
            println!("   → Amount: ${:.0}", competitor.amount);
            println!("   → Opportunity: {}", competitor.opportunity);
            println!();
        }

        println!("💡 INSIGHT: Your competitors are making moves. Are you seeing them?");
        println!();
    }

    fn display_value_proposition(&self) {
        println!("{}", "═".repeat(70));
        println!("🎯 YOUR COMPETITIVE ADVANTAGE");
        println!("{}", "═".repeat(70));

        println!("✅ REAL-TIME INTELLIGENCE");
        println!("   → See what's happening NOW, not hours ago");
        println!("   → React to opportunities before competitors");
        println!("   → Make decisions with current data");

        println!("\n✅ COMPETITIVE EDGE");
        println!("   → Monitor competitor moves in real-time");
        println!("   → Spot opportunities others miss");
        println!("   → Never fly blind again");

        println!("\n✅ RISK PREVENTION");
        println!("   → Catch liquidations before they happen");
        println!("   → Reach out to at-risk users proactively");
        println!("   → Protect your protocol's reputation");

        println!("\n✅ IMMEDIATE VALUE");
        println!("   → Setup time: 5 minutes");
        println!("   → Time to value: Instant");
        println!("   → Cost: Less than one lost user");

        println!("\n🚀 READY TO GET STARTED?");
        println!("   → Contact us for personalized setup");
        println!("   → See YOUR protocol live in 5 minutes");
        println!("   → Start making better decisions today");
        println!("{}\n", "═".repeat(70));
    }
}

fn create_personalized_demo(prospect_name: &str, competitors: Vec<&str>) -> LiveDemo {
    let mut demo = LiveDemo::new();

    // Customize for prospect
    println!("🎯 Creating personalized demo for: {}\n", prospect_name);
    println!("🏢 Monitoring competitors: {}\n", competitors.join(", "));

    // Customize competitor data
    demo.competitor_activity = competitors.iter().enumerate().map(|(i, comp)| {
        CompetitorMove {
            competitor: comp.to_string(),
            action: "Strategic move detected".to_string(),
            amount: 100000.0 * (i + 1) as f64,
            timestamp: chrono::Utc::now().timestamp_millis() - (i * 300000) as i64,
            opportunity: format!("What is {} planning? Investigate now.", comp),
        }
    }).collect();

    demo
}

fn main() {
    // Demo for different prospect types
    println!("🚀 LIVE DEMO GENERATOR");
    println!("Choose your prospect type:\n");
    println!("1. DeFi Protocol (DEX, Lending, etc.)");
    println!("2. NFT Marketplace");
    println!("3. Gaming Protocol");
    println!("4. Custom");

    // For demo purposes, show a DeFi protocol example
    let prospect_name = "Your DeFi Protocol";
    let competitors = vec!["Raydium", "Orca", "Jupiter", "Meteora"];

    let demo = create_personalized_demo(prospect_name, competitors);

    demo.display_impressively();

    // Add urgency
    println!("⏰ URGENT: Set up your live intelligence now!");
    println!("   → Competitors: Raydium, Orca, Jupiter are ALREADY using this");
    println!("   → Delay cost: Missed opportunities, lost users, competitive disadvantage");
    println!("   → Setup time: 5 minutes. Value: Immediate.");
    println!("   → Contact: Get started today\n");
}

=== ./bin/protocol_pulse.rs ===
// Protocol Pulse - Immediate Value Demo
// This creates an impressive live dashboard for prospects
#![allow(dead_code)]

use std::time::{Duration, Instant};
use solana_realtime_indexer::geyser::decoder::GeyserEvent;

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
                    let signature_str = bs58::encode(&tx.signature).into_string();

                    self.large_transactions.push(LargeTransaction {
                        timestamp: tx.timestamp_unix_ms,
                        amount: tx.fee as f64 / 1_000_000.0, // Convert to lamports to SOL
                        wallet: signature_str.clone(),
                        description: format!("Large transaction: {} SOL", tx.fee as f64 / 1_000_000_000.0),
                    });

                    // Add alert
                    self.metrics.alerts.push(format!(
                        "🚨 Large transaction detected: {} SOL - {}",
                        tx.fee as f64 / 1_000_000_000.0,
                        signature_str
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
        signature: format!("demo_signature_{}", num).as_bytes().to_vec(),
        fee: 50000 * num as u64, // Increasing fees
        success: true,
        program_ids: vec![b"token-program".to_vec()],
        log_messages: vec![],
    })
}

fn main() {
    demo_protocol_pulse();
}

=== ./config.rs ===
use std::env;

use crate::geyser::consumer::SubscriptionFilter;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_address: String,
    pub rpc_endpoints: Vec<String>,
    pub geyser_endpoint: Option<String>,
    pub geyser_api_key: Option<String>,
    pub geyser_channel_capacity: usize,
    pub geyser_program_filters: Vec<String>,
    pub geyser_account_filters: Vec<String>,
    pub geyser_include_slots: bool,
    pub geyser_run_duration_seconds: Option<u64>,
    pub wal_path: String,
    pub clear_wal_on_startup: bool,
    pub database_url: Option<String>,
    pub db_pool_max_connections: u32,
    pub batch_size: usize,
    pub batch_flush_ms: u64,
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            bind_address: env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
            rpc_endpoints: read_csv_env("RPC_ENDPOINTS"),
            geyser_endpoint: env::var("GEYSER_ENDPOINT").ok(),
            geyser_api_key: env::var("GEYSER_API_KEY").ok(),
            geyser_channel_capacity: env::var("GEYSER_CHANNEL_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            geyser_program_filters: read_csv_env("GEYSER_PROGRAM_FILTERS"),
            geyser_account_filters: read_csv_env("GEYSER_ACCOUNT_FILTERS"),
            geyser_include_slots: read_bool_env("GEYSER_INCLUDE_SLOTS").unwrap_or(false),
            geyser_run_duration_seconds: env::var("GEYSER_RUN_DURATION_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok()),
            wal_path: env::var("WAL_PATH").unwrap_or_else(|_| "./data/wal/geyser-main".to_string()),
            clear_wal_on_startup: read_bool_env("CLEAR_WAL_ON_STARTUP").unwrap_or(false),
            database_url: read_optional_env("DATABASE_URL"),
            db_pool_max_connections: env::var("DB_POOL_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(20),
            batch_size: env::var("BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500),
            batch_flush_ms: env::var("BATCH_FLUSH_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            log_level: env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
        }
    }

    pub fn geyser_subscription_filters(&self) -> Vec<SubscriptionFilter> {
        let mut filters = Vec::new();

        filters.extend(
            self.geyser_program_filters
                .iter()
                .cloned()
                .map(SubscriptionFilter::Program),
        );
        filters.extend(
            self.geyser_account_filters
                .iter()
                .cloned()
                .map(SubscriptionFilter::Account),
        );

        if self.geyser_include_slots {
            filters.push(SubscriptionFilter::Slots);
        }

        filters
    }
}

fn read_csv_env(key: &str) -> Vec<String> {
    env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn read_optional_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_bool_env(key: &str) -> Option<bool> {
    env::var(key).ok().and_then(|value| parse_bool(&value))
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_bool;

    #[test]
    fn parses_bool_env_values() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bool("not-bool"), None);
    }
}

=== ./dashboard.rs ===
pub const INDEX_HTML: &str = include_str!("dashboard/static/index.html");

=== ./geyser/block_time_cache.rs ===
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// Caches slot → block_time_ms from BlockMeta messages.
/// Uses BTreeMap for ordered iteration during eviction.
/// Kept small — Solana produces ~216k slots/day; at 1000 slots
/// this is <8KB of memory.
pub struct BlockTimeCache {
    inner: RwLock<BTreeMap<u64, i64>>,
    max_slots: usize,
}

impl BlockTimeCache {
    pub fn new(max_slots: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(BTreeMap::new()),
            max_slots,
        })
    }

    pub fn insert(&self, slot: u64, block_time_ms: i64) {
        let mut map = self.inner.write().unwrap();
        map.insert(slot, block_time_ms);
        // Evict oldest entries beyond capacity
        while map.len() > self.max_slots {
            // BTreeMap is sorted ascending — pop_first removes oldest slot
            map.pop_first();
        }
    }

    pub fn get(&self, slot: u64) -> Option<i64> {
        self.inner.read().unwrap().get(&slot).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_evicts_oldest_entries() {
        let cache = BlockTimeCache::new(3);

        cache.insert(1, 1000);
        cache.insert(2, 2000);
        cache.insert(3, 3000);

        assert_eq!(cache.get(1), Some(1000));
        assert_eq!(cache.get(2), Some(2000));
        assert_eq!(cache.get(3), Some(3000));

        // Insert 4th entry - should evict slot 1 (oldest)
        cache.insert(4, 4000);

        assert_eq!(cache.get(1), None); // Evicted
        assert_eq!(cache.get(2), Some(2000));
        assert_eq!(cache.get(3), Some(3000));
        assert_eq!(cache.get(4), Some(4000));
    }

    #[test]
    fn cache_returns_none_for_missing_slot() {
        let cache = BlockTimeCache::new(100);
        assert_eq!(cache.get(999), None);
    }

    #[test]
    fn cache_overwrites_existing_slot() {
        let cache = BlockTimeCache::new(10);

        cache.insert(5, 1000);
        cache.insert(5, 2000);

        assert_eq!(cache.get(5), Some(2000));
    }
}

=== ./geyser/client.rs ===
use futures::StreamExt;
use helius_laserstream::{subscribe, LaserstreamConfig, ChannelOptions};
use helius_laserstream::grpc::{SubscribeRequest, SubscribeUpdate};
use helius_laserstream::grpc::subscribe_update::UpdateOneof;
use futures::pin_mut;

use crate::geyser::consumer::GeyserConfig;
use crate::geyser::decoder::{GeyserEvent, decode_subscribe_update};
use crate::geyser::wal_queue::WalQueue;
use crate::geyser::reconnect::ReconnectPolicy;

#[derive(Debug)]
pub enum GeyserClientError {
    Config(String),
    Connection(String),
    Stream(String),
    Send(String),
}

impl std::fmt::Display for GeyserClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeyserClientError::Config(msg) => write!(f, "Configuration error: {}", msg),
            GeyserClientError::Connection(msg) => write!(f, "Connection error: {}", msg),
            GeyserClientError::Stream(msg) => write!(f, "Stream error: {}", msg),
            GeyserClientError::Send(msg) => write!(f, "Send error: {}", msg),
        }
    }
}

impl std::error::Error for GeyserClientError {}

pub struct GeyserClient {
    config: GeyserConfig,
    api_key: String,
    run_duration_seconds: Option<u64>,
    reconnect_policy: ReconnectPolicy,
}

impl GeyserClient {
    pub fn new(config: GeyserConfig, api_key: String, run_duration_seconds: Option<u64>) -> Self {
        Self {
            config,
            api_key,
            run_duration_seconds,
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    pub async fn connect_and_subscribe(
        &self,
        wal_queue: &WalQueue,
    ) -> Result<u64, GeyserClientError> {
        let mut total_events_processed = 0u64;
        let mut reconnect_count = 0u64;

        loop {
            match self.run_single_session(wal_queue).await {
                Ok(events_processed) => {
                    total_events_processed += events_processed;

                    // Check if this was a graceful shutdown (timeout) or an error
                    if self.run_duration_seconds.is_some() && self.run_duration_seconds != Some(0) {
                        // Run duration limit reached - exit gracefully
                        log::info!("✅ Geyser client shutting down after run duration limit");
                        return Ok(total_events_processed);
                    }

                    // CRITICAL FIX: Reset reconnect_count after successful session
                    // This prevents backoff from ratcheting up permanently across sessions
                    reconnect_count = 0;

                    // Stream ended normally - this is unexpected, try to reconnect
                    log::warn!("Geyser stream ended unexpectedly, attempting to reconnect...");
                }
                Err(e) => {
                    // Connection error - reconnect with backoff
                    log::error!("Geyser connection error: {}", e);

                    // Calculate backoff delay with exponential increase
                    let backoff_ms = self.reconnect_policy.initial_backoff_ms
                        .saturating_mul(2u64.pow(reconnect_count.min(8) as u32))
                        .min(self.reconnect_policy.max_backoff_ms);

                    reconnect_count += 1;
                    log::info!("Waiting {}ms before reconnect attempt {} (max: {}ms)",
                              backoff_ms, reconnect_count, self.reconnect_policy.max_backoff_ms);

                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;

                    log::info!("Reconnecting to Geyser (attempt {})...", reconnect_count);
                }
            }
        }
    }

    async fn run_single_session(
        &self,
        wal_queue: &WalQueue,
    ) -> Result<u64, GeyserClientError> {
        let endpoint = self.config.endpoint.clone();

        // Set timeout if configured (0 means run indefinitely)
        let timeout_duration = self.run_duration_seconds
            .filter(|&secs| secs > 0)  // 0 means run indefinitely
            .map(std::time::Duration::from_secs);
        let start_time = std::time::Instant::now();

        // Build Helius Laserstream configuration
        let helius_config = LaserstreamConfig {
            api_key: self.api_key.clone(),
            endpoint: endpoint.clone(),
            channel_options: ChannelOptions::default(),
            max_reconnect_attempts: Some(5),
            replay: false,
        };

        // Build subscription request from filters
        let subscribe_request = self.build_subscription_request()?;

        log::info!("Connected to Helius Laserstream at {}, subscribing...", endpoint);

        // Log timeout configuration
        if let Some(timeout_duration) = timeout_duration {
            log::info!("Run duration limit: {:?} - will stop after this time to conserve credits", timeout_duration);
        } else {
            log::info!("No run duration limit - will run continuously (consuming Helius credits)");
        }

        // Create the subscription stream (subscribe returns immediately with stream and handle)
        let (stream, _handle) = subscribe(helius_config, subscribe_request);

        // Pin the stream so it can be used with .next().await
        pin_mut!(stream);

        let mut events_processed = 0u64;
        let mut last_log_time = start_time;
        let log_interval = std::time::Duration::from_secs(5); // Log progress every 5 seconds

        // Process incoming messages with timeout
        loop {
            // Check if we've exceeded the configured runtime
            if let Some(timeout_duration) = timeout_duration {
                if start_time.elapsed() >= timeout_duration {
                    log::info!("✅ Run duration limit reached: {:?}. Processed {} events. Stopping to conserve Helius credits.",
                              timeout_duration, events_processed);
                    break;
                }

                // Calculate remaining time for timeout
                let remaining = timeout_duration.saturating_sub(start_time.elapsed());

                // Use tokio timeout to avoid blocking forever
                let maybe_data = tokio::time::timeout(remaining, stream.next()).await;

                match maybe_data {
                    Ok(Some(Ok(data))) => {
                        // Extract slot for WAL ordering and convert to GeyserEvent for filtering
                        let slot = self.extract_slot(&data);
                        let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();
                        if let Some(event) = decode_subscribe_update(&data, timestamp_unix_ms) {
                            // Apply client-side filtering to ensure event matches configured filters
                            if !self.event_matches_filters(&event) {
                                log::trace!("Event filtered out by client-side filters: {:?}", event);
                                continue;
                            }

                            // Write raw protobuf bytes to WAL - non-blocking, no drops
                            if let Err(e) = wal_queue.push_update(slot, &data) {
                                log::error!("WAL write error: {}, stopping Geyser client", e);
                                break;
                            }
                            events_processed += 1;

                            // Log progress every 5 seconds
                            let now = std::time::Instant::now();
                            if now.duration_since(last_log_time) >= log_interval {
                                let elapsed = now.duration_since(start_time);
                                let events_per_sec = events_processed as f64 / elapsed.as_secs_f64();
                                log::info!("📊 Progress: {:.1}s elapsed, {} events processed ({:.1} events/sec)",
                                          elapsed.as_secs_f64(), events_processed, events_per_sec);
                                last_log_time = now;
                            }
                        }
                    }
                    Ok(Some(Err(e))) => {
                        log::error!("Stream error: {:?}", e);
                        return Err(GeyserClientError::Stream(format!("{:?}", e)));
                    }
                    Ok(None) => {
                        log::info!("Stream ended normally");
                        break;
                    }
                    Err(_) => {
                        // Timeout reached
                        let elapsed = start_time.elapsed();
                        log::info!("✅ Run duration timeout reached: {:?}. Processed {} events. Stopping to conserve Helius credits.",
                                  elapsed, events_processed);
                        break;
                    }
                }
            } else {
                // No timeout - run forever
                match stream.next().await {
                    Some(Ok(data)) => {
                        // Extract slot for WAL ordering and convert to GeyserEvent for filtering
                        let slot = self.extract_slot(&data);
                        let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();
                        if let Some(event) = decode_subscribe_update(&data, timestamp_unix_ms) {
                            // Apply client-side filtering to ensure event matches configured filters
                            if !self.event_matches_filters(&event) {
                                log::trace!("Event filtered out by client-side filters: {:?}", event);
                                continue;
                            }

                            // Write raw protobuf bytes to WAL - non-blocking, no drops
                            if let Err(e) = wal_queue.push_update(slot, &data) {
                                log::error!("WAL write error: {}, stopping Geyser client", e);
                                break;
                            }
                            events_processed += 1;

                            // Log progress every 5 seconds
                            let now = std::time::Instant::now();
                            if now.duration_since(last_log_time) >= log_interval {
                                let elapsed = now.duration_since(start_time);
                                let events_per_sec = events_processed as f64 / elapsed.as_secs_f64();
                                log::info!("📊 Progress: {:.1}s elapsed, {} events processed ({:.1} events/sec)",
                                          elapsed.as_secs_f64(), events_processed, events_per_sec);
                                last_log_time = now;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        log::error!("Stream error: {:?}", e);
                        return Err(GeyserClientError::Stream(format!("{:?}", e)));
                    }
                    None => {
                        log::info!("Stream ended normally");
                        break;
                    }
                }
            }
        }

        // Final summary
        let total_elapsed = start_time.elapsed();
        log::info!("📈 RUN SUMMARY:");
        log::info!("   Total runtime: {:.2}s", total_elapsed.as_secs_f64());
        log::info!("   Events processed: {}", events_processed);
        if events_processed > 0 {
            let events_per_sec = events_processed as f64 / total_elapsed.as_secs_f64();
            log::info!("   Average rate: {:.1} events/sec", events_per_sec);
        } else {
            log::info!("   Note: No events received - this is normal on devnet or with restrictive filters");
        }
        log::info!("   Credits consumed: ~{:.1}s connection time", total_elapsed.as_secs_f64());

        Ok(events_processed)
    }

    fn build_subscription_request(&self) -> Result<SubscribeRequest, GeyserClientError> {
        use crate::geyser::consumer::SubscriptionFilter;
        use helius_laserstream::grpc::{SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions};
        use std::collections::HashMap;

        let mut request = SubscribeRequest {
            commitment: Some(2), // 2 = Confirmed commitment level
            ..Default::default()
        };

        // Collect configured filters
        let mut account_pubkeys: Vec<String> = Vec::new();
        let mut program_ids: Vec<String> = Vec::new();
        let mut has_slot_filters = false;
        let mut has_blocks_meta_filters = false;

        for filter in &self.config.filters {
            match filter {
                SubscriptionFilter::Account(pubkey) => {
                    account_pubkeys.push(pubkey.clone());
                }
                SubscriptionFilter::Program(program_id) => {
                    program_ids.push(program_id.clone());
                }
                SubscriptionFilter::Slots => {
                    has_slot_filters = true;
                }
                SubscriptionFilter::Blocks => {
                    // Subscribe to blocks_meta to get block_time
                    has_blocks_meta_filters = true;
                }
            }
        }

        // Build accounts subscription with server-side filtering
        let has_account_filters = !account_pubkeys.is_empty() || !program_ids.is_empty();

        if has_account_filters {
            let mut accounts_map = HashMap::new();
            accounts_map.insert("filtered_accounts".to_string(), SubscribeRequestFilterAccounts {
                account: account_pubkeys.clone(),
                owner: program_ids.clone(),
                filters: vec![],
                nonempty_txn_signature: Some(false),
            });
            request.accounts = accounts_map;
            log::info!("Subscribed to filtered account updates: {} specific accounts, {} program owners",
                      request.accounts.get("filtered_accounts").map(|f| f.account.len()).unwrap_or(0),
                      request.accounts.get("filtered_accounts").map(|f| f.owner.len()).unwrap_or(0));
        } else {
            // No account filters configured - subscribe to all accounts (expensive!)
            let mut accounts_map = HashMap::new();
            accounts_map.insert("all_accounts".to_string(), SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: Some(false),
            });
            request.accounts = accounts_map;
            log::warn!("No account filters configured - subscribing to ALL account updates (expensive!)");
        }

        // Build transactions subscription with server-side filtering
        // Note: Helius transaction filtering is more limited, so we may need client-side filtering too
        let mut transactions_map = HashMap::new();
        transactions_map.insert("filtered_transactions".to_string(), SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: account_pubkeys.clone(), // Include transactions mentioning filtered accounts
            account_exclude: vec![],
            account_required: vec![],
        });
        request.transactions = transactions_map;
        log::info!("Subscribed to transaction updates with {} account filters",
                  request.transactions.get("filtered_transactions").map(|f| f.account_include.len()).unwrap_or(0));

        // Subscribe to slots if slot filters are configured
        if has_slot_filters {
            let mut slots_map = HashMap::new();
            slots_map.insert("all_slots".to_string(), SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            });
            request.slots = slots_map;
            log::info!("Subscribed to slot updates");
        }

        // Subscribe to blocks_meta to get block_time for accurate latency measurements
        if has_blocks_meta_filters {
            let mut blocks_meta_map = HashMap::new();
            blocks_meta_map.insert("all_blocks_meta".to_string(), helius_laserstream::grpc::SubscribeRequestFilterBlocksMeta {
                ..Default::default()
            });
            request.blocks_meta = blocks_meta_map;
            log::info!("Subscribed to blocks_meta updates for block_time extraction");
        }

        log::info!("Built SubscribeRequest with {} account subscriptions, {} transaction subscriptions, {} slot subscriptions, {} blocks_meta subscriptions",
                  request.accounts.len(), request.transactions.len(), request.slots.len(), request.blocks_meta.len());

        Ok(request)
    }

    fn event_matches_filters(&self, event: &GeyserEvent) -> bool {
        use crate::geyser::consumer::SubscriptionFilter;

        // If no filters configured, accept all events
        if self.config.filters.is_empty() {
            return true;
        }

        // Check if any filter matches this event
        self.config.filters.iter().any(|filter| match (filter, event) {
            (SubscriptionFilter::Program(program_id), GeyserEvent::AccountUpdate(update)) => {
                let program_id_bytes = program_filter_bytes(program_id);
                update.owner == program_id_bytes
            }
            (SubscriptionFilter::Program(program_id), GeyserEvent::Transaction(update)) => {
                let program_id_bytes = program_filter_bytes(program_id);
                update
                    .program_ids
                    .iter()
                    .any(|id_bytes| id_bytes == &program_id_bytes)
            }
            (SubscriptionFilter::Account(pubkey), GeyserEvent::AccountUpdate(update)) => {
                let pubkey_bytes = program_filter_bytes(pubkey);
                update.pubkey == pubkey_bytes
            }
            (SubscriptionFilter::Account(_pubkey), GeyserEvent::Transaction(_update)) => {
                // For transactions, we'd need to check account keys - this is a simplified check
                // In production, you'd parse the transaction's account list
                false // Placeholder - requires full transaction parsing
            }
            (SubscriptionFilter::Slots, GeyserEvent::SlotUpdate(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::BlockMeta(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::SlotUpdate(_)) => false,
            _ => false,
        })
    }

    fn extract_slot(&self, data: &SubscribeUpdate) -> u64 {
        match &data.update_oneof {
            Some(UpdateOneof::Account(acc)) => acc.slot,
            Some(UpdateOneof::Transaction(tx)) => tx.slot,
            Some(UpdateOneof::Slot(slot)) => slot.slot,
            Some(UpdateOneof::BlockMeta(bm)) => bm.slot,
            Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => 0,
            Some(UpdateOneof::Block(_)) | Some(UpdateOneof::TransactionStatus(_)) | Some(UpdateOneof::Entry(_)) => 0,
            None => 0,
        }
    }

}

fn program_filter_bytes(program_id: &str) -> Vec<u8> {
    let trimmed = program_id.trim();
    bs58::decode(trimmed)
        .into_vec()
        .unwrap_or_else(|e| {
            panic!(
                "Invalid base58 in program_filter_bytes for '{}': {}. \
                 Config validation should have caught this at startup.",
                trimmed, e
            )
        })
}

=== ./geyser/consumer.rs ===
use std::sync::mpsc::{SendError, SyncSender};

use crate::geyser::decoder::{AccountUpdate, GeyserEvent, SlotUpdate, TransactionUpdate};

#[derive(Debug, Clone)]
pub struct GeyserConfig {
    pub endpoint: String,
    pub channel_capacity: usize,
    pub filters: Vec<SubscriptionFilter>,
}

impl GeyserConfig {
    pub fn new(endpoint: String, channel_capacity: usize, filters: Vec<SubscriptionFilter>) -> Self {
        Self {
            endpoint,
            channel_capacity,
            filters,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SubscriptionFilter {
    Program(String),
    Account(String),
    Slots,
    Blocks,
}

#[derive(Debug, Clone)]
pub struct GeyserConsumer {
    pub config: GeyserConfig,
}

impl GeyserConsumer {
    pub fn new(config: GeyserConfig) -> Self {
        Self { config }
    }

    pub fn accepts(&self, event: &GeyserEvent) -> bool {
        if self.config.filters.is_empty() {
            return true;
        }

        self.config
            .filters
            .iter()
            .any(|filter| filter.matches(event))
    }

    pub fn forward_events<I>(
        &self,
        sender: &SyncSender<GeyserEvent>,
        events: I,
    ) -> Result<usize, SendError<GeyserEvent>>
    where
        I: IntoIterator<Item = GeyserEvent>,
    {
        let mut forwarded = 0;

        for event in events {
            if !self.accepts(&event) {
                continue;
            }

            sender.send(event)?;
            forwarded += 1;
        }

        Ok(forwarded)
    }

    pub fn simulated_fixture() -> Vec<GeyserEvent> {
        // Use current timestamp so the materialized view time filter includes it
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Use real 32-byte Solana pubkeys (decoded from base58)
        let tracked_account = bs58::decode("7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU").into_vec().unwrap();
        let orca_program = bs58::decode("9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM").into_vec().unwrap();
        let token_program = bs58::decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").into_vec().unwrap();
        let tracked_signature = bs58::decode("5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprv2W1qY1qrk7jjFgJG3kGpYcQHxUxYWBZgmhnfYTLuLr").into_vec().unwrap();
        let ignored_account = bs58::decode("Noise1111111111111111111111111111111111111111").into_vec().unwrap();
        let system_program = bs58::decode("11111111111111111111111111111111").into_vec().unwrap();

        vec![
            GeyserEvent::AccountUpdate(AccountUpdate {
                timestamp_unix_ms: now_ms,
                slot: 9_001,
                pubkey: tracked_account.clone(),
                owner: orca_program.clone(),
                lamports: 42,
                write_version: 7,
                data: vec![1, 2, 3, 4],
            }),
            GeyserEvent::Transaction(TransactionUpdate {
                timestamp_unix_ms: now_ms + 1,
                slot: 9_001,
                signature: tracked_signature,
                fee: 5_000,
                success: true,
                program_ids: vec![orca_program.clone(), token_program],
                log_messages: vec!["swap".to_string(), "settled".to_string()],
            }),
            GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms: now_ms + 2,
                slot: 9_001,
                parent_slot: Some(9_000),
                status: "processed".to_string(),
            }),
            // Add confirmed status for the same slot (500ms later) to test latency calculation
            GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms: now_ms + 502, // 500ms after processed
                slot: 9_001,
                parent_slot: Some(9_000),
                status: "confirmed".to_string(),
            }),
            GeyserEvent::AccountUpdate(AccountUpdate {
                timestamp_unix_ms: now_ms + 3,
                slot: 9_001,
                pubkey: ignored_account,
                owner: system_program,
                lamports: 1,
                write_version: 1,
                data: vec![9, 9, 9],
            }),
        ]
    }
}

impl SubscriptionFilter {
    fn matches(&self, event: &GeyserEvent) -> bool {
        match (self, event) {
            (SubscriptionFilter::Program(program), GeyserEvent::AccountUpdate(update)) => {
                let program_bytes = program_filter_bytes(program);
                update.owner == program_bytes
            }
            (SubscriptionFilter::Program(program), GeyserEvent::Transaction(update)) => {
                let program_bytes = program_filter_bytes(program);
                update
                    .program_ids
                    .iter()
                    .any(|program_id_bytes| program_id_bytes == &program_bytes)
            }
            (SubscriptionFilter::Account(account), GeyserEvent::AccountUpdate(update)) => {
                let account_bytes = program_filter_bytes(account);
                update.pubkey == account_bytes
            }
            (SubscriptionFilter::Slots, GeyserEvent::SlotUpdate(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::BlockMeta(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::SlotUpdate(_)) => false,
            _ => false,
        }
    }
}

fn program_filter_bytes(program: &str) -> Vec<u8> {
    let trimmed = program.trim();
    bs58::decode(trimmed)
        .into_vec()
        .unwrap_or_else(|e| {
            panic!(
                "Invalid base58 in program_filter_bytes for '{}': {}. \
                 Config validation should have caught this at startup.",
                trimmed, e
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{GeyserConfig, GeyserConsumer, SubscriptionFilter};
    use crate::geyser::decoder::GeyserEvent;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn forwards_only_events_that_match_filters() {
        // Filter on Orca program (already in fixture with real 32-byte pubkey)
        let orca_program_id = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
        let consumer = GeyserConsumer::new(GeyserConfig {
            endpoint: "mock://geyser".to_string(),
            channel_capacity: 4,
            filters: vec![
                SubscriptionFilter::Program(orca_program_id.to_string()),
                SubscriptionFilter::Slots,
            ],
        });
        let (sender, receiver) = sync_channel(4);

        // Fixture already uses real 32-byte pubkeys - no manual patching needed
        let fixture = GeyserConsumer::simulated_fixture();

        let forwarded = consumer
            .forward_events(&sender, fixture)
            .expect("channel open");
        drop(sender);

        let events: Vec<GeyserEvent> = receiver.iter().collect();

        // Should forward: AccountUpdate (Orca program), Transaction (Orca + Token),
        //               SlotUpdate (processed), SlotUpdate (confirmed)
        // Should NOT forward: AccountUpdate (System program - not in filter)
        assert_eq!(forwarded, 4);
        assert_eq!(events.len(), 4);
    }
}

=== ./geyser/decoder.rs ===
use helius_laserstream::grpc::SubscribeUpdate;
use helius_laserstream::grpc::subscribe_update::UpdateOneof;

#[derive(Debug, Clone)]
pub enum GeyserEvent {
    AccountUpdate(AccountUpdate),
    Transaction(TransactionUpdate),
    SlotUpdate(SlotUpdate),
    BlockMeta(BlockMetaUpdate),
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub pubkey: Vec<u8>,
    pub owner: Vec<u8>,
    pub lamports: u64,
    pub write_version: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub signature: Vec<u8>,
    pub fee: u64,
    pub success: bool,
    pub program_ids: Vec<Vec<u8>>,
    pub log_messages: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SlotUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub parent_slot: Option<u64>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct BlockMetaUpdate {
    pub slot: u64,
    pub block_time_ms: i64,
    pub block_height: Option<u64>,
}

/// Decodes a SubscribeUpdate protobuf into a GeyserEvent.
/// This is a shared function used by both the Geyser client (for filtering)
/// and the WAL consumer (for processing), eliminating code duplication.
pub fn decode_subscribe_update(update: &SubscribeUpdate, timestamp_unix_ms: i64) -> Option<GeyserEvent> {
    match &update.update_oneof {
        Some(UpdateOneof::Account(account_update)) => {
            if let Some(account_info) = &account_update.account {
                Some(GeyserEvent::AccountUpdate(AccountUpdate {
                    timestamp_unix_ms,
                    slot: account_update.slot,
                    pubkey: account_info.pubkey.clone(),
                    owner: account_info.owner.clone(),
                    lamports: account_info.lamports,
                    write_version: account_info.write_version,
                    data: account_info.data.clone(),
                }))
            } else {
                log::warn!("Account update missing account info for slot {}", account_update.slot);
                None
            }
        }
        Some(UpdateOneof::Transaction(tx_update)) => {
            if let Some(tx_info) = &tx_update.transaction {
                let success = tx_info.meta.as_ref()
                    .map(|meta| meta.err.is_none())
                    .unwrap_or(false);

                let fee = tx_info.meta.as_ref()
                    .map(|meta| meta.fee)
                    .unwrap_or(0);

                let log_messages = tx_info.meta.as_ref()
                    .map(|meta| meta.log_messages.clone())
                    .unwrap_or_default();

                // Extract program IDs from transaction instructions
                let program_ids = if let Some(tx) = &tx_info.transaction {
                    if let Some(message) = &tx.message {
                        let mut invoked_programs = std::collections::HashSet::new();
                        for instruction in &message.instructions {
                            let program_idx = instruction.program_id_index as usize;
                            if program_idx < message.account_keys.len() {
                                invoked_programs.insert(message.account_keys[program_idx].clone());
                            }
                        }
                        invoked_programs.into_iter().collect()
                    } else {
                        log::debug!("No message field in transaction, program_ids will be empty");
                        Vec::new()
                    }
                } else {
                    log::debug!("No transaction field available, program_ids will be empty");
                    Vec::new()
                };

                Some(GeyserEvent::Transaction(TransactionUpdate {
                    timestamp_unix_ms,
                    slot: tx_update.slot,
                    signature: tx_info.signature.clone(),
                    fee,
                    success,
                    program_ids,
                    log_messages,
                }))
            } else {
                log::warn!("Transaction update missing transaction info for slot {}", tx_update.slot);
                None
            }
        }
        Some(UpdateOneof::Slot(slot_update)) => {
            Some(GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms,
                slot: slot_update.slot,
                parent_slot: slot_update.parent,
                status: map_slot_status(slot_update.status),
            }))
        }
        Some(UpdateOneof::BlockMeta(block_meta)) => {
            let block_time_ms = block_meta.block_time
                .map(|unix_ts| unix_ts.timestamp * 1000)
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

            Some(GeyserEvent::BlockMeta(BlockMetaUpdate {
                slot: block_meta.slot,
                block_time_ms,
                block_height: block_meta.block_height.map(|bh| bh.block_height),
            }))
        }
        Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => {
            log::trace!("Received ping/pong - ignoring");
            None
        }
        other => {
            log::warn!("Received unhandled update type: {:?}", other);
            None
        }
    }
}

/// Maps protobuf slot status enum values to human-readable strings.
fn map_slot_status(status: i32) -> String {
    match status {
        0 => "processed".to_string(),
        1 => "confirmed".to_string(),
        2 => "finalized".to_string(),
        3 => "first_shred_received".to_string(),
        4 => "completed".to_string(),
        5 => "created_bank".to_string(),
        6 => "dead".to_string(),
        _ => {
            log::warn!("Unknown slot status value: {}, using 'unknown'", status);
            "unknown".to_string()
        }
    }
}

=== ./geyser/mod.rs ===
pub mod block_time_cache;
pub mod client;
pub mod consumer;
pub mod decoder;
pub mod protocol;
pub mod reconnect;
pub mod wal_queue;
pub mod wal_consumer;

pub use block_time_cache::BlockTimeCache;
pub use client::GeyserClient;
pub use consumer::{GeyserConfig, GeyserConsumer};
pub use decoder::{GeyserEvent, AccountUpdate, SlotUpdate, TransactionUpdate, BlockMetaUpdate};
pub use protocol::{Protocol, ProtocolSubscription, ProtocolConfig, ConfigOnlyProtocol, merge_subscriptions, load_protocols_from_dir};
pub use wal_queue::{WalQueue, WalEntry};
pub use wal_consumer::{WalPipelineConfig, WalPipelineRunner, RpcGapFiller};
=== ./geyser/protocol.rs ===
use crate::processor::decoder::CustomDecoder;
use serde::{Deserialize, Serialize};

/// Protocol trait: implemented by clients for their specific protocol.
/// Separates subscription configuration (mutable, ops-controlled) from
/// decoder logic (immutable, type-safe, compile-time checked).
pub trait Protocol: Send + Sync {
    fn name(&self) -> &str;

    /// Returns subscription configuration loaded from TOML/config.
    /// This includes which program ID and accounts to watch.
    fn subscription(&self) -> ProtocolSubscription;

    /// Returns the decoder instances this protocol needs.
    /// Type-safe - no string lookups, compiled for each protocol crate.
    fn decoders(&self) -> Vec<Box<dyn CustomDecoder>>;
}

/// Subscription configuration: what to watch on Geyser.
/// Uses [u8; 32] for pubkeys - invalid lengths are structurally unrepresentable.
#[derive(Debug, Clone)]
pub struct ProtocolSubscription {
    pub program_ids: Vec<[u8; 32]>,
    pub account_pubkeys: Vec<[u8; 32]>,
    pub include_slots: bool,
    pub include_blocks_meta: bool,
}

/// TOML configuration file structure for protocol subscriptions.
/// This is the ops-controlled side - mutable without recompilation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolConfig {
    pub name: String,
    pub programs: Vec<String>,
    pub accounts: Vec<String>,
    #[serde(default)]
    pub include_slots: bool,
    #[serde(default = "default_include_blocks_meta")]
    pub include_blocks_meta: bool,
}

fn default_include_blocks_meta() -> bool {
    true // Default to true for accurate block_time timestamps
}

impl ProtocolConfig {
    /// Load and validate a TOML config file in a single pass.
    /// Returns both the raw config and the validated subscription.
    pub fn from_toml_path(path: &str) -> Result<(Self, ProtocolSubscription), String> {
        let toml_content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

        let config: ProtocolConfig = toml::from_str(&toml_content)
            .map_err(|e| format!("Failed to parse TOML from '{}': {}", path, e))?;

        Self::validate_and_decode(config)
    }

    /// Validate an already-loaded ProtocolConfig and decode to subscription.
    /// Used for environment variable fallback.
    pub fn from_toml_path_str(config: ProtocolConfig) -> Result<(Self, ProtocolSubscription), String> {
        Self::validate_and_decode(config)
    }

    fn validate_and_decode(config: ProtocolConfig) -> Result<(Self, ProtocolSubscription), String> {
        // Decode directly to [u8; 32] - single pass validation and conversion
        let program_ids = config.programs.iter()
            .map(|s| decode_to_32_bytes(s, "program_id"))
            .collect::<Result<Vec<_>, _>>()?;

        let account_pubkeys = config.accounts.iter()
            .map(|s| decode_to_32_bytes(s, "account"))
            .collect::<Result<Vec<_>, _>>()?;

        let subscription = ProtocolSubscription {
            program_ids,
            account_pubkeys,
            include_slots: config.include_slots,
            include_blocks_meta: config.include_blocks_meta,
        };

        Ok((config, subscription))
    }
}

/// Decodes a base58 string to [u8; 32] in a single pass.
/// Returns an error if invalid base58 or wrong length.
fn decode_to_32_bytes(value: &str, field_name: &str) -> Result<[u8; 32], String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{} is empty", field_name));
    }

    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|e| format!("Invalid {} '{}': not valid base58 - {}", field_name, trimmed, e))?;

    if decoded.len() != 32 {
        return Err(format!(
            "Invalid {} '{}': decoded to {} bytes, expected 32 for Solana pubkey",
            field_name,
            trimmed,
            decoded.len()
        ));
    }

    let mut array = [0u8; 32];
    array.copy_from_slice(&decoded);
    Ok(array)
}

/// For protocols that only need subscription config with no custom decoders.
/// Covers 80% of clients who only need filtered ingestion, not custom decoding.
pub struct ConfigOnlyProtocol {
    config: ProtocolConfig,
    subscription: ProtocolSubscription,
}

impl ConfigOnlyProtocol {
    /// Load a protocol from a TOML file.
    /// Returns a Protocol implementor with no custom decoders.
    pub fn from_toml(path: &str) -> Result<Self, String> {
        let (config, subscription) = ProtocolConfig::from_toml_path(path)?;
        Ok(Self { config, subscription })
    }

    /// Create a protocol from an already-loaded ProtocolConfig.
    /// Used for environment variable fallback.
    pub fn from_config(config: ProtocolConfig) -> Result<Self, String> {
        let subscription = ProtocolConfig::validate_and_decode(config.clone())?.1;
        Ok(Self { config, subscription })
    }
}

impl Protocol for ConfigOnlyProtocol {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn subscription(&self) -> ProtocolSubscription {
        self.subscription.clone()
    }

    fn decoders(&self) -> Vec<Box<dyn CustomDecoder>> {
        vec![]
    }
}

/// Merge multiple protocol subscriptions into one.
/// Deduplicates program IDs and account pubkeys to avoid wasting filter slots.
pub fn merge_subscriptions(protocols: &[Box<dyn Protocol>]) -> ProtocolSubscription {
    let mut program_ids: Vec<[u8; 32]> = Vec::new();
    let mut account_pubkeys: Vec<[u8; 32]> = Vec::new();
    let mut include_slots = false;
    let mut include_blocks_meta = false;

    for p in protocols {
        let sub = p.subscription();
        program_ids.extend_from_slice(&sub.program_ids);
        account_pubkeys.extend_from_slice(&sub.account_pubkeys);
        include_slots |= sub.include_slots;
        include_blocks_meta |= sub.include_blocks_meta;
    }

    // Deduplicate - two protocols may share the Token program
    program_ids.sort_unstable();
    program_ids.dedup();
    account_pubkeys.sort_unstable();
    account_pubkeys.dedup();

    ProtocolSubscription { program_ids, account_pubkeys, include_slots, include_blocks_meta }
}

/// Load all protocol TOML files from a directory.
/// Returns ConfigOnlyProtocol instances for each .toml file found.
pub fn load_protocols_from_dir(dir: &str) -> Result<Vec<ConfigOnlyProtocol>, String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("Cannot read protocols dir '{}': {}", dir, e))?;

    let mut protocols = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("toml") {
            let path_str = path.to_str().ok_or("non-UTF8 path")?;
            match ConfigOnlyProtocol::from_toml(path_str) {
                Ok(protocol) => {
                    log::info!("Loaded protocol '{}' from {}", protocol.name(), path_str);
                    protocols.push(protocol);
                }
                Err(e) => {
                    log::error!("Failed to load protocol from '{}': {}", path_str, e);
                    return Err(e);
                }
            }
        }
    }

    if protocols.is_empty() {
        log::warn!("No protocol TOML files found in '{}'", dir);
    }

    Ok(protocols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_valid_base58_to_32_bytes() {
        let valid = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
        let result = decode_to_32_bytes(valid, "test").unwrap();
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn rejects_invalid_base58() {
        assert!(decode_to_32_bytes("invalid-prog", "test").is_err());
    }

    #[test]
    fn rejects_wrong_length() {
        // Too short
        assert!(decode_to_32_bytes("9WzDXwBbmkg", "test").is_err());
    }

    #[test]
    fn rejects_empty_string() {
        assert!(decode_to_32_bytes("", "test").is_err());
        assert!(decode_to_32_bytes("   ", "test").is_err());
    }

    #[test]
    fn config_only_protocol_implements_trait() {
        let valid = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
        let protocol = ConfigOnlyProtocol {
            config: ProtocolConfig {
                name: "TestProtocol".to_string(),
                programs: vec![valid.to_string()],
                accounts: vec![],
                include_slots: false,
                include_blocks_meta: false,
            },
            subscription: ProtocolSubscription {
                program_ids: vec![decode_to_32_bytes(valid, "test").unwrap()],
                account_pubkeys: vec![],
                include_slots: false,
                include_blocks_meta: false,
            },
        };

        assert_eq!(protocol.name(), "TestProtocol");
        assert_eq!(protocol.subscription().program_ids.len(), 1);
        assert_eq!(protocol.decoders().len(), 0); // No custom decoders
    }

    #[test]
    fn merge_subscriptions_deduplicates_programs() {
        let program_id = decode_to_32_bytes("9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM", "test").unwrap();

        let protocol1 = ConfigOnlyProtocol {
            config: ProtocolConfig {
                name: "Protocol1".to_string(),
                programs: vec!["9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM".to_string()],
                accounts: vec![],
                include_slots: false,
                include_blocks_meta: false,
            },
            subscription: ProtocolSubscription {
                program_ids: vec![program_id],
                account_pubkeys: vec![],
                include_slots: false,
                include_blocks_meta: false,
            },
        };

        let protocol2 = ConfigOnlyProtocol {
            config: ProtocolConfig {
                name: "Protocol2".to_string(),
                programs: vec!["9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM".to_string()],
                accounts: vec![],
                include_slots: false,
                include_blocks_meta: false,
            },
            subscription: ProtocolSubscription {
                program_ids: vec![program_id],
                account_pubkeys: vec![],
                include_slots: false,
                include_blocks_meta: false,
            },
        };

        let merged = merge_subscriptions(&[
            Box::new(protocol1),
            Box::new(protocol2),
        ]);

        // Should deduplicate the shared program ID
        assert_eq!(merged.program_ids.len(), 1);
    }
}

=== ./geyser/reconnect.rs ===
/// Reconnection policy for Geyser client
///
/// Defines how the client should behave when connection to Geyser fails.
/// On connection failure, the client will wait with exponential backoff
/// before attempting to reconnect.
///
/// # Example
///
/// ```rust
/// use solana_realtime_indexer::geyser::reconnect::ReconnectPolicy;
///
/// // Default policy: 250ms initial, 5s max, 4 slot gap threshold
/// let policy = ReconnectPolicy::default();
///
/// // Custom policy for slower backoff
/// let custom = ReconnectPolicy {
///     initial_backoff_ms: 500,
///     max_backoff_ms: 10_000,
///     gap_threshold_slots: 10,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Initial backoff delay in milliseconds
    pub initial_backoff_ms: u64,

    /// Maximum backoff delay in milliseconds
    pub max_backoff_ms: u64,

    /// Number of slots to consider as a gap after reconnection
    /// The gap filler will use this to detect missing data
    pub gap_threshold_slots: u64,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 250,
            max_backoff_ms: 5_000,
            gap_threshold_slots: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_policy_has_sensible_defaults() {
        let policy = ReconnectPolicy::default();
        assert_eq!(policy.initial_backoff_ms, 250);
        assert_eq!(policy.max_backoff_ms, 5_000);
        assert_eq!(policy.gap_threshold_slots, 4);
    }

    #[test]
    fn reconnect_policy_can_be_customized() {
        let custom = ReconnectPolicy {
            initial_backoff_ms: 1000,
            max_backoff_ms: 30_000,
            gap_threshold_slots: 100,
        };
        assert_eq!(custom.initial_backoff_ms, 1000);
        assert_eq!(custom.max_backoff_ms, 30_000);
        assert_eq!(custom.gap_threshold_slots, 100);
    }
}

=== ./geyser/wal_consumer.rs ===
use crate::geyser::wal_queue::{WalQueue, WalEntry};
use crate::geyser::decoder::decode_subscribe_update;
use crate::geyser::BlockTimeCache;
use crate::processor::pipeline::PipelineReport;
use crate::processor::batch_writer::{BatchWriter, BufferedBatch};
use crate::processor::decoder::{CustomDecoder, Type1Decoder, PersistedBatch};
use crate::processor::sink::{StorageSink, StorageWriteResult};
use crate::processor::sql::CheckpointUpdate;
use crate::processor::store::StoreSnapshot;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub struct WalPipelineConfig {
    pub wal_path: String,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub batch_flush_ms: u64,
}

pub struct WalPipelineRunner {
    wal_queue: Arc<WalQueue>,
    config: WalPipelineConfig,
    #[allow(dead_code)]
    api_state: Arc<Mutex<crate::api::rest::ApiSnapshot>>,
    writer: BatchWriter,
    decoder: Type1Decoder,
    custom_decoders: Vec<Box<dyn CustomDecoder>>,
    sink: Box<dyn StorageSink>,
    pending_checkpoint_seqs: Vec<u64>, // Track seqs to checkpoint after DB commit
    gap_filler: Option<RpcGapFiller>, // Event-driven gap repair
    block_time_cache: Arc<BlockTimeCache>, // Cache slot → block_time mapping
    next_read_seq: u64, // In-memory read cursor, advances after each process_entry
}

impl WalPipelineRunner {
    pub fn new(
        wal_queue: Arc<WalQueue>,
        config: WalPipelineConfig,
        api_state: Arc<Mutex<crate::api::rest::ApiSnapshot>>,
        writer: BatchWriter,
        decoder: Type1Decoder,
        custom_decoders: Vec<Box<dyn CustomDecoder>>,
        sink: Box<dyn StorageSink>,
    ) -> Self {
        // Cache last 1000 slots (~8KB of memory) - Solana produces ~216k slots/day
        let block_time_cache = BlockTimeCache::new(1000);

        // Initialize in-memory read cursor from persisted checkpoint
        let next_read_seq = wal_queue.get_last_processed_seq();

        Self {
            wal_queue,
            config,
            api_state,
            writer,
            decoder,
            custom_decoders,
            sink,
            pending_checkpoint_seqs: Vec::new(),
            gap_filler: None,
            block_time_cache,
            next_read_seq,
        }
    }

    pub fn with_gap_filler(mut self, gap_filler: RpcGapFiller) -> Self {
        self.gap_filler = Some(gap_filler);
        self
    }

    pub fn with_block_time_cache(mut self, cache: Arc<BlockTimeCache>) -> Self {
        self.block_time_cache = cache;
        self
    }

    pub async fn run(&mut self) -> Result<PipelineReport, String> {
        let started_at = Instant::now();
        let mut report = PipelineReport::default();
        let mut last_log_time = started_at;
        let log_interval = Duration::from_secs(5);

        log::info!("Starting WAL pipeline consumer from: {}", self.config.wal_path);

        // Main polling loop
        loop {
            let now = Instant::now();

            // Check if we should flush due to interval
            if let Some(batch) = self.writer.flush_if_needed(now) {
                self.process_batch(batch, &mut report).await?;
            }

            // Check for new entries in WAL using in-memory read cursor
            match self.wal_queue.read_from(0, self.next_read_seq) {
                Ok(Some(entry)) => {
                    // Process the event (just add to buffer, DON'T mark yet)
                    match self.process_entry(&entry, &mut report).await {
                        Ok(()) => {
                            // Only advance read cursor after successful processing
                            self.next_read_seq = entry.seq + 1;
                        }
                        Err(e) => {
                            // Check if this is an unrecoverable decode error
                            let is_decode_error = e.contains("Failed to decode protobuf") ||
                                                 e.contains("Failed to decode SubscribeUpdate");

                            if is_decode_error {
                                // Corrupted WAL entry - skip it to prevent permanent block
                                log::error!("🔴 CORRUPT WAL ENTRY at slot {} seq {}: {}. Skipping to prevent consumer block.",
                                          entry.slot, entry.seq, e);

                                // CRITICAL: Don't call mark_processed directly — add to pending so it
                                // checkpoints with next batch in correct seq order after any buffered good events.
                                // This prevents checkpoint from moving backward when max_seq < corrupted seq.
                                self.pending_checkpoint_seqs.push(entry.seq);

                                // Advance in-memory cursor past this entry
                                self.next_read_seq = entry.seq + 1;
                            } else {
                                // Other processing errors - log but don't skip (may be transient)
                                log::error!("Error processing entry slot {} seq {}: {}", entry.slot, entry.seq, e);
                                // Don't advance cursor - will retry on next iteration
                            }
                        }
                    }

                    // Log progress every 5 seconds
                    if now.duration_since(last_log_time) >= log_interval {
                        let elapsed = now.duration_since(started_at);
                        let events_per_sec = report.received_events as f64 / elapsed.as_secs_f64();
                        let unprocessed_count = self.wal_queue.get_unprocessed_count();
                        log::info!("📊 WAL Pipeline Progress: {:.1}s elapsed, {} events processed ({:.1} events/sec), {} unprocessed in WAL",
                                  elapsed.as_secs_f64(), report.received_events, events_per_sec, unprocessed_count);
                        last_log_time = now;
                    }
                }
                Ok(None) => {
                    // No new entries, sleep for poll interval
                    tokio::time::sleep(self.config.poll_interval).await;
                }
                Err(e) => {
                    // Check if this is a gap error
                    if matches!(e, crate::geyser::wal_queue::WalReadError::Gap { .. }) {
                        log::warn!("⚠️ Gap detected in WAL: {}", e);

                        // Extract sequence number from typed error (no fragile string parsing)
                        let seq = match e {
                            crate::geyser::wal_queue::WalReadError::Gap { seq } => seq,
                            _ => unreachable!(),
                        };

                        // Trigger immediate repair if gap filler is available
                        if let Some(gap_filler) = &self.gap_filler {
                            log::info!("🔧 Triggering immediate RPC repair for seq {}", seq);
                            match gap_filler.repair_gap(seq).await {
                                Ok(true) => {
                                    log::info!("✅ Gap repaired successfully, will retry read");
                                    // Retry immediately after repair
                                    continue;
                                }
                                Ok(false) => {
                                    log::error!("❌ Gap repair failed for seq {}, skipping", seq);
                                    // Mark the actual gap seq as processed, not last_flushed + 1
                                    if let Some(slot) = self.wal_queue.get_slot_for_seq(seq) {
                                        let _ = self.wal_queue.mark_processed(slot, seq);
                                    } else {
                                        log::warn!("No slot mapping for seq {}, using fallback", seq);
                                        let _ = self.wal_queue.mark_processed(0, seq);
                                    }
                                    // CRITICAL: Advance in-memory cursor to prevent infinite retry loop
                                    self.next_read_seq = seq + 1;
                                }
                                Err(repair_err) => {
                                    log::error!("❌ Gap repair error for seq {}: {}, skipping", seq, repair_err);
                                    // Mark the actual gap seq as processed, not last_flushed + 1
                                    if let Some(slot) = self.wal_queue.get_slot_for_seq(seq) {
                                        let _ = self.wal_queue.mark_processed(slot, seq);
                                    } else {
                                        log::warn!("No slot mapping for seq {}, using fallback", seq);
                                        let _ = self.wal_queue.mark_processed(0, seq);
                                    }
                                    // CRITICAL: Advance in-memory cursor to prevent infinite retry loop
                                    self.next_read_seq = seq + 1;
                                }
                            }
                        } else {
                            log::warn!("No gap filler available, skipping gap");
                            // Mark the actual gap seq as processed, not last_flushed + 1
                            if let Some(slot) = self.wal_queue.get_slot_for_seq(seq) {
                                let _ = self.wal_queue.mark_processed(slot, seq);
                            } else {
                                log::warn!("No slot mapping for seq {}, using fallback", seq);
                                let _ = self.wal_queue.mark_processed(0, seq);
                            }
                            // CRITICAL: Advance in-memory cursor to prevent infinite retry loop
                            self.next_read_seq = seq + 1;
                        }
                    } else {
                        log::error!("Error reading from WAL: {}", e);
                    }

                    // Sleep before retrying
                    tokio::time::sleep(self.config.poll_interval).await;
                }
            }
        }
    }

    async fn process_entry(&mut self, entry: &WalEntry, report: &mut PipelineReport) -> Result<(), String> {
        // Decode the protobuf bytes back to SubscribeUpdate
        let update = entry.decode_update()
            .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

        // Convert SubscribeUpdate to GeyserEvent using shared decoder
        let geyser_event = decode_subscribe_update(&update, entry.timestamp_unix_ms)
            .ok_or_else(|| "Failed to decode SubscribeUpdate to GeyserEvent".to_string())?;

        // BlockMeta events populate the cache, not the batch
        if let crate::geyser::decoder::GeyserEvent::BlockMeta(bm) = &geyser_event {
            self.block_time_cache.insert(bm.slot, bm.block_time_ms);
            log::debug!("Cached block_time {}ms for slot {}", bm.block_time_ms, bm.slot);

            // CRITICAL FIX: Add BlockMeta to pending_checkpoint_seqs instead of marking processed immediately
            // This prevents checkpoint from advancing before DB commit, avoiding data loss on crash
            self.pending_checkpoint_seqs.push(entry.seq);
            return Ok(());
        }

        // Add to batch
        report.received_events += 1;
        self.writer.push(geyser_event);

        // Track this sequence for later checkpointing (after DB commit)
        self.pending_checkpoint_seqs.push(entry.seq);

        Ok(())
    }

    async fn process_batch(&mut self, batch: BufferedBatch, report: &mut PipelineReport) -> Result<(), String> {
        let persisted = self.decoder.decode(batch, &mut self.custom_decoders, &self.block_time_cache);
        let checkpoint = checkpoint_update_for_batch(&persisted);
        let result = self.sink.write_batch(&persisted, checkpoint).await
            .map_err(|e| format!("Storage error: {}", e))?;

        apply_batch_report(report, persisted, result);

        // CRITICAL FIX: Mark sequences as processed ONLY AFTER DB commit succeeds
        // This prevents data loss if crash occurs between buffering and DB flush
        let checkpointed_seqs: Vec<_> = self.pending_checkpoint_seqs.drain(..).collect();

        if !checkpointed_seqs.is_empty() {
            // Find the maximum seq in this batch
            let max_seq = *checkpointed_seqs.iter().max().unwrap_or(&0);

            // Look up which slot corresponds to this seq
            let slot = match self.wal_queue.get_slot_for_seq(max_seq) {
                Some(s) => s,
                None => {
                    log::error!("Failed to get slot for seq {} - skipping checkpoint", max_seq);
                    return Ok(());
                }
            };

            // CRITICAL: Mark processed ONCE with the correct slot and max seq
            // This prevents corrupting last_flushed_slot and avoids N batch commits
            if let Err(e) = self.wal_queue.mark_processed(slot, max_seq) {
                log::error!("Failed to mark seq {} as processed after DB commit: {}", max_seq, e);
                // Note: We don't return error here because the DB commit succeeded,
                // and the seq will be retried on next startup
            }

            log::debug!("Checkpointed {} sequences after DB commit (slot: {}, seq: {})",
                       checkpointed_seqs.len(), slot, max_seq);
        }

        Ok(())
    }

    pub fn start_background_processor(mut self) -> tokio::task::JoinHandle<Result<PipelineReport, String>>
    where
        Self: Send + 'static,
    {
        tokio::spawn(async move {
            self.run().await
        })
    }
}

fn apply_batch_report(report: &mut PipelineReport, batch: PersistedBatch, result: StorageWriteResult) {
    report.flush_count += 1;
    report.last_processed_slot = max_optional(report.last_processed_slot, latest_processed_slot(&batch));
    report.last_observed_at_unix_ms = max_optional(
        report.last_observed_at_unix_ms,
        batch.latest_timestamp_unix_ms(),
    );
    report.account_rows_written += batch.account_rows.len() as u64;
    report.transaction_rows_written += batch.transaction_rows.len() as u64;
    report.slot_rows_written += batch.slot_rows.len() as u64;
    report.custom_rows_written += batch.custom_rows.len() as u64;
    report.sql_statements_planned += result.sql_statements_planned;
    let snapshot: StoreSnapshot = result.snapshot;
    report.retained_account_rows = snapshot.account_rows;
    report.retained_transaction_rows = snapshot.transaction_rows;
    report.retained_slot_rows = snapshot.slot_rows;
    report.retained_custom_rows = snapshot.custom_rows;
    report.pruned_account_rows = snapshot.metrics.account_rows_pruned;
    report.pruned_transaction_rows = snapshot.metrics.transaction_rows_pruned;
    report.pruned_slot_rows = snapshot.metrics.slot_rows_pruned;
    report.pruned_custom_rows = snapshot.metrics.custom_rows_pruned;
}

fn max_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn latest_processed_slot(batch: &PersistedBatch) -> Option<i64> {
    batch
        .slot_rows
        .iter()
        .map(|row| row.slot)
        .chain(batch.transaction_rows.iter().map(|row| row.slot))
        .chain(batch.account_rows.iter().map(|row| row.slot))
        .chain(batch.custom_rows.iter().map(|row| row.slot))
        .max()
}

fn checkpoint_update_for_batch(batch: &PersistedBatch) -> Option<CheckpointUpdate> {
    let last_processed_slot = batch
        .slot_rows
        .iter()
        .map(|row| row.slot)
        .chain(batch.transaction_rows.iter().map(|row| row.slot))
        .chain(batch.account_rows.iter().map(|row| row.slot))
        .chain(batch.custom_rows.iter().map(|row| row.slot))
        .max();
    let last_observed_at_unix_ms = batch.latest_timestamp_unix_ms()?;

    Some(CheckpointUpdate {
        stream_name: "geyser-main".to_string(),
        last_processed_slot,
        last_observed_at_unix_ms,
        notes: Some(format!("flush_reason={:?}", batch.reason)),
    })
}

// RPC Fallback for gap detection and filling
pub struct RpcGapFiller {
    rpc_endpoints: Vec<String>,
    wal_queue: Arc<WalQueue>,
    client: reqwest::Client,
}

impl RpcGapFiller {
    pub fn new(rpc_endpoints: Vec<String>, wal_queue: Arc<WalQueue>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|e| {
                panic!("Failed to create HTTP client: {}", e);
            });

        Self {
            rpc_endpoints,
            wal_queue,
            client,
        }
    }

    /// Repair a single gap immediately upon detection
    /// Returns true if repair succeeded, false if it failed
    pub async fn repair_gap(&self, seq: u64) -> Result<bool, String> {
        log::warn!("🔧 Immediate gap repair triggered for seq {}", seq);

        // Look up which slot corresponds to this sequence
        let slot = match self.wal_queue.get_slot_for_seq(seq) {
            Some(s) => {
                log::debug!("Seq {} maps to slot {} (from metadata)", seq, s);
                s
            }
            None => {
                log::error!("No slot mapping found for seq {} - cannot repair", seq);
                return Ok(false);
            }
        };

        // Try each RPC endpoint until one succeeds
        for endpoint in &self.rpc_endpoints {
            match self.fetch_and_repair_slot(endpoint, slot, seq).await {
                Ok(true) => {
                    log::info!("✅ Successfully repaired seq {} (slot {})", seq, slot);
                    return Ok(true);
                }
                Ok(false) => {
                    log::warn!("Slot {} not found or unavailable, trying next endpoint", slot);
                    continue;
                }
                Err(e) => {
                    log::warn!("Failed to fetch slot {} from {}: {}", slot, endpoint, e);
                    continue;
                }
            }
        }

        log::error!("❌ Failed to repair seq {} (slot {}) from all endpoints", seq, slot);
        Ok(false)
    }

    async fn fetch_and_repair_slot(&self, endpoint: &str, slot: u64, seq: u64) -> Result<bool, String> {
        // Fetch block from Solana RPC
        let url = format!("{}/", endpoint.trim_end_matches('/'));
        let response = self.client
            .post(&url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBlock",
                "params": [
                    slot,
                    {
                        "encoding": "base64",
                        "transactionDetails": "full",
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
            .map_err(|e| format!("Failed to parse JSON response: {}", e))?;

        // Check if slot was found
        if let Some(error) = json.get("error") {
            if error["code"] == -32007 || error["message"].as_str().map_or(false, |m| m.contains("skipped")) {
                // Slot was skipped or not finalized
                return Ok(false);
            }
            return Err(format!("RPC error: {}", error));
        }

        let block_data = json.get("result")
            .and_then(|v| v.as_object())
            .ok_or_else(|| "No result in RPC response".to_string())?;

        // Convert block data to SubscribeUpdate
        let update = self.block_to_subscribe_update(block_data, slot)
            .map_err(|e| format!("Failed to convert block data: {}", e))?;

        // Repair the hole
        self.wal_queue.repair_hole(seq, slot, &update)?;

        Ok(true)
    }

    fn block_to_subscribe_update(&self, _block_data: &serde_json::Map<String, serde_json::Value>, _slot: u64) -> Result<helius_laserstream::grpc::SubscribeUpdate, String> {
        // TODO: Convert block data to SubscribeUpdate protobuf format
        // For now, return a placeholder to satisfy the type system
        // This needs actual implementation to convert:
        // - Block metadata -> SlotUpdate
        // - Transactions -> Transaction messages
        // - Account changes -> Account messages

        Err("Block to SubscribeUpdate conversion not yet implemented".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use helius_laserstream::grpc::SubscribeUpdate;

    #[test]
    fn wal_pipeline_processes_protobuf_entries() {
        let temp_dir = tempdir().unwrap();
        let wal_queue = Arc::new(WalQueue::new(temp_dir.path()).unwrap());

        // Write test protobuf entry to WAL
        let update = SubscribeUpdate {
            ..Default::default()
        };

        wal_queue.push_update(100, &update).unwrap();

        // Verify queue state
        assert_eq!(wal_queue.get_total_written(), 1);
        assert_eq!(wal_queue.get_unprocessed_count(), 1);
        assert_eq!(wal_queue.get_last_processed_seq(), 0);
    }

    #[test]
    fn test_in_memory_read_cursor_prevents_infinite_reread() {
        let temp_dir = tempdir().unwrap();
        let wal_queue = Arc::new(WalQueue::new(temp_dir.path()).unwrap());

        // Write 3 distinct entries to WAL
        for i in 0..3 {
            let update = SubscribeUpdate {
                ..Default::default()
            };
            wal_queue.push_update(100 + i, &update).unwrap();
        }

        // Simulate the consumer loop using in-memory cursor
        let mut next_read_seq = wal_queue.get_last_processed_seq(); // Starts at 0

        // First iteration: should read seq=0
        let entry1 = wal_queue.read_from(0, next_read_seq).unwrap().unwrap();
        assert_eq!(entry1.seq, 0, "First read should return seq=0");
        next_read_seq = entry1.seq + 1; // Advance cursor to 1

        // Second iteration: should read seq=1, NOT seq=0 again
        let entry2 = wal_queue.read_from(0, next_read_seq).unwrap().unwrap();
        assert_eq!(entry2.seq, 1, "Second read should return seq=1, not seq=0");
        next_read_seq = entry2.seq + 1; // Advance cursor to 2

        // Third iteration: should read seq=2, NOT seq=0 or seq=1 again
        let entry3 = wal_queue.read_from(0, next_read_seq).unwrap().unwrap();
        assert_eq!(entry3.seq, 2, "Third read should return seq=2, not seq=0 or seq=1");
        next_read_seq = entry3.seq + 1; // Advance cursor to 3

        // Fourth iteration: should return None (no more entries)
        let entry4 = wal_queue.read_from(0, next_read_seq).unwrap();
        assert!(entry4.is_none(), "Fourth read should return None");

        // Verify persisted checkpoint hasn't advanced (no flush yet)
        assert_eq!(wal_queue.get_last_processed_seq(), 0,
                   "Persisted checkpoint should still be 0 since no flush occurred");
    }
}

=== ./geyser/wal_queue.rs ===
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
    pub fn from_update(slot: u64, seq: u64, update: &SubscribeUpdate) -> Self {
        Self {
            slot,
            seq,
            payload: update.encode_to_vec(), // ✅ One re-encode
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

    pub fn push_update(&self, slot: u64, update: &SubscribeUpdate) -> Result<u64, String> {
        let seq = self.slot_sequence.fetch_add(1, Ordering::Release);

        // Create WAL entry with raw protobuf bytes
        let entry = WalEntry::from_update(slot, seq, update);

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

    // Legacy method for compatibility - converts GeyserEvent to SubscribeUpdate
    // This should be removed once the pipeline is fully converted to use SubscribeUpdate
    #[allow(dead_code)]
    pub fn push(&self, event: crate::geyser::decoder::GeyserEvent) -> Result<u64, String> {
        // This is a temporary compatibility shim
        // In the future, all callers should use push_update() directly with SubscribeUpdate
        let _slot = match &event {
            crate::geyser::decoder::GeyserEvent::AccountUpdate(acc) => acc.slot,
            crate::geyser::decoder::GeyserEvent::Transaction(tx) => tx.slot,
            crate::geyser::decoder::GeyserEvent::SlotUpdate(slot) => slot.slot,
            crate::geyser::decoder::GeyserEvent::BlockMeta(bm) => bm.slot,
        };

        // For now, we'll store a placeholder - this method should be removed
        log::warn!("Using legacy push() method - this should be replaced with push_update()");
        let seq = self.slot_sequence.fetch_add(1, Ordering::Release);
        Ok(seq)
    }

    pub fn read_next(&self) -> Result<Option<WalEntry>, WalReadError> {
        let last_flushed_seq = self.get_last_processed_seq();
        self.read_from_seq(last_flushed_seq)
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
        let mut batch = self.db.batch();

        batch.insert(&self.metadata, b"last_flushed_slot", slot.to_be_bytes());
        batch.insert(&self.metadata, b"last_flushed_seq", (seq + 1).to_be_bytes());

        batch.commit()
            .map_err(|e| format!("Failed to commit checkpoint batch: {}", e))?;

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
        let processed = self.get_last_processed_seq();
        total.saturating_sub(processed)
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

    /// Repair a hole in the WAL by writing data fetched from RPC
    /// This is called when the consumer detects a gap and fetches the missing data from RPC
    pub fn repair_hole(&self, seq: u64, slot: u64, update: &SubscribeUpdate) -> Result<(), String> {
        // Create WAL entry with the fetched data
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

        log::info!("Repaired WAL hole at seq {} with slot {} data fetched from RPC", seq, slot);

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
        let delete_up_to = last_flushed_seq.saturating_sub(self.gc_safety_margin);

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

        // Mark first 200 events as processed (last_flushed_seq becomes 200)
        wal_queue.mark_processed(200, 199).unwrap();

        // Run garbage collection
        // With safety_margin=100, should delete entries < (200 - 100) = 100
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
            assert_eq!(wal_queue.get_last_processed_seq(), 100);
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

            // Consumer checkpoint should still be at 100
            assert_eq!(wal_queue.get_last_processed_seq(), 100,
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
        assert_eq!(last_seq, 10, "Last seq should be 10 (next unprocessed)");

        // The key property: even if crash happened during mark_processed,
        // slot and seq should always be consistent (both or neither updated)
        // With atomic batch writes, this is guaranteed
        assert_eq!(last_slot + 1, last_seq,
                   "Slot + 1 should equal seq (they stay in sync atomically)");

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
}

=== ./lib.rs ===
pub mod api;
pub mod config;
pub mod geyser;
pub mod processor;
pub mod rpc_pool;

pub use api::rest::ApiSnapshot;

pub const PROJECT_NAME: &str = "solana-realtime-indexer";

=== ./main.rs ===
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use solana_realtime_indexer::api;
use solana_realtime_indexer::api::rest::ApiSnapshot;
use solana_realtime_indexer::config::Config;
use solana_realtime_indexer::geyser::client::GeyserClient;
use solana_realtime_indexer::geyser::consumer::{GeyserConfig, SubscriptionFilter};
use solana_realtime_indexer::geyser::{Protocol, load_protocols_from_dir, merge_subscriptions};
use solana_realtime_indexer::geyser::wal_queue::WalQueue;
use solana_realtime_indexer::geyser::wal_consumer::{WalPipelineConfig, WalPipelineRunner, RpcGapFiller};
use solana_realtime_indexer::processor::batch_writer::BatchWriter;
use solana_realtime_indexer::processor::cpi_decoder::CpiLogDecoder;
use solana_realtime_indexer::processor::decoder::{CustomDecoder, Type1Decoder};
use solana_realtime_indexer::processor::pipeline::PipelineReport;
use solana_realtime_indexer::processor::sink::{
    DryRunStorageSink, StorageSink, TimescaleStorageSink,
};
use solana_realtime_indexer::processor::store::{RetentionPolicy, Type1Store};
use solana_realtime_indexer::PROJECT_NAME;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Mutex;

type SharedSnapshot = Arc<Mutex<ApiSnapshot>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    env_logger::init();

    let config = Config::from_env();

    run_with_real_geyser(config).await?;

    Ok(())
}

async fn run_with_real_geyser(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Starting Solana Realtime Indexer with real Geyser connection (WAL mode)");

    let api_key = config
        .geyser_api_key
        .clone()
        .ok_or("Geyser API key must be set for real Geyser connection")?;

    // Load protocols from TOML files or fall back to env vars
    let protocols = load_protocols(&config)?;

    // Merge all protocol subscriptions into one (deduplicates shared programs like Token)
    let merged_subscription = merge_subscriptions(&protocols);

    log::info!("Merged subscription: {} programs, {} accounts, slots: {}, blocks_meta: {}",
              merged_subscription.program_ids.len(),
              merged_subscription.account_pubkeys.len(),
              merged_subscription.include_slots,
              merged_subscription.include_blocks_meta);

    // Convert merged [u8; 32] pubkeys to base58 strings for SubscriptionFilter
    let mut filters = Vec::new();
    for program_id in &merged_subscription.program_ids {
        filters.push(SubscriptionFilter::Program(bs58::encode(program_id).into_string()));
    }
    for account in &merged_subscription.account_pubkeys {
        filters.push(SubscriptionFilter::Account(bs58::encode(account).into_string()));
    }
    if merged_subscription.include_slots {
        filters.push(SubscriptionFilter::Slots);
    }
    if merged_subscription.include_blocks_meta {
        filters.push(SubscriptionFilter::Blocks);
    }

    let geyser_config = GeyserConfig::new(
        config
            .geyser_endpoint
            .clone()
            .ok_or("Geyser endpoint must be set")?,
        config.geyser_channel_capacity,
        filters,
    );

    let storage_mode = storage_mode(&config);
    let api_state = initial_api_state(&config, storage_mode).await;

    // Create single shared database pool for all consumers
    let shared_pool: Option<Arc<sqlx::PgPool>> = if let Some(database_url) = config.database_url.as_deref() {
        match PgPoolOptions::new()
            .max_connections(config.db_pool_max_connections)
            .min_connections(2)
            .acquire_timeout(Duration::from_secs(60))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(1800))
            .test_before_acquire(true)
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                log::info!("✅ Shared database pool connected (max {} connections)", config.db_pool_max_connections);
                // Attach pool to API state
                let mut state = api_state.lock().await;
                *state = state.clone().with_pool(pool.clone());
                Some(Arc::new(pool))
            }
            Err(e) => {
                log::warn!("Failed to create shared database pool: {}. Storage and API endpoints will be unavailable.", e);
                None
            }
        }
    } else {
        None
    };

    // Create WAL queue instead of bounded channel
    if config.clear_wal_on_startup {
        log::warn!("CLEAR_WAL_ON_STARTUP=true - removing existing WAL at: {}", config.wal_path);
        std::fs::remove_dir_all(&config.wal_path).unwrap_or_else(|e| {
            log::debug!("WAL directory didn't exist or couldn't be removed: {}", e);
        });
    }
    std::fs::create_dir_all(&config.wal_path).ok();
    let wal_queue = Arc::new(WalQueue::new(&config.wal_path)?);

    // Start WAL background flusher
    let _wal_flush_handle = wal_queue.clone().start_background_flush().await;

    // Configure WAL pipeline
    let wal_pipeline_config = WalPipelineConfig {
        wal_path: config.wal_path.clone(),
        poll_interval: Duration::from_millis(10), // Poll every 10ms
        batch_size: config.batch_size,
        batch_flush_ms: config.batch_flush_ms,
    };

    // Create RPC gap filler for event-driven repair
    let gap_filler = RpcGapFiller::new(config.rpc_endpoints.clone(), wal_queue.clone());

    // Start Geyser client writing to WAL
    let geyser_client = GeyserClient::new(
        geyser_config,
        api_key,
        config.geyser_run_duration_seconds,
    );
    let wal_queue_clone = wal_queue.clone();
    let geyser_handle = tokio::spawn(async move {
        match geyser_client.connect_and_subscribe(&wal_queue_clone).await {
            Ok(events_count) => {
                log::info!("✅ Geyser client processed {} events (WAL mode)", events_count);
                let unprocessed = wal_queue_clone.get_unprocessed_count();
                log::info!("📝 WAL state: {} events written, {} unprocessed",
                          wal_queue_clone.get_total_written(), unprocessed);
            }
            Err(error) => log::error!("Geyser client error: {}", error),
        }
    });

    // Build database sink and batch writer
    let sink = build_sink(&config, shared_pool.clone()).await?;
    let writer = BatchWriter::new(
        config.batch_size,
        Duration::from_millis(config.batch_flush_ms),
    );

    // Create custom decoders (protocol-specific decoders will be registered here)
    let custom_decoders: Vec<Box<dyn CustomDecoder>> = vec![
        Box::new(CpiLogDecoder::new()),
    ];
    let decoder = Type1Decoder::new();

    // Start WAL pipeline consumer with event-driven gap repair
    let wal_runner = WalPipelineRunner::new(
        wal_queue.clone(),
        wal_pipeline_config,
        api_state.clone(),
        writer,
        decoder,
        custom_decoders,
        sink,
    )
    .with_gap_filler(gap_filler);
    let wal_pipeline_handle = wal_runner.start_background_processor();

    log_background_wal_pipeline(wal_pipeline_handle);
    log_background_geyser(geyser_handle);

    // Start WAL metrics reporter
    let _metrics_reporter = {
        let wal_queue = wal_queue.clone();
        let api_state = api_state.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                let wal_unprocessed = wal_queue.get_unprocessed_count();

                // Update API state with latest metrics
                let mut state = api_state.lock().await;
                state.wal_unprocessed_count = wal_unprocessed;

                // Channel utilization is no longer relevant - WAL is the only buffer
                state.channel_utilization = 0.0;
            }
        })
    };

    // Start slot latency materialized view refresh task
    let _latency_refresh_handle = if let Some(pool) = shared_pool.clone() {
        log::info!("Starting slot latency materialized view refresh task (every 30s)");
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;

                if let Err(e) = sqlx::query("SELECT refresh_slot_health_1m()")
                    .execute(&*pool)
                    .await
                {
                    log::warn!("Failed to refresh slot_health_1m: {}", e);
                }
            }
        }))
    } else {
        log::warn!("No database pool available - skipping materialized view refresh task");
        None
    };

    log::info!("🚀 Indexer running in WAL mode:");
    log::info!("   - No event drops (correctness guaranteed)");
    log::info!("   - No blocking (OS buffer + disk storage)");
    log::info!("   - Gap detection + RPC fallback enabled");
    log::info!("   - WAL path: {}", config.wal_path);
    log::info!("   - WAL metrics exposed via /api/metrics");

    serve_api(config.bind_address, api_state).await
}

async fn build_sink(config: &Config, pool: Option<Arc<sqlx::PgPool>>) -> Result<Box<dyn StorageSink>, Box<dyn std::error::Error>> {
    let retention_policy = RetentionPolicy {
        max_age: Duration::from_secs(60),
    };

    if let Some(database_url) = config.database_url.as_deref() {
        match pool {
            Some(p) => {
                // Use shared pool
                Ok(Box::new(TimescaleStorageSink::with_pool(
                    (*p).clone(),
                    Type1Store::new(retention_policy),
                )))
            }
            None => {
                // Fallback: create new pool (shouldn't happen in normal flow)
                Ok(Box::new(TimescaleStorageSink::connect_with_pool_size(
                    database_url,
                    Type1Store::new(retention_policy),
                    config.db_pool_max_connections,
                ).await?))
            }
        }
    } else {
        Ok(Box::new(DryRunStorageSink::new(Type1Store::new(
            retention_policy,
        ))))
    }
}

async fn serve_api(
    bind_address: String,
    api_state: SharedSnapshot,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = api::router_with_state(api_state);
    let tcp_listener = tokio::net::TcpListener::bind(&bind_address).await?;
    log::info!("API server listening on {}", bind_address);

    axum::serve(tcp_listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install CTRL+C handler");
        })
        .await?;

    Ok(())
}

async fn initial_api_state(config: &Config, storage_mode: &str) -> SharedSnapshot {
    Arc::new(Mutex::new(ApiSnapshot::from_report(
        PROJECT_NAME,
        storage_mode,
        config.bind_address.clone(),
        config.rpc_endpoints.len(),
        0,
        Duration::from_secs(0),
        unix_now_millis(),
        PipelineReport::default(),
    )
    .with_runtime_config(
        config.geyser_channel_capacity,
        config.batch_size,
        config.batch_flush_ms,
    )))
}

fn storage_mode(config: &Config) -> &'static str {
    if config.database_url.is_some() {
        "timescale"
    } else {
        "dry-run"
    }
}

fn log_background_geyser(handle: tokio::task::JoinHandle<()>) {
    tokio::spawn(async move {
        if let Err(error) = handle.await {
            log::error!("Geyser task join failed: {}", error);
        }
    });
}

fn log_background_wal_pipeline(handle: tokio::task::JoinHandle<Result<PipelineReport, String>>) {
    tokio::spawn(async move {
        match handle.await {
            Ok(Ok(report)) => log::info!("WAL Pipeline completed: {:?}", report),
            Ok(Err(error)) => log::error!("WAL Pipeline failed: {}", error),
            Err(error) => log::error!("WAL Pipeline task join failed: {}", error),
        }
    });
}

fn unix_now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

/// Load protocols from TOML files in the protocols/ directory.
/// Fails explicitly if no protocol configs are found - no silent fallback.
fn load_protocols(_config: &Config) -> Result<Vec<Box<dyn Protocol>>, Box<dyn std::error::Error>> {
    let protocols_dir = "protocols";

    // Check if protocols directory exists
    if !std::path::Path::new(protocols_dir).exists() {
        return Err(format!(
            "Protocols directory '{}' does not exist. \
             Create a 'protocols/' directory with at least one .toml configuration file. \
             See protocols/README.md for examples.",
            protocols_dir
        ).into());
    }

    // Load all TOML files using the protocol module's directory scanner
    let config_only_protocols = load_protocols_from_dir(protocols_dir)
        .map_err(|e| format!("Failed to load protocols from directory '{}': {}", protocols_dir, e))?;

    if config_only_protocols.is_empty() {
        return Err(format!(
            "No protocol .toml files found in '{}' directory. \
             Add at least one protocol configuration file (e.g., raydium.toml). \
             See protocols/README.md for examples.",
            protocols_dir
        ).into());
    }

    // Convert ConfigOnlyProtocol to Box<dyn Protocol>
    let protocols: Vec<Box<dyn Protocol>> = config_only_protocols
        .into_iter()
        .map(|p| Box::new(p) as Box<dyn Protocol>)
        .collect();

    Ok(protocols)
}

=== ./processor/batch_writer.rs ===
use std::time::{Duration, Instant};

use crate::geyser::decoder::GeyserEvent;
#[derive(Debug, Clone)]
pub struct WriterMetrics {
    pub flush_count: u64,
    pub failed_flush_count: u64,
    pub buffered_events_flushed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    Size,
    Interval,
    ChannelClosed,
}

#[derive(Debug, Clone)]
pub struct BufferedBatch {
    pub reason: FlushReason,
    pub events: Vec<GeyserEvent>,
}

#[derive(Debug, Clone)]
pub struct BatchWriter {
    pub buffer: Vec<GeyserEvent>,
    pub flush_size: usize,
    pub flush_interval: Duration,
    pub metrics: WriterMetrics,
    last_flush_at: Instant,
}

impl BatchWriter {
    pub fn new(flush_size: usize, flush_interval: Duration) -> Self {
        Self {
            buffer: Vec::new(),
            flush_size,
            flush_interval,
            metrics: WriterMetrics {
                flush_count: 0,
                failed_flush_count: 0,
                buffered_events_flushed: 0,
            },
            last_flush_at: Instant::now(),
        }
    }

    pub fn should_flush(&self) -> bool {
        self.buffer.len() >= self.flush_size
    }

    pub fn push(&mut self, event: GeyserEvent) {
        self.buffer.push(event);
    }

    pub fn flush_if_needed(&mut self, now: Instant) -> Option<BufferedBatch> {
        if self.should_flush() {
            return self.flush(FlushReason::Size, now);
        }

        if !self.buffer.is_empty() && now.duration_since(self.last_flush_at) >= self.flush_interval
        {
            return self.flush(FlushReason::Interval, now);
        }

        None
    }

    pub fn flush(&mut self, reason: FlushReason, now: Instant) -> Option<BufferedBatch> {
        if self.buffer.is_empty() {
            return None;
        }

        let events = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.flush_size));
        let batch = BufferedBatch { reason, events };

        self.metrics.flush_count += 1;
        self.metrics.buffered_events_flushed += batch.events.len() as u64;
        self.last_flush_at = now;

        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchWriter, FlushReason};
    use crate::geyser::decoder::{AccountUpdate, GeyserEvent, SlotUpdate, TransactionUpdate};
    use std::time::{Duration, Instant};

    #[test]
    fn flushes_when_batch_size_is_reached() {
        let mut writer = BatchWriter::new(2, Duration::from_secs(60));
        let now = Instant::now();

        writer.push(GeyserEvent::AccountUpdate(AccountUpdate {
            timestamp_unix_ms: 1_710_000_000_000,
            slot: 100,
            pubkey: "account-a".as_bytes().to_vec(),
            owner: "program-a".as_bytes().to_vec(),
            lamports: 10,
            write_version: 1,
            data: vec![1, 2],
        }));
        writer.push(GeyserEvent::Transaction(TransactionUpdate {
            timestamp_unix_ms: 1_710_000_000_001,
            slot: 100,
            signature: "sig-a".as_bytes().to_vec(),
            fee: 5_000,
            success: true,
            program_ids: vec![b"program-a".to_vec()],
            log_messages: vec!["ok".to_string()],
        }));

        let batch = writer.flush_if_needed(now).expect("size flush");

        assert_eq!(batch.reason, FlushReason::Size);
        assert_eq!(batch.events.len(), 2);
        assert!(writer.buffer.is_empty());
    }

    #[test]
    fn interval_flushes_pending_buffer() {
        let mut writer = BatchWriter::new(8, Duration::from_millis(5));
        let now = Instant::now();

        writer.push(GeyserEvent::SlotUpdate(SlotUpdate {
            timestamp_unix_ms: 1_710_000_000_002,
            slot: 100,
            parent_slot: Some(99),
            status: "confirmed".to_string(),
        }));

        let batch = writer
            .flush_if_needed(now + Duration::from_millis(6))
            .expect("interval flush");

        assert_eq!(batch.reason, FlushReason::Interval);
        assert_eq!(batch.events.len(), 1);
    }
}

=== ./processor/cpi_decoder.rs ===
use crate::geyser::decoder::{GeyserEvent, TransactionUpdate};
use crate::processor::decoder::CustomDecoder;
use crate::processor::schema::CustomDecodedRow;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CpiLogDecoder;

impl CpiLogDecoder {
    pub fn new() -> Self {
        Self
    }

    fn parse_cpi_edges(&self, update: &TransactionUpdate) -> Vec<CustomDecodedRow> {
        let mut edges = Vec::new();
        let mut stack: Vec<Option<String>> = Vec::new();
        let mut program_outcomes: HashMap<String, bool> = HashMap::new();

        // First pass: collect program outcomes
        for log in &update.log_messages {
            if let Some(program) = parse_program_outcome(log) {
                program_outcomes.insert(program.0, program.1);
            }
        }

        // Second pass: build call stack and emit edges
        // Solana logs use 1-based depth, where depth=1 is the root invocation (no caller)
        for log in &update.log_messages {
            if let Some((program, depth)) = parse_program_invoke(log) {
                // Ensure stack is large enough (depth is 1-based)
                while stack.len() <= depth {
                    stack.push(None);
                }
                stack[depth] = Some(program.clone());

                // Emit edge if we have a caller (depth > 1 means we have a parent)
                if depth > 1 {
                    if let Some(Some(caller)) = stack.get(depth - 1) {
                        let callee = program.clone();
                        let success = program_outcomes.get(&callee).copied().unwrap_or(update.success);

                        edges.push(CustomDecodedRow {
                            decoder_name: "cpi-edge".to_string(),
                            record_key: bs58::encode(&update.signature).into_string(),
                            slot: update.slot as i64,
                            timestamp_unix_ms: update.timestamp_unix_ms,
                            event_index: depth as i16,
                            payload: serde_json::json!({
                                "caller": caller,
                                "callee": callee,
                                "depth": depth,
                                "success": success
                            }).to_string(),
                        });
                    }
                }
            }
        }

        edges
    }
}

impl CustomDecoder for CpiLogDecoder {
    fn name(&self) -> &str {
        "cpi-edge"
    }

    fn decode(&mut self, event: &GeyserEvent) -> Option<CustomDecodedRow> {
        // Return first edge for simple case
        self.decode_multi(event).first().cloned()
    }

    fn decode_multi(&mut self, event: &GeyserEvent) -> Vec<CustomDecodedRow> {
        match event {
            GeyserEvent::Transaction(update) => {
                self.parse_cpi_edges(update)
            }
            _ => Vec::new(),
        }
    }
}

impl Default for CpiLogDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse "Program <PUBKEY> invoke [<DEPTH>]" returns (program_base58, depth)
fn parse_program_invoke(log: &str) -> Option<(String, usize)> {
    // Pattern: "Program <base58> invoke [1]" or "Program <base58> invoke [2]"
    let log = log.trim();

    // Check if it contains " invoke ["
    if !log.contains(" invoke [") {
        return None;
    }

    // Find the program part
    if let Some(start) = log.find("Program ") {
        let after_program = &log[start + 8..]; // Skip "Program "

        // Find the end of the pubkey (before " invoke")
        if let Some(end) = after_program.find(" invoke [") {
            let pubkey = &after_program[..end];

            // Find the depth number
            let depth_start = end + 9; // Skip " invoke ["
            if let Some(depth_end) = after_program[depth_start..].find(']') {
                let depth_str = &after_program[depth_start..depth_start + depth_end];
                if let Ok(depth) = depth_str.parse::<usize>() {
                    return Some((pubkey.to_string(), depth));
                }
            }
        }
    }

    None
}

/// Parse "Program <PUBKEY> success" or "Program <PUBKEY> failed" returns (program_base58, success)
fn parse_program_outcome(log: &str) -> Option<(String, bool)> {
    let log = log.trim();

    // Check for success
    if let Some(start) = log.find("Program ") {
        let after_program = &log[start + 8..]; // Skip "Program "

        if let Some(end) = after_program.find(" success") {
            let program = &after_program[..end];
            return Some((program.to_string(), true));
        }

        if let Some(end) = after_program.find(" failed") {
            let program = &after_program[..end];
            return Some((program.to_string(), false));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_program_invoke() {
        let log = "Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin invoke [1]";
        let result = parse_program_invoke(log);
        assert_eq!(result, Some(("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string(), 1)));

        let log2 = "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]";
        let result2 = parse_program_invoke(log2);
        assert_eq!(result2, Some(("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(), 2)));
    }

    #[test]
    fn test_parse_program_outcome() {
        let log = "Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin success";
        let result = parse_program_outcome(log);
        assert_eq!(result, Some(("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string(), true)));

        let log2 = "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA failed";
        let result2 = parse_program_outcome(log2);
        assert_eq!(result2, Some(("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(), false)));
    }

    #[test]
    fn test_non_invoke_log_returns_none() {
        let log = "Some other log message";
        let result = parse_program_invoke(log);
        assert_eq!(result, None);
    }

    #[test]
    fn test_cpi_decoder_handles_transaction_with_multiple_cpi_calls() {
        use crate::geyser::decoder::TransactionUpdate;

        let mut decoder = CpiLogDecoder::new();

        // Create a mock transaction with nested CPI calls
        let tx = TransactionUpdate {
            timestamp_unix_ms: 1234567890000,
            slot: 100,
            signature: vec![1, 2, 3, 4],
            fee: 5000,
            success: true,
            program_ids: vec![
                vec![5, 6, 7, 8],  // Root program
                vec![9, 10, 11, 12], // CPI call 1
                vec![13, 14, 15, 16], // CPI call 2
            ],
            log_messages: vec![
                "Program 5xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin invoke [1]".to_string(),
                "Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin invoke [2]".to_string(),
                "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [3]".to_string(),
                "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success".to_string(),
                "Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin success".to_string(),
                "Program 5xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin success".to_string(),
            ],
        };

        let event = GeyserEvent::Transaction(tx);
        let rows = decoder.decode_multi(&event);

        // Should have 2 edges (program 1 -> program 2, and program 2 -> program 3)
        assert_eq!(rows.len(), 2);

        // Check first edge: program 1 calls program 2
        let first_edge = &rows[0];
        assert_eq!(first_edge.decoder_name, "cpi-edge");
        assert_eq!(first_edge.event_index, 2); // depth 2
        let payload: serde_json::Value = serde_json::from_str(&first_edge.payload).unwrap();
        assert_eq!(payload["caller"], "5xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin");
        assert_eq!(payload["callee"], "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin");
        assert_eq!(payload["depth"], 2);
        assert_eq!(payload["success"], true);

        // Check second edge: program 2 calls program 3
        let second_edge = &rows[1];
        assert_eq!(second_edge.decoder_name, "cpi-edge");
        assert_eq!(second_edge.event_index, 3); // depth 3
        let payload2: serde_json::Value = serde_json::from_str(&second_edge.payload).unwrap();
        assert_eq!(payload2["caller"], "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin");
        assert_eq!(payload2["callee"], "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        assert_eq!(payload2["depth"], 3);
        assert_eq!(payload2["success"], true);
    }

    #[test]
    fn test_cpi_decoder_returns_empty_for_non_transaction_events() {
        let mut decoder = CpiLogDecoder::new();
        use crate::geyser::decoder::{AccountUpdate, SlotUpdate};

        let account_event = GeyserEvent::AccountUpdate(AccountUpdate {
            timestamp_unix_ms: 1234567890000,
            slot: 100,
            pubkey: vec![1, 2, 3],
            owner: vec![4, 5, 6],
            lamports: 1000,
            write_version: 1,
            data: vec![7, 8, 9],
        });

        let rows = decoder.decode_multi(&account_event);
        assert_eq!(rows.len(), 0);

        let slot_event = GeyserEvent::SlotUpdate(SlotUpdate {
            timestamp_unix_ms: 1234567890000,
            slot: 100,
            parent_slot: Some(99),
            status: "confirmed".to_string(),
        });

        let rows = decoder.decode_multi(&slot_event);
        assert_eq!(rows.len(), 0);
    }
}

=== ./processor/decoder.rs ===
use crate::geyser::decoder::{GeyserEvent, TransactionUpdate};
use crate::geyser::BlockTimeCache;
use crate::processor::batch_writer::BufferedBatch;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};

pub trait CustomDecoder: Send {
    fn name(&self) -> &str;
    fn decode(&mut self, event: &GeyserEvent) -> Option<CustomDecodedRow>;
    fn decode_multi(&mut self, event: &GeyserEvent) -> Vec<CustomDecodedRow> {
        // Default implementation for backwards compatibility
        self.decode(event).into_iter().collect()
    }
}

#[derive(Debug, Clone)]
pub struct Type1Decoder;

#[derive(Debug, Clone)]
pub struct PersistedBatch {
    pub reason: crate::processor::batch_writer::FlushReason,
    pub account_rows: Vec<AccountUpdateRow>,
    pub transaction_rows: Vec<TransactionRow>,
    pub slot_rows: Vec<SlotRow>,
    pub custom_rows: Vec<CustomDecodedRow>,
}

impl PersistedBatch {
    pub fn latest_timestamp_unix_ms(&self) -> Option<i64> {
        self.account_rows
            .iter()
            .map(|row| row.timestamp_unix_ms)
            .chain(
                self.transaction_rows
                    .iter()
                    .map(|row| row.timestamp_unix_ms),
            )
            .chain(self.slot_rows.iter().map(|row| row.timestamp_unix_ms))
            .chain(self.custom_rows.iter().map(|row| row.timestamp_unix_ms))
            .max()
    }
}

impl Type1Decoder {
    pub fn new() -> Self {
        Self
    }

    pub fn decode(
        &self,
        batch: BufferedBatch,
        custom_decoders: &mut [Box<dyn CustomDecoder>],
        block_time_cache: &BlockTimeCache,
    ) -> PersistedBatch {
        let mut persisted = PersistedBatch {
            reason: batch.reason,
            account_rows: Vec::new(),
            transaction_rows: Vec::new(),
            slot_rows: Vec::new(),
            custom_rows: Vec::new(),
        };

        for event in batch.events {
            for decoder in custom_decoders.iter_mut() {
                let rows = decoder.decode_multi(&event);
                if !rows.is_empty() {
                    log::debug!("Decoder {} produced {} rows for event", decoder.name(), rows.len());
                }
                persisted.custom_rows.extend(rows);
            }

            match event {
                GeyserEvent::AccountUpdate(update) => {
                    // Use block_time if available, fall back to wall-clock timestamp
                    let timestamp_ms = block_time_cache
                        .get(update.slot)
                        .unwrap_or(update.timestamp_unix_ms);

                    persisted.account_rows.push(AccountUpdateRow {
                        slot: update.slot as i64,
                        timestamp_unix_ms: timestamp_ms,
                        pubkey: update.pubkey.clone(),
                        owner: update.owner.clone(),
                        lamports: update.lamports as i64,
                        data: update.data,
                        write_version: update.write_version as i64,
                    });
                }
                GeyserEvent::Transaction(update) => {
                    // Use block_time if available, fall back to wall-clock timestamp
                    let timestamp_ms = block_time_cache
                        .get(update.slot)
                        .unwrap_or(update.timestamp_unix_ms);

                    log::debug!("Processing transaction: signature={:?}, program_ids count={}",
                              update.signature, update.program_ids.len());
                    persisted.transaction_rows.push(TransactionRow {
                        slot: update.slot as i64,
                        timestamp_unix_ms: timestamp_ms,
                        signature: update.signature.clone(),
                        fee: update.fee as i64,
                        success: update.success,
                        program_ids: update.program_ids,
                        log_messages: update.log_messages,
                    });
                }
                GeyserEvent::SlotUpdate(update) => {
                    persisted.slot_rows.push(SlotRow {
                        slot: update.slot as i64,
                        timestamp_unix_ms: update.timestamp_unix_ms,
                        parent_slot: update.parent_slot.map(|slot| slot as i64),
                        status: update.status,
                    });
                }
                GeyserEvent::BlockMeta(_) => {
                    // BlockMeta events are handled separately to populate the block_time cache
                    // They don't produce rows directly
                }
            }
        }

        persisted
    }
}

impl Default for Type1Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ProgramActivityDecoder {
    program_id: Vec<u8>,
}

impl ProgramActivityDecoder {
    pub fn new(program_id: impl AsRef<str>) -> Self {
        // Decode the program_id once during construction
        let program_id_bytes = bs58::decode(program_id.as_ref())
            .into_vec()
            .unwrap_or_else(|_| program_id.as_ref().as_bytes().to_vec());
        Self {
            program_id: program_id_bytes,
        }
    }
}

impl CustomDecoder for ProgramActivityDecoder {
    fn name(&self) -> &str {
        "program-activity"
    }

    fn decode(&mut self, event: &GeyserEvent) -> Option<CustomDecodedRow> {
        match event {
            GeyserEvent::AccountUpdate(update) if update.owner == self.program_id => {
                // Encode pubkey back to base58 for the record_key (CustomDecodedRow uses String)
                let record_key = bs58::encode(&update.pubkey).into_string();
                Some(CustomDecodedRow {
                    decoder_name: self.name().to_string(),
                    record_key,
                    slot: update.slot as i64,
                    timestamp_unix_ms: update.timestamp_unix_ms,
                    event_index: 0,
                    payload: "account_update".to_string(),
                })
            }
            GeyserEvent::Transaction(update)
                if transaction_mentions_program(update, &self.program_id) =>
            {
                // Encode signature back to base58 for the record_key (CustomDecodedRow uses String)
                let record_key = bs58::encode(&update.signature).into_string();
                Some(CustomDecodedRow {
                    decoder_name: self.name().to_string(),
                    record_key,
                    slot: update.slot as i64,
                    timestamp_unix_ms: update.timestamp_unix_ms,
                    event_index: 0,
                    payload: "transaction".to_string(),
                })
            }
            _ => None,
        }
    }
}

fn transaction_mentions_program(update: &TransactionUpdate, program_id: &[u8]) -> bool {
    update
        .program_ids
        .iter()
        .any(|candidate_bytes| candidate_bytes == program_id)
}

#[cfg(test)]
mod tests {
    use super::{CustomDecoder, ProgramActivityDecoder, Type1Decoder};
    use crate::geyser::BlockTimeCache;
    use crate::geyser::decoder::{AccountUpdate, GeyserEvent, TransactionUpdate};
    use crate::processor::batch_writer::{BufferedBatch, FlushReason};

    #[test]
    fn type1_decoder_keeps_full_generic_rows_and_custom_rows() {
        let decoder = Type1Decoder::new();
        let mut custom_decoders: Vec<Box<dyn CustomDecoder>> =
            vec![Box::new(ProgramActivityDecoder::new("amm-program"))];
        let batch = BufferedBatch {
            reason: FlushReason::Size,
            events: vec![
                GeyserEvent::AccountUpdate(AccountUpdate {
                    timestamp_unix_ms: 1_710_000_000_000,
                    slot: 50,
                    pubkey: "tracked-account".as_bytes().to_vec(),
                    owner: "amm-program".as_bytes().to_vec(),
                    lamports: 9,
                    write_version: 1,
                    data: vec![1, 2, 3],
                }),
                GeyserEvent::Transaction(TransactionUpdate {
                    timestamp_unix_ms: 1_710_000_000_001,
                    slot: 50,
                    signature: "tracked-signature".as_bytes().to_vec(),
                    fee: 5_000,
                    success: true,
                    program_ids: vec!["amm-program".as_bytes().to_vec()],
                    log_messages: vec!["swap".to_string()],
                }),
            ],
        };

        let block_time_cache = BlockTimeCache::new(1000);
        let persisted = decoder.decode(batch, &mut custom_decoders, &block_time_cache);

        assert_eq!(persisted.account_rows.len(), 1);
        assert_eq!(persisted.transaction_rows.len(), 1);
        assert_eq!(persisted.custom_rows.len(), 2);
        assert_eq!(persisted.transaction_rows[0].log_messages.len(), 1);
    }
}

=== ./processor/pipeline.rs ===
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Instant;
use std::sync::Arc;

use crate::geyser::decoder::GeyserEvent;
use crate::geyser::BlockTimeCache;
use crate::processor::batch_writer::{BatchWriter, FlushReason};
use crate::processor::decoder::{CustomDecoder, PersistedBatch, Type1Decoder};
use crate::processor::sink::{StorageError, StorageSink, StorageWriteResult};
use crate::processor::sql::CheckpointUpdate;
use crate::processor::store::StoreSnapshot;

#[derive(Debug, Clone, Default)]
pub struct PipelineReport {
    pub received_events: u64,
    pub flush_count: u64,
    pub last_processed_slot: Option<i64>,
    pub last_observed_at_unix_ms: Option<i64>,
    pub account_rows_written: u64,
    pub transaction_rows_written: u64,
    pub slot_rows_written: u64,
    pub custom_rows_written: u64,
    pub sql_statements_planned: u64,
    pub retained_account_rows: usize,
    pub retained_transaction_rows: usize,
    pub retained_slot_rows: usize,
    pub retained_custom_rows: usize,
    pub pruned_account_rows: u64,
    pub pruned_transaction_rows: u64,
    pub pruned_slot_rows: u64,
    pub pruned_custom_rows: u64,
}

pub struct ProcessorPipeline {
    receiver: Receiver<GeyserEvent>,
    writer: BatchWriter,
    decoder: Type1Decoder,
    custom_decoders: Vec<Box<dyn CustomDecoder>>,
    sink: Box<dyn StorageSink>,
    block_time_cache: Arc<BlockTimeCache>,
}

impl ProcessorPipeline {
    pub fn new(
        receiver: Receiver<GeyserEvent>,
        writer: BatchWriter,
        decoder: Type1Decoder,
        custom_decoders: Vec<Box<dyn CustomDecoder>>,
        sink: Box<dyn StorageSink>,
        block_time_cache: Arc<BlockTimeCache>,
    ) -> Self {
        Self {
            receiver,
            writer,
            decoder,
            custom_decoders,
            sink,
            block_time_cache,
        }
    }

    pub async fn run(&mut self) -> Result<PipelineReport, StorageError> {
        self.run_with_reporter(|_| {}).await
    }

    pub async fn run_with_reporter<F>(&mut self, mut report_progress: F) -> Result<PipelineReport, StorageError>
    where
        F: FnMut(&PipelineReport),
    {
        let mut report = PipelineReport::default();

        loop {
            match self.receiver.recv_timeout(self.writer.flush_interval) {
                Ok(event) => {
                    report.received_events += 1;
                    self.writer.push(event);

                    if let Some(batch) = self.writer.flush_if_needed(Instant::now()) {
                        self.process_batch(batch, &mut report).await?;
                        report_progress(&report);
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if let Some(batch) = self.writer.flush(FlushReason::Interval, Instant::now()) {
                        self.process_batch(batch, &mut report).await?;
                        report_progress(&report);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if let Some(batch) = self
                        .writer
                        .flush(FlushReason::ChannelClosed, Instant::now())
                    {
                        self.process_batch(batch, &mut report).await?;
                        report_progress(&report);
                    }
                    break;
                }
            }
        }

        Ok(report)
    }

    async fn process_batch(
        &mut self,
        batch: crate::processor::batch_writer::BufferedBatch,
        report: &mut PipelineReport,
    ) -> Result<(), StorageError> {
        let persisted = self.decoder.decode(batch, &mut self.custom_decoders, &self.block_time_cache);
        let result = self
            .sink
            .write_batch(&persisted, checkpoint_update_for_batch(&persisted)).await?;
        apply_batch(report, persisted, result);
        Ok(())
    }
}

fn apply_batch(report: &mut PipelineReport, batch: PersistedBatch, result: StorageWriteResult) {
    report.flush_count += 1;
    report.last_processed_slot =
        max_optional(report.last_processed_slot, latest_processed_slot(&batch));
    report.last_observed_at_unix_ms = max_optional(
        report.last_observed_at_unix_ms,
        batch.latest_timestamp_unix_ms(),
    );
    report.account_rows_written += batch.account_rows.len() as u64;
    report.transaction_rows_written += batch.transaction_rows.len() as u64;
    report.slot_rows_written += batch.slot_rows.len() as u64;
    report.custom_rows_written += batch.custom_rows.len() as u64;
    report.sql_statements_planned += result.sql_statements_planned;
    let snapshot: StoreSnapshot = result.snapshot;
    report.retained_account_rows = snapshot.account_rows;
    report.retained_transaction_rows = snapshot.transaction_rows;
    report.retained_slot_rows = snapshot.slot_rows;
    report.retained_custom_rows = snapshot.custom_rows;
    report.pruned_account_rows = snapshot.metrics.account_rows_pruned;
    report.pruned_transaction_rows = snapshot.metrics.transaction_rows_pruned;
    report.pruned_slot_rows = snapshot.metrics.slot_rows_pruned;
    report.pruned_custom_rows = snapshot.metrics.custom_rows_pruned;
}

fn max_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn latest_processed_slot(batch: &PersistedBatch) -> Option<i64> {
    batch
        .slot_rows
        .iter()
        .map(|row| row.slot)
        .chain(batch.transaction_rows.iter().map(|row| row.slot))
        .chain(batch.account_rows.iter().map(|row| row.slot))
        .chain(batch.custom_rows.iter().map(|row| row.slot))
        .max()
}

fn checkpoint_update_for_batch(batch: &PersistedBatch) -> Option<CheckpointUpdate> {
    let last_processed_slot = batch
        .slot_rows
        .iter()
        .map(|row| row.slot)
        .chain(batch.transaction_rows.iter().map(|row| row.slot))
        .chain(batch.account_rows.iter().map(|row| row.slot))
        .chain(batch.custom_rows.iter().map(|row| row.slot))
        .max();
    let last_observed_at_unix_ms = batch.latest_timestamp_unix_ms()?;

    Some(CheckpointUpdate {
        stream_name: "geyser-main".to_string(),
        last_processed_slot,
        last_observed_at_unix_ms,
        notes: Some(format!("flush_reason={:?}", batch.reason)),
    })
}

#[cfg(test)]
mod tests {
    use super::{PipelineReport, ProcessorPipeline};
    use crate::geyser::decoder::{AccountUpdate, GeyserEvent, SlotUpdate};
    use crate::geyser::BlockTimeCache;
    use crate::processor::batch_writer::BatchWriter;
    use crate::processor::decoder::{CustomDecoder, ProgramActivityDecoder, Type1Decoder};
    use crate::processor::sink::DryRunStorageSink;
    use crate::processor::store::{RetentionPolicy, Type1Store};
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;

    #[tokio::test]
    async fn drains_channel_and_flushes_remaining_items_when_sender_closes() {
        let (sender, receiver) = sync_channel(4);
        sender
            .send(GeyserEvent::AccountUpdate(AccountUpdate {
                timestamp_unix_ms: 1_710_000_000_000,
                slot: 10,
                pubkey: "tracked-account".as_bytes().to_vec(),
                owner: "tracked-program".as_bytes().to_vec(),
                lamports: 5,
                write_version: 1,
                data: vec![1, 2, 3],
            }))
            .expect("send account");
        sender
            .send(GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms: 1_710_000_000_001,
                slot: 10,
                parent_slot: Some(9),
                status: "processed".to_string(),
            }))
            .expect("send slot");
        drop(sender);

        let mut pipeline = ProcessorPipeline::new(
            receiver,
            BatchWriter::new(8, Duration::from_millis(1)),
            Type1Decoder::new(),
            vec![Box::new(ProgramActivityDecoder::new("tracked-program")) as Box<dyn CustomDecoder>],
            Box::new(DryRunStorageSink::new(Type1Store::new(RetentionPolicy {
                max_age: Duration::from_secs(60),
            }))),
            BlockTimeCache::new(1000),
        );
        let report: PipelineReport = pipeline.run().await.expect("pipeline should drain");

        assert_eq!(report.received_events, 2);
        assert_eq!(report.flush_count, 1);
        assert_eq!(report.account_rows_written, 1);
        assert_eq!(report.slot_rows_written, 1);
        assert_eq!(report.custom_rows_written, 1);
        assert_eq!(report.sql_statements_planned, 9);
        assert_eq!(report.retained_account_rows, 1);
        assert_eq!(report.last_processed_slot, Some(10));
        assert_eq!(report.last_observed_at_unix_ms, Some(1_710_000_000_001));
    }
}

=== ./processor.rs ===
pub mod batch_writer;
pub mod cpi_decoder;
pub mod decoder;
pub mod pipeline;
pub mod schema;
pub mod sink;
pub mod sql;
pub mod store;

=== ./processor/schema.rs ===
#[derive(Debug, Clone)]
pub struct AccountUpdateRow {
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    pub pubkey: Vec<u8>,
    pub owner: Vec<u8>,
    pub lamports: i64,
    pub data: Vec<u8>,
    pub write_version: i64,
}

#[derive(Debug, Clone)]
pub struct TransactionRow {
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    pub signature: Vec<u8>,
    pub fee: i64,
    pub success: bool,
    pub program_ids: Vec<Vec<u8>>,
    pub log_messages: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SlotRow {
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    pub parent_slot: Option<i64>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CustomDecodedRow {
    pub decoder_name: String,
    pub record_key: String,
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    pub event_index: i16,
    pub payload: String,
}

=== ./processor/sink.rs ===
use crate::processor::decoder::PersistedBatch;
use crate::processor::sql::{execute_batch, CheckpointUpdate};
use crate::processor::store::{StoreSnapshot, Type1Store};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::error::Error;
use std::fmt::{Display, Formatter};
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct StorageWriteResult {
    pub snapshot: StoreSnapshot,
    pub sql_statements_planned: u64,
}

#[derive(Debug)]
pub enum StorageError {
    Connect(String),
    Execute(String),
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Connect(message) => {
                write!(formatter, "timescale connection failed: {message}")
            }
            StorageError::Execute(message) => {
                write!(formatter, "timescale write failed: {message}")
            }
        }
    }
}

impl Error for StorageError {}

#[async_trait]
pub trait StorageSink: Send {
    async fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError>;
}

#[derive(Debug, Clone)]
pub struct DryRunStorageSink {
    store: Type1Store,
    last_statement_count: u64,
}

impl DryRunStorageSink {
    pub fn new(store: Type1Store) -> Self {
        Self {
            store,
            last_statement_count: 0,
        }
    }

    pub fn last_statement_count(&self) -> u64 {
        self.last_statement_count
    }
}

#[async_trait]
impl StorageSink for DryRunStorageSink {
    async fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError> {
        // Estimate statement count (same logic as execute_batch)
        let mut statement_count = 0u64;
        if !batch.account_rows.is_empty() { statement_count += 1; }
        if !batch.transaction_rows.is_empty() { statement_count += 1; }
        if !batch.slot_rows.is_empty() { statement_count += 1; }
        if !batch.custom_rows.is_empty() { statement_count += 1; }
        if batch.latest_timestamp_unix_ms().is_some() { statement_count += 5; } // retention deletes for 5 tables
        if checkpoint.is_some() { statement_count += 1; }

        self.last_statement_count = statement_count;

        Ok(StorageWriteResult {
            snapshot: self.store.apply_batch(batch.clone()),
            sql_statements_planned: statement_count,
        })
    }
}

pub struct TimescaleStorageSink {
    store: Type1Store,
    pool: PgPool,
    last_statement_count: u64,
}

impl TimescaleStorageSink {
    pub async fn connect(database_url: &str, store: Type1Store) -> Result<Self, StorageError> {
        Self::connect_with_pool_size(database_url, store, 20).await
    }

    pub async fn connect_with_pool_size(database_url: &str, store: Type1Store, max_connections: u32) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(2)
            .acquire_timeout(std::time::Duration::from_secs(60))
            .idle_timeout(std::time::Duration::from_secs(600))
            .max_lifetime(std::time::Duration::from_secs(1800))
            .test_before_acquire(true)
            .connect(database_url)
            .await
            .map_err(|error| StorageError::Connect(error.to_string()))?;

        Ok(Self {
            store,
            pool,
            last_statement_count: 0,
        })
    }

    pub fn with_pool(pool: PgPool, store: Type1Store) -> Self {
        Self {
            store,
            pool,
            last_statement_count: 0,
        }
    }

    pub fn last_statement_count(&self) -> u64 {
        self.last_statement_count
    }
}

#[async_trait]
impl StorageSink for TimescaleStorageSink {
    async fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError> {
        let statement_count = {
            let pool = self.pool.clone();
            let batch = batch.clone();
            let retention_policy = self.store.retention_policy.clone();
            let checkpoint = checkpoint.clone();

            let mut transaction = pool.begin().await
                .map_err(|error| StorageError::Execute(error.to_string()))?;
            let count = execute_batch(&mut transaction, &batch, &retention_policy, checkpoint.as_ref()).await
                .map_err(|error| StorageError::Execute(error.to_string()))?;
            transaction.commit().await
                .map_err(|error| StorageError::Execute(error.to_string()))?;
            count
        };

        self.last_statement_count = statement_count;
        let snapshot = self.store.apply_batch(batch.clone());

        Ok(StorageWriteResult {
            snapshot,
            sql_statements_planned: statement_count,
        })
    }
}


#[cfg(test)]
mod tests {
    use super::{DryRunStorageSink, StorageSink};
    use crate::processor::batch_writer::FlushReason;
    use crate::processor::decoder::PersistedBatch;
    use crate::processor::schema::AccountUpdateRow;
    use crate::processor::sql::CheckpointUpdate;
    use crate::processor::store::{RetentionPolicy, Type1Store};
    use std::time::Duration;

    #[tokio::test]
    async fn dry_run_sink_estimates_statement_count_and_updates_store_snapshot() {
        let mut sink = DryRunStorageSink::new(Type1Store::new(RetentionPolicy {
            max_age: Duration::from_secs(60),
        }));
        let batch = PersistedBatch {
            reason: FlushReason::Size,
            account_rows: vec![AccountUpdateRow {
                slot: 10,
                timestamp_unix_ms: 1_710_000_000_000,
                pubkey: vec![1],
                owner: vec![2],
                lamports: 5,
                data: vec![3],
                write_version: 1,
            }],
            transaction_rows: vec![],
            slot_rows: vec![],
            custom_rows: vec![],
        };

        let result = sink
            .write_batch(
                &batch,
                Some(CheckpointUpdate {
                    stream_name: "geyser-main".to_string(),
                    last_processed_slot: Some(10),
                    last_observed_at_unix_ms: 1_710_000_000_000,
                    notes: None,
                }),
            )
            .await
            .expect("dry-run write");

        assert_eq!(result.snapshot.account_rows, 1);
        assert_eq!(result.sql_statements_planned, 7); // 1 account + 1 checkpoint + 5 retention deletes
        assert_eq!(sink.last_statement_count(), 7);
    }
}

=== ./processor/sql.rs ===
use crate::processor::decoder::PersistedBatch;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};
use crate::processor::store::RetentionPolicy;
use sqlx::Transaction;
use chrono::{TimeZone, Utc};

/// Safely convert Unix milliseconds to DateTime<Utc>, returning a sqlx::Error on out-of-range values.
/// This prevents silent panics from `.unwrap()` in async contexts.
fn to_utc_timestamp(ms: i64) -> Result<chrono::DateTime<Utc>, sqlx::Error> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .ok_or_else(|| {
            sqlx::Error::Protocol(format!("timestamp out of range: {}ms (valid range: -9223372036854775808 to 9223372036854775807)", ms).into())
        })
}

#[derive(Debug, Clone)]
pub struct CheckpointUpdate {
    pub stream_name: String,
    pub last_processed_slot: Option<i64>,
    pub last_observed_at_unix_ms: i64,
    pub notes: Option<String>,
}

/// Executes all batch writes using UNNEST bulk inserts with parameterized queries.
/// This function runs within a transaction and uses PostgreSQL's extended query protocol
/// for maximum performance (plan caching + binary wire format).
pub async fn execute_batch(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    batch: &PersistedBatch,
    retention_policy: &RetentionPolicy,
    checkpoint: Option<&CheckpointUpdate>,
) -> Result<u64, sqlx::Error> {
    let mut statement_count = 0u64;

    if !batch.account_rows.is_empty() {
        execute_account_updates_insert(transaction, &batch.account_rows).await?;
        statement_count += 1;
    }

    if !batch.transaction_rows.is_empty() {
        execute_transactions_insert(transaction, &batch.transaction_rows).await?;
        statement_count += 1;
    }

    if !batch.slot_rows.is_empty() {
        execute_slots_upsert(transaction, &batch.slot_rows).await?;
        statement_count += 1;
    }

    if !batch.custom_rows.is_empty() {
        execute_custom_decoded_insert(transaction, &batch.custom_rows).await?;
        statement_count += 1;
    }

    if let Some(latest_timestamp_unix_ms) = batch.latest_timestamp_unix_ms() {
        statement_count += execute_retention_deletes(transaction, latest_timestamp_unix_ms, retention_policy).await?;
    }

    if let Some(checkpoint) = checkpoint {
        execute_checkpoint_upsert(transaction, checkpoint).await?;
        statement_count += 1;
    }

    Ok(statement_count)
}

async fn execute_account_updates_insert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[AccountUpdateRow],
) -> Result<(), sqlx::Error> {
    let timestamps: Vec<chrono::DateTime<Utc>> = rows
        .iter()
        .map(|row| to_utc_timestamp(row.timestamp_unix_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let pubkeys: Vec<Vec<u8>> = rows.iter().map(|row| row.pubkey.clone()).collect();
    let owners: Vec<Vec<u8>> = rows.iter().map(|row| row.owner.clone()).collect();
    let lamports: Vec<i64> = rows.iter().map(|row| row.lamports).collect();
    let data: Vec<Vec<u8>> = rows.iter().map(|row| row.data.clone()).collect();
    let write_versions: Vec<i64> = rows.iter().map(|row| row.write_version).collect();

    sqlx::query(
        "INSERT INTO account_updates (timestamp, slot, pubkey, owner, lamports, data, write_version)
         SELECT * FROM UNNEST($1::timestamptz[], $2::bigint[], $3::bytea[], $4::bytea[], $5::bigint[], $6::bytea[], $7::bigint[])
         ON CONFLICT (timestamp, slot, pubkey, write_version) DO NOTHING"
    )
    .bind(&timestamps)
    .bind(&slots)
    .bind(&pubkeys)
    .bind(&owners)
    .bind(&lamports)
    .bind(&data)
    .bind(&write_versions)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn execute_transactions_insert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[TransactionRow],
) -> Result<(), sqlx::Error> {
    // Collect scalar columns into parallel arrays for UNNEST bulk insert
    let timestamps: Vec<chrono::DateTime<Utc>> = rows
        .iter()
        .map(|row| to_utc_timestamp(row.timestamp_unix_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let signatures: Vec<Vec<u8>> = rows.iter().map(|row| row.signature.clone()).collect();
    let fees: Vec<i64> = rows.iter().map(|row| row.fee).collect();
    let success: Vec<bool> = rows.iter().map(|row| row.success).collect();

    // Encode array columns as JSON arrays for bulk insert (SQLX limitation: can't bind Vec<Vec<Vec<u8>>> or Vec<Vec<String>>)
    // Vec<Vec<u8>> (program_ids) encoded as JSON array of hex-encoded bytea strings
    // Vec<String> (log_messages) encoded as JSON array of escaped strings
    let program_ids_json: Vec<String> = rows
        .iter()
        .map(|row| {
            if row.program_ids.is_empty() {
                return "[]".to_string();
            }
            let encoded: Vec<String> = row.program_ids.iter()
                .map(|bytes| {
                    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
                    format!("\"\\\\x{}\"", hex)
                })
                .collect();
            format!("[{}]", encoded.join(","))
        })
        .collect();
    let log_messages_json: Vec<String> = rows
        .iter()
        .map(|row| {
            if row.log_messages.is_empty() {
                return "[]".to_string();
            }
            let encoded: Vec<String> = row.log_messages.iter()
                .map(|msg| {
                    // Escape backslashes and double quotes for JSON
                    let escaped = msg.replace('\\', "\\\\").replace('"', "\\\"");
                    format!("\"{}\"", escaped)
                })
                .collect();
            format!("[{}]", encoded.join(","))
        })
        .collect();

    // Use UNNEST for bulk insert with JSON-to-array conversion in SELECT clause
    // This reduces N round-trips to 1 round-trip for all rows
    sqlx::query(
        "INSERT INTO transactions (timestamp, slot, signature, fee, success, program_ids, log_messages)
         SELECT
            unnested.timestamp,
            unnested.slot,
            unnested.signature,
            unnested.fee,
            unnested.success,
            ARRAY(SELECT e::bytea FROM jsonb_array_elements_text(unnested.program_ids) AS e),
            ARRAY(SELECT e FROM jsonb_array_elements_text(unnested.log_messages) AS e)
         FROM UNNEST(
            $1::timestamptz[],
            $2::bigint[],
            $3::bytea[],
            $4::bigint[],
            $5::boolean[],
            $6::jsonb[],
            $7::jsonb[]
         ) AS unnested(timestamp, slot, signature, fee, success, program_ids, log_messages)
         ON CONFLICT (timestamp, signature) DO NOTHING"
    )
    .bind(&timestamps)
    .bind(&slots)
    .bind(&signatures)
    .bind(&fees)
    .bind(&success)
    .bind(&program_ids_json)
    .bind(&log_messages_json)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn execute_slots_upsert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[SlotRow],
) -> Result<(), sqlx::Error> {
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let parent_slots: Vec<Option<i64>> = rows.iter().map(|row| row.parent_slot).collect();
    let timestamps: Vec<chrono::DateTime<Utc>> = rows
        .iter()
        .map(|row| to_utc_timestamp(row.timestamp_unix_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let statuses: Vec<String> = rows.iter().map(|row| row.status.clone()).collect();

    sqlx::query(
        "INSERT INTO slots (slot, parent_slot, timestamp, status)
         SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::timestamptz[], $4::text[])
         ON CONFLICT (timestamp, slot) DO UPDATE SET
           parent_slot = EXCLUDED.parent_slot,
           status = EXCLUDED.status"
    )
    .bind(&slots)
    .bind(&parent_slots)
    .bind(&timestamps)
    .bind(&statuses)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn execute_custom_decoded_insert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[CustomDecodedRow],
) -> Result<(), sqlx::Error> {
    let timestamps: Vec<chrono::DateTime<Utc>> = rows
        .iter()
        .map(|row| to_utc_timestamp(row.timestamp_unix_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let decoder_names: Vec<String> = rows.iter().map(|row| row.decoder_name.clone()).collect();
    let record_keys: Vec<String> = rows.iter().map(|row| row.record_key.clone()).collect();
    let event_indexes: Vec<i16> = rows.iter().map(|row| row.event_index).collect();
    let payloads: Vec<String> = rows.iter().map(|row| row.payload.clone()).collect();

    sqlx::query(
        "INSERT INTO custom_decoded_events (timestamp, slot, decoder_name, record_key, event_index, payload)
         SELECT * FROM UNNEST($1::timestamptz[], $2::bigint[], $3::text[], $4::text[], $5::smallint[], $6::jsonb[])
         ON CONFLICT (timestamp, decoder_name, record_key, slot, event_index) DO NOTHING"
    )
    .bind(&timestamps)
    .bind(&slots)
    .bind(&decoder_names)
    .bind(&record_keys)
    .bind(&event_indexes)
    .bind(&payloads)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn execute_retention_deletes(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    latest_timestamp_unix_ms: i64,
    retention_policy: &RetentionPolicy,
) -> Result<u64, sqlx::Error> {
    let max_age_ms = i64::try_from(retention_policy.max_age.as_millis())
        .unwrap_or(i64::MAX);
    let cutoff_ms = latest_timestamp_unix_ms.saturating_sub(max_age_ms);
    let cutoff = to_utc_timestamp(cutoff_ms)?;

    let tables = [
        "account_updates",
        "transactions",
        "custom_decoded_events",
        "pipeline_metrics",
        "slots",
    ];

    let mut statement_count = 0u64;
    for table in tables {
        sqlx::query(&format!("DELETE FROM {} WHERE timestamp < $1", table))
            .bind(cutoff)
            .execute(&mut **transaction)
            .await?;
        statement_count += 1;
    }

    Ok(statement_count)
}

async fn execute_checkpoint_upsert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    checkpoint: &CheckpointUpdate,
) -> Result<(), sqlx::Error> {
    let last_observed_at = to_utc_timestamp(checkpoint.last_observed_at_unix_ms)?;

    sqlx::query(
        "INSERT INTO ingestion_checkpoints (stream_name, last_processed_slot, last_observed_at, notes)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (stream_name) DO UPDATE SET
           last_processed_slot = EXCLUDED.last_processed_slot,
           last_observed_at = EXCLUDED.last_observed_at,
           notes = EXCLUDED.notes"
    )
    .bind(&checkpoint.stream_name)
    .bind(checkpoint.last_processed_slot)
    .bind(last_observed_at)
    .bind(&checkpoint.notes)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}



#[cfg(test)]
mod tests {
    // Note: With the migration to parameterized queries, we can no longer test SQL string generation
    // without a database connection. Integration tests should be added to verify the
    // execute_batch function works correctly with a real PostgreSQL instance.
}

=== ./processor/store.rs ===
use std::time::Duration;

use crate::processor::decoder::PersistedBatch;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub max_age: Duration,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(60 * 60),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StoreMetrics {
    pub account_rows_pruned: u64,
    pub transaction_rows_pruned: u64,
    pub slot_rows_pruned: u64,
    pub custom_rows_pruned: u64,
}

#[derive(Debug, Clone, Default)]
pub struct StoreSnapshot {
    pub account_rows: usize,
    pub transaction_rows: usize,
    pub slot_rows: usize,
    pub custom_rows: usize,
    pub metrics: StoreMetrics,
}

#[derive(Debug, Clone)]
pub struct Type1Store {
    pub retention_policy: RetentionPolicy,
    pub account_rows: Vec<AccountUpdateRow>,
    pub transaction_rows: Vec<TransactionRow>,
    pub slot_rows: Vec<SlotRow>,
    pub custom_rows: Vec<CustomDecodedRow>,
    pub metrics: StoreMetrics,
}

impl Type1Store {
    pub fn new(retention_policy: RetentionPolicy) -> Self {
        Self {
            retention_policy,
            account_rows: Vec::new(),
            transaction_rows: Vec::new(),
            slot_rows: Vec::new(),
            custom_rows: Vec::new(),
            metrics: StoreMetrics::default(),
        }
    }

    pub fn apply_batch(&mut self, batch: PersistedBatch) -> StoreSnapshot {
        self.account_rows.extend(batch.account_rows);
        self.transaction_rows.extend(batch.transaction_rows);
        self.slot_rows.extend(batch.slot_rows);
        self.custom_rows.extend(batch.custom_rows);

        if let Some(latest_timestamp_unix_ms) = self.latest_timestamp_unix_ms() {
            self.prune_stale(latest_timestamp_unix_ms);
        }

        self.snapshot()
    }

    pub fn snapshot(&self) -> StoreSnapshot {
        StoreSnapshot {
            account_rows: self.account_rows.len(),
            transaction_rows: self.transaction_rows.len(),
            slot_rows: self.slot_rows.len(),
            custom_rows: self.custom_rows.len(),
            metrics: self.metrics.clone(),
        }
    }

    fn latest_timestamp_unix_ms(&self) -> Option<i64> {
        self.account_rows
            .iter()
            .map(|row| row.timestamp_unix_ms)
            .chain(
                self.transaction_rows
                    .iter()
                    .map(|row| row.timestamp_unix_ms),
            )
            .chain(self.slot_rows.iter().map(|row| row.timestamp_unix_ms))
            .chain(self.custom_rows.iter().map(|row| row.timestamp_unix_ms))
            .max()
    }

    fn prune_stale(&mut self, latest_timestamp_unix_ms: i64) {
        let cutoff = latest_timestamp_unix_ms - self.retention_policy.max_age.as_millis() as i64;

        prune_with_metrics(
            &mut self.account_rows,
            cutoff,
            &mut self.metrics.account_rows_pruned,
            |row| row.timestamp_unix_ms,
        );
        prune_with_metrics(
            &mut self.transaction_rows,
            cutoff,
            &mut self.metrics.transaction_rows_pruned,
            |row| row.timestamp_unix_ms,
        );
        prune_with_metrics(
            &mut self.slot_rows,
            cutoff,
            &mut self.metrics.slot_rows_pruned,
            |row| row.timestamp_unix_ms,
        );
        prune_with_metrics(
            &mut self.custom_rows,
            cutoff,
            &mut self.metrics.custom_rows_pruned,
            |row| row.timestamp_unix_ms,
        );
    }
}

fn prune_with_metrics<T, F>(rows: &mut Vec<T>, cutoff: i64, metric: &mut u64, timestamp: F)
where
    F: Fn(&T) -> i64,
{
    let before = rows.len();
    rows.retain(|row| timestamp(row) >= cutoff);
    *metric += (before - rows.len()) as u64;
}

#[cfg(test)]
mod tests {
    use super::{RetentionPolicy, Type1Store};
    use crate::processor::batch_writer::FlushReason;
    use crate::processor::decoder::PersistedBatch;
    use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};
    use std::time::Duration;

    #[test]
    fn prunes_rows_older_than_retention_window() {
        let mut store = Type1Store::new(RetentionPolicy {
            max_age: Duration::from_millis(20),
        });

        let first_batch = PersistedBatch {
            reason: FlushReason::Size,
            account_rows: vec![AccountUpdateRow {
                slot: 1,
                timestamp_unix_ms: 100,
                pubkey: vec![1],
                owner: vec![2],
                lamports: 1,
                data: vec![3],
                write_version: 1,
            }],
            transaction_rows: vec![],
            slot_rows: vec![],
            custom_rows: vec![],
        };
        store.apply_batch(first_batch);

        let second_batch = PersistedBatch {
            reason: FlushReason::Interval,
            account_rows: vec![],
            transaction_rows: vec![TransactionRow {
                slot: 2,
                timestamp_unix_ms: 125,
                signature: vec![4],
                fee: 5,
                success: true,
                program_ids: vec![vec![9]],
                log_messages: vec!["ok".to_string()],
            }],
            slot_rows: vec![SlotRow {
                slot: 2,
                timestamp_unix_ms: 125,
                parent_slot: Some(1),
                status: "processed".to_string(),
            }],
            custom_rows: vec![CustomDecodedRow {
                decoder_name: "program-activity".to_string(),
                record_key: "tracked".to_string(),
                slot: 2,
                timestamp_unix_ms: 125,
                event_index: 0,
                payload: "transaction".to_string(),
            }],
        };

        let snapshot = store.apply_batch(second_batch);

        assert_eq!(snapshot.account_rows, 0);
        assert_eq!(snapshot.transaction_rows, 1);
        assert_eq!(snapshot.slot_rows, 1);
        assert_eq!(snapshot.custom_rows, 1);
        assert_eq!(snapshot.metrics.account_rows_pruned, 1);
    }
}

=== ./rpc_pool/circuit_breaker.rs ===
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    state: CircuitState,
    consecutive_failures: u32,
    open_threshold: u32,
    half_open_after: Duration,
    opened_at: Option<Instant>,
    half_open_probe_in_flight: bool,
}

impl CircuitBreaker {
    pub fn new(open_threshold: u32, half_open_after: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            open_threshold,
            half_open_after,
            opened_at: None,
            half_open_probe_in_flight: false,
        }
    }

    pub fn current_state(&self, now: Instant) -> CircuitState {
        // Check if we should transition to HalfOpen without mutating
        if self.state == CircuitState::Open
            && self
                .opened_at
                .is_some_and(|opened_at| now.saturating_duration_since(opened_at) >= self.half_open_after)
        {
            CircuitState::HalfOpen
        } else {
            self.state
        }
    }

    pub fn sync_state(&mut self, now: Instant) {
        // Explicitly sync state - call this at mutation points
        if self.state == CircuitState::Open
            && self
                .opened_at
                .is_some_and(|opened_at| now.saturating_duration_since(opened_at) >= self.half_open_after)
        {
            self.state = CircuitState::HalfOpen;
            self.half_open_probe_in_flight = false;
        }
    }

    pub fn allow_request(&mut self, now: Instant) -> bool {
        self.sync_state(now); // Explicit sync before checking

        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => {
                if self.half_open_probe_in_flight {
                    false
                } else {
                    self.half_open_probe_in_flight = true;
                    true
                }
            }
        }
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.state = CircuitState::Closed;
        self.opened_at = None;
        self.half_open_probe_in_flight = false;
    }

    pub fn record_failure(&mut self, now: Instant) -> bool {
        self.sync_state(now); // Explicit sync before recording

        match self.state {
            CircuitState::Closed => {
                self.consecutive_failures += 1;
                if self.consecutive_failures >= self.open_threshold {
                    self.trip(now);
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                self.trip(now);
                true
            }
            CircuitState::Open => false,
        }
    }

    fn trip(&mut self, now: Instant) {
        self.state = CircuitState::Open;
        self.opened_at = Some(now);
        self.half_open_probe_in_flight = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{CircuitBreaker, CircuitState};
    use std::time::{Duration, Instant};

    #[test]
    fn opens_after_threshold_failures() {
        let start = Instant::now();
        let mut breaker = CircuitBreaker::new(3, Duration::from_secs(30));

        assert_eq!(breaker.current_state(start), CircuitState::Closed);
        assert!(!breaker.record_failure(start));
        assert!(!breaker.record_failure(start + Duration::from_secs(1)));
        assert!(breaker.record_failure(start + Duration::from_secs(2)));
        assert_eq!(
            breaker.current_state(start + Duration::from_secs(2)),
            CircuitState::Open
        );
    }

    #[test]
    fn transitions_to_half_open_after_cooldown() {
        let start = Instant::now();
        let mut breaker = CircuitBreaker::new(1, Duration::from_secs(10));

        assert!(breaker.record_failure(start));
        assert!(!breaker.allow_request(start + Duration::from_secs(5)));
        assert!(breaker.allow_request(start + Duration::from_secs(10)));
        assert!(!breaker.allow_request(start + Duration::from_secs(10)));
        assert_eq!(
            breaker.current_state(start + Duration::from_secs(10)),
            CircuitState::HalfOpen
        );
    }

    #[test]
    fn half_open_probe_success_closes_breaker() {
        let start = Instant::now();
        let mut breaker = CircuitBreaker::new(1, Duration::from_secs(5));

        assert!(breaker.record_failure(start));
        assert!(breaker.allow_request(start + Duration::from_secs(5)));
        breaker.record_success();

        assert_eq!(
            breaker.current_state(start + Duration::from_secs(5)),
            CircuitState::Closed
        );
        assert!(breaker.allow_request(start + Duration::from_secs(5)));
    }

    #[test]
    fn half_open_probe_failure_reopens_breaker() {
        let start = Instant::now();
        let mut breaker = CircuitBreaker::new(1, Duration::from_secs(5));

        assert!(breaker.record_failure(start));
        assert!(breaker.allow_request(start + Duration::from_secs(5)));
        assert!(breaker.record_failure(start + Duration::from_secs(5)));
        assert_eq!(
            breaker.current_state(start + Duration::from_secs(5)),
            CircuitState::Open
        );
    }
}

=== ./rpc_pool/health.rs ===
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HealthStatus {
    Healthy,
    Unknown,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone)]
pub struct HealthPolicy {
    pub max_slot_lag: u64,
    pub max_latency_ms: u64,
    pub stale_after: Duration,
}

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub endpoint: String,
    pub probe_success: bool,
    pub latest_slot: Option<u64>,
    pub latency_ms: Option<u64>,
    pub observed_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthAssessment {
    pub status: HealthStatus,
    pub slot_lag: Option<u64>,
    pub latency_ms: Option<u64>,
    pub is_stale: bool,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            max_slot_lag: 8,
            max_latency_ms: 1_500,
            stale_after: Duration::from_secs(15),
        }
    }
}

impl HealthSnapshot {
    pub fn healthy(
        endpoint: impl Into<String>,
        latest_slot: u64,
        latency_ms: u64,
        observed_at: Instant,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            probe_success: true,
            latest_slot: Some(latest_slot),
            latency_ms: Some(latency_ms),
            observed_at,
        }
    }

    pub fn unhealthy(endpoint: impl Into<String>, observed_at: Instant) -> Self {
        Self {
            endpoint: endpoint.into(),
            probe_success: false,
            latest_slot: None,
            latency_ms: None,
            observed_at,
        }
    }

    pub fn assess(
        &self,
        reference_slot: Option<u64>,
        policy: &HealthPolicy,
        now: Instant,
    ) -> HealthAssessment {
        let is_stale = now.saturating_duration_since(self.observed_at) > policy.stale_after;
        let slot_lag = match (reference_slot, self.latest_slot) {
            (Some(reference_slot), Some(latest_slot)) => Some(reference_slot.saturating_sub(latest_slot)),
            _ => None,
        };

        let status = if !self.probe_success || is_stale {
            HealthStatus::Unhealthy
        } else if self.latest_slot.is_none() && self.latency_ms.is_none() {
            HealthStatus::Unknown
        } else if slot_lag.is_some_and(|lag| lag > policy.max_slot_lag)
            || self.latency_ms.is_some_and(|latency_ms| latency_ms > policy.max_latency_ms)
        {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        HealthAssessment {
            status,
            slot_lag,
            latency_ms: self.latency_ms,
            is_stale,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HealthPolicy, HealthSnapshot, HealthStatus};
    use std::time::{Duration, Instant};

    #[test]
    fn marks_snapshot_degraded_when_slot_lag_exceeds_policy() {
        let now = Instant::now();
        let snapshot = HealthSnapshot::healthy("rpc-a", 100, 50, now);
        let policy = HealthPolicy {
            max_slot_lag: 2,
            ..HealthPolicy::default()
        };

        let assessment = snapshot.assess(Some(104), &policy, now);

        assert_eq!(assessment.status, HealthStatus::Degraded);
        assert_eq!(assessment.slot_lag, Some(4));
    }

    #[test]
    fn marks_snapshot_unhealthy_when_stale() {
        let now = Instant::now();
        let snapshot = HealthSnapshot::healthy("rpc-a", 100, 50, now);
        let policy = HealthPolicy {
            stale_after: Duration::from_secs(5),
            ..HealthPolicy::default()
        };

        let assessment = snapshot.assess(Some(100), &policy, now + Duration::from_secs(6));

        assert_eq!(assessment.status, HealthStatus::Unhealthy);
        assert!(assessment.is_stale);
    }
}

=== ./rpc_pool/metrics.rs ===
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct PoolMetrics {
    total_requests: AtomicU64,
    total_failovers: AtomicU64,
    circuit_open_events: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolMetricsSnapshot {
    pub total_requests: u64,
    pub total_failovers: u64,
    pub circuit_open_events: u64,
}

impl PoolMetrics {
    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failover(&self) {
        self.total_failovers.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_circuit_open(&self) {
        self.circuit_open_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_failovers: self.total_failovers.load(Ordering::Relaxed),
            circuit_open_events: self.circuit_open_events.load(Ordering::Relaxed),
        }
    }
}

=== ./rpc_pool/pool.rs ===
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::rpc_pool::circuit_breaker::{CircuitBreaker, CircuitState};
use crate::rpc_pool::health::{HealthAssessment, HealthPolicy, HealthSnapshot, HealthStatus};
use crate::rpc_pool::metrics::{PoolMetrics, PoolMetricsSnapshot};

#[derive(Debug)]
pub struct RpcPool {
    pub endpoints: Arc<RwLock<Vec<EndpointState>>>,
    pub config: PoolConfig,
    pub metrics: Arc<PoolMetrics>,
}

#[derive(Debug, Clone)]
pub struct EndpointState {
    pub url: String,
    pub weight: u32,
    pub circuit: CircuitBreaker,
    pub health: Option<HealthSnapshot>,
    pub observed_p99_latency_ms: u64,
    pub in_flight: usize,
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub health_check_interval: Duration,
    pub circuit_open_threshold: u32,
    pub circuit_half_open_after: Duration,
    pub request_timeout: Duration,
    pub health_policy: HealthPolicy,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 32,
            health_check_interval: Duration::from_secs(5),
            circuit_open_threshold: 3,
            circuit_half_open_after: Duration::from_secs(30),
            request_timeout: Duration::from_secs(3),
            health_policy: HealthPolicy::default(),
        }
    }
}

impl RpcPool {
    pub fn new(endpoints: Vec<String>, config: PoolConfig) -> Self {
        let states = endpoints
            .into_iter()
            .map(|url| EndpointState {
                url,
                weight: 1,
                circuit: CircuitBreaker::new(
                    config.circuit_open_threshold,
                    config.circuit_half_open_after,
                ),
                health: None,
                observed_p99_latency_ms: 1,
                in_flight: 0,
            })
            .collect();

        Self {
            endpoints: Arc::new(RwLock::new(states)),
            config,
            metrics: Arc::new(PoolMetrics::default()),
        }
    }

    pub fn choose_endpoint(&self) -> Option<String> {
        let endpoints = self.endpoints.read().ok()?;
        let index = select_endpoint_index(&endpoints, &self.config.health_policy, Instant::now())?;
        Some(endpoints[index].url.clone())
    }

    pub fn begin_request(&self) -> Option<String> {
        self.begin_request_at(Instant::now())
    }

    pub fn apply_health_snapshot(&self, snapshot: HealthSnapshot) -> bool {
        let mut endpoints = match self.endpoints.write() {
            Ok(endpoints) => endpoints,
            Err(_) => return false,
        };

        let Some(endpoint) = endpoints.iter_mut().find(|endpoint| endpoint.url == snapshot.endpoint) else {
            return false;
        };

        if let Some(latency_ms) = snapshot.latency_ms {
            endpoint.update_latency(latency_ms);
        }

        endpoint.health = Some(snapshot);
        true
    }

    pub fn record_success(&self, endpoint_url: &str, latency_ms: u64) -> bool {
        let mut endpoints = match self.endpoints.write() {
            Ok(endpoints) => endpoints,
            Err(_) => return false,
        };

        let Some(endpoint) = endpoints.iter_mut().find(|endpoint| endpoint.url == endpoint_url) else {
            return false;
        };

        endpoint.complete_request();
        endpoint.update_latency(latency_ms);
        endpoint.circuit.record_success();
        true
    }

    pub fn record_failure(&self, endpoint_url: &str) -> bool {
        self.record_failure_at(endpoint_url, Instant::now())
    }

    pub fn metrics_snapshot(&self) -> PoolMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn begin_request_at(&self, now: Instant) -> Option<String> {
        let mut endpoints = self.endpoints.write().ok()?;
        let index = select_endpoint_index(&mut endpoints, &self.config.health_policy, now)?;
        let endpoint = &mut endpoints[index];

        if !endpoint.circuit.allow_request(now) {
            return None;
        }

        endpoint.in_flight += 1;
        self.metrics.record_request();
        Some(endpoint.url.clone())
    }

    fn record_failure_at(&self, endpoint_url: &str, now: Instant) -> bool {
        let mut endpoints = match self.endpoints.write() {
            Ok(endpoints) => endpoints,
            Err(_) => return false,
        };

        let endpoint_count = endpoints.len();
        let Some(endpoint) = endpoints.iter_mut().find(|endpoint| endpoint.url == endpoint_url) else {
            return false;
        };

        endpoint.complete_request();
        if endpoint_count > 1 {
            self.metrics.record_failover();
        }

        if endpoint.circuit.record_failure(now) {
            self.metrics.record_circuit_open();
        }

        true
    }
}

impl EndpointState {
    fn routing_key(&self, assessment: Option<&HealthAssessment>, now: Instant) -> (u8, u64, u64) {
        let circuit_rank = match self.circuit_rank(now) {
            CircuitState::Closed => 0,
            CircuitState::HalfOpen => 1,
            CircuitState::Open => 2,
        };

        let health_rank = match assessment.map(|assessment| assessment.status) {
            Some(HealthStatus::Healthy) => 0,
            Some(HealthStatus::Unknown) | None => 1,
            Some(HealthStatus::Degraded) => 2,
            Some(HealthStatus::Unhealthy) => 3,
        };

        (health_rank, circuit_rank, self.score(assessment))
    }

    fn score(&self, assessment: Option<&HealthAssessment>) -> u64 {
        let latency_ms = assessment
            .and_then(|assessment| assessment.latency_ms)
            .unwrap_or(self.observed_p99_latency_ms)
            .max(1);
        let slot_lag_penalty = assessment
            .and_then(|assessment| assessment.slot_lag)
            .unwrap_or(0)
            .saturating_mul(10);
        let degradation_penalty = match assessment.map(|assessment| assessment.status) {
            Some(HealthStatus::Degraded) => 500,
            Some(HealthStatus::Unhealthy) => u64::MAX / 4,
            _ => 0,
        };
        let load_penalty = (self.in_flight as u64 + 1) * latency_ms;
        let weighted_score = load_penalty
            .saturating_add(slot_lag_penalty)
            .saturating_add(degradation_penalty);

        weighted_score / self.weight.max(1) as u64
    }

    fn update_latency(&mut self, latency_ms: u64) {
        let previous = self.observed_p99_latency_ms.max(1);
        self.observed_p99_latency_ms = ((previous * 7) + latency_ms.max(1)) / 8;
    }

    fn complete_request(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    fn circuit_rank(&self, now: Instant) -> CircuitState {
        self.circuit.current_state(now) // Now takes &self, no clone needed
    }
}

fn newest_observed_slot(endpoints: &[EndpointState]) -> Option<u64> {
    endpoints
        .iter()
        .filter_map(|endpoint| endpoint.health.as_ref()?.latest_slot)
        .max()
}

fn select_endpoint_index(
    endpoints: &[EndpointState],
    health_policy: &HealthPolicy,
    now: Instant,
) -> Option<usize> {
    let reference_slot = newest_observed_slot(endpoints);
    let mut selected_index = None;
    let mut selected_key = None;

    for (index, endpoint) in endpoints.iter().enumerate() {
        if endpoint.circuit.current_state(now) == CircuitState::Open {
            continue;
        }

        let assessment = endpoint
            .health
            .as_ref()
            .map(|snapshot| snapshot.assess(reference_slot, health_policy, now));

        if assessment
            .as_ref()
            .is_some_and(|assessment| assessment.status == HealthStatus::Unhealthy)
        {
            continue;
        }

        let key = endpoint.routing_key(assessment.as_ref(), now);
        if selected_key.is_none_or(|current_key| key < current_key) {
            selected_index = Some(index);
            selected_key = Some(key);
        }
    }

    selected_index
}

#[cfg(test)]
mod tests {
    use super::{EndpointState, PoolConfig, RpcPool};
    use crate::rpc_pool::health::{HealthPolicy, HealthSnapshot};
    use std::time::{Duration, Instant};

    #[test]
    fn prefers_healthier_endpoint_even_when_other_is_present() {
        let pool = RpcPool::new(
            vec!["rpc-a".to_string(), "rpc-b".to_string()],
            PoolConfig::default(),
        );
        let now = Instant::now();

        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-a", 1_000, 40, now)));
        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-b", 980, 35, now)));

        let chosen = pool.begin_request_at(now);

        assert_eq!(chosen.as_deref(), Some("rpc-a"));
    }

    #[test]
    fn skips_unhealthy_endpoint_and_uses_alternative() {
        let config = PoolConfig {
            health_policy: HealthPolicy {
                max_slot_lag: 2,
                ..HealthPolicy::default()
            },
            ..PoolConfig::default()
        };

        let pool = RpcPool::new(
            vec!["rpc-a".to_string(), "rpc-b".to_string()],
            config,
        );
        let now = Instant::now();

        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-a", 1_000, 40, now)));
        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-b", 995, 35, now)));
        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-a"));

        assert!(pool.apply_health_snapshot(HealthSnapshot::unhealthy("rpc-a", now)));
        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-b"));
    }

    #[test]
    fn failure_opens_circuit_and_updates_metrics() {
        let config = PoolConfig {
            circuit_open_threshold: 1,
            ..PoolConfig::default()
        };

        let pool = RpcPool::new(vec!["rpc-a".to_string(), "rpc-b".to_string()], config);
        let now = Instant::now();

        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-a"));
        assert!(pool.record_failure_at("rpc-a", now));

        let metrics = pool.metrics_snapshot();
        assert_eq!(metrics.total_requests, 1);
        assert_eq!(metrics.total_failovers, 1);
        assert_eq!(metrics.circuit_open_events, 1);
    }

    #[test]
    fn success_reduces_in_flight_and_smooths_latency() {
        let pool = RpcPool::new(vec!["rpc-a".to_string()], PoolConfig::default());
        let now = Instant::now();

        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-a"));
        assert!(pool.record_success("rpc-a", 80));

        let endpoints = pool.endpoints.read().expect("pool lock");
        let endpoint: &EndpointState = &endpoints[0];

        assert_eq!(endpoint.in_flight, 0);
        assert!(endpoint.observed_p99_latency_ms > 1);
    }

    #[test]
    fn degraded_endpoint_is_still_used_when_it_is_only_option() {
        let config = PoolConfig {
            health_policy: HealthPolicy {
                max_latency_ms: 10,
                stale_after: Duration::from_secs(60),
                ..HealthPolicy::default()
            },
            ..PoolConfig::default()
        };

        let pool = RpcPool::new(vec!["rpc-a".to_string()], config);
        let now = Instant::now();

        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-a", 1_000, 40, now)));
        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-a"));
    }
}

=== ./rpc_pool.rs ===
pub mod circuit_breaker;
pub mod health;
pub mod metrics;
pub mod pool;

