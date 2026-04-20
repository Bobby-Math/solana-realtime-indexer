use std::time::Duration;

use axum::{extract::State, Json};
use serde::Serialize;

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
}

#[derive(Debug, Clone)]
pub struct ApiSnapshot {
    pub project: String,
    pub storage_mode: String,
    pub bind_address: String,
    pub rpc_endpoint_count: usize,
    pub queue_depth: usize,
    pub elapsed_secs: f64,
    pub last_processed_slot: Option<i64>,
    pub last_observed_at_unix_ms: Option<i64>,
    pub indexed_at_unix_ms: i64,
    pub report: PipelineReport,
}

impl ApiSnapshot {
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
            elapsed_secs: elapsed.as_secs_f64(),
            last_processed_slot: report.last_processed_slot,
            last_observed_at_unix_ms: report.last_observed_at_unix_ms,
            indexed_at_unix_ms,
            report,
        }
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
            .map(|observed_at| self.indexed_at_unix_ms.saturating_sub(observed_at))
    }
}

pub async fn health(State(state): State<SharedApiState>) -> Json<HealthResponse> {
    Json(state.health_response())
}

pub async fn metrics(State(state): State<SharedApiState>) -> Json<MetricsResponse> {
    Json(state.metrics_response())
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
        );
        let health = snapshot.health_response();
        let metrics = snapshot.metrics_response();

        assert_eq!(health.last_processed_slot, Some(55));
        assert_eq!(health.slot_to_indexed_lag_ms, Some(25));
        assert_eq!(metrics.ingest_events_per_sec, 50.0);
        assert_eq!(metrics.db_rows_written_per_sec, 50.0);
    }
}
