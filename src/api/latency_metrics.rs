use axum::{extract::State, Json};
use serde::Serialize;
use sqlx::Row;

use crate::api::SharedApiState;

#[derive(Debug, Clone, Serialize)]
pub struct LatencyMetricsResponse {
    pub avg_latency_ms: f64,
    pub max_latency_ms: f64,
    pub min_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub data_points: Vec<LatencyDataPoint>,
    pub sampled_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyDataPoint {
    pub slot: i64,
    pub block_time: String,
    pub db_write_time: String,
    pub latency_ms: f64,
}

pub async fn get_latency_metrics(
    State(state): State<SharedApiState>,
) -> Result<Json<LatencyMetricsResponse>, Json<&'static str>> {
    // Extract pool clone under lock, then release immediately
    let pool = {
        let snapshot = state.read().await;
        snapshot.pool.clone()
    };

    let pool = pool.ok_or("Database pool not available")?;

    // Query latency metrics from the last 60 minutes
    let rows = sqlx::query(
        r#"
        SELECT
            slot,
            block_time,
            timestamp as db_write_time,
            CAST(EXTRACT(EPOCH FROM (timestamp - block_time)) * 1000 AS DOUBLE PRECISION) as latency_ms
        FROM slots
        WHERE block_time IS NOT NULL
          AND timestamp > NOW() - INTERVAL '60 minutes'
        ORDER BY timestamp DESC
        LIMIT 1000
        "#,
    )
    .fetch_all(&pool)
    .await
    .map_err(|_| "Failed to query latency metrics")?;

    let data_points: Vec<LatencyDataPoint> = rows
        .into_iter()
        .filter_map(|row| {
            let slot: i64 = row.get("slot");
            let block_time: Option<chrono::DateTime<chrono::Utc>> = row.get("block_time");
            let db_write_time: chrono::DateTime<chrono::Utc> = row.get("db_write_time");
            let latency_ms: Option<f64> = row.get("latency_ms");

            match (block_time, latency_ms) {
                (Some(bt), Some(latency)) if latency >= 0.0 => Some(LatencyDataPoint {
                    slot,
                    block_time: bt.to_rfc3339(),
                    db_write_time: db_write_time.to_rfc3339(),
                    latency_ms: latency,
                }),
                _ => None,
            }
        })
        .collect();

    if data_points.is_empty() {
        return Ok(Json(LatencyMetricsResponse {
            avg_latency_ms: 0.0,
            max_latency_ms: 0.0,
            min_latency_ms: 0.0,
            p50_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            data_points: vec![],
            sampled_at: chrono::Utc::now().to_rfc3339(),
        }));
    }

    // Calculate percentiles
    let mut latencies: Vec<f64> = data_points.iter().map(|dp| dp.latency_ms).collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let len = latencies.len();
    let avg_latency_ms = latencies.iter().sum::<f64>() / len as f64;
    let max_latency_ms = latencies.last().copied().unwrap_or(0.0);
    let min_latency_ms = latencies.first().copied().unwrap_or(0.0);
    let p50_latency_ms = latencies[len * 50 / 100];
    let p95_latency_ms = latencies[len * 95 / 100];
    let p99_latency_ms = latencies[len * 99 / 100];

    Ok(Json(LatencyMetricsResponse {
        avg_latency_ms,
        max_latency_ms,
        min_latency_ms,
        p50_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        data_points,
        sampled_at: chrono::Utc::now().to_rfc3339(),
    }))
}
