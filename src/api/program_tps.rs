use axum::{extract::State, Json};
use serde::Serialize;
use sqlx::Row;

use crate::api::SharedApiState;

#[derive(Debug, Clone, Serialize)]
pub struct ProgramTpsEntry {
    pub program_id: String,
    pub tps: f64,
    pub tx_count: u64,
    pub failure_rate_percent: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProgramTpsResponse {
    pub leaderboard: Vec<ProgramTpsEntry>,
    pub time_window_minutes: i32,
    pub total_tx_count: u64,
}

pub async fn get_program_tps(
    State(state): State<SharedApiState>,
) -> Result<Json<ProgramTpsResponse>, Json<&'static str>> {
    let pool = {
        let snapshot = state.read().await;
        snapshot.pool.clone()
    };

    let pool = pool.ok_or("Database pool not available")?;

    let rows = sqlx::query(
        r#"
        SELECT
            tp.program_id,
            COUNT(*) AS tx_count,
            COUNT(*) FILTER (WHERE NOT t.success) AS failure_count,
            CAST(EXTRACT(EPOCH FROM MAX(tp.timestamp) - MIN(tp.timestamp)) AS DOUBLE PRECISION) AS time_span_seconds
        FROM transaction_program_ids tp
        JOIN transactions t ON t.signature = tp.signature
        WHERE tp.timestamp > NOW() - INTERVAL '5 minutes'
        GROUP BY tp.program_id
        ORDER BY tx_count DESC
        LIMIT 10
        "#
    )
    .fetch_all(&pool)
    .await
    .map_err(|_| "Failed to query program TPS")?;

    let leaderboard: Vec<ProgramTpsEntry> = rows
        .into_iter()
        .map(|row| {
            let (program_id, tx_count, failure_count, time_span_seconds): (
                Vec<u8>,
                i64,
                i64,
                Option<f64>,
            ) = (row.get(0), row.get(1), row.get(2), row.get(3));

            let time_span_seconds = time_span_seconds.unwrap_or(300.0).max(1.0);
            let tps = tx_count as f64 / time_span_seconds;
            let failure_rate = if tx_count > 0 {
                (failure_count as f64 / tx_count as f64) * 100.0
            } else {
                0.0
            };

            ProgramTpsEntry {
                program_id: bs58::encode(program_id).into_string(),
                tps,
                tx_count: tx_count as u64,
                failure_rate_percent: failure_rate,
            }
        })
        .collect();

    let total_tx_count = leaderboard.iter().map(|e| e.tx_count).sum();

    Ok(Json(ProgramTpsResponse {
        leaderboard,
        time_window_minutes: 5,
        total_tx_count,
    }))
}
