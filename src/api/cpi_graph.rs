use axum::{extract::State, Json};
use serde::Serialize;
use sqlx::Row;

use crate::api::SharedApiState;

#[derive(Debug, Clone, Serialize)]
pub struct CpiEdge {
    pub caller_program: String,
    pub callee_program: String,
    pub call_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CpiGraphResponse {
    pub edges: Vec<CpiEdge>,
    pub time_window_minutes: i32,
    pub total_calls: u64,
}

pub async fn get_cpi_graph(
    State(state): State<SharedApiState>,
) -> Result<Json<CpiGraphResponse>, Json<&'static str>> {
    let pool = {
        let snapshot = state.read().await;
        snapshot.pool.clone()
    };

    let pool = pool.ok_or("Database pool not available")?;

    let rows = sqlx::query(
        r#"
        WITH tx_programs AS (
            SELECT
                t.signature,
                tp.program_id,
                tp.position
            FROM transactions t
            JOIN transaction_program_ids tp ON t.timestamp = tp.timestamp AND t.signature = tp.signature
            WHERE t.timestamp > NOW() - INTERVAL '5 minutes'
        ),
        cpi_inference AS (
            SELECT
                caller.program_id AS caller_program,
                callee.program_id AS callee_program,
                COUNT(*) AS call_count
            FROM tx_programs caller
            JOIN tx_programs callee ON caller.signature = callee.signature
            WHERE caller.position = 1
              AND callee.position > 1
            GROUP BY caller.program_id, callee.program_id
        )
        SELECT
            caller_program,
            callee_program,
            call_count
        FROM cpi_inference
        ORDER BY call_count DESC
        LIMIT 50
        "#
    )
    .fetch_all(&pool)
    .await
    .map_err(|_| "Failed to query CPI graph")?;

    let edges: Vec<CpiEdge> = rows
        .into_iter()
        .map(|row| {
            let (caller_program, callee_program, call_count): (Vec<u8>, Vec<u8>, i64) =
                (row.get(0), row.get(1), row.get(2));
            CpiEdge {
                caller_program: bs58::encode(caller_program).into_string(),
                callee_program: bs58::encode(callee_program).into_string(),
                call_count: call_count as u64,
            }
        })
        .collect();

    let total_calls = edges.iter().map(|e| e.call_count).sum();

    Ok(Json(CpiGraphResponse {
        edges,
        time_window_minutes: 5,
        total_calls,
    }))
}
