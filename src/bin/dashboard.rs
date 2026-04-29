// Solana Realtime Indexer - Live Dashboard
// Serves three signal panels for investor/client demos

use axum::{
    extract::State,
    response::{Html, Json},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::net::SocketAddr;

// Dashboard configuration
const DASHBOARD_PORT: u16 = 3001;

// Response types for the three signal panels

#[derive(Debug, Serialize, Deserialize)]
struct CpiEdge {
    caller_program: String,
    callee_program: String,
    call_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CpiGraphResponse {
    edges: Vec<CpiEdge>,
    time_window_minutes: i32,
    total_calls: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SlotLatencyPoint {
    timestamp: String,
    avg_latency_ms: f64,
    max_latency_ms: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct NetworkStressResponse {
    data_points: Vec<SlotLatencyPoint>,
    current_avg_latency_ms: f64,
    baseline_avg_ms: f64,
    stress_level: String, // "normal", "elevated", "high"
}

#[derive(Debug, Serialize, Deserialize)]
struct ProgramTpsEntry {
    program_id: String,
    tps: f64,
    tx_count: u64,
    failure_rate_percent: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProgramTpsResponse {
    leaderboard: Vec<ProgramTpsEntry>,
    time_window_minutes: i32,
    total_tx_count: u64,
}

// Error response
#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

// Query functions

/// Query CPI graph edges from transaction_program_ids
/// Shows cross-program call patterns over the last 5 minutes
async fn query_cpi_graph(pool: &PgPool) -> Result<CpiGraphResponse, sqlx::Error> {
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
            -- Infer CPI calls from program co-occurrence patterns
            -- Programs appearing at positions 2+ are likely called by programs at position 1
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
    .fetch_all(pool)
    .await?;

    let edges: Vec<CpiEdge> = rows
        .into_iter()
        .map(|row| {
            let (caller_program, callee_program, call_count): (Vec<u8>, Vec<u8>, i64) =
                (row.get(0), row.get(1), row.get(2));
            CpiEdge {
                caller_program: encode_program_id(caller_program),
                callee_program: encode_program_id(callee_program),
                call_count: call_count as u64,
            }
        })
        .collect();

    let total_calls = edges.iter().map(|e| e.call_count).sum();

    Ok(CpiGraphResponse {
        edges,
        time_window_minutes: 5,
        total_calls,
    })
}

/// Query slot confirmation latency for network stress
/// Shows time from processed -> confirmed status
async fn query_network_stress(pool: &PgPool) -> Result<NetworkStressResponse, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
            time_bucket('10 minutes', s_confirmed.timestamp) AS bucket,
            AVG(EXTRACT(EPOCH FROM
                s_confirmed.timestamp - s_processed.timestamp
            ) * 1000) AS avg_latency_ms,
            MAX(EXTRACT(EPOCH FROM
                s_confirmed.timestamp - s_processed.timestamp
            ) * 1000) AS max_latency_ms
        FROM slots s_confirmed
        JOIN slots s_processed ON s_confirmed.slot = s_processed.slot
        WHERE s_confirmed.status = 'confirmed'
          AND s_processed.status = 'processed'
          AND s_confirmed.timestamp > NOW() - INTERVAL '1 hour'
        GROUP BY bucket
        ORDER BY bucket DESC
        "#
    )
    .fetch_all(pool)
    .await?;

    let data_points: Vec<SlotLatencyPoint> = rows
        .into_iter()
        .filter_map(|row| {
            let bucket: Option<chrono::DateTime<chrono::Utc>> = row.get(0);
            let avg_latency_ms: Option<f64> = row.get(1);
            let max_latency_ms: Option<f64> = row.get(2);

            match bucket {
                Some(ts) => Some(SlotLatencyPoint {
                    timestamp: ts.to_rfc3339(),
                    avg_latency_ms: avg_latency_ms.unwrap_or(0.0),
                    max_latency_ms: max_latency_ms.unwrap_or(0.0),
                }),
                None => None,
            }
        })
        .collect();

    // Calculate 7-day baseline for stress level
    let baseline_row = sqlx::query(
        r#"
        SELECT
            AVG(EXTRACT(EPOCH FROM
                s_confirmed.timestamp - s_processed.timestamp
            ) * 1000) AS baseline_avg_ms
        FROM slots s_confirmed
        JOIN slots s_processed ON s_confirmed.slot = s_processed.slot
        WHERE s_confirmed.status = 'confirmed'
          AND s_processed.status = 'processed'
          AND s_confirmed.timestamp > NOW() - INTERVAL '7 days'
        "#
    )
    .fetch_one(pool)
    .await?;

    let baseline_avg_ms: Option<f64> = baseline_row.get(0);
    let baseline_avg_ms = baseline_avg_ms.unwrap_or(0.0);
    let current_avg_latency_ms = data_points.first()
        .map(|p| p.avg_latency_ms)
        .unwrap_or(0.0);

    // Calculate stress level
    let stress_level = if current_avg_latency_ms > baseline_avg_ms * 2.0 {
        "high"
    } else if current_avg_latency_ms > baseline_avg_ms * 1.5 {
        "elevated"
    } else {
        "normal"
    };

    Ok(NetworkStressResponse {
        data_points,
        current_avg_latency_ms,
        baseline_avg_ms,
        stress_level: stress_level.to_string(),
    })
}

/// Query program TPS leaderboard
/// Shows top programs by transaction rate over last 5 minutes
async fn query_program_tps(pool: &PgPool) -> Result<ProgramTpsResponse, sqlx::Error> {
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
    .fetch_all(pool)
    .await?;

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
                program_id: encode_program_id(program_id),
                tps,
                tx_count: tx_count as u64,
                failure_rate_percent: failure_rate,
            }
        })
        .collect();

    let total_tx_count = leaderboard.iter().map(|e| e.tx_count).sum();

    Ok(ProgramTpsResponse {
        leaderboard,
        time_window_minutes: 5,
        total_tx_count,
    })
}

// Helper function to encode program IDs
fn encode_program_id(bytes: Vec<u8>) -> String {
    bs58::encode(bytes).into_string()
}

// API handlers

async fn get_cpi_graph(
    State(pool): State<PgPool>,
) -> Result<Json<CpiGraphResponse>, Json<ErrorResponse>> {
    match query_cpi_graph(&pool).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => Err(Json(ErrorResponse {
            error: format!("Failed to query CPI graph: {}", e),
        })),
    }
}

async fn get_network_stress(
    State(pool): State<PgPool>,
) -> Result<Json<NetworkStressResponse>, Json<ErrorResponse>> {
    match query_network_stress(&pool).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => Err(Json(ErrorResponse {
            error: format!("Failed to query network stress: {}", e),
        })),
    }
}

async fn get_program_tps(
    State(pool): State<PgPool>,
) -> Result<Json<ProgramTpsResponse>, Json<ErrorResponse>> {
    match query_program_tps(&pool).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => Err(Json(ErrorResponse {
            error: format!("Failed to query program TPS: {}", e),
        })),
    }
}

async fn serve_dashboard() -> Html<&'static str> {
    Html(include_str!("../../src/static/dashboard.html"))
}

// Main function

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    dotenvy::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    log::info!("🚀 Starting Solana Realtime Indexer Dashboard");

    // Get database URL from environment
    let database_url = std::env::var("DASHBOARD_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("DASHBOARD_DATABASE_URL or DATABASE_URL must be set");

    // Create connection pool with read-only user
    let pool = PgPool::connect(&database_url).await?;

    log::info!("✅ Connected to database");

    // Build Axum router
    let app = Router::new()
        .route("/api/cpi-graph", get(get_cpi_graph))
        .route("/api/network-stress", get(get_network_stress))
        .route("/api/program-tps", get(get_program_tps))
        .route("/", get(serve_dashboard))
        .with_state(pool);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], DASHBOARD_PORT));
    log::info!("📊 Dashboard serving on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
