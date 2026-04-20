// Self-Service API - Handle web form submissions and generate live dashboards
use axum::{extract::State, Json, web::Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

// Handle form submission
pub async fn handle_protocol_request(
    State(state): State<SelfServiceState>,
    Json(request): Json<ProtocolRequest>,
) -> Json<ProtocolDashboard> {
    log::info!("Received protocol request from: {}", request.email);

    // Create personalized dashboard
    let dashboard = create_personalized_dashboard(request).await;

    // In production, you would:
    // 1. Validate the wallet address
    // 2. Set up monitoring for their protocol
    // 3. Configure competitor tracking
    // 4. Create database schema for their data
    // 5. Send email with dashboard link

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

    // Generate mock metrics (in production, this would be real data)
    let metrics = ProtocolMetrics {
        total_value_locked: 12.4,
        active_users: 147,
        health_score: 94.0,
        transactions_per_second: 1847.0,
        revenue_24h: 3240.0,
    };

    // Generate alerts based on their input
    let alerts = generate_personalized_alerts(&request.protocol_name, &competitors);

    ProtocolDashboard {
        protocol_name: request.protocol_name.clone(),
        wallet_address: request.wallet_address,
        competitors: competitors.clone(),
        dashboard_url: format!("https://your-dashboard.com/{}", dashboard_id),
        metrics,
        alerts,
        created_at: chrono::Utc::now().to_rfc3339(),
    }
}

fn generate_personalized_alerts(protocol_name: &str, competitors: &[String]) -> Vec<ProtocolAlert> {
    let mut alerts = vec![
        ProtocolAlert {
            severity: "HIGH".to_string(),
            title: "🚨 Large Transaction Detected".to_string(),
            message: format!("Wallet 0x123... moved 500K USDC - partnership opportunity for {}?", protocol_name),
            action_required: true,
            timestamp: chrono::Utc::now().to_rfc3339(),
        },
        ProtocolAlert {
            severity: "MEDIUM".to_string(),
            title: "🐋 Whale Accumulation".to_string(),
            message: format!("Wallet 0x456... accumulated 50K of {} tokens - increased interest?", protocol_name),
            action_required: true,
            timestamp: chrono::Utc::now().to_rfc3339(),
        },
    ];

    // Add competitor-specific alerts
    for competitor in competitors {
        alerts.push(ProtocolAlert {
            severity: "INFO".to_string(),
            title: format!("🕵️ {} Activity Detected".to_string(), competitor),
            message: format!("{} made significant treasury movement - what are they planning?", competitor),
            action_required: true,
            timestamp: chrono::Utc::now().to_rfc3339(),
        });
    }

    alerts
}

#[derive(Clone)]
pub struct SelfServiceState {
    pub db_pool: Option<sqlx::PgPool>,
}

// Register the route
pub fn register_self_service_routes(router: axum::Router) -> axum::Router {
    router.route("/api/protocol-monitor", axum::routing::post(handle_protocol_request))
}

// Example usage in main.rs:
/*
use axum::{Router, routing::get};
use solana_realtime_indexer::api::self_service::{register_self_service_routes, SelfServiceState};

let app = Router::new()
    .merge(api::router(api_snapshot))
    .merge(register_self_service_routes(Router::new()))
    .with_state(Arc::new(api_snapshot));
*/