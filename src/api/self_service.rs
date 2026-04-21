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
