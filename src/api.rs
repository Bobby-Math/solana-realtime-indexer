use std::sync::Arc;
use tokio::sync::RwLock;

use axum::{routing::{get, post}, Router};
use axum::response::Html;

pub mod cpi_graph;
pub mod latency_metrics;
pub mod middleware;
pub mod network_stress;
pub mod program_tps;
pub mod rest;
pub mod self_service;
pub mod websocket;

pub type SharedApiState = Arc<RwLock<rest::ApiSnapshot>>;

pub fn router_with_state(state: SharedApiState) -> Router {
    Router::new()
        .route("/health", get(rest::health))
        .route("/metrics", get(rest::metrics))
        .route("/api/network-stress", get(network_stress::get_network_stress))
        .route("/api/latency-metrics", get(latency_metrics::get_latency_metrics))
        .route("/api/cpi-graph", get(cpi_graph::get_cpi_graph))
        .route("/api/program-tps", get(program_tps::get_program_tps))
        .route("/api/protocol-monitor", post(self_service::handle_protocol_request))
        .route("/", get(dashboard_html))
        .with_state(state)
}

async fn dashboard_html() -> Html<&'static str> {
    Html(include_str!("static/dashboard.html"))
}
