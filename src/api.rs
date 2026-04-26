use std::sync::Arc;
use tokio::sync::RwLock;

use axum::{routing::{get, post}, Router};

pub mod middleware;
pub mod network_stress;
pub mod rest;
pub mod self_service;
pub mod websocket;

pub type SharedApiState = Arc<RwLock<rest::ApiSnapshot>>;

pub fn router_with_state(state: SharedApiState) -> Router {
    Router::new()
        .route("/health", get(rest::health))
        .route("/metrics", get(rest::metrics))
        .route("/api/network-stress", get(network_stress::get_network_stress))
        .route("/api/protocol-monitor", post(self_service::handle_protocol_request))
        .with_state(state)
}
