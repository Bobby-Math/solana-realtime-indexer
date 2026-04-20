use std::sync::Arc;

use axum::{routing::get, Router};

pub mod middleware;
pub mod rest;
pub mod self_service;
pub mod websocket;

pub type SharedApiState = Arc<rest::ApiSnapshot>;

pub fn router(snapshot: rest::ApiSnapshot) -> Router {
    Router::new()
        .route("/health", get(rest::health))
        .route("/metrics", get(rest::metrics))
        .with_state(Arc::new(snapshot))
}
