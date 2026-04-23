pub mod api;
pub mod config;
pub mod geyser;
pub mod processor;
pub mod rpc_pool;

pub use api::rest::ApiSnapshot;

pub const PROJECT_NAME: &str = "solana-realtime-indexer";
