#[derive(Debug, Clone)]
pub struct HealthResponse {
    pub slot_lag: u64,
    pub queue_depth: usize,
    pub rpc_healthy_endpoints: usize,
}
