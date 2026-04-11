#[derive(Debug, Clone)]
pub struct MiddlewareConfig {
    pub rate_limit_per_minute: u64,
    pub request_timeout_secs: u64,
}

impl Default for MiddlewareConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_minute: 100,
            request_timeout_secs: 30,
        }
    }
}
