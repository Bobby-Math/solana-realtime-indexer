#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub gap_threshold_slots: u64,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 250,
            max_backoff_ms: 5_000,
            gap_threshold_slots: 4,
        }
    }
}
