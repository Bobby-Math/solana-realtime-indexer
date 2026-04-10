use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct PoolMetrics {
    total_requests: AtomicU64,
    total_failovers: AtomicU64,
    circuit_open_events: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolMetricsSnapshot {
    pub total_requests: u64,
    pub total_failovers: u64,
    pub circuit_open_events: u64,
}

impl PoolMetrics {
    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failover(&self) {
        self.total_failovers.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_circuit_open(&self) {
        self.circuit_open_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_failovers: self.total_failovers.load(Ordering::Relaxed),
            circuit_open_events: self.circuit_open_events.load(Ordering::Relaxed),
        }
    }
}
