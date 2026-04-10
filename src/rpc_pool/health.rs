use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HealthStatus {
    Healthy,
    Unknown,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone)]
pub struct HealthPolicy {
    pub max_slot_lag: u64,
    pub max_latency_ms: u64,
    pub stale_after: Duration,
}

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub endpoint: String,
    pub probe_success: bool,
    pub latest_slot: Option<u64>,
    pub latency_ms: Option<u64>,
    pub observed_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthAssessment {
    pub status: HealthStatus,
    pub slot_lag: Option<u64>,
    pub latency_ms: Option<u64>,
    pub is_stale: bool,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            max_slot_lag: 8,
            max_latency_ms: 1_500,
            stale_after: Duration::from_secs(15),
        }
    }
}

impl HealthSnapshot {
    pub fn healthy(
        endpoint: impl Into<String>,
        latest_slot: u64,
        latency_ms: u64,
        observed_at: Instant,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            probe_success: true,
            latest_slot: Some(latest_slot),
            latency_ms: Some(latency_ms),
            observed_at,
        }
    }

    pub fn unhealthy(endpoint: impl Into<String>, observed_at: Instant) -> Self {
        Self {
            endpoint: endpoint.into(),
            probe_success: false,
            latest_slot: None,
            latency_ms: None,
            observed_at,
        }
    }

    pub fn assess(
        &self,
        reference_slot: Option<u64>,
        policy: &HealthPolicy,
        now: Instant,
    ) -> HealthAssessment {
        let is_stale = now.saturating_duration_since(self.observed_at) > policy.stale_after;
        let slot_lag = match (reference_slot, self.latest_slot) {
            (Some(reference_slot), Some(latest_slot)) => Some(reference_slot.saturating_sub(latest_slot)),
            _ => None,
        };

        let status = if !self.probe_success || is_stale {
            HealthStatus::Unhealthy
        } else if self.latest_slot.is_none() && self.latency_ms.is_none() {
            HealthStatus::Unknown
        } else if slot_lag.is_some_and(|lag| lag > policy.max_slot_lag)
            || self.latency_ms.is_some_and(|latency_ms| latency_ms > policy.max_latency_ms)
        {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        HealthAssessment {
            status,
            slot_lag,
            latency_ms: self.latency_ms,
            is_stale,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HealthPolicy, HealthSnapshot, HealthStatus};
    use std::time::{Duration, Instant};

    #[test]
    fn marks_snapshot_degraded_when_slot_lag_exceeds_policy() {
        let now = Instant::now();
        let snapshot = HealthSnapshot::healthy("rpc-a", 100, 50, now);
        let policy = HealthPolicy {
            max_slot_lag: 2,
            ..HealthPolicy::default()
        };

        let assessment = snapshot.assess(Some(104), &policy, now);

        assert_eq!(assessment.status, HealthStatus::Degraded);
        assert_eq!(assessment.slot_lag, Some(4));
    }

    #[test]
    fn marks_snapshot_unhealthy_when_stale() {
        let now = Instant::now();
        let snapshot = HealthSnapshot::healthy("rpc-a", 100, 50, now);
        let policy = HealthPolicy {
            stale_after: Duration::from_secs(5),
            ..HealthPolicy::default()
        };

        let assessment = snapshot.assess(Some(100), &policy, now + Duration::from_secs(6));

        assert_eq!(assessment.status, HealthStatus::Unhealthy);
        assert!(assessment.is_stale);
    }
}
