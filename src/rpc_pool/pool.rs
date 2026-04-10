use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::rpc_pool::circuit_breaker::{CircuitBreaker, CircuitState};
use crate::rpc_pool::health::{HealthAssessment, HealthPolicy, HealthSnapshot, HealthStatus};
use crate::rpc_pool::metrics::{PoolMetrics, PoolMetricsSnapshot};

#[derive(Debug)]
pub struct RpcPool {
    pub endpoints: Arc<RwLock<Vec<EndpointState>>>,
    pub config: PoolConfig,
    pub metrics: Arc<PoolMetrics>,
}

#[derive(Debug, Clone)]
pub struct EndpointState {
    pub url: String,
    pub weight: u32,
    pub circuit: CircuitBreaker,
    pub health: Option<HealthSnapshot>,
    pub observed_p99_latency_ms: u64,
    pub in_flight: usize,
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub health_check_interval: Duration,
    pub circuit_open_threshold: u32,
    pub circuit_half_open_after: Duration,
    pub request_timeout: Duration,
    pub health_policy: HealthPolicy,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 32,
            health_check_interval: Duration::from_secs(5),
            circuit_open_threshold: 3,
            circuit_half_open_after: Duration::from_secs(30),
            request_timeout: Duration::from_secs(3),
            health_policy: HealthPolicy::default(),
        }
    }
}

impl RpcPool {
    pub fn new(endpoints: Vec<String>, config: PoolConfig) -> Self {
        let states = endpoints
            .into_iter()
            .map(|url| EndpointState {
                url,
                weight: 1,
                circuit: CircuitBreaker::new(
                    config.circuit_open_threshold,
                    config.circuit_half_open_after,
                ),
                health: None,
                observed_p99_latency_ms: 1,
                in_flight: 0,
            })
            .collect();

        Self {
            endpoints: Arc::new(RwLock::new(states)),
            config,
            metrics: Arc::new(PoolMetrics::default()),
        }
    }

    pub fn choose_endpoint(&self) -> Option<String> {
        let mut endpoints = self.endpoints.write().ok()?;
        let index = select_endpoint_index(&mut endpoints, &self.config.health_policy, Instant::now())?;
        Some(endpoints[index].url.clone())
    }

    pub fn begin_request(&self) -> Option<String> {
        self.begin_request_at(Instant::now())
    }

    pub fn apply_health_snapshot(&self, snapshot: HealthSnapshot) -> bool {
        let mut endpoints = match self.endpoints.write() {
            Ok(endpoints) => endpoints,
            Err(_) => return false,
        };

        let Some(endpoint) = endpoints.iter_mut().find(|endpoint| endpoint.url == snapshot.endpoint) else {
            return false;
        };

        if let Some(latency_ms) = snapshot.latency_ms {
            endpoint.update_latency(latency_ms);
        }

        endpoint.health = Some(snapshot);
        true
    }

    pub fn record_success(&self, endpoint_url: &str, latency_ms: u64) -> bool {
        let mut endpoints = match self.endpoints.write() {
            Ok(endpoints) => endpoints,
            Err(_) => return false,
        };

        let Some(endpoint) = endpoints.iter_mut().find(|endpoint| endpoint.url == endpoint_url) else {
            return false;
        };

        endpoint.complete_request();
        endpoint.update_latency(latency_ms);
        endpoint.circuit.record_success();
        true
    }

    pub fn record_failure(&self, endpoint_url: &str) -> bool {
        self.record_failure_at(endpoint_url, Instant::now())
    }

    pub fn metrics_snapshot(&self) -> PoolMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn begin_request_at(&self, now: Instant) -> Option<String> {
        let mut endpoints = self.endpoints.write().ok()?;
        let index = select_endpoint_index(&mut endpoints, &self.config.health_policy, now)?;
        let endpoint = &mut endpoints[index];

        if !endpoint.circuit.allow_request(now) {
            return None;
        }

        endpoint.in_flight += 1;
        self.metrics.record_request();
        Some(endpoint.url.clone())
    }

    fn record_failure_at(&self, endpoint_url: &str, now: Instant) -> bool {
        let mut endpoints = match self.endpoints.write() {
            Ok(endpoints) => endpoints,
            Err(_) => return false,
        };

        let endpoint_count = endpoints.len();
        let Some(endpoint) = endpoints.iter_mut().find(|endpoint| endpoint.url == endpoint_url) else {
            return false;
        };

        endpoint.complete_request();
        if endpoint_count > 1 {
            self.metrics.record_failover();
        }

        if endpoint.circuit.record_failure(now) {
            self.metrics.record_circuit_open();
        }

        true
    }
}

impl EndpointState {
    fn routing_key(&self, assessment: Option<&HealthAssessment>, now: Instant) -> (u8, u64, u64) {
        let circuit_rank = match self.circuit_rank(now) {
            CircuitState::Closed => 0,
            CircuitState::HalfOpen => 1,
            CircuitState::Open => 2,
        };

        let health_rank = match assessment.map(|assessment| assessment.status) {
            Some(HealthStatus::Healthy) => 0,
            Some(HealthStatus::Unknown) | None => 1,
            Some(HealthStatus::Degraded) => 2,
            Some(HealthStatus::Unhealthy) => 3,
        };

        (health_rank, circuit_rank, self.score(assessment))
    }

    fn score(&self, assessment: Option<&HealthAssessment>) -> u64 {
        let latency_ms = assessment
            .and_then(|assessment| assessment.latency_ms)
            .unwrap_or(self.observed_p99_latency_ms)
            .max(1);
        let slot_lag_penalty = assessment
            .and_then(|assessment| assessment.slot_lag)
            .unwrap_or(0)
            .saturating_mul(10);
        let degradation_penalty = match assessment.map(|assessment| assessment.status) {
            Some(HealthStatus::Degraded) => 500,
            Some(HealthStatus::Unhealthy) => u64::MAX / 4,
            _ => 0,
        };
        let load_penalty = (self.in_flight as u64 + 1) * latency_ms;
        let weighted_score = load_penalty
            .saturating_add(slot_lag_penalty)
            .saturating_add(degradation_penalty);

        weighted_score / self.weight.max(1) as u64
    }

    fn update_latency(&mut self, latency_ms: u64) {
        let previous = self.observed_p99_latency_ms.max(1);
        self.observed_p99_latency_ms = ((previous * 7) + latency_ms.max(1)) / 8;
    }

    fn complete_request(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    fn circuit_rank(&self, now: Instant) -> CircuitState {
        let mut circuit = self.circuit.clone();
        circuit.current_state(now)
    }
}

fn newest_observed_slot(endpoints: &[EndpointState]) -> Option<u64> {
    endpoints
        .iter()
        .filter_map(|endpoint| endpoint.health.as_ref()?.latest_slot)
        .max()
}

fn select_endpoint_index(
    endpoints: &mut [EndpointState],
    health_policy: &HealthPolicy,
    now: Instant,
) -> Option<usize> {
    let reference_slot = newest_observed_slot(endpoints);
    let mut selected_index = None;
    let mut selected_key = None;

    for (index, endpoint) in endpoints.iter_mut().enumerate() {
        if endpoint.circuit.current_state(now) == CircuitState::Open {
            continue;
        }

        let assessment = endpoint
            .health
            .as_ref()
            .map(|snapshot| snapshot.assess(reference_slot, health_policy, now));

        if assessment
            .as_ref()
            .is_some_and(|assessment| assessment.status == HealthStatus::Unhealthy)
        {
            continue;
        }

        let key = endpoint.routing_key(assessment.as_ref(), now);
        if selected_key.map_or(true, |current_key| key < current_key) {
            selected_index = Some(index);
            selected_key = Some(key);
        }
    }

    selected_index
}

#[cfg(test)]
mod tests {
    use super::{EndpointState, PoolConfig, RpcPool};
    use crate::rpc_pool::health::{HealthPolicy, HealthSnapshot};
    use std::time::{Duration, Instant};

    #[test]
    fn prefers_healthier_endpoint_even_when_other_is_present() {
        let pool = RpcPool::new(
            vec!["rpc-a".to_string(), "rpc-b".to_string()],
            PoolConfig::default(),
        );
        let now = Instant::now();

        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-a", 1_000, 40, now)));
        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-b", 980, 35, now)));

        let chosen = pool.begin_request_at(now);

        assert_eq!(chosen.as_deref(), Some("rpc-a"));
    }

    #[test]
    fn skips_unhealthy_endpoint_and_uses_alternative() {
        let mut config = PoolConfig::default();
        config.health_policy = HealthPolicy {
            max_slot_lag: 2,
            ..HealthPolicy::default()
        };

        let pool = RpcPool::new(
            vec!["rpc-a".to_string(), "rpc-b".to_string()],
            config,
        );
        let now = Instant::now();

        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-a", 1_000, 40, now)));
        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-b", 995, 35, now)));
        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-a"));

        assert!(pool.apply_health_snapshot(HealthSnapshot::unhealthy("rpc-a", now)));
        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-b"));
    }

    #[test]
    fn failure_opens_circuit_and_updates_metrics() {
        let mut config = PoolConfig::default();
        config.circuit_open_threshold = 1;

        let pool = RpcPool::new(vec!["rpc-a".to_string(), "rpc-b".to_string()], config);
        let now = Instant::now();

        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-a"));
        assert!(pool.record_failure_at("rpc-a", now));

        let metrics = pool.metrics_snapshot();
        assert_eq!(metrics.total_requests, 1);
        assert_eq!(metrics.total_failovers, 1);
        assert_eq!(metrics.circuit_open_events, 1);
    }

    #[test]
    fn success_reduces_in_flight_and_smooths_latency() {
        let pool = RpcPool::new(vec!["rpc-a".to_string()], PoolConfig::default());
        let now = Instant::now();

        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-a"));
        assert!(pool.record_success("rpc-a", 80));

        let endpoints = pool.endpoints.read().expect("pool lock");
        let endpoint: &EndpointState = &endpoints[0];

        assert_eq!(endpoint.in_flight, 0);
        assert!(endpoint.observed_p99_latency_ms > 1);
    }

    #[test]
    fn degraded_endpoint_is_still_used_when_it_is_only_option() {
        let mut config = PoolConfig::default();
        config.health_policy = HealthPolicy {
            max_latency_ms: 10,
            stale_after: Duration::from_secs(60),
            ..HealthPolicy::default()
        };

        let pool = RpcPool::new(vec!["rpc-a".to_string()], config);
        let now = Instant::now();

        assert!(pool.apply_health_snapshot(HealthSnapshot::healthy("rpc-a", 1_000, 40, now)));
        assert_eq!(pool.begin_request_at(now).as_deref(), Some("rpc-a"));
    }
}
