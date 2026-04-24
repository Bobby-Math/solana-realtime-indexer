use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    state: CircuitState,
    consecutive_failures: u32,
    open_threshold: u32,
    half_open_after: Duration,
    opened_at: Option<Instant>,
    half_open_probe_in_flight: bool,
}

impl CircuitBreaker {
    pub fn new(open_threshold: u32, half_open_after: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            open_threshold,
            half_open_after,
            opened_at: None,
            half_open_probe_in_flight: false,
        }
    }

    pub fn current_state(&self, now: Instant) -> CircuitState {
        // Check if we should transition to HalfOpen without mutating
        if self.state == CircuitState::Open
            && self
                .opened_at
                .is_some_and(|opened_at| now.saturating_duration_since(opened_at) >= self.half_open_after)
        {
            CircuitState::HalfOpen
        } else {
            self.state
        }
    }

    pub fn sync_state(&mut self, now: Instant) {
        // Explicitly sync state - call this at mutation points
        if self.state == CircuitState::Open
            && self
                .opened_at
                .is_some_and(|opened_at| now.saturating_duration_since(opened_at) >= self.half_open_after)
        {
            self.state = CircuitState::HalfOpen;
            self.half_open_probe_in_flight = false;
        }
    }

    pub fn allow_request(&mut self, now: Instant) -> bool {
        self.sync_state(now); // Explicit sync before checking

        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => {
                if self.half_open_probe_in_flight {
                    false
                } else {
                    self.half_open_probe_in_flight = true;
                    true
                }
            }
        }
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.state = CircuitState::Closed;
        self.opened_at = None;
        self.half_open_probe_in_flight = false;
    }

    pub fn record_failure(&mut self, now: Instant) -> bool {
        self.sync_state(now); // Explicit sync before recording

        match self.state {
            CircuitState::Closed => {
                self.consecutive_failures += 1;
                if self.consecutive_failures >= self.open_threshold {
                    self.trip(now);
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                self.trip(now);
                true
            }
            CircuitState::Open => false,
        }
    }

    fn trip(&mut self, now: Instant) {
        self.state = CircuitState::Open;
        self.opened_at = Some(now);
        self.half_open_probe_in_flight = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{CircuitBreaker, CircuitState};
    use std::time::{Duration, Instant};

    #[test]
    fn opens_after_threshold_failures() {
        let start = Instant::now();
        let mut breaker = CircuitBreaker::new(3, Duration::from_secs(30));

        assert_eq!(breaker.current_state(start), CircuitState::Closed);
        assert!(!breaker.record_failure(start));
        assert!(!breaker.record_failure(start + Duration::from_secs(1)));
        assert!(breaker.record_failure(start + Duration::from_secs(2)));
        assert_eq!(
            breaker.current_state(start + Duration::from_secs(2)),
            CircuitState::Open
        );
    }

    #[test]
    fn transitions_to_half_open_after_cooldown() {
        let start = Instant::now();
        let mut breaker = CircuitBreaker::new(1, Duration::from_secs(10));

        assert!(breaker.record_failure(start));
        assert!(!breaker.allow_request(start + Duration::from_secs(5)));
        assert!(breaker.allow_request(start + Duration::from_secs(10)));
        assert!(!breaker.allow_request(start + Duration::from_secs(10)));
        assert_eq!(
            breaker.current_state(start + Duration::from_secs(10)),
            CircuitState::HalfOpen
        );
    }

    #[test]
    fn half_open_probe_success_closes_breaker() {
        let start = Instant::now();
        let mut breaker = CircuitBreaker::new(1, Duration::from_secs(5));

        assert!(breaker.record_failure(start));
        assert!(breaker.allow_request(start + Duration::from_secs(5)));
        breaker.record_success();

        assert_eq!(
            breaker.current_state(start + Duration::from_secs(5)),
            CircuitState::Closed
        );
        assert!(breaker.allow_request(start + Duration::from_secs(5)));
    }

    #[test]
    fn half_open_probe_failure_reopens_breaker() {
        let start = Instant::now();
        let mut breaker = CircuitBreaker::new(1, Duration::from_secs(5));

        assert!(breaker.record_failure(start));
        assert!(breaker.allow_request(start + Duration::from_secs(5)));
        assert!(breaker.record_failure(start + Duration::from_secs(5)));
        assert_eq!(
            breaker.current_state(start + Duration::from_secs(5)),
            CircuitState::Open
        );
    }
}
