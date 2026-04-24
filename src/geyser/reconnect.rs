/// Reconnection policy for Geyser client
///
/// Defines how the client should behave when connection to Geyser fails.
/// On connection failure, the client will wait with exponential backoff
/// before attempting to reconnect.
///
/// # Example
///
/// ```rust
/// use solana_realtime_indexer::geyser::reconnect::ReconnectPolicy;
///
/// // Default policy: 250ms initial, 5s max, 4 slot gap threshold
/// let policy = ReconnectPolicy::default();
///
/// // Custom policy for slower backoff
/// let custom = ReconnectPolicy {
///     initial_backoff_ms: 500,
///     max_backoff_ms: 10_000,
///     gap_threshold_slots: 10,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Initial backoff delay in milliseconds
    pub initial_backoff_ms: u64,

    /// Maximum backoff delay in milliseconds
    pub max_backoff_ms: u64,

    /// Number of slots to consider as a gap after reconnection
    /// The gap filler will use this to detect missing data
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_policy_has_sensible_defaults() {
        let policy = ReconnectPolicy::default();
        assert_eq!(policy.initial_backoff_ms, 250);
        assert_eq!(policy.max_backoff_ms, 5_000);
        assert_eq!(policy.gap_threshold_slots, 4);
    }

    #[test]
    fn reconnect_policy_can_be_customized() {
        let custom = ReconnectPolicy {
            initial_backoff_ms: 1000,
            max_backoff_ms: 30_000,
            gap_threshold_slots: 100,
        };
        assert_eq!(custom.initial_backoff_ms, 1000);
        assert_eq!(custom.max_backoff_ms, 30_000);
        assert_eq!(custom.gap_threshold_slots, 100);
    }
}
