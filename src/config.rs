use std::env;

use crate::geyser::consumer::SubscriptionFilter;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_address: String,
    pub rpc_endpoints: Vec<String>,
    pub geyser_endpoint: Option<String>,
    pub geyser_api_key: Option<String>,
    pub geyser_program_filters: Vec<String>,
    pub geyser_account_filters: Vec<String>,
    pub geyser_include_slots: bool,
    pub geyser_run_duration_seconds: Option<u64>,
    pub database_url: Option<String>,
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            bind_address: env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
            rpc_endpoints: read_csv_env("RPC_ENDPOINTS"),
            geyser_endpoint: env::var("GEYSER_ENDPOINT").ok(),
            geyser_api_key: env::var("GEYSER_API_KEY").ok(),
            geyser_program_filters: read_csv_env("GEYSER_PROGRAM_FILTERS"),
            geyser_account_filters: read_csv_env("GEYSER_ACCOUNT_FILTERS"),
            geyser_include_slots: read_bool_env("GEYSER_INCLUDE_SLOTS").unwrap_or(false),
            geyser_run_duration_seconds: env::var("GEYSER_RUN_DURATION_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok()),
            database_url: env::var("DATABASE_URL").ok(),
            log_level: env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
        }
    }

    pub fn geyser_subscription_filters(&self) -> Vec<SubscriptionFilter> {
        let mut filters = Vec::new();

        filters.extend(
            self.geyser_program_filters
                .iter()
                .cloned()
                .map(SubscriptionFilter::Program),
        );
        filters.extend(
            self.geyser_account_filters
                .iter()
                .cloned()
                .map(SubscriptionFilter::Account),
        );

        if self.geyser_include_slots {
            filters.push(SubscriptionFilter::Slots);
        }

        filters
    }
}

fn read_csv_env(key: &str) -> Vec<String> {
    env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn read_bool_env(key: &str) -> Option<bool> {
    env::var(key).ok().and_then(|value| parse_bool(&value))
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_bool;

    #[test]
    fn parses_bool_env_values() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bool("not-bool"), None);
    }
}
