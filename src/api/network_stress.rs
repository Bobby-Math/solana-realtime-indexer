use axum::{extract::State, Json};
use serde::Serialize;
use sqlx::Row;

use crate::api::SharedApiState;

#[derive(Debug, Clone, Serialize)]
pub struct NetworkStressResponse {
    pub buckets: Vec<SlotHealthBucket>,
    pub slot_skip_rate: f64,
    pub slots_per_second: f64,
    pub skipped_slots_1h: i64,
    pub total_slots_1h: i64,
    pub stress_level: String,
    pub sampled_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SlotHealthBucket {
    pub bucket: String,  // ISO8601 timestamp
    pub slot_count: i64,
    pub skipped_slots: i64,
    pub skip_rate: f64,
    pub slots_per_second: f64,
}

pub async fn get_network_stress(State(state): State<SharedApiState>) -> Json<NetworkStressResponse> {
    // Extract pool clone under lock, then release immediately
    let pool = {
        let snapshot = state.read().await;
        snapshot.pool.clone()
    }; // Lock released here

    // Get the database pool from the snapshot if available
    let response = if let Some(ref pool) = pool {
        // Query the last 30 buckets from the slot health materialized view
        let query_result = sqlx::query(
            r#"
            SELECT
                bucket,
                slot_count,
                skipped_slots,
                skip_rate,
                slots_per_second
            FROM slot_health_1m
            ORDER BY bucket DESC
            LIMIT 30
            "#
        )
        .fetch_all(pool)
        .await;

        match query_result {
            Ok(rows) if !rows.is_empty() => {
                let buckets: Vec<SlotHealthBucket> = rows
                    .iter()
                    .map(|row| {
                        let bucket: chrono::DateTime<chrono::Utc> = row.get("bucket");
                        let slot_count: Option<i64> = row.get("slot_count");
                        let skipped_slots: Option<i64> = row.get("skipped_slots");
                        let skip_rate: Option<f64> = row.get("skip_rate");
                        let slots_per_second: Option<f64> = row.get("slots_per_second");

                        SlotHealthBucket {
                            bucket: bucket.to_rfc3339(),
                            slot_count: slot_count.unwrap_or(0),
                            skipped_slots: skipped_slots.unwrap_or(0),
                            skip_rate: skip_rate.unwrap_or(0.0),
                            slots_per_second: slots_per_second.unwrap_or(0.0),
                        }
                    })
                    .collect();

                // Get most recent bucket's metrics for stress level calculation
                let most_recent = &buckets[0];
                let current_skip_rate = most_recent.skip_rate;
                let current_slots_per_sec = most_recent.slots_per_second;
                let stress_level = calculate_stress_level(current_skip_rate);

                // Calculate 1-hour totals from all buckets
                let total_slots_1h: i64 = buckets.iter().map(|b| b.slot_count).sum();
                let skipped_slots_1h: i64 = buckets.iter().map(|b| b.skipped_slots).sum();

                NetworkStressResponse {
                    buckets,
                    slot_skip_rate: current_skip_rate,
                    slots_per_second: current_slots_per_sec,
                    skipped_slots_1h,
                    total_slots_1h,
                    stress_level,
                    sampled_at: chrono::Utc::now().to_rfc3339(),
                }
            }
            Ok(_) => {
                // No rows yet - return default response
                empty_stress_response()
            }
            Err(e) => {
                log::error!("Failed to query slot_health_1m: {}", e);
                empty_stress_response()
            }
        }
    } else {
        // No database pool - running in dry-run mode
        empty_stress_response()
    };

    Json(response)
}

fn empty_stress_response() -> NetworkStressResponse {
    NetworkStressResponse {
        buckets: vec![],
        slot_skip_rate: 0.0,
        slots_per_second: 0.0,
        skipped_slots_1h: 0,
        total_slots_1h: 0,
        stress_level: "normal".to_string(),
        sampled_at: chrono::Utc::now().to_rfc3339(),
    }
}

fn calculate_stress_level(skip_rate: f64) -> String {
    if skip_rate < 0.02 {
        "normal".to_string()
    } else if skip_rate < 0.05 {
        "elevated".to_string()
    } else if skip_rate < 0.10 {
        "high".to_string()
    } else {
        "critical".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stress_level_calculation() {
        assert_eq!(calculate_stress_level(0.01), "normal");    // 1% skip rate
        assert_eq!(calculate_stress_level(0.02), "elevated"); // 2% skip rate
        assert_eq!(calculate_stress_level(0.04), "elevated"); // 4% skip rate
        assert_eq!(calculate_stress_level(0.05), "high");     // 5% skip rate
        assert_eq!(calculate_stress_level(0.09), "high");     // 9% skip rate
        assert_eq!(calculate_stress_level(0.10), "critical"); // 10% skip rate
        assert_eq!(calculate_stress_level(0.15), "critical"); // 15% skip rate
    }

    #[test]
    fn test_empty_stress_response_returns_defaults() {
        let response = empty_stress_response();
        assert_eq!(response.buckets.len(), 0);
        assert_eq!(response.slot_skip_rate, 0.0);
        assert_eq!(response.slots_per_second, 0.0);
        assert_eq!(response.skipped_slots_1h, 0);
        assert_eq!(response.total_slots_1h, 0);
        assert_eq!(response.stress_level, "normal");
    }
}
