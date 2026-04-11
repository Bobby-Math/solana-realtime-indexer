use std::time::Duration;

use crate::processor::decoder::PersistedBatch;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub max_age: Duration,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(60 * 60),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StoreMetrics {
    pub account_rows_pruned: u64,
    pub transaction_rows_pruned: u64,
    pub slot_rows_pruned: u64,
    pub custom_rows_pruned: u64,
}

#[derive(Debug, Clone, Default)]
pub struct StoreSnapshot {
    pub account_rows: usize,
    pub transaction_rows: usize,
    pub slot_rows: usize,
    pub custom_rows: usize,
    pub metrics: StoreMetrics,
}

#[derive(Debug, Clone)]
pub struct Type1Store {
    pub retention_policy: RetentionPolicy,
    pub account_rows: Vec<AccountUpdateRow>,
    pub transaction_rows: Vec<TransactionRow>,
    pub slot_rows: Vec<SlotRow>,
    pub custom_rows: Vec<CustomDecodedRow>,
    pub metrics: StoreMetrics,
}

impl Type1Store {
    pub fn new(retention_policy: RetentionPolicy) -> Self {
        Self {
            retention_policy,
            account_rows: Vec::new(),
            transaction_rows: Vec::new(),
            slot_rows: Vec::new(),
            custom_rows: Vec::new(),
            metrics: StoreMetrics::default(),
        }
    }

    pub fn apply_batch(&mut self, batch: PersistedBatch) -> StoreSnapshot {
        self.account_rows.extend(batch.account_rows);
        self.transaction_rows.extend(batch.transaction_rows);
        self.slot_rows.extend(batch.slot_rows);
        self.custom_rows.extend(batch.custom_rows);

        if let Some(latest_timestamp_unix_ms) = self.latest_timestamp_unix_ms() {
            self.prune_stale(latest_timestamp_unix_ms);
        }

        self.snapshot()
    }

    pub fn snapshot(&self) -> StoreSnapshot {
        StoreSnapshot {
            account_rows: self.account_rows.len(),
            transaction_rows: self.transaction_rows.len(),
            slot_rows: self.slot_rows.len(),
            custom_rows: self.custom_rows.len(),
            metrics: self.metrics.clone(),
        }
    }

    fn latest_timestamp_unix_ms(&self) -> Option<i64> {
        self.account_rows
            .iter()
            .map(|row| row.timestamp_unix_ms)
            .chain(
                self.transaction_rows
                    .iter()
                    .map(|row| row.timestamp_unix_ms),
            )
            .chain(self.slot_rows.iter().map(|row| row.timestamp_unix_ms))
            .chain(self.custom_rows.iter().map(|row| row.timestamp_unix_ms))
            .max()
    }

    fn prune_stale(&mut self, latest_timestamp_unix_ms: i64) {
        let cutoff = latest_timestamp_unix_ms - self.retention_policy.max_age.as_millis() as i64;

        prune_with_metrics(
            &mut self.account_rows,
            cutoff,
            &mut self.metrics.account_rows_pruned,
            |row| row.timestamp_unix_ms,
        );
        prune_with_metrics(
            &mut self.transaction_rows,
            cutoff,
            &mut self.metrics.transaction_rows_pruned,
            |row| row.timestamp_unix_ms,
        );
        prune_with_metrics(
            &mut self.slot_rows,
            cutoff,
            &mut self.metrics.slot_rows_pruned,
            |row| row.timestamp_unix_ms,
        );
        prune_with_metrics(
            &mut self.custom_rows,
            cutoff,
            &mut self.metrics.custom_rows_pruned,
            |row| row.timestamp_unix_ms,
        );
    }
}

fn prune_with_metrics<T, F>(rows: &mut Vec<T>, cutoff: i64, metric: &mut u64, timestamp: F)
where
    F: Fn(&T) -> i64,
{
    let before = rows.len();
    rows.retain(|row| timestamp(row) >= cutoff);
    *metric += (before - rows.len()) as u64;
}

#[cfg(test)]
mod tests {
    use super::{RetentionPolicy, Type1Store};
    use crate::processor::batch_writer::FlushReason;
    use crate::processor::decoder::PersistedBatch;
    use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};
    use std::time::Duration;

    #[test]
    fn prunes_rows_older_than_retention_window() {
        let mut store = Type1Store::new(RetentionPolicy {
            max_age: Duration::from_millis(20),
        });

        let first_batch = PersistedBatch {
            reason: FlushReason::Size,
            account_rows: vec![AccountUpdateRow {
                slot: 1,
                timestamp_unix_ms: 100,
                pubkey: vec![1],
                owner: vec![2],
                lamports: 1,
                data: vec![3],
                write_version: 1,
            }],
            transaction_rows: vec![],
            slot_rows: vec![],
            custom_rows: vec![],
        };
        store.apply_batch(first_batch);

        let second_batch = PersistedBatch {
            reason: FlushReason::Interval,
            account_rows: vec![],
            transaction_rows: vec![TransactionRow {
                slot: 2,
                timestamp_unix_ms: 125,
                signature: vec![4],
                fee: 5,
                success: true,
                program_ids: vec![vec![9]],
                log_messages: vec!["ok".to_string()],
            }],
            slot_rows: vec![SlotRow {
                slot: 2,
                timestamp_unix_ms: 125,
                parent_slot: Some(1),
                status: "processed".to_string(),
            }],
            custom_rows: vec![CustomDecodedRow {
                decoder_name: "program-activity".to_string(),
                record_key: "tracked".to_string(),
                slot: 2,
                timestamp_unix_ms: 125,
                payload: "transaction".to_string(),
            }],
        };

        let snapshot = store.apply_batch(second_batch);

        assert_eq!(snapshot.account_rows, 0);
        assert_eq!(snapshot.transaction_rows, 1);
        assert_eq!(snapshot.slot_rows, 1);
        assert_eq!(snapshot.custom_rows, 1);
        assert_eq!(snapshot.metrics.account_rows_pruned, 1);
    }
}
