use crate::processor::decoder::PersistedBatch;
use crate::processor::sql::{execute_batch, CheckpointUpdate};
use crate::processor::store::{StoreSnapshot, Type1Store};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::error::Error;
use std::fmt::{Display, Formatter};
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct StorageWriteResult {
    pub snapshot: StoreSnapshot,
    pub sql_statements_planned: u64,
}

#[derive(Debug)]
pub enum StorageError {
    Connect(String),
    Execute(String),
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Connect(message) => {
                write!(formatter, "timescale connection failed: {message}")
            }
            StorageError::Execute(message) => {
                write!(formatter, "timescale write failed: {message}")
            }
        }
    }
}

impl Error for StorageError {}

#[async_trait]
pub trait StorageSink: Send {
    async fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError>;
}

#[derive(Debug, Clone)]
pub struct DryRunStorageSink {
    store: Type1Store,
    last_statement_count: u64,
}

impl DryRunStorageSink {
    pub fn new(store: Type1Store) -> Self {
        Self {
            store,
            last_statement_count: 0,
        }
    }

    pub fn last_statement_count(&self) -> u64 {
        self.last_statement_count
    }
}

#[async_trait]
impl StorageSink for DryRunStorageSink {
    async fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError> {
        // Estimate statement count (same logic as execute_batch)
        let mut statement_count = 0u64;
        if !batch.account_rows.is_empty() { statement_count += 1; }
        if !batch.transaction_rows.is_empty() { statement_count += 1; }
        if !batch.slot_rows.is_empty() { statement_count += 1; }
        if !batch.custom_rows.is_empty() { statement_count += 1; }
        if batch.latest_timestamp_unix_ms().is_some() { statement_count += 4; } // retention deletes for 4 tables
        if checkpoint.is_some() { statement_count += 1; }

        self.last_statement_count = statement_count;

        Ok(StorageWriteResult {
            snapshot: self.store.apply_batch(batch.clone()),
            sql_statements_planned: statement_count,
        })
    }
}

pub struct TimescaleStorageSink {
    store: Type1Store,
    pool: PgPool,
    last_statement_count: u64,
}

impl TimescaleStorageSink {
    pub async fn connect(database_url: &str, store: Type1Store) -> Result<Self, StorageError> {
        Self::connect_with_pool_size(database_url, store, 20).await
    }

    pub async fn connect_with_pool_size(database_url: &str, store: Type1Store, max_connections: u32) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(2)
            .acquire_timeout(std::time::Duration::from_secs(60))
            .idle_timeout(std::time::Duration::from_secs(600))
            .max_lifetime(std::time::Duration::from_secs(1800))
            .test_before_acquire(true)
            .connect(database_url)
            .await
            .map_err(|error| StorageError::Connect(error.to_string()))?;

        Ok(Self {
            store,
            pool,
            last_statement_count: 0,
        })
    }

    pub fn with_pool(pool: PgPool, store: Type1Store) -> Self {
        Self {
            store,
            pool,
            last_statement_count: 0,
        }
    }

    pub fn last_statement_count(&self) -> u64 {
        self.last_statement_count
    }
}

#[async_trait]
impl StorageSink for TimescaleStorageSink {
    async fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError> {
        let statement_count = {
            let pool = self.pool.clone();
            let batch = batch.clone();
            let retention_policy = self.store.retention_policy.clone();
            let checkpoint = checkpoint.clone();

            let mut transaction = pool.begin().await
                .map_err(|error| StorageError::Execute(error.to_string()))?;
            let count = execute_batch(&mut transaction, &batch, &retention_policy, checkpoint.as_ref()).await
                .map_err(|error| StorageError::Execute(error.to_string()))?;
            transaction.commit().await
                .map_err(|error| StorageError::Execute(error.to_string()))?;
            count
        };

        self.last_statement_count = statement_count;
        let snapshot = self.store.apply_batch(batch.clone());

        Ok(StorageWriteResult {
            snapshot,
            sql_statements_planned: statement_count,
        })
    }
}


#[cfg(test)]
mod tests {
    use super::{DryRunStorageSink, StorageSink};
    use crate::processor::batch_writer::FlushReason;
    use crate::processor::decoder::PersistedBatch;
    use crate::processor::schema::AccountUpdateRow;
    use crate::processor::sql::CheckpointUpdate;
    use crate::processor::store::{RetentionPolicy, Type1Store};
    use std::time::Duration;

    #[tokio::test]
    async fn dry_run_sink_estimates_statement_count_and_updates_store_snapshot() {
        let mut sink = DryRunStorageSink::new(Type1Store::new(RetentionPolicy {
            max_age: Duration::from_secs(60),
        }));
        let batch = PersistedBatch {
            reason: FlushReason::Size,
            account_rows: vec![AccountUpdateRow {
                slot: 10,
                timestamp_unix_ms: 1_710_000_000_000,
                pubkey: vec![1],
                owner: vec![2],
                lamports: 5,
                data: vec![3],
                write_version: 1,
            }],
            transaction_rows: vec![],
            slot_rows: vec![],
            custom_rows: vec![],
        };

        let result = sink
            .write_batch(
                &batch,
                Some(CheckpointUpdate {
                    stream_name: "geyser-main".to_string(),
                    last_processed_slot: Some(10),
                    last_observed_at_unix_ms: 1_710_000_000_000,
                    notes: None,
                }),
            )
            .await
            .expect("dry-run write");

        assert_eq!(result.snapshot.account_rows, 1);
        assert_eq!(result.sql_statements_planned, 6); // 1 account + 1 checkpoint + 4 retention deletes
        assert_eq!(sink.last_statement_count(), 6);
    }
}
