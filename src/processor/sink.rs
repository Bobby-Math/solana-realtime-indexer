use crate::processor::decoder::PersistedBatch;
use crate::processor::sql::{execute_batch, CheckpointUpdate};
use crate::processor::store::{StoreSnapshot, Type1Store};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::error::Error;
use std::fmt::{Display, Formatter};
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Default)]
pub struct StorageWriteResult {
    pub snapshot: StoreSnapshot,
    pub sql_statements_planned: u64,
}

#[derive(Debug)]
pub enum StorageError {
    RuntimeInit(String),
    Connect(String),
    Execute(String),
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::RuntimeInit(message) => {
                write!(formatter, "storage runtime init failed: {message}")
            }
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

pub trait StorageSink: Send {
    fn write_batch(
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

impl StorageSink for DryRunStorageSink {
    fn write_batch(
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

struct PoolWithRuntime {
    pool: PgPool,
    runtime: Runtime,
}

pub struct TimescaleStorageSink {
    store: Type1Store,
    pool: PoolWithRuntime,
    last_statement_count: u64,
}

impl TimescaleStorageSink {
    pub fn connect(database_url: &str, store: Type1Store) -> Result<Self, StorageError> {
        Self::connect_with_pool_size(database_url, store, 20)
    }

    pub fn connect_with_pool_size(database_url: &str, store: Type1Store, max_connections: u32) -> Result<Self, StorageError> {
        // Check if we're already in a runtime
        let in_runtime = tokio::runtime::Handle::try_current().is_ok();

        let (pool, runtime) = if in_runtime {
            // We're in a runtime, create the pool using spawn_blocking
            let database_url = database_url.to_string();
            let (sender, receiver) = std::sync::mpsc::channel();

            std::thread::spawn(move || {
                let rt = Runtime::new().unwrap();
                let pool = rt.block_on(
                    PgPoolOptions::new()
                        .max_connections(max_connections)
                        .min_connections(2)
                        .acquire_timeout(std::time::Duration::from_secs(60))
                        .idle_timeout(std::time::Duration::from_secs(600))
                        .max_lifetime(std::time::Duration::from_secs(1800))
                        .test_before_acquire(true)
                        .connect(&database_url),
                );
                sender.send((pool, rt)).unwrap();
            });

            let (pool, rt) = receiver.recv()
                .map_err(|e| StorageError::Connect(format!("thread communication error: {}", e)))?;
            let pool = pool.map_err(|e| StorageError::Connect(format!("connection error: {}", e)))?;
            (pool, rt)
        } else {
            // Not in a runtime, create directly
            let rt = Runtime::new()
                .map_err(|error| StorageError::RuntimeInit(error.to_string()))?;

            let pool = rt.block_on(
                PgPoolOptions::new()
                    .max_connections(max_connections)
                    .min_connections(2)
                    .acquire_timeout(std::time::Duration::from_secs(60))
                    .idle_timeout(std::time::Duration::from_secs(600))
                    .max_lifetime(std::time::Duration::from_secs(1800))
                    .test_before_acquire(true)
                    .connect(database_url),
            )
                .map_err(|error| StorageError::Connect(error.to_string()))?;

            (pool, rt)
        };

        Ok(Self {
            store,
            pool: PoolWithRuntime { pool, runtime },
            last_statement_count: 0,
        })
    }

    pub fn last_statement_count(&self) -> u64 {
        self.last_statement_count
    }
}

impl StorageSink for TimescaleStorageSink {
    fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError> {
        // Use our dedicated runtime for database operations
        let statement_count = self.pool.runtime.block_on({
            let pool = self.pool.pool.clone();
            let batch = batch.clone();
            let retention_policy = self.store.retention_policy.clone();
            let checkpoint = checkpoint.clone();

            async move {
                let mut transaction = pool.begin().await?;
                let count = execute_batch(&mut transaction, &batch, &retention_policy, checkpoint.as_ref()).await?;
                transaction.commit().await?;
                Ok::<u64, sqlx::Error>(count)
            }
        })
            .map_err(|error| StorageError::Execute(error.to_string()))?;

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

    #[test]
    fn dry_run_sink_estimates_statement_count_and_updates_store_snapshot() {
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
            .expect("dry-run write");

        assert_eq!(result.snapshot.account_rows, 1);
        assert_eq!(result.sql_statements_planned, 6); // 1 account + 1 checkpoint + 4 retention deletes
        assert_eq!(sink.last_statement_count(), 6);
    }
}
