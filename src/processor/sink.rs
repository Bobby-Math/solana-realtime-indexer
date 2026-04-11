use crate::processor::decoder::PersistedBatch;
use crate::processor::sql::{build_write_plan, CheckpointUpdate, SqlWritePlan};
use crate::processor::store::{StoreSnapshot, Type1Store};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::error::Error;
use std::fmt::{Display, Formatter};
use tokio::runtime::{Builder, Runtime};

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

pub trait StorageSink {
    fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError>;
}

#[derive(Debug, Clone)]
pub struct DryRunStorageSink {
    store: Type1Store,
    last_plan: Option<SqlWritePlan>,
}

impl DryRunStorageSink {
    pub fn new(store: Type1Store) -> Self {
        Self {
            store,
            last_plan: None,
        }
    }

    pub fn last_plan(&self) -> Option<&SqlWritePlan> {
        self.last_plan.as_ref()
    }
}

impl StorageSink for DryRunStorageSink {
    fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError> {
        let plan = build_write_plan(batch, &self.store.retention_policy, checkpoint);
        let sql_statements_planned = plan.statement_count() as u64;
        self.last_plan = Some(plan);

        Ok(StorageWriteResult {
            snapshot: self.store.apply_batch(batch.clone()),
            sql_statements_planned,
        })
    }
}

pub struct TimescaleStorageSink {
    store: Type1Store,
    runtime: Runtime,
    pool: PgPool,
    last_plan: Option<SqlWritePlan>,
}

impl TimescaleStorageSink {
    pub fn connect(database_url: &str, store: Type1Store) -> Result<Self, StorageError> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|error| StorageError::RuntimeInit(error.to_string()))?;
        let pool = runtime
            .block_on(
                PgPoolOptions::new()
                    .max_connections(5)
                    .connect(database_url),
            )
            .map_err(|error| StorageError::Connect(error.to_string()))?;

        Ok(Self {
            store,
            runtime,
            pool,
            last_plan: None,
        })
    }

    pub fn last_plan(&self) -> Option<&SqlWritePlan> {
        self.last_plan.as_ref()
    }
}

impl StorageSink for TimescaleStorageSink {
    fn write_batch(
        &mut self,
        batch: &PersistedBatch,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<StorageWriteResult, StorageError> {
        let plan = build_write_plan(batch, &self.store.retention_policy, checkpoint);
        let sql_statements_planned = plan.statement_count() as u64;

        self.runtime
            .block_on(execute_plan(&self.pool, &plan))
            .map_err(|error| StorageError::Execute(error.to_string()))?;
        self.last_plan = Some(plan);

        Ok(StorageWriteResult {
            snapshot: self.store.apply_batch(batch.clone()),
            sql_statements_planned,
        })
    }
}

async fn execute_plan(pool: &PgPool, plan: &SqlWritePlan) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;

    for statement in &plan.statements {
        sqlx::query(statement).execute(&mut *transaction).await?;
    }

    transaction.commit().await
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
    fn dry_run_sink_plans_sql_and_updates_store_snapshot() {
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
        assert_eq!(result.sql_statements_planned, 6);
        assert!(sink
            .last_plan()
            .expect("last plan")
            .statements
            .last()
            .expect("checkpoint statement")
            .contains("INSERT INTO ingestion_checkpoints"));
    }
}
