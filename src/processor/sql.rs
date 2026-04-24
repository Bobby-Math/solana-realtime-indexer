use crate::processor::decoder::PersistedBatch;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};
use crate::processor::store::RetentionPolicy;
use sqlx::Transaction;
use chrono::{TimeZone, Utc};

/// Safely convert Unix milliseconds to DateTime<Utc>, returning a sqlx::Error on out-of-range values.
/// This prevents silent panics from `.unwrap()` in async contexts.
fn to_utc_timestamp(ms: i64) -> Result<chrono::DateTime<Utc>, sqlx::Error> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .ok_or_else(|| {
            sqlx::Error::Protocol(format!("timestamp out of range: {}ms (valid range: -9223372036854775808 to 9223372036854775807)", ms).into())
        })
}

#[derive(Debug, Clone)]
pub struct CheckpointUpdate {
    pub stream_name: String,
    pub last_processed_slot: Option<i64>,
    pub last_observed_at_unix_ms: i64,
    pub notes: Option<String>,
}

/// Executes all batch writes using UNNEST bulk inserts with parameterized queries.
/// This function runs within a transaction and uses PostgreSQL's extended query protocol
/// for maximum performance (plan caching + binary wire format).
pub async fn execute_batch(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    batch: &PersistedBatch,
    retention_policy: &RetentionPolicy,
    checkpoint: Option<&CheckpointUpdate>,
) -> Result<u64, sqlx::Error> {
    let mut statement_count = 0u64;

    if !batch.account_rows.is_empty() {
        execute_account_updates_insert(transaction, &batch.account_rows).await?;
        statement_count += 1;
    }

    if !batch.transaction_rows.is_empty() {
        execute_transactions_insert(transaction, &batch.transaction_rows).await?;
        statement_count += 1;
    }

    if !batch.slot_rows.is_empty() {
        execute_slots_upsert(transaction, &batch.slot_rows).await?;
        statement_count += 1;
    }

    if !batch.custom_rows.is_empty() {
        execute_custom_decoded_insert(transaction, &batch.custom_rows).await?;
        statement_count += 1;
    }

    if let Some(latest_timestamp_unix_ms) = batch.latest_timestamp_unix_ms() {
        statement_count += execute_retention_deletes(transaction, latest_timestamp_unix_ms, retention_policy).await?;
    }

    if let Some(checkpoint) = checkpoint {
        execute_checkpoint_upsert(transaction, checkpoint).await?;
        statement_count += 1;
    }

    Ok(statement_count)
}

async fn execute_account_updates_insert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[AccountUpdateRow],
) -> Result<(), sqlx::Error> {
    let timestamps: Vec<chrono::DateTime<Utc>> = rows
        .iter()
        .map(|row| to_utc_timestamp(row.timestamp_unix_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let pubkeys: Vec<Vec<u8>> = rows.iter().map(|row| row.pubkey.clone()).collect();
    let owners: Vec<Vec<u8>> = rows.iter().map(|row| row.owner.clone()).collect();
    let lamports: Vec<i64> = rows.iter().map(|row| row.lamports).collect();
    let data: Vec<Vec<u8>> = rows.iter().map(|row| row.data.clone()).collect();
    let write_versions: Vec<i64> = rows.iter().map(|row| row.write_version).collect();

    sqlx::query(
        "INSERT INTO account_updates (timestamp, slot, pubkey, owner, lamports, data, write_version)
         SELECT * FROM UNNEST($1::timestamptz[], $2::bigint[], $3::bytea[], $4::bytea[], $5::bigint[], $6::bytea[], $7::bigint[])
         ON CONFLICT (timestamp, slot, pubkey, write_version) DO NOTHING"
    )
    .bind(&timestamps)
    .bind(&slots)
    .bind(&pubkeys)
    .bind(&owners)
    .bind(&lamports)
    .bind(&data)
    .bind(&write_versions)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn execute_transactions_insert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[TransactionRow],
) -> Result<(), sqlx::Error> {
    for row in rows {
        let timestamp = to_utc_timestamp(row.timestamp_unix_ms)?;

        // Decode program_ids from base58 to bytes client-side (PostgreSQL doesn't support base58 decode)
        // Row already has Vec<Vec<u8>>, so we can bind directly as bytea[]
        let program_ids_bytes: Vec<Vec<u8>> = row.program_ids
            .iter()
            .map(|bytes| bytes.clone())
            .collect();

        // Bind log_messages directly as text[] - SQLX handles string arrays natively
        sqlx::query(
            "INSERT INTO transactions (timestamp, slot, signature, fee, success, program_ids, log_messages)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (timestamp, signature) DO NOTHING"
        )
        .bind(timestamp)
        .bind(row.slot)
        .bind(&row.signature)
        .bind(row.fee)
        .bind(row.success)
        .bind(&program_ids_bytes)
        .bind(&row.log_messages)
        .execute(&mut **transaction)
        .await?;
    }

    Ok(())
}

async fn execute_slots_upsert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[SlotRow],
) -> Result<(), sqlx::Error> {
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let parent_slots: Vec<Option<i64>> = rows.iter().map(|row| row.parent_slot).collect();
    let timestamps: Vec<chrono::DateTime<Utc>> = rows
        .iter()
        .map(|row| to_utc_timestamp(row.timestamp_unix_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let statuses: Vec<String> = rows.iter().map(|row| row.status.clone()).collect();

    sqlx::query(
        "INSERT INTO slots (slot, parent_slot, timestamp, status)
         SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::timestamptz[], $4::text[])
         ON CONFLICT (timestamp, slot) DO UPDATE SET
           parent_slot = EXCLUDED.parent_slot,
           status = EXCLUDED.status"
    )
    .bind(&slots)
    .bind(&parent_slots)
    .bind(&timestamps)
    .bind(&statuses)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn execute_custom_decoded_insert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[CustomDecodedRow],
) -> Result<(), sqlx::Error> {
    let timestamps: Vec<chrono::DateTime<Utc>> = rows
        .iter()
        .map(|row| to_utc_timestamp(row.timestamp_unix_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let decoder_names: Vec<String> = rows.iter().map(|row| row.decoder_name.clone()).collect();
    let record_keys: Vec<String> = rows.iter().map(|row| row.record_key.clone()).collect();
    let event_indexes: Vec<i16> = rows.iter().map(|row| row.event_index).collect();
    let payloads: Vec<String> = rows.iter().map(|row| row.payload.clone()).collect();

    sqlx::query(
        "INSERT INTO custom_decoded_events (timestamp, slot, decoder_name, record_key, event_index, payload)
         SELECT * FROM UNNEST($1::timestamptz[], $2::bigint[], $3::text[], $4::text[], $5::smallint[], $6::jsonb[])
         ON CONFLICT (timestamp, decoder_name, record_key, slot, event_index) DO NOTHING"
    )
    .bind(&timestamps)
    .bind(&slots)
    .bind(&decoder_names)
    .bind(&record_keys)
    .bind(&event_indexes)
    .bind(&payloads)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn execute_retention_deletes(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    latest_timestamp_unix_ms: i64,
    retention_policy: &RetentionPolicy,
) -> Result<u64, sqlx::Error> {
    let max_age_ms = i64::try_from(retention_policy.max_age.as_millis())
        .unwrap_or(i64::MAX);
    let cutoff_ms = latest_timestamp_unix_ms.saturating_sub(max_age_ms);
    let cutoff = to_utc_timestamp(cutoff_ms)?;

    let tables = [
        "account_updates",
        "transactions",
        "custom_decoded_events",
        "pipeline_metrics",
        "slots",
    ];

    let mut statement_count = 0u64;
    for table in tables {
        sqlx::query(&format!("DELETE FROM {} WHERE timestamp < $1", table))
            .bind(cutoff)
            .execute(&mut **transaction)
            .await?;
        statement_count += 1;
    }

    Ok(statement_count)
}

async fn execute_checkpoint_upsert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    checkpoint: &CheckpointUpdate,
) -> Result<(), sqlx::Error> {
    let last_observed_at = to_utc_timestamp(checkpoint.last_observed_at_unix_ms)?;

    sqlx::query(
        "INSERT INTO ingestion_checkpoints (stream_name, last_processed_slot, last_observed_at, notes)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (stream_name) DO UPDATE SET
           last_processed_slot = EXCLUDED.last_processed_slot,
           last_observed_at = EXCLUDED.last_observed_at,
           notes = EXCLUDED.notes"
    )
    .bind(&checkpoint.stream_name)
    .bind(checkpoint.last_processed_slot)
    .bind(last_observed_at)
    .bind(&checkpoint.notes)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}



#[cfg(test)]
mod tests {
    // Note: With the migration to parameterized queries, we can no longer test SQL string generation
    // without a database connection. Integration tests should be added to verify the
    // execute_batch function works correctly with a real PostgreSQL instance.
}
