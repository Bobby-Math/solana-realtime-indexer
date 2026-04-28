use crate::processor::decoder::PersistedBatch;
use crate::processor::sanitize::sanitize_log_message;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};
use crate::processor::store::RetentionPolicy;
use chrono::{TimeZone, Utc};
use sqlx::Transaction;

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
    _retention_policy: &RetentionPolicy,
    checkpoint: Option<&CheckpointUpdate>,
) -> Result<u64, sqlx::Error> {
    let mut statement_count = 0u64;

    if !batch.account_rows.is_empty() {
        execute_account_updates_insert(transaction, &batch.account_rows).await?;
        statement_count += 1;
    }

    if !batch.transaction_rows.is_empty() {
        let pid_count = execute_transactions_insert(transaction, &batch.transaction_rows).await?;
        statement_count += 1;
        if pid_count > 0 {
            statement_count += 1;
        }
    }

    if !batch.slot_rows.is_empty() {
        execute_slots_upsert(transaction, &batch.slot_rows).await?;
        statement_count += 1;
    }

    if !batch.custom_rows.is_empty() {
        execute_custom_decoded_insert(transaction, &batch.custom_rows).await?;
        statement_count += 1;
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
    let pubkeys: Vec<&[u8]> = rows.iter().map(|row| row.pubkey.as_ref()).collect();
    let owners: Vec<&[u8]> = rows.iter().map(|row| row.owner.as_ref()).collect();
    let lamports: Vec<i64> = rows.iter().map(|row| row.lamports).collect();
    let data: Vec<&[u8]> = rows.iter().map(|row| row.data.as_ref()).collect();
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

pub async fn execute_transactions_insert(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    rows: &[TransactionRow],
) -> Result<usize, sqlx::Error> {
    // ── Part 1: flat transaction rows (no program_ids) ─────────────────────────
    let timestamps: Vec<chrono::DateTime<Utc>> = rows
        .iter()
        .map(|row| to_utc_timestamp(row.timestamp_unix_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let signatures: Vec<&[u8]> = rows.iter().map(|row| row.signature.as_ref()).collect();
    let fees: Vec<i64> = rows.iter().map(|row| row.fee).collect();
    let successes: Vec<bool> = rows.iter().map(|row| row.success).collect();
    let log_messages_json: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            serde_json::Value::Array(
                row.log_messages
                    .iter()
                    .map(|msg| serde_json::Value::String(sanitize_log_message(msg)))
                    .collect(),
            )
        })
        .collect();

    sqlx::query(
        "INSERT INTO transactions (timestamp, slot, signature, fee, success, log_messages)
         SELECT * FROM UNNEST(
             $1::timestamptz[], $2::bigint[], $3::bytea[],
             $4::bigint[],      $5::bool[],   $6::jsonb[]
         )
         ON CONFLICT (timestamp, signature) DO NOTHING",
    )
    .bind(&timestamps)
    .bind(&slots)
    .bind(&signatures)
    .bind(&fees)
    .bind(&successes)
    .bind(&log_messages_json)
    .execute(&mut **transaction)
    .await?;

    // ── Part 2: exploded program_id rows (one per program_id) ───────────────────
    let total_pids: usize = rows.iter().map(|row| row.program_ids.len()).sum();
    if total_pids == 0 {
        return Ok(0);
    }

    let mut pid_timestamps: Vec<chrono::DateTime<Utc>> = Vec::with_capacity(total_pids);
    let mut pid_signatures: Vec<[u8; 64]> = Vec::with_capacity(total_pids);
    let mut pid_program_ids: Vec<&[u8]> = Vec::with_capacity(total_pids);
    let mut pid_positions: Vec<i16> = Vec::with_capacity(total_pids);

    for row in rows {
        let ts = to_utc_timestamp(row.timestamp_unix_ms)?;
        for (pos, program_id) in row.program_ids.iter().enumerate() {
            pid_timestamps.push(ts);
            pid_signatures.push(row.signature);
            pid_program_ids.push(program_id.as_ref());
            pid_positions.push(pos as i16);
        }
    }

    let pid_signature_slices: Vec<&[u8]> = pid_signatures
        .iter()
        .map(|signature| signature.as_ref())
        .collect();

    sqlx::query(
        "INSERT INTO transaction_program_ids (timestamp, signature, program_id, position)
         SELECT * FROM UNNEST(
             $1::timestamptz[], $2::bytea[], $3::bytea[], $4::smallint[]
         )
         ON CONFLICT DO NOTHING",
    )
    .bind(&pid_timestamps)
    .bind(&pid_signature_slices)
    .bind(&pid_program_ids)
    .bind(&pid_positions)
    .execute(&mut **transaction)
    .await?;

    Ok(total_pids)
}

pub async fn execute_slots_upsert(
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
           status = EXCLUDED.status",
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
    // Convert serde_json::Value to JSONB for PostgreSQL
    // This is safe because serde_json::Value is guaranteed to be valid JSON
    let payloads: Vec<serde_json::Value> = rows.iter().map(|row| row.payload.clone()).collect();

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
