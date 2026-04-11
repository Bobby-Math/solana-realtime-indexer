use crate::processor::decoder::PersistedBatch;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};
use crate::processor::store::RetentionPolicy;

#[derive(Debug, Clone)]
pub struct CheckpointUpdate {
    pub stream_name: String,
    pub last_processed_slot: Option<i64>,
    pub last_observed_at_unix_ms: i64,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SqlWritePlan {
    pub statements: Vec<String>,
}

impl SqlWritePlan {
    pub fn statement_count(&self) -> usize {
        self.statements.len()
    }
}

pub fn build_write_plan(
    batch: &PersistedBatch,
    retention_policy: &RetentionPolicy,
    checkpoint: Option<CheckpointUpdate>,
) -> SqlWritePlan {
    let mut plan = SqlWritePlan::default();

    if !batch.account_rows.is_empty() {
        plan.statements
            .push(build_account_updates_insert(&batch.account_rows));
    }
    if !batch.transaction_rows.is_empty() {
        plan.statements
            .push(build_transactions_insert(&batch.transaction_rows));
    }
    if !batch.slot_rows.is_empty() {
        plan.statements.push(build_slots_upsert(&batch.slot_rows));
    }
    if !batch.custom_rows.is_empty() {
        plan.statements
            .push(build_custom_decoded_insert(&batch.custom_rows));
    }

    if let Some(latest_timestamp_unix_ms) = batch.latest_timestamp_unix_ms() {
        plan.statements.extend(build_retention_deletes(
            latest_timestamp_unix_ms,
            retention_policy,
        ));
    }

    if let Some(checkpoint) = checkpoint {
        plan.statements.push(build_checkpoint_upsert(&checkpoint));
    }

    plan
}

fn build_account_updates_insert(rows: &[AccountUpdateRow]) -> String {
    let values = rows
        .iter()
        .map(|row| {
            format!(
                "({}, {}, {}, {}, {}, {}, {})",
                sql_timestamptz_from_unix_ms(row.timestamp_unix_ms),
                row.slot,
                sql_bytea(&row.pubkey),
                sql_bytea(&row.owner),
                row.lamports,
                sql_optional_bytea(Some(&row.data)),
                row.write_version,
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ");

    format!(
        "INSERT INTO account_updates (timestamp, slot, pubkey, owner, lamports, data, write_version)\nVALUES\n    {values}\nON CONFLICT (timestamp, slot, pubkey, write_version) DO NOTHING;"
    )
}

fn build_transactions_insert(rows: &[TransactionRow]) -> String {
    let values = rows
        .iter()
        .map(|row| {
            format!(
                "({}, {}, {}, {}, {}, {}, {})",
                sql_timestamptz_from_unix_ms(row.timestamp_unix_ms),
                row.slot,
                sql_bytea(&row.signature),
                row.fee,
                sql_bool(row.success),
                sql_bytea_array(&row.program_ids),
                sql_text_array(&row.log_messages),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ");

    format!(
        "INSERT INTO transactions (timestamp, slot, signature, fee, success, program_ids, log_messages)\nVALUES\n    {values}\nON CONFLICT (timestamp, signature) DO NOTHING;"
    )
}

fn build_slots_upsert(rows: &[SlotRow]) -> String {
    let values = rows
        .iter()
        .map(|row| {
            format!(
                "({}, {}, {}, {})",
                row.slot,
                sql_optional_i64(row.parent_slot),
                sql_timestamptz_from_unix_ms(row.timestamp_unix_ms),
                sql_text(&row.status),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ");

    format!(
        "INSERT INTO slots (slot, parent_slot, timestamp, status)\nVALUES\n    {values}\nON CONFLICT (slot) DO UPDATE SET\n    parent_slot = EXCLUDED.parent_slot,\n    timestamp = EXCLUDED.timestamp,\n    status = EXCLUDED.status;"
    )
}

fn build_custom_decoded_insert(rows: &[CustomDecodedRow]) -> String {
    let values = rows
        .iter()
        .map(|row| {
            format!(
                "({}, {}, {}, {}, {})",
                sql_timestamptz_from_unix_ms(row.timestamp_unix_ms),
                row.slot,
                sql_text(&row.decoder_name),
                sql_text(&row.record_key),
                sql_text(&row.payload),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ");

    format!(
        "INSERT INTO custom_decoded_events (timestamp, slot, decoder_name, record_key, payload)\nVALUES\n    {values}\nON CONFLICT (timestamp, decoder_name, record_key, slot) DO NOTHING;"
    )
}

fn build_retention_deletes(
    latest_timestamp_unix_ms: i64,
    retention_policy: &RetentionPolicy,
) -> Vec<String> {
    let cutoff_unix_ms = latest_timestamp_unix_ms - retention_policy.max_age.as_millis() as i64;
    let cutoff = sql_timestamptz_from_unix_ms(cutoff_unix_ms);

    vec![
        format!("DELETE FROM account_updates WHERE timestamp < {cutoff};"),
        format!("DELETE FROM transactions WHERE timestamp < {cutoff};"),
        format!("DELETE FROM custom_decoded_events WHERE timestamp < {cutoff};"),
        format!("DELETE FROM pipeline_metrics WHERE timestamp < {cutoff};"),
    ]
}

fn build_checkpoint_upsert(checkpoint: &CheckpointUpdate) -> String {
    format!(
        "INSERT INTO ingestion_checkpoints (stream_name, last_processed_slot, last_observed_at, notes)\nVALUES ({}, {}, {}, {})\nON CONFLICT (stream_name) DO UPDATE SET\n    last_processed_slot = EXCLUDED.last_processed_slot,\n    last_observed_at = EXCLUDED.last_observed_at,\n    notes = EXCLUDED.notes;",
        sql_text(&checkpoint.stream_name),
        sql_optional_i64(checkpoint.last_processed_slot),
        sql_timestamptz_from_unix_ms(checkpoint.last_observed_at_unix_ms),
        sql_optional_text(checkpoint.notes.as_deref()),
    )
}

fn sql_timestamptz_from_unix_ms(unix_ms: i64) -> String {
    format!("to_timestamp({unix_ms} / 1000.0)")
}

fn sql_optional_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

fn sql_optional_text(value: Option<&str>) -> String {
    value.map(sql_text).unwrap_or_else(|| "NULL".to_string())
}

fn sql_optional_bytea(value: Option<&[u8]>) -> String {
    value.map(sql_bytea).unwrap_or_else(|| "NULL".to_string())
}

fn sql_bytea(bytes: &[u8]) -> String {
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("E'\\\\x{hex}'")
}

fn sql_bytea_array(values: &[Vec<u8>]) -> String {
    if values.is_empty() {
        return "ARRAY[]::BYTEA[]".to_string();
    }

    let inner = values
        .iter()
        .map(|value| sql_bytea(value))
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{inner}]")
}

fn sql_text_array(values: &[String]) -> String {
    if values.is_empty() {
        return "ARRAY[]::TEXT[]".to_string();
    }

    let inner = values
        .iter()
        .map(|value| sql_text(value))
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{inner}]")
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_bool(value: bool) -> &'static str {
    if value {
        "TRUE"
    } else {
        "FALSE"
    }
}

#[cfg(test)]
mod tests {
    use super::{build_write_plan, CheckpointUpdate};
    use crate::processor::batch_writer::FlushReason;
    use crate::processor::decoder::PersistedBatch;
    use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};
    use crate::processor::store::RetentionPolicy;
    use std::time::Duration;

    #[test]
    fn builds_raw_sql_for_generic_custom_and_checkpoint_writes() {
        let batch = PersistedBatch {
            reason: FlushReason::Size,
            account_rows: vec![AccountUpdateRow {
                slot: 10,
                timestamp_unix_ms: 1_710_000_000_000,
                pubkey: vec![1, 2],
                owner: vec![3, 4],
                lamports: 5,
                data: vec![6, 7],
                write_version: 8,
            }],
            transaction_rows: vec![TransactionRow {
                slot: 10,
                timestamp_unix_ms: 1_710_000_000_001,
                signature: vec![8, 9],
                fee: 5_000,
                success: true,
                program_ids: vec![vec![1, 1]],
                log_messages: vec!["swap".to_string()],
            }],
            slot_rows: vec![SlotRow {
                slot: 10,
                timestamp_unix_ms: 1_710_000_000_002,
                parent_slot: Some(9),
                status: "processed".to_string(),
            }],
            custom_rows: vec![CustomDecodedRow {
                decoder_name: "program-activity".to_string(),
                record_key: "tracked-account".to_string(),
                slot: 10,
                timestamp_unix_ms: 1_710_000_000_003,
                payload: "account_update".to_string(),
            }],
        };

        let plan = build_write_plan(
            &batch,
            &RetentionPolicy {
                max_age: Duration::from_secs(60),
            },
            Some(CheckpointUpdate {
                stream_name: "geyser-main".to_string(),
                last_processed_slot: Some(10),
                last_observed_at_unix_ms: 1_710_000_000_003,
                notes: Some("ok".to_string()),
            }),
        );

        assert_eq!(plan.statement_count(), 9);
        assert!(plan.statements[0].contains("INSERT INTO account_updates"));
        assert!(plan.statements[1].contains("INSERT INTO transactions"));
        assert!(plan.statements[2].contains("INSERT INTO slots"));
        assert!(plan.statements[3].contains("INSERT INTO custom_decoded_events"));
        assert!(plan.statements[8].contains("INSERT INTO ingestion_checkpoints"));
    }
}
