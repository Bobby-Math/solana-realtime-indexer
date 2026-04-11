use crate::geyser::decoder::{GeyserEvent, TransactionUpdate};
use crate::processor::batch_writer::BufferedBatch;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};

pub trait CustomDecoder: Send {
    fn name(&self) -> &str;
    fn decode(&mut self, event: &GeyserEvent) -> Option<CustomDecodedRow>;
}

#[derive(Debug, Clone)]
pub struct Type1Decoder;

#[derive(Debug, Clone)]
pub struct PersistedBatch {
    pub reason: crate::processor::batch_writer::FlushReason,
    pub account_rows: Vec<AccountUpdateRow>,
    pub transaction_rows: Vec<TransactionRow>,
    pub slot_rows: Vec<SlotRow>,
    pub custom_rows: Vec<CustomDecodedRow>,
}

impl PersistedBatch {
    pub fn latest_timestamp_unix_ms(&self) -> Option<i64> {
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
}

impl Type1Decoder {
    pub fn new() -> Self {
        Self
    }

    pub fn decode(
        &self,
        batch: BufferedBatch,
        custom_decoders: &mut [Box<dyn CustomDecoder>],
    ) -> PersistedBatch {
        let mut persisted = PersistedBatch {
            reason: batch.reason,
            account_rows: Vec::new(),
            transaction_rows: Vec::new(),
            slot_rows: Vec::new(),
            custom_rows: Vec::new(),
        };

        for event in batch.events {
            for decoder in custom_decoders.iter_mut() {
                if let Some(row) = decoder.decode(&event) {
                    persisted.custom_rows.push(row);
                }
            }

            match event {
                GeyserEvent::AccountUpdate(update) => {
                    persisted.account_rows.push(AccountUpdateRow {
                        slot: update.slot as i64,
                        timestamp_unix_ms: update.timestamp_unix_ms,
                        pubkey: update.pubkey.into_bytes(),
                        owner: update.owner.into_bytes(),
                        lamports: update.lamports as i64,
                        data: update.data,
                        write_version: update.write_version as i64,
                    });
                }
                GeyserEvent::Transaction(update) => {
                    persisted.transaction_rows.push(TransactionRow {
                        slot: update.slot as i64,
                        timestamp_unix_ms: update.timestamp_unix_ms,
                        signature: update.signature.into_bytes(),
                        fee: update.fee as i64,
                        success: update.success,
                        program_ids: update
                            .program_ids
                            .into_iter()
                            .map(String::into_bytes)
                            .collect(),
                        log_messages: update.log_messages,
                    });
                }
                GeyserEvent::SlotUpdate(update) => {
                    persisted.slot_rows.push(SlotRow {
                        slot: update.slot as i64,
                        timestamp_unix_ms: update.timestamp_unix_ms,
                        parent_slot: update.parent_slot.map(|slot| slot as i64),
                        status: update.status,
                    });
                }
            }
        }

        persisted
    }
}

#[derive(Debug, Clone)]
pub struct ProgramActivityDecoder {
    program_id: String,
}

impl ProgramActivityDecoder {
    pub fn new(program_id: impl Into<String>) -> Self {
        Self {
            program_id: program_id.into(),
        }
    }
}

impl CustomDecoder for ProgramActivityDecoder {
    fn name(&self) -> &str {
        "program-activity"
    }

    fn decode(&mut self, event: &GeyserEvent) -> Option<CustomDecodedRow> {
        match event {
            GeyserEvent::AccountUpdate(update) if update.owner == self.program_id => {
                Some(CustomDecodedRow {
                    decoder_name: self.name().to_string(),
                    record_key: update.pubkey.clone(),
                    slot: update.slot as i64,
                    timestamp_unix_ms: update.timestamp_unix_ms,
                    payload: "account_update".to_string(),
                })
            }
            GeyserEvent::Transaction(update)
                if transaction_mentions_program(update, &self.program_id) =>
            {
                Some(CustomDecodedRow {
                    decoder_name: self.name().to_string(),
                    record_key: update.signature.clone(),
                    slot: update.slot as i64,
                    timestamp_unix_ms: update.timestamp_unix_ms,
                    payload: "transaction".to_string(),
                })
            }
            _ => None,
        }
    }
}

fn transaction_mentions_program(update: &TransactionUpdate, program_id: &str) -> bool {
    update
        .program_ids
        .iter()
        .any(|candidate| candidate == program_id)
}

#[cfg(test)]
mod tests {
    use super::{CustomDecoder, ProgramActivityDecoder, Type1Decoder};
    use crate::geyser::decoder::{AccountUpdate, GeyserEvent, TransactionUpdate};
    use crate::processor::batch_writer::{BufferedBatch, FlushReason};

    #[test]
    fn type1_decoder_keeps_full_generic_rows_and_custom_rows() {
        let decoder = Type1Decoder::new();
        let mut custom_decoders: Vec<Box<dyn CustomDecoder>> =
            vec![Box::new(ProgramActivityDecoder::new("amm-program"))];
        let batch = BufferedBatch {
            reason: FlushReason::Size,
            events: vec![
                GeyserEvent::AccountUpdate(AccountUpdate {
                    timestamp_unix_ms: 1_710_000_000_000,
                    slot: 50,
                    pubkey: "tracked-account".to_string(),
                    owner: "amm-program".to_string(),
                    lamports: 9,
                    write_version: 1,
                    data: vec![1, 2, 3],
                }),
                GeyserEvent::Transaction(TransactionUpdate {
                    timestamp_unix_ms: 1_710_000_000_001,
                    slot: 50,
                    signature: "tracked-signature".to_string(),
                    fee: 5_000,
                    success: true,
                    program_ids: vec!["amm-program".to_string()],
                    log_messages: vec!["swap".to_string()],
                }),
            ],
        };

        let persisted = decoder.decode(batch, &mut custom_decoders);

        assert_eq!(persisted.account_rows.len(), 1);
        assert_eq!(persisted.transaction_rows.len(), 1);
        assert_eq!(persisted.custom_rows.len(), 2);
        assert_eq!(persisted.transaction_rows[0].log_messages.len(), 1);
    }
}
