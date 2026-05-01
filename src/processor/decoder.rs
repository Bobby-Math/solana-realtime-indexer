use crate::geyser::decoder::{GeyserEvent, TransactionUpdate};
use crate::geyser::BlockTimeCache;
use crate::processor::batch_writer::BufferedBatch;
use crate::processor::schema::{AccountUpdateRow, CustomDecodedRow, SlotRow, TransactionRow};

pub trait CustomDecoder: Send {
    fn name(&self) -> &str;
    fn decode(&mut self, event: &GeyserEvent) -> Option<CustomDecodedRow>;
    fn decode_multi(&mut self, event: &GeyserEvent) -> Vec<CustomDecodedRow> {
        // Default implementation for backwards compatibility
        self.decode(event).into_iter().collect()
    }
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
    // Track slot and timestamp even for events that don't produce rows (e.g., BlockMeta)
    // This ensures checkpoint updates happen even when batch has zero rows
    pub last_processed_slot: Option<i64>,
    pub last_observed_at_unix_ms: Option<i64>,
    pub last_on_chain_block_time_ms: Option<i64>,
}

impl PersistedBatch {
    pub fn latest_timestamp_unix_ms(&self) -> Option<i64> {
        // Use tracked timestamp first, fall back to extracting from rows
        self.last_observed_at_unix_ms.or_else(|| {
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
        })
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
        block_time_cache: &BlockTimeCache,
    ) -> PersistedBatch {
        let mut persisted = PersistedBatch {
            reason: batch.reason,
            account_rows: Vec::new(),
            transaction_rows: Vec::new(),
            slot_rows: Vec::new(),
            custom_rows: Vec::new(),
            last_processed_slot: None,
            last_observed_at_unix_ms: None,
            last_on_chain_block_time_ms: None,
        };

        for event in batch.events {
            // Track slot, arrival time, and on-chain block time separately.
            let event_info = match &event {
                GeyserEvent::AccountUpdate(u) => Some((
                    u.slot as i64,
                    u.timestamp_unix_ms,
                    block_time_cache.get(u.slot),
                )),
                GeyserEvent::Transaction(u) => Some((
                    u.slot as i64,
                    u.timestamp_unix_ms,
                    block_time_cache.get(u.slot),
                )),
                GeyserEvent::SlotUpdate(u) => Some((u.slot as i64, u.timestamp_unix_ms, None)),
                GeyserEvent::BlockMeta(u) => {
                    Some((u.slot as i64, u.observed_at_unix_ms, Some(u.block_time_ms)))
                }
            };

            if let Some((slot, observed_at, on_chain_block_time)) = event_info {
                persisted.last_processed_slot = persisted.last_processed_slot.max(Some(slot));
                persisted.last_observed_at_unix_ms =
                    persisted.last_observed_at_unix_ms.max(Some(observed_at));
                persisted.last_on_chain_block_time_ms =
                    max_optional(persisted.last_on_chain_block_time_ms, on_chain_block_time);
            }

            for decoder in custom_decoders.iter_mut() {
                let rows = decoder.decode_multi(&event);
                if !rows.is_empty() {
                    log::debug!(
                        "Decoder {} produced {} rows for event",
                        decoder.name(),
                        rows.len()
                    );
                }
                persisted.custom_rows.extend(rows);
            }

            match event {
                GeyserEvent::AccountUpdate(update) => {
                    let crate::geyser::decoder::AccountUpdate {
                        timestamp_unix_ms,
                        slot,
                        pubkey,
                        owner,
                        lamports,
                        write_version,
                        data,
                    } = update;

                    // Use block_time if available, fall back to wall-clock timestamp
                    let timestamp_ms = block_time_cache.get(slot).unwrap_or(timestamp_unix_ms);

                    persisted.account_rows.push(AccountUpdateRow {
                        slot: slot as i64,
                        timestamp_unix_ms: timestamp_ms,
                        pubkey,
                        owner,
                        lamports: lamports as i64,
                        data,
                        write_version: write_version as i64,
                    });
                }
                GeyserEvent::Transaction(update) => {
                    let crate::geyser::decoder::TransactionUpdate {
                        timestamp_unix_ms,
                        slot,
                        signature: raw_signature,
                        fee,
                        success,
                        accounts: _,
                        program_ids: raw_program_ids,
                        log_messages,
                    } = update;

                    // Use block_time if available, fall back to wall-clock timestamp
                    let timestamp_ms = block_time_cache.get(slot).unwrap_or(timestamp_unix_ms);

                    let signature = TransactionRow::signature_from_slice(&raw_signature);
                    let mut program_ids = Vec::with_capacity(raw_program_ids.len());
                    let mut invalid_program_ids = 0usize;

                    for program_id in raw_program_ids {
                        match TransactionRow::program_id_from_slice(&program_id) {
                            Some(program_id) => program_ids.push(program_id),
                            None => invalid_program_ids += 1,
                        }
                    }

                    if invalid_program_ids > 0 {
                        log::warn!(
                            "Dropped {} malformed program_ids while decoding transaction at slot {}",
                            invalid_program_ids,
                            slot
                        );
                    }

                    log::debug!(
                        "Processing transaction: slot={} program_ids={}",
                        slot,
                        program_ids.len()
                    );
                    persisted.transaction_rows.push(TransactionRow {
                        slot: slot as i64,
                        timestamp_unix_ms: timestamp_ms,
                        signature,
                        fee: fee as i64,
                        success,
                        program_ids,
                        log_messages,
                    });
                }
                GeyserEvent::SlotUpdate(update) => {
                    // Get block_time from cache if available (from BlockMeta events)
                    let block_time = block_time_cache.get(update.slot);

                    persisted.slot_rows.push(SlotRow {
                        slot: update.slot as i64,
                        timestamp_unix_ms: update.timestamp_unix_ms,
                        parent_slot: update.parent_slot.map(|slot| slot as i64),
                        status: update.status,
                        block_time,
                    });
                }
                GeyserEvent::BlockMeta(_) => {
                    // BlockMeta events are handled separately to populate the block_time cache
                    // They don't produce rows directly
                }
            }
        }

        persisted
    }
}

impl Default for Type1Decoder {
    fn default() -> Self {
        Self::new()
    }
}

fn max_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[derive(Debug, Clone)]
pub struct ProgramActivityDecoder {
    program_id: Vec<u8>,
}

impl ProgramActivityDecoder {
    pub fn new(program_id: impl AsRef<str>) -> Self {
        // Decode the program_id once during construction
        let program_id_bytes = bs58::decode(program_id.as_ref())
            .into_vec()
            .unwrap_or_else(|_| program_id.as_ref().as_bytes().to_vec());
        Self {
            program_id: program_id_bytes,
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
                // Encode pubkey back to base58 for the record_key (CustomDecodedRow uses String)
                let record_key = bs58::encode(&update.pubkey).into_string();
                Some(CustomDecodedRow {
                    decoder_name: self.name().to_string(),
                    record_key,
                    slot: update.slot as i64,
                    timestamp_unix_ms: update.timestamp_unix_ms,
                    event_index: 0,
                    payload: serde_json::json!("account_update"),
                })
            }
            GeyserEvent::Transaction(update)
                if transaction_mentions_program(update, &self.program_id) =>
            {
                // Encode signature back to base58 for the record_key (CustomDecodedRow uses String)
                let record_key = bs58::encode(&update.signature).into_string();
                Some(CustomDecodedRow {
                    decoder_name: self.name().to_string(),
                    record_key,
                    slot: update.slot as i64,
                    timestamp_unix_ms: update.timestamp_unix_ms,
                    event_index: 0,
                    payload: serde_json::json!("transaction"),
                })
            }
            _ => None,
        }
    }
}

fn transaction_mentions_program(update: &TransactionUpdate, program_id: &[u8]) -> bool {
    update
        .program_ids
        .iter()
        .any(|candidate_bytes| candidate_bytes == program_id)
        || update
            .accounts
            .iter()
            .any(|candidate_bytes| candidate_bytes == program_id)
}

#[cfg(test)]
mod tests {
    use super::{CustomDecoder, ProgramActivityDecoder, Type1Decoder};
    use crate::geyser::decoder::{AccountUpdate, GeyserEvent, TransactionUpdate};
    use crate::geyser::BlockTimeCache;
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
                    pubkey: "tracked-account".as_bytes().to_vec(),
                    owner: "amm-program".as_bytes().to_vec(),
                    lamports: 9,
                    write_version: 1,
                    data: vec![1, 2, 3],
                }),
                GeyserEvent::Transaction(TransactionUpdate {
                    timestamp_unix_ms: 1_710_000_000_001,
                    slot: 50,
                    signature: "tracked-signature".as_bytes().to_vec(),
                    fee: 5_000,
                    success: true,
                    accounts: vec![
                        "tracked-account".as_bytes().to_vec(),
                        "amm-program".as_bytes().to_vec(),
                    ],
                    program_ids: vec!["amm-program".as_bytes().to_vec()],
                    log_messages: vec!["swap".to_string()],
                }),
            ],
        };

        let block_time_cache = BlockTimeCache::new(1000);
        let persisted = decoder.decode(batch, &mut custom_decoders, &block_time_cache);

        assert_eq!(persisted.account_rows.len(), 1);
        assert_eq!(persisted.transaction_rows.len(), 1);
        assert_eq!(persisted.custom_rows.len(), 2);
        assert_eq!(persisted.transaction_rows[0].log_messages.len(), 1);
    }

    #[test]
    fn type1_decoder_drops_malformed_program_ids() {
        let decoder = Type1Decoder::new();
        let mut custom_decoders: Vec<Box<dyn CustomDecoder>> = Vec::new();
        let batch = BufferedBatch {
            reason: FlushReason::Size,
            events: vec![GeyserEvent::Transaction(TransactionUpdate {
                timestamp_unix_ms: 1_710_000_000_001,
                slot: 50,
                signature: vec![7; 64],
                fee: 5_000,
                success: true,
                accounts: vec![],
                program_ids: vec![vec![1; 32], vec![2; 31], vec![3; 33]],
                log_messages: vec![],
            })],
        };

        let block_time_cache = BlockTimeCache::new(1000);
        let persisted = decoder.decode(batch, &mut custom_decoders, &block_time_cache);

        assert_eq!(persisted.transaction_rows.len(), 1);
        assert_eq!(persisted.transaction_rows[0].program_ids.len(), 1);
        assert_eq!(persisted.transaction_rows[0].program_ids[0], [1; 32]);
    }
}
