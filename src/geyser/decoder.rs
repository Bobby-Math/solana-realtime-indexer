use helius_laserstream::grpc::SubscribeUpdate;
use helius_laserstream::grpc::subscribe_update::UpdateOneof;

#[derive(Debug, Clone)]
pub enum GeyserEvent {
    AccountUpdate(AccountUpdate),
    Transaction(TransactionUpdate),
    SlotUpdate(SlotUpdate),
    BlockMeta(BlockMetaUpdate),
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub pubkey: Vec<u8>,
    pub owner: Vec<u8>,
    pub lamports: u64,
    pub write_version: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub signature: Vec<u8>,
    pub fee: u64,
    pub success: bool,
    pub accounts: Vec<Vec<u8>>,
    pub program_ids: Vec<Vec<u8>>,
    pub log_messages: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SlotUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub parent_slot: Option<u64>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct BlockMetaUpdate {
    pub slot: u64,
    pub observed_at_unix_ms: i64,
    pub block_time_ms: i64,
    pub block_height: Option<u64>,
}

/// Decodes a SubscribeUpdate protobuf into a GeyserEvent.
/// This is a shared function used by both the Geyser client (for filtering)
/// and the WAL consumer (for processing), eliminating code duplication.
pub fn decode_subscribe_update(update: &SubscribeUpdate, timestamp_unix_ms: i64) -> Option<GeyserEvent> {
    match &update.update_oneof {
        Some(UpdateOneof::Account(account_update)) => {
            if let Some(account_info) = &account_update.account {
                Some(GeyserEvent::AccountUpdate(AccountUpdate {
                    timestamp_unix_ms,
                    slot: account_update.slot,
                    pubkey: account_info.pubkey.clone(),
                    owner: account_info.owner.clone(),
                    lamports: account_info.lamports,
                    write_version: account_info.write_version,
                    data: account_info.data.clone(),
                }))
            } else {
                log::warn!("Account update missing account info for slot {}", account_update.slot);
                None
            }
        }
        Some(UpdateOneof::Transaction(tx_update)) => {
            if let Some(tx_info) = &tx_update.transaction {
                let success = tx_info.meta.as_ref()
                    .map(|meta| meta.err.is_none())
                    .unwrap_or(false);

                let fee = tx_info.meta.as_ref()
                    .map(|meta| meta.fee)
                    .unwrap_or(0);

                let log_messages = tx_info.meta.as_ref()
                    .map(|meta| meta.log_messages.clone())
                    .unwrap_or_default();

                // Extract program IDs from transaction instructions (order-preserving dedup)
                let (program_ids, accounts) = if let Some(tx) = &tx_info.transaction {
                    if let Some(message) = &tx.message {
                        let mut seen = std::collections::HashSet::new();
                        let program_ids: Vec<Vec<u8>> = message.instructions
                            .iter()
                            .filter_map(|instruction| {
                                let program_idx = instruction.program_id_index as usize;
                                message.account_keys.get(program_idx)
                            })
                            .filter(|key| seen.insert(key.as_slice()))
                            .cloned()
                            .collect();
                        let accounts = message.account_keys.clone();
                        (program_ids, accounts)
                    } else {
                        log::debug!("No message field in transaction, program_ids and accounts will be empty");
                        (Vec::new(), Vec::new())
                    }
                } else {
                    log::debug!("No transaction field available, program_ids and accounts will be empty");
                    (Vec::new(), Vec::new())
                };

                Some(GeyserEvent::Transaction(TransactionUpdate {
                    timestamp_unix_ms,
                    slot: tx_update.slot,
                    signature: tx_info.signature.clone(),
                    fee,
                    success,
                    accounts,
                    program_ids,
                    log_messages,
                }))
            } else {
                log::warn!("Transaction update missing transaction info for slot {}", tx_update.slot);
                None
            }
        }
        Some(UpdateOneof::Slot(slot_update)) => {
            Some(GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms,
                slot: slot_update.slot,
                parent_slot: slot_update.parent,
                status: map_slot_status(slot_update.status),
            }))
        }
        Some(UpdateOneof::BlockMeta(block_meta)) => {
            let block_time_ms = block_meta.block_time
                .map(|unix_ts| unix_ts.timestamp * 1000)
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

            Some(GeyserEvent::BlockMeta(BlockMetaUpdate {
                slot: block_meta.slot,
                observed_at_unix_ms: timestamp_unix_ms,
                block_time_ms,
                block_height: block_meta.block_height.map(|bh| bh.block_height),
            }))
        }
        Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => {
            log::trace!("Received ping/pong - ignoring");
            None
        }
        other => {
            log::warn!("Received unhandled update type: {:?}", other);
            None
        }
    }
}

/// Maps protobuf slot status enum values to human-readable strings.
fn map_slot_status(status: i32) -> String {
    match status {
        0 => "processed".to_string(),
        1 => "confirmed".to_string(),
        2 => "finalized".to_string(),
        3 => "first_shred_received".to_string(),
        4 => "completed".to_string(),
        5 => "created_bank".to_string(),
        6 => "dead".to_string(),
        _ => {
            log::warn!("Unknown slot status value: {}, using 'unknown'", status);
            "unknown".to_string()
        }
    }
}
