use std::sync::mpsc::{SendError, SyncSender};

use crate::geyser::decoder::{AccountUpdate, GeyserEvent, SlotUpdate, TransactionUpdate};
use helius_laserstream::grpc::subscribe_update::UpdateOneof;
use helius_laserstream::grpc::SubscribeUpdate;

#[derive(Debug, Clone)]
pub struct GeyserConfig {
    pub endpoint: String,
    pub channel_capacity: usize,
    pub filters: Vec<SubscriptionFilter>,
}

impl GeyserConfig {
    pub fn new(
        endpoint: String,
        channel_capacity: usize,
        filters: Vec<SubscriptionFilter>,
    ) -> Self {
        Self {
            endpoint,
            channel_capacity,
            filters,
        }
    }
}

/// Subscription filter with validated base58 bytes.
/// The base58 string is parsed once at construction time, not on every event match.
#[derive(Debug, Clone)]
pub enum SubscriptionFilter {
    Program([u8; 32]),
    Account([u8; 32]),
    Slots,
    Blocks,
}

impl SubscriptionFilter {
    /// Create a new Program filter, validating the base58 string at construction time.
    /// Returns Err if the base58 string is invalid.
    pub fn program(program_id: String) -> Result<Self, String> {
        let trimmed = program_id.trim();
        decode_pubkey(trimmed, "program").map(SubscriptionFilter::Program)
    }

    /// Create a new Account filter, validating the base58 string at construction time.
    /// Returns Err if the base58 string is invalid.
    pub fn account(pubkey: String) -> Result<Self, String> {
        let trimmed = pubkey.trim();
        decode_pubkey(trimmed, "account").map(SubscriptionFilter::Account)
    }

    pub fn program_bytes(program_id: [u8; 32]) -> Self {
        SubscriptionFilter::Program(program_id)
    }

    pub fn account_bytes(pubkey: [u8; 32]) -> Self {
        SubscriptionFilter::Account(pubkey)
    }

    /// Create a new Slots filter.
    pub fn slots() -> Self {
        SubscriptionFilter::Slots
    }

    /// Create a new Blocks filter.
    pub fn blocks() -> Self {
        SubscriptionFilter::Blocks
    }

    pub fn matches_update(&self, update: &SubscribeUpdate) -> bool {
        match (self, &update.update_oneof) {
            (
                SubscriptionFilter::Program(program_bytes),
                Some(UpdateOneof::Account(account_update)),
            ) => account_update
                .account
                .as_ref()
                .map(|account| account.owner.as_slice() == program_bytes)
                .unwrap_or(false),
            (
                SubscriptionFilter::Program(program_bytes),
                Some(UpdateOneof::Transaction(tx_update)),
            ) => transaction_mentions_account_key(tx_update, program_bytes),
            (
                SubscriptionFilter::Account(account_bytes),
                Some(UpdateOneof::Account(account_update)),
            ) => account_update
                .account
                .as_ref()
                .map(|account| account.pubkey.as_slice() == account_bytes)
                .unwrap_or(false),
            (
                SubscriptionFilter::Account(account_bytes),
                Some(UpdateOneof::Transaction(tx_update)),
            ) => transaction_mentions_account_key(tx_update, account_bytes),
            (SubscriptionFilter::Slots, Some(UpdateOneof::Slot(_))) => true,
            (SubscriptionFilter::Blocks, Some(UpdateOneof::BlockMeta(_))) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GeyserConsumer {
    pub config: GeyserConfig,
}

impl GeyserConsumer {
    pub fn new(config: GeyserConfig) -> Self {
        Self { config }
    }

    pub fn accepts(&self, event: &GeyserEvent) -> bool {
        if self.config.filters.is_empty() {
            return true;
        }

        self.config
            .filters
            .iter()
            .any(|filter| filter.matches(event))
    }

    pub fn forward_events<I>(
        &self,
        sender: &SyncSender<GeyserEvent>,
        events: I,
    ) -> Result<usize, SendError<GeyserEvent>>
    where
        I: IntoIterator<Item = GeyserEvent>,
    {
        let mut forwarded = 0;

        for event in events {
            if !self.accepts(&event) {
                continue;
            }

            sender.send(event)?;
            forwarded += 1;
        }

        Ok(forwarded)
    }

    pub fn simulated_fixture() -> Vec<GeyserEvent> {
        // Use current timestamp so the materialized view time filter includes it
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Use real 32-byte Solana pubkeys (decoded from base58)
        let tracked_account = bs58::decode("7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU")
            .into_vec()
            .unwrap();
        let orca_program = bs58::decode("9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM")
            .into_vec()
            .unwrap();
        let token_program = bs58::decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
            .into_vec()
            .unwrap();
        let tracked_signature = bs58::decode("5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprv2W1qY1qrk7jjFgJG3kGpYcQHxUxYWBZgmhnfYTLuLr").into_vec().unwrap();
        let ignored_account = bs58::decode("Noise1111111111111111111111111111111111111111")
            .into_vec()
            .unwrap();
        let system_program = bs58::decode("11111111111111111111111111111111")
            .into_vec()
            .unwrap();

        vec![
            GeyserEvent::AccountUpdate(AccountUpdate {
                timestamp_unix_ms: now_ms,
                slot: 9_001,
                pubkey: tracked_account.clone(),
                owner: orca_program.clone(),
                lamports: 42,
                write_version: 7,
                data: vec![1, 2, 3, 4],
            }),
            GeyserEvent::Transaction(TransactionUpdate {
                timestamp_unix_ms: now_ms + 1,
                slot: 9_001,
                signature: tracked_signature,
                fee: 5_000,
                success: true,
                accounts: vec![
                    tracked_account.clone(),
                    orca_program.clone(),
                    token_program.clone(),
                ],
                program_ids: vec![orca_program.clone(), token_program],
                log_messages: vec!["swap".to_string(), "settled".to_string()],
            }),
            GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms: now_ms + 2,
                slot: 9_001,
                parent_slot: Some(9_000),
                status: "processed".to_string(),
            }),
            // Add confirmed status for the same slot (500ms later) to test latency calculation
            GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms: now_ms + 502, // 500ms after processed
                slot: 9_001,
                parent_slot: Some(9_000),
                status: "confirmed".to_string(),
            }),
            GeyserEvent::AccountUpdate(AccountUpdate {
                timestamp_unix_ms: now_ms + 3,
                slot: 9_001,
                pubkey: ignored_account,
                owner: system_program,
                lamports: 1,
                write_version: 1,
                data: vec![9, 9, 9],
            }),
        ]
    }
}

impl SubscriptionFilter {
    fn matches(&self, event: &GeyserEvent) -> bool {
        match (self, event) {
            (SubscriptionFilter::Program(program_bytes), GeyserEvent::AccountUpdate(update)) => {
                update.owner.as_slice() == program_bytes
            }
            (SubscriptionFilter::Program(program_bytes), GeyserEvent::Transaction(update)) => {
                update
                    .program_ids
                    .iter()
                    .any(|id_bytes| id_bytes.as_slice() == program_bytes)
                    || update
                        .accounts
                        .iter()
                        .any(|account| account.as_slice() == program_bytes)
            }
            (SubscriptionFilter::Account(account_bytes), GeyserEvent::AccountUpdate(update)) => {
                update.pubkey.as_slice() == account_bytes
            }
            (SubscriptionFilter::Account(account_bytes), GeyserEvent::Transaction(update)) => {
                update
                    .accounts
                    .iter()
                    .any(|acc| acc.as_slice() == account_bytes)
            }
            (SubscriptionFilter::Slots, GeyserEvent::SlotUpdate(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::BlockMeta(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::SlotUpdate(_)) => false,
            _ => false,
        }
    }
}

fn decode_pubkey(value: &str, kind: &str) -> Result<[u8; 32], String> {
    let bytes = bs58::decode(value)
        .into_vec()
        .map_err(|e| format!("Invalid base58 for {} '{}': {}", kind, value, e))?;

    bytes
        .try_into()
        .map_err(|_| format!("Invalid {} '{}': expected 32 decoded bytes", kind, value))
}

fn transaction_mentions_account_key(
    tx_update: &helius_laserstream::grpc::SubscribeUpdateTransaction,
    candidate: &[u8; 32],
) -> bool {
    tx_update
        .transaction
        .as_ref()
        .and_then(|tx_info| tx_info.transaction.as_ref().map(|tx| (tx_info, tx)))
        .map(|(tx_info, tx)| {
            tx.message
                .as_ref()
                .map(|message| {
                    message
                        .account_keys
                        .iter()
                        .chain(tx_info.meta.as_ref().into_iter().flat_map(|meta| {
                            meta.loaded_writable_addresses
                                .iter()
                                .chain(meta.loaded_readonly_addresses.iter())
                        }))
                        .any(|key| key.as_slice() == candidate)
                })
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod filter_tests {
    use super::{
        transaction_mentions_account_key, GeyserEvent, SubscriptionFilter, TransactionUpdate,
    };
    use helius_laserstream::grpc::{
        subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateTransaction,
        SubscribeUpdateTransactionInfo,
    };
    use helius_laserstream::solana::storage::confirmed_block::{
        CompiledInstruction, Message, Transaction, TransactionStatusMeta,
    };

    #[test]
    fn program_filter_matches_transaction_when_program_is_loaded_address() {
        let tracked_program = [7u8; 32];
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot: 42,
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: vec![1; 64],
                    transaction: Some(Transaction {
                        signatures: vec![],
                        message: Some(Message {
                            header: None,
                            account_keys: vec![vec![1u8; 32]],
                            recent_blockhash: vec![],
                            instructions: vec![CompiledInstruction {
                                program_id_index: 0,
                                accounts: vec![],
                                data: vec![],
                            }],
                            versioned: true,
                            address_table_lookups: vec![],
                        }),
                    }),
                    meta: Some(TransactionStatusMeta {
                        loaded_writable_addresses: vec![],
                        loaded_readonly_addresses: vec![tracked_program.to_vec()],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            })),
            ..Default::default()
        };

        assert!(SubscriptionFilter::program_bytes(tracked_program).matches_update(&update));
        if let Some(UpdateOneof::Transaction(tx_update)) = &update.update_oneof {
            assert!(transaction_mentions_account_key(
                tx_update,
                &tracked_program
            ));
        } else {
            panic!("expected transaction update");
        }
    }

    #[test]
    fn program_filter_matches_decoded_transaction_when_program_is_only_in_accounts() {
        let tracked_program = [9u8; 32];
        let event = GeyserEvent::Transaction(TransactionUpdate {
            timestamp_unix_ms: 1,
            slot: 1,
            signature: vec![1; 64],
            fee: 5_000,
            success: true,
            accounts: vec![tracked_program.to_vec()],
            program_ids: vec![],
            log_messages: vec![],
        });

        assert!(SubscriptionFilter::program_bytes(tracked_program).matches(&event));
    }
}

#[cfg(test)]
mod tests {
    use super::{GeyserConfig, GeyserConsumer, SubscriptionFilter};
    use crate::geyser::decoder::GeyserEvent;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn forwards_only_events_that_match_filters() {
        // Filter on Orca program (already in fixture with real 32-byte pubkey)
        let orca_program_id = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
        let consumer = GeyserConsumer::new(GeyserConfig {
            endpoint: "mock://geyser".to_string(),
            channel_capacity: 4,
            filters: vec![
                SubscriptionFilter::program(orca_program_id.to_string()).expect("valid base58"),
                SubscriptionFilter::slots(),
            ],
        });
        let (sender, receiver) = sync_channel(4);

        // Fixture already uses real 32-byte pubkeys - no manual patching needed
        let fixture = GeyserConsumer::simulated_fixture();

        let forwarded = consumer
            .forward_events(&sender, fixture)
            .expect("channel open");
        drop(sender);

        let events: Vec<GeyserEvent> = receiver.iter().collect();

        // Should forward: AccountUpdate (Orca program), Transaction (Orca + Token),
        //               SlotUpdate (processed), SlotUpdate (confirmed)
        // Should NOT forward: AccountUpdate (System program - not in filter)
        assert_eq!(forwarded, 4);
        assert_eq!(events.len(), 4);
    }

    #[test]
    fn account_subscription_matches_transactions() {
        // Verify that account-based subscriptions now match transactions involving that account
        let tracked_account = "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU";
        let consumer = GeyserConsumer::new(GeyserConfig {
            endpoint: "mock://geyser".to_string(),
            channel_capacity: 4,
            filters: vec![
                SubscriptionFilter::account(tracked_account.to_string()).expect("valid base58")
            ],
        });
        let (sender, receiver) = sync_channel(4);

        let fixture = GeyserConsumer::simulated_fixture();

        let forwarded = consumer
            .forward_events(&sender, fixture)
            .expect("channel open");
        drop(sender);

        let events: Vec<GeyserEvent> = receiver.iter().collect();

        // Should forward: AccountUpdate (tracked account), Transaction (involves tracked account)
        // Should NOT forward: AccountUpdate (ignored account), SlotUpdates
        assert_eq!(
            forwarded, 2,
            "Should forward account update and transaction"
        );
        assert_eq!(events.len(), 2);

        // Verify we got both the account update and the transaction
        assert!(matches!(events[0], GeyserEvent::AccountUpdate(_)));
        assert!(matches!(events[1], GeyserEvent::Transaction(_)));
    }
}
