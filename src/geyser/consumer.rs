use std::sync::mpsc::{SendError, SyncSender};

use crate::geyser::decoder::{AccountUpdate, GeyserEvent, SlotUpdate, TransactionUpdate};

#[derive(Debug, Clone)]
pub struct GeyserConfig {
    pub endpoint: String,
    pub channel_capacity: usize,
    pub filters: Vec<SubscriptionFilter>,
}

impl GeyserConfig {
    pub fn new(endpoint: String, channel_capacity: usize, filters: Vec<SubscriptionFilter>) -> Self {
        Self {
            endpoint,
            channel_capacity,
            filters,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SubscriptionFilter {
    Program(String),
    Account(String),
    Slots,
    Blocks,
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
        let tracked_account = bs58::decode("7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU").into_vec().unwrap();
        let orca_program = bs58::decode("9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM").into_vec().unwrap();
        let token_program = bs58::decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").into_vec().unwrap();
        let tracked_signature = bs58::decode("5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprv2W1qY1qrk7jjFgJG3kGpYcQHxUxYWBZgmhnfYTLuLr").into_vec().unwrap();
        let ignored_account = bs58::decode("Noise1111111111111111111111111111111111111111").into_vec().unwrap();
        let system_program = bs58::decode("11111111111111111111111111111111").into_vec().unwrap();

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
            (SubscriptionFilter::Program(program), GeyserEvent::AccountUpdate(update)) => {
                let program_bytes = program_filter_bytes(program);
                update.owner == program_bytes
            }
            (SubscriptionFilter::Program(program), GeyserEvent::Transaction(update)) => {
                let program_bytes = program_filter_bytes(program);
                update
                    .program_ids
                    .iter()
                    .any(|program_id_bytes| program_id_bytes == &program_bytes)
            }
            (SubscriptionFilter::Account(account), GeyserEvent::AccountUpdate(update)) => {
                let account_bytes = program_filter_bytes(account);
                update.pubkey == account_bytes
            }
            (SubscriptionFilter::Slots, GeyserEvent::SlotUpdate(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::BlockMeta(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::SlotUpdate(_)) => false,
            _ => false,
        }
    }
}

fn program_filter_bytes(program: &str) -> Vec<u8> {
    let trimmed = program.trim();
    bs58::decode(trimmed)
        .into_vec()
        .unwrap_or_else(|e| {
            panic!(
                "Invalid base58 in program_filter_bytes for '{}': {}. \
                 Config validation should have caught this at startup.",
                trimmed, e
            )
        })
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
                SubscriptionFilter::Program(orca_program_id.to_string()),
                SubscriptionFilter::Slots,
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
}
