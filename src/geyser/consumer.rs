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
        vec![
            GeyserEvent::AccountUpdate(AccountUpdate {
                timestamp_unix_ms: 1_710_000_000_000,
                slot: 9_001,
                pubkey: "tracked-account".to_string(),
                owner: "amm-program".to_string(),
                lamports: 42,
                write_version: 7,
                data: vec![1, 2, 3, 4],
            }),
            GeyserEvent::Transaction(TransactionUpdate {
                timestamp_unix_ms: 1_710_000_000_001,
                slot: 9_001,
                signature: "tracked-signature".to_string(),
                fee: 5_000,
                success: true,
                program_ids: vec!["amm-program".to_string(), "token-program".to_string()],
                log_messages: vec!["swap".to_string(), "settled".to_string()],
            }),
            GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms: 1_710_000_000_002,
                slot: 9_001,
                parent_slot: Some(9_000),
                status: "processed".to_string(),
            }),
            GeyserEvent::AccountUpdate(AccountUpdate {
                timestamp_unix_ms: 1_710_000_000_003,
                slot: 9_001,
                pubkey: "ignored-account".to_string(),
                owner: "noise-program".to_string(),
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
                update.owner == *program
            }
            (SubscriptionFilter::Program(program), GeyserEvent::Transaction(update)) => update
                .program_ids
                .iter()
                .any(|program_id| program_id == program),
            (SubscriptionFilter::Account(account), GeyserEvent::AccountUpdate(update)) => {
                update.pubkey == *account
            }
            (SubscriptionFilter::Slots, GeyserEvent::SlotUpdate(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::SlotUpdate(_)) => false,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GeyserConfig, GeyserConsumer, SubscriptionFilter};
    use crate::geyser::decoder::GeyserEvent;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn forwards_only_events_that_match_filters() {
        let consumer = GeyserConsumer::new(GeyserConfig {
            endpoint: "mock://geyser".to_string(),
            channel_capacity: 4,
            filters: vec![
                SubscriptionFilter::Program("amm-program".to_string()),
                SubscriptionFilter::Slots,
            ],
        });
        let (sender, receiver) = sync_channel(4);

        let forwarded = consumer
            .forward_events(&sender, GeyserConsumer::simulated_fixture())
            .expect("channel open");
        drop(sender);

        let events: Vec<GeyserEvent> = receiver.iter().collect();

        assert_eq!(forwarded, 3);
        assert_eq!(events.len(), 3);
    }
}
