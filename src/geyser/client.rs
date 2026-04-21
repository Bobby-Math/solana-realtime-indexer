use futures::StreamExt;
use helius_laserstream::{subscribe, LaserstreamConfig, ChannelOptions};
use helius_laserstream::grpc::{SubscribeRequest, SubscribeUpdate};
use helius_laserstream::grpc::subscribe_update::UpdateOneof;
use futures::pin_mut;

use crate::geyser::consumer::GeyserConfig;
use crate::geyser::decoder::{GeyserEvent, TransactionUpdate, AccountUpdate, SlotUpdate};
use crate::geyser::wal_queue::WalQueue;

#[derive(Debug)]
pub enum GeyserClientError {
    Config(String),
    Connection(String),
    Stream(String),
    Send(String),
}

impl std::fmt::Display for GeyserClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeyserClientError::Config(msg) => write!(f, "Configuration error: {}", msg),
            GeyserClientError::Connection(msg) => write!(f, "Connection error: {}", msg),
            GeyserClientError::Stream(msg) => write!(f, "Stream error: {}", msg),
            GeyserClientError::Send(msg) => write!(f, "Send error: {}", msg),
        }
    }
}

impl std::error::Error for GeyserClientError {}

pub struct GeyserClient {
    config: GeyserConfig,
    api_key: String,
    run_duration_seconds: Option<u64>,
}

impl GeyserClient {
    pub fn new(config: GeyserConfig, api_key: String, run_duration_seconds: Option<u64>) -> Self {
        Self { config, api_key, run_duration_seconds }
    }

    pub async fn connect_and_subscribe(
        &self,
        wal_queue: &WalQueue,
    ) -> Result<u64, GeyserClientError> {
        let endpoint = self.config.endpoint.clone();

        // Set timeout if configured (0 means run indefinitely)
        let timeout_duration = self.run_duration_seconds
            .filter(|&secs| secs > 0)  // 0 means run indefinitely
            .map(std::time::Duration::from_secs);
        let start_time = std::time::Instant::now();

        // Build Helius Laserstream configuration
        let helius_config = LaserstreamConfig {
            api_key: self.api_key.clone(),
            endpoint: endpoint.clone(),
            channel_options: ChannelOptions::default(),
            max_reconnect_attempts: Some(5),
            replay: false,
        };

        // Build subscription request from filters
        let subscribe_request = self.build_subscription_request()?;

        log::info!("Connected to Helius Laserstream at {}, subscribing...", endpoint);

        // Log timeout configuration
        if let Some(timeout_duration) = timeout_duration {
            log::info!("Run duration limit: {:?} - will stop after this time to conserve credits", timeout_duration);
        } else {
            log::info!("No run duration limit - will run continuously (consuming Helius credits)");
        }

        // Create the subscription stream (subscribe returns immediately with stream and handle)
        let (stream, _handle) = subscribe(helius_config, subscribe_request);

        // Pin the stream so it can be used with .next().await
        pin_mut!(stream);

        let mut events_processed = 0u64;
        let mut last_log_time = start_time;
        let log_interval = std::time::Duration::from_secs(5); // Log progress every 5 seconds

        // Process incoming messages with timeout
        loop {
            // Check if we've exceeded the configured runtime
            if let Some(timeout_duration) = timeout_duration {
                if start_time.elapsed() >= timeout_duration {
                    log::info!("✅ Run duration limit reached: {:?}. Processed {} events. Stopping to conserve Helius credits.",
                              timeout_duration, events_processed);
                    break;
                }

                // Calculate remaining time for timeout
                let remaining = timeout_duration.saturating_sub(start_time.elapsed());

                // Use tokio timeout to avoid blocking forever
                let maybe_data = tokio::time::timeout(remaining, stream.next()).await;

                match maybe_data {
                    Ok(Some(Ok(data))) => {
                        // Extract slot for WAL ordering and convert to GeyserEvent for filtering
                        let slot = self.extract_slot(&data);
                        if let Some(event) = self.handle_helius_data(data.clone()) {
                            // Apply client-side filtering to ensure event matches configured filters
                            if !self.event_matches_filters(&event) {
                                log::trace!("Event filtered out by client-side filters: {:?}", event);
                                continue;
                            }

                            // Write raw protobuf bytes to WAL - non-blocking, no drops
                            if let Err(e) = wal_queue.push_update(slot, &data) {
                                log::error!("WAL write error: {}, stopping Geyser client", e);
                                break;
                            }
                            events_processed += 1;

                            // Log progress every 5 seconds
                            let now = std::time::Instant::now();
                            if now.duration_since(last_log_time) >= log_interval {
                                let elapsed = now.duration_since(start_time);
                                let events_per_sec = events_processed as f64 / elapsed.as_secs_f64();
                                log::info!("📊 Progress: {:.1}s elapsed, {} events processed ({:.1} events/sec)",
                                          elapsed.as_secs_f64(), events_processed, events_per_sec);
                                last_log_time = now;
                            }
                        }
                    }
                    Ok(Some(Err(e))) => {
                        log::error!("Stream error: {:?}", e);
                        return Err(GeyserClientError::Stream(format!("{:?}", e)));
                    }
                    Ok(None) => {
                        log::info!("Stream ended normally");
                        break;
                    }
                    Err(_) => {
                        // Timeout reached
                        let elapsed = start_time.elapsed();
                        log::info!("✅ Run duration timeout reached: {:?}. Processed {} events. Stopping to conserve Helius credits.",
                                  elapsed, events_processed);
                        break;
                    }
                }
            } else {
                // No timeout - run forever
                match stream.next().await {
                    Some(Ok(data)) => {
                        // Extract slot for WAL ordering and convert to GeyserEvent for filtering
                        let slot = self.extract_slot(&data);
                        if let Some(event) = self.handle_helius_data(data.clone()) {
                            // Apply client-side filtering to ensure event matches configured filters
                            if !self.event_matches_filters(&event) {
                                log::trace!("Event filtered out by client-side filters: {:?}", event);
                                continue;
                            }

                            // Write raw protobuf bytes to WAL - non-blocking, no drops
                            if let Err(e) = wal_queue.push_update(slot, &data) {
                                log::error!("WAL write error: {}, stopping Geyser client", e);
                                break;
                            }
                            events_processed += 1;

                            // Log progress every 5 seconds
                            let now = std::time::Instant::now();
                            if now.duration_since(last_log_time) >= log_interval {
                                let elapsed = now.duration_since(start_time);
                                let events_per_sec = events_processed as f64 / elapsed.as_secs_f64();
                                log::info!("📊 Progress: {:.1}s elapsed, {} events processed ({:.1} events/sec)",
                                          elapsed.as_secs_f64(), events_processed, events_per_sec);
                                last_log_time = now;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        log::error!("Stream error: {:?}", e);
                        return Err(GeyserClientError::Stream(format!("{:?}", e)));
                    }
                    None => {
                        log::info!("Stream ended normally");
                        break;
                    }
                }
            }
        }

        // Final summary
        let total_elapsed = start_time.elapsed();
        log::info!("📈 RUN SUMMARY:");
        log::info!("   Total runtime: {:.2}s", total_elapsed.as_secs_f64());
        log::info!("   Events processed: {}", events_processed);
        if events_processed > 0 {
            let events_per_sec = events_processed as f64 / total_elapsed.as_secs_f64();
            log::info!("   Average rate: {:.1} events/sec", events_per_sec);
        } else {
            log::info!("   Note: No events received - this is normal on devnet or with restrictive filters");
        }
        log::info!("   Credits consumed: ~{:.1}s connection time", total_elapsed.as_secs_f64());

        Ok(events_processed)
    }

    fn build_subscription_request(&self) -> Result<SubscribeRequest, GeyserClientError> {
        use crate::geyser::consumer::SubscriptionFilter;
        use helius_laserstream::grpc::{SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions};
        use std::collections::HashMap;

        let mut request = SubscribeRequest {
            commitment: Some(2), // 2 = Confirmed commitment level
            ..Default::default()
        };

        // Collect configured filters
        let mut account_pubkeys: Vec<String> = Vec::new();
        let mut program_ids: Vec<String> = Vec::new();
        let mut has_slot_filters = false;

        for filter in &self.config.filters {
            match filter {
                SubscriptionFilter::Account(pubkey) => {
                    account_pubkeys.push(pubkey.clone());
                }
                SubscriptionFilter::Program(program_id) => {
                    program_ids.push(program_id.clone());
                }
                SubscriptionFilter::Slots => {
                    has_slot_filters = true;
                }
                SubscriptionFilter::Blocks => {
                    // Blocks metadata - not directly supported in basic Laserstream filters
                    log::warn!("Blocks filter not directly supported, will be handled via client-side filtering");
                }
            }
        }

        // Build accounts subscription with server-side filtering
        let has_account_filters = !account_pubkeys.is_empty() || !program_ids.is_empty();

        if has_account_filters {
            let mut accounts_map = HashMap::new();
            accounts_map.insert("filtered_accounts".to_string(), SubscribeRequestFilterAccounts {
                account: account_pubkeys.clone(),
                owner: program_ids.clone(),
                filters: vec![],
                nonempty_txn_signature: Some(false),
            });
            request.accounts = accounts_map;
            log::info!("Subscribed to filtered account updates: {} specific accounts, {} program owners",
                      request.accounts.get("filtered_accounts").map(|f| f.account.len()).unwrap_or(0),
                      request.accounts.get("filtered_accounts").map(|f| f.owner.len()).unwrap_or(0));
        } else {
            // No account filters configured - subscribe to all accounts (expensive!)
            let mut accounts_map = HashMap::new();
            accounts_map.insert("all_accounts".to_string(), SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: Some(false),
            });
            request.accounts = accounts_map;
            log::warn!("No account filters configured - subscribing to ALL account updates (expensive!)");
        }

        // Build transactions subscription with server-side filtering
        // Note: Helius transaction filtering is more limited, so we may need client-side filtering too
        let mut transactions_map = HashMap::new();
        transactions_map.insert("filtered_transactions".to_string(), SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: account_pubkeys.clone(), // Include transactions mentioning filtered accounts
            account_exclude: vec![],
            account_required: vec![],
        });
        request.transactions = transactions_map;
        log::info!("Subscribed to transaction updates with {} account filters",
                  request.transactions.get("filtered_transactions").map(|f| f.account_include.len()).unwrap_or(0));

        // Subscribe to slots if slot filters are configured
        if has_slot_filters {
            let mut slots_map = HashMap::new();
            slots_map.insert("all_slots".to_string(), SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            });
            request.slots = slots_map;
            log::info!("Subscribed to slot updates");
        }

        log::info!("Built SubscribeRequest with {} account subscriptions, {} transaction subscriptions, {} slot subscriptions",
                  request.accounts.len(), request.transactions.len(), request.slots.len());

        Ok(request)
    }

    fn event_matches_filters(&self, event: &GeyserEvent) -> bool {
        use crate::geyser::consumer::SubscriptionFilter;

        // If no filters configured, accept all events
        if self.config.filters.is_empty() {
            return true;
        }

        // Check if any filter matches this event
        self.config.filters.iter().any(|filter| match (filter, event) {
            (SubscriptionFilter::Program(program_id), GeyserEvent::AccountUpdate(update)) => {
                let program_id_bytes = program_filter_bytes(program_id);
                update.owner == program_id_bytes
            }
            (SubscriptionFilter::Program(program_id), GeyserEvent::Transaction(update)) => {
                let program_id_bytes = program_filter_bytes(program_id);
                update
                    .program_ids
                    .iter()
                    .any(|id_bytes| id_bytes == &program_id_bytes)
            }
            (SubscriptionFilter::Account(pubkey), GeyserEvent::AccountUpdate(update)) => {
                let pubkey_bytes = program_filter_bytes(pubkey);
                update.pubkey == pubkey_bytes
            }
            (SubscriptionFilter::Account(_pubkey), GeyserEvent::Transaction(_update)) => {
                // For transactions, we'd need to check account keys - this is a simplified check
                // In production, you'd parse the transaction's account list
                false // Placeholder - requires full transaction parsing
            }
            (SubscriptionFilter::Slots, GeyserEvent::SlotUpdate(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::SlotUpdate(_)) => false,
            _ => false,
        })
    }

    fn extract_slot(&self, data: &SubscribeUpdate) -> u64 {
        match &data.update_oneof {
            Some(UpdateOneof::Account(acc)) => acc.slot,
            Some(UpdateOneof::Transaction(tx)) => tx.slot,
            Some(UpdateOneof::Slot(slot)) => slot.slot,
            Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => 0,
            Some(UpdateOneof::Block(_)) | Some(UpdateOneof::BlockMeta(_)) | Some(UpdateOneof::TransactionStatus(_)) | Some(UpdateOneof::Entry(_)) => 0,
            None => 0,
        }
    }

    fn handle_helius_data(&self, data: SubscribeUpdate) -> Option<GeyserEvent> {
        let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();

        // Parse the update based on its type
        match data.update_oneof? {
            UpdateOneof::Account(account_update) => {
                log::debug!("Received account update for slot {}", account_update.slot);

                // Extract account info if available
                if let Some(account_info) = account_update.account {
                    log::trace!("Account update: pubkey (bytes), owner (bytes), lamports={}, slot={}",
                              account_info.lamports, account_update.slot);

                    Some(GeyserEvent::AccountUpdate(AccountUpdate {
                        timestamp_unix_ms,
                        slot: account_update.slot,
                        pubkey: account_info.pubkey.clone(),
                        owner: account_info.owner.clone(),
                        lamports: account_info.lamports,
                        write_version: account_info.write_version,
                        data: account_info.data,
                    }))
                } else {
                    log::warn!("Account update missing account info for slot {}", account_update.slot);
                    None
                }
            }
            UpdateOneof::Transaction(tx_update) => {
                log::debug!("Received transaction update for slot {}", tx_update.slot);

                // Extract transaction info if available
                if let Some(tx_info) = tx_update.transaction {
                    // Determine success (err is None means success)
                    let success = tx_info.meta.as_ref()
                        .map(|meta| meta.err.is_none())
                        .unwrap_or(false);  // If no meta, assume failed

                    // Extract fee
                    let fee = tx_info.meta.as_ref()
                        .map(|meta| meta.fee)
                        .unwrap_or(0);

                    // Extract log messages
                    let log_messages = tx_info.meta.as_ref()
                        .map(|meta| meta.log_messages.clone())
                        .unwrap_or_default();

                    // Extract program_ids from transaction data
                    // In Solana transactions, each instruction has a program_id_index that points
                    // to the account key that is the program being invoked
                    let program_ids = if let Some(tx) = &tx_info.transaction {
                        // Access the message field which contains account_keys and instructions
                        if let Some(message) = &tx.message {
                            // Extract program IDs from instructions (keep as raw bytes, not base58 strings)
                            let mut invoked_programs = std::collections::HashSet::new();
                            for instruction in &message.instructions {
                                let program_idx = instruction.program_id_index as usize;
                                if program_idx < message.account_keys.len() {
                                    // Store as raw bytes directly for database compatibility
                                    invoked_programs.insert(message.account_keys[program_idx].clone());
                                }
                            }
                            invoked_programs.into_iter().collect()
                        } else {
                            log::debug!("No message field in transaction, program_ids will be empty");
                            Vec::new()
                        }
                    } else {
                        // Fallback: try to extract from transaction meta if available
                        // This is less reliable but better than nothing
                        log::debug!("No transaction field available, program_ids will be empty");
                        Vec::new()
                    };

                    log::trace!("Transaction update: signature (bytes), success={}, fee={}, slot={}",
                              success, fee, tx_update.slot);

                    Some(GeyserEvent::Transaction(TransactionUpdate {
                        timestamp_unix_ms,
                        slot: tx_update.slot,
                        signature: tx_info.signature.clone(),
                        fee,
                        success,
                        program_ids,
                        log_messages,
                    }))
                } else {
                    log::warn!("Transaction update missing transaction info for slot {}", tx_update.slot);
                    None
                }
            }
            UpdateOneof::Slot(slot_update) => {
                log::debug!("Received slot update: {}", slot_update.slot);
                Some(GeyserEvent::SlotUpdate(SlotUpdate {
                    timestamp_unix_ms,
                    slot: slot_update.slot,
                    parent_slot: slot_update.parent,
                    status: format!("{:?}", slot_update.status),
                }))
            }
            UpdateOneof::Ping(_) => {
                log::trace!("Received ping - ignoring");
                None
            }
            UpdateOneof::Pong(_) => {
                log::trace!("Received pong - ignoring");
                None
            }
            other => {
                log::warn!("Received unhandled update type: {:?}", std::mem::discriminant(&other));
                None
            }
        }
    }
}

fn program_filter_bytes(program_id: &str) -> Vec<u8> {
    bs58::decode(program_id)
        .into_vec()
        .unwrap_or_else(|_| program_id.as_bytes().to_vec())
}
