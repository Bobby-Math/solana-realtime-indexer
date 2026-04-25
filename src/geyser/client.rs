use futures::StreamExt;
use helius_laserstream::{subscribe, LaserstreamConfig, ChannelOptions};
use helius_laserstream::grpc::{SubscribeRequest, SubscribeUpdate};
use helius_laserstream::grpc::subscribe_update::UpdateOneof;
use futures::pin_mut;
use prost::Message;

use crate::geyser::consumer::GeyserConfig;
use crate::geyser::decoder::{GeyserEvent, decode_subscribe_update};
use crate::geyser::wal_queue::WalQueue;
use crate::geyser::reconnect::ReconnectPolicy;

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
    reconnect_policy: ReconnectPolicy,
}

impl GeyserClient {
    pub fn new(config: GeyserConfig, api_key: String, run_duration_seconds: Option<u64>) -> Self {
        Self {
            config,
            api_key,
            run_duration_seconds,
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    pub async fn connect_and_subscribe(
        &self,
        wal_queue: &WalQueue,
    ) -> Result<u64, GeyserClientError> {
        let mut total_events_processed = 0u64;
        let mut reconnect_count = 0u64;

        loop {
            match self.run_single_session(wal_queue).await {
                Ok(events_processed) => {
                    total_events_processed += events_processed;

                    // Check if this was a graceful shutdown (timeout) or an error
                    if self.run_duration_seconds.is_some() && self.run_duration_seconds != Some(0) {
                        // Run duration limit reached - exit gracefully
                        log::info!("✅ Geyser client shutting down after run duration limit");
                        return Ok(total_events_processed);
                    }

                    // CRITICAL FIX: Reset reconnect_count after successful session
                    // This prevents backoff from ratcheting up permanently across sessions
                    reconnect_count = 0;

                    // Stream ended normally - this is unexpected, try to reconnect
                    log::warn!("Geyser stream ended unexpectedly, attempting to reconnect...");
                }
                Err(e) => {
                    // Connection error - reconnect with backoff
                    log::error!("Geyser connection error: {}", e);

                    // Calculate backoff delay with exponential increase
                    let backoff_ms = self.reconnect_policy.initial_backoff_ms
                        .saturating_mul(2u64.pow(reconnect_count.min(8) as u32))
                        .min(self.reconnect_policy.max_backoff_ms);

                    reconnect_count += 1;
                    log::info!("Waiting {}ms before reconnect attempt {} (max: {}ms)",
                              backoff_ms, reconnect_count, self.reconnect_policy.max_backoff_ms);

                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;

                    log::info!("Reconnecting to Geyser (attempt {})...", reconnect_count);
                }
            }
        }
    }

    async fn run_single_session(
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
                        let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();
                        if let Some(event) = decode_subscribe_update(&data, timestamp_unix_ms) {
                            // Apply client-side filtering to ensure event matches configured filters
                            if !self.event_matches_filters(&event) {
                                log::trace!("Event filtered out by client-side filters: {:?}", event);
                                continue;
                            }

                            // Encode once and write raw bytes to WAL - eliminates re-encode overhead
                            let raw_protobuf_bytes = data.encode_to_vec();
                            if let Err(e) = wal_queue.push_raw_bytes(slot, &raw_protobuf_bytes) {
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
                        let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();
                        if let Some(event) = decode_subscribe_update(&data, timestamp_unix_ms) {
                            // Apply client-side filtering to ensure event matches configured filters
                            if !self.event_matches_filters(&event) {
                                log::trace!("Event filtered out by client-side filters: {:?}", event);
                                continue;
                            }

                            // Encode once and write raw bytes to WAL - eliminates re-encode overhead
                            let raw_protobuf_bytes = data.encode_to_vec();
                            if let Err(e) = wal_queue.push_raw_bytes(slot, &raw_protobuf_bytes) {
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
        let mut has_blocks_meta_filters = false;

        for filter in &self.config.filters {
            match filter {
                SubscriptionFilter::Account(pubkey, _bytes) => {
                    account_pubkeys.push(pubkey.clone());
                }
                SubscriptionFilter::Program(program_id, _bytes) => {
                    program_ids.push(program_id.clone());
                }
                SubscriptionFilter::Slots => {
                    has_slot_filters = true;
                }
                SubscriptionFilter::Blocks => {
                    // Subscribe to blocks_meta to get block_time
                    has_blocks_meta_filters = true;
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

        // Subscribe to blocks_meta to get block_time for accurate latency measurements
        if has_blocks_meta_filters {
            let mut blocks_meta_map = HashMap::new();
            blocks_meta_map.insert("all_blocks_meta".to_string(), helius_laserstream::grpc::SubscribeRequestFilterBlocksMeta {
                ..Default::default()
            });
            request.blocks_meta = blocks_meta_map;
            log::info!("Subscribed to blocks_meta updates for block_time extraction");
        }

        log::info!("Built SubscribeRequest with {} account subscriptions, {} transaction subscriptions, {} slot subscriptions, {} blocks_meta subscriptions",
                  request.accounts.len(), request.transactions.len(), request.slots.len(), request.blocks_meta.len());

        Ok(request)
    }

    fn event_matches_filters(&self, event: &GeyserEvent) -> bool {
        use crate::geyser::consumer::SubscriptionFilter;

        // If no filters configured, accept all events
        if self.config.filters.is_empty() {
            return true;
        }

        // Check if any filter matches this event
        // Note: Filters are pre-validated and contain parsed bytes, no parsing needed here
        self.config.filters.iter().any(|filter| match (filter, event) {
            (SubscriptionFilter::Program(_program_str, program_bytes), GeyserEvent::AccountUpdate(update)) => {
                update.owner == *program_bytes
            }
            (SubscriptionFilter::Program(_program_str, program_bytes), GeyserEvent::Transaction(update)) => {
                update
                    .program_ids
                    .iter()
                    .any(|id_bytes| id_bytes == program_bytes)
            }
            (SubscriptionFilter::Account(_account_str, account_bytes), GeyserEvent::AccountUpdate(update)) => {
                update.pubkey == *account_bytes
            }
            (SubscriptionFilter::Account(_account_str, account_bytes), GeyserEvent::Transaction(update)) => {
                update.accounts.iter().any(|acc| acc == account_bytes)
            }
            (SubscriptionFilter::Slots, GeyserEvent::SlotUpdate(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::BlockMeta(_)) => true,
            (SubscriptionFilter::Blocks, GeyserEvent::SlotUpdate(_)) => false,
            _ => false,
        })
    }

    fn extract_slot(&self, data: &SubscribeUpdate) -> u64 {
        match &data.update_oneof {
            Some(UpdateOneof::Account(acc)) => acc.slot,
            Some(UpdateOneof::Transaction(tx)) => tx.slot,
            Some(UpdateOneof::Slot(slot)) => slot.slot,
            Some(UpdateOneof::BlockMeta(bm)) => bm.slot,
            Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => 0,
            Some(UpdateOneof::Block(_)) | Some(UpdateOneof::TransactionStatus(_)) | Some(UpdateOneof::Entry(_)) => 0,
            None => 0,
        }
    }

}

