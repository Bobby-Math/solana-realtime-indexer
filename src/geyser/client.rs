use futures::StreamExt;
use std::sync::mpsc::SyncSender;
use helius_laserstream::{subscribe, LaserstreamConfig, ChannelOptions};
use helius_laserstream::grpc::{SubscribeRequest, SubscribeUpdate};
use futures::pin_mut;

use crate::geyser::consumer::GeyserConfig;
use crate::geyser::decoder::{GeyserEvent, TransactionUpdate};

// Import tokio::time if we need timeout functionality
use tokio::time as tokio_time;

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
        sender: SyncSender<GeyserEvent>,
    ) -> Result<u64, GeyserClientError> {
        let endpoint = self.config.endpoint.clone();

        // Set timeout if configured
        let timeout_duration = self.run_duration_seconds.map(std::time::Duration::from_secs);
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
                        // Convert Helius data to GeyserEvent
                        if let Some(event) = self.handle_helius_data(data) {
                            if sender.send(event).is_err() {
                                log::error!("Channel closed, stopping Geyser client");
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
                        // Convert Helius data to GeyserEvent
                        if let Some(event) = self.handle_helius_data(data) {
                            if sender.send(event).is_err() {
                                log::error!("Channel closed, stopping Geyser client");
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
        let request = SubscribeRequest {
            commitment: Some(2), // 2 = Confirmed commitment level
            ..Default::default()
        };

        // For now, use a simple subscription request
        // TODO: Add transaction and account filters when Helius SDK documentation is clear
        if !self.config.filters.is_empty() {
            log::info!("Applying {} filters (basic subscription for now)", self.config.filters.len());
        }

        Ok(request)
    }

    fn handle_helius_data(&self, data: SubscribeUpdate) -> Option<GeyserEvent> {
        let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();

        // For now, just log that we received data and create a placeholder event
        // TODO: Implement proper conversion once we understand the Helius SDK types
        log::info!("Received Helius data: {:?}", data);

        // Create a placeholder transaction event for now
        Some(GeyserEvent::Transaction(TransactionUpdate {
            timestamp_unix_ms,
            slot: 0,
            signature: format!("{:?}", data),
            fee: 5000,
            success: true,
            program_ids: vec![],
            log_messages: vec![],
        }))
    }
}