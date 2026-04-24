use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use solana_realtime_indexer::api;
use solana_realtime_indexer::api::rest::ApiSnapshot;
use solana_realtime_indexer::config::Config;
use solana_realtime_indexer::geyser::client::GeyserClient;
use solana_realtime_indexer::geyser::consumer::{GeyserConfig, SubscriptionFilter};
use solana_realtime_indexer::geyser::{Protocol, load_protocols_from_dir, merge_subscriptions};
use solana_realtime_indexer::geyser::wal_queue::WalQueue;
use solana_realtime_indexer::geyser::wal_consumer::{WalPipelineConfig, WalPipelineRunner, RpcGapFiller};
use solana_realtime_indexer::processor::batch_writer::BatchWriter;
use solana_realtime_indexer::processor::cpi_decoder::CpiLogDecoder;
use solana_realtime_indexer::processor::decoder::{CustomDecoder, Type1Decoder};
use solana_realtime_indexer::processor::pipeline::PipelineReport;
use solana_realtime_indexer::processor::sink::{
    DryRunStorageSink, StorageSink, TimescaleStorageSink,
};
use solana_realtime_indexer::processor::store::{RetentionPolicy, Type1Store};
use solana_realtime_indexer::PROJECT_NAME;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Mutex;

type SharedSnapshot = Arc<Mutex<ApiSnapshot>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    env_logger::init();

    let config = Config::from_env();

    run_with_real_geyser(config).await?;

    Ok(())
}

async fn run_with_real_geyser(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Starting Solana Realtime Indexer with real Geyser connection (WAL mode)");

    let api_key = config
        .geyser_api_key
        .clone()
        .ok_or("Geyser API key must be set for real Geyser connection")?;

    // Load protocols from TOML files or fall back to env vars
    let protocols = load_protocols(&config)?;

    // Merge all protocol subscriptions into one (deduplicates shared programs like Token)
    let merged_subscription = merge_subscriptions(&protocols);

    log::info!("Merged subscription: {} programs, {} accounts, slots: {}",
              merged_subscription.program_ids.len(),
              merged_subscription.account_pubkeys.len(),
              merged_subscription.include_slots);

    // Convert merged [u8; 32] pubkeys to base58 strings for SubscriptionFilter
    let mut filters = Vec::new();
    for program_id in &merged_subscription.program_ids {
        filters.push(SubscriptionFilter::Program(bs58::encode(program_id).into_string()));
    }
    for account in &merged_subscription.account_pubkeys {
        filters.push(SubscriptionFilter::Account(bs58::encode(account).into_string()));
    }
    if merged_subscription.include_slots {
        filters.push(SubscriptionFilter::Slots);
    }

    let geyser_config = GeyserConfig::new(
        config
            .geyser_endpoint
            .clone()
            .ok_or("Geyser endpoint must be set")?,
        config.geyser_channel_capacity,
        filters,
    );

    let storage_mode = storage_mode(&config);
    let api_state = initial_api_state(&config, storage_mode).await;

    // Create API connection pool if database URL is available
    if let Some(database_url) = config.database_url.as_deref() {
        match PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                log::info!("API database pool connected successfully");
                // Attach pool to API state
                let mut state = api_state.lock().await;
                *state = state.clone().with_pool(pool);
            }
            Err(e) => {
                log::warn!("Failed to create API database pool: {}. Network stress endpoint will be unavailable.", e);
            }
        }
    }

    // Create WAL queue instead of bounded channel
    if config.clear_wal_on_startup {
        log::warn!("CLEAR_WAL_ON_STARTUP=true - removing existing WAL at: {}", config.wal_path);
        std::fs::remove_dir_all(&config.wal_path).unwrap_or_else(|e| {
            log::debug!("WAL directory didn't exist or couldn't be removed: {}", e);
        });
    }
    std::fs::create_dir_all(&config.wal_path).ok();
    let wal_queue = Arc::new(WalQueue::new(&config.wal_path)?);

    // Start WAL background flusher
    let _wal_flush_handle = wal_queue.clone().start_background_flush().await;

    // Configure WAL pipeline
    let wal_pipeline_config = WalPipelineConfig {
        wal_path: config.wal_path.clone(),
        poll_interval: Duration::from_millis(10), // Poll every 10ms
        batch_size: config.batch_size,
        batch_flush_ms: config.batch_flush_ms,
    };

    // Start RPC gap filler
    let gap_filler = RpcGapFiller::new(config.rpc_endpoints.clone(), wal_queue.clone());
    let gap_filler_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(e) = gap_filler.detect_and_fill_gaps().await {
                log::error!("Gap filler error: {}", e);
            }
        }
    });

    // Start Geyser client writing to WAL
    let geyser_client = GeyserClient::new(
        geyser_config,
        api_key,
        config.geyser_run_duration_seconds,
    );
    let wal_queue_clone = wal_queue.clone();
    let geyser_handle = tokio::spawn(async move {
        match geyser_client.connect_and_subscribe(&wal_queue_clone).await {
            Ok(events_count) => {
                log::info!("✅ Geyser client processed {} events (WAL mode)", events_count);
                let unprocessed = wal_queue_clone.get_unprocessed_count();
                log::info!("📝 WAL state: {} events written, {} unprocessed",
                          wal_queue_clone.get_total_written(), unprocessed);
            }
            Err(error) => log::error!("Geyser client error: {}", error),
        }
    });

    // Build database sink and batch writer
    let sink = build_sink(&config)?;
    let writer = BatchWriter::new(
        config.batch_size,
        Duration::from_millis(config.batch_flush_ms),
    );

    // Create custom decoders (protocol-specific decoders will be registered here)
    let custom_decoders: Vec<Box<dyn CustomDecoder>> = vec![
        Box::new(CpiLogDecoder::new()),
    ];
    let decoder = Type1Decoder::new();

    // Start WAL pipeline consumer (now handles batching and DB writes directly)
    let wal_runner = WalPipelineRunner::new(
        wal_queue.clone(),
        wal_pipeline_config,
        api_state.clone(),
        writer,
        decoder,
        custom_decoders,
        sink,
    );
    let wal_pipeline_handle = wal_runner.start_background_processor();

    log_background_wal_pipeline(wal_pipeline_handle);
    log_background_geyser(geyser_handle);
    log_background_gap_filler(gap_filler_handle);

    // Start WAL metrics reporter
    let _metrics_reporter = {
        let wal_queue = wal_queue.clone();
        let api_state = api_state.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                let wal_unprocessed = wal_queue.get_unprocessed_count();

                // Update API state with latest metrics
                let mut state = api_state.lock().await;
                state.wal_unprocessed_count = wal_unprocessed;

                // Channel utilization is no longer relevant - WAL is the only buffer
                state.channel_utilization = 0.0;
            }
        })
    };

    // Start slot latency materialized view refresh task
    let _latency_refresh_handle = if let Some(database_url) = config.database_url.as_deref() {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(database_url)
            .await;

        match pool {
            Ok(pool) => {
                log::info!("Starting slot latency materialized view refresh task (every 30s)");
                Some(tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(30));
                    loop {
                        interval.tick().await;

                        if let Err(e) = sqlx::query("SELECT refresh_slot_health_1m()")
                            .execute(&pool)
                            .await
                        {
                            log::warn!("Failed to refresh slot_health_1m: {}", e);
                        }
                    }
                }))
            }
            Err(e) => {
                log::warn!("Failed to create pool for latency refresh: {}", e);
                None
            }
        }
    } else {
        None
    };

    log::info!("🚀 Indexer running in WAL mode:");
    log::info!("   - No event drops (correctness guaranteed)");
    log::info!("   - No blocking (OS buffer + disk storage)");
    log::info!("   - Gap detection + RPC fallback enabled");
    log::info!("   - WAL path: {}", config.wal_path);
    log::info!("   - WAL metrics exposed via /api/metrics");

    serve_api(config.bind_address, api_state).await
}

fn build_sink(config: &Config) -> Result<Box<dyn StorageSink>, Box<dyn std::error::Error>> {
    let retention_policy = RetentionPolicy {
        max_age: Duration::from_secs(60),
    };

    if let Some(database_url) = config.database_url.as_deref() {
        Ok(Box::new(TimescaleStorageSink::connect_with_pool_size(
            database_url,
            Type1Store::new(retention_policy),
            config.db_pool_max_connections,
        )?))
    } else {
        Ok(Box::new(DryRunStorageSink::new(Type1Store::new(
            retention_policy,
        ))))
    }
}

async fn serve_api(
    bind_address: String,
    api_state: SharedSnapshot,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = api::router_with_state(api_state);
    let tcp_listener = tokio::net::TcpListener::bind(&bind_address).await?;
    log::info!("API server listening on {}", bind_address);

    axum::serve(tcp_listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install CTRL+C handler");
        })
        .await?;

    Ok(())
}

async fn initial_api_state(config: &Config, storage_mode: &str) -> SharedSnapshot {
    Arc::new(Mutex::new(ApiSnapshot::from_report(
        PROJECT_NAME,
        storage_mode,
        config.bind_address.clone(),
        config.rpc_endpoints.len(),
        0,
        Duration::from_secs(0),
        unix_now_millis(),
        PipelineReport::default(),
    )
    .with_runtime_config(
        config.geyser_channel_capacity,
        config.batch_size,
        config.batch_flush_ms,
    )))
}

fn storage_mode(config: &Config) -> &'static str {
    if config.database_url.is_some() {
        "timescale"
    } else {
        "dry-run"
    }
}

fn log_background_geyser(handle: tokio::task::JoinHandle<()>) {
    tokio::spawn(async move {
        if let Err(error) = handle.await {
            log::error!("Geyser task join failed: {}", error);
        }
    });
}

fn log_background_wal_pipeline(handle: tokio::task::JoinHandle<Result<PipelineReport, String>>) {
    tokio::spawn(async move {
        match handle.await {
            Ok(Ok(report)) => log::info!("WAL Pipeline completed: {:?}", report),
            Ok(Err(error)) => log::error!("WAL Pipeline failed: {}", error),
            Err(error) => log::error!("WAL Pipeline task join failed: {}", error),
        }
    });
}

fn log_background_gap_filler(handle: tokio::task::JoinHandle<()>) {
    tokio::spawn(async move {
        if let Err(error) = handle.await {
            log::error!("Gap filler task join failed: {}", error);
        }
    });
}

fn unix_now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

/// Load protocols from TOML files in the protocols/ directory.
/// Fails explicitly if no protocol configs are found - no silent fallback.
fn load_protocols(_config: &Config) -> Result<Vec<Box<dyn Protocol>>, Box<dyn std::error::Error>> {
    let protocols_dir = "protocols";

    // Check if protocols directory exists
    if !std::path::Path::new(protocols_dir).exists() {
        return Err(format!(
            "Protocols directory '{}' does not exist. \
             Create a 'protocols/' directory with at least one .toml configuration file. \
             See protocols/README.md for examples.",
            protocols_dir
        ).into());
    }

    // Load all TOML files using the protocol module's directory scanner
    let config_only_protocols = load_protocols_from_dir(protocols_dir)
        .map_err(|e| format!("Failed to load protocols from directory '{}': {}", protocols_dir, e))?;

    if config_only_protocols.is_empty() {
        return Err(format!(
            "No protocol .toml files found in '{}' directory. \
             Add at least one protocol configuration file (e.g., raydium.toml). \
             See protocols/README.md for examples.",
            protocols_dir
        ).into());
    }

    // Convert ConfigOnlyProtocol to Box<dyn Protocol>
    let protocols: Vec<Box<dyn Protocol>> = config_only_protocols
        .into_iter()
        .map(|p| Box::new(p) as Box<dyn Protocol>)
        .collect();

    Ok(protocols)
}
