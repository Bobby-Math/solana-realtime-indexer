use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use solana_realtime_indexer::api;
use solana_realtime_indexer::api::rest::ApiSnapshot;
use solana_realtime_indexer::config::Config;
use solana_realtime_indexer::geyser::client::GeyserClient;
use solana_realtime_indexer::geyser::consumer::{GeyserConfig, GeyserConsumer};
use solana_realtime_indexer::processor::batch_writer::BatchWriter;
use solana_realtime_indexer::processor::decoder::{
    CustomDecoder, ProgramActivityDecoder, Type1Decoder,
};
use solana_realtime_indexer::processor::pipeline::ProcessorPipeline;
use solana_realtime_indexer::processor::sink::{
    DryRunStorageSink, StorageSink, TimescaleStorageSink,
};
use solana_realtime_indexer::processor::store::{RetentionPolicy, Type1Store};
use solana_realtime_indexer::PROJECT_NAME;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env file
    dotenvy::dotenv().ok();

    // Initialize logger
    env_logger::init();

    let config = Config::from_env();

    // Determine which mode to run in
    let use_real_geyser = config.geyser_endpoint.is_some()
        && !config.geyser_endpoint.as_ref().map(|e| e.starts_with("mock")).unwrap_or(false);

    if use_real_geyser {
        run_with_real_geyser(config)?;
    } else {
        run_with_simulated_data(config)?;
    }

    Ok(())
}

fn run_with_real_geyser(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Starting Solana Realtime Indexer with real Geyser connection");

    let api_key = config
        .geyser_api_key
        .clone()
        .expect("Geyser API key must be set for real Geyser connection");

    let geyser_config = GeyserConfig::new(
        config
            .geyser_endpoint
            .clone()
            .expect("Geyser endpoint must be set"),
        8,
        config.geyser_subscription_filters(),
    );

    let (sender, receiver) = sync_channel(geyser_config.channel_capacity);

    let custom_decoders: Vec<Box<dyn CustomDecoder>> =
        vec![Box::new(ProgramActivityDecoder::new("amm-program"))];

    let retention_policy = RetentionPolicy {
        max_age: Duration::from_secs(60),
    };

    let storage_mode = if config.database_url.is_some() {
        "timescale"
    } else {
        "dry-run"
    };

    let sink: Box<dyn StorageSink> = if let Some(database_url) = config.database_url.as_deref() {
        Box::new(TimescaleStorageSink::connect(
            database_url,
            Type1Store::new(retention_policy.clone()),
        )?)
    } else {
        Box::new(DryRunStorageSink::new(Type1Store::new(retention_policy)))
    };

    // Spawn Geyser client in async runtime
    let geyser_client = GeyserClient::new(
        geyser_config.clone(),
        api_key,
        config.geyser_run_duration_seconds,
    );
    let sender_clone = sender.clone();

    let client_handle = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new()
            .expect("Failed to create tokio runtime");

        rt.block_on(async {
            match geyser_client.connect_and_subscribe(sender_clone).await {
                Ok(events_count) => {
                    log::info!("Geyser client processed {} events", events_count);
                    events_count as i64
                }
                Err(e) => {
                    log::error!("Geyser client error: {}", e);
                    -1
                }
            }
        })
    });

    // Run the processor pipeline
    let pipeline_started_at = Instant::now();
    let mut pipeline = ProcessorPipeline::new(
        receiver,
        BatchWriter::new(2, Duration::from_millis(10)),
        Type1Decoder::new(),
        custom_decoders,
        sink,
    );

    let report = pipeline.run()?;
    let elapsed = pipeline_started_at.elapsed();

    // Wait for client to finish
    let forwarded = client_handle
        .join()
        .map_err(|_| "Failed to join client thread")?
        .max(0) as u64;

    print_final_report(&config, storage_mode, elapsed, forwarded, report, "real-geyser");

    Ok(())
}

fn run_with_simulated_data(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Starting Solana Realtime Indexer with simulated data");

    let geyser_consumer = GeyserConsumer::new(GeyserConfig {
        endpoint: config
            .geyser_endpoint
            .clone()
            .unwrap_or_else(|| "mock://local-geyser".to_string()),
        channel_capacity: 8,
        filters: config.geyser_subscription_filters(),
    });

    let (sender, receiver) = sync_channel(geyser_consumer.config.channel_capacity);
    let fixture = GeyserConsumer::simulated_fixture();

    let custom_decoders: Vec<Box<dyn CustomDecoder>> =
        vec![Box::new(ProgramActivityDecoder::new("amm-program"))];

    let retention_policy = RetentionPolicy {
        max_age: Duration::from_secs(60),
    };

    let storage_mode = if config.database_url.is_some() {
        "timescale"
    } else {
        "dry-run"
    };

    let sink: Box<dyn StorageSink> = if let Some(database_url) = config.database_url.as_deref() {
        Box::new(TimescaleStorageSink::connect(
            database_url,
            Type1Store::new(retention_policy.clone()),
        )?)
    } else {
        Box::new(DryRunStorageSink::new(Type1Store::new(retention_policy)))
    };

    let pipeline_started_at = Instant::now();
    let producer = thread::spawn(move || geyser_consumer.forward_events(&sender, fixture));

    let mut pipeline = ProcessorPipeline::new(
        receiver,
        BatchWriter::new(2, Duration::from_millis(10)),
        Type1Decoder::new(),
        custom_decoders,
        sink,
    );

    let report = pipeline.run()?;
    let elapsed = pipeline_started_at.elapsed();

    let forwarded = producer
        .join()
        .expect("producer thread should complete")
        .expect("channel should remain open while forwarding") as u64;

    print_final_report(&config, storage_mode, elapsed, forwarded, report, "simulated-data");

    Ok(())
}

fn print_final_report(
    config: &Config,
    storage_mode: &str,
    elapsed: Duration,
    forwarded_events: u64,
    report: solana_realtime_indexer::processor::pipeline::PipelineReport,
    status: &str,
) {
    let api_snapshot = ApiSnapshot::from_report(
        PROJECT_NAME,
        storage_mode,
        config.bind_address.clone(),
        config.rpc_endpoints.len(),
        0,
        elapsed,
        unix_now_millis(),
        report.clone(),
    );

    let _api_router = api::router(api_snapshot);

    println!("{PROJECT_NAME}");
    println!("bind_address={}", config.bind_address);
    println!("rpc_endpoints={}", config.rpc_endpoints.len());
    println!(
        "geyser_endpoint={}",
        config
            .geyser_endpoint
            .as_deref()
            .unwrap_or("mock://local-geyser")
    );
    println!("storage_mode={storage_mode}");
    println!("forwarded_events={forwarded_events}");
    println!("received_events={}", report.received_events);
    println!("flushes={}", report.flush_count);
    println!("account_rows_written={}", report.account_rows_written);
    println!(
        "transaction_rows_written={}",
        report.transaction_rows_written
    );
    println!("slot_rows_written={}", report.slot_rows_written);
    println!("custom_rows_written={}", report.custom_rows_written);
    println!("sql_statements_planned={}", report.sql_statements_planned);
    println!("retained_account_rows={}", report.retained_account_rows);
    println!(
        "retained_transaction_rows={}",
        report.retained_transaction_rows
    );
    println!("retained_slot_rows={}", report.retained_slot_rows);
    println!("retained_custom_rows={}", report.retained_custom_rows);
    println!("pruned_account_rows={}", report.pruned_account_rows);
    println!("pruned_transaction_rows={}", report.pruned_transaction_rows);
    println!("pruned_slot_rows={}", report.pruned_slot_rows);
    println!("pruned_custom_rows={}", report.pruned_custom_rows);
    println!("api_routes=/health,/metrics");
    println!("status={status}");
}

fn unix_now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
