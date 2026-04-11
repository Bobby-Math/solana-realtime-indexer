use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;

use solana_realtime_indexer::config::Config;
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
    let config = Config::from_env();
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

    let producer = thread::spawn(move || geyser_consumer.forward_events(&sender, fixture));
    let mut pipeline = ProcessorPipeline::new(
        receiver,
        BatchWriter::new(2, Duration::from_millis(10)),
        Type1Decoder::new(),
        custom_decoders,
        sink,
    );
    let report = pipeline.run()?;
    let forwarded = producer
        .join()
        .expect("producer thread should complete")
        .expect("channel should remain open while forwarding");

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
    println!("forwarded_events={forwarded}");
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
    println!("status=type1-decoder-and-custom-decoder-simulated");
    Ok(())
}
