use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Instant;

use crate::geyser::decoder::GeyserEvent;
use crate::processor::batch_writer::{BatchWriter, FlushReason};
use crate::processor::decoder::{CustomDecoder, PersistedBatch, Type1Decoder};
use crate::processor::sink::{StorageError, StorageSink, StorageWriteResult};
use crate::processor::sql::CheckpointUpdate;
use crate::processor::store::StoreSnapshot;

#[derive(Debug, Clone, Default)]
pub struct PipelineReport {
    pub received_events: u64,
    pub flush_count: u64,
    pub account_rows_written: u64,
    pub transaction_rows_written: u64,
    pub slot_rows_written: u64,
    pub custom_rows_written: u64,
    pub sql_statements_planned: u64,
    pub retained_account_rows: usize,
    pub retained_transaction_rows: usize,
    pub retained_slot_rows: usize,
    pub retained_custom_rows: usize,
    pub pruned_account_rows: u64,
    pub pruned_transaction_rows: u64,
    pub pruned_slot_rows: u64,
    pub pruned_custom_rows: u64,
}

pub struct ProcessorPipeline {
    receiver: Receiver<GeyserEvent>,
    writer: BatchWriter,
    decoder: Type1Decoder,
    custom_decoders: Vec<Box<dyn CustomDecoder>>,
    sink: Box<dyn StorageSink>,
}

impl ProcessorPipeline {
    pub fn new(
        receiver: Receiver<GeyserEvent>,
        writer: BatchWriter,
        decoder: Type1Decoder,
        custom_decoders: Vec<Box<dyn CustomDecoder>>,
        sink: Box<dyn StorageSink>,
    ) -> Self {
        Self {
            receiver,
            writer,
            decoder,
            custom_decoders,
            sink,
        }
    }

    pub fn run(&mut self) -> Result<PipelineReport, StorageError> {
        let mut report = PipelineReport::default();

        loop {
            match self.receiver.recv_timeout(self.writer.flush_interval) {
                Ok(event) => {
                    report.received_events += 1;
                    self.writer.push(event);

                    if let Some(batch) = self.writer.flush_if_needed(Instant::now()) {
                        self.process_batch(batch, &mut report)?;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if let Some(batch) = self.writer.flush(FlushReason::Interval, Instant::now()) {
                        self.process_batch(batch, &mut report)?;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if let Some(batch) = self
                        .writer
                        .flush(FlushReason::ChannelClosed, Instant::now())
                    {
                        self.process_batch(batch, &mut report)?;
                    }
                    break;
                }
            }
        }

        Ok(report)
    }

    fn process_batch(
        &mut self,
        batch: crate::processor::batch_writer::BufferedBatch,
        report: &mut PipelineReport,
    ) -> Result<(), StorageError> {
        let persisted = self.decoder.decode(batch, &mut self.custom_decoders);
        let result = self
            .sink
            .write_batch(&persisted, checkpoint_update_for_batch(&persisted))?;
        apply_batch(report, persisted, result);
        Ok(())
    }
}

fn apply_batch(report: &mut PipelineReport, batch: PersistedBatch, result: StorageWriteResult) {
    report.flush_count += 1;
    report.account_rows_written += batch.account_rows.len() as u64;
    report.transaction_rows_written += batch.transaction_rows.len() as u64;
    report.slot_rows_written += batch.slot_rows.len() as u64;
    report.custom_rows_written += batch.custom_rows.len() as u64;
    report.sql_statements_planned += result.sql_statements_planned;
    let snapshot: StoreSnapshot = result.snapshot;
    report.retained_account_rows = snapshot.account_rows;
    report.retained_transaction_rows = snapshot.transaction_rows;
    report.retained_slot_rows = snapshot.slot_rows;
    report.retained_custom_rows = snapshot.custom_rows;
    report.pruned_account_rows = snapshot.metrics.account_rows_pruned;
    report.pruned_transaction_rows = snapshot.metrics.transaction_rows_pruned;
    report.pruned_slot_rows = snapshot.metrics.slot_rows_pruned;
    report.pruned_custom_rows = snapshot.metrics.custom_rows_pruned;
}

fn checkpoint_update_for_batch(batch: &PersistedBatch) -> Option<CheckpointUpdate> {
    let last_processed_slot = batch
        .slot_rows
        .iter()
        .map(|row| row.slot)
        .chain(batch.transaction_rows.iter().map(|row| row.slot))
        .chain(batch.account_rows.iter().map(|row| row.slot))
        .chain(batch.custom_rows.iter().map(|row| row.slot))
        .max();
    let last_observed_at_unix_ms = batch.latest_timestamp_unix_ms()?;

    Some(CheckpointUpdate {
        stream_name: "geyser-main".to_string(),
        last_processed_slot,
        last_observed_at_unix_ms,
        notes: Some(format!("flush_reason={:?}", batch.reason)),
    })
}

#[cfg(test)]
mod tests {
    use super::{PipelineReport, ProcessorPipeline};
    use crate::geyser::decoder::{AccountUpdate, GeyserEvent, SlotUpdate};
    use crate::processor::batch_writer::BatchWriter;
    use crate::processor::decoder::{CustomDecoder, ProgramActivityDecoder, Type1Decoder};
    use crate::processor::sink::DryRunStorageSink;
    use crate::processor::store::{RetentionPolicy, Type1Store};
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;

    #[test]
    fn drains_channel_and_flushes_remaining_items_when_sender_closes() {
        let (sender, receiver) = sync_channel(4);
        sender
            .send(GeyserEvent::AccountUpdate(AccountUpdate {
                timestamp_unix_ms: 1_710_000_000_000,
                slot: 10,
                pubkey: "tracked-account".to_string(),
                owner: "tracked-program".to_string(),
                lamports: 5,
                write_version: 1,
                data: vec![1, 2, 3],
            }))
            .expect("send account");
        sender
            .send(GeyserEvent::SlotUpdate(SlotUpdate {
                timestamp_unix_ms: 1_710_000_000_001,
                slot: 10,
                parent_slot: Some(9),
                status: "processed".to_string(),
            }))
            .expect("send slot");
        drop(sender);

        let mut pipeline = ProcessorPipeline::new(
            receiver,
            BatchWriter::new(8, Duration::from_millis(1)),
            Type1Decoder::new(),
            vec![Box::new(ProgramActivityDecoder::new("tracked-program")) as Box<dyn CustomDecoder>],
            Box::new(DryRunStorageSink::new(Type1Store::new(RetentionPolicy {
                max_age: Duration::from_secs(60),
            }))),
        );
        let report: PipelineReport = pipeline.run().expect("pipeline should drain");

        assert_eq!(report.received_events, 2);
        assert_eq!(report.flush_count, 1);
        assert_eq!(report.account_rows_written, 1);
        assert_eq!(report.slot_rows_written, 1);
        assert_eq!(report.custom_rows_written, 1);
        assert_eq!(report.sql_statements_planned, 8);
        assert_eq!(report.retained_account_rows, 1);
    }
}
