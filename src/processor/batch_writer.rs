use std::time::{Duration, Instant};

use crate::geyser::decoder::GeyserEvent;
#[derive(Debug, Clone)]
pub struct WriterMetrics {
    pub flush_count: u64,
    pub failed_flush_count: u64,
    pub buffered_events_flushed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    Size,
    Interval,
    ChannelClosed,
}

#[derive(Debug, Clone)]
pub struct BufferedBatch {
    pub reason: FlushReason,
    pub events: Vec<GeyserEvent>,
}

#[derive(Debug, Clone)]
pub struct BatchWriter {
    pub buffer: Vec<GeyserEvent>,
    pub flush_size: usize,
    pub flush_interval: Duration,
    pub metrics: WriterMetrics,
    last_flush_at: Instant,
}

impl BatchWriter {
    pub fn new(flush_size: usize, flush_interval: Duration) -> Self {
        Self {
            buffer: Vec::new(),
            flush_size,
            flush_interval,
            metrics: WriterMetrics {
                flush_count: 0,
                failed_flush_count: 0,
                buffered_events_flushed: 0,
            },
            last_flush_at: Instant::now(),
        }
    }

    pub fn should_flush(&self) -> bool {
        self.buffer.len() >= self.flush_size
    }

    pub fn push(&mut self, event: GeyserEvent) {
        self.buffer.push(event);
    }

    pub fn flush_if_needed(&mut self, now: Instant) -> Option<BufferedBatch> {
        if self.should_flush() {
            return self.flush(FlushReason::Size, now);
        }

        if !self.buffer.is_empty() && now.duration_since(self.last_flush_at) >= self.flush_interval
        {
            return self.flush(FlushReason::Interval, now);
        }

        None
    }

    pub fn flush(&mut self, reason: FlushReason, now: Instant) -> Option<BufferedBatch> {
        if self.buffer.is_empty() {
            return None;
        }

        let events = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.flush_size));
        let batch = BufferedBatch { reason, events };

        self.metrics.flush_count += 1;
        self.metrics.buffered_events_flushed += batch.events.len() as u64;
        self.last_flush_at = now;

        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchWriter, FlushReason};
    use crate::geyser::decoder::{AccountUpdate, GeyserEvent, SlotUpdate, TransactionUpdate};
    use std::time::{Duration, Instant};

    #[test]
    fn flushes_when_batch_size_is_reached() {
        let mut writer = BatchWriter::new(2, Duration::from_secs(60));
        let now = Instant::now();

        writer.push(GeyserEvent::AccountUpdate(AccountUpdate {
            timestamp_unix_ms: 1_710_000_000_000,
            slot: 100,
            pubkey: "account-a".as_bytes().to_vec(),
            owner: "program-a".as_bytes().to_vec(),
            lamports: 10,
            write_version: 1,
            data: vec![1, 2],
        }));
        writer.push(GeyserEvent::Transaction(TransactionUpdate {
            timestamp_unix_ms: 1_710_000_000_001,
            slot: 100,
            signature: "sig-a".as_bytes().to_vec(),
            fee: 5_000,
            success: true,
            accounts: vec![b"account-a".to_vec(), b"program-a".to_vec()],
            program_ids: vec![b"program-a".to_vec()],
            log_messages: vec!["ok".to_string()],
        }));

        let batch = writer.flush_if_needed(now).expect("size flush");

        assert_eq!(batch.reason, FlushReason::Size);
        assert_eq!(batch.events.len(), 2);
        assert!(writer.buffer.is_empty());
    }

    #[test]
    fn interval_flushes_pending_buffer() {
        let mut writer = BatchWriter::new(8, Duration::from_millis(5));
        let now = Instant::now();

        writer.push(GeyserEvent::SlotUpdate(SlotUpdate {
            timestamp_unix_ms: 1_710_000_000_002,
            slot: 100,
            parent_slot: Some(99),
            status: "confirmed".to_string(),
        }));

        let batch = writer
            .flush_if_needed(now + Duration::from_millis(6))
            .expect("interval flush");

        assert_eq!(batch.reason, FlushReason::Interval);
        assert_eq!(batch.events.len(), 1);
    }
}
