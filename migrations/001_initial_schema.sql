CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS account_updates (
    timestamp TIMESTAMPTZ NOT NULL,
    slot BIGINT NOT NULL,
    pubkey BYTEA NOT NULL,
    owner BYTEA NOT NULL,
    lamports BIGINT NOT NULL CHECK (lamports >= 0),
    data BYTEA,
    write_version BIGINT NOT NULL,
    PRIMARY KEY (timestamp, slot, pubkey, write_version)
);

CREATE TABLE IF NOT EXISTS transactions (
    timestamp TIMESTAMPTZ NOT NULL,
    slot BIGINT NOT NULL,
    signature BYTEA NOT NULL,
    fee BIGINT NOT NULL CHECK (fee >= 0),
    success BOOLEAN NOT NULL,
    program_ids BYTEA[] NOT NULL DEFAULT '{}',
    log_messages TEXT[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (timestamp, signature)
);

CREATE TABLE IF NOT EXISTS slots (
    slot BIGINT PRIMARY KEY,
    parent_slot BIGINT,
    timestamp TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS custom_decoded_events (
    timestamp TIMESTAMPTZ NOT NULL,
    slot BIGINT NOT NULL,
    decoder_name TEXT NOT NULL,
    record_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (timestamp, decoder_name, record_key, slot)
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    events_per_second DOUBLE PRECISION,
    write_latency_p99_ms DOUBLE PRECISION,
    queue_depth INT,
    active_connections INT,
    batches_flushed BIGINT,
    stale_rows_pruned BIGINT
);

CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    stream_name TEXT PRIMARY KEY,
    last_processed_slot BIGINT,
    last_observed_at TIMESTAMPTZ NOT NULL,
    notes TEXT
);

SELECT create_hypertable('account_updates', 'timestamp', if_not_exists => TRUE);
SELECT create_hypertable('transactions', 'timestamp', if_not_exists => TRUE);
SELECT create_hypertable('custom_decoded_events', 'timestamp', if_not_exists => TRUE);
SELECT create_hypertable('pipeline_metrics', 'timestamp', if_not_exists => TRUE);
