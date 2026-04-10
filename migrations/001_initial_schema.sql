CREATE TABLE IF NOT EXISTS account_updates (
    slot BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    pubkey BYTEA NOT NULL,
    owner BYTEA NOT NULL,
    lamports BIGINT NOT NULL,
    data BYTEA,
    write_version BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    slot BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    signature BYTEA NOT NULL,
    fee BIGINT NOT NULL,
    success BOOLEAN NOT NULL,
    program_ids BYTEA[],
    log_messages TEXT[]
);

CREATE TABLE IF NOT EXISTS slots (
    slot BIGINT PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    parent_slot BIGINT,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    events_per_second DOUBLE PRECISION,
    write_latency_p99 DOUBLE PRECISION,
    queue_depth INT,
    active_connections INT
);
