-- Migration: TimescaleDB-native retention and compression policies
--
-- Moves retention off the hot write path. Inline DELETEs on hypertables extend
-- every ingest transaction and create avoidable dead tuples. Timescale policies
-- handle this in the background by dropping/compressing chunks instead.

SELECT set_chunk_time_interval('account_updates',         INTERVAL '15 minutes');
SELECT set_chunk_time_interval('transactions',            INTERVAL '15 minutes');
SELECT set_chunk_time_interval('transaction_program_ids', INTERVAL '15 minutes');
SELECT set_chunk_time_interval('custom_decoded_events',   INTERVAL '15 minutes');
SELECT set_chunk_time_interval('slots',                   INTERVAL '15 minutes');

ALTER TABLE account_updates SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC',
    timescaledb.compress_segmentby = 'pubkey'
);

ALTER TABLE transactions SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC'
);

ALTER TABLE custom_decoded_events SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC',
    timescaledb.compress_segmentby = 'decoder_name'
);

ALTER TABLE slots SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('account_updates',       INTERVAL '15 minutes', if_not_exists => TRUE);
SELECT add_compression_policy('transactions',          INTERVAL '15 minutes', if_not_exists => TRUE);
SELECT add_compression_policy('custom_decoded_events', INTERVAL '15 minutes', if_not_exists => TRUE);
SELECT add_compression_policy('slots',                 INTERVAL '15 minutes', if_not_exists => TRUE);

SELECT add_retention_policy('account_updates',       INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('transactions',          INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('custom_decoded_events', INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('slots',                 INTERVAL '1 day', if_not_exists => TRUE);
