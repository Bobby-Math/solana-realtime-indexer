-- Migration: Normalize program_ids into dedicated child table
-- This removes the base64 encoding overhead and enables efficient program-based queries
-- At 50k tx/sec with ~4 program IDs each, this saves ~200k base64 encode/decode ops/sec

-- Step 1: Create the new transaction_program_ids child table
CREATE TABLE IF NOT EXISTS transaction_program_ids (
    timestamp TIMESTAMPTZ NOT NULL,
    signature BYTEA NOT NULL,
    program_id BYTEA NOT NULL,
    position SMALLINT NOT NULL,
    PRIMARY KEY (timestamp, signature, program_id, position)
);

-- Step 2: Convert to hypertable with same chunk interval as transactions
SELECT create_hypertable('transaction_program_ids', 'timestamp', chunk_time_interval => INTERVAL '2 hours', if_not_exists => TRUE);

-- Step 3: Create index for program-based queries
-- This is the primary query pattern: "all transactions for program X in time range"
-- timestamp DESC enables efficient chunk pruning on hypertable
CREATE INDEX IF NOT EXISTS idx_transaction_program_ids_program_timestamp
    ON transaction_program_ids (program_id, timestamp DESC);

-- Step 4: Migrate existing data from transactions.program_ids to transaction_program_ids
-- This handles both fresh installs and existing deployments with data
INSERT INTO transaction_program_ids (timestamp, signature, program_id, position)
SELECT
    t.timestamp,
    t.signature,
    UNNEST(t.program_ids) AS program_id,
    generate_series(1, array_length(t.program_ids, 1)) AS position
FROM transactions t
WHERE t.program_ids IS NOT NULL AND array_length(t.program_ids, 1) > 0
ON CONFLICT (timestamp, signature, program_id, position) DO NOTHING;

-- Step 5: Add retention policy to match transactions table
-- Default retention is 1 hour, configurable via environment variable
SELECT add_retention_policy('transaction_program_ids', INTERVAL '1 hour');

-- Step 6: Set up compression for completed chunks
-- Compress after 15 minutes (completed chunks) with segment-by on program_id
-- This makes program-filtered queries scan only relevant compressed segments
ALTER TABLE transaction_program_ids SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC',
    timescaledb.compress_segmentby = 'program_id'
);
SELECT add_compression_policy('transaction_program_ids', INTERVAL '15 minutes');

-- Step 7: Remove the old program_ids column from transactions table
-- This is done after data migration to ensure no data loss
ALTER TABLE transactions DROP COLUMN IF EXISTS program_ids;

-- Step 8: Remove the old GIN index on program_ids (no longer needed)
DROP INDEX IF EXISTS idx_transactions_program_ids;
