-- Migration: Add support for structured JSON payloads and wallet clustering

-- IMPORTANT DEPLOYMENT NOTE:
-- If materialized views already exist that reference the payload column, this ALTER will fail.
-- Drop dependent views first, run this migration, then recreate them.
-- Example: DROP MATERIALIZED VIEW IF EXISTS some_view_cascading;

-- Structural changes first: add event_index column for multi-event transaction support
-- Required for multi-event transactions to avoid primary key violations
ALTER TABLE custom_decoded_events
ADD COLUMN IF NOT EXISTS event_index SMALLINT NOT NULL DEFAULT 0;

-- NOTE: TimescaleDB does not support ALTER PRIMARY KEY on hypertables.
-- Existing deployments with the old 4-column primary key must either:
--   1. Truncate custom_decoded_events and re-ingest from Geyser, OR
--   2. Rebuild the table using the table migration procedure (see docs)
-- Fresh installs from migration 001 already have the correct 5-column primary key.
-- This migration adds the event_index column but cannot repair the PK constraint.

-- Type modification: change payload from TEXT to JSONB for structured event data
-- Note: to_jsonb() safely wraps arbitrary text as JSON string (payload may contain bare strings)
ALTER TABLE custom_decoded_events
ALTER COLUMN payload TYPE JSONB USING to_jsonb(payload);

-- Note: GIN index deferred - jsonb_path_ops only accelerates @> containment operator,
-- not the ->> equality queries used in most of our signals. Add later if needed.

-- Fix 3: Add fee_payer and signers for wallet clustering
ALTER TABLE transactions
ADD COLUMN IF NOT EXISTS fee_payer BYTEA,
ADD COLUMN IF NOT EXISTS signers BYTEA[];

-- Add index for fee payer queries
-- Note: timestamp DESC first for hypertable chunk pruning optimization
-- Note: partial index excludes NULLs to avoid bloat from pre-migration rows
CREATE INDEX IF NOT EXISTS idx_transactions_fee_payer
ON transactions(timestamp DESC, fee_payer)
WHERE fee_payer IS NOT NULL;

-- Add index for signer clustering queries
CREATE INDEX IF NOT EXISTS idx_transactions_signers
ON transactions USING GIN (signers);
