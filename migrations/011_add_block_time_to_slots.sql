-- Add block_time column to slots table
-- This allows measuring true slot-to-DB write latency:
-- Time from block production (block_time) → Database write (timestamp)

-- Add the column (nullable for existing rows)
ALTER TABLE slots
ADD COLUMN IF NOT EXISTS block_time TIMESTAMPTZ;

-- Create index for efficient latency queries
CREATE INDEX IF NOT EXISTS slots_block_time_idx
ON slots (block_time DESC)
WHERE block_time IS NOT NULL;

-- Comment explaining the purpose
COMMENT ON COLUMN slots.block_time IS 'Unix timestamp when the block was produced on Solana (from BlockMeta event). Used to measure slot-to-DB write latency.';
