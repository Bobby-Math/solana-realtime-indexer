-- Migration: Fix log_messages column type from TEXT[] to JSONB
-- Corrects data corruption where JSON arrays were stored as single text strings

-- Step 1: Rewrite existing data from malformed text representation to proper JSONB
-- to_jsonb() safely converts existing TEXT[] entries to JSONB arrays
ALTER TABLE transactions
    ALTER COLUMN log_messages TYPE JSONB
    USING to_jsonb(log_messages);

-- Step 2: Set default to empty JSONB array (matches sql.rs parameter type)
ALTER TABLE transactions
    ALTER COLUMN log_messages SET DEFAULT '[]'::jsonb;
