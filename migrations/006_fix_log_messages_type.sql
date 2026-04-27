-- Migration: Fix log_messages column type from TEXT[] to JSONB
-- Corrects data corruption where JSON arrays were stored as single text strings

-- Step 1: Drop existing default (TEXT[] default can't cast to JSONB)
ALTER TABLE transactions ALTER COLUMN log_messages DROP DEFAULT;

-- Step 2: Change column type, converting existing data from TEXT[] to JSONB
-- to_jsonb() properly converts TEXT[] arrays to JSONB arrays
ALTER TABLE transactions
    ALTER COLUMN log_messages TYPE JSONB
    USING to_jsonb(log_messages);

-- Step 3: Set default to empty JSONB array (matches sql.rs parameter type)
ALTER TABLE transactions
    ALTER COLUMN log_messages SET DEFAULT '[]'::jsonb;
