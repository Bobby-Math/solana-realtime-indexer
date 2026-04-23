-- Fix payload column type to match actual usage
-- The CpiLogDecoder stores JSON data, but the column was defined as TEXT
ALTER TABLE custom_decoded_events
    ALTER COLUMN payload TYPE JSONB
    USING payload::JSONB;
