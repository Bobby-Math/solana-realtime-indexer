-- Slot health materialized view
-- Computes network health metrics (skip rate, production rate) from finalized slots
-- Replaces the latency-based view since devnet only sends finalized status

-- Step 1: Create materialized view for slot health metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS slot_health_1m AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    COUNT(*) AS slot_count,
    SUM(slot - parent_slot - 1) AS skipped_slots,
    CAST(SUM(slot - parent_slot - 1) AS DOUBLE PRECISION) / NULLIF(COUNT(*) + SUM(slot - parent_slot - 1), 0) AS skip_rate,
    CAST(COUNT(*) AS DOUBLE PRECISION) / 60.0 AS slots_per_second
FROM slots
WHERE status = 'finalized'
    AND timestamp > NOW() - INTERVAL '1 hour'
GROUP BY bucket
ORDER BY bucket DESC;

-- Create index for fast querying
CREATE INDEX IF NOT EXISTS slot_health_1m_bucket_idx
    ON slot_health_1m (bucket DESC);

-- Create a function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_slot_health_1m()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW slot_health_1m;
END;
$$ LANGUAGE plpgsql;

-- Note: This materialized view needs to be refreshed periodically.
-- The application (solana-realtime-indexer) will automatically call
-- refresh_slot_health_1m() every 30 seconds to keep data fresh.
