-- Fix slot_health_1m to include processed and confirmed slots
-- This is needed after changing commitment level from 2 (finalized) to 0 (processed)

DROP MATERIALIZED VIEW IF EXISTS slot_health_1m;

CREATE MATERIALIZED VIEW slot_health_1m AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    COUNT(*) AS slot_count,
    CAST(SUM(COALESCE(slot - parent_slot - 1, 0)) AS BIGINT) AS skipped_slots,
    CAST(SUM(COALESCE(slot - parent_slot - 1, 0)) AS DOUBLE PRECISION)
        / NULLIF(COUNT(*) + SUM(COALESCE(slot - parent_slot - 1, 0)), 0) AS skip_rate,
    CAST(COUNT(*) AS DOUBLE PRECISION) / 60.0 AS slots_per_second
FROM slots
WHERE status IN ('processed', 'confirmed', 'finalized')
    AND timestamp > NOW() - INTERVAL '1 hour'
GROUP BY bucket
ORDER BY bucket DESC;

CREATE INDEX IF NOT EXISTS slot_health_1m_bucket_idx
    ON slot_health_1m (bucket DESC);
