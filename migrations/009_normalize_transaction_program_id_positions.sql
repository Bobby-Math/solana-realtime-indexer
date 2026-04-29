-- Normalize transaction_program_ids.position to 1-based indexing everywhere.
--
-- Historical rows created by migration 005 are 1-based already. A later writer
-- bug emitted 0-based positions for live-ingested rows. This migration repairs
-- those live rows without touching already-correct historical data or deleting
-- the newly inserted canonical rows.

BEGIN;

WITH live_style_transactions AS (
    SELECT DISTINCT timestamp, signature
    FROM transaction_program_ids
    WHERE position = 0
)
UPDATE transaction_program_ids tp
SET position = -(tp.position + 1)
FROM live_style_transactions live
WHERE tp.timestamp = live.timestamp
  AND tp.signature = live.signature;

INSERT INTO transaction_program_ids (timestamp, signature, program_id, position)
SELECT
    timestamp,
    signature,
    program_id,
    -position AS position
FROM transaction_program_ids
WHERE position < 0
ON CONFLICT DO NOTHING;

DELETE FROM transaction_program_ids
WHERE position < 0;

COMMIT;
