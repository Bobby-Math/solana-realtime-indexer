# solana-realtime-indexer

High-performance Solana realtime indexer:

`RPC pool + Geyser streaming -> TimescaleDB -> public query API -> live dashboard`

Core indexer metrics: `ingest_events_per_sec`, `db_rows_written_per_sec`, and `slot_to_indexed_lag_ms`.

| Metric | V1 target | Stretch target |
| --- | ---: | ---: |
| `slot_to_indexed_lag_ms` | p95 < 400ms | p95 < 100ms |
| `ingest_events_per_sec` | 25k+ | 50k+ |
| `db_rows_written_per_sec` | 20k+ | 50k+ |
