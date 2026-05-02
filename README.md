# solana-realtime-indexer

High-performance Solana realtime indexer with live deployment at [solana.bobby-math.dev](https://solana.bobby-math.dev)

`Geyser streaming -> WAL -> TimescaleDB -> REST API -> live dashboard`

**Current Devnet Performance:**
- 40+ TPS sustained throughput
- 1280ms p50 slot-to-DB latency
- 1859ms p99 slot-to-DB latency

| Metric | V1 target | Stretch target |
| --- | ---: | ---: |
| `slot_to_indexed_lag_ms` | p95 < 400ms | p95 < 100ms |
| `ingest_events_per_sec` | 25k+ | 50k+ |
| `db_rows_written_per_sec` | 20k+ | 50k+ |

## 🚀 Quick Start

```bash
# Configure environment
cp .env.example .env
# Edit .env with your Helius Geyser endpoint and database URL

# Start TimescaleDB
docker compose up -d

# Run database migrations
./scripts/run-migrations.sh

# Run indexer
cargo run
```

## ✅ Features

- **Real-time Geyser Streaming**: Full Helius Geyser integration with gRPC
- **Write-Ahead Log (WAL)**: Crash recovery with automatic restart from last checkpoint
- **RPC Gap Filling**: Automatic repair of missing slots via RPC fallback
- **Type-Safe Decoding**: Fixed-width types for signatures and program IDs
- **Custom Program Indexing**: Protocol-specific decoders from IDLs
- **High-Performance Pipeline**: Batch processing with UNNEST bulk inserts
- **Production Storage**: TimescaleDB with compression and retention policies
- **REST API & Dashboard**: Real-time metrics and program TPS leaderboard

## 🗄️ Migrations

The current production schema expects the full migration set in `migrations/`,
including:

- `006_fix_log_messages_type.sql` to store `transactions.log_messages` as `JSONB`
- `008_timescaledb_retention_compression.sql` to move retention/compression to
  TimescaleDB background policies instead of inline deletes on the ingest path

## 🔧 Configuration

See `.env.example` for all configuration options including:
- Helius Geyser endpoint and subscription filters
- RPC endpoints for failover
- Database connection
- Logging levels
