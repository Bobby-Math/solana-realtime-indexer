# solana-realtime-indexer

High-performance Solana realtime indexer:

`RPC pool + Geyser streaming -> TimescaleDB -> public query API -> live dashboard`

Core indexer metrics: `ingest_events_per_sec`, `db_rows_written_per_sec`, and `slot_to_indexed_lag_ms`.

| Metric | V1 target | Stretch target |
| --- | ---: | ---: |
| `slot_to_indexed_lag_ms` | p95 < 400ms | p95 < 100ms |
| `ingest_events_per_sec` | 25k+ | 50k+ |
| `db_rows_written_per_sec` | 20k+ | 50k+ |

## 🚀 Quick Start

### Development Mode (Safe - Uses Simulated Data)
```bash
cargo run
```

### Production Mode (Real Helius Geyser Stream)
```bash
# Configure your Helius Geyser endpoint
cp .env.example .env
echo "GEYSER_ENDPOINT=https://your-helius-geyser-endpoint.com" >> .env

# Start TimescaleDB
docker compose up -d

# Run database migrations
./scripts/run-migrations.sh

# Run with real Geyser connection
cargo run
```

## ✅ Features

- **Real-time Geyser Streaming**: Full Helius Geyser integration with gRPC
- **Custom Program Indexing**: Protocol-specific decoders from IDLs
- **High-Performance Pipeline**: Batch processing with configurable throughput
- **Production Storage**: TimescaleDB with retention policies
- **REST API**: Health and metrics endpoints
- **Dual Mode**: Safe development mode + production Geyser mode

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
