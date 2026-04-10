# solana-infra-pipeline

High-performance Solana data pipeline:

`RPC pool + Geyser streaming -> TimescaleDB -> public query API -> live dashboard`

The public architecture overview lives in [`ARCHITECTURE.md`](ARCHITECTURE.md). Private notes and personal context can stay in local-only `docs/`.

## Current state

- Rust crate and module tree scaffolded
- Docs-first architecture moved under `docs/`
- Initial database migrations, deployment files, and workflow placeholders added
- First RPC experiment note added under `docs/experiments/`

## Next build step

Implement Module 1 end-to-end:

1. request routing in `src/rpc_pool/pool.rs`
2. circuit transitions in `src/rpc_pool/circuit_breaker.rs`
3. health sampling in `src/rpc_pool/health.rs`
4. unit tests and a baseline benchmark
