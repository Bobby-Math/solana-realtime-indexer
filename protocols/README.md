# Protocol Configuration

This directory contains TOML configuration files for protocols you want to index.

## Design Philosophy

The modular design separates **what to subscribe to** (mutable, ops-controlled) from **how to decode it** (immutable, type-safe Rust code).

### Two Separate Concerns

**Concern 1 — What to subscribe to** (pure data, belongs in TOML):
- Program IDs
- Account pubkeys
- Slot inclusion
- Commitment level

**Concern 2 — How to decode the data** (Rust code, belongs in the trait system):
- Custom decoders for protocol-specific instruction parsing
- Type-safe implementations
- Compile-time guarantees

## Protocol Config Files

Each protocol gets its own `.toml` file:

```toml
# protocols/raydium.toml
name = "Raydium AMM"

programs = [
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
]

accounts = []
include_slots = false
```

### Validation

At startup, all program IDs and accounts are validated in a single pass:
- Must be valid base58
- Must decode to exactly 32 bytes (Solana pubkey size)
- Invalid configurations fail fast with clear error messages

### ⚠️ WARNING: Token Program

**DO NOT** include the Token program (`TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA`) as a program filter unless you absolutely need every SPL token account on Solana. This will capture millions of updates per second.

For most DEX indexing, you only need the DEX program itself — Helius will send you transactions that invoke both the DEX and Token programs.

## Protocol Trait System

### For 80% of Clients (Config-Only)

If you only need filtered ingestion without custom decoding, use `ConfigOnlyProtocol`:

```rust
use solana_realtime_indexer::geyser::{ConfigOnlyProtocol, load_protocols_from_dir, merge_subscriptions};

// Load all TOML files from protocols/ directory
let protocols = load_protocols_from_dir("protocols")?;

// Or load a single protocol
let raydium = ConfigOnlyProtocol::from_toml("protocols/raydium.toml")?;

// Convert to Box<dyn Protocol> for merging
let protocol_boxes: Vec<Box<dyn Protocol>> = protocols
    .into_iter()
    .map(|p| Box::new(p) as Box<dyn Protocol>)
    .collect();

// Merge multiple protocols (deduplicates shared programs like Token)
let merged_subscription = merge_subscriptions(&protocol_boxes);
```

### For Advanced Clients (Custom Decoders)

Implement the `Protocol` trait for protocol-specific decoding logic:

```rust
use solana_realtime_indexer::geyser::Protocol;
use solana_realtime_indexer::processor::decoder::CustomDecoder;

pub struct MyProtocol {
    config: ProtocolConfig,
    subscription: ProtocolSubscription,
}

impl MyProtocol {
    pub fn from_toml(path: &str) -> Result<Self, String> {
        let (config, subscription) = ProtocolConfig::from_toml_path(path)?;
        Ok(Self { config, subscription })
    }
}

impl Protocol for MyProtocol {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn subscription(&self) -> ProtocolSubscription {
        self.subscription.clone()
    }

    fn decoders(&self) -> Vec<Box<dyn CustomDecoder>> {
        vec![
            Box::new(MySwapDecoder::new()),
            Box::new(MyLiquidityDecoder::new()),
        ]
    }
}
```

## Multi-Protocol Merging

The system automatically deduplicates program IDs and accounts when merging multiple protocols:

```rust
// If both protocols include the Token program, it will only appear once in the merged subscription
let merged = merge_subscriptions(&[protocol1, protocol2]);

assert_eq!(merged.program_ids.len(), 1); // Not 2!
```

This prevents wasting filter slots when multiple protocols share common programs.

## Requirements

The indexer **requires** a `protocols/` directory with at least one `.toml` configuration file. If the directory doesn't exist or contains no configuration files, the indexer will fail to start with a clear error message.

This is intentional - silent fallback to environment variables is dangerous and can lead to misconfigured deployments.

## Benefits

1. **No recompilation needed** — Add new protocols by dropping TOML files
2. **Type-safe decoding** — Rust trait system with `[u8; 32]` pubkeys
3. **Clear separation** — Ops control subscriptions (TOML), devs control decoding (Rust traits)
4. **Multi-protocol support** — Index multiple protocols simultaneously with automatic deduplication
5. **Validation at startup** — Single-pass validation catches configuration errors before connecting to Geyser
6. **Zero boilerplate** — `ConfigOnlyProtocol` covers most use cases without writing any Rust code
