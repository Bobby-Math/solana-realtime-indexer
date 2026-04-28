/// Row types for the TimescaleDB hypertables.
///
/// Design invariants:
/// - Every field maps 1:1 to a persisted table column or a child-table source field.
/// - `signature` is `[u8; 64]` because Solana signatures are always 64 bytes.
///   This makes signature copies allocation-free in the `transaction_program_ids`
///   explode loop.
/// - `program_ids` stays on `TransactionRow` as source data for the normalized
///   `transaction_program_ids` child table; it is not a `transactions` column.
#[derive(Debug, Clone)]
pub struct AccountUpdateRow {
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    pub pubkey: Vec<u8>,
    pub owner: Vec<u8>,
    pub lamports: i64,
    pub data: Vec<u8>,
    pub write_version: i64,
}

/// One row per confirmed transaction.
#[derive(Debug, Clone)]
pub struct TransactionRow {
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    /// Solana Ed25519 signature. Fixed-size for type-level length guarantees and
    /// allocation-free copies in the hot SQL batching path.
    pub signature: [u8; 64],
    pub fee: i64,
    pub success: bool,
    /// Source data for `transaction_program_ids`.
    pub program_ids: Vec<Vec<u8>>,
    pub log_messages: Vec<String>,
}

impl TransactionRow {
    /// Convert a raw signature slice into the fixed-width representation.
    ///
    /// Valid Solana signatures are exactly 64 bytes. We still guard here so
    /// malformed test or RPC gap-fill data cannot panic.
    pub fn signature_from_slice(bytes: &[u8]) -> [u8; 64] {
        let mut signature = [0u8; 64];
        let len = bytes.len().min(signature.len());
        signature[..len].copy_from_slice(&bytes[..len]);
        signature
    }
}

#[derive(Debug, Clone)]
pub struct SlotRow {
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    pub parent_slot: Option<i64>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CustomDecodedRow {
    pub decoder_name: String,
    pub record_key: String,
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    pub event_index: i16,
    /// JSON payload - typed to guarantee validity at compile time.
    /// Use `serde_json::json!()` macro to construct with type safety.
    /// The database will validate this is JSONB at insert time.
    pub payload: serde_json::Value,
}

#[cfg(test)]
pub mod test_helpers {
    use super::TransactionRow;

    pub fn test_signature(seed: u8) -> [u8; 64] {
        let mut signature = [0u8; 64];
        signature[0] = seed;
        signature
    }

    pub fn make_transaction_row(slot: i64, timestamp_unix_ms: i64, sig_seed: u8) -> TransactionRow {
        TransactionRow {
            slot,
            timestamp_unix_ms,
            signature: test_signature(sig_seed),
            fee: 5_000,
            success: true,
            program_ids: vec![vec![9, 10, 11, 12]],
            log_messages: vec!["ok".to_string()],
        }
    }
}
