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

#[derive(Debug, Clone)]
pub struct TransactionRow {
    pub slot: i64,
    pub timestamp_unix_ms: i64,
    pub signature: Vec<u8>,
    pub fee: i64,
    pub success: bool,
    pub program_ids: Vec<Vec<u8>>,
    pub log_messages: Vec<String>,
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
