#[derive(Debug, Clone)]
pub enum GeyserEvent {
    AccountUpdate(AccountUpdate),
    Transaction(TransactionUpdate),
    SlotUpdate(SlotUpdate),
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub pubkey: String,
    pub owner: String,
    pub lamports: u64,
    pub write_version: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub signature: String,
    pub fee: u64,
    pub success: bool,
    pub program_ids: Vec<String>,
    pub log_messages: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SlotUpdate {
    pub timestamp_unix_ms: i64,
    pub slot: u64,
    pub parent_slot: Option<u64>,
    pub status: String,
}
