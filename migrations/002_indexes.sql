CREATE INDEX IF NOT EXISTS idx_account_updates_pubkey_timestamp
    ON account_updates (pubkey, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_signature
    ON transactions (signature);

CREATE INDEX IF NOT EXISTS idx_transactions_slot_timestamp
    ON transactions (slot, timestamp DESC);
