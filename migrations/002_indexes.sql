CREATE INDEX IF NOT EXISTS idx_account_updates_pubkey_timestamp
    ON account_updates (pubkey, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_account_updates_owner_timestamp
    ON account_updates (owner, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_account_updates_slot_timestamp
    ON account_updates (slot, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_signature
    ON transactions (signature, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_slot_timestamp
    ON transactions (slot, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_program_ids
    ON transactions USING GIN (program_ids);

CREATE INDEX IF NOT EXISTS idx_slots_parent_slot
    ON slots (parent_slot);

CREATE INDEX IF NOT EXISTS idx_custom_decoded_events_decoder_record_timestamp
    ON custom_decoded_events (decoder_name, record_key, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_custom_decoded_events_slot_timestamp
    ON custom_decoded_events (slot, timestamp DESC);
