use crate::geyser::decoder::{GeyserEvent, TransactionUpdate};
use crate::processor::decoder::CustomDecoder;
use crate::processor::schema::CustomDecodedRow;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CpiLogDecoder;

impl CpiLogDecoder {
    pub fn new() -> Self {
        Self
    }

    fn parse_cpi_edges(&self, update: &TransactionUpdate) -> Vec<CustomDecodedRow> {
        let mut edges = Vec::new();
        let mut stack: Vec<Option<String>> = Vec::new();
        let mut program_outcomes: HashMap<String, bool> = HashMap::new();

        // First pass: collect program outcomes
        for log in &update.log_messages {
            if let Some(program) = parse_program_outcome(log) {
                program_outcomes.insert(program.0, program.1);
            }
        }

        // Second pass: build call stack and emit edges
        // Solana logs use 1-based depth, where depth=1 is the root invocation (no caller)
        for log in &update.log_messages {
            if let Some((program, depth)) = parse_program_invoke(log) {
                // Ensure stack is large enough (depth is 1-based)
                while stack.len() <= depth {
                    stack.push(None);
                }
                stack[depth] = Some(program.clone());

                // Emit edge if we have a caller (depth > 1 means we have a parent)
                if depth > 1 {
                    if let Some(Some(caller)) = stack.get(depth - 1) {
                        let callee = program.clone();
                        let success = program_outcomes.get(&callee).copied().unwrap_or(update.success);

                        edges.push(CustomDecodedRow {
                            decoder_name: "cpi-edge".to_string(),
                            record_key: bs58::encode(&update.signature).into_string(),
                            slot: update.slot as i64,
                            timestamp_unix_ms: update.timestamp_unix_ms,
                            event_index: depth as i16,
                            payload: serde_json::json!({
                                "caller": caller,
                                "callee": callee,
                                "depth": depth,
                                "success": success
                            }).to_string(),
                        });
                    }
                }
            }
        }

        edges
    }
}

impl CustomDecoder for CpiLogDecoder {
    fn name(&self) -> &str {
        "cpi-edge"
    }

    fn decode(&mut self, event: &GeyserEvent) -> Option<CustomDecodedRow> {
        // Return first edge for simple case
        self.decode_multi(event).first().cloned()
    }

    fn decode_multi(&mut self, event: &GeyserEvent) -> Vec<CustomDecodedRow> {
        match event {
            GeyserEvent::Transaction(update) => {
                self.parse_cpi_edges(update)
            }
            _ => Vec::new(),
        }
    }
}

impl Default for CpiLogDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse "Program <PUBKEY> invoke [<DEPTH>]" returns (program_base58, depth)
fn parse_program_invoke(log: &str) -> Option<(String, usize)> {
    // Pattern: "Program <base58> invoke [1]" or "Program <base58> invoke [2]"
    let log = log.trim();

    // Check if it contains " invoke ["
    if !log.contains(" invoke [") {
        return None;
    }

    // Find the program part
    if let Some(start) = log.find("Program ") {
        let after_program = &log[start + 8..]; // Skip "Program "

        // Find the end of the pubkey (before " invoke")
        if let Some(end) = after_program.find(" invoke [") {
            let pubkey = &after_program[..end];

            // Find the depth number
            let depth_start = end + 9; // Skip " invoke ["
            if let Some(depth_end) = after_program[depth_start..].find(']') {
                let depth_str = &after_program[depth_start..depth_start + depth_end];
                if let Ok(depth) = depth_str.parse::<usize>() {
                    return Some((pubkey.to_string(), depth));
                }
            }
        }
    }

    None
}

/// Parse "Program <PUBKEY> success" or "Program <PUBKEY> failed" returns (program_base58, success)
fn parse_program_outcome(log: &str) -> Option<(String, bool)> {
    let log = log.trim();

    // Check for success
    if let Some(start) = log.find("Program ") {
        let after_program = &log[start + 8..]; // Skip "Program "

        if let Some(end) = after_program.find(" success") {
            let program = &after_program[..end];
            return Some((program.to_string(), true));
        }

        if let Some(end) = after_program.find(" failed") {
            let program = &after_program[..end];
            return Some((program.to_string(), false));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_program_invoke() {
        let log = "Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin invoke [1]";
        let result = parse_program_invoke(log);
        assert_eq!(result, Some(("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string(), 1)));

        let log2 = "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]";
        let result2 = parse_program_invoke(log2);
        assert_eq!(result2, Some(("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(), 2)));
    }

    #[test]
    fn test_parse_program_outcome() {
        let log = "Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin success";
        let result = parse_program_outcome(log);
        assert_eq!(result, Some(("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string(), true)));

        let log2 = "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA failed";
        let result2 = parse_program_outcome(log2);
        assert_eq!(result2, Some(("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(), false)));
    }

    #[test]
    fn test_non_invoke_log_returns_none() {
        let log = "Some other log message";
        let result = parse_program_invoke(log);
        assert_eq!(result, None);
    }

    #[test]
    fn test_cpi_decoder_handles_transaction_with_multiple_cpi_calls() {
        use crate::geyser::decoder::TransactionUpdate;

        let mut decoder = CpiLogDecoder::new();

        // Create a mock transaction with nested CPI calls
        let tx = TransactionUpdate {
            timestamp_unix_ms: 1234567890000,
            slot: 100,
            signature: vec![1, 2, 3, 4],
            fee: 5000,
            success: true,
            program_ids: vec![
                vec![5, 6, 7, 8],  // Root program
                vec![9, 10, 11, 12], // CPI call 1
                vec![13, 14, 15, 16], // CPI call 2
            ],
            log_messages: vec![
                "Program 5xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin invoke [1]".to_string(),
                "Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin invoke [2]".to_string(),
                "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [3]".to_string(),
                "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success".to_string(),
                "Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin success".to_string(),
                "Program 5xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin success".to_string(),
            ],
        };

        let event = GeyserEvent::Transaction(tx);
        let rows = decoder.decode_multi(&event);

        // Should have 2 edges (program 1 -> program 2, and program 2 -> program 3)
        assert_eq!(rows.len(), 2);

        // Check first edge: program 1 calls program 2
        let first_edge = &rows[0];
        assert_eq!(first_edge.decoder_name, "cpi-edge");
        assert_eq!(first_edge.event_index, 2); // depth 2
        let payload: serde_json::Value = serde_json::from_str(&first_edge.payload).unwrap();
        assert_eq!(payload["caller"], "5xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin");
        assert_eq!(payload["callee"], "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin");
        assert_eq!(payload["depth"], 2);
        assert_eq!(payload["success"], true);

        // Check second edge: program 2 calls program 3
        let second_edge = &rows[1];
        assert_eq!(second_edge.decoder_name, "cpi-edge");
        assert_eq!(second_edge.event_index, 3); // depth 3
        let payload2: serde_json::Value = serde_json::from_str(&second_edge.payload).unwrap();
        assert_eq!(payload2["caller"], "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin");
        assert_eq!(payload2["callee"], "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        assert_eq!(payload2["depth"], 3);
        assert_eq!(payload2["success"], true);
    }

    #[test]
    fn test_cpi_decoder_returns_empty_for_non_transaction_events() {
        let mut decoder = CpiLogDecoder::new();
        use crate::geyser::decoder::{AccountUpdate, SlotUpdate};

        let account_event = GeyserEvent::AccountUpdate(AccountUpdate {
            timestamp_unix_ms: 1234567890000,
            slot: 100,
            pubkey: vec![1, 2, 3],
            owner: vec![4, 5, 6],
            lamports: 1000,
            write_version: 1,
            data: vec![7, 8, 9],
        });

        let rows = decoder.decode_multi(&account_event);
        assert_eq!(rows.len(), 0);

        let slot_event = GeyserEvent::SlotUpdate(SlotUpdate {
            timestamp_unix_ms: 1234567890000,
            slot: 100,
            parent_slot: Some(99),
            status: "confirmed".to_string(),
        });

        let rows = decoder.decode_multi(&slot_event);
        assert_eq!(rows.len(), 0);
    }
}
