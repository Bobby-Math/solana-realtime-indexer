use crate::processor::decoder::CustomDecoder;
use serde::{Deserialize, Serialize};

/// Protocol trait: implemented by clients for their specific protocol.
/// Separates subscription configuration (mutable, ops-controlled) from
/// decoder logic (immutable, type-safe, compile-time checked).
pub trait Protocol: Send + Sync {
    fn name(&self) -> &str;

    /// Returns subscription configuration loaded from TOML/config.
    /// This includes which program ID and accounts to watch.
    fn subscription(&self) -> ProtocolSubscription;

    /// Returns the decoder instances this protocol needs.
    /// Type-safe - no string lookups, compiled for each protocol crate.
    fn decoders(&self) -> Vec<Box<dyn CustomDecoder>>;
}

/// Subscription configuration: what to watch on Geyser.
/// Uses [u8; 32] for pubkeys - invalid lengths are structurally unrepresentable.
#[derive(Debug, Clone)]
pub struct ProtocolSubscription {
    pub program_ids: Vec<[u8; 32]>,
    pub account_pubkeys: Vec<[u8; 32]>,
    pub include_slots: bool,
}

/// TOML configuration file structure for protocol subscriptions.
/// This is the ops-controlled side - mutable without recompilation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolConfig {
    pub name: String,
    pub programs: Vec<String>,
    pub accounts: Vec<String>,
    #[serde(default)]
    pub include_slots: bool,
}

impl ProtocolConfig {
    /// Load and validate a TOML config file in a single pass.
    /// Returns both the raw config and the validated subscription.
    pub fn from_toml_path(path: &str) -> Result<(Self, ProtocolSubscription), String> {
        let toml_content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

        let config: ProtocolConfig = toml::from_str(&toml_content)
            .map_err(|e| format!("Failed to parse TOML from '{}': {}", path, e))?;

        Self::validate_and_decode(config)
    }

    /// Validate an already-loaded ProtocolConfig and decode to subscription.
    /// Used for environment variable fallback.
    pub fn from_toml_path_str(config: ProtocolConfig) -> Result<(Self, ProtocolSubscription), String> {
        Self::validate_and_decode(config)
    }

    fn validate_and_decode(config: ProtocolConfig) -> Result<(Self, ProtocolSubscription), String> {
        // Decode directly to [u8; 32] - single pass validation and conversion
        let program_ids = config.programs.iter()
            .map(|s| decode_to_32_bytes(s, "program_id"))
            .collect::<Result<Vec<_>, _>>()?;

        let account_pubkeys = config.accounts.iter()
            .map(|s| decode_to_32_bytes(s, "account"))
            .collect::<Result<Vec<_>, _>>()?;

        let subscription = ProtocolSubscription {
            program_ids,
            account_pubkeys,
            include_slots: config.include_slots,
        };

        Ok((config, subscription))
    }
}

/// Decodes a base58 string to [u8; 32] in a single pass.
/// Returns an error if invalid base58 or wrong length.
fn decode_to_32_bytes(value: &str, field_name: &str) -> Result<[u8; 32], String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{} is empty", field_name));
    }

    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|e| format!("Invalid {} '{}': not valid base58 - {}", field_name, trimmed, e))?;

    if decoded.len() != 32 {
        return Err(format!(
            "Invalid {} '{}': decoded to {} bytes, expected 32 for Solana pubkey",
            field_name,
            trimmed,
            decoded.len()
        ));
    }

    let mut array = [0u8; 32];
    array.copy_from_slice(&decoded);
    Ok(array)
}

/// For protocols that only need subscription config with no custom decoders.
/// Covers 80% of clients who only need filtered ingestion, not custom decoding.
pub struct ConfigOnlyProtocol {
    config: ProtocolConfig,
    subscription: ProtocolSubscription,
}

impl ConfigOnlyProtocol {
    /// Load a protocol from a TOML file.
    /// Returns a Protocol implementor with no custom decoders.
    pub fn from_toml(path: &str) -> Result<Self, String> {
        let (config, subscription) = ProtocolConfig::from_toml_path(path)?;
        Ok(Self { config, subscription })
    }

    /// Create a protocol from an already-loaded ProtocolConfig.
    /// Used for environment variable fallback.
    pub fn from_config(config: ProtocolConfig) -> Result<Self, String> {
        let subscription = ProtocolConfig::validate_and_decode(config.clone())?.1;
        Ok(Self { config, subscription })
    }
}

impl Protocol for ConfigOnlyProtocol {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn subscription(&self) -> ProtocolSubscription {
        self.subscription.clone()
    }

    fn decoders(&self) -> Vec<Box<dyn CustomDecoder>> {
        vec![]
    }
}

/// Merge multiple protocol subscriptions into one.
/// Deduplicates program IDs and account pubkeys to avoid wasting filter slots.
pub fn merge_subscriptions(protocols: &[Box<dyn Protocol>]) -> ProtocolSubscription {
    let mut program_ids: Vec<[u8; 32]> = Vec::new();
    let mut account_pubkeys: Vec<[u8; 32]> = Vec::new();
    let mut include_slots = false;

    for p in protocols {
        let sub = p.subscription();
        program_ids.extend_from_slice(&sub.program_ids);
        account_pubkeys.extend_from_slice(&sub.account_pubkeys);
        include_slots |= sub.include_slots;
    }

    // Deduplicate - two protocols may share the Token program
    program_ids.sort_unstable();
    program_ids.dedup();
    account_pubkeys.sort_unstable();
    account_pubkeys.dedup();

    ProtocolSubscription { program_ids, account_pubkeys, include_slots }
}

/// Load all protocol TOML files from a directory.
/// Returns ConfigOnlyProtocol instances for each .toml file found.
pub fn load_protocols_from_dir(dir: &str) -> Result<Vec<ConfigOnlyProtocol>, String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("Cannot read protocols dir '{}': {}", dir, e))?;

    let mut protocols = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("toml") {
            let path_str = path.to_str().ok_or("non-UTF8 path")?;
            match ConfigOnlyProtocol::from_toml(path_str) {
                Ok(protocol) => {
                    log::info!("Loaded protocol '{}' from {}", protocol.name(), path_str);
                    protocols.push(protocol);
                }
                Err(e) => {
                    log::error!("Failed to load protocol from '{}': {}", path_str, e);
                    return Err(e);
                }
            }
        }
    }

    if protocols.is_empty() {
        log::warn!("No protocol TOML files found in '{}'", dir);
    }

    Ok(protocols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_valid_base58_to_32_bytes() {
        let valid = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
        let result = decode_to_32_bytes(valid, "test").unwrap();
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn rejects_invalid_base58() {
        assert!(decode_to_32_bytes("invalid-prog", "test").is_err());
    }

    #[test]
    fn rejects_wrong_length() {
        // Too short
        assert!(decode_to_32_bytes("9WzDXwBbmkg", "test").is_err());
    }

    #[test]
    fn rejects_empty_string() {
        assert!(decode_to_32_bytes("", "test").is_err());
        assert!(decode_to_32_bytes("   ", "test").is_err());
    }

    #[test]
    fn config_only_protocol_implements_trait() {
        let valid = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
        let protocol = ConfigOnlyProtocol {
            config: ProtocolConfig {
                name: "TestProtocol".to_string(),
                programs: vec![valid.to_string()],
                accounts: vec![],
                include_slots: false,
            },
            subscription: ProtocolSubscription {
                program_ids: vec![decode_to_32_bytes(valid, "test").unwrap()],
                account_pubkeys: vec![],
                include_slots: false,
            },
        };

        assert_eq!(protocol.name(), "TestProtocol");
        assert_eq!(protocol.subscription().program_ids.len(), 1);
        assert_eq!(protocol.decoders().len(), 0); // No custom decoders
    }

    #[test]
    fn merge_subscriptions_deduplicates_programs() {
        let program_id = decode_to_32_bytes("9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM", "test").unwrap();

        let protocol1 = ConfigOnlyProtocol {
            config: ProtocolConfig {
                name: "Protocol1".to_string(),
                programs: vec!["9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM".to_string()],
                accounts: vec![],
                include_slots: false,
            },
            subscription: ProtocolSubscription {
                program_ids: vec![program_id],
                account_pubkeys: vec![],
                include_slots: false,
            },
        };

        let protocol2 = ConfigOnlyProtocol {
            config: ProtocolConfig {
                name: "Protocol2".to_string(),
                programs: vec!["9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM".to_string()],
                accounts: vec![],
                include_slots: false,
            },
            subscription: ProtocolSubscription {
                program_ids: vec![program_id],
                account_pubkeys: vec![],
                include_slots: false,
            },
        };

        let merged = merge_subscriptions(&[
            Box::new(protocol1),
            Box::new(protocol2),
        ]);

        // Should deduplicate the shared program ID
        assert_eq!(merged.program_ids.len(), 1);
    }
}
