// Data sanitization layer - mathematical safety for all incoming data

/// Sanitizes a string for safe database storage
/// - Replaces invalid UTF-8 sequences with the Unicode replacement character (U+FFFD)
/// - Removes null bytes
/// - Truncates excessively long strings
pub fn sanitize_string(input: &str, max_length_chars: usize) -> String {
    // Remove null bytes (can cause issues with PostgreSQL text types)
    let without_nulls: String = input.chars().filter(|c| *c != '\0').collect();

    // Truncate using the same unit we enforce: Unicode scalar values.
    if without_nulls.chars().count() > max_length_chars {
        without_nulls.chars().take(max_length_chars).collect()
    } else {
        without_nulls
    }
}

/// Sanitizes a Vec<u8> that should be valid UTF-8
/// Returns a valid String, replacing invalid sequences with U+FFFD
pub fn sanitize_utf8(bytes: &[u8]) -> String {
    collapse_replacement_runs(&String::from_utf8_lossy(bytes))
}

/// Sanitizes log messages for database storage
/// Log messages can contain arbitrary binary data from programs
pub fn sanitize_log_message(msg: &str) -> String {
    // Max log message length: 1,048,576 Unicode scalar values.
    // This keeps the truncation unit consistent with sanitize_string.
    const MAX_LOG_LENGTH_CHARS: usize = 1_048_576;

    let filtered: String = msg
        .chars()
        .filter(|c| *c != '\0' && *c != '\u{FFFD}')
        .collect();

    sanitize_string(&filtered, MAX_LOG_LENGTH_CHARS)
}

/// Sanitizes a vector of log messages
pub fn sanitize_log_messages(messages: &[String]) -> Vec<String> {
    messages
        .iter()
        .map(|msg| sanitize_log_message(msg))
        .collect()
}

fn collapse_replacement_runs(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut last_was_replacement = false;

    for ch in input.chars() {
        if ch == '\u{FFFD}' {
            if !last_was_replacement {
                output.push(ch);
                last_was_replacement = true;
            }
        } else {
            output.push(ch);
            last_was_replacement = false;
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_string_removes_null_bytes() {
        let input = "hello\0world";
        let result = sanitize_string(input, 100);
        assert_eq!(result, "helloworld");
    }

    #[test]
    fn test_sanitize_string_truncates_long_strings() {
        let input = "a".repeat(1000);
        let result = sanitize_string(&input, 100);
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_sanitize_string_truncates_by_codepoint_count() {
        let input = "🙂🙂🙂";
        let result = sanitize_string(input, 2);
        assert_eq!(result, "🙂🙂");
        assert_eq!(result.chars().count(), 2);
    }

    #[test]
    fn test_sanitize_utf8_handles_invalid_sequences() {
        let invalid_utf8: Vec<u8> = vec![0xFF, 0xFE, 0x61]; // Invalid UTF-8 followed by 'a'
        let result = sanitize_utf8(&invalid_utf8);
        assert_eq!(result, "�a"); // Replacement char + 'a'
    }

    #[test]
    fn test_sanitize_log_message_handles_binary_data() {
        let binary_msg = "Program invoke\x00\u{FFFD}\u{FFFD} data";
        let result = sanitize_log_message(binary_msg);
        assert_eq!(result, "Program invoke data");
    }
}
