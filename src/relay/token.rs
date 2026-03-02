//! Relay join token encode/decode.
//!
//! Tokens let devices share relay configuration compactly:
//!   v0x01: public broker  — 0x01 + 16 UUID bytes + 1 broker index → 18 bytes → ~24 char base64url
//!   v0x02: private broker — 0x02 + 16 UUID bytes + URL bytes → variable length base64url

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use super::DEFAULT_BROKERS;

/// Encode relay_id and broker URL into a compact join token.
pub fn encode_join_token(relay_id: &str, broker_url: &str) -> Option<String> {
    let uuid_bytes = uuid_to_bytes(relay_id)?;

    // Check if broker matches a known public broker → v0x01
    for (i, (host, port)) in DEFAULT_BROKERS.iter().enumerate() {
        let mqtts = format!("mqtts://{}:{}", host, port);
        let mqtt = format!("mqtt://{}:{}", host, port);
        if broker_url == mqtts || broker_url == mqtt {
            let mut buf = Vec::with_capacity(18);
            buf.push(0x01);
            buf.extend_from_slice(&uuid_bytes);
            buf.push(i as u8);
            return Some(URL_SAFE_NO_PAD.encode(&buf));
        }
    }

    // Private broker → v0x02
    let mut buf = Vec::with_capacity(17 + broker_url.len());
    buf.push(0x02);
    buf.extend_from_slice(&uuid_bytes);
    buf.extend_from_slice(broker_url.as_bytes());
    Some(URL_SAFE_NO_PAD.encode(&buf))
}

/// Decode a join token back to (relay_id, broker_url).
pub fn decode_join_token(token: &str) -> Option<(String, String)> {
    // Add base64 padding if needed
    let padded = match token.len() % 4 {
        2 => format!("{}==", token),
        3 => format!("{}=", token),
        _ => token.to_string(),
    };

    let raw = URL_SAFE_NO_PAD.decode(padded.trim_end_matches('=')).ok()?;

    // Minimum: 1 version + 16 UUID = 17 bytes
    if raw.len() < 17 {
        return None;
    }

    let version = raw[0];
    let relay_id = bytes_to_uuid(&raw[1..17])?;

    match version {
        0x01 => {
            if raw.len() < 18 {
                return None;
            }
            // Public broker index
            let idx = raw[17] as usize;
            if idx >= DEFAULT_BROKERS.len() {
                return None;
            }
            let (host, port) = DEFAULT_BROKERS[idx];
            Some((relay_id, format!("mqtts://{}:{}", host, port)))
        }
        0x02 => {
            // Private broker URL
            let broker_url = std::str::from_utf8(&raw[17..]).ok()?;
            Some((relay_id, broker_url.to_string()))
        }
        _ => None,
    }
}

/// Parse UUID string (with hyphens) to 16 bytes.
fn uuid_to_bytes(uuid_str: &str) -> Option<[u8; 16]> {
    let hex: String = uuid_str.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

/// Format 16 bytes as hyphenated UUID string.
fn bytes_to_uuid(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 16 {
        return None;
    }
    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_roundtrip() {
        let uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let bytes = uuid_to_bytes(uuid).unwrap();
        let back = bytes_to_uuid(&bytes).unwrap();
        assert_eq!(uuid, back);
    }

    #[test]
    fn test_uuid_to_bytes_invalid() {
        assert!(uuid_to_bytes("not-a-uuid").is_none());
        assert!(uuid_to_bytes("").is_none());
    }

    #[test]
    fn test_encode_decode_v0x01_public_broker() {
        let relay_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

        // Test each public broker
        for (i, (host, port)) in DEFAULT_BROKERS.iter().enumerate() {
            let broker = format!("mqtts://{}:{}", host, port);
            let token = encode_join_token(relay_id, &broker).unwrap();
            let (decoded_id, decoded_broker) = decode_join_token(&token).unwrap();
            assert_eq!(decoded_id, relay_id, "broker index {}", i);
            assert_eq!(decoded_broker, broker, "broker index {}", i);
        }
    }

    #[test]
    fn test_encode_decode_v0x02_private_broker() {
        let relay_id = "12345678-1234-1234-1234-123456789abc";
        let broker = "mqtts://my-private-broker.example.com:8883";

        let token = encode_join_token(relay_id, broker).unwrap();
        let (decoded_id, decoded_broker) = decode_join_token(&token).unwrap();
        assert_eq!(decoded_id, relay_id);
        assert_eq!(decoded_broker, broker);
    }

    #[test]
    fn test_v0x01_token_is_compact() {
        let relay_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let broker = format!("mqtts://{}:{}", DEFAULT_BROKERS[0].0, DEFAULT_BROKERS[0].1);
        let token = encode_join_token(relay_id, &broker).unwrap();
        // 18 bytes → 24 chars base64url (no padding)
        assert_eq!(token.len(), 24);
    }

    #[test]
    fn test_decode_invalid_token() {
        assert!(decode_join_token("").is_none());
        assert!(decode_join_token("short").is_none());
        assert!(decode_join_token("!!!invalid-base64!!!").is_none());
    }

    #[test]
    fn test_decode_v0x01_bad_broker_index() {
        // Craft token with broker index 255 (out of range)
        let uuid_bytes = uuid_to_bytes("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let mut buf = vec![0x01];
        buf.extend_from_slice(&uuid_bytes);
        buf.push(255);
        let token = URL_SAFE_NO_PAD.encode(&buf);
        assert!(decode_join_token(&token).is_none());
    }

    #[test]
    fn test_cross_language_parity_v0x01() {
        // Known v0x01 test vector: relay_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890", broker="mqtts://broker.emqx.io:8883"
        let expected_token = "AaGyw9Tl9niQq83vEjRWeJAA";
        let (relay_id, broker) = decode_join_token(expected_token).unwrap();
        assert_eq!(relay_id, "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert_eq!(broker, "mqtts://broker.emqx.io:8883");

        // Encode should produce identical token
        let rust_token = encode_join_token(&relay_id, &broker).unwrap();
        assert_eq!(rust_token, expected_token);
    }

    #[test]
    fn test_cross_language_parity_v0x02() {
        // Known v0x02 test vector: relay_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890", broker="mqtts://my-private-broker.example.com:8883"
        let expected_token = "AqGyw9Tl9niQq83vEjRWeJBtcXR0czovL215LXByaXZhdGUtYnJva2VyLmV4YW1wbGUuY29tOjg4ODM";
        let (relay_id, broker) = decode_join_token(expected_token).unwrap();
        assert_eq!(relay_id, "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert_eq!(broker, "mqtts://my-private-broker.example.com:8883");

        // Encode should produce identical token
        let rust_token = encode_join_token(&relay_id, &broker).unwrap();
        assert_eq!(rust_token, expected_token);
    }
}
