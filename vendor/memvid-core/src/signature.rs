use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use ed25519_dalek::{Signature, VerifyingKey};
use serde::Serialize;
use std::convert::TryInto;
use uuid::Uuid;

use crate::error::{MemvidError, Result};

const SIGNING_SCHEMA_VERSION: u8 = 1;

#[derive(Serialize)]
struct TicketSignaturePayload<'a> {
    version: u8,
    memory_id: &'a Uuid,
    issuer: &'a str,
    seq_no: i64,
    expires_in: u64,
    capacity_bytes: Option<u64>,
}

#[derive(Serialize)]
struct ModelSignaturePayload<'a> {
    version: u8,
    name: &'a str,
    model_version: &'a str,
    checksum: &'a str,
    size_bytes: u64,
}

fn ticket_message_bytes(
    memory_id: &Uuid,
    issuer: &str,
    seq_no: i64,
    expires_in: u64,
    capacity_bytes: Option<u64>,
) -> Result<Vec<u8>> {
    let payload = TicketSignaturePayload {
        version: SIGNING_SCHEMA_VERSION,
        memory_id,
        issuer,
        seq_no,
        expires_in,
        capacity_bytes,
    };
    serde_json::to_vec(&payload).map_err(|err| MemvidError::TicketSignatureInvalid {
        reason: format!("failed to serialize ticket payload: {err}").into_boxed_str(),
    })
}

fn model_message_bytes(
    name: &str,
    model_version: &str,
    checksum_hex: &str,
    size_bytes: u64,
) -> Result<Vec<u8>> {
    let payload = ModelSignaturePayload {
        version: SIGNING_SCHEMA_VERSION,
        name,
        model_version,
        checksum: checksum_hex,
        size_bytes,
    };
    serde_json::to_vec(&payload).map_err(|err| MemvidError::ModelSignatureInvalid {
        reason: format!("failed to serialize model payload: {err}").into_boxed_str(),
    })
}

pub fn verify_ticket_signature(
    verifying_key: &VerifyingKey,
    memory_id: &Uuid,
    issuer: &str,
    seq_no: i64,
    expires_in: u64,
    capacity_bytes: Option<u64>,
    signature_bytes: &[u8],
) -> Result<()> {
    let message = ticket_message_bytes(memory_id, issuer, seq_no, expires_in, capacity_bytes)?;
    let signature = to_signature(signature_bytes)
        .map_err(|reason| MemvidError::TicketSignatureInvalid { reason })?;
    verifying_key
        .verify_strict(&message, &signature)
        .map_err(|_| MemvidError::TicketSignatureInvalid {
            reason: "ticket signature mismatch".into(),
        })
}

pub fn verify_model_manifest(
    verifying_key: &VerifyingKey,
    name: &str,
    model_version: &str,
    checksum_hex: &str,
    size_bytes: u64,
    signature_bytes: &[u8],
) -> Result<()> {
    let message = model_message_bytes(name, model_version, checksum_hex, size_bytes)?;
    let signature = to_signature(signature_bytes)
        .map_err(|reason| MemvidError::ModelSignatureInvalid { reason })?;
    verifying_key
        .verify_strict(&message, &signature)
        .map_err(|_| MemvidError::ModelSignatureInvalid {
            reason: "model signature mismatch".into(),
        })
}

fn to_signature(bytes: &[u8]) -> std::result::Result<Signature, Box<str>> {
    let array: [u8; 64] = bytes
        .try_into()
        .map_err(|_| Box::<str>::from("signature must be exactly 64 bytes"))?;
    Ok(Signature::from_bytes(&array))
}

pub fn parse_ed25519_public_key_base64(encoded: &str) -> Result<VerifyingKey> {
    let trimmed = encoded.trim();
    let bytes =
        BASE64_STANDARD
            .decode(trimmed)
            .map_err(|err| MemvidError::TicketSignatureInvalid {
                reason: format!("invalid base64 public key: {err}").into_boxed_str(),
            })?;
    let array: [u8; 32] =
        bytes
            .as_slice()
            .try_into()
            .map_err(|_| MemvidError::TicketSignatureInvalid {
                reason: "public key must be 32 bytes".into(),
            })?;
    VerifyingKey::from_bytes(&array).map_err(|err| MemvidError::TicketSignatureInvalid {
        reason: format!("invalid public key: {err}").into_boxed_str(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn test_signing_key() -> SigningKey {
        let seed = [7u8; 32];
        SigningKey::from_bytes(&seed)
    }

    /// Verify the JSON format matches what the dashboard produces
    #[test]
    fn test_payload_json_format() {
        let memory_id = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let payload = TicketSignaturePayload {
            version: 1,
            memory_id: &memory_id,
            issuer: "memvid-dashboard",
            seq_no: 2,
            expires_in: 86400,
            capacity_bytes: Some(10737418240),
        };
        let json = serde_json::to_string(&payload).unwrap();
        // Must match dashboard format exactly for signature verification
        let expected = r#"{"version":1,"memory_id":"123e4567-e89b-12d3-a456-426614174000","issuer":"memvid-dashboard","seq_no":2,"expires_in":86400,"capacity_bytes":10737418240}"#;
        assert_eq!(json, expected, "JSON format must match dashboard");
    }

    #[test]
    fn ticket_roundtrip() {
        let signing = test_signing_key();
        let verifying = signing.verifying_key();
        let memory_id = Uuid::nil();
        let message = ticket_message_bytes(&memory_id, "issuer", 5, 60, Some(42)).unwrap();
        let signature = signing.sign(&message);
        verify_ticket_signature(
            &verifying,
            &memory_id,
            "issuer",
            5,
            60,
            Some(42),
            &signature.to_bytes(),
        )
        .unwrap();
    }

    #[test]
    fn model_roundtrip() {
        let signing = test_signing_key();
        let verifying = signing.verifying_key();
        let message = model_message_bytes("model", "1.0.0", "abc123", 1024).unwrap();
        let signature = signing.sign(&message);
        verify_model_manifest(
            &verifying,
            "model",
            "1.0.0",
            "abc123",
            1024,
            &signature.to_bytes(),
        )
        .unwrap();
    }

    #[test]
    fn parse_public_key() {
        let signing = test_signing_key();
        let verifying = signing.verifying_key();
        let encoded = BASE64_STANDARD.encode(verifying.as_bytes());
        let parsed = parse_ed25519_public_key_base64(&encoded).unwrap();
        assert_eq!(parsed.as_bytes(), verifying.as_bytes());
    }

    /// End-to-end test verifying the signature flow works correctly.
    /// Uses a test keypair (NOT production keys).
    #[test]
    fn test_signature_flow_e2e() {
        // Test keypair - NOT production keys
        let signing_key = test_signing_key();
        let verifying_key = signing_key.verifying_key();

        // Create and sign a ticket payload (mimicking dashboard)
        let memory_id = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let message =
            ticket_message_bytes(&memory_id, "memvid-dashboard", 2, 86400, Some(10737418240))
                .unwrap();
        let signature = signing_key.sign(&message);

        // Verify signature (mimicking CLI/core)
        verify_ticket_signature(
            &verifying_key,
            &memory_id,
            "memvid-dashboard",
            2,
            86400,
            Some(10737418240),
            &signature.to_bytes(),
        )
        .expect("signature verification should pass");
    }

    /// Verify the embedded public key constant is valid and parseable
    #[test]
    fn test_embedded_pubkey_valid() {
        use crate::constants::MEMVID_TICKET_PUBKEY;
        let key = parse_ed25519_public_key_base64(MEMVID_TICKET_PUBKEY);
        assert!(key.is_ok(), "Embedded MEMVID_TICKET_PUBKEY must be valid");
    }

    /// Test verification with actual dashboard-signed data
    #[test]
    fn test_dashboard_signature_verification() {
        use crate::constants::MEMVID_TICKET_PUBKEY;

        // Parse embedded public key
        let verifying_key = parse_ed25519_public_key_base64(MEMVID_TICKET_PUBKEY).unwrap();

        // Exact payload from dashboard (must match byte-for-byte)
        let memory_id = Uuid::parse_str("69601cef-bea5-7ba3-fec3-9b5c00000000").unwrap();
        let message =
            ticket_message_bytes(&memory_id, "memvid-dashboard", 9, 86400, Some(10737418240))
                .unwrap();

        println!("Rust payload: {}", String::from_utf8_lossy(&message));

        // Signature from dashboard (seq_no=9)
        let sig_base64 = "OUVSB4rKCSPDlP+rrZN1AlkI6k2zDdNaZb5HKPZDTjqhnCHBYKXg4lyEE4aevDN7rLpdFjINiCCaBEBaH35vDw==";
        let sig_bytes = BASE64_STANDARD.decode(sig_base64).unwrap();

        // Verify
        verify_ticket_signature(
            &verifying_key,
            &memory_id,
            "memvid-dashboard",
            9,
            86400,
            Some(10737418240),
            &sig_bytes,
        )
        .expect("Dashboard signature should verify");
    }
}
