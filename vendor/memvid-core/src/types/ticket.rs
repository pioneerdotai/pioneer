//! Ticket metadata exchanged with the control plane.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketRef {
    pub issuer: String,
    pub seq_no: i64,
    pub expires_in_secs: u64,
    #[serde(default)]
    pub capacity_bytes: u64,
    /// Whether this ticket was cryptographically verified against the Memvid public key.
    /// Only tickets applied via `apply_signed_ticket()` will have this set to true.
    #[serde(default)]
    pub verified: bool,
}

/// Ticket information provided by the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticket {
    pub issuer: String,
    pub seq_no: i64,
    pub expires_in_secs: u64,
    pub capacity_bytes: Option<u64>,
}

impl Ticket {
    #[must_use]
    pub fn new<I: Into<String>>(issuer: I, seq_no: i64) -> Self {
        Self {
            issuer: issuer.into(),
            seq_no,
            expires_in_secs: 0,
            capacity_bytes: None,
        }
    }

    #[must_use]
    pub fn expires_in_secs(mut self, value: u64) -> Self {
        self.expires_in_secs = value;
        self
    }

    #[must_use]
    pub fn capacity_bytes(mut self, value: u64) -> Self {
        self.capacity_bytes = Some(value);
        self
    }
}

/// A cryptographically signed ticket from the Memvid control plane.
/// Contains a signature that can be verified against the embedded public key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedTicket {
    /// The issuer identifier (e.g., "memvid.com").
    pub issuer: String,
    /// Monotonically increasing sequence number for replay protection.
    pub seq_no: i64,
    /// How long this ticket is valid (in seconds from issuance).
    pub expires_in_secs: u64,
    /// The capacity in bytes this ticket grants (None = use tier default).
    pub capacity_bytes: Option<u64>,
    /// The memory ID this ticket is bound to.
    pub memory_id: Uuid,
    /// The Ed25519 signature over the ticket payload.
    #[serde(with = "base64_bytes")]
    pub signature: Vec<u8>,
}

impl SignedTicket {
    /// Creates a new signed ticket with all required fields.
    #[must_use]
    pub fn new(
        issuer: impl Into<String>,
        seq_no: i64,
        expires_in_secs: u64,
        capacity_bytes: Option<u64>,
        memory_id: Uuid,
        signature: Vec<u8>,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            seq_no,
            expires_in_secs,
            capacity_bytes,
            memory_id,
            signature,
        }
    }
}

/// Serde helper for base64-encoded byte vectors.
mod base64_bytes {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&BASE64_STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        BASE64_STANDARD.decode(&s).map_err(serde::de::Error::custom)
    }
}
