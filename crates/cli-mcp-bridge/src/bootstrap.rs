use serde::{Deserialize, Serialize};
use std::fmt;
use zeroize::{Zeroize, ZeroizeOnDrop};

pub const NONCE_BYTES: usize = 32;
pub const MAX_BOOTSTRAP_BYTES: usize = 16 * 1024;
const MAX_SESSION_ID_BYTES: usize = 128;
const MAX_ENDPOINT_BYTES: usize = 4 * 1024;
const REDACTED_SECRET: &str = "[REDACTED]";

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BridgeSessionId(String);

impl BridgeSessionId {
    pub fn new(value: impl Into<String>) -> Result<Self, BootstrapDecodeError> {
        let value = value.into();
        validate_opaque_text(
            value.as_str(),
            MAX_SESSION_ID_BYTES,
            BootstrapDecodeError::InvalidSessionId,
        )?;
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(BootstrapDecodeError::InvalidSessionId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for BridgeSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("BridgeSessionId")
            .field(&self.0)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BridgeGeneration(u64);

impl BridgeGeneration {
    pub fn new(value: u64) -> Result<Self, BootstrapDecodeError> {
        if value == 0 {
            return Err(BootstrapDecodeError::InvalidGeneration);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct BootstrapNonce([u8; NONCE_BYTES]);

impl BootstrapNonce {
    pub fn new(bytes: [u8; NONCE_BYTES]) -> Result<Self, BootstrapDecodeError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(BootstrapDecodeError::InvalidNonce);
        }
        Ok(Self(bytes))
    }

    pub fn expose_secret(&self) -> &[u8; NONCE_BYTES] {
        &self.0
    }
}

impl fmt::Debug for BootstrapNonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REDACTED_SECRET)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeEndpointKind {
    UnixDomainSocket,
    WindowsNamedPipe,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BridgeEndpoint {
    kind: BridgeEndpointKind,
    address: String,
}

impl BridgeEndpoint {
    pub fn new(
        kind: BridgeEndpointKind,
        address: impl Into<String>,
    ) -> Result<Self, BootstrapDecodeError> {
        let address = address.into();
        validate_opaque_text(
            address.as_str(),
            MAX_ENDPOINT_BYTES,
            BootstrapDecodeError::InvalidEndpoint,
        )?;
        Ok(Self { kind, address })
    }

    pub const fn kind(&self) -> BridgeEndpointKind {
        self.kind
    }

    pub fn address(&self) -> &str {
        self.address.as_str()
    }
}

impl fmt::Debug for BridgeEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BridgeEndpoint")
            .field("kind", &self.kind)
            .field("address", &self.address)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct BootstrapDocument {
    pub session_id: BridgeSessionId,
    pub generation: BridgeGeneration,
    pub endpoint: BridgeEndpoint,
    pub nonce: BootstrapNonce,
    pub expires_at_unix_ms: u64,
}

impl BootstrapDocument {
    pub fn encode(&self) -> Result<Vec<u8>, BootstrapEncodeError> {
        let wire = WireBootstrapDocument {
            version: 1,
            session_id: self.session_id.as_str(),
            generation: self.generation.get(),
            endpoint_kind: self.endpoint.kind(),
            endpoint_address: self.endpoint.address(),
            nonce_hex: encode_hex(self.nonce.expose_secret()),
            expires_at_unix_ms: self.expires_at_unix_ms,
        };
        let encoded =
            serde_json::to_vec(&wire).map_err(|_| BootstrapEncodeError::EncodingFailed)?;
        if encoded.len() > MAX_BOOTSTRAP_BYTES {
            return Err(BootstrapEncodeError::TooLarge {
                actual: encoded.len(),
                max: MAX_BOOTSTRAP_BYTES,
            });
        }
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BootstrapDecodeError> {
        if bytes.len() > MAX_BOOTSTRAP_BYTES {
            return Err(BootstrapDecodeError::TooLarge {
                actual: bytes.len(),
                max: MAX_BOOTSTRAP_BYTES,
            });
        }
        let mut wire: OwnedWireBootstrapDocument =
            serde_json::from_slice(bytes).map_err(|_| BootstrapDecodeError::Malformed)?;
        if wire.version != 1 {
            return Err(BootstrapDecodeError::UnsupportedVersion(wire.version));
        }
        if wire.expires_at_unix_ms == 0 {
            return Err(BootstrapDecodeError::InvalidExpiry);
        }
        let nonce = BootstrapNonce::new(decode_nonce(wire.nonce_hex.as_str())?)?;
        Ok(Self {
            session_id: BridgeSessionId::new(std::mem::take(&mut wire.session_id))?,
            generation: BridgeGeneration::new(wire.generation)?,
            endpoint: BridgeEndpoint::new(
                wire.endpoint_kind,
                std::mem::take(&mut wire.endpoint_address),
            )?,
            nonce,
            expires_at_unix_ms: wire.expires_at_unix_ms,
        })
    }

    pub fn attach_request(&self) -> AttachRequest {
        AttachRequest {
            session_id: self.session_id.clone(),
            generation: self.generation,
            nonce: self.nonce.clone(),
        }
    }
}

impl fmt::Debug for BootstrapDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BootstrapDocument")
            .field("session_id", &self.session_id)
            .field("generation", &self.generation)
            .field("endpoint", &self.endpoint)
            .field("nonce", &REDACTED_SECRET)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AttachRequest {
    pub session_id: BridgeSessionId,
    pub generation: BridgeGeneration,
    pub nonce: BootstrapNonce,
}

impl AttachRequest {
    pub fn encode(&self) -> Result<Vec<u8>, BootstrapEncodeError> {
        let wire = WireAttachRequest {
            version: 1,
            session_id: self.session_id.as_str(),
            generation: self.generation.get(),
            nonce_hex: encode_hex(self.nonce.expose_secret()),
        };
        let encoded =
            serde_json::to_vec(&wire).map_err(|_| BootstrapEncodeError::EncodingFailed)?;
        if encoded.len() > MAX_BOOTSTRAP_BYTES {
            return Err(BootstrapEncodeError::TooLarge {
                actual: encoded.len(),
                max: MAX_BOOTSTRAP_BYTES,
            });
        }
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BootstrapDecodeError> {
        if bytes.len() > MAX_BOOTSTRAP_BYTES {
            return Err(BootstrapDecodeError::TooLarge {
                actual: bytes.len(),
                max: MAX_BOOTSTRAP_BYTES,
            });
        }
        let mut wire: OwnedWireAttachRequest =
            serde_json::from_slice(bytes).map_err(|_| BootstrapDecodeError::Malformed)?;
        if wire.version != 1 {
            return Err(BootstrapDecodeError::UnsupportedVersion(wire.version));
        }
        let nonce = BootstrapNonce::new(decode_nonce(wire.nonce_hex.as_str())?)?;
        Ok(Self {
            session_id: BridgeSessionId::new(std::mem::take(&mut wire.session_id))?,
            generation: BridgeGeneration::new(wire.generation)?,
            nonce,
        })
    }
}

impl fmt::Debug for AttachRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachRequest")
            .field("session_id", &self.session_id)
            .field("generation", &self.generation)
            .field("nonce", &REDACTED_SECRET)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapEncodeError {
    EncodingFailed,
    TooLarge { actual: usize, max: usize },
}

impl fmt::Display for BootstrapEncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EncodingFailed => formatter.write_str("failed to encode CLI MCP bootstrap"),
            Self::TooLarge { actual, max } => {
                write!(
                    formatter,
                    "CLI MCP bootstrap is too large: {actual} > {max}"
                )
            }
        }
    }
}

impl std::error::Error for BootstrapEncodeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapDecodeError {
    TooLarge { actual: usize, max: usize },
    Malformed,
    UnsupportedVersion(u16),
    InvalidSessionId,
    InvalidGeneration,
    InvalidEndpoint,
    InvalidNonce,
    InvalidExpiry,
}

impl fmt::Display for BootstrapDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { actual, max } => {
                write!(
                    formatter,
                    "CLI MCP bootstrap is too large: {actual} > {max}"
                )
            }
            Self::Malformed => formatter.write_str("CLI MCP bootstrap is malformed"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported CLI MCP bootstrap version {version}")
            }
            Self::InvalidSessionId => formatter.write_str("invalid CLI MCP bridge session id"),
            Self::InvalidGeneration => formatter.write_str("invalid CLI MCP bridge generation"),
            Self::InvalidEndpoint => formatter.write_str("invalid CLI MCP bridge endpoint"),
            Self::InvalidNonce => formatter.write_str("invalid CLI MCP bootstrap nonce"),
            Self::InvalidExpiry => formatter.write_str("invalid CLI MCP bootstrap expiry"),
        }
    }
}

impl std::error::Error for BootstrapDecodeError {}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireBootstrapDocument<'a> {
    version: u16,
    session_id: &'a str,
    generation: u64,
    endpoint_kind: BridgeEndpointKind,
    endpoint_address: &'a str,
    nonce_hex: String,
    expires_at_unix_ms: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OwnedWireBootstrapDocument {
    version: u16,
    session_id: String,
    generation: u64,
    endpoint_kind: BridgeEndpointKind,
    endpoint_address: String,
    nonce_hex: String,
    expires_at_unix_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireAttachRequest<'a> {
    version: u16,
    session_id: &'a str,
    generation: u64,
    nonce_hex: String,
}

impl Drop for WireBootstrapDocument<'_> {
    fn drop(&mut self) {
        self.nonce_hex.zeroize();
    }
}

impl Drop for WireAttachRequest<'_> {
    fn drop(&mut self) {
        self.nonce_hex.zeroize();
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OwnedWireAttachRequest {
    version: u16,
    session_id: String,
    generation: u64,
    nonce_hex: String,
}

impl Drop for OwnedWireBootstrapDocument {
    fn drop(&mut self) {
        self.nonce_hex.zeroize();
    }
}

impl Drop for OwnedWireAttachRequest {
    fn drop(&mut self) {
        self.nonce_hex.zeroize();
    }
}

fn validate_opaque_text(
    value: &str,
    max_bytes: usize,
    error: BootstrapDecodeError,
) -> Result<(), BootstrapDecodeError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value
            .chars()
            .any(|character| character.is_control() || character == '\0')
    {
        return Err(error);
    }
    Ok(())
}

fn encode_hex(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(ALPHABET[usize::from(byte >> 4)]));
        encoded.push(char::from(ALPHABET[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_nonce(value: &str) -> Result<[u8; NONCE_BYTES], BootstrapDecodeError> {
    if value.len() != NONCE_BYTES * 2 {
        return Err(BootstrapDecodeError::InvalidNonce);
    }
    let mut bytes = [0_u8; NONCE_BYTES];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (decode_hex_nibble(pair[0])? << 4) | decode_hex_nibble(pair[1])?;
    }
    Ok(bytes)
}

fn decode_hex_nibble(value: u8) -> Result<u8, BootstrapDecodeError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(BootstrapDecodeError::InvalidNonce),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_document() -> BootstrapDocument {
        BootstrapDocument {
            session_id: BridgeSessionId::new("bridge-session-1").expect("valid session id"),
            generation: BridgeGeneration::new(7).expect("valid generation"),
            endpoint: BridgeEndpoint::new(
                BridgeEndpointKind::UnixDomainSocket,
                "/private/session/bridge.sock",
            )
            .expect("valid endpoint"),
            nonce: BootstrapNonce::new([0x5a; NONCE_BYTES]).expect("valid nonce"),
            expires_at_unix_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn bootstrap_round_trip_preserves_transport_scope() {
        let document = sample_document();
        let encoded = document.encode().expect("bootstrap should encode");
        let decoded =
            BootstrapDocument::decode(encoded.as_slice()).expect("bootstrap should decode");

        assert_eq!(decoded, document);
        assert_eq!(decoded.attach_request(), document.attach_request());
    }

    #[test]
    fn attach_round_trip_preserves_transport_scope() {
        let request = sample_document().attach_request();
        let encoded = request.encode().expect("attach should encode");
        let decoded = AttachRequest::decode(encoded.as_slice()).expect("attach should decode");

        assert_eq!(decoded, request);
    }

    #[test]
    fn secret_debug_is_redacted() {
        let document = sample_document();
        let canary = encode_hex(document.nonce.expose_secret());

        assert!(!format!("{document:?}").contains(canary.as_str()));
        assert!(!format!("{:?}", document.attach_request()).contains(canary.as_str()));
        assert_eq!(format!("{:?}", document.nonce), REDACTED_SECRET);
    }

    #[test]
    fn bootstrap_rejects_invalid_or_oversized_input() {
        assert_eq!(
            BootstrapDocument::decode(&vec![b'x'; MAX_BOOTSTRAP_BYTES + 1]),
            Err(BootstrapDecodeError::TooLarge {
                actual: MAX_BOOTSTRAP_BYTES + 1,
                max: MAX_BOOTSTRAP_BYTES,
            })
        );
        assert_eq!(
            BootstrapDocument::decode(br#"{"version":99}"#),
            Err(BootstrapDecodeError::Malformed)
        );
    }
}
