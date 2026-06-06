//! Gateway secret reference helpers.

use std::{error::Error, fmt};

pub const MAX_GATEWAY_SECRET_REF_LEN: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GatewayAuthTokenRef(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewaySecretRefError {
    Empty,
    TooLong { max_len: usize },
    ContainsPathSeparator,
}

pub trait GatewayAuthTokenRefNamer {
    fn auth_token_ref_for_endpoint(
        &self,
        endpoint_id: &str,
    ) -> Result<GatewayAuthTokenRef, GatewaySecretRefError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EndpointIdGatewayAuthTokenRefNamer;

impl GatewayAuthTokenRef {
    pub fn new(value: impl AsRef<str>) -> Result<Self, GatewaySecretRefError> {
        let value = value.as_ref().trim();
        if value.is_empty() {
            return Err(GatewaySecretRefError::Empty);
        }
        if value.len() > MAX_GATEWAY_SECRET_REF_LEN {
            return Err(GatewaySecretRefError::TooLong {
                max_len: MAX_GATEWAY_SECRET_REF_LEN,
            });
        }
        if value.contains('/') || value.contains('\\') {
            return Err(GatewaySecretRefError::ContainsPathSeparator);
        }

        Ok(Self(value.to_owned()))
    }

    pub fn for_endpoint_id(endpoint_id: &str) -> Result<Self, GatewaySecretRefError> {
        Self::new(endpoint_id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl GatewayAuthTokenRefNamer for EndpointIdGatewayAuthTokenRefNamer {
    fn auth_token_ref_for_endpoint(
        &self,
        endpoint_id: &str,
    ) -> Result<GatewayAuthTokenRef, GatewaySecretRefError> {
        GatewayAuthTokenRef::for_endpoint_id(endpoint_id)
    }
}

pub fn normalize_gateway_auth_token(token: &str) -> Option<String> {
    let trimmed = token.trim();
    (!trimmed.is_empty()).then_some(trimmed.to_owned())
}

pub fn gateway_auth_token_label(name: &str, address: &str) -> String {
    format!("{} ({})", name.trim(), address.trim())
}

impl fmt::Display for GatewaySecretRefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "gateway secret ref must not be empty"),
            Self::TooLong { max_len } => {
                write!(f, "gateway secret ref must be at most {max_len} bytes")
            }
            Self::ContainsPathSeparator => {
                write!(f, "gateway secret ref must not contain path separators")
            }
        }
    }
}

impl Error for GatewaySecretRefError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_secret_ref_trims_and_rejects_invalid_values() {
        assert_eq!(
            GatewayAuthTokenRef::new("  remote-123  ")
                .expect("valid ref")
                .as_str(),
            "remote-123"
        );
        assert_eq!(
            GatewayAuthTokenRef::new("   ").expect_err("empty ref"),
            GatewaySecretRefError::Empty
        );
        assert_eq!(
            GatewayAuthTokenRef::new("../remote-123").expect_err("path ref"),
            GatewaySecretRefError::ContainsPathSeparator
        );
    }

    #[test]
    fn gateway_secret_ref_namer_uses_endpoint_id() {
        let namer = EndpointIdGatewayAuthTokenRefNamer;
        let token_ref = namer
            .auth_token_ref_for_endpoint("remote-123")
            .expect("token ref");

        assert_eq!(token_ref.as_str(), "remote-123");
    }

    #[test]
    fn gateway_secret_helpers_trim_token_and_label() {
        assert_eq!(
            normalize_gateway_auth_token("  bearer-token  ").as_deref(),
            Some("bearer-token")
        );
        assert!(normalize_gateway_auth_token("   ").is_none());
        assert_eq!(
            gateway_auth_token_label(" Remote ", " 127.0.0.1:22000 "),
            "Remote (127.0.0.1:22000)"
        );
    }
}
