use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use pioneer_protocol::{
    DEVICE_ACTIVATION_ALPHABET, device_activation_locator, encode_device_activation_entropy,
    normalize_device_activation_code,
};
use sha2::Sha256;
use zeroize::Zeroize;

use super::{AuthError, AuthErrorCode};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpaqueCredentialKind {
    Refresh,
    DeviceActivation,
}

impl OpaqueCredentialKind {
    const fn domain(self) -> &'static [u8] {
        match self {
            Self::Refresh => b"refresh\0",
            Self::DeviceActivation => b"device_activation\0",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct OpaqueCredential {
    kind: OpaqueCredentialKind,
    value: String,
}

impl OpaqueCredential {
    pub(crate) fn expose_for_exchange(&self) -> &str {
        self.value.as_str()
    }
}

impl std::fmt::Debug for OpaqueCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpaqueCredential")
            .field("kind", &self.kind)
            .field("value", &"[redacted]")
            .finish()
    }
}

impl std::fmt::Display for OpaqueCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[redacted]")
    }
}

impl Drop for OpaqueCredential {
    fn drop(&mut self) {
        self.value.zeroize();
    }
}

pub(crate) struct OpaqueCredentialFactory {
    hmac_key: Vec<u8>,
    entropy_bytes: usize,
}

impl OpaqueCredentialFactory {
    pub(crate) fn new(hmac_key: &[u8], entropy_bytes: usize) -> Result<Self, AuthError> {
        if hmac_key.len() < 32 || entropy_bytes < 32 {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        Ok(Self {
            hmac_key: hmac_key.to_vec(),
            entropy_bytes,
        })
    }

    pub(crate) fn generate_refresh(&self) -> OpaqueCredential {
        let mut entropy = vec![0u8; self.entropy_bytes];
        rand::fill(entropy.as_mut_slice());
        let encoded = URL_SAFE_NO_PAD.encode(&entropy);
        entropy.zeroize();
        OpaqueCredential {
            kind: OpaqueCredentialKind::Refresh,
            value: format!("prf_{encoded}"),
        }
    }

    pub(crate) fn generate_device_activation(&self) -> OpaqueCredential {
        let mut entropy = [0u8; 5];
        rand::fill(&mut entropy);
        let encoded = encode_device_activation_entropy(entropy);
        entropy.zeroize();
        OpaqueCredential {
            kind: OpaqueCredentialKind::DeviceActivation,
            value: encoded,
        }
    }

    pub(crate) fn generate_device_activation_for_locator(
        &self,
        locator: &str,
    ) -> Result<OpaqueCredential, AuthError> {
        if locator.len() != 1
            || !DEVICE_ACTIVATION_ALPHABET
                .iter()
                .any(|candidate| locator.as_bytes().first() == Some(candidate))
        {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        let mut credential = self.generate_device_activation();
        credential.value.replace_range(..1, locator);
        Ok(credential)
    }

    #[cfg(test)]
    pub(crate) fn refresh_from_entropy(
        &self,
        entropy: &[u8],
    ) -> Result<OpaqueCredential, AuthError> {
        self.credential_from_entropy(OpaqueCredentialKind::Refresh, entropy)
    }

    #[cfg(test)]
    pub(crate) fn device_activation_from_entropy(
        &self,
        entropy: &[u8],
    ) -> Result<OpaqueCredential, AuthError> {
        if entropy.len() != 5 {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        let mut fixed = [0u8; 5];
        fixed.copy_from_slice(entropy);
        Ok(OpaqueCredential {
            kind: OpaqueCredentialKind::DeviceActivation,
            value: encode_device_activation_entropy(fixed),
        })
    }

    #[cfg(test)]
    fn credential_from_entropy(
        &self,
        kind: OpaqueCredentialKind,
        entropy: &[u8],
    ) -> Result<OpaqueCredential, AuthError> {
        if entropy.len() != self.entropy_bytes {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        let encoded = URL_SAFE_NO_PAD.encode(entropy);
        let value = match kind {
            OpaqueCredentialKind::Refresh => format!("prf_{encoded}"),
            OpaqueCredentialKind::DeviceActivation => {
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
        };
        Ok(OpaqueCredential { kind, value })
    }

    pub(crate) fn fingerprint(&self, credential: &OpaqueCredential) -> [u8; 32] {
        fingerprint(
            self.hmac_key.as_slice(),
            credential.kind.domain(),
            credential.value.as_bytes(),
        )
    }

    pub(crate) fn fingerprint_refresh_raw(&self, token: &str) -> [u8; 32] {
        fingerprint(self.hmac_key.as_slice(), b"refresh\0", token.as_bytes())
    }

    pub(crate) fn fingerprint_device_activation_raw(&self, code: &str) -> [u8; 32] {
        let canonical = normalize_device_activation_code(code)
            .expect("classified device activation code must be canonicalizable");
        fingerprint(
            self.hmac_key.as_slice(),
            b"device_activation\0",
            canonical.as_bytes(),
        )
    }

    pub(crate) fn device_activation_locator(
        &self,
        credential: &OpaqueCredential,
    ) -> Result<String, AuthError> {
        if credential.kind != OpaqueCredentialKind::DeviceActivation {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        device_activation_locator(credential.value.as_str())
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))
    }

    pub(crate) fn fingerprint_device_activation_locator(
        &self,
        credential: &OpaqueCredential,
    ) -> Result<[u8; 32], AuthError> {
        let locator = self.device_activation_locator(credential)?;
        self.fingerprint_device_activation_locator_symbol(locator.as_str())
    }

    pub(crate) fn fingerprint_device_activation_locator_raw(
        &self,
        code: &str,
    ) -> Result<[u8; 32], AuthError> {
        let locator = device_activation_locator(code)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        self.fingerprint_device_activation_locator_symbol(locator.as_str())
    }

    pub(crate) fn fingerprint_device_activation_locator_symbol(
        &self,
        locator: &str,
    ) -> Result<[u8; 32], AuthError> {
        if locator.len() != 1
            || !DEVICE_ACTIVATION_ALPHABET
                .iter()
                .any(|candidate| locator.as_bytes().first() == Some(candidate))
        {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        Ok(fingerprint(
            self.hmac_key.as_slice(),
            b"device_activation_locator\0",
            locator.as_bytes(),
        ))
    }
}

impl Drop for OpaqueCredentialFactory {
    fn drop(&mut self) {
        self.hmac_key.zeroize();
    }
}

fn fingerprint(key: &[u8], domain: &[u8], value: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts arbitrary key length");
    mac.update(domain);
    mac.update(value);
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_fixtures_are_redacted_and_domain_separated() {
        let factory = OpaqueCredentialFactory::new(&[7; 64], 32).unwrap();
        let entropy = [1; 32];
        let refresh = factory.refresh_from_entropy(&entropy).unwrap();
        let repeated_refresh = factory.refresh_from_entropy(&entropy).unwrap();
        let activation = factory.device_activation_from_entropy(&[1; 5]).unwrap();
        assert!(refresh.expose_for_exchange().starts_with("prf_"));
        assert_eq!(activation.expose_for_exchange().len(), 8);
        assert!(normalize_device_activation_code(activation.expose_for_exchange()).is_ok());
        assert_eq!(
            factory
                .device_activation_locator(&activation)
                .expect("activation locator")
                .len(),
            1
        );
        assert_eq!(
            refresh.expose_for_exchange(),
            repeated_refresh.expose_for_exchange()
        );
        assert_eq!(
            factory.fingerprint(&refresh),
            factory.fingerprint(&repeated_refresh)
        );
        assert_ne!(
            factory.fingerprint(&refresh),
            factory.fingerprint(&activation)
        );
        let locator_hash = factory
            .fingerprint_device_activation_locator(&activation)
            .expect("locator fingerprint");
        assert_eq!(
            locator_hash,
            factory
                .fingerprint_device_activation_locator_raw(activation.expose_for_exchange())
                .expect("raw locator fingerprint")
        );
        assert_ne!(locator_hash, factory.fingerprint(&activation));
        assert!(!format!("{refresh:?} {refresh}").contains(refresh.expose_for_exchange()));
    }

    #[test]
    fn requested_locator_replaces_only_the_transient_routing_symbol() {
        let factory = OpaqueCredentialFactory::new(&[7; 64], 32).unwrap();
        let activation = factory
            .generate_device_activation_for_locator("Z")
            .expect("activation");
        assert!(activation.expose_for_exchange().starts_with('Z'));
        assert_eq!(activation.expose_for_exchange().len(), 8);
    }
}
