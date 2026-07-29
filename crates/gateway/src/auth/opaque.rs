use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use pioneer_protocol::{
    AUTH_DOMAIN_ID_LEN, AuthSessionId, DEVICE_ACTIVATION_ALPHABET, REFRESH_CREDENTIAL_BODY_LEN,
    REFRESH_CREDENTIAL_PREFIX, TokenFamilyId, device_activation_locator,
    encode_device_activation_entropy, normalize_device_activation_code,
};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use super::{AuthError, AuthErrorCode};

type HmacSha256 = Hmac<Sha256>;
const REFRESH_CREDENTIAL_VERSION: u8 = 2;
const REFRESH_NONCE_BYTES: usize = 32;
const REFRESH_MAC_BYTES: usize = 32;
const REFRESH_PAYLOAD_BYTES: usize =
    1 + AUTH_DOMAIN_ID_LEN + AUTH_DOMAIN_ID_LEN + 8 + 8 + REFRESH_NONCE_BYTES;
const REFRESH_ENVELOPE_BYTES: usize = REFRESH_PAYLOAD_BYTES + REFRESH_MAC_BYTES;
const REFRESH_MAC_DOMAIN: &[u8] = b"refresh_envelope_v2\0";

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

#[derive(PartialEq, Eq)]
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedRefreshCredential {
    pub(crate) session_id: AuthSessionId,
    pub(crate) token_family_id: TokenFamilyId,
    pub(crate) generation: u64,
    pub(crate) expires_at_unix: u64,
}

impl OpaqueCredentialFactory {
    pub(crate) fn new(hmac_key: &[u8]) -> Result<Self, AuthError> {
        if hmac_key.len() < 32 {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        Ok(Self {
            hmac_key: hmac_key.to_vec(),
        })
    }

    pub(crate) fn generate_refresh(
        &self,
        session_id: &AuthSessionId,
        token_family_id: &TokenFamilyId,
        generation: u64,
        expires_at_unix: u64,
    ) -> OpaqueCredential {
        let mut nonce = [0u8; REFRESH_NONCE_BYTES];
        rand::fill(&mut nonce);
        let credential = self.build_refresh(
            session_id,
            token_family_id,
            generation,
            expires_at_unix,
            &nonce,
        );
        nonce.zeroize();
        credential
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
    pub(crate) fn refresh_from_nonce(
        &self,
        session_id: &AuthSessionId,
        token_family_id: &TokenFamilyId,
        generation: u64,
        expires_at_unix: u64,
        nonce: &[u8; REFRESH_NONCE_BYTES],
    ) -> OpaqueCredential {
        self.build_refresh(
            session_id,
            token_family_id,
            generation,
            expires_at_unix,
            nonce,
        )
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

    fn build_refresh(
        &self,
        session_id: &AuthSessionId,
        token_family_id: &TokenFamilyId,
        generation: u64,
        expires_at_unix: u64,
        nonce: &[u8; REFRESH_NONCE_BYTES],
    ) -> OpaqueCredential {
        let mut envelope = Vec::with_capacity(REFRESH_ENVELOPE_BYTES);
        envelope.push(REFRESH_CREDENTIAL_VERSION);
        envelope.extend_from_slice(session_id.as_str().as_bytes());
        envelope.extend_from_slice(token_family_id.as_str().as_bytes());
        envelope.extend_from_slice(&generation.to_be_bytes());
        envelope.extend_from_slice(&expires_at_unix.to_be_bytes());
        envelope.extend_from_slice(nonce);
        let mac = fingerprint(self.hmac_key.as_slice(), REFRESH_MAC_DOMAIN, &envelope);
        envelope.extend_from_slice(&mac);
        debug_assert_eq!(envelope.len(), REFRESH_ENVELOPE_BYTES);
        let encoded = Zeroizing::new(URL_SAFE_NO_PAD.encode(&envelope));
        envelope.zeroize();
        debug_assert_eq!(encoded.len(), REFRESH_CREDENTIAL_BODY_LEN);
        OpaqueCredential {
            kind: OpaqueCredentialKind::Refresh,
            value: format!("{REFRESH_CREDENTIAL_PREFIX}{}", encoded.as_str()),
        }
    }

    pub(crate) fn verify_refresh_raw(
        &self,
        token: &str,
    ) -> Result<VerifiedRefreshCredential, AuthError> {
        let body = token
            .strip_prefix(REFRESH_CREDENTIAL_PREFIX)
            .filter(|body| body.len() == REFRESH_CREDENTIAL_BODY_LEN)
            .ok_or_else(|| AuthError::new(AuthErrorCode::MalformedCredential))?;
        let envelope = Zeroizing::new(
            URL_SAFE_NO_PAD
                .decode(body)
                .map_err(|_| AuthError::new(AuthErrorCode::MalformedCredential))?,
        );
        if envelope.len() != REFRESH_ENVELOPE_BYTES {
            return Err(AuthError::new(AuthErrorCode::MalformedCredential));
        }
        let (payload, presented_mac) = envelope.split_at(REFRESH_PAYLOAD_BYTES);
        let mut mac = HmacSha256::new_from_slice(self.hmac_key.as_slice())
            .expect("HMAC accepts arbitrary key length");
        mac.update(REFRESH_MAC_DOMAIN);
        mac.update(payload);
        mac.verify_slice(presented_mac)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        if payload[0] != REFRESH_CREDENTIAL_VERSION {
            return Err(AuthError::new(AuthErrorCode::UnsupportedCredential));
        }

        let session_start = 1;
        let family_start = session_start + AUTH_DOMAIN_ID_LEN;
        let generation_start = family_start + AUTH_DOMAIN_ID_LEN;
        let expiry_start = generation_start + 8;
        let session_id = std::str::from_utf8(&payload[session_start..family_start])
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let token_family_id = std::str::from_utf8(&payload[family_start..generation_start])
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let generation = u64::from_be_bytes(
            payload[generation_start..expiry_start]
                .try_into()
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
        );
        let expires_at_unix = u64::from_be_bytes(
            payload[expiry_start..expiry_start + 8]
                .try_into()
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
        );
        Ok(VerifiedRefreshCredential {
            session_id: AuthSessionId::new(session_id.to_owned())
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
            token_family_id: TokenFamilyId::new(token_family_id.to_owned())
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
            generation,
            expires_at_unix,
        })
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
        let factory = OpaqueCredentialFactory::new(&[7; 64]).unwrap();
        let session_id = AuthSessionId::new("S00000000000000000001").unwrap();
        let token_family_id = TokenFamilyId::new("F00000000000000000001").unwrap();
        let nonce = [1; REFRESH_NONCE_BYTES];
        let refresh = factory.refresh_from_nonce(&session_id, &token_family_id, 7, 12_345, &nonce);
        let repeated_refresh =
            factory.refresh_from_nonce(&session_id, &token_family_id, 7, 12_345, &nonce);
        let activation = factory.device_activation_from_entropy(&[1; 5]).unwrap();
        assert!(
            refresh
                .expose_for_exchange()
                .starts_with(REFRESH_CREDENTIAL_PREFIX)
        );
        assert_eq!(
            refresh
                .expose_for_exchange()
                .strip_prefix(REFRESH_CREDENTIAL_PREFIX)
                .unwrap()
                .len(),
            REFRESH_CREDENTIAL_BODY_LEN
        );
        assert_eq!(
            factory
                .verify_refresh_raw(refresh.expose_for_exchange())
                .unwrap(),
            VerifiedRefreshCredential {
                session_id,
                token_family_id,
                generation: 7,
                expires_at_unix: 12_345,
            }
        );
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
        let factory = OpaqueCredentialFactory::new(&[7; 64]).unwrap();
        let activation = factory
            .generate_device_activation_for_locator("Z")
            .expect("activation");
        assert!(activation.expose_for_exchange().starts_with('Z'));
        assert_eq!(activation.expose_for_exchange().len(), 8);
    }

    #[test]
    fn refresh_envelope_rejects_tampering_and_legacy_tokens() {
        let factory = OpaqueCredentialFactory::new(&[7; 64]).unwrap();
        let other_factory = OpaqueCredentialFactory::new(&[8; 64]).unwrap();
        let session_id = AuthSessionId::new("S00000000000000000001").unwrap();
        let token_family_id = TokenFamilyId::new("F00000000000000000001").unwrap();
        let refresh =
            factory.refresh_from_nonce(&session_id, &token_family_id, 0, 12_345, &[1; 32]);
        let mut tampered = refresh.expose_for_exchange().to_owned();
        let last = tampered.pop().unwrap();
        tampered.push(if last == 'A' { 'B' } else { 'A' });
        assert_eq!(
            factory.verify_refresh_raw(&tampered).unwrap_err().code(),
            AuthErrorCode::InvalidCredential
        );
        assert_eq!(
            other_factory
                .verify_refresh_raw(refresh.expose_for_exchange())
                .unwrap_err()
                .code(),
            AuthErrorCode::InvalidCredential
        );
        assert!(
            factory
                .verify_refresh_raw(&format!("prf_{}", "r".repeat(43)))
                .is_err()
        );
    }
}
