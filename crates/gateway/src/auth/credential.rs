use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use zeroize::Zeroizing;

use pioneer_protocol::{
    MAX_OPAQUE_CREDENTIAL_BODY_LEN, MIN_OPAQUE_CREDENTIAL_BODY_LEN,
    normalize_device_activation_code,
};

use super::{AuthError, AuthErrorCode};

const MAX_PRESENTED_CREDENTIAL_BYTES: usize = 8 * 1024;

#[derive(Clone, PartialEq, Eq)]
struct RedactedCredential(Zeroizing<String>);

impl RedactedCredential {
    fn new(value: &str) -> Self {
        Self(Zeroizing::new(value.to_owned()))
    }

    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for RedactedCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[redacted]")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PresentedCredentialKind {
    AccessV2,
    Refresh,
    DeviceActivation,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PresentedCredential {
    kind: PresentedCredentialKind,
    secret: RedactedCredential,
}

impl PresentedCredential {
    pub(crate) fn classify(raw: &str) -> Result<Self, AuthError> {
        if raw.is_empty() || raw.len() > MAX_PRESENTED_CREDENTIAL_BYTES {
            return Err(AuthError::new(AuthErrorCode::MalformedCredential));
        }
        let (kind, canonical) = if let Some(body) = raw.strip_prefix("prf_") {
            validate_opaque_body(body)?;
            (PresentedCredentialKind::Refresh, raw.to_owned())
        } else if let Ok(canonical) = normalize_device_activation_code(raw) {
            (PresentedCredentialKind::DeviceActivation, canonical)
        } else {
            (classify_jwt(raw)?, raw.to_owned())
        };
        Ok(Self {
            kind,
            secret: RedactedCredential::new(canonical.as_str()),
        })
    }

    pub(crate) const fn kind(&self) -> PresentedCredentialKind {
        self.kind
    }

    pub(crate) fn expose_for_authentication(&self) -> &str {
        self.secret.expose()
    }
}

fn validate_opaque_body(body: &str) -> Result<(), AuthError> {
    if !(MIN_OPAQUE_CREDENTIAL_BODY_LEN..=MAX_OPAQUE_CREDENTIAL_BODY_LEN).contains(&body.len())
        || !body
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(AuthError::new(AuthErrorCode::MalformedCredential));
    }
    Ok(())
}

impl std::fmt::Debug for PresentedCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PresentedCredential")
            .field("kind", &self.kind)
            .field("secret", &self.secret)
            .finish()
    }
}

#[derive(Deserialize)]
struct ClassifierClaims {
    #[serde(default)]
    ver: Option<u8>,
    #[serde(default)]
    typ: Option<String>,
    #[serde(default)]
    purpose: Option<String>,
}

fn classify_jwt(raw: &str) -> Result<PresentedCredentialKind, AuthError> {
    let mut segments = raw.split('.');
    let _header = segments.next();
    let payload = segments
        .next()
        .ok_or_else(|| AuthError::new(AuthErrorCode::UnsupportedCredential))?;
    if segments.next().is_none() || segments.next().is_some() {
        return Err(AuthError::new(AuthErrorCode::MalformedCredential));
    }
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| AuthError::new(AuthErrorCode::MalformedCredential))?;
    let claims: ClassifierClaims = serde_json::from_slice(&payload)
        .map_err(|_| AuthError::new(AuthErrorCode::MalformedCredential))?;

    match (claims.ver, claims.typ.as_deref(), claims.purpose.as_deref()) {
        (Some(2), Some("access"), Some("gateway_access")) => Ok(PresentedCredentialKind::AccessV2),
        (Some(_), _, _) => Err(AuthError::new(AuthErrorCode::UnsupportedCredential)),
        _ => Err(AuthError::new(AuthErrorCode::UnsupportedCredential)),
    }
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

    use super::*;

    fn jwt(payload: serde_json::Value) -> String {
        format!(
            "{}.{}.signature",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256"}"#),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap())
        )
    }

    #[test]
    fn classifier_is_bounded_typed_and_redacted() {
        let cases = [
            (
                jwt(serde_json::json!({"ver":2,"typ":"access","purpose":"gateway_access"})),
                PresentedCredentialKind::AccessV2,
                None,
            ),
            (
                format!("prf_{}", "r".repeat(MIN_OPAQUE_CREDENTIAL_BODY_LEN)),
                PresentedCredentialKind::Refresh,
                None,
            ),
            (
                "k7m4-p9q2".to_owned(),
                PresentedCredentialKind::DeviceActivation,
                Some("K7M4P9Q2"),
            ),
        ];
        for (raw, expected, canonical) in cases {
            let credential = PresentedCredential::classify(&raw).expect("credential class");
            assert_eq!(credential.kind(), expected);
            assert_eq!(
                credential.expose_for_authentication(),
                canonical.unwrap_or(raw.as_str())
            );
            let rendered = format!("{credential:?}");
            assert!(!rendered.contains(&raw));
            assert!(rendered.contains("[redacted]"));
        }
    }

    #[test]
    fn malformed_and_unknown_credentials_fail_closed_without_echo() {
        for raw in ["", "unknown", "a.b.c.d", "a.***.c"] {
            let error = PresentedCredential::classify(raw).expect_err("must reject");
            assert!(!format!("{error:?} {error}").contains(raw) || raw.is_empty());
        }
        for raw in [
            "prf_too_short".to_owned(),
            format!("prf_{}+", "r".repeat(MIN_OPAQUE_CREDENTIAL_BODY_LEN)),
        ] {
            assert_eq!(
                PresentedCredential::classify(&raw).unwrap_err().code(),
                AuthErrorCode::MalformedCredential
            );
        }
        for old_or_invalid_activation in [
            format!(
                "device_G00000000000000000001_{}",
                "e".repeat(MAX_OPAQUE_CREDENTIAL_BODY_LEN)
            ),
            "K7M4-P9U2".to_owned(),
        ] {
            assert!(PresentedCredential::classify(&old_or_invalid_activation).is_err());
        }
    }

    #[test]
    fn old_shared_superuser_jwt_is_not_a_supported_credential() {
        let raw = jwt(serde_json::json!({
            "sub": "superuser",
            "role": "superuser"
        }));
        assert_eq!(
            PresentedCredential::classify(&raw).unwrap_err().code(),
            AuthErrorCode::UnsupportedCredential
        );
    }
}
