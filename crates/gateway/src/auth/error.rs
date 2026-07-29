#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum AuthErrorCode {
    MissingCredential,
    DuplicateCredential,
    MalformedCredential,
    UnsupportedCredential,
    InvalidCredential,
    CredentialExpired,
    GatewayIdentityMismatch,
    SessionRevoked,
    SessionExpired,
    SessionCompromised,
    DeviceActivationConsumed,
    DeviceActivationExpired,
    AuthNotReady,
    MethodNotAllowed,
    ExchangeTimeout,
}

impl AuthErrorCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::MissingCredential => "missing_credential",
            Self::DuplicateCredential => "duplicate_credential",
            Self::MalformedCredential => "malformed_credential",
            Self::UnsupportedCredential => "unsupported_credential",
            Self::InvalidCredential => "invalid_credential",
            Self::CredentialExpired => "credential_expired",
            Self::GatewayIdentityMismatch => "gateway_identity_mismatch",
            Self::SessionRevoked => "session_revoked",
            Self::SessionExpired => "session_expired",
            Self::SessionCompromised => "session_compromised",
            Self::DeviceActivationConsumed => "device_activation_consumed",
            Self::DeviceActivationExpired => "device_activation_expired",
            Self::AuthNotReady => "auth_not_ready",
            Self::MethodNotAllowed => "auth_method_not_allowed",
            Self::ExchangeTimeout => "auth_exchange_timeout",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthError {
    code: AuthErrorCode,
}

impl AuthError {
    pub(crate) const fn new(code: AuthErrorCode) -> Self {
        Self { code }
    }

    pub(crate) const fn code(&self) -> AuthErrorCode {
        self.code
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for AuthError {}
