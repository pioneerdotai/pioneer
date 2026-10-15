//! Safe recovery diagnostics. Never reconstruct these facts from raw error text.
use crate::{
    ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage, ProviderTransportKind,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Allowlisted OpenRouter Chat Completions metadata.error_type values.
/// Diagnostic only: these values must not drive recovery policy.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorReason {
    ContextLengthExceeded,
    MaxTokensExceeded,
    TokenLimitExceeded,
    StringTooLong,
    Authentication,
    PermissionDenied,
    PaymentRequired,
    RateLimitExceeded,
    ProviderOverloaded,
    ProviderUnavailable,
    InvalidRequest,
    InvalidPrompt,
    NotFound,
    PreconditionFailed,
    PayloadTooLarge,
    Unprocessable,
    ContentPolicyViolation,
    Refusal,
    InvalidImage,
    ImageTooLarge,
    ImageTooSmall,
    UnsupportedImageFormat,
    ImageNotFound,
    ImageDownloadFailed,
    Server,
    Timeout,
    Unmapped,
}

impl ProviderErrorReason {
    pub fn from_openrouter_code(code: &str) -> Option<Self> {
        match code {
            "context_length_exceeded" => Some(Self::ContextLengthExceeded),
            "max_tokens_exceeded" => Some(Self::MaxTokensExceeded),
            "token_limit_exceeded" => Some(Self::TokenLimitExceeded),
            "string_too_long" => Some(Self::StringTooLong),
            "authentication" => Some(Self::Authentication),
            "permission_denied" => Some(Self::PermissionDenied),
            "payment_required" => Some(Self::PaymentRequired),
            "rate_limit_exceeded" => Some(Self::RateLimitExceeded),
            "provider_overloaded" => Some(Self::ProviderOverloaded),
            "provider_unavailable" => Some(Self::ProviderUnavailable),
            "invalid_request" => Some(Self::InvalidRequest),
            "invalid_prompt" => Some(Self::InvalidPrompt),
            "not_found" => Some(Self::NotFound),
            "precondition_failed" => Some(Self::PreconditionFailed),
            "payload_too_large" => Some(Self::PayloadTooLarge),
            "unprocessable" => Some(Self::Unprocessable),
            "content_policy_violation" => Some(Self::ContentPolicyViolation),
            "refusal" => Some(Self::Refusal),
            "invalid_image" => Some(Self::InvalidImage),
            "image_too_large" => Some(Self::ImageTooLarge),
            "image_too_small" => Some(Self::ImageTooSmall),
            "unsupported_image_format" => Some(Self::UnsupportedImageFormat),
            "image_not_found" => Some(Self::ImageNotFound),
            "image_download_failed" => Some(Self::ImageDownloadFailed),
            "server" => Some(Self::Server),
            "timeout" => Some(Self::Timeout),
            "unmapped" => Some(Self::Unmapped),
            _ => None,
        }
    }

    pub fn public_description(self) -> &'static str {
        match self {
            Self::ContextLengthExceeded => "The context limit was exceeded.",
            Self::MaxTokensExceeded => "The output token limit was reached.",
            Self::TokenLimitExceeded => "A provider token budget was exceeded.",
            Self::StringTooLong => "A request field exceeded the length limit.",
            Self::Authentication => "Provider authentication failed.",
            Self::PermissionDenied => "The provider denied permission.",
            Self::PaymentRequired => "Provider credits were insufficient.",
            Self::RateLimitExceeded => "The provider rate limit was exceeded.",
            Self::ProviderOverloaded => "The provider was overloaded.",
            Self::ProviderUnavailable => "The provider returned an invalid or empty response.",
            Self::InvalidRequest => "The provider request was invalid.",
            Self::InvalidPrompt => "The provider prompt was invalid.",
            Self::NotFound => "A provider resource was unavailable.",
            Self::PreconditionFailed => "A provider request precondition failed.",
            Self::PayloadTooLarge => "The provider request was too large.",
            Self::Unprocessable => "The provider could not process the request.",
            Self::ContentPolicyViolation => "A provider content filter rejected the request.",
            Self::Refusal => "The provider reported a model refusal.",
            Self::InvalidImage => "An input image was invalid.",
            Self::ImageTooLarge => "An input image was too large.",
            Self::ImageTooSmall => "An input image was too small.",
            Self::UnsupportedImageFormat => "An input image format was unsupported.",
            Self::ImageNotFound => "An input image was unavailable.",
            Self::ImageDownloadFailed => "The provider could not download an input image.",
            Self::Server => "The provider reported an internal error.",
            Self::Timeout => "The provider timed out.",
            Self::Unmapped => "The provider reported an unclassified error.",
        }
    }
}

/// Bounded opaque correlation ID; never interpolated into public prose.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(try_from = "String", into = "String")]
pub struct ProviderRequestId(String);

impl JsonSchema for ProviderRequestId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ProviderRequestId".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "maxLength": 128,
            "pattern": "^(gen-|req-|cmpl-|chatcmpl-)[A-Za-z0-9_-]+$"
        })
    }
}

impl TryFrom<String> for ProviderRequestId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let prefix = ["gen-", "req-", "cmpl-", "chatcmpl-"]
            .into_iter()
            .find(|prefix| value.starts_with(prefix));
        if value.len() > 128
            || !prefix.is_some_and(|prefix| value.len() > prefix.len())
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("invalid provider correlation ID");
        }
        Ok(Self(value))
    }
}

impl From<ProviderRequestId> for String {
    fn from(value: ProviderRequestId) -> Self {
        value.0
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryProviderFailure {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_reason: Option<crate::ProviderErrorReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<crate::ProviderRequestId>,

    pub class: ProviderFailureClass,
    pub stage: ProviderFailureStage,
    pub transport: ProviderTransportKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl From<&ProviderFailureDetails> for RecoveryProviderFailure {
    fn from(failure: &ProviderFailureDetails) -> Self {
        Self {
            error_reason: failure.error_reason,
            request_id: failure.request_id.clone(),
            class: failure.class,
            stage: failure.stage,
            transport: failure.transport,
            http_status: failure
                .http_status
                .filter(|status| (100..=599).contains(status)),
            retry_after_ms: failure.retry_after_ms,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStopReason {
    AttemptsExhausted,
    WallClockExhausted,
    NoProgress,
    PolicyRejected,
    TerminalFailure,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryDiagnostic {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<RecoveryProviderFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<RecoveryStopReason>,
}

impl RecoveryDiagnostic {
    pub fn provider(failure: &ProviderFailureDetails) -> Self {
        Self {
            last_failure: Some(failure.into()),
            stop_reason: None,
        }
    }

    /// All prose is controlled; only a validated numeric HTTP status is interpolated.
    pub fn public_message(&self) -> String {
        self.message_with_ending(match self.stop_reason {
            Some(RecoveryStopReason::AttemptsExhausted) => {
                "Recovery stopped: attempt limit exhausted."
            }
            Some(RecoveryStopReason::WallClockExhausted) => {
                "Recovery stopped: time limit exhausted."
            }
            Some(RecoveryStopReason::NoProgress) => "Recovery stopped: no progress limit reached.",
            Some(RecoveryStopReason::PolicyRejected) => {
                "Recovery stopped: the failure is final under the recovery policy."
            }
            Some(RecoveryStopReason::TerminalFailure) | None => "Recovery failed.",
        })
    }

    pub fn public_retry_message(&self) -> String {
        self.message_with_ending("Recovery retry scheduled.")
    }

    fn message_with_ending(&self, ending: &str) -> String {
        let mut message = String::new();
        if let Some(failure) = &self.last_failure {
            use ProviderFailureClass::*;
            let description = match failure.class {
                NetworkTransient => "failed due to a network error",
                RateLimit => "was rate limited",
                Provider5xx => "failed at the provider",
                AuthExpired => "failed because authentication expired",
                AuthOrPermission | PermissionDenied => {
                    "was denied due to authentication or permissions"
                }
                ModelNotFound => "failed because the model was unavailable",
                PromptTooLong | ContextTooLarge => "exceeded the context limit",
                MaxOutputTokens => "exceeded the output token limit",
                StreamStall => "stalled while streaming",
                StreamTruncated => "ended with an incomplete stream",
                EmptyResponse => "returned an empty response",
                ProviderRejected => "was rejected",
                UnsupportedParameter
                | UnsupportedCapability
                | UnsupportedImageInput
                | UnsupportedToolCalling
                | UnsupportedStreaming => "used an unsupported capability",
                MalformedProviderRequest | InvalidRequest => "was invalid",
                Unknown => "failed",
            };
            message = format!("Last provider request {description}");
            if let Some(status) = failure
                .http_status
                .filter(|status| (100..=599).contains(status))
            {
                message.push_str(&format!(": HTTP {status}"));
            }
            message.push_str(". ");
            if let Some(reason) = failure.error_reason {
                message.push_str(reason.public_description());
                message.push(' ');
            }
        }
        message.push_str(ending);
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_codes_and_ids_are_bounded_and_legacy_records_remain_readable() {
        for id in [
            "",
            "gen-",
            "gen-secret?key=value",
            "gen-../credentials",
            "gen-\ncredential",
            &format!("gen-{}", "a".repeat(129)),
        ] {
            assert!(ProviderRequestId::try_from(id.to_owned()).is_err());
            assert!(serde_json::from_value::<ProviderRequestId>(serde_json::json!(id)).is_err());
        }
        let legacy: RecoveryProviderFailure = serde_json::from_str(r#"{"class":"provider_5xx","stage":"mid_stream","transport":"stream","httpStatus":502}"#).unwrap();
        assert!(legacy.error_reason.is_none());
        assert!(legacy.request_id.is_none());
        assert!(serde_json::from_str::<RecoveryProviderFailure>(r#"{"class":"provider_5xx","stage":"mid_stream","transport":"stream","errorReason":"secret-value"}"#).is_err());
        assert!(ProviderErrorReason::from_openrouter_code("secret-value").is_none());
        let failure: ProviderFailureDetails = serde_json::from_str(r#"{"provider":"openrouter","model":"fixture","transport":"stream","class":"provider_5xx","stage":"mid_stream","is_recoverable_hint":true}"#).unwrap();
        assert!(failure.error_reason.is_none());
        assert!(failure.request_id.is_none());
        let legacy_public: crate::PublicTaskFailure = serde_json::from_str(r#"{"class":"policy","error":{"version":1,"code":"policy_denied","stage":"execution","message":"This operation is not permitted.","retryable":false,"correlation_id":"fixture"}}"#).unwrap();
        assert!(legacy_public.recovery_diagnostic.is_none());
    }

    #[test]
    fn only_typed_facts_cross_the_public_boundary() {
        let private =
            "https://secret.example/request?token=credential /private/provider.json body: HTTP 401";
        let failure = ProviderFailureDetails {
            error_reason: None,
            request_id: None,
            provider: private.to_owned(),
            model: private.to_owned(),
            transport: ProviderTransportKind::Stream,
            class: ProviderFailureClass::AuthOrPermission,
            stage: ProviderFailureStage::Connect,
            http_status: Some(403),
            provider_code: Some(private.to_owned()),
            retry_after_ms: Some(2000),
            is_recoverable_hint: true,
            message: Some(private.to_owned()),
        };
        let mut diagnostic = RecoveryDiagnostic::provider(&failure);
        diagnostic.stop_reason = Some(RecoveryStopReason::AttemptsExhausted);
        let message = diagnostic.public_message();
        assert!(message.contains("HTTP 403"));
        assert!(message.contains("attempt limit exhausted"));
        assert!(!message.contains("401"));
        let stored = serde_json::to_string(&diagnostic).unwrap();
        assert!(!stored.contains(private));
        assert_eq!(
            serde_json::from_str::<RecoveryDiagnostic>(&stored).unwrap(),
            diagnostic
        );
        for reason in [
            RecoveryStopReason::WallClockExhausted,
            RecoveryStopReason::NoProgress,
            RecoveryStopReason::PolicyRejected,
            RecoveryStopReason::TerminalFailure,
        ] {
            diagnostic.stop_reason = Some(reason);
            assert_ne!(diagnostic.public_message(), message);
            assert!(diagnostic.public_message().contains("HTTP 403"));
        }
    }

    #[test]
    fn legacy_partial_and_invalid_diagnostics_do_not_invent_http_status() {
        let legacy: crate::TaskError = serde_json::from_value(serde_json::json!({
            "code": "child_turn_failed", "message": "HTTP 403 secret body", "class": "unknown"
        }))
        .unwrap();
        assert!(legacy.recovery_diagnostic.is_none());
        assert!(legacy.recovery_public_message().is_none());
        let empty: RecoveryDiagnostic = serde_json::from_str("{}").unwrap();
        assert_eq!(empty.public_message(), "Recovery failed.");
        let mut partial = RecoveryDiagnostic {
            last_failure: None,
            stop_reason: Some(RecoveryStopReason::AttemptsExhausted),
        };
        assert!(!partial.public_message().contains("HTTP"));
        partial.last_failure = Some(RecoveryProviderFailure {
            error_reason: None,
            request_id: None,
            class: ProviderFailureClass::Unknown,
            stage: ProviderFailureStage::Finalize,
            transport: ProviderTransportKind::Stream,
            http_status: Some(65535),
            retry_after_ms: None,
        });
        assert!(!partial.public_message().contains("HTTP"));
        assert!(
            serde_json::from_str::<RecoveryDiagnostic>(r#"{"stopReason":"invented"}"#).is_err()
        );
    }
}
