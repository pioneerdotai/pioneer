//! Safe recovery diagnostics. Never reconstruct these facts from raw error text.
use crate::{
    ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage, ProviderTransportKind,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryProviderFailure {
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
        }
        message.push_str(ending);
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_typed_facts_cross_the_public_boundary() {
        let private =
            "https://secret.example/request?token=credential /private/provider.json body: HTTP 401";
        let failure = ProviderFailureDetails {
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
