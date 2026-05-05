use crate::{HookDiagnosticCode, HookDiagnosticMessage, HookMetadata};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const REDACTED_DIAGNOSTIC_MESSAGE: &str = "diagnostic redacted";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookDiagnosticSeverity {
    Debug,
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookDiagnostic {
    pub code: HookDiagnosticCode,
    pub message: HookDiagnosticMessage,
    pub severity: HookDiagnosticSeverity,
    pub safe_for_user: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: HookMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookDiagnosticPreview {
    pub code: HookDiagnosticCode,
    pub message: HookDiagnosticMessage,
    pub severity: HookDiagnosticSeverity,
    pub safe_for_user: bool,
    pub redacted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookDiagnosticRedactionPolicy {
    pub max_message_chars: usize,
    pub include_unsafe_messages: bool,
}

impl Default for HookDiagnosticRedactionPolicy {
    fn default() -> Self {
        Self {
            max_message_chars: 512,
            include_unsafe_messages: false,
        }
    }
}

impl HookDiagnosticRedactionPolicy {
    pub fn new(max_message_chars: usize, include_unsafe_messages: bool) -> Self {
        Self {
            max_message_chars: max_message_chars.max(3),
            include_unsafe_messages,
        }
    }

    pub fn normalized(&self) -> Self {
        Self::new(self.max_message_chars, self.include_unsafe_messages)
    }
}

impl HookDiagnostic {
    pub fn new(
        code: HookDiagnosticCode,
        message: HookDiagnosticMessage,
        severity: HookDiagnosticSeverity,
    ) -> Self {
        Self {
            code,
            message,
            severity,
            safe_for_user: false,
            metadata: BTreeMap::new(),
        }
    }

    pub fn preview(&self, policy: &HookDiagnosticRedactionPolicy) -> HookDiagnosticPreview {
        let policy = policy.normalized();
        let (message, redacted) = redact_message(
            &self.message,
            self.safe_for_user,
            policy.include_unsafe_messages,
            policy.max_message_chars,
        );
        HookDiagnosticPreview {
            code: self.code.clone(),
            message,
            severity: self.severity,
            safe_for_user: self.safe_for_user || !policy.include_unsafe_messages,
            redacted: redacted || !self.safe_for_user,
        }
    }

    pub fn redacted(&self, policy: &HookDiagnosticRedactionPolicy) -> Self {
        let preview = self.preview(policy);
        Self {
            code: preview.code,
            message: preview.message,
            severity: preview.severity,
            safe_for_user: preview.safe_for_user,
            metadata: BTreeMap::new(),
        }
    }
}

pub fn redact_message(
    message: &HookDiagnosticMessage,
    safe_for_user: bool,
    include_unsafe_messages: bool,
    max_message_chars: usize,
) -> (HookDiagnosticMessage, bool) {
    if !safe_for_user && !include_unsafe_messages {
        return (
            HookDiagnosticMessage::new(REDACTED_DIAGNOSTIC_MESSAGE)
                .expect("redacted diagnostic message is valid"),
            true,
        );
    }

    let limit = max_message_chars.max(3);
    let value = message.as_str();
    if value.chars().count() <= limit {
        return (message.clone(), false);
    }

    let truncated = value
        .chars()
        .take(limit.saturating_sub(3))
        .chain("...".chars())
        .collect::<String>();
    (
        HookDiagnosticMessage::new(truncated).expect("bounded diagnostic message is not empty"),
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_severity_serializes_stably() {
        assert_eq!(
            serde_json::to_value(HookDiagnosticSeverity::Warning).expect("severity serializes"),
            serde_json::json!("warning")
        );
    }

    #[test]
    fn diagnostic_preview_redacts_unsafe_message() {
        let diagnostic = HookDiagnostic::new(
            HookDiagnosticCode::new("hook.failed").expect("valid code"),
            HookDiagnosticMessage::new("password=super-secret-token").expect("valid message"),
            HookDiagnosticSeverity::Warning,
        );

        let preview = diagnostic.preview(&HookDiagnosticRedactionPolicy::default());

        assert_eq!(preview.message.as_str(), REDACTED_DIAGNOSTIC_MESSAGE);
        assert!(preview.safe_for_user);
        assert!(preview.redacted);
    }

    #[test]
    fn diagnostic_preview_bounds_safe_message() {
        let mut diagnostic = HookDiagnostic::new(
            HookDiagnosticCode::new("hook.failed").expect("valid code"),
            HookDiagnosticMessage::new("abcdef").expect("valid message"),
            HookDiagnosticSeverity::Warning,
        );
        diagnostic.safe_for_user = true;

        let preview = diagnostic.preview(&HookDiagnosticRedactionPolicy::new(5, false));

        assert_eq!(preview.message.as_str(), "ab...");
        assert!(preview.safe_for_user);
        assert!(preview.redacted);
    }
}
