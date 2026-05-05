use crate::{HookDiagnosticCode, HookDiagnosticMessage, HookId, HookPhase, HookSubscriptionId};
use serde::{Deserialize, Serialize};
use std::fmt;

pub type HookResult<T> = Result<T, HookError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookError {
    pub code: HookDiagnosticCode,
    pub message: HookDiagnosticMessage,
    pub retryable: bool,
    pub safe_for_user: bool,
}

impl HookError {
    pub fn new(code: HookDiagnosticCode, message: HookDiagnosticMessage) -> Self {
        Self {
            code,
            message,
            retryable: false,
            safe_for_user: false,
        }
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_safe_for_user(mut self, safe_for_user: bool) -> Self {
        self.safe_for_user = safe_for_user;
        self
    }
}

impl fmt::Display for HookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for HookError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookRegistryError {
    DuplicateHandlerId(HookId),
    DuplicateSubscriptionId(HookSubscriptionId),
    MissingHandler(HookId),
    UnsupportedPhase { hook_id: HookId, phase: HookPhase },
    EmptySupportedPhases(HookId),
    LockPoisoned(&'static str),
}

impl fmt::Display for HookRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateHandlerId(hook_id) => {
                write!(formatter, "duplicate hook handler id `{}`", hook_id)
            }
            Self::DuplicateSubscriptionId(subscription_id) => {
                write!(
                    formatter,
                    "duplicate hook subscription id `{}`",
                    subscription_id
                )
            }
            Self::MissingHandler(hook_id) => {
                write!(formatter, "missing hook handler `{}`", hook_id)
            }
            Self::UnsupportedPhase { hook_id, phase } => {
                write!(
                    formatter,
                    "hook handler `{}` does not support phase `{}`",
                    hook_id, phase
                )
            }
            Self::EmptySupportedPhases(hook_id) => {
                write!(
                    formatter,
                    "hook handler `{}` must support at least one phase",
                    hook_id
                )
            }
            Self::LockPoisoned(lock_name) => {
                write!(formatter, "hook registry lock `{}` is poisoned", lock_name)
            }
        }
    }
}

impl std::error::Error for HookRegistryError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_error_display_is_stable() {
        let error = HookError::new(
            HookDiagnosticCode::new("hook.failed").expect("valid code"),
            HookDiagnosticMessage::new("hook failed").expect("valid message"),
        )
        .with_retryable(true)
        .with_safe_for_user(true);

        assert_eq!(error.to_string(), "hook.failed: hook failed");
        assert!(error.retryable);
        assert!(error.safe_for_user);
    }
}
