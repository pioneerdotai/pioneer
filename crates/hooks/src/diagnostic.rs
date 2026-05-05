use crate::{HookDiagnosticCode, HookDiagnosticMessage, HookMetadata};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
}
