use crate::{
    HookAuditEventKind, HookContributionId, HookDiagnostic, HookDiagnosticCode,
    HookDiagnosticMessage, HookDiagnosticSeverity, HookDomain, HookId, HookPolicyKey,
    HookPromptContent, HookPromptSectionTitle, HookSectionId, HookSourceId, HookSourceLabel,
    HookSubscriptionId, HookValue,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HookSourceKind {
    Workspace,
    Thread,
    Turn,
    Task,
    Agent,
    Tool,
    Document,
    File,
    Url,
    HookRun,
    Custom(String),
}

impl HookSourceKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Workspace => "workspace",
            Self::Thread => "thread",
            Self::Turn => "turn",
            Self::Task => "task",
            Self::Agent => "agent",
            Self::Tool => "tool",
            Self::Document => "document",
            Self::File => "file",
            Self::Url => "url",
            Self::HookRun => "hook_run",
            Self::Custom(kind) => kind.as_str(),
        }
    }
}

impl From<&str> for HookSourceKind {
    fn from(value: &str) -> Self {
        match value {
            "workspace" => Self::Workspace,
            "thread" => Self::Thread,
            "turn" => Self::Turn,
            "task" => Self::Task,
            "agent" => Self::Agent,
            "tool" => Self::Tool,
            "document" => Self::Document,
            "file" => Self::File,
            "url" => Self::Url,
            "hook_run" => Self::HookRun,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl From<String> for HookSourceKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "workspace" => Self::Workspace,
            "thread" => Self::Thread,
            "turn" => Self::Turn,
            "task" => Self::Task,
            "agent" => Self::Agent,
            "tool" => Self::Tool,
            "document" => Self::Document,
            "file" => Self::File,
            "url" => Self::Url,
            "hook_run" => Self::HookRun,
            _ => Self::Custom(value),
        }
    }
}

impl fmt::Display for HookSourceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for HookSourceKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HookSourceKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookSourceRef {
    pub kind: HookSourceKind,
    pub id: HookSourceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<HookSourceLabel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyContribution {
    pub domain: HookDomain,
    pub key: HookPolicyKey,
    pub value: HookValue,
    pub priority: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptContextContribution {
    pub contribution_id: HookContributionId,
    pub domain: HookDomain,
    pub priority: i32,
    pub content: HookPromptContent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<HookSourceRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptSectionContribution {
    pub section_id: HookSectionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<HookPromptSectionTitle>,
    pub domain: HookDomain,
    pub priority: i32,
    pub content: HookPromptContent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptManifestDiagnosticContribution {
    pub code: HookDiagnosticCode,
    pub message: HookDiagnosticMessage,
    pub severity: HookDiagnosticSeverity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_id: Option<HookId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<HookSubscriptionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditContribution {
    pub event_kind: HookAuditEventKind,
    pub details: HookValue,
    pub safe_for_user: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum HookContribution {
    Policy(PolicyContribution),
    PromptContext(PromptContextContribution),
    PromptSection(PromptSectionContribution),
    PromptManifestDiagnostic(PromptManifestDiagnosticContribution),
    Audit(AuditContribution),
    Noop,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_section_contribution_roundtrips() {
        let contribution = HookContribution::PromptSection(PromptSectionContribution {
            section_id: HookSectionId::new("dynamic.context").expect("valid section id"),
            title: Some(HookPromptSectionTitle::new("Dynamic Context").expect("valid title")),
            domain: HookDomain::new("test").expect("valid domain"),
            priority: 10,
            content: HookPromptContent::new("context").expect("valid content"),
            max_chars: Some(200),
            diagnostics: Vec::new(),
            truncated: false,
        });

        let value = serde_json::to_value(&contribution).expect("contribution should serialize");
        assert_eq!(value["kind"], "prompt_section");
        let decoded: HookContribution =
            serde_json::from_value(value).expect("contribution should deserialize");
        assert_eq!(decoded, contribution);
    }

    #[test]
    fn source_ref_roundtrips() {
        let source = HookSourceRef {
            kind: HookSourceKind::Thread,
            id: HookSourceId::new("thread-1").expect("valid source id"),
            label: Some(HookSourceLabel::new("Thread").expect("valid label")),
        };
        let value = serde_json::to_value(&source).expect("source should serialize");
        let decoded: HookSourceRef =
            serde_json::from_value(value).expect("source should deserialize");
        assert_eq!(decoded, source);
    }

    #[test]
    fn audit_contribution_roundtrips() {
        let contribution = HookContribution::Audit(AuditContribution {
            event_kind: HookAuditEventKind::new("hook.test").expect("valid event kind"),
            details: HookValue::Bool(true),
            safe_for_user: true,
        });
        let value = serde_json::to_value(&contribution).expect("audit should serialize");
        let decoded: HookContribution =
            serde_json::from_value(value).expect("audit should deserialize");
        assert_eq!(decoded, contribution);
    }
}
