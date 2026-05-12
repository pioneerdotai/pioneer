use crate::{
    HookAuditEventKind, HookBackgroundJobId, HookCapability, HookContributionId, HookDiagnostic,
    HookDiagnosticCode, HookDiagnosticMessage, HookDiagnosticSeverity, HookDomain, HookId,
    HookPolicyKey, HookPromptContent, HookPromptSectionTitle, HookRunIdempotencyKey, HookSectionId,
    HookSourceId, HookSourceLabel, HookSubscriptionId, HookToolBundleId, HookToolName, HookValue,
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
    pub contribution_id: HookContributionId,
    pub section_id: HookSectionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<HookPromptSectionTitle>,
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
pub struct ToolBundleContribution {
    pub contribution_id: HookContributionId,
    pub bundle_id: HookToolBundleId,
    pub domain: HookDomain,
    pub priority: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_names: Vec<HookToolName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptManifestDiagnosticContribution {
    pub code: HookDiagnosticCode,
    pub message: HookDiagnosticMessage,
    pub severity: HookDiagnosticSeverity,
    #[serde(default)]
    pub safe_for_user: bool,
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
pub struct BackgroundJobContribution {
    pub contribution_id: HookContributionId,
    pub job_id: HookBackgroundJobId,
    pub domain: HookDomain,
    pub priority: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<HookRunIdempotencyKey>,
    pub payload: HookValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum HookContribution {
    Policy(PolicyContribution),
    PromptContext(PromptContextContribution),
    PromptSection(PromptSectionContribution),
    ToolBundle(ToolBundleContribution),
    PromptManifestDiagnostic(PromptManifestDiagnosticContribution),
    Audit(AuditContribution),
    BackgroundJob(BackgroundJobContribution),
    Noop,
}

impl HookContribution {
    pub fn required_capability(&self) -> Option<HookCapability> {
        match self {
            Self::Policy(_) => Some(static_capability("contribute_policy")),
            Self::PromptContext(_) => Some(static_capability("contribute_prompt_context")),
            Self::PromptSection(_) => Some(static_capability("contribute_prompt_section")),
            Self::ToolBundle(_) => Some(static_capability("contribute_tool_bundle")),
            Self::PromptManifestDiagnostic(_) => {
                Some(static_capability("contribute_prompt_manifest_diagnostic"))
            }
            Self::Audit(_) => Some(static_capability("emit_audit")),
            Self::BackgroundJob(_) => Some(static_capability("schedule_background_job")),
            Self::Noop => None,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Policy(_) => "policy",
            Self::PromptContext(_) => "prompt_context",
            Self::PromptSection(_) => "prompt_section",
            Self::ToolBundle(_) => "tool_bundle",
            Self::PromptManifestDiagnostic(_) => "prompt_manifest_diagnostic",
            Self::Audit(_) => "audit",
            Self::BackgroundJob(_) => "background_job",
            Self::Noop => "noop",
        }
    }
}

fn static_capability(value: &'static str) -> HookCapability {
    HookCapability::new(value).expect("static hook capability is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_section_contribution_roundtrips() {
        let contribution = HookContribution::PromptSection(PromptSectionContribution {
            contribution_id: HookContributionId::new("dynamic.context")
                .expect("valid contribution id"),
            section_id: HookSectionId::new("dynamic.context").expect("valid section id"),
            title: Some(HookPromptSectionTitle::new("Dynamic Context").expect("valid title")),
            domain: HookDomain::new("test").expect("valid domain"),
            priority: 10,
            content: HookPromptContent::new("context").expect("valid content"),
            max_chars: Some(200),
            source_refs: Vec::new(),
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
    fn prompt_section_declares_required_capability() {
        let contribution = HookContribution::PromptSection(PromptSectionContribution {
            contribution_id: HookContributionId::new("dynamic.context")
                .expect("valid contribution id"),
            section_id: HookSectionId::new("dynamic.context").expect("valid section id"),
            title: None,
            domain: HookDomain::new("test").expect("valid domain"),
            priority: 10,
            content: HookPromptContent::new("context").expect("valid content"),
            max_chars: None,
            source_refs: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
        });

        assert_eq!(contribution.kind_name(), "prompt_section");
        assert_eq!(
            contribution
                .required_capability()
                .expect("prompt section requires a capability"),
            HookCapability::new("contribute_prompt_section").expect("valid capability"),
        );
    }

    #[test]
    fn noop_requires_no_capability() {
        assert_eq!(HookContribution::Noop.required_capability(), None);
        assert_eq!(HookContribution::Noop.kind_name(), "noop");
    }

    #[test]
    fn background_job_contribution_roundtrips_and_declares_capability() {
        let contribution = HookContribution::BackgroundJob(BackgroundJobContribution {
            contribution_id: HookContributionId::new("background.extract")
                .expect("valid contribution id"),
            job_id: HookBackgroundJobId::new("memory.extract").expect("valid job id"),
            domain: HookDomain::new("memory").expect("valid domain"),
            priority: 20,
            idempotency_key: Some(
                HookRunIdempotencyKey::new("turn-1:memory.extract").expect("valid key"),
            ),
            payload: HookValue::Text("extract durable facts".to_owned()),
            diagnostics: Vec::new(),
        });

        let value = serde_json::to_value(&contribution).expect("contribution should serialize");
        assert_eq!(value["kind"], "background_job");
        let decoded: HookContribution =
            serde_json::from_value(value).expect("contribution should deserialize");

        assert_eq!(decoded, contribution);
        assert_eq!(decoded.kind_name(), "background_job");
        assert_eq!(
            decoded
                .required_capability()
                .expect("background job requires a capability"),
            HookCapability::new("schedule_background_job").expect("valid capability"),
        );
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
    fn tool_bundle_contribution_roundtrips() {
        let contribution = HookContribution::ToolBundle(ToolBundleContribution {
            contribution_id: HookContributionId::new("test.tool_bundle")
                .expect("valid contribution id"),
            bundle_id: HookToolBundleId::new("test.memory_tools").expect("valid bundle id"),
            domain: HookDomain::new("memory").expect("valid domain"),
            priority: 20,
            tool_names: vec![
                HookToolName::new("memory_search").expect("valid tool name"),
                HookToolName::new("memory_remember").expect("valid tool name"),
            ],
            diagnostics: Vec::new(),
        });

        let value = serde_json::to_value(&contribution).expect("tool bundle should serialize");
        assert_eq!(value["kind"], "tool_bundle");
        let decoded: HookContribution =
            serde_json::from_value(value).expect("tool bundle should deserialize");
        assert_eq!(decoded, contribution);
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

    #[test]
    fn phase_11_prompt_manifest_diagnostic_safe_for_user_defaults_false() {
        let value = serde_json::json!({
            "code": "test.diagnostic",
            "message": "diagnostic",
            "severity": "warning"
        });

        let decoded: PromptManifestDiagnosticContribution =
            serde_json::from_value(value).expect("diagnostic contribution should deserialize");

        assert!(!decoded.safe_for_user);
    }

    #[test]
    fn phase_11_prompt_manifest_diagnostic_can_opt_into_safe_message() {
        let contribution = PromptManifestDiagnosticContribution {
            code: HookDiagnosticCode::new("test.safe_diagnostic").expect("valid code"),
            message: HookDiagnosticMessage::new("safe diagnostic").expect("valid message"),
            severity: HookDiagnosticSeverity::Warning,
            safe_for_user: true,
            hook_id: None,
            subscription_id: None,
        };

        let value = serde_json::to_value(&contribution).expect("diagnostic should serialize");
        assert_eq!(value["safe_for_user"], true);
        let decoded: PromptManifestDiagnosticContribution =
            serde_json::from_value(value).expect("diagnostic should deserialize");

        assert!(decoded.safe_for_user);
    }
}
