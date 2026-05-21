#![allow(dead_code)]

use pioneer_memory::hooks::{
    ActiveRecallPlan, ActiveRecallPlanJson, DeterministicRecallContextSummary,
    MemoryActiveRecallDecisionContext, MemoryActiveRecallDecisionRequest,
    normalize_active_recall_plan, parse_active_memory_decision_json,
};
use pioneer_protocol::ThreadMode;
use pioneer_tools::BuiltinToolDomain;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;

const PREFLIGHT_DIAGNOSTIC_CODE_MAX_CHARS: usize = 160;
const PREFLIGHT_DIAGNOSTIC_MAX_COUNT: usize = 16;
const PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS: usize = 512;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightInput {
    pub turn: TurnPreflightTurnInput,
    pub tools: TurnPreflightToolsInput,
    pub memory: TurnPreflightMemoryInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightTurnInput {
    pub has_workspace_id: bool,
    pub has_thread_id: bool,
    pub has_turn_id: bool,
    pub thread_mode: ThreadMode,
    pub provider_tool_calling: bool,
    pub input_text_preview: String,
    pub input_text_char_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightToolsInput {
    pub core_tools: Vec<String>,
    pub candidate_tools: Vec<TurnPreflightCandidateTool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightCandidateTool {
    pub name: String,
    pub domain: BuiltinToolDomain,
    pub summary: String,
    pub mutation: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightMemoryInput {
    pub deterministic_summary: DeterministicRecallContextSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_recall: Option<TurnPreflightMemoryActiveRecallInput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightMemoryActiveRecallInput {
    pub decision_context: MemoryActiveRecallDecisionContext,
    pub decision_request: MemoryActiveRecallDecisionRequest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProviderTurnPreflightPlan {
    pub tools: ProviderTurnPreflightToolsPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<ProviderTurnPreflightMemoryPlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<TurnPreflightDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProviderTurnPreflightToolsPlan {
    pub visible_tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProviderTurnPreflightMemoryPlan {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_recall: Option<ActiveRecallPlanJson>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightPlan {
    pub source: TurnPreflightPlanSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<TurnPreflightFallbackReason>,
    pub tools: TurnPreflightToolsPlan,
    pub memory: TurnPreflightMemoryPlan,
    pub diagnostics: TurnPreflightDiagnostics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_call: Option<TurnPreflightProviderCallMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightToolsPlan {
    pub visible_tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightMemoryPlan {
    pub active_recall: TurnPreflightMemoryActiveRecallPlan,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightMemoryActiveRecallPlan {
    pub source: TurnPreflightPlanSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<TurnPreflightFallbackReason>,
    pub decision: ActiveRecallPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnPreflightPlanSource {
    Provider,
    HostLocal,
    Fallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnPreflightFallbackReason {
    Timeout,
    ProviderError,
    InvalidJson,
    ValidationError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightDiagnostics {
    #[serde(default)]
    pub preflight_failed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<TurnPreflightDiagnostic>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub module_diagnostics: BTreeMap<String, Vec<TurnPreflightDiagnostic>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightProviderCallMetadata {
    pub provider: String,
    pub model: String,
    pub attempt: u32,
    pub input_chars: usize,
    pub output_chars: usize,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TurnPreflightDiagnostic {
    pub code: TurnPreflightDiagnosticCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<TurnPreflightDiagnosticMessage>,
}

pub(crate) fn normalize_provider_turn_preflight_plan(
    mut plan: ProviderTurnPreflightPlan,
) -> ProviderTurnPreflightPlan {
    plan.tools.visible_tools = normalize_visible_tool_names(plan.tools.visible_tools);
    plan.diagnostics = normalize_preflight_diagnostics(plan.diagnostics);
    plan
}

pub(crate) fn parse_provider_turn_preflight_plan_json(
    raw: &str,
) -> Result<ProviderTurnPreflightPlan, serde_json::Error> {
    let plan = serde_json::from_str::<ProviderTurnPreflightPlan>(raw.trim())?;
    validate_provider_turn_preflight_plan(&plan)?;
    Ok(normalize_provider_turn_preflight_plan(plan))
}

pub(crate) fn validate_provider_turn_preflight_plan(
    plan: &ProviderTurnPreflightPlan,
) -> Result<(), serde_json::Error> {
    if let Some(active_recall) = plan
        .memory
        .as_ref()
        .and_then(|memory| memory.active_recall.as_ref())
    {
        parse_provider_memory_active_recall_plan(active_recall)?;
    }
    Ok(())
}

pub(crate) fn normalize_visible_tool_names(tool_names: Vec<String>) -> Vec<String> {
    let mut normalized = tool_names
        .into_iter()
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

pub(crate) fn normalize_preflight_diagnostics(
    diagnostics: Vec<TurnPreflightDiagnostic>,
) -> Vec<TurnPreflightDiagnostic> {
    diagnostics
        .into_iter()
        .take(PREFLIGHT_DIAGNOSTIC_MAX_COUNT)
        .collect()
}

pub(crate) fn normalize_module_diagnostics(
    module_diagnostics: BTreeMap<String, Vec<TurnPreflightDiagnostic>>,
) -> BTreeMap<String, Vec<TurnPreflightDiagnostic>> {
    module_diagnostics
        .into_iter()
        .map(|(module, diagnostics)| (module, normalize_preflight_diagnostics(diagnostics)))
        .collect()
}

pub(crate) fn parse_provider_memory_active_recall_plan(
    plan: &ActiveRecallPlanJson,
) -> Result<ActiveRecallPlan, serde_json::Error> {
    let raw = serde_json::to_string(plan)?;
    parse_active_memory_decision_json(raw.as_str())
}

pub(crate) fn wrap_memory_active_recall_plan(
    source: TurnPreflightPlanSource,
    fallback_reason: Option<TurnPreflightFallbackReason>,
    decision: ActiveRecallPlan,
) -> TurnPreflightMemoryActiveRecallPlan {
    TurnPreflightMemoryActiveRecallPlan {
        source,
        fallback_reason,
        decision: normalize_active_recall_plan(decision),
    }
}

pub(crate) fn fallback_turn_preflight_plan(
    fallback_reason: TurnPreflightFallbackReason,
    active_recall: TurnPreflightMemoryActiveRecallPlan,
    diagnostics: Vec<TurnPreflightDiagnostic>,
    module_diagnostics: BTreeMap<String, Vec<TurnPreflightDiagnostic>>,
    provider_call: Option<TurnPreflightProviderCallMetadata>,
) -> TurnPreflightPlan {
    TurnPreflightPlan {
        source: TurnPreflightPlanSource::Fallback,
        fallback_reason: Some(fallback_reason),
        tools: TurnPreflightToolsPlan {
            visible_tools: Vec::new(),
        },
        memory: TurnPreflightMemoryPlan { active_recall },
        diagnostics: TurnPreflightDiagnostics {
            preflight_failed: true,
            diagnostics: normalize_preflight_diagnostics(diagnostics),
            module_diagnostics: normalize_module_diagnostics(module_diagnostics),
        },
        provider_call,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnPreflightDiagnosticCode(String);

impl TurnPreflightDiagnosticCode {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, TurnPreflightTextError> {
        let value = value.into();
        validate_structured_code(
            "TurnPreflightDiagnosticCode",
            value.as_str(),
            PREFLIGHT_DIAGNOSTIC_CODE_MAX_CHARS,
        )?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Serialize for TurnPreflightDiagnosticCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TurnPreflightDiagnosticCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TurnPreflightDiagnosticCodeVisitor {
            type_name: "TurnPreflightDiagnosticCode",
            max_chars: PREFLIGHT_DIAGNOSTIC_CODE_MAX_CHARS,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnPreflightDiagnosticMessage(String);

impl TurnPreflightDiagnosticMessage {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, TurnPreflightTextError> {
        let value = value.into();
        validate_bounded_text(
            "TurnPreflightDiagnosticMessage",
            value.as_str(),
            PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS,
        )?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Serialize for TurnPreflightDiagnosticMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TurnPreflightDiagnosticMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TurnPreflightDiagnosticMessageVisitor {
            type_name: "TurnPreflightDiagnosticMessage",
            max_chars: PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnPreflightTextError {
    type_name: &'static str,
    reason: &'static str,
}

impl TurnPreflightTextError {
    fn new(type_name: &'static str, reason: &'static str) -> Self {
        Self { type_name, reason }
    }
}

impl fmt::Display for TurnPreflightTextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.type_name, self.reason)
    }
}

impl std::error::Error for TurnPreflightTextError {}

struct TurnPreflightDiagnosticCodeVisitor {
    type_name: &'static str,
    max_chars: usize,
}

impl<'de> Visitor<'de> for TurnPreflightDiagnosticCodeVisitor {
    type Value = TurnPreflightDiagnosticCode;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a non-empty structured {} string up to {} chars",
            self.type_name, self.max_chars
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        validate_structured_code(self.type_name, value, self.max_chars).map_err(E::custom)?;
        Ok(TurnPreflightDiagnosticCode(value.to_owned()))
    }
}

struct TurnPreflightDiagnosticMessageVisitor {
    type_name: &'static str,
    max_chars: usize,
}

impl<'de> Visitor<'de> for TurnPreflightDiagnosticMessageVisitor {
    type Value = TurnPreflightDiagnosticMessage;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a non-empty {} string up to {} chars",
            self.type_name, self.max_chars
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        validate_bounded_text(self.type_name, value, self.max_chars).map_err(E::custom)?;
        Ok(TurnPreflightDiagnosticMessage(value.to_owned()))
    }
}

fn validate_structured_code(
    type_name: &'static str,
    value: &str,
    max_chars: usize,
) -> Result<(), TurnPreflightTextError> {
    validate_bounded_text(type_name, value, max_chars)?;
    if value.chars().any(char::is_whitespace) {
        return Err(TurnPreflightTextError::new(
            type_name,
            "cannot contain whitespace",
        ));
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(TurnPreflightTextError::new(
            type_name,
            "must contain only lowercase ascii letters, digits, dots, underscores or hyphens",
        ));
    }
    Ok(())
}

fn validate_bounded_text(
    type_name: &'static str,
    value: &str,
    max_chars: usize,
) -> Result<(), TurnPreflightTextError> {
    if value.trim().is_empty() {
        return Err(TurnPreflightTextError::new(type_name, "cannot be empty"));
    }
    if value.chars().count() > max_chars {
        return Err(TurnPreflightTextError::new(type_name, "is too long"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_memory::hooks::{
        ActiveMemoryDecisionReasonCode, ActiveMemoryDecisionReasonCodeJson,
        ActiveMemoryDecisionStatus, ActiveRecallMode, ActiveRecallPlanJsonStatus,
        ActiveRecallTarget, MemoryActiveRecallMode, MemoryActiveRecallPlannerFallbackPolicy,
        MemoryEpisodicRecallCapabilities, parse_active_memory_decision_json,
    };
    use pioneer_protocol::{
        MemoryAttribute, MemoryCategory, MemoryFactClass, MemoryScopeKind, MemorySubject,
    };
    use serde_json::{Value as JsonValue, json};
    use std::collections::BTreeSet;

    fn diagnostic_code(value: impl Into<String>) -> TurnPreflightDiagnosticCode {
        TurnPreflightDiagnosticCode::new(value).expect("test diagnostic code must be valid")
    }

    fn diagnostic_message(value: &str) -> TurnPreflightDiagnosticMessage {
        TurnPreflightDiagnosticMessage::new(value).expect("test diagnostic message must be valid")
    }

    fn sample_input() -> TurnPreflightInput {
        TurnPreflightInput {
            turn: TurnPreflightTurnInput {
                has_workspace_id: true,
                has_thread_id: true,
                has_turn_id: true,
                thread_mode: ThreadMode::Agent,
                provider_tool_calling: true,
                input_text_preview: "как меня зовут?".to_owned(),
                input_text_char_count: 15,
            },
            tools: TurnPreflightToolsInput {
                core_tools: vec![
                    "exec_command".to_owned(),
                    "write_stdin".to_owned(),
                    "read_file".to_owned(),
                    "list_dir".to_owned(),
                    "grep_files".to_owned(),
                    "apply_patch".to_owned(),
                    "web_search".to_owned(),
                    "web_fetch".to_owned(),
                    "download_url".to_owned(),
                    "read_skill".to_owned(),
                    "request_tools".to_owned(),
                ],
                candidate_tools: vec![
                    TurnPreflightCandidateTool {
                        name: "memory_search".to_owned(),
                        domain: BuiltinToolDomain::Memory,
                        summary: "Search durable memory for relevant facts.".to_owned(),
                        mutation: false,
                    },
                    TurnPreflightCandidateTool {
                        name: "memory_get".to_owned(),
                        domain: BuiltinToolDomain::Memory,
                        summary: "Read one durable memory record by id.".to_owned(),
                        mutation: false,
                    },
                ],
            },
            memory: TurnPreflightMemoryInput {
                deterministic_summary: DeterministicRecallContextSummary {
                    memory_ids: BTreeSet::new(),
                    rendered_line_fingerprints: BTreeSet::new(),
                    context_count: 0,
                    context_chars: 0,
                    sufficient: false,
                },
                active_recall: Some(TurnPreflightMemoryActiveRecallInput {
                    decision_context: MemoryActiveRecallDecisionContext {
                        workspace_id: "ws_1".to_owned(),
                        thread_id: "thr_1".to_owned(),
                        turn_id: "turn_1".to_owned(),
                        mode: ThreadMode::Agent,
                        input_text_preview: "как меня зовут?".to_owned(),
                        model: Some("thread-model".to_owned()),
                        model_provider: Some("thread-provider".to_owned()),
                    },
                    decision_request: MemoryActiveRecallDecisionRequest {
                        deterministic_context_count: 0,
                        deterministic_context_chars: 0,
                        deterministic_memory_ids: Vec::new(),
                        deterministic_sufficient: false,
                        deterministic_recall_empty: true,
                        has_workspace_context: true,
                        has_task_context: false,
                        input_length_bucket: "very_short".to_owned(),
                        config_mode: MemoryActiveRecallMode::Hybrid,
                        read_allowed: true,
                        active_memory_allowed: true,
                        explicit_no_memory: false,
                        input_text_char_count: 15,
                        available_modes: vec![
                            "profile".to_owned(),
                            "project".to_owned(),
                            "exact_canonical".to_owned(),
                        ],
                        available_scoped_contexts: vec!["thread".to_owned()],
                        episodic_capabilities: MemoryEpisodicRecallCapabilities {
                            current_thread_search: true,
                            related_thread_search: false,
                            current_task_context: false,
                            completed_task_summary: false,
                        },
                        max_queries: 3,
                        top_k_per_query: 5,
                        max_prompt_chars: 1_500,
                        max_input_chars: 4_000,
                        max_output_chars: 2_000,
                        fallback_policy: MemoryActiveRecallPlannerFallbackPolicy::Deterministic,
                    },
                }),
            },
        }
    }

    fn sample_provider_plan_json() -> JsonValue {
        json!({
            "tools": {
                "visibleTools": ["memory_search", "memory_get"]
            },
            "memory": {
                "activeRecall": {
                    "status": "run",
                    "reasonCode": "memory_likely",
                    "confidence": 0.92,
                    "modes": ["profile", "exact_canonical"],
                    "targets": [
                        {
                            "scopeKind": "user",
                            "factClass": "user_identity",
                            "category": "identity",
                            "subject": "current_user",
                            "attribute": "name",
                            "canonicalKey": "identity.current_user.name"
                        }
                    ],
                    "diagnostics": [
                        "memory.active_recall.identity_lookup"
                    ]
                }
            },
            "diagnostics": [
                {
                    "code": "preflight.tools.memory_selected",
                    "message": "Identity question needs memory read tools."
                }
            ]
        })
    }

    #[test]
    fn preflight_input_serializes_stable_json_without_tool_schemas() {
        let value = serde_json::to_value(sample_input()).expect("input serializes");

        assert_eq!(
            value["turn"]["inputTextPreview"],
            JsonValue::String("как меня зовут?".to_owned())
        );
        assert_eq!(value["turn"]["hasWorkspaceId"], json!(true));
        assert_eq!(value["turn"]["hasThreadId"], json!(true));
        assert_eq!(value["turn"]["hasTurnId"], json!(true));
        assert_eq!(value["turn"]["inputTextCharCount"], json!(15));
        assert_eq!(
            value["tools"]["coreTools"],
            json!([
                "exec_command",
                "write_stdin",
                "read_file",
                "list_dir",
                "grep_files",
                "apply_patch",
                "web_search",
                "web_fetch",
                "download_url",
                "read_skill",
                "request_tools"
            ])
        );
        assert_eq!(
            value["tools"]["candidateTools"],
            json!([
                {
                    "name": "memory_search",
                    "domain": "memory",
                    "summary": "Search durable memory for relevant facts.",
                    "mutation": false
                },
                {
                    "name": "memory_get",
                    "domain": "memory",
                    "summary": "Read one durable memory record by id.",
                    "mutation": false
                }
            ])
        );
        assert!(value.get("coreTools").is_none());
        assert!(value.get("candidateTools").is_none());
        assert_eq!(
            value["memory"]["deterministicSummary"],
            json!({
                "contextCount": 0,
                "contextChars": 0,
                "sufficient": false
            })
        );
        let mut deterministic_with_internal_fields = sample_input();
        deterministic_with_internal_fields
            .memory
            .deterministic_summary
            .rendered_line_fingerprints
            .insert("workspace project policy: private rendered line".to_owned());
        let deterministic_value =
            serde_json::to_value(deterministic_with_internal_fields).expect("input serializes");
        assert!(
            deterministic_value["memory"]["deterministicSummary"]
                .get("renderedLineFingerprints")
                .is_none()
        );
        let serialized = serde_json::to_string(&value).expect("value serializes");
        assert!(!serialized.contains("\"parameters\""));
        assert!(!serialized.contains("\"properties\""));
        assert!(!serialized.contains("\"jsonSchema\""));
        assert!(!serialized.contains("\"policy\""));
        assert!(!serialized.contains("recallAllowed"));
        assert!(!serialized.contains("readToolsAllowed"));
        assert!(!serialized.contains("rememberToolAllowed"));
        assert!(!serialized.contains("forgetToolAllowed"));
        assert!(!serialized.contains("activeRecallAllowed"));
        assert!(!serialized.contains("\"source\""));
        assert!(!serialized.contains("\"alwaysVisible\""));
        assert!(!serialized.contains("\"currentlyVisible\""));
    }

    #[test]
    fn preflight_input_reuses_active_recall_decision_request_contract() {
        let value = serde_json::to_value(sample_input()).expect("input serializes");

        assert_eq!(
            value["memory"]["activeRecall"]["decisionContext"]["inputTextPreview"],
            JsonValue::String("как меня зовут?".to_owned())
        );
        assert_eq!(
            value["memory"]["activeRecall"]["decisionRequest"]["configMode"],
            JsonValue::String("hybrid".to_owned())
        );
        assert_eq!(
            value["memory"]["activeRecall"]["decisionRequest"]["fallbackPolicy"],
            JsonValue::String("deterministic".to_owned())
        );

        let serialized = serde_json::to_string(&value).expect("value serializes");
        assert!(!serialized.contains("plannerEnabled"));
        assert!(!serialized.contains("plannerNeeded"));
    }

    #[test]
    fn preflight_input_candidate_tools_reject_core_dynamic_and_source_fields() {
        for domain in ["core", "dynamic"] {
            let mut value = serde_json::to_value(sample_input()).expect("input serializes");
            value["tools"]["candidateTools"][0]["domain"] = json!(domain);

            serde_json::from_value::<TurnPreflightInput>(value)
                .expect_err("candidate tool domains must be limited to lazy builtin domains");
        }

        let mut with_source = serde_json::to_value(sample_input()).expect("input serializes");
        with_source["tools"]["candidateTools"][0]["source"] = json!("builtin");

        serde_json::from_value::<TurnPreflightInput>(with_source)
            .expect_err("candidate tools must not carry provenance/source fields");
    }

    #[test]
    fn provider_preflight_plan_parses_concrete_visible_tools_and_memory_active_recall() {
        let parsed: ProviderTurnPreflightPlan =
            serde_json::from_value(sample_provider_plan_json()).expect("provider plan parses");

        assert_eq!(
            parsed.tools.visible_tools,
            vec!["memory_search", "memory_get"]
        );
        let active_recall = parsed
            .memory
            .expect("memory plan")
            .active_recall
            .expect("active recall plan");
        assert_eq!(active_recall.status, ActiveRecallPlanJsonStatus::Run);
        assert_eq!(
            active_recall.reason_code,
            ActiveMemoryDecisionReasonCodeJson::MemoryLikely
        );
        assert_eq!(
            active_recall.modes,
            vec![ActiveRecallMode::Profile, ActiveRecallMode::ExactCanonical]
        );
        assert_eq!(
            active_recall.targets[0].canonical_key.as_deref(),
            Some("identity.current_user.name")
        );
    }

    #[test]
    fn provider_preflight_plan_json_parse_helper_is_strict_and_normalizes() {
        let raw = json!({
            "tools": {
                "visibleTools": [" memory_get ", "", "memory_search", "memory_get"]
            },
            "memory": {
                "activeRecall": {
                    "status": "run",
                    "reasonCode": "memory_likely",
                    "confidence": 0.92,
                    "modes": ["exact_canonical"],
                    "targets": [
                        {
                            "canonicalKey": "identity.current_user.name"
                        }
                    ]
                }
            }
        })
        .to_string();

        let parsed = parse_provider_turn_preflight_plan_json(raw.as_str())
            .expect("provider plan should parse and normalize");

        assert_eq!(
            parsed.tools.visible_tools,
            vec!["memory_get".to_owned(), "memory_search".to_owned()]
        );

        let malformed = parse_provider_turn_preflight_plan_json("{");
        assert!(malformed.is_err());

        let host_owned = json!({
            "source": "provider",
            "tools": {
                "visibleTools": []
            }
        })
        .to_string();
        let error = parse_provider_turn_preflight_plan_json(host_owned.as_str())
            .expect_err("provider plan must reject top-level host-owned fields");
        assert!(error.to_string().contains("source"));
    }

    #[test]
    fn provider_preflight_plan_normalizes_visible_tools_and_diagnostics() {
        let diagnostics = (0..20)
            .map(|index| TurnPreflightDiagnostic {
                code: diagnostic_code(format!("preflight.test.{index}")),
                message: None,
            })
            .collect();
        let plan = ProviderTurnPreflightPlan {
            tools: ProviderTurnPreflightToolsPlan {
                visible_tools: vec![
                    " memory_get ".to_owned(),
                    String::new(),
                    "memory_search".to_owned(),
                    "memory_get".to_owned(),
                ],
            },
            memory: None,
            diagnostics,
        };

        let normalized = normalize_provider_turn_preflight_plan(plan);

        assert_eq!(
            normalized.tools.visible_tools,
            vec!["memory_get".to_owned(), "memory_search".to_owned()]
        );
        assert_eq!(normalized.diagnostics.len(), PREFLIGHT_DIAGNOSTIC_MAX_COUNT);
        assert_eq!(
            normalized
                .diagnostics
                .last()
                .map(|diagnostic| diagnostic.code.as_str()),
            Some("preflight.test.15")
        );
    }

    #[test]
    fn provider_preflight_plan_rejects_host_owned_top_level_fields() {
        for field in [
            "source",
            "fallbackReason",
            "providerCall",
            "provider",
            "model",
            "attempt",
            "inputChars",
            "outputChars",
            "elapsedMs",
            "visibleTools",
        ] {
            let mut value = sample_provider_plan_json();
            value[field] = json!("host_owned");
            let error = serde_json::from_value::<ProviderTurnPreflightPlan>(value)
                .expect_err("provider plan must reject host-owned top-level fields");
            assert!(
                error.to_string().contains(field),
                "error `{error}` did not mention `{field}`"
            );
        }
    }

    #[test]
    fn provider_preflight_plan_rejects_host_owned_diagnostics_shape() {
        let mut value = sample_provider_plan_json();
        value["diagnostics"] = json!({
            "preflightFailed": true,
            "diagnostics": [{ "code": "preflight.failed" }]
        });

        serde_json::from_value::<ProviderTurnPreflightPlan>(value)
            .expect_err("provider diagnostics must not accept final-plan diagnostics object");
    }

    #[test]
    fn provider_preflight_plan_rejects_invalid_or_overlong_diagnostics() {
        let mut invalid_code = sample_provider_plan_json();
        invalid_code["diagnostics"] = json!([{ "code": "Preflight.Timeout" }]);
        serde_json::from_value::<ProviderTurnPreflightPlan>(invalid_code)
            .expect_err("provider diagnostic codes must be structured");

        let mut overlong_code = sample_provider_plan_json();
        overlong_code["diagnostics"] =
            json!([{ "code": "x".repeat(PREFLIGHT_DIAGNOSTIC_CODE_MAX_CHARS + 1) }]);
        serde_json::from_value::<ProviderTurnPreflightPlan>(overlong_code)
            .expect_err("provider diagnostic codes must be bounded");

        let mut overlong_message = sample_provider_plan_json();
        overlong_message["diagnostics"] = json!([{
            "code": "preflight.message_too_long",
            "message": "x".repeat(PREFLIGHT_DIAGNOSTIC_MESSAGE_MAX_CHARS + 1)
        }]);
        serde_json::from_value::<ProviderTurnPreflightPlan>(overlong_message)
            .expect_err("provider diagnostic messages must be bounded");
    }

    #[test]
    fn provider_memory_active_recall_uses_existing_memory_boundary() {
        let mut value = sample_provider_plan_json();
        value["memory"]["activeRecall"]["debugFallback"] = json!(true);
        value["memory"]["activeRecall"]["providerUsed"] = json!(true);
        value["memory"]["activeRecall"]["source"] = json!("provider");

        let parsed: ProviderTurnPreflightPlan =
            serde_json::from_value(value).expect("provider plan uses memory active recall parser");
        let active_recall = parsed
            .memory
            .expect("memory plan")
            .active_recall
            .expect("active recall plan");

        assert_eq!(active_recall.status, ActiveRecallPlanJsonStatus::Run);
        let serialized = serde_json::to_value(active_recall).expect("active recall serializes");
        assert!(serialized.get("debugFallback").is_none());
        assert!(serialized.get("providerUsed").is_none());
        assert!(serialized.get("source").is_none());
    }

    #[test]
    fn provider_memory_active_recall_validation_uses_existing_memory_parser() {
        let mut value = sample_provider_plan_json();
        value["memory"]["activeRecall"]["modes"] = json!([]);

        let parsed: ProviderTurnPreflightPlan =
            serde_json::from_value(value.clone()).expect("provider plan shape parses");
        let active_recall = parsed
            .memory
            .expect("memory plan")
            .active_recall
            .expect("active recall plan");

        let error = parse_provider_memory_active_recall_plan(&active_recall)
            .expect_err("memory parser should reject invalid active recall semantics");
        assert!(
            error.to_string().contains("requires at least one mode"),
            "unexpected error: {error}"
        );

        let raw = serde_json::to_string(&value).expect("provider plan serializes");
        let error = parse_provider_turn_preflight_plan_json(raw.as_str())
            .expect_err("full provider parse helper should validate active recall");
        assert!(
            error.to_string().contains("requires at least one mode"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn host_local_memory_active_recall_wrapper_uses_memory_normalization() {
        let active_recall = wrap_memory_active_recall_plan(
            TurnPreflightPlanSource::HostLocal,
            None,
            ActiveRecallPlan {
                status: ActiveMemoryDecisionStatus::Run,
                reason_code: ActiveMemoryDecisionReasonCode::MemoryLikely,
                confidence: 4.0,
                modes: vec![
                    ActiveRecallMode::Project,
                    ActiveRecallMode::Profile,
                    ActiveRecallMode::Profile,
                ],
                targets: Vec::new(),
                debug_fallback: false,
                provider_used: false,
                provider_fallback_used: false,
                provider_input_chars: None,
                provider_output_chars: None,
                diagnostics: vec![String::new(), "host_local".to_owned()],
            },
        );

        assert_eq!(active_recall.decision.confidence, 1.0);
        assert_eq!(active_recall.decision.modes.len(), 2);
        assert!(
            active_recall
                .decision
                .modes
                .contains(&ActiveRecallMode::Profile)
        );
        assert!(
            active_recall
                .decision
                .modes
                .contains(&ActiveRecallMode::Project)
        );
        assert_eq!(active_recall.decision.diagnostics, vec!["host_local"]);
    }

    #[test]
    fn final_preflight_plan_accepts_host_owned_metadata_and_module_fallbacks() {
        let active_recall = wrap_memory_active_recall_plan(
            TurnPreflightPlanSource::HostLocal,
            None,
            ActiveRecallPlan {
                status: ActiveMemoryDecisionStatus::Run,
                reason_code: ActiveMemoryDecisionReasonCode::MemoryLikely,
                confidence: 1.0,
                modes: vec![ActiveRecallMode::ExactCanonical],
                targets: vec![ActiveRecallTarget {
                    scope_kind: Some(MemoryScopeKind::User),
                    fact_class: Some(MemoryFactClass::UserIdentity),
                    category: Some(MemoryCategory::Identity),
                    subject: Some(MemorySubject::CurrentUser),
                    attribute: Some(MemoryAttribute::Name),
                    canonical_key: Some("identity.current_user.name".to_owned()),
                }],
                debug_fallback: false,
                provider_used: false,
                provider_fallback_used: false,
                provider_input_chars: None,
                provider_output_chars: None,
                diagnostics: vec!["memory.active_recall.host_local".to_owned()],
            },
        );
        let plan = fallback_turn_preflight_plan(
            TurnPreflightFallbackReason::Timeout,
            active_recall,
            vec![TurnPreflightDiagnostic {
                code: diagnostic_code("preflight.timeout"),
                message: Some(diagnostic_message("provider timed out")),
            }],
            BTreeMap::from([(
                "tools".to_owned(),
                vec![TurnPreflightDiagnostic {
                    code: diagnostic_code("preflight.tools.no_optional"),
                    message: None,
                }],
            )]),
            Some(TurnPreflightProviderCallMetadata {
                provider: "thread".to_owned(),
                model: "thread-model".to_owned(),
                attempt: 2,
                input_chars: 1200,
                output_chars: 0,
                elapsed_ms: 30_000,
            }),
        );

        let value = serde_json::to_value(&plan).expect("final plan serializes");
        assert_eq!(value["source"], "fallback");
        assert_eq!(value["fallbackReason"], "timeout");
        assert_eq!(value["tools"]["visibleTools"], json!([]));
        assert!(value.get("visibleTools").is_none());
        assert_eq!(value["diagnostics"]["preflightFailed"], true);
        assert_eq!(value["providerCall"]["attempt"], 2);
        assert_eq!(
            value["memory"]["activeRecall"]["source"],
            JsonValue::String("host_local".to_owned())
        );
        assert_eq!(
            value["memory"]["activeRecall"]["decision"]["reasonCode"],
            JsonValue::String("memory_likely".to_owned())
        );

        let decoded: TurnPreflightPlan =
            serde_json::from_value(value).expect("final plan deserializes");
        assert_eq!(decoded.source, TurnPreflightPlanSource::Fallback);
        assert_eq!(
            decoded.memory.active_recall.source,
            TurnPreflightPlanSource::HostLocal
        );
    }

    #[test]
    fn final_preflight_plan_can_represent_provider_owned_memory_plan() {
        let provider_call = TurnPreflightProviderCallMetadata {
            provider: "openai".to_owned(),
            model: "gpt-test".to_owned(),
            attempt: 1,
            input_chars: 900,
            output_chars: 240,
            elapsed_ms: 800,
        };
        let provider_plan: ProviderTurnPreflightPlan =
            serde_json::from_value(sample_provider_plan_json()).expect("provider plan parses");
        let provider_active_recall = provider_plan
            .memory
            .and_then(|memory| memory.active_recall)
            .expect("provider active recall");
        let decision_json = serde_json::to_string(&provider_active_recall)
            .expect("provider active recall serializes");
        let decision = parse_active_memory_decision_json(decision_json.as_str())
            .expect("provider active recall uses existing memory parser");
        let final_plan = TurnPreflightPlan {
            source: TurnPreflightPlanSource::Provider,
            fallback_reason: None,
            tools: TurnPreflightToolsPlan {
                visible_tools: provider_plan.tools.visible_tools,
            },
            memory: TurnPreflightMemoryPlan {
                active_recall: TurnPreflightMemoryActiveRecallPlan {
                    source: TurnPreflightPlanSource::Provider,
                    fallback_reason: None,
                    decision,
                },
            },
            diagnostics: TurnPreflightDiagnostics {
                preflight_failed: false,
                diagnostics: provider_plan.diagnostics,
                module_diagnostics: BTreeMap::new(),
            },
            provider_call: Some(provider_call),
        };

        assert_eq!(final_plan.source, TurnPreflightPlanSource::Provider);
        assert_eq!(
            final_plan.provider_call.as_ref().map(|call| call.attempt),
            Some(1)
        );
    }

    #[test]
    fn final_preflight_plan_can_represent_synthetic_module_fallback() {
        let value = json!({
            "source": "fallback",
            "fallbackReason": "validation_error",
            "tools": {
                "visibleTools": []
            },
            "memory": {
                "activeRecall": {
                    "source": "fallback",
                    "fallbackReason": "validation_error",
                    "decision": {
                        "status": "skip",
                        "reasonCode": "provider_skip",
                        "confidence": 0.0,
                        "modes": [],
                        "targets": [],
                        "debugFallback": false,
                        "providerUsed": false,
                        "providerFallbackUsed": true,
                        "diagnostics": [
                            "memory.active_recall.fallback"
                        ]
                    }
                }
            },
            "diagnostics": {
                "preflightFailed": true,
                "diagnostics": [
                    { "code": "preflight.validation_error" }
                ],
                "moduleDiagnostics": {
                    "memory.activeRecall": [
                        { "code": "memory.active_recall.provider_invalid_json" }
                    ]
                }
            }
        });

        let plan: TurnPreflightPlan =
            serde_json::from_value(value).expect("synthetic fallback final plan parses");
        assert_eq!(plan.source, TurnPreflightPlanSource::Fallback);
        assert!(plan.diagnostics.preflight_failed);
        assert_eq!(
            plan.memory.active_recall.fallback_reason,
            Some(TurnPreflightFallbackReason::ValidationError)
        );
        assert!(plan.memory.active_recall.decision.provider_fallback_used);
    }
}
