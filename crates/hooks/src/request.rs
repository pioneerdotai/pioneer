use crate::{
    HookCompactionId, HookContext, HookContribution, HookDiagnostic, HookId, HookMetadata,
    HookPhase, HookPolicySet, HookPromptContextSet, HookThreadId, HookToolName, HookTurnId,
    HookValue, HookWorkspaceId,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HookInputKind {
    TurnPrePolicy,
    TurnPrePromptContext,
    TurnPreToolMaterialization,
    TurnPrePromptCompile,
    TurnPostPromptCompile,
    TurnPostTurn,
    TurnPreCompaction,
    Custom(String),
}

impl HookInputKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::TurnPrePolicy => "turn.pre_policy",
            Self::TurnPrePromptContext => "turn.pre_prompt_context",
            Self::TurnPreToolMaterialization => "turn.pre_tool_materialization",
            Self::TurnPrePromptCompile => "turn.pre_prompt_compile",
            Self::TurnPostPromptCompile => "turn.post_prompt_compile",
            Self::TurnPostTurn => "turn.post_turn",
            Self::TurnPreCompaction => "turn.pre_compaction",
            Self::Custom(kind) => kind.as_str(),
        }
    }
}

impl From<HookPhase> for HookInputKind {
    fn from(phase: HookPhase) -> Self {
        match phase {
            HookPhase::TurnPrePolicy => Self::TurnPrePolicy,
            HookPhase::TurnPrePromptContext => Self::TurnPrePromptContext,
            HookPhase::TurnPreToolMaterialization => Self::TurnPreToolMaterialization,
            HookPhase::TurnPrePromptCompile => Self::TurnPrePromptCompile,
            HookPhase::TurnPostPromptCompile => Self::TurnPostPromptCompile,
            HookPhase::TurnPostTurn => Self::TurnPostTurn,
            HookPhase::TurnPreCompaction => Self::TurnPreCompaction,
        }
    }
}

impl From<&str> for HookInputKind {
    fn from(value: &str) -> Self {
        match value {
            "turn.pre_policy" => Self::TurnPrePolicy,
            "turn.pre_prompt_context" => Self::TurnPrePromptContext,
            "turn.pre_tool_materialization" => Self::TurnPreToolMaterialization,
            "turn.pre_prompt_compile" => Self::TurnPrePromptCompile,
            "turn.post_prompt_compile" => Self::TurnPostPromptCompile,
            "turn.post_turn" => Self::TurnPostTurn,
            "turn.pre_compaction" => Self::TurnPreCompaction,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl From<String> for HookInputKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "turn.pre_policy" => Self::TurnPrePolicy,
            "turn.pre_prompt_context" => Self::TurnPrePromptContext,
            "turn.pre_tool_materialization" => Self::TurnPreToolMaterialization,
            "turn.pre_prompt_compile" => Self::TurnPrePromptCompile,
            "turn.post_prompt_compile" => Self::TurnPostPromptCompile,
            "turn.post_turn" => Self::TurnPostTurn,
            "turn.pre_compaction" => Self::TurnPreCompaction,
            _ => Self::Custom(value),
        }
    }
}

impl fmt::Display for HookInputKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for HookInputKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HookInputKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookInput {
    pub kind: HookInputKind,
    pub payload: HookInputPayload,
}

impl HookInput {
    pub fn empty(kind: impl Into<HookInputKind>) -> Self {
        Self {
            kind: kind.into(),
            payload: HookInputPayload::Empty,
        }
    }

    pub fn custom(kind: impl Into<HookInputKind>, payload: HookValue) -> Self {
        Self {
            kind: kind.into(),
            payload: HookInputPayload::Custom(payload),
        }
    }

    pub fn turn_pre_policy(payload: TurnPrePolicyHookInput) -> Self {
        Self {
            kind: HookInputKind::TurnPrePolicy,
            payload: HookInputPayload::TurnPrePolicy(payload),
        }
    }

    pub fn turn_pre_prompt_context(payload: TurnPrePromptContextHookInput) -> Self {
        Self {
            kind: HookInputKind::TurnPrePromptContext,
            payload: HookInputPayload::TurnPrePromptContext(payload),
        }
    }

    pub fn turn_pre_prompt_compile(payload: TurnPrePromptCompileHookInput) -> Self {
        Self {
            kind: HookInputKind::TurnPrePromptCompile,
            payload: HookInputPayload::TurnPrePromptCompile(payload),
        }
    }

    pub fn turn_post_turn(payload: TurnPostTurnHookInput) -> Self {
        Self {
            kind: HookInputKind::TurnPostTurn,
            payload: HookInputPayload::TurnPostTurn(payload),
        }
    }

    pub fn turn_pre_tool_materialization(payload: TurnPreToolMaterializationHookInput) -> Self {
        Self {
            kind: HookInputKind::TurnPreToolMaterialization,
            payload: HookInputPayload::TurnPreToolMaterialization(payload),
        }
    }

    pub fn turn_pre_compaction(payload: TurnPreCompactionHookInput) -> Self {
        Self {
            kind: HookInputKind::TurnPreCompaction,
            payload: HookInputPayload::TurnPreCompaction(payload),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum HookInputPayload {
    Empty,
    TurnPrePolicy(TurnPrePolicyHookInput),
    TurnPrePromptContext(TurnPrePromptContextHookInput),
    TurnPreToolMaterialization(TurnPreToolMaterializationHookInput),
    TurnPrePromptCompile(TurnPrePromptCompileHookInput),
    TurnPostTurn(TurnPostTurnHookInput),
    TurnPreCompaction(TurnPreCompactionHookInput),
    Custom(HookValue),
}

impl Default for HookInputPayload {
    fn default() -> Self {
        Self::Empty
    }
}

pub const DEFAULT_POST_TURN_USER_TEXT_PREVIEW_MAX_CHARS: usize = 4_000;
pub const DEFAULT_POST_TURN_ASSISTANT_TEXT_PREVIEW_MAX_CHARS: usize = 4_000;
pub const DEFAULT_POST_TURN_ERROR_PREVIEW_MAX_CHARS: usize = 1_000;
pub const DEFAULT_POST_TURN_TOOL_EVENT_MAX_COUNT: usize = 40;
pub const DEFAULT_POST_TURN_DOMAIN_EVENT_MAX_COUNT: usize = 40;
pub const DEFAULT_POST_TURN_DOMAIN_EVENT_MESSAGE_MAX_CHARS: usize = 1_000;
pub const DEFAULT_PRE_COMPACTION_EXISTING_SUMMARY_PREVIEW_MAX_CHARS: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPrePolicyHookInput {
    pub input_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
}

impl TurnPrePolicyHookInput {
    pub fn from_parts(
        input_text: impl Into<String>,
        model: Option<impl Into<String>>,
        model_provider: Option<impl Into<String>>,
    ) -> Self {
        Self {
            input_text: input_text.into(),
            model: model.map(Into::into),
            model_provider: model_provider.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPrePromptContextHookInput {
    pub input_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
}

impl TurnPrePromptContextHookInput {
    pub fn from_parts(
        input_text: impl Into<String>,
        model: Option<impl Into<String>>,
        model_provider: Option<impl Into<String>>,
    ) -> Self {
        Self {
            input_text: input_text.into(),
            model: model.map(Into::into),
            model_provider: model_provider.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPreToolMaterializationHookInput {
    pub provider_tool_calling: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub existing_tool_names: Vec<HookToolName>,
}

impl TurnPreToolMaterializationHookInput {
    pub fn from_parts(
        provider_tool_calling: bool,
        mut existing_tool_names: Vec<HookToolName>,
    ) -> Self {
        existing_tool_names.sort();
        existing_tool_names.dedup();

        Self {
            provider_tool_calling,
            existing_tool_names,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPrePromptCompileHookInput {
    pub provider_tool_calling: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available_tool_names: Vec<HookToolName>,
}

impl TurnPrePromptCompileHookInput {
    pub fn from_parts(
        provider_tool_calling: bool,
        mut available_tool_names: Vec<HookToolName>,
    ) -> Self {
        available_tool_names.sort();
        available_tool_names.dedup();

        Self {
            provider_tool_calling,
            available_tool_names,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPostTurnHookInputLimits {
    pub user_text_preview_max_chars: usize,
    pub assistant_text_preview_max_chars: usize,
    pub error_preview_max_chars: usize,
    pub tool_event_max_count: usize,
    pub domain_event_max_count: usize,
    pub domain_event_message_max_chars: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPreCompactionHookInputLimits {
    pub existing_summary_preview_max_chars: usize,
}

impl Default for TurnPreCompactionHookInputLimits {
    fn default() -> Self {
        Self {
            existing_summary_preview_max_chars:
                DEFAULT_PRE_COMPACTION_EXISTING_SUMMARY_PREVIEW_MAX_CHARS,
        }
    }
}

impl TurnPreCompactionHookInputLimits {
    pub fn normalized(mut self) -> Self {
        self.existing_summary_preview_max_chars = self.existing_summary_preview_max_chars.max(1);
        self
    }
}

impl Default for TurnPostTurnHookInputLimits {
    fn default() -> Self {
        Self {
            user_text_preview_max_chars: DEFAULT_POST_TURN_USER_TEXT_PREVIEW_MAX_CHARS,
            assistant_text_preview_max_chars: DEFAULT_POST_TURN_ASSISTANT_TEXT_PREVIEW_MAX_CHARS,
            error_preview_max_chars: DEFAULT_POST_TURN_ERROR_PREVIEW_MAX_CHARS,
            tool_event_max_count: DEFAULT_POST_TURN_TOOL_EVENT_MAX_COUNT,
            domain_event_max_count: DEFAULT_POST_TURN_DOMAIN_EVENT_MAX_COUNT,
            domain_event_message_max_chars: DEFAULT_POST_TURN_DOMAIN_EVENT_MESSAGE_MAX_CHARS,
        }
    }
}

impl TurnPostTurnHookInputLimits {
    pub fn normalized(mut self) -> Self {
        self.user_text_preview_max_chars = self.user_text_preview_max_chars.max(1);
        self.assistant_text_preview_max_chars = self.assistant_text_preview_max_chars.max(1);
        self.error_preview_max_chars = self.error_preview_max_chars.max(1);
        self.tool_event_max_count = self.tool_event_max_count.max(1);
        self.domain_event_max_count = self.domain_event_max_count.max(1);
        self.domain_event_message_max_chars = self.domain_event_message_max_chars.max(1);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookTextPreview {
    pub text: String,
    pub original_chars: usize,
    pub truncated: bool,
    pub max_chars: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPreCompactionTrigger {
    ContextBudgetThreshold,
    Manual,
    Recovery,
    Scheduled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPreCompactionSourceKind {
    ConversationHistory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPreCompactionSourceRange {
    pub source_kind: TurnPreCompactionSourceKind,
    pub loaded_completed_turn_count: usize,
    pub source_entry_count: usize,
    pub max_loaded_turns: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_summary_turn_count: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPreCompactionTokenBudget {
    pub max_context_tokens: usize,
    pub response_reserve_tokens: usize,
    pub history_budget_tokens: usize,
    pub estimated_current_tokens: usize,
    pub compression_threshold_tokens: usize,
    pub target_summary_tokens: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPreCompactionSummaryStrategy {
    ProgressiveFullHistorySummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPreCompactionSummaryPolicy {
    pub strategy: TurnPreCompactionSummaryStrategy,
    pub compression_threshold_bps: u16,
    pub compression_target_bps: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPreCompactionRawTurnRetention {
    RetainOriginalTurns,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPreCompactionSummaryStorage {
    ThreadSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPreCompactionRetentionPolicy {
    pub raw_turn_retention: TurnPreCompactionRawTurnRetention,
    pub summary_storage: TurnPreCompactionSummaryStorage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPreCompactionHookInput {
    pub workspace_id: HookWorkspaceId,
    pub thread_id: HookThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<HookTurnId>,
    pub compaction_id: HookCompactionId,
    pub trigger: TurnPreCompactionTrigger,
    pub source_range: TurnPreCompactionSourceRange,
    pub token_budget: TurnPreCompactionTokenBudget,
    pub summary_policy: TurnPreCompactionSummaryPolicy,
    pub retention_policy: TurnPreCompactionRetentionPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_summary_preview: Option<HookTextPreview>,
    pub limits: TurnPreCompactionHookInputLimits,
}

impl TurnPreCompactionHookInput {
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        workspace_id: HookWorkspaceId,
        thread_id: HookThreadId,
        turn_id: Option<HookTurnId>,
        compaction_id: HookCompactionId,
        trigger: TurnPreCompactionTrigger,
        source_range: TurnPreCompactionSourceRange,
        token_budget: TurnPreCompactionTokenBudget,
        summary_policy: TurnPreCompactionSummaryPolicy,
        retention_policy: TurnPreCompactionRetentionPolicy,
        existing_summary: Option<impl AsRef<str>>,
        limits: TurnPreCompactionHookInputLimits,
    ) -> Self {
        let limits = limits.normalized();
        let existing_summary_preview = existing_summary
            .filter(|summary| !summary.as_ref().is_empty())
            .map(|summary| {
                HookTextPreview::from_text(summary, limits.existing_summary_preview_max_chars)
            });

        Self {
            workspace_id,
            thread_id,
            turn_id,
            compaction_id,
            trigger,
            source_range,
            token_budget,
            summary_policy,
            retention_policy,
            existing_summary_preview,
            limits,
        }
    }
}

impl HookTextPreview {
    pub fn from_text(text: impl AsRef<str>, max_chars: usize) -> Self {
        let text = text.as_ref();
        let max_chars = max_chars.max(1);
        let original_chars = text.chars().count();
        let truncated = original_chars > max_chars;
        let preview = if truncated {
            text.chars().take(max_chars).collect()
        } else {
            text.to_owned()
        };

        Self {
            text: preview,
            original_chars,
            truncated,
            max_chars,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPostTurnStatus {
    Succeeded,
    Failed,
    ProviderFailure,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPostTurnToolStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPostTurnToolOutcomeStatus {
    Ok,
    RecoverableError,
    FatalError,
    PartialSuccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPostTurnToolErrorClass {
    InvalidArguments,
    NotFound,
    PermissionDenied,
    CommandNotFound,
    Timeout,
    Cancelled,
    ExecutionFailed,
    NeedsNarrowing,
    Internal,
    OutputTruncated,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPostTurnToolEventSummary {
    pub item_id: String,
    pub item_type: String,
    pub tool_name: String,
    pub attempt_number: u32,
    pub status: TurnPostTurnToolStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_status: Option<TurnPostTurnToolOutcomeStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<TurnPostTurnToolErrorClass>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum TurnPostTurnDomain {
    Tool,
    Task,
    Memory,
    Mcp,
    Skill,
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPostTurnDomainEventSummary {
    pub domain: TurnPostTurnDomain,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<HookTextPreview>,
}

impl TurnPostTurnDomainEventSummary {
    pub fn bounded(mut self, max_message_chars: usize) -> Self {
        if let Some(message) = self.message.take() {
            self.message = Some(HookTextPreview::from_text(
                message.text,
                max_message_chars.max(1),
            ));
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPostTurnHookInput {
    pub status: TurnPostTurnStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_text: Option<HookTextPreview>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assistant_text: Option<HookTextPreview>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<HookTextPreview>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_events: Vec<TurnPostTurnToolEventSummary>,
    pub tool_events_truncated: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain_events: Vec<TurnPostTurnDomainEventSummary>,
    pub domain_events_truncated: bool,
    pub limits: TurnPostTurnHookInputLimits,
}

impl TurnPostTurnHookInput {
    pub fn from_parts(
        status: TurnPostTurnStatus,
        user_text: Option<impl AsRef<str>>,
        assistant_text: Option<impl AsRef<str>>,
        error: Option<impl AsRef<str>>,
        mut tool_events: Vec<TurnPostTurnToolEventSummary>,
        mut domain_events: Vec<TurnPostTurnDomainEventSummary>,
        limits: TurnPostTurnHookInputLimits,
    ) -> Self {
        let limits = limits.normalized();
        let user_text = user_text
            .filter(|text| !text.as_ref().is_empty())
            .map(|text| HookTextPreview::from_text(text, limits.user_text_preview_max_chars));
        let assistant_text = assistant_text
            .filter(|text| !text.as_ref().is_empty())
            .map(|text| HookTextPreview::from_text(text, limits.assistant_text_preview_max_chars));
        let error = error
            .filter(|text| !text.as_ref().is_empty())
            .map(|text| HookTextPreview::from_text(text, limits.error_preview_max_chars));

        let tool_events_truncated = tool_events.len() > limits.tool_event_max_count;
        tool_events.truncate(limits.tool_event_max_count);

        let domain_events_truncated = domain_events.len() > limits.domain_event_max_count;
        domain_events.truncate(limits.domain_event_max_count);
        let domain_events = domain_events
            .into_iter()
            .map(|event| event.bounded(limits.domain_event_message_max_chars))
            .collect();

        Self {
            status,
            user_text,
            assistant_text,
            error,
            tool_events,
            tool_events_truncated,
            domain_events,
            domain_events_truncated,
            limits,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookHandlerRequest {
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub context: HookContext,
    pub input: HookInput,
    #[serde(default, skip_serializing_if = "HookPolicySet::is_empty")]
    pub policy_set: HookPolicySet,
    #[serde(default, skip_serializing_if = "HookPromptContextSet::is_empty")]
    pub prompt_context_set: HookPromptContextSet,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookHandlerResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contributions: Vec<HookContribution>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: HookMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HookCompactionId, HookMetadataKey, HookThreadId, HookTurnId, HookWorkspaceId};

    #[test]
    fn handler_request_response_roundtrip() {
        let request = HookHandlerRequest {
            hook_id: HookId::new("policy.turn_classifier").expect("valid id"),
            phase: HookPhase::TurnPrePolicy,
            context: HookContext {
                thread_id: Some(HookThreadId::new("thread-1").expect("valid thread id")),
                turn_id: Some(HookTurnId::new("turn-1").expect("valid turn id")),
                ..HookContext::default()
            },
            input: HookInput::custom(
                HookInputKind::TurnPrePolicy,
                HookValue::Object(BTreeMap::from([
                    (
                        HookMetadataKey::new("await_policy").expect("valid metadata key"),
                        HookValue::Text("deadline".to_owned()),
                    ),
                    (
                        HookMetadataKey::new("timeout_ms").expect("valid metadata key"),
                        HookValue::I64(500),
                    ),
                ])),
            ),
            policy_set: HookPolicySet::empty(),
            prompt_context_set: HookPromptContextSet::empty(),
        };

        let value = serde_json::to_value(&request).expect("request should serialize");
        let decoded: HookHandlerRequest =
            serde_json::from_value(value).expect("request should deserialize");
        assert_eq!(decoded, request);

        let response = HookHandlerResponse::default();
        let value = serde_json::to_value(&response).expect("response should serialize");
        let decoded: HookHandlerResponse =
            serde_json::from_value(value).expect("response should deserialize");
        assert_eq!(decoded, response);
    }

    #[test]
    fn turn_pre_policy_input_roundtrips() {
        let input = TurnPrePolicyHookInput::from_parts(
            "remember my birthday",
            Some("gpt-test"),
            Some("test-provider"),
        );

        let value = serde_json::to_value(&input).expect("pre-policy input serializes");
        let decoded: TurnPrePolicyHookInput =
            serde_json::from_value(value).expect("pre-policy input deserializes");

        assert_eq!(decoded, input);
    }

    #[test]
    fn hook_input_payload_distinguishes_pre_policy() {
        let input = TurnPrePolicyHookInput::from_parts(
            "turn text",
            Option::<String>::None,
            Option::<String>::None,
        );
        let hook_input = HookInput::turn_pre_policy(input.clone());

        assert_eq!(hook_input.kind, HookInputKind::TurnPrePolicy);
        assert_eq!(hook_input.payload, HookInputPayload::TurnPrePolicy(input));
    }

    #[test]
    fn turn_pre_prompt_context_input_roundtrips() {
        let input = TurnPrePromptContextHookInput::from_parts(
            "what do you remember?",
            Some("gpt-test"),
            Some("test-provider"),
        );

        let value = serde_json::to_value(&input).expect("pre-prompt-context input serializes");
        let decoded: TurnPrePromptContextHookInput =
            serde_json::from_value(value).expect("pre-prompt-context input deserializes");

        assert_eq!(decoded, input);
    }

    #[test]
    fn hook_input_payload_distinguishes_pre_prompt_context() {
        let input = TurnPrePromptContextHookInput::from_parts(
            "turn text",
            Option::<String>::None,
            Option::<String>::None,
        );
        let hook_input = HookInput::turn_pre_prompt_context(input.clone());

        assert_eq!(hook_input.kind, HookInputKind::TurnPrePromptContext);
        assert_eq!(
            hook_input.payload,
            HookInputPayload::TurnPrePromptContext(input)
        );
    }

    #[test]
    fn turn_pre_prompt_compile_input_sorts_and_dedups_tool_names() {
        let input = TurnPrePromptCompileHookInput::from_parts(
            true,
            vec![
                HookToolName::new("memory_search").expect("valid tool name"),
                HookToolName::new("shell").expect("valid tool name"),
                HookToolName::new("memory_search").expect("valid tool name"),
            ],
        );

        assert!(input.provider_tool_calling);
        assert_eq!(
            input.available_tool_names,
            vec![
                HookToolName::new("memory_search").expect("valid tool name"),
                HookToolName::new("shell").expect("valid tool name"),
            ]
        );
    }

    #[test]
    fn turn_pre_prompt_compile_input_roundtrips() {
        let input = TurnPrePromptCompileHookInput::from_parts(
            false,
            vec![HookToolName::new("shell").expect("valid tool name")],
        );

        let value = serde_json::to_value(&input).expect("pre-prompt-compile input serializes");
        let decoded: TurnPrePromptCompileHookInput =
            serde_json::from_value(value).expect("pre-prompt-compile input deserializes");

        assert_eq!(decoded, input);
    }

    #[test]
    fn hook_input_payload_distinguishes_pre_prompt_compile() {
        let input = TurnPrePromptCompileHookInput::from_parts(
            true,
            vec![HookToolName::new("shell").expect("valid tool name")],
        );
        let hook_input = HookInput::turn_pre_prompt_compile(input.clone());

        assert_eq!(hook_input.kind, HookInputKind::TurnPrePromptCompile);
        assert_eq!(
            hook_input.payload,
            HookInputPayload::TurnPrePromptCompile(input)
        );
    }

    #[test]
    fn turn_pre_tool_materialization_input_sorts_and_dedups_tool_names() {
        let input = TurnPreToolMaterializationHookInput::from_parts(
            true,
            vec![
                HookToolName::new("memory_search").expect("valid tool name"),
                HookToolName::new("shell").expect("valid tool name"),
                HookToolName::new("memory_search").expect("valid tool name"),
            ],
        );

        assert!(input.provider_tool_calling);
        assert_eq!(
            input.existing_tool_names,
            vec![
                HookToolName::new("memory_search").expect("valid tool name"),
                HookToolName::new("shell").expect("valid tool name"),
            ]
        );
    }

    #[test]
    fn turn_pre_tool_materialization_input_roundtrips() {
        let input = TurnPreToolMaterializationHookInput::from_parts(
            false,
            vec![HookToolName::new("shell").expect("valid tool name")],
        );

        let value = serde_json::to_value(&input).expect("pre-tool input serializes");
        let decoded: TurnPreToolMaterializationHookInput =
            serde_json::from_value(value).expect("pre-tool input deserializes");

        assert_eq!(decoded, input);
    }

    #[test]
    fn hook_input_payload_distinguishes_pre_tool_materialization() {
        let empty = HookInput::empty(HookInputKind::TurnPrePolicy);
        assert_eq!(empty.payload, HookInputPayload::Empty);

        let pre_tool = HookInput::turn_pre_tool_materialization(
            TurnPreToolMaterializationHookInput::from_parts(
                true,
                vec![HookToolName::new("shell").expect("valid tool name")],
            ),
        );

        assert_eq!(pre_tool.kind, HookInputKind::TurnPreToolMaterialization);
        assert!(matches!(
            pre_tool.payload,
            HookInputPayload::TurnPreToolMaterialization(_)
        ));
    }

    #[test]
    fn hook_input_kind_maps_pre_tool_materialization_phase() {
        assert_eq!(
            HookInputKind::from(HookPhase::TurnPreToolMaterialization),
            HookInputKind::TurnPreToolMaterialization
        );
        assert_eq!(
            HookInputKind::from("turn.pre_tool_materialization"),
            HookInputKind::TurnPreToolMaterialization
        );
        assert_eq!(
            serde_json::to_value(HookInputKind::TurnPreToolMaterialization)
                .expect("input kind serializes"),
            serde_json::json!("turn.pre_tool_materialization")
        );
    }

    #[test]
    fn turn_post_turn_input_bounds_text_and_events() {
        let limits = TurnPostTurnHookInputLimits {
            user_text_preview_max_chars: 4,
            assistant_text_preview_max_chars: 5,
            error_preview_max_chars: 6,
            tool_event_max_count: 1,
            domain_event_max_count: 1,
            domain_event_message_max_chars: 3,
        };
        let input = TurnPostTurnHookInput::from_parts(
            TurnPostTurnStatus::Succeeded,
            Some("abcdef"),
            Some("abcdef"),
            Some("abcdefg"),
            vec![
                TurnPostTurnToolEventSummary {
                    item_id: "tool-1".to_owned(),
                    item_type: "tool".to_owned(),
                    tool_name: "shell".to_owned(),
                    attempt_number: 1,
                    status: TurnPostTurnToolStatus::Succeeded,
                    outcome_status: Some(TurnPostTurnToolOutcomeStatus::Ok),
                    error_class: None,
                },
                TurnPostTurnToolEventSummary {
                    item_id: "tool-2".to_owned(),
                    item_type: "tool".to_owned(),
                    tool_name: "shell".to_owned(),
                    attempt_number: 1,
                    status: TurnPostTurnToolStatus::Failed,
                    outcome_status: Some(TurnPostTurnToolOutcomeStatus::FatalError),
                    error_class: Some(TurnPostTurnToolErrorClass::Internal),
                },
            ],
            vec![
                TurnPostTurnDomainEventSummary {
                    domain: TurnPostTurnDomain::Task,
                    code: Some("task.done".to_owned()),
                    item_id: Some("task-1".to_owned()),
                    message: Some(HookTextPreview::from_text("abcdef", 10)),
                },
                TurnPostTurnDomainEventSummary {
                    domain: TurnPostTurnDomain::Memory,
                    code: None,
                    item_id: None,
                    message: None,
                },
            ],
            limits,
        );

        assert_eq!(input.user_text.as_ref().expect("user preview").text, "abcd");
        assert!(input.user_text.as_ref().expect("user preview").truncated);
        assert_eq!(
            input
                .assistant_text
                .as_ref()
                .expect("assistant preview")
                .text,
            "abcde"
        );
        assert_eq!(input.error.as_ref().expect("error preview").text, "abcdef");
        assert_eq!(input.tool_events.len(), 1);
        assert!(input.tool_events_truncated);
        assert_eq!(input.domain_events.len(), 1);
        assert!(input.domain_events_truncated);
        assert_eq!(
            input.domain_events[0]
                .message
                .as_ref()
                .expect("domain preview")
                .text,
            "abc"
        );

        let hook_input = HookInput::turn_post_turn(input.clone());
        assert_eq!(hook_input.kind, HookInputKind::TurnPostTurn);
        assert_eq!(hook_input.payload, HookInputPayload::TurnPostTurn(input));
    }

    #[test]
    fn phase_12_turn_post_turn_input_roundtrips() {
        let input = TurnPostTurnHookInput::from_parts(
            TurnPostTurnStatus::Succeeded,
            Some("user"),
            Some("assistant"),
            Option::<&str>::None,
            Vec::new(),
            Vec::new(),
            TurnPostTurnHookInputLimits::default(),
        );
        let value = serde_json::to_value(&input).expect("post-turn input serializes");
        let decoded: TurnPostTurnHookInput =
            serde_json::from_value(value).expect("post-turn input deserializes");
        assert_eq!(decoded, input);
    }

    #[test]
    fn phase_12_hook_input_payload_distinguishes_empty_and_post_turn() {
        let empty = HookInput::empty(HookInputKind::TurnPrePolicy);
        assert_eq!(empty.payload, HookInputPayload::Empty);

        let post_turn = HookInput::turn_post_turn(TurnPostTurnHookInput::from_parts(
            TurnPostTurnStatus::Succeeded,
            Some("user"),
            Some("assistant"),
            Option::<&str>::None,
            Vec::new(),
            Vec::new(),
            TurnPostTurnHookInputLimits::default(),
        ));
        assert!(matches!(
            post_turn.payload,
            HookInputPayload::TurnPostTurn(_)
        ));
    }

    #[test]
    fn phase_12_text_preview_truncates_by_char_boundary() {
        let preview = HookTextPreview::from_text("aé日b", 3);
        assert_eq!(preview.text, "aé日");
        assert_eq!(preview.original_chars, 4);
        assert!(preview.truncated);
    }

    #[test]
    fn phase_12_post_turn_limits_normalize_to_nonzero_values() {
        let limits = TurnPostTurnHookInputLimits {
            user_text_preview_max_chars: 0,
            assistant_text_preview_max_chars: 0,
            error_preview_max_chars: 0,
            tool_event_max_count: 0,
            domain_event_max_count: 0,
            domain_event_message_max_chars: 0,
        }
        .normalized();

        assert_eq!(limits.user_text_preview_max_chars, 1);
        assert_eq!(limits.assistant_text_preview_max_chars, 1);
        assert_eq!(limits.error_preview_max_chars, 1);
        assert_eq!(limits.tool_event_max_count, 1);
        assert_eq!(limits.domain_event_max_count, 1);
        assert_eq!(limits.domain_event_message_max_chars, 1);
    }

    fn phase_13_pre_compaction_input(
        existing_summary: Option<&str>,
        limits: TurnPreCompactionHookInputLimits,
    ) -> TurnPreCompactionHookInput {
        TurnPreCompactionHookInput::from_parts(
            HookWorkspaceId::new("workspace-1").expect("valid workspace id"),
            HookThreadId::new("thread-1").expect("valid thread id"),
            Some(HookTurnId::new("turn-1").expect("valid turn id")),
            HookCompactionId::new("cmp-1").expect("valid compaction id"),
            TurnPreCompactionTrigger::ContextBudgetThreshold,
            TurnPreCompactionSourceRange {
                source_kind: TurnPreCompactionSourceKind::ConversationHistory,
                loaded_completed_turn_count: 3,
                source_entry_count: 3,
                max_loaded_turns: 200,
                existing_summary_turn_count: Some(2),
            },
            TurnPreCompactionTokenBudget {
                max_context_tokens: 1_000,
                response_reserve_tokens: 100,
                history_budget_tokens: 900,
                estimated_current_tokens: 800,
                compression_threshold_tokens: 720,
                target_summary_tokens: 90,
            },
            TurnPreCompactionSummaryPolicy {
                strategy: TurnPreCompactionSummaryStrategy::ProgressiveFullHistorySummary,
                compression_threshold_bps: 8_000,
                compression_target_bps: 1_000,
            },
            TurnPreCompactionRetentionPolicy {
                raw_turn_retention: TurnPreCompactionRawTurnRetention::RetainOriginalTurns,
                summary_storage: TurnPreCompactionSummaryStorage::ThreadSummary,
            },
            existing_summary,
            limits,
        )
    }

    #[test]
    fn phase_13_turn_pre_compaction_input_roundtrips() {
        let input = phase_13_pre_compaction_input(
            Some("existing summary"),
            TurnPreCompactionHookInputLimits::default(),
        );

        let value = serde_json::to_value(&input).expect("pre-compaction input serializes");
        let decoded: TurnPreCompactionHookInput =
            serde_json::from_value(value).expect("pre-compaction input deserializes");

        assert_eq!(decoded, input);
        assert_eq!(decoded.workspace_id.as_str(), "workspace-1");
        assert_eq!(decoded.thread_id.as_str(), "thread-1");
        assert_eq!(
            decoded.turn_id.as_ref().expect("turn id").as_str(),
            "turn-1"
        );
        assert_eq!(decoded.compaction_id.as_str(), "cmp-1");
    }

    #[test]
    fn phase_13_hook_input_payload_distinguishes_pre_compaction() {
        let empty = HookInput::empty(HookInputKind::TurnPrePolicy);
        assert_eq!(empty.payload, HookInputPayload::Empty);

        let post_turn = HookInput::turn_post_turn(TurnPostTurnHookInput::from_parts(
            TurnPostTurnStatus::Succeeded,
            Some("user"),
            Some("assistant"),
            Option::<&str>::None,
            Vec::new(),
            Vec::new(),
            TurnPostTurnHookInputLimits::default(),
        ));
        assert!(matches!(
            post_turn.payload,
            HookInputPayload::TurnPostTurn(_)
        ));

        let pre_compaction = HookInput::turn_pre_compaction(phase_13_pre_compaction_input(
            Option::<&str>::None,
            TurnPreCompactionHookInputLimits::default(),
        ));
        assert_eq!(pre_compaction.kind, HookInputKind::TurnPreCompaction);
        assert!(matches!(
            pre_compaction.payload,
            HookInputPayload::TurnPreCompaction(_)
        ));
    }

    #[test]
    fn phase_13_existing_summary_preview_is_bounded() {
        let input = phase_13_pre_compaction_input(
            Some("aé日b"),
            TurnPreCompactionHookInputLimits {
                existing_summary_preview_max_chars: 3,
            },
        );

        let preview = input
            .existing_summary_preview
            .expect("summary preview should be present");
        assert_eq!(preview.text, "aé日");
        assert_eq!(preview.original_chars, 4);
        assert!(preview.truncated);
        assert_eq!(preview.max_chars, 3);
    }

    #[test]
    fn phase_13_pre_compaction_limits_normalize_to_nonzero_values() {
        let limits = TurnPreCompactionHookInputLimits {
            existing_summary_preview_max_chars: 0,
        }
        .normalized();

        assert_eq!(limits.existing_summary_preview_max_chars, 1);

        let input = phase_13_pre_compaction_input(
            Some("summary"),
            TurnPreCompactionHookInputLimits {
                existing_summary_preview_max_chars: 0,
            },
        );
        assert_eq!(input.limits.existing_summary_preview_max_chars, 1);
    }

    #[test]
    fn phase_13_pre_compaction_uses_typed_ids() {
        let input = phase_13_pre_compaction_input(
            Option::<&str>::None,
            TurnPreCompactionHookInputLimits::default(),
        );

        assert_eq!(
            input.workspace_id,
            HookWorkspaceId::new("workspace-1").unwrap()
        );
        assert_eq!(input.thread_id, HookThreadId::new("thread-1").unwrap());
        assert_eq!(
            input.turn_id,
            Some(HookTurnId::new("turn-1").expect("valid turn id"))
        );
        assert_eq!(
            input.compaction_id,
            HookCompactionId::new("cmp-1").expect("valid compaction id")
        );
    }
}
