use crate::hooks::AgentToolBundleArtifactStore;
use chrono::{DateTime, Utc};
use pioneer_hooks::HookHandler;
use pioneer_hooks::{
    HookAwaitPolicy, HookCapabilities, HookCapability, HookContribution, HookContributionId,
    HookDiagnostic, HookDiagnosticCode, HookDiagnosticMessage, HookDiagnosticSeverity, HookDomain,
    HookError, HookExecutionPolicy, HookFailurePolicy, HookHandlerRequest, HookHandlerResponse,
    HookId, HookInputPayload, HookKind, HookMetadata, HookMetadataKey, HookPhase, HookPolicyKey,
    HookPolicySet, HookPromptContent, HookRegistryError, HookResult, HookRuntime, HookSectionId,
    HookSourceId, HookSourceKind, HookSourceRef, HookSubscription, HookSubscriptionDependencies,
    HookSubscriptionId, HookSubscriptionVisibility, HookToolBundleId, HookToolName, HookValue,
    PolicyContribution, PromptContextContribution, PromptSectionContribution,
    ToolBundleContribution, TurnPrePolicyHookInput, TurnPrePromptCompileHookInput,
    TurnPrePromptContextHookInput,
};
use pioneer_promt::{
    MemoryRecallPromptInput, MemoryRecallPromptItem, MemoryRecallPromptPolicy,
    render_memory_recall_context_block, render_memory_recall_prompt,
};
use pioneer_protocol::{MemoryCategory, MemoryScope, MemoryScopeKind, ThreadMode};
use pioneer_tools::ToolExtensionBundle;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

pub const MEMORY_SEARCH_TOOL: &str = "memory_search";
pub const MEMORY_GET_TOOL: &str = "memory_get";
pub const MEMORY_REMEMBER_TOOL: &str = "memory_remember";
pub const MEMORY_FORGET_TOOL: &str = "memory_forget";

const MEMORY_POLICY_DOMAIN: &str = "memory";
const MEMORY_TURN_POLICY_KEY: &str = "turn_policy";
const MEMORY_POLICY_CLASSIFIER_HOOK_ID: &str = "memory.policy_classifier";
const MEMORY_POLICY_CLASSIFIER_SUBSCRIPTION_ID: &str = "memory.policy_classifier.default";
const MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY: &str = "memory.policy_classifier.override";
const MEMORY_TURN_POLICY_DEFAULT_METADATA_KEY: &str = "memory.policy_classifier.default_policy";
const MEMORY_TURN_POLICY_CLASSIFIER_ENABLED_METADATA_KEY: &str =
    "memory.policy_classifier.classifier_enabled";
const MEMORY_TURN_POLICY_FALLBACK_METADATA_KEY: &str = "memory.policy_classifier.fallback";
const MEMORY_TOOL_BUNDLE_HOOK_ID: &str = "memory.tool_bundle";
const MEMORY_TOOL_BUNDLE_SUBSCRIPTION_ID: &str = "memory.tool_bundle.default";
const MEMORY_TOOL_BUNDLE_CONTRIBUTION_ID_PREFIX: &str = "memory.tool_bundle.contribution";
const MEMORY_TOOL_BUNDLE_ID_PREFIX: &str = "memory.tool_bundle.bundle";
const MEMORY_DETERMINISTIC_RECALL_HOOK_ID: &str = "memory.deterministic_recall";
const MEMORY_DETERMINISTIC_RECALL_SUBSCRIPTION_ID: &str = "memory.deterministic_recall.default";
const MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID: &str = "memory.deterministic_recall.context";
const MEMORY_ACTIVE_RECALL_HOOK_ID: &str = "memory.active_recall";
const MEMORY_ACTIVE_RECALL_SUBSCRIPTION_ID: &str = "memory.active_recall.default";
const MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID: &str = "memory.active_recall.context";
const MEMORY_PROMPT_CONTRACT_HOOK_ID: &str = "memory.prompt_contract";
const MEMORY_PROMPT_CONTRACT_SUBSCRIPTION_ID: &str = "memory.prompt_contract.default";
const MEMORY_PROMPT_CONTRACT_CONTRIBUTION_ID: &str = "memory.prompt_contract.section";
const MEMORY_PROMPT_CONTRACT_SECTION_ID: &str = "memory_recall";
const MEMORY_ACTIVE_RECALL_GENERIC_QUERY: &str = "durable user identity preferences biography communication style recurring instructions project facts project decisions procedures constraints todos ongoing tasks";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryTurnContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub mode: ThreadMode,
    pub input_text: String,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecallRequest {
    pub query: String,
    pub categories: Vec<MemoryCategory>,
    pub top_k: Option<u32>,
    pub max_chars: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecallItem {
    pub memory_id: String,
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub content: String,
    pub score: Option<f32>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MemoryRecallSnapshot {
    pub items: Vec<MemoryRecallItem>,
    pub diagnostics: Vec<String>,
    pub truncated: bool,
}

impl MemoryRecallSnapshot {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryActiveRecallMode {
    Disabled,
    DeterministicOnly,
    Hybrid,
    StrictDebug,
}

impl Default for MemoryActiveRecallMode {
    fn default() -> Self {
        Self::Hybrid
    }
}

impl MemoryActiveRecallMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::DeterministicOnly => "deterministic_only",
            Self::Hybrid => "hybrid",
            Self::StrictDebug => "strict_debug",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryActiveRecallConfig {
    pub mode: MemoryActiveRecallMode,
    pub timeout_ms: u64,
    pub max_queries: usize,
    pub top_k_per_query: u32,
    pub max_prompt_chars: usize,
    pub deterministic_sufficient_min_items: usize,
    pub deterministic_sufficient_min_chars: usize,
}

impl Default for MemoryActiveRecallConfig {
    fn default() -> Self {
        Self {
            mode: MemoryActiveRecallMode::Hybrid,
            timeout_ms: 800,
            max_queries: 3,
            top_k_per_query: 5,
            max_prompt_chars: 1_500,
            deterministic_sufficient_min_items: 1,
            deterministic_sufficient_min_chars: 600,
        }
    }
}

impl MemoryActiveRecallConfig {
    pub fn normalized(&self) -> Self {
        Self {
            mode: self.mode,
            timeout_ms: self.timeout_ms.max(1),
            max_queries: self.max_queries.max(1),
            top_k_per_query: self.top_k_per_query.max(1),
            max_prompt_chars: self.max_prompt_chars.max(1),
            deterministic_sufficient_min_items: self.deterministic_sufficient_min_items.max(1),
            deterministic_sufficient_min_chars: self.deterministic_sufficient_min_chars.max(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryLoopConfig {
    pub active_recall: MemoryActiveRecallConfig,
}

impl MemoryLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            active_recall: self.active_recall.normalized(),
        }
    }
}

#[derive(Clone, Default)]
pub struct MemoryToolMaterialization {
    pub bundles: Vec<pioneer_tools::ToolExtensionBundle>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryRecallPolicy {
    Allow,
    Disabled,
}

impl MemoryRecallPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Disabled => "disabled",
        }
    }

    fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("recall")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPromptPolicy {
    Full,
    ReadOnly,
    ForgetOnly,
    Disabled,
}

impl MemoryPromptPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::ReadOnly => "read_only",
            Self::ForgetOnly => "forget_only",
            Self::Disabled => "disabled",
        }
    }

    fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "full" => Ok(Self::Full),
            "read_only" => Ok(Self::ReadOnly),
            "forget_only" => Ok(Self::ForgetOnly),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("prompt")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryReadToolPolicy {
    Allow,
    ForgetOnly,
    Disabled,
}

impl MemoryReadToolPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::ForgetOnly => "forget_only",
            Self::Disabled => "disabled",
        }
    }

    fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "forget_only" => Ok(Self::ForgetOnly),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("read_tools")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryMutationToolPolicy {
    Allow,
    Disabled,
}

impl MemoryMutationToolPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Disabled => "disabled",
        }
    }

    fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("mutation_tool")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryExtractionPolicy {
    Allow,
    Disabled,
}

impl MemoryExtractionPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Disabled => "disabled",
        }
    }

    fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value(
                "post_turn_extraction",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryActiveContextPolicy {
    Allow,
    Disabled,
}

impl MemoryActiveContextPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Disabled => "disabled",
        }
    }

    fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "allow" => Ok(Self::Allow),
            "disabled" => Ok(Self::Disabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("active_memory")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPolicySource {
    StructuredOverride,
    PreMemoryClassifier,
    DefaultFallback,
}

impl MemoryPolicySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StructuredOverride => "structured_override",
            Self::PreMemoryClassifier => "pre_memory_classifier",
            Self::DefaultFallback => "default_fallback",
        }
    }

    fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "structured_override" => Ok(Self::StructuredOverride),
            "pre_memory_classifier" => Ok(Self::PreMemoryClassifier),
            "default_fallback" => Ok(Self::DefaultFallback),
            _ => Err(MemoryPolicyDecodeError::invalid_value("source")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPolicyReasonCode {
    DefaultAllowRead,
    MemoryNoUse,
    MemoryNoSave,
    ExplicitRemember,
    ExplicitForget,
    StructuredOverride,
    ClassifierUnavailable,
    ClassifierInvalidJson,
    ClassifierLowConfidence,
    ChatModeDisabled,
    MemoryRuntimeDisabled,
}

impl MemoryPolicyReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DefaultAllowRead => "default_allow_read",
            Self::MemoryNoUse => "memory_no_use",
            Self::MemoryNoSave => "memory_no_save",
            Self::ExplicitRemember => "explicit_remember",
            Self::ExplicitForget => "explicit_forget",
            Self::StructuredOverride => "structured_override",
            Self::ClassifierUnavailable => "classifier_unavailable",
            Self::ClassifierInvalidJson => "classifier_invalid_json",
            Self::ClassifierLowConfidence => "classifier_low_confidence",
            Self::ChatModeDisabled => "chat_mode_disabled",
            Self::MemoryRuntimeDisabled => "memory_runtime_disabled",
        }
    }

    pub(crate) fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "default_allow_read" => Ok(Self::DefaultAllowRead),
            "memory_no_use" => Ok(Self::MemoryNoUse),
            "memory_no_save" => Ok(Self::MemoryNoSave),
            "explicit_remember" => Ok(Self::ExplicitRemember),
            "explicit_forget" => Ok(Self::ExplicitForget),
            "structured_override" => Ok(Self::StructuredOverride),
            "classifier_unavailable" => Ok(Self::ClassifierUnavailable),
            "classifier_invalid_json" => Ok(Self::ClassifierInvalidJson),
            "classifier_low_confidence" => Ok(Self::ClassifierLowConfidence),
            "chat_mode_disabled" => Ok(Self::ChatModeDisabled),
            "memory_runtime_disabled" => Ok(Self::MemoryRuntimeDisabled),
            _ => Err(MemoryPolicyDecodeError::invalid_value("reason_code")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryClassifierFallbackPolicy {
    DefaultAllow,
    StrictDeny,
    AllowReadOnly,
}

impl MemoryClassifierFallbackPolicy {
    fn from_str(value: &str) -> Result<Self, MemoryPolicyDecodeError> {
        match value {
            "default_allow" => Ok(Self::DefaultAllow),
            "strict_deny" => Ok(Self::StrictDeny),
            "allow_read_only" => Ok(Self::AllowReadOnly),
            _ => Err(MemoryPolicyDecodeError::invalid_value("fallback")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPolicyDecodeError {
    field: &'static str,
    reason: &'static str,
}

impl MemoryPolicyDecodeError {
    fn missing(field: &'static str) -> Self {
        Self {
            field,
            reason: "missing",
        }
    }

    fn invalid_type(field: &'static str) -> Self {
        Self {
            field,
            reason: "invalid_type",
        }
    }

    fn invalid_value(field: &'static str) -> Self {
        Self {
            field,
            reason: "invalid_value",
        }
    }
}

impl fmt::Display for MemoryPolicyDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.field, self.reason)
    }
}

impl std::error::Error for MemoryPolicyDecodeError {}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryTurnPolicy {
    pub recall: MemoryRecallPolicy,
    pub prompt: MemoryPromptPolicy,
    pub read_tools: MemoryReadToolPolicy,
    pub remember_tool: MemoryMutationToolPolicy,
    pub forget_tool: MemoryMutationToolPolicy,
    pub post_turn_extraction: MemoryExtractionPolicy,
    pub active_memory: MemoryActiveContextPolicy,
    pub explicit_remember: bool,
    pub explicit_forget: bool,
    pub forget_target_hint: Option<String>,
    pub reason_code: MemoryPolicyReasonCode,
    pub confidence: f32,
    pub source: MemoryPolicySource,
    pub detected_language: Option<String>,
    pub diagnostics: Vec<String>,
}

impl MemoryTurnPolicy {
    pub fn normal_default_allow() -> Self {
        Self {
            recall: MemoryRecallPolicy::Allow,
            prompt: MemoryPromptPolicy::Full,
            read_tools: MemoryReadToolPolicy::Allow,
            remember_tool: MemoryMutationToolPolicy::Allow,
            forget_tool: MemoryMutationToolPolicy::Allow,
            post_turn_extraction: MemoryExtractionPolicy::Disabled,
            active_memory: MemoryActiveContextPolicy::Allow,
            explicit_remember: false,
            explicit_forget: false,
            forget_target_hint: None,
            reason_code: MemoryPolicyReasonCode::DefaultAllowRead,
            confidence: 1.0,
            source: MemoryPolicySource::PreMemoryClassifier,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn proactive_write_allowed() -> Self {
        Self::normal_default_allow()
    }

    pub fn explicit_remember() -> Self {
        Self {
            explicit_remember: true,
            reason_code: MemoryPolicyReasonCode::ExplicitRemember,
            ..Self::normal_default_allow()
        }
    }

    pub fn explicit_forget(forget_target_hint: Option<String>) -> Self {
        Self {
            recall: MemoryRecallPolicy::Disabled,
            prompt: MemoryPromptPolicy::ForgetOnly,
            read_tools: MemoryReadToolPolicy::ForgetOnly,
            remember_tool: MemoryMutationToolPolicy::Disabled,
            forget_tool: MemoryMutationToolPolicy::Allow,
            post_turn_extraction: MemoryExtractionPolicy::Disabled,
            active_memory: MemoryActiveContextPolicy::Disabled,
            explicit_remember: false,
            explicit_forget: true,
            forget_target_hint,
            reason_code: MemoryPolicyReasonCode::ExplicitForget,
            confidence: 1.0,
            source: MemoryPolicySource::PreMemoryClassifier,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn no_use() -> Self {
        Self {
            recall: MemoryRecallPolicy::Disabled,
            prompt: MemoryPromptPolicy::Disabled,
            read_tools: MemoryReadToolPolicy::Disabled,
            remember_tool: MemoryMutationToolPolicy::Disabled,
            forget_tool: MemoryMutationToolPolicy::Disabled,
            post_turn_extraction: MemoryExtractionPolicy::Disabled,
            active_memory: MemoryActiveContextPolicy::Disabled,
            explicit_remember: false,
            explicit_forget: false,
            forget_target_hint: None,
            reason_code: MemoryPolicyReasonCode::MemoryNoUse,
            confidence: 1.0,
            source: MemoryPolicySource::PreMemoryClassifier,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn no_save() -> Self {
        Self {
            recall: MemoryRecallPolicy::Allow,
            prompt: MemoryPromptPolicy::ReadOnly,
            read_tools: MemoryReadToolPolicy::Allow,
            remember_tool: MemoryMutationToolPolicy::Disabled,
            forget_tool: MemoryMutationToolPolicy::Allow,
            post_turn_extraction: MemoryExtractionPolicy::Disabled,
            active_memory: MemoryActiveContextPolicy::Allow,
            explicit_remember: false,
            explicit_forget: false,
            forget_target_hint: None,
            reason_code: MemoryPolicyReasonCode::MemoryNoSave,
            confidence: 1.0,
            source: MemoryPolicySource::PreMemoryClassifier,
            detected_language: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn default_allow_fallback(reason_code: MemoryPolicyReasonCode) -> Self {
        Self::normal_default_allow().with_source(
            MemoryPolicySource::DefaultFallback,
            reason_code,
            0.0,
        )
    }

    pub fn strict_deny_fallback(reason_code: MemoryPolicyReasonCode) -> Self {
        Self::no_use().with_source(MemoryPolicySource::DefaultFallback, reason_code, 0.0)
    }

    pub fn allow_read_only_fallback(reason_code: MemoryPolicyReasonCode) -> Self {
        Self::no_save().with_source(MemoryPolicySource::DefaultFallback, reason_code, 0.0)
    }

    pub fn with_source(
        mut self,
        source: MemoryPolicySource,
        reason_code: MemoryPolicyReasonCode,
        confidence: f32,
    ) -> Self {
        self.source = source;
        self.reason_code = reason_code;
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    pub fn with_diagnostic(mut self, diagnostic: impl Into<String>) -> Self {
        self.diagnostics.push(diagnostic.into());
        self
    }

    pub fn with_detected_language(mut self, language: Option<String>) -> Self {
        self.detected_language = language.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        });
        self
    }

    pub(crate) fn allow_pre_turn_recall(&self) -> bool {
        self.recall == MemoryRecallPolicy::Allow
    }

    pub(crate) fn allow_memory_prompt(&self) -> bool {
        self.prompt != MemoryPromptPolicy::Disabled
    }

    pub(crate) fn allows_any_memory_tool(&self) -> bool {
        self.allows_memory_tool(MEMORY_SEARCH_TOOL)
            || self.allows_memory_tool(MEMORY_GET_TOOL)
            || self.allows_memory_tool(MEMORY_REMEMBER_TOOL)
            || self.allows_memory_tool(MEMORY_FORGET_TOOL)
    }

    pub(crate) fn allows_memory_tool(&self, tool_name: &str) -> bool {
        match tool_name {
            MEMORY_SEARCH_TOOL | MEMORY_GET_TOOL => {
                matches!(
                    self.read_tools,
                    MemoryReadToolPolicy::Allow | MemoryReadToolPolicy::ForgetOnly
                )
            }
            MEMORY_REMEMBER_TOOL => self.remember_tool == MemoryMutationToolPolicy::Allow,
            MEMORY_FORGET_TOOL => self.forget_tool == MemoryMutationToolPolicy::Allow,
            _ => false,
        }
    }

    pub(crate) fn recall_prompt_policy(&self) -> Option<MemoryRecallPromptPolicy> {
        match self.prompt {
            MemoryPromptPolicy::Full => Some(MemoryRecallPromptPolicy::Full),
            MemoryPromptPolicy::ReadOnly => Some(MemoryRecallPromptPolicy::ReadOnly),
            MemoryPromptPolicy::ForgetOnly => Some(MemoryRecallPromptPolicy::ForgetOnly),
            MemoryPromptPolicy::Disabled => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryTurnPolicyContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub mode: ThreadMode,
    pub input_text: String,
    pub model: Option<String>,
    pub model_provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryTurnPolicyOverride {
    pub policy: MemoryTurnPolicy,
}

impl MemoryTurnPolicyOverride {
    pub fn new(policy: MemoryTurnPolicy) -> Self {
        Self { policy }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryTurnPolicyRequest {
    pub default_policy: MemoryTurnPolicy,
    pub structured_override: Option<MemoryTurnPolicyOverride>,
    pub classifier_enabled: bool,
    pub fallback: MemoryClassifierFallbackPolicy,
}

impl Default for MemoryTurnPolicyRequest {
    fn default() -> Self {
        Self {
            default_policy: MemoryTurnPolicy::normal_default_allow(),
            structured_override: None,
            classifier_enabled: true,
            fallback: MemoryClassifierFallbackPolicy::DefaultAllow,
        }
    }
}

#[async_trait::async_trait]
pub trait AgentMemoryProvider: Send + Sync {
    async fn recall_memory(
        &self,
        context: MemoryTurnContext,
        request: MemoryRecallRequest,
    ) -> Result<MemoryRecallSnapshot, String>;

    async fn materialize_memory_tools(
        &self,
        context: MemoryTurnContext,
    ) -> Result<MemoryToolMaterialization, String>;
}

#[async_trait::async_trait]
pub trait AgentMemoryTurnPolicyProvider: Send + Sync {
    async fn resolve_memory_turn_policy(
        &self,
        context: MemoryTurnPolicyContext,
        request: MemoryTurnPolicyRequest,
    ) -> Result<MemoryTurnPolicy, String>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryActiveRecallDecisionContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub mode: ThreadMode,
    pub input_text_preview: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryActiveRecallDecisionRequest {
    pub deterministic_context_count: usize,
    pub deterministic_context_chars: usize,
    pub deterministic_memory_ids: Vec<String>,
    pub config_mode: MemoryActiveRecallMode,
}

#[async_trait::async_trait]
pub trait AgentActiveMemoryDecisionProvider: Send + Sync {
    async fn resolve_active_memory_decision_json(
        &self,
        context: MemoryActiveRecallDecisionContext,
        request: MemoryActiveRecallDecisionRequest,
    ) -> Result<String, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveMemoryDecisionStatus {
    Skip,
    Run,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveMemoryDecisionReasonCode {
    PolicyDisabled,
    ConfigDisabled,
    DeterministicOnly,
    DeterministicSufficient,
    TrivialSelfContained,
    MemoryLikely,
    StrictDebug,
    ProviderRun,
    ProviderSkip,
    ProviderUncertain,
}

impl ActiveMemoryDecisionReasonCode {
    fn as_str(self) -> &'static str {
        match self {
            Self::PolicyDisabled => "policy_disabled",
            Self::ConfigDisabled => "config_disabled",
            Self::DeterministicOnly => "deterministic_only",
            Self::DeterministicSufficient => "deterministic_sufficient",
            Self::TrivialSelfContained => "trivial_self_contained",
            Self::MemoryLikely => "memory_likely",
            Self::StrictDebug => "strict_debug",
            Self::ProviderRun => "provider_run",
            Self::ProviderSkip => "provider_skip",
            Self::ProviderUncertain => "provider_uncertain",
        }
    }

    fn diagnostic_code(self) -> &'static str {
        match self {
            Self::PolicyDisabled => "memory.active_recall.policy_disabled",
            Self::ConfigDisabled => "memory.active_recall.config_disabled",
            Self::DeterministicOnly => "memory.active_recall.deterministic_only",
            Self::DeterministicSufficient => "memory.active_recall.deterministic_sufficient",
            Self::TrivialSelfContained => "memory.active_recall.skipped",
            Self::MemoryLikely | Self::StrictDebug | Self::ProviderRun => {
                "memory.active_recall.started"
            }
            Self::ProviderSkip => "memory.active_recall.skipped",
            Self::ProviderUncertain => "memory.active_recall.uncertain",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ActiveMemoryDecision {
    status: ActiveMemoryDecisionStatus,
    reason_code: ActiveMemoryDecisionReasonCode,
    confidence: f32,
    query_hints: Vec<String>,
    diagnostics: Vec<String>,
}

#[derive(Clone)]
struct MemoryHookTurnState {
    context: MemoryTurnContext,
}

#[derive(Default)]
struct MemoryHookTurnStateStore {
    states: Mutex<BTreeMap<String, MemoryHookTurnState>>,
}

impl MemoryHookTurnStateStore {
    fn set_turn_context(&self, context: MemoryTurnContext) {
        if let Ok(mut states) = self.states.lock() {
            states.insert(
                memory_hook_state_key(
                    context.workspace_id.as_str(),
                    context.thread_id.as_str(),
                    context.turn_id.as_str(),
                ),
                MemoryHookTurnState { context },
            );
        }
    }

    fn state(&self, request: &HookHandlerRequest) -> Option<MemoryHookTurnState> {
        let workspace_id = request.context.workspace_id.as_ref()?.as_str();
        let thread_id = request.context.thread_id.as_ref()?.as_str();
        let turn_id = request.context.turn_id.as_ref()?.as_str();
        self.states.lock().ok().and_then(|states| {
            states
                .get(&memory_hook_state_key(workspace_id, thread_id, turn_id))
                .cloned()
        })
    }
}

fn memory_hook_state_key(workspace_id: &str, thread_id: &str, turn_id: &str) -> String {
    format!("{workspace_id}\n{thread_id}\n{turn_id}")
}

pub(crate) fn install_memory_hooks(
    runtime: &Arc<HookRuntime>,
    memory_provider: Arc<dyn AgentMemoryProvider>,
    policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    tool_bundle_artifacts: Arc<AgentToolBundleArtifactStore>,
    memory_config: MemoryLoopConfig,
) -> Result<(), HookRegistryError> {
    let memory_config = memory_config.normalized();
    let active_recall_config = memory_config.active_recall.clone();
    let state = Arc::new(MemoryHookTurnStateStore::default());
    register_memory_hook_handler(
        runtime,
        Arc::new(MemoryPolicyClassifierHook {
            policy_provider,
            state: state.clone(),
        }),
        MEMORY_POLICY_CLASSIFIER_SUBSCRIPTION_ID,
        HookPhase::TurnPrePolicy,
        0,
    )?;
    register_memory_hook_handler(
        runtime,
        Arc::new(MemoryDeterministicRecallHook {
            memory_provider: memory_provider.clone(),
        }),
        MEMORY_DETERMINISTIC_RECALL_SUBSCRIPTION_ID,
        HookPhase::TurnPrePromptContext,
        0,
    )?;
    register_memory_hook_handler_with_options(
        runtime,
        Arc::new(ActiveMemoryRecallHook {
            memory_provider: memory_provider.clone(),
            decision_provider: None,
            config: active_recall_config.clone(),
        }),
        MEMORY_ACTIVE_RECALL_SUBSCRIPTION_ID,
        HookPhase::TurnPrePromptContext,
        -10,
        HookExecutionPolicy {
            await_policy: HookAwaitPolicy::Deadline,
            timeout_ms: Some(active_recall_config.timeout_ms),
            max_parallelism: None,
        },
        HookSubscriptionDependencies::new(
            [
                HookSubscriptionId::new(MEMORY_DETERMINISTIC_RECALL_SUBSCRIPTION_ID)
                    .expect("static subscription id is valid"),
            ],
            [],
        ),
        HookSubscriptionVisibility::Internal,
    )?;
    register_memory_hook_handler(
        runtime,
        Arc::new(MemoryToolBundleHook {
            memory_provider: memory_provider.clone(),
            state: state.clone(),
            tool_bundle_artifacts,
        }),
        MEMORY_TOOL_BUNDLE_SUBSCRIPTION_ID,
        HookPhase::TurnPreToolMaterialization,
        0,
    )?;
    register_memory_hook_handler(
        runtime,
        Arc::new(MemoryPromptContractHook),
        MEMORY_PROMPT_CONTRACT_SUBSCRIPTION_ID,
        HookPhase::TurnPrePromptCompile,
        0,
    )?;
    Ok(())
}

fn register_memory_hook_handler(
    runtime: &Arc<HookRuntime>,
    handler: Arc<dyn HookHandler>,
    subscription_id: &'static str,
    phase: HookPhase,
    priority: i32,
) -> Result<(), HookRegistryError> {
    register_memory_hook_handler_with_options(
        runtime,
        handler,
        subscription_id,
        phase,
        priority,
        HookExecutionPolicy {
            await_policy: HookAwaitPolicy::Blocking,
            timeout_ms: None,
            max_parallelism: None,
        },
        HookSubscriptionDependencies::default(),
        HookSubscriptionVisibility::Internal,
    )
}

fn register_memory_hook_handler_with_options(
    runtime: &Arc<HookRuntime>,
    handler: Arc<dyn HookHandler>,
    subscription_id: &'static str,
    phase: HookPhase,
    priority: i32,
    execution_policy: HookExecutionPolicy,
    dependencies: HookSubscriptionDependencies,
    visibility: HookSubscriptionVisibility,
) -> Result<(), HookRegistryError> {
    let hook_id = handler.id();
    if !runtime.handlers().contains_handler(&hook_id)? {
        runtime.handlers().register_handler(handler)?;
    }

    let subscription_id =
        HookSubscriptionId::new(subscription_id).expect("static subscription id is valid");
    if runtime
        .subscriptions()
        .get_subscription(&subscription_id)?
        .is_none()
    {
        runtime.subscriptions().register_subscription(
            runtime.handlers().as_ref(),
            HookSubscription::new(subscription_id, hook_id, phase)
                .with_priority(priority)
                .with_dependencies(dependencies)
                .with_execution_policy(execution_policy)
                .with_failure_policy(HookFailurePolicy::BestEffort)
                .with_visibility(visibility),
        )?;
    }
    Ok(())
}

struct MemoryPolicyClassifierHook {
    policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    state: Arc<MemoryHookTurnStateStore>,
}

struct MemoryToolBundleHook {
    memory_provider: Arc<dyn AgentMemoryProvider>,
    state: Arc<MemoryHookTurnStateStore>,
    tool_bundle_artifacts: Arc<AgentToolBundleArtifactStore>,
}

struct MemoryDeterministicRecallHook {
    memory_provider: Arc<dyn AgentMemoryProvider>,
}

struct ActiveMemoryRecallHook {
    memory_provider: Arc<dyn AgentMemoryProvider>,
    decision_provider: Option<Arc<dyn AgentActiveMemoryDecisionProvider>>,
    config: MemoryActiveRecallConfig,
}

struct MemoryPromptContractHook;

#[async_trait::async_trait]
impl HookHandler for MemoryPolicyClassifierHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_POLICY_CLASSIFIER_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePolicy]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_policy_classifier_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_pre_policy_input(&request)?;
        let workspace_id = required_context_id(
            request.context.workspace_id.as_ref().map(|id| id.as_str()),
            "workspace_id",
        )?;
        let thread_id = required_context_id(
            request.context.thread_id.as_ref().map(|id| id.as_str()),
            "thread_id",
        )?;
        let turn_id = required_context_id(
            request.context.turn_id.as_ref().map(|id| id.as_str()),
            "turn_id",
        )?;

        let context = MemoryTurnPolicyContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            mode: ThreadMode::Agent,
            input_text: input.input_text.clone(),
            model: input.model.clone(),
            model_provider: input.model_provider.clone(),
        };
        let (policy_request, request_diagnostics) =
            memory_turn_policy_request_from_metadata(&request.context.metadata);
        let policy =
            resolve_memory_turn_policy(self.policy_provider.as_ref(), context, policy_request)
                .await;
        let turn_context = MemoryTurnContext {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            mode: ThreadMode::Agent,
            input_text: input.input_text.clone(),
            task_id: None,
            agent_id: None,
        };
        self.state.set_turn_context(turn_context);

        let mut response = HookHandlerResponse::default();
        response.diagnostics.extend(hook_diagnostics_from_strings(
            request_diagnostics.as_slice(),
        ));
        response
            .diagnostics
            .extend(hook_diagnostics_from_strings(policy.diagnostics.as_slice()));
        response
            .contributions
            .push(HookContribution::Policy(memory_policy_contribution(
                &policy,
            )));
        Ok(response)
    }
}

#[async_trait::async_trait]
impl HookHandler for MemoryToolBundleHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_TOOL_BUNDLE_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPreToolMaterialization]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_tool_bundle_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let Some(state) = self.state.state(&request) else {
            return Ok(memory_missing_state_response(MEMORY_TOOL_BUNDLE_HOOK_ID));
        };
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory tool bundle skipped: policy_decode_failed {error}"),
                ));
                return Ok(response);
            }
            None => {
                return Ok(memory_missing_policy_response(MEMORY_TOOL_BUNDLE_HOOK_ID));
            }
        };
        if !turn_pre_tool_materialization_allows_tools(&request) {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.tools_omitted",
                "memory tool bundle skipped: provider_tool_calling=false",
            ));
            return Ok(response);
        }
        if !policy.allows_any_memory_tool() {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.tools_omitted",
                format!(
                    "memory tool bundle skipped: no tools allowed by policy source={} reason={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str()
                ),
            ));
            return Ok(response);
        }

        let mut materialization = match self
            .memory_provider
            .materialize_memory_tools(state.context.clone())
            .await
        {
            Ok(materialization) => filter_memory_tool_materialization(materialization, &policy),
            Err(error) => {
                let mut response = HookHandlerResponse::default();
                let _ = error;
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.tools_failed",
                    "memory tool bundle materialization failed",
                ));
                return Ok(response);
            }
        };

        let mut response = HookHandlerResponse::default();
        response.diagnostics.extend(hook_diagnostics_from_strings(
            materialization.diagnostics.as_slice(),
        ));
        if materialization.bundles.is_empty() {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.tools_omitted",
                format!(
                    "memory tool bundle skipped: materializer returned no exposed tools source={} reason={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str()
                ),
            ));
            return Ok(response);
        }
        for (index, bundle) in materialization.bundles.drain(..).enumerate() {
            let bundle_id =
                HookToolBundleId::new(format!("{MEMORY_TOOL_BUNDLE_ID_PREFIX}.{index}"))
                    .expect("static bundle id is valid");
            self.tool_bundle_artifacts.insert(
                state.context.turn_id.clone(),
                bundle_id.clone(),
                bundle.clone(),
            );
            response.contributions.push(HookContribution::ToolBundle(
                memory_tool_bundle_contribution(index, bundle_id, &bundle, &policy),
            ));
        }
        Ok(response)
    }
}

#[async_trait::async_trait]
impl HookHandler for MemoryDeterministicRecallHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_DETERMINISTIC_RECALL_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePromptContext]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_deterministic_recall_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_pre_prompt_context_input(&request)?;
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory deterministic recall skipped: policy_decode_failed {error}"),
                ));
                return Ok(response);
            }
            None => {
                return Ok(memory_missing_policy_response(
                    MEMORY_DETERMINISTIC_RECALL_HOOK_ID,
                ));
            }
        };
        if !policy.allow_pre_turn_recall() {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.recall_omitted",
                format!(
                    "memory deterministic recall skipped: source={} reason={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str()
                ),
            ));
            return Ok(response);
        }

        let context = memory_turn_context_from_prompt_context_request(&request, input)?;
        let mut response = HookHandlerResponse::default();
        let recall_snapshot = match self
            .memory_provider
            .recall_memory(
                context.clone(),
                memory_recall_request(context.input_text.as_str()),
            )
            .await
        {
            Ok(snapshot) => {
                response.diagnostics.extend(hook_diagnostics_from_strings(
                    snapshot.diagnostics.as_slice(),
                ));
                snapshot
            }
            Err(error) => {
                let _ = error;
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.recall_failed",
                    "memory deterministic recall failed",
                ));
                return Ok(response);
            }
        };

        if let Some(contribution) = memory_recall_prompt_context_contribution(recall_snapshot) {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.recall_context_contributed",
                "memory deterministic recall contributed prompt context",
            ));
            response
                .contributions
                .push(HookContribution::PromptContext(contribution));
        }
        Ok(response)
    }
}

#[async_trait::async_trait]
impl HookHandler for ActiveMemoryRecallHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_ACTIVE_RECALL_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePromptContext]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_active_recall_capabilities(self.decision_provider.is_some())
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_pre_prompt_context_input(&request)?;
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory active recall skipped: policy_decode_failed {error}"),
                ));
                return Ok(response);
            }
            None => return Ok(memory_missing_policy_response(MEMORY_ACTIVE_RECALL_HOOK_ID)),
        };
        let config = self.config.normalized();
        let deterministic =
            deterministic_recall_context_summary(&request.prompt_context_set, &config);
        let context = memory_turn_context_from_prompt_context_request(&request, input)?;
        let mut response = HookHandlerResponse::default();
        let decision = resolve_active_memory_decision(
            self.decision_provider.as_ref(),
            &context,
            input,
            &policy,
            &config,
            &deterministic,
        )
        .await;
        response.diagnostics.extend(hook_diagnostics_from_strings(
            decision.diagnostics.as_slice(),
        ));
        response
            .diagnostics
            .push(active_memory_decision_observability_diagnostic(
                &decision,
                &deterministic,
            ));

        if decision.status != ActiveMemoryDecisionStatus::Run {
            response.diagnostics.push(memory_safe_info_diagnostic(
                decision.reason_code.diagnostic_code(),
                format!(
                    "memory active recall skipped: reason={:?} confidence={:.2}",
                    decision.reason_code, decision.confidence
                ),
            ));
            return Ok(response);
        }

        let queries = active_memory_query_plan(input.input_text.as_str(), &decision, &config);
        if queries.is_empty() {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.no_query",
                "memory active recall skipped: no bounded query available",
            ));
            return Ok(response);
        }

        let mut active_items = Vec::new();
        let mut active_truncated = false;
        for query in queries {
            match self
                .memory_provider
                .recall_memory(
                    context.clone(),
                    MemoryRecallRequest {
                        query,
                        categories: Vec::new(),
                        top_k: Some(config.top_k_per_query),
                        max_chars: Some(config.max_prompt_chars),
                    },
                )
                .await
            {
                Ok(snapshot) => {
                    response.diagnostics.extend(hook_diagnostics_from_strings(
                        snapshot.diagnostics.as_slice(),
                    ));
                    active_truncated |= snapshot.truncated;
                    active_items.extend(snapshot.items);
                }
                Err(error) => {
                    let _ = error;
                    response.diagnostics.push(memory_safe_warning_diagnostic(
                        "memory.active_recall.failed",
                        "memory active recall failed",
                    ));
                    return Ok(response);
                }
            }
        }

        let active_dedup = dedup_active_recall_items_with_lines(
            active_items,
            &deterministic.memory_ids,
            &deterministic.rendered_line_fingerprints,
        );
        response
            .diagnostics
            .push(active_memory_dedup_observability_diagnostic(
                &deterministic,
                &active_dedup,
            ));
        if active_dedup.items.is_empty() {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.no_hits",
                "memory active recall returned no non-duplicate memory context",
            ));
            return Ok(response);
        }

        if let Some(contribution) = memory_active_recall_prompt_context_contribution(
            active_dedup.items,
            active_truncated,
            &config,
        ) {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.active_recall.context_contributed",
                "memory active recall contributed prompt context",
            ));
            response
                .contributions
                .push(HookContribution::PromptContext(contribution));
        }
        Ok(response)
    }
}

#[async_trait::async_trait]
impl HookHandler for MemoryPromptContractHook {
    fn id(&self) -> HookId {
        HookId::new(MEMORY_PROMPT_CONTRACT_HOOK_ID).expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePromptCompile]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_prompt_contract_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let input = turn_pre_prompt_compile_input(&request)?;
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_safe_warning_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory prompt contract skipped: policy_decode_failed {error}"),
                ));
                return Ok(response);
            }
            None => {
                return Ok(memory_missing_policy_response(
                    MEMORY_PROMPT_CONTRACT_HOOK_ID,
                ));
            }
        };
        if !input.provider_tool_calling {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.prompt_omitted",
                "memory prompt contract skipped: provider_tool_calling=false",
            ));
            return Ok(response);
        }
        if !policy.allow_memory_prompt() {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.prompt_omitted",
                format!(
                    "memory prompt contract skipped: source={} reason={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str()
                ),
            ));
            return Ok(response);
        }

        let available_tool_names = memory_tool_names_from_prompt_compile_input(input);
        if available_tool_names.is_empty() {
            let mut response = HookHandlerResponse::default();
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.prompt_omitted",
                "memory prompt contract skipped: no visible memory tools",
            ));
            return Ok(response);
        }

        let Some(prompt_policy) = policy.recall_prompt_policy() else {
            return Ok(HookHandlerResponse::default());
        };
        let recall_context = memory_recall_context_from_prompt_context_set(
            &request.prompt_context_set,
            prompt_policy,
        );
        let mut response = HookHandlerResponse::default();
        if let Some(contribution) = memory_recall_prompt_section_contribution_from_context(
            available_tool_names,
            prompt_policy,
            recall_context.clone(),
            recall_context.truncated,
        ) {
            response.diagnostics.push(memory_safe_info_diagnostic(
                "memory.prompt_rendered",
                format!(
                    "memory prompt contract rendered: source={} reason={} recalled_contexts={}",
                    policy.source.as_str(),
                    policy.reason_code.as_str(),
                    recall_context.count
                ),
            ));
            response
                .diagnostics
                .push(memory_prompt_recall_dedup_diagnostic(&recall_context));
            response.contributions.push(contribution);
        }
        Ok(response)
    }
}

fn memory_policy_classifier_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("call_provider").expect("static capability is valid"),
        HookCapability::new("contribute_policy").expect("static capability is valid"),
    ])
}

fn memory_tool_bundle_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("write_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_tool_bundle").expect("static capability is valid"),
    ])
}

fn memory_deterministic_recall_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_context").expect("static capability is valid"),
    ])
}

fn memory_active_recall_capabilities(provider_enabled: bool) -> HookCapabilities {
    let mut capabilities = vec![
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_context").expect("static capability is valid"),
    ];
    if provider_enabled {
        capabilities
            .push(HookCapability::new("call_provider").expect("static capability is valid"));
    }
    HookCapabilities::new(capabilities)
}

fn memory_prompt_contract_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_section").expect("static capability is valid"),
    ])
}

fn turn_pre_policy_input(request: &HookHandlerRequest) -> HookResult<&TurnPrePolicyHookInput> {
    match &request.input.payload {
        HookInputPayload::TurnPrePolicy(input) => Ok(input),
        _ => Err(memory_hook_error(
            "memory.invalid_input",
            "memory policy hook expected turn pre-policy input",
        )),
    }
}

fn turn_pre_prompt_context_input(
    request: &HookHandlerRequest,
) -> HookResult<&TurnPrePromptContextHookInput> {
    match &request.input.payload {
        HookInputPayload::TurnPrePromptContext(input) => Ok(input),
        _ => Err(memory_hook_error(
            "memory.invalid_input",
            "memory deterministic recall hook expected turn pre-prompt-context input",
        )),
    }
}

fn turn_pre_tool_materialization_allows_tools(request: &HookHandlerRequest) -> bool {
    match &request.input.payload {
        HookInputPayload::TurnPreToolMaterialization(input) => input.provider_tool_calling,
        _ => false,
    }
}

fn turn_pre_prompt_compile_input(
    request: &HookHandlerRequest,
) -> HookResult<&TurnPrePromptCompileHookInput> {
    match &request.input.payload {
        HookInputPayload::TurnPrePromptCompile(input) => Ok(input),
        _ => Err(memory_hook_error(
            "memory.invalid_input",
            "memory prompt contract hook expected turn pre-prompt-compile input",
        )),
    }
}

fn required_context_id<'a>(value: Option<&'a str>, field: &'static str) -> HookResult<&'a str> {
    value.ok_or_else(|| {
        memory_hook_error(
            "memory.missing_context",
            format!("memory hook request missing {field}"),
        )
    })
}

fn memory_policy_contribution(policy: &MemoryTurnPolicy) -> PolicyContribution {
    PolicyContribution {
        domain: memory_policy_domain(),
        key: memory_turn_policy_key(),
        value: memory_turn_policy_to_hook_value(policy),
        priority: 500,
        diagnostics: hook_diagnostics_from_strings(policy.diagnostics.as_slice()),
    }
}

pub(crate) fn memory_turn_policy_to_hook_value(policy: &MemoryTurnPolicy) -> HookValue {
    let mut object = BTreeMap::new();
    insert_policy_text(&mut object, "recall", policy.recall.as_str());
    insert_policy_text(&mut object, "prompt", policy.prompt.as_str());
    insert_policy_text(&mut object, "read_tools", policy.read_tools.as_str());
    insert_policy_text(&mut object, "remember_tool", policy.remember_tool.as_str());
    insert_policy_text(&mut object, "forget_tool", policy.forget_tool.as_str());
    insert_policy_text(
        &mut object,
        "post_turn_extraction",
        policy.post_turn_extraction.as_str(),
    );
    insert_policy_text(&mut object, "active_memory", policy.active_memory.as_str());
    insert_policy_bool(&mut object, "explicit_remember", policy.explicit_remember);
    insert_policy_bool(&mut object, "explicit_forget", policy.explicit_forget);
    insert_policy_text_optional(
        &mut object,
        "forget_target_hint",
        policy.forget_target_hint.as_deref(),
    );
    insert_policy_text(&mut object, "reason_code", policy.reason_code.as_str());
    object.insert(
        hook_metadata_key("confidence"),
        HookValue::F64(policy.confidence.clamp(0.0, 1.0) as f64),
    );
    insert_policy_text(&mut object, "source", policy.source.as_str());
    insert_policy_text_optional(
        &mut object,
        "detected_language",
        policy.detected_language.as_deref(),
    );
    if !policy.diagnostics.is_empty() {
        object.insert(
            hook_metadata_key("diagnostics_summary"),
            HookValue::List(
                policy
                    .diagnostics
                    .iter()
                    .map(|diagnostic| HookValue::Text(safe_memory_policy_diagnostic(diagnostic)))
                    .collect(),
            ),
        );
    }
    HookValue::Object(object)
}

pub(crate) fn memory_turn_policy_from_hook_value(
    value: &HookValue,
) -> Result<MemoryTurnPolicy, MemoryPolicyDecodeError> {
    let HookValue::Object(object) = value else {
        return Err(MemoryPolicyDecodeError::invalid_type("turn_policy"));
    };

    let mut policy = MemoryTurnPolicy {
        recall: MemoryRecallPolicy::from_str(required_text(object, "recall")?)?,
        prompt: MemoryPromptPolicy::from_str(required_text(object, "prompt")?)?,
        read_tools: MemoryReadToolPolicy::from_str(required_text(object, "read_tools")?)?,
        remember_tool: MemoryMutationToolPolicy::from_str(required_text(object, "remember_tool")?)?,
        forget_tool: MemoryMutationToolPolicy::from_str(required_text(object, "forget_tool")?)?,
        post_turn_extraction: MemoryExtractionPolicy::from_str(required_text(
            object,
            "post_turn_extraction",
        )?)?,
        active_memory: MemoryActiveContextPolicy::from_str(required_text(
            object,
            "active_memory",
        )?)?,
        explicit_remember: required_bool(object, "explicit_remember")?,
        explicit_forget: required_bool(object, "explicit_forget")?,
        forget_target_hint: optional_text(object, "forget_target_hint")?,
        reason_code: MemoryPolicyReasonCode::from_str(required_text(object, "reason_code")?)?,
        confidence: required_f32(object, "confidence")?.clamp(0.0, 1.0),
        source: MemoryPolicySource::from_str(required_text(object, "source")?)?,
        detected_language: optional_text(object, "detected_language")?,
        diagnostics: optional_text_list(object, "diagnostics_summary")?,
    };
    policy.detected_language = policy.detected_language.and_then(|language| {
        let trimmed = language.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    });
    Ok(policy)
}

pub(crate) fn memory_turn_policy_from_hook_policy_set(
    policy_set: &HookPolicySet,
) -> Option<Result<MemoryTurnPolicy, MemoryPolicyDecodeError>> {
    policy_set
        .get(&memory_policy_domain(), &memory_turn_policy_key())
        .map(|entry| memory_turn_policy_from_hook_value(&entry.value))
}

fn memory_policy_domain() -> HookDomain {
    HookDomain::new(MEMORY_POLICY_DOMAIN).expect("static domain is valid")
}

fn memory_turn_policy_key() -> HookPolicyKey {
    HookPolicyKey::new(MEMORY_TURN_POLICY_KEY).expect("static policy key is valid")
}

fn hook_metadata_key(key: &'static str) -> HookMetadataKey {
    HookMetadataKey::new(key).expect("static metadata key is valid")
}

fn insert_usize_metadata(object: &mut HookMetadata, key: &'static str, value: usize) {
    object.insert(
        hook_metadata_key(key),
        HookValue::I64(i64::try_from(value).unwrap_or(i64::MAX)),
    );
}

fn insert_policy_text(
    object: &mut BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
    value: &'static str,
) {
    object.insert(hook_metadata_key(key), HookValue::Text(value.to_owned()));
}

fn insert_policy_text_optional(
    object: &mut BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
    value: Option<&str>,
) {
    match value.and_then(|text| {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    }) {
        Some(value) => {
            object.insert(hook_metadata_key(key), HookValue::Text(value.to_owned()));
        }
        None => {
            object.insert(hook_metadata_key(key), HookValue::Null);
        }
    }
}

fn insert_policy_bool(
    object: &mut BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
    value: bool,
) {
    object.insert(hook_metadata_key(key), HookValue::Bool(value));
}

fn required_value<'a>(
    object: &'a BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<&'a HookValue, MemoryPolicyDecodeError> {
    object
        .get(&hook_metadata_key(key))
        .ok_or_else(|| MemoryPolicyDecodeError::missing(key))
}

fn required_text<'a>(
    object: &'a BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<&'a str, MemoryPolicyDecodeError> {
    match required_value(object, key)? {
        HookValue::Text(value) if !value.trim().is_empty() => Ok(value.trim()),
        _ => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

fn optional_text(
    object: &BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<Option<String>, MemoryPolicyDecodeError> {
    match object.get(&hook_metadata_key(key)) {
        None | Some(HookValue::Null) => Ok(None),
        Some(HookValue::Text(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_owned()))
            }
        }
        Some(_) => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

fn required_bool(
    object: &BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<bool, MemoryPolicyDecodeError> {
    match required_value(object, key)? {
        HookValue::Bool(value) => Ok(*value),
        _ => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

fn required_f32(
    object: &BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<f32, MemoryPolicyDecodeError> {
    match required_value(object, key)? {
        HookValue::F64(value) => Ok(*value as f32),
        HookValue::I64(value) => Ok(*value as f32),
        _ => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

fn optional_text_list(
    object: &BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<Vec<String>, MemoryPolicyDecodeError> {
    match object.get(&hook_metadata_key(key)) {
        None | Some(HookValue::Null) => Ok(Vec::new()),
        Some(HookValue::List(values)) => values
            .iter()
            .map(|value| match value {
                HookValue::Text(text) => Ok(safe_memory_policy_diagnostic(text)),
                _ => Err(MemoryPolicyDecodeError::invalid_type(key)),
            })
            .collect(),
        Some(_) => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

fn memory_tool_bundle_contribution(
    index: usize,
    bundle_id: HookToolBundleId,
    bundle: &ToolExtensionBundle,
    policy: &MemoryTurnPolicy,
) -> ToolBundleContribution {
    let tool_names = bundle
        .specs
        .iter()
        .filter_map(|configured| HookToolName::new(configured.spec.name.clone()).ok())
        .collect::<Vec<_>>();
    ToolBundleContribution {
        contribution_id: HookContributionId::new(format!(
            "{MEMORY_TOOL_BUNDLE_CONTRIBUTION_ID_PREFIX}.{index}"
        ))
        .expect("static contribution id is valid"),
        bundle_id,
        domain: HookDomain::new("memory").expect("static domain is valid"),
        priority: 100,
        diagnostics: vec![memory_safe_info_diagnostic(
            "memory.tools_exposed",
            format!(
                "memory tool bundle exposed: source={} reason={} tools={}",
                policy.source.as_str(),
                policy.reason_code.as_str(),
                hook_tool_names_csv(&tool_names)
            ),
        )],
        tool_names,
    }
}

fn memory_turn_context_from_prompt_context_request(
    request: &HookHandlerRequest,
    input: &TurnPrePromptContextHookInput,
) -> HookResult<MemoryTurnContext> {
    let workspace_id = required_context_id(
        request.context.workspace_id.as_ref().map(|id| id.as_str()),
        "workspace_id",
    )?;
    let thread_id = required_context_id(
        request.context.thread_id.as_ref().map(|id| id.as_str()),
        "thread_id",
    )?;
    let turn_id = required_context_id(
        request.context.turn_id.as_ref().map(|id| id.as_str()),
        "turn_id",
    )?;
    Ok(MemoryTurnContext {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        mode: ThreadMode::Agent,
        input_text: input.input_text.clone(),
        task_id: request
            .context
            .task_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
        agent_id: request
            .context
            .agent_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
    })
}

fn memory_recall_request(input_text: &str) -> MemoryRecallRequest {
    MemoryRecallRequest {
        query: input_text.to_owned(),
        categories: Vec::new(),
        top_k: Some(5),
        max_chars: Some(1_500),
    }
}

#[derive(Debug, Clone, Default)]
struct DeterministicRecallContextSummary {
    memory_ids: BTreeSet<String>,
    rendered_line_fingerprints: BTreeSet<String>,
    context_count: usize,
    context_chars: usize,
    sufficient: bool,
}

fn deterministic_recall_context_summary(
    prompt_context_set: &pioneer_hooks::HookPromptContextSet,
    config: &MemoryActiveRecallConfig,
) -> DeterministicRecallContextSummary {
    let mut summary = DeterministicRecallContextSummary::default();
    for entry in prompt_context_set.entries() {
        if entry.domain.as_str() != MEMORY_POLICY_DOMAIN
            || entry.contribution_id.as_str() != MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID
        {
            continue;
        }
        summary.context_count += 1;
        summary.context_chars += entry.content.as_str().chars().count();
        summary
            .rendered_line_fingerprints
            .extend(rendered_line_fingerprints(entry.content.as_str()));
        for source_ref in &entry.source_refs {
            if source_ref.kind.as_str() == "memory" {
                summary.memory_ids.insert(source_ref.id.as_str().to_owned());
            }
        }
    }
    summary.sufficient = !summary.memory_ids.is_empty()
        && (summary.memory_ids.len() >= config.deterministic_sufficient_min_items
            || summary.context_chars >= config.deterministic_sufficient_min_chars);
    summary
}

async fn resolve_active_memory_decision(
    provider: Option<&Arc<dyn AgentActiveMemoryDecisionProvider>>,
    context: &MemoryTurnContext,
    input: &TurnPrePromptContextHookInput,
    policy: &MemoryTurnPolicy,
    config: &MemoryActiveRecallConfig,
    deterministic: &DeterministicRecallContextSummary,
) -> ActiveMemoryDecision {
    if !policy.allow_pre_turn_recall()
        || policy.active_memory == MemoryActiveContextPolicy::Disabled
    {
        return ActiveMemoryDecision {
            status: ActiveMemoryDecisionStatus::Skip,
            reason_code: ActiveMemoryDecisionReasonCode::PolicyDisabled,
            confidence: policy.confidence,
            query_hints: Vec::new(),
            diagnostics: Vec::new(),
        };
    }
    match config.mode {
        MemoryActiveRecallMode::Disabled => {
            return ActiveMemoryDecision {
                status: ActiveMemoryDecisionStatus::Skip,
                reason_code: ActiveMemoryDecisionReasonCode::ConfigDisabled,
                confidence: 1.0,
                query_hints: Vec::new(),
                diagnostics: Vec::new(),
            };
        }
        MemoryActiveRecallMode::DeterministicOnly => {
            return ActiveMemoryDecision {
                status: ActiveMemoryDecisionStatus::Skip,
                reason_code: ActiveMemoryDecisionReasonCode::DeterministicOnly,
                confidence: 1.0,
                query_hints: Vec::new(),
                diagnostics: Vec::new(),
            };
        }
        MemoryActiveRecallMode::StrictDebug => {
            return ActiveMemoryDecision {
                status: ActiveMemoryDecisionStatus::Run,
                reason_code: ActiveMemoryDecisionReasonCode::StrictDebug,
                confidence: 1.0,
                query_hints: Vec::new(),
                diagnostics: Vec::new(),
            };
        }
        MemoryActiveRecallMode::Hybrid => {}
    }

    if deterministic.sufficient {
        return ActiveMemoryDecision {
            status: ActiveMemoryDecisionStatus::Skip,
            reason_code: ActiveMemoryDecisionReasonCode::DeterministicSufficient,
            confidence: 0.9,
            query_hints: Vec::new(),
            diagnostics: Vec::new(),
        };
    }

    if let Some(provider) = provider {
        let request = MemoryActiveRecallDecisionRequest {
            deterministic_context_count: deterministic.context_count,
            deterministic_context_chars: deterministic.context_chars,
            deterministic_memory_ids: deterministic.memory_ids.iter().cloned().collect(),
            config_mode: config.mode,
        };
        match provider
            .resolve_active_memory_decision_json(
                MemoryActiveRecallDecisionContext {
                    workspace_id: context.workspace_id.clone(),
                    thread_id: context.thread_id.clone(),
                    turn_id: context.turn_id.clone(),
                    mode: context.mode,
                    input_text_preview: truncate_chars(input.input_text.as_str(), 1_000),
                },
                request,
            )
            .await
        {
            Ok(json) => match parse_active_memory_decision_json(json.as_str()) {
                Ok(decision) => {
                    return decision;
                }
                Err(_) => {
                    return ActiveMemoryDecision {
                        status: ActiveMemoryDecisionStatus::Skip,
                        reason_code: ActiveMemoryDecisionReasonCode::ProviderUncertain,
                        confidence: 0.0,
                        query_hints: Vec::new(),
                        diagnostics: vec!["memory.active_recall.invalid_json".to_owned()],
                    };
                }
            },
            Err(_) => {
                return ActiveMemoryDecision {
                    status: ActiveMemoryDecisionStatus::Skip,
                    reason_code: ActiveMemoryDecisionReasonCode::ProviderUncertain,
                    confidence: 0.0,
                    query_hints: Vec::new(),
                    diagnostics: vec!["memory.active_recall.provider_failed".to_owned()],
                };
            }
        }
    }

    local_active_memory_decision(input.input_text.as_str(), "")
}

fn local_active_memory_decision(input_text: &str, diagnostic: &str) -> ActiveMemoryDecision {
    let mut diagnostics = Vec::new();
    if !diagnostic.trim().is_empty() {
        diagnostics.push(diagnostic.to_owned());
    }
    if is_trivial_self_contained_turn(input_text) {
        return ActiveMemoryDecision {
            status: ActiveMemoryDecisionStatus::Skip,
            reason_code: ActiveMemoryDecisionReasonCode::TrivialSelfContained,
            confidence: 0.75,
            query_hints: Vec::new(),
            diagnostics,
        };
    }
    ActiveMemoryDecision {
        status: ActiveMemoryDecisionStatus::Run,
        reason_code: ActiveMemoryDecisionReasonCode::MemoryLikely,
        confidence: 0.65,
        query_hints: vec![MEMORY_ACTIVE_RECALL_GENERIC_QUERY.to_owned()],
        diagnostics,
    }
}

fn active_memory_decision_observability_diagnostic(
    decision: &ActiveMemoryDecision,
    deterministic: &DeterministicRecallContextSummary,
) -> HookDiagnostic {
    memory_safe_info_diagnostic(
        "memory.active_recall.decision",
        format!(
            "memory active recall decision: status={} reason={} confidence={:.2} deterministic_sufficient={} deterministic_contexts={} deterministic_chars={}",
            active_memory_decision_status_name(decision.status),
            decision.reason_code.as_str(),
            decision.confidence,
            deterministic.sufficient,
            deterministic.context_count,
            deterministic.context_chars
        ),
    )
}

fn active_memory_dedup_observability_diagnostic(
    deterministic: &DeterministicRecallContextSummary,
    dedup: &ActiveRecallDedupResult,
) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.active_recall.dedup",
        format!(
            "memory active recall dedup: deterministic_recall_count={} active_raw_count={} active_duplicate_count={} active_rendered_count={} duplicate_only={}",
            deterministic.memory_ids.len(),
            dedup.raw_count,
            dedup.duplicate_count(),
            dedup.rendered_count(),
            dedup.duplicate_only()
        ),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "deterministic_recall_count",
        deterministic.memory_ids.len(),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_raw_count",
        dedup.raw_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_id_count",
        dedup.duplicate_id_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_line_count",
        dedup.duplicate_line_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_rendered_count",
        dedup.rendered_count(),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_duplicate_only"),
        HookValue::Bool(dedup.duplicate_only()),
    );
    diagnostic
}

fn memory_prompt_recall_dedup_diagnostic(context: &MemoryRecallPromptContext) -> HookDiagnostic {
    let mut diagnostic = memory_safe_info_diagnostic(
        "memory.prompt_recall.dedup",
        format!(
            "memory prompt recall dedup: deterministic_recall_count={} active_raw_count={} active_duplicate_count={} active_rendered_count={} active_synthesis_rendered={} active_duplicate_only={}",
            context.deterministic_memory_count,
            context.active_raw_count,
            context.active_duplicate_count(),
            context.active_rendered_count,
            context.active_synthesis_rendered,
            context.active_duplicate_only()
        ),
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "deterministic_recall_count",
        context.deterministic_memory_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_raw_count",
        context.active_raw_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_id_count",
        context.active_duplicate_id_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_duplicate_line_count",
        context.active_duplicate_line_count,
    );
    insert_usize_metadata(
        &mut diagnostic.metadata,
        "active_rendered_count",
        context.active_rendered_count,
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_synthesis_rendered"),
        HookValue::Bool(context.active_synthesis_rendered),
    );
    diagnostic.metadata.insert(
        hook_metadata_key("active_duplicate_only"),
        HookValue::Bool(context.active_duplicate_only()),
    );
    diagnostic
}

fn active_memory_decision_status_name(status: ActiveMemoryDecisionStatus) -> &'static str {
    match status {
        ActiveMemoryDecisionStatus::Skip => "skip",
        ActiveMemoryDecisionStatus::Run => "run",
        ActiveMemoryDecisionStatus::Uncertain => "uncertain",
    }
}

fn parse_active_memory_decision_json(raw: &str) -> Result<ActiveMemoryDecision, serde_json::Error> {
    let parsed = serde_json::from_str::<ActiveMemoryDecisionJson>(raw.trim())?;
    let status = match parsed.status {
        ActiveMemoryDecisionJsonStatus::Skip => ActiveMemoryDecisionStatus::Skip,
        ActiveMemoryDecisionJsonStatus::Run => ActiveMemoryDecisionStatus::Run,
        ActiveMemoryDecisionJsonStatus::Uncertain => ActiveMemoryDecisionStatus::Uncertain,
    };
    let reason_code = match status {
        ActiveMemoryDecisionStatus::Skip => ActiveMemoryDecisionReasonCode::ProviderSkip,
        ActiveMemoryDecisionStatus::Run => ActiveMemoryDecisionReasonCode::ProviderRun,
        ActiveMemoryDecisionStatus::Uncertain => ActiveMemoryDecisionReasonCode::ProviderUncertain,
    };
    Ok(ActiveMemoryDecision {
        status,
        reason_code,
        confidence: parsed.confidence.clamp(0.0, 1.0),
        query_hints: parsed
            .query_hints
            .into_iter()
            .filter_map(|hint| bounded_nonempty_text(hint.as_str(), 240))
            .take(3)
            .collect(),
        diagnostics: parsed
            .diagnostics
            .into_iter()
            .filter_map(|diagnostic| bounded_nonempty_text(diagnostic.as_str(), 160))
            .collect(),
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActiveMemoryDecisionJson {
    status: ActiveMemoryDecisionJsonStatus,
    #[serde(default)]
    confidence: f32,
    #[serde(default)]
    query_hints: Vec<String>,
    #[serde(default)]
    diagnostics: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ActiveMemoryDecisionJsonStatus {
    Skip,
    Run,
    Uncertain,
}

fn is_trivial_self_contained_turn(input_text: &str) -> bool {
    let trimmed = input_text.trim();
    if trimmed.is_empty() {
        return true;
    }
    let word_count = trimmed.split_whitespace().count();
    let char_count = trimmed.chars().count();
    word_count <= 5 && char_count <= 48
}

fn active_memory_query_plan(
    input_text: &str,
    decision: &ActiveMemoryDecision,
    config: &MemoryActiveRecallConfig,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut queries = Vec::new();
    for query in decision
        .query_hints
        .iter()
        .map(String::as_str)
        .chain([MEMORY_ACTIVE_RECALL_GENERIC_QUERY, input_text])
    {
        let Some(query) = bounded_nonempty_text(query, 500) else {
            continue;
        };
        let key = query.to_lowercase();
        if seen.insert(key) {
            queries.push(query);
        }
        if queries.len() >= config.max_queries {
            break;
        }
    }
    queries
}

#[derive(Debug, Clone, Default)]
struct ActiveRecallDedupResult {
    items: Vec<MemoryRecallItem>,
    raw_count: usize,
    duplicate_id_count: usize,
    duplicate_line_count: usize,
}

impl ActiveRecallDedupResult {
    fn rendered_count(&self) -> usize {
        self.items.len()
    }

    fn duplicate_count(&self) -> usize {
        self.duplicate_id_count + self.duplicate_line_count
    }

    fn duplicate_only(&self) -> bool {
        self.raw_count > 0 && self.rendered_count() == 0 && self.duplicate_count() > 0
    }
}

fn dedup_active_recall_items_with_lines(
    items: Vec<MemoryRecallItem>,
    deterministic_ids: &BTreeSet<String>,
    deterministic_line_fingerprints: &BTreeSet<String>,
) -> ActiveRecallDedupResult {
    let mut seen = deterministic_ids.clone();
    let mut seen_lines = deterministic_line_fingerprints.clone();
    let mut result = ActiveRecallDedupResult {
        raw_count: items.len(),
        ..ActiveRecallDedupResult::default()
    };
    let mut deduped = Vec::new();
    for item in items {
        let memory_id = item.memory_id.trim();
        if memory_id.is_empty() || !seen.insert(memory_id.to_owned()) {
            result.duplicate_id_count += 1;
            continue;
        }
        if let Some(fingerprint) = memory_recall_item_rendered_line_fingerprint(&item)
            && !seen_lines.insert(fingerprint)
        {
            result.duplicate_line_count += 1;
            continue;
        }
        deduped.push(item);
    }
    result.items = deduped;
    result
}

fn memory_recall_item_rendered_line_fingerprint(item: &MemoryRecallItem) -> Option<String> {
    let prompt_item = memory_recall_prompt_item(item.clone());
    let (line, _) = render_memory_recall_context_block(&[prompt_item], false);
    rendered_line_fingerprint(line.as_str())
}

fn rendered_line_fingerprints(content: &str) -> BTreeSet<String> {
    content
        .lines()
        .filter_map(rendered_line_fingerprint)
        .collect()
}

fn active_memory_context_lines(content: &str) -> Vec<&str> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != "Active memory context:")
        .collect()
}

fn rendered_memory_line_id(line: &str) -> Option<String> {
    let line = line.trim();
    let metadata = line.strip_prefix("- [")?;
    let end = metadata
        .char_indices()
        .find_map(|(index, ch)| (ch == ',' || ch == ']').then_some(index))?;
    let memory_id = metadata[..end].trim();
    if memory_id.is_empty() {
        None
    } else {
        Some(memory_id.to_owned())
    }
}

fn rendered_line_fingerprint(line: &str) -> Option<String> {
    let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn memory_active_recall_prompt_context_contribution(
    items: Vec<MemoryRecallItem>,
    snapshot_truncated: bool,
    config: &MemoryActiveRecallConfig,
) -> Option<PromptContextContribution> {
    if items.is_empty() {
        return None;
    }
    let source_refs = memory_recall_source_refs(items.as_slice());
    let prompt_items = items
        .into_iter()
        .map(memory_recall_prompt_item)
        .collect::<Vec<_>>();
    let (content, rendered_truncated) =
        render_memory_recall_context_block(prompt_items.as_slice(), snapshot_truncated);
    if content.trim().is_empty() {
        return None;
    }
    let mut content = content;
    let mut truncated = rendered_truncated;
    let content_chars = content.chars().count();
    if content_chars > config.max_prompt_chars {
        content = truncate_chars(content.as_str(), config.max_prompt_chars);
        truncated = true;
    }
    Some(PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        domain: memory_policy_domain(),
        priority: 490,
        content: HookPromptContent::new(content).ok()?,
        max_chars: Some(config.max_prompt_chars),
        source_refs,
        diagnostics: Vec::new(),
        truncated,
    })
}

fn bounded_nonempty_text(value: &str, max_chars: usize) -> Option<String> {
    let trimmed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if trimmed.is_empty() {
        None
    } else {
        Some(truncate_chars(trimmed.as_str(), max_chars))
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn memory_recall_prompt_context_contribution(
    recall_snapshot: MemoryRecallSnapshot,
) -> Option<PromptContextContribution> {
    if recall_snapshot.items.is_empty() {
        return None;
    }
    let truncated = recall_snapshot.truncated;
    let source_refs = memory_recall_source_refs(recall_snapshot.items.as_slice());
    let prompt_items = recall_snapshot
        .items
        .into_iter()
        .map(memory_recall_prompt_item)
        .collect::<Vec<_>>();
    let (content, truncated) =
        render_memory_recall_context_block(prompt_items.as_slice(), truncated);
    let content = HookPromptContent::new(content).ok()?;
    Some(PromptContextContribution {
        contribution_id: HookContributionId::new(MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        domain: memory_policy_domain(),
        priority: 500,
        content,
        max_chars: Some(1_500),
        source_refs,
        diagnostics: Vec::new(),
        truncated,
    })
}

fn memory_recall_source_refs(items: &[MemoryRecallItem]) -> Vec<HookSourceRef> {
    let mut seen = BTreeSet::new();
    items
        .iter()
        .filter_map(|item| {
            let memory_id = item.memory_id.trim();
            if memory_id.is_empty() || !seen.insert(memory_id.to_owned()) {
                return None;
            }
            Some(HookSourceRef {
                kind: HookSourceKind::Custom("memory".to_owned()),
                id: HookSourceId::new(memory_id.to_owned()).ok()?,
                label: None,
            })
        })
        .collect()
}

fn memory_tool_names_from_prompt_compile_input(
    input: &TurnPrePromptCompileHookInput,
) -> Vec<String> {
    let available = input
        .available_tool_names
        .iter()
        .map(|name| name.as_str())
        .collect::<BTreeSet<_>>();

    [
        MEMORY_SEARCH_TOOL,
        MEMORY_GET_TOOL,
        MEMORY_REMEMBER_TOOL,
        MEMORY_FORGET_TOOL,
    ]
    .into_iter()
    .filter(|name| available.contains(name))
    .map(str::to_owned)
    .collect()
}

#[derive(Debug, Clone, Default)]
struct MemoryRecallPromptContext {
    deterministic_content: Option<String>,
    active_content: Option<String>,
    count: usize,
    deterministic_count: usize,
    deterministic_memory_count: usize,
    active_raw_count: usize,
    active_duplicate_id_count: usize,
    active_duplicate_line_count: usize,
    active_rendered_count: usize,
    active_synthesis_rendered: bool,
    truncated: bool,
}

impl MemoryRecallPromptContext {
    fn active_duplicate_count(&self) -> usize {
        self.active_duplicate_id_count + self.active_duplicate_line_count
    }

    fn active_duplicate_only(&self) -> bool {
        self.active_raw_count > 0
            && self.active_rendered_count == 0
            && self.active_duplicate_count() > 0
    }
}

fn memory_recall_context_from_prompt_context_set(
    prompt_context_set: &pioneer_hooks::HookPromptContextSet,
    prompt_policy: MemoryRecallPromptPolicy,
) -> MemoryRecallPromptContext {
    if prompt_policy == MemoryRecallPromptPolicy::ForgetOnly {
        return MemoryRecallPromptContext::default();
    }

    let mut context = MemoryRecallPromptContext::default();
    let mut deterministic_content = String::new();
    let mut active_content = String::new();
    let mut deterministic_ids = BTreeSet::new();
    let mut seen_line_fingerprints = BTreeSet::new();
    let mut active_ids = BTreeSet::new();
    for entry in prompt_context_set.entries() {
        if entry.domain.as_str() != MEMORY_POLICY_DOMAIN {
            continue;
        }
        match entry.contribution_id.as_str() {
            MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID => {
                let entry_content = entry.content.as_str().trim();
                if entry_content.is_empty() {
                    continue;
                }
                if !deterministic_content.is_empty() {
                    deterministic_content.push('\n');
                }
                deterministic_content.push_str(entry_content);
                context.count += 1;
                context.deterministic_count += 1;
                context.truncated |= entry.truncated;
                seen_line_fingerprints.extend(rendered_line_fingerprints(entry_content));
                for source_ref in &entry.source_refs {
                    if source_ref.kind.as_str() == "memory"
                        && deterministic_ids.insert(source_ref.id.as_str().to_owned())
                    {
                        context.deterministic_memory_count += 1;
                    }
                }
            }
            MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID => {
                let entry_content = entry.content.as_str().trim();
                if entry_content.is_empty() {
                    continue;
                }
                context.count += 1;
                context.truncated |= entry.truncated;
                for line in active_memory_context_lines(entry_content) {
                    context.active_raw_count += 1;
                    let parsed_id = rendered_memory_line_id(line);
                    if let Some(memory_id) = parsed_id.as_deref() {
                        if deterministic_ids.contains(memory_id)
                            || !active_ids.insert(memory_id.to_owned())
                        {
                            context.active_duplicate_id_count += 1;
                            continue;
                        }
                    }
                    let Some(fingerprint) = rendered_line_fingerprint(line) else {
                        continue;
                    };
                    if !seen_line_fingerprints.insert(fingerprint) {
                        context.active_duplicate_line_count += 1;
                        continue;
                    }
                    if !active_content.is_empty() {
                        active_content.push('\n');
                    }
                    active_content.push_str(line);
                    context.active_rendered_count += 1;
                    context.active_synthesis_rendered |= parsed_id.is_none();
                }
            }
            _ => {}
        }
    }
    context.deterministic_content = if deterministic_content.trim().is_empty() {
        None
    } else {
        Some(deterministic_content)
    };
    context.active_content = if active_content.trim().is_empty() {
        None
    } else {
        Some(active_content)
    };
    context
}

fn hook_diagnostics_from_strings(messages: &[String]) -> Vec<HookDiagnostic> {
    messages
        .iter()
        .map(|message| {
            memory_hook_diagnostic("memory.diagnostic", safe_memory_policy_diagnostic(message))
        })
        .collect()
}

fn memory_missing_state_response(hook: &'static str) -> HookHandlerResponse {
    let mut response = HookHandlerResponse::default();
    response.diagnostics.push(memory_safe_warning_diagnostic(
        "memory.missing_state",
        format!("{hook} skipped because memory turn policy state was unavailable"),
    ));
    response
}

fn memory_missing_policy_response(hook: &'static str) -> HookHandlerResponse {
    let mut response = HookHandlerResponse::default();
    response.diagnostics.push(memory_safe_warning_diagnostic(
        "memory.missing_policy",
        format!("{hook} skipped because memory hook policy was unavailable"),
    ));
    response
}

fn memory_hook_diagnostic(code: &'static str, message: impl Into<String>) -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(message.into())
            .expect("diagnostic message should be non-empty"),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user: false,
        metadata: HookMetadata::default(),
    }
}

fn memory_safe_info_diagnostic(code: &'static str, message: impl Into<String>) -> HookDiagnostic {
    memory_safe_diagnostic(code, message, HookDiagnosticSeverity::Info)
}

fn memory_safe_warning_diagnostic(
    code: &'static str,
    message: impl Into<String>,
) -> HookDiagnostic {
    memory_safe_diagnostic(code, message, HookDiagnosticSeverity::Warning)
}

fn memory_safe_diagnostic(
    code: &'static str,
    message: impl Into<String>,
    severity: HookDiagnosticSeverity,
) -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(safe_memory_policy_diagnostic(message.into().as_str()))
            .expect("safe diagnostic message should be non-empty"),
        severity,
        safe_for_user: true,
        metadata: HookMetadata::default(),
    }
}

fn memory_hook_error(code: &'static str, message: impl Into<String>) -> HookError {
    HookError::new(
        HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        HookDiagnosticMessage::new(message.into()).expect("hook error message should be non-empty"),
    )
}

fn memory_turn_policy_request_from_metadata(
    metadata: &HookMetadata,
) -> (MemoryTurnPolicyRequest, Vec<String>) {
    let mut request = MemoryTurnPolicyRequest::default();
    let mut diagnostics = Vec::new();

    if let Some(value) = metadata.get(&hook_metadata_key(MEMORY_TURN_POLICY_DEFAULT_METADATA_KEY)) {
        match memory_turn_policy_from_hook_value(value) {
            Ok(default_policy) => request.default_policy = default_policy,
            Err(error) => diagnostics.push(format!(
                "memory.policy.metadata_invalid: default_policy {error}"
            )),
        }
    }

    if let Some(value) = metadata.get(&hook_metadata_key(MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY))
    {
        match memory_turn_policy_from_hook_value(value) {
            Ok(policy) => {
                request.structured_override = Some(MemoryTurnPolicyOverride::new(policy));
            }
            Err(error) => {
                diagnostics.push(format!("memory.policy.metadata_invalid: override {error}"))
            }
        }
    }

    if let Some(value) = metadata.get(&hook_metadata_key(
        MEMORY_TURN_POLICY_CLASSIFIER_ENABLED_METADATA_KEY,
    )) {
        match value {
            HookValue::Bool(enabled) => request.classifier_enabled = *enabled,
            _ => diagnostics
                .push("memory.policy.metadata_invalid: classifier_enabled invalid_type".to_owned()),
        }
    }

    if let Some(value) = metadata.get(&hook_metadata_key(MEMORY_TURN_POLICY_FALLBACK_METADATA_KEY))
    {
        match value {
            HookValue::Text(fallback) => match MemoryClassifierFallbackPolicy::from_str(fallback) {
                Ok(fallback) => request.fallback = fallback,
                Err(error) => {
                    diagnostics.push(format!("memory.policy.metadata_invalid: fallback {error}"))
                }
            },
            _ => {
                diagnostics.push("memory.policy.metadata_invalid: fallback invalid_type".to_owned())
            }
        }
    }

    (request, diagnostics)
}

fn safe_memory_policy_diagnostic(message: &str) -> String {
    const MAX_DIAGNOSTIC_CHARS: usize = 300;
    let mut safe = message.replace(['\r', '\n'], " ");
    safe.truncate(MAX_DIAGNOSTIC_CHARS);
    if safe.trim().is_empty() {
        "memory diagnostic unavailable".to_owned()
    } else {
        safe
    }
}

pub(crate) async fn resolve_memory_turn_policy(
    provider: Option<&Arc<dyn AgentMemoryTurnPolicyProvider>>,
    context: MemoryTurnPolicyContext,
    request: MemoryTurnPolicyRequest,
) -> MemoryTurnPolicy {
    if context.mode == ThreadMode::Chat {
        return MemoryTurnPolicy::no_use().with_source(
            MemoryPolicySource::StructuredOverride,
            MemoryPolicyReasonCode::ChatModeDisabled,
            1.0,
        );
    }

    if let Some(override_policy) = request.structured_override {
        return override_policy.policy.with_source(
            MemoryPolicySource::StructuredOverride,
            MemoryPolicyReasonCode::StructuredOverride,
            1.0,
        );
    }

    if !request.classifier_enabled {
        return fallback_policy(
            request.default_policy,
            request.fallback,
            MemoryPolicyReasonCode::ClassifierUnavailable,
            Some("memory.policy.fallback_used: classifier disabled"),
        );
    }

    let Some(provider) = provider else {
        return fallback_policy(
            request.default_policy,
            request.fallback,
            MemoryPolicyReasonCode::ClassifierUnavailable,
            Some("memory.policy.fallback_used: classifier provider unavailable"),
        );
    };

    match provider
        .resolve_memory_turn_policy(context, request.clone())
        .await
    {
        Ok(policy) => policy,
        Err(error) => {
            let reason_code = fallback_reason_for_classifier_error(error.as_str());
            fallback_policy(
                request.default_policy,
                request.fallback,
                reason_code,
                Some(format!(
                    "memory.policy.classifier_failed: reason={}",
                    reason_code.as_str()
                )),
            )
        }
    }
}

fn fallback_reason_for_classifier_error(error: &str) -> MemoryPolicyReasonCode {
    if error.contains("classifier_unavailable")
        || error.contains("missing model")
        || error.contains("failed to create")
        || error.contains("request failed")
    {
        MemoryPolicyReasonCode::ClassifierUnavailable
    } else {
        MemoryPolicyReasonCode::ClassifierInvalidJson
    }
}

fn fallback_policy(
    default_policy: MemoryTurnPolicy,
    fallback: MemoryClassifierFallbackPolicy,
    reason_code: MemoryPolicyReasonCode,
    diagnostic: Option<impl Into<String>>,
) -> MemoryTurnPolicy {
    let mut policy = match fallback {
        MemoryClassifierFallbackPolicy::DefaultAllow => {
            default_policy.with_source(MemoryPolicySource::DefaultFallback, reason_code, 0.0)
        }
        MemoryClassifierFallbackPolicy::StrictDeny => {
            MemoryTurnPolicy::strict_deny_fallback(reason_code)
        }
        MemoryClassifierFallbackPolicy::AllowReadOnly => {
            MemoryTurnPolicy::allow_read_only_fallback(reason_code)
        }
    };
    if let Some(diagnostic) = diagnostic {
        let diagnostic: String = diagnostic.into();
        policy
            .diagnostics
            .push(safe_memory_policy_diagnostic(diagnostic.as_str()));
    }
    policy
}

#[cfg(test)]
pub(crate) fn memory_tool_names(materialization: &MemoryToolMaterialization) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut names = Vec::new();
    for bundle in &materialization.bundles {
        for configured in &bundle.specs {
            let name = configured.spec.name.trim();
            if !name.is_empty() && seen.insert(name.to_owned()) {
                names.push(name.to_owned());
            }
        }
    }
    names
}

pub(crate) fn filter_memory_tool_materialization(
    materialization: MemoryToolMaterialization,
    policy: &MemoryTurnPolicy,
) -> MemoryToolMaterialization {
    let mut diagnostics = materialization.diagnostics;
    let mut removed_tools = Vec::new();
    let mut bundles = Vec::new();

    for bundle in materialization.bundles {
        let mut allowed_spec_names = HashSet::new();
        let specs = bundle
            .specs
            .into_iter()
            .filter(|configured| {
                let name = configured.spec.name.as_str();
                let allowed = policy.allows_memory_tool(name);
                if allowed {
                    allowed_spec_names.insert(name.to_owned());
                } else {
                    removed_tools.push(name.to_owned());
                }
                allowed
            })
            .collect::<Vec<_>>();

        let handlers = bundle
            .handlers
            .into_iter()
            .filter(|(name, _)| {
                let allowed = allowed_spec_names.contains(name);
                if !allowed && policy.allows_memory_tool(name.as_str()) {
                    removed_tools.push(name.clone());
                }
                allowed
            })
            .collect::<Vec<_>>();

        if !specs.is_empty() || !handlers.is_empty() {
            bundles.push(pioneer_tools::ToolExtensionBundle { specs, handlers });
        }
    }

    removed_tools.sort();
    removed_tools.dedup();
    if !removed_tools.is_empty() {
        diagnostics.push(format!(
            "memory.policy.tools_filtered: source={} reason={} removed={}",
            policy.source.as_str(),
            policy.reason_code.as_str(),
            removed_tools.join(",")
        ));
    }

    MemoryToolMaterialization {
        bundles,
        diagnostics,
    }
}

fn hook_tool_names_csv(tool_names: &[HookToolName]) -> String {
    if tool_names.is_empty() {
        return "none".to_owned();
    }
    tool_names
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
pub(crate) fn memory_recall_prompt_input(
    available_tool_names: Vec<String>,
    policy: MemoryRecallPromptPolicy,
    recall_snapshot: MemoryRecallSnapshot,
) -> MemoryRecallPromptInput {
    MemoryRecallPromptInput {
        available_tool_names,
        policy,
        recalled_items: recall_snapshot
            .items
            .into_iter()
            .map(memory_recall_prompt_item)
            .collect(),
        recalled_context: None,
        active_context: None,
        truncated: recall_snapshot.truncated,
    }
}

fn memory_recall_prompt_section_contribution_from_context(
    available_tool_names: Vec<String>,
    policy: MemoryRecallPromptPolicy,
    recall_context: MemoryRecallPromptContext,
    truncated: bool,
) -> Option<HookContribution> {
    memory_recall_prompt_section_contribution_from_input(MemoryRecallPromptInput {
        available_tool_names,
        policy,
        recalled_items: Vec::new(),
        recalled_context: recall_context.deterministic_content,
        active_context: recall_context.active_content,
        truncated: truncated || recall_context.truncated,
    })
}

fn memory_recall_prompt_section_contribution_from_input(
    prompt_input: MemoryRecallPromptInput,
) -> Option<HookContribution> {
    let prompt = render_memory_recall_prompt(&prompt_input)?;
    Some(HookContribution::PromptSection(PromptSectionContribution {
        contribution_id: HookContributionId::new(MEMORY_PROMPT_CONTRACT_CONTRIBUTION_ID)
            .expect("static contribution id is valid"),
        section_id: HookSectionId::new(MEMORY_PROMPT_CONTRACT_SECTION_ID)
            .expect("static section id is valid"),
        title: None,
        domain: HookDomain::new("memory").expect("static domain is valid"),
        priority: 500,
        content: HookPromptContent::new(prompt).ok()?,
        max_chars: None,
        diagnostics: Vec::new(),
        truncated: false,
    }))
}

fn memory_recall_prompt_item(item: MemoryRecallItem) -> MemoryRecallPromptItem {
    MemoryRecallPromptItem {
        memory_id: item.memory_id,
        scope_label: scope_label(&item.scope),
        category_label: category_label(item.category).to_owned(),
        key: item.key,
        content: item.content,
        score: item.score,
        updated_at_label: date_label(item.updated_at),
    }
}

fn date_label(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| timestamp.to_string())
}

fn scope_label(scope: &MemoryScope) -> String {
    match scope.kind {
        MemoryScopeKind::User => "user".to_owned(),
        MemoryScopeKind::Workspace => format!("workspace:{}", scope.key),
        MemoryScopeKind::Thread => format!("thread:{}", scope.key),
        MemoryScopeKind::Agent => format!("agent:{}", scope.key),
        MemoryScopeKind::Task => format!("task:{}", scope.key),
    }
}

fn category_label(category: MemoryCategory) -> &'static str {
    match category {
        MemoryCategory::Identity => "identity",
        MemoryCategory::Preference => "preference",
        MemoryCategory::Biography => "biography",
        MemoryCategory::Relationship => "relationship",
        MemoryCategory::RecurringInstruction => "recurring_instruction",
        MemoryCategory::ProjectPolicy => "project_policy",
        MemoryCategory::ProjectFact => "project_fact",
        MemoryCategory::ProjectDecision => "project_decision",
        MemoryCategory::Procedure => "procedure",
        MemoryCategory::Todo => "todo",
        MemoryCategory::Constraint => "constraint",
        MemoryCategory::CommunicationStyle => "communication_style",
        MemoryCategory::Custom => "custom",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_hooks::{
        HookContext, HookInput, HookPhaseRequest, HookPromptContextLimits, HookPromptContextSet,
        HookRegistry, HookRuntime, HookSubscriptionRegistry, HookThreadId, HookToolName,
        HookTurnId, HookWorkspaceId, TurnPrePromptCompileHookInput, TurnPrePromptContextHookInput,
        TurnPreToolMaterializationHookInput,
    };
    use pioneer_tools::{ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolSpec};
    use serde_json::json;

    fn user_scope() -> MemoryScope {
        MemoryScope {
            kind: MemoryScopeKind::User,
            key: "global".to_owned(),
        }
    }

    #[test]
    fn memory_active_recall_config_defaults_to_bounded_hybrid() {
        let config = MemoryLoopConfig::default().normalized();

        assert_eq!(config.active_recall.mode, MemoryActiveRecallMode::Hybrid);
        assert!(config.active_recall.timeout_ms > 0);
        assert!(config.active_recall.max_queries > 0);
        assert!(config.active_recall.top_k_per_query > 0);
        assert!(config.active_recall.max_prompt_chars > 0);

        let zero = MemoryActiveRecallConfig {
            timeout_ms: 0,
            max_queries: 0,
            top_k_per_query: 0,
            max_prompt_chars: 0,
            deterministic_sufficient_min_items: 0,
            deterministic_sufficient_min_chars: 0,
            ..MemoryActiveRecallConfig::default()
        }
        .normalized();
        assert_eq!(zero.timeout_ms, 1);
        assert_eq!(zero.max_queries, 1);
        assert_eq!(zero.top_k_per_query, 1);
        assert_eq!(zero.max_prompt_chars, 1);
    }

    #[test]
    fn memory_turn_policy_constructors_have_separate_controls() {
        let default_policy = MemoryTurnPolicy::normal_default_allow();
        assert!(default_policy.allow_pre_turn_recall());
        assert!(default_policy.allows_memory_tool(MEMORY_SEARCH_TOOL));
        assert!(default_policy.allows_memory_tool(MEMORY_REMEMBER_TOOL));
        assert_eq!(default_policy.detected_language, None);
        assert_eq!(
            default_policy.post_turn_extraction,
            MemoryExtractionPolicy::Disabled
        );
        assert_eq!(
            default_policy.active_memory,
            MemoryActiveContextPolicy::Allow
        );

        let no_save = MemoryTurnPolicy::no_save();
        assert!(no_save.allow_pre_turn_recall());
        assert!(no_save.allows_memory_tool(MEMORY_SEARCH_TOOL));
        assert!(!no_save.allows_memory_tool(MEMORY_REMEMBER_TOOL));
        assert!(no_save.allows_memory_tool(MEMORY_FORGET_TOOL));
        assert_eq!(no_save.active_memory, MemoryActiveContextPolicy::Allow);

        let forget = MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned()));
        assert!(!forget.allow_pre_turn_recall());
        assert!(forget.allows_memory_tool(MEMORY_SEARCH_TOOL));
        assert!(forget.allows_memory_tool(MEMORY_GET_TOOL));
        assert!(!forget.allows_memory_tool(MEMORY_REMEMBER_TOOL));
        assert!(forget.allows_memory_tool(MEMORY_FORGET_TOOL));
    }

    #[test]
    fn memory_turn_policy_hook_value_roundtrips_full_policy() {
        let policy = MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned()))
            .with_detected_language(Some("ru".to_owned()))
            .with_diagnostic("memory.policy.resolved: safe");

        let value = memory_turn_policy_to_hook_value(&policy);
        let decoded =
            memory_turn_policy_from_hook_value(&value).expect("policy hook value decodes");

        assert_eq!(decoded, policy);
        let HookValue::Object(object) = value else {
            panic!("policy should be encoded as object");
        };
        assert!(object.contains_key(&hook_metadata_key("recall")));
        assert!(object.contains_key(&hook_metadata_key("remember_tool")));
        assert!(object.contains_key(&hook_metadata_key("detected_language")));
        assert!(object.contains_key(&hook_metadata_key("diagnostics_summary")));
    }

    #[test]
    fn memory_policy_contribution_emits_full_policy_object() {
        let policy = MemoryTurnPolicy::no_save().with_detected_language(Some("de".to_owned()));
        let contribution = memory_policy_contribution(&policy);

        assert_eq!(contribution.domain.as_str(), MEMORY_POLICY_DOMAIN);
        assert_eq!(contribution.key.as_str(), MEMORY_TURN_POLICY_KEY);
        let decoded = memory_turn_policy_from_hook_value(&contribution.value)
            .expect("contribution should contain full policy");
        assert_eq!(decoded, policy);
        assert_ne!(
            contribution.value,
            HookValue::Text(MemoryPolicyReasonCode::MemoryNoSave.as_str().to_owned())
        );
    }

    #[test]
    fn memory_turn_policy_decodes_from_hook_policy_set() {
        let policy =
            MemoryTurnPolicy::explicit_remember().with_detected_language(Some("es".into()));
        let set = HookPolicySet::merge_contributions([memory_policy_contribution(&policy)]);

        let decoded = memory_turn_policy_from_hook_policy_set(&set)
            .expect("memory policy entry exists")
            .expect("memory policy entry decodes");

        assert_eq!(decoded, policy);
    }

    #[test]
    fn memory_turn_policy_from_hook_policy_set_reports_malformed_policy() {
        let malformed = PolicyContribution {
            domain: memory_policy_domain(),
            key: memory_turn_policy_key(),
            value: HookValue::Text("memory_no_use".to_owned()),
            priority: 500,
            diagnostics: Vec::new(),
        };
        let set = HookPolicySet::merge_contributions([malformed]);

        let decoded =
            memory_turn_policy_from_hook_policy_set(&set).expect("memory policy entry exists");

        assert!(decoded.is_err());
    }

    #[test]
    fn memory_policy_classifier_hook_descriptor_is_stable_and_narrow() {
        let hook = MemoryPolicyClassifierHook {
            policy_provider: None,
            state: Arc::new(MemoryHookTurnStateStore::default()),
        };

        assert_eq!(hook.id().as_str(), MEMORY_POLICY_CLASSIFIER_HOOK_ID);
        assert_eq!(hook.supported_phases(), vec![HookPhase::TurnPrePolicy]);
        let capabilities = hook.capabilities();
        assert!(capabilities.contains(
            &HookCapability::new("contribute_policy").expect("static capability is valid")
        ));
        assert!(
            capabilities.contains(
                &HookCapability::new("call_provider").expect("static capability is valid")
            )
        );
        assert!(!capabilities.contains(
            &HookCapability::new("read_domain_context").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("write_domain_context").expect("static capability is valid")
        ));
        assert!(
            !capabilities
                .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
        );
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
        ));
    }

    #[test]
    fn memory_tool_bundle_hook_descriptor_is_stable_and_narrow() {
        let hook = MemoryToolBundleHook {
            memory_provider: Arc::new(TestMemoryProvider::with_materialization(
                empty_tool_materialization(),
            )),
            state: Arc::new(MemoryHookTurnStateStore::default()),
            tool_bundle_artifacts: Arc::new(AgentToolBundleArtifactStore::new()),
        };

        assert_eq!(hook.id().as_str(), MEMORY_TOOL_BUNDLE_HOOK_ID);
        assert_eq!(
            hook.supported_phases(),
            vec![HookPhase::TurnPreToolMaterialization]
        );
        let capabilities = hook.capabilities();
        assert!(
            capabilities
                .contains(&HookCapability::new("memory").expect("static capability is valid"))
        );
        assert!(capabilities.contains(
            &HookCapability::new("read_domain_context").expect("static capability is valid")
        ));
        assert!(capabilities.contains(
            &HookCapability::new("write_domain_context").expect("static capability is valid")
        ));
        assert!(capabilities.contains(
            &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
        ));
        assert!(
            !capabilities.contains(
                &HookCapability::new("call_provider").expect("static capability is valid")
            )
        );
        assert!(
            !capabilities
                .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
        );
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_policy").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
        ));
    }

    #[test]
    fn memory_deterministic_recall_hook_descriptor_is_stable_and_narrow() {
        let hook = MemoryDeterministicRecallHook {
            memory_provider: Arc::new(TestRecallMemoryProvider::with_recall(
                MemoryRecallSnapshot::empty(),
            )),
        };

        assert_eq!(hook.id().as_str(), MEMORY_DETERMINISTIC_RECALL_HOOK_ID);
        assert_eq!(
            hook.supported_phases(),
            vec![HookPhase::TurnPrePromptContext]
        );
        let capabilities = hook.capabilities();
        assert!(
            capabilities
                .contains(&HookCapability::new("memory").expect("static capability is valid"))
        );
        assert!(capabilities.contains(
            &HookCapability::new("read_domain_context").expect("static capability is valid")
        ));
        assert!(capabilities.contains(
            &HookCapability::new("contribute_prompt_context").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("write_domain_context").expect("static capability is valid")
        ));
        assert!(
            !capabilities
                .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
        );
    }

    #[test]
    fn memory_active_recall_hook_descriptor_is_stable_and_read_only() {
        let hook = ActiveMemoryRecallHook {
            memory_provider: Arc::new(TestRecallMemoryProvider::with_recall(
                MemoryRecallSnapshot::empty(),
            )),
            decision_provider: None,
            config: MemoryActiveRecallConfig::default(),
        };

        assert_eq!(hook.id().as_str(), MEMORY_ACTIVE_RECALL_HOOK_ID);
        assert_eq!(
            hook.supported_phases(),
            vec![HookPhase::TurnPrePromptContext]
        );
        let capabilities = hook.capabilities();
        assert!(
            capabilities
                .contains(&HookCapability::new("memory").expect("static capability is valid"))
        );
        assert!(capabilities.contains(
            &HookCapability::new("read_domain_context").expect("static capability is valid")
        ));
        assert!(capabilities.contains(
            &HookCapability::new("contribute_prompt_context").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("write_domain_context").expect("static capability is valid")
        ));
        assert!(
            !capabilities
                .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
        );
        assert!(
            !capabilities.contains(
                &HookCapability::new("call_provider").expect("static capability is valid")
            )
        );

        let hook_with_provider = ActiveMemoryRecallHook {
            memory_provider: Arc::new(TestRecallMemoryProvider::with_recall(
                MemoryRecallSnapshot::empty(),
            )),
            decision_provider: Some(Arc::new(TestActiveMemoryDecisionProvider::json(
                r#"{"status":"skip","confidence":1.0}"#,
            ))),
            config: MemoryActiveRecallConfig::default(),
        };
        assert!(
            hook_with_provider.capabilities().contains(
                &HookCapability::new("call_provider").expect("static capability is valid")
            )
        );
    }

    #[test]
    fn phase_15_install_memory_hooks_registers_active_recall_with_deadline_dependency() {
        let runtime = Arc::new(HookRuntime::new(
            Arc::new(HookRegistry::new()),
            Arc::new(HookSubscriptionRegistry::new()),
        ));
        let artifacts = Arc::new(AgentToolBundleArtifactStore::new());
        install_memory_hooks(
            &runtime,
            Arc::new(TestRecallMemoryProvider::with_recall(
                MemoryRecallSnapshot::empty(),
            )),
            None,
            artifacts,
            MemoryLoopConfig {
                active_recall: MemoryActiveRecallConfig {
                    timeout_ms: 321,
                    ..MemoryActiveRecallConfig::default()
                },
            },
        )
        .expect("memory hooks install");

        let subscription_id = HookSubscriptionId::new(MEMORY_ACTIVE_RECALL_SUBSCRIPTION_ID)
            .expect("static subscription id is valid");
        let subscription = runtime
            .subscriptions()
            .get_subscription(&subscription_id)
            .expect("subscription lookup succeeds")
            .expect("active recall subscription registered");

        assert_eq!(subscription.hook_id.as_str(), MEMORY_ACTIVE_RECALL_HOOK_ID);
        assert_eq!(subscription.phase, HookPhase::TurnPrePromptContext);
        assert_eq!(
            subscription.execution_policy.await_policy,
            HookAwaitPolicy::Deadline
        );
        assert_eq!(subscription.execution_policy.timeout_ms, Some(321));
        assert_eq!(subscription.failure_policy, HookFailurePolicy::BestEffort);
        assert_eq!(
            subscription.dependencies.after,
            vec![
                HookSubscriptionId::new(MEMORY_DETERMINISTIC_RECALL_SUBSCRIPTION_ID)
                    .expect("static subscription id is valid")
            ]
        );
        assert_eq!(
            subscription.visibility,
            HookSubscriptionVisibility::Internal
        );
    }

    #[tokio::test]
    async fn phase_15_active_memory_timeout_falls_back_without_prompt_context() {
        let runtime = Arc::new(HookRuntime::new(
            Arc::new(HookRegistry::new()),
            Arc::new(HookSubscriptionRegistry::new()),
        ));
        let handler = Arc::new(ActiveMemoryRecallHook {
            memory_provider: Arc::new(SlowRecallMemoryProvider),
            decision_provider: None,
            config: MemoryActiveRecallConfig {
                mode: MemoryActiveRecallMode::StrictDebug,
                max_queries: 1,
                ..MemoryActiveRecallConfig::default()
            },
        });
        runtime
            .handlers()
            .register_handler(handler)
            .expect("active handler registers");
        let subscription_id = HookSubscriptionId::new(MEMORY_ACTIVE_RECALL_SUBSCRIPTION_ID)
            .expect("static subscription id is valid");
        runtime
            .subscriptions()
            .register_subscription(
                runtime.handlers().as_ref(),
                HookSubscription::new(
                    subscription_id,
                    HookId::new(MEMORY_ACTIVE_RECALL_HOOK_ID).expect("static hook id is valid"),
                    HookPhase::TurnPrePromptContext,
                )
                .with_execution_policy(HookExecutionPolicy {
                    await_policy: HookAwaitPolicy::Deadline,
                    timeout_ms: Some(1),
                    max_parallelism: None,
                })
                .with_failure_policy(HookFailurePolicy::BestEffort),
            )
            .expect("active subscription registers");

        let response = runtime
            .run_phase(
                HookPhaseRequest::new(
                    HookPhase::TurnPrePromptContext,
                    HookContext {
                        workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
                        thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
                        turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
                        ..HookContext::default()
                    },
                    HookInput::turn_pre_prompt_context(TurnPrePromptContextHookInput::from_parts(
                        "continue the previous memory-aware architecture work",
                        Some("test-model"),
                        Some("test-provider"),
                    )),
                )
                .with_policy_set(memory_policy_set(&MemoryTurnPolicy::normal_default_allow())),
            )
            .await
            .expect("best-effort timeout should not fail phase");

        assert!(response.contributions.is_empty());
        assert!(
            response
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.as_str() == "hook.timeout")
        );
    }

    #[test]
    fn memory_prompt_contract_hook_descriptor_is_stable_and_narrow() {
        let hook = MemoryPromptContractHook;

        assert_eq!(hook.id().as_str(), MEMORY_PROMPT_CONTRACT_HOOK_ID);
        assert_eq!(
            hook.supported_phases(),
            vec![HookPhase::TurnPrePromptCompile]
        );
        let capabilities = hook.capabilities();
        assert!(
            capabilities
                .contains(&HookCapability::new("memory").expect("static capability is valid"))
        );
        assert!(capabilities.contains(
            &HookCapability::new("read_domain_context").expect("static capability is valid")
        ));
        assert!(capabilities.contains(
            &HookCapability::new("contribute_prompt_section").expect("static capability is valid")
        ));
        assert!(
            !capabilities.contains(
                &HookCapability::new("call_provider").expect("static capability is valid")
            )
        );
        assert!(
            !capabilities
                .contains(&HookCapability::new("call_tools").expect("static capability is valid"))
        );
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_policy").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_tool_bundle").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("contribute_prompt_context").expect("static capability is valid")
        ));
        assert!(!capabilities.contains(
            &HookCapability::new("write_domain_context").expect("static capability is valid")
        ));
    }

    #[test]
    fn memory_tool_bundle_contribution_uses_stable_ids_and_policy_diagnostic() {
        let policy = MemoryTurnPolicy::normal_default_allow();
        let bundle = test_memory_tool_bundle(&[
            MEMORY_SEARCH_TOOL,
            MEMORY_GET_TOOL,
            MEMORY_REMEMBER_TOOL,
            MEMORY_FORGET_TOOL,
        ]);
        let bundle_id = HookToolBundleId::new(format!("{MEMORY_TOOL_BUNDLE_ID_PREFIX}.7"))
            .expect("valid bundle id");

        let contribution = memory_tool_bundle_contribution(7, bundle_id.clone(), &bundle, &policy);

        assert_eq!(
            contribution.contribution_id.as_str(),
            "memory.tool_bundle.contribution.7"
        );
        assert_eq!(contribution.bundle_id, bundle_id);
        assert_eq!(contribution.domain.as_str(), MEMORY_POLICY_DOMAIN);
        assert_eq!(
            hook_tool_names_to_strings(&contribution.tool_names),
            vec![
                MEMORY_SEARCH_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ]
        );
        assert!(contribution.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.tools_exposed"
                && diagnostic.safe_for_user
                && diagnostic
                    .message
                    .as_str()
                    .contains("reason=default_allow_read")
        }));
    }

    #[tokio::test]
    async fn memory_tool_bundle_hook_applies_policy_visibility_matrix() {
        let cases = vec![
            (
                MemoryTurnPolicy::normal_default_allow(),
                vec![
                    MEMORY_SEARCH_TOOL,
                    MEMORY_GET_TOOL,
                    MEMORY_REMEMBER_TOOL,
                    MEMORY_FORGET_TOOL,
                ],
                1,
            ),
            (MemoryTurnPolicy::no_use(), Vec::new(), 0),
            (
                MemoryTurnPolicy::no_save(),
                vec![MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
                1,
            ),
            (
                MemoryTurnPolicy::explicit_remember(),
                vec![
                    MEMORY_SEARCH_TOOL,
                    MEMORY_GET_TOOL,
                    MEMORY_REMEMBER_TOOL,
                    MEMORY_FORGET_TOOL,
                ],
                1,
            ),
            (
                MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned())),
                vec![MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
                1,
            ),
        ];

        for (policy, expected_tools, expected_materialize_calls) in cases {
            let provider = Arc::new(TestMemoryProvider::with_materialization(
                standard_tool_materialization(),
            ));
            let state = Arc::new(MemoryHookTurnStateStore::default());
            state.set_turn_context(test_memory_turn_context());
            let hook = MemoryToolBundleHook {
                memory_provider: provider.clone(),
                state,
                tool_bundle_artifacts: Arc::new(AgentToolBundleArtifactStore::new()),
            };

            let response = hook
                .execute(test_tool_bundle_hook_request(
                    HookPolicySet::merge_contributions([memory_policy_contribution(&policy)]),
                    true,
                ))
                .await
                .expect("tool bundle hook executes");

            assert_eq!(
                provider.materialize_call_count(),
                expected_materialize_calls,
                "policy {:?}",
                policy.reason_code
            );
            assert_eq!(
                response_tool_names(&response),
                expected_tools,
                "policy {:?}",
                policy.reason_code
            );
        }
    }

    #[tokio::test]
    async fn memory_tool_bundle_hook_omits_tools_without_valid_policy_or_tool_calling() {
        let provider = Arc::new(TestMemoryProvider::with_materialization(
            standard_tool_materialization(),
        ));
        let state = Arc::new(MemoryHookTurnStateStore::default());
        state.set_turn_context(test_memory_turn_context());
        let hook = MemoryToolBundleHook {
            memory_provider: provider.clone(),
            state,
            tool_bundle_artifacts: Arc::new(AgentToolBundleArtifactStore::new()),
        };

        let missing = hook
            .execute(test_tool_bundle_hook_request(HookPolicySet::empty(), true))
            .await
            .expect("missing policy is best-effort");
        assert!(response_tool_names(&missing).is_empty());
        assert!(
            missing
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code.as_str() == "memory.missing_policy" })
        );

        let malformed = PolicyContribution {
            domain: memory_policy_domain(),
            key: memory_turn_policy_key(),
            value: HookValue::Text("memory_no_use".to_owned()),
            priority: 500,
            diagnostics: Vec::new(),
        };
        let malformed = hook
            .execute(test_tool_bundle_hook_request(
                HookPolicySet::merge_contributions([malformed]),
                true,
            ))
            .await
            .expect("malformed policy is best-effort");
        assert!(response_tool_names(&malformed).is_empty());
        assert!(malformed.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.policy_decode_failed" && diagnostic.safe_for_user
        }));

        let disabled = hook
            .execute(test_tool_bundle_hook_request(
                HookPolicySet::merge_contributions([memory_policy_contribution(
                    &MemoryTurnPolicy::normal_default_allow(),
                )]),
                false,
            ))
            .await
            .expect("provider tool-calling disabled is best-effort");
        assert!(response_tool_names(&disabled).is_empty());
        assert!(disabled.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.tools_omitted"
                && diagnostic
                    .message
                    .as_str()
                    .contains("provider_tool_calling=false")
        }));
        assert_eq!(provider.materialize_call_count(), 0);
    }

    #[tokio::test]
    async fn memory_tool_bundle_hook_does_not_execute_tool_handlers_during_materialization() {
        let provider = Arc::new(TestMemoryProvider::with_materialization(
            panicking_handler_tool_materialization(),
        ));
        let state = Arc::new(MemoryHookTurnStateStore::default());
        state.set_turn_context(test_memory_turn_context());
        let hook = MemoryToolBundleHook {
            memory_provider: provider.clone(),
            state,
            tool_bundle_artifacts: Arc::new(AgentToolBundleArtifactStore::new()),
        };

        let response = hook
            .execute(test_tool_bundle_hook_request(
                HookPolicySet::merge_contributions([memory_policy_contribution(
                    &MemoryTurnPolicy::normal_default_allow(),
                )]),
                true,
            ))
            .await
            .expect("tool bundle hook executes without invoking handlers");

        assert_eq!(provider.materialize_call_count(), 1);
        assert_eq!(
            response_tool_names(&response),
            vec![
                MEMORY_SEARCH_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL
            ]
        );
    }

    #[tokio::test]
    async fn memory_tool_bundle_hook_materialization_error_is_safe_best_effort() {
        let provider = Arc::new(TestMemoryProvider::failing(
            "raw provider error must not leak",
        ));
        let state = Arc::new(MemoryHookTurnStateStore::default());
        state.set_turn_context(test_memory_turn_context());
        let hook = MemoryToolBundleHook {
            memory_provider: provider.clone(),
            state,
            tool_bundle_artifacts: Arc::new(AgentToolBundleArtifactStore::new()),
        };

        let response = hook
            .execute(test_tool_bundle_hook_request(
                HookPolicySet::merge_contributions([memory_policy_contribution(
                    &MemoryTurnPolicy::normal_default_allow(),
                )]),
                true,
            ))
            .await
            .expect("materialization error is best-effort");

        assert_eq!(provider.materialize_call_count(), 1);
        assert!(response_tool_names(&response).is_empty());
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.tools_failed"
                && diagnostic.safe_for_user
                && !diagnostic.message.as_str().contains("raw provider error")
        }));
    }

    #[tokio::test]
    async fn memory_deterministic_recall_hook_contributes_prompt_context_from_policy_set() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            recalled_city_snapshot(),
        ));
        let hook = MemoryDeterministicRecallHook {
            memory_provider: provider.clone(),
        };

        let response = hook
            .execute(test_prompt_context_hook_request(memory_policy_set(
                &MemoryTurnPolicy::normal_default_allow(),
            )))
            .await
            .expect("recall hook executes");

        assert_eq!(provider.recall_call_count(), 1);
        assert_eq!(provider.materialize_call_count(), 0);
        let contributions = response.contributions;
        assert_eq!(contributions.len(), 1);
        let HookContribution::PromptContext(context) = &contributions[0] else {
            panic!("recall hook should contribute prompt context only");
        };
        assert_eq!(
            context.contribution_id.as_str(),
            MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID
        );
        assert_eq!(context.domain.as_str(), MEMORY_POLICY_DOMAIN);
        assert!(context.content.as_str().contains("User likes Porto."));
        assert_eq!(context.source_refs.len(), 1);
        assert_eq!(context.source_refs[0].id.as_str(), "mem_city");
    }

    #[tokio::test]
    async fn memory_deterministic_recall_hook_skips_without_allowed_policy() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            recalled_city_snapshot(),
        ));
        let hook = MemoryDeterministicRecallHook {
            memory_provider: provider.clone(),
        };

        let response = hook
            .execute(test_prompt_context_hook_request(memory_policy_set(
                &MemoryTurnPolicy::no_use(),
            )))
            .await
            .expect("recall hook executes");

        assert_eq!(provider.recall_call_count(), 0);
        assert!(response.contributions.is_empty());
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.recall_omitted" && diagnostic.safe_for_user
        }));
    }

    #[tokio::test]
    async fn memory_deterministic_recall_hook_skips_malformed_policy_safely() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            recalled_city_snapshot(),
        ));
        let hook = MemoryDeterministicRecallHook {
            memory_provider: provider.clone(),
        };

        let response = hook
            .execute(test_prompt_context_hook_request(
                malformed_memory_policy_set(),
            ))
            .await
            .expect("malformed policy is best-effort");

        assert_eq!(provider.recall_call_count(), 0);
        assert!(response.contributions.is_empty());
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.policy_decode_failed" && diagnostic.safe_for_user
        }));
    }

    #[tokio::test]
    async fn memory_deterministic_recall_hook_failure_is_safe_best_effort() {
        let provider = Arc::new(TestRecallMemoryProvider::failing_recall(
            "raw provider error must not leak",
        ));
        let hook = MemoryDeterministicRecallHook {
            memory_provider: provider.clone(),
        };

        let response = hook
            .execute(test_prompt_context_hook_request(memory_policy_set(
                &MemoryTurnPolicy::normal_default_allow(),
            )))
            .await
            .expect("recall failure is best-effort");

        assert_eq!(provider.recall_call_count(), 1);
        assert!(response.contributions.is_empty());
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.recall_failed"
                && diagnostic.safe_for_user
                && !diagnostic.message.as_str().contains("raw provider error")
        }));
    }

    #[tokio::test]
    async fn phase_15_active_memory_hook_contributes_read_only_prompt_context() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            active_project_snapshot(),
        ));
        let hook = ActiveMemoryRecallHook {
            memory_provider: provider.clone(),
            decision_provider: None,
            config: MemoryActiveRecallConfig {
                max_queries: 1,
                ..MemoryActiveRecallConfig::default()
            },
        };

        let response = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                HookPromptContextSet::default(),
                "continue the architecture work using prior project decisions and constraints",
            ))
            .await
            .expect("active recall hook executes");

        assert_eq!(provider.recall_call_count(), 1);
        assert_eq!(provider.materialize_call_count(), 0);
        let request = provider
            .recall_requests()
            .into_iter()
            .next()
            .expect("active recall request recorded");
        assert_eq!(request.top_k, Some(5));
        assert_eq!(request.max_chars, Some(1_500));
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.active_recall.decision"
                && diagnostic
                    .message
                    .as_str()
                    .contains("deterministic_sufficient=false")
        }));
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.active_recall.context_contributed"
        }));
        let contributions = response.contributions;
        assert_eq!(contributions.len(), 1);
        let HookContribution::PromptContext(context) = &contributions[0] else {
            panic!("active recall should contribute prompt context only");
        };
        assert_eq!(
            context.contribution_id.as_str(),
            MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID
        );
        assert_eq!(context.domain.as_str(), MEMORY_POLICY_DOMAIN);
        assert!(!context.content.as_str().contains("Active memory context:"));
        assert!(
            context
                .content
                .as_str()
                .contains("Use hooks for memory domains.")
        );
        assert_eq!(context.source_refs.len(), 1);
        assert_eq!(context.source_refs[0].id.as_str(), "mem_active_project");
    }

    #[tokio::test]
    async fn phase_15_active_memory_hook_runs_for_memory_sensitive_turns() {
        for input_text in [
            "continue the previous architecture implementation with the same constraints",
            "use my durable preferences and identity details when answering this",
            "apply the project decisions and history from our earlier work",
            "before answering, consider what we discussed in prior threads",
        ] {
            let provider = Arc::new(TestRecallMemoryProvider::with_recall(
                active_project_snapshot(),
            ));
            let hook = ActiveMemoryRecallHook {
                memory_provider: provider.clone(),
                decision_provider: None,
                config: MemoryActiveRecallConfig {
                    max_queries: 1,
                    ..MemoryActiveRecallConfig::default()
                },
            };

            let response = hook
                .execute(test_active_prompt_context_hook_request(
                    memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                    HookPromptContextSet::default(),
                    input_text,
                ))
                .await
                .expect("active recall hook executes");

            assert_eq!(provider.recall_call_count(), 1, "{input_text}");
            assert!(
                response
                    .contributions
                    .iter()
                    .any(|contribution| matches!(contribution, HookContribution::PromptContext(_))),
                "{input_text}"
            );
            assert!(response.diagnostics.iter().any(|diagnostic| {
                diagnostic.code.as_str() == "memory.active_recall.decision"
                    && diagnostic.message.as_str().contains("status=run")
            }));
        }
    }

    #[tokio::test]
    async fn phase_15_active_memory_hook_uses_valid_strict_json_query_hints() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            active_project_snapshot(),
        ));
        let hook = ActiveMemoryRecallHook {
            memory_provider: provider.clone(),
            decision_provider: Some(Arc::new(TestActiveMemoryDecisionProvider::json(
                r#"{"status":"run","confidence":0.92,"queryHints":["project hook runtime constraints"],"diagnostics":["provider ok"]}"#,
            ))),
            config: MemoryActiveRecallConfig {
                max_queries: 1,
                ..MemoryActiveRecallConfig::default()
            },
        };

        let response = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                HookPromptContextSet::default(),
                "finish the architecture task",
            ))
            .await
            .expect("active recall hook executes");

        assert_eq!(provider.recall_call_count(), 1);
        let request = provider
            .recall_requests()
            .into_iter()
            .next()
            .expect("strict JSON query hint should drive recall");
        assert_eq!(request.query, "project hook runtime constraints");
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.active_recall.decision"
                && diagnostic.message.as_str().contains("reason=provider_run")
        }));
        assert!(
            response
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.message.as_str().contains("provider ok") })
        );
    }

    #[tokio::test]
    async fn phase_15_active_memory_hook_respects_policy_config_and_trivial_skips() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            active_project_snapshot(),
        ));
        let hook = ActiveMemoryRecallHook {
            memory_provider: provider.clone(),
            decision_provider: None,
            config: MemoryActiveRecallConfig::default(),
        };

        let no_use = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&MemoryTurnPolicy::no_use()),
                HookPromptContextSet::default(),
                "continue prior decisions",
            ))
            .await
            .expect("no-use policy is best-effort");
        assert!(no_use.contributions.is_empty());

        let mut active_disabled_policy = MemoryTurnPolicy::normal_default_allow();
        active_disabled_policy.active_memory = MemoryActiveContextPolicy::Disabled;
        let disabled_policy = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&active_disabled_policy),
                HookPromptContextSet::default(),
                "continue prior decisions",
            ))
            .await
            .expect("disabled active policy is best-effort");
        assert!(disabled_policy.contributions.is_empty());

        let deterministic_only = ActiveMemoryRecallHook {
            memory_provider: provider.clone(),
            decision_provider: None,
            config: MemoryActiveRecallConfig {
                mode: MemoryActiveRecallMode::DeterministicOnly,
                ..MemoryActiveRecallConfig::default()
            },
        }
        .execute(test_active_prompt_context_hook_request(
            memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
            HookPromptContextSet::default(),
            "continue prior decisions",
        ))
        .await
        .expect("deterministic-only config is best-effort");
        assert!(deterministic_only.contributions.is_empty());

        let trivial = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                HookPromptContextSet::default(),
                "what time?",
            ))
            .await
            .expect("trivial turn is best-effort");
        assert!(trivial.contributions.is_empty());
        assert_eq!(provider.recall_call_count(), 0);
    }

    #[tokio::test]
    async fn phase_15_active_memory_hook_skips_when_deterministic_is_sufficient() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            active_project_snapshot(),
        ));
        let hook = ActiveMemoryRecallHook {
            memory_provider: provider.clone(),
            decision_provider: None,
            config: MemoryActiveRecallConfig::default(),
        };
        let deterministic_context = prompt_context_set_from_prompt_context_contribution(
            memory_recall_prompt_context_contribution(recalled_city_snapshot())
                .expect("deterministic context contribution"),
        );

        let response = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                deterministic_context,
                "continue the previous work with the same constraints",
            ))
            .await
            .expect("active recall hook executes");

        assert!(response.contributions.is_empty());
        assert_eq!(provider.recall_call_count(), 0);
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.active_recall.deterministic_sufficient"
        }));
    }

    #[tokio::test]
    async fn phase_15_active_memory_hook_deduplicates_deterministic_ids() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            recalled_city_snapshot(),
        ));
        let hook = ActiveMemoryRecallHook {
            memory_provider: provider.clone(),
            decision_provider: None,
            config: MemoryActiveRecallConfig {
                mode: MemoryActiveRecallMode::StrictDebug,
                max_queries: 1,
                deterministic_sufficient_min_items: 99,
                ..MemoryActiveRecallConfig::default()
            },
        };
        let deterministic_context = prompt_context_set_from_prompt_context_contribution(
            memory_recall_prompt_context_contribution(recalled_city_snapshot())
                .expect("deterministic context contribution"),
        );

        let response = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                deterministic_context,
                "continue the previous memory-dependent task",
            ))
            .await
            .expect("active recall hook executes");

        assert_eq!(provider.recall_call_count(), 1);
        assert!(response.contributions.is_empty());
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.active_recall.no_hits"
                && diagnostic.message.as_str().contains("non-duplicate")
        }));
    }

    #[tokio::test]
    async fn phase_15_active_memory_hook_ignores_malformed_internal_json() {
        let provider = Arc::new(TestRecallMemoryProvider::with_recall(
            active_project_snapshot(),
        ));
        let hook = ActiveMemoryRecallHook {
            memory_provider: provider.clone(),
            decision_provider: Some(Arc::new(TestActiveMemoryDecisionProvider::json(
                "{not json",
            ))),
            config: MemoryActiveRecallConfig::default(),
        };

        let response = hook
            .execute(test_active_prompt_context_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                HookPromptContextSet::default(),
                "continue the architecture work using prior project decisions and constraints",
            ))
            .await
            .expect("malformed provider json is best-effort");

        assert!(response.contributions.is_empty());
        assert_eq!(provider.recall_call_count(), 0);
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .message
                .as_str()
                .contains("memory.active_recall.invalid_json")
        }));
    }

    #[tokio::test]
    async fn memory_prompt_contract_hook_renders_from_prompt_context_and_compile_input() {
        let recall_provider = Arc::new(TestRecallMemoryProvider::with_recall(
            recalled_city_snapshot(),
        ));
        let recall_hook = MemoryDeterministicRecallHook {
            memory_provider: recall_provider,
        };
        let recall_response = recall_hook
            .execute(test_prompt_context_hook_request(memory_policy_set(
                &MemoryTurnPolicy::normal_default_allow(),
            )))
            .await
            .expect("recall hook executes");
        let prompt_context_set = prompt_context_set_from_response(recall_response);
        let hook = MemoryPromptContractHook;

        let response = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                true,
                &[
                    MEMORY_FORGET_TOOL,
                    MEMORY_REMEMBER_TOOL,
                    MEMORY_GET_TOOL,
                    MEMORY_SEARCH_TOOL,
                ],
                prompt_context_set,
            ))
            .await
            .expect("prompt contract hook executes");

        let content = prompt_section_content(response).expect("prompt section is rendered");
        assert!(content.contains(
            "Available memory tools: memory_search, memory_get, memory_remember, memory_forget."
        ));
        assert!(content.contains("User likes Porto."));
        assert!(content.contains("Call memory_remember proactively"));
    }

    #[tokio::test]
    async fn phase_15_memory_prompt_contract_consumes_active_context_allowlist() {
        let hook = MemoryPromptContractHook;
        let active_context = memory_active_recall_prompt_context_contribution(
            active_project_snapshot().items,
            false,
            &MemoryActiveRecallConfig::default(),
        )
        .expect("active prompt context contribution");
        let unrelated_memory_context = PromptContextContribution {
            contribution_id: HookContributionId::new("memory.unrelated.context")
                .expect("valid contribution id"),
            domain: memory_policy_domain(),
            priority: 480,
            content: HookPromptContent::new("Unrelated memory-domain context must stay out.")
                .expect("valid prompt content"),
            max_chars: Some(500),
            source_refs: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
        };
        let prompt_context_set = HookPromptContextSet::aggregate_contributions(
            [active_context, unrelated_memory_context],
            HookPromptContextLimits::default(),
        );

        let response = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                true,
                &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
                prompt_context_set,
            ))
            .await
            .expect("prompt contract hook executes");

        let content = prompt_section_content(response).expect("prompt section is rendered");
        assert!(content.contains("Active memory context:"));
        assert!(content.contains("Use hooks for memory domains."));
        assert!(!content.contains("Unrelated memory-domain context"));
    }

    #[tokio::test]
    async fn phase_16_deterministic_only_recall_omits_active_heading() {
        let hook = MemoryPromptContractHook;
        let deterministic_context =
            memory_recall_prompt_context_contribution(recalled_city_snapshot())
                .expect("deterministic prompt context");
        let prompt_context_set = HookPromptContextSet::aggregate_contributions(
            [deterministic_context],
            HookPromptContextLimits::default(),
        );

        let response = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                true,
                &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
                prompt_context_set,
            ))
            .await
            .expect("prompt contract hook executes");

        let content = prompt_section_content(response).expect("prompt section is rendered");
        assert!(content.contains("Relevant memories:"));
        assert!(content.contains("User likes Porto."));
        assert!(!content.contains("Active memory context:"));
    }

    #[tokio::test]
    async fn phase_16_duplicate_active_memory_id_is_suppressed() {
        let hook = MemoryPromptContractHook;
        let deterministic_context =
            memory_recall_prompt_context_contribution(recalled_city_snapshot())
                .expect("deterministic prompt context");
        let active_context = memory_active_recall_prompt_context_contribution(
            recalled_city_snapshot().items,
            false,
            &MemoryActiveRecallConfig::default(),
        )
        .expect("active prompt context");
        let prompt_context_set = HookPromptContextSet::aggregate_contributions(
            [deterministic_context, active_context],
            HookPromptContextLimits::default(),
        );

        let response = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                true,
                &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
                prompt_context_set,
            ))
            .await
            .expect("prompt contract hook executes");
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.prompt_recall.dedup"
                && diagnostic.message.as_str().contains("active_raw_count=1")
                && diagnostic
                    .message
                    .as_str()
                    .contains("active_duplicate_count=1")
                && diagnostic
                    .message
                    .as_str()
                    .contains("active_rendered_count=0")
                && diagnostic
                    .message
                    .as_str()
                    .contains("active_duplicate_only=true")
        }));
        let content = prompt_section_content(response).expect("prompt section is rendered");
        assert_eq!(content.matches("User likes Porto.").count(), 1);
        assert!(!content.contains("Active memory context:"));
    }

    #[tokio::test]
    async fn phase_16_mixed_active_duplicates_keep_only_unique_context() {
        let hook = MemoryPromptContractHook;
        let deterministic_context =
            memory_recall_prompt_context_contribution(recalled_city_snapshot())
                .expect("deterministic prompt context");
        let mut active_items = recalled_city_snapshot().items;
        active_items.extend(active_project_snapshot().items);
        let active_context = memory_active_recall_prompt_context_contribution(
            active_items,
            false,
            &MemoryActiveRecallConfig::default(),
        )
        .expect("active prompt context");
        let prompt_context_set = HookPromptContextSet::aggregate_contributions(
            [deterministic_context, active_context],
            HookPromptContextLimits::default(),
        );

        let response = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                true,
                &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
                prompt_context_set,
            ))
            .await
            .expect("prompt contract hook executes");
        let content = prompt_section_content(response).expect("prompt section is rendered");

        assert_eq!(content.matches("User likes Porto.").count(), 1);
        assert!(content.contains("Active memory context:"));
        assert!(content.contains("Use hooks for memory domains."));
        let active_section = content
            .split("Active memory context:")
            .nth(1)
            .expect("active section should render");
        assert!(!active_section.contains("mem_city"));
    }

    #[tokio::test]
    async fn phase_16_exact_active_line_duplicate_is_suppressed() {
        let hook = MemoryPromptContractHook;
        let deterministic_context = PromptContextContribution {
            contribution_id: HookContributionId::new(MEMORY_DETERMINISTIC_RECALL_CONTRIBUTION_ID)
                .expect("valid contribution id"),
            domain: memory_policy_domain(),
            priority: 500,
            content: HookPromptContent::new("Shared synthesized line.")
                .expect("valid prompt content"),
            max_chars: Some(500),
            source_refs: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
        };
        let active_context = PromptContextContribution {
            contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
                .expect("valid contribution id"),
            domain: memory_policy_domain(),
            priority: 490,
            content: HookPromptContent::new("Shared synthesized line.")
                .expect("valid prompt content"),
            max_chars: Some(500),
            source_refs: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
        };
        let prompt_context_set = HookPromptContextSet::aggregate_contributions(
            [deterministic_context, active_context],
            HookPromptContextLimits::default(),
        );

        let response = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                true,
                &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
                prompt_context_set,
            ))
            .await
            .expect("prompt contract hook executes");
        let content = prompt_section_content(response).expect("prompt section is rendered");

        assert_eq!(content.matches("Shared synthesized line.").count(), 1);
        assert!(!content.contains("Active memory context:"));
    }

    #[tokio::test]
    async fn phase_16_active_synthesis_context_is_kept_when_unique() {
        let hook = MemoryPromptContractHook;
        let deterministic_context =
            memory_recall_prompt_context_contribution(recalled_city_snapshot())
                .expect("deterministic prompt context");
        let active_context = PromptContextContribution {
            contribution_id: HookContributionId::new(MEMORY_ACTIVE_RECALL_CONTRIBUTION_ID)
                .expect("valid contribution id"),
            domain: memory_policy_domain(),
            priority: 490,
            content: HookPromptContent::new("User is continuing Pioneer memory architecture work.")
                .expect("valid prompt content"),
            max_chars: Some(500),
            source_refs: vec![memory_source_ref("mem_city")],
            diagnostics: Vec::new(),
            truncated: false,
        };
        let prompt_context_set = HookPromptContextSet::aggregate_contributions(
            [deterministic_context, active_context],
            HookPromptContextLimits::default(),
        );

        let response = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                true,
                &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL],
                prompt_context_set,
            ))
            .await
            .expect("prompt contract hook executes");
        assert!(response.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "memory.prompt_recall.dedup"
                && diagnostic
                    .message
                    .as_str()
                    .contains("active_synthesis_rendered=true")
        }));
        let content = prompt_section_content(response).expect("prompt section is rendered");

        assert!(content.contains("Relevant memories:"));
        assert!(content.contains("Active memory context:"));
        assert!(content.contains("User is continuing Pioneer memory architecture work."));
    }

    #[tokio::test]
    async fn memory_prompt_contract_hook_policy_and_tool_visibility_matrix() {
        let hook = MemoryPromptContractHook;
        let prompt_context_set = HookPromptContextSet::default();

        let no_use = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::no_use()),
                true,
                &[MEMORY_SEARCH_TOOL],
                prompt_context_set.clone(),
            ))
            .await
            .expect("no-use policy is best-effort");
        assert!(prompt_section_content(no_use).is_none());

        let no_tools = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                true,
                &["exec_command"],
                prompt_context_set.clone(),
            ))
            .await
            .expect("no visible memory tools is best-effort");
        assert!(prompt_section_content(no_tools).is_none());

        let no_provider_tool_calling = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::normal_default_allow()),
                false,
                &[MEMORY_SEARCH_TOOL],
                prompt_context_set.clone(),
            ))
            .await
            .expect("provider tool-calling disabled is best-effort");
        assert!(prompt_section_content(no_provider_tool_calling).is_none());

        let malformed = hook
            .execute(test_prompt_compile_hook_request(
                malformed_memory_policy_set(),
                true,
                &[MEMORY_SEARCH_TOOL],
                prompt_context_set,
            ))
            .await
            .expect("malformed policy is best-effort");
        assert!(prompt_section_content(malformed).is_none());
    }

    #[tokio::test]
    async fn memory_prompt_contract_hook_renders_no_save_and_forget_contracts() {
        let hook = MemoryPromptContractHook;

        let no_save = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::no_save()),
                true,
                &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
                HookPromptContextSet::default(),
            ))
            .await
            .expect("no-save prompt contract executes");
        let no_save_content = prompt_section_content(no_save).expect("no-save section renders");
        assert!(no_save_content.contains("Memory writes are disabled for this turn"));
        assert!(no_save_content.contains("Do not store, update, infer, or extract new memories"));
        assert!(!no_save_content.contains("memory_remember"));

        let forget = hook
            .execute(test_prompt_compile_hook_request(
                memory_policy_set(&MemoryTurnPolicy::explicit_forget(Some(
                    "birthday".to_owned(),
                ))),
                true,
                &[MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL],
                HookPromptContextSet::default(),
            ))
            .await
            .expect("forget prompt contract executes");
        let forget_content = prompt_section_content(forget).expect("forget section renders");
        assert!(
            forget_content
                .contains("If the user asks you to forget something, call memory_forget.")
        );
        assert!(forget_content.contains("only to identify and forget"));
        assert!(!forget_content.contains("memory_remember"));
    }

    #[tokio::test]
    async fn memory_policy_classifier_hook_uses_metadata_structured_override() {
        struct PanickingProvider;

        #[async_trait::async_trait]
        impl AgentMemoryTurnPolicyProvider for PanickingProvider {
            async fn resolve_memory_turn_policy(
                &self,
                _context: MemoryTurnPolicyContext,
                _request: MemoryTurnPolicyRequest,
            ) -> Result<MemoryTurnPolicy, String> {
                panic!("structured override should bypass classifier provider")
            }
        }

        let hook = MemoryPolicyClassifierHook {
            policy_provider: Some(Arc::new(PanickingProvider)),
            state: Arc::new(MemoryHookTurnStateStore::default()),
        };
        let mut metadata = HookMetadata::default();
        metadata.insert(
            hook_metadata_key(MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY),
            memory_turn_policy_to_hook_value(&MemoryTurnPolicy::no_use()),
        );

        let response = hook
            .execute(test_policy_hook_request(metadata))
            .await
            .expect("hook executes");

        let contribution = response
            .contributions
            .into_iter()
            .find_map(|contribution| match contribution {
                HookContribution::Policy(policy) => Some(policy),
                _ => None,
            })
            .expect("policy contribution exists");
        let policy = memory_turn_policy_from_hook_value(&contribution.value)
            .expect("policy contribution decodes");

        assert_eq!(policy.source, MemoryPolicySource::StructuredOverride);
        assert!(!policy.allow_pre_turn_recall());
        assert!(!policy.allows_any_memory_tool());
    }

    #[tokio::test]
    async fn memory_policy_classifier_hook_accepts_structured_override_variants() {
        let variants = vec![
            MemoryTurnPolicy::no_use(),
            MemoryTurnPolicy::no_save(),
            MemoryTurnPolicy::explicit_remember(),
            MemoryTurnPolicy::explicit_forget(Some("birthday".to_owned())),
        ];

        for expected in variants {
            let hook = MemoryPolicyClassifierHook {
                policy_provider: None,
                state: Arc::new(MemoryHookTurnStateStore::default()),
            };
            let mut metadata = HookMetadata::default();
            metadata.insert(
                hook_metadata_key(MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY),
                memory_turn_policy_to_hook_value(&expected),
            );

            let response = hook
                .execute(test_policy_hook_request(metadata))
                .await
                .expect("hook executes");
            let contribution = response
                .contributions
                .into_iter()
                .find_map(|contribution| match contribution {
                    HookContribution::Policy(policy) => Some(policy),
                    _ => None,
                })
                .expect("policy contribution exists");
            let policy = memory_turn_policy_from_hook_value(&contribution.value)
                .expect("policy contribution decodes");

            assert_eq!(policy.recall, expected.recall);
            assert_eq!(policy.prompt, expected.prompt);
            assert_eq!(policy.read_tools, expected.read_tools);
            assert_eq!(policy.remember_tool, expected.remember_tool);
            assert_eq!(policy.forget_tool, expected.forget_tool);
            assert_eq!(policy.post_turn_extraction, expected.post_turn_extraction);
            assert_eq!(policy.active_memory, expected.active_memory);
            assert_eq!(policy.explicit_remember, expected.explicit_remember);
            assert_eq!(policy.explicit_forget, expected.explicit_forget);
            assert_eq!(policy.forget_target_hint, expected.forget_target_hint);
            assert_eq!(policy.source, MemoryPolicySource::StructuredOverride);
            assert_eq!(
                policy.reason_code,
                MemoryPolicyReasonCode::StructuredOverride
            );
        }
    }

    #[tokio::test]
    async fn structured_override_wins_over_classifier() {
        struct AllowAllProvider;

        #[async_trait::async_trait]
        impl AgentMemoryTurnPolicyProvider for AllowAllProvider {
            async fn resolve_memory_turn_policy(
                &self,
                _context: MemoryTurnPolicyContext,
                _request: MemoryTurnPolicyRequest,
            ) -> Result<MemoryTurnPolicy, String> {
                Ok(MemoryTurnPolicy::normal_default_allow())
            }
        }

        let provider: Arc<dyn AgentMemoryTurnPolicyProvider> = Arc::new(AllowAllProvider);
        let policy = resolve_memory_turn_policy(
            Some(&provider),
            MemoryTurnPolicyContext {
                workspace_id: "ws".to_owned(),
                thread_id: "thr".to_owned(),
                turn_id: "turn".to_owned(),
                mode: ThreadMode::Agent,
                input_text: "anything".to_owned(),
                model: None,
                model_provider: None,
            },
            MemoryTurnPolicyRequest {
                structured_override: Some(
                    MemoryTurnPolicyOverride::new(MemoryTurnPolicy::no_use()),
                ),
                ..MemoryTurnPolicyRequest::default()
            },
        )
        .await;

        assert_eq!(policy.source, MemoryPolicySource::StructuredOverride);
        assert!(!policy.allows_any_memory_tool());
        assert!(!policy.allow_pre_turn_recall());
    }

    #[tokio::test]
    async fn classifier_error_uses_default_allow_fallback() {
        struct FailingProvider;

        #[async_trait::async_trait]
        impl AgentMemoryTurnPolicyProvider for FailingProvider {
            async fn resolve_memory_turn_policy(
                &self,
                _context: MemoryTurnPolicyContext,
                _request: MemoryTurnPolicyRequest,
            ) -> Result<MemoryTurnPolicy, String> {
                Err("invalid json".to_owned())
            }
        }

        let provider: Arc<dyn AgentMemoryTurnPolicyProvider> = Arc::new(FailingProvider);
        let policy = resolve_memory_turn_policy(
            Some(&provider),
            MemoryTurnPolicyContext {
                workspace_id: "ws".to_owned(),
                thread_id: "thr".to_owned(),
                turn_id: "turn".to_owned(),
                mode: ThreadMode::Agent,
                input_text: "hola".to_owned(),
                model: None,
                model_provider: None,
            },
            MemoryTurnPolicyRequest::default(),
        )
        .await;

        assert_eq!(policy.source, MemoryPolicySource::DefaultFallback);
        assert_eq!(
            policy.reason_code,
            MemoryPolicyReasonCode::ClassifierInvalidJson
        );
        assert!(policy.allows_memory_tool(MEMORY_REMEMBER_TOOL));
        assert!(policy.allow_pre_turn_recall());
        assert!(
            policy
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.contains("classifier_failed"))
        );
    }

    #[test]
    fn memory_tool_filtering_applies_turn_policy() {
        let materialization = MemoryToolMaterialization {
            bundles: vec![pioneer_tools::ToolExtensionBundle {
                specs: [
                    MEMORY_SEARCH_TOOL,
                    MEMORY_GET_TOOL,
                    MEMORY_REMEMBER_TOOL,
                    MEMORY_FORGET_TOOL,
                ]
                .into_iter()
                .map(test_tool_spec)
                .collect(),
                handlers: [
                    MEMORY_SEARCH_TOOL,
                    MEMORY_GET_TOOL,
                    MEMORY_REMEMBER_TOOL,
                    MEMORY_FORGET_TOOL,
                ]
                .into_iter()
                .map(|name| {
                    (
                        name.to_owned(),
                        Arc::new(TestToolHandler) as Arc<dyn pioneer_tools::ToolHandler>,
                    )
                })
                .collect(),
            }],
            diagnostics: Vec::new(),
        };

        let filtered = filter_memory_tool_materialization(
            materialization,
            &MemoryTurnPolicy::explicit_forget(None),
        );
        assert_eq!(
            memory_tool_names(&filtered),
            vec![MEMORY_SEARCH_TOOL, MEMORY_GET_TOOL, MEMORY_FORGET_TOOL]
        );
        assert!(
            filtered
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.contains(MEMORY_REMEMBER_TOOL))
        );
    }

    #[test]
    fn memory_recall_prompt_input_maps_domain_snapshot_to_prompt_dto() {
        let input = memory_recall_prompt_input(
            vec!["memory_search".to_owned()],
            MemoryRecallPromptPolicy::Full,
            MemoryRecallSnapshot {
                items: vec![MemoryRecallItem {
                    memory_id: "mem_123".to_owned(),
                    scope: user_scope(),
                    category: MemoryCategory::Identity,
                    key: Some("name".to_owned()),
                    content: "User's name is Alexander.".to_owned(),
                    score: Some(1.0),
                    updated_at: 1_714_867_200,
                }],
                diagnostics: vec!["internal diagnostic must not leak".to_owned()],
                truncated: true,
            },
        );

        assert_eq!(input.available_tool_names, vec!["memory_search"]);
        assert_eq!(input.policy, MemoryRecallPromptPolicy::Full);
        assert!(input.truncated);
        assert_eq!(input.recalled_items.len(), 1);
        let item = &input.recalled_items[0];
        assert_eq!(item.memory_id, "mem_123");
        assert_eq!(item.scope_label, "user");
        assert_eq!(item.category_label, "identity");
        assert_eq!(item.key.as_deref(), Some("name"));
        assert_eq!(item.content, "User's name is Alexander.");
        assert_eq!(item.score, Some(1.0));
        assert_eq!(item.updated_at_label, "2024-05-05");
    }

    #[test]
    fn memory_prompt_scope_labels_keep_domain_resolution_in_agent() {
        assert_eq!(scope_label(&user_scope()), "user");
        assert_eq!(
            scope_label(&MemoryScope {
                kind: MemoryScopeKind::Workspace,
                key: "ws_123".to_owned(),
            }),
            "workspace:ws_123"
        );
        assert_eq!(
            scope_label(&MemoryScope {
                kind: MemoryScopeKind::Agent,
                key: "agent_123".to_owned(),
            }),
            "agent:agent_123"
        );
    }

    fn test_policy_hook_request(metadata: HookMetadata) -> HookHandlerRequest {
        HookHandlerRequest {
            hook_id: HookId::new(MEMORY_POLICY_CLASSIFIER_HOOK_ID)
                .expect("static hook id is valid"),
            phase: HookPhase::TurnPrePolicy,
            context: HookContext {
                workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
                thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
                turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
                metadata,
                ..HookContext::default()
            },
            input: HookInput::turn_pre_policy(TurnPrePolicyHookInput::from_parts(
                "No guardes esto.",
                Some("test-model"),
                Some("test-provider"),
            )),
            policy_set: HookPolicySet::empty(),
            prompt_context_set: HookPromptContextSet::default(),
        }
    }

    fn test_tool_bundle_hook_request(
        policy_set: HookPolicySet,
        provider_tool_calling: bool,
    ) -> HookHandlerRequest {
        HookHandlerRequest {
            hook_id: HookId::new(MEMORY_TOOL_BUNDLE_HOOK_ID).expect("static hook id is valid"),
            phase: HookPhase::TurnPreToolMaterialization,
            context: HookContext {
                workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
                thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
                turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
                ..HookContext::default()
            },
            input: HookInput::turn_pre_tool_materialization(
                TurnPreToolMaterializationHookInput::from_parts(provider_tool_calling, Vec::new()),
            ),
            policy_set,
            prompt_context_set: HookPromptContextSet::default(),
        }
    }

    fn test_prompt_context_hook_request(policy_set: HookPolicySet) -> HookHandlerRequest {
        HookHandlerRequest {
            hook_id: HookId::new(MEMORY_DETERMINISTIC_RECALL_HOOK_ID)
                .expect("static hook id is valid"),
            phase: HookPhase::TurnPrePromptContext,
            context: HookContext {
                workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
                thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
                turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
                ..HookContext::default()
            },
            input: HookInput::turn_pre_prompt_context(TurnPrePromptContextHookInput::from_parts(
                "what do you remember about my city?",
                Some("test-model"),
                Some("test-provider"),
            )),
            policy_set,
            prompt_context_set: HookPromptContextSet::default(),
        }
    }

    fn test_active_prompt_context_hook_request(
        policy_set: HookPolicySet,
        prompt_context_set: HookPromptContextSet,
        input_text: &str,
    ) -> HookHandlerRequest {
        HookHandlerRequest {
            hook_id: HookId::new(MEMORY_ACTIVE_RECALL_HOOK_ID).expect("static hook id is valid"),
            phase: HookPhase::TurnPrePromptContext,
            context: HookContext {
                workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
                thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
                turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
                ..HookContext::default()
            },
            input: HookInput::turn_pre_prompt_context(TurnPrePromptContextHookInput::from_parts(
                input_text,
                Some("test-model"),
                Some("test-provider"),
            )),
            policy_set,
            prompt_context_set,
        }
    }

    fn test_prompt_compile_hook_request(
        policy_set: HookPolicySet,
        provider_tool_calling: bool,
        available_tool_names: &[&str],
        prompt_context_set: HookPromptContextSet,
    ) -> HookHandlerRequest {
        HookHandlerRequest {
            hook_id: HookId::new(MEMORY_PROMPT_CONTRACT_HOOK_ID).expect("static hook id is valid"),
            phase: HookPhase::TurnPrePromptCompile,
            context: HookContext {
                workspace_id: Some(HookWorkspaceId::new("ws").expect("valid workspace id")),
                thread_id: Some(HookThreadId::new("thr").expect("valid thread id")),
                turn_id: Some(HookTurnId::new("turn").expect("valid turn id")),
                ..HookContext::default()
            },
            input: HookInput::turn_pre_prompt_compile(TurnPrePromptCompileHookInput::from_parts(
                provider_tool_calling,
                available_tool_names
                    .iter()
                    .map(|name| HookToolName::new(*name).expect("valid tool name"))
                    .collect(),
            )),
            policy_set,
            prompt_context_set,
        }
    }

    fn memory_policy_set(policy: &MemoryTurnPolicy) -> HookPolicySet {
        HookPolicySet::merge_contributions([memory_policy_contribution(policy)])
    }

    fn malformed_memory_policy_set() -> HookPolicySet {
        HookPolicySet::merge_contributions([PolicyContribution {
            domain: memory_policy_domain(),
            key: memory_turn_policy_key(),
            value: HookValue::Text("memory_no_use".to_owned()),
            priority: 500,
            diagnostics: Vec::new(),
        }])
    }

    fn recalled_city_snapshot() -> MemoryRecallSnapshot {
        MemoryRecallSnapshot {
            items: vec![MemoryRecallItem {
                memory_id: "mem_city".to_owned(),
                scope: user_scope(),
                category: MemoryCategory::Preference,
                key: Some("city".to_owned()),
                content: "User likes Porto.".to_owned(),
                score: Some(0.91),
                updated_at: 1_714_867_200,
            }],
            diagnostics: Vec::new(),
            truncated: false,
        }
    }

    fn active_project_snapshot() -> MemoryRecallSnapshot {
        MemoryRecallSnapshot {
            items: vec![MemoryRecallItem {
                memory_id: "mem_active_project".to_owned(),
                scope: user_scope(),
                category: MemoryCategory::ProjectDecision,
                key: Some("hooks".to_owned()),
                content: "Use hooks for memory domains.".to_owned(),
                score: Some(0.88),
                updated_at: 1_714_867_200,
            }],
            diagnostics: Vec::new(),
            truncated: false,
        }
    }

    fn prompt_context_set_from_response(response: HookHandlerResponse) -> HookPromptContextSet {
        HookPromptContextSet::aggregate_hook_contributions(
            response.contributions,
            HookPromptContextLimits::default(),
        )
    }

    fn prompt_context_set_from_prompt_context_contribution(
        contribution: PromptContextContribution,
    ) -> HookPromptContextSet {
        HookPromptContextSet::aggregate_contributions(
            [contribution],
            HookPromptContextLimits::default(),
        )
    }

    fn memory_source_ref(memory_id: &str) -> HookSourceRef {
        HookSourceRef {
            kind: HookSourceKind::Custom("memory".to_owned()),
            id: HookSourceId::new(memory_id.to_owned()).expect("valid memory source id"),
            label: None,
        }
    }

    fn prompt_section_content(response: HookHandlerResponse) -> Option<String> {
        response.contributions.into_iter().find_map(|contribution| {
            let HookContribution::PromptSection(section) = contribution else {
                return None;
            };
            Some(section.content.as_str().to_owned())
        })
    }

    fn test_memory_turn_context() -> MemoryTurnContext {
        MemoryTurnContext {
            workspace_id: "ws".to_owned(),
            thread_id: "thr".to_owned(),
            turn_id: "turn".to_owned(),
            mode: ThreadMode::Agent,
            input_text: "remember my preference".to_owned(),
            task_id: None,
            agent_id: None,
        }
    }

    fn test_tool_spec(name: &str) -> ConfiguredToolSpec {
        ConfiguredToolSpec::new(
            ToolSpec::new(
                name,
                "test memory tool",
                json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                PayloadKind::Function,
            ),
            ExecutionClass::Shared,
            pioneer_tools::dynamic_unknown_output_policy(),
        )
    }

    fn test_memory_tool_bundle(names: &[&str]) -> ToolExtensionBundle {
        ToolExtensionBundle {
            specs: names.iter().map(|name| test_tool_spec(name)).collect(),
            handlers: names
                .iter()
                .map(|name| {
                    (
                        (*name).to_owned(),
                        Arc::new(TestToolHandler) as Arc<dyn pioneer_tools::ToolHandler>,
                    )
                })
                .collect(),
        }
    }

    fn empty_tool_materialization() -> MemoryToolMaterialization {
        MemoryToolMaterialization {
            bundles: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    fn standard_tool_materialization() -> MemoryToolMaterialization {
        MemoryToolMaterialization {
            bundles: vec![test_memory_tool_bundle(&[
                MEMORY_SEARCH_TOOL,
                MEMORY_GET_TOOL,
                MEMORY_REMEMBER_TOOL,
                MEMORY_FORGET_TOOL,
            ])],
            diagnostics: Vec::new(),
        }
    }

    fn panicking_handler_tool_materialization() -> MemoryToolMaterialization {
        let handler: Arc<dyn pioneer_tools::ToolHandler> = Arc::new(PanickingToolHandler);
        MemoryToolMaterialization {
            bundles: vec![ToolExtensionBundle {
                specs: [
                    MEMORY_SEARCH_TOOL,
                    MEMORY_GET_TOOL,
                    MEMORY_REMEMBER_TOOL,
                    MEMORY_FORGET_TOOL,
                ]
                .into_iter()
                .map(test_tool_spec)
                .collect(),
                handlers: [
                    MEMORY_SEARCH_TOOL,
                    MEMORY_GET_TOOL,
                    MEMORY_REMEMBER_TOOL,
                    MEMORY_FORGET_TOOL,
                ]
                .into_iter()
                .map(|name| (name.to_owned(), handler.clone()))
                .collect(),
            }],
            diagnostics: Vec::new(),
        }
    }

    fn response_tool_names(response: &HookHandlerResponse) -> Vec<&'static str> {
        response
            .contributions
            .iter()
            .flat_map(|contribution| match contribution {
                HookContribution::ToolBundle(bundle) => {
                    hook_tool_names_to_static(&bundle.tool_names)
                }
                _ => Vec::new(),
            })
            .collect()
    }

    fn hook_tool_names_to_static(tool_names: &[HookToolName]) -> Vec<&'static str> {
        tool_names
            .iter()
            .filter_map(|name| match name.as_str() {
                MEMORY_SEARCH_TOOL => Some(MEMORY_SEARCH_TOOL),
                MEMORY_GET_TOOL => Some(MEMORY_GET_TOOL),
                MEMORY_REMEMBER_TOOL => Some(MEMORY_REMEMBER_TOOL),
                MEMORY_FORGET_TOOL => Some(MEMORY_FORGET_TOOL),
                _ => None,
            })
            .collect()
    }

    fn hook_tool_names_to_strings(tool_names: &[HookToolName]) -> Vec<&str> {
        tool_names.iter().map(|name| name.as_str()).collect()
    }

    struct TestMemoryProvider {
        materialization: Result<MemoryToolMaterialization, String>,
        materialize_calls: Arc<Mutex<usize>>,
    }

    impl TestMemoryProvider {
        fn with_materialization(materialization: MemoryToolMaterialization) -> Self {
            Self {
                materialization: Ok(materialization),
                materialize_calls: Arc::new(Mutex::new(0)),
            }
        }

        fn failing(error: impl Into<String>) -> Self {
            Self {
                materialization: Err(error.into()),
                materialize_calls: Arc::new(Mutex::new(0)),
            }
        }

        fn materialize_call_count(&self) -> usize {
            *self
                .materialize_calls
                .lock()
                .expect("materialize call count lock poisoned")
        }
    }

    struct TestRecallMemoryProvider {
        recall_result: Result<MemoryRecallSnapshot, String>,
        recall_calls: Arc<Mutex<usize>>,
        recall_requests: Arc<Mutex<Vec<MemoryRecallRequest>>>,
        materialize_calls: Arc<Mutex<usize>>,
    }

    impl TestRecallMemoryProvider {
        fn with_recall(recall_result: MemoryRecallSnapshot) -> Self {
            Self {
                recall_result: Ok(recall_result),
                recall_calls: Arc::new(Mutex::new(0)),
                recall_requests: Arc::new(Mutex::new(Vec::new())),
                materialize_calls: Arc::new(Mutex::new(0)),
            }
        }

        fn failing_recall(error: impl Into<String>) -> Self {
            Self {
                recall_result: Err(error.into()),
                recall_calls: Arc::new(Mutex::new(0)),
                recall_requests: Arc::new(Mutex::new(Vec::new())),
                materialize_calls: Arc::new(Mutex::new(0)),
            }
        }

        fn recall_call_count(&self) -> usize {
            *self.recall_calls.lock().expect("recall lock poisoned")
        }

        fn materialize_call_count(&self) -> usize {
            *self
                .materialize_calls
                .lock()
                .expect("materialize lock poisoned")
        }

        fn recall_requests(&self) -> Vec<MemoryRecallRequest> {
            self.recall_requests
                .lock()
                .expect("recall request lock poisoned")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl AgentMemoryProvider for TestRecallMemoryProvider {
        async fn recall_memory(
            &self,
            _context: MemoryTurnContext,
            request: MemoryRecallRequest,
        ) -> Result<MemoryRecallSnapshot, String> {
            *self.recall_calls.lock().expect("recall lock poisoned") += 1;
            self.recall_requests
                .lock()
                .expect("recall request lock poisoned")
                .push(request);
            self.recall_result.clone()
        }

        async fn materialize_memory_tools(
            &self,
            _context: MemoryTurnContext,
        ) -> Result<MemoryToolMaterialization, String> {
            *self
                .materialize_calls
                .lock()
                .expect("materialize lock poisoned") += 1;
            Ok(empty_tool_materialization())
        }
    }

    #[async_trait::async_trait]
    impl AgentMemoryProvider for TestMemoryProvider {
        async fn recall_memory(
            &self,
            _context: MemoryTurnContext,
            _request: MemoryRecallRequest,
        ) -> Result<MemoryRecallSnapshot, String> {
            Ok(MemoryRecallSnapshot::empty())
        }

        async fn materialize_memory_tools(
            &self,
            _context: MemoryTurnContext,
        ) -> Result<MemoryToolMaterialization, String> {
            *self
                .materialize_calls
                .lock()
                .expect("materialize call count lock poisoned") += 1;
            self.materialization.clone()
        }
    }

    struct SlowRecallMemoryProvider;

    #[async_trait::async_trait]
    impl AgentMemoryProvider for SlowRecallMemoryProvider {
        async fn recall_memory(
            &self,
            _context: MemoryTurnContext,
            _request: MemoryRecallRequest,
        ) -> Result<MemoryRecallSnapshot, String> {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(active_project_snapshot())
        }

        async fn materialize_memory_tools(
            &self,
            _context: MemoryTurnContext,
        ) -> Result<MemoryToolMaterialization, String> {
            panic!("active memory recall timeout test must not materialize tools")
        }
    }

    struct TestActiveMemoryDecisionProvider {
        json: String,
    }

    impl TestActiveMemoryDecisionProvider {
        fn json(json: impl Into<String>) -> Self {
            Self { json: json.into() }
        }
    }

    #[async_trait::async_trait]
    impl AgentActiveMemoryDecisionProvider for TestActiveMemoryDecisionProvider {
        async fn resolve_active_memory_decision_json(
            &self,
            _context: MemoryActiveRecallDecisionContext,
            _request: MemoryActiveRecallDecisionRequest,
        ) -> Result<String, String> {
            Ok(self.json.clone())
        }
    }

    struct PanickingToolHandler;

    #[async_trait::async_trait]
    impl pioneer_tools::ToolHandler for PanickingToolHandler {
        async fn handle(
            &self,
            _invocation: pioneer_tools::ToolInvocation,
            _trace: pioneer_tools::ToolEventTrace,
        ) -> Result<Box<dyn pioneer_tools::ToolOutput>, pioneer_tools::ToolError> {
            panic!("tool materialization must not execute memory tool handlers")
        }
    }

    struct TestToolHandler;

    #[async_trait::async_trait]
    impl pioneer_tools::ToolHandler for TestToolHandler {
        async fn handle(
            &self,
            _invocation: pioneer_tools::ToolInvocation,
            _trace: pioneer_tools::ToolEventTrace,
        ) -> Result<Box<dyn pioneer_tools::ToolOutput>, pioneer_tools::ToolError> {
            Ok(Box::new(pioneer_tools::FunctionToolOutput::new("ok", true)))
        }
    }
}
