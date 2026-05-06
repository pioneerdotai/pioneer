use crate::hooks::AgentToolBundleArtifactStore;
use chrono::{DateTime, Utc};
use pioneer_hooks::HookHandler;
use pioneer_hooks::{
    HookAwaitPolicy, HookCapabilities, HookCapability, HookContribution, HookContributionId,
    HookDiagnostic, HookDiagnosticCode, HookDiagnosticMessage, HookDiagnosticSeverity, HookDomain,
    HookError, HookExecutionPolicy, HookFailurePolicy, HookHandlerRequest, HookHandlerResponse,
    HookId, HookInputPayload, HookKind, HookMetadata, HookMetadataKey, HookPhase, HookPolicyKey,
    HookPolicySet, HookPromptContent, HookRegistryError, HookResult, HookRuntime, HookSectionId,
    HookSubscription, HookSubscriptionId, HookToolBundleId, HookToolName, HookValue,
    PolicyContribution, PromptSectionContribution, ToolBundleContribution, TurnPrePolicyHookInput,
};
use pioneer_promt::{
    MemoryRecallPromptInput, MemoryRecallPromptItem, MemoryRecallPromptPolicy,
    render_memory_recall_prompt,
};
use pioneer_protocol::{MemoryCategory, MemoryScope, MemoryScopeKind, ThreadMode};
use pioneer_tools::ToolExtensionBundle;
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
            active_memory: MemoryActiveContextPolicy::Disabled,
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
            active_memory: MemoryActiveContextPolicy::Disabled,
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

#[derive(Clone)]
struct MemoryHookTurnState {
    context: MemoryTurnContext,
    available_tool_names: Vec<String>,
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
                MemoryHookTurnState {
                    context,
                    available_tool_names: Vec::new(),
                },
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

    fn set_available_tool_names(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        available_tool_names: Vec<String>,
    ) {
        if let Ok(mut states) = self.states.lock()
            && let Some(state) =
                states.get_mut(&memory_hook_state_key(workspace_id, thread_id, turn_id))
        {
            state.available_tool_names = available_tool_names;
        }
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
) -> Result<(), HookRegistryError> {
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
        Arc::new(MemoryToolMaterializationHook {
            memory_provider: memory_provider.clone(),
            state: state.clone(),
            tool_bundle_artifacts,
        }),
        "memory.tool_materialization.default",
        HookPhase::TurnPreToolMaterialization,
        0,
    )?;
    register_memory_hook_handler(
        runtime,
        Arc::new(MemoryPromptSectionHook {
            memory_provider,
            state,
        }),
        "memory.prompt_section.default",
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
                .with_execution_policy(HookExecutionPolicy {
                    await_policy: HookAwaitPolicy::Blocking,
                    timeout_ms: None,
                    max_parallelism: None,
                })
                .with_failure_policy(HookFailurePolicy::BestEffort),
        )?;
    }
    Ok(())
}

struct MemoryPolicyClassifierHook {
    policy_provider: Option<Arc<dyn AgentMemoryTurnPolicyProvider>>,
    state: Arc<MemoryHookTurnStateStore>,
}

struct MemoryToolMaterializationHook {
    memory_provider: Arc<dyn AgentMemoryProvider>,
    state: Arc<MemoryHookTurnStateStore>,
    tool_bundle_artifacts: Arc<AgentToolBundleArtifactStore>,
}

struct MemoryPromptSectionHook {
    memory_provider: Arc<dyn AgentMemoryProvider>,
    state: Arc<MemoryHookTurnStateStore>,
}

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
impl HookHandler for MemoryToolMaterializationHook {
    fn id(&self) -> HookId {
        HookId::new("memory.tool_materialization").expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPreToolMaterialization]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_hook_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let Some(state) = self.state.state(&request) else {
            return Ok(memory_missing_state_response("memory.tool_materialization"));
        };
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_hook_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory tool materialization skipped: {error}"),
                ));
                return Ok(response);
            }
            None => {
                return Ok(memory_missing_policy_response(
                    "memory.tool_materialization",
                ));
            }
        };
        if !turn_pre_tool_materialization_allows_tools(&request) || !policy.allows_any_memory_tool()
        {
            return Ok(HookHandlerResponse::default());
        }

        let mut materialization = match self
            .memory_provider
            .materialize_memory_tools(state.context.clone())
            .await
        {
            Ok(materialization) => filter_memory_tool_materialization(materialization, &policy),
            Err(error) => {
                let mut response = HookHandlerResponse::default();
                response
                    .diagnostics
                    .push(memory_hook_diagnostic("memory.tools_failed", error));
                return Ok(response);
            }
        };

        let available_tool_names = memory_tool_names(&materialization);
        self.state.set_available_tool_names(
            state.context.workspace_id.as_str(),
            state.context.thread_id.as_str(),
            state.context.turn_id.as_str(),
            available_tool_names,
        );

        let mut response = HookHandlerResponse::default();
        response.diagnostics.extend(hook_diagnostics_from_strings(
            materialization.diagnostics.as_slice(),
        ));
        for (index, bundle) in materialization.bundles.drain(..).enumerate() {
            let bundle_id = HookToolBundleId::new(format!("memory.runtime.bundle.{index}"))
                .expect("static bundle id is valid");
            self.tool_bundle_artifacts.insert(
                state.context.turn_id.clone(),
                bundle_id.clone(),
                bundle.clone(),
            );
            response.contributions.push(HookContribution::ToolBundle(
                memory_tool_bundle_contribution(index, bundle_id, &bundle),
            ));
        }
        Ok(response)
    }
}

#[async_trait::async_trait]
impl HookHandler for MemoryPromptSectionHook {
    fn id(&self) -> HookId {
        HookId::new("memory.prompt_section").expect("static hook id is valid")
    }

    fn kind(&self) -> HookKind {
        HookKind::new("memory").expect("static hook kind is valid")
    }

    fn supported_phases(&self) -> Vec<HookPhase> {
        vec![HookPhase::TurnPrePromptCompile]
    }

    fn capabilities(&self) -> HookCapabilities {
        memory_hook_capabilities()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
        let Some(state) = self.state.state(&request) else {
            return Ok(memory_missing_state_response("memory.prompt_section"));
        };
        let policy = match memory_turn_policy_from_hook_policy_set(&request.policy_set) {
            Some(Ok(policy)) => policy,
            Some(Err(error)) => {
                let mut response = HookHandlerResponse::default();
                response.diagnostics.push(memory_hook_diagnostic(
                    "memory.policy_decode_failed",
                    format!("memory prompt section skipped: {error}"),
                ));
                return Ok(response);
            }
            None => return Ok(memory_missing_policy_response("memory.prompt_section")),
        };
        if state.available_tool_names.is_empty() {
            return Ok(HookHandlerResponse::default());
        }

        let mut response = HookHandlerResponse::default();
        let recall_snapshot = if policy.allow_pre_turn_recall() {
            match self
                .memory_provider
                .recall_memory(
                    state.context.clone(),
                    memory_recall_request(state.context.input_text.as_str()),
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
                    response
                        .diagnostics
                        .push(memory_hook_diagnostic("memory.recall_failed", error));
                    MemoryRecallSnapshot::empty()
                }
            }
        } else {
            MemoryRecallSnapshot::empty()
        };

        if policy.allow_memory_prompt()
            && let Some(prompt_policy) = policy.recall_prompt_policy()
            && let Some(contribution) = memory_recall_prompt_section_contribution(
                state.available_tool_names,
                prompt_policy,
                recall_snapshot,
            )
        {
            response.contributions.push(contribution);
        }
        Ok(response)
    }
}

fn memory_hook_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("write_domain_context").expect("static capability is valid"),
        HookCapability::new("call_provider").expect("static capability is valid"),
        HookCapability::new("call_tools").expect("static capability is valid"),
        HookCapability::new("contribute_policy").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_section").expect("static capability is valid"),
        HookCapability::new("contribute_tool_bundle").expect("static capability is valid"),
    ])
}

fn memory_policy_classifier_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("call_provider").expect("static capability is valid"),
        HookCapability::new("contribute_policy").expect("static capability is valid"),
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

fn turn_pre_tool_materialization_allows_tools(request: &HookHandlerRequest) -> bool {
    match &request.input.payload {
        HookInputPayload::TurnPreToolMaterialization(input) => input.provider_tool_calling,
        _ => false,
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
) -> ToolBundleContribution {
    ToolBundleContribution {
        contribution_id: HookContributionId::new(format!("memory.runtime.contribution.{index}"))
            .expect("static contribution id is valid"),
        bundle_id,
        domain: HookDomain::new("memory").expect("static domain is valid"),
        priority: 100,
        tool_names: bundle
            .specs
            .iter()
            .filter_map(|configured| HookToolName::new(configured.spec.name.clone()).ok())
            .collect(),
        diagnostics: Vec::new(),
    }
}

fn memory_recall_request(input_text: &str) -> MemoryRecallRequest {
    MemoryRecallRequest {
        query: input_text.to_owned(),
        categories: Vec::new(),
        top_k: Some(5),
        max_chars: Some(1_500),
    }
}

fn hook_diagnostics_from_strings(messages: &[String]) -> Vec<HookDiagnostic> {
    messages
        .iter()
        .map(|message| memory_hook_diagnostic("memory.diagnostic", message.clone()))
        .collect()
}

fn memory_missing_state_response(hook: &'static str) -> HookHandlerResponse {
    let mut response = HookHandlerResponse::default();
    response.diagnostics.push(memory_hook_diagnostic(
        "memory.missing_state",
        format!("{hook} skipped because memory turn policy state was unavailable"),
    ));
    response
}

fn memory_missing_policy_response(hook: &'static str) -> HookHandlerResponse {
    let mut response = HookHandlerResponse::default();
    response.diagnostics.push(memory_hook_diagnostic(
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
    safe
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
            "memory.policy.tools_filtered: reason={} removed={}",
            policy.reason_code.as_str(),
            removed_tools.join(",")
        ));
    }

    MemoryToolMaterialization {
        bundles,
        diagnostics,
    }
}

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
        truncated: recall_snapshot.truncated,
    }
}

pub(crate) fn memory_recall_prompt_section_contribution(
    available_tool_names: Vec<String>,
    policy: MemoryRecallPromptPolicy,
    recall_snapshot: MemoryRecallSnapshot,
) -> Option<HookContribution> {
    let prompt_input = memory_recall_prompt_input(available_tool_names, policy, recall_snapshot);
    let prompt = render_memory_recall_prompt(&prompt_input)?;
    Some(HookContribution::PromptSection(PromptSectionContribution {
        contribution_id: HookContributionId::new("memory.recall_prompt")
            .expect("static contribution id is valid"),
        section_id: HookSectionId::new("memory_recall").expect("static section id is valid"),
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
        HookContext, HookInput, HookPromptContextSet, HookThreadId, HookTurnId, HookWorkspaceId,
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

        let no_save = MemoryTurnPolicy::no_save();
        assert!(no_save.allow_pre_turn_recall());
        assert!(no_save.allows_memory_tool(MEMORY_SEARCH_TOOL));
        assert!(!no_save.allows_memory_tool(MEMORY_REMEMBER_TOOL));
        assert!(no_save.allows_memory_tool(MEMORY_FORGET_TOOL));

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
