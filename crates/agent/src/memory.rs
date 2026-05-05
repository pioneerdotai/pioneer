use chrono::{DateTime, Utc};
use pioneer_promt::{MemoryRecallPromptInput, MemoryRecallPromptItem, MemoryRecallPromptPolicy};
use pioneer_protocol::{MemoryCategory, MemoryScope, MemoryScopeKind, ThreadMode};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

pub const MEMORY_SEARCH_TOOL: &str = "memory_search";
pub const MEMORY_GET_TOOL: &str = "memory_get";
pub const MEMORY_REMEMBER_TOOL: &str = "memory_remember";
pub const MEMORY_FORGET_TOOL: &str = "memory_forget";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPromptPolicy {
    Full,
    ReadOnly,
    ForgetOnly,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryReadToolPolicy {
    Allow,
    ForgetOnly,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryMutationToolPolicy {
    Allow,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryExtractionPolicy {
    Allow,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryActiveContextPolicy {
    Allow,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPolicySource {
    StructuredOverride,
    PreMemoryClassifier,
    DefaultFallback,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryClassifierFallbackPolicy {
    DefaultAllow,
    StrictDeny,
    AllowReadOnly,
}

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
        Err(error) => fallback_policy(
            request.default_policy,
            request.fallback,
            fallback_reason_for_classifier_error(error.as_str()),
            Some(format!("memory.policy.classifier_failed: {error}")),
        ),
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
        policy.diagnostics.push(diagnostic.into());
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
