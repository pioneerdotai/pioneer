use super::*;
use serde::{Deserialize, Serialize};

#[async_trait::async_trait]
pub trait AgentMemoryProvider: Send + Sync {
    async fn recall_memory(
        &self,
        context: MemoryTurnContext,
        request: MemoryRecallRequest,
    ) -> Result<MemoryRecallSnapshot, String>;

    async fn recall_memory_mode(
        &self,
        _context: MemoryTurnContext,
        _request: MemoryModeRecallParams,
    ) -> Result<MemoryRecallSnapshot, String> {
        Ok(MemoryRecallSnapshot {
            items: Vec::new(),
            diagnostics: vec!["memory.active_recall.mode_provider_unavailable".to_owned()],
            truncated: false,
        })
    }

    async fn materialize_memory_tools(
        &self,
        context: MemoryTurnContext,
    ) -> Result<MemoryToolMaterialization, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEpisodicRecallSourceKind {
    #[default]
    CurrentThread,
    RelatedThread,
    CurrentTask,
    CompletedTask,
    TranscriptSummary,
}

impl MemoryEpisodicRecallSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CurrentThread => "current_thread",
            Self::RelatedThread => "related_thread",
            Self::CurrentTask => "current_task",
            Self::CompletedTask => "completed_task",
            Self::TranscriptSummary => "transcript_summary",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEpisodicRecallBoundary {
    #[default]
    Snippet,
    Summary,
}

impl MemoryEpisodicRecallBoundary {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Snippet => "snippet",
            Self::Summary => "summary",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEpisodicRecallVisibility {
    #[default]
    Public,
    Hidden,
    Deleted,
    PrivateUnavailable,
}

impl MemoryEpisodicRecallVisibility {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Hidden => "hidden",
            Self::Deleted => "deleted",
            Self::PrivateUnavailable => "private_unavailable",
        }
    }

    pub fn is_prompt_visible(self) -> bool {
        self == Self::Public
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEpisodicRecallCapabilities {
    pub current_thread_search: bool,
    pub related_thread_search: bool,
    pub current_task_context: bool,
    pub completed_task_summary: bool,
}

impl MemoryEpisodicRecallCapabilities {
    pub fn any(&self) -> bool {
        self.current_thread_search
            || self.related_thread_search
            || self.current_task_context
            || self.completed_task_summary
    }

    pub fn supports_source(&self, source: MemoryEpisodicRecallSourceKind) -> bool {
        match source {
            MemoryEpisodicRecallSourceKind::CurrentThread
            | MemoryEpisodicRecallSourceKind::TranscriptSummary => self.current_thread_search,
            MemoryEpisodicRecallSourceKind::RelatedThread => self.related_thread_search,
            MemoryEpisodicRecallSourceKind::CurrentTask => self.current_task_context,
            MemoryEpisodicRecallSourceKind::CompletedTask => self.completed_task_summary,
        }
    }

    pub fn available_context_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        if self.current_thread_search {
            names.push("current_thread".to_owned());
        }
        if self.related_thread_search {
            names.push("related_threads".to_owned());
        }
        if self.current_task_context {
            names.push("current_task".to_owned());
        }
        if self.completed_task_summary {
            names.push("completed_tasks".to_owned());
        }
        names
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEpisodicRecallProvenance {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_unix: Option<i64>,
    pub source: MemoryEpisodicRecallSourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retrieval_score: Option<f32>,
    pub boundary: MemoryEpisodicRecallBoundary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEpisodicRecallItem {
    pub id: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub provenance: MemoryEpisodicRecallProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix: Option<i64>,
    #[serde(default)]
    pub visibility: MemoryEpisodicRecallVisibility,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEpisodicRecallResponse {
    pub items: Vec<MemoryEpisodicRecallItem>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryCurrentThreadRecallRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub query: String,
    #[serde(default)]
    pub targets: Vec<MemoryRecallTarget>,
    pub top_k: u32,
    pub max_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRelatedThreadRecallRequest {
    pub workspace_id: String,
    pub current_thread_id: String,
    pub query: String,
    #[serde(default)]
    pub targets: Vec<MemoryRecallTarget>,
    pub top_k: u32,
    pub max_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryCurrentTaskRecallRequest {
    pub workspace_id: String,
    pub thread_id: String,
    pub task_id: String,
    pub query: String,
    #[serde(default)]
    pub targets: Vec<MemoryRecallTarget>,
    pub top_k: u32,
    pub max_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryCompletedTaskRecallRequest {
    pub workspace_id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub query: String,
    #[serde(default)]
    pub targets: Vec<MemoryRecallTarget>,
    pub top_k: u32,
    pub max_chars: usize,
}

#[async_trait::async_trait]
pub trait AgentEpisodicRecallProvider: Send + Sync {
    async fn recall_capabilities(
        &self,
        _context: MemoryTurnContext,
    ) -> MemoryEpisodicRecallCapabilities {
        MemoryEpisodicRecallCapabilities::default()
    }

    async fn recall_current_thread(
        &self,
        _request: MemoryCurrentThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        Ok(MemoryEpisodicRecallResponse {
            diagnostics: vec!["memory.episodic_recall.current_thread_unavailable".to_owned()],
            ..MemoryEpisodicRecallResponse::default()
        })
    }

    async fn recall_related_threads(
        &self,
        _request: MemoryRelatedThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        Ok(MemoryEpisodicRecallResponse {
            diagnostics: vec!["memory.episodic_recall.related_threads_unavailable".to_owned()],
            ..MemoryEpisodicRecallResponse::default()
        })
    }

    async fn recall_current_task(
        &self,
        _request: MemoryCurrentTaskRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        Ok(MemoryEpisodicRecallResponse {
            diagnostics: vec!["memory.episodic_recall.current_task_unavailable".to_owned()],
            ..MemoryEpisodicRecallResponse::default()
        })
    }

    async fn recall_completed_tasks(
        &self,
        _request: MemoryCompletedTaskRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        Ok(MemoryEpisodicRecallResponse {
            diagnostics: vec!["memory.episodic_recall.completed_tasks_unavailable".to_owned()],
            ..MemoryEpisodicRecallResponse::default()
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryManifestRequest {
    pub max_items: usize,
    pub max_item_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MemoryManifest {
    pub active: Vec<MemoryManifestActiveItem>,
    pub candidates: Vec<MemoryManifestCandidateItem>,
    pub diagnostics: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryManifestActiveItem {
    pub memory_id: String,
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub content_preview: String,
    pub updated_at: i64,
    pub status: MemoryStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryManifestCandidateItem {
    pub candidate_id: String,
    pub scope: MemoryScope,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub content_preview: String,
    pub status: MemoryCandidateStatus,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryPostTurnExtractorContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub mode: ThreadMode,
    pub model: Option<String>,
    pub model_provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryPostTurnExtractorRequest {
    pub user_text: String,
    pub assistant_text: String,
    pub tool_events_summary: String,
    pub domain_events_summary: String,
    pub manifest: MemoryManifest,
    pub max_facts: usize,
}

impl MemoryPostTurnExtractorRequest {
    pub fn render_prompt(&self) -> String {
        render_memory_post_turn_extractor_prompt(&MemoryPostTurnExtractorPromptInput {
            user_text: self.user_text.clone(),
            assistant_text: self.assistant_text.clone(),
            tool_events_summary: self.tool_events_summary.clone(),
            domain_events_summary: self.domain_events_summary.clone(),
            memory_manifest: render_memory_manifest(&self.manifest),
            max_facts: self.max_facts,
        })
    }
}

#[async_trait::async_trait]
pub trait AgentMemoryPostTurnExtractorProvider: Send + Sync {
    async fn extract_post_turn_memory_json(
        &self,
        context: MemoryPostTurnExtractorContext,
        request: MemoryPostTurnExtractorRequest,
    ) -> Result<String, String>;
}

#[async_trait::async_trait]
pub trait AgentMemoryWriteProvider: Send + Sync {
    async fn load_memory_manifest(
        &self,
        context: MemoryTurnContext,
        request: MemoryManifestRequest,
    ) -> Result<MemoryManifest, String>;

    async fn write_semantic_memory(
        &self,
        context: MemoryTurnContext,
        params: MemorySemanticWriteParams,
    ) -> Result<MemorySemanticWriteResponse, String>;
}

#[async_trait::async_trait]
pub trait AgentMemoryTurnPolicyProvider: Send + Sync {
    async fn resolve_memory_turn_policy(
        &self,
        context: MemoryTurnPolicyContext,
        request: MemoryTurnPolicyRequest,
    ) -> Result<MemoryTurnPolicy, String>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemoryActiveRecallDecisionContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub mode: ThreadMode,
    pub input_text_preview: String,
    pub model: Option<String>,
    pub model_provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemoryActiveRecallDecisionRequest {
    pub deterministic_context_count: usize,
    pub deterministic_context_chars: usize,
    pub deterministic_memory_ids: Vec<String>,
    pub deterministic_sufficient: bool,
    pub deterministic_recall_empty: bool,
    pub has_workspace_context: bool,
    pub has_task_context: bool,
    pub input_length_bucket: String,
    pub config_mode: MemoryActiveRecallMode,
    pub read_allowed: bool,
    pub active_memory_allowed: bool,
    pub explicit_no_memory: bool,
    pub input_text_char_count: usize,
    pub available_modes: Vec<String>,
    pub available_scoped_contexts: Vec<String>,
    pub episodic_capabilities: MemoryEpisodicRecallCapabilities,
    pub max_queries: usize,
    pub top_k_per_query: u32,
    pub max_prompt_chars: usize,
    pub max_input_chars: usize,
    pub max_output_chars: usize,
    pub fallback_policy: MemoryActiveRecallPlannerFallbackPolicy,
}

impl MemoryActiveRecallDecisionRequest {
    pub fn sanitized_input_json(&self, context: &MemoryActiveRecallDecisionContext) -> String {
        let payload = MemoryActiveRecallPlannerSanitizedInput {
            workspace_id_present: !context.workspace_id.trim().is_empty(),
            thread_id_present: !context.thread_id.trim().is_empty(),
            turn_id_present: !context.turn_id.trim().is_empty(),
            model_present: context
                .model
                .as_deref()
                .is_some_and(|model| !model.trim().is_empty()),
            model_provider_present: context
                .model_provider
                .as_deref()
                .is_some_and(|provider| !provider.trim().is_empty()),
            thread_mode: match context.mode {
                ThreadMode::Agent => "agent".to_owned(),
                ThreadMode::Chat => "chat".to_owned(),
            },
            input_text_preview: context.input_text_preview.clone(),
            input_text_char_count: self.input_text_char_count,
            deterministic_context_count: self.deterministic_context_count,
            deterministic_context_chars: self.deterministic_context_chars,
            deterministic_memory_ids: self.deterministic_memory_ids.clone(),
            deterministic_sufficient: self.deterministic_sufficient,
            deterministic_recall_empty: self.deterministic_recall_empty,
            has_workspace_context: self.has_workspace_context,
            has_task_context: self.has_task_context,
            input_length_bucket: self.input_length_bucket.clone(),
            config_mode: self.config_mode.as_str().to_owned(),
            read_allowed: self.read_allowed,
            active_memory_allowed: self.active_memory_allowed,
            explicit_no_memory: self.explicit_no_memory,
            available_modes: self.available_modes.clone(),
            available_scoped_contexts: self.available_scoped_contexts.clone(),
            episodic_capabilities: self.episodic_capabilities.clone(),
            budgets: MemoryActiveRecallPlannerBudgetInput {
                max_queries: self.max_queries,
                top_k_per_query: self.top_k_per_query,
                max_prompt_chars: self.max_prompt_chars,
                max_input_chars: self.max_input_chars,
                max_output_chars: self.max_output_chars,
            },
            fallback_policy: self.fallback_policy.as_str().to_owned(),
        };
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct MemoryActiveRecallPlannerSanitizedInput {
    workspace_id_present: bool,
    thread_id_present: bool,
    turn_id_present: bool,
    model_present: bool,
    model_provider_present: bool,
    thread_mode: String,
    input_text_preview: String,
    input_text_char_count: usize,
    deterministic_context_count: usize,
    deterministic_context_chars: usize,
    deterministic_memory_ids: Vec<String>,
    deterministic_sufficient: bool,
    deterministic_recall_empty: bool,
    has_workspace_context: bool,
    has_task_context: bool,
    input_length_bucket: String,
    config_mode: String,
    read_allowed: bool,
    active_memory_allowed: bool,
    explicit_no_memory: bool,
    available_modes: Vec<String>,
    available_scoped_contexts: Vec<String>,
    episodic_capabilities: MemoryEpisodicRecallCapabilities,
    budgets: MemoryActiveRecallPlannerBudgetInput,
    fallback_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct MemoryActiveRecallPlannerBudgetInput {
    max_queries: usize,
    top_k_per_query: u32,
    max_prompt_chars: usize,
    max_input_chars: usize,
    max_output_chars: usize,
}
