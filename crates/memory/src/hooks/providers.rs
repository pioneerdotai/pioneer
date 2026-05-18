use super::*;

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
    pub deterministic_sufficient: bool,
    pub deterministic_recall_empty: bool,
    pub has_workspace_context: bool,
    pub has_task_context: bool,
    pub input_length_bucket: String,
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
