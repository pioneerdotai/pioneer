use super::*;

#[async_trait::async_trait]
pub trait AgentMemoryProvider: Send + Sync {
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
    pub durable_terminal_effect: Option<MemoryDurableTerminalEffectClaim>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryDurableTerminalEffectClaim {
    pub effect_id: String,
    pub claim_token: String,
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
    /// Providers that own durable checkpoints must validate the typed response
    /// with `validate_memory_post_turn_response_format` before publication and
    /// after replay decoding. Fresh format failures may retry within the outbox
    /// budget; invalid immutable checkpoints must fail permanently.
    async fn extract_post_turn_memory_json(
        &self,
        context: MemoryPostTurnExtractorContext,
        request: MemoryPostTurnExtractorRequest,
    ) -> HookResult<String>;
}

#[async_trait::async_trait]
pub trait AgentMemoryWriteProvider: Send + Sync {
    async fn load_memory_manifest(
        &self,
        context: MemoryTurnContext,
        request: MemoryManifestRequest,
    ) -> Result<MemoryManifest, crate::MemoryManifestFailure>;

    async fn write_semantic_memory(
        &self,
        context: MemoryTurnContext,
        params: MemorySemanticWriteParams,
    ) -> Result<MemorySemanticWriteResponse, crate::MemoryWriteFailure>;
}
