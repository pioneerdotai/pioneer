use async_trait::async_trait;
use pioneer_memory::hooks::{
    AgentEpisodicRecallProvider, MemoryCurrentThreadRecallRequest,
    MemoryEpisodicRecallCapabilities, MemoryEpisodicRecallResponse,
};

/// Optional recall adapter sharing the authorized indexed search of threads_search.
/// Not installed in the production Memory hook runtime.
pub(crate) struct ThreadEpisodicMemoryRecallProvider {
    processor: std::sync::Weak<crate::message::MessageProcessor>,
}
impl ThreadEpisodicMemoryRecallProvider {
    pub(crate) fn new(processor: std::sync::Weak<crate::message::MessageProcessor>) -> Self {
        Self { processor }
    }
}
#[async_trait]
impl AgentEpisodicRecallProvider for ThreadEpisodicMemoryRecallProvider {
    async fn recall_capabilities(
        &self,
        _context: pioneer_memory::hooks::MemoryTurnContext,
    ) -> MemoryEpisodicRecallCapabilities {
        let enabled = self
            .processor
            .upgrade()
            .is_some_and(|p| p.thread_search_recall_enabled());
        MemoryEpisodicRecallCapabilities {
            current_thread_search: enabled,
            // The default episodic operation searches the complete authorized
            // workspace, including the current thread. Do not run that same
            // search again as a separate related/workspace planner operation.
            related_thread_search: false,
            workspace_thread_search: false,
            full_input_query: true,
            current_task_context: false,
            completed_task_summary: false,
        }
    }
    async fn recall_current_thread(
        &self,
        request: MemoryCurrentThreadRecallRequest,
    ) -> Result<MemoryEpisodicRecallResponse, String> {
        let processor = self
            .processor
            .upgrade()
            .ok_or_else(|| "episodic search unavailable".to_owned())?;
        processor
            .recall_thread_search(request)
            .await
            .map_err(|_| "episodic search unavailable or access denied".to_owned())
    }
}
