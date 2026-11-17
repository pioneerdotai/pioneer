use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryTurnContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub conversation_thread_id: Option<String>,
    pub turn_id: String,
    pub mode: ThreadMode,
    pub input_text: String,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
    pub principal_id: Option<String>,
}

impl MemoryTurnContext {
    pub fn effective_conversation_thread_id(&self) -> &str {
        self.conversation_thread_id
            .as_deref()
            .filter(|thread_id| !thread_id.trim().is_empty())
            .unwrap_or(self.thread_id.as_str())
    }
}
