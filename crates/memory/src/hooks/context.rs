use super::*;

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
