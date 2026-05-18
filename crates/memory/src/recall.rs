use pioneer_protocol::{
    MemoryAttribute, MemoryCategory, MemoryFactClass, MemoryScope, MemoryScopeKind, MemorySubject,
};

#[derive(Debug, Clone, Default)]
pub struct MemoryRecallParams {
    pub query: String,
    pub scopes: Vec<MemoryScope>,
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
pub struct MemoryRecallResponse {
    pub items: Vec<MemoryRecallItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryRecallMode {
    Profile,
    Project,
    Durable,
    ThreadEpisodic,
    TaskContext,
    ExactCanonical,
}

impl MemoryRecallMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Profile => "profile",
            Self::Project => "project",
            Self::Durable => "durable",
            Self::ThreadEpisodic => "thread_episodic",
            Self::TaskContext => "task_context",
            Self::ExactCanonical => "exact_canonical",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryRecallTarget {
    pub scope_kind: Option<MemoryScopeKind>,
    pub fact_class: Option<MemoryFactClass>,
    pub category: Option<MemoryCategory>,
    pub subject: Option<MemorySubject>,
    pub attribute: Option<MemoryAttribute>,
    pub canonical_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryModeRecallParams {
    pub mode: MemoryRecallMode,
    pub targets: Vec<MemoryRecallTarget>,
    pub top_k: Option<u32>,
    pub max_chars: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MemoryModeRecallResponse {
    pub items: Vec<MemoryRecallItem>,
    pub diagnostics: Vec<String>,
    pub truncated: bool,
    pub skipped_reason: Option<String>,
}

pub(crate) fn compact_recall_content(content: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let compact = content.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_chars(compact.as_str(), max_chars)
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut char_count = 0;
    let mut end = 0;
    for (index, ch) in value.char_indices() {
        if char_count == max_chars {
            break;
        }
        char_count += 1;
        end = index + ch.len_utf8();
    }

    if char_count < max_chars {
        value.to_owned()
    } else {
        value[..end].to_owned()
    }
}
