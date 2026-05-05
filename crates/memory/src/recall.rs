use pioneer_protocol::{MemoryCategory, MemoryScope};

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
