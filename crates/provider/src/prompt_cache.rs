//! Optional diagnostic comparisons. These IDs never carry conversation state.
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

#[derive(Clone)]
struct Scope {
    workspace: String,
    thread: String,
    purpose: &'static str,
}
tokio::task_local! {
    static SCOPE: Scope;
    static CONVERSATION: (String, String);
}

/// The runtime already knows the logical conversation owner. Attached agents
/// retain their own execution thread; detached chat executions share the user's
/// conversation across turns. Never infer this identity from prompt text.
pub async fn conversation_scope<T>(
    workspace: String,
    thread: String,
    future: impl Future<Output = T>,
) -> T {
    CONVERSATION.scope((workspace, thread), future).await
}

/// Explicit execution identity keeps concurrent agents and preflight separate.
pub async fn scope<T>(
    workspace: &str,
    thread: &str,
    purpose: &'static str,
    future: impl Future<Output = T>,
) -> T {
    let owner = CONVERSATION
        .try_with(|(w, t)| (w == workspace).then(|| t.clone()))
        .ok()
        .flatten()
        .unwrap_or_else(|| thread.to_owned());
    SCOPE
        .scope(
            Scope {
                workspace: workspace.into(),
                thread: owner,
                purpose,
            },
            future,
        )
        .await
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    authority: String,
    workspace: String,
    thread: String,
    purpose: &'static str,
}
type Comparisons = BTreeMap<Key, (Instant, String)>;
fn comparisons() -> &'static Mutex<Comparisons> {
    static COMPARISONS: OnceLock<Mutex<Comparisons>> = OnceLock::new();
    COMPARISONS.get_or_init(Default::default)
}

#[derive(Clone)]
pub(crate) struct Comparison {
    key: Key,
    pub(crate) response_id: Option<String>,
}
impl Comparison {
    pub(crate) fn begin(authority: &str) -> Option<Self> {
        let scope = SCOPE.try_with(Clone::clone).ok()?;
        if scope.workspace.is_empty() || scope.thread.is_empty() {
            return None;
        }
        let key = Key {
            authority: authority.into(),
            workspace: scope.workspace,
            thread: scope.thread,
            purpose: scope.purpose,
        };
        let recent = comparisons()
            .lock()
            .ok()
            .and_then(|values| values.get(&key).map(|(_, id)| id.clone()));
        // Deliberately do not use inherited assistant replay as a baseline:
        // a newly launched subagent must not compare against its parent.
        Some(Self {
            key,
            response_id: recent,
        })
    }

    pub(crate) fn completed(&self, response: &Value) {
        let Some(id) = crate::usage::native_id(response["id"].as_str()) else {
            return;
        };
        let Ok(mut values) = comparisons().lock() else {
            return;
        };
        // A runtime pointer, not an unbounded journal. Eviction only skips a
        // comparison; it never changes input, retry policy or model execution.
        if values.len() >= 1024 && !values.contains_key(&self.key) {
            if let Some(oldest) = values
                .iter()
                .min_by_key(|(_, (time, _))| *time)
                .map(|(key, _)| key.clone())
            {
                values.remove(&oldest);
            }
        }
        values.insert(self.key.clone(), (Instant::now(), id));
    }
}

/// Keep documented metadata only. Malformed/unknown diagnostics never fail a turn.
pub(crate) fn diagnostics(value: &Value) -> Option<Value> {
    let kind = value.get("type")?.as_str()?;
    if !matches!(
        kind,
        "cache_hit" | "cache_miss" | "comparison_response_not_found" | "unavailable"
    ) {
        return None;
    }
    let mut result = json!({"type":kind});
    if let Some(reason) = value.get("reason").and_then(Value::as_str).filter(|s| {
        !s.is_empty() && s.len() <= 96 && s.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
    }) {
        result["reason"] = json!(reason);
    }
    for key in ["comparison_reusable_tokens", "cache_missed_tokens"] {
        if let Some(count) = value.get(key).and_then(Value::as_u64) {
            result[key] = json!(count);
        }
    }
    Some(result)
}
