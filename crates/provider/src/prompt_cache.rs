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
    task_owner: Option<String>,
}
tokio::task_local! {
    static SCOPE: Scope;
    static CONVERSATION: (String, String, Option<String>);
}

/// The runtime already knows the logical conversation owner. Attached agents
/// retain their own execution thread; detached chat executions share the user's
/// conversation across turns. Never infer this identity from prompt text.
pub async fn conversation_scope<T>(
    workspace: String,
    thread: String,
    future: impl Future<Output = T>,
) -> T {
    conversation_task_scope(workspace, thread, None, future).await
}

/// A runtime-owned task tree may share affinity where the vendor explicitly
/// groups subagents. This does not change ordinary conversation/comparison IDs.
pub async fn conversation_task_scope<T>(
    workspace: String,
    thread: String,
    task_owner: Option<String>,
    future: impl Future<Output = T>,
) -> T {
    CONVERSATION
        .scope((workspace, thread, task_owner), future)
        .await
}

/// Explicit execution identity keeps concurrent agents and preflight separate.
pub async fn scope<T>(
    workspace: &str,
    thread: &str,
    purpose: &'static str,
    future: impl Future<Output = T>,
) -> T {
    let owner = CONVERSATION
        .try_with(|(w, t, _)| (w == workspace).then(|| t.clone()))
        .ok()
        .flatten()
        .unwrap_or_else(|| thread.to_owned());
    SCOPE
        .scope(
            Scope {
                workspace: workspace.into(),
                thread: owner,
                purpose,
                task_owner: CONVERSATION
                    .try_with(|(w, _, owner)| (w == workspace).then(|| owner.clone()).flatten())
                    .ok()
                    .flatten(),
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
    /// Stable routing identity, independent of response IDs, models and turns.
    pub(crate) fn affinity_key(&self) -> Option<String> {
        use sha2::Digest;
        let identity = serde_json::to_vec(&(
            "pioneer-gateway-session-v1",
            &self.key.authority,
            &self.key.workspace,
            &self.key.thread,
            self.key.purpose,
        ))
        .ok()?;
        Some(hex::encode(sha2::Sha256::digest(identity)))
    }

    /// Baseten groups one task and its related subagents. Other vendor policies
    /// continue using the separate conversation owner in `affinity_key`.
    pub(crate) fn task_affinity_key(authority: &str) -> Option<String> {
        let scope = SCOPE.try_with(Clone::clone).ok()?;
        let conversation_owner = format!("conversation:{}", scope.thread);
        let owner = scope.task_owner.as_deref().unwrap_or(&conversation_owner);
        if scope.workspace.is_empty() || scope.thread.is_empty() || owner.is_empty() {
            return None;
        }
        use sha2::Digest;
        Some(hex::encode(sha2::Sha256::digest(
            serde_json::to_vec(&(
                "pioneer-baseten-task-session-v1",
                authority,
                &scope.workspace,
                owner,
                scope.purpose,
            ))
            .ok()?,
        )))
    }

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

/// Routing outcomes are bounded diagnostics, never session IDs or hit counters.
pub(crate) fn gateway_routing(response: &Value) -> Option<Value> {
    let routing = response
        .get("provider_metadata")?
        .get("gateway")?
        .get("routing")?;
    let mut result = serde_json::Map::new();
    if let Some(source) = routing["clientSessionIdSource"]
        .as_str()
        .filter(|s| matches!(*s, "explicit" | "agent" | "fingerprint"))
    {
        result.insert("clientSessionIdSource".into(), json!(source));
    }
    let affinity = &routing["affinity"];
    let mut outcome = serde_json::Map::new();
    for key in ["outcome", "pinnedProvider", "displacedProvider"] {
        if let Some(value) = affinity[key].as_str().filter(|s| {
            !s.is_empty()
                && s.len() <= 96
                && s.bytes().all(|b| {
                    b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-')
                })
        }) {
            outcome.insert(key.into(), json!(value));
        }
    }
    if !outcome.is_empty() {
        result.insert("affinity".into(), Value::Object(outcome));
    }
    (!result.is_empty()).then_some(Value::Object(result))
}
