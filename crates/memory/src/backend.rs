use anyhow::{Result, bail};
use async_trait::async_trait;
use pioneer_protocol::{MemoryCategory, MemoryScope, MemorySensitivity};
use std::collections::BTreeMap;
use tokio::sync::RwLock;

#[async_trait]
pub trait MemoryBackend: Send + Sync {
    async fn put(&self, request: BackendPutRequest) -> Result<BackendPutResult>;
    async fn get(&self, memory_id: &str) -> Result<Option<BackendPayload>>;
    async fn search(&self, request: BackendSearchRequest) -> Result<Vec<BackendSearchHit>>;
    async fn delete(&self, memory_id: &str) -> Result<BackendDeleteResult>;
}

#[derive(Debug, Clone)]
pub struct BackendPutRequest {
    pub memory_id: String,
    pub scope: MemoryScope,
    pub namespace: Option<String>,
    pub category: MemoryCategory,
    pub key: Option<String>,
    pub content: String,
    pub sensitivity: MemorySensitivity,
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BackendPutResult {
    pub capsule_id: Option<String>,
    pub capsule_ref: Option<String>,
    pub frame_id: Option<i64>,
    pub frame_uri: Option<String>,
    pub frame_version: i64,
    pub backend_metadata_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BackendPayload {
    pub memory_id: String,
    pub content: String,
    pub snippet: Option<String>,
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BackendSearchRequest {
    pub query: String,
    pub scopes: Vec<MemoryScope>,
    pub limit: u32,
}

#[derive(Debug, Clone)]
pub struct BackendSearchHit {
    pub memory_id: String,
    pub score: Option<f32>,
    pub snippet: Option<String>,
    pub matched_terms: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackendDeleteResult {
    pub deleted: bool,
}

#[derive(Debug, Default)]
pub struct NoopMemoryBackend;

#[async_trait]
impl MemoryBackend for NoopMemoryBackend {
    async fn put(&self, _request: BackendPutRequest) -> Result<BackendPutResult> {
        bail!("memory backend is not configured");
    }

    async fn get(&self, _memory_id: &str) -> Result<Option<BackendPayload>> {
        bail!("memory backend is not configured");
    }

    async fn search(&self, _request: BackendSearchRequest) -> Result<Vec<BackendSearchHit>> {
        bail!("memory backend is not configured");
    }

    async fn delete(&self, _memory_id: &str) -> Result<BackendDeleteResult> {
        bail!("memory backend is not configured");
    }
}

#[derive(Debug, Default)]
pub struct InMemoryMemoryBackend {
    inner: RwLock<InMemoryBackendState>,
}

#[derive(Debug, Default)]
struct InMemoryBackendState {
    records: BTreeMap<String, StoredMemoryPayload>,
    delete_noop: bool,
    delete_error: Option<String>,
}

#[derive(Debug, Clone)]
struct StoredMemoryPayload {
    memory_id: String,
    scope: MemoryScope,
    namespace: Option<String>,
    category: MemoryCategory,
    key: Option<String>,
    content: String,
    sensitivity: MemorySensitivity,
    metadata_json: Option<String>,
    frame_version: i64,
}

impl InMemoryMemoryBackend {
    pub async fn remove_payload(&self, memory_id: &str) {
        self.inner.write().await.records.remove(memory_id);
    }

    pub async fn set_delete_noop(&self, enabled: bool) {
        self.inner.write().await.delete_noop = enabled;
    }

    pub async fn set_delete_error(&self, error: Option<String>) {
        self.inner.write().await.delete_error = error;
    }

    pub async fn insert_stale_payload(&self, request: BackendPutRequest) {
        let frame_version = self
            .inner
            .read()
            .await
            .records
            .get(request.memory_id.as_str())
            .map(|record| record.frame_version + 1)
            .unwrap_or(1);
        self.inner.write().await.records.insert(
            request.memory_id.clone(),
            StoredMemoryPayload {
                memory_id: request.memory_id,
                scope: request.scope,
                namespace: request.namespace,
                category: request.category,
                key: request.key,
                content: request.content,
                sensitivity: request.sensitivity,
                metadata_json: request.metadata_json,
                frame_version,
            },
        );
    }

    pub async fn raw_search(&self, request: BackendSearchRequest) -> Result<Vec<BackendSearchHit>> {
        self.search(request).await
    }
}

#[async_trait]
impl MemoryBackend for InMemoryMemoryBackend {
    async fn put(&self, request: BackendPutRequest) -> Result<BackendPutResult> {
        let mut state = self.inner.write().await;
        let frame_version = state
            .records
            .get(request.memory_id.as_str())
            .map(|record| record.frame_version + 1)
            .unwrap_or(1);
        let memory_id = request.memory_id.clone();
        state.records.insert(
            memory_id.clone(),
            StoredMemoryPayload {
                memory_id: request.memory_id,
                scope: request.scope,
                namespace: request.namespace,
                category: request.category,
                key: request.key,
                content: request.content,
                sensitivity: request.sensitivity,
                metadata_json: request.metadata_json,
                frame_version,
            },
        );

        Ok(BackendPutResult {
            capsule_id: None,
            capsule_ref: Some(format!("memory://in-memory/{memory_id}")),
            frame_id: None,
            frame_uri: Some(format!(
                "memory://in-memory/{memory_id}#frame-{frame_version}"
            )),
            frame_version,
            backend_metadata_json: None,
        })
    }

    async fn get(&self, memory_id: &str) -> Result<Option<BackendPayload>> {
        Ok(self
            .inner
            .read()
            .await
            .records
            .get(memory_id)
            .map(|record| BackendPayload {
                memory_id: record.memory_id.clone(),
                content: record.content.clone(),
                snippet: None,
                metadata_json: record.metadata_json.clone(),
            }))
    }

    async fn search(&self, request: BackendSearchRequest) -> Result<Vec<BackendSearchHit>> {
        let query = request.query.trim().to_lowercase();
        if query.is_empty() {
            return Ok(Vec::new());
        }
        let query_terms = query
            .split_whitespace()
            .filter(|term| !term.is_empty())
            .collect::<Vec<_>>();

        let mut hits = self
            .inner
            .read()
            .await
            .records
            .values()
            .filter(|record| {
                request.scopes.is_empty()
                    || request.scopes.iter().any(|scope| scope == &record.scope)
            })
            .filter_map(|record| score_record(record, query.as_str(), &query_terms))
            .collect::<Vec<_>>();

        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.memory_id.cmp(&right.memory_id))
        });
        hits.truncate(request.limit as usize);
        Ok(hits)
    }

    async fn delete(&self, memory_id: &str) -> Result<BackendDeleteResult> {
        let mut state = self.inner.write().await;
        if let Some(error) = state.delete_error.clone() {
            bail!(error);
        }
        if state.delete_noop {
            return Ok(BackendDeleteResult { deleted: false });
        }
        Ok(BackendDeleteResult {
            deleted: state.records.remove(memory_id).is_some(),
        })
    }
}

fn score_record(
    record: &StoredMemoryPayload,
    query: &str,
    query_terms: &[&str],
) -> Option<BackendSearchHit> {
    let haystack = format!(
        "{} {} {:?} {:?} {:?}",
        record.content,
        record.key.as_deref().unwrap_or_default(),
        record.category,
        record.sensitivity,
        record.namespace
    )
    .to_lowercase();

    let score = if haystack.contains(query) {
        1.0
    } else {
        let matched = query_terms
            .iter()
            .filter(|term| haystack.contains(**term))
            .count();
        if matched == query_terms.len() && matched > 0 {
            0.8
        } else if matched > 0 {
            0.4
        } else {
            return None;
        }
    };

    let matched_terms = query_terms
        .iter()
        .filter(|term| haystack.contains(**term))
        .map(|term| (*term).to_owned())
        .collect::<Vec<_>>();

    Some(BackendSearchHit {
        memory_id: record.memory_id.clone(),
        score: Some(score),
        snippet: Some(snippet_for(record.content.as_str(), query)),
        matched_terms,
    })
}

fn snippet_for(content: &str, query: &str) -> String {
    let max_chars = 160;
    let lower = content.to_lowercase();
    let start = lower.find(query).unwrap_or(0);
    content.chars().skip(start).take(max_chars).collect()
}
