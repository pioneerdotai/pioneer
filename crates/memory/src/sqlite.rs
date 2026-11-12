//! SQLite storage adapter for the existing MemoryService backend protocol.
use crate::backend::*;
use anyhow::Result;
use async_trait::async_trait;
use pioneer_crud::CrudStore;
use std::sync::Arc;

pub struct SqliteMemoryBackend {
    store: Arc<CrudStore>,
}

impl SqliteMemoryBackend {
    pub fn new(store: Arc<CrudStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl MemoryBackend for SqliteMemoryBackend {
    fn with_maintenance_access(&self) -> Option<Arc<dyn MemoryBackend>> {
        Some(Arc::new(Self::new(Arc::new(
            self.store.with_maintenance_access(),
        ))))
    }

    async fn put(&self, request: BackendPutRequest) -> Result<BackendPutResult> {
        let revision = self
            .store
            .put_memory_payload(&request.memory_id, request.scope, &request.content)
            .await?;
        Ok(BackendPutResult {
            publication_guard: Some(revision),
            publication_content: None,
            payload_version: revision,
            backend_metadata_json: None,
        })
    }

    async fn prepare_put(&self, request: BackendPutRequest) -> Result<BackendPutResult> {
        let previous = self
            .store
            .get_memory_payload_versioned(&request.memory_id, request.scope)
            .await?;
        let revision = previous.map_or(0, |(_, revision)| revision);
        Ok(BackendPutResult {
            payload_version: revision
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("payload revision exhausted"))?,
            publication_guard: Some(revision),
            publication_content: Some(request.content),
            backend_metadata_json: None,
        })
    }

    async fn get(&self, request: BackendGetRequest) -> Result<Option<BackendPayload>> {
        Ok(self
            .store
            .get_memory_payload_versioned(&request.memory_id, request.scope)
            .await?
            .map(|(content, revision)| BackendPayload {
                durable_revision: Some(revision),
                memory_id: request.memory_id,
                content,
                snippet: None,
                metadata_json: None,
            }))
    }

    async fn search(&self, request: BackendSearchRequest) -> Result<Vec<BackendSearchHit>> {
        Ok(self
            .store
            .search_memory_payloads(
                &request.query,
                request.filter,
                &request.allowed_sensitivities,
                request.now_unix,
                request.limit,
            )
            .await?
            .into_iter()
            .enumerate()
            .map(|(rank, (memory_id, content))| BackendSearchHit {
                memory_id,
                score: Some(61.0 / (61.0 + rank as f32)),
                snippet: Some(content.chars().take(240).collect()),
                matched_terms: Vec::new(),
            })
            .collect())
    }

    async fn delete(&self, request: BackendDeleteRequest) -> Result<BackendDeleteResult> {
        Ok(BackendDeleteResult {
            deleted: self
                .store
                .delete_memory_payload(
                    &request.memory_id,
                    request.scope,
                    request.scope_key_hash.as_deref(),
                )
                .await?,
        })
    }
}
