//! SQLite adapter for the existing episodic search contract.
use super::*;

pub struct SqliteThreadEpisodicBackend {
    store: Arc<pioneer_crud::CrudStore>,
}
impl SqliteThreadEpisodicBackend {
    pub fn new(store: Arc<pioneer_crud::CrudStore>) -> Self {
        Self { store }
    }
}
#[async_trait]
impl ThreadEpisodicSearchBackend for SqliteThreadEpisodicBackend {
    async fn search_page(
        &self,
        request: ThreadEpisodicSearchRequest,
        page_size: usize,
        cursor: Option<String>,
        embedding: Option<ThreadEpisodicIndexEmbedding>,
    ) -> Result<ThreadEpisodicSearchPage, ThreadEpisodicSearchError> {
        use pioneer_crud::{ThreadEpisodicSearchEmbedding, ThreadEpisodicSearchQuery};
        request.profile.validate()?;
        let embedding = if request.profile.mode == ThreadEpisodicSearchMode::Lexical {
            None
        } else {
            embedding
        };
        if request.profile.mode == ThreadEpisodicSearchMode::Semantic && embedding.is_none() {
            return Err(ThreadEpisodicSearchError::non_retryable(
                "semantic search requires a query embedding",
            ));
        }
        let fail = |error: anyhow::Error| ThreadEpisodicSearchError::retryable(error.to_string());
        let offset = cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<u64>()
            .map_err(|_| ThreadEpisodicSearchError::non_retryable("invalid retrieval cursor"))?;
        let page_size = page_size.clamp(1, 256);
        if let Some(embedding) = &embedding {
            validate_index_embedding(embedding)?;
        }
        let query = ThreadEpisodicSearchQuery {
            workspace_id: request.workspace_id.clone(),
            thread_id: (!request.thread_id.is_empty()).then_some(request.thread_id.clone()),
            access: request.access.clone(),
            query: request.query.clone(),
            lexical: request.profile.mode != ThreadEpisodicSearchMode::Semantic,
            embedding: embedding.map(|embedding| ThreadEpisodicSearchEmbedding {
                provider_id: embedding.identity.provider_id,
                model: embedding.identity.model,
                dimension: embedding.identity.dimension,
                normalized: embedding.identity.normalized,
                vector: embedding.vector,
            }),
            created_after_unix: request.profile.recent_start_unix,
            offset,
            limit: (page_size + 1) as u32,
            snippet_chars: request.profile.snippet_chars,
        };
        let result = self
            .store
            .search_thread_episodic_projection(&query)
            .await
            .map_err(fail)?;
        let mut rows = result.rows;
        let warnings = if result.coverage_incomplete {
            vec!["search_projection_incomplete".to_owned()]
        } else {
            Vec::new()
        };
        let more = rows.len() > page_size;
        rows.truncate(page_size);
        let hits = rows
            .into_iter()
            .map(|row| {
                let item = row.item;
                let mut metadata = BTreeMap::new();
                metadata.insert(
                    "pioneer.thread_episodic.source_text_hash".to_owned(),
                    item.source_text_hash,
                );
                ThreadEpisodicSearchHit {
                    workspace_id: item.workspace_id,
                    thread_id: item.thread_id,
                    turn_id: item.turn_id,
                    item_id: item.item_id,
                    index_item_id: item.id,
                    source_actor_role: match item.source_actor_role {
                        pioneer_crud::ThreadEpisodicSourceActorRole::User => "user",
                        pioneer_crud::ThreadEpisodicSourceActorRole::Assistant => "assistant",
                        pioneer_crud::ThreadEpisodicSourceActorRole::Task => "task",
                        pioneer_crud::ThreadEpisodicSourceActorRole::SystemVisible => {
                            "system_visible"
                        }
                    }
                    .to_owned(),
                    source_runtime_kind: match item.source_runtime_kind {
                        pioneer_crud::ThreadEpisodicSourceRuntimeKind::UserTurn => "user_turn",
                        pioneer_crud::ThreadEpisodicSourceRuntimeKind::AssistantTurn => {
                            "assistant_turn"
                        }
                        pioneer_crud::ThreadEpisodicSourceRuntimeKind::TaskResult => "task_result",
                        pioneer_crud::ThreadEpisodicSourceRuntimeKind::CompactionSummary => {
                            "compaction_summary"
                        }
                    }
                    .to_owned(),
                    source_context: item.source_context,
                    visibility: ThreadEpisodicVisibility::UserVisible,
                    status: ThreadEpisodicItemStatus::Active,
                    // The repository returns an already bounded Unicode snippet.
                    text: row.text,
                    lexical_rank: row.lexical_rank,
                    semantic_rank: row.semantic_rank,
                    created_at_unix: Some(item.created_at.timestamp()),
                    metadata,
                }
            })
            .collect();
        Ok(ThreadEpisodicSearchPage {
            hits,
            next_cursor: more.then(|| offset.saturating_add(page_size as u64).to_string()),
            warnings,
        })
    }
}
