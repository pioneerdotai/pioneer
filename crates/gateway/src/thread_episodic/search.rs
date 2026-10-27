//! Shared source and embedding preparation for indexed conversation retrieval.
use super::*;
use pioneer_memory::ThreadEpisodicMemvidSearchHit;

impl ThreadEpisodicRecallService {
    /// Use the indexing resolver and its checked query embedding exactly once per search call.
    pub(crate) async fn prepare_thread_search_embedding(
        &self,
        workspace_id: &str,
        query: &str,
    ) -> Result<Option<ThreadEpisodicMemvidIndexEmbedding>> {
        let config = self.config.read().map(|c| c.clone()).unwrap_or_default();
        let vector = self.vector_search_config_for_workspace(workspace_id, &config.vector_search);
        if !vector.has_selected_embedding_model() {
            return Ok(None);
        }
        let embedder = self
            .resolve_hybrid_recall_embedder(workspace_id)
            .await
            .map_err(anyhow::Error::msg)?;
        let query = query.to_owned();
        tokio::task::spawn_blocking(move || {
            let identity = embedder.identity();
            let embedding = embedder.provider().embed_query_checked(query.as_str());
            embedding
                .map_err(|e| anyhow::anyhow!("{e}"))
                .and_then(|vector| {
                    ThreadEpisodicMemvidIndexEmbedding::new(identity, vector)
                        .map_err(|e| anyhow::anyhow!("{}", e.message))
                })
                .map(Some)
        })
        .await?
    }

    /// Resolve the current canonical source. Stale index text is never a fallback.
    pub(crate) async fn resolve_thread_search_source(
        &self,
        workspace_id: &str,
        hit: &ThreadEpisodicMemvidSearchHit,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        let Some(item) = self
            .current_thread_search_source(workspace_id, &hit.index_item_id)
            .await?
        else {
            return Ok(None);
        };
        if hit.workspace_id != workspace_id
            || item.thread_id != hit.thread_id
            || item.turn_id != hit.turn_id
            || item.item_id != hit.item_id
            || hit.metadata.get("pioneer.thread_episodic.source_text_hash")
                != Some(&item.source_text_hash)
        {
            return Ok(None);
        }
        Ok(Some(item))
    }

    pub(crate) async fn current_thread_search_source(
        &self,
        workspace_id: &str,
        index_item_id: &str,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        let Some(item) = self
            .crud_store
            .find_thread_episodic_item(index_item_id)
            .await?
        else {
            return Ok(None);
        };
        if item.workspace_id != workspace_id
            || item.deleted_at.is_some()
            || item.status != ThreadEpisodicItemStatus::Active
            || !matches!(
                item.visibility,
                ThreadEpisodicItemVisibility::UserVisible
                    | ThreadEpisodicItemVisibility::ParentVisible
            )
            || !thread_episodic_source_context_is_recallable(&item.source_context)
            || self
                .crud_store
                .find_thread_episodic_exclusion_by_item(workspace_id, &item.thread_id, &item.id)
                .await?
                .is_some()
        {
            return Ok(None);
        }
        if !self
            .crud_store
            .get_turn_message_guards_by_thread_and_ids(
                &item.thread_id,
                std::slice::from_ref(&item.turn_id),
            )
            .await?
            .iter()
            .any(|turn| turn.id == item.turn_id && turn.message_deleted_at.is_none())
        {
            return Ok(None);
        }
        match self.reconstruct_item_text(&item).await {
            Ok(text) if !looks_secret_like(&text) => Ok(Some(item)),
            _ => Ok(None),
        }
    }

    pub(crate) fn thread_search_backend(&self) -> &Arc<dyn ThreadEpisodicMemvidBackend> {
        &self.backend
    }
}
