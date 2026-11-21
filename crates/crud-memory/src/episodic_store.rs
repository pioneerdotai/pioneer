//! Scoped persistence and source revalidation for episodic memory.
use crate::MemoryStore;
use crate::episodic_repository as thread_episodic_repository;
use crate::episodic_source_store;
use crate::thread_episodic::*;
use crate::thread_episodic_source;
use crate::util::unix_to_datetime;
use anyhow::{Context, Result};
use pioneer_crud_conversations::{thread_metadata as thread, turn_payload, turn_reads as turn};
use pioneer_protocol::TurnItem;
#[cfg(any(test, feature = "test-support"))]
use sea_orm::ConnectionTrait;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};

impl MemoryStore {
    pub async fn delete_all_thread_episodic_items(&self) -> Result<u64> {
        self.retry_write(|| async move {
            thread_episodic_repository::delete_all_items(&self.connection).await
        })
        .await
    }

    pub async fn delete_thread_episodic_items_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.retry_write(|| {
            let workspace_id = workspace_id.to_owned();
            async move {
                thread_episodic_repository::delete_items_for_workspace(
                    &self.connection,
                    workspace_id.as_str(),
                )
                .await
            }
        })
        .await
    }

    pub async fn delete_rebuildable_thread_episodic_items_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.retry_write(|| {
            let workspace_id = workspace_id.to_owned();
            async move {
                thread_episodic_repository::delete_rebuildable_items_for_workspace(
                    &self.connection,
                    workspace_id.as_str(),
                )
                .await
            }
        })
        .await
    }

    pub async fn delete_all_thread_episodic_exclusions(&self) -> Result<u64> {
        self.retry_write(|| async move {
            thread_episodic_repository::delete_all_exclusions(&self.connection).await
        })
        .await
    }

    pub async fn delete_thread_episodic_exclusions_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.retry_write(|| {
            let workspace_id = workspace_id.to_owned();
            async move {
                thread_episodic_repository::delete_exclusions_for_workspace(
                    &self.connection,
                    workspace_id.as_str(),
                )
                .await
            }
        })
        .await
    }

    pub async fn delete_all_thread_episodic_index_jobs(&self) -> Result<u64> {
        self.retry_write(|| async move {
            thread_episodic_repository::delete_all_index_jobs(&self.connection).await
        })
        .await
    }

    pub async fn delete_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.retry_write(|| {
            let workspace_id = workspace_id.to_owned();
            async move {
                thread_episodic_repository::delete_index_jobs_for_workspace(
                    &self.connection,
                    workspace_id.as_str(),
                )
                .await
            }
        })
        .await
    }

    pub async fn delete_all_thread_episodic_thread_directory_entries(&self) -> Result<u64> {
        self.retry_write(|| async move {
            thread_episodic_repository::delete_all_thread_directory_entries(&self.connection).await
        })
        .await
    }

    pub async fn delete_thread_episodic_thread_directory_entries_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        self.retry_write(|| {
            let workspace_id = workspace_id.to_owned();
            async move {
                thread_episodic_repository::delete_thread_directory_entries_for_workspace(
                    &self.connection,
                    workspace_id.as_str(),
                )
                .await
            }
        })
        .await
    }

    pub async fn thread_search_source_batch(
        &self,
        workspace_id: &str,
        ids: &[String],
    ) -> Result<
        Vec<(
            ThreadEpisodicItemRecord,
            ThreadEpisodicCanonicalItem,
            Option<String>,
        )>,
    > {
        let rows =
            thread_episodic_repository::search_source_batch(&self.connection, workspace_id, ids)
                .await?;
        let mut result = Vec::with_capacity(rows.len());
        for (item, canonical, payload, title) in rows {
            let item = crate::thread_episodic::thread_episodic_item_record_from_model(item)?;
            let Ok(source) = serde_json::from_str::<TurnItem>(&payload) else {
                continue;
            };
            if source.item_id() != item.item_id {
                continue;
            }
            result.push((
                item,
                ThreadEpisodicCanonicalItem {
                    committed: thread_episodic_source::source_status_is_committed(
                        canonical.status.as_deref(),
                    ),
                    item: source,
                    source_payload: payload,
                },
                title,
            ));
        }
        Ok(result)
    }

    pub async fn find_thread_episodic_item(
        &self,
        index_item_id: &str,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        thread_episodic_repository::find_item_by_id(&self.connection, index_item_id)
            .await?
            .map(crate::thread_episodic::thread_episodic_item_record_from_model)
            .transpose()
    }

    pub async fn find_thread_episodic_embedding_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<Option<ThreadEpisodicEmbeddingArtifactRecord>> {
        thread_episodic_repository::find_embedding_artifact_by_id(&self.connection, artifact_id)
            .await?
            .map(crate::thread_episodic::thread_episodic_embedding_artifact_record_from_model)
            .transpose()
    }

    pub async fn insert_thread_episodic_embedding_artifact_if_absent(
        &self,
        artifact: NewThreadEpisodicEmbeddingArtifactRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicEmbeddingArtifactRecord> {
        let prepared = thread_episodic_repository::prepare_embedding_artifact(
            artifact,
            unix_to_datetime(now_unix),
        )?;
        let row = self
            .retry_write(|| {
                let prepared = prepared.clone();
                async move {
                    thread_episodic_repository::insert_embedding_artifact_if_absent(
                        &self.connection,
                        prepared,
                    )
                    .await
                }
            })
            .await?;
        crate::thread_episodic::thread_episodic_embedding_artifact_record_from_model(row)
    }

    pub async fn touch_thread_episodic_embedding_artifact(
        &self,
        artifact_id: &str,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicEmbeddingArtifactRecord>> {
        let artifact_id = artifact_id.to_owned();
        let row = self
            .retry_write(|| {
                let artifact_id = artifact_id.clone();
                async move {
                    thread_episodic_repository::touch_embedding_artifact(
                        &self.connection,
                        &artifact_id,
                        unix_to_datetime(now_unix),
                    )
                    .await
                }
            })
            .await?;
        row.map(crate::thread_episodic::thread_episodic_embedding_artifact_record_from_model)
            .transpose()
    }

    pub async fn upsert_thread_episodic_item(
        &self,
        item: NewThreadEpisodicItemRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicItemRecord> {
        self.retry_write(|| {
            let item = item.clone();
            async move {
                let row = thread_episodic_repository::upsert_item_by_source_identity(
                    &self.connection,
                    item,
                    unix_to_datetime(now_unix),
                )
                .await?;
                crate::thread_episodic::thread_episodic_item_record_from_model(row)
            }
        })
        .await
    }

    pub async fn reconcile_thread_episodic_source_version(
        &self,
        expected_source_payload: &str,
        mut item: NewThreadEpisodicItemRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicSourceReconcileOutcome> {
        let source_item: TurnItem = serde_json::from_str(expected_source_payload)?;
        let committed = thread_episodic_source::committed_item_ingestion_input_from_parts(
            &item.workspace_id,
            &item.thread_id,
            &item.turn_id,
            source_item,
        )
        .context("invalid episodic source identity")?;
        let context = thread_episodic_repository::load_source_projection_context(
            &self.connection,
            &item.thread_id,
            &item.turn_id,
            &item.item_id,
            committed.item_type,
        )
        .await?;
        item.projection_group_id = context.projection_group_id(&committed, &item.source_text_hash);
        self.retry_write(|| {
            let expected_source_payload =
                turn_payload::PreparedItemPayload::new(expected_source_payload.to_owned());
            let item = item.clone();
            let context = context.clone();
            let item_type = committed.item_type;
            async move {
                let transaction =
                    self.connection.begin().await.context(
                        "failed to begin thread episodic source reconciliation transaction",
                    )?;
                let outcome: ThreadEpisodicSourceReconcileOutcome = async {
                    let Some(thread_model) =
                        thread::find_thread_by_id(&transaction, item.thread_id.as_str()).await?
                    else {
                        return Ok(ThreadEpisodicSourceReconcileOutcome::SourceChanged);
                    };
                    if thread_model.workspace_id != item.workspace_id {
                        anyhow::bail!(
                            "thread episodic source workspace changed during reconciliation"
                        );
                    }
                    if turn::find_turn_by_thread_and_id(
                        &transaction,
                        item.thread_id.as_str(),
                        item.turn_id.as_str(),
                    )
                    .await?
                    .is_none()
                    {
                        return Ok(ThreadEpisodicSourceReconcileOutcome::SourceChanged);
                    }
                    let Some(source) = turn::find_turn_item_metadata(
                        &transaction,
                        item.turn_id.as_str(),
                        item.item_id.as_str(),
                    )
                    .await?
                    else {
                        return Ok(ThreadEpisodicSourceReconcileOutcome::SourceChanged);
                    };
                    if !expected_source_payload
                        .matches(&transaction, &source)
                        .await?
                        || !thread_episodic_source::source_status_is_committed(
                            source.status.as_deref(),
                        )
                        || context
                            != thread_episodic_repository::load_source_projection_context(
                                &transaction,
                                &item.thread_id,
                                &item.turn_id,
                                &item.item_id,
                                item_type,
                            )
                            .await?
                    {
                        return Ok(ThreadEpisodicSourceReconcileOutcome::SourceChanged);
                    }
                    thread_episodic_repository::reconcile_item_source_version(
                        &transaction,
                        item,
                        unix_to_datetime(now_unix),
                    )
                    .await
                }
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread episodic source reconciliation")?;
                Ok(outcome)
            }
        })
        .await
    }

    pub async fn retire_thread_episodic_source_occurrence(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        expected_source_payload: Option<&str>,
        now_unix: i64,
    ) -> Result<ThreadEpisodicSourceReconcileOutcome> {
        self.retry_write(|| {
            let workspace_id = workspace_id.to_owned();
            let thread_id = thread_id.to_owned();
            let turn_id = turn_id.to_owned();
            let item_id = item_id.to_owned();
            let expected_source_payload = expected_source_payload
                .map(|s| turn_payload::PreparedItemPayload::new(s.to_owned()));
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin thread episodic source retirement transaction")?;
                let outcome: ThreadEpisodicSourceReconcileOutcome = async {
                    match thread::find_thread_by_id(&transaction, thread_id.as_str()).await? {
                        Some(thread_model) if thread_model.workspace_id != workspace_id => {
                            anyhow::bail!(
                                "thread episodic retirement source workspace ownership mismatch"
                            );
                        }
                        None if expected_source_payload.is_some() => {
                            return Ok(ThreadEpisodicSourceReconcileOutcome::SourceChanged);
                        }
                        _ => {}
                    }
                    match turn::find_turn_by_id(&transaction, turn_id.as_str()).await? {
                        Some(turn_model) if turn_model.thread_id != thread_id => {
                            anyhow::bail!(
                                "thread episodic retirement source turn ownership mismatch"
                            );
                        }
                        None if expected_source_payload.is_some() => {
                            return Ok(ThreadEpisodicSourceReconcileOutcome::SourceChanged);
                        }
                        _ => {}
                    }
                    let current = turn::find_turn_item_metadata(
                        &transaction,
                        turn_id.as_str(),
                        item_id.as_str(),
                    )
                    .await?;
                    let source_is_stable = match (&expected_source_payload, current.as_ref()) {
                        (Some(expected), Some(current)) => {
                            expected.matches(&transaction, current).await?
                        }
                        (None, None) => true,
                        _ => false,
                    };
                    if !source_is_stable {
                        return Ok(ThreadEpisodicSourceReconcileOutcome::SourceChanged);
                    }
                    thread_episodic_repository::retire_source_occurrence(
                        &transaction,
                        workspace_id.as_str(),
                        thread_id.as_str(),
                        turn_id.as_str(),
                        item_id.as_str(),
                        unix_to_datetime(now_unix),
                    )
                    .await
                }
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread episodic source retirement")?;
                Ok(outcome)
            }
        })
        .await
    }

    pub async fn find_thread_episodic_item_by_source_identity(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        text_hash: &str,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        thread_episodic_repository::find_item_by_source_identity(
            &self.connection,
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            text_hash,
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_item_record_from_model)
        .transpose()
    }

    pub async fn list_thread_episodic_items_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        thread_episodic_repository::list_items_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_item_record_from_model)
        .collect()
    }

    pub async fn list_recallable_thread_episodic_items_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        thread_episodic_repository::list_recallable_items_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_item_record_from_model)
        .collect()
    }

    pub async fn find_thread_episodic_index_job(
        &self,
        job_id: &str,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::find_index_job_by_id(&self.connection, job_id)
            .await?
            .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
            .transpose()
    }

    pub async fn find_thread_episodic_index_job_by_item(
        &self,
        index_item_id: &str,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::find_index_job_by_index_item(&self.connection, index_item_id)
            .await?
            .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
            .transpose()
    }

    pub async fn delete_incomplete_thread_episodic_index_job(&self, job_id: &str) -> Result<bool> {
        self.retry_write(|| {
            let job_id = job_id.to_owned();
            async move {
                thread_episodic_repository::delete_incomplete_index_job(
                    &self.connection,
                    job_id.as_str(),
                )
                .await
            }
        })
        .await
    }

    pub async fn delete_thread_episodic_index_job_if_suppressed(
        &self,
        workspace_id: &str,
        job_id: &str,
    ) -> Result<bool> {
        let workspace_id = workspace_id.to_owned();
        let job_id = job_id.to_owned();
        self.retry_write(|| {
            let workspace_id = workspace_id.clone();
            let job_id = job_id.clone();
            async move {
                let transaction = self.connection.begin().await.context(
                    "failed to begin suppressed thread episodic job cleanup transaction",
                )?;
                let deleted = match thread_episodic_repository::delete_index_job_if_suppressed(
                    &transaction,
                    workspace_id.as_str(),
                    job_id.as_str(),
                )
                .await
                {
                    Ok(deleted) => deleted,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };
                transaction.commit().await.context(
                    "failed to commit suppressed thread episodic job cleanup transaction",
                )?;
                Ok(deleted)
            }
        })
        .await
    }

    pub async fn list_thread_episodic_refill_workspace_ids(&self) -> Result<Vec<String>> {
        thread_episodic_repository::list_refill_workspace_ids(&self.connection).await
    }

    pub async fn list_thread_episodic_refill_threads(
        &self,
    ) -> Result<Vec<ThreadEpisodicRefillThread>> {
        thread_episodic_repository::list_refill_threads(&self.connection).await
    }

    pub async fn list_thread_episodic_refill_threads_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<ThreadEpisodicRefillThread>> {
        thread_episodic_repository::list_refill_threads_for_workspace(
            &self.connection,
            workspace_id,
        )
        .await
    }

    pub async fn list_thread_episodic_refill_threads_for_workspace_and_event_ids(
        &self,
        workspace_id: &str,
        event_ids: &[&str],
    ) -> Result<Vec<ThreadEpisodicRefillThread>> {
        thread_episodic_repository::list_refill_threads_for_workspace_and_event_ids(
            &self.connection,
            workspace_id,
            event_ids,
        )
        .await
    }

    pub async fn count_thread_episodic_refill_sources(
        &self,
    ) -> Result<ThreadEpisodicRefillSourceCounts> {
        thread_episodic_repository::count_refill_sources(&self.connection).await
    }

    pub async fn count_thread_episodic_refill_sources_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<ThreadEpisodicRefillSourceCounts> {
        thread_episodic_repository::count_refill_sources_for_workspace(
            &self.connection,
            workspace_id,
        )
        .await
    }

    pub async fn list_thread_episodic_refill_indexable_items(
        &self,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        thread_episodic_repository::list_refill_indexable_items(&self.connection, limit)
            .await?
            .into_iter()
            .map(crate::thread_episodic::thread_episodic_item_record_from_model)
            .collect()
    }

    pub async fn list_thread_episodic_refill_indexable_items_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        thread_episodic_repository::list_refill_indexable_items_for_workspace(
            &self.connection,
            workspace_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_item_record_from_model)
        .collect()
    }

    pub async fn count_incomplete_thread_episodic_index_jobs(&self) -> Result<u64> {
        thread_episodic_repository::count_incomplete_index_jobs(&self.connection).await
    }

    pub async fn count_incomplete_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        thread_episodic_repository::count_incomplete_index_jobs_for_workspace(
            &self.connection,
            workspace_id,
        )
        .await
    }

    pub async fn terminal_thread_episodic_index_job_exists_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<bool> {
        thread_episodic_repository::terminal_index_job_exists_for_workspace(
            &self.connection,
            workspace_id,
        )
        .await
    }

    pub async fn unfinished_thread_episodic_index_job_exists_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<bool> {
        thread_episodic_repository::unfinished_index_job_exists_for_workspace(
            &self.connection,
            workspace_id,
        )
        .await
    }

    pub async fn thread_episodic_index_job_exists_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<bool> {
        thread_episodic_repository::index_job_exists_for_workspace(&self.connection, workspace_id)
            .await
    }

    pub async fn count_canceled_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<u64> {
        thread_episodic_repository::count_canceled_index_jobs_for_workspace(
            &self.connection,
            workspace_id,
        )
        .await
    }

    pub async fn list_canceled_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::list_canceled_index_jobs_for_workspace(
            &self.connection,
            workspace_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .collect()
    }

    pub async fn list_canceled_thread_episodic_index_jobs_for_workspace_threads_after_id(
        &self,
        workspace_id: &str,
        thread_ids: &[String],
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::list_canceled_index_jobs_for_workspace_threads_after_id(
            &self.connection,
            workspace_id,
            thread_ids,
            after_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .collect()
    }

    pub async fn next_scheduled_thread_episodic_index_job_at_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Option<i64>> {
        Ok(
            thread_episodic_repository::find_next_scheduled_index_job_for_workspace(
                &self.connection,
                workspace_id,
            )
            .await?
            .map(|row| row.next_run_at.timestamp()),
        )
    }

    pub async fn list_thread_episodic_index_jobs_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::list_index_jobs_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .collect()
    }

    pub async fn list_failed_or_stale_thread_episodic_index_jobs_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        stale_before_unix: i64,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::list_failed_or_stale_index_jobs_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            unix_to_datetime(stale_before_unix),
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .collect()
    }

    pub async fn list_due_thread_episodic_index_jobs_after(
        &self,
        now_unix: i64,
        after: Option<&ThreadEpisodicIndexJobRecord>,
        through: Option<&ThreadEpisodicIndexJobRecord>,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::list_due_index_jobs_after(
            &self.connection,
            unix_to_datetime(now_unix),
            after,
            through,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .collect()
    }

    pub async fn thread_episodic_discovery_round_end(
        &self,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        thread_episodic_repository::discovery_round_end(
            &self.connection,
            unix_to_datetime(now_unix),
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .transpose()
    }

    pub async fn next_scheduled_thread_episodic_index_job_at(&self) -> Result<Option<i64>> {
        Ok(
            thread_episodic_repository::next_scheduled_index_job_at(&self.connection, None)
                .await?
                .map(crate::util::deadline_unix_seconds),
        )
    }

    pub async fn next_future_thread_episodic_index_job_at(
        &self,
        after_unix: i64,
    ) -> Result<Option<i64>> {
        Ok(thread_episodic_repository::next_scheduled_index_job_at(
            &self.connection,
            Some(unix_to_datetime(after_unix)),
        )
        .await?
        .map(crate::util::deadline_unix_seconds))
    }

    pub async fn claim_thread_episodic_index_job_if_due(
        &self,
        job_id: &str,
        now_unix: i64,
        max_attempts: i64,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        let Some(snapshot) = self.find_thread_episodic_index_job(job_id).await? else {
            return Ok(None);
        };
        self.claim_thread_episodic_index_job_from_snapshot(
            &snapshot,
            now_unix,
            max_attempts,
            None,
            None,
        )
        .await
    }

    pub async fn claim_thread_episodic_index_job_from_snapshot(
        &self,
        snapshot: &ThreadEpisodicIndexJobRecord,
        now_unix: i64,
        max_attempts: i64,
        mutation_started: Option<&std::sync::atomic::AtomicBool>,
        accepting_claims: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Option<ThreadEpisodicIndexJobRecord>> {
        // Do not replay this transaction after an ambiguous commit. Physical
        // serialization is provided by a lock on the durable job row.
        async {
            #[cfg(any(test, feature = "test-support"))]
            {
                if self.episodic_claim_test_state.take_episodic_claim_fault_for_test(&snapshot.id, "panic_poll") {
                    panic!("controlled claim polling unwind");
                }
                if self.episodic_claim_test_state.take_episodic_claim_fault_for_test(&snapshot.id, "storage") {
                    anyhow::bail!("controlled claim storage failure");
                }
                if self.episodic_claim_test_state.take_episodic_claim_fault_for_test(&snapshot.id, "lock") {
                    self.connection.execute_unprepared("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001', MESSAGE='fixture claim conflict'; END $$").await?;
                }
            }
            let claimed = self.claim_thread_episodic_index_job_once(
                    snapshot, now_unix, max_attempts, mutation_started, accepting_claims,
                )
                .await?;
            #[cfg(any(test, feature = "test-support"))]
            {
                if self.episodic_claim_test_state.take_episodic_claim_fault_for_test(&snapshot.id, "panic_commit") {
                    panic!("controlled claim acknowledgement unwind");
                }
                if self.episodic_claim_test_state.take_episodic_claim_fault_for_test(&snapshot.id, "commit_unknown") {
                    anyhow::bail!("controlled loss of claim acknowledgement");
                }
            }
            #[cfg(any(test, feature = "test-support"))]
            if self
                .episodic_claim_test_state.episodic_ambiguous_claim_commit
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                anyhow::bail!("controlled loss of episodic claim commit acknowledgement");
            }
            Ok(claimed)
        }
        .await
    }

    pub async fn claim_due_thread_episodic_index_jobs_for_workspace(
        &self,
        workspace_id: &str,
        now_unix: i64,
        limit: u64,
        max_attempts: i64,
    ) -> Result<ThreadEpisodicIndexClaimBatch> {
        #[cfg(any(test, feature = "test-support"))]
        self.episodic_claim_test_state
            .episodic_claim_discoveries
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let candidates = thread_episodic_repository::list_due_index_jobs_for_workspace(
            &self.connection,
            workspace_id,
            unix_to_datetime(now_unix),
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_index_job_record_from_model)
        .collect::<Result<Vec<_>>>()?;
        let mut batch = ThreadEpisodicIndexClaimBatch::default();
        for candidate in candidates {
            use futures_util::FutureExt;
            let result = std::panic::AssertUnwindSafe(async {
                #[cfg(any(test, feature = "test-support"))]
                if self
                    .episodic_claim_test_state
                    .take_episodic_claim_fault_for_test(&candidate.id, "panic_create")
                {
                    panic!("controlled claim creation unwind");
                }
                self.claim_thread_episodic_index_job_from_snapshot(
                    &candidate,
                    now_unix,
                    max_attempts,
                    None,
                    None,
                )
                .await
            })
            .catch_unwind()
            .await;
            match result {
                Ok(Ok(Some(claim))) => batch.claimed.push(claim),
                Ok(Ok(None)) => {}
                Ok(Err(_)) => batch.failures.push((candidate, "claim_storage")),
                Err(_) => batch.failures.push((candidate, "claim_unwind")),
            }
        }
        Ok(batch)
    }

    pub async fn mark_thread_episodic_item_indexed(
        &self,
        index_item_id: &str,
        update: ThreadEpisodicItemIndexedUpdate,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        self.retry_write(|| {
            let index_item_id = index_item_id.to_owned();
            let update = update.clone();
            async move {
                thread_episodic_repository::mark_item_indexed(
                    &self.connection,
                    index_item_id.as_str(),
                    update,
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(crate::thread_episodic::thread_episodic_item_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn mark_thread_episodic_item_failed(
        &self,
        index_item_id: &str,
        now_unix: i64,
    ) -> Result<Option<ThreadEpisodicItemRecord>> {
        self.retry_write(|| {
            let index_item_id = index_item_id.to_owned();
            async move {
                thread_episodic_repository::mark_item_failed(
                    &self.connection,
                    index_item_id.as_str(),
                    unix_to_datetime(now_unix),
                )
                .await?
                .map(crate::thread_episodic::thread_episodic_item_record_from_model)
                .transpose()
            }
        })
        .await
    }

    pub async fn tombstone_thread_episodic_items_for_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        now_unix: i64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        self.retry_write(|| async move {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin thread episodic item tombstone transaction")?;
            let rows = thread_episodic_repository::mark_items_deleted_by_source_item(
                &transaction,
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                unix_to_datetime(now_unix),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit thread episodic item tombstone")?;
            rows.into_iter()
                .map(crate::thread_episodic::thread_episodic_item_record_from_model)
                .collect()
        })
        .await
    }

    pub async fn tombstone_thread_episodic_items_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        now_unix: i64,
    ) -> Result<Vec<ThreadEpisodicItemRecord>> {
        self.retry_write(|| async move {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin thread episodic thread tombstone transaction")?;
            let rows = thread_episodic_repository::mark_items_deleted_for_thread(
                &transaction,
                workspace_id,
                thread_id,
                unix_to_datetime(now_unix),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit thread episodic thread tombstone")?;
            rows.into_iter()
                .map(crate::thread_episodic::thread_episodic_item_record_from_model)
                .collect()
        })
        .await
    }

    pub async fn find_thread_episodic_exclusion_by_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        index_item_id: &str,
    ) -> Result<Option<ThreadEpisodicExclusionRecord>> {
        thread_episodic_repository::find_exclusion_by_index_item(
            &self.connection,
            workspace_id,
            thread_id,
            index_item_id,
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_exclusion_record_from_model)
        .transpose()
    }

    pub async fn thread_episodic_source_occurrence_is_excluded(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Result<bool> {
        thread_episodic_repository::source_occurrence_is_excluded(
            &self.connection,
            workspace_id,
            thread_id,
            turn_id,
            item_id,
        )
        .await
    }

    pub async fn exclude_thread_episodic_item(
        &self,
        exclusion: NewThreadEpisodicExclusionRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicExclusionRecord> {
        self.retry_write(|| {
            let exclusion = exclusion.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin thread episodic exclusion transaction")?;
                let row = thread_episodic_repository::insert_exclusion_if_absent(
                    &transaction,
                    exclusion,
                    unix_to_datetime(now_unix),
                )
                .await?;
                thread_episodic_repository::apply_exclusion_to_item(
                    &transaction,
                    row.index_item_id.as_str(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread episodic exclusion")?;
                crate::thread_episodic::thread_episodic_exclusion_record_from_model(row)
            }
        })
        .await
    }

    pub async fn list_thread_episodic_exclusions_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicExclusionRecord>> {
        thread_episodic_repository::list_exclusions_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_exclusion_record_from_model)
        .collect()
    }

    pub async fn list_thread_episodic_recall_events_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicRecallEventRecord>> {
        Ok(thread_episodic_repository::list_recall_events_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
            limit,
        )
        .await?
        .into_iter()
        .map(crate::thread_episodic::thread_episodic_recall_event_record_from_model)
        .collect())
    }

    pub async fn insert_thread_episodic_recall_event(
        &self,
        event: NewThreadEpisodicRecallEventRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicRecallEventRecord> {
        self.retry_write(|| {
            let event = event.clone();
            async move {
                let row = thread_episodic_repository::insert_recall_event(
                    &self.connection,
                    event,
                    unix_to_datetime(now_unix),
                )
                .await?;
                Ok(crate::thread_episodic::thread_episodic_recall_event_record_from_model(row))
            }
        })
        .await
    }

    pub async fn find_thread_episodic_thread_directory_entry(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<Option<ThreadEpisodicThreadDirectoryRecord>> {
        Ok(thread_episodic_repository::find_thread_directory_entry(
            &self.connection,
            workspace_id,
            thread_id,
        )
        .await?
        .map(crate::thread_episodic::thread_episodic_thread_directory_record_from_model))
    }

    pub async fn upsert_thread_episodic_thread_directory_entry(
        &self,
        record: NewThreadEpisodicThreadDirectoryRecord,
        now_unix: i64,
    ) -> Result<ThreadEpisodicThreadDirectoryRecord> {
        self.retry_write(|| {
            let record = record.clone();
            async move {
                let row = thread_episodic_repository::upsert_thread_directory_entry(
                    &self.connection,
                    record,
                    unix_to_datetime(now_unix),
                )
                .await?;
                Ok(crate::thread_episodic::thread_episodic_thread_directory_record_from_model(row))
            }
        })
        .await
    }

    pub async fn list_selectable_thread_episodic_thread_directory_entries(
        &self,
        selection: ThreadEpisodicThreadDirectorySelection,
    ) -> Result<Vec<ThreadEpisodicThreadDirectoryRecord>> {
        Ok(
            thread_episodic_repository::list_selectable_thread_directory_entries(
                &self.connection,
                selection,
            )
            .await?
            .into_iter()
            .map(crate::thread_episodic::thread_episodic_thread_directory_record_from_model)
            .collect(),
        )
    }

    pub async fn list_thread_episodic_thread_directory_entries_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicThreadDirectoryRecord>> {
        Ok(
            thread_episodic_repository::list_thread_directory_entries_for_workspace(
                &self.connection,
                workspace_id,
                limit,
            )
            .await?
            .into_iter()
            .map(crate::thread_episodic::thread_episodic_thread_directory_record_from_model)
            .collect(),
        )
    }

    pub async fn list_thread_episodic_thread_directory_entries_for_workspace_scoped(
        &self,
        workspace_id: &str,
        allowed_thread_ids: &[String],
        limit: u64,
    ) -> Result<Vec<ThreadEpisodicThreadDirectoryRecord>> {
        Ok(
            thread_episodic_repository::list_thread_directory_entries_for_workspace_scoped(
                &self.connection,
                workspace_id,
                allowed_thread_ids,
                limit,
            )
            .await?
            .into_iter()
            .map(crate::thread_episodic::thread_episodic_thread_directory_record_from_model)
            .collect(),
        )
    }

    pub async fn count_active_thread_episodic_items_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<i64> {
        thread_episodic_repository::count_active_items_for_thread(
            &self.connection,
            workspace_id,
            thread_id,
        )
        .await
    }

    pub async fn get_thread_episodic_canonical_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<ThreadEpisodicCanonicalItem>> {
        let Some((actual_thread_id, actual_workspace_id)) =
            turn::get_turn_location(&self.connection, turn_id).await?
        else {
            return Ok(None);
        };
        if actual_thread_id != thread_id || actual_workspace_id != workspace_id {
            anyhow::bail!(
                "canonical thread episodic source ownership mismatch for `{workspace_id}/{thread_id}/{turn_id}/{item_id}`"
            );
        }
        use pioneer_entity::turn_item as stored;
        let query = stored::Entity::find()
            .filter(stored::Column::TurnId.eq(turn_id))
            .filter(stored::Column::ItemId.eq(item_id));
        let Some((model, raw)) = turn_payload::exact_items(&self.connection, query)
            .await?
            .pop()
        else {
            return Ok(None);
        };
        let item = serde_json::from_str::<TurnItem>(&raw).with_context(|| {
            format!("failed to decode canonical thread episodic turn item `{turn_id}/{item_id}`")
        })?;
        if item.item_id() != item_id {
            anyhow::bail!(
                "canonical thread episodic source item identity mismatch for `{turn_id}/{item_id}`"
            );
        }
        Ok(Some(ThreadEpisodicCanonicalItem {
            committed: thread_episodic_source::source_status_is_committed(model.status.as_deref()),
            item,
            source_payload: raw,
        }))
    }

    pub async fn prepare_terminal_payload_episodic_save(
        &self,
        row: &pioneer_entity::turn_item::Model,
        payload_json: &str,
    ) -> Result<Option<episodic_source_store::PreparedSourceSave>> {
        let item: TurnItem = serde_json::from_str(payload_json)?;
        // Terminalization of private tool output changes no admissible indexed
        // source. Do not discover the surrounding history for such repairs.
        if thread_episodic_source::committed_item_source_actor_role(&item).is_none() {
            return Ok(None);
        }
        let (thread_id, workspace_id) = turn::get_turn_location(&self.connection, &row.turn_id)
            .await?
            .context("payload repair source ownership missing")?;
        episodic_source_store::prepare_episodic_source_save(
            &self.connection,
            &workspace_id,
            &thread_id,
            &row.turn_id,
            &item,
        )
        .await
        .map(Some)
    }

    pub async fn search_thread_episodic_projection(
        &self,
        request: &ThreadEpisodicSearchQuery,
    ) -> Result<ThreadEpisodicSearchResult> {
        crate::episodic_search::search_projection(&self.connection, request).await
    }
}

#[cfg(test)]
#[path = "episodic_requeue_tests.rs"]
mod requeue_tests;
