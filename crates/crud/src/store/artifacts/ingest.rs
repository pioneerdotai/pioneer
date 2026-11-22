use crate::{
    ArtifactBindingTargetRecord, ArtifactBlobRecord, ArtifactExternalRefKey,
    ArtifactExternalRefRecord, ArtifactGcPlanRecord, ArtifactListFilterRecord,
    ArtifactListPageRecord, ArtifactProjectionBlobRecord, ArtifactProjectionRecord, ArtifactRecord,
    ArtifactVersionBlobRecord, ArtifactWorkspaceUsageRecord, ConversationArtifactRefLimits,
    ConversationTurnArtifactRefs, CrudStore, IngestArtifactMetadataRecord, IngestedArtifactRecord,
    NewArtifactBlobRecord, UpsertArtifactExternalRefRequest, find_turn_work_projection,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_artifacts::artifact as artifact_repository;
use pioneer_protocol::{
    ArtifactBindingSummary, ArtifactProjectionKind, ArtifactProjectionStatus, ArtifactStatus,
    ArtifactSummary,
};
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::collections::{BTreeMap, HashMap};

impl CrudStore {
    pub async fn ingest_artifact_metadata(
        &self,
        blob: NewArtifactBlobRecord,
        artifact: IngestArtifactMetadataRecord,
        binding: Option<ArtifactBindingTargetRecord>,
        version_metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<IngestedArtifactRecord> {
        self.ingest_artifact_metadata_with_identity(
            blob,
            artifact,
            binding,
            version_metadata,
            None,
            None,
            None,
        )
        .await
    }

    pub async fn ingest_artifact_metadata_with_identity(
        &self,
        blob: NewArtifactBlobRecord,
        artifact: IngestArtifactMetadataRecord,
        binding: Option<ArtifactBindingTargetRecord>,
        version_metadata: BTreeMap<String, serde_json::Value>,
        identity: Option<String>,
        limits: Option<(u64, u64)>,
        commit_guard: Option<(u64, tokio_util::sync::CancellationToken)>,
    ) -> Result<IngestedArtifactRecord> {
        let prepared = pioneer_crud_artifacts::ingest::PreparedArtifactIngest::new(
            blob,
            artifact,
            binding,
            version_metadata,
            identity,
            limits,
        )?;
        let store = pioneer_crud_artifacts::ingest::ArtifactIngestStore::new(
            &self.connection,
            &self.html_preview_io_work,
        );
        if prepared.publication_target().is_none() {
            return store
                .commit_without_publication(prepared, commit_guard)
                .await;
        }
        let maintenance = self.connection.work_signal("maintenance.html").changing();
        let result = self
            .retry_write_on_conflict(|| {
                let prepared = prepared.clone();
                let publication = prepared.publication_target().cloned();
                let at = prepared.prepared_at();
                let commit_guard = commit_guard.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin artifact ingest transaction")?;
                    let transaction = pioneer_crud_artifacts::ingest::authorize_artifact_ingest(
                        transaction,
                        commit_guard.as_ref(),
                    )
                    .await?;
                    let record = match prepared.apply(&transaction).await {
                        Ok(record) => record,
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(error.into());
                        }
                    };

                    if let Some(target) = publication {
                        let turn_id = target.turn_id.as_deref().unwrap_or_default();
                        if find_turn_work_projection(&transaction, turn_id)
                            .await?
                            .is_none()
                        {
                            pioneer_crud_timeline::timeline_live_projection::project_cli_runtime_turn_binding_state(
                                &transaction, turn_id, at,
                            ).await?;
                            artifact_repository::ArtifactRepository::new()
                                .touch_html_turn_projection(
                                    &transaction,
                                    &record.artifact.workspace_id,
                                    target.thread_id.as_deref().unwrap_or_default(),
                                    turn_id,
                                    at,
                                )
                                .await?;
                        }
                    }
                    if commit_guard
                        .as_ref()
                        .is_some_and(|(_, cancel)| cancel.is_cancelled())
                    {
                        transaction.rollback().await?;
                        bail!("HTML invocation cancelled before commit");
                    }
                    transaction
                        .commit()
                        .await
                        .context("failed to commit artifact ingest transaction")?;
                    Ok(record)
                }
            })
            .await;
        if result.is_ok() {
            if prepared.has_released_copies() {
                self.notify_html_preview_io_work();
            }
            maintenance.completed(true);
        }
        result
    }

    pub async fn find_or_create_artifact_blob(
        &self,
        blob: NewArtifactBlobRecord,
    ) -> Result<ArtifactBlobRecord> {
        pioneer_crud_artifacts::mutations::find_or_create_blob(&self.connection, blob).await
    }

    pub async fn bind_artifact(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
        target: ArtifactBindingTargetRecord,
        metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<ArtifactBindingSummary> {
        pioneer_crud_artifacts::mutations::bind_artifact(
            &self.connection,
            workspace_id,
            artifact_id,
            version_id,
            target,
            metadata,
        )
        .await
    }

    pub async fn get_artifact_summary(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> Result<ArtifactSummary> {
        artifact_repository::ArtifactRepository::new()
            .get_artifact_summary(&self.connection, workspace_id, artifact_id, version_id)
            .await
            .map_err(Into::into)
    }

    pub async fn html_publication_summaries(
        &self,
        workspace_id: &str,
        sources: &[(String, String)],
    ) -> Result<HashMap<(String, String), Vec<ArtifactSummary>>> {
        artifact_repository::ArtifactRepository::new()
            .html_publication_summaries(&self.connection, workspace_id, sources)
            .await
            .map_err(Into::into)
    }

    pub async fn list_exact_artifact_refs(
        &self,
        workspace_id: &str,
        requested: &[(String, String)],
    ) -> Result<HashMap<(String, String), pioneer_protocol::ArtifactRef>> {
        artifact_repository::ArtifactRepository::new()
            .list_exact_artifact_refs(&self.connection, workspace_id, requested)
            .await
            .map_err(Into::into)
    }

    pub async fn get_artifact_version_blob(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> Result<ArtifactVersionBlobRecord> {
        artifact_repository::ArtifactRepository::new()
            .get_artifact_version_blob(&self.connection, workspace_id, artifact_id, version_id)
            .await
            .map_err(Into::into)
    }

    pub async fn get_artifact_projection_blob(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
        projection_kind: ArtifactProjectionKind,
    ) -> Result<ArtifactProjectionBlobRecord> {
        artifact_repository::ArtifactRepository::new()
            .get_artifact_projection_blob(
                &self.connection,
                workspace_id,
                artifact_id,
                version_id,
                projection_kind,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn list_thread_artifacts(
        &self,
        workspace_id: &str,
        thread_id: &str,
        filter: ArtifactListFilterRecord,
    ) -> Result<ArtifactListPageRecord> {
        artifact_repository::ArtifactRepository::new()
            .list_thread_artifacts(&self.connection, workspace_id, thread_id, filter)
            .await
            .map_err(Into::into)
    }

    pub async fn list_artifacts(
        &self,
        workspace_id: &str,
        filter: ArtifactListFilterRecord,
    ) -> Result<ArtifactListPageRecord> {
        artifact_repository::ArtifactRepository::new()
            .list_artifacts(&self.connection, workspace_id, filter)
            .await
            .map_err(Into::into)
    }

    pub async fn list_artifact_ids_for_authorized_thread_roots(
        &self,
        workspace_id: &str,
        authorized_thread_roots: &[Vec<String>],
    ) -> Result<Vec<String>> {
        pioneer_crud_access::authorization_scope::list_artifact_ids_for_authorized_thread_roots(
            &self.connection,
            workspace_id,
            authorized_thread_roots,
        )
        .await
    }

    pub async fn list_conversation_artifact_refs(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_ids: &[String],
        limits: ConversationArtifactRefLimits,
    ) -> Result<BTreeMap<String, ConversationTurnArtifactRefs>> {
        artifact_repository::ArtifactRepository::new()
            .list_conversation_artifact_refs(
                &self.connection,
                workspace_id,
                thread_id,
                turn_ids,
                limits,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn update_artifact_status(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        status: ArtifactStatus,
        deleted_at: Option<DateTimeWithTimeZone>,
    ) -> Result<ArtifactRecord> {
        pioneer_crud_artifacts::mutations::update_status(
            &self.connection,
            workspace_id,
            artifact_id,
            status,
            deleted_at,
        )
        .await
    }

    pub async fn replace_artifact_projection(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: &str,
        projection_kind: ArtifactProjectionKind,
        status: ArtifactProjectionStatus,
        text_content: Option<String>,
        metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<ArtifactProjectionRecord> {
        self.replace_artifact_projection_with_blob(
            workspace_id,
            artifact_id,
            artifact_version_id,
            projection_kind,
            status,
            text_content,
            None,
            metadata,
        )
        .await
    }

    pub async fn replace_artifact_projection_with_blob(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: &str,
        projection_kind: ArtifactProjectionKind,
        status: ArtifactProjectionStatus,
        text_content: Option<String>,
        blob_id: Option<String>,
        metadata: BTreeMap<String, serde_json::Value>,
    ) -> Result<ArtifactProjectionRecord> {
        pioneer_crud_artifacts::mutations::replace_projection(
            &self.connection,
            workspace_id,
            artifact_id,
            artifact_version_id,
            projection_kind,
            status,
            text_content,
            blob_id,
            metadata,
        )
        .await
    }

    pub async fn list_artifact_projections(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: Option<&str>,
    ) -> Result<Vec<ArtifactProjectionRecord>> {
        artifact_repository::list_projections(
            &self.connection,
            workspace_id,
            artifact_id,
            artifact_version_id,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn find_active_artifact_external_ref(
        &self,
        key: &ArtifactExternalRefKey,
        now_unix_ms: i64,
    ) -> Result<Option<ArtifactExternalRefRecord>> {
        artifact_repository::find_active_external_ref(&self.connection, key, now_unix_ms)
            .await
            .map_err(Into::into)
    }

    pub async fn upsert_artifact_external_ref(
        &self,
        request: UpsertArtifactExternalRefRequest,
    ) -> Result<ArtifactExternalRefRecord> {
        pioneer_crud_artifacts::mutations::upsert_external_ref(&self.connection, request).await
    }

    pub async fn prune_expired_artifact_external_refs(
        &self,
        workspace_id: &str,
        now_unix_ms: i64,
    ) -> Result<u64> {
        pioneer_crud_artifacts::mutations::prune_expired_external_refs(
            &self.connection,
            workspace_id,
            now_unix_ms,
        )
        .await
    }

    pub async fn artifact_workspace_usage(
        &self,
        workspace_id: &str,
    ) -> Result<ArtifactWorkspaceUsageRecord> {
        artifact_repository::workspace_usage(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn plan_artifact_gc(
        &self,
        workspace_id: &str,
        now_unix_ms: i64,
        grace_secs: u64,
    ) -> Result<ArtifactGcPlanRecord> {
        artifact_repository::plan_gc_with_grace(
            &self.connection,
            workspace_id,
            now_unix_ms,
            grace_secs,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn delete_artifact_blob_row(&self, workspace_id: &str, blob_id: &str) -> Result<u64> {
        pioneer_crud_artifacts::mutations::delete_blob_row(&self.connection, workspace_id, blob_id)
            .await
    }

    pub async fn delete_artifact_projection_row(
        &self,
        workspace_id: &str,
        projection_id: &str,
    ) -> Result<u64> {
        pioneer_crud_artifacts::mutations::delete_projection_row(
            &self.connection,
            workspace_id,
            projection_id,
        )
        .await
    }

    pub async fn count_artifacts_by_workspace(&self, workspace_id: &str) -> Result<u64> {
        artifact_repository::count_artifacts_by_workspace(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn count_artifact_blobs_by_workspace(&self, workspace_id: &str) -> Result<u64> {
        artifact_repository::count_blobs_by_workspace(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn count_artifact_versions_by_workspace(&self, workspace_id: &str) -> Result<u64> {
        artifact_repository::count_versions_by_workspace(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn count_artifact_bindings_by_workspace(&self, workspace_id: &str) -> Result<u64> {
        artifact_repository::count_bindings_by_workspace(&self.connection, workspace_id)
            .await
            .map_err(Into::into)
    }

    pub async fn insert_test_artifact_blob(
        &self,
        record: NewArtifactBlobRecord,
        created_at_unix_ms: i64,
        id: String,
    ) -> Result<ArtifactBlobRecord> {
        self.retry_write_on_conflict(|| {
            let record = record.clone();
            let id = id.clone();
            async move {
                artifact_repository::insert_test_blob(
                    &self.connection,
                    record,
                    created_at_unix_ms,
                    id,
                )
                .await
                .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn update_test_artifact_status(&self, artifact_id: &str, status: &str) -> Result<()> {
        self.retry_write_on_conflict(|| {
            let artifact_id = artifact_id.to_owned();
            let status = status.to_owned();
            async move {
                artifact_repository::update_test_artifact_status(
                    &self.connection,
                    &artifact_id,
                    &status,
                )
                .await
                .map_err(Into::into)
            }
        })
        .await
    }
}
