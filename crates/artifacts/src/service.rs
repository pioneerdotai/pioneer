use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use pioneer_crud::{
    ArtifactBindingTargetRecord, ArtifactListFilterRecord, CrudStore, IngestArtifactMetadataRecord,
    NewArtifactBlobRecord,
};
use pioneer_protocol::{
    ArtifactBindingSummary, ArtifactReadParams, ArtifactReadResponse, ArtifactRef, ArtifactStatus,
    ArtifactSummary,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::blob_store::{ArtifactBlobInput, ArtifactBlobStore};
use crate::error::{ArtifactError, ArtifactResult};
use crate::gc::{
    ArtifactGcPlan, ArtifactGcPolicy, ArtifactGcReport, execute_gc_with_policy, plan_gc_with_policy,
};
use crate::mime::{OCTET_STREAM, classify_kind, sanitize_display_name};
use crate::models::{
    ArtifactListFilter, ArtifactListPage, BindArtifactRequest, IngestArtifactBytesRequest,
    IngestArtifactTempFileRequest,
};
use crate::projection::{
    ArtifactProjectionRecord, create_inline_projections, list_projections,
    supports_inline_plain_text_projection, supports_thumbnail_projection,
};
use crate::quota::{ArtifactQuotaPolicy, ArtifactWorkspaceUsage};
use crate::security::read_validated_local_file;
use crate::source::{ArtifactSource, IngestArtifactSourceRequest};

#[derive(Clone)]
pub struct ArtifactService {
    pub(crate) store: Arc<CrudStore>,
    pub(crate) blob_store: Arc<dyn ArtifactBlobStore>,
    quota_policy: ArtifactQuotaPolicy,
    gc_policy: ArtifactGcPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDownloadSnapshot {
    pub artifact: ArtifactRef,
    pub workspace_id: String,
    pub artifact_id: String,
    pub artifact_version_id: String,
    pub blob_id: String,
    pub storage_key: String,
    pub display_name: String,
    pub mime_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
}

impl ArtifactService {
    pub fn new(store: Arc<CrudStore>, blob_store: Arc<dyn ArtifactBlobStore>) -> Self {
        Self {
            store,
            blob_store,
            quota_policy: ArtifactQuotaPolicy::default(),
            gc_policy: ArtifactGcPolicy::default(),
        }
    }

    pub fn new_with_quota(
        store: Arc<CrudStore>,
        blob_store: Arc<dyn ArtifactBlobStore>,
        quota_policy: ArtifactQuotaPolicy,
    ) -> Self {
        Self {
            store,
            blob_store,
            quota_policy,
            gc_policy: ArtifactGcPolicy::default(),
        }
    }

    pub fn new_with_policies(
        store: Arc<CrudStore>,
        blob_store: Arc<dyn ArtifactBlobStore>,
        quota_policy: ArtifactQuotaPolicy,
        gc_policy: ArtifactGcPolicy,
    ) -> Self {
        Self {
            store,
            blob_store,
            quota_policy,
            gc_policy,
        }
    }

    pub async fn ingest_bytes(
        &self,
        request: IngestArtifactBytesRequest,
    ) -> ArtifactResult<ArtifactSummary> {
        validate_ingest_request(&request)?;
        self.enforce_quota(&request.workspace_id, request.bytes.len() as u64)
            .await?;
        let projection_bytes = (!request.bytes.is_empty()).then(|| request.bytes.clone());
        self.ingest_blob_input(
            request.clone(),
            ArtifactBlobInput::Bytes(request.bytes.clone()),
            projection_bytes,
        )
        .await
    }

    pub async fn ingest_temp_file(
        &self,
        request: IngestArtifactTempFileRequest,
    ) -> ArtifactResult<ArtifactSummary> {
        validate_workspace_id(&request.workspace_id)?;
        validate_non_empty("display_name", &request.display_name)?;
        let file_metadata = tokio::fs::metadata(&request.temp_path)
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to stat temp artifact input {}",
                    request.temp_path.display()
                ),
                source,
            })?;
        let file_size = file_metadata.len();
        if file_size > i64::MAX as u64 {
            return Err(ArtifactError::InvalidRequest {
                message: "artifact temp file exceeds i64 storage limit".to_owned(),
            });
        }
        self.enforce_quota(&request.workspace_id, file_size).await?;
        let projection_bytes = usize::try_from(file_size).ok().and_then(|size| {
            supports_inline_plain_text_projection(request.kind, request.mime_type.as_deref(), size)
                .then_some(size)
        });
        let projection_bytes = if projection_bytes.is_some() && file_size > 0 {
            Some(
                tokio::fs::read(&request.temp_path)
                    .await
                    .map_err(|source| ArtifactError::Io {
                        message: format!(
                            "failed to read temp artifact input {} for projection",
                            request.temp_path.display()
                        ),
                        source,
                    })?,
            )
        } else {
            None
        };
        let metadata_request = IngestArtifactBytesRequest {
            workspace_id: request.workspace_id,
            primary_thread_id: request.primary_thread_id,
            bytes: Vec::new(),
            display_name: request.display_name,
            kind: request.kind,
            mime_type: request.mime_type,
            created_by_kind: request.created_by_kind,
            created_by_actor_id: request.created_by_actor_id,
            binding: request.binding,
            metadata: request.metadata,
        };
        self.ingest_blob_input(
            metadata_request,
            ArtifactBlobInput::TempFile {
                path: request.temp_path,
            },
            projection_bytes,
        )
        .await
    }

    async fn ingest_blob_input(
        &self,
        request: IngestArtifactBytesRequest,
        input: ArtifactBlobInput,
        projection_bytes: Option<Vec<u8>>,
    ) -> ArtifactResult<ArtifactSummary> {
        let stored_blob = self
            .blob_store
            .put_bytes(&request.workspace_id, input)
            .await?;

        let ingested = self
            .store
            .ingest_artifact_metadata(
                NewArtifactBlobRecord {
                    workspace_id: request.workspace_id.clone(),
                    sha256: stored_blob.sha256,
                    size_bytes: stored_blob.size_bytes,
                    mime_type: request.mime_type.clone(),
                    storage_backend: stored_blob.storage_backend,
                    storage_key: stored_blob.storage_key,
                    metadata: Default::default(),
                },
                metadata_record_from_ingest(&request),
                request
                    .binding
                    .as_ref()
                    .map(binding_target_record_from_model),
                request.metadata.clone(),
            )
            .await?;

        if projection_bytes.is_some()
            || supports_thumbnail_projection(request.kind, request.mime_type.as_deref())
        {
            let bytes = projection_bytes.as_deref().unwrap_or(&[]);
            let _ = create_inline_projections(
                self.store.as_ref(),
                &request.workspace_id,
                &ingested.artifact.id,
                &ingested.version.id,
                request.kind,
                request.mime_type.as_deref(),
                bytes,
            )
            .await;
        }
        let summary = self
            .store
            .get_artifact_summary(&request.workspace_id, &ingested.artifact.id, None)
            .await?;
        Ok(summary)
    }

    pub async fn ingest_source(
        &self,
        request: IngestArtifactSourceRequest,
    ) -> ArtifactResult<ArtifactSummary> {
        match request.source {
            ArtifactSource::Bytes(bytes) => {
                let mime_type = request.mime_type;
                let kind = request
                    .kind
                    .unwrap_or_else(|| classify_kind(mime_type.as_deref(), None));
                self.ingest_bytes(IngestArtifactBytesRequest {
                    workspace_id: request.workspace_id,
                    primary_thread_id: request.primary_thread_id,
                    bytes,
                    display_name: request
                        .display_name
                        .as_deref()
                        .map(sanitize_display_name)
                        .unwrap_or_else(|| "artifact".to_owned()),
                    kind,
                    mime_type: Some(mime_type.unwrap_or_else(|| OCTET_STREAM.to_owned())),
                    created_by_kind: request.created_by_kind,
                    created_by_actor_id: request.created_by_actor_id,
                    binding: request.binding,
                    metadata: request.metadata,
                })
                .await
            }
            ArtifactSource::LocalPath(path) => {
                let policy =
                    request
                        .local_path_policy
                        .ok_or_else(|| ArtifactError::LocalPathRejected {
                            message: "local path policy is required".to_owned(),
                        })?;
                let local_file = read_validated_local_file(&path, &policy).await?;
                let mime_type = request
                    .mime_type
                    .unwrap_or_else(|| local_file.mime_type.clone());
                let kind = request.kind.unwrap_or_else(|| {
                    classify_kind(Some(&mime_type), Some(local_file.canonical_path.as_path()))
                });
                let display_name = request
                    .display_name
                    .as_deref()
                    .map(sanitize_display_name)
                    .unwrap_or_else(|| local_file.display_name.clone());
                let mut metadata = request.metadata;
                append_local_path_metadata(&mut metadata, &local_file);

                self.ingest_bytes(IngestArtifactBytesRequest {
                    workspace_id: request.workspace_id,
                    primary_thread_id: request.primary_thread_id,
                    bytes: local_file.bytes,
                    display_name,
                    kind,
                    mime_type: Some(mime_type),
                    created_by_kind: request.created_by_kind,
                    created_by_actor_id: request.created_by_actor_id,
                    binding: request.binding,
                    metadata,
                })
                .await
            }
        }
    }

    pub async fn bind_artifact(
        &self,
        request: BindArtifactRequest,
    ) -> ArtifactResult<ArtifactBindingSummary> {
        validate_workspace_id(&request.workspace_id)?;
        validate_non_empty("artifact_id", &request.artifact_id)?;
        Ok(self
            .store
            .bind_artifact(
                &request.workspace_id,
                &request.artifact_id,
                request.version_id.as_deref(),
                binding_target_record_from_model(&request.target),
                request.metadata,
            )
            .await?)
    }

    pub async fn get_artifact(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> ArtifactResult<ArtifactSummary> {
        validate_workspace_id(workspace_id)?;
        validate_non_empty("artifact_id", artifact_id)?;
        Ok(self
            .store
            .get_artifact_summary(workspace_id, artifact_id, version_id)
            .await?)
    }

    pub async fn list_thread_artifacts(
        &self,
        workspace_id: &str,
        thread_id: &str,
        filter: ArtifactListFilter,
    ) -> ArtifactResult<ArtifactListPage> {
        validate_workspace_id(workspace_id)?;
        validate_non_empty("thread_id", thread_id)?;
        let page = self
            .store
            .list_thread_artifacts(workspace_id, thread_id, list_filter_record(filter))
            .await?;
        Ok(ArtifactListPage {
            items: page.items,
            next_cursor: page.next_cursor,
        })
    }

    pub async fn list_artifacts(
        &self,
        workspace_id: &str,
        filter: ArtifactListFilter,
    ) -> ArtifactResult<ArtifactListPage> {
        validate_workspace_id(workspace_id)?;
        let page = self
            .store
            .list_artifacts(workspace_id, list_filter_record(filter))
            .await?;
        Ok(ArtifactListPage {
            items: page.items,
            next_cursor: page.next_cursor,
        })
    }

    pub async fn workspace_usage(
        &self,
        workspace_id: &str,
    ) -> ArtifactResult<ArtifactWorkspaceUsage> {
        let usage = self.store.artifact_workspace_usage(workspace_id).await?;
        let mut usage = ArtifactWorkspaceUsage {
            workspace_id: usage.workspace_id,
            bytes: usage.bytes,
            files: usage.files,
            warning: None,
        };
        usage.warning = self.quota_policy.workspace_warning(&usage);
        Ok(usage)
    }

    pub async fn list_projections(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        artifact_version_id: Option<&str>,
    ) -> ArtifactResult<Vec<ArtifactProjectionRecord>> {
        list_projections(
            self.store.as_ref(),
            workspace_id,
            artifact_id,
            artifact_version_id,
        )
        .await
    }

    pub async fn gc_dry_run(
        &self,
        workspace_id: &str,
        now_unix_ms: i64,
    ) -> ArtifactResult<ArtifactGcPlan> {
        plan_gc_with_policy(
            self.store.as_ref(),
            workspace_id,
            now_unix_ms,
            self.gc_policy,
        )
        .await
    }

    pub async fn gc_execute(
        &self,
        workspace_id: &str,
        now_unix_ms: i64,
    ) -> ArtifactResult<ArtifactGcReport> {
        execute_gc_with_policy(
            self.store.as_ref(),
            self.blob_store.as_ref(),
            workspace_id,
            now_unix_ms,
            self.gc_policy,
        )
        .await
    }

    pub async fn read_artifact(
        &self,
        params: ArtifactReadParams,
        max_read_bytes: u64,
    ) -> ArtifactResult<ArtifactReadResponse> {
        validate_workspace_id(&params.workspace_id)?;
        validate_non_empty("artifact_id", &params.artifact_id)?;
        let max_read_bytes = max_read_bytes.max(1);
        let summary = self
            .get_artifact(
                params.workspace_id.as_str(),
                params.artifact_id.as_str(),
                params.version_id.as_deref(),
            )
            .await?;
        if summary.artifact.status != ArtifactStatus::Ready {
            return Err(ArtifactError::InvalidRequest {
                message: format!(
                    "artifact `{}` is not ready for read: {:?}",
                    params.artifact_id, summary.artifact.status
                ),
            });
        }

        let blob = self
            .store
            .get_artifact_version_blob(
                params.workspace_id.as_str(),
                params.artifact_id.as_str(),
                params.version_id.as_deref(),
            )
            .await?;
        let total_size_bytes = blob.size_bytes;
        let offset = params.offset.unwrap_or(0).min(total_size_bytes);
        let remaining = total_size_bytes.saturating_sub(offset);
        let requested = params.max_bytes.unwrap_or(max_read_bytes).max(1);
        let read_len = remaining.min(requested).min(max_read_bytes);

        let handle = self
            .blob_store
            .open_read(params.workspace_id.as_str(), blob.storage_key.as_str())
            .await?;
        let mut file = handle.into_inner();
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!("failed to seek artifact `{}`", params.artifact_id),
                source,
            })?;
        let mut bytes = Vec::with_capacity(usize::try_from(read_len).unwrap_or_default());
        file.take(read_len)
            .read_to_end(&mut bytes)
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!("failed to read artifact `{}`", params.artifact_id),
                source,
            })?;
        let len = u64::try_from(bytes.len()).unwrap_or_default();

        Ok(ArtifactReadResponse {
            artifact: summary.artifact,
            offset,
            len,
            total_size_bytes,
            sha256: blob.sha256,
            content_base64: BASE64.encode(bytes),
            truncated: offset.saturating_add(len) < total_size_bytes,
        })
    }

    async fn enforce_quota(&self, workspace_id: &str, incoming_bytes: u64) -> ArtifactResult<()> {
        self.quota_policy.check_file_size(incoming_bytes)?;
        let usage = self.workspace_usage(workspace_id).await?;
        self.quota_policy
            .check_workspace_headroom(&usage, incoming_bytes)
    }

    pub async fn download_snapshot(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> ArtifactResult<ArtifactDownloadSnapshot> {
        validate_workspace_id(workspace_id)?;
        validate_non_empty("artifact_id", artifact_id)?;
        let summary = self
            .get_artifact(workspace_id, artifact_id, version_id)
            .await?;
        if summary.artifact.status != ArtifactStatus::Ready {
            return Err(ArtifactError::InvalidRequest {
                message: format!(
                    "artifact `{artifact_id}` is not ready for download: {:?}",
                    summary.artifact.status
                ),
            });
        }

        let blob = self
            .store
            .get_artifact_version_blob(workspace_id, artifact_id, version_id)
            .await?;
        let size_bytes = blob.size_bytes;

        Ok(ArtifactDownloadSnapshot {
            artifact: summary.artifact.clone(),
            workspace_id: workspace_id.to_owned(),
            artifact_id: artifact_id.to_owned(),
            artifact_version_id: blob.artifact_version_id,
            blob_id: blob.blob_id,
            storage_key: blob.storage_key,
            display_name: summary.artifact.display_name,
            mime_type: summary.artifact.mime_type,
            size_bytes,
            sha256: blob.sha256,
        })
    }

    pub async fn read_blob_range(
        &self,
        workspace_id: &str,
        storage_key: &str,
        offset: u64,
        len: u64,
    ) -> ArtifactResult<Vec<u8>> {
        validate_workspace_id(workspace_id)?;
        let handle = self.blob_store.open_read(workspace_id, storage_key).await?;
        let mut file = handle.into_inner();
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|source| ArtifactError::Io {
                message: "failed to seek artifact blob".to_owned(),
                source,
            })?;
        let mut bytes = Vec::with_capacity(usize::try_from(len).unwrap_or_default());
        file.take(len)
            .read_to_end(&mut bytes)
            .await
            .map_err(|source| ArtifactError::Io {
                message: "failed to read artifact blob".to_owned(),
                source,
            })?;
        Ok(bytes)
    }

    pub async fn delete_artifact(
        &self,
        workspace_id: &str,
        artifact_id: &str,
    ) -> ArtifactResult<ArtifactStatus> {
        validate_workspace_id(workspace_id)?;
        validate_non_empty("artifact_id", artifact_id)?;
        self.store
            .update_artifact_status(
                workspace_id,
                artifact_id,
                ArtifactStatus::Deleted,
                Some(Utc::now().fixed_offset()),
            )
            .await?;
        Ok(ArtifactStatus::Deleted)
    }

    pub async fn restore_artifact(
        &self,
        workspace_id: &str,
        artifact_id: &str,
    ) -> ArtifactResult<ArtifactStatus> {
        validate_workspace_id(workspace_id)?;
        validate_non_empty("artifact_id", artifact_id)?;
        let summary = self.get_artifact(workspace_id, artifact_id, None).await?;
        if summary.artifact.status != ArtifactStatus::Deleted {
            return Err(ArtifactError::InvalidRequest {
                message: format!(
                    "artifact `{artifact_id}` cannot be restored from {:?}",
                    summary.artifact.status
                ),
            });
        }
        self.store
            .get_artifact_version_blob(
                workspace_id,
                artifact_id,
                summary.artifact.version_id.as_deref(),
            )
            .await?;
        self.store
            .update_artifact_status(workspace_id, artifact_id, ArtifactStatus::Ready, None)
            .await?;
        Ok(ArtifactStatus::Ready)
    }
}

fn validate_ingest_request(request: &IngestArtifactBytesRequest) -> ArtifactResult<()> {
    validate_workspace_id(&request.workspace_id)?;
    validate_non_empty("display_name", &request.display_name)?;
    if request.bytes.len() > i64::MAX as usize {
        return Err(ArtifactError::InvalidRequest {
            message: "artifact bytes exceed i64 storage limit".to_owned(),
        });
    }
    Ok(())
}

fn validate_workspace_id(workspace_id: &str) -> ArtifactResult<()> {
    validate_non_empty("workspace_id", workspace_id)
}

fn validate_non_empty(field: &str, value: &str) -> ArtifactResult<()> {
    if value.trim().is_empty() {
        return Err(ArtifactError::InvalidRequest {
            message: format!("{field} is required"),
        });
    }
    Ok(())
}

fn append_local_path_metadata(
    metadata: &mut BTreeMap<String, serde_json::Value>,
    local_file: &crate::security::ValidatedLocalFile,
) {
    metadata.insert("source_kind".to_owned(), json!("local_path"));
    metadata.insert(
        "source_file_name".to_owned(),
        json!(local_file.display_name),
    );
    if let Some(original_file_name) = &local_file.original_file_name {
        metadata.insert("original_file_name".to_owned(), json!(original_file_name));
    }
}

fn metadata_record_from_ingest(
    request: &IngestArtifactBytesRequest,
) -> IngestArtifactMetadataRecord {
    IngestArtifactMetadataRecord {
        workspace_id: request.workspace_id.clone(),
        primary_thread_id: request.primary_thread_id.clone(),
        display_name: request.display_name.clone(),
        kind: request.kind,
        mime_type: request.mime_type.clone(),
        created_by_kind: request.created_by_kind,
        created_by_actor_id: request.created_by_actor_id.clone(),
        metadata: request.metadata.clone(),
    }
}

fn binding_target_record_from_model(
    target: &crate::models::ArtifactBindingTarget,
) -> ArtifactBindingTargetRecord {
    ArtifactBindingTargetRecord {
        thread_id: target.thread_id.clone(),
        turn_id: target.turn_id.clone(),
        message_id: target.message_id.clone(),
        turn_item_id: target.turn_item_id.clone(),
        tool_call_id: target.tool_call_id.clone(),
        task_id: target.task_id.clone(),
        task_run_id: target.task_run_id.clone(),
        binding_kind: target.binding_kind,
        direction: target.direction,
        role: target.role,
        item_index: target.item_index,
    }
}

fn list_filter_record(filter: ArtifactListFilter) -> ArtifactListFilterRecord {
    ArtifactListFilterRecord {
        limit: filter.limit,
        cursor: filter.cursor,
        include_deleted: filter.include_deleted,
        kinds: filter.kinds,
        thread_id: filter.thread_id,
        turn_id: filter.turn_id,
        message_id: filter.message_id,
        task_id: filter.task_id,
        task_run_id: filter.task_run_id,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        ArtifactExternalRefKey, CrudStore, NewArtifactBlobRecord, UpsertArtifactExternalRefRequest,
    };
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
        ArtifactProjectionKind, ArtifactProjectionStatus,
    };
    use pioneer_provider::{AttachmentDataSource, InputContentType};
    use sea_orm::{ConnectionTrait, Database};

    use super::*;
    use crate::local_blob_store::LocalArtifactBlobStore;
    use crate::models::{ArtifactBindingTarget, ArtifactListFilter};
    use crate::security::ArtifactLocalPathPolicy;
    use crate::source::{ArtifactSource, IngestArtifactSourceRequest};

    struct TestHarness {
        service: ArtifactService,
        store: Arc<CrudStore>,
        _temp: tempfile::TempDir,
    }

    async fn setup() -> TestHarness {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite memory");
        Migrator::up(&db, None).await.expect("migrations apply");
        db.execute_unprepared(
            "INSERT INTO workspace (id, name, is_active, is_current) VALUES ('ws_a', 'A', 1, 1)",
        )
        .await
        .expect("insert workspace A");
        db.execute_unprepared(
            "INSERT INTO workspace (id, name, is_active, is_current) VALUES ('ws_b', 'B', 1, 0)",
        )
        .await
        .expect("insert workspace B");

        let temp = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(CrudStore::new(db.clone()));
        let blob_store = Arc::new(LocalArtifactBlobStore::new(temp.path()));
        let service = ArtifactService::new(store.clone(), blob_store);

        TestHarness {
            service,
            store,
            _temp: temp,
        }
    }

    async fn setup_with_quota(quota_policy: ArtifactQuotaPolicy) -> TestHarness {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite memory");
        Migrator::up(&db, None).await.expect("migrations apply");
        db.execute_unprepared(
            "INSERT INTO workspace (id, name, is_active, is_current) VALUES ('ws_a', 'A', 1, 1)",
        )
        .await
        .expect("insert workspace A");
        db.execute_unprepared(
            "INSERT INTO workspace (id, name, is_active, is_current) VALUES ('ws_b', 'B', 1, 0)",
        )
        .await
        .expect("insert workspace B");

        let temp = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(CrudStore::new(db.clone()));
        let blob_store = Arc::new(LocalArtifactBlobStore::new(temp.path()));
        let service = ArtifactService::new_with_quota(store.clone(), blob_store, quota_policy);

        TestHarness {
            service,
            store,
            _temp: temp,
        }
    }

    fn target(thread_id: &str, turn_id: &str) -> ArtifactBindingTarget {
        ArtifactBindingTarget {
            thread_id: Some(thread_id.to_owned()),
            turn_id: Some(turn_id.to_owned()),
            message_id: Some(format!("msg_{turn_id}")),
            turn_item_id: None,
            tool_call_id: None,
            task_id: None,
            task_run_id: None,
            binding_kind: ArtifactBindingKind::AgentOutput,
            direction: ArtifactBindingDirection::Output,
            role: None,
            item_index: Some(0),
        }
    }

    fn ingest_request(
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        bytes: &[u8],
        display_name: &str,
    ) -> IngestArtifactBytesRequest {
        IngestArtifactBytesRequest {
            workspace_id: workspace_id.to_owned(),
            primary_thread_id: Some(thread_id.to_owned()),
            bytes: bytes.to_vec(),
            display_name: display_name.to_owned(),
            kind: ArtifactKind::Text,
            mime_type: Some("text/plain".to_owned()),
            created_by_kind: ArtifactCreatedByKind::Agent,
            created_by_actor_id: Some("agent_a".to_owned()),
            binding: Some(target(thread_id, turn_id)),
            metadata: BTreeMap::new(),
        }
    }

    fn local_source_request(
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        path: std::path::PathBuf,
        policy: ArtifactLocalPathPolicy,
    ) -> IngestArtifactSourceRequest {
        IngestArtifactSourceRequest {
            workspace_id: workspace_id.to_owned(),
            primary_thread_id: Some(thread_id.to_owned()),
            source: ArtifactSource::LocalPath(path),
            display_name: None,
            kind: None,
            mime_type: None,
            created_by_kind: ArtifactCreatedByKind::User,
            created_by_actor_id: Some("user_a".to_owned()),
            binding: Some(target(thread_id, turn_id)),
            metadata: BTreeMap::new(),
            local_path_policy: Some(policy),
        }
    }

    #[tokio::test]
    async fn ingest_bytes_creates_blob_artifact_version_and_binding() {
        let harness = setup().await;

        let summary = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello",
                "hello.txt",
            ))
            .await
            .expect("ingest bytes");

        assert_eq!(summary.workspace_id, "ws_a");
        assert_eq!(summary.primary_thread_id.as_deref(), Some("thr_a"));
        assert_eq!(summary.artifact.display_name, "hello.txt");
        assert_eq!(summary.artifact.size_bytes, Some(5));
        assert_eq!(summary.bindings.len(), 1);
        assert_eq!(summary.bindings[0].thread_id.as_deref(), Some("thr_a"));

        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 1);
        assert_eq!(count_artifacts(harness.store.as_ref(), "ws_a").await, 1);
        assert_eq!(count_versions(harness.store.as_ref(), "ws_a").await, 1);
        assert_eq!(count_bindings(harness.store.as_ref(), "ws_a").await, 1);
    }

    #[tokio::test]
    async fn projection_created_for_small_text_artifact() {
        let harness = setup().await;
        let summary = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello projection",
                "projection.txt",
            ))
            .await
            .expect("ingest bytes");
        assert!(summary.artifact.preview.is_some());

        let projections = harness
            .service
            .list_projections(
                "ws_a",
                summary.artifact.artifact_id.as_str(),
                summary.artifact.version_id.as_deref(),
            )
            .await
            .expect("list projections");

        assert_eq!(projections.len(), 1);
        assert_eq!(projections[0].status, ArtifactProjectionStatus::Ready);
        assert_eq!(
            projections[0].text_content.as_deref(),
            Some("hello projection")
        );
    }

    #[tokio::test]
    async fn projection_failure_does_not_fail_artifact_ingest() {
        let harness = setup().await;
        let summary = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                &[0xff],
                "invalid.txt",
            ))
            .await
            .expect("ingest should survive projection failure");

        let projections = harness
            .service
            .list_projections(
                "ws_a",
                summary.artifact.artifact_id.as_str(),
                summary.artifact.version_id.as_deref(),
            )
            .await
            .expect("list projections");

        assert_eq!(projections.len(), 1);
        assert_eq!(projections[0].status, ArtifactProjectionStatus::Failed);
        assert_eq!(projections[0].text_content, None);
    }

    #[tokio::test]
    async fn projection_regeneration_replaces_existing_projection() {
        let harness = setup().await;
        let summary = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"first",
                "projection.txt",
            ))
            .await
            .expect("ingest bytes");
        let artifact_id = summary.artifact.artifact_id.as_str();
        let version_id = summary.artifact.version_id.as_deref().expect("version id");

        crate::projection::create_inline_projections(
            harness.store.as_ref(),
            "ws_a",
            artifact_id,
            version_id,
            ArtifactKind::Text,
            Some("text/plain"),
            b"second",
        )
        .await
        .expect("regenerate projection");
        let projections = harness
            .service
            .list_projections("ws_a", artifact_id, Some(version_id))
            .await
            .expect("list projections");

        assert_eq!(projections.len(), 1);
        assert_eq!(projections[0].text_content.as_deref(), Some("second"));
    }

    #[tokio::test]
    async fn thumbnail_projection_is_created_for_image_artifact() {
        let harness = setup().await;
        let mut request = ingest_request("ws_a", "thr_a", "turn_a", b"not-a-real-png", "image.png");
        request.kind = ArtifactKind::Image;
        request.mime_type = Some("image/png".to_owned());

        let summary = harness
            .service
            .ingest_bytes(request)
            .await
            .expect("ingest image");
        let projections = harness
            .service
            .list_projections(
                "ws_a",
                summary.artifact.artifact_id.as_str(),
                summary.artifact.version_id.as_deref(),
            )
            .await
            .expect("list projections");

        assert_eq!(projections.len(), 1);
        assert_eq!(
            projections[0].projection_kind,
            ArtifactProjectionKind::Thumbnail
        );
        assert_eq!(projections[0].status, ArtifactProjectionStatus::Pending);
    }

    #[tokio::test]
    async fn quota_rejects_workspace_over_limit_per_workspace() {
        let harness = setup_with_quota(ArtifactQuotaPolicy {
            max_workspace_bytes: 8,
            max_file_bytes: 8,
            max_files_per_workspace: 10,
            warn_at_percent: 80,
        })
        .await;
        harness
            .service
            .ingest_bytes(ingest_request("ws_a", "thr_a", "turn_a", b"12345", "a.txt"))
            .await
            .expect("first ingest");

        let error = harness
            .service
            .ingest_bytes(ingest_request("ws_a", "thr_a", "turn_b", b"1234", "b.txt"))
            .await
            .expect_err("workspace quota should reject");

        assert!(matches!(error, ArtifactError::QuotaExceeded { .. }));
        harness
            .service
            .ingest_bytes(ingest_request("ws_b", "thr_b", "turn_b", b"1234", "b.txt"))
            .await
            .expect("other workspace has separate quota");
    }

    #[tokio::test]
    async fn quota_rejects_temp_file_over_limit() {
        let harness = setup_with_quota(ArtifactQuotaPolicy {
            max_workspace_bytes: 64,
            max_file_bytes: 3,
            max_files_per_workspace: 10,
            warn_at_percent: 80,
        })
        .await;
        let path = harness._temp.path().join("too-large.txt");
        tokio::fs::write(&path, b"1234")
            .await
            .expect("write temp input");

        let error = harness
            .service
            .ingest_temp_file(IngestArtifactTempFileRequest {
                workspace_id: "ws_a".to_owned(),
                primary_thread_id: Some("thr_a".to_owned()),
                temp_path: path,
                display_name: "too-large.txt".to_owned(),
                kind: ArtifactKind::Text,
                mime_type: Some("text/plain".to_owned()),
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: Some("user_a".to_owned()),
                binding: Some(target("thr_a", "turn_a")),
                metadata: BTreeMap::new(),
            })
            .await
            .expect_err("temp file quota should reject");

        assert!(matches!(error, ArtifactError::QuotaExceeded { .. }));
    }

    #[tokio::test]
    async fn gc_dry_run_reports_orphan_and_execute_deletes_only_orphan() {
        let harness = setup().await;
        let referenced = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"referenced",
                "referenced.txt",
            ))
            .await
            .expect("referenced ingest");
        harness
            .store
            .upsert_artifact_external_ref(UpsertArtifactExternalRefRequest {
                key: ArtifactExternalRefKey {
                    workspace_id: "ws_a".to_owned(),
                    artifact_id: referenced.artifact.artifact_id.clone(),
                    artifact_version_id: referenced.artifact.version_id.clone(),
                    provider: "openai".to_owned(),
                    model_family: Some("gpt-test".to_owned()),
                    transport_kind: "upload".to_owned(),
                },
                external_id: "file-expired".to_owned(),
                external_uri: None,
                expires_at_unix_ms: Some(500),
                metadata: BTreeMap::new(),
            })
            .await
            .expect("insert expired external ref");
        let orphan_sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        harness
            .store
            .insert_test_artifact_blob(
                NewArtifactBlobRecord {
                    workspace_id: "ws_a".to_owned(),
                    sha256: orphan_sha.to_owned(),
                    size_bytes: 7,
                    mime_type: Some("text/plain".to_owned()),
                    storage_backend: "local".to_owned(),
                    storage_key: format!("sha256/01/23/{orphan_sha}"),
                    metadata: BTreeMap::new(),
                },
                0,
                "orphan_blob".to_owned(),
            )
            .await
            .expect("insert orphan blob");

        let plan = harness
            .service
            .gc_dry_run("ws_a", 200_000_000)
            .await
            .expect("gc dry run");

        assert_eq!(plan.orphan_blobs.len(), 1);
        assert_eq!(plan.expired_external_refs, 1);
        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 2);

        let report = harness
            .service
            .gc_execute("ws_a", 200_000_000)
            .await
            .expect("gc execute");

        assert_eq!(report.deleted_blobs, 1);
        assert_eq!(report.pruned_external_refs, 1);
        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 1);
    }

    #[tokio::test]
    async fn gc_dry_run_keeps_orphan_inside_grace_period() {
        let harness = setup().await;
        let orphan_sha = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        harness
            .store
            .insert_test_artifact_blob(
                NewArtifactBlobRecord {
                    workspace_id: "ws_a".to_owned(),
                    sha256: orphan_sha.to_owned(),
                    size_bytes: 7,
                    mime_type: Some("text/plain".to_owned()),
                    storage_backend: "local".to_owned(),
                    storage_key: format!("sha256/ab/cd/{orphan_sha}"),
                    metadata: BTreeMap::new(),
                },
                0,
                "young_orphan_blob".to_owned(),
            )
            .await
            .expect("insert orphan blob");

        let plan = harness
            .service
            .gc_dry_run("ws_a", 1_000)
            .await
            .expect("gc dry run");

        assert!(plan.orphan_blobs.is_empty());
        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 1);
    }

    #[tokio::test]
    async fn list_by_thread_returns_artifact_summary() {
        let harness = setup().await;
        let created = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello",
                "hello.txt",
            ))
            .await
            .expect("ingest bytes");

        let page = harness
            .service
            .list_thread_artifacts("ws_a", "thr_a", ArtifactListFilter::default())
            .await
            .expect("list artifacts");

        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].artifact.artifact_id,
            created.artifact.artifact_id
        );
        assert_eq!(page.next_cursor, None);
    }

    #[tokio::test]
    async fn list_thread_artifacts_paginates_with_cursor() {
        let harness = setup().await;
        for index in 0..3 {
            harness
                .service
                .ingest_bytes(ingest_request(
                    "ws_a",
                    "thr_a",
                    format!("turn_{index}").as_str(),
                    format!("artifact {index}").as_bytes(),
                    format!("artifact-{index}.txt").as_str(),
                ))
                .await
                .expect("ingest bytes");
        }

        let first_page = harness
            .service
            .list_thread_artifacts(
                "ws_a",
                "thr_a",
                ArtifactListFilter {
                    limit: Some(2),
                    ..ArtifactListFilter::default()
                },
            )
            .await
            .expect("list first page");
        let cursor = first_page.next_cursor.clone().expect("next cursor");
        let second_page = harness
            .service
            .list_thread_artifacts(
                "ws_a",
                "thr_a",
                ArtifactListFilter {
                    limit: Some(2),
                    cursor: Some(cursor),
                    ..ArtifactListFilter::default()
                },
            )
            .await
            .expect("list second page");

        assert_eq!(first_page.items.len(), 2);
        assert_eq!(second_page.items.len(), 1);
        assert_eq!(second_page.next_cursor, None);
        let first_ids = first_page
            .items
            .iter()
            .map(|summary| summary.artifact.artifact_id.as_str())
            .collect::<Vec<_>>();
        assert!(
            !first_ids.contains(&second_page.items[0].artifact.artifact_id.as_str()),
            "second page repeated an artifact from the first page"
        );
    }

    #[tokio::test]
    async fn same_bytes_same_workspace_reuse_artifact_blob() {
        let harness = setup().await;

        harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a", "thr_a", "turn_a", b"same", "one.txt",
            ))
            .await
            .expect("first ingest");
        harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a", "thr_b", "turn_b", b"same", "two.txt",
            ))
            .await
            .expect("second ingest");

        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 1);
        assert_eq!(count_artifacts(harness.store.as_ref(), "ws_a").await, 2);
    }

    #[tokio::test]
    async fn same_bytes_different_workspace_create_separate_blobs() {
        let harness = setup().await;

        harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a", "thr_a", "turn_a", b"same", "one.txt",
            ))
            .await
            .expect("first ingest");
        harness
            .service
            .ingest_bytes(ingest_request(
                "ws_b", "thr_b", "turn_b", b"same", "two.txt",
            ))
            .await
            .expect("second ingest");

        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 1);
        assert_eq!(count_blobs(harness.store.as_ref(), "ws_b").await, 1);
    }

    #[tokio::test]
    async fn get_artifact_with_wrong_workspace_is_not_found() {
        let harness = setup().await;
        let created = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello",
                "hello.txt",
            ))
            .await
            .expect("ingest bytes");

        let error = harness
            .service
            .get_artifact("ws_b", &created.artifact.artifact_id, None)
            .await
            .expect_err("wrong workspace should not resolve artifact");

        assert!(matches!(error, ArtifactError::NotFound { .. }));
    }

    #[tokio::test]
    async fn download_snapshot_resolves_version_blob_and_read_range() {
        let harness = setup().await;
        let created = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello download",
                "download.txt",
            ))
            .await
            .expect("ingest bytes");

        let snapshot = harness
            .service
            .download_snapshot(
                "ws_a",
                created.artifact.artifact_id.as_str(),
                created.artifact.version_id.as_deref(),
            )
            .await
            .expect("download snapshot");

        assert_eq!(snapshot.artifact_id, created.artifact.artifact_id);
        assert_eq!(
            Some(snapshot.artifact_version_id.as_str()),
            created.artifact.version_id.as_deref()
        );
        assert_eq!(snapshot.size_bytes, 14);

        let bytes = harness
            .service
            .read_blob_range(
                "ws_a",
                snapshot.storage_key.as_str(),
                6,
                "download".len() as u64,
            )
            .await
            .expect("read blob range");
        assert_eq!(bytes, b"download");
    }

    #[tokio::test]
    async fn resolve_provider_attachment_materializes_blob_path_with_metadata() {
        let harness = setup().await;
        let created = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello provider",
                "provider.txt",
            ))
            .await
            .expect("ingest bytes");

        let resolved = harness
            .service
            .resolve_provider_attachment(
                "ws_a",
                created.artifact.artifact_id.as_str(),
                created.artifact.version_id.as_deref(),
            )
            .await
            .expect("resolve provider attachment");

        assert_eq!(resolved.content_type, InputContentType::File);
        assert_eq!(resolved.attachment.name.as_deref(), Some("provider.txt"));
        assert_eq!(resolved.attachment.mime_type, "text/plain");
        assert_eq!(resolved.attachment.size_bytes, Some(14));
        assert_eq!(resolved.attachment.sha256, created.artifact.sha256);
        let artifact_context = resolved
            .attachment
            .artifact
            .as_ref()
            .expect("artifact context should be attached");
        assert_eq!(artifact_context.workspace_id, "ws_a");
        assert_eq!(artifact_context.artifact_id, created.artifact.artifact_id);
        assert_eq!(
            artifact_context.artifact_version_id,
            created.artifact.version_id
        );
        let AttachmentDataSource::Path { path } = &resolved.attachment.source else {
            panic!("expected materialized path attachment");
        };
        assert_ne!(path, "provider.txt");
        assert_eq!(
            tokio::fs::read(path).await.expect("read materialized file"),
            b"hello provider"
        );
    }

    #[tokio::test]
    async fn resolve_provider_attachment_rejects_deleted_artifact() {
        let harness = setup().await;
        let created = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello provider",
                "provider.txt",
            ))
            .await
            .expect("ingest bytes");
        harness
            .store
            .update_test_artifact_status(&created.artifact.artifact_id, "deleted")
            .await
            .expect("mark deleted");

        let error = harness
            .service
            .resolve_provider_attachment("ws_a", created.artifact.artifact_id.as_str(), None)
            .await
            .expect_err("deleted artifact should be rejected");

        assert!(matches!(error, ArtifactError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn resolve_provider_attachment_rejects_quarantined_artifact() {
        let harness = setup().await;
        let created = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello provider",
                "provider.txt",
            ))
            .await
            .expect("ingest bytes");
        update_artifact_status(
            harness.store.as_ref(),
            &created.artifact.artifact_id,
            "quarantined",
        )
        .await;

        let error = harness
            .service
            .resolve_provider_attachment("ws_a", created.artifact.artifact_id.as_str(), None)
            .await
            .expect_err("quarantined artifact should be rejected");

        assert!(matches!(error, ArtifactError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn resolve_provider_attachment_missing_artifact_is_not_found() {
        let harness = setup().await;

        let error = harness
            .service
            .resolve_provider_attachment("ws_a", "artifact_missing", None)
            .await
            .expect_err("missing artifact should not resolve");

        assert!(matches!(error, ArtifactError::NotFound { .. }));
    }

    #[tokio::test]
    async fn bind_existing_artifact_adds_binding_without_duplicating_blob() {
        let harness = setup().await;
        let created = harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "thr_a",
                "turn_a",
                b"hello",
                "hello.txt",
            ))
            .await
            .expect("ingest bytes");

        let binding = harness
            .service
            .bind_artifact(BindArtifactRequest {
                workspace_id: "ws_a".to_owned(),
                artifact_id: created.artifact.artifact_id.clone(),
                version_id: None,
                target: target("thr_a", "turn_b"),
                metadata: BTreeMap::new(),
            })
            .await
            .expect("bind artifact");

        assert_eq!(binding.turn_id.as_deref(), Some("turn_b"));
        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 1);
        assert_eq!(count_bindings(harness.store.as_ref(), "ws_a").await, 2);

        let summary = harness
            .service
            .get_artifact("ws_a", &created.artifact.artifact_id, None)
            .await
            .expect("get artifact");
        assert_eq!(summary.bindings.len(), 2);
    }

    #[tokio::test]
    async fn local_path_inside_allowed_root_ingests_successfully() {
        let harness = setup().await;
        let root = harness._temp.path().join("allowed");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let file = root.join("report.txt");
        tokio::fs::write(&file, b"local text")
            .await
            .expect("write local file");

        let summary = harness
            .service
            .ingest_source(local_source_request(
                "ws_a",
                "thr_a",
                "turn_a",
                file,
                ArtifactLocalPathPolicy::new(vec![root]),
            ))
            .await
            .expect("ingest local path");

        assert_eq!(summary.artifact.display_name, "report.txt");
        assert_eq!(summary.artifact.kind, ArtifactKind::Text);
        assert_eq!(summary.artifact.mime_type.as_deref(), Some("text/plain"));
        assert_eq!(summary.bindings.len(), 1);
    }

    #[tokio::test]
    async fn local_path_outside_allowed_root_is_rejected() {
        let harness = setup().await;
        let root = harness._temp.path().join("allowed");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let outside = harness._temp.path().join("outside.txt");
        tokio::fs::write(&outside, b"outside")
            .await
            .expect("write outside file");

        let error = harness
            .service
            .ingest_source(local_source_request(
                "ws_a",
                "thr_a",
                "turn_a",
                outside,
                ArtifactLocalPathPolicy::new(vec![root]),
            ))
            .await
            .expect_err("outside path should be rejected");

        assert!(matches!(error, ArtifactError::LocalPathRejected { .. }));
        assert_eq!(count_artifacts(harness.store.as_ref(), "ws_a").await, 0);
    }

    #[tokio::test]
    async fn local_path_traversal_is_rejected() {
        let harness = setup().await;
        let root = harness._temp.path().join("allowed");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let outside = harness._temp.path().join("outside.txt");
        tokio::fs::write(&outside, b"outside")
            .await
            .expect("write outside file");
        let traversal = root.join("../outside.txt");

        let error = harness
            .service
            .ingest_source(local_source_request(
                "ws_a",
                "thr_a",
                "turn_a",
                traversal,
                ArtifactLocalPathPolicy::new(vec![root]),
            ))
            .await
            .expect_err("traversal path should be rejected");

        assert!(matches!(error, ArtifactError::LocalPathRejected { .. }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_path_symlink_inside_root_pointing_outside_is_rejected() {
        use std::os::unix::fs::symlink;

        let harness = setup().await;
        let root = harness._temp.path().join("allowed");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let outside = harness._temp.path().join("outside.txt");
        tokio::fs::write(&outside, b"outside")
            .await
            .expect("write outside file");
        let link = root.join("linked.txt");
        symlink(&outside, &link).expect("create symlink");

        let error = harness
            .service
            .ingest_source(local_source_request(
                "ws_a",
                "thr_a",
                "turn_a",
                link,
                ArtifactLocalPathPolicy::new(vec![root]),
            ))
            .await
            .expect_err("symlink should be rejected");

        assert!(matches!(error, ArtifactError::LocalPathRejected { .. }));
    }

    #[tokio::test]
    async fn local_path_directory_is_rejected() {
        let harness = setup().await;
        let root = harness._temp.path().join("allowed");
        tokio::fs::create_dir_all(&root).await.expect("create root");

        let error = harness
            .service
            .ingest_source(local_source_request(
                "ws_a",
                "thr_a",
                "turn_a",
                root.clone(),
                ArtifactLocalPathPolicy::new(vec![root]),
            ))
            .await
            .expect_err("directory should be rejected");

        assert!(matches!(error, ArtifactError::LocalPathRejected { .. }));
    }

    #[tokio::test]
    async fn local_path_oversized_file_is_rejected_before_commit() {
        let harness = setup().await;
        let root = harness._temp.path().join("allowed");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let file = root.join("big.bin");
        tokio::fs::write(&file, b"12345")
            .await
            .expect("write local file");
        let mut policy = ArtifactLocalPathPolicy::new(vec![root]);
        policy.max_file_bytes = 4;

        let error = harness
            .service
            .ingest_source(local_source_request(
                "ws_a", "thr_a", "turn_a", file, policy,
            ))
            .await
            .expect_err("oversized path should be rejected");

        assert!(matches!(error, ArtifactError::LocalPathRejected { .. }));
        assert_eq!(count_artifacts(harness.store.as_ref(), "ws_a").await, 0);
        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 0);
    }

    #[tokio::test]
    async fn local_path_successful_ingestion_creates_binding() {
        let harness = setup().await;
        let root = harness._temp.path().join("allowed");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let file = root.join("bound.txt");
        tokio::fs::write(&file, b"bound")
            .await
            .expect("write local file");

        let summary = harness
            .service
            .ingest_source(local_source_request(
                "ws_a",
                "thr_a",
                "turn_a",
                file,
                ArtifactLocalPathPolicy::new(vec![root]),
            ))
            .await
            .expect("ingest local path");

        assert_eq!(summary.bindings.len(), 1);
        assert_eq!(summary.bindings[0].turn_id.as_deref(), Some("turn_a"));
    }

    #[tokio::test]
    async fn local_path_same_file_in_two_workspaces_stays_workspace_scoped() {
        let harness = setup().await;
        let root = harness._temp.path().join("allowed");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let file = root.join("shared.txt");
        tokio::fs::write(&file, b"shared")
            .await
            .expect("write local file");

        harness
            .service
            .ingest_source(local_source_request(
                "ws_a",
                "thr_a",
                "turn_a",
                file.clone(),
                ArtifactLocalPathPolicy::new(vec![root.clone()]),
            ))
            .await
            .expect("ingest workspace A");
        harness
            .service
            .ingest_source(local_source_request(
                "ws_b",
                "thr_b",
                "turn_b",
                file,
                ArtifactLocalPathPolicy::new(vec![root]),
            ))
            .await
            .expect("ingest workspace B");

        assert_eq!(count_blobs(harness.store.as_ref(), "ws_a").await, 1);
        assert_eq!(count_blobs(harness.store.as_ref(), "ws_b").await, 1);
    }

    async fn count_blobs(store: &CrudStore, workspace_id: &str) -> u64 {
        store
            .count_artifact_blobs_by_workspace(workspace_id)
            .await
            .expect("count blobs")
    }

    async fn count_artifacts(store: &CrudStore, workspace_id: &str) -> u64 {
        store
            .count_artifacts_by_workspace(workspace_id)
            .await
            .expect("count artifacts")
    }

    async fn count_versions(store: &CrudStore, workspace_id: &str) -> u64 {
        store
            .count_artifact_versions_by_workspace(workspace_id)
            .await
            .expect("count versions")
    }

    async fn count_bindings(store: &CrudStore, workspace_id: &str) -> u64 {
        store
            .count_artifact_bindings_by_workspace(workspace_id)
            .await
            .expect("count bindings")
    }

    async fn update_artifact_status(store: &CrudStore, artifact_id: &str, status: &str) {
        store
            .update_test_artifact_status(artifact_id, status)
            .await
            .expect("update artifact status");
    }
}
