use std::collections::{BTreeMap, HashSet};
use std::fmt;

use chrono::{TimeZone, Utc};
use pioneer_entity::{
    artifact, artifact_binding, artifact_blob, artifact_external_ref, artifact_projection,
    artifact_version,
};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactBindingSummary, ArtifactCreatedByKind,
    ArtifactKind, ArtifactPreviewRef, ArtifactProjectionKind, ArtifactProjectionStatus,
    ArtifactRef, ArtifactRole, ArtifactStatus, ArtifactSummary, generate_id,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DbErr, EntityTrait, IntoActiveModel,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    entity::prelude::DateTimeWithTimeZone,
};

const DEFAULT_LIST_LIMIT: u64 = 50;
const MAX_LIST_LIMIT: u64 = 200;

pub type ArtifactCrudResult<T> = Result<T, ArtifactCrudError>;

#[derive(Debug)]
pub enum ArtifactCrudError {
    InvalidRequest {
        message: String,
    },
    NotFound {
        message: String,
    },
    Database {
        message: String,
        source: DbErr,
    },
    Json {
        message: String,
        source: serde_json::Error,
    },
}

impl fmt::Display for ArtifactCrudError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArtifactCrudError::InvalidRequest { message } => {
                write!(f, "invalid artifact request: {message}")
            }
            ArtifactCrudError::NotFound { message } => write!(f, "artifact not found: {message}"),
            ArtifactCrudError::Database { message, source } => write!(f, "{message}: {source}"),
            ArtifactCrudError::Json { message, source } => write!(f, "{message}: {source}"),
        }
    }
}

impl std::error::Error for ArtifactCrudError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ArtifactCrudError::Database { source, .. } => Some(source),
            ArtifactCrudError::Json { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ArtifactRepository;

impl ArtifactRepository {
    pub(crate) fn new() -> Self {
        Self
    }

    pub async fn find_blob_by_workspace_sha<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        sha256: &str,
        size_bytes: u64,
        storage_backend: &str,
    ) -> ArtifactCrudResult<Option<ArtifactBlobRecord>> {
        let size_bytes = to_i64_size(size_bytes)?;
        artifact_blob::Entity::find()
            .filter(artifact_blob::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(artifact_blob::Column::Sha256.eq(sha256.to_owned()))
            .filter(artifact_blob::Column::SizeBytes.eq(size_bytes))
            .filter(artifact_blob::Column::StorageBackend.eq(storage_backend.to_owned()))
            .one(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact blob".to_owned(),
                source,
            })
            .map(|row| row.map(blob_record_from_model))
    }

    pub async fn create_blob<C: ConnectionTrait>(
        &self,
        db: &C,
        record: NewArtifactBlobRecord,
    ) -> ArtifactCrudResult<ArtifactBlobRecord> {
        let now = now();
        let id = generate_id(21);
        let model = artifact_blob::ActiveModel {
            id: Set(id),
            workspace_id: Set(record.workspace_id),
            sha256: Set(record.sha256),
            size_bytes: Set(to_i64_size(record.size_bytes)?),
            mime_type: Set(record.mime_type),
            storage_backend: Set(record.storage_backend),
            storage_key: Set(record.storage_key),
            encryption_key_id: Set(None),
            created_at: Set(now),
            last_verified_at: Set(None),
            metadata_json: Set(metadata_to_db(&record.metadata)?),
        }
        .insert(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to insert artifact blob".to_owned(),
            source,
        })?;
        Ok(blob_record_from_model(model))
    }

    pub async fn find_or_create_blob<C: ConnectionTrait>(
        &self,
        db: &C,
        record: NewArtifactBlobRecord,
    ) -> ArtifactCrudResult<ArtifactBlobRecord> {
        if let Some(existing) = self
            .find_blob_by_workspace_sha(
                db,
                &record.workspace_id,
                &record.sha256,
                record.size_bytes,
                &record.storage_backend,
            )
            .await?
        {
            return Ok(existing);
        }

        match self.create_blob(db, record.clone()).await {
            Ok(model) => Ok(model),
            Err(ArtifactCrudError::Database { .. }) => self
                .find_blob_by_workspace_sha(
                    db,
                    &record.workspace_id,
                    &record.sha256,
                    record.size_bytes,
                    &record.storage_backend,
                )
                .await?
                .ok_or_else(|| ArtifactCrudError::NotFound {
                    message: "artifact blob insert failed and no existing row was found".to_owned(),
                }),
            Err(error) => Err(error),
        }
    }

    pub async fn create_artifact<C: ConnectionTrait>(
        &self,
        db: &C,
        request: &IngestArtifactMetadataRecord,
    ) -> ArtifactCrudResult<ArtifactRecord> {
        let now = now();
        artifact::ActiveModel {
            id: Set(generate_id(21)),
            workspace_id: Set(request.workspace_id.clone()),
            primary_thread_id: Set(request.primary_thread_id.clone()),
            current_version_id: Set(None),
            display_name: Set(request.display_name.clone()),
            kind: Set(kind_to_db(request.kind)),
            mime_type: Set(request.mime_type.clone()),
            status: Set(status_to_db(ArtifactStatus::Ready)),
            created_by_kind: Set(created_by_kind_to_db(request.created_by_kind)),
            created_by_actor_id: Set(request.created_by_actor_id.clone()),
            created_at: Set(now),
            updated_at: Set(now),
            deleted_at: Set(None),
            metadata_json: Set(metadata_to_db(&request.metadata)?),
        }
        .insert(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to insert artifact".to_owned(),
            source,
        })
        .map(artifact_record_from_model)
    }

    pub async fn create_version<C: ConnectionTrait>(
        &self,
        db: &C,
        artifact: &ArtifactRecord,
        blob: &ArtifactBlobRecord,
        binding: Option<&ArtifactBindingTargetRecord>,
        metadata: &BTreeMap<String, serde_json::Value>,
    ) -> ArtifactCrudResult<ArtifactVersionRecord> {
        let version = artifact_version::ActiveModel {
            id: Set(generate_id(21)),
            workspace_id: Set(artifact.workspace_id.clone()),
            artifact_id: Set(artifact.id.clone()),
            version_number: Set(1),
            blob_id: Set(blob.id.clone()),
            source_uri: Set(None),
            source_path_redacted: Set(None),
            created_by_turn_id: Set(binding.and_then(|target| target.turn_id.clone())),
            created_by_message_id: Set(binding.and_then(|target| target.message_id.clone())),
            created_by_turn_item_id: Set(binding.and_then(|target| target.turn_item_id.clone())),
            created_by_tool_call_id: Set(binding.and_then(|target| target.tool_call_id.clone())),
            created_by_task_id: Set(binding.and_then(|target| target.task_id.clone())),
            created_by_task_run_id: Set(binding.and_then(|target| target.task_run_id.clone())),
            created_at: Set(now()),
            metadata_json: Set(metadata_to_db(metadata)?),
        }
        .insert(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to insert artifact version".to_owned(),
            source,
        })?;
        Ok(version_record_from_model(version))
    }

    pub async fn update_current_version<C: ConnectionTrait>(
        &self,
        db: &C,
        artifact: ArtifactRecord,
        version_id: &str,
    ) -> ArtifactCrudResult<ArtifactRecord> {
        let Some(model) = artifact::Entity::find()
            .filter(artifact::Column::WorkspaceId.eq(artifact.workspace_id.clone()))
            .filter(artifact::Column::Id.eq(artifact.id.clone()))
            .one(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact for current version update".to_owned(),
                source,
            })?
        else {
            return Err(ArtifactCrudError::NotFound {
                message: format!(
                    "workspace={} artifact={}",
                    artifact.workspace_id, artifact.id
                ),
            });
        };
        let mut active = model.into_active_model();
        active.current_version_id = Set(Some(version_id.to_owned()));
        active.updated_at = Set(now());
        active
            .update(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to update artifact current version".to_owned(),
                source,
            })
            .map(artifact_record_from_model)
    }

    pub async fn create_binding<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
        target: &ArtifactBindingTargetRecord,
        metadata: &BTreeMap<String, serde_json::Value>,
    ) -> ArtifactCrudResult<ArtifactBindingSummary> {
        let model = artifact_binding::ActiveModel {
            id: Set(generate_id(21)),
            artifact_id: Set(artifact_id.to_owned()),
            artifact_version_id: Set(version_id.map(ToOwned::to_owned)),
            workspace_id: Set(workspace_id.to_owned()),
            thread_id: Set(target.thread_id.clone()),
            turn_id: Set(target.turn_id.clone()),
            message_id: Set(target.message_id.clone()),
            turn_item_id: Set(target.turn_item_id.clone()),
            tool_call_id: Set(target.tool_call_id.clone()),
            task_id: Set(target.task_id.clone()),
            task_run_id: Set(target.task_run_id.clone()),
            binding_kind: Set(binding_kind_to_db(target.binding_kind)),
            direction: Set(binding_direction_to_db(target.direction)),
            item_index: Set(target.item_index),
            role: Set(target.role.map(role_to_db)),
            created_at: Set(now()),
            metadata_json: Set(metadata_to_db(metadata)?),
        }
        .insert(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to insert artifact binding".to_owned(),
            source,
        })?;
        binding_summary_from_model(&model)
    }

    pub async fn get_artifact_summary<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> ArtifactCrudResult<ArtifactSummary> {
        let artifact = self
            .find_artifact(db, workspace_id, artifact_id)
            .await?
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!("workspace={workspace_id} artifact={artifact_id}"),
            })?;

        let resolved_version_id = version_id
            .map(ToOwned::to_owned)
            .or_else(|| artifact.current_version_id.clone())
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!("artifact `{artifact_id}` has no current version"),
            })?;

        let version = artifact_version::Entity::find()
            .filter(artifact_version::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(artifact_version::Column::ArtifactId.eq(artifact_id.to_owned()))
            .filter(artifact_version::Column::Id.eq(resolved_version_id.clone()))
            .one(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact version".to_owned(),
                source,
            })?
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!(
                    "workspace={workspace_id} artifact={artifact_id} version={resolved_version_id}"
                ),
            })?;

        let blob = artifact_blob::Entity::find()
            .filter(artifact_blob::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(artifact_blob::Column::Id.eq(version.blob_id.clone()))
            .one(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact blob".to_owned(),
                source,
            })?
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!("workspace={workspace_id} blob={}", version.blob_id),
            })?;

        let bindings = artifact_binding::Entity::find()
            .filter(artifact_binding::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(artifact_binding::Column::ArtifactId.eq(artifact_id.to_owned()))
            .order_by_desc(artifact_binding::Column::CreatedAt)
            .all(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact bindings".to_owned(),
                source,
            })?;

        let preview = projection_preview(
            db,
            workspace_id,
            artifact_id,
            &version.id,
            blob.mime_type.clone(),
            u64::try_from(blob.size_bytes).ok(),
        )
        .await?;

        summary_from_models(artifact, version, blob, bindings, preview)
    }

    pub async fn get_artifact_version_blob<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> ArtifactCrudResult<ArtifactVersionBlobRecord> {
        let artifact = self
            .find_artifact(db, workspace_id, artifact_id)
            .await?
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!("workspace={workspace_id} artifact={artifact_id}"),
            })?;

        let resolved_version_id = version_id
            .map(ToOwned::to_owned)
            .or_else(|| artifact.current_version_id.clone())
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!("artifact `{artifact_id}` has no current version"),
            })?;

        let version = artifact_version::Entity::find()
            .filter(artifact_version::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(artifact_version::Column::ArtifactId.eq(artifact_id.to_owned()))
            .filter(artifact_version::Column::Id.eq(resolved_version_id.clone()))
            .one(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact version".to_owned(),
                source,
            })?
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!(
                    "workspace={workspace_id} artifact={artifact_id} version={resolved_version_id}"
                ),
            })?;

        let blob = artifact_blob::Entity::find()
            .filter(artifact_blob::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(artifact_blob::Column::Id.eq(version.blob_id.clone()))
            .one(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact blob".to_owned(),
                source,
            })?
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!("workspace={workspace_id} blob={}", version.blob_id),
            })?;

        Ok(ArtifactVersionBlobRecord {
            artifact_id: artifact.id,
            workspace_id: artifact.workspace_id,
            artifact_version_id: version.id,
            blob_id: blob.id,
            storage_key: blob.storage_key,
            size_bytes: blob.size_bytes.max(0) as u64,
            sha256: blob.sha256,
        })
    }

    pub async fn list_thread_artifacts<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        thread_id: &str,
        filter: ArtifactListFilterRecord,
    ) -> ArtifactCrudResult<ArtifactListPageRecord> {
        self.list_artifacts(
            db,
            workspace_id,
            ArtifactListFilterRecord {
                thread_id: Some(thread_id.to_owned()),
                ..filter
            },
        )
        .await
    }

    pub async fn list_artifacts<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        filter: ArtifactListFilterRecord,
    ) -> ArtifactCrudResult<ArtifactListPageRecord> {
        let limit = filter
            .limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .min(MAX_LIST_LIMIT);
        let offset = decode_list_cursor(filter.cursor.as_deref())?;
        let query_limit = limit.saturating_add(1);
        let has_binding_filter = filter.thread_id.is_some()
            || filter.turn_id.is_some()
            || filter.message_id.is_some()
            || filter.task_id.is_some()
            || filter.task_run_id.is_some();

        let artifact_ids = if has_binding_filter {
            let mut query = artifact_binding::Entity::find()
                .filter(artifact_binding::Column::WorkspaceId.eq(workspace_id.to_owned()));

            if let Some(thread_id) = &filter.thread_id {
                query = query.filter(artifact_binding::Column::ThreadId.eq(thread_id.clone()));
            }
            if let Some(turn_id) = &filter.turn_id {
                query = query.filter(artifact_binding::Column::TurnId.eq(turn_id.clone()));
            }
            if let Some(message_id) = &filter.message_id {
                query = query.filter(artifact_binding::Column::MessageId.eq(message_id.clone()));
            }
            if let Some(task_id) = &filter.task_id {
                query = query.filter(artifact_binding::Column::TaskId.eq(task_id.clone()));
            }
            if let Some(task_run_id) = &filter.task_run_id {
                query = query.filter(artifact_binding::Column::TaskRunId.eq(task_run_id.clone()));
            }

            let bindings = query
                .order_by_desc(artifact_binding::Column::CreatedAt)
                .order_by_desc(artifact_binding::Column::Id)
                .limit(query_limit)
                .offset(offset)
                .all(db)
                .await
                .map_err(|source| ArtifactCrudError::Database {
                    message: "failed to list artifact bindings".to_owned(),
                    source,
                })?;

            let mut artifact_ids = Vec::<String>::new();
            for binding in bindings {
                if !artifact_ids.iter().any(|id| id == &binding.artifact_id) {
                    artifact_ids.push(binding.artifact_id);
                }
            }
            artifact_ids
        } else {
            artifact::Entity::find()
                .filter(artifact::Column::WorkspaceId.eq(workspace_id.to_owned()))
                .order_by_desc(artifact::Column::UpdatedAt)
                .order_by_desc(artifact::Column::Id)
                .limit(query_limit)
                .offset(offset)
                .all(db)
                .await
                .map_err(|source| ArtifactCrudError::Database {
                    message: "failed to list workspace artifacts".to_owned(),
                    source,
                })?
                .into_iter()
                .map(|artifact| artifact.id)
                .collect()
        };

        let mut summaries = Vec::new();
        for artifact_id in artifact_ids {
            let summary = self
                .get_artifact_summary(db, workspace_id, &artifact_id, None)
                .await?;
            if !filter.include_deleted && summary.artifact.status == ArtifactStatus::Deleted {
                continue;
            }
            if !filter.kinds.is_empty() && !filter.kinds.contains(&summary.artifact.kind) {
                continue;
            }
            summaries.push(summary);
        }

        let has_next = summaries.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        if has_next {
            summaries.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        }
        Ok(ArtifactListPageRecord {
            items: summaries,
            next_cursor: has_next.then(|| offset.saturating_add(limit).to_string()),
        })
    }

    pub async fn update_artifact_status<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        artifact_id: &str,
        status: ArtifactStatus,
        deleted_at: Option<DateTimeWithTimeZone>,
    ) -> ArtifactCrudResult<ArtifactRecord> {
        let artifact = self
            .find_artifact(db, workspace_id, artifact_id)
            .await?
            .ok_or_else(|| ArtifactCrudError::NotFound {
                message: format!("workspace={workspace_id} artifact={artifact_id}"),
            })?;
        let mut active = artifact.into_active_model();
        active.status = Set(status_to_db(status));
        active.updated_at = Set(now());
        active.deleted_at = Set(deleted_at);
        active
            .update(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to update artifact status".to_owned(),
                source,
            })
            .map(artifact_record_from_model)
    }

    async fn find_artifact<C: ConnectionTrait>(
        &self,
        db: &C,
        workspace_id: &str,
        artifact_id: &str,
    ) -> ArtifactCrudResult<Option<artifact::Model>> {
        artifact::Entity::find()
            .filter(artifact::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(artifact::Column::Id.eq(artifact_id.to_owned()))
            .one(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact".to_owned(),
                source,
            })
    }
}

#[derive(Debug, Clone)]
pub struct NewArtifactBlobRecord {
    pub workspace_id: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub mime_type: Option<String>,
    pub storage_backend: String,
    pub storage_key: String,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestArtifactMetadataRecord {
    pub workspace_id: String,
    pub primary_thread_id: Option<String>,
    pub display_name: String,
    pub kind: ArtifactKind,
    pub mime_type: Option<String>,
    pub created_by_kind: ArtifactCreatedByKind,
    pub created_by_actor_id: Option<String>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBindingTargetRecord {
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub message_id: Option<String>,
    pub turn_item_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub task_id: Option<String>,
    pub task_run_id: Option<String>,
    pub binding_kind: ArtifactBindingKind,
    pub direction: ArtifactBindingDirection,
    pub role: Option<ArtifactRole>,
    pub item_index: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArtifactListFilterRecord {
    pub limit: Option<u64>,
    pub cursor: Option<String>,
    pub include_deleted: bool,
    pub kinds: Vec<ArtifactKind>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub message_id: Option<String>,
    pub task_id: Option<String>,
    pub task_run_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactListPageRecord {
    pub items: Vec<ArtifactSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBlobRecord {
    pub id: String,
    pub workspace_id: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub mime_type: Option<String>,
    pub storage_backend: String,
    pub storage_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub id: String,
    pub workspace_id: String,
    pub current_version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactVersionRecord {
    pub id: String,
    pub workspace_id: String,
    pub artifact_id: String,
    pub blob_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestedArtifactRecord {
    pub artifact: ArtifactRecord,
    pub version: ArtifactVersionRecord,
    pub blob: ArtifactBlobRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactVersionBlobRecord {
    pub artifact_id: String,
    pub workspace_id: String,
    pub artifact_version_id: String,
    pub blob_id: String,
    pub storage_key: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactProjectionRecord {
    pub id: String,
    pub workspace_id: String,
    pub artifact_id: String,
    pub artifact_version_id: String,
    pub projection_kind: ArtifactProjectionKind,
    pub status: ArtifactProjectionStatus,
    pub text_content: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactExternalRefKey {
    pub workspace_id: String,
    pub artifact_id: String,
    pub artifact_version_id: Option<String>,
    pub provider: String,
    pub model_family: Option<String>,
    pub transport_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertArtifactExternalRefRequest {
    pub key: ArtifactExternalRefKey,
    pub external_id: String,
    pub external_uri: Option<String>,
    pub expires_at_unix_ms: Option<i64>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactExternalRefRecord {
    pub id: String,
    pub workspace_id: String,
    pub artifact_id: String,
    pub artifact_version_id: Option<String>,
    pub provider: String,
    pub model_family: Option<String>,
    pub transport_kind: String,
    pub external_id: String,
    pub external_uri: Option<String>,
    pub expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactWorkspaceUsageRecord {
    pub workspace_id: String,
    pub bytes: u64,
    pub files: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactGcPlanRecord {
    pub workspace_id: String,
    pub orphan_blobs: Vec<ArtifactGcBlobCandidateRecord>,
    pub stale_projections: Vec<String>,
    pub expired_external_refs: u64,
    pub estimated_bytes_reclaimable: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactGcBlobCandidateRecord {
    pub blob_id: String,
    pub storage_key: String,
    pub size_bytes: u64,
}

pub async fn replace_projection<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    artifact_id: &str,
    artifact_version_id: &str,
    projection_kind: ArtifactProjectionKind,
    status: ArtifactProjectionStatus,
    text_content: Option<String>,
    metadata: BTreeMap<String, serde_json::Value>,
) -> ArtifactCrudResult<ArtifactProjectionRecord> {
    let now = now();
    artifact_projection::Entity::delete_many()
        .filter(artifact_projection::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact_projection::Column::ArtifactId.eq(artifact_id.to_owned()))
        .filter(artifact_projection::Column::ArtifactVersionId.eq(artifact_version_id.to_owned()))
        .filter(
            artifact_projection::Column::ProjectionKind
                .eq(projection_kind_to_db(projection_kind).to_owned()),
        )
        .exec(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to replace artifact projection".to_owned(),
            source,
        })?;
    let model = artifact_projection::ActiveModel {
        id: Set(generate_id(21)),
        workspace_id: Set(workspace_id.to_owned()),
        artifact_id: Set(artifact_id.to_owned()),
        artifact_version_id: Set(artifact_version_id.to_owned()),
        projection_kind: Set(projection_kind_to_db(projection_kind).to_owned()),
        status: Set(projection_status_to_db(status).to_owned()),
        text_content: Set(text_content),
        blob_id: Set(None),
        metadata_json: Set(metadata_to_db(&metadata)?),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(db)
    .await
    .map_err(|source| ArtifactCrudError::Database {
        message: "failed to insert artifact projection".to_owned(),
        source,
    })?;
    projection_record_from_model(model)
}

pub async fn list_projections<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    artifact_id: &str,
    artifact_version_id: Option<&str>,
) -> ArtifactCrudResult<Vec<ArtifactProjectionRecord>> {
    let mut query = artifact_projection::Entity::find()
        .filter(artifact_projection::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact_projection::Column::ArtifactId.eq(artifact_id.to_owned()));
    if let Some(version_id) = artifact_version_id {
        query =
            query.filter(artifact_projection::Column::ArtifactVersionId.eq(version_id.to_owned()));
    }
    let rows = query
        .all(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to list artifact projections".to_owned(),
            source,
        })?;
    rows.into_iter().map(projection_record_from_model).collect()
}

pub async fn find_active_external_ref<C: ConnectionTrait>(
    db: &C,
    key: &ArtifactExternalRefKey,
    now_unix_ms: i64,
) -> ArtifactCrudResult<Option<ArtifactExternalRefRecord>> {
    let rows = query_external_refs_by_key(db, key)
        .await?
        .into_iter()
        .filter(|row| {
            row.expires_at_unix_ms
                .map(|expires_at| expires_at > now_unix_ms)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    Ok(rows.into_iter().next())
}

pub async fn upsert_external_ref<C: ConnectionTrait>(
    db: &C,
    request: UpsertArtifactExternalRefRequest,
) -> ArtifactCrudResult<ArtifactExternalRefRecord> {
    validate_external_ref_key(&request.key)?;
    validate_non_empty("external_id", &request.external_id)?;

    let expires_at = request.expires_at_unix_ms.map(unix_ms_to_datetime);
    let metadata_json = metadata_to_db(&request.metadata)?;

    if let Some(existing) = query_external_ref_models_by_key(db, &request.key)
        .await?
        .into_iter()
        .next()
    {
        let mut active = existing.into_active_model();
        active.external_id = Set(request.external_id);
        active.external_uri = Set(request.external_uri);
        active.expires_at = Set(expires_at);
        active.metadata_json = Set(metadata_json);
        return active
            .update(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to update artifact external ref".to_owned(),
                source,
            })
            .map(external_ref_record_from_model);
    }

    artifact_external_ref::ActiveModel {
        id: Set(generate_id(21)),
        workspace_id: Set(request.key.workspace_id),
        artifact_id: Set(request.key.artifact_id),
        artifact_version_id: Set(request.key.artifact_version_id),
        provider: Set(request.key.provider),
        model_family: Set(request.key.model_family),
        transport_kind: Set(request.key.transport_kind),
        external_id: Set(request.external_id),
        external_uri: Set(request.external_uri),
        expires_at: Set(expires_at),
        created_at: Set(now()),
        metadata_json: Set(metadata_json),
    }
    .insert(db)
    .await
    .map_err(|source| ArtifactCrudError::Database {
        message: "failed to insert artifact external ref".to_owned(),
        source,
    })
    .map(external_ref_record_from_model)
}

pub async fn prune_expired_external_refs<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now_unix_ms: i64,
) -> ArtifactCrudResult<u64> {
    validate_non_empty("workspace_id", workspace_id)?;
    let now = unix_ms_to_datetime(now_unix_ms);
    let result = artifact_external_ref::Entity::delete_many()
        .filter(artifact_external_ref::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact_external_ref::Column::ExpiresAt.lt(now))
        .exec(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to prune expired artifact external refs".to_owned(),
            source,
        })?;
    Ok(result.rows_affected)
}

pub async fn workspace_usage<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> ArtifactCrudResult<ArtifactWorkspaceUsageRecord> {
    let blobs = artifact_blob::Entity::find()
        .filter(artifact_blob::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .all(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to query artifact workspace usage".to_owned(),
            source,
        })?;
    let active_artifact_ids = artifact::Entity::find()
        .filter(artifact::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact::Column::Status.ne("deleted"))
        .all(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to query active artifacts for workspace usage".to_owned(),
            source,
        })?
        .into_iter()
        .map(|artifact| artifact.id)
        .collect::<HashSet<_>>();

    let referenced_blob_ids = artifact_version::Entity::find()
        .filter(artifact_version::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .all(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to query artifact versions for workspace usage".to_owned(),
            source,
        })?
        .into_iter()
        .filter(|version| active_artifact_ids.contains(&version.artifact_id))
        .map(|version| version.blob_id)
        .collect::<HashSet<_>>();

    let active_blobs = blobs
        .iter()
        .filter(|blob| referenced_blob_ids.contains(&blob.id))
        .collect::<Vec<_>>();
    let bytes = active_blobs
        .iter()
        .map(|row| row.size_bytes.max(0) as u64)
        .sum::<u64>();
    let files = active_blobs.len() as u64;
    Ok(ArtifactWorkspaceUsageRecord {
        workspace_id: workspace_id.to_owned(),
        bytes,
        files,
    })
}

pub async fn plan_gc_with_grace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now_unix_ms: i64,
    grace_secs: u64,
) -> ArtifactCrudResult<ArtifactGcPlanRecord> {
    let cutoff_ms = gc_cutoff_ms(now_unix_ms, grace_secs);
    let blobs = artifact_blob::Entity::find()
        .filter(artifact_blob::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .all(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to query artifact blobs for gc".to_owned(),
            source,
        })?;
    let mut orphan_blobs = Vec::new();
    for blob in blobs {
        let refs = artifact_version::Entity::find()
            .filter(artifact_version::Column::WorkspaceId.eq(workspace_id.to_owned()))
            .filter(artifact_version::Column::BlobId.eq(blob.id.clone()))
            .all(db)
            .await
            .map_err(|source| ArtifactCrudError::Database {
                message: "failed to query artifact blob references for gc".to_owned(),
                source,
            })?;
        if refs.is_empty() && blob.created_at.timestamp_millis() <= cutoff_ms {
            orphan_blobs.push(ArtifactGcBlobCandidateRecord {
                blob_id: blob.id,
                storage_key: blob.storage_key,
                size_bytes: blob.size_bytes.max(0) as u64,
            });
        }
    }

    let stale_projections = artifact_projection::Entity::find()
        .filter(artifact_projection::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact_projection::Column::Status.eq("stale"))
        .all(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to query stale artifact projections".to_owned(),
            source,
        })?
        .into_iter()
        .filter(|projection| projection.updated_at.timestamp_millis() <= cutoff_ms)
        .map(|projection| projection.id)
        .collect::<Vec<_>>();

    Ok(ArtifactGcPlanRecord {
        workspace_id: workspace_id.to_owned(),
        estimated_bytes_reclaimable: orphan_blobs.iter().map(|blob| blob.size_bytes).sum(),
        orphan_blobs,
        stale_projections,
        expired_external_refs: count_expired_external_refs(db, workspace_id, now_unix_ms).await?,
    })
}

pub async fn delete_blob_row<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    blob_id: &str,
) -> ArtifactCrudResult<u64> {
    let result = artifact_blob::Entity::delete_many()
        .filter(artifact_blob::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact_blob::Column::Id.eq(blob_id.to_owned()))
        .exec(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to delete orphan artifact blob row".to_owned(),
            source,
        })?;
    Ok(result.rows_affected)
}

pub async fn delete_projection_row<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    projection_id: &str,
) -> ArtifactCrudResult<u64> {
    let result = artifact_projection::Entity::delete_many()
        .filter(artifact_projection::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact_projection::Column::Id.eq(projection_id.to_owned()))
        .exec(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to delete stale artifact projection".to_owned(),
            source,
        })?;
    Ok(result.rows_affected)
}

pub async fn count_artifacts_by_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> ArtifactCrudResult<u64> {
    artifact::Entity::find()
        .filter(artifact::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .count(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to count artifacts".to_owned(),
            source,
        })
}

pub async fn count_blobs_by_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> ArtifactCrudResult<u64> {
    artifact_blob::Entity::find()
        .filter(artifact_blob::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .count(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to count artifact blobs".to_owned(),
            source,
        })
}

pub async fn count_versions_by_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> ArtifactCrudResult<u64> {
    artifact_version::Entity::find()
        .filter(artifact_version::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .count(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to count artifact versions".to_owned(),
            source,
        })
}

pub async fn count_bindings_by_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> ArtifactCrudResult<u64> {
    artifact_binding::Entity::find()
        .filter(artifact_binding::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .count(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to count artifact bindings".to_owned(),
            source,
        })
}

pub async fn insert_test_blob<C: ConnectionTrait>(
    db: &C,
    record: NewArtifactBlobRecord,
    created_at_unix_ms: i64,
    id: String,
) -> ArtifactCrudResult<ArtifactBlobRecord> {
    let created_at = unix_ms_to_datetime(created_at_unix_ms);
    artifact_blob::ActiveModel {
        id: Set(id),
        workspace_id: Set(record.workspace_id),
        sha256: Set(record.sha256),
        size_bytes: Set(to_i64_size(record.size_bytes)?),
        mime_type: Set(record.mime_type),
        storage_backend: Set(record.storage_backend),
        storage_key: Set(record.storage_key),
        encryption_key_id: Set(None),
        created_at: Set(created_at),
        last_verified_at: Set(None),
        metadata_json: Set(metadata_to_db(&record.metadata)?),
    }
    .insert(db)
    .await
    .map_err(|source| ArtifactCrudError::Database {
        message: "failed to insert test artifact blob".to_owned(),
        source,
    })
    .map(blob_record_from_model)
}

pub async fn update_test_artifact_status<C: ConnectionTrait>(
    db: &C,
    artifact_id: &str,
    status: &str,
) -> ArtifactCrudResult<()> {
    let model = artifact::Entity::find_by_id(artifact_id.to_owned())
        .one(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to query test artifact".to_owned(),
            source,
        })?
        .ok_or_else(|| ArtifactCrudError::NotFound {
            message: format!("artifact={artifact_id}"),
        })?;
    let mut active = model.into_active_model();
    active.status = Set(status.to_owned());
    active
        .update(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to update test artifact status".to_owned(),
            source,
        })?;
    Ok(())
}

async fn query_external_refs_by_key<C: ConnectionTrait>(
    db: &C,
    key: &ArtifactExternalRefKey,
) -> ArtifactCrudResult<Vec<ArtifactExternalRefRecord>> {
    query_external_ref_models_by_key(db, key)
        .await?
        .into_iter()
        .map(external_ref_record_from_model)
        .collect::<Vec<_>>()
        .pipe(Ok)
}

async fn query_external_ref_models_by_key<C: ConnectionTrait>(
    db: &C,
    key: &ArtifactExternalRefKey,
) -> ArtifactCrudResult<Vec<artifact_external_ref::Model>> {
    validate_external_ref_key(key)?;
    let mut query = artifact_external_ref::Entity::find()
        .filter(artifact_external_ref::Column::WorkspaceId.eq(key.workspace_id.clone()))
        .filter(artifact_external_ref::Column::ArtifactId.eq(key.artifact_id.clone()))
        .filter(artifact_external_ref::Column::Provider.eq(key.provider.clone()))
        .filter(artifact_external_ref::Column::TransportKind.eq(key.transport_kind.clone()));

    query = match &key.artifact_version_id {
        Some(version_id) => {
            query.filter(artifact_external_ref::Column::ArtifactVersionId.eq(version_id.clone()))
        }
        None => query.filter(artifact_external_ref::Column::ArtifactVersionId.is_null()),
    };

    query = match &key.model_family {
        Some(model_family) => {
            query.filter(artifact_external_ref::Column::ModelFamily.eq(model_family.clone()))
        }
        None => query.filter(artifact_external_ref::Column::ModelFamily.is_null()),
    };

    query
        .order_by_desc(artifact_external_ref::Column::CreatedAt)
        .all(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to query artifact external ref".to_owned(),
            source,
        })
}

async fn projection_preview<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    artifact_id: &str,
    version_id: &str,
    mime_type: Option<String>,
    size_bytes: Option<u64>,
) -> ArtifactCrudResult<Option<ArtifactPreviewRef>> {
    let projection = artifact_projection::Entity::find()
        .filter(artifact_projection::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact_projection::Column::ArtifactId.eq(artifact_id.to_owned()))
        .filter(artifact_projection::Column::ArtifactVersionId.eq(version_id.to_owned()))
        .order_by_desc(artifact_projection::Column::UpdatedAt)
        .one(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to query artifact preview projection".to_owned(),
            source,
        })?;

    Ok(projection.map(|projection| ArtifactPreviewRef {
        projection_kind: projection_kind_from_db(projection.projection_kind.as_str()),
        status: projection_status_from_db(projection.status.as_str()),
        artifact_id: artifact_id.to_owned(),
        version_id: version_id.to_owned(),
        mime_type,
        size_bytes,
    }))
}

async fn count_expired_external_refs<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now_unix_ms: i64,
) -> ArtifactCrudResult<u64> {
    let now = unix_ms_to_datetime(now_unix_ms);
    artifact_external_ref::Entity::find()
        .filter(artifact_external_ref::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact_external_ref::Column::ExpiresAt.lt(now))
        .count(db)
        .await
        .map_err(|source| ArtifactCrudError::Database {
            message: "failed to count expired artifact external refs".to_owned(),
            source,
        })
}

fn summary_from_models(
    artifact: artifact::Model,
    version: artifact_version::Model,
    blob: artifact_blob::Model,
    bindings: Vec<artifact_binding::Model>,
    preview: Option<ArtifactPreviewRef>,
) -> ArtifactCrudResult<ArtifactSummary> {
    let metadata = metadata_from_db(&artifact.metadata_json)?;
    let binding_summaries = bindings
        .iter()
        .map(binding_summary_from_model)
        .collect::<ArtifactCrudResult<Vec<_>>>()?;

    Ok(ArtifactSummary {
        artifact: ArtifactRef {
            artifact_id: artifact.id.clone(),
            version_id: Some(version.id),
            display_name: artifact.display_name,
            kind: kind_from_db(&artifact.kind)?,
            mime_type: artifact.mime_type,
            size_bytes: Some(u64::try_from(blob.size_bytes).unwrap_or(0)),
            sha256: Some(blob.sha256),
            status: status_from_db(&artifact.status)?,
            preview,
        },
        workspace_id: artifact.workspace_id,
        primary_thread_id: artifact.primary_thread_id,
        created_by_kind: created_by_kind_from_db(&artifact.created_by_kind)?,
        created_by_actor_id: artifact.created_by_actor_id,
        created_at: datetime_to_unix_ms(artifact.created_at),
        updated_at: datetime_to_unix_ms(artifact.updated_at),
        bindings: binding_summaries,
        metadata,
    })
}

fn binding_summary_from_model(
    binding: &artifact_binding::Model,
) -> ArtifactCrudResult<ArtifactBindingSummary> {
    Ok(ArtifactBindingSummary {
        binding_id: binding.id.clone(),
        workspace_id: binding.workspace_id.clone(),
        thread_id: binding.thread_id.clone(),
        turn_id: binding.turn_id.clone(),
        message_id: binding.message_id.clone(),
        turn_item_id: binding.turn_item_id.clone(),
        tool_call_id: binding.tool_call_id.clone(),
        task_id: binding.task_id.clone(),
        task_run_id: binding.task_run_id.clone(),
        binding_kind: binding_kind_from_db(&binding.binding_kind)?,
        direction: binding_direction_from_db(&binding.direction)?,
        item_index: binding.item_index,
        role: binding.role.as_deref().map(role_from_db).transpose()?,
        created_at: datetime_to_unix_ms(binding.created_at),
    })
}

fn blob_record_from_model(model: artifact_blob::Model) -> ArtifactBlobRecord {
    ArtifactBlobRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        sha256: model.sha256,
        size_bytes: model.size_bytes.max(0) as u64,
        mime_type: model.mime_type,
        storage_backend: model.storage_backend,
        storage_key: model.storage_key,
    }
}

fn artifact_record_from_model(model: artifact::Model) -> ArtifactRecord {
    ArtifactRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        current_version_id: model.current_version_id,
    }
}

fn version_record_from_model(model: artifact_version::Model) -> ArtifactVersionRecord {
    ArtifactVersionRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        artifact_id: model.artifact_id,
        blob_id: model.blob_id,
    }
}

fn projection_record_from_model(
    model: artifact_projection::Model,
) -> ArtifactCrudResult<ArtifactProjectionRecord> {
    Ok(ArtifactProjectionRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        artifact_id: model.artifact_id,
        artifact_version_id: model.artifact_version_id,
        projection_kind: projection_kind_from_db(model.projection_kind.as_str()),
        status: projection_status_from_db(model.status.as_str()),
        text_content: model.text_content,
    })
}

fn external_ref_record_from_model(
    model: artifact_external_ref::Model,
) -> ArtifactExternalRefRecord {
    ArtifactExternalRefRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        artifact_id: model.artifact_id,
        artifact_version_id: model.artifact_version_id,
        provider: model.provider,
        model_family: model.model_family,
        transport_kind: model.transport_kind,
        external_id: model.external_id,
        external_uri: model.external_uri,
        expires_at_unix_ms: model.expires_at.map(datetime_to_unix_ms),
    }
}

fn validate_external_ref_key(key: &ArtifactExternalRefKey) -> ArtifactCrudResult<()> {
    validate_non_empty("workspace_id", &key.workspace_id)?;
    validate_non_empty("artifact_id", &key.artifact_id)?;
    validate_non_empty("provider", &key.provider)?;
    validate_non_empty("transport_kind", &key.transport_kind)?;
    Ok(())
}

fn validate_non_empty(field: &str, value: &str) -> ArtifactCrudResult<()> {
    if value.trim().is_empty() {
        return Err(ArtifactCrudError::InvalidRequest {
            message: format!("{field} is required"),
        });
    }
    Ok(())
}

fn decode_list_cursor(cursor: Option<&str>) -> ArtifactCrudResult<u64> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    cursor
        .parse::<u64>()
        .map_err(|_| ArtifactCrudError::InvalidRequest {
            message: "artifact list cursor is invalid".to_owned(),
        })
}

fn to_i64_size(value: u64) -> ArtifactCrudResult<i64> {
    i64::try_from(value).map_err(|_| ArtifactCrudError::InvalidRequest {
        message: format!("artifact size {value} exceeds i64 storage limit"),
    })
}

fn now() -> DateTimeWithTimeZone {
    Utc::now().fixed_offset()
}

fn datetime_to_unix_ms(value: DateTimeWithTimeZone) -> i64 {
    value.timestamp_millis()
}

fn unix_ms_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_millis_opt(value)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

fn gc_cutoff_ms(now_unix_ms: i64, grace_secs: u64) -> i64 {
    let grace_ms = grace_secs
        .min(i64::MAX as u64 / 1_000)
        .saturating_mul(1_000);
    now_unix_ms.saturating_sub(i64::try_from(grace_ms).unwrap_or(i64::MAX))
}

fn metadata_to_db(value: &BTreeMap<String, serde_json::Value>) -> ArtifactCrudResult<String> {
    serde_json::to_string(value).map_err(|source| ArtifactCrudError::Json {
        message: "failed to encode artifact metadata".to_owned(),
        source,
    })
}

fn metadata_from_db(value: &str) -> ArtifactCrudResult<BTreeMap<String, serde_json::Value>> {
    serde_json::from_str(value).map_err(|source| ArtifactCrudError::Json {
        message: "failed to decode artifact metadata".to_owned(),
        source,
    })
}

fn kind_to_db(value: ArtifactKind) -> String {
    enum_to_db(value)
}

fn kind_from_db(value: &str) -> ArtifactCrudResult<ArtifactKind> {
    enum_from_db(value, "artifact kind")
}

fn status_to_db(value: ArtifactStatus) -> String {
    enum_to_db(value)
}

fn status_from_db(value: &str) -> ArtifactCrudResult<ArtifactStatus> {
    enum_from_db(value, "artifact status")
}

fn created_by_kind_to_db(value: ArtifactCreatedByKind) -> String {
    enum_to_db(value)
}

fn created_by_kind_from_db(value: &str) -> ArtifactCrudResult<ArtifactCreatedByKind> {
    enum_from_db(value, "artifact created_by_kind")
}

fn binding_kind_to_db(value: ArtifactBindingKind) -> String {
    enum_to_db(value)
}

fn binding_kind_from_db(value: &str) -> ArtifactCrudResult<ArtifactBindingKind> {
    enum_from_db(value, "artifact binding kind")
}

fn binding_direction_to_db(value: ArtifactBindingDirection) -> String {
    enum_to_db(value)
}

fn binding_direction_from_db(value: &str) -> ArtifactCrudResult<ArtifactBindingDirection> {
    enum_from_db(value, "artifact binding direction")
}

fn role_to_db(value: ArtifactRole) -> String {
    enum_to_db(value)
}

fn role_from_db(value: &str) -> ArtifactCrudResult<ArtifactRole> {
    enum_from_db(value, "artifact role")
}

fn projection_kind_to_db(kind: ArtifactProjectionKind) -> &'static str {
    match kind {
        ArtifactProjectionKind::PlainText => "plain_text",
        ArtifactProjectionKind::Thumbnail => "thumbnail",
        ArtifactProjectionKind::JsonSummary => "json_summary",
        ArtifactProjectionKind::PdfText => "pdf_text",
    }
}

fn projection_status_to_db(status: ArtifactProjectionStatus) -> &'static str {
    match status {
        ArtifactProjectionStatus::Pending => "pending",
        ArtifactProjectionStatus::Ready => "ready",
        ArtifactProjectionStatus::Failed => "failed",
        ArtifactProjectionStatus::Stale => "stale",
    }
}

fn projection_kind_from_db(value: &str) -> ArtifactProjectionKind {
    match value {
        "plain_text" => ArtifactProjectionKind::PlainText,
        "thumbnail" => ArtifactProjectionKind::Thumbnail,
        "json_summary" => ArtifactProjectionKind::JsonSummary,
        "pdf_text" => ArtifactProjectionKind::PdfText,
        _ => ArtifactProjectionKind::PlainText,
    }
}

fn projection_status_from_db(value: &str) -> ArtifactProjectionStatus {
    match value {
        "ready" => ArtifactProjectionStatus::Ready,
        "failed" => ArtifactProjectionStatus::Failed,
        "stale" => ArtifactProjectionStatus::Stale,
        _ => ArtifactProjectionStatus::Pending,
    }
}

fn enum_to_db<T: serde::Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn enum_from_db<T: serde::de::DeserializeOwned>(value: &str, field: &str) -> ArtifactCrudResult<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|source| {
        ArtifactCrudError::Json {
            message: format!("failed to decode {field} value `{value}`"),
            source,
        }
    })
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}
