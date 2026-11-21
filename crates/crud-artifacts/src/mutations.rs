use crate::artifact::{
    self, ArtifactBindingTargetRecord, ArtifactBlobRecord, ArtifactCrudError,
    ArtifactExternalRefRecord, ArtifactProjectionRecord, ArtifactRecord, ArtifactRepository,
    NewArtifactBlobRecord, UpsertArtifactExternalRefRequest,
};
use anyhow::{Context, Result};
use pioneer_entity::artifact as artifact_row;
use pioneer_protocol::{
    ArtifactBindingSummary, ArtifactProjectionKind, ArtifactProjectionStatus, ArtifactStatus,
    generate_id,
};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::{
    ColumnTrait, EntityTrait, QueryFilter, QuerySelect, TransactionTrait,
    entity::prelude::DateTimeWithTimeZone,
};
use std::{collections::BTreeMap, future::Future, time::Duration};

async fn retry_write<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry_with_backoff(
        operation,
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

pub async fn update_status(
    db: &SqliteDatabase,
    workspace_id: &str,
    artifact_id: &str,
    status: ArtifactStatus,
    deleted_at: Option<DateTimeWithTimeZone>,
) -> Result<ArtifactRecord> {
    let change = db.work_signal("maintenance.html").changing();
    let at = chrono::Utc::now().fixed_offset();
    let updated = retry_write(|| async {
        let transaction = db.begin().await?;
        let repository = ArtifactRepository::new();
        let result = async {
            let artifact = repository
                .update_artifact_status(&transaction, workspace_id, artifact_id, status, deleted_at)
                .await?;
            repository
                .touch_html_artifact_projection(&transaction, workspace_id, artifact_id, at)
                .await?;
            Ok::<_, artifact::ArtifactCrudError>(artifact)
        }
        .await;
        match result {
            Ok(artifact) => {
                transaction.commit().await?;
                Ok(artifact)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error.into())
            }
        }
    })
    .await?;
    change.completed(true);
    Ok(updated)
}

pub async fn replace_projection(
    db: &SqliteDatabase,
    workspace_id: &str,
    artifact_id: &str,
    artifact_version_id: &str,
    projection_kind: ArtifactProjectionKind,
    status: ArtifactProjectionStatus,
    text_content: Option<String>,
    blob_id: Option<String>,
    metadata: BTreeMap<String, serde_json::Value>,
) -> Result<ArtifactProjectionRecord> {
    let change = db.work_signal("maintenance.html").changing();
    let updated = retry_write(|| {
        let text_content = text_content.clone();
        let blob_id = blob_id.clone();
        let metadata = metadata.clone();
        async move {
            artifact::replace_projection(
                db,
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
            .map_err(Into::into)
        }
    })
    .await?;
    change.completed(true);
    Ok(updated)
}

pub async fn find_or_create_blob(
    db: &SqliteDatabase,
    blob: NewArtifactBlobRecord,
) -> Result<ArtifactBlobRecord> {
    let id = generate_id(21);
    let at = chrono::Utc::now().fixed_offset();
    let size = artifact::checked_size_bytes(blob.size_bytes)?;
    let metadata = artifact::metadata_to_db(&blob.metadata)?;
    let change = db.work_signal("maintenance.html").changing();
    let result = retry_write(|| async {
        Ok(ArtifactRepository::new()
            .find_or_create_blob_prepared(db, blob.clone(), id.clone(), at, size, metadata.clone())
            .await?)
    })
    .await?;
    change.completed(true);
    Ok(result)
}

pub async fn bind_artifact(
    db: &SqliteDatabase,
    workspace_id: &str,
    artifact_id: &str,
    version_id: Option<&str>,
    target: ArtifactBindingTargetRecord,
    metadata: BTreeMap<String, serde_json::Value>,
) -> Result<ArtifactBindingSummary> {
    let binding_id = generate_id(21);
    let created_at = chrono::Utc::now().fixed_offset();
    let metadata_json = artifact::metadata_to_db(&metadata)?;
    let change = db.work_signal("maintenance.html").changing();
    let result = retry_write(|| async {
        let repository = ArtifactRepository::new();
        let resolved_version_id = repository
            .resolve_binding_version(db, workspace_id, artifact_id, version_id)
            .await?;
        let transaction = db
            .begin()
            .await
            .context("failed to begin artifact bind transaction")?;
        let result = async {
            if version_id.is_none() {
                // Keep the current-version pointer stable until commit.
                let locked = artifact_row::Entity::find_by_id(artifact_id.to_owned())
                    .filter(artifact_row::Column::WorkspaceId.eq(workspace_id))
                    .select_only()
                    .column(artifact_row::Column::Id)
                    .lock_shared()
                    .into_tuple::<String>()
                    .one(&transaction)
                    .await
                    .map_err(|source| ArtifactCrudError::Database {
                        message: "failed to lock artifact for binding".into(),
                        source,
                    })?;
                if locked.is_none() {
                    return Err(ArtifactCrudError::NotFound {
                        message: format!("workspace={workspace_id} artifact={artifact_id}"),
                    });
                }
            }
            let fenced_version_id = repository
                .resolve_binding_version(&transaction, workspace_id, artifact_id, version_id)
                .await?;
            if fenced_version_id != resolved_version_id {
                return Err(ArtifactCrudError::InvalidRequest {
                    message: "artifact current version changed during binding preparation".into(),
                });
            }
            repository
                .create_binding_prepared(
                    &transaction,
                    workspace_id,
                    artifact_id,
                    Some(&resolved_version_id),
                    &target,
                    binding_id.clone(),
                    created_at,
                    metadata_json.clone(),
                )
                .await
        }
        .await;
        let binding = match result {
            Ok(binding) => binding,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error.into());
            }
        };
        transaction
            .commit()
            .await
            .context("failed to commit artifact bind transaction")?;
        Ok(binding)
    })
    .await?;
    change.completed(true);
    Ok(result)
}

pub async fn upsert_external_ref(
    db: &SqliteDatabase,
    request: UpsertArtifactExternalRefRequest,
) -> Result<ArtifactExternalRefRecord> {
    let change = db.work_signal("maintenance.html").changing();
    let result =
        retry_write(|| async { Ok(artifact::upsert_external_ref(db, request.clone()).await?) })
            .await?;
    change.completed(true);
    Ok(result)
}

pub async fn prune_expired_external_refs(
    db: &SqliteDatabase,
    workspace_id: &str,
    now_unix_ms: i64,
) -> Result<u64> {
    let change = db.work_signal("maintenance.html").changing();
    let removed = retry_write(|| async {
        Ok(artifact::prune_expired_external_refs(db, workspace_id, now_unix_ms).await?)
    })
    .await?;
    change.completed(removed > 0);
    Ok(removed)
}

pub async fn delete_blob_row(
    db: &SqliteDatabase,
    workspace_id: &str,
    blob_id: &str,
) -> Result<u64> {
    let change = db.work_signal("maintenance.html").changing();
    let removed =
        retry_write(|| async { Ok(artifact::delete_blob_row(db, workspace_id, blob_id).await?) })
            .await?;
    change.completed(removed > 0);
    Ok(removed)
}

pub async fn delete_projection_row(
    db: &SqliteDatabase,
    workspace_id: &str,
    projection_id: &str,
) -> Result<u64> {
    let change = db.work_signal("maintenance.html").changing();
    let removed = retry_write(|| async {
        Ok(artifact::delete_projection_row(db, workspace_id, projection_id).await?)
    })
    .await?;
    change.completed(removed > 0);
    Ok(removed)
}
