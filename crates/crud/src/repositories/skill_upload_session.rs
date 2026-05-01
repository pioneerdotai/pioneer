use anyhow::{Context, Result};
use pioneer_entity::skill_upload_session;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, Set,
};

pub async fn upsert_skill_upload_session<C: ConnectionTrait>(
    db: &C,
    record: &crate::SkillUploadSessionRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    skill_upload_session::Entity::insert(skill_upload_session::ActiveModel {
        upload_id: Set(record.upload_id.clone()),
        workspace_id: Set(record.workspace_id.clone()),
        connection_id: Set(i64::try_from(record.connection_id).unwrap_or(i64::MAX)),
        status: Set(record.status.clone()),
        file_name: Set(record.file_name.clone()),
        archive_format: Set(record.archive_format.clone()),
        compressed_size_bytes: Set(i64::try_from(record.compressed_size_bytes).unwrap_or(i64::MAX)),
        received_bytes: Set(i64::try_from(record.received_bytes).unwrap_or(i64::MAX)),
        sha256: Set(record.sha256.clone()),
        payload_path: Set(record.payload_path.clone()),
        created_at_unix: Set(record.created_at_unix),
        expires_at_unix: Set(record.expires_at_unix),
        finalized_at_unix: Set(record.finalized_at_unix),
        consumed_at_unix: Set(record.consumed_at_unix),
        aborted_at_unix: Set(record.aborted_at_unix),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::column(skill_upload_session::Column::UploadId)
            .update_columns([
                skill_upload_session::Column::WorkspaceId,
                skill_upload_session::Column::ConnectionId,
                skill_upload_session::Column::Status,
                skill_upload_session::Column::FileName,
                skill_upload_session::Column::ArchiveFormat,
                skill_upload_session::Column::CompressedSizeBytes,
                skill_upload_session::Column::ReceivedBytes,
                skill_upload_session::Column::Sha256,
                skill_upload_session::Column::PayloadPath,
                skill_upload_session::Column::ExpiresAtUnix,
                skill_upload_session::Column::FinalizedAtUnix,
                skill_upload_session::Column::ConsumedAtUnix,
                skill_upload_session::Column::AbortedAtUnix,
                skill_upload_session::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to upsert skill upload session `{}`",
            record.upload_id
        )
    })?;

    Ok(())
}

pub async fn find_skill_upload_session<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
) -> Result<Option<skill_upload_session::Model>> {
    skill_upload_session::Entity::find_by_id(upload_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to query skill upload session `{upload_id}`"))
}

pub async fn list_expired_skill_upload_sessions<C: ConnectionTrait>(
    db: &C,
    now_unix: i64,
) -> Result<Vec<skill_upload_session::Model>> {
    skill_upload_session::Entity::find()
        .filter(skill_upload_session::Column::ExpiresAtUnix.lte(now_unix))
        .filter(
            skill_upload_session::Column::Status
                .is_in(["receiving".to_owned(), "finalized".to_owned()]),
        )
        .order_by_asc(skill_upload_session::Column::ExpiresAtUnix)
        .all(db)
        .await
        .context("failed to query expired skill upload sessions")
}

pub async fn list_stale_skill_upload_sessions<C: ConnectionTrait>(
    db: &C,
    now_unix: i64,
) -> Result<Vec<skill_upload_session::Model>> {
    skill_upload_session::Entity::find()
        .filter(
            Condition::any()
                .add(skill_upload_session::Column::ExpiresAtUnix.lte(now_unix))
                .add(skill_upload_session::Column::Status.is_in([
                    "aborted".to_owned(),
                    "consumed".to_owned(),
                    "expired".to_owned(),
                ])),
        )
        .order_by_asc(skill_upload_session::Column::ExpiresAtUnix)
        .all(db)
        .await
        .context("failed to query stale skill upload sessions")
}

pub async fn update_skill_upload_received_bytes<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    received_bytes: u64,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<skill_upload_session::Model>> {
    let Some(model) = find_skill_upload_session(db, upload_id).await? else {
        return Ok(None);
    };

    let mut active = model.into_active_model();
    active.received_bytes = Set(i64::try_from(received_bytes).unwrap_or(i64::MAX));
    active.updated_at = Set(updated_at);
    let updated = active
        .update(db)
        .await
        .with_context(|| format!("failed to update received bytes for upload `{upload_id}`"))?;
    Ok(Some(updated))
}

pub async fn update_skill_upload_status<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    status: &str,
    finalized_at_unix: Option<i64>,
    consumed_at_unix: Option<i64>,
    aborted_at_unix: Option<i64>,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<skill_upload_session::Model>> {
    let Some(model) = find_skill_upload_session(db, upload_id).await? else {
        return Ok(None);
    };

    let mut active = model.into_active_model();
    active.status = Set(status.to_owned());
    if finalized_at_unix.is_some() {
        active.finalized_at_unix = Set(finalized_at_unix);
    }
    if consumed_at_unix.is_some() {
        active.consumed_at_unix = Set(consumed_at_unix);
    }
    if aborted_at_unix.is_some() {
        active.aborted_at_unix = Set(aborted_at_unix);
    }
    active.updated_at = Set(updated_at);
    let updated = active
        .update(db)
        .await
        .with_context(|| format!("failed to update status for upload `{upload_id}`"))?;
    Ok(Some(updated))
}
