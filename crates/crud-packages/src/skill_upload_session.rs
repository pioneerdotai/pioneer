use crate::skill_records::SkillUploadSessionRecord;
use anyhow::{Context, Result};
use pioneer_entity::skill_upload_session;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, Set,
};

pub async fn insert_skill_upload_session<C: ConnectionTrait>(
    db: &C,
    record: &crate::skill_records::SkillUploadSessionRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    skill_upload_session::Entity::insert(skill_upload_session::ActiveModel {
        upload_id: Set(record.upload_id.clone()),
        workspace_id: Set(record.workspace_id.clone()),
        connection_id: Set(i64::try_from(record.connection_id).unwrap_or(i64::MAX)),
        status: Set(record.status.clone()),
        purpose: Set(record.purpose.clone()),
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
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert skill upload session `{}`",
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

/// Earliest future expiry after processing the currently eligible uploads.
pub async fn next_skill_upload_deadline<C: ConnectionTrait>(
    db: &C,
    now: i64,
) -> Result<Option<i64>> {
    Ok(skill_upload_session::Entity::find()
        .select_only()
        .column(skill_upload_session::Column::ExpiresAtUnix)
        .filter(skill_upload_session::Column::Status.is_in(["receiving", "finalized"]))
        .filter(skill_upload_session::Column::ExpiresAtUnix.gt(now))
        .order_by_asc(skill_upload_session::Column::ExpiresAtUnix)
        .limit(1)
        .into_tuple::<i64>()
        .one(db)
        .await?)
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

pub async fn transition_skill_upload_status<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    expected_statuses: &[&str],
    status: &str,
    finalized_at_unix: Option<i64>,
    consumed_at_unix: Option<i64>,
    aborted_at_unix: Option<i64>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if expected_statuses.is_empty() {
        anyhow::bail!("skill upload transition requires at least one expected status");
    }
    let mut update = skill_upload_session::Entity::update_many()
        .filter(skill_upload_session::Column::UploadId.eq(upload_id.to_owned()))
        .filter(
            skill_upload_session::Column::Status.is_in(
                expected_statuses
                    .iter()
                    .map(|status| (*status).to_owned())
                    .collect::<Vec<_>>(),
            ),
        )
        .filter(skill_upload_session::Column::ConsumedAtUnix.is_null())
        .filter(skill_upload_session::Column::AbortedAtUnix.is_null())
        .col_expr(
            skill_upload_session::Column::Status,
            Expr::value(status.to_owned()),
        )
        .col_expr(
            skill_upload_session::Column::UpdatedAt,
            Expr::value(updated_at),
        );
    if let Some(finalized_at_unix) = finalized_at_unix {
        update = update.col_expr(
            skill_upload_session::Column::FinalizedAtUnix,
            Expr::value(finalized_at_unix),
        );
    }
    if let Some(consumed_at_unix) = consumed_at_unix {
        update = update.col_expr(
            skill_upload_session::Column::ConsumedAtUnix,
            Expr::value(consumed_at_unix),
        );
    }
    if let Some(aborted_at_unix) = aborted_at_unix {
        update = update.col_expr(
            skill_upload_session::Column::AbortedAtUnix,
            Expr::value(aborted_at_unix),
        );
    }
    let result = update.exec(db).await.with_context(|| {
        format!(
            "failed to transition upload `{upload_id}` from [{}] to `{status}`",
            expected_statuses.join(", ")
        )
    })?;
    Ok(result.rows_affected == 1)
}

pub fn skill_upload_session_record_from_model(
    model: pioneer_entity::skill_upload_session::Model,
) -> SkillUploadSessionRecord {
    SkillUploadSessionRecord {
        purpose: model.purpose,
        upload_id: model.upload_id,
        workspace_id: model.workspace_id,
        connection_id: u64::try_from(model.connection_id).unwrap_or_default(),
        status: model.status,
        file_name: model.file_name,
        archive_format: model.archive_format,
        compressed_size_bytes: u64::try_from(model.compressed_size_bytes).unwrap_or_default(),
        received_bytes: u64::try_from(model.received_bytes).unwrap_or_default(),
        sha256: model.sha256,
        payload_path: model.payload_path,
        created_at_unix: model.created_at_unix,
        expires_at_unix: model.expires_at_unix,
        finalized_at_unix: model.finalized_at_unix,
        consumed_at_unix: model.consumed_at_unix,
        aborted_at_unix: model.aborted_at_unix,
    }
}
