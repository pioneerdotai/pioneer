//! Thread metadata reads and projections on the caller's connection.
use anyhow::{Context, Result};
use pioneer_entity::thread;
use pioneer_protocol::{ThreadStatus, TurnAuthorSnapshot};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, Func};
use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};

use crate::collaboration::validate_turn_author_snapshot;
use crate::values::thread_status_to_db;

const THREAD_PREVIEW_AUTHOR_JSON_MAX_BYTES: usize = 4_096;

pub fn prepare_preview_author_json(author: Option<&TurnAuthorSnapshot>) -> Result<Option<String>> {
    let Some(author) = author else {
        return Ok(None);
    };
    validate_turn_author_snapshot(author)?;
    let json = serde_json::to_string(author).context("failed to encode thread preview author")?;
    if json.len() > THREAD_PREVIEW_AUTHOR_JSON_MAX_BYTES {
        anyhow::bail!("thread preview author exceeds {THREAD_PREVIEW_AUTHOR_JSON_MAX_BYTES} bytes");
    }
    Ok(Some(json))
}

pub fn preview_author_from_json(json: Option<&str>) -> Result<Option<TurnAuthorSnapshot>> {
    let Some(json) = json else {
        return Ok(None);
    };
    if json.len() > THREAD_PREVIEW_AUTHOR_JSON_MAX_BYTES {
        anyhow::bail!(
            "persisted thread preview author exceeds {THREAD_PREVIEW_AUTHOR_JSON_MAX_BYTES} bytes"
        );
    }
    let author = serde_json::from_str::<TurnAuthorSnapshot>(json)
        .context("persisted thread preview author is invalid")?;
    validate_turn_author_snapshot(&author)?;
    Ok(Some(author))
}

/// Replaces an automatically-derived thread preview without overwriting a
/// preview that another committed operation has already changed.
pub async fn replace_thread_preview_if_matches<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    expected_preview: &str,
    replacement_preview: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = thread::Entity::update_many()
        .col_expr(
            thread::Column::Preview,
            Expr::value(replacement_preview.to_owned()),
        )
        .col_expr(thread::Column::UpdatedAt, Expr::value(updated_at))
        .filter(thread::Column::Id.eq(thread_id.to_owned()))
        .filter(thread::Column::Preview.eq(expected_preview.to_owned()))
        .exec(db)
        .await
        .context("failed to replace derived thread preview")?;
    Ok(result.rows_affected > 0)
}

/// Applies only the thread fields owned by an immediately-completed Message.
///
/// The TurnStarted payload carries a full Thread snapshot for compatibility
/// with the canonical event envelope. Re-upserting that snapshot would let a
/// concurrent Message restore stale execution or management state. The
/// Message transition owns only the first derived preview and a monotonic
/// activity timestamp on an already-persisted Thread.
pub async fn touch_thread_for_completed_message_prepared<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    derived_preview: &str,
    preview_author_json: Option<String>,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    if !derived_preview.is_empty() {
        thread::Entity::update_many()
            .col_expr(
                thread::Column::Preview,
                Expr::value(derived_preview.to_owned()),
            )
            .col_expr(
                thread::Column::PreviewAuthorJson,
                Expr::value(preview_author_json),
            )
            .filter(thread::Column::Id.eq(thread_id.to_owned()))
            .filter(thread::Column::Preview.eq(String::new()))
            .exec(db)
            .await
            .context("failed to initialize thread preview from Message")?;
    }

    thread::Entity::update_many()
        .col_expr(thread::Column::UpdatedAt, Expr::value(updated_at))
        .filter(thread::Column::Id.eq(thread_id.to_owned()))
        .filter(thread::Column::UpdatedAt.lt(updated_at))
        .exec(db)
        .await
        .context("failed to advance thread activity from Message")?;

    Ok(())
}

pub async fn find_thread_by_id<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<thread::Model>> {
    thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread by id")
}

pub async fn update_thread_status<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    status: ThreadStatus,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    thread::Entity::update_many()
        .col_expr(
            thread::Column::Status,
            Expr::value(thread_status_to_db(status)),
        )
        .col_expr(
            thread::Column::UpdatedAt,
            Expr::from(Func::greatest([
                Expr::col(thread::Column::UpdatedAt),
                Expr::val(updated_at),
            ])),
        )
        .filter(thread::Column::Id.eq(thread_id))
        .exec(db)
        .await
        .context("failed to update thread status")?;

    Ok(())
}

pub async fn update_thread_name<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    name: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let Some(model) = thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread for name update")?
    else {
        return Ok(());
    };

    let mut active_model: thread::ActiveModel = model.into();
    active_model.name = Set(Some(name.to_owned()));
    active_model.updated_at = Set(updated_at);
    active_model
        .update(db)
        .await
        .context("failed to update thread name")?;

    Ok(())
}

pub async fn update_thread_name_if_changed<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    name: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(model) = thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread for conditional name update")?
    else {
        return Ok(false);
    };

    if model.name.as_deref() == Some(name) {
        return Ok(false);
    }

    let mut active_model: thread::ActiveModel = model.into();
    active_model.name = Set(Some(name.to_owned()));
    active_model.updated_at = Set(updated_at);
    active_model
        .update(db)
        .await
        .context("failed to conditionally update thread name")?;

    Ok(true)
}

pub async fn update_thread_summary<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    summary: &str,
    turn_count: i64,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let Some(model) = thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread for summary update")?
    else {
        return Ok(());
    };

    let mut active_model: thread::ActiveModel = model.into();
    active_model.summary = Set(Some(summary.to_owned()));
    active_model.summary_turn_count = Set(Some(turn_count));
    active_model.updated_at = Set(updated_at);
    active_model
        .update(db)
        .await
        .context("failed to update thread summary")?;

    Ok(())
}
