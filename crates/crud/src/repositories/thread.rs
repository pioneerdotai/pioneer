use anyhow::{Context, Result};
use pioneer_entity::thread;
use pioneer_protocol::{PersistedActorRef, Thread, ThreadStatus};
use sea_orm::entity::ActiveModelTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    thread_mode_to_db, thread_origin_kind_to_db, thread_sidebar_visibility_to_db,
    thread_status_to_db,
};
use crate::repositories::identity::{actor_ref_from_db, actor_ref_to_db};

pub async fn upsert_thread<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    upsert_thread_with_actor_columns(db, thread_model, None, None, created_at, updated_at).await
}

pub async fn upsert_thread_with_creator<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: &PersistedActorRef,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    if let Some(existing) = find_thread_by_id(db, thread_model.id.as_str()).await? {
        let existing_creator = actor_ref_from_db(
            existing.created_by_actor_kind.as_deref(),
            existing.created_by_actor_id.as_deref(),
        )
        .with_context(|| {
            format!(
                "thread `{}` has an invalid persisted creator pair",
                thread_model.id
            )
        })?;
        if existing_creator.is_none() {
            anyhow::bail!(
                "thread `{}` is missing its persisted creator",
                thread_model.id
            );
        }
    }
    let (actor_kind, actor_id) = actor_ref_to_db(creator);
    upsert_thread_with_actor_columns(
        db,
        thread_model,
        actor_kind,
        actor_id,
        created_at,
        updated_at,
    )
    .await
}

async fn upsert_thread_with_actor_columns<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    created_by_actor_kind: Option<String>,
    created_by_actor_id: Option<String>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    thread::Entity::insert(thread::ActiveModel {
        id: Set(thread_model.id.clone()),
        workspace_id: Set(thread_model.workspace_id.clone()),
        name: Set(thread_model.name.clone()),
        preview: Set(thread_model.preview.clone()),
        mode: Set(thread_mode_to_db(thread_model.mode).to_owned()),
        model: Set(thread_model.model.clone()),
        model_provider: Set(thread_model.model_provider.clone()),
        status: Set(thread_status_to_db(thread_model.status).to_owned()),
        origin_kind: Set(thread_origin_kind_to_db(thread_model.origin_kind).to_owned()),
        sidebar_visibility: Set(
            thread_sidebar_visibility_to_db(thread_model.sidebar_visibility).to_owned(),
        ),
        agent_nickname: Set(thread_model.agent_nickname.clone()),
        agent_role: Set(thread_model.agent_role.clone()),
        created_by_actor_id: Set(created_by_actor_id),
        created_by_actor_kind: Set(created_by_actor_kind),
        summary: Set(None),
        summary_turn_count: Set(None),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::column(thread::Column::Id)
            .update_columns([
                thread::Column::WorkspaceId,
                thread::Column::Name,
                thread::Column::Preview,
                thread::Column::Mode,
                thread::Column::Model,
                thread::Column::ModelProvider,
                thread::Column::Status,
                thread::Column::OriginKind,
                thread::Column::SidebarVisibility,
                thread::Column::AgentNickname,
                thread::Column::AgentRole,
                thread::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert thread")?;

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

pub async fn list_threads_by_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    limit: u64,
) -> Result<Vec<thread::Model>> {
    thread::Entity::find()
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(thread::Column::SidebarVisibility.eq("visible"))
        .order_by_desc(thread::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list threads by workspace")
}

pub async fn update_thread_status<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    status: ThreadStatus,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let Some(model) = thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread for status update")?
    else {
        return Ok(());
    };

    let updated_at = model.updated_at.max(updated_at);
    let mut active_model: thread::ActiveModel = model.into();
    active_model.status = Set(thread_status_to_db(status).to_owned());
    active_model.updated_at = Set(updated_at);
    active_model
        .update(db)
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
