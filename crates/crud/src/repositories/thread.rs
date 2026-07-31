use anyhow::{Context, Result};
use pioneer_entity::thread;
use pioneer_protocol::{
    PersistedActorRef, PrincipalId, Thread, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
};
use sea_orm::entity::ActiveModelTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
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

/// Inserts a new user-addressable thread without any upsert semantics.
///
/// The caller chooses only a user-selectable access class. Internal threads
/// have separate trusted creation paths and can never be manufactured through
/// the public thread/start transaction.
pub async fn insert_user_thread_with_creator<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: &PersistedActorRef,
    access_class: super::membership::PersistedThreadAccessClass,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    if access_class == super::membership::PersistedThreadAccessClass::Internal {
        anyhow::bail!("internal access class is not valid for a user thread");
    }
    if matches!(
        thread_model.origin_kind,
        ThreadOriginKind::TaskRun | ThreadOriginKind::System
    ) || thread_model.sidebar_visibility != ThreadSidebarVisibility::Visible
    {
        anyhow::bail!("internal origin or visibility is not valid for a user thread");
    }

    let (created_by_actor_kind, created_by_actor_id) = actor_ref_to_db(creator);
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
        access_class: Set(
            super::membership::persisted_thread_access_class_to_db(access_class).to_owned(),
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
    .exec_without_returning(db)
    .await
    .context("failed to insert user thread")?;

    Ok(())
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
        access_class: Set(if matches!(
            thread_model.origin_kind,
            pioneer_protocol::ThreadOriginKind::TaskRun
                | pioneer_protocol::ThreadOriginKind::System
        ) || matches!(
            thread_model.sidebar_visibility,
            pioneer_protocol::ThreadSidebarVisibility::Hidden
        ) {
            super::membership::persisted_thread_access_class_to_db(
                super::membership::PersistedThreadAccessClass::Internal,
            )
        } else {
            super::membership::persisted_thread_access_class_to_db(
                super::membership::PersistedThreadAccessClass::Private,
            )
        }
        .to_owned()),
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

/// Applies the public thread-management surface to one exact user thread.
///
/// Member callers pass their principal as `expected_creator`; Superuser passes
/// `None`. Membership rows are deliberately untouched so a
/// private→workspace→private roundtrip restores the same explicit audience.
pub async fn update_user_thread_management(
    db: &DatabaseTransaction,
    workspace_id: &str,
    thread_id: &str,
    expected_creator: Option<&PrincipalId>,
    name: Option<&str>,
    access_class: Option<super::membership::PersistedThreadAccessClass>,
    archived: Option<bool>,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<bool>> {
    let Some(model) = thread::Entity::find_by_id(thread_id.to_owned())
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .one(db)
        .await
        .context("failed to load exact thread for management")?
    else {
        return Ok(None);
    };
    let current_access =
        super::membership::persisted_thread_access_class_from_db(model.access_class.as_str())
            .context("managed thread has an invalid access class")?;
    if current_access == super::membership::PersistedThreadAccessClass::Internal
        || matches!(
            model.origin_kind.as_str(),
            "task_run" | "taskRun" | "system"
        )
        || model.sidebar_visibility != "visible"
    {
        anyhow::bail!("internal threads are not mutable through the user management path");
    }
    if access_class == Some(super::membership::PersistedThreadAccessClass::Internal) {
        anyhow::bail!("internal access class is not user-selectable");
    }
    if let Some(expected_creator) = expected_creator {
        let creator = actor_ref_from_db(
            model.created_by_actor_kind.as_deref(),
            model.created_by_actor_id.as_deref(),
        )
        .context("managed thread has an invalid creator")?;
        if creator.as_ref() != Some(&PersistedActorRef::Principal(expected_creator.clone())) {
            return Ok(None);
        }
    }

    let current_name = model.name.clone();
    let current_status = model.status.clone();
    let updated_at = model.updated_at.max(updated_at);
    let mut changed = false;
    let mut active: thread::ActiveModel = model.into();
    if let Some(name) = name
        && current_name.as_deref() != Some(name)
    {
        active.name = Set(Some(name.to_owned()));
        changed = true;
    }
    if let Some(access_class) = access_class
        && current_access != access_class
    {
        active.access_class =
            Set(super::membership::persisted_thread_access_class_to_db(access_class).to_owned());
        changed = true;
    }
    if let Some(archived) = archived {
        let target_status = if archived {
            "closed"
        } else if current_status == "closed" {
            "idle"
        } else {
            current_status.as_str()
        };
        if current_status != target_status {
            active.status = Set(target_status.to_owned());
            changed = true;
        }
    }
    if changed {
        active.updated_at = Set(updated_at);
        active
            .update(db)
            .await
            .context("failed to update exact user thread management state")?;
    }
    Ok(Some(changed))
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
