//! Thread creation and exact in-transaction management updates.
use anyhow::{Context, Result};
use pioneer_crud_access::identity::{actor_ref_from_db, actor_ref_to_db};
use pioneer_crud_access::membership::{
    PersistedThreadAccessClass, persisted_thread_access_class_from_db,
    persisted_thread_access_class_to_db,
};
use pioneer_entity::thread;
use pioneer_protocol::{
    PersistedActorRef, PrincipalId, Thread, ThreadOriginKind, ThreadSidebarVisibility,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QuerySelect, Set,
};

use crate::thread_metadata::{find_thread_by_id, prepare_preview_author_json};
use crate::values::{
    thread_mode_to_db, thread_origin_kind_to_db, thread_sidebar_visibility_to_db,
    thread_status_to_db,
};

pub async fn upsert_thread_with_creator<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: &PersistedActorRef,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
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
        let Some(existing_creator) = existing_creator else {
            anyhow::bail!(
                "thread `{}` is missing its persisted creator",
                thread_model.id
            );
        };
        if &existing_creator != creator {
            anyhow::bail!(
                "thread `{}` has a different persisted creator",
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
        None,
        created_at,
        updated_at,
    )
    .await
}

/// Persists a trusted internal or agent-created user-addressable Thread with
/// an explicit immutable access class. Agent creation cannot infer the access
/// class from origin because a delegated visible root is workspace
/// scoped while its creator is an AgentExecution rather than a Principal.
pub async fn upsert_agent_thread_with_creator<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: &PersistedActorRef,
    access_class: PersistedThreadAccessClass,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    match access_class {
        PersistedThreadAccessClass::Internal
            if thread_model.sidebar_visibility == ThreadSidebarVisibility::Hidden
                && thread_model.visibility.is_none() => {}
        PersistedThreadAccessClass::Workspace
            if thread_model.sidebar_visibility == ThreadSidebarVisibility::Visible
                && thread_model.visibility
                    == Some(pioneer_protocol::ThreadVisibility::Workspace)
                && !matches!(
                    thread_model.origin_kind,
                    ThreadOriginKind::TaskRun | ThreadOriginKind::System
                ) => {}
        _ => anyhow::bail!("agent-created thread has an inconsistent access projection"),
    }
    if let Some(existing) = find_thread_by_id(db, thread_model.id.as_str()).await? {
        let existing_creator = actor_ref_from_db(
            existing.created_by_actor_kind.as_deref(),
            existing.created_by_actor_id.as_deref(),
        )?;
        let existing_access = persisted_thread_access_class_from_db(&existing.access_class)?;
        if existing_creator.as_ref() != Some(creator) || existing_access != access_class {
            anyhow::bail!("agent-created thread id was reused with different access facts");
        }
    }
    let (actor_kind, actor_id) = actor_ref_to_db(creator);
    upsert_thread_with_actor_columns(
        db,
        thread_model,
        actor_kind,
        actor_id,
        Some(access_class),
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
    access_class: PersistedThreadAccessClass,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
    public_start: Option<i64>,
) -> Result<()> {
    if access_class == PersistedThreadAccessClass::Internal {
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
    let preview_author_json = prepare_preview_author_json(thread_model.preview_author.as_ref())?;
    anyhow::ensure!(
        public_start.is_some() == (access_class == PersistedThreadAccessClass::Workspace),
        "thread completion inbox frontier does not match access class",
    );
    thread::Entity::insert(thread::ActiveModel {
        workspace_unread_started_at: sea_orm::NotSet,
        id: Set(thread_model.id.clone()),
        workspace_id: Set(thread_model.workspace_id.clone()),
        name: Set(thread_model.name.clone()),
        preview: Set(thread_model.preview.clone()),
        preview_author_json: Set(preview_author_json),
        mode: Set(thread_mode_to_db(thread_model.mode).to_owned()),
        model: Set(thread_model.model.clone()),
        model_provider: Set(thread_model.model_provider.clone()),
        status: Set(thread_status_to_db(thread_model.status).to_owned()),
        origin_kind: Set(thread_origin_kind_to_db(thread_model.origin_kind).to_owned()),
        sidebar_visibility: Set(
            thread_sidebar_visibility_to_db(thread_model.sidebar_visibility).to_owned(),
        ),
        access_class: Set(persisted_thread_access_class_to_db(access_class).to_owned()),
        agent_nickname: Set(thread_model.agent_nickname.clone()),
        agent_role: Set(thread_model.agent_role.clone()),
        created_by_actor_id: Set(created_by_actor_id),
        created_by_actor_kind: Set(created_by_actor_kind),
        inbox_workspace_after_sequence: Set(public_start),
        inbox_workspace_started_at: Set(public_start.map(|_| created_at)),
        summary: Set(None),
        summary_turn_count: Set(None),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
        ..Default::default()
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
    access_class: Option<PersistedThreadAccessClass>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let preview_author_json = prepare_preview_author_json(thread_model.preview_author.as_ref())?;
    upsert_thread_with_actor_columns_prepared(
        db,
        thread_model,
        created_by_actor_kind,
        created_by_actor_id,
        access_class,
        preview_author_json,
        created_at,
        updated_at,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn upsert_thread_with_actor_columns_prepared<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    created_by_actor_kind: Option<String>,
    created_by_actor_id: Option<String>,
    access_class: Option<PersistedThreadAccessClass>,
    preview_author_json: Option<String>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let old_workspace = thread::Entity::find_by_id(&thread_model.id)
        .select_only()
        .column(thread::Column::WorkspaceId)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?;
    let mut conflict = OnConflict::column(thread::Column::Id);
    conflict.update_columns([
        thread::Column::WorkspaceId,
        thread::Column::Name,
        thread::Column::Preview,
        thread::Column::PreviewAuthorJson,
        thread::Column::Mode,
        thread::Column::Model,
        thread::Column::ModelProvider,
        thread::Column::Status,
        thread::Column::OriginKind,
        thread::Column::SidebarVisibility,
        thread::Column::AgentNickname,
        thread::Column::AgentRole,
        thread::Column::UpdatedAt,
    ]);
    if let Some(kind) = created_by_actor_kind.as_deref() {
        conflict.action_and_where(
            Expr::col((thread::Entity, thread::Column::CreatedByActorKind)).eq(kind),
        );
        let id = Expr::col((thread::Entity, thread::Column::CreatedByActorId));
        conflict.action_and_where(match created_by_actor_id.as_deref() {
            Some(actor) => id.eq(actor),
            None => id.is_null(),
        });
    }
    if let Some(access) = access_class {
        conflict.action_and_where(
            Expr::col((thread::Entity, thread::Column::AccessClass))
                .eq(persisted_thread_access_class_to_db(access)),
        );
    }
    let changed = thread::Entity::insert(thread::ActiveModel {
        workspace_unread_started_at: sea_orm::NotSet,
        id: Set(thread_model.id.clone()),
        workspace_id: Set(thread_model.workspace_id.clone()),
        name: Set(thread_model.name.clone()),
        preview: Set(thread_model.preview.clone()),
        preview_author_json: Set(preview_author_json),
        mode: Set(thread_mode_to_db(thread_model.mode).to_owned()),
        model: Set(thread_model.model.clone()),
        model_provider: Set(thread_model.model_provider.clone()),
        status: Set(thread_status_to_db(thread_model.status).to_owned()),
        origin_kind: Set(thread_origin_kind_to_db(thread_model.origin_kind).to_owned()),
        sidebar_visibility: Set(
            thread_sidebar_visibility_to_db(thread_model.sidebar_visibility).to_owned(),
        ),
        access_class: Set(
            persisted_thread_access_class_to_db(access_class.unwrap_or_else(|| {
                if matches!(
                    thread_model.origin_kind,
                    pioneer_protocol::ThreadOriginKind::TaskRun
                        | pioneer_protocol::ThreadOriginKind::System
                ) || matches!(
                    thread_model.sidebar_visibility,
                    pioneer_protocol::ThreadSidebarVisibility::Hidden
                ) {
                    PersistedThreadAccessClass::Internal
                } else {
                    PersistedThreadAccessClass::Private
                }
            }))
            .to_owned(),
        ),
        agent_nickname: Set(thread_model.agent_nickname.clone()),
        agent_role: Set(thread_model.agent_role.clone()),
        created_by_actor_id: Set(created_by_actor_id),
        created_by_actor_kind: Set(created_by_actor_kind),
        summary: Set(None),
        summary_turn_count: Set(None),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
        ..Default::default()
    })
    .on_conflict(conflict)
    .exec_without_returning(db)
    .await
    .context("failed to upsert thread")?;
    anyhow::ensure!(
        changed != 0,
        "thread id was concurrently bound to different access facts"
    );
    Ok(old_workspace.as_ref() != Some(&thread_model.workspace_id))
}

pub async fn upsert_projected_thread<C: ConnectionTrait>(
    db: &C,
    thread_model: &Thread,
    creator: Option<&PersistedActorRef>,
    preview_author_json: Option<String>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if let Some(creator) = creator
        && let Some(existing) = find_thread_by_id(db, thread_model.id.as_str()).await?
    {
        let existing_creator = actor_ref_from_db(
            existing.created_by_actor_kind.as_deref(),
            existing.created_by_actor_id.as_deref(),
        )
        .with_context(|| {
            format!(
                "thread `{}` has an invalid persisted creator pair",
                thread_model.id
            )
        })?
        .with_context(|| {
            format!(
                "thread `{}` is missing its persisted creator",
                thread_model.id
            )
        })?;
        if &existing_creator != creator {
            anyhow::bail!(
                "thread `{}` has a different persisted creator",
                thread_model.id
            );
        }
    }
    let (actor_kind, actor_id) = creator.map(actor_ref_to_db).unwrap_or((None, None));
    upsert_thread_with_actor_columns_prepared(
        db,
        thread_model,
        actor_kind,
        actor_id,
        None,
        preview_author_json,
        created_at,
        updated_at,
    )
    .await
}

/// Applies the public thread-management surface to one exact user thread.
///
/// Member callers pass their principal as `expected_creator`; Superuser passes
/// `None`. Membership rows are deliberately untouched so a
/// private→workspace→private roundtrip restores the same explicit audience.
/// Change receipt derived from the old row inside the same writer transaction.
/// A rename/archive with an unchanged visibility must not invalidate authorization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThreadManagementChange {
    pub changed: bool,
    pub access_changed: bool,
}

pub async fn update_user_thread_management(
    db: &DatabaseTransaction,
    workspace_id: &str,
    thread_id: &str,
    expected_creator: Option<&PrincipalId>,
    name: Option<&str>,
    access_class: Option<PersistedThreadAccessClass>,
    archived: Option<bool>,
    updated_at: DateTimeWithTimeZone,
    read_completion_sequence: impl Future<Output = Result<i64>> + Send,
) -> Result<Option<ThreadManagementChange>> {
    let Some(model) = thread::Entity::find_by_id(thread_id.to_owned())
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .lock_exclusive()
        .one(db)
        .await
        .context("failed to load exact thread for management")?
    else {
        return Ok(None);
    };
    let current_access = persisted_thread_access_class_from_db(model.access_class.as_str())
        .context("managed thread has an invalid access class")?;
    if current_access == PersistedThreadAccessClass::Internal
        || matches!(
            model.origin_kind.as_str(),
            "task_run" | "taskRun" | "system"
        )
        || model.sidebar_visibility != "visible"
    {
        anyhow::bail!("internal threads are not mutable through the user management path");
    }
    if access_class == Some(PersistedThreadAccessClass::Internal) {
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
    // Metadata timestamps can be ahead of this operation. Access begins at
    // the actual management operation, not that unrelated monotonic maximum.
    let access_started_at = updated_at;
    let updated_at = std::cmp::max(model.updated_at, updated_at);
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
        if access_class == PersistedThreadAccessClass::Workspace {
            active.inbox_workspace_after_sequence = Set(Some(read_completion_sequence.await?));
            active.inbox_workspace_started_at = Set(Some(access_started_at));
        }
        active.access_class = Set(persisted_thread_access_class_to_db(access_class).to_owned());
        if access_class == PersistedThreadAccessClass::Workspace {
            active.workspace_unread_started_at = Set(updated_at.timestamp());
        }
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
    Ok(Some(ThreadManagementChange {
        changed,
        access_changed: access_class.is_some_and(|next| next != current_access),
    }))
}
