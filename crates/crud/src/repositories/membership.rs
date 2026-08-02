use anyhow::{Context, Result, bail};
use pioneer_entity::{
    gateway_principal, thread, thread_membership, workspace, workspace_membership,
};
use pioneer_protocol::{
    GatewayId, PersistedActorRef, PrincipalId, PrincipalKind, PrincipalStatus, RoleKey,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{OnConflict, Query, SelectStatement};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

use super::identity::{
    GatewayPrincipalRecord, actor_ref_to_db, gateway_principal_record_from_model,
    principal_kind_from_db, principal_kind_to_db, principal_status_from_db,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PersistedThreadAccessClass {
    Private,
    Workspace,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivateThreadParticipantMutation {
    Applied {
        changed: bool,
        participant_ids: Vec<PrincipalId>,
    },
    TargetUnavailable,
    MandatoryCreator,
}

pub fn persisted_thread_access_class_to_db(value: PersistedThreadAccessClass) -> &'static str {
    match value {
        PersistedThreadAccessClass::Private => "private",
        PersistedThreadAccessClass::Workspace => "workspace",
        PersistedThreadAccessClass::Internal => "internal",
    }
}

pub fn persisted_thread_access_class_from_db(value: &str) -> Result<PersistedThreadAccessClass> {
    match value {
        "private" => Ok(PersistedThreadAccessClass::Private),
        "workspace" => Ok(PersistedThreadAccessClass::Workspace),
        "internal" => Ok(PersistedThreadAccessClass::Internal),
        unknown => bail!("unknown persisted thread access class `{unknown}`"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewWorkspaceMembership {
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub workspace_id: String,
    pub granted_by: PersistedActorRef,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDirectoryCursor {
    pub nickname_key: String,
    pub principal_id: PrincipalId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDirectoryPage {
    pub principals: Vec<GatewayPrincipalRecord>,
    pub next_cursor: Option<MemberDirectoryCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMembershipDeletion {
    pub changed: bool,
    pub removed_private_thread_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalMembershipDeletion {
    pub workspace_ids: Vec<String>,
    pub private_thread_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewThreadMembership {
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub workspace_id: String,
    pub thread_id: String,
    pub added_by: PersistedActorRef,
    pub now: DateTimeWithTimeZone,
}

pub async fn find_workspace_membership<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    workspace_id: &str,
) -> Result<Option<workspace_membership::Model>> {
    workspace_membership::Entity::find_by_id((principal_id.to_string(), workspace_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load workspace membership for principal `{principal_id}` \
                 and workspace `{workspace_id}`"
            )
        })
}

pub async fn list_workspace_memberships_for_principal<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
) -> Result<Vec<workspace_membership::Model>> {
    workspace_membership::Entity::find()
        .filter(workspace_membership::Column::PrincipalId.eq(principal_id.to_string()))
        .order_by_asc(workspace_membership::Column::WorkspaceId)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list workspace memberships for principal `{principal_id}`")
        })
}

pub async fn list_workspace_memberships_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<workspace_membership::Model>> {
    workspace_membership::Entity::find()
        .filter(workspace_membership::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_asc(workspace_membership::Column::PrincipalId)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list workspace memberships for workspace `{workspace_id}`")
        })
}

/// Returns only active workspaces granted to the exact ordinary principal.
///
/// The membership predicate is part of the SQL query so callers never load an
/// unscoped workspace collection and filter it in application memory.
pub async fn list_active_workspaces_for_principal<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
) -> Result<Vec<workspace::Model>> {
    workspace::Entity::find()
        .filter(workspace::Column::IsActive.eq(true))
        .filter(workspace::Column::Id.in_subquery(workspace_membership_workspace_ids(principal_id)))
        .order_by_asc(workspace::Column::CreatedAt)
        .order_by_asc(workspace::Column::Id)
        .all(db)
        .await
        .with_context(|| format!("failed to list active workspaces for principal `{principal_id}`"))
}

/// Resolves an active workspace only when the exact ordinary principal has a
/// current persisted membership.
pub async fn find_active_workspace_for_principal<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    workspace_id: &str,
) -> Result<Option<workspace::Model>> {
    workspace::Entity::find_by_id(workspace_id.to_owned())
        .filter(workspace::Column::IsActive.eq(true))
        .filter(workspace::Column::Id.in_subquery(workspace_membership_workspace_ids(principal_id)))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to resolve an active workspace for principal `{principal_id}`")
        })
}

fn workspace_membership_workspace_ids(principal_id: &PrincipalId) -> SelectStatement {
    Query::select()
        .column(workspace_membership::Column::WorkspaceId)
        .from(workspace_membership::Entity)
        .and_where(workspace_membership::Column::PrincipalId.eq(principal_id.to_string()))
        .to_owned()
}

fn thread_membership_thread_ids(principal_id: &PrincipalId) -> SelectStatement {
    Query::select()
        .column(thread_membership::Column::ThreadId)
        .from(thread_membership::Entity)
        .and_where(thread_membership::Column::PrincipalId.eq(principal_id.to_string()))
        .to_owned()
}

/// Lists only ordinary visible threads reachable by the exact Member.
///
/// Workspace and thread membership predicates are applied in SQL before
/// ordering and limiting, so inaccessible rows cannot affect tree contents or
/// counts.
pub async fn list_accessible_threads_for_principal<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    workspace_id: &str,
    limit: u64,
) -> Result<Vec<thread::Model>> {
    // SeaORM's limit is a `u64`, while SQLite accepts only a signed 64-bit
    // integer. Callers use `u64::MAX` to request the complete authorized set;
    // clamp that sentinel centrally instead of emitting an invalid SQL LIMIT.
    let limit = limit.min(i64::MAX as u64);
    thread::Entity::find()
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread::Column::WorkspaceId
                .in_subquery(workspace_membership_workspace_ids(principal_id)),
        )
        .filter(thread::Column::SidebarVisibility.eq("visible"))
        .filter(
            Condition::any()
                .add(thread::Column::AccessClass.eq("workspace"))
                .add(
                    Condition::all()
                        .add(thread::Column::AccessClass.eq("private"))
                        .add(
                            thread::Column::Id
                                .in_subquery(thread_membership_thread_ids(principal_id)),
                        ),
                ),
        )
        .order_by_desc(thread::Column::UpdatedAt)
        .order_by_asc(thread::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list accessible threads for principal `{principal_id}` \
                 in workspace `{workspace_id}`"
            )
        })
}

/// Resolves one visible thread with the complete Member ACL encoded in SQL.
pub async fn find_accessible_thread_for_principal<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    workspace_id: &str,
    thread_id: &str,
) -> Result<Option<thread::Model>> {
    thread::Entity::find_by_id(thread_id.to_owned())
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread::Column::WorkspaceId
                .in_subquery(workspace_membership_workspace_ids(principal_id)),
        )
        .filter(thread::Column::SidebarVisibility.eq("visible"))
        .filter(
            Condition::any()
                .add(thread::Column::AccessClass.eq("workspace"))
                .add(
                    Condition::all()
                        .add(thread::Column::AccessClass.eq("private"))
                        .add(
                            thread::Column::Id
                                .in_subquery(thread_membership_thread_ids(principal_id)),
                        ),
                ),
        )
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to resolve accessible thread `{thread_id}` for principal \
                 `{principal_id}` in workspace `{workspace_id}`"
            )
        })
}

fn shared_workspace_principal_ids(viewer_principal_id: &PrincipalId) -> SelectStatement {
    Query::select()
        .column(workspace_membership::Column::PrincipalId)
        .from(workspace_membership::Entity)
        .and_where(
            workspace_membership::Column::WorkspaceId
                .in_subquery(active_workspace_membership_ids(viewer_principal_id)),
        )
        .to_owned()
}

fn active_workspace_membership_ids(viewer_principal_id: &PrincipalId) -> SelectStatement {
    let active_workspace_ids = Query::select()
        .column(workspace::Column::Id)
        .from(workspace::Entity)
        .and_where(workspace::Column::IsActive.eq(true))
        .to_owned();
    Query::select()
        .column(workspace_membership::Column::WorkspaceId)
        .from(workspace_membership::Entity)
        .and_where(workspace_membership::Column::PrincipalId.eq(viewer_principal_id.to_string()))
        .and_where(workspace_membership::Column::WorkspaceId.in_subquery(active_workspace_ids))
        .to_owned()
}

/// Resolves a directory profile only when it is eligible for the Member's
/// shared-workspace directory.
pub async fn find_shared_workspace_principal_for_principal<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    viewer_principal_id: &PrincipalId,
    target_principal_id: &PrincipalId,
) -> Result<Option<GatewayPrincipalRecord>> {
    gateway_principal::Entity::find_by_id(target_principal_id.to_string())
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(gateway_principal::Column::Kind.eq(principal_kind_to_db(PrincipalKind::User)))
        .filter(gateway_principal::Column::RoleKey.eq(pioneer_protocol::MEMBER_ROLE_KEY))
        .filter(gateway_principal::Column::Status.is_in(["active", "suspended"]))
        .filter(
            gateway_principal::Column::Id
                .in_subquery(shared_workspace_principal_ids(viewer_principal_id)),
        )
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to resolve shared-workspace directory profile \
                 `{target_principal_id}` for principal `{viewer_principal_id}`"
            )
        })?
        .map(gateway_principal_record_from_model)
        .transpose()
}

pub async fn list_member_directory_page<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    viewer_principal_id: &PrincipalId,
    viewer_kind: PrincipalKind,
    cursor: Option<&MemberDirectoryCursor>,
    limit: u64,
) -> Result<MemberDirectoryPage> {
    if limit == 0 {
        bail!("member directory page limit must be positive");
    }
    let mut query = gateway_principal::Entity::find()
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()));
    if viewer_kind == PrincipalKind::User {
        query = query.filter(
            Condition::any()
                .add(gateway_principal::Column::Id.eq(viewer_principal_id.to_string()))
                .add(
                    Condition::all()
                        .add(
                            gateway_principal::Column::Kind
                                .eq(principal_kind_to_db(PrincipalKind::Superuser)),
                        )
                        .add(gateway_principal::Column::Status.eq("active")),
                )
                .add(
                    Condition::all()
                        .add(
                            gateway_principal::Column::Kind
                                .eq(principal_kind_to_db(PrincipalKind::User)),
                        )
                        .add(
                            gateway_principal::Column::RoleKey
                                .eq(pioneer_protocol::MEMBER_ROLE_KEY),
                        )
                        .add(gateway_principal::Column::Status.is_in(["active", "suspended"]))
                        .add(
                            gateway_principal::Column::Id
                                .in_subquery(shared_workspace_principal_ids(viewer_principal_id)),
                        ),
                ),
        );
    }
    if let Some(cursor) = cursor {
        query = query.filter(
            Condition::any()
                .add(gateway_principal::Column::NicknameKey.gt(cursor.nickname_key.clone()))
                .add(
                    Condition::all()
                        .add(gateway_principal::Column::NicknameKey.eq(cursor.nickname_key.clone()))
                        .add(gateway_principal::Column::Id.gt(cursor.principal_id.to_string())),
                ),
        );
    }
    let fetch_limit = limit
        .checked_add(1)
        .context("member directory page limit overflow")?;
    let mut rows = query
        .order_by_asc(gateway_principal::Column::NicknameKey)
        .order_by_asc(gateway_principal::Column::Id)
        .limit(fetch_limit)
        .all(db)
        .await
        .context("failed to list ACL-scoped member directory page")?;
    let has_more = rows.len() as u64 > limit;
    if has_more {
        rows.pop();
    }
    let next_cursor = has_more
        .then(|| rows.last())
        .flatten()
        .map(|row| {
            Ok::<_, anyhow::Error>(MemberDirectoryCursor {
                nickname_key: row.nickname_key.clone(),
                principal_id: PrincipalId::new(row.id.clone())
                    .context("persisted member directory principal id is invalid")?,
            })
        })
        .transpose()?;
    Ok(MemberDirectoryPage {
        principals: rows
            .into_iter()
            .map(gateway_principal_record_from_model)
            .collect::<Result<Vec<_>>>()?,
        next_cursor,
    })
}

pub async fn list_workspace_member_principals_page<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    workspace_id: &str,
    cursor: Option<&MemberDirectoryCursor>,
    limit: u64,
) -> Result<MemberDirectoryPage> {
    if limit == 0 {
        bail!("workspace member page limit must be positive");
    }
    let member_ids = Query::select()
        .column(workspace_membership::Column::PrincipalId)
        .from(workspace_membership::Entity)
        .and_where(workspace_membership::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .to_owned();
    let mut query = gateway_principal::Entity::find()
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(gateway_principal::Column::Kind.eq(principal_kind_to_db(PrincipalKind::User)))
        .filter(gateway_principal::Column::RoleKey.eq(pioneer_protocol::MEMBER_ROLE_KEY))
        .filter(gateway_principal::Column::Status.is_in(["active", "suspended"]))
        .filter(gateway_principal::Column::Id.in_subquery(member_ids));
    if let Some(cursor) = cursor {
        query = query.filter(
            Condition::any()
                .add(gateway_principal::Column::NicknameKey.gt(cursor.nickname_key.clone()))
                .add(
                    Condition::all()
                        .add(gateway_principal::Column::NicknameKey.eq(cursor.nickname_key.clone()))
                        .add(gateway_principal::Column::Id.gt(cursor.principal_id.to_string())),
                ),
        );
    }
    let fetch_limit = limit
        .checked_add(1)
        .context("workspace member page limit overflow")?;
    let mut rows = query
        .order_by_asc(gateway_principal::Column::NicknameKey)
        .order_by_asc(gateway_principal::Column::Id)
        .limit(fetch_limit)
        .all(db)
        .await
        .context("failed to list explicit workspace member page")?;
    let has_more = rows.len() as u64 > limit;
    if has_more {
        rows.pop();
    }
    let next_cursor = has_more
        .then(|| rows.last())
        .flatten()
        .map(|row| {
            Ok::<_, anyhow::Error>(MemberDirectoryCursor {
                nickname_key: row.nickname_key.clone(),
                principal_id: PrincipalId::new(row.id.clone())
                    .context("persisted workspace member principal id is invalid")?,
            })
        })
        .transpose()?;
    Ok(MemberDirectoryPage {
        principals: rows
            .into_iter()
            .map(gateway_principal_record_from_model)
            .collect::<Result<Vec<_>>>()?,
        next_cursor,
    })
}

pub async fn insert_workspace_membership(
    db: &DatabaseTransaction,
    input: &NewWorkspaceMembership,
) -> Result<workspace_membership::Model> {
    validate_scope_id("workspace", input.workspace_id.as_str())?;
    validate_member_principal(db, &input.gateway_id, &input.principal_id).await?;
    validate_actor(db, &input.gateway_id, &input.granted_by).await?;
    ensure_workspace_exists(db, input.workspace_id.as_str()).await?;
    let (actor_kind, actor_id) = actor_ref_to_db(&input.granted_by);

    workspace_membership::Entity::insert(workspace_membership::ActiveModel {
        principal_id: Set(input.principal_id.to_string()),
        workspace_id: Set(input.workspace_id.clone()),
        granted_by_actor_kind: Set(actor_kind.context("membership actor kind must be present")?),
        granted_by_actor_id: Set(actor_id),
        created_at: Set(input.now),
        updated_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::columns([
            workspace_membership::Column::PrincipalId,
            workspace_membership::Column::WorkspaceId,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert workspace membership for principal `{}` and workspace `{}`",
            input.principal_id, input.workspace_id
        )
    })?;

    find_workspace_membership(db, &input.principal_id, input.workspace_id.as_str())
        .await?
        .context("workspace membership disappeared after idempotent insert")
}

pub async fn delete_workspace_membership(
    db: &DatabaseTransaction,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
    workspace_id: &str,
) -> Result<WorkspaceMembershipDeletion> {
    validate_scope_id("workspace", workspace_id)?;
    validate_principal_gateway(db, gateway_id, principal_id).await?;
    if find_workspace_membership(db, principal_id, workspace_id)
        .await?
        .is_none()
    {
        return Ok(WorkspaceMembershipDeletion {
            changed: false,
            removed_private_thread_ids: Vec::new(),
        });
    }
    let workspace_thread_ids = thread::Entity::find()
        .select_only()
        .column(thread::Column::Id)
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to load workspace threads before membership removal")?;
    let removed_private_thread_ids = if workspace_thread_ids.is_empty() {
        Vec::new()
    } else {
        let thread_ids = thread_membership::Entity::find()
            .select_only()
            .column(thread_membership::Column::ThreadId)
            .filter(thread_membership::Column::PrincipalId.eq(principal_id.to_string()))
            .filter(thread_membership::Column::ThreadId.is_in(workspace_thread_ids))
            .order_by_asc(thread_membership::Column::ThreadId)
            .into_tuple::<String>()
            .all(db)
            .await
            .context("failed to capture dependent private-thread memberships")?;
        if !thread_ids.is_empty() {
            thread_membership::Entity::delete_many()
                .filter(thread_membership::Column::PrincipalId.eq(principal_id.to_string()))
                .filter(thread_membership::Column::ThreadId.is_in(thread_ids.clone()))
                .exec(db)
                .await
                .context("failed to remove dependent private-thread memberships")?;
        }
        thread_ids
    };
    let result = workspace_membership::Entity::delete_by_id((
        principal_id.to_string(),
        workspace_id.to_owned(),
    ))
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to delete workspace membership for principal `{principal_id}` \
             and workspace `{workspace_id}`"
        )
    })?;
    Ok(WorkspaceMembershipDeletion {
        changed: result.rows_affected == 1,
        removed_private_thread_ids,
    })
}

pub async fn delete_all_memberships_for_principal(
    db: &DatabaseTransaction,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
) -> Result<PrincipalMembershipDeletion> {
    validate_principal_gateway(db, gateway_id, principal_id).await?;
    let memberships = list_workspace_memberships_for_principal(db, principal_id).await?;
    let mut workspace_ids = Vec::with_capacity(memberships.len());
    let mut private_thread_ids = Vec::new();
    for membership in memberships {
        let deletion = delete_workspace_membership(
            db,
            gateway_id,
            principal_id,
            membership.workspace_id.as_str(),
        )
        .await?;
        if deletion.changed {
            workspace_ids.push(membership.workspace_id);
            private_thread_ids.extend(deletion.removed_private_thread_ids);
        }
    }
    let dangling_thread_ids = thread_membership::Entity::find()
        .select_only()
        .column(thread_membership::Column::ThreadId)
        .filter(thread_membership::Column::PrincipalId.eq(principal_id.to_string()))
        .order_by_asc(thread_membership::Column::ThreadId)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to capture remaining principal thread memberships")?;
    if !dangling_thread_ids.is_empty() {
        thread_membership::Entity::delete_many()
            .filter(thread_membership::Column::PrincipalId.eq(principal_id.to_string()))
            .exec(db)
            .await
            .context("failed to remove remaining principal thread memberships")?;
        private_thread_ids.extend(dangling_thread_ids);
    }
    workspace_ids.sort();
    workspace_ids.dedup();
    private_thread_ids.sort();
    private_thread_ids.dedup();
    Ok(PrincipalMembershipDeletion {
        workspace_ids,
        private_thread_ids,
    })
}

pub async fn find_thread_membership<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    principal_id: &PrincipalId,
) -> Result<Option<thread_membership::Model>> {
    thread_membership::Entity::find_by_id((thread_id.to_owned(), principal_id.to_string()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load thread membership for thread `{thread_id}` \
                 and principal `{principal_id}`"
            )
        })
}

pub async fn list_thread_memberships_for_principal<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
) -> Result<Vec<thread_membership::Model>> {
    thread_membership::Entity::find()
        .filter(thread_membership::Column::PrincipalId.eq(principal_id.to_string()))
        .order_by_asc(thread_membership::Column::ThreadId)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list thread memberships for principal `{principal_id}`")
        })
}

pub async fn list_thread_memberships_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Vec<thread_membership::Model>> {
    thread_membership::Entity::find()
        .filter(thread_membership::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(thread_membership::Column::PrincipalId)
        .all(db)
        .await
        .with_context(|| format!("failed to list thread memberships for thread `{thread_id}`"))
}

pub async fn insert_thread_membership(
    db: &DatabaseTransaction,
    input: &NewThreadMembership,
) -> Result<thread_membership::Model> {
    validate_scope_id("workspace", input.workspace_id.as_str())?;
    validate_scope_id("thread", input.thread_id.as_str())?;
    validate_member_principal(db, &input.gateway_id, &input.principal_id).await?;
    validate_actor(db, &input.gateway_id, &input.added_by).await?;
    validate_private_thread_scope(db, input.workspace_id.as_str(), input.thread_id.as_str())
        .await?;
    if find_workspace_membership(db, &input.principal_id, input.workspace_id.as_str())
        .await?
        .is_none()
    {
        bail!(
            "cannot add principal `{}` to thread `{}` without workspace `{}` membership",
            input.principal_id,
            input.thread_id,
            input.workspace_id
        );
    }
    let (actor_kind, actor_id) = actor_ref_to_db(&input.added_by);

    thread_membership::Entity::insert(thread_membership::ActiveModel {
        thread_id: Set(input.thread_id.clone()),
        principal_id: Set(input.principal_id.to_string()),
        added_by_actor_kind: Set(actor_kind.context("membership actor kind must be present")?),
        added_by_actor_id: Set(actor_id),
        created_at: Set(input.now),
        updated_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::columns([
            thread_membership::Column::ThreadId,
            thread_membership::Column::PrincipalId,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert thread membership for thread `{}` and principal `{}`",
            input.thread_id, input.principal_id
        )
    })?;

    find_thread_membership(db, input.thread_id.as_str(), &input.principal_id)
        .await?
        .context("thread membership disappeared after idempotent insert")
}

pub async fn delete_thread_membership(
    db: &DatabaseTransaction,
    gateway_id: &GatewayId,
    workspace_id: &str,
    thread_id: &str,
    principal_id: &PrincipalId,
) -> Result<bool> {
    validate_scope_id("workspace", workspace_id)?;
    validate_scope_id("thread", thread_id)?;
    validate_persisted_member_principal(db, gateway_id, principal_id).await?;
    validate_private_thread_scope(db, workspace_id, thread_id).await?;
    if find_workspace_membership(db, principal_id, workspace_id)
        .await?
        .is_none()
    {
        bail!(
            "cannot remove principal `{principal_id}` from thread `{thread_id}` \
             without its parent workspace membership"
        );
    }
    let result =
        thread_membership::Entity::delete_by_id((thread_id.to_owned(), principal_id.to_string()))
            .exec(db)
            .await
            .with_context(|| {
                format!(
                    "failed to delete thread membership for thread `{thread_id}` \
                     and principal `{principal_id}`"
                )
            })?;
    Ok(result.rows_affected == 1)
}

async fn validate_member_principal<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
) -> Result<()> {
    let model = validate_principal_gateway(db, gateway_id, principal_id).await?;
    let role = model
        .role_key
        .as_deref()
        .map(RoleKey::new)
        .transpose()
        .context("membership target has an invalid persisted role key")?;
    if principal_kind_from_db(model.kind.as_str())? != PrincipalKind::User
        || !role.as_ref().is_some_and(RoleKey::is_supported)
        || principal_status_from_db(model.status.as_str())? != PrincipalStatus::Active
    {
        bail!("membership target is not an eligible ordinary Member principal");
    }
    Ok(())
}

async fn validate_persisted_member_principal<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
) -> Result<()> {
    let model = validate_principal_gateway(db, gateway_id, principal_id).await?;
    let role = model
        .role_key
        .as_deref()
        .map(RoleKey::new)
        .transpose()
        .context("membership target has an invalid persisted role key")?;
    let status = principal_status_from_db(model.status.as_str())?;
    if principal_kind_from_db(model.kind.as_str())? != PrincipalKind::User
        || !role.as_ref().is_some_and(RoleKey::is_supported)
        || !matches!(status, PrincipalStatus::Active | PrincipalStatus::Suspended)
    {
        bail!("persisted membership target is not an ordinary Member principal");
    }
    Ok(())
}

pub(crate) async fn private_thread_participant_target_is_eligible<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    workspace_id: &str,
    principal_id: &PrincipalId,
    allow_suspended: bool,
) -> Result<bool> {
    let Some(model) = gateway_principal::Entity::find_by_id(principal_id.to_string())
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .one(db)
        .await
        .context("failed to resolve private-thread participant target")?
    else {
        return Ok(false);
    };
    let role = model
        .role_key
        .as_deref()
        .map(RoleKey::new)
        .transpose()
        .context("private-thread participant target has an invalid persisted role key")?;
    let status = principal_status_from_db(model.status.as_str())
        .context("private-thread participant target has an invalid persisted status")?;
    let status_is_eligible = status == PrincipalStatus::Active
        || (allow_suspended && status == PrincipalStatus::Suspended);
    if principal_kind_from_db(model.kind.as_str())? != PrincipalKind::User
        || !role.as_ref().is_some_and(RoleKey::is_supported)
        || !status_is_eligible
    {
        return Ok(false);
    }

    Ok(find_workspace_membership(db, principal_id, workspace_id)
        .await?
        .is_some())
}

async fn validate_principal_gateway<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
) -> Result<gateway_principal::Model> {
    gateway_principal::Entity::find_by_id(principal_id.to_string())
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .one(db)
        .await
        .context("failed to validate membership principal scope")?
        .context("membership principal does not belong to the requested Gateway")
}

async fn validate_actor<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    actor: &PersistedActorRef,
) -> Result<()> {
    let PersistedActorRef::Principal(actor_id) = actor else {
        return Ok(());
    };
    let actor = validate_principal_gateway(db, gateway_id, actor_id).await?;
    if principal_status_from_db(actor.status.as_str())? != PrincipalStatus::Active {
        bail!("membership actor principal must be active");
    }
    Ok(())
}

async fn ensure_workspace_exists<C: ConnectionTrait>(db: &C, workspace_id: &str) -> Result<()> {
    if workspace::Entity::find_by_id(workspace_id.to_owned())
        .one(db)
        .await
        .context("failed to validate membership workspace")?
        .is_none()
    {
        bail!("membership workspace does not exist");
    }
    Ok(())
}

async fn validate_private_thread_scope<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<()> {
    let model = thread::Entity::find_by_id(thread_id.to_owned())
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .one(db)
        .await
        .context("failed to validate membership thread scope")?
        .context("membership thread does not belong to the requested workspace")?;
    if persisted_thread_access_class_from_db(model.access_class.as_str())?
        != PersistedThreadAccessClass::Private
    {
        bail!("explicit thread membership is valid only for a private thread");
    }
    Ok(())
}

fn validate_scope_id(kind: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() != 21
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        bail!("{kind} id must be a 21-character ASCII alphanumeric value");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_entity::{gateway_principal, thread, turn_event, workspace};
    use pioneer_protocol::{
        AUTH_DOMAIN_ID_LEN, GATEWAY_ID_LEN, GatewayId, PRINCIPAL_ID_LEN, PersistedActorRef,
        PrincipalId, SandboxMode, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, ThreadVisibility, Turn, TurnPermissionAuditEvent,
        TurnPermissionAuditEventKind, TurnStatus, generate_id,
    };
    use sea_orm::{ActiveModelTrait, Database, DatabaseConnection, Set, TransactionTrait};

    struct Fixture {
        database: DatabaseConnection,
        gateway_id: GatewayId,
        superuser_id: PrincipalId,
        member_id: PrincipalId,
        red_workspace_id: String,
        blue_workspace_id: String,
        private_thread_id: String,
    }

    async fn fixture() -> Fixture {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("connect isolated membership database");
        Migrator::up(&database, None)
            .await
            .expect("migrate isolated membership database");
        let now = chrono::Utc::now().fixed_offset();
        let gateway_id = GatewayId::new(generate_id(GATEWAY_ID_LEN)).expect("gateway id");
        let superuser_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("superuser id");
        let member_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("member id");
        super::super::identity::create_gateway_singleton(&database, &gateway_id, 1, now)
            .await
            .expect("create gateway");
        super::super::identity::create_superuser(
            &database,
            &superuser_id,
            &gateway_id,
            "Superuser",
            "superuser",
            "superuser",
            now,
        )
        .await
        .expect("create superuser");
        gateway_principal::ActiveModel {
            id: Set(member_id.to_string()),
            gateway_id: Set(gateway_id.to_string()),
            kind: Set("user".to_owned()),
            role_key: Set(Some("member".to_owned())),
            status: Set("active".to_owned()),
            display_name: Set("Member".to_owned()),
            nickname: Set("member".to_owned()),
            nickname_key: Set("member".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            removed_at: Set(None),
            authorization_guard: Set(1),
        }
        .insert(&database)
        .await
        .expect("create test-only Member");

        let red_workspace_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let blue_workspace_id = generate_id(AUTH_DOMAIN_ID_LEN);
        for (id, name) in [
            (red_workspace_id.as_str(), "Red"),
            (blue_workspace_id.as_str(), "Blue"),
        ] {
            workspace::ActiveModel {
                id: Set(id.to_owned()),
                name: Set(name.to_owned()),
                is_active: Set(true),
                is_current: Set(false),
                created_at: Set(now),
                updated_at: Set(now),
            }
            .insert(&database)
            .await
            .expect("create workspace");
        }

        let private_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        thread::ActiveModel {
            id: Set(private_thread_id.clone()),
            workspace_id: Set(red_workspace_id.clone()),
            name: Set(Some("Private".to_owned())),
            preview: Set(String::new()),
            mode: Set("chat".to_owned()),
            model: Set("test".to_owned()),
            model_provider: Set("test".to_owned()),
            status: Set("active".to_owned()),
            origin_kind: Set("user".to_owned()),
            sidebar_visibility: Set("visible".to_owned()),
            access_class: Set("private".to_owned()),
            agent_nickname: Set(None),
            agent_role: Set(None),
            summary: Set(None),
            summary_turn_count: Set(Some(0)),
            created_at: Set(now),
            updated_at: Set(now),
            created_by_actor_id: Set(Some(superuser_id.to_string())),
            created_by_actor_kind: Set(Some("principal".to_owned())),
        }
        .insert(&database)
        .await
        .expect("create private thread");

        Fixture {
            database,
            gateway_id,
            superuser_id,
            member_id,
            red_workspace_id,
            blue_workspace_id,
            private_thread_id,
        }
    }

    #[tokio::test]
    async fn composite_membership_inserts_are_idempotent_and_exact() {
        let fixture = fixture().await;
        let transaction = fixture.database.begin().await.expect("begin");
        let now = chrono::Utc::now().fixed_offset();
        let workspace_grant = NewWorkspaceMembership {
            gateway_id: fixture.gateway_id.clone(),
            principal_id: fixture.member_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
            now,
        };
        let first = insert_workspace_membership(&transaction, &workspace_grant)
            .await
            .expect("first workspace grant");
        let duplicate = insert_workspace_membership(&transaction, &workspace_grant)
            .await
            .expect("idempotent workspace grant");
        assert_eq!(first, duplicate);

        let thread_grant = NewThreadMembership {
            gateway_id: fixture.gateway_id.clone(),
            principal_id: fixture.member_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            thread_id: fixture.private_thread_id.clone(),
            added_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
            now,
        };
        let first = insert_thread_membership(&transaction, &thread_grant)
            .await
            .expect("first thread grant");
        let duplicate = insert_thread_membership(&transaction, &thread_grant)
            .await
            .expect("idempotent thread grant");
        assert_eq!(first, duplicate);
        assert_eq!(
            list_workspace_memberships_for_principal(&transaction, &fixture.member_id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            list_thread_memberships_for_principal(&transaction, &fixture.member_id)
                .await
                .unwrap()
                .len(),
            1
        );
        transaction.commit().await.expect("commit");
    }

    #[tokio::test]
    async fn active_workspace_queries_apply_membership_in_sql() {
        let fixture = fixture().await;
        let transaction = fixture.database.begin().await.expect("begin");
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::System,
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .expect("grant red workspace");
        transaction.commit().await.expect("commit");

        let visible = list_active_workspaces_for_principal(&fixture.database, &fixture.member_id)
            .await
            .expect("list exact workspace grants");
        assert_eq!(
            visible
                .iter()
                .map(|workspace| workspace.id.as_str())
                .collect::<Vec<_>>(),
            vec![fixture.red_workspace_id.as_str()]
        );
        assert!(
            find_active_workspace_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.red_workspace_id.as_str(),
            )
            .await
            .expect("resolve granted workspace")
            .is_some()
        );
        assert!(
            find_active_workspace_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.blue_workspace_id.as_str(),
            )
            .await
            .expect("resolve ungranted workspace")
            .is_none()
        );

        let mut red: workspace::ActiveModel =
            workspace::Entity::find_by_id(fixture.red_workspace_id.clone())
                .one(&fixture.database)
                .await
                .expect("query red workspace")
                .expect("red workspace exists")
                .into();
        red.is_active = Set(false);
        red.update(&fixture.database)
            .await
            .expect("deactivate red workspace");
        assert!(
            list_active_workspaces_for_principal(&fixture.database, &fixture.member_id)
                .await
                .expect("list after deactivation")
                .is_empty(),
            "inactive memberships must be filtered by the authoritative SQL query"
        );
    }

    #[tokio::test]
    async fn complete_thread_acl_query_accepts_the_unbounded_caller_sentinel() {
        let fixture = fixture().await;
        let transaction = fixture.database.begin().await.expect("begin");
        let now = chrono::Utc::now().fixed_offset();
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .expect("grant workspace");
        insert_thread_membership(
            &transaction,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: fixture.private_thread_id.clone(),
                added_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .expect("grant private thread");
        transaction.commit().await.expect("commit grants");

        let threads = list_accessible_threads_for_principal(
            &fixture.database,
            &fixture.member_id,
            fixture.red_workspace_id.as_str(),
            u64::MAX,
        )
        .await
        .expect("unbounded authorized thread list must remain valid SQLite");
        assert_eq!(
            threads
                .into_iter()
                .map(|thread| thread.id)
                .collect::<Vec<_>>(),
            vec![fixture.private_thread_id]
        );
    }

    #[tokio::test]
    async fn thread_membership_rejects_cross_workspace_and_missing_parent_grant() {
        let fixture = fixture().await;
        let transaction = fixture.database.begin().await.expect("begin");
        let now = chrono::Utc::now().fixed_offset();
        let input = NewThreadMembership {
            gateway_id: fixture.gateway_id.clone(),
            principal_id: fixture.member_id.clone(),
            workspace_id: fixture.blue_workspace_id.clone(),
            thread_id: fixture.private_thread_id.clone(),
            added_by: PersistedActorRef::System,
            now,
        };
        assert!(
            insert_thread_membership(&transaction, &input)
                .await
                .is_err()
        );

        let input = NewThreadMembership {
            workspace_id: fixture.red_workspace_id.clone(),
            ..input
        };
        assert!(
            insert_thread_membership(&transaction, &input)
                .await
                .is_err()
        );
        transaction.rollback().await.expect("rollback");
    }

    #[tokio::test]
    async fn member_thread_creation_is_private_and_rolls_back_without_parent_membership() {
        let fixture = fixture().await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let timestamp = chrono::Utc::now().timestamp();
        let new_thread = Thread {
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            name: Some("Atomic private thread".to_owned()),
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let creator = PersistedActorRef::Principal(fixture.member_id.clone());

        store
            .create_member_private_thread(
                &new_thread,
                &fixture.gateway_id,
                &fixture.member_id,
                creator.clone(),
            )
            .await
            .expect_err("missing workspace membership must abort the whole transaction");
        assert!(
            thread::Entity::find_by_id(thread_id.clone())
                .one(&fixture.database)
                .await
                .expect("query rolled-back thread")
                .is_none(),
            "failed creator membership must leave no partial thread"
        );

        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin membership grant");
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .expect("grant parent workspace");
        transaction.commit().await.expect("commit membership grant");

        store
            .create_member_private_thread(
                &new_thread,
                &fixture.gateway_id,
                &fixture.member_id,
                creator,
            )
            .await
            .expect("authorized member thread transaction");
        let persisted = thread::Entity::find_by_id(thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query committed thread")
            .expect("thread committed");
        assert_eq!(persisted.access_class, "private");
        assert!(
            find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                .await
                .expect("query creator membership")
                .is_some(),
            "private creator membership must commit with the thread"
        );
    }

    #[tokio::test]
    async fn member_first_turn_creates_thread_membership_and_events_in_one_transaction() {
        let fixture = fixture().await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let timestamp = chrono::Utc::now().timestamp();
        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let turn_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let thread = Thread {
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            name: None,
            preview: "first message".to_owned(),
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: Some(ThreadVisibility::Private),
            turns: Vec::new(),
        };
        let permission_profile = pioneer_protocol::default_turn_permission_profile_snapshot();
        let turn = Turn {
            id: turn_id.clone(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: permission_profile.clone(),
        };
        let audit = TurnPermissionAuditEvent {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            event_kind: TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: permission_profile.mode,
            profile_source: permission_profile.source,
            security_snapshot_id: None,
            security_snapshot_version: None,
            security_reason_code: None,
            security_capability: None,
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        };
        let actor = PersistedActorRef::Principal(fixture.member_id.clone());

        store
            .materialize_new_member_turn_start_with_reasoning_effort_and_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                None,
                actor.clone(),
                audit.clone(),
                &fixture.gateway_id,
                &fixture.member_id,
            )
            .await
            .expect_err("missing workspace membership must roll back the initial turn");
        assert!(
            thread::Entity::find_by_id(thread_id.clone())
                .one(&fixture.database)
                .await
                .expect("query rolled-back initial thread")
                .is_none()
        );
        assert!(
            crate::find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                .await
                .expect("query rolled-back creator membership")
                .is_none()
        );
        assert!(
            turn_event::Entity::find()
                .filter(turn_event::Column::TurnId.eq(turn_id.clone()))
                .all(&fixture.database)
                .await
                .expect("query rolled-back turn events")
                .is_empty()
        );

        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin workspace membership grant");
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .expect("grant creator workspace membership");
        transaction
            .commit()
            .await
            .expect("commit workspace membership grant");

        store
            .materialize_new_member_turn_start_with_reasoning_effort_and_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                None,
                actor,
                audit,
                &fixture.gateway_id,
                &fixture.member_id,
            )
            .await
            .expect("first turn should atomically materialize the private thread");

        let persisted = thread::Entity::find_by_id(thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query materialized thread")
            .expect("materialized thread should exist");
        assert_eq!(persisted.access_class, "private");
        assert!(
            crate::find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                .await
                .expect("query committed creator membership")
                .is_some()
        );
        assert!(
            store
                .get_turn(&thread_id, &turn_id)
                .await
                .expect("query committed first turn")
                .is_some()
        );
        assert_eq!(
            turn_event::Entity::find()
                .filter(turn_event::Column::TurnId.eq(turn_id))
                .all(&fixture.database)
                .await
                .expect("query committed first-turn events")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn public_thread_creation_cannot_select_internal_access() {
        let fixture = fixture().await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let timestamp = chrono::Utc::now().timestamp();
        let new_thread = Thread {
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id,
            name: None,
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };

        store
            .create_superuser_thread(
                &new_thread,
                PersistedActorRef::Principal(fixture.superuser_id),
                PersistedThreadAccessClass::Internal,
            )
            .await
            .expect_err("user thread path must reject internal access");
        assert!(
            thread::Entity::find_by_id(thread_id)
                .one(&fixture.database)
                .await
                .expect("query rejected thread")
                .is_none()
        );
    }

    #[tokio::test]
    async fn thread_start_scope_distinguishes_missing_existing_and_parent_mismatch() {
        let fixture = fixture().await;
        assert_eq!(
            crate::resolve_thread_start_authorization_scope(
                &fixture.database,
                "missing-thread",
                fixture.red_workspace_id.as_str(),
            )
            .await
            .expect("resolve missing thread"),
            crate::ThreadStartAuthorizationScope::Missing
        );
        assert_eq!(
            crate::resolve_thread_start_authorization_scope(
                &fixture.database,
                fixture.private_thread_id.as_str(),
                fixture.red_workspace_id.as_str(),
            )
            .await
            .expect("resolve exact thread"),
            crate::ThreadStartAuthorizationScope::Existing
        );
        assert_eq!(
            crate::resolve_thread_start_authorization_scope(
                &fixture.database,
                fixture.private_thread_id.as_str(),
                fixture.blue_workspace_id.as_str(),
            )
            .await
            .expect("resolve mismatched parent"),
            crate::ThreadStartAuthorizationScope::ParentMismatch
        );
    }

    #[tokio::test]
    async fn creator_management_preserves_memberships_across_visibility_and_archive_roundtrip() {
        let fixture = fixture().await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let participant_id =
            PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("participant id");
        let now = chrono::Utc::now().fixed_offset();
        gateway_principal::ActiveModel {
            id: Set(participant_id.to_string()),
            gateway_id: Set(fixture.gateway_id.to_string()),
            kind: Set("user".to_owned()),
            role_key: Set(Some("member".to_owned())),
            status: Set("active".to_owned()),
            display_name: Set("Participant".to_owned()),
            nickname: Set("participant".to_owned()),
            nickname_key: Set("participant".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            removed_at: Set(None),
            authorization_guard: Set(1),
        }
        .insert(&fixture.database)
        .await
        .expect("create participant");
        let transaction = fixture.database.begin().await.expect("begin grants");
        for principal_id in [&fixture.member_id, &participant_id] {
            insert_workspace_membership(
                &transaction,
                &NewWorkspaceMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: (*principal_id).clone(),
                    workspace_id: fixture.red_workspace_id.clone(),
                    granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                    now,
                },
            )
            .await
            .expect("grant workspace");
        }
        transaction.commit().await.expect("commit grants");

        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let timestamp = now.timestamp();
        let new_thread = Thread {
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            name: Some("Managed".to_owned()),
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        store
            .create_member_private_thread(
                &new_thread,
                &fixture.gateway_id,
                &fixture.member_id,
                PersistedActorRef::Principal(fixture.member_id.clone()),
            )
            .await
            .expect("create creator-owned private thread");
        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin participant grant");
        insert_thread_membership(
            &transaction,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: participant_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: thread_id.clone(),
                added_by: PersistedActorRef::Principal(fixture.member_id.clone()),
                now,
            },
        )
        .await
        .expect("add explicit participant");
        transaction
            .commit()
            .await
            .expect("commit participant grant");

        assert_eq!(
            store
                .update_user_thread_management(
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&participant_id),
                    Some("Not allowed"),
                    None,
                    None,
                )
                .await
                .expect("non-creator update decision"),
            None,
            "participant membership must not transfer creator management rights"
        );
        assert_eq!(
            store
                .update_user_thread_management(
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&fixture.member_id),
                    Some("Renamed"),
                    Some(PersistedThreadAccessClass::Workspace),
                    Some(true),
                )
                .await
                .expect("creator publish and archive"),
            Some(true)
        );
        let published = thread::Entity::find_by_id(thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query published thread")
            .expect("published thread exists");
        assert_eq!(published.access_class, "workspace");
        assert_eq!(published.status, "closed");
        assert_eq!(published.name.as_deref(), Some("Renamed"));

        assert_eq!(
            store
                .update_user_thread_management(
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    None,
                    None,
                    Some(PersistedThreadAccessClass::Private),
                    Some(false),
                )
                .await
                .expect("Superuser private restore"),
            Some(true)
        );
        let restored = thread::Entity::find_by_id(thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query restored thread")
            .expect("restored thread exists");
        assert_eq!(restored.access_class, "private");
        assert_eq!(restored.status, "idle");
        for principal_id in [&fixture.member_id, &participant_id] {
            assert!(
                find_thread_membership(&fixture.database, &thread_id, principal_id)
                    .await
                    .expect("query preserved membership")
                    .is_some(),
                "visibility transition must preserve every explicit membership"
            );
        }
    }

    #[tokio::test]
    async fn participant_service_is_idempotent_scoped_and_preserves_creator_membership() {
        let fixture = fixture().await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let participant_id =
            PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("participant id");
        let now = chrono::Utc::now().fixed_offset();
        gateway_principal::ActiveModel {
            id: Set(participant_id.to_string()),
            gateway_id: Set(fixture.gateway_id.to_string()),
            kind: Set("user".to_owned()),
            role_key: Set(Some("member".to_owned())),
            status: Set("active".to_owned()),
            display_name: Set("Participant service".to_owned()),
            nickname: Set("participant_service".to_owned()),
            nickname_key: Set("participant_service".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            removed_at: Set(None),
            authorization_guard: Set(1),
        }
        .insert(&fixture.database)
        .await
        .expect("create participant");
        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin workspace grants");
        for principal_id in [&fixture.member_id, &participant_id] {
            insert_workspace_membership(
                &transaction,
                &NewWorkspaceMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: (*principal_id).clone(),
                    workspace_id: fixture.red_workspace_id.clone(),
                    granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                    now,
                },
            )
            .await
            .expect("grant participant workspace");
        }
        transaction.commit().await.expect("commit workspace grants");

        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let new_thread = Thread {
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: now.timestamp(),
            updated_at: now.timestamp(),
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        store
            .create_member_private_thread(
                &new_thread,
                &fixture.gateway_id,
                &fixture.member_id,
                PersistedActorRef::Principal(fixture.member_id.clone()),
            )
            .await
            .expect("create participant test thread");

        let first = store
            .add_private_thread_participant(
                &fixture.gateway_id,
                fixture.red_workspace_id.as_str(),
                thread_id.as_str(),
                Some(&fixture.member_id),
                &participant_id,
                PersistedActorRef::Principal(fixture.member_id.clone()),
            )
            .await
            .expect("first participant add")
            .expect("authorized participant add");
        let PrivateThreadParticipantMutation::Applied {
            changed,
            participant_ids: _,
        } = first
        else {
            panic!("eligible participant add must be applied");
        };
        assert!(changed);
        let duplicate = store
            .add_private_thread_participant(
                &fixture.gateway_id,
                fixture.red_workspace_id.as_str(),
                thread_id.as_str(),
                Some(&fixture.member_id),
                &participant_id,
                PersistedActorRef::Principal(fixture.member_id.clone()),
            )
            .await
            .expect("duplicate participant add")
            .expect("authorized duplicate add");
        let PrivateThreadParticipantMutation::Applied {
            changed,
            participant_ids,
        } = duplicate
        else {
            panic!("eligible duplicate participant add must be applied idempotently");
        };
        assert!(!changed);
        assert_eq!(participant_ids.len(), 2);

        let mut participant: gateway_principal::ActiveModel =
            gateway_principal::Entity::find_by_id(participant_id.to_string())
                .one(&fixture.database)
                .await
                .expect("query participant")
                .expect("participant exists")
                .into();
        participant.status = Set("suspended".to_owned());
        participant
            .update(&fixture.database)
            .await
            .expect("suspend existing participant");
        let suspended_participants = store
            .list_private_thread_participant_ids(
                &fixture.gateway_id,
                fixture.red_workspace_id.as_str(),
                thread_id.as_str(),
                Some(&fixture.member_id),
            )
            .await
            .expect("suspended participant list")
            .expect("authorized suspended participant list");
        let mut expected_suspended_participants =
            vec![fixture.member_id.clone(), participant_id.clone()];
        expected_suspended_participants.sort();
        assert_eq!(
            suspended_participants, expected_suspended_participants,
            "suspend preserves current membership rows and directory visibility"
        );

        let member_creator_removal = store
            .remove_private_thread_participant(
                &fixture.gateway_id,
                fixture.red_workspace_id.as_str(),
                thread_id.as_str(),
                Some(&fixture.member_id),
                &fixture.member_id,
            )
            .await
            .expect("Member creator removal must be classified")
            .expect("Member creator manages this private thread");
        assert_eq!(
            member_creator_removal,
            PrivateThreadParticipantMutation::MandatoryCreator
        );
        assert!(
            find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                .await
                .expect("query creator membership")
                .is_some()
        );
        let superuser_creator_removal = store
            .remove_private_thread_participant(
                &fixture.gateway_id,
                fixture.red_workspace_id.as_str(),
                thread_id.as_str(),
                None,
                &fixture.member_id,
            )
            .await
            .expect("Superuser creator removal must be classified")
            .expect("Superuser can manage this private thread");
        assert_eq!(
            superuser_creator_removal,
            PrivateThreadParticipantMutation::MandatoryCreator,
            "normal Superuser participant mutation must also preserve the creator membership"
        );
        assert!(
            find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                .await
                .expect("query creator membership after Superuser attempt")
                .is_some()
        );

        let removed = store
            .remove_private_thread_participant(
                &fixture.gateway_id,
                fixture.red_workspace_id.as_str(),
                thread_id.as_str(),
                Some(&fixture.member_id),
                &participant_id,
            )
            .await
            .expect("suspended participant remove")
            .expect("authorized participant remove");
        let PrivateThreadParticipantMutation::Applied {
            changed,
            participant_ids,
        } = removed
        else {
            panic!("persisted suspended participant must remain removable");
        };
        assert!(changed);
        assert_eq!(participant_ids, vec![fixture.member_id.clone()]);

        let mut participant: gateway_principal::ActiveModel =
            gateway_principal::Entity::find_by_id(participant_id.to_string())
                .one(&fixture.database)
                .await
                .expect("query participant")
                .expect("participant exists")
                .into();
        participant.status = Set("suspended".to_owned());
        participant
            .update(&fixture.database)
            .await
            .expect("suspend participant");
        let suspended_add = store
            .add_private_thread_participant(
                &fixture.gateway_id,
                fixture.red_workspace_id.as_str(),
                thread_id.as_str(),
                Some(&fixture.member_id),
                &participant_id,
                PersistedActorRef::Principal(fixture.member_id.clone()),
            )
            .await
            .expect("suspended target rejection must be classified")
            .expect("creator remains authorized");
        assert_eq!(
            suspended_add,
            PrivateThreadParticipantMutation::TargetUnavailable
        );
    }

    #[tokio::test]
    async fn member_thread_collection_filters_private_workspace_and_internal_rows_in_sql() {
        let fixture = fixture().await;
        let member_b_id =
            PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("second member id");
        let now = chrono::Utc::now().fixed_offset();
        gateway_principal::ActiveModel {
            id: Set(member_b_id.to_string()),
            gateway_id: Set(fixture.gateway_id.to_string()),
            kind: Set("user".to_owned()),
            role_key: Set(Some("member".to_owned())),
            status: Set("active".to_owned()),
            display_name: Set("Member B".to_owned()),
            nickname: Set("member_b_matrix".to_owned()),
            nickname_key: Set("member_b_matrix".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            removed_at: Set(None),
            authorization_guard: Set(1),
        }
        .insert(&fixture.database)
        .await
        .expect("create second Member");

        let transaction = fixture.database.begin().await.expect("begin grants");
        for (principal_id, workspace_id) in [
            (&fixture.member_id, &fixture.red_workspace_id),
            (&fixture.member_id, &fixture.blue_workspace_id),
            (&member_b_id, &fixture.red_workspace_id),
        ] {
            insert_workspace_membership(
                &transaction,
                &NewWorkspaceMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: (*principal_id).clone(),
                    workspace_id: (*workspace_id).clone(),
                    granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                    now,
                },
            )
            .await
            .expect("grant overlapping workspace matrix");
        }
        insert_thread_membership(
            &transaction,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: fixture.private_thread_id.clone(),
                added_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                now,
            },
        )
        .await
        .expect("grant private thread");
        transaction.commit().await.expect("commit grants");

        let workspace_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let internal_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let peer_private_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let blue_workspace_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        for (thread_id, workspace_id, access_class, origin_kind, creator_id) in [
            (
                workspace_thread_id.as_str(),
                fixture.red_workspace_id.as_str(),
                "workspace",
                "user",
                fixture.superuser_id.to_string(),
            ),
            (
                internal_thread_id.as_str(),
                fixture.red_workspace_id.as_str(),
                "internal",
                "task_run",
                fixture.superuser_id.to_string(),
            ),
            (
                peer_private_thread_id.as_str(),
                fixture.red_workspace_id.as_str(),
                "private",
                "user",
                member_b_id.to_string(),
            ),
            (
                blue_workspace_thread_id.as_str(),
                fixture.blue_workspace_id.as_str(),
                "workspace",
                "user",
                fixture.member_id.to_string(),
            ),
        ] {
            thread::ActiveModel {
                id: Set(thread_id.to_owned()),
                workspace_id: Set(workspace_id.to_owned()),
                name: Set(None),
                preview: Set(String::new()),
                mode: Set("chat".to_owned()),
                model: Set("test".to_owned()),
                model_provider: Set("test".to_owned()),
                status: Set("idle".to_owned()),
                origin_kind: Set(origin_kind.to_owned()),
                // Deliberately visible to exercise access_class fail-closed filtering.
                sidebar_visibility: Set("visible".to_owned()),
                access_class: Set(access_class.to_owned()),
                agent_nickname: Set(None),
                agent_role: Set(None),
                summary: Set(None),
                summary_turn_count: Set(None),
                created_at: Set(now),
                updated_at: Set(now),
                created_by_actor_id: Set(Some(creator_id)),
                created_by_actor_kind: Set(Some("principal".to_owned())),
            }
            .insert(&fixture.database)
            .await
            .expect("insert collection fixture thread");
        }

        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin peer private grant");
        insert_thread_membership(
            &transaction,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: member_b_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: peer_private_thread_id.clone(),
                added_by: PersistedActorRef::Principal(member_b_id.clone()),
                now,
            },
        )
        .await
        .expect("grant peer private thread");
        transaction
            .commit()
            .await
            .expect("commit peer private grant");

        let member_a_red = list_accessible_threads_for_principal(
            &fixture.database,
            &fixture.member_id,
            fixture.red_workspace_id.as_str(),
            100,
        )
        .await
        .expect("list Member A red threads")
        .into_iter()
        .map(|thread| thread.id)
        .collect::<BTreeSet<_>>();
        assert_eq!(
            member_a_red,
            BTreeSet::from([
                fixture.private_thread_id.clone(),
                workspace_thread_id.clone()
            ])
        );
        assert!(!member_a_red.contains(&internal_thread_id));
        assert!(!member_a_red.contains(&peer_private_thread_id));
        assert_eq!(
            find_accessible_thread_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.red_workspace_id.as_str(),
                workspace_thread_id.as_str(),
            )
            .await
            .expect("resolve accessible workspace thread")
            .map(|thread| thread.id),
            Some(workspace_thread_id.clone())
        );
        assert!(
            find_accessible_thread_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.red_workspace_id.as_str(),
                peer_private_thread_id.as_str(),
            )
            .await
            .expect("hide peer private thread")
            .is_none()
        );
        assert!(
            find_accessible_thread_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.blue_workspace_id.as_str(),
                workspace_thread_id.as_str(),
            )
            .await
            .expect("reject exact parent mismatch")
            .is_none()
        );

        let member_b_red = list_accessible_threads_for_principal(
            &fixture.database,
            &member_b_id,
            fixture.red_workspace_id.as_str(),
            100,
        )
        .await
        .expect("list Member B red threads")
        .into_iter()
        .map(|thread| thread.id)
        .collect::<BTreeSet<_>>();
        assert_eq!(
            member_b_red,
            BTreeSet::from([peer_private_thread_id.clone(), workspace_thread_id.clone()])
        );
        assert!(!member_b_red.contains(&fixture.private_thread_id));
        assert!(!member_b_red.contains(&internal_thread_id));

        let member_a_blue = list_accessible_threads_for_principal(
            &fixture.database,
            &fixture.member_id,
            fixture.blue_workspace_id.as_str(),
            100,
        )
        .await
        .expect("list Member A blue threads")
        .into_iter()
        .map(|thread| thread.id)
        .collect::<BTreeSet<_>>();
        assert_eq!(
            member_a_blue,
            BTreeSet::from([blue_workspace_thread_id.clone()])
        );

        let member_b_blue = list_accessible_threads_for_principal(
            &fixture.database,
            &member_b_id,
            fixture.blue_workspace_id.as_str(),
            100,
        )
        .await
        .expect("list ungranted Member B blue threads")
        .into_iter()
        .map(|thread| thread.id)
        .collect::<BTreeSet<_>>();
        assert!(member_b_blue.is_empty());
    }

    #[tokio::test]
    async fn directory_acl_precedes_pagination_and_hides_unrelated_profiles() {
        let fixture = fixture().await;
        let now = chrono::Utc::now().fixed_offset();
        let shared_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("shared id");
        let unrelated_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("unrelated id");
        let removed_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("removed id");
        for (id, nickname) in [
            (&shared_id, "zzz_shared"),
            (&unrelated_id, "aaa_unrelated"),
            (&removed_id, "removed_author"),
        ] {
            gateway_principal::ActiveModel {
                id: Set(id.to_string()),
                gateway_id: Set(fixture.gateway_id.to_string()),
                kind: Set("user".to_owned()),
                role_key: Set(Some("member".to_owned())),
                status: Set("active".to_owned()),
                display_name: Set(nickname.to_owned()),
                nickname: Set(nickname.to_owned()),
                nickname_key: Set(nickname.to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                removed_at: Set(None),
                authorization_guard: Set(1),
            }
            .insert(&fixture.database)
            .await
            .expect("insert directory principal");
        }

        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin directory grants");
        for (principal_id, workspace_id) in [
            (&fixture.member_id, &fixture.red_workspace_id),
            (&shared_id, &fixture.red_workspace_id),
            (&removed_id, &fixture.red_workspace_id),
            (&unrelated_id, &fixture.blue_workspace_id),
        ] {
            insert_workspace_membership(
                &transaction,
                &NewWorkspaceMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: (*principal_id).clone(),
                    workspace_id: (*workspace_id).clone(),
                    granted_by: PersistedActorRef::System,
                    now,
                },
            )
            .await
            .expect("insert directory membership");
        }
        transaction.commit().await.expect("commit directory grants");

        for (principal_id, status, removed_at) in [
            (&shared_id, "suspended", None),
            (&removed_id, "removed", Some(now)),
        ] {
            let mut principal: gateway_principal::ActiveModel =
                gateway_principal::Entity::find_by_id(principal_id.to_string())
                    .one(&fixture.database)
                    .await
                    .expect("query directory principal")
                    .expect("directory principal exists")
                    .into();
            principal.status = Set(status.to_owned());
            principal.removed_at = Set(removed_at);
            principal
                .update(&fixture.database)
                .await
                .expect("update directory principal status");
        }

        let page = list_member_directory_page(
            &fixture.database,
            &fixture.gateway_id,
            &fixture.member_id,
            PrincipalKind::User,
            None,
            1,
        )
        .await
        .expect("list one authorized directory row");
        assert_eq!(page.principals.len(), 1);
        assert_ne!(
            page.principals[0].id, unrelated_id,
            "an unrelated row sorted first must not consume the authorized page"
        );

        assert!(
            find_shared_workspace_principal_for_principal(
                &fixture.database,
                &fixture.gateway_id,
                &fixture.member_id,
                &shared_id,
            )
            .await
            .expect("resolve suspended shared profile")
            .is_some()
        );
        assert!(
            find_shared_workspace_principal_for_principal(
                &fixture.database,
                &fixture.gateway_id,
                &fixture.member_id,
                &unrelated_id,
            )
            .await
            .expect("hide unrelated profile")
            .is_none()
        );
        assert!(
            find_shared_workspace_principal_for_principal(
                &fixture.database,
                &fixture.gateway_id,
                &fixture.member_id,
                &removed_id,
            )
            .await
            .expect("removed profile is not an interactive directory entry")
            .is_none()
        );

        let superuser_directory = list_member_directory_page(
            &fixture.database,
            &fixture.gateway_id,
            &fixture.superuser_id,
            PrincipalKind::Superuser,
            None,
            100,
        )
        .await
        .expect("list complete Superuser directory")
        .principals;
        let superuser_ids = superuser_directory
            .into_iter()
            .map(|principal| principal.id)
            .collect::<BTreeSet<_>>();
        assert!(superuser_ids.contains(&fixture.superuser_id));
        assert!(superuser_ids.contains(&shared_id));
        assert!(superuser_ids.contains(&unrelated_id));
        assert!(
            superuser_ids.contains(&removed_id),
            "removed actor identity remains available to historical Superuser inspection"
        );

        let mut red_workspace: workspace::ActiveModel =
            workspace::Entity::find_by_id(fixture.red_workspace_id.clone())
                .one(&fixture.database)
                .await
                .expect("query shared workspace")
                .expect("shared workspace exists")
                .into();
        red_workspace.is_active = Set(false);
        red_workspace
            .update(&fixture.database)
            .await
            .expect("deactivate shared workspace");
        assert!(
            find_shared_workspace_principal_for_principal(
                &fixture.database,
                &fixture.gateway_id,
                &fixture.member_id,
                &shared_id,
            )
            .await
            .expect("inactive workspace must not establish directory scope")
            .is_none()
        );
        let member_ids = list_member_directory_page(
            &fixture.database,
            &fixture.gateway_id,
            &fixture.member_id,
            PrincipalKind::User,
            None,
            100,
        )
        .await
        .expect("list directory after workspace deactivation")
        .principals
        .into_iter()
        .map(|principal| principal.id)
        .collect::<BTreeSet<_>>();
        assert!(!member_ids.contains(&shared_id));
    }

    #[tokio::test]
    async fn deleting_workspace_membership_removes_dependent_thread_grants() {
        let fixture = fixture().await;
        let transaction = fixture.database.begin().await.expect("begin");
        let now = chrono::Utc::now().fixed_offset();
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .unwrap();
        insert_thread_membership(
            &transaction,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: fixture.private_thread_id.clone(),
                added_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .unwrap();
        let deletion = delete_workspace_membership(
            &transaction,
            &fixture.gateway_id,
            &fixture.member_id,
            fixture.red_workspace_id.as_str(),
        )
        .await
        .unwrap();
        assert!(deletion.changed);
        assert_eq!(
            deletion.removed_private_thread_ids,
            vec![fixture.private_thread_id.clone()]
        );
        assert!(
            find_thread_membership(
                &transaction,
                fixture.private_thread_id.as_str(),
                &fixture.member_id
            )
            .await
            .unwrap()
            .is_none()
        );
        transaction.commit().await.expect("commit");
    }

    #[test]
    fn persisted_thread_access_class_conversion_is_strict() {
        for value in [
            PersistedThreadAccessClass::Private,
            PersistedThreadAccessClass::Workspace,
            PersistedThreadAccessClass::Internal,
        ] {
            assert_eq!(
                persisted_thread_access_class_from_db(persisted_thread_access_class_to_db(value))
                    .unwrap(),
                value
            );
        }
        assert!(persisted_thread_access_class_from_db("public").is_err());
    }
}
