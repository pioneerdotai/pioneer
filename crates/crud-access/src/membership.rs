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
    QueryOrder, QuerySelect, QueryTrait, Select, Set,
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

/// Maximum input IDs and returned rows for one authorization-facts query.
/// Membership queries use at most 130 binds: 128 IDs, scope and row limit.
pub use crate::AUTHORIZATION_FACTS_BATCH_LIMIT;

pub async fn load_workspace_membership_principal_ids<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    principal_ids: &[PrincipalId],
) -> Result<Vec<String>> {
    if principal_ids.len() > AUTHORIZATION_FACTS_BATCH_LIMIT {
        bail!("workspace authorization facts batch exceeds limit");
    }
    if principal_ids.is_empty() {
        return Ok(Vec::new());
    }
    workspace_membership::Entity::find()
        .select_only()
        .column(workspace_membership::Column::PrincipalId)
        .filter(workspace_membership::Column::WorkspaceId.eq(workspace_id))
        .filter(
            workspace_membership::Column::PrincipalId
                .is_in(principal_ids.iter().map(ToString::to_string)),
        )
        .limit(AUTHORIZATION_FACTS_BATCH_LIMIT as u64)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to load bounded workspace authorization memberships")
}

pub async fn load_thread_membership_principal_ids<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    principal_ids: &[PrincipalId],
) -> Result<Vec<String>> {
    if principal_ids.len() > AUTHORIZATION_FACTS_BATCH_LIMIT {
        bail!("thread authorization facts batch exceeds limit");
    }
    if principal_ids.is_empty() {
        return Ok(Vec::new());
    }
    thread_membership::Entity::find()
        .select_only()
        .column(thread_membership::Column::PrincipalId)
        .filter(thread_membership::Column::ThreadId.eq(thread_id))
        .filter(
            thread_membership::Column::PrincipalId
                .is_in(principal_ids.iter().map(ToString::to_string)),
        )
        .limit(AUTHORIZATION_FACTS_BATCH_LIMIT as u64)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to load bounded thread authorization memberships")
}

pub async fn find_workspace_membership<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    workspace_id: &str,
) -> Result<Option<workspace_membership::Model>> {
    workspace_grant(principal_id, workspace_id)
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load workspace membership for principal `{principal_id}` \
                 and workspace `{workspace_id}`"
            )
        })
}

fn workspace_grant(
    principal_id: &PrincipalId,
    workspace_id: &str,
) -> Select<workspace_membership::Entity> {
    workspace_membership::Entity::find()
        .filter(workspace_membership::Column::PrincipalId.eq(principal_id.as_str()))
        .filter(workspace_membership::Column::WorkspaceId.eq(workspace_id))
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

/// Internal maintenance snapshot, bounded by the active/id keyset index.
pub async fn list_active_workspace_page<C: ConnectionTrait>(
    db: &C,
    after: Option<&str>,
) -> Result<Vec<workspace::Model>> {
    let mut query = workspace::Entity::find().filter(workspace::Column::IsActive.eq(true));
    if let Some(after) = after {
        query = query.filter(workspace::Column::Id.gt(after.to_owned()));
    }
    query
        .order_by_asc(workspace::Column::Id)
        .limit(64)
        .all(db)
        .await
        .context("failed to page active workspaces")
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
    inbox_after_sequence: i64,
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
        inbox_after_sequence: Set(Some(inbox_after_sequence)),
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
    // Hold the parent grant before deleting its dependent private-thread grants.
    if workspace_grant(principal_id, workspace_id)
        .lock_exclusive()
        .one(db)
        .await?
        .is_none()
    {
        return Ok(WorkspaceMembershipDeletion {
            changed: false,
            removed_private_thread_ids: Vec::new(),
        });
    }
    let workspace_threads = thread::Entity::find()
        .select_only()
        .column(thread::Column::Id)
        .filter(thread::Column::WorkspaceId.eq(workspace_id))
        .into_query();
    let mut removed_private_thread_ids: Vec<_> = thread_membership::Entity::delete_many()
        .filter(thread_membership::Column::PrincipalId.eq(principal_id.as_str()))
        .filter(thread_membership::Column::ThreadId.in_subquery(workspace_threads))
        .exec_with_returning(db)
        .await
        .context("failed to remove dependent private-thread memberships")?
        .into_iter()
        .map(|row| row.thread_id)
        .collect();
    removed_private_thread_ids.sort();
    let result = workspace_membership::Entity::delete_many()
        .filter(workspace_membership::Column::PrincipalId.eq(principal_id.as_str()))
        .filter(workspace_membership::Column::WorkspaceId.eq(workspace_id))
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
    thread_membership::Entity::find()
        .filter(thread_membership::Column::ThreadId.eq(thread_id))
        .filter(thread_membership::Column::PrincipalId.eq(principal_id.as_str()))
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
    inbox_after_sequence: i64,
) -> Result<thread_membership::Model> {
    validate_scope_id("workspace", input.workspace_id.as_str())?;
    validate_scope_id("thread", input.thread_id.as_str())?;
    validate_member_principal(db, &input.gateway_id, &input.principal_id).await?;
    validate_actor(db, &input.gateway_id, &input.added_by).await?;
    validate_private_thread_scope(db, input.workspace_id.as_str(), input.thread_id.as_str())
        .await?;
    if workspace_grant(&input.principal_id, input.workspace_id.as_str())
        .lock_shared()
        .one(db)
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
        inbox_after_sequence: Set(Some(inbox_after_sequence)),
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
    let result = thread_membership::Entity::delete_many()
        .filter(thread_membership::Column::ThreadId.eq(thread_id))
        .filter(thread_membership::Column::PrincipalId.eq(principal_id.as_str()))
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
        || role.is_none()
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
        || role.is_none()
        || !matches!(status, PrincipalStatus::Active | PrincipalStatus::Suspended)
    {
        bail!("persisted membership target is not an ordinary Member principal");
    }
    Ok(())
}

pub async fn private_thread_participant_target_is_eligible<C: ConnectionTrait>(
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
        || role.is_none()
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
        .lock_shared()
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
        .lock_shared()
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
