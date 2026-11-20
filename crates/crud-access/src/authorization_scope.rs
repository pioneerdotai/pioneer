use std::collections::{BTreeSet, HashMap};

use crate::task_scope::{TaskScopeAlias, task_scope_query};
use anyhow::{Context, Result};
use pioneer_entity::{
    artifact, artifact_binding, auth_session, mcp_server_installation, skill_workspace_policy,
    task, task_run, thread, thread_lineage, turn, workspace,
};
use pioneer_protocol::{PersistedActorRef, PrincipalId};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityName, EntityTrait, JoinType, QueryFilter,
    QuerySelect,
};

use super::identity::actor_ref_from_db;
use super::membership::{PersistedThreadAccessClass, persisted_thread_access_class_from_db};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceAuthorizationScope {
    pub workspace_id: String,
    pub is_active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadAuthorizationScope {
    pub workspace_id: String,
    pub thread_id: String,
    pub workspace_is_active: bool,
    pub access_class: PersistedThreadAccessClass,
    pub creator_principal_id: Option<PrincipalId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadStartAuthorizationScope {
    Missing,
    ParentMismatch,
    Existing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnAuthorizationScope {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub thread: ThreadAuthorizationScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactAuthorizationScope {
    pub workspace_id: String,
    pub thread_id: Option<String>,
    pub artifact_id: String,
    pub workspace_is_active: bool,
    pub thread: Option<ThreadAuthorizationScope>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeDraftArtifactAuthorizationScope {
    pub workspace_id: String,
    pub thread_id: String,
    pub artifact_id: String,
    pub workspace_is_active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskAuthorizationScope {
    pub workspace_id: String,
    pub root_thread_id: Option<String>,
    pub task_id: String,
    pub workspace_is_active: bool,
    pub root_thread: Option<ThreadAuthorizationScope>,
    pub initiating_principal_id: Option<PrincipalId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionAuthorizationScope {
    pub gateway_id: String,
    pub principal_id: String,
    pub session_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedCapabilityScopeKind {
    Skill,
    McpServer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityAuthorizationScope {
    pub workspace_id: String,
    pub capability_id: String,
    pub workspace_is_active: bool,
    pub enabled: bool,
}

pub async fn resolve_workspace_authorization_scope<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Option<WorkspaceAuthorizationScope>> {
    Ok(workspace::Entity::find_by_id(workspace_id)
        .select_only()
        .columns([workspace::Column::Id, workspace::Column::IsActive])
        .into_tuple::<(String, bool)>()
        .one(db)
        .await
        .context("failed to resolve workspace authorization scope")?
        .map(|(workspace_id, is_active)| WorkspaceAuthorizationScope {
            workspace_id,
            is_active,
        }))
}

pub async fn resolve_thread_authorization_scope<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    expected_workspace_id: Option<&str>,
) -> Result<Option<ThreadAuthorizationScope>> {
    let mut query = thread::Entity::find_by_id(thread_id)
        .select_only()
        .columns([
            thread::Column::WorkspaceId,
            thread::Column::Id,
            thread::Column::AccessClass,
            thread::Column::CreatedByActorKind,
            thread::Column::CreatedByActorId,
        ])
        .join(
            JoinType::InnerJoin,
            thread::Entity::belongs_to(workspace::Entity)
                .from(thread::Column::WorkspaceId)
                .to(workspace::Column::Id)
                .into(),
        )
        .column_as(workspace::Column::IsActive, "workspace_is_active");
    if let Some(workspace_id) = expected_workspace_id {
        query = query.filter(thread::Column::WorkspaceId.eq(workspace_id));
    }
    let Some((
        workspace_id,
        thread_id,
        access_class,
        created_by_kind,
        created_by_id,
        workspace_is_active,
    )) = query
        .into_tuple::<(String, String, String, Option<String>, Option<String>, bool)>()
        .one(db)
        .await
        .context("failed to resolve thread authorization scope")?
    else {
        return Ok(None);
    };
    let access_class = match persisted_thread_access_class_from_db(&access_class) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let creator_principal_id =
        match actor_ref_from_db(created_by_kind.as_deref(), created_by_id.as_deref()) {
            Ok(Some(PersistedActorRef::Principal(principal_id))) => Some(principal_id),
            Ok(Some(PersistedActorRef::System | PersistedActorRef::AgentExecution(_)) | None) => {
                None
            }
            Err(_) => return Ok(None),
        };
    Ok(Some(ThreadAuthorizationScope {
        workspace_id,
        thread_id,
        workspace_is_active,
        access_class,
        creator_principal_id,
    }))
}

/// Page-local facts for the existing internal-child policy. No access decision
/// is cached or granted here. All reads are bounded metadata projections.
pub struct InternalThreadAuthorizationFacts {
    pub child: ThreadAuthorizationScope,
    pub root: ThreadAuthorizationScope,
    pub parent_turn_id: String,
    pub workspace_member: bool,
    pub root_thread_member: bool,
    pub root_collaboration_thread_id: Option<String>,
}

pub async fn load_internal_thread_authorization_facts<C: ConnectionTrait>(
    db: &C,
    child_ids: &[String],
    principal: &PrincipalId,
) -> Result<Vec<InternalThreadAuthorizationFacts>> {
    let ids = child_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut result = Vec::new();
    for chunk in ids.chunks(64) {
        let lineages = thread_lineage::Entity::find()
            .filter(thread_lineage::Column::ChildThreadId.is_in(chunk.iter().cloned()))
            .all(db)
            .await?;
        let thread_ids = chunk
            .iter()
            .cloned()
            .chain(lineages.iter().map(|l| l.root_thread_id.clone()))
            .collect::<BTreeSet<_>>();
        let rows = thread::Entity::find()
            .select_only()
            .columns([
                thread::Column::Id,
                thread::Column::WorkspaceId,
                thread::Column::AccessClass,
                thread::Column::CreatedByActorKind,
                thread::Column::CreatedByActorId,
            ])
            .filter(thread::Column::Id.is_in(thread_ids.iter().cloned()))
            .into_tuple::<(String, String, String, Option<String>, Option<String>)>()
            .all(db)
            .await?;
        let workspace_ids = rows.iter().map(|r| r.1.clone()).collect::<BTreeSet<_>>();
        let workspaces = workspace::Entity::find()
            .select_only()
            .columns([workspace::Column::Id, workspace::Column::IsActive])
            .filter(workspace::Column::Id.is_in(workspace_ids.iter().cloned()))
            .into_tuple::<(String, bool)>()
            .all(db)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();
        let workspace_members = pioneer_entity::workspace_membership::Entity::find()
            .select_only()
            .column(pioneer_entity::workspace_membership::Column::WorkspaceId)
            .filter(pioneer_entity::workspace_membership::Column::WorkspaceId.is_in(workspace_ids))
            .filter(
                pioneer_entity::workspace_membership::Column::PrincipalId.eq(principal.as_str()),
            )
            .into_tuple::<String>()
            .all(db)
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let thread_members = pioneer_entity::thread_membership::Entity::find()
            .select_only()
            .column(pioneer_entity::thread_membership::Column::ThreadId)
            .filter(
                pioneer_entity::thread_membership::Column::ThreadId
                    .is_in(thread_ids.iter().cloned()),
            )
            .filter(pioneer_entity::thread_membership::Column::PrincipalId.eq(principal.as_str()))
            .into_tuple::<String>()
            .all(db)
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let root_lineages = thread_lineage::Entity::find()
            .filter(thread_lineage::Column::ChildThreadId.is_in(thread_ids))
            .all(db)
            .await?
            .into_iter()
            .map(|l| (l.child_thread_id.clone(), l))
            .collect::<HashMap<_, _>>();
        let mut scopes = HashMap::new();
        for (id, workspace_id, access, kind, actor) in rows {
            let Some(active) = workspaces.get(&workspace_id) else {
                continue;
            };
            let Ok(access_class) = persisted_thread_access_class_from_db(&access) else {
                continue;
            };
            let creator_principal_id = match actor_ref_from_db(kind.as_deref(), actor.as_deref()) {
                Ok(Some(PersistedActorRef::Principal(p))) => Some(p),
                Ok(_) => None,
                Err(_) => continue,
            };
            scopes.insert(
                id.clone(),
                ThreadAuthorizationScope {
                    workspace_id,
                    thread_id: id,
                    workspace_is_active: *active,
                    access_class,
                    creator_principal_id,
                },
            );
        }
        for lineage in lineages {
            let (Some(child), Some(root), Some(parent_turn_id)) = (
                scopes.get(&lineage.child_thread_id),
                scopes.get(&lineage.root_thread_id),
                lineage.created_by_turn_id,
            ) else {
                continue;
            };
            if child.access_class != PersistedThreadAccessClass::Internal
                || child.thread_id == root.thread_id
                || child.workspace_id != root.workspace_id
            {
                continue;
            }
            // authorize_thread also requires an internal root to have valid
            // non-self lineage, even for an absolute-policy principal.
            if root.access_class == PersistedThreadAccessClass::Internal
                && !root_lineages
                    .get(&root.thread_id)
                    .is_some_and(|l| l.root_thread_id != root.thread_id)
            {
                continue;
            }
            result.push(InternalThreadAuthorizationFacts {
                workspace_member: workspace_members.contains(&root.workspace_id),
                root_thread_member: thread_members.contains(&root.thread_id),
                child: child.clone(),
                root: root.clone(),
                parent_turn_id,
                root_collaboration_thread_id: (root.access_class
                    == PersistedThreadAccessClass::Internal)
                    .then(|| root_lineages[&root.thread_id].root_thread_id.clone()),
            });
        }
    }
    Ok(result)
}

pub async fn resolve_thread_start_authorization_scope<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    workspace_id: &str,
) -> Result<ThreadStartAuthorizationScope> {
    let Some(model) = thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve thread/start authorization scope")?
    else {
        return Ok(ThreadStartAuthorizationScope::Missing);
    };
    if model.workspace_id != workspace_id {
        return Ok(ThreadStartAuthorizationScope::ParentMismatch);
    }
    Ok(ThreadStartAuthorizationScope::Existing)
}

pub async fn resolve_turn_authorization_scope<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    expected_workspace_id: Option<&str>,
    expected_thread_id: Option<&str>,
) -> Result<Option<TurnAuthorizationScope>> {
    let Some((id, thread_id)) = turn::Entity::find_by_id(turn_id.to_owned())
        .select_only()
        .columns([turn::Column::Id, turn::Column::ThreadId])
        .into_tuple::<(String, String)>()
        .one(db)
        .await
        .context("failed to resolve turn authorization scope")?
    else {
        return Ok(None);
    };
    if expected_thread_id.is_some_and(|expected| expected != thread_id.as_str()) {
        return Ok(None);
    }
    let Some(thread) =
        resolve_thread_authorization_scope(db, thread_id.as_str(), expected_workspace_id).await?
    else {
        return Ok(None);
    };
    Ok(Some(TurnAuthorizationScope {
        workspace_id: thread.workspace_id.clone(),
        thread_id: thread.thread_id.clone(),
        turn_id: id,
        thread,
    }))
}

pub async fn resolve_artifact_authorization_scope<C: ConnectionTrait>(
    db: &C,
    artifact_id: &str,
    expected_workspace_id: Option<&str>,
    expected_thread_id: Option<&str>,
) -> Result<Option<ArtifactAuthorizationScope>> {
    let Some(model) = artifact::Entity::find_by_id(artifact_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve artifact authorization scope")?
    else {
        return Ok(None);
    };
    if expected_workspace_id.is_some_and(|expected| expected != model.workspace_id.as_str()) {
        return Ok(None);
    }
    let Some(workspace) =
        resolve_workspace_authorization_scope(db, model.workspace_id.as_str()).await?
    else {
        return Ok(None);
    };

    let bindings = artifact_binding::Entity::find()
        .filter(artifact_binding::Column::ArtifactId.eq(model.id.clone()))
        .all(db)
        .await
        .context("failed to load artifact authorization bindings")?;
    let mut thread_ids = BTreeSet::new();
    if let Some(thread_id) = model.primary_thread_id.as_ref() {
        let Some(root_thread_id) = artifact_authorization_root_thread_id(
            db,
            model.workspace_id.as_str(),
            thread_id.as_str(),
        )
        .await?
        else {
            return Ok(None);
        };
        thread_ids.insert(root_thread_id);
    }
    for binding in bindings {
        if binding.workspace_id != model.workspace_id {
            return Ok(None);
        }
        let is_thread_bound_draft_upload =
            binding.binding_kind == "draft_upload" && binding.thread_id.is_some();
        if let Some(thread_id) = binding.thread_id.as_ref() {
            let Some(root_thread_id) = artifact_authorization_root_thread_id(
                db,
                model.workspace_id.as_str(),
                thread_id.as_str(),
            )
            .await?
            else {
                return Ok(None);
            };
            thread_ids.insert(root_thread_id);
        }
        if let Some(turn_id) = binding.turn_id.as_ref() {
            let scope = resolve_turn_authorization_scope(
                db,
                turn_id.as_str(),
                Some(&model.workspace_id),
                None,
            )
            .await?;
            if let Some(scope) = scope {
                let Some(root_thread_id) = artifact_authorization_root_thread_id(
                    db,
                    model.workspace_id.as_str(),
                    scope.thread_id.as_str(),
                )
                .await?
                else {
                    return Ok(None);
                };
                thread_ids.insert(root_thread_id);
            } else {
                // Draft uploads are intentionally bound to a planned turn ID
                // before that turn is materialized. The already-validated
                // thread binding is the authorization root during this gap.
                if !is_thread_bound_draft_upload {
                    return Ok(None);
                }
            }
        }
        if let Some(task_id) = binding.task_id {
            let Some(scope) = resolve_task_authorization_scope(
                db,
                task_id.as_str(),
                Some(&model.workspace_id),
                None,
            )
            .await?
            else {
                return Ok(None);
            };
            let Some(thread_id) = scope.root_thread_id else {
                return Ok(None);
            };
            thread_ids.insert(thread_id);
        }
        if let Some(task_run_id) = binding.task_run_id {
            let Some(run) = task_run::Entity::find_by_id(task_run_id)
                .one(db)
                .await
                .context("failed to resolve artifact task-run binding")?
            else {
                return Ok(None);
            };
            let Some(scope) = resolve_task_authorization_scope(
                db,
                run.task_id.as_str(),
                Some(&model.workspace_id),
                None,
            )
            .await?
            else {
                return Ok(None);
            };
            let Some(thread_id) = scope.root_thread_id else {
                return Ok(None);
            };
            thread_ids.insert(thread_id);
        }
    }
    if thread_ids.len() > 1 {
        return Ok(None);
    }
    let thread_id = thread_ids.into_iter().next();
    if expected_thread_id.is_some_and(|expected| thread_id.as_deref() != Some(expected)) {
        return Ok(None);
    }
    let thread = match thread_id.as_deref() {
        Some(thread_id) => {
            let Some(scope) = resolve_thread_authorization_scope(
                db,
                thread_id,
                Some(model.workspace_id.as_str()),
            )
            .await?
            else {
                return Ok(None);
            };
            Some(scope)
        }
        None => None,
    };

    Ok(Some(ArtifactAuthorizationScope {
        workspace_id: model.workspace_id,
        thread_id,
        artifact_id: model.id,
        workspace_is_active: workspace.is_active,
        thread,
    }))
}

/// Resolves an upload owned by an exact runtime-only draft. This intentionally
/// does not fall back to the generic artifact resolver: until the first Turn
/// commits there is no persisted Thread from which to derive authorization.
pub async fn resolve_runtime_draft_artifact_authorization_scope<C: ConnectionTrait>(
    db: &C,
    artifact_id: &str,
    expected_workspace_id: &str,
    expected_thread_id: &str,
    expected_creator_principal_id: &PrincipalId,
) -> Result<Option<RuntimeDraftArtifactAuthorizationScope>> {
    let Some(model) = artifact::Entity::find_by_id(artifact_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve runtime draft artifact authorization scope")?
    else {
        return Ok(None);
    };
    if model.workspace_id != expected_workspace_id
        || model.primary_thread_id.as_deref() != Some(expected_thread_id)
        || model.status != "ready"
        || model.deleted_at.is_some()
        || model.created_by_kind != "user"
        || model.created_by_actor_id.as_deref() != Some(expected_creator_principal_id.as_str())
    {
        return Ok(None);
    }
    let Some(workspace) = resolve_workspace_authorization_scope(db, expected_workspace_id).await?
    else {
        return Ok(None);
    };
    let bindings = artifact_binding::Entity::find()
        .filter(artifact_binding::Column::ArtifactId.eq(model.id.clone()))
        .all(db)
        .await
        .context("failed to load runtime draft artifact authorization bindings")?;
    if !runtime_draft_bindings_are_exact(
        bindings.as_slice(),
        expected_workspace_id,
        expected_thread_id,
    ) {
        return Ok(None);
    }
    Ok(Some(RuntimeDraftArtifactAuthorizationScope {
        workspace_id: model.workspace_id,
        thread_id: expected_thread_id.to_owned(),
        artifact_id: model.id,
        workspace_is_active: workspace.is_active,
    }))
}

/// Lists only completed uploads that belong exclusively to an abandoned
/// runtime draft. A persisted Thread makes cleanup a no-op, so a concurrent
/// successful first-turn materialization wins over disconnect cleanup.
pub async fn list_abandoned_runtime_draft_artifact_ids<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<Vec<String>> {
    if thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to check runtime draft persistence before artifact cleanup")?
        .is_some()
    {
        return Ok(Vec::new());
    }
    let mut candidates = artifact::Entity::find()
        .filter(artifact::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(artifact::Column::PrimaryThreadId.eq(thread_id.to_owned()))
        .filter(artifact::Column::DeletedAt.is_null())
        .all(db)
        .await
        .context("failed to list abandoned runtime draft artifact candidates")?;
    candidates.sort_by(|left, right| left.id.cmp(&right.id));

    let mut artifact_ids = Vec::new();
    for candidate in candidates {
        let bindings = artifact_binding::Entity::find()
            .filter(artifact_binding::Column::ArtifactId.eq(candidate.id.clone()))
            .all(db)
            .await
            .context("failed to load abandoned runtime draft artifact bindings")?;
        if runtime_draft_bindings_are_exact(bindings.as_slice(), workspace_id, thread_id) {
            artifact_ids.push(candidate.id);
        }
    }
    Ok(artifact_ids)
}

fn runtime_draft_bindings_are_exact(
    bindings: &[artifact_binding::Model],
    workspace_id: &str,
    thread_id: &str,
) -> bool {
    !bindings.is_empty()
        && bindings.iter().all(|binding| {
            binding.workspace_id == workspace_id
                && binding.thread_id.as_deref() == Some(thread_id)
                && binding.binding_kind == "draft_upload"
                && binding.direction == "input"
                && binding.role.as_deref() == Some("user")
                && binding.message_id.is_none()
                && binding.turn_item_id.is_none()
                && binding.tool_call_id.is_none()
                && binding.task_id.is_none()
                && binding.task_run_id.is_none()
        })
}

async fn artifact_authorization_root_thread_id<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<Option<String>> {
    let Some(thread_scope) =
        resolve_thread_authorization_scope(db, thread_id, Some(workspace_id)).await?
    else {
        return Ok(None);
    };
    if thread_scope.access_class != PersistedThreadAccessClass::Internal {
        return Ok(Some(thread_scope.thread_id));
    }

    let Some(lineage) = thread_lineage::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve internal artifact thread lineage")?
    else {
        // A standalone internal artifact remains a valid persisted resource,
        // but its internal parent facts make it Superuser/System-only.
        return Ok(Some(thread_scope.thread_id));
    };
    if lineage.root_thread_id == thread_id {
        return Ok(None);
    }
    let Some(root) =
        resolve_thread_authorization_scope(db, lineage.root_thread_id.as_str(), Some(workspace_id))
            .await?
    else {
        return Ok(None);
    };
    if root.access_class == PersistedThreadAccessClass::Internal {
        return Ok(None);
    }
    Ok(Some(root.thread_id))
}

pub async fn resolve_artifact_binding_authorization_root<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: Option<&str>,
    turn_id: Option<&str>,
    task_id: Option<&str>,
    task_run_id: Option<&str>,
) -> Result<Option<String>> {
    let mut roots = BTreeSet::new();
    if let Some(thread_id) = thread_id {
        let Some(root) = artifact_authorization_root_thread_id(db, workspace_id, thread_id).await?
        else {
            return Ok(None);
        };
        roots.insert(root);
    }
    if let Some(turn_id) = turn_id {
        let Some(turn) =
            resolve_turn_authorization_scope(db, turn_id, Some(workspace_id), None).await?
        else {
            return Ok(None);
        };
        let Some(root) =
            artifact_authorization_root_thread_id(db, workspace_id, turn.thread_id.as_str())
                .await?
        else {
            return Ok(None);
        };
        roots.insert(root);
    }
    if let Some(task_id) = task_id {
        let Some(task) =
            resolve_task_authorization_scope(db, task_id, Some(workspace_id), None).await?
        else {
            return Ok(None);
        };
        let Some(root) = task.root_thread_id else {
            return Ok(None);
        };
        roots.insert(root);
    }
    if let Some(task_run_id) = task_run_id {
        let Some(run) = task_run::Entity::find_by_id(task_run_id.to_owned())
            .one(db)
            .await
            .context("failed to resolve artifact binding task run")?
        else {
            return Ok(None);
        };
        let Some(task) =
            resolve_task_authorization_scope(db, run.task_id.as_str(), Some(workspace_id), None)
                .await?
        else {
            return Ok(None);
        };
        let Some(root) = task.root_thread_id else {
            return Ok(None);
        };
        roots.insert(root);
    }
    if roots.len() != 1 {
        return Ok(None);
    }
    Ok(roots.into_iter().next())
}

#[derive(sea_orm::FromQueryResult)]
struct TaskScopeRow {
    id: String,
    workspace_id: String,
    is_active: bool,
    thread_id: Option<String>,
    owner_kind: String,
    owner_id: Option<String>,
}

pub async fn resolve_task_authorization_scope<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    expected_workspace_id: Option<&str>,
    expected_root_thread_id: Option<&str>,
) -> Result<Option<TaskAuthorizationScope>> {
    use TaskScopeAlias as S;
    use sea_orm::{
        FromQueryResult,
        sea_query::{Expr, ExprTrait, Query},
    };
    let mut query = task_scope_query(Query::select().from_as(task::Entity, S::Task).to_owned());
    query
        .expr_as(Expr::col((S::Task, task::Column::Id)), task::Column::Id)
        .expr_as(
            Expr::col((S::Task, task::Column::WorkspaceId)),
            task::Column::WorkspaceId,
        )
        .expr_as(
            Expr::col((S::Workspace, workspace::Column::IsActive)),
            workspace::Column::IsActive,
        )
        .expr_as(
            Expr::col((S::Thread, thread::Column::Id)),
            turn::Column::ThreadId,
        )
        .expr_as(
            Expr::col((S::Root, task::Column::OwnerKind)),
            task::Column::OwnerKind,
        )
        .expr_as(
            Expr::col((S::Root, task::Column::OwnerId)),
            task::Column::OwnerId,
        )
        .and_where(Expr::col((S::Task, task::Column::Id)).eq(task_id));
    let Some(row) = db.query_one(&query).await? else {
        return Ok(None);
    };
    let row = TaskScopeRow::from_query_result(&row, "")?;
    if expected_workspace_id.is_some_and(|id| id != row.workspace_id)
        || expected_root_thread_id.is_some_and(|id| Some(id) != row.thread_id.as_deref())
    {
        return Ok(None);
    }
    let root_thread = match row.thread_id.as_deref() {
        Some(id) => {
            let Some(scope) =
                resolve_thread_authorization_scope(db, id, Some(&row.workspace_id)).await?
            else {
                return Ok(None);
            };
            Some(scope)
        }
        None => None,
    };
    let initiating_principal_id = if row.owner_kind == "user" {
        Some(PrincipalId::new(
            row.owner_id.context("Task user owner is missing")?,
        )?)
    } else {
        None
    };
    Ok(Some(TaskAuthorizationScope {
        workspace_id: row.workspace_id,
        root_thread_id: row.thread_id,
        task_id: row.id,
        workspace_is_active: row.is_active,
        root_thread,
        initiating_principal_id,
    }))
}

pub async fn resolve_session_authorization_scope<C: ConnectionTrait>(
    db: &C,
    session_id: &str,
) -> Result<Option<SessionAuthorizationScope>> {
    Ok(auth_session::Entity::find_by_id(session_id)
        .select_only()
        .columns([
            auth_session::Column::GatewayId,
            auth_session::Column::PrincipalId,
            auth_session::Column::Id,
        ])
        .into_tuple::<(String, String, String)>()
        .one(db)
        .await
        .context("failed to resolve session authorization scope")?
        .map(
            |(gateway_id, principal_id, session_id)| SessionAuthorizationScope {
                gateway_id,
                principal_id,
                session_id,
            },
        ))
}

pub async fn resolve_persisted_capability_authorization_scope<C: ConnectionTrait>(
    db: &C,
    kind: PersistedCapabilityScopeKind,
    workspace_id: &str,
    capability_id: &str,
) -> Result<Option<CapabilityAuthorizationScope>> {
    let Some(workspace) = resolve_workspace_authorization_scope(db, workspace_id).await? else {
        return Ok(None);
    };
    let enabled = match kind {
        PersistedCapabilityScopeKind::Skill => {
            // Absence of an override inherits the server-owned global default;
            // an explicit false is the only durable local deny. The exact
            // effective policy and live package identity are resolved by the
            // shared skill projection boundary.
            skill_workspace_policy::Entity::find()
                .filter(skill_workspace_policy::Column::WorkspaceId.eq(workspace_id.to_owned()))
                .filter(skill_workspace_policy::Column::SkillId.eq(capability_id.to_owned()))
                .one(db)
                .await
                .context("failed to resolve workspace skill capability policy")?
                .map_or(true, |policy| policy.enabled != Some(false))
        }
        PersistedCapabilityScopeKind::McpServer => {
            let Some(server) = mcp_server_installation::Entity::find()
                .filter(mcp_server_installation::Column::ScopeKind.eq("workspace"))
                .filter(mcp_server_installation::Column::ScopeKey.eq(workspace_id.to_owned()))
                .filter(
                    Condition::any()
                        .add(mcp_server_installation::Column::Id.eq(capability_id.to_owned()))
                        .add(mcp_server_installation::Column::Name.eq(capability_id.to_owned())),
                )
                .one(db)
                .await
                .context("failed to resolve workspace MCP capability policy")?
            else {
                return Ok(None);
            };
            return Ok(Some(CapabilityAuthorizationScope {
                workspace_id: workspace.workspace_id,
                capability_id: server.name,
                workspace_is_active: workspace.is_active,
                enabled: server.enabled,
            }));
        }
    };
    Ok(Some(CapabilityAuthorizationScope {
        workspace_id: workspace.workspace_id,
        capability_id: capability_id.to_owned(),
        workspace_is_active: workspace.is_active,
        enabled,
    }))
}

/// Thin workspace discovery. The cursor is taken before scope filtering, so a
/// page of inaccessible or malformed tasks cannot stop discovery prematurely.
async fn list_running_task_ids<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    after_task_id: &str,
) -> Result<Vec<String>> {
    use sea_orm::sea_query::{Expr, ExprTrait, Order, Query};
    let query = Query::select()
        .distinct()
        .column((task_run::Entity, task_run::Column::TaskId))
        .from(task_run::Entity)
        .cross_join(task::Entity.table_ref())
        .and_where(
            Expr::col((task::Entity, task::Column::Id))
                .equals((task_run::Entity, task_run::Column::TaskId)),
        )
        .and_where(Expr::col((task::Entity, task::Column::WorkspaceId)).eq(workspace_id))
        .and_where(
            Expr::col((task_run::Entity, task_run::Column::Status)).is_in(["starting", "running"]),
        )
        .and_where(Expr::col((task_run::Entity, task_run::Column::TaskId)).gt(after_task_id))
        .order_by((task_run::Entity, task_run::Column::TaskId), Order::Asc)
        .limit(512)
        .to_owned();
    db.query_all(&query)
        .await?
        .into_iter()
        .map(|row| row.try_get("", "task_id").map_err(Into::into))
        .collect()
}

/// One discovery page, with scope lookup bounded independently of active-run
/// density. Final TaskRead authorization remains the Gateway resolver's job.
pub async fn list_running_task_scopes<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_ids: &[String],
    principal_id: &PrincipalId,
    after_task_id: &str,
) -> Result<RunningTaskScopePage> {
    if thread_ids.is_empty() {
        return Ok(RunningTaskScopePage::default());
    }
    anyhow::ensure!(
        thread_ids.len() <= 500,
        "task activity candidate batch exceeds tree limit"
    );
    let ids = list_running_task_ids(db, workspace_id, after_task_id).await?;
    let mut page = RunningTaskScopePage {
        after_task_id: ids.last().cloned(),
        scopes: Vec::new(),
    };
    for batch in ids.chunks(128) {
        page.scopes.extend(
            list_running_task_scopes_for_ids(db, workspace_id, thread_ids, principal_id, batch)
                .await?
                .scopes,
        );
    }
    Ok(page)
}

/// One bounded page of executing tasks, using the same causal root and admission
/// checks as `resolve_task_authorization_scope`. No run history or payload JSON
/// is loaded. Membership facts are read in the same statement as the scope.
async fn list_running_task_scopes_for_ids<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_ids: &[String],
    principal_id: &PrincipalId,
    task_ids: &[String],
) -> Result<RunningTaskScopePage> {
    let rows = crate::task_activity::running_task_scopes(
        db,
        workspace_id,
        thread_ids,
        principal_id.as_str(),
        task_ids,
    )
    .await?;
    let mut page = RunningTaskScopePage::default();
    for row in rows {
        let task_id = row.task_id;
        page.after_task_id = Some(task_id.clone());
        let initiating_principal_id = if row.owner_kind == "user" {
            let Some(id) = row.owner_id.and_then(|id| PrincipalId::new(id).ok()) else {
                continue;
            };
            Some(id)
        } else {
            None
        };
        let mut creator_principal_id = None;
        let mut valid_actors = true;
        for (kind, id) in [
            (row.cause_actor_kind, row.cause_actor_id),
            (row.root_actor_kind, row.root_actor_id),
        ] {
            match actor_ref_from_db(kind.as_deref(), id.as_deref()) {
                Ok(Some(PersistedActorRef::Principal(id))) => creator_principal_id = Some(id),
                Ok(_) => creator_principal_id = None,
                Err(_) => {
                    valid_actors = false;
                    break;
                }
            }
        }
        if !valid_actors {
            continue;
        }
        let thread_id = row.root_thread_id;
        let workspace_is_active = row.is_active;
        page.scopes.push((
            TaskAuthorizationScope {
                workspace_id: workspace_id.to_owned(),
                root_thread_id: Some(thread_id.clone()),
                task_id,
                workspace_is_active,
                root_thread: Some(ThreadAuthorizationScope {
                    workspace_id: workspace_id.to_owned(),
                    thread_id,
                    workspace_is_active,
                    access_class: persisted_thread_access_class_from_db(&row.access_class)?,
                    creator_principal_id,
                }),
                initiating_principal_id,
            },
            row.workspace_member,
            row.thread_member,
        ));
    }
    Ok(page)
}

#[derive(Default)]
pub struct RunningTaskScopePage {
    pub after_task_id: Option<String>,
    pub scopes: Vec<(TaskAuthorizationScope, bool, bool)>,
}
