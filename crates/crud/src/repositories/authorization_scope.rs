use std::collections::BTreeSet;

use anyhow::{Context, Result};
use pioneer_entity::{
    artifact, artifact_binding, auth_session, mcp_server_installation, skill_workspace_policy,
    task, task_execution_admission, task_run, thread, thread_lineage, turn, workspace,
};
use pioneer_protocol::{PersistedActorRef, PrincipalId};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityName, EntityTrait, QueryFilter, QuerySelect,
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
    Ok(workspace::Entity::find_by_id(workspace_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve workspace authorization scope")?
        .map(|model| WorkspaceAuthorizationScope {
            workspace_id: model.id,
            is_active: model.is_active,
        }))
}

pub async fn resolve_thread_authorization_scope<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    expected_workspace_id: Option<&str>,
) -> Result<Option<ThreadAuthorizationScope>> {
    let Some(model) = thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve thread authorization scope")?
    else {
        return Ok(None);
    };
    if expected_workspace_id.is_some_and(|expected| expected != model.workspace_id.as_str()) {
        return Ok(None);
    }
    thread_scope_from_model(db, model).await
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

// Canonical Task scope shared by exact TaskRead resolution and inbox set queries.
// Every reference uses the same typed aliases so a child's owner/provenance
// cannot replace its root Task's ACL.
#[derive(Clone, Copy, sea_orm::DeriveIden)]
pub(crate) enum TaskScopeAlias {
    #[sea_orm(iden = "t")]
    Task,
    #[sea_orm(iden = "scope_root")]
    Root,
    #[sea_orm(iden = "scope_workspace")]
    Workspace,
    #[sea_orm(iden = "scope_turn")]
    Turn,
    #[sea_orm(iden = "scope_origin")]
    Origin,
    #[sea_orm(iden = "scope_lineage")]
    Lineage,
    #[sea_orm(iden = "scope_thread")]
    Thread,
    #[sea_orm(iden = "scope_admission")]
    Admission,
    #[sea_orm(iden = "turn_thread")]
    TurnThread,
}

fn root_owner_thread() -> sea_orm::sea_query::Expr {
    use sea_orm::sea_query::{Expr, ExprTrait};
    Expr::case(
        Expr::col((TaskScopeAlias::Root, task::Column::OwnerKind)).eq("thread"),
        Expr::col((TaskScopeAlias::Root, task::Column::OwnerId)),
    )
    .finally(Expr::val(None::<String>))
    .into()
}

fn explicit_origin_thread() -> sea_orm::sea_query::Expr {
    use sea_orm::sea_query::{Expr, Func};
    Func::coalesce([
        Expr::col((TaskScopeAlias::Root, task::Column::CreatedByThreadId)),
        root_owner_thread(),
    ])
    .into()
}

fn origin_thread() -> sea_orm::sea_query::Expr {
    use sea_orm::sea_query::{Expr, Func};
    Func::coalesce([
        Expr::col((TaskScopeAlias::Root, task::Column::CreatedByThreadId)),
        root_owner_thread(),
        Expr::col((TaskScopeAlias::Turn, turn::Column::ThreadId)),
    ])
    .into()
}

fn valid_actor_scope(alias: TaskScopeAlias) -> Condition {
    use sea_orm::sea_query::{Expr, ExprTrait, Func, extension::sqlite::SqliteExpr};
    let kind = Expr::col((alias, thread::Column::CreatedByActorKind));
    let id = Expr::col((alias, thread::Column::CreatedByActorId));
    Condition::any()
        .add(
            Condition::all()
                .add(kind.clone().is_null())
                .add(id.clone().is_null()),
        )
        .add(
            Condition::all()
                .add(kind.clone().eq("system"))
                .add(id.clone().is_null()),
        )
        .add(
            Condition::all()
                .add(kind.is_in(["principal", "agent_execution"]))
                .add(Func::char_length(id.clone()).eq(21))
                .add(id.glob("*[^A-Za-z0-9]*").not()),
        )
}

pub(crate) fn task_scope_query(
    mut query: sea_orm::sea_query::SelectStatement,
) -> sea_orm::sea_query::SelectStatement {
    use TaskScopeAlias as S;
    use sea_orm::sea_query::{
        Expr, ExprTrait, Func, JoinType, Query, extension::sqlite::SqliteExpr,
    };
    // SeaQuery intentionally ignores ON for CROSS JOIN, so its typed equality
    // predicates belong in WHERE. SQLite retains this left-to-right join order.
    query
        .cross_join(task::Entity.table_ref().alias(S::Root))
        .and_where(Expr::col((S::Root, task::Column::Id)).eq(Func::coalesce([
            Expr::col((S::Task, task::Column::RootTaskId)),
            Expr::col((S::Task, task::Column::Id)),
        ])))
        .and_where(
            Expr::col((S::Root, task::Column::WorkspaceId))
                .equals((S::Task, task::Column::WorkspaceId)),
        )
        .and_where(Expr::col((S::Root, task::Column::RootTaskId)).is_null())
        .cross_join(workspace::Entity.table_ref().alias(S::Workspace))
        .and_where(
            Expr::col((S::Workspace, workspace::Column::Id))
                .equals((S::Task, task::Column::WorkspaceId)),
        )
        .join_as(
            JoinType::LeftJoin,
            turn::Entity,
            S::Turn,
            Expr::col((S::Turn, turn::Column::Id)).equals((S::Root, task::Column::CreatedByTurnId)),
        )
        .join_as(
            JoinType::LeftJoin,
            thread::Entity,
            S::Origin,
            Condition::all()
                .add(Expr::col((S::Origin, thread::Column::Id)).eq(origin_thread()))
                .add(
                    Expr::col((S::Origin, thread::Column::WorkspaceId))
                        .equals((S::Task, task::Column::WorkspaceId)),
                ),
        )
        .join_as(
            JoinType::LeftJoin,
            thread_lineage::Entity,
            S::Lineage,
            Expr::col((S::Lineage, thread_lineage::Column::ChildThreadId))
                .equals((S::Origin, thread::Column::Id)),
        )
        .join_as(
            JoinType::LeftJoin,
            thread::Entity,
            S::Thread,
            Condition::all()
                .add(
                    Expr::col((S::Thread, thread::Column::Id)).eq(Expr::case(
                        Expr::col((S::Origin, thread::Column::AccessClass)).eq("internal"),
                        Expr::col((S::Lineage, thread_lineage::Column::RootThreadId)),
                    )
                    .finally(Expr::col((S::Origin, thread::Column::Id)))),
                )
                .add(
                    Expr::col((S::Thread, thread::Column::WorkspaceId))
                        .equals((S::Task, task::Column::WorkspaceId)),
                ),
        )
        .join_as(
            JoinType::LeftJoin,
            task_execution_admission::Entity,
            S::Admission,
            Expr::col((S::Admission, task_execution_admission::Column::TaskId))
                .equals((S::Task, task::Column::Id)),
        );
    let creator_turn_thread = Query::select()
        .expr(Expr::val(1))
        .from_as(thread::Entity, S::TurnThread)
        .and_where(
            Expr::col((S::TurnThread, thread::Column::Id))
                .equals((S::Turn, turn::Column::ThreadId)),
        )
        .and_where(
            Expr::col((S::TurnThread, thread::Column::WorkspaceId))
                .equals((S::Task, task::Column::WorkspaceId)),
        )
        .to_owned();
    query
        .cond_where(
            Condition::any()
                .add(Expr::col((S::Root, task::Column::CreatedByTurnId)).is_null())
                .add(
                    Condition::all()
                        .add(Expr::col((S::Turn, turn::Column::Id)).is_not_null())
                        .add(Expr::exists(creator_turn_thread))
                        .add(
                            Condition::any()
                                .add(explicit_origin_thread().is_null())
                                .add(
                                    Expr::col((S::Turn, turn::Column::ThreadId))
                                        .eq(explicit_origin_thread()),
                                ),
                        ),
                ),
        )
        .cond_where(
            Condition::any().add(origin_thread().is_null()).add(
                Condition::all()
                    .add(Expr::col((S::Origin, thread::Column::Id)).is_not_null())
                    .add(Expr::col((S::Origin, thread::Column::AccessClass)).is_in([
                        "private",
                        "workspace",
                        "internal",
                    ]))
                    .add(Expr::col((S::Thread, thread::Column::Id)).is_not_null())
                    .add(
                        Expr::col((S::Thread, thread::Column::AccessClass))
                            .is_in(["private", "workspace"]),
                    )
                    .add(valid_actor_scope(S::Origin))
                    .add(valid_actor_scope(S::Thread)),
            ),
        )
        .cond_where(
            Condition::any()
                .add(Expr::col((S::Admission, task_execution_admission::Column::TaskId)).is_null())
                .add(
                    Expr::col((S::Admission, task_execution_admission::Column::RootThreadId))
                        .equals((S::Thread, thread::Column::Id)),
                ),
        )
        .cond_where(
            Condition::any()
                .add(Expr::col((S::Root, task::Column::OwnerKind)).ne("user"))
                .add(
                    Condition::all()
                        .add(Func::char_length(Expr::col((S::Root, task::Column::OwnerId))).eq(21))
                        .add(
                            Expr::col((S::Root, task::Column::OwnerId))
                                .glob("*[^A-Za-z0-9]*")
                                .not(),
                        ),
                ),
        );
    query
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
    Ok(auth_session::Entity::find_by_id(session_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve session authorization scope")?
        .map(|model| SessionAuthorizationScope {
            gateway_id: model.gateway_id,
            principal_id: model.principal_id,
            session_id: model.id,
        }))
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

async fn thread_scope_from_model<C: ConnectionTrait>(
    db: &C,
    model: thread::Model,
) -> Result<Option<ThreadAuthorizationScope>> {
    let Some(workspace) =
        resolve_workspace_authorization_scope(db, model.workspace_id.as_str()).await?
    else {
        return Ok(None);
    };
    let access_class = match persisted_thread_access_class_from_db(model.access_class.as_str()) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let creator_principal_id = match actor_ref_from_db(
        model.created_by_actor_kind.as_deref(),
        model.created_by_actor_id.as_deref(),
    ) {
        Ok(Some(PersistedActorRef::Principal(principal_id))) => Some(principal_id),
        Ok(Some(PersistedActorRef::System | PersistedActorRef::AgentExecution(_)) | None) => None,
        Err(_) => return Ok(None),
    };
    Ok(Some(ThreadAuthorizationScope {
        workspace_id: model.workspace_id,
        thread_id: model.id,
        workspace_is_active: workspace.is_active,
        access_class,
        creator_principal_id,
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
    use sea_orm::{Statement, Value};
    if thread_ids.is_empty() || task_ids.is_empty() {
        return Ok(RunningTaskScopePage::default());
    }
    anyhow::ensure!(
        thread_ids.len() <= 500,
        "task activity candidate batch exceeds tree limit"
    );
    // DISTINCT selects no run fields. Task/root/thread/turn IDs, lineage child
    // and admission task are unique: at most one scope row per task. Thus all
    // <=128 candidates fit LIMIT 128, even with several active runs per task.
    anyhow::ensure!(
        task_ids.len() <= 128,
        "task scope lookup exceeds batch limit"
    );
    let task_placeholders = vec!["?"; task_ids.len()].join(",");
    let placeholders = vec!["?"; thread_ids.len()].join(",");
    let sql = format!(
        r#"
SELECT DISTINCT t.id AS task_id, rt.id AS root_thread_id, rt.access_class,
       rt.created_by_actor_kind, rt.created_by_actor_id,
       c.created_by_actor_kind AS cause_actor_kind, c.created_by_actor_id AS cause_actor_id,
       r.owner_kind, r.owner_id, w.is_active,
       EXISTS (SELECT 1 FROM workspace_membership wm
               WHERE wm.workspace_id = w.id AND wm.principal_id = ?) AS workspace_member,
       EXISTS (SELECT 1 FROM thread_membership tm
               WHERE tm.thread_id = rt.id AND tm.principal_id = ?) AS thread_member
FROM task_run tr
JOIN task t ON t.id = tr.task_id
JOIN task r ON r.id = COALESCE(t.root_task_id, t.id)
           AND r.workspace_id = t.workspace_id AND r.root_task_id IS NULL
JOIN workspace w ON w.id = t.workspace_id
LEFT JOIN turn ct ON ct.id = r.created_by_turn_id
JOIN thread c ON c.id = COALESCE(r.created_by_thread_id,
                    CASE WHEN r.owner_kind = 'thread' THEN r.owner_id END, ct.thread_id)
             AND c.workspace_id = t.workspace_id
LEFT JOIN thread_lineage l ON l.child_thread_id = c.id
JOIN thread rt ON rt.id = CASE WHEN c.access_class = 'internal' THEN l.root_thread_id ELSE c.id END
              AND rt.workspace_id = t.workspace_id
LEFT JOIN task_execution_admission a ON a.task_id = t.id
WHERE tr.status IN ('starting', 'running') AND t.workspace_id = ? AND t.id IN ({task_placeholders})
  AND rt.id IN ({placeholders}) AND rt.access_class IN ('private', 'workspace')
  AND c.access_class IN ('private', 'workspace', 'internal')
  AND (r.created_by_turn_id IS NULL OR ct.thread_id = c.id)
  AND (a.task_id IS NULL OR a.root_thread_id = rt.id)
ORDER BY t.id LIMIT 128
"#
    );
    let mut values: Vec<Value> = vec![
        principal_id.to_string().into(),
        principal_id.to_string().into(),
        workspace_id.to_owned().into(),
    ];
    values.extend(task_ids.iter().cloned().map(Into::into));
    values.extend(thread_ids.iter().cloned().map(Into::into));
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            values,
        ))
        .await
        .context("failed to load running task scopes")?;
    let mut page = RunningTaskScopePage::default();
    for row in rows {
        let task_id: String = row.try_get("", "task_id")?;
        page.after_task_id = Some(task_id.clone());
        let owner_kind: String = row.try_get("", "owner_kind")?;
        let owner_id: Option<String> = row.try_get("", "owner_id")?;
        let initiating_principal_id = if owner_kind == "user" {
            let Some(id) = owner_id.and_then(|id| PrincipalId::new(id).ok()) else {
                continue;
            };
            Some(id)
        } else {
            None
        };
        let mut creator_principal_id = None;
        let mut valid_actors = true;
        for (kind_column, id_column) in [
            ("cause_actor_kind", "cause_actor_id"),
            ("created_by_actor_kind", "created_by_actor_id"),
        ] {
            let kind: Option<String> = row.try_get("", kind_column)?;
            let id: Option<String> = row.try_get("", id_column)?;
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
        let thread_id: String = row.try_get("", "root_thread_id")?;
        let access_class: String = row.try_get("", "access_class")?;
        let workspace_is_active = row.try_get("", "is_active")?;
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
                    access_class: persisted_thread_access_class_from_db(&access_class)?,
                    creator_principal_id,
                }),
                initiating_principal_id,
            },
            row.try_get("", "workspace_member")?,
            row.try_get("", "thread_member")?,
        ));
    }
    Ok(page)
}

#[derive(Default)]
pub struct RunningTaskScopePage {
    pub after_task_id: Option<String>,
    pub scopes: Vec<(TaskAuthorizationScope, bool, bool)>,
}
