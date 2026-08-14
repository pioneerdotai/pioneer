use std::collections::BTreeSet;

use anyhow::{Context, Result};
use pioneer_entity::{
    artifact, artifact_binding, auth_session, mcp_server_installation, skill_workspace_policy,
    task, task_execution_admission, task_run, thread, thread_lineage, turn, workspace,
};
use pioneer_protocol::{PersistedActorRef, PrincipalId};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};

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
    let Some(model) = turn::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve turn authorization scope")?
    else {
        return Ok(None);
    };
    if expected_thread_id.is_some_and(|expected| expected != model.thread_id.as_str()) {
        return Ok(None);
    }
    let Some(thread) =
        resolve_thread_authorization_scope(db, model.thread_id.as_str(), expected_workspace_id)
            .await?
    else {
        return Ok(None);
    };
    Ok(Some(TurnAuthorizationScope {
        workspace_id: thread.workspace_id.clone(),
        thread_id: thread.thread_id.clone(),
        turn_id: model.id,
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
        if let Some(thread_id) = binding.thread_id {
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
        if let Some(turn_id) = binding.turn_id {
            let Some(scope) = resolve_turn_authorization_scope(
                db,
                turn_id.as_str(),
                Some(&model.workspace_id),
                None,
            )
            .await?
            else {
                return Ok(None);
            };
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

pub async fn resolve_task_authorization_scope<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    expected_workspace_id: Option<&str>,
    expected_root_thread_id: Option<&str>,
) -> Result<Option<TaskAuthorizationScope>> {
    let Some(model) = task::Entity::find_by_id(task_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve task authorization scope")?
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

    let root = if let Some(root_task_id) = model.root_task_id.as_ref() {
        let Some(root) = task::Entity::find_by_id(root_task_id.clone())
            .one(db)
            .await
            .context("failed to resolve task root authorization scope")?
        else {
            return Ok(None);
        };
        if root.workspace_id != model.workspace_id || root.root_task_id.is_some() {
            return Ok(None);
        }
        root
    } else {
        model.clone()
    };

    let mut creating_thread_id = root.created_by_thread_id.clone().or_else(|| {
        (root.owner_kind == "thread")
            .then(|| root.owner_id.clone())
            .flatten()
    });
    if let Some(turn_id) = root.created_by_turn_id.as_ref() {
        let Some(turn_scope) = resolve_turn_authorization_scope(
            db,
            turn_id.as_str(),
            Some(model.workspace_id.as_str()),
            creating_thread_id.as_deref(),
        )
        .await?
        else {
            return Ok(None);
        };
        creating_thread_id.get_or_insert(turn_scope.thread_id);
    }

    // `created_by_thread_id` is exact causal provenance and may identify an
    // internal child. Authorization belongs to that child's durable
    // collaboration root, never to the initiating principal and never to the
    // internal row itself.
    let root_thread_id = match creating_thread_id.as_deref() {
        Some(thread_id) => {
            artifact_authorization_root_thread_id(db, model.workspace_id.as_str(), thread_id)
                .await?
        }
        None => None,
    };
    if expected_root_thread_id.is_some_and(|expected| root_thread_id.as_deref() != Some(expected)) {
        return Ok(None);
    }

    if let Some(admission) = task_execution_admission::Entity::find_by_id(model.id.clone())
        .one(db)
        .await
        .context("failed to resolve Task execution admission authority")?
        && root_thread_id.as_deref() != Some(admission.root_thread_id.as_str())
    {
        return Ok(None);
    }
    let root_thread = match root_thread_id.as_deref() {
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
            if scope.access_class == PersistedThreadAccessClass::Internal {
                return Ok(None);
            }
            Some(scope)
        }
        None => None,
    };
    let initiating_principal_id = if root.owner_kind == "user" {
        let Some(owner_id) = root.owner_id.as_deref() else {
            return Ok(None);
        };
        match PrincipalId::new(owner_id) {
            Ok(principal_id) => Some(principal_id),
            Err(_) => return Ok(None),
        }
    } else {
        None
    };

    Ok(Some(TaskAuthorizationScope {
        workspace_id: model.workspace_id,
        root_thread_id,
        task_id: model.id,
        workspace_is_active: workspace.is_active,
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
        Ok(Some(PersistedActorRef::System) | None) => None,
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
