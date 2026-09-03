use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result, bail};
use pioneer_crud::{CrudStore, PersistedThreadAccessClass};
use pioneer_protocol::{
    AuthSessionId, DeviceId, PrincipalId, PrincipalKind, PrincipalStatus, RoleKey, TurnStatus,
};
use tokio::sync::RwLock;

use crate::auth::AuthenticatedSessionPrincipal;

use super::{
    AccessChangeSignal, AuthorizationService, ExecutionAuthorizationContext,
    ExecutionContinuityPolicy, ResolvedResourceAccess, ResourceAction,
    RevalidatedExecutionAuthorization, RuntimePrincipalPolicy, ThreadAccessClass,
    ThreadAccessFacts, ThreadResourceClass, WorkspaceAccessFacts,
};

const EXECUTION_AUTHORITY_DEVICE_ID: &str = "DExecutionAuthority00";
const EXECUTION_AUTHORITY_SESSION_ID: &str = "SExecutionAuthority00";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecutionLeaseState {
    Active,
    Fenced,
    StopRequested,
}

#[derive(Clone, Debug)]
struct ExecutionLease {
    execution_id: String,
    workspace_id: String,
    root_thread_id: String,
    actor_principal_id: PrincipalId,
    actor_session_id: pioneer_protocol::AuthSessionId,
    admitted_role_key: String,
    admitted_policy_generation: u64,
    projected_policy_generation: u64,
    continuity_policy: ExecutionContinuityPolicy,
    continuation_action: ResourceAction,
    continuation_provider: Option<(String, String)>,
    continuation_cli_runtime: Option<(String, String)>,
    granted_skill_ids: Vec<String>,
    granted_mcp_server_ids: Vec<String>,
    admitted_permission_cap: pioneer_protocol::TurnPermissionProfileCap,
    immutable_actions: BTreeSet<ResourceAction>,
    projected_actions: BTreeSet<ResourceAction>,
    collaborator_ids: BTreeSet<PrincipalId>,
    action_collaborators: BTreeMap<ResourceAction, BTreeSet<PrincipalId>>,
    state: ExecutionLeaseState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionLeaseGuard {
    pub(crate) execution_id: String,
    pub(crate) policy_generation: u64,
    pub(crate) actor_principal_id: PrincipalId,
    pub(crate) actor_session_id: pioneer_protocol::AuthSessionId,
    pub(crate) authorizing_principal_id: PrincipalId,
    pub(crate) admitted_policy_generation: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ExecutionLeaseReprojection {
    pub(crate) fenced_execution_ids: Vec<String>,
    pub(crate) affected_root_thread_ids: Vec<String>,
}

#[derive(Default)]
pub(crate) struct ExecutionLeaseRegistry {
    leases: RwLock<HashMap<String, ExecutionLease>>,
}

impl ExecutionLeaseRegistry {
    pub(crate) async fn register(
        &self,
        store: &CrudStore,
        execution_id: &str,
        context: &ExecutionAuthorizationContext,
        policy_generation: u64,
    ) -> Result<ExecutionLeaseGuard> {
        if execution_id.trim().is_empty() || execution_id != execution_id.trim() {
            bail!("execution lease requires an exact execution id");
        }
        let continuation_action = context.continuation_action();
        let immutable_actions = context
            .granted_action_names()
            .iter()
            .filter_map(|name| ResourceAction::from_safe_name(name.as_str()))
            .collect::<BTreeSet<_>>();
        if !immutable_actions.contains(&continuation_action) {
            bail!("execution admission does not contain its backend continuation action");
        }
        let lease = ExecutionLease {
            execution_id: execution_id.to_owned(),
            workspace_id: context.workspace_id().to_owned(),
            root_thread_id: context.root_thread_id().to_owned(),
            actor_principal_id: context.initiating_principal_id().clone(),
            actor_session_id: context.initiating_session_id().clone(),
            admitted_role_key: context.role_key().to_owned(),
            admitted_policy_generation: context.policy_revision(),
            projected_policy_generation: policy_generation,
            continuity_policy: context.continuity_policy(),
            continuation_action,
            continuation_provider: context
                .continuation_provider()
                .map(|(provider, model)| (provider.to_owned(), model.to_owned())),
            continuation_cli_runtime: context
                .continuation_cli_runtime()
                .map(|(runtime_id, model)| (runtime_id.to_owned(), model.to_owned())),
            granted_skill_ids: context.granted_skill_ids().to_vec(),
            granted_mcp_server_ids: context.granted_mcp_server_ids().to_vec(),
            admitted_permission_cap: context.permission_profile_cap().clone(),
            immutable_actions,
            projected_actions: BTreeSet::new(),
            collaborator_ids: BTreeSet::new(),
            action_collaborators: BTreeMap::new(),
            state: ExecutionLeaseState::Fenced,
        };
        let projected = project_lease(store, lease).await?;
        if projected.state != ExecutionLeaseState::Active {
            bail!("execution lease has no current collaborator authority");
        }
        let continuation_collaborators =
            projected
                .action_collaborators
                .get(&continuation_action)
                .context("execution lease has no continuation collaborators")?;
        let authorizing_principal =
            preferred_collaboration_authority_principal(store, continuation_collaborators)
                .await?
                .context("execution lease has no continuation collaborator")?;
        let guard = ExecutionLeaseGuard {
            execution_id: execution_id.to_owned(),
            policy_generation: projected.projected_policy_generation,
            actor_principal_id: projected.actor_principal_id.clone(),
            actor_session_id: projected.actor_session_id.clone(),
            authorizing_principal_id: authorizing_principal.principal_id,
            admitted_policy_generation: projected.admitted_policy_generation,
        };
        self.leases
            .write()
            .await
            .insert(execution_id.to_owned(), projected);
        Ok(guard)
    }

    pub(crate) async fn guard(
        &self,
        store: &CrudStore,
        execution_id: &str,
        action: ResourceAction,
        current_policy_generation: u64,
    ) -> Result<ExecutionLeaseGuard> {
        let scope = pioneer_crud::resolve_turn_authorization_scope(
            &store.database_connection(),
            execution_id,
            None,
            None,
        )
        .await?
        .context("execution turn no longer exists")?;
        let (_, turn) = store
            .get_turn(scope.thread_id.as_str(), execution_id)
            .await?
            .context("execution turn disappeared during lease validation")?;
        if turn.status != TurnStatus::InProgress {
            bail!("execution lease is not active");
        }
        let needs_restore = !self.leases.read().await.contains_key(execution_id);
        if needs_restore {
            let context = ExecutionAuthorizationContext::load_for_turn(store, execution_id).await?;
            self.register(store, execution_id, &context, current_policy_generation)
                .await?;
        }

        let stale = self
            .leases
            .read()
            .await
            .get(execution_id)
            .is_none_or(|lease| lease.projected_policy_generation != current_policy_generation);
        if stale {
            self.reproject_execution(store, execution_id, current_policy_generation)
                .await?;
        }

        let (
            action_collaborators,
            projected_policy_generation,
            actor_principal_id,
            actor_session_id,
            admitted_policy_generation,
        ) = {
            let leases = self.leases.read().await;
            let lease = leases
                .get(execution_id)
                .context("execution lease disappeared during validation")?;
            if lease.state != ExecutionLeaseState::Active
                || !lease.immutable_actions.contains(&action)
                || !lease.projected_actions.contains(&action)
            {
                bail!(
                    "execution lease is fenced for action `{}`",
                    action.safe_name()
                );
            }
            (
                lease
                    .action_collaborators
                    .get(&action)
                    .cloned()
                    .context("execution action has no current collaboration authority")?,
                lease.projected_policy_generation,
                lease.actor_principal_id.clone(),
                lease.actor_session_id.clone(),
                lease.admitted_policy_generation,
            )
        };
        let authorizing_principal_id =
            preferred_collaboration_authority_principal(store, &action_collaborators)
                .await?
                .context("execution action has no current collaboration authority")?
                .principal_id;
        Ok(ExecutionLeaseGuard {
            execution_id: execution_id.to_owned(),
            policy_generation: projected_policy_generation,
            actor_principal_id,
            actor_session_id,
            authorizing_principal_id,
            admitted_policy_generation,
        })
    }

    /// Returns `Ok(true)` only when the already-projected continuation lease
    /// is sufficient to admit a lossy progress fragment without storage I/O.
    /// Missing and stale leases return `Ok(false)` so the caller can restore
    /// or reproject through the full durable guard. A current fenced lease is
    /// an authoritative denial and must never be treated as a cache miss.
    pub(crate) async fn guard_progress_cached(
        &self,
        execution_id: &str,
        current_policy_generation: u64,
    ) -> Result<bool> {
        if execution_id.trim().is_empty() || execution_id != execution_id.trim() {
            bail!("execution lease requires an exact execution id");
        }
        let leases = self.leases.read().await;
        let Some(lease) = leases.get(execution_id) else {
            return Ok(false);
        };
        if lease.projected_policy_generation != current_policy_generation {
            return Ok(false);
        }
        if lease.state != ExecutionLeaseState::Active
            || !lease.immutable_actions.contains(&lease.continuation_action)
            || !lease.projected_actions.contains(&lease.continuation_action)
        {
            bail!(
                "execution lease is fenced for action `{}`",
                lease.continuation_action.safe_name()
            );
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn revalidate_for_turn(
        &self,
        store: &CrudStore,
        context: &ExecutionAuthorizationContext,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        action: ResourceAction,
        current_policy_generation: u64,
    ) -> Result<RevalidatedExecutionAuthorization> {
        let guard = self
            .guard(store, turn_id, action, current_policy_generation)
            .await?;
        let principal =
            collaboration_authority_principal(store, &guard.authorizing_principal_id).await?;
        context
            .revalidate_for_collaborator(
                store,
                &principal,
                workspace_id,
                thread_id,
                turn_id,
                action,
                current_policy_generation,
            )
            .await
    }

    pub(crate) async fn revalidate_context(
        &self,
        store: &CrudStore,
        context: &ExecutionAuthorizationContext,
        action: ResourceAction,
        current_policy_generation: u64,
    ) -> Result<RevalidatedExecutionAuthorization> {
        if !context.grants_action(action) {
            bail!(
                "execution admission manifest does not grant action `{}`",
                action.safe_name()
            );
        }
        let continuation_action = context.continuation_action();
        let immutable_actions = context
            .granted_action_names()
            .iter()
            .filter_map(|name| ResourceAction::from_safe_name(name.as_str()))
            .collect::<BTreeSet<_>>();
        let projected = project_lease(
            store,
            ExecutionLease {
                execution_id: "context-projection".to_owned(),
                workspace_id: context.workspace_id().to_owned(),
                root_thread_id: context.root_thread_id().to_owned(),
                actor_principal_id: context.initiating_principal_id().clone(),
                actor_session_id: context.initiating_session_id().clone(),
                admitted_role_key: context.role_key().to_owned(),
                admitted_policy_generation: context.policy_revision(),
                projected_policy_generation: current_policy_generation,
                continuity_policy: context.continuity_policy(),
                continuation_action,
                continuation_provider: context
                    .continuation_provider()
                    .map(|(provider, model)| (provider.to_owned(), model.to_owned())),
                continuation_cli_runtime: context
                    .continuation_cli_runtime()
                    .map(|(runtime_id, model)| (runtime_id.to_owned(), model.to_owned())),
                granted_skill_ids: context.granted_skill_ids().to_vec(),
                granted_mcp_server_ids: context.granted_mcp_server_ids().to_vec(),
                admitted_permission_cap: context.permission_profile_cap().clone(),
                immutable_actions,
                projected_actions: BTreeSet::new(),
                collaborator_ids: BTreeSet::new(),
                action_collaborators: BTreeMap::new(),
                state: ExecutionLeaseState::Fenced,
            },
        )
        .await?;
        if projected.state != ExecutionLeaseState::Active {
            bail!("execution context has no current backend continuation authority");
        }
        let action_collaborators = projected
            .action_collaborators
            .get(&action)
            .context("execution context action has no current collaborator")?;
        let principal = preferred_collaboration_authority_principal(store, action_collaborators)
            .await?
            .context("execution context action has no current collaborator")?;
        context
            .revalidate_root_for_collaborator(store, &principal, action, current_policy_generation)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn revalidate_post_turn(
        &self,
        store: &CrudStore,
        context: &ExecutionAuthorizationContext,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        action: ResourceAction,
        current_policy_generation: u64,
    ) -> Result<RevalidatedExecutionAuthorization> {
        context
            .verify_turn_scope(store, workspace_id, thread_id, turn_id)
            .await?;
        self.revalidate_context(store, context, action, current_policy_generation)
            .await
    }

    pub(crate) async fn reproject_scope(
        &self,
        store: &CrudStore,
        signal: &AccessChangeSignal,
    ) -> Result<ExecutionLeaseReprojection> {
        let execution_ids = self
            .leases
            .read()
            .await
            .values()
            .filter(|lease| {
                lease.workspace_id == signal.workspace_id
                    && signal
                        .thread_id
                        .as_deref()
                        .is_none_or(|thread_id| thread_id == lease.root_thread_id)
            })
            .map(|lease| lease.execution_id.clone())
            .collect::<Vec<_>>();
        let mut outcome = ExecutionLeaseReprojection::default();
        for execution_id in execution_ids {
            let lease = self
                .reproject_execution(store, execution_id.as_str(), signal.authorization_revision)
                .await?;
            if lease.state != ExecutionLeaseState::Active {
                outcome.fenced_execution_ids.push(execution_id.clone());
                outcome
                    .affected_root_thread_ids
                    .push(lease.root_thread_id.clone());
            }
        }
        outcome.fenced_execution_ids.sort();
        outcome.affected_root_thread_ids.sort();
        outcome.affected_root_thread_ids.dedup();
        Ok(outcome)
    }

    pub(crate) async fn finish(&self, execution_id: &str) {
        self.leases.write().await.remove(execution_id);
    }

    async fn reproject_execution(
        &self,
        store: &CrudStore,
        execution_id: &str,
        policy_generation: u64,
    ) -> Result<ExecutionLease> {
        let lease = self
            .leases
            .read()
            .await
            .get(execution_id)
            .cloned()
            .context("execution lease is unavailable")?;
        let mut projected = project_lease(store, lease).await?;
        projected.projected_policy_generation = policy_generation;
        self.leases
            .write()
            .await
            .insert(execution_id.to_owned(), projected.clone());
        Ok(projected)
    }
}

async fn project_lease(store: &CrudStore, mut lease: ExecutionLease) -> Result<ExecutionLease> {
    let policy = AuthorizationService::new();
    let scope = pioneer_crud::resolve_thread_authorization_scope(
        &store.database_connection(),
        lease.root_thread_id.as_str(),
        Some(lease.workspace_id.as_str()),
    )
    .await?
    .context("execution root thread no longer exists")?;
    if !scope.workspace_is_active || scope.access_class == PersistedThreadAccessClass::Internal {
        lease.state = terminal_state_for_policy(lease.continuity_policy);
        lease.projected_actions.clear();
        lease.collaborator_ids.clear();
        lease.action_collaborators.clear();
        return Ok(lease);
    }

    // A collaborator's broader role may authorize control of an already
    // admitted execution, but it must never silently widen the execution's
    // own current role ceiling. Re-resolve the role recorded in the durable
    // admission envelope on every projection so a code-defined policy change
    // fences provider/CLI continuation and selected operational resources
    // before the next side effect, even while an absolute Superuser exists.
    let admitted_role_key = RoleKey::new(lease.admitted_role_key.clone())
        .context("execution admission role key is invalid")?;
    let admitted_role = super::RoleDefinitionRegistry::new()
        .resolve_key(&admitted_role_key)
        .context("execution admission role is no longer registered")?;
    let admitted_role_argument =
        (admitted_role.principal_kind == PrincipalKind::User).then_some(&admitted_role_key);
    let current_admitted_role_cap = policy
        .turn_permission_profile_cap(admitted_role.principal_kind, admitted_role_argument)
        .context("execution admission role has no current permission ceiling")?;
    let admitted_permission_profile =
        pioneer_protocol::task_permission_cap_snapshot(&lease.admitted_permission_cap);
    let current_admitted_role_profile =
        pioneer_protocol::task_permission_cap_snapshot(&current_admitted_role_cap);
    let admitted_role_cap_covers_execution = pioneer_protocol::intersect_turn_permission_profiles(
        &admitted_permission_profile,
        &current_admitted_role_profile,
        pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
    ) == admitted_permission_profile;
    let admitted_role_backend_allowed = match (
        lease.continuation_provider.as_ref(),
        lease.continuation_cli_runtime.as_ref(),
    ) {
        (Some((provider, model)), None) => policy.provider_model_allowed(
            admitted_role.principal_kind,
            admitted_role_argument,
            provider.as_str(),
            model.as_str(),
        ),
        (None, Some((runtime_id, model))) => policy.cli_model_allowed(
            admitted_role.principal_kind,
            admitted_role_argument,
            runtime_id.as_str(),
            model.as_str(),
        ),
        _ => false,
    };
    let admitted_role_skills_allowed = lease.granted_skill_ids.iter().all(|skill_id| {
        policy.skill_allowed(
            admitted_role.principal_kind,
            admitted_role_argument,
            skill_id.as_str(),
        )
    });
    let admitted_role_mcp_allowed = lease.granted_mcp_server_ids.iter().all(|server_id| {
        policy.mcp_server_allowed(
            admitted_role.principal_kind,
            admitted_role_argument,
            server_id.as_str(),
        )
    });
    let admitted_role_allows_continuation = admitted_role_cap_covers_execution
        && admitted_role_backend_allowed
        && admitted_role_skills_allowed
        && admitted_role_mcp_allowed;

    // Evaluate every active registered principal through the same role policy.
    // Scoped roles must prove current root collaboration membership; absolute
    // roles are admitted by their data-defined resource policy. This avoids
    // both actor-only ownership and special branches for named roles.
    let candidate_ids = pioneer_crud::list_gateway_principals(&store.database_connection())
        .await?
        .into_iter()
        .filter_map(|principal| PrincipalId::new(principal.id).ok())
        .collect::<BTreeSet<_>>();

    let mut projected_actions = BTreeSet::new();
    let mut collaborator_ids = BTreeSet::new();
    let mut action_collaborators = BTreeMap::<ResourceAction, BTreeSet<PrincipalId>>::new();
    for principal_id in candidate_ids {
        let Some(principal) =
            pioneer_crud::load_principal_by_id(&store.database_connection(), &principal_id).await?
        else {
            continue;
        };
        if principal.status != PrincipalStatus::Active {
            continue;
        }
        let role_key = match principal.kind {
            PrincipalKind::Superuser if principal.role_key.is_none() => None,
            PrincipalKind::User => Some(
                principal
                    .role_key
                    .as_deref()
                    .map(RoleKey::new)
                    .transpose()?
                    .context("execution collaborator has no registered role")?,
            ),
            PrincipalKind::Superuser => continue,
        };
        let Some(runtime_policy) =
            policy.runtime_principal_policy(principal.kind, role_key.as_ref())
        else {
            continue;
        };
        let Some(current_permission_cap) =
            policy.turn_permission_profile_cap(principal.kind, role_key.as_ref())
        else {
            continue;
        };
        let current_permission_profile =
            pioneer_protocol::task_permission_cap_snapshot(&current_permission_cap);
        let current_permission_cap_covers_admission =
            pioneer_protocol::intersect_turn_permission_profiles(
                &admitted_permission_profile,
                &current_permission_profile,
                pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
            ) == admitted_permission_profile;
        let workspace_member = pioneer_crud::find_workspace_membership(
            &store.database_connection(),
            &principal_id,
            lease.workspace_id.as_str(),
        )
        .await?
        .is_some();
        let thread_member = if scope.access_class == PersistedThreadAccessClass::Private {
            pioneer_crud::find_thread_membership(
                &store.database_connection(),
                lease.root_thread_id.as_str(),
                &principal_id,
            )
            .await?
            .is_some()
        } else {
            false
        };
        if runtime_policy == RuntimePrincipalPolicy::ScopedCollaboration
            && (!workspace_member
                || (scope.access_class == PersistedThreadAccessClass::Private && !thread_member))
        {
            continue;
        }
        let backend_allowed = match (
            lease.continuation_provider.as_ref(),
            lease.continuation_cli_runtime.as_ref(),
        ) {
            (Some((provider, model)), None) => policy.provider_model_allowed(
                principal.kind,
                role_key.as_ref(),
                provider.as_str(),
                model.as_str(),
            ),
            (None, Some((runtime_id, model))) => policy.cli_model_allowed(
                principal.kind,
                role_key.as_ref(),
                runtime_id.as_str(),
                model.as_str(),
            ),
            _ => false,
        };
        let facts = ThreadAccessFacts {
            workspace: WorkspaceAccessFacts {
                workspace_active: true,
                workspace_member,
            },
            access_class: match scope.access_class {
                PersistedThreadAccessClass::Private => ThreadAccessClass::Private,
                PersistedThreadAccessClass::Workspace => ThreadAccessClass::Workspace,
                PersistedThreadAccessClass::Internal => ThreadAccessClass::Internal,
            },
            resource_class: ThreadResourceClass::Root,
            thread_member,
            thread_creator: scope.creator_principal_id.as_ref() == Some(&principal_id),
        };
        let mut principal_has_action = false;
        for action in lease.immutable_actions.iter().copied() {
            // Provider/model selectors constrain admission and continuation of
            // that backend. They must not erase independent collaboration
            // actions over an already admitted execution. A future observer or
            // approver may therefore observe/respond/cancel without gaining
            // authority to start or resume the selected backend.
            if action == lease.continuation_action
                && (!admitted_role_allows_continuation
                    || !backend_allowed
                    || !current_permission_cap_covers_admission)
            {
                continue;
            }
            if action == ResourceAction::SkillUse
                && (!admitted_role_skills_allowed
                    || lease.granted_skill_ids.iter().any(|skill_id| {
                        !policy.skill_allowed(principal.kind, role_key.as_ref(), skill_id.as_str())
                    }))
            {
                continue;
            }
            if action == ResourceAction::McpUse
                && (!admitted_role_mcp_allowed
                    || lease.granted_mcp_server_ids.iter().any(|server_id| {
                        !policy.mcp_server_allowed(
                            principal.kind,
                            role_key.as_ref(),
                            server_id.as_str(),
                        )
                    }))
            {
                continue;
            }
            let gate = policy.authorize_action(principal.kind, role_key.as_ref(), action);
            if policy
                .authorize_resource(&gate, action, ResolvedResourceAccess::Thread(facts))
                .is_allowed()
            {
                projected_actions.insert(action);
                action_collaborators
                    .entry(action)
                    .or_default()
                    .insert(principal_id.clone());
                principal_has_action = true;
            }
        }
        if principal_has_action {
            collaborator_ids.insert(principal_id);
        }
    }

    lease.projected_actions = projected_actions;
    lease.collaborator_ids = collaborator_ids;
    lease.action_collaborators = action_collaborators;
    lease.state = if lease.projected_actions.contains(&lease.continuation_action) {
        ExecutionLeaseState::Active
    } else {
        terminal_state_for_policy(lease.continuity_policy)
    };
    Ok(lease)
}

async fn collaboration_authority_principal(
    store: &CrudStore,
    principal_id: &PrincipalId,
) -> Result<AuthenticatedSessionPrincipal> {
    let principal = pioneer_crud::load_principal_by_id(&store.database_connection(), principal_id)
        .await?
        .context("execution collaboration principal no longer exists")?;
    if principal.status != PrincipalStatus::Active {
        bail!("execution collaboration principal is not active");
    }
    let role_key = match principal.kind {
        PrincipalKind::Superuser if principal.role_key.is_none() => None,
        PrincipalKind::User => Some(
            principal
                .role_key
                .as_deref()
                .map(RoleKey::new)
                .transpose()?
                .context("execution collaboration principal has no role")?,
        ),
        PrincipalKind::Superuser => {
            bail!("execution collaboration Superuser has an invalid role")
        }
    };
    Ok(AuthenticatedSessionPrincipal {
        gateway_id: principal.gateway_id,
        principal_id: principal.id,
        kind: principal.kind,
        role_key,
        // These sentinels make the projection explicitly non-bearer. The
        // initiating session remains separately preserved on the lease as
        // actor provenance and is never substituted for collaboration proof.
        device_id: DeviceId::new(EXECUTION_AUTHORITY_DEVICE_ID)
            .expect("static execution authority device id is valid"),
        session_id: AuthSessionId::new(EXECUTION_AUTHORITY_SESSION_ID)
            .expect("static execution authority session id is valid"),
        access_jti: "non-bearer-execution-collaboration".to_owned(),
        access_expires_at_unix: 0,
    })
}

async fn preferred_collaboration_authority_principal(
    store: &CrudStore,
    principal_ids: &BTreeSet<PrincipalId>,
) -> Result<Option<AuthenticatedSessionPrincipal>> {
    let policy = AuthorizationService::new();
    let mut absolute = None;
    for principal_id in principal_ids {
        let principal = collaboration_authority_principal(store, principal_id).await?;
        match policy.runtime_principal_policy(principal.kind, principal.role_key.as_ref()) {
            Some(RuntimePrincipalPolicy::ScopedCollaboration) => return Ok(Some(principal)),
            Some(RuntimePrincipalPolicy::Absolute) if absolute.is_none() => {
                absolute = Some(principal);
            }
            Some(RuntimePrincipalPolicy::Absolute) | None => {}
        }
    }
    Ok(absolute)
}

const fn terminal_state_for_policy(policy: ExecutionContinuityPolicy) -> ExecutionLeaseState {
    match policy {
        ExecutionContinuityPolicy::ContinueShared
        | ExecutionContinuityPolicy::FenceOnAuthorityLoss => ExecutionLeaseState::Fenced,
        ExecutionContinuityPolicy::StopOnAuthorityLoss => ExecutionLeaseState::StopRequested,
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::ConnectionTrait;

    use super::*;
    use crate::tests::authorization::{
        IsolatedEpic4Harness, MEMBER_A_ID, MEMBER_B_ID, THREAD_RED_PRIVATE_A_ID, WORKSPACE_RED_ID,
    };

    fn principal(
        principal_id: &str,
        kind: PrincipalKind,
        role_key: Option<RoleKey>,
        device_id: &str,
        session_id: &str,
    ) -> AuthenticatedSessionPrincipal {
        AuthenticatedSessionPrincipal {
            gateway_id: pioneer_protocol::GatewayId::new(
                crate::auth::test_support::TEST_GATEWAY_ID,
            )
            .expect("gateway id"),
            principal_id: PrincipalId::new(principal_id).expect("principal id"),
            kind,
            role_key,
            device_id: DeviceId::new(device_id).expect("device id"),
            session_id: AuthSessionId::new(session_id).expect("session id"),
            access_jti: "non-bearer-lease-test".to_owned(),
            access_expires_at_unix: 0,
        }
    }

    async fn add_member_b_to_private_root(harness: &IsolatedEpic4Harness) {
        harness
            .database
            .execute_unprepared(&format!(
                "INSERT INTO thread_membership(\
                    thread_id,principal_id,added_by_actor_kind,added_by_actor_id,\
                    created_at,updated_at\
                 ) VALUES(\
                    '{THREAD_RED_PRIVATE_A_ID}','{MEMBER_B_ID}','system',NULL,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
            ))
            .await
            .expect("add Member B to private root");
    }

    #[tokio::test]
    async fn current_role_permission_cap_fences_a_wider_admitted_execution() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated authorization fixture");
        let store = CrudStore::new(harness.database.clone());
        let member = principal(
            MEMBER_A_ID,
            PrincipalKind::User,
            Some(RoleKey::new("member").expect("member role")),
            "D00000000000000000001",
            "S00000000000000000001",
        );
        let wide_context = ExecutionAuthorizationContext::for_test(
            &member,
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            None,
        );
        let registry = ExecutionLeaseRegistry::default();
        let error = registry
            .register(&store, "V00000000000000000009", &wide_context, 2)
            .await
            .expect_err("current supervised role must not continue a full-access admission");
        assert!(format!("{error:#}").contains("no current collaborator authority"));

        let supervised = pioneer_protocol::compile_turn_permission_profile(
            pioneer_protocol::TurnPermissionMode::Supervised,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );
        let supervised_context = ExecutionAuthorizationContext::for_test(
            &member,
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            &supervised,
            None,
        );
        registry
            .register(&store, "V00000000000000000010", &supervised_context, 2)
            .await
            .expect("equal current role cap keeps the execution active");
    }

    #[tokio::test]
    async fn progress_cache_admits_only_current_active_continuation_lease() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated authorization fixture");
        let store = CrudStore::new(harness.database.clone());
        let member = principal(
            MEMBER_A_ID,
            PrincipalKind::User,
            Some(RoleKey::member()),
            "D00000000000000000001",
            "S00000000000000000001",
        );
        let supervised = pioneer_protocol::compile_turn_permission_profile(
            pioneer_protocol::TurnPermissionMode::Supervised,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );
        let context = ExecutionAuthorizationContext::for_test(
            &member,
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            &supervised,
            None,
        );
        let registry = ExecutionLeaseRegistry::default();
        let execution_id = "V00000000000000000010";
        registry
            .register(&store, execution_id, &context, 7)
            .await
            .expect("register current execution lease");

        assert!(
            registry
                .guard_progress_cached(execution_id, 7)
                .await
                .expect("current progress lease should validate")
        );
        assert!(
            !registry
                .guard_progress_cached(execution_id, 8)
                .await
                .expect("stale progress lease should request durable reprojection")
        );

        registry
            .leases
            .write()
            .await
            .get_mut(execution_id)
            .expect("registered lease")
            .state = ExecutionLeaseState::Fenced;
        assert!(
            registry
                .guard_progress_cached(execution_id, 7)
                .await
                .expect_err("a current fenced lease must fail closed")
                .to_string()
                .contains("fenced")
        );

        registry.finish(execution_id).await;
        assert!(
            !registry
                .guard_progress_cached(execution_id, 7)
                .await
                .expect("a missing lease should request durable restoration")
        );
    }

    #[tokio::test]
    async fn superuser_started_execution_is_controlled_by_every_current_thread_member() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated authorization fixture");
        add_member_b_to_private_root(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let superuser = principal(
            crate::auth::test_support::TEST_SUPERUSER_ID,
            PrincipalKind::Superuser,
            None,
            "D00000000000000000001",
            "S00000000000000000001",
        );
        let context = ExecutionAuthorizationContext::for_test(
            &superuser,
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            None,
        );
        let registry = ExecutionLeaseRegistry::default();
        registry
            .register(&store, "V00000000000000000001", &context, 1)
            .await
            .expect("register collaborative lease");

        let leases = registry.leases.read().await;
        let lease = leases
            .get("V00000000000000000001")
            .expect("registered lease");
        let member_a = PrincipalId::new(MEMBER_A_ID).unwrap();
        let member_b = PrincipalId::new(MEMBER_B_ID).unwrap();
        for action in [
            ResourceAction::AgentExecutionObserve,
            ResourceAction::AgentExecutionCancel,
            ResourceAction::AgentExecutionSteer,
            ResourceAction::AgentRequestRespond,
            ResourceAction::TaskCreate,
        ] {
            let collaborators = lease
                .action_collaborators
                .get(&action)
                .expect("Member action is projected");
            assert!(collaborators.contains(&member_a));
            assert!(collaborators.contains(&member_b));
        }
    }

    #[tokio::test]
    async fn participant_add_reprojects_running_execution_without_fencing_it() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated authorization fixture");
        let store = CrudStore::new(harness.database.clone());
        let member_a = principal(
            MEMBER_A_ID,
            PrincipalKind::User,
            Some(RoleKey::member()),
            "D0000000000000000000A",
            "S0000000000000000000A",
        );
        let supervised = pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
            pioneer_protocol::TurnPermissionMode::Supervised,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );
        let context = ExecutionAuthorizationContext::for_test(
            &member_a,
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            &supervised,
            None,
        );
        let registry = ExecutionLeaseRegistry::default();
        registry
            .register(&store, "V00000000000000000004", &context, 1)
            .await
            .expect("register running collaborative execution");

        let member_b = PrincipalId::new(MEMBER_B_ID).expect("Member B id");
        {
            let leases = registry.leases.read().await;
            let lease = leases
                .get("V00000000000000000004")
                .expect("registered lease");
            assert!(
                lease
                    .action_collaborators
                    .get(&ResourceAction::AgentRequestRespond)
                    .is_none_or(|ids| !ids.contains(&member_b))
            );
        }

        add_member_b_to_private_root(&harness).await;
        let signal = crate::authorization::AuthorizationInvalidationHub::default()
            .publish(
                pioneer_protocol::AccessChangeKind::ThreadParticipantAdded,
                Some(member_b.clone()),
                WORKSPACE_RED_ID,
                Some(THREAD_RED_PRIVATE_A_ID.to_owned()),
            )
            .await
            .expect("publish participant-add invalidation");
        let reprojection = registry
            .reproject_scope(&store, &signal)
            .await
            .expect("reproject shared execution");
        assert!(
            reprojection.fenced_execution_ids.is_empty(),
            "adding a collaborator must not stop the running shared execution"
        );

        let leases = registry.leases.read().await;
        let lease = leases
            .get("V00000000000000000004")
            .expect("reprojected lease");
        for action in [
            ResourceAction::AgentExecutionCancel,
            ResourceAction::AgentExecutionSteer,
            ResourceAction::AgentRequestRespond,
            ResourceAction::TaskCreate,
        ] {
            assert!(
                lease
                    .action_collaborators
                    .get(&action)
                    .is_some_and(|ids| ids.contains(&member_b)),
                "new collaborator must receive current `{}` authority",
                action.safe_name()
            );
        }
    }

    #[tokio::test]
    async fn collaboration_action_does_not_require_backend_start_authority() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated authorization fixture");
        add_member_b_to_private_root(&harness).await;
        harness
            .database
            .execute_unprepared(&format!(
                "UPDATE gateway_principal SET role_key='synthetic_observer' \
                 WHERE id='{MEMBER_B_ID}'"
            ))
            .await
            .expect("assign test observer role");
        let store = CrudStore::new(harness.database.clone());
        let superuser = principal(
            crate::auth::test_support::TEST_SUPERUSER_ID,
            PrincipalKind::Superuser,
            None,
            "D00000000000000000001",
            "S00000000000000000001",
        );
        let context = ExecutionAuthorizationContext::for_test(
            &superuser,
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            None,
        );
        let projected = project_lease(
            &store,
            ExecutionLease {
                execution_id: "V00000000000000000002".to_owned(),
                workspace_id: WORKSPACE_RED_ID.to_owned(),
                root_thread_id: THREAD_RED_PRIVATE_A_ID.to_owned(),
                actor_principal_id: superuser.principal_id.clone(),
                actor_session_id: superuser.session_id.clone(),
                admitted_role_key: context.role_key().to_owned(),
                admitted_policy_generation: 1,
                projected_policy_generation: 1,
                continuity_policy: ExecutionContinuityPolicy::StopOnAuthorityLoss,
                continuation_action: context.continuation_action(),
                continuation_provider: context
                    .continuation_provider()
                    .map(|(provider, model)| (provider.to_owned(), model.to_owned())),
                continuation_cli_runtime: None,
                granted_skill_ids: Vec::new(),
                granted_mcp_server_ids: Vec::new(),
                admitted_permission_cap: context.permission_profile_cap().clone(),
                immutable_actions: context
                    .granted_action_names()
                    .iter()
                    .filter_map(|name| ResourceAction::from_safe_name(name))
                    .collect(),
                projected_actions: BTreeSet::new(),
                collaborator_ids: BTreeSet::new(),
                action_collaborators: BTreeMap::new(),
                state: ExecutionLeaseState::Fenced,
            },
        )
        .await
        .expect("project lease");
        let observer = PrincipalId::new(MEMBER_B_ID).unwrap();
        assert!(
            projected
                .action_collaborators
                .get(&ResourceAction::AgentExecutionObserve)
                .is_some_and(|ids| ids.contains(&observer))
        );
        assert!(
            projected
                .action_collaborators
                .get(&ResourceAction::ProviderUse)
                .is_none_or(|ids| !ids.contains(&observer))
        );
        assert!(
            projected
                .action_collaborators
                .get(&ResourceAction::AgentExecutionCancel)
                .is_none_or(|ids| !ids.contains(&observer))
        );
    }

    #[tokio::test]
    async fn approval_role_can_respond_without_start_or_backend_authority() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated authorization fixture");
        add_member_b_to_private_root(&harness).await;
        harness
            .database
            .execute_unprepared(&format!(
                "UPDATE gateway_principal SET role_key='synthetic_approver' \
                 WHERE id='{MEMBER_B_ID}'"
            ))
            .await
            .expect("assign test approver role");
        let store = CrudStore::new(harness.database.clone());
        let superuser = principal(
            crate::auth::test_support::TEST_SUPERUSER_ID,
            PrincipalKind::Superuser,
            None,
            "D00000000000000000001",
            "S00000000000000000001",
        );
        let context = ExecutionAuthorizationContext::for_test(
            &superuser,
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            None,
        );
        let projected = project_lease(
            &store,
            ExecutionLease {
                execution_id: "V00000000000000000003".to_owned(),
                workspace_id: WORKSPACE_RED_ID.to_owned(),
                root_thread_id: THREAD_RED_PRIVATE_A_ID.to_owned(),
                actor_principal_id: superuser.principal_id.clone(),
                actor_session_id: superuser.session_id.clone(),
                admitted_role_key: context.role_key().to_owned(),
                admitted_policy_generation: 1,
                projected_policy_generation: 1,
                continuity_policy: ExecutionContinuityPolicy::StopOnAuthorityLoss,
                continuation_action: context.continuation_action(),
                continuation_provider: context
                    .continuation_provider()
                    .map(|(provider, model)| (provider.to_owned(), model.to_owned())),
                continuation_cli_runtime: None,
                granted_skill_ids: Vec::new(),
                granted_mcp_server_ids: Vec::new(),
                admitted_permission_cap: context.permission_profile_cap().clone(),
                immutable_actions: context
                    .granted_action_names()
                    .iter()
                    .filter_map(|name| ResourceAction::from_safe_name(name))
                    .collect(),
                projected_actions: BTreeSet::new(),
                collaborator_ids: BTreeSet::new(),
                action_collaborators: BTreeMap::new(),
                state: ExecutionLeaseState::Fenced,
            },
        )
        .await
        .expect("project lease");
        let approver = PrincipalId::new(MEMBER_B_ID).unwrap();
        assert!(
            projected
                .action_collaborators
                .get(&ResourceAction::AgentRequestRespond)
                .is_some_and(|ids| ids.contains(&approver))
        );
        for action in [
            ResourceAction::AgentTurnStart,
            ResourceAction::ProviderUse,
            ResourceAction::AgentExecutionCancel,
        ] {
            assert!(
                projected
                    .action_collaborators
                    .get(&action)
                    .is_none_or(|ids| !ids.contains(&approver))
            );
        }
    }

    #[tokio::test]
    async fn revoked_initiator_session_does_not_orphan_shared_task_authority() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated authorization fixture");
        add_member_b_to_private_root(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let member_a = principal(
            MEMBER_A_ID,
            PrincipalKind::User,
            Some(RoleKey::member()),
            "D0000000000000000000A",
            "S0000000000000000000A",
        );
        let context = ExecutionAuthorizationContext::for_test(
            &member_a,
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            &pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            None,
        );
        let initiating_session =
            pioneer_crud::load_session(&harness.database, &member_a.session_id)
                .await
                .expect("load initiating session")
                .expect("fixture initiating session");
        pioneer_crud::mark_session_revoked(
            &harness.database,
            initiating_session,
            pioneer_protocol::AuthSessionRevokeReason::SelfRevoke,
            chrono::Utc::now().into(),
        )
        .await
        .expect("revoke initiating bearer");
        harness
            .database
            .execute_unprepared(&format!(
                "DELETE FROM thread_membership \
                   WHERE thread_id='{THREAD_RED_PRIVATE_A_ID}' AND principal_id='{MEMBER_A_ID}'",
            ))
            .await
            .expect("remove initiating collaboration access");

        let revalidated = ExecutionLeaseRegistry::default()
            .revalidate_context(&store, &context, ResourceAction::TaskCreate, 2)
            .await
            .expect("remaining thread Member keeps shared Task authority");
        assert_eq!(revalidated.principal().principal_id.as_str(), MEMBER_B_ID);
    }
}
