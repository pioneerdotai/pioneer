//! Shell-neutral authorization-loss lifecycle.
//!
//! The Gateway remains the authority. This module only invalidates client
//! projections after the server reports that access may have changed.

use crate::{
    cli_runtime::approvals::PendingRequestsReduction,
    notifications::effects::ClientEffect,
    state::{client_state::ClientState, selectors},
    threads::start::ThreadStartCoordinator,
};
use pioneer_protocol::{
    AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION, AccessChangeKind, AccessChangedNotification,
    AuthMeResponse, AuthSessionListItem, AuthSessionStatus, AuthorizationCapabilitySnapshot,
    AuthorizationRolePresentation, AuthorizationThreadCapabilities, DeviceStatus, PrincipalId,
};
use std::collections::BTreeMap;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationProjectionAcceptance {
    Accepted,
    Stale,
    Conflict,
    Incompatible,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AuthorizationProjectionManifest {
    schema_version: u32,
    principal_id: PrincipalId,
    role_key: String,
    role: AuthorizationRolePresentation,
    global: pioneer_protocol::AuthorizationGlobalCapabilities,
}

/// Coherent authorization projections for one connection epoch. All shells
/// feed every scoped response through this store before exposing any bit to UI
/// consumers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthorizationProjectionStore {
    accepted_revision: Option<u64>,
    manifest: Option<AuthorizationProjectionManifest>,
    workspaces: BTreeMap<String, pioneer_protocol::AuthorizationWorkspaceCapabilitySnapshot>,
    threads: BTreeMap<String, pioneer_protocol::AuthorizationThreadCapabilitySnapshot>,
}

impl AuthorizationProjectionStore {
    pub fn accepted_revision(&self) -> Option<u64> {
        self.accepted_revision
    }

    pub fn clear_epoch(&mut self) {
        *self = Self::default();
    }

    /// Advances the consistency fence before a replacement snapshot arrives.
    /// Old projections become unavailable immediately.
    pub fn invalidate_for_revision(&mut self, revision: u64) {
        if self
            .accepted_revision
            .is_none_or(|accepted| revision > accepted)
        {
            self.accepted_revision = Some(revision);
            self.manifest = None;
            self.workspaces.clear();
            self.threads.clear();
        }
    }

    pub fn accept(
        &mut self,
        snapshot: AuthorizationCapabilitySnapshot,
    ) -> AuthorizationProjectionAcceptance {
        if snapshot.schema_version != AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION {
            return AuthorizationProjectionAcceptance::Incompatible;
        }
        if snapshot.role_key.trim().is_empty()
            || snapshot.role.key.trim().is_empty()
            || snapshot.role.key != snapshot.role_key
        {
            return AuthorizationProjectionAcceptance::Incompatible;
        }
        if snapshot.thread.is_some() && snapshot.workspace.is_none() {
            return AuthorizationProjectionAcceptance::Incompatible;
        }
        if snapshot.workspace.as_ref().is_some_and(|workspace| {
            workspace.workspace_id.trim().is_empty()
                || workspace
                    .operational_resources
                    .fingerprint
                    .trim()
                    .is_empty()
                || workspace.execution_draft_policy.fingerprint
                    != workspace.operational_resources.fingerprint
                || workspace.execution_draft_policy.resources != workspace.operational_resources
        }) {
            return AuthorizationProjectionAcceptance::Incompatible;
        }
        if let (Some(workspace), Some(thread)) = (&snapshot.workspace, &snapshot.thread)
            && (workspace.workspace_id != thread.workspace_id
                || thread.thread_id.trim().is_empty()
                || workspace.workspace_id.trim().is_empty())
        {
            return AuthorizationProjectionAcceptance::Incompatible;
        }
        if self
            .accepted_revision
            .is_some_and(|accepted| snapshot.authorization_revision < accepted)
        {
            return AuthorizationProjectionAcceptance::Stale;
        }

        let incoming_manifest = AuthorizationProjectionManifest {
            schema_version: snapshot.schema_version,
            principal_id: snapshot.principal_id.clone(),
            role_key: snapshot.role_key.clone(),
            role: snapshot.role.clone(),
            global: snapshot.global.clone(),
        };
        let generation_changed = self
            .accepted_revision
            .is_none_or(|accepted| snapshot.authorization_revision > accepted);
        if !generation_changed {
            if self
                .manifest
                .as_ref()
                .is_some_and(|manifest| manifest != &incoming_manifest)
            {
                return AuthorizationProjectionAcceptance::Conflict;
            }
            if let Some(workspace) = snapshot.workspace.as_ref()
                && self
                    .workspaces
                    .get(workspace.workspace_id.as_str())
                    .is_some_and(|current| current != workspace)
            {
                return AuthorizationProjectionAcceptance::Conflict;
            }
            if let Some(thread) = snapshot.thread.as_ref()
                && self
                    .threads
                    .get(thread.thread_id.as_str())
                    .is_some_and(|current| current != thread)
            {
                return AuthorizationProjectionAcceptance::Conflict;
            }
        }

        if generation_changed {
            self.workspaces.clear();
            self.threads.clear();
        }
        self.accepted_revision = Some(snapshot.authorization_revision);
        self.manifest = Some(incoming_manifest);
        if let Some(workspace) = snapshot.workspace {
            self.workspaces
                .insert(workspace.workspace_id.clone(), workspace);
        }
        if let Some(thread) = snapshot.thread {
            self.threads.insert(thread.thread_id.clone(), thread);
        }
        AuthorizationProjectionAcceptance::Accepted
    }

    pub fn snapshot(
        &self,
        workspace_id: Option<&str>,
        thread_id: Option<&str>,
    ) -> Option<AuthorizationCapabilitySnapshot> {
        let revision = self.accepted_revision?;
        let manifest = self.manifest.as_ref()?;
        let workspace = match workspace_id {
            Some(id) => Some(self.workspaces.get(id)?.clone()),
            None => None,
        };
        let thread = match thread_id {
            Some(id) => Some(self.threads.get(id)?.clone()),
            None => None,
        };
        if thread.as_ref().is_some_and(|thread| {
            workspace
                .as_ref()
                .is_none_or(|workspace| workspace.workspace_id != thread.workspace_id)
        }) {
            return None;
        }
        Some(AuthorizationCapabilitySnapshot {
            schema_version: manifest.schema_version,
            authorization_revision: revision,
            principal_id: manifest.principal_id.clone(),
            role_key: manifest.role_key.clone(),
            role: manifest.role.clone(),
            global: manifest.global.clone(),
            workspace,
            thread,
        })
    }
}

/// Shell-neutral projection of the Gateway capability snapshot.
///
/// These flags are presentation hints only. The Gateway remains authoritative
/// for every operation and callers must still handle an authoritative denial.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PrincipalPresentationCapabilities {
    pub can_create_workspace: bool,
    pub can_manage_workspace: bool,
    pub can_read_own_notifications: bool,
    pub can_acknowledge_own_notifications: bool,
    pub can_manage_gateway_settings: bool,
    pub can_manage_capabilities: bool,
    pub can_use_providers: bool,
    pub can_use_cli_runtimes: bool,
    pub can_use_skills: bool,
    pub can_use_mcp: bool,
    pub can_run_tasks: bool,
    pub can_manage_all_threads: bool,
    pub can_view_invitations: bool,
    pub can_create_invitation: bool,
    pub can_view_member_directory: bool,
    pub can_add_workspace_member: bool,
    pub can_manage_member_lifecycle: bool,
    pub can_remove_workspace_member: bool,
    pub can_manage_own_sessions: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CurrentPrincipalPresentation {
    pub principal_id: PrincipalId,
    pub display_name: String,
    pub nickname: String,
    pub role: AuthorizationRolePresentation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
    pub read_only: bool,
    pub capabilities: PrincipalPresentationCapabilities,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatusPresentation {
    Active,
    Pending,
    Expired,
    Revoked,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionListRowPresentation {
    pub status: SessionStatusPresentation,
    pub actionable: bool,
}

pub fn session_list_row_presentation(item: &AuthSessionListItem) -> SessionListRowPresentation {
    let status = match (item.session.status, item.device.status) {
        (AuthSessionStatus::Active, DeviceStatus::Active) => SessionStatusPresentation::Active,
        (AuthSessionStatus::Pending, _) | (_, DeviceStatus::Pending) => {
            SessionStatusPresentation::Pending
        }
        (AuthSessionStatus::Expired, _) => SessionStatusPresentation::Expired,
        (AuthSessionStatus::Revoked, _) | (_, DeviceStatus::Revoked) => {
            SessionStatusPresentation::Revoked
        }
    };
    SessionListRowPresentation {
        status,
        actionable: status == SessionStatusPresentation::Active,
    }
}

/// Project the authenticated identity from one coherent server-owned
/// authorization manifest. The caller must reject a missing or mismatched
/// manifest rather than derive role metadata locally.
pub fn current_principal_presentation(
    auth: &AuthMeResponse,
    capabilities: PrincipalPresentationCapabilities,
    role: &AuthorizationRolePresentation,
) -> CurrentPrincipalPresentation {
    CurrentPrincipalPresentation {
        principal_id: auth.principal.id.clone(),
        display_name: auth.principal.display_name.clone(),
        nickname: auth.principal.nickname.clone(),
        role: role.clone(),
        avatar_revision: auth.principal.avatar_revision.clone(),
        read_only: false,
        capabilities,
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadPresentationCapabilities {
    pub can_read: bool,
    pub can_write: bool,
    pub can_start_turn: bool,
    pub can_observe_agent_execution: bool,
    pub can_cancel_agent_execution: bool,
    pub can_resume_agent_execution: bool,
    pub can_steer_agent_execution: bool,
    pub can_respond_to_agent_requests: bool,
    pub can_control_cli_runtime: bool,
    pub can_create_task: bool,
    pub can_review_tasks: bool,
    pub can_cancel_tasks: bool,
    pub can_read_artifacts: bool,
    pub can_write_artifacts: bool,
    pub can_bind_artifacts: bool,
    pub can_manage_thread: bool,
    pub can_manage_private_participants: bool,
    pub can_move: bool,
}

/// Flatten the context-aware server snapshot for existing shell components.
/// This performs no role inference: every output bit comes from the Gateway.
pub fn principal_presentation_capabilities(
    snapshot: &AuthorizationCapabilitySnapshot,
) -> PrincipalPresentationCapabilities {
    if snapshot.schema_version != AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION {
        return PrincipalPresentationCapabilities::default();
    }
    let workspace = snapshot.workspace.as_ref().map(|value| &value.capabilities);
    PrincipalPresentationCapabilities {
        can_create_workspace: snapshot.global.can_create_workspace,
        can_manage_workspace: workspace.is_some_and(|value| value.can_manage),
        can_read_own_notifications: workspace.is_some_and(|value| value.can_read_own_notifications),
        can_acknowledge_own_notifications: workspace
            .is_some_and(|value| value.can_acknowledge_own_notifications),
        can_manage_gateway_settings: snapshot.global.can_manage_gateway_settings,
        can_manage_capabilities: snapshot.global.can_manage_capabilities,
        can_use_providers: workspace.is_some_and(|value| value.can_use_providers),
        can_use_cli_runtimes: workspace.is_some_and(|value| value.can_use_cli_runtimes),
        can_use_skills: workspace.is_some_and(|value| value.can_use_skills),
        can_use_mcp: workspace.is_some_and(|value| value.can_use_mcp),
        can_run_tasks: workspace.is_some_and(|value| value.can_run_tasks),
        can_manage_all_threads: snapshot.global.can_manage_all_threads,
        can_view_invitations: snapshot.global.can_view_invitations,
        can_create_invitation: snapshot.global.can_create_invitation,
        can_view_member_directory: snapshot.global.can_view_member_directory,
        can_add_workspace_member: workspace.is_some_and(|value| value.can_add_member),
        can_manage_member_lifecycle: snapshot.global.can_manage_member_lifecycle,
        can_remove_workspace_member: workspace.is_some_and(|value| value.can_remove_member),
        can_manage_own_sessions: snapshot.global.can_manage_own_sessions,
    }
}

/// Validate that a server snapshot belongs to the request context before it
/// is cached by a shell. Missing scoped projections are valid and mean that
/// the scoped capability set is empty; mismatched projections are rejected.
pub fn authorization_capability_snapshot_is_compatible(
    snapshot: &AuthorizationCapabilitySnapshot,
    expected_principal_id: &PrincipalId,
    expected_workspace_id: Option<&str>,
    expected_thread_id: Option<&str>,
) -> bool {
    if snapshot.schema_version != AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION
        || &snapshot.principal_id != expected_principal_id
        || snapshot.role_key.trim().is_empty()
        || snapshot.role.key != snapshot.role_key
    {
        return false;
    }
    if snapshot
        .workspace
        .as_ref()
        .is_some_and(|workspace| Some(workspace.workspace_id.as_str()) != expected_workspace_id)
    {
        return false;
    }
    if snapshot.thread.as_ref().is_some_and(|thread| {
        Some(thread.thread_id.as_str()) != expected_thread_id
            || Some(thread.workspace_id.as_str()) != expected_workspace_id
    }) {
        return false;
    }
    true
}

/// Project Gateway-owned thread flags into the shell vocabulary.
pub fn thread_presentation_capabilities(
    capabilities: Option<&AuthorizationThreadCapabilities>,
) -> ThreadPresentationCapabilities {
    capabilities.map_or_else(ThreadPresentationCapabilities::default, |capabilities| {
        ThreadPresentationCapabilities {
            can_read: capabilities.can_read,
            can_write: capabilities.can_write,
            can_start_turn: capabilities.can_start_turn,
            can_observe_agent_execution: capabilities.can_observe_agent_execution,
            can_cancel_agent_execution: capabilities.can_cancel_agent_execution,
            can_resume_agent_execution: capabilities.can_resume_agent_execution,
            can_steer_agent_execution: capabilities.can_steer_agent_execution,
            can_respond_to_agent_requests: capabilities.can_respond_to_agent_requests,
            can_control_cli_runtime: capabilities.can_control_cli_runtime,
            can_create_task: capabilities.can_create_task,
            can_review_tasks: capabilities.can_review_tasks,
            can_cancel_tasks: capabilities.can_cancel_tasks,
            can_read_artifacts: capabilities.can_read_artifacts,
            can_write_artifacts: capabilities.can_write_artifacts,
            can_bind_artifacts: capabilities.can_bind_artifacts,
            can_manage_thread: capabilities.can_manage,
            can_manage_private_participants: capabilities.can_manage_private_participants,
            can_move: capabilities.can_move,
        }
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadAuthorizationScope {
    pub thread_id: String,
    pub workspace_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessChangedPlan {
    pub authorization_revision: u64,
    pub workspace_id: String,
    pub change: AccessChangeKind,
    pub apply: bool,
    pub invalidate_thread_ids: Vec<String>,
    pub clear_active_workspace: bool,
    pub clear_active_thread: bool,
    pub clear_workspace_capability_projections: bool,
    pub effects: Vec<ClientEffect>,
}

pub fn plan_access_changed(
    notification: &AccessChangedNotification,
    previous_authorization_revision: Option<u64>,
    active_workspace_id: Option<&str>,
    active_thread_id: Option<&str>,
    known_threads: &[ThreadAuthorizationScope],
) -> AccessChangedPlan {
    if previous_authorization_revision
        .is_some_and(|revision| revision >= notification.authorization_revision)
    {
        return AccessChangedPlan {
            authorization_revision: notification.authorization_revision,
            workspace_id: notification.workspace_id.clone(),
            change: notification.change,
            apply: false,
            invalidate_thread_ids: Vec::new(),
            clear_active_workspace: false,
            clear_active_thread: false,
            clear_workspace_capability_projections: false,
            effects: Vec::new(),
        };
    }

    let workspace_wide = notification.change == AccessChangeKind::WorkspaceMembership;
    let access_retained = notification.outcome == pioneer_protocol::AccessChangeOutcome::Retained;
    let potentially_restrictive_without_exact_thread = matches!(
        notification.change,
        AccessChangeKind::ThreadVisibility | AccessChangeKind::ThreadParticipantRemoved
    );
    let exact_thread_id = notification
        .thread_id
        .as_deref()
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty());
    let mut invalidate_thread_ids = if access_retained {
        Vec::new()
    } else if workspace_wide
        || (exact_thread_id.is_none() && potentially_restrictive_without_exact_thread)
    {
        known_threads
            .iter()
            .filter(|scope| scope.workspace_id == notification.workspace_id)
            .map(|scope| scope.thread_id.clone())
            .collect::<Vec<_>>()
    } else {
        exact_thread_id.map(str::to_owned).into_iter().collect()
    };
    invalidate_thread_ids.sort();
    invalidate_thread_ids.dedup();

    let clear_active_workspace = !access_retained
        && workspace_wide
        && active_workspace_id == Some(notification.workspace_id.as_str());
    let clear_active_thread = clear_active_workspace
        || active_thread_id.is_some_and(|active_thread_id| {
            invalidate_thread_ids
                .binary_search_by(|thread_id| thread_id.as_str().cmp(active_thread_id))
                .is_ok()
        });

    let mut effects = Vec::new();
    if !invalidate_thread_ids.is_empty() {
        effects.push(ClientEffect::UnsubscribeThreads {
            thread_ids: invalidate_thread_ids.clone(),
        });
    }
    if workspace_wide {
        effects.push(ClientEffect::RefreshWorkspaceList);
    }

    AccessChangedPlan {
        authorization_revision: notification.authorization_revision,
        workspace_id: notification.workspace_id.clone(),
        change: notification.change,
        apply: true,
        invalidate_thread_ids,
        clear_active_workspace,
        clear_active_thread,
        clear_workspace_capability_projections: clear_active_workspace,
        effects,
    }
}

pub fn apply_access_changed_to_client_state(
    state: &mut ClientState,
    notification: &AccessChangedNotification,
) -> AccessChangedPlan {
    let active_workspace_id = selectors::active_workspace_id(state).map(str::to_owned);
    let active_thread_id = state.threads.active_thread_id.clone();
    let known_threads = state
        .threads
        .coordinators
        .iter()
        .map(|(thread_id, coordinator)| ThreadAuthorizationScope {
            thread_id: thread_id.clone(),
            workspace_id: coordinator.workspace_id.clone(),
        })
        .collect::<Vec<_>>();
    let plan = plan_access_changed(
        notification,
        state.gateway.authorization_revision,
        active_workspace_id.as_deref(),
        active_thread_id.as_deref(),
        known_threads.as_slice(),
    );
    if !plan.apply {
        return plan;
    }

    state.administration.apply_access_changed(notification);
    state.gateway.authorization_revision = Some(plan.authorization_revision);
    if plan.change == AccessChangeKind::WorkspaceMembership
        && notification.outcome == pioneer_protocol::AccessChangeOutcome::Revoked
    {
        state
            .workspaces
            .workspaces
            .retain(|workspace| workspace.id != plan.workspace_id);
    }
    state.workspaces.error = None;
    if plan.change == AccessChangeKind::WorkspaceMembership
        && notification.outcome == pioneer_protocol::AccessChangeOutcome::Revoked
        && state.workspaces.preferred_workspace_id.as_deref() == Some(plan.workspace_id.as_str())
    {
        state.workspaces.preferred_workspace_id = None;
    }

    let invalidated_thread_ids = plan
        .invalidate_thread_ids
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let workspace_wide = plan.change == AccessChangeKind::WorkspaceMembership;
    let workspace_access_lost =
        workspace_wide && notification.outcome == pioneer_protocol::AccessChangeOutcome::Revoked;
    state
        .threads
        .coordinators
        .retain(|thread_id, _| !invalidated_thread_ids.contains(thread_id.as_str()));
    state.threads.placements.retain(|thread_id, placement| {
        !invalidated_thread_ids.contains(thread_id.as_str())
            && !(workspace_access_lost && placement.workspace_id == plan.workspace_id)
    });

    if workspace_access_lost {
        let removed_folder_ids = state
            .threads
            .folders
            .iter()
            .filter(|(_, folder)| folder.workspace_id == plan.workspace_id)
            .map(|(folder_id, _)| folder_id.clone())
            .collect::<Vec<_>>();
        state
            .threads
            .folders
            .retain(|_, folder| folder.workspace_id != plan.workspace_id);
        for folder_id in removed_folder_ids {
            state.threads.folder_expanded.remove(folder_id.as_str());
        }
        state
            .threads
            .agents_doc_summaries
            .retain(|_, summary| summary.workspace_id != plan.workspace_id);
    }
    state
        .threads
        .last_active_thread_by_workspace
        .retain(|workspace_id, thread_id| {
            !(workspace_access_lost && workspace_id == &plan.workspace_id)
                && !invalidated_thread_ids.contains(thread_id.as_str())
        });
    state
        .threads
        .draft_thread_by_workspace
        .retain(|workspace_id, thread_id| {
            !(workspace_access_lost && workspace_id == &plan.workspace_id)
                && !invalidated_thread_ids.contains(thread_id.as_str())
        });

    if plan.clear_active_thread {
        state.threads.active_thread_id = None;
    }
    if state
        .threads
        .draft_thread_id
        .as_deref()
        .is_some_and(|thread_id| invalidated_thread_ids.contains(thread_id))
    {
        state.threads.draft_thread_id = None;
    }

    if workspace_access_lost {
        state
            .pending_requests
            .apply(PendingRequestsReduction::ClearWorkspace {
                workspace_id: plan.workspace_id.clone(),
            });
    } else {
        for thread_id in &plan.invalidate_thread_ids {
            state
                .pending_requests
                .apply(PendingRequestsReduction::ThreadClosed {
                    workspace_id: plan.workspace_id.clone(),
                    thread_id: thread_id.clone(),
                });
        }
    }
    state
        .semantic_timelines
        .threads_by_id
        .retain(|thread_id, _| !invalidated_thread_ids.contains(thread_id.as_str()));
    state
        .threads
        .ready_turn_resume_threads
        .retain(|thread_id| !invalidated_thread_ids.contains(thread_id.as_str()));
    state
        .threads
        .ready_turn_resume_thread_set
        .retain(|thread_id| !invalidated_thread_ids.contains(thread_id.as_str()));

    if plan.clear_workspace_capability_projections {
        state.providers.clear_for_workspace_switch();
        state.mcp = Default::default();
        state.skills = Default::default();
        state.threads.start = ThreadStartCoordinator::default();
        state.threads.start_requested = false;
        state.threads.ready_turn_resume_threads.clear();
        state.threads.ready_turn_resume_thread_set.clear();
    }

    plan
}

pub fn clear_authorization_revision_for_endpoint_change(state: &mut ClientState) {
    state.gateway.authorization_revision = None;
    state.administration.clear_for_session_termination();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cli_runtime::approvals::{PendingRequest, PendingRequestState},
        threads::coordinator::ThreadCoordinator,
    };
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthSessionId, AuthSessionSnapshot, AuthorizationGlobalCapabilities,
        AuthorizationThreadCapabilities, AuthorizationWorkspaceCapabilities,
        AuthorizationWorkspaceCapabilitySnapshot, ClientKind, DeviceId, Thread, ThreadMode,
        ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, ThreadVisibility, TokenFamilyId,
        TurnPermissionActionKind, TurnPermissionApprovalRequest, TurnPermissionDecisionReason,
        Workspace,
    };
    use std::collections::HashMap;

    fn projection_snapshot(
        revision: u64,
        role_key: &str,
        workspace_id: Option<&str>,
        thread_id: Option<&str>,
    ) -> AuthorizationCapabilitySnapshot {
        let resources = pioneer_protocol::AuthorizationOperationalResourceProjection {
            fingerprint: format!("projection-{revision}"),
            ..Default::default()
        };
        AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: revision,
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            role_key: role_key.to_owned(),
            role: AuthorizationRolePresentation {
                key: role_key.to_owned(),
                display_name: "Projected role".to_owned(),
                description: "Projection test role".to_owned(),
                built_in: false,
            },
            global: AuthorizationGlobalCapabilities::default(),
            workspace: workspace_id.map(|workspace_id| AuthorizationWorkspaceCapabilitySnapshot {
                workspace_id: workspace_id.to_owned(),
                capabilities: AuthorizationWorkspaceCapabilities::default(),
                operational_resources: resources.clone(),
                execution_draft_policy:
                    pioneer_protocol::AuthorizationExecutionDraftPolicyProjection {
                        fingerprint: resources.fingerprint.clone(),
                        resources: resources.clone(),
                        permission_options: Vec::new(),
                        can_attach_artifacts: false,
                        mcp_invocation_limits: Default::default(),
                    },
            }),
            thread: thread_id.map(|thread_id| {
                pioneer_protocol::AuthorizationThreadCapabilitySnapshot {
                    workspace_id: workspace_id.unwrap_or_default().to_owned(),
                    thread_id: thread_id.to_owned(),
                    capabilities: AuthorizationThreadCapabilities::default(),
                }
            }),
        }
    }

    #[test]
    fn projection_store_accepts_only_one_coherent_manifest_per_generation() {
        let mut store = AuthorizationProjectionStore::default();
        let initial = projection_snapshot(7, "future_role", Some("workspace-a"), Some("thread-a"));
        assert_eq!(
            store.accept(initial.clone()),
            AuthorizationProjectionAcceptance::Accepted
        );
        assert_eq!(
            store.snapshot(Some("workspace-a"), Some("thread-a")),
            Some(initial)
        );

        let conflict = projection_snapshot(7, "different_role", Some("workspace-b"), None);
        assert_eq!(
            store.accept(conflict),
            AuthorizationProjectionAcceptance::Conflict
        );
        assert!(store.snapshot(Some("workspace-b"), None).is_none());

        let newer = projection_snapshot(8, "future_role", Some("workspace-b"), None);
        assert_eq!(
            store.accept(newer.clone()),
            AuthorizationProjectionAcceptance::Accepted
        );
        assert!(
            store
                .snapshot(Some("workspace-a"), Some("thread-a"))
                .is_none()
        );
        assert_eq!(store.snapshot(Some("workspace-b"), None), Some(newer));
        assert_eq!(
            store.accept(projection_snapshot(7, "future_role", None, None)),
            AuthorizationProjectionAcceptance::Stale
        );

        let mut inconsistent = projection_snapshot(9, "future_role", Some("workspace-b"), None);
        inconsistent
            .workspace
            .as_mut()
            .expect("workspace projection")
            .execution_draft_policy
            .resources
            .providers
            .ids
            .push("unpublished-provider".to_owned());
        assert_eq!(
            store.accept(inconsistent),
            AuthorizationProjectionAcceptance::Incompatible
        );
        assert_eq!(store.accepted_revision(), Some(8));
    }

    #[test]
    fn projection_store_rejects_internally_incoherent_role_and_scope_contracts() {
        let mut store = AuthorizationProjectionStore::default();
        let mut role_mismatch = projection_snapshot(1, "future_role", None, None);
        role_mismatch.role.key = "another_role".to_owned();
        assert_eq!(
            store.accept(role_mismatch),
            AuthorizationProjectionAcceptance::Incompatible
        );

        let mut policy_mismatch = projection_snapshot(1, "future_role", Some("workspace-a"), None);
        policy_mismatch
            .workspace
            .as_mut()
            .unwrap()
            .execution_draft_policy
            .fingerprint = "different-projection".to_owned();
        assert_eq!(
            store.accept(policy_mismatch),
            AuthorizationProjectionAcceptance::Incompatible
        );

        let child_without_workspace = projection_snapshot(1, "future_role", None, Some("thread-a"));
        assert_eq!(
            store.accept(child_without_workspace),
            AuthorizationProjectionAcceptance::Incompatible
        );
    }

    #[test]
    fn session_rows_share_authoritative_status_and_actionability() {
        let mut item = AuthSessionListItem {
            current: false,
            last_seen_at_unix: 1,
            device: AuthDeviceSnapshot {
                id: DeviceId::new("D00000000000000000001").unwrap(),
                installation_id: "install-1".to_owned(),
                display_name: "Device".to_owned(),
                client_kind: ClientKind::Mobile,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: AuthSessionId::new("S00000000000000000001").unwrap(),
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                status: AuthSessionStatus::Active,
                refresh_generation: 1,
                refresh_expires_at_unix: 2,
            },
        };
        assert_eq!(
            session_list_row_presentation(&item),
            SessionListRowPresentation {
                status: SessionStatusPresentation::Active,
                actionable: true,
            }
        );

        item.device.status = DeviceStatus::Revoked;
        assert_eq!(
            session_list_row_presentation(&item),
            SessionListRowPresentation {
                status: SessionStatusPresentation::Revoked,
                actionable: false,
            }
        );
    }

    #[test]
    fn principal_capabilities_are_projected_without_role_inference() {
        let snapshot = AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: 42,
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            // A future code-defined role must not require a client release:
            // presentation is driven exclusively by the server-owned bits.
            role_key: "future_role".to_owned(),
            role: AuthorizationRolePresentation {
                key: "future_role".to_owned(),
                display_name: "Future role".to_owned(),
                description: "Test role".to_owned(),
                built_in: false,
            },
            global: AuthorizationGlobalCapabilities {
                can_create_workspace: true,
                can_manage_gateway_settings: false,
                can_manage_capabilities: true,
                can_manage_providers: true,
                can_manage_mcp: false,
                can_manage_skills: true,
                can_manage_cli_runtimes: false,
                can_manage_all_threads: false,
                can_view_invitations: true,
                can_create_invitation: false,
                invitation_role_options: Vec::new(),
                can_view_member_directory: true,
                can_manage_member_lifecycle: false,
                can_manage_own_sessions: true,
            },
            workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                workspace_id: "workspace_1".to_owned(),
                operational_resources:
                    pioneer_protocol::AuthorizationOperationalResourceProjection {
                        fingerprint: "fixture-policy".to_owned(),
                        ..Default::default()
                    },
                capabilities: AuthorizationWorkspaceCapabilities {
                    can_read: true,
                    can_create_thread: true,
                    can_manage: false,
                    can_read_own_notifications: true,
                    can_acknowledge_own_notifications: true,
                    can_use_providers: true,
                    can_use_cli_runtimes: true,
                    can_use_skills: false,
                    can_use_mcp: true,
                    can_run_tasks: true,
                    can_read_artifacts: true,
                    can_write_artifacts: true,
                    execution_limits: Default::default(),
                    agent_permission_options: Vec::new(),
                    can_list_members: true,
                    can_add_member: true,
                    can_remove_member: false,
                    thread_visibility_options: vec![ThreadVisibility::Private],
                },
                execution_draft_policy:
                    pioneer_protocol::AuthorizationExecutionDraftPolicyProjection {
                        fingerprint: "fixture-policy".to_owned(),
                        resources: pioneer_protocol::AuthorizationOperationalResourceProjection {
                            fingerprint: "fixture-policy".to_owned(),
                            ..Default::default()
                        },
                        permission_options: Vec::new(),
                        can_attach_artifacts: true,
                        mcp_invocation_limits: Default::default(),
                    },
            }),
            thread: None,
        };

        assert_eq!(
            principal_presentation_capabilities(&snapshot),
            PrincipalPresentationCapabilities {
                can_create_workspace: true,
                can_manage_workspace: false,
                can_read_own_notifications: true,
                can_acknowledge_own_notifications: true,
                can_manage_gateway_settings: false,
                can_manage_capabilities: true,
                can_use_providers: true,
                can_use_cli_runtimes: true,
                can_use_skills: false,
                can_use_mcp: true,
                can_run_tasks: true,
                can_manage_all_threads: false,
                can_view_invitations: true,
                can_create_invitation: false,
                can_view_member_directory: true,
                can_add_workspace_member: true,
                can_manage_member_lifecycle: false,
                can_remove_workspace_member: false,
                can_manage_own_sessions: true,
            }
        );
    }

    #[test]
    fn thread_capabilities_are_projected_only_from_gateway_flags() {
        let capabilities = AuthorizationThreadCapabilities {
            can_read: true,
            can_write: true,
            can_start_turn: true,
            can_observe_agent_execution: true,
            can_cancel_agent_execution: true,
            can_resume_agent_execution: true,
            can_steer_agent_execution: true,
            can_respond_to_agent_requests: true,
            can_control_cli_runtime: true,
            can_create_task: true,
            can_review_tasks: true,
            can_cancel_tasks: true,
            can_read_artifacts: true,
            can_write_artifacts: true,
            can_bind_artifacts: true,
            can_manage: true,
            can_manage_private_participants: true,
            can_move: false,
            ..AuthorizationThreadCapabilities::default()
        };
        assert_eq!(
            thread_presentation_capabilities(Some(&capabilities)),
            ThreadPresentationCapabilities {
                can_read: true,
                can_write: true,
                can_start_turn: true,
                can_observe_agent_execution: true,
                can_cancel_agent_execution: true,
                can_resume_agent_execution: true,
                can_steer_agent_execution: true,
                can_respond_to_agent_requests: true,
                can_control_cli_runtime: true,
                can_create_task: true,
                can_review_tasks: true,
                can_cancel_tasks: true,
                can_read_artifacts: true,
                can_write_artifacts: true,
                can_bind_artifacts: true,
                can_manage_thread: true,
                can_manage_private_participants: true,
                can_move: false,
            }
        );
        assert_eq!(
            thread_presentation_capabilities(Some(&AuthorizationThreadCapabilities {
                can_manage_private_participants: false,
                ..capabilities
            })),
            ThreadPresentationCapabilities {
                can_read: true,
                can_write: true,
                can_start_turn: true,
                can_observe_agent_execution: true,
                can_cancel_agent_execution: true,
                can_resume_agent_execution: true,
                can_steer_agent_execution: true,
                can_respond_to_agent_requests: true,
                can_control_cli_runtime: true,
                can_create_task: true,
                can_review_tasks: true,
                can_cancel_tasks: true,
                can_read_artifacts: true,
                can_write_artifacts: true,
                can_bind_artifacts: true,
                can_manage_thread: true,
                can_manage_private_participants: false,
                can_move: false,
            }
        );
        assert_eq!(
            thread_presentation_capabilities(None),
            ThreadPresentationCapabilities::default()
        );
    }

    #[test]
    fn capability_snapshot_context_validation_rejects_wrong_version_or_scope() {
        let principal_id = PrincipalId::new("P00000000000000000001").unwrap();
        let snapshot = AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: 1,
            principal_id: principal_id.clone(),
            role_key: "member".to_owned(),
            role: AuthorizationRolePresentation {
                key: "member".to_owned(),
                display_name: "Member".to_owned(),
                description: "Test role".to_owned(),
                built_in: true,
            },
            global: AuthorizationGlobalCapabilities::default(),
            workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                workspace_id: "workspace-a".to_owned(),
                operational_resources:
                    pioneer_protocol::AuthorizationOperationalResourceProjection {
                        fingerprint: "fixture-policy".to_owned(),
                        ..Default::default()
                    },
                capabilities: AuthorizationWorkspaceCapabilities::default(),
                execution_draft_policy:
                    pioneer_protocol::AuthorizationExecutionDraftPolicyProjection {
                        fingerprint: "fixture-policy".to_owned(),
                        resources: pioneer_protocol::AuthorizationOperationalResourceProjection {
                            fingerprint: "fixture-policy".to_owned(),
                            ..Default::default()
                        },
                        permission_options: Vec::new(),
                        can_attach_artifacts: false,
                        mcp_invocation_limits: Default::default(),
                    },
            }),
            thread: None,
        };

        assert!(authorization_capability_snapshot_is_compatible(
            &snapshot,
            &principal_id,
            Some("workspace-a"),
            None,
        ));
        assert!(!authorization_capability_snapshot_is_compatible(
            &snapshot,
            &principal_id,
            Some("workspace-b"),
            None,
        ));
        assert!(!authorization_capability_snapshot_is_compatible(
            &snapshot,
            &principal_id,
            None,
            None,
        ));
        let mut missing_workspace = snapshot.clone();
        missing_workspace.workspace = None;
        assert!(authorization_capability_snapshot_is_compatible(
            &missing_workspace,
            &principal_id,
            Some("workspace-a"),
            None,
        ));
        let mut missing_thread = snapshot.clone();
        assert!(authorization_capability_snapshot_is_compatible(
            &missing_thread,
            &principal_id,
            Some("workspace-a"),
            Some("thread-a"),
        ));
        missing_thread.thread = Some(pioneer_protocol::AuthorizationThreadCapabilitySnapshot {
            workspace_id: "workspace-a".to_owned(),
            thread_id: "thread-a".to_owned(),
            capabilities: AuthorizationThreadCapabilities::default(),
        });
        assert!(authorization_capability_snapshot_is_compatible(
            &missing_thread,
            &principal_id,
            Some("workspace-a"),
            Some("thread-a"),
        ));
        let mut future = snapshot;
        future.schema_version = AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION + 1;
        assert_eq!(
            principal_presentation_capabilities(&future),
            PrincipalPresentationCapabilities::default()
        );
        assert!(!authorization_capability_snapshot_is_compatible(
            &future,
            &principal_id,
            Some("workspace-a"),
            None,
        ));
    }

    fn workspace(id: &str) -> Workspace {
        Workspace {
            id: id.to_owned(),
            name: format!("{id} workspace"),
            is_active: true,
            is_current: false,
            created_at: 1,
            updated_at: 2,
        }
    }

    fn thread(thread_id: &str, workspace_id: &str) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: Some(format!("{thread_id} protected title")),
            preview: "protected preview".to_owned(),
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 2,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        }
    }

    fn pending_request(workspace_id: &str, thread_id: &str) -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: format!("request_{thread_id}"),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: format!("turn_{thread_id}"),
            visible_thread_ids: Vec::new(),
            tool_name: "exec_command".to_owned(),
            action: TurnPermissionActionKind::ShellCommand,
            scope_hash: format!("scope_{thread_id}"),
            reason: TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: None,
            details: Vec::new(),
        })
    }

    #[test]
    fn access_change_removes_only_affected_workspace_state_and_keeps_session() {
        let mut state = ClientState::default();
        state.workspaces.workspaces = vec![workspace("ws_allowed"), workspace("ws_revoked")];
        state.workspaces.preferred_workspace_id = Some("ws_revoked".to_owned());
        state.threads.active_thread_id = Some("thread_revoked".to_owned());
        state.threads.coordinators = HashMap::from([
            (
                "thread_allowed".to_owned(),
                ThreadCoordinator::new(thread("thread_allowed", "ws_allowed")),
            ),
            (
                "thread_revoked".to_owned(),
                ThreadCoordinator::new(thread("thread_revoked", "ws_revoked")),
            ),
        ]);
        state.gateway.ws_connection_id = Some(17);
        state.gateway.bootstrap_complete = true;
        state
            .semantic_timelines
            .thread_mut("thread_allowed".to_owned());
        state
            .semantic_timelines
            .thread_mut("thread_revoked".to_owned());
        state.pending_requests = PendingRequestState::default();
        state
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "ws_allowed",
                "thread_allowed",
            )));
        state
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "ws_revoked",
                "thread_revoked",
            )));

        let plan = apply_access_changed_to_client_state(
            &mut state,
            &AccessChangedNotification {
                authorization_revision: 7,
                workspace_id: "ws_revoked".to_owned(),
                thread_id: None,
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::WorkspaceMembership,
            },
        );

        assert!(plan.apply);
        assert_eq!(plan.invalidate_thread_ids, vec!["thread_revoked"]);
        assert_eq!(state.gateway.authorization_revision, Some(7));
        assert_eq!(state.gateway.ws_connection_id, Some(17));
        assert!(state.gateway.bootstrap_complete);
        assert_eq!(state.workspaces.workspaces, vec![workspace("ws_allowed")]);
        assert!(state.workspaces.preferred_workspace_id.is_none());
        assert!(state.threads.active_thread_id.is_none());
        assert!(state.threads.coordinators.contains_key("thread_allowed"));
        assert!(!state.threads.coordinators.contains_key("thread_revoked"));
        assert!(
            state
                .semantic_timelines
                .threads_by_id
                .contains_key("thread_allowed")
        );
        assert!(
            !state
                .semantic_timelines
                .threads_by_id
                .contains_key("thread_revoked")
        );
        assert_eq!(state.pending_requests.requests().len(), 1);
        assert_eq!(
            state.pending_requests.requests()[0].workspace_id,
            "ws_allowed"
        );
        assert_eq!(
            plan.effects,
            vec![
                ClientEffect::UnsubscribeThreads {
                    thread_ids: vec!["thread_revoked".to_owned()]
                },
                ClientEffect::RefreshWorkspaceList,
            ]
        );
    }

    #[test]
    fn thread_access_change_clears_protected_thread_state_but_keeps_workspace_access() {
        let mut state = ClientState::default();
        state.workspaces.workspaces = vec![workspace("ws_allowed"), workspace("ws_affected")];
        state.workspaces.preferred_workspace_id = Some("ws_affected".to_owned());
        state.threads.active_thread_id = Some("thread_affected".to_owned());
        state.threads.coordinators = HashMap::from([
            (
                "thread_allowed".to_owned(),
                ThreadCoordinator::new(thread("thread_allowed", "ws_allowed")),
            ),
            (
                "thread_kept_same_workspace".to_owned(),
                ThreadCoordinator::new(thread("thread_kept_same_workspace", "ws_affected")),
            ),
            (
                "thread_affected".to_owned(),
                ThreadCoordinator::new(thread("thread_affected", "ws_affected")),
            ),
        ]);
        state.gateway.ws_connection_id = Some(19);
        state.gateway.bootstrap_complete = true;
        state.threads.last_active_thread_by_workspace = HashMap::from([(
            "ws_affected".to_owned(),
            "thread_kept_same_workspace".to_owned(),
        )]);
        state.threads.draft_thread_by_workspace = HashMap::from([(
            "ws_affected".to_owned(),
            "thread_kept_same_workspace".to_owned(),
        )]);
        state
            .threads
            .ready_turn_resume_threads
            .push_back("thread_kept_same_workspace".to_owned());
        state
            .threads
            .ready_turn_resume_threads
            .push_back("thread_affected".to_owned());
        state
            .threads
            .ready_turn_resume_thread_set
            .insert("thread_kept_same_workspace".to_owned());
        state
            .threads
            .ready_turn_resume_thread_set
            .insert("thread_affected".to_owned());
        state
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "ws_affected",
                "thread_kept_same_workspace",
            )));
        state
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "ws_affected",
                "thread_affected",
            )));

        let plan = apply_access_changed_to_client_state(
            &mut state,
            &AccessChangedNotification {
                authorization_revision: 8,
                workspace_id: "ws_affected".to_owned(),
                thread_id: Some("thread_affected".to_owned()),
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::ThreadParticipantRemoved,
            },
        );

        assert!(plan.apply);
        assert!(!plan.clear_active_workspace);
        assert!(plan.clear_active_thread);
        assert_eq!(
            state.workspaces.workspaces,
            vec![workspace("ws_allowed"), workspace("ws_affected")]
        );
        assert_eq!(
            state.workspaces.preferred_workspace_id.as_deref(),
            Some("ws_affected")
        );
        assert!(state.threads.active_thread_id.is_none());
        assert!(!state.threads.coordinators.contains_key("thread_affected"));
        assert!(state.threads.coordinators.contains_key("thread_allowed"));
        assert!(
            state
                .threads
                .coordinators
                .contains_key("thread_kept_same_workspace")
        );
        assert_eq!(
            state
                .threads
                .last_active_thread_by_workspace
                .get("ws_affected")
                .map(String::as_str),
            Some("thread_kept_same_workspace")
        );
        assert_eq!(
            state
                .threads
                .draft_thread_by_workspace
                .get("ws_affected")
                .map(String::as_str),
            Some("thread_kept_same_workspace")
        );
        assert_eq!(
            state
                .threads
                .ready_turn_resume_threads
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["thread_kept_same_workspace"]
        );
        assert_eq!(
            state.threads.ready_turn_resume_thread_set,
            std::collections::HashSet::from(["thread_kept_same_workspace".to_owned()])
        );
        assert_eq!(state.pending_requests.requests().len(), 1);
        assert_eq!(
            state.pending_requests.requests()[0].thread_id.as_deref(),
            Some("thread_kept_same_workspace")
        );
        assert_eq!(state.gateway.ws_connection_id, Some(19));
        assert!(state.gateway.bootstrap_complete);
    }

    #[test]
    fn revoked_thread_change_without_exact_id_fails_closed_per_workspace() {
        let known_threads = vec![
            ThreadAuthorizationScope {
                thread_id: "thread_a".to_owned(),
                workspace_id: "ws_affected".to_owned(),
            },
            ThreadAuthorizationScope {
                thread_id: "thread_b".to_owned(),
                workspace_id: "ws_affected".to_owned(),
            },
            ThreadAuthorizationScope {
                thread_id: "thread_other".to_owned(),
                workspace_id: "ws_other".to_owned(),
            },
        ];

        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 9,
                workspace_id: "ws_affected".to_owned(),
                thread_id: None,
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::ThreadParticipantRemoved,
            },
            None,
            Some("ws_affected"),
            Some("thread_a"),
            known_threads.as_slice(),
        );

        assert_eq!(plan.invalidate_thread_ids, vec!["thread_a", "thread_b"]);
        assert!(plan.clear_active_thread);
        assert!(!plan.clear_active_workspace);
    }

    #[test]
    fn retained_additive_thread_change_without_exact_id_preserves_known_threads() {
        let known_threads = vec![ThreadAuthorizationScope {
            thread_id: "thread_existing".to_owned(),
            workspace_id: "ws_affected".to_owned(),
        }];

        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 10,
                workspace_id: "ws_affected".to_owned(),
                thread_id: None,
                outcome: pioneer_protocol::AccessChangeOutcome::Retained,
                change: AccessChangeKind::ThreadParticipantAdded,
            },
            None,
            Some("ws_affected"),
            Some("thread_existing"),
            known_threads.as_slice(),
        );

        assert!(plan.invalidate_thread_ids.is_empty());
        assert!(!plan.clear_active_thread);
        assert!(!plan.clear_active_workspace);
        assert!(plan.effects.is_empty());
    }

    #[test]
    fn retained_thread_visibility_change_preserves_active_thread_and_cache() {
        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 11,
                workspace_id: "ws_affected".to_owned(),
                thread_id: Some("thread_current".to_owned()),
                outcome: pioneer_protocol::AccessChangeOutcome::Retained,
                change: AccessChangeKind::ThreadVisibility,
            },
            Some(10),
            Some("ws_affected"),
            Some("thread_current"),
            &[ThreadAuthorizationScope {
                thread_id: "thread_current".to_owned(),
                workspace_id: "ws_affected".to_owned(),
            }],
        );

        assert!(plan.apply);
        assert!(plan.invalidate_thread_ids.is_empty());
        assert!(!plan.clear_active_thread);
        assert!(plan.effects.is_empty());
    }

    #[test]
    fn stale_access_change_cannot_clear_newer_endpoint_state() {
        let mut state = ClientState::default();
        state.gateway.authorization_revision = Some(9);
        state.gateway.ws_connection_id = Some(22);
        state.workspaces.workspaces = vec![workspace("ws_a")];

        let plan = apply_access_changed_to_client_state(
            &mut state,
            &AccessChangedNotification {
                authorization_revision: 8,
                workspace_id: "ws_a".to_owned(),
                thread_id: Some("thread_current".to_owned()),
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::ThreadParticipantRemoved,
            },
        );

        assert!(!plan.apply);
        assert!(plan.effects.is_empty());
        assert_eq!(state.gateway.authorization_revision, Some(9));
        assert_eq!(state.gateway.ws_connection_id, Some(22));
        assert_eq!(state.workspaces.workspaces, vec![workspace("ws_a")]);
    }

    #[test]
    fn endpoint_change_clears_revision_without_erasing_device_session_state() {
        let mut state = ClientState::default();
        state.gateway.authorization_revision = Some(11);
        state.gateway.ws_connection_id = Some(42);
        state.gateway.bootstrap_complete = true;

        clear_authorization_revision_for_endpoint_change(&mut state);

        assert_eq!(state.gateway.authorization_revision, None);
        assert_eq!(state.gateway.ws_connection_id, Some(42));
        assert!(state.gateway.bootstrap_complete);
    }

    #[test]
    fn access_change_for_one_endpoint_cannot_mutate_another_endpoint_cache() {
        let mut endpoint_a = ClientState::default();
        endpoint_a.workspaces.workspaces = vec![workspace("ws_a")];
        endpoint_a.workspaces.preferred_workspace_id = Some("ws_a".to_owned());

        let mut endpoint_b = ClientState::default();
        endpoint_b.workspaces.workspaces = vec![workspace("ws_b")];
        endpoint_b.workspaces.preferred_workspace_id = Some("ws_b".to_owned());
        endpoint_b.gateway.ws_connection_id = Some(200);

        apply_access_changed_to_client_state(
            &mut endpoint_a,
            &AccessChangedNotification {
                authorization_revision: 1,
                workspace_id: "ws_a".to_owned(),
                thread_id: None,
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::WorkspaceMembership,
            },
        );

        assert!(endpoint_a.workspaces.workspaces.is_empty());
        assert_eq!(endpoint_b.workspaces.workspaces, vec![workspace("ws_b")]);
        assert_eq!(
            endpoint_b.workspaces.preferred_workspace_id.as_deref(),
            Some("ws_b")
        );
        assert_eq!(endpoint_b.gateway.ws_connection_id, Some(200));
        assert_eq!(endpoint_b.gateway.authorization_revision, None);
    }
}
