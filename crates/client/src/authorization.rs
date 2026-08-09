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
    AccessChangeKind, AccessChangedNotification, AuthMeResponse, AuthSessionListItem,
    AuthSessionStatus, DeviceStatus, MemberSummary, PrincipalId, PrincipalKind, RoleKey,
};

/// Global, shell-neutral discoverability derived from the authenticated
/// principal snapshot.
///
/// These flags are presentation hints only. The Gateway remains authoritative
/// for every operation and callers must still handle an authoritative denial.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PrincipalPresentationCapabilities {
    pub can_view_invitations: bool,
    pub can_create_invitation: bool,
    pub can_view_member_directory: bool,
    pub can_add_workspace_member: bool,
    pub can_manage_member_lifecycle: bool,
    pub can_remove_workspace_member: bool,
    pub can_manage_own_sessions: bool,
}

/// Stable UI vocabulary for the authenticated principal kind. `Unknown` keeps
/// clients fail-closed when a future protocol kind reaches a newer boundary.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CurrentPrincipalKindPresentation {
    Superuser,
    Member,
    Unknown,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CurrentPrincipalPresentation {
    pub principal_id: PrincipalId,
    pub display_name: String,
    pub nickname: String,
    pub kind: CurrentPrincipalKindPresentation,
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

pub fn current_principal_kind_presentation(
    kind: Option<PrincipalKind>,
) -> CurrentPrincipalKindPresentation {
    match kind {
        Some(PrincipalKind::Superuser) => CurrentPrincipalKindPresentation::Superuser,
        Some(PrincipalKind::User) => CurrentPrincipalKindPresentation::Member,
        None => CurrentPrincipalKindPresentation::Unknown,
    }
}

/// Project the authenticated identity. A directory row may contribute only an
/// already-disclosed avatar revision and only when it names the same principal;
/// auth/me remains the identity authority.
pub fn current_principal_presentation(
    auth: &AuthMeResponse,
    visible_member: Option<&MemberSummary>,
) -> CurrentPrincipalPresentation {
    let avatar_revision = visible_member
        .filter(|member| member.principal_id == auth.principal.id)
        .and_then(|member| member.avatar_revision.clone());

    CurrentPrincipalPresentation {
        principal_id: auth.principal.id.clone(),
        display_name: auth.principal.display_name.clone(),
        nickname: auth.principal.nickname.clone(),
        kind: current_principal_kind_presentation(Some(auth.principal.kind)),
        avatar_revision,
        read_only: true,
        capabilities: principal_presentation_capabilities_from_auth(auth),
    }
}

/// Server-owned resource facts needed to decide whether thread-management UI
/// is discoverable. Shells must obtain these facts from an authoritative
/// thread detail/action response rather than infer them from a cached list row.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadPresentationFacts {
    pub is_user_thread: bool,
    pub is_private_thread: bool,
    pub current_principal_is_creator: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadPresentationCapabilities {
    pub can_manage_thread: bool,
    pub can_manage_private_participants: bool,
}

/// Derive global presentation capabilities from server-owned identity facts.
/// Missing/future principal kinds and unsupported roles fail closed.
pub fn principal_presentation_capabilities(
    principal_kind: Option<PrincipalKind>,
    role_key: Option<&RoleKey>,
) -> PrincipalPresentationCapabilities {
    match (principal_kind, role_key) {
        (Some(PrincipalKind::Superuser), None) => PrincipalPresentationCapabilities {
            can_view_invitations: true,
            can_create_invitation: true,
            can_view_member_directory: true,
            can_add_workspace_member: true,
            can_manage_member_lifecycle: true,
            can_remove_workspace_member: true,
            can_manage_own_sessions: true,
        },
        (Some(PrincipalKind::User), Some(role_key)) if role_key.is_supported() => {
            PrincipalPresentationCapabilities {
                can_view_invitations: true,
                can_create_invitation: true,
                can_view_member_directory: true,
                can_add_workspace_member: true,
                can_manage_member_lifecycle: false,
                can_remove_workspace_member: false,
                can_manage_own_sessions: true,
            }
        }
        _ => PrincipalPresentationCapabilities::default(),
    }
}

pub fn principal_presentation_capabilities_from_auth(
    auth: &AuthMeResponse,
) -> PrincipalPresentationCapabilities {
    principal_presentation_capabilities(Some(auth.principal.kind), auth.role_key.as_ref())
}

/// Derive resource-scoped thread discoverability. This output is never an
/// authorization proof and must not be sent back as one.
pub fn thread_presentation_capabilities(
    principal_kind: Option<PrincipalKind>,
    role_key: Option<&RoleKey>,
    facts: ThreadPresentationFacts,
) -> ThreadPresentationCapabilities {
    let recognized_superuser =
        matches!(principal_kind, Some(PrincipalKind::Superuser)) && role_key.is_none();
    let recognized_member = matches!(principal_kind, Some(PrincipalKind::User))
        && role_key.is_some_and(RoleKey::is_supported);
    let may_manage = facts.is_user_thread
        && (recognized_superuser || (recognized_member && facts.current_principal_is_creator));

    ThreadPresentationCapabilities {
        can_manage_thread: may_manage,
        can_manage_private_participants: may_manage && facts.is_private_thread,
    }
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
    let access_explicitly_retained = notification.access_lost == Some(false);
    let potentially_restrictive_without_exact_thread = matches!(
        notification.change,
        AccessChangeKind::ThreadVisibility | AccessChangeKind::ThreadParticipantRemoved
    );
    let exact_thread_id = notification
        .thread_id
        .as_deref()
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty());
    let mut invalidate_thread_ids = if access_explicitly_retained {
        Vec::new()
    } else if workspace_wide
        || (exact_thread_id.is_none() && potentially_restrictive_without_exact_thread)
    {
        // Compatibility with an older Gateway that cannot identify a
        // restrictive thread-scoped change must remain fail closed.
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

    let clear_active_workspace = !access_explicitly_retained
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
        && notification.access_lost != Some(false)
    {
        state
            .workspaces
            .workspaces
            .retain(|workspace| workspace.id != plan.workspace_id);
    }
    state.workspaces.error = None;
    if plan.change == AccessChangeKind::WorkspaceMembership
        && notification.access_lost != Some(false)
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
    let workspace_access_lost = workspace_wide && notification.access_lost != Some(false);
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
        AuthDeviceSnapshot, AuthSessionId, AuthSessionSnapshot, ClientKind, DeviceId, Thread,
        ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, TokenFamilyId,
        TurnPermissionActionKind, TurnPermissionApprovalRequest, TurnPermissionDecisionReason,
        Workspace,
    };
    use std::collections::HashMap;

    #[test]
    fn current_principal_kind_has_safe_future_fallback() {
        assert_eq!(
            current_principal_kind_presentation(Some(PrincipalKind::Superuser)),
            CurrentPrincipalKindPresentation::Superuser
        );
        assert_eq!(
            current_principal_kind_presentation(Some(PrincipalKind::User)),
            CurrentPrincipalKindPresentation::Member
        );
        assert_eq!(
            current_principal_kind_presentation(None),
            CurrentPrincipalKindPresentation::Unknown
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
    fn principal_capability_matrix_is_bounded_and_future_roles_fail_closed() {
        struct Case {
            kind: Option<PrincipalKind>,
            role: Option<RoleKey>,
            expected: PrincipalPresentationCapabilities,
        }

        let cases = [
            Case {
                kind: Some(PrincipalKind::Superuser),
                role: None,
                expected: PrincipalPresentationCapabilities {
                    can_view_invitations: true,
                    can_create_invitation: true,
                    can_view_member_directory: true,
                    can_add_workspace_member: true,
                    can_manage_member_lifecycle: true,
                    can_remove_workspace_member: true,
                    can_manage_own_sessions: true,
                },
            },
            Case {
                kind: Some(PrincipalKind::User),
                role: Some(RoleKey::member()),
                expected: PrincipalPresentationCapabilities {
                    can_view_invitations: true,
                    can_create_invitation: true,
                    can_view_member_directory: true,
                    can_add_workspace_member: true,
                    can_manage_member_lifecycle: false,
                    can_remove_workspace_member: false,
                    can_manage_own_sessions: true,
                },
            },
            Case {
                kind: Some(PrincipalKind::User),
                role: Some(RoleKey::new("future_role").expect("valid future role")),
                expected: PrincipalPresentationCapabilities::default(),
            },
            Case {
                kind: None,
                role: None,
                expected: PrincipalPresentationCapabilities::default(),
            },
            Case {
                kind: Some(PrincipalKind::Superuser),
                role: Some(RoleKey::member()),
                expected: PrincipalPresentationCapabilities::default(),
            },
        ];

        for case in cases {
            assert_eq!(
                principal_presentation_capabilities(case.kind, case.role.as_ref()),
                case.expected
            );
        }
    }

    #[test]
    fn thread_capabilities_require_recognized_principal_and_explicit_resource_facts() {
        let private_creator = ThreadPresentationFacts {
            is_user_thread: true,
            is_private_thread: true,
            current_principal_is_creator: true,
        };
        let member = RoleKey::member();

        assert_eq!(
            thread_presentation_capabilities(
                Some(PrincipalKind::User),
                Some(&member),
                private_creator,
            ),
            ThreadPresentationCapabilities {
                can_manage_thread: true,
                can_manage_private_participants: true,
            }
        );
        assert_eq!(
            thread_presentation_capabilities(
                Some(PrincipalKind::User),
                Some(&member),
                ThreadPresentationFacts {
                    current_principal_is_creator: false,
                    ..private_creator
                },
            ),
            ThreadPresentationCapabilities::default()
        );
        assert_eq!(
            thread_presentation_capabilities(
                Some(PrincipalKind::Superuser),
                None,
                ThreadPresentationFacts {
                    is_private_thread: false,
                    current_principal_is_creator: false,
                    ..private_creator
                },
            ),
            ThreadPresentationCapabilities {
                can_manage_thread: true,
                can_manage_private_participants: false,
            }
        );
        assert_eq!(
            thread_presentation_capabilities(None, None, private_creator,),
            ThreadPresentationCapabilities::default()
        );
        assert_eq!(
            thread_presentation_capabilities(
                Some(PrincipalKind::Superuser),
                None,
                ThreadPresentationFacts {
                    is_user_thread: false,
                    ..private_creator
                },
            ),
            ThreadPresentationCapabilities::default()
        );
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
                access_lost: None,
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
                access_lost: None,
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
    fn restrictive_legacy_thread_change_without_exact_id_fails_closed_per_workspace() {
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
                access_lost: None,
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
    fn additive_legacy_thread_change_without_exact_id_does_not_destroy_known_threads() {
        let known_threads = vec![ThreadAuthorizationScope {
            thread_id: "thread_existing".to_owned(),
            workspace_id: "ws_affected".to_owned(),
        }];

        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 10,
                workspace_id: "ws_affected".to_owned(),
                thread_id: None,
                access_lost: None,
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
                access_lost: Some(false),
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
                access_lost: None,
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
                access_lost: None,
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
