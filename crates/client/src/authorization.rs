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
use pioneer_protocol::{AccessChangeKind, AccessChangedNotification};

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
    let potentially_restrictive_without_exact_thread = matches!(
        notification.change,
        AccessChangeKind::ThreadVisibility | AccessChangeKind::ThreadParticipantRemoved
    );
    let exact_thread_id = notification
        .thread_id
        .as_deref()
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty());
    let mut invalidate_thread_ids = if workspace_wide
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

    let clear_active_workspace =
        workspace_wide && active_workspace_id == Some(notification.workspace_id.as_str());
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
    // Reloading the server-filtered catalog is the re-authorization step.
    // The client never turns the notification or its cache into a grant.
    effects.push(ClientEffect::RefreshWorkspaceList);

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
    if plan.change == AccessChangeKind::WorkspaceMembership {
        state
            .workspaces
            .workspaces
            .retain(|workspace| workspace.id != plan.workspace_id);
    }
    state.workspaces.error = None;
    if plan.change == AccessChangeKind::WorkspaceMembership
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
    state
        .threads
        .coordinators
        .retain(|thread_id, _| !invalidated_thread_ids.contains(thread_id.as_str()));
    state.threads.placements.retain(|thread_id, placement| {
        !invalidated_thread_ids.contains(thread_id.as_str())
            && !(workspace_wide && placement.workspace_id == plan.workspace_id)
    });

    // Folder and inherited Agents.md visibility is an aggregate of the
    // server-filtered tree. An exact thread eviction cannot prove which
    // ancestors remain visible through another thread, so invalidate this
    // projection and let the authoritative tree reload repopulate it.
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
    state
        .threads
        .last_active_thread_by_workspace
        .retain(|workspace_id, thread_id| {
            !(workspace_wide && workspace_id == &plan.workspace_id)
                && !invalidated_thread_ids.contains(thread_id.as_str())
        });
    state
        .threads
        .draft_thread_by_workspace
        .retain(|workspace_id, thread_id| {
            !(workspace_wide && workspace_id == &plan.workspace_id)
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

    if workspace_wide {
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
        Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
        TurnPermissionActionKind, TurnPermissionApprovalRequest, TurnPermissionDecisionReason,
        Workspace,
    };
    use std::collections::HashMap;

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
        assert_eq!(plan.effects, vec![ClientEffect::RefreshWorkspaceList]);
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
