//! Shared client state selectors.

use crate::{
    agents_doc::scope as agents_doc_scope,
    cli_runtime::approvals::PendingRequest,
    composer::model_selection::{
        self as composer_model_selection, ComposerModelSelection, ComposerModelSelectionCandidate,
    },
    composer::permissions::{self as composer_permissions, ComposerPermissionModeOption},
    conversation::{Conversation, state_machine::TurnFlowState},
    state::{
        client_state::{ClientState, ThreadAgentsDocSummaryKey, WorkspaceThreadState},
        snapshot::{ActiveThreadPhaseSnapshot, ActiveThreadStatusSnapshot},
    },
    threads::{coordinator::ThreadCoordinator, tree as thread_tree},
    workspaces::selectors as workspace_selectors,
};
use pioneer_protocol::{
    Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, TurnPermissionMode, Workspace,
};
use std::collections::HashMap;

pub fn current_active_thread_id(state: &ClientState) -> Option<&str> {
    state.threads.active_thread_id.as_deref()
}

pub fn draft_thread_id(state: &ClientState) -> Option<&str> {
    state.threads.draft_thread_id.as_deref()
}

pub fn preferred_workspace_id(state: &ClientState) -> Option<&str> {
    state.workspaces.preferred_workspace_id.as_deref()
}

pub fn workspaces(state: &ClientState) -> &[Workspace] {
    state.workspaces.workspaces.as_slice()
}

pub fn workspace_by_id<'a>(
    workspaces: &'a [Workspace],
    workspace_id: &str,
) -> Option<&'a Workspace> {
    workspace_selectors::workspace_by_id(workspaces, workspace_id)
}

pub fn pending_requests(state: &ClientState) -> &[PendingRequest] {
    state.pending_requests.requests()
}

pub fn active_thread_pending_requests(state: &ClientState) -> Vec<PendingRequest> {
    state
        .pending_requests
        .pending_for_scope(active_workspace_id(state), current_active_thread_id(state))
}

pub fn active_workspace_id<'a>(state: &'a ClientState) -> Option<&'a str> {
    resolve_active_workspace_id(preferred_workspace_id(state), workspaces(state))
}

pub fn active_workspace(state: &ClientState) -> Option<&Workspace> {
    let workspace_id = active_workspace_id(state)?;
    workspace_by_id(workspaces(state), workspace_id)
}

pub fn last_active_thread_for_workspace<'a>(
    state: &'a ClientState,
    workspace_id: &str,
) -> Option<&'a str> {
    remembered_thread_for_workspace(&state.threads.last_active_thread_by_workspace, workspace_id)
}

pub fn draft_thread_for_workspace<'a>(
    state: &'a ClientState,
    workspace_id: &str,
) -> Option<&'a str> {
    remembered_thread_for_workspace(&state.threads.draft_thread_by_workspace, workspace_id)
}

pub fn has_in_flight_thread_start(state: &ClientState) -> bool {
    has_in_flight_thread_start_from(
        state.threads.start.in_progress,
        state.threads.start.pending_thread_id.as_deref(),
    )
}

pub fn current_composer_permission_mode(
    selected_mode: Option<TurnPermissionMode>,
) -> TurnPermissionMode {
    selected_mode.unwrap_or_else(composer_permissions::default_composer_permission_mode)
}

pub fn composer_permission_mode_options() -> [ComposerPermissionModeOption; 3] {
    composer_permissions::composer_permission_mode_options()
}

pub fn composer_permission_mode_option(mode: TurnPermissionMode) -> ComposerPermissionModeOption {
    composer_permissions::composer_permission_mode_option(mode)
}

pub fn remembered_thread_for_workspace<'a>(
    remembered_threads: &'a HashMap<String, String>,
    workspace_id: &str,
) -> Option<&'a str> {
    thread_tree::remembered_thread_for_workspace(remembered_threads, workspace_id)
}

pub fn has_in_flight_thread_start_from(
    thread_start_in_progress: bool,
    pending_thread_id: Option<&str>,
) -> bool {
    thread_start_in_progress || pending_thread_id.is_some()
}

pub fn thread_coordinator<'a>(
    state: &'a ClientState,
    thread_id: &str,
) -> Option<&'a ThreadCoordinator> {
    thread_coordinator_from(&state.threads.coordinators, thread_id)
}

pub fn thread_coordinator_from<'a>(
    coordinators: &'a HashMap<String, ThreadCoordinator>,
    thread_id: &str,
) -> Option<&'a ThreadCoordinator> {
    coordinators.get(thread_id)
}

pub fn thread_conversation<'a>(
    state: &'a ClientState,
    thread_id: &str,
) -> Option<&'a Conversation> {
    thread_conversation_from(&state.threads.coordinators, thread_id)
}

pub fn thread_workspace_id<'a>(state: &'a ClientState, thread_id: &str) -> Option<&'a str> {
    thread_workspace_id_from(&state.threads.coordinators, thread_id)
}

pub fn thread_workspace_matches(state: &ClientState, thread_id: &str, workspace_id: &str) -> bool {
    thread_workspace_id(state, thread_id) == Some(workspace_id)
}

pub fn model_selector_workspace_id(state: &ClientState) -> String {
    model_selector_workspace_id_from(
        active_workspace_id(state),
        current_active_thread_id(state),
        &state.threads.coordinators,
    )
}

pub fn thread_conversation_from<'a>(
    coordinators: &'a HashMap<String, ThreadCoordinator>,
    thread_id: &str,
) -> Option<&'a Conversation> {
    thread_coordinator_from(coordinators, thread_id).map(|coordinator| &coordinator.conversation)
}

pub fn thread_workspace_id_from<'a>(
    coordinators: &'a HashMap<String, ThreadCoordinator>,
    thread_id: &str,
) -> Option<&'a str> {
    thread_coordinator_from(coordinators, thread_id)
        .map(|coordinator| coordinator.workspace_id.as_str())
}

pub fn model_selector_workspace_id_from(
    active_workspace_id: Option<&str>,
    active_thread_id: Option<&str>,
    coordinators: &HashMap<String, ThreadCoordinator>,
) -> String {
    active_workspace_id
        .or_else(|| {
            active_thread_id.and_then(|thread_id| thread_workspace_id_from(coordinators, thread_id))
        })
        .map(str::to_owned)
        .unwrap_or_default()
}

pub fn resolve_composer_model_selection_from(
    active_thread_id: Option<&str>,
    active_workspace_id: Option<&str>,
    coordinators: &HashMap<String, ThreadCoordinator>,
) -> Option<ComposerModelSelection> {
    composer_model_selection::resolve_composer_model_selection(
        active_thread_id,
        active_workspace_id,
        composer_model_selection_candidates_from(coordinators),
    )
}

pub fn composer_model_selection_candidates_from(
    coordinators: &HashMap<String, ThreadCoordinator>,
) -> Vec<ComposerModelSelectionCandidate> {
    coordinators
        .iter()
        .filter_map(|(thread_id, coordinator)| {
            let thread = coordinator.thread()?;
            Some(ComposerModelSelectionCandidate {
                thread_id: thread_id.clone(),
                workspace_id: coordinator.workspace_id.clone(),
                updated_at: coordinator.updated_at(),
                has_turns: thread_has_known_turns(coordinator, thread),
                selection: ComposerModelSelection::from_thread(thread),
            })
        })
        .collect()
}

fn thread_has_known_turns(coordinator: &ThreadCoordinator, thread: &Thread) -> bool {
    !thread.turns.is_empty() || !coordinator.conversation.projection().turns.is_empty()
}

pub fn thread_folders_for_workspace<'a>(
    folders: &'a HashMap<String, ThreadFolder>,
    workspace_id: &str,
) -> Vec<&'a ThreadFolder> {
    thread_tree::thread_folders_for_workspace(folders, workspace_id)
}

pub fn thread_agents_doc_summary<'a>(
    summaries: &'a HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    folder_id: Option<&str>,
) -> Option<&'a ThreadAgentsDocSummary> {
    agents_doc_scope::thread_agents_doc_summary(summaries, folder_id)
}

pub fn thread_agents_doc_summary_for_workspace<'a>(
    summaries: &'a HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    folder_id: Option<&str>,
    workspace_id: &str,
) -> Option<&'a ThreadAgentsDocSummary> {
    agents_doc_scope::thread_agents_doc_summary_for_workspace(summaries, folder_id, workspace_id)
}

pub fn thread_placements_for_workspace<'a>(
    placements: &'a HashMap<String, ThreadPlacement>,
    workspace_id: &str,
) -> Vec<&'a ThreadPlacement> {
    thread_tree::thread_placements_for_workspace(placements, workspace_id)
}

pub fn has_known_threads_for_workspace(
    coordinators: &HashMap<String, ThreadCoordinator>,
    workspace_id: &str,
) -> bool {
    coordinators
        .values()
        .any(|coordinator| coordinator.workspace_id == workspace_id)
}

pub fn sorted_thread_ids_from_coordinators(
    coordinators: &HashMap<String, ThreadCoordinator>,
    draft_thread_id: Option<&str>,
    workspace_id: Option<&str>,
) -> Vec<String> {
    thread_tree::sorted_thread_ids_from_coordinators(coordinators, draft_thread_id, workspace_id)
}

pub fn sorted_thread_ids_for_workspace(state: &ClientState, workspace_id: &str) -> Vec<String> {
    sorted_thread_ids_from_coordinators(
        &state.threads.coordinators,
        draft_thread_id(state),
        Some(workspace_id),
    )
}

pub fn has_any_in_flight_turn(state: &ClientState) -> bool {
    has_any_in_flight_turn_in(&state.threads.coordinators)
}

pub fn in_flight_turn_id_for_thread(state: &ClientState, thread_id: &str) -> Option<String> {
    in_flight_turn_id_for_thread_in(&state.threads.coordinators, thread_id)
}

pub fn is_thread_history_loading(
    coordinators: &HashMap<String, ThreadCoordinator>,
    thread_id: &str,
) -> bool {
    coordinators
        .get(thread_id)
        .is_some_and(|coordinator| coordinator.history_loading)
}

pub fn is_thread_history_loaded(
    coordinators: &HashMap<String, ThreadCoordinator>,
    thread_id: &str,
) -> bool {
    coordinators
        .get(thread_id)
        .is_some_and(|coordinator| coordinator.history_loaded)
}

pub fn active_thread_conversation<'a>(
    active_thread_id: Option<&str>,
    coordinators: &'a HashMap<String, ThreadCoordinator>,
) -> Option<&'a Conversation> {
    active_thread_id.and_then(|thread_id| thread_conversation_from(coordinators, thread_id))
}

pub fn has_any_in_flight_turn_in(coordinators: &HashMap<String, ThreadCoordinator>) -> bool {
    coordinators
        .values()
        .any(|coordinator| coordinator.conversation.in_flight_turn_id().is_some())
}

pub fn in_flight_turn_id_for_thread_in(
    coordinators: &HashMap<String, ThreadCoordinator>,
    thread_id: &str,
) -> Option<String> {
    thread_conversation_from(coordinators, thread_id)
        .and_then(|conversation| conversation.in_flight_turn_id().map(str::to_owned))
}

pub fn active_thread_phase_snapshot(
    conversation: Option<&Conversation>,
) -> ActiveThreadPhaseSnapshot {
    match conversation.map(Conversation::turn_flow_state) {
        Some(TurnFlowState::Starting { .. }) => ActiveThreadPhaseSnapshot::Starting,
        Some(TurnFlowState::Running { .. }) => ActiveThreadPhaseSnapshot::Running,
        Some(TurnFlowState::Cancelling { .. }) => ActiveThreadPhaseSnapshot::Cancelling,
        Some(TurnFlowState::Completing { .. }) => ActiveThreadPhaseSnapshot::Completing,
        Some(TurnFlowState::Completed { .. }) => ActiveThreadPhaseSnapshot::Completed,
        Some(TurnFlowState::Failed { .. }) => ActiveThreadPhaseSnapshot::Failed,
        Some(TurnFlowState::Blocked { .. }) => ActiveThreadPhaseSnapshot::Blocked,
        Some(TurnFlowState::Cancelled { .. }) => ActiveThreadPhaseSnapshot::Cancelled,
        Some(TurnFlowState::Idle) | None => ActiveThreadPhaseSnapshot::Idle,
    }
}

pub fn active_thread_status_snapshot(
    gateway_connected: bool,
    active_thread_id: Option<&str>,
    has_in_flight_thread_start: bool,
    conversation: Option<&Conversation>,
) -> ActiveThreadStatusSnapshot {
    if !gateway_connected {
        return ActiveThreadStatusSnapshot::GatewayDisconnected;
    }

    if active_thread_id.is_none() && has_in_flight_thread_start {
        return ActiveThreadStatusSnapshot::StartingThread;
    }

    let phase = active_thread_phase_snapshot(conversation);

    if phase == ActiveThreadPhaseSnapshot::Completing {
        return ActiveThreadStatusSnapshot::FinishingTurn;
    }

    if let Some(turn_id) = conversation.and_then(Conversation::in_flight_turn_id) {
        return ActiveThreadStatusSnapshot::TurnRunning {
            turn_id: turn_id.to_owned(),
        };
    }

    match phase {
        ActiveThreadPhaseSnapshot::Failed => ActiveThreadStatusSnapshot::PreviousTurnFailed,
        ActiveThreadPhaseSnapshot::Cancelled => ActiveThreadStatusSnapshot::TurnCancelled,
        ActiveThreadPhaseSnapshot::Completed => ActiveThreadStatusSnapshot::TurnCompleted,
        ActiveThreadPhaseSnapshot::Starting => ActiveThreadStatusSnapshot::StartingTurn,
        ActiveThreadPhaseSnapshot::Running => ActiveThreadStatusSnapshot::AgentProcessing,
        ActiveThreadPhaseSnapshot::Idle
        | ActiveThreadPhaseSnapshot::Cancelling
        | ActiveThreadPhaseSnapshot::Completing
        | ActiveThreadPhaseSnapshot::Blocked => ActiveThreadStatusSnapshot::Ready,
    }
}

pub fn normalize_workspace_id(value: Option<String>) -> Option<String> {
    workspace_selectors::normalize_workspace_id(value)
}

pub fn resolve_active_workspace_id<'a>(
    persisted_workspace_id: Option<&str>,
    workspaces: &'a [Workspace],
) -> Option<&'a str> {
    workspace_selectors::resolve_active_workspace_id(persisted_workspace_id, workspaces)
}

pub fn resolve_thread_tree_workspace_id(
    active_workspace_id: Option<&str>,
    preferred_workspace_id: Option<&str>,
    runtime_workspace_id: Option<&str>,
) -> Option<String> {
    workspace_selectors::resolve_workspace_scope(
        active_workspace_id,
        preferred_workspace_id,
        runtime_workspace_id,
    )
}

pub fn workspace_switch_is_noop(
    current_workspace_id: Option<&str>,
    target_workspace_id: &str,
) -> bool {
    workspace_selectors::workspace_switch_is_noop(current_workspace_id, target_workspace_id)
}

pub fn workspace_switch_target_is_known_active(
    workspaces: &[Workspace],
    target_workspace_id: &str,
) -> bool {
    workspace_selectors::workspace_switch_target_is_known_active(workspaces, target_workspace_id)
}

pub fn remember_workspace_thread_state(
    workspace_id: &str,
    active_thread_id: Option<&str>,
    draft_thread_id: Option<&str>,
    pending_thread_id: Option<&str>,
    thread_workspace_matches: impl Fn(&str, &str) -> bool,
) -> WorkspaceThreadState {
    thread_tree::remember_workspace_thread_state(
        workspace_id,
        active_thread_id,
        draft_thread_id,
        pending_thread_id,
        thread_workspace_matches,
    )
}

pub fn restore_workspace_thread_state(
    workspace_id: &str,
    last_active_thread_id: Option<&str>,
    draft_thread_id: Option<&str>,
    thread_workspace_matches: impl Fn(&str, &str) -> bool,
) -> WorkspaceThreadState {
    thread_tree::restore_workspace_thread_state(
        workspace_id,
        last_active_thread_id,
        draft_thread_id,
        thread_workspace_matches,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::approvals::{PendingRequest, PendingRequestsReduction};
    use crate::conversation::ConversationEvent;
    use pioneer_protocol::{
        Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
        TurnPermissionApprovalRequest,
    };

    fn workspace(id: &str, is_active: bool, is_current: bool) -> Workspace {
        Workspace {
            id: id.to_owned(),
            name: format!("{id} workspace"),
            is_active,
            is_current,
            created_at: 1,
            updated_at: 2,
        }
    }

    fn thread(thread_id: &str, workspace_id: &str, updated_at: i64) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: updated_at,
            updated_at,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        }
    }

    fn coordinator(thread_id: &str, workspace_id: &str, updated_at: i64) -> ThreadCoordinator {
        ThreadCoordinator::new(thread(thread_id, workspace_id, updated_at))
    }

    fn pending_request(request_id: &str, workspace_id: &str, thread_id: &str) -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: request_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: "turn".to_owned(),
            tool_name: "exec_command".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: format!("{request_id}_scope"),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: None,
            details: Vec::new(),
        })
    }

    #[test]
    fn resolve_active_workspace_prefers_valid_persisted_id() {
        let workspaces = vec![
            workspace("ws_1", true, true),
            workspace("ws_2", true, false),
        ];

        assert_eq!(
            resolve_active_workspace_id(Some("ws_2"), workspaces.as_slice()),
            Some("ws_2")
        );
    }

    #[test]
    fn resolve_active_workspace_ignores_invalid_persisted_id_and_uses_current() {
        let workspaces = vec![
            workspace("ws_1", true, false),
            workspace("ws_2", true, true),
        ];

        assert_eq!(
            resolve_active_workspace_id(Some("missing"), workspaces.as_slice()),
            Some("ws_2")
        );
    }

    #[test]
    fn active_thread_pending_requests_selects_shared_scope() {
        let mut state = ClientState::default();
        state.workspaces.preferred_workspace_id = Some("ws_a".to_owned());
        state.workspaces.workspaces = vec![workspace("ws_a", true, true)];
        state.threads.active_thread_id = Some("thread_a".to_owned());

        state
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "req_a", "ws_a", "thread_a",
            )));
        state
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "req_b", "ws_a", "thread_b",
            )));
        state
            .pending_requests
            .apply(PendingRequestsReduction::Opened(pending_request(
                "req_c", "ws_b", "thread_a",
            )));

        let requests = active_thread_pending_requests(&state);

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].request_id, "req_a");
        assert_eq!(pending_requests(&state).len(), 3);
    }

    #[test]
    fn workspace_filter_sorted_thread_ids_ignores_other_workspace_and_draft() {
        let coordinators = HashMap::from([
            (
                "thread_a_old".to_owned(),
                coordinator("thread_a_old", "ws_a", 10),
            ),
            (
                "thread_a_new".to_owned(),
                coordinator("thread_a_new", "ws_a", 30),
            ),
            (
                "thread_a_draft".to_owned(),
                coordinator("thread_a_draft", "ws_a", 40),
            ),
            (
                "thread_b_newer".to_owned(),
                coordinator("thread_b_newer", "ws_b", 100),
            ),
        ]);

        assert_eq!(
            sorted_thread_ids_from_coordinators(
                &coordinators,
                Some("thread_a_draft"),
                Some("ws_a")
            ),
            vec!["thread_a_new".to_owned(), "thread_a_old".to_owned()]
        );
    }

    #[test]
    fn workspace_thread_state_restores_valid_draft_when_last_active_missing() {
        let threads = HashMap::from([("draft_a", "ws_a"), ("thr_b", "ws_b")]);
        let matches_workspace = |thread_id: &str, workspace_id: &str| {
            threads.get(thread_id).copied() == Some(workspace_id)
        };

        let restored = restore_workspace_thread_state(
            "ws_a",
            Some("thr_missing"),
            Some("draft_a"),
            matches_workspace,
        );

        assert_eq!(
            restored,
            WorkspaceThreadState {
                active_thread_id: Some("draft_a".to_owned()),
                draft_thread_id: Some("draft_a".to_owned()),
            }
        );
    }

    #[test]
    fn root_thread_selectors_read_coordinator_map_without_shell_state() {
        let mut coordinators =
            HashMap::from([("thread_a".to_owned(), coordinator("thread_a", "ws_a", 30))]);
        coordinators
            .get_mut("thread_a")
            .expect("thread fixture")
            .history_loaded = true;

        assert_eq!(
            thread_workspace_id_from(&coordinators, "thread_a"),
            Some("ws_a")
        );
        assert!(is_thread_history_loaded(&coordinators, "thread_a"));
        assert!(!is_thread_history_loading(&coordinators, "thread_a"));
        assert_eq!(
            model_selector_workspace_id_from(None, Some("thread_a"), &coordinators),
            "ws_a"
        );
        assert_eq!(
            model_selector_workspace_id_from(Some("ws_selected"), Some("thread_a"), &coordinators),
            "ws_selected"
        );
    }

    #[test]
    fn composer_permission_selector_defaults_to_full_access() {
        assert_eq!(
            current_composer_permission_mode(None),
            TurnPermissionMode::FullAccess
        );
        assert_eq!(
            current_composer_permission_mode(Some(TurnPermissionMode::Supervised)),
            TurnPermissionMode::Supervised
        );
    }

    #[test]
    fn composer_permission_selector_exposes_display_metadata() {
        let options = composer_permission_mode_options();

        assert_eq!(options.len(), 3);
        assert_eq!(options[0].mode, TurnPermissionMode::FullAccess);
        assert_eq!(options[1].mode, TurnPermissionMode::AutoAcceptEdits);
        assert_eq!(options[2].mode, TurnPermissionMode::Supervised);
        assert_eq!(
            composer_permission_mode_option(TurnPermissionMode::AutoAcceptEdits).label,
            "Auto-accept edits"
        );
    }

    #[test]
    fn composer_model_selection_candidates_include_local_conversation_turns() {
        let mut coordinators =
            HashMap::from([("thread_a".to_owned(), coordinator("thread_a", "ws_a", 30))]);
        coordinators
            .get_mut("thread_a")
            .expect("thread fixture")
            .conversation
            .apply(ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                pending_request_id: "request_a".to_owned(),
                user_text: "hello".to_owned(),
                attachments: Vec::new(),
            });

        let candidates = composer_model_selection_candidates_from(&coordinators);

        assert_eq!(candidates.len(), 1);
        assert!(candidates[0].has_turns);
        assert_eq!(
            candidates[0]
                .selection
                .as_ref()
                .map(|s| s.provider.as_str()),
            Some("openai")
        );
        assert_eq!(
            candidates[0].selection.as_ref().map(|s| s.model.as_str()),
            Some("gpt-5.4")
        );
    }

    #[test]
    fn composer_model_selection_resolves_latest_workspace_thread_when_active_is_empty() {
        let coordinators = HashMap::from([
            (
                "active_empty".to_owned(),
                coordinator("active_empty", "ws_a", 50),
            ),
            (
                "older_with_turn".to_owned(),
                coordinator("older_with_turn", "ws_a", 60),
            ),
            (
                "newer_with_turn".to_owned(),
                coordinator("newer_with_turn", "ws_a", 70),
            ),
            (
                "other_workspace".to_owned(),
                coordinator("other_workspace", "ws_b", 100),
            ),
        ]);
        let mut coordinators = coordinators;
        for thread_id in ["older_with_turn", "newer_with_turn", "other_workspace"] {
            coordinators
                .get_mut(thread_id)
                .expect("thread fixture")
                .conversation
                .apply(ConversationEvent::LocalTurnStartRequested {
                    thread_id: thread_id.to_owned(),
                    turn_id: format!("{thread_id}_turn"),
                    pending_request_id: format!("{thread_id}_request"),
                    user_text: "hello".to_owned(),
                    attachments: Vec::new(),
                });
        }

        let selection = resolve_composer_model_selection_from(
            Some("active_empty"),
            Some("ws_a"),
            &coordinators,
        );

        assert_eq!(
            selection,
            Some(ComposerModelSelection {
                provider: "openai".to_owned(),
                model: "gpt-5.4".to_owned(),
                selected_reasoning_effort: None,
            })
        );
    }

    #[test]
    fn active_thread_status_snapshot_is_ui_neutral() {
        assert_eq!(
            active_thread_status_snapshot(false, Some("thread_a"), false, None),
            ActiveThreadStatusSnapshot::GatewayDisconnected
        );
        assert_eq!(
            active_thread_status_snapshot(true, None, true, None),
            ActiveThreadStatusSnapshot::StartingThread
        );
        assert_eq!(
            active_thread_status_snapshot(true, None, false, None),
            ActiveThreadStatusSnapshot::Ready
        );
    }

    #[test]
    fn active_thread_phase_snapshot_tracks_turn_lifecycle_without_string_matching() {
        let mut conversation = Conversation::new("thread_a");
        assert_eq!(
            active_thread_phase_snapshot(Some(&conversation)),
            ActiveThreadPhaseSnapshot::Idle
        );

        conversation.apply(ConversationEvent::LocalTurnStartRequested {
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            pending_request_id: "request_a".to_owned(),
            user_text: "hello".to_owned(),
            attachments: Vec::new(),
        });
        assert_eq!(
            active_thread_phase_snapshot(Some(&conversation)),
            ActiveThreadPhaseSnapshot::Starting
        );

        conversation.apply(ConversationEvent::LocalTurnStartAccepted {
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            pending_request_id: "request_a".to_owned(),
        });
        assert_eq!(
            active_thread_phase_snapshot(Some(&conversation)),
            ActiveThreadPhaseSnapshot::Running
        );
        assert_eq!(
            active_thread_status_snapshot(true, Some("thread_a"), false, Some(&conversation)),
            ActiveThreadStatusSnapshot::TurnRunning {
                turn_id: "turn_a".to_owned(),
            }
        );

        conversation.apply(ConversationEvent::LocalTurnCancelRequested {
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
        });
        assert_eq!(
            active_thread_phase_snapshot(Some(&conversation)),
            ActiveThreadPhaseSnapshot::Cancelling
        );
    }
}
