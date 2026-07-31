//! UI-neutral client state snapshots.

use crate::{
    state::{client_state::ClientState, selectors},
    threads::coordinator::ThreadCoordinator,
};
use pioneer_protocol::Workspace;
use std::collections::HashMap;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientSnapshot {
    pub workspace: WorkspaceSnapshot,
    pub threads: ThreadListSnapshot,
    pub active_thread: ActiveThreadSnapshot,
    pub has_in_flight_thread_start: bool,
    pub has_any_in_flight_turn: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct WorkspaceSnapshot {
    pub preferred_workspace_id: Option<String>,
    pub active_workspace_id: Option<String>,
    pub loading: bool,
    pub error: Option<String>,
    pub action_in_progress: bool,
    pub workspace_count: usize,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ThreadListSnapshot {
    pub active_thread_id: Option<String>,
    pub draft_thread_id: Option<String>,
    pub active_workspace_thread_ids: Vec<String>,
    pub has_known_threads_for_active_workspace: bool,
    pub loading: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ActiveThreadSnapshot {
    pub thread_id: Option<String>,
    pub workspace_id: Option<String>,
    pub is_draft: bool,
    pub history_loading: bool,
    pub history_loaded: bool,
    pub in_flight_turn_id: Option<String>,
    pub phase: ActiveThreadPhaseSnapshot,
    pub status: ActiveThreadStatusSnapshot,
}

impl Default for ActiveThreadSnapshot {
    fn default() -> Self {
        Self {
            thread_id: None,
            workspace_id: None,
            is_draft: false,
            history_loading: false,
            history_loaded: false,
            in_flight_turn_id: None,
            phase: ActiveThreadPhaseSnapshot::Idle,
            status: ActiveThreadStatusSnapshot::Ready,
        }
    }
}

impl ActiveThreadSnapshot {
    pub fn has_in_flight_turn(&self) -> bool {
        self.in_flight_turn_id.is_some()
    }

    pub fn is_cancelling_turn(&self) -> bool {
        self.phase == ActiveThreadPhaseSnapshot::Cancelling
    }

    pub fn can_request_turn_cancel(&self, gateway_connected: bool) -> bool {
        self.has_in_flight_turn() && gateway_connected && !self.is_cancelling_turn()
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ActiveThreadPhaseSnapshot {
    #[default]
    Idle,
    Starting,
    Running,
    Cancelling,
    Completing,
    Completed,
    Failed,
    Blocked,
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ActiveThreadStatusSnapshot {
    GatewayDisconnected,
    StartingThread,
    FinishingTurn,
    TurnRunning { turn_id: String },
    PreviousTurnFailed,
    TurnCancelled,
    TurnCompleted,
    Ready,
    StartingTurn,
    AgentProcessing,
}

#[derive(Clone, Copy)]
pub struct ClientSnapshotInput<'a> {
    pub active_thread_id: Option<&'a str>,
    pub draft_thread_id: Option<&'a str>,
    pub preferred_workspace_id: Option<&'a str>,
    pub workspaces: &'a [Workspace],
    pub workspaces_loading: bool,
    pub workspaces_error: Option<&'a str>,
    pub workspace_action_in_progress: bool,
    pub thread_list_loading: bool,
    pub thread_start_in_progress: bool,
    pub pending_thread_id: Option<&'a str>,
    pub coordinators: &'a HashMap<String, ThreadCoordinator>,
    pub gateway_connected: bool,
}

impl ClientSnapshot {
    pub fn from_state(state: &ClientState) -> Self {
        Self::from_parts(ClientSnapshotInput {
            active_thread_id: selectors::current_active_thread_id(state),
            draft_thread_id: selectors::draft_thread_id(state),
            preferred_workspace_id: selectors::preferred_workspace_id(state),
            workspaces: selectors::workspaces(state),
            workspaces_loading: state.workspaces.loading,
            workspaces_error: state.workspaces.error.as_deref(),
            workspace_action_in_progress: state.workspaces.action_in_progress,
            thread_list_loading: state.threads.list_loading,
            thread_start_in_progress: state.threads.start.in_progress,
            pending_thread_id: state.threads.start.pending_thread_id.as_deref(),
            coordinators: &state.threads.coordinators,
            gateway_connected: state.gateway.ws_connection_id.is_some(),
        })
    }

    pub fn from_parts(input: ClientSnapshotInput<'_>) -> Self {
        let active_workspace_id =
            selectors::resolve_active_workspace_id(input.preferred_workspace_id, input.workspaces)
                .map(str::to_owned);
        let active_workspace_thread_ids = active_workspace_id
            .as_deref()
            .map(|workspace_id| {
                selectors::sorted_thread_ids_from_coordinators(
                    input.coordinators,
                    input.draft_thread_id,
                    Some(workspace_id),
                )
            })
            .unwrap_or_default();
        let has_known_threads_for_active_workspace =
            active_workspace_id.as_deref().is_some_and(|workspace_id| {
                selectors::has_known_threads_for_workspace(input.coordinators, workspace_id)
            });
        let has_in_flight_thread_start = selectors::has_in_flight_thread_start_from(
            input.thread_start_in_progress,
            input.pending_thread_id,
        );
        let has_any_in_flight_turn = selectors::has_any_in_flight_turn_in(input.coordinators);
        let active_conversation =
            selectors::active_thread_conversation(input.active_thread_id, input.coordinators);
        let active_thread_workspace_id = input
            .active_thread_id
            .and_then(|thread_id| {
                selectors::thread_workspace_id_from(input.coordinators, thread_id)
            })
            .map(str::to_owned);
        let active_thread_status = selectors::active_thread_status_snapshot(
            input.gateway_connected,
            input.active_thread_id,
            has_in_flight_thread_start,
            active_conversation,
        );
        let active_thread_phase = selectors::active_thread_phase_snapshot(active_conversation);

        Self {
            workspace: WorkspaceSnapshot {
                preferred_workspace_id: input.preferred_workspace_id.map(str::to_owned),
                active_workspace_id,
                loading: input.workspaces_loading,
                error: input.workspaces_error.map(str::to_owned),
                action_in_progress: input.workspace_action_in_progress,
                workspace_count: input.workspaces.len(),
            },
            threads: ThreadListSnapshot {
                active_thread_id: input.active_thread_id.map(str::to_owned),
                draft_thread_id: input.draft_thread_id.map(str::to_owned),
                active_workspace_thread_ids,
                has_known_threads_for_active_workspace,
                loading: input.thread_list_loading,
            },
            active_thread: ActiveThreadSnapshot {
                thread_id: input.active_thread_id.map(str::to_owned),
                workspace_id: active_thread_workspace_id,
                is_draft: input.active_thread_id.is_some()
                    && input.active_thread_id == input.draft_thread_id,
                history_loading: input.active_thread_id.is_some_and(|thread_id| {
                    selectors::is_thread_history_loading(input.coordinators, thread_id)
                }),
                history_loaded: input.active_thread_id.is_some_and(|thread_id| {
                    selectors::is_thread_history_loaded(input.coordinators, thread_id)
                }),
                in_flight_turn_id: input.active_thread_id.and_then(|thread_id| {
                    selectors::in_flight_turn_id_for_thread_in(input.coordinators, thread_id)
                }),
                phase: active_thread_phase,
                status: active_thread_status,
            },
            has_in_flight_thread_start,
            has_any_in_flight_turn,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::threads::coordinator::ThreadCoordinator;
    use pioneer_protocol::{
        Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
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
            visibility: None,
            turns: Vec::new(),
        }
    }

    #[test]
    fn client_snapshot_projects_workspace_and_thread_list_dtos() {
        let workspaces = vec![
            workspace("ws_a", true, true),
            workspace("ws_b", true, false),
        ];
        let coordinators = HashMap::from([
            (
                "thread_old".to_owned(),
                ThreadCoordinator::new(thread("thread_old", "ws_a", 10)),
            ),
            (
                "thread_new".to_owned(),
                ThreadCoordinator::new(thread("thread_new", "ws_a", 30)),
            ),
            (
                "thread_b".to_owned(),
                ThreadCoordinator::new(thread("thread_b", "ws_b", 40)),
            ),
            (
                "pending_child".to_owned(),
                ThreadCoordinator::pending("pending_child", "ws_a"),
            ),
        ]);

        let snapshot = ClientSnapshot::from_parts(ClientSnapshotInput {
            active_thread_id: Some("thread_new"),
            draft_thread_id: None,
            preferred_workspace_id: Some("ws_a"),
            workspaces: workspaces.as_slice(),
            workspaces_loading: false,
            workspaces_error: None,
            workspace_action_in_progress: false,
            thread_list_loading: false,
            thread_start_in_progress: false,
            pending_thread_id: None,
            coordinators: &coordinators,
            gateway_connected: true,
        });

        assert_eq!(
            snapshot.workspace.active_workspace_id.as_deref(),
            Some("ws_a")
        );
        assert_eq!(
            snapshot.threads.active_workspace_thread_ids,
            vec!["thread_new".to_owned(), "thread_old".to_owned()]
        );
        assert_eq!(snapshot.active_thread.workspace_id.as_deref(), Some("ws_a"));
        assert_eq!(
            snapshot.active_thread.phase,
            ActiveThreadPhaseSnapshot::Idle
        );
        assert_eq!(
            snapshot.active_thread.status,
            ActiveThreadStatusSnapshot::Ready
        );
        assert!(!snapshot.active_thread.has_in_flight_turn());
        assert!(!snapshot.active_thread.can_request_turn_cancel(true));
    }
}
