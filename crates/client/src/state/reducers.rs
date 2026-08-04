//! Shared client state reducers.

use crate::state::client_state::{GatewayConnectionState, GatewayStatusLevel};
use crate::{
    agents_doc::scope as agents_doc_scope,
    cli_runtime::approvals::{
        PendingRequestsReduction, reduce_pending_request_thread_closed_cleanup,
    },
    gateway::{endpoint::GatewayBaseUrl, runtime::ActiveGatewayState, types::GatewayEndpointKind},
    notifications::effects::ClientEffect,
    state::client_state::{ClientState, ThreadAgentsDocSummaryKey},
    threads::{
        coordinator::ThreadCoordinator,
        resume as thread_resume, session as thread_session,
        start::{self as thread_start, ThreadStartCoordinator},
        tree as thread_tree,
    },
    workspaces::actions as workspace_actions,
};
use pioneer_protocol::{Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, Workspace};
use std::collections::{HashMap, HashSet, VecDeque};

pub fn set_active_thread_id(state: &mut ClientState, thread_id: Option<String>) -> bool {
    thread_session::set_active_thread_id(&mut state.threads.active_thread_id, thread_id)
}

pub fn clear_active_thread_if_matches(state: &mut ClientState, thread_id: &str) -> bool {
    thread_session::clear_active_thread_if_matches(&mut state.threads.active_thread_id, thread_id)
}

pub fn set_draft_thread_id(state: &mut ClientState, thread_id: Option<String>) {
    thread_session::set_draft_thread_id(&mut state.threads.draft_thread_id, thread_id);
}

pub fn clear_draft_thread_if_matches(state: &mut ClientState, thread_id: &str) -> bool {
    thread_session::clear_draft_thread_if_matches(&mut state.threads.draft_thread_id, thread_id)
}

pub fn collect_known_thread_ids_for_unsubscribe(
    coordinators: &HashMap<String, ThreadCoordinator>,
    active_thread_id: Option<&str>,
) -> Vec<String> {
    let mut known_thread_ids = coordinators.keys().cloned().collect::<HashSet<_>>();
    if let Some(thread_id) = active_thread_id {
        known_thread_ids.insert(thread_id.to_owned());
    }
    let mut thread_ids = known_thread_ids.into_iter().collect::<Vec<_>>();
    thread_ids.sort();
    thread_ids
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewaySwitchCleanupPlan {
    pub thread_list_loading: bool,
    pub clear_active_thread: bool,
    pub clear_thread_conversations: bool,
    pub rebuild_thread_tree: bool,
    pub reset_thread_start: bool,
    pub clear_thread_start_queue: bool,
    pub clear_turn_resume_queue: bool,
    pub clear_gateway_settings: bool,
    pub gateway_settings_loading: bool,
    pub gateway_settings_error: Option<String>,
    pub effects: Vec<ClientEffect>,
}

pub fn plan_gateway_switch_cleanup(
    coordinators: &HashMap<String, ThreadCoordinator>,
    active_thread_id: Option<&str>,
) -> GatewaySwitchCleanupPlan {
    let thread_ids = collect_known_thread_ids_for_unsubscribe(coordinators, active_thread_id);
    let effects = if thread_ids.is_empty() {
        Vec::new()
    } else {
        vec![ClientEffect::UnsubscribeThreads { thread_ids }]
    };

    GatewaySwitchCleanupPlan {
        thread_list_loading: false,
        clear_active_thread: true,
        clear_thread_conversations: true,
        rebuild_thread_tree: true,
        reset_thread_start: true,
        clear_thread_start_queue: true,
        clear_turn_resume_queue: true,
        clear_gateway_settings: true,
        gateway_settings_loading: false,
        gateway_settings_error: None,
        effects,
    }
}

pub fn reset_thread_start_coordinator(start: &mut ThreadStartCoordinator) {
    thread_start::reset_thread_start_coordinator(start);
}

pub fn enqueue_thread_start_request(start_requested: &mut bool) {
    thread_start::enqueue_thread_start_request(start_requested);
}

pub fn dequeue_thread_start_request(start_requested: &mut bool) -> bool {
    thread_start::dequeue_thread_start_request(start_requested)
}

pub fn clear_thread_start_request(start_requested: &mut bool) {
    thread_start::clear_thread_start_request(start_requested);
}

pub fn enqueue_turn_resume_thread(
    ready_threads: &mut VecDeque<String>,
    ready_thread_set: &mut HashSet<String>,
    thread_id: String,
) -> bool {
    thread_resume::enqueue_turn_resume_thread(ready_threads, ready_thread_set, thread_id)
}

pub fn dequeue_turn_resume_thread(
    ready_threads: &mut VecDeque<String>,
    ready_thread_set: &mut HashSet<String>,
) -> Option<String> {
    thread_resume::dequeue_turn_resume_thread(ready_threads, ready_thread_set)
}

pub fn clear_turn_resume_queue(
    ready_threads: &mut VecDeque<String>,
    ready_thread_set: &mut HashSet<String>,
) {
    thread_resume::clear_turn_resume_queue(ready_threads, ready_thread_set);
}

pub fn queue_thread_list_refresh(state: &mut ClientState) {
    thread_tree::queue_thread_tree_refresh(&mut state.threads.list_refresh_requested);
}

pub fn take_thread_list_refresh_request(state: &mut ClientState) -> bool {
    thread_tree::take_thread_tree_refresh_request(&mut state.threads.list_refresh_requested)
}

pub fn apply_pending_requests_reduction(
    state: &mut ClientState,
    reduction: PendingRequestsReduction,
) -> bool {
    state.pending_requests.apply(reduction)
}

pub fn clear_thread_pending_requests(
    state: &mut ClientState,
    workspace_id: String,
    thread_id: String,
) -> bool {
    apply_pending_requests_reduction(
        state,
        reduce_pending_request_thread_closed_cleanup(workspace_id, thread_id),
    )
}

pub fn remove_thread_scoped_entries(
    thread_id: &str,
    draft_thread_id: &mut Option<String>,
    coordinators: &mut HashMap<String, ThreadCoordinator>,
    thread_placements: &mut HashMap<String, ThreadPlacement>,
) -> bool {
    let cleared_draft = if draft_thread_id.as_deref() == Some(thread_id) {
        *draft_thread_id = None;
        true
    } else {
        false
    };
    coordinators.remove(thread_id);
    thread_placements.remove(thread_id);
    cleared_draft
}

#[allow(clippy::too_many_arguments)]
pub fn clear_thread_client_state(
    draft_thread_id: &mut Option<String>,
    coordinators: &mut HashMap<String, ThreadCoordinator>,
    thread_folders: &mut HashMap<String, ThreadFolder>,
    thread_placements: &mut HashMap<String, ThreadPlacement>,
    thread_agents_doc_summaries: &mut HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    thread_folder_expanded: &mut HashMap<String, bool>,
    thread_tree_selected_node_id: &mut Option<String>,
) {
    *draft_thread_id = None;
    coordinators.clear();
    thread_folders.clear();
    thread_placements.clear();
    thread_agents_doc_summaries.clear();
    thread_folder_expanded.clear();
    *thread_tree_selected_node_id = None;
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewayStatusMessage {
    Connecting,
    ConnectingNamed {
        endpoint_name: String,
    },
    StartingLocal,
    Reconnecting {
        endpoint_name: String,
        attempt: u32,
        delay_ms: u64,
    },
    ConnectedEndpoint {
        endpoint_name: String,
        gateway_base_url: GatewayBaseUrl,
    },
    Connected,
    NotConfigured,
    Unavailable,
    LocalStopped {
        gateway_base_url: GatewayBaseUrl,
    },
    RemoteUnavailable {
        endpoint_name: String,
        gateway_base_url: GatewayBaseUrl,
    },
    LocalConflictAt {
        gateway_base_url: GatewayBaseUrl,
    },
    LocalConflict,
    FailedCheck {
        error: String,
    },
    SubsystemFailed {
        error: String,
    },
    SubsystemNotReady,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewayStatusTextUpdate {
    KeepExisting,
    Set(GatewayStatusMessage),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct GatewayStatusEndpoint {
    pub name: String,
    pub gateway_base_url: GatewayBaseUrl,
    pub kind: GatewayEndpointKind,
}

impl GatewayStatusEndpoint {
    pub fn new(
        name: impl Into<String>,
        gateway_base_url: GatewayBaseUrl,
        kind: GatewayEndpointKind,
    ) -> Self {
        Self {
            name: name.into(),
            gateway_base_url,
            kind,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayStatusInput {
    pub connecting: bool,
    pub current_status_is_empty: bool,
    pub runtime_state: Option<Result<ActiveGatewayState, String>>,
    pub active_endpoint: Option<GatewayStatusEndpoint>,
    pub has_ready_ws_connection: bool,
    pub gateway_error: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct GatewayStatusProjection {
    pub status: GatewayStatusTextUpdate,
    pub status_level: GatewayStatusLevel,
    pub connection_state: GatewayConnectionState,
    pub clear_gateway_error: bool,
}

pub fn project_gateway_status(input: GatewayStatusInput) -> GatewayStatusProjection {
    if input.connecting {
        return GatewayStatusProjection {
            status: if input.current_status_is_empty {
                GatewayStatusTextUpdate::Set(GatewayStatusMessage::Connecting)
            } else {
                GatewayStatusTextUpdate::KeepExisting
            },
            status_level: GatewayStatusLevel::Neutral,
            connection_state: GatewayConnectionState::Connecting,
            clear_gateway_error: false,
        };
    }

    match input.runtime_state {
        Some(Ok(ActiveGatewayState::NotConfigured)) => GatewayStatusProjection {
            status: GatewayStatusTextUpdate::Set(GatewayStatusMessage::NotConfigured),
            status_level: GatewayStatusLevel::Degraded,
            connection_state: GatewayConnectionState::Idle,
            clear_gateway_error: false,
        },
        Some(Ok(ActiveGatewayState::Connected)) => {
            if input.has_ready_ws_connection {
                GatewayStatusProjection {
                    status: GatewayStatusTextUpdate::Set(connected_gateway_status(
                        input.active_endpoint,
                    )),
                    status_level: GatewayStatusLevel::Connected,
                    connection_state: GatewayConnectionState::Connected,
                    clear_gateway_error: true,
                }
            } else {
                GatewayStatusProjection {
                    status: GatewayStatusTextUpdate::Set(GatewayStatusMessage::Unavailable),
                    status_level: GatewayStatusLevel::Failed,
                    connection_state: GatewayConnectionState::Disconnected,
                    clear_gateway_error: false,
                }
            }
        }
        Some(Ok(ActiveGatewayState::Unreachable)) => GatewayStatusProjection {
            status: GatewayStatusTextUpdate::Set(unreachable_gateway_status(input.active_endpoint)),
            status_level: GatewayStatusLevel::Failed,
            connection_state: GatewayConnectionState::Disconnected,
            clear_gateway_error: false,
        },
        Some(Ok(ActiveGatewayState::LocalAddressConflict)) => GatewayStatusProjection {
            status: GatewayStatusTextUpdate::Set(local_conflict_gateway_status(
                input.active_endpoint,
            )),
            status_level: GatewayStatusLevel::Failed,
            connection_state: GatewayConnectionState::Disconnected,
            clear_gateway_error: false,
        },
        Some(Err(error)) => GatewayStatusProjection {
            status: GatewayStatusTextUpdate::Set(GatewayStatusMessage::FailedCheck { error }),
            status_level: GatewayStatusLevel::Failed,
            connection_state: GatewayConnectionState::Disconnected,
            clear_gateway_error: false,
        },
        None => match input.gateway_error {
            Some(error) => GatewayStatusProjection {
                status: GatewayStatusTextUpdate::Set(GatewayStatusMessage::SubsystemFailed {
                    error,
                }),
                status_level: GatewayStatusLevel::Failed,
                connection_state: GatewayConnectionState::Disconnected,
                clear_gateway_error: false,
            },
            None => GatewayStatusProjection {
                status: GatewayStatusTextUpdate::Set(GatewayStatusMessage::SubsystemNotReady),
                status_level: GatewayStatusLevel::Neutral,
                connection_state: GatewayConnectionState::Idle,
                clear_gateway_error: false,
            },
        },
    }
}

fn connected_gateway_status(
    active_endpoint: Option<GatewayStatusEndpoint>,
) -> GatewayStatusMessage {
    match active_endpoint {
        Some(active) => GatewayStatusMessage::ConnectedEndpoint {
            endpoint_name: active.name,
            gateway_base_url: active.gateway_base_url,
        },
        None => GatewayStatusMessage::Connected,
    }
}

fn unreachable_gateway_status(
    active_endpoint: Option<GatewayStatusEndpoint>,
) -> GatewayStatusMessage {
    match active_endpoint {
        Some(active) => {
            unavailable_gateway_status(active.name, active.kind, active.gateway_base_url)
        }
        None => GatewayStatusMessage::Unavailable,
    }
}

fn local_conflict_gateway_status(
    active_endpoint: Option<GatewayStatusEndpoint>,
) -> GatewayStatusMessage {
    match active_endpoint {
        Some(active) => GatewayStatusMessage::LocalConflictAt {
            gateway_base_url: active.gateway_base_url,
        },
        None => GatewayStatusMessage::LocalConflict,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayConnectionEvent {
    Connecting {
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
    },
    Connected {
        endpoint_name: String,
        gateway_base_url: GatewayBaseUrl,
        queue_skills_refresh: bool,
    },
    Reconnecting {
        endpoint_name: String,
        attempt: u32,
        delay_ms: u64,
        reason: String,
        should_resume_in_flight_turn: bool,
    },
    Disconnected {
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        gateway_base_url: GatewayBaseUrl,
        reason: String,
        should_resume_in_flight_turn: bool,
    },
    ConnectFailed {
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        gateway_base_url: GatewayBaseUrl,
        error: String,
        should_resume_in_flight_turn: bool,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewaySettingsConnectionError {
    GatewayNotConnected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewaySettingsConnectionUpdate {
    pub clear_settings: bool,
    pub loading: bool,
    pub error: Option<GatewaySettingsConnectionError>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayConnectionReduction {
    pub status: GatewayStatusMessage,
    pub status_level: GatewayStatusLevel,
    pub connection_state: GatewayConnectionState,
    pub gateway_error: Option<String>,
    pub settings: Option<GatewaySettingsConnectionUpdate>,
    pub thread_list_loading: Option<bool>,
    pub workspaces_loading: Option<bool>,
    pub workspaces_error: Option<Option<String>>,
    pub clear_active_thread: bool,
    pub reset_thread_start: bool,
    pub clear_thread_start_queue: bool,
    pub clear_turn_resume_queue: bool,
    pub effects: Vec<ClientEffect>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayOperationSuccessInfo {
    pub ws_connection_id: Option<u64>,
    pub ws_connected_ready: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayOperationFinishOutcome {
    Success(GatewayOperationSuccessInfo),
    Failure { error: String },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct GatewayOperationBeginReduction {
    pub connecting: bool,
    pub connection_state: GatewayConnectionState,
    pub status_level: GatewayStatusLevel,
    pub gateway_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayOperationFinishReduction {
    pub connecting: bool,
    pub clear_setup_action: bool,
    pub bootstrap_complete: bool,
    pub ws_connection_id: Option<u64>,
    pub status: Option<GatewayStatusMessage>,
    pub status_level: Option<GatewayStatusLevel>,
    pub connection_state: GatewayConnectionState,
    pub gateway_error: Option<String>,
    pub clear_active_thread: bool,
    pub reset_thread_start: bool,
    pub clear_thread_start_queue: bool,
    pub clear_turn_resume_queue: bool,
    pub disconnect_ws: bool,
    pub refresh_gateway_status: bool,
    pub sync_gateway_setup_form_state: bool,
    pub effects: Vec<ClientEffect>,
    pub drive_turn_resume_queue: bool,
}

pub fn reduce_gateway_connection_event(
    event: GatewayConnectionEvent,
) -> GatewayConnectionReduction {
    match event {
        GatewayConnectionEvent::Connecting {
            endpoint_name,
            endpoint_kind,
        } => GatewayConnectionReduction {
            status: if endpoint_kind == GatewayEndpointKind::Local {
                GatewayStatusMessage::StartingLocal
            } else {
                GatewayStatusMessage::ConnectingNamed { endpoint_name }
            },
            status_level: GatewayStatusLevel::Neutral,
            connection_state: GatewayConnectionState::Connecting,
            gateway_error: None,
            settings: Some(GatewaySettingsConnectionUpdate {
                clear_settings: true,
                loading: false,
                error: None,
            }),
            thread_list_loading: None,
            workspaces_loading: None,
            workspaces_error: None,
            clear_active_thread: false,
            reset_thread_start: false,
            clear_thread_start_queue: false,
            clear_turn_resume_queue: false,
            effects: Vec::new(),
        },
        GatewayConnectionEvent::Connected {
            endpoint_name,
            gateway_base_url,
            queue_skills_refresh,
        } => {
            let mut effects = vec![
                ClientEffect::RefreshWorkspaceList,
                ClientEffect::RefreshGatewaySettings,
                ClientEffect::RefreshProviderLists,
            ];
            if queue_skills_refresh {
                effects.push(ClientEffect::QueueSkillsRefresh);
            }
            effects.push(ClientEffect::EnqueueInFlightTurnsForResume);

            GatewayConnectionReduction {
                status: GatewayStatusMessage::ConnectedEndpoint {
                    endpoint_name,
                    gateway_base_url,
                },
                status_level: GatewayStatusLevel::Connected,
                connection_state: GatewayConnectionState::Connected,
                gateway_error: None,
                settings: None,
                thread_list_loading: Some(false),
                workspaces_loading: Some(false),
                workspaces_error: Some(None),
                clear_active_thread: false,
                reset_thread_start: true,
                clear_thread_start_queue: true,
                clear_turn_resume_queue: true,
                effects,
            }
        }
        GatewayConnectionEvent::Reconnecting {
            endpoint_name,
            attempt,
            delay_ms,
            reason,
            should_resume_in_flight_turn,
        } => GatewayConnectionReduction {
            status: GatewayStatusMessage::Reconnecting {
                endpoint_name,
                attempt,
                delay_ms,
            },
            status_level: GatewayStatusLevel::Degraded,
            connection_state: GatewayConnectionState::Reconnecting,
            gateway_error: Some(reason),
            settings: Some(gateway_disconnected_settings_update()),
            thread_list_loading: Some(false),
            workspaces_loading: Some(false),
            workspaces_error: None,
            clear_active_thread: !should_resume_in_flight_turn,
            reset_thread_start: true,
            clear_thread_start_queue: true,
            clear_turn_resume_queue: true,
            effects: Vec::new(),
        },
        GatewayConnectionEvent::Disconnected {
            endpoint_name,
            endpoint_kind,
            gateway_base_url,
            reason,
            should_resume_in_flight_turn,
        } => GatewayConnectionReduction {
            status: unavailable_gateway_status(endpoint_name, endpoint_kind, gateway_base_url),
            status_level: GatewayStatusLevel::Failed,
            connection_state: GatewayConnectionState::Disconnected,
            gateway_error: Some(reason),
            settings: Some(gateway_disconnected_settings_update()),
            thread_list_loading: Some(false),
            workspaces_loading: Some(false),
            workspaces_error: None,
            clear_active_thread: !should_resume_in_flight_turn,
            reset_thread_start: true,
            clear_thread_start_queue: true,
            clear_turn_resume_queue: true,
            effects: Vec::new(),
        },
        GatewayConnectionEvent::ConnectFailed {
            endpoint_name,
            endpoint_kind,
            gateway_base_url,
            error,
            should_resume_in_flight_turn,
        } => GatewayConnectionReduction {
            status: unavailable_gateway_status(endpoint_name, endpoint_kind, gateway_base_url),
            status_level: GatewayStatusLevel::Failed,
            connection_state: GatewayConnectionState::Disconnected,
            gateway_error: Some(error),
            settings: Some(gateway_disconnected_settings_update()),
            thread_list_loading: Some(false),
            workspaces_loading: Some(false),
            workspaces_error: None,
            clear_active_thread: !should_resume_in_flight_turn,
            reset_thread_start: true,
            clear_thread_start_queue: true,
            clear_turn_resume_queue: true,
            effects: Vec::new(),
        },
    }
}

pub fn reduce_gateway_operation_begin() -> GatewayOperationBeginReduction {
    GatewayOperationBeginReduction {
        connecting: true,
        connection_state: GatewayConnectionState::Connecting,
        status_level: GatewayStatusLevel::Neutral,
        gateway_error: None,
    }
}

pub fn reduce_gateway_operation_finish(
    outcome: GatewayOperationFinishOutcome,
) -> GatewayOperationFinishReduction {
    match outcome {
        GatewayOperationFinishOutcome::Success(success) => {
            let waiting_for_ws_ready =
                success.ws_connection_id.is_some() && !success.ws_connected_ready;
            let connected_ready = success.ws_connection_id.is_some() && success.ws_connected_ready;
            let connection_state = if waiting_for_ws_ready {
                GatewayConnectionState::Connecting
            } else if success.ws_connection_id.is_some() {
                GatewayConnectionState::Connected
            } else {
                GatewayConnectionState::Idle
            };

            GatewayOperationFinishReduction {
                connecting: false,
                clear_setup_action: true,
                bootstrap_complete: true,
                ws_connection_id: success.ws_connection_id,
                status: waiting_for_ws_ready.then_some(GatewayStatusMessage::Connecting),
                status_level: waiting_for_ws_ready.then_some(GatewayStatusLevel::Neutral),
                connection_state,
                gateway_error: None,
                clear_active_thread: success.ws_connection_id.is_none(),
                reset_thread_start: true,
                clear_thread_start_queue: true,
                clear_turn_resume_queue: true,
                disconnect_ws: success.ws_connection_id.is_none(),
                refresh_gateway_status: !waiting_for_ws_ready,
                sync_gateway_setup_form_state: !waiting_for_ws_ready,
                effects: if connected_ready {
                    vec![
                        ClientEffect::RefreshWorkspaceList,
                        ClientEffect::RefreshGatewaySettings,
                        ClientEffect::RefreshProviderLists,
                        ClientEffect::EnqueueInFlightTurnsForResume,
                    ]
                } else {
                    Vec::new()
                },
                drive_turn_resume_queue: connected_ready,
            }
        }
        GatewayOperationFinishOutcome::Failure { error } => GatewayOperationFinishReduction {
            connecting: false,
            clear_setup_action: true,
            bootstrap_complete: true,
            ws_connection_id: None,
            status: None,
            status_level: None,
            connection_state: GatewayConnectionState::Disconnected,
            gateway_error: Some(error),
            clear_active_thread: true,
            reset_thread_start: true,
            clear_thread_start_queue: true,
            clear_turn_resume_queue: true,
            disconnect_ws: true,
            refresh_gateway_status: true,
            sync_gateway_setup_form_state: true,
            effects: Vec::new(),
            drive_turn_resume_queue: false,
        },
    }
}

fn gateway_disconnected_settings_update() -> GatewaySettingsConnectionUpdate {
    GatewaySettingsConnectionUpdate {
        clear_settings: true,
        loading: false,
        error: Some(GatewaySettingsConnectionError::GatewayNotConnected),
    }
}

fn unavailable_gateway_status(
    endpoint_name: String,
    endpoint_kind: GatewayEndpointKind,
    gateway_base_url: GatewayBaseUrl,
) -> GatewayStatusMessage {
    match endpoint_kind {
        GatewayEndpointKind::Local => GatewayStatusMessage::LocalStopped { gateway_base_url },
        GatewayEndpointKind::Remote => GatewayStatusMessage::RemoteUnavailable {
            endpoint_name,
            gateway_base_url,
        },
    }
}

pub fn remember_last_active_thread_for_workspace(
    state: &mut ClientState,
    workspace_id: &str,
    thread_id: Option<String>,
) {
    thread_session::remember_thread_for_workspace(
        &mut state.threads.last_active_thread_by_workspace,
        workspace_id,
        thread_id,
    );
}

pub fn remember_draft_thread_for_workspace(
    state: &mut ClientState,
    workspace_id: &str,
    thread_id: Option<String>,
) {
    thread_session::remember_thread_for_workspace(
        &mut state.threads.draft_thread_by_workspace,
        workspace_id,
        thread_id,
    );
}

pub fn toggle_thread_folder_expanded(state: &mut ClientState, folder_id: &str) -> bool {
    thread_tree::toggle_thread_folder_expanded(&mut state.threads.folder_expanded, folder_id)
}

pub fn set_thread_folder_expanded(state: &mut ClientState, folder_id: &str, expanded: bool) {
    thread_tree::set_thread_folder_expanded(
        &mut state.threads.folder_expanded,
        folder_id,
        expanded,
    );
}

pub fn set_workspaces(state: &mut ClientState, workspaces: Vec<Workspace>) {
    state.workspaces.workspaces = workspaces;
}

pub fn upsert_workspace_catalog_item(workspaces: &mut Vec<Workspace>, workspace: Workspace) {
    workspace_actions::upsert_workspace_catalog_item(workspaces, workspace);
}

pub fn upsert_thread_coordinator<'a>(
    state: &'a mut ClientState,
    thread_id: &str,
    workspace_id: &str,
) -> &'a mut ThreadCoordinator {
    upsert_thread_coordinator_in(&mut state.threads.coordinators, thread_id, workspace_id)
}

pub fn upsert_thread_coordinator_in<'a>(
    coordinators: &'a mut HashMap<String, ThreadCoordinator>,
    thread_id: &str,
    workspace_id: &str,
) -> &'a mut ThreadCoordinator {
    let coordinator = coordinators
        .entry(thread_id.to_owned())
        .or_insert_with(|| ThreadCoordinator::pending(thread_id, workspace_id));
    coordinator.set_workspace_id(workspace_id);
    coordinator
}

pub fn upsert_thread_snapshot(state: &mut ClientState, thread: Thread) -> &mut ThreadCoordinator {
    upsert_thread_snapshot_in(&mut state.threads.coordinators, thread)
}

pub fn upsert_thread_snapshot_in(
    coordinators: &mut HashMap<String, ThreadCoordinator>,
    thread: Thread,
) -> &mut ThreadCoordinator {
    let thread_id = thread.id.clone();
    match coordinators.entry(thread_id) {
        std::collections::hash_map::Entry::Occupied(mut occupied) => {
            occupied.get_mut().set_snapshot(thread);
            occupied.into_mut()
        }
        std::collections::hash_map::Entry::Vacant(vacant) => {
            vacant.insert(ThreadCoordinator::new(thread))
        }
    }
}

pub fn set_thread_tree_snapshot(
    state: &mut ClientState,
    folders: Vec<ThreadFolder>,
    placements: Vec<ThreadPlacement>,
    agents_docs: Vec<ThreadAgentsDocSummary>,
) {
    let normalized = thread_tree::normalize_thread_tree_snapshot(
        folders,
        placements,
        &state.threads.folder_expanded,
    );
    state.threads.folders = normalized.folders_by_id;
    state.threads.folder_expanded = normalized.folder_expanded;
    state.threads.placements = normalized.placements_by_thread_id;
    state.threads.agents_doc_summaries = thread_agents_doc_summaries_by_scope(agents_docs);
}

pub fn thread_agents_doc_summaries_by_scope(
    summaries: Vec<ThreadAgentsDocSummary>,
) -> HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary> {
    agents_doc_scope::thread_agents_doc_summaries_by_scope(summaries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::approvals::PendingRequest;
    use pioneer_protocol::{
        ThreadAgentsDocStatus, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
        TurnPermissionApprovalRequest,
    };

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

    fn pending_request(request_id: &str, workspace_id: &str, thread_id: &str) -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: request_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: "turn".to_owned(),
            visible_thread_ids: Vec::new(),
            tool_name: "exec_command".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: format!("{request_id}_scope"),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: None,
            details: Vec::new(),
        })
    }

    #[test]
    fn clear_thread_client_state_clears_thread_scoped_state() {
        let mut draft_thread_id = Some("thread_a".to_owned());
        let mut coordinators = HashMap::from([(
            "thread_a".to_owned(),
            ThreadCoordinator::new(thread("thread_a", "ws", 1)),
        )]);
        let mut thread_folders = HashMap::from([(
            "folder_a".to_owned(),
            ThreadFolder {
                id: "folder_a".to_owned(),
                workspace_id: "ws".to_owned(),
                parent_folder_id: None,
                name: "Folder".to_owned(),
                created_at: 1,
                updated_at: 1,
            },
        )]);
        let mut thread_placements = HashMap::from([(
            "thread_a".to_owned(),
            ThreadPlacement {
                thread_id: "thread_a".to_owned(),
                workspace_id: "ws".to_owned(),
                folder_id: Some("folder_a".to_owned()),
            },
        )]);
        let mut thread_agents_doc_summaries = HashMap::from([(
            ThreadAgentsDocSummaryKey::Root,
            ThreadAgentsDocSummary {
                id: "agents_doc_root".to_owned(),
                workspace_id: "ws".to_owned(),
                folder_id: None,
                status: ThreadAgentsDocStatus::Active,
                content_sha256: "sha".to_owned(),
                version: 1,
                char_count: 10,
                updated_at: 1,
            },
        )]);
        let mut thread_folder_expanded = HashMap::from([("folder_a".to_owned(), true)]);
        let mut thread_tree_selected_node_id = Some("folder_a".to_owned());
        clear_thread_client_state(
            &mut draft_thread_id,
            &mut coordinators,
            &mut thread_folders,
            &mut thread_placements,
            &mut thread_agents_doc_summaries,
            &mut thread_folder_expanded,
            &mut thread_tree_selected_node_id,
        );

        assert!(draft_thread_id.is_none());
        assert!(coordinators.is_empty());
        assert!(thread_folders.is_empty());
        assert!(thread_placements.is_empty());
        assert!(thread_agents_doc_summaries.is_empty());
        assert!(thread_folder_expanded.is_empty());
        assert!(thread_tree_selected_node_id.is_none());
    }

    #[test]
    fn unsubscribe_ids_include_active_thread_and_are_deduped() {
        let coordinators = HashMap::from([
            (
                "thread_b".to_owned(),
                ThreadCoordinator::new(thread("thread_b", "ws", 2)),
            ),
            (
                "thread_a".to_owned(),
                ThreadCoordinator::new(thread("thread_a", "ws", 1)),
            ),
        ]);

        assert_eq!(
            collect_known_thread_ids_for_unsubscribe(&coordinators, Some("thread_missing")),
            vec![
                "thread_a".to_owned(),
                "thread_b".to_owned(),
                "thread_missing".to_owned()
            ]
        );
        assert_eq!(
            collect_known_thread_ids_for_unsubscribe(&coordinators, Some("thread_a")),
            vec!["thread_a".to_owned(), "thread_b".to_owned()]
        );
    }

    #[test]
    fn gateway_switch_cleanup_plan_returns_unsubscribe_effect_and_clear_flags() {
        let coordinators = HashMap::from([(
            "thread_a".to_owned(),
            ThreadCoordinator::new(thread("thread_a", "ws", 1)),
        )]);

        let plan = plan_gateway_switch_cleanup(&coordinators, Some("thread_active"));

        assert_eq!(plan.thread_list_loading, false);
        assert!(plan.clear_active_thread);
        assert!(plan.clear_thread_conversations);
        assert!(plan.rebuild_thread_tree);
        assert!(plan.reset_thread_start);
        assert!(plan.clear_thread_start_queue);
        assert!(plan.clear_turn_resume_queue);
        assert!(plan.clear_gateway_settings);
        assert_eq!(
            plan.effects,
            vec![ClientEffect::UnsubscribeThreads {
                thread_ids: vec!["thread_a".to_owned(), "thread_active".to_owned()]
            }]
        );
    }

    #[test]
    fn thread_list_refresh_request_batches_until_taken() {
        let mut state = ClientState::default();

        assert!(!take_thread_list_refresh_request(&mut state));
        queue_thread_list_refresh(&mut state);
        queue_thread_list_refresh(&mut state);
        assert!(state.threads.list_refresh_requested);
        assert!(take_thread_list_refresh_request(&mut state));
        assert!(!state.threads.list_refresh_requested);
        assert!(!take_thread_list_refresh_request(&mut state));
    }

    #[test]
    fn pending_request_reducer_replaces_and_cleans_thread_scope() {
        let mut state = ClientState::default();

        assert!(apply_pending_requests_reduction(
            &mut state,
            PendingRequestsReduction::Opened(pending_request("req_a", "ws", "thread"))
        ));
        assert!(apply_pending_requests_reduction(
            &mut state,
            PendingRequestsReduction::Opened(pending_request("req_b", "ws", "thread"))
        ));
        assert_eq!(state.pending_requests.requests().len(), 2);

        let mut replacement = pending_request("req_a", "ws", "thread");
        replacement.message = Some("replacement".to_owned());
        assert!(apply_pending_requests_reduction(
            &mut state,
            PendingRequestsReduction::Opened(replacement)
        ));
        assert_eq!(state.pending_requests.requests().len(), 2);
        assert_eq!(
            state
                .pending_requests
                .request("req_a")
                .and_then(|request| request.message.as_deref()),
            Some("replacement")
        );

        assert!(clear_thread_pending_requests(
            &mut state,
            "ws".to_owned(),
            "thread".to_owned()
        ));
        assert!(state.pending_requests.is_empty());
    }

    #[test]
    fn thread_start_request_queue_batches_until_taken_or_cleared() {
        let mut start_requested = false;

        assert!(!dequeue_thread_start_request(&mut start_requested));
        enqueue_thread_start_request(&mut start_requested);
        enqueue_thread_start_request(&mut start_requested);
        assert!(dequeue_thread_start_request(&mut start_requested));
        assert!(!dequeue_thread_start_request(&mut start_requested));

        enqueue_thread_start_request(&mut start_requested);
        clear_thread_start_request(&mut start_requested);
        assert!(!start_requested);
    }

    #[test]
    fn turn_resume_queue_reducer_dedupes_dequeues_and_clears() {
        let mut ready_threads = VecDeque::new();
        let mut ready_thread_set = HashSet::new();

        assert!(enqueue_turn_resume_thread(
            &mut ready_threads,
            &mut ready_thread_set,
            "thread_a".to_owned(),
        ));
        assert!(!enqueue_turn_resume_thread(
            &mut ready_threads,
            &mut ready_thread_set,
            "thread_a".to_owned(),
        ));
        assert!(enqueue_turn_resume_thread(
            &mut ready_threads,
            &mut ready_thread_set,
            "thread_b".to_owned(),
        ));

        assert_eq!(
            dequeue_turn_resume_thread(&mut ready_threads, &mut ready_thread_set).as_deref(),
            Some("thread_a")
        );
        assert!(!ready_thread_set.contains("thread_a"));
        assert!(ready_thread_set.contains("thread_b"));

        clear_turn_resume_queue(&mut ready_threads, &mut ready_thread_set);
        assert!(ready_threads.is_empty());
        assert!(ready_thread_set.is_empty());
    }

    #[test]
    fn thread_coordinator_map_upserts_pending_and_snapshots_existing() {
        let mut coordinators = HashMap::new();

        let pending = upsert_thread_coordinator_in(&mut coordinators, "thread_a", "ws_a");
        assert_eq!(pending.workspace_id, "ws_a");
        assert!(pending.thread().is_none());

        let snapshot = thread("thread_a", "ws_b", 42);
        let coordinator = upsert_thread_snapshot_in(&mut coordinators, snapshot);

        assert_eq!(coordinator.workspace_id, "ws_b");
        assert_eq!(
            coordinator.thread().map(|thread| thread.updated_at),
            Some(42)
        );
        assert_eq!(coordinators.len(), 1);
    }

    #[test]
    fn remove_thread_scoped_entries_clears_only_target_thread_state() {
        let mut draft_thread_id = Some("thread_a".to_owned());
        let mut coordinators = HashMap::from([
            (
                "thread_a".to_owned(),
                ThreadCoordinator::new(thread("thread_a", "ws", 1)),
            ),
            (
                "thread_b".to_owned(),
                ThreadCoordinator::new(thread("thread_b", "ws", 2)),
            ),
        ]);
        let mut placements = HashMap::from([
            (
                "thread_a".to_owned(),
                ThreadPlacement {
                    thread_id: "thread_a".to_owned(),
                    workspace_id: "ws".to_owned(),
                    folder_id: None,
                },
            ),
            (
                "thread_b".to_owned(),
                ThreadPlacement {
                    thread_id: "thread_b".to_owned(),
                    workspace_id: "ws".to_owned(),
                    folder_id: None,
                },
            ),
        ]);
        assert!(remove_thread_scoped_entries(
            "thread_a",
            &mut draft_thread_id,
            &mut coordinators,
            &mut placements,
        ));

        assert!(draft_thread_id.is_none());
        assert!(!coordinators.contains_key("thread_a"));
        assert!(coordinators.contains_key("thread_b"));
        assert!(!placements.contains_key("thread_a"));
        assert!(placements.contains_key("thread_b"));
    }

    #[test]
    fn connection_reducer_projects_connected_effects() {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Connected {
            endpoint_name: "Local".to_owned(),
            gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                "127.0.0.1:17878",
            )
            .unwrap(),
            queue_skills_refresh: true,
        });

        assert_eq!(
            reduction.status,
            GatewayStatusMessage::ConnectedEndpoint {
                endpoint_name: "Local".to_owned(),
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                    "127.0.0.1:17878"
                )
                .unwrap(),
            }
        );
        assert_eq!(reduction.status_level, GatewayStatusLevel::Connected);
        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Connected
        );
        assert_eq!(reduction.thread_list_loading, Some(false));
        assert_eq!(reduction.workspaces_loading, Some(false));
        assert_eq!(reduction.workspaces_error, Some(None));
        assert!(reduction.reset_thread_start);
        assert!(reduction.clear_thread_start_queue);
        assert!(reduction.clear_turn_resume_queue);
        assert_eq!(
            reduction.effects,
            vec![
                ClientEffect::RefreshWorkspaceList,
                ClientEffect::RefreshGatewaySettings,
                ClientEffect::RefreshProviderLists,
                ClientEffect::QueueSkillsRefresh,
                ClientEffect::EnqueueInFlightTurnsForResume,
            ]
        );
    }

    #[test]
    fn gateway_operation_begin_reducer_projects_connecting_state() {
        let reduction = reduce_gateway_operation_begin();

        assert!(reduction.connecting);
        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Connecting
        );
        assert_eq!(reduction.status_level, GatewayStatusLevel::Neutral);
        assert!(reduction.gateway_error.is_none());
    }

    #[test]
    fn gateway_operation_finish_reducer_projects_ready_connection() {
        let reduction = reduce_gateway_operation_finish(GatewayOperationFinishOutcome::Success(
            GatewayOperationSuccessInfo {
                ws_connection_id: Some(7),
                ws_connected_ready: true,
            },
        ));

        assert!(!reduction.connecting);
        assert!(reduction.clear_setup_action);
        assert!(reduction.bootstrap_complete);
        assert_eq!(reduction.ws_connection_id, Some(7));
        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Connected
        );
        assert!(reduction.gateway_error.is_none());
        assert!(!reduction.clear_active_thread);
        assert!(!reduction.disconnect_ws);
        assert!(reduction.refresh_gateway_status);
        assert!(reduction.sync_gateway_setup_form_state);
        assert!(reduction.drive_turn_resume_queue);
        assert_eq!(
            reduction.effects,
            vec![
                ClientEffect::RefreshWorkspaceList,
                ClientEffect::RefreshGatewaySettings,
                ClientEffect::RefreshProviderLists,
                ClientEffect::EnqueueInFlightTurnsForResume,
            ]
        );
    }

    #[test]
    fn gateway_operation_finish_reducer_waits_for_ws_ready() {
        let reduction = reduce_gateway_operation_finish(GatewayOperationFinishOutcome::Success(
            GatewayOperationSuccessInfo {
                ws_connection_id: Some(7),
                ws_connected_ready: false,
            },
        ));

        assert_eq!(reduction.status, Some(GatewayStatusMessage::Connecting));
        assert_eq!(reduction.status_level, Some(GatewayStatusLevel::Neutral));
        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Connecting
        );
        assert!(!reduction.refresh_gateway_status);
        assert!(!reduction.sync_gateway_setup_form_state);
        assert!(reduction.effects.is_empty());
        assert!(!reduction.drive_turn_resume_queue);
    }

    #[test]
    fn gateway_operation_finish_reducer_projects_failure() {
        let reduction = reduce_gateway_operation_finish(GatewayOperationFinishOutcome::Failure {
            error: "network failed".to_owned(),
        });

        assert!(!reduction.connecting);
        assert!(reduction.clear_setup_action);
        assert!(reduction.bootstrap_complete);
        assert_eq!(reduction.ws_connection_id, None);
        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Disconnected
        );
        assert_eq!(reduction.gateway_error.as_deref(), Some("network failed"));
        assert!(reduction.clear_active_thread);
        assert!(reduction.reset_thread_start);
        assert!(reduction.clear_thread_start_queue);
        assert!(reduction.clear_turn_resume_queue);
        assert!(reduction.disconnect_ws);
        assert!(reduction.refresh_gateway_status);
        assert!(reduction.sync_gateway_setup_form_state);
        assert!(reduction.effects.is_empty());
    }

    #[test]
    fn gateway_status_projection_preserves_existing_connecting_text() {
        let projection = project_gateway_status(GatewayStatusInput {
            connecting: true,
            current_status_is_empty: false,
            runtime_state: Some(Ok(ActiveGatewayState::Connected)),
            active_endpoint: None,
            has_ready_ws_connection: false,
            gateway_error: None,
        });

        assert_eq!(
            projection,
            GatewayStatusProjection {
                status: GatewayStatusTextUpdate::KeepExisting,
                status_level: GatewayStatusLevel::Neutral,
                connection_state: GatewayConnectionState::Connecting,
                clear_gateway_error: false,
            }
        );
    }

    #[test]
    fn gateway_status_projection_maps_runtime_states_to_ui_neutral_metadata() {
        let connected = project_gateway_status(GatewayStatusInput {
            connecting: false,
            current_status_is_empty: true,
            runtime_state: Some(Ok(ActiveGatewayState::Connected)),
            active_endpoint: Some(GatewayStatusEndpoint::new(
                "Remote",
                GatewayBaseUrl::parse_presentation("https://gateway.example").unwrap(),
                GatewayEndpointKind::Remote,
            )),
            has_ready_ws_connection: true,
            gateway_error: Some("old error".to_owned()),
        });

        assert_eq!(
            connected,
            GatewayStatusProjection {
                status: GatewayStatusTextUpdate::Set(GatewayStatusMessage::ConnectedEndpoint {
                    endpoint_name: "Remote".to_owned(),
                    gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                        "https://gateway.example"
                    )
                    .unwrap(),
                }),
                status_level: GatewayStatusLevel::Connected,
                connection_state: GatewayConnectionState::Connected,
                clear_gateway_error: true,
            }
        );

        let unreachable = project_gateway_status(GatewayStatusInput {
            connecting: false,
            current_status_is_empty: true,
            runtime_state: Some(Ok(ActiveGatewayState::Unreachable)),
            active_endpoint: Some(GatewayStatusEndpoint::new(
                "Local",
                GatewayBaseUrl::parse_presentation("127.0.0.1:17878").unwrap(),
                GatewayEndpointKind::Local,
            )),
            has_ready_ws_connection: false,
            gateway_error: None,
        });

        assert_eq!(
            unreachable.status,
            GatewayStatusTextUpdate::Set(GatewayStatusMessage::LocalStopped {
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                    "127.0.0.1:17878"
                )
                .unwrap(),
            })
        );
        assert_eq!(unreachable.status_level, GatewayStatusLevel::Failed);
        assert_eq!(
            unreachable.connection_state,
            GatewayConnectionState::Disconnected
        );
    }

    #[test]
    fn gateway_status_projection_maps_runtime_absence_and_errors() {
        let failed_check = project_gateway_status(GatewayStatusInput {
            connecting: false,
            current_status_is_empty: true,
            runtime_state: Some(Err("probe failed".to_owned())),
            active_endpoint: None,
            has_ready_ws_connection: false,
            gateway_error: None,
        });
        assert_eq!(
            failed_check.status,
            GatewayStatusTextUpdate::Set(GatewayStatusMessage::FailedCheck {
                error: "probe failed".to_owned(),
            })
        );
        assert_eq!(failed_check.status_level, GatewayStatusLevel::Failed);

        let subsystem_failed = project_gateway_status(GatewayStatusInput {
            connecting: false,
            current_status_is_empty: true,
            runtime_state: None,
            active_endpoint: None,
            has_ready_ws_connection: false,
            gateway_error: Some("boot failed".to_owned()),
        });
        assert_eq!(
            subsystem_failed.status,
            GatewayStatusTextUpdate::Set(GatewayStatusMessage::SubsystemFailed {
                error: "boot failed".to_owned(),
            })
        );
        assert_eq!(
            subsystem_failed.connection_state,
            GatewayConnectionState::Disconnected
        );

        let subsystem_not_ready = project_gateway_status(GatewayStatusInput {
            connecting: false,
            current_status_is_empty: true,
            runtime_state: None,
            active_endpoint: None,
            has_ready_ws_connection: false,
            gateway_error: None,
        });
        assert_eq!(
            subsystem_not_ready.status,
            GatewayStatusTextUpdate::Set(GatewayStatusMessage::SubsystemNotReady)
        );
        assert_eq!(
            subsystem_not_ready.connection_state,
            GatewayConnectionState::Idle
        );
    }

    #[test]
    fn connection_reducer_preserves_resumable_active_thread_on_reconnect() {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Reconnecting {
            endpoint_name: "Remote".to_owned(),
            attempt: 2,
            delay_ms: 500,
            reason: "network".to_owned(),
            should_resume_in_flight_turn: true,
        });

        assert_eq!(reduction.status_level, GatewayStatusLevel::Degraded);
        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Reconnecting
        );
        assert_eq!(reduction.gateway_error.as_deref(), Some("network"));
        assert!(!reduction.clear_active_thread);
        assert_eq!(
            reduction.settings,
            Some(GatewaySettingsConnectionUpdate {
                clear_settings: true,
                loading: false,
                error: Some(GatewaySettingsConnectionError::GatewayNotConnected),
            })
        );
    }

    #[test]
    fn connection_reducer_clears_active_thread_when_disconnect_cannot_resume() {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Disconnected {
            endpoint_name: "Remote".to_owned(),
            endpoint_kind: GatewayEndpointKind::Remote,
            gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                "example.test",
            )
            .unwrap(),
            reason: "closed".to_owned(),
            should_resume_in_flight_turn: false,
        });

        assert_eq!(
            reduction.status,
            GatewayStatusMessage::RemoteUnavailable {
                endpoint_name: "Remote".to_owned(),
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                    "example.test"
                )
                .unwrap(),
            }
        );
        assert_eq!(reduction.status_level, GatewayStatusLevel::Failed);
        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Disconnected
        );
        assert!(reduction.clear_active_thread);
        assert!(reduction.effects.is_empty());
    }
}
