//! Shared client state reducers.

use crate::state::client_state::{GatewayConnectionState, GatewayStatusLevel};
use crate::{
    agents_doc::scope as agents_doc_scope,
    composer::draft as composer_draft,
    gateway::{runtime::ActiveGatewayState, types::GatewayEndpointKind},
    notifications::effects::ClientEffect,
    state::client_state::{ClientState, ThreadAgentsDocSummaryKey},
    threads::{
        coordinator::ThreadCoordinator,
        resume as thread_resume,
        start::{self as thread_start, ThreadStartCoordinator},
        tree as thread_tree,
    },
    workspaces::actions as workspace_actions,
};
use pioneer_protocol::{Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, Workspace};
use std::collections::{HashMap, HashSet, VecDeque};

pub fn set_active_thread_id(state: &mut ClientState, thread_id: Option<String>) -> bool {
    let changed = state.threads.active_thread_id != thread_id;
    state.threads.active_thread_id = thread_id;
    changed
}

pub fn clear_active_thread_if_matches(state: &mut ClientState, thread_id: &str) -> bool {
    if state.threads.active_thread_id.as_deref() == Some(thread_id) {
        state.threads.active_thread_id = None;
        return true;
    }
    false
}

pub fn set_draft_thread_id(state: &mut ClientState, thread_id: Option<String>) {
    state.threads.draft_thread_id = thread_id;
}

pub fn clear_draft_thread_if_matches(state: &mut ClientState, thread_id: &str) -> bool {
    if state.threads.draft_thread_id.as_deref() == Some(thread_id) {
        state.threads.draft_thread_id = None;
        return true;
    }
    false
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

pub fn remove_thread_scoped_entries<AttachmentDraft, CapabilityDraft, RefreshState>(
    thread_id: &str,
    draft_thread_id: &mut Option<String>,
    coordinators: &mut HashMap<String, ThreadCoordinator>,
    thread_drafts: &mut HashMap<String, String>,
    thread_draft_attachments: &mut HashMap<String, AttachmentDraft>,
    thread_draft_capabilities: &mut HashMap<String, CapabilityDraft>,
    thread_placements: &mut HashMap<String, ThreadPlacement>,
    turn_timeline_refresh: &mut HashMap<(String, String), RefreshState>,
) -> bool {
    let cleared_draft = if draft_thread_id.as_deref() == Some(thread_id) {
        *draft_thread_id = None;
        true
    } else {
        false
    };
    coordinators.remove(thread_id);
    thread_drafts.remove(thread_id);
    thread_draft_attachments.remove(thread_id);
    thread_draft_capabilities.remove(thread_id);
    thread_placements.remove(thread_id);
    turn_timeline_refresh.retain(|(refresh_thread_id, _), _| refresh_thread_id != thread_id);
    cleared_draft
}

#[allow(clippy::too_many_arguments)]
pub fn clear_thread_client_state<Attachment, Capability, RefreshState>(
    draft_thread_id: &mut Option<String>,
    coordinators: &mut HashMap<String, ThreadCoordinator>,
    thread_drafts: &mut HashMap<String, String>,
    thread_draft_attachments: &mut HashMap<String, Vec<Attachment>>,
    thread_draft_capabilities: &mut HashMap<String, Vec<Capability>>,
    composer_attachments: &mut Vec<Attachment>,
    composer_capabilities: &mut Vec<Capability>,
    composer_upload_in_progress: &mut bool,
    composer_upload_error: &mut Option<String>,
    composer_selected_provider: &mut Option<String>,
    composer_selected_model: &mut Option<String>,
    composer_model_selection_manually_selected: &mut bool,
    thread_folders: &mut HashMap<String, ThreadFolder>,
    thread_placements: &mut HashMap<String, ThreadPlacement>,
    thread_agents_doc_summaries: &mut HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    thread_folder_expanded: &mut HashMap<String, bool>,
    thread_tree_selected_node_id: &mut Option<String>,
    turn_timeline_refresh: &mut HashMap<(String, String), RefreshState>,
) {
    *draft_thread_id = None;
    coordinators.clear();
    composer_draft::clear_all_composer_drafts(
        thread_drafts,
        thread_draft_attachments,
        thread_draft_capabilities,
    );
    composer_attachments.clear();
    composer_capabilities.clear();
    *composer_upload_in_progress = false;
    *composer_upload_error = None;
    *composer_selected_provider = None;
    *composer_selected_model = None;
    *composer_model_selection_manually_selected = false;
    thread_folders.clear();
    thread_placements.clear();
    thread_agents_doc_summaries.clear();
    thread_folder_expanded.clear();
    *thread_tree_selected_node_id = None;
    turn_timeline_refresh.clear();
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
        address: String,
    },
    Connected,
    NotConfigured,
    Unavailable,
    LocalStopped {
        address: String,
    },
    RemoteUnavailable {
        endpoint_name: String,
        address: String,
    },
    LocalConflictAt {
        address: String,
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
    pub address: String,
    pub kind: GatewayEndpointKind,
}

impl GatewayStatusEndpoint {
    pub fn new(
        name: impl Into<String>,
        address: impl Into<String>,
        kind: GatewayEndpointKind,
    ) -> Self {
        Self {
            name: name.into(),
            address: address.into(),
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
            address: active.address,
        },
        None => GatewayStatusMessage::Connected,
    }
}

fn unreachable_gateway_status(
    active_endpoint: Option<GatewayStatusEndpoint>,
) -> GatewayStatusMessage {
    match active_endpoint {
        Some(active) => unavailable_gateway_status(active.name, active.kind, active.address),
        None => GatewayStatusMessage::Unavailable,
    }
}

fn local_conflict_gateway_status(
    active_endpoint: Option<GatewayStatusEndpoint>,
) -> GatewayStatusMessage {
    match active_endpoint {
        Some(active) => GatewayStatusMessage::LocalConflictAt {
            address: active.address,
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
        address: String,
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
        address: String,
        reason: String,
        should_resume_in_flight_turn: bool,
    },
    ConnectFailed {
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        error: String,
        should_resume_in_flight_turn: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewaySettingsConnectionUpdate {
    pub clear_settings: bool,
    pub loading: bool,
    pub error: Option<String>,
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
            address,
            queue_skills_refresh,
        } => {
            let mut effects = vec![
                ClientEffect::RefreshWorkspaceList,
                ClientEffect::RefreshGatewaySettings,
            ];
            if queue_skills_refresh {
                effects.push(ClientEffect::QueueSkillsRefresh);
            }
            effects.push(ClientEffect::EnqueueInFlightTurnsForResume);

            GatewayConnectionReduction {
                status: GatewayStatusMessage::ConnectedEndpoint {
                    endpoint_name,
                    address,
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
            address,
            reason,
            should_resume_in_flight_turn,
        } => GatewayConnectionReduction {
            status: unavailable_gateway_status(endpoint_name, endpoint_kind, address),
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
            address,
            error,
            should_resume_in_flight_turn,
        } => GatewayConnectionReduction {
            status: unavailable_gateway_status(endpoint_name, endpoint_kind, address),
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

fn gateway_disconnected_settings_update() -> GatewaySettingsConnectionUpdate {
    GatewaySettingsConnectionUpdate {
        clear_settings: true,
        loading: false,
        error: Some("Gateway is not connected".to_owned()),
    }
}

fn unavailable_gateway_status(
    endpoint_name: String,
    endpoint_kind: GatewayEndpointKind,
    address: String,
) -> GatewayStatusMessage {
    match endpoint_kind {
        GatewayEndpointKind::Local => GatewayStatusMessage::LocalStopped { address },
        GatewayEndpointKind::Remote => GatewayStatusMessage::RemoteUnavailable {
            endpoint_name,
            address,
        },
    }
}

pub fn remember_last_active_thread_for_workspace(
    state: &mut ClientState,
    workspace_id: &str,
    thread_id: Option<String>,
) {
    thread_tree::remember_thread_for_workspace(
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
    thread_tree::remember_thread_for_workspace(
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
    use pioneer_protocol::{
        ThreadAgentsDocStatus, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
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

    #[test]
    fn clear_thread_client_state_clears_thread_composer_and_selection_state() {
        let mut draft_thread_id = Some("thread_a".to_owned());
        let mut coordinators = HashMap::from([(
            "thread_a".to_owned(),
            ThreadCoordinator::new(thread("thread_a", "ws", 1)),
        )]);
        let mut thread_drafts = HashMap::from([("thread_a".to_owned(), "draft".to_owned())]);
        let mut thread_draft_attachments =
            HashMap::from([("thread_a".to_owned(), vec!["attachment".to_owned()])]);
        let mut thread_draft_capabilities =
            HashMap::from([("thread_a".to_owned(), vec!["skill".to_owned()])]);
        let mut composer_attachments = vec!["active-attachment".to_owned()];
        let mut composer_capabilities = vec!["active-skill".to_owned()];
        let mut composer_upload_in_progress = true;
        let mut composer_upload_error = Some("failed".to_owned());
        let mut composer_selected_provider = Some("openai".to_owned());
        let mut composer_selected_model = Some("gpt-5.4".to_owned());
        let mut composer_model_selection_manually_selected = true;
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
        let mut turn_timeline_refresh =
            HashMap::from([(("thread_a".to_owned(), "turn_a".to_owned()), true)]);

        clear_thread_client_state(
            &mut draft_thread_id,
            &mut coordinators,
            &mut thread_drafts,
            &mut thread_draft_attachments,
            &mut thread_draft_capabilities,
            &mut composer_attachments,
            &mut composer_capabilities,
            &mut composer_upload_in_progress,
            &mut composer_upload_error,
            &mut composer_selected_provider,
            &mut composer_selected_model,
            &mut composer_model_selection_manually_selected,
            &mut thread_folders,
            &mut thread_placements,
            &mut thread_agents_doc_summaries,
            &mut thread_folder_expanded,
            &mut thread_tree_selected_node_id,
            &mut turn_timeline_refresh,
        );

        assert!(draft_thread_id.is_none());
        assert!(coordinators.is_empty());
        assert!(thread_drafts.is_empty());
        assert!(thread_draft_attachments.is_empty());
        assert!(thread_draft_capabilities.is_empty());
        assert!(composer_attachments.is_empty());
        assert!(composer_capabilities.is_empty());
        assert!(!composer_upload_in_progress);
        assert!(composer_upload_error.is_none());
        assert!(composer_selected_provider.is_none());
        assert!(composer_selected_model.is_none());
        assert!(!composer_model_selection_manually_selected);
        assert!(thread_folders.is_empty());
        assert!(thread_placements.is_empty());
        assert!(thread_agents_doc_summaries.is_empty());
        assert!(thread_folder_expanded.is_empty());
        assert!(thread_tree_selected_node_id.is_none());
        assert!(turn_timeline_refresh.is_empty());
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
    fn remove_thread_scoped_entries_clears_only_target_thread() {
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
        let mut drafts = HashMap::from([
            ("thread_a".to_owned(), "draft a".to_owned()),
            ("thread_b".to_owned(), "draft b".to_owned()),
        ]);
        let mut attachments = HashMap::from([
            ("thread_a".to_owned(), vec!["a"]),
            ("thread_b".to_owned(), vec!["b"]),
        ]);
        let mut capabilities = HashMap::from([
            ("thread_a".to_owned(), vec!["a"]),
            ("thread_b".to_owned(), vec!["b"]),
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
        let mut refresh = HashMap::from([
            (("thread_a".to_owned(), "turn_1".to_owned()), 1u8),
            (("thread_b".to_owned(), "turn_2".to_owned()), 2u8),
        ]);

        assert!(remove_thread_scoped_entries(
            "thread_a",
            &mut draft_thread_id,
            &mut coordinators,
            &mut drafts,
            &mut attachments,
            &mut capabilities,
            &mut placements,
            &mut refresh,
        ));

        assert!(draft_thread_id.is_none());
        assert!(!coordinators.contains_key("thread_a"));
        assert!(coordinators.contains_key("thread_b"));
        assert!(!refresh.contains_key(&("thread_a".to_owned(), "turn_1".to_owned())));
        assert!(refresh.contains_key(&("thread_b".to_owned(), "turn_2".to_owned())));
    }

    #[test]
    fn connection_reducer_projects_connected_effects() {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Connected {
            endpoint_name: "Local".to_owned(),
            address: "127.0.0.1:17878".to_owned(),
            queue_skills_refresh: true,
        });

        assert_eq!(
            reduction.status,
            GatewayStatusMessage::ConnectedEndpoint {
                endpoint_name: "Local".to_owned(),
                address: "127.0.0.1:17878".to_owned(),
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
                ClientEffect::QueueSkillsRefresh,
                ClientEffect::EnqueueInFlightTurnsForResume,
            ]
        );
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
                "https://gateway.example",
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
                    address: "https://gateway.example".to_owned(),
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
                "127.0.0.1:17878",
                GatewayEndpointKind::Local,
            )),
            has_ready_ws_connection: false,
            gateway_error: None,
        });

        assert_eq!(
            unreachable.status,
            GatewayStatusTextUpdate::Set(GatewayStatusMessage::LocalStopped {
                address: "127.0.0.1:17878".to_owned(),
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
                error: Some("Gateway is not connected".to_owned()),
            })
        );
    }

    #[test]
    fn connection_reducer_clears_active_thread_when_disconnect_cannot_resume() {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Disconnected {
            endpoint_name: "Remote".to_owned(),
            endpoint_kind: GatewayEndpointKind::Remote,
            address: "example.test".to_owned(),
            reason: "closed".to_owned(),
            should_resume_in_flight_turn: false,
        });

        assert_eq!(
            reduction.status,
            GatewayStatusMessage::RemoteUnavailable {
                endpoint_name: "Remote".to_owned(),
                address: "example.test".to_owned(),
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
