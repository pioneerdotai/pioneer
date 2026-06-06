//! Aggregate client state.

use crate::{
    mcp::list::McpListState,
    providers::list::ProviderListState,
    skills::catalog::SkillCatalogState,
    threads::{coordinator::ThreadCoordinator, start::ThreadStartCoordinator},
    turns::timeline_refresh::TurnTimelineRefreshState,
};
use pioneer_protocol::{
    GatewaySettingsSnapshot, ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, Workspace,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
};

pub use crate::agents_doc::scope::ThreadAgentsDocSummaryKey;
pub use crate::threads::tree::WorkspaceThreadState;

#[derive(Default)]
pub struct ClientState {
    pub threads: ThreadsState,
    pub workspaces: WorkspacesState,
    pub gateway: GatewayClientState,
    pub providers: ProvidersState,
    pub mcp: McpState,
    pub skills: SkillsState,
    pub settings: SettingsState,
}

#[derive(Default)]
pub struct ThreadsState {
    pub coordinators: HashMap<String, ThreadCoordinator>,
    pub folders: HashMap<String, ThreadFolder>,
    pub placements: HashMap<String, ThreadPlacement>,
    pub agents_doc_summaries: HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    pub folder_expanded: HashMap<String, bool>,
    pub list_loading: bool,
    pub list_refresh_requested: bool,
    pub active_thread_id: Option<String>,
    pub draft_thread_id: Option<String>,
    pub last_active_thread_by_workspace: HashMap<String, String>,
    pub draft_thread_by_workspace: HashMap<String, String>,
    pub start: ThreadStartCoordinator,
    pub start_requested: bool,
    pub ready_turn_resume_threads: VecDeque<String>,
    pub ready_turn_resume_thread_set: HashSet<String>,
    pub turn_timeline_refresh: HashMap<TurnTimelineRefreshKey, TurnTimelineRefreshState>,
}

#[derive(Default)]
pub struct WorkspacesState {
    pub preferred_workspace_id: Option<String>,
    pub workspaces: Vec<Workspace>,
    pub loading: bool,
    pub error: Option<String>,
    pub action_in_progress: bool,
}

#[derive(Default)]
pub struct GatewayClientState {
    pub connection_epoch: u64,
    pub ws_connection_id: Option<u64>,
    pub bootstrap_complete: bool,
    pub settings: Option<GatewaySettingsSnapshot>,
    pub settings_loading: bool,
    pub settings_error: Option<String>,
}

pub type ProvidersState = ProviderListState;

pub type McpState = McpListState;

pub type SkillsState = SkillCatalogState;

#[derive(Default)]
pub struct SettingsState {
    pub loading: bool,
    pub error: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewayStatusLevel {
    Neutral,
    Connected,
    Degraded,
    Failed,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewayConnectionState {
    Idle,
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
}

impl GatewayConnectionState {
    pub fn is_transitioning(self) -> bool {
        matches!(self, Self::Connecting | Self::Reconnecting)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TurnTimelineRefreshKey {
    pub thread_id: String,
    pub turn_id: String,
}

impl TurnTimelineRefreshKey {
    pub fn new(thread_id: impl Into<String>, turn_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
        }
    }
}

impl From<(String, String)> for TurnTimelineRefreshKey {
    fn from((thread_id, turn_id): (String, String)) -> Self {
        Self { thread_id, turn_id }
    }
}

impl From<TurnTimelineRefreshKey> for (String, String) {
    fn from(key: TurnTimelineRefreshKey) -> Self {
        (key.thread_id, key.turn_id)
    }
}
