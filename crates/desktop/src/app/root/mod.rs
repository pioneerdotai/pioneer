mod model_selection;
mod mutations;
mod queries;
mod state;
mod view;

pub(in crate::app) use queries::resolve_active_workspace_id;

use crate::{
    app::{
        conversation::Conversation,
        editor::AgentsDocEditor,
        gateway_setup::{GatewaySetupDialogState, GatewaySetupFormState},
        skills::details::table::SkillDiagnosticsTableDelegate,
        thread::{ThreadCoordinator, view::timeline::model::TimelineRow},
    },
    gateway::{GatewayRuntime, GatewayWsClient, GatewayWsCommandSender},
};
use gpui::{prelude::*, *};
use gpui_component::{
    VirtualListScrollHandle, input::InputState, table::TableState, tree::TreeState,
};
use gpui_terminal::TerminalView;
use pioneer_protocol::{
    ArtifactRef, ArtifactSummary, GatewaySettingsSnapshot, McpListItem, McpServerDetailsResponse,
    SkillHealthItem, SkillListItem, Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadMode,
    ThreadPlacement, Workspace,
};
use std::{
    cell::RefCell,
    collections::hash_map::Entry,
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    rc::Rc,
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GatewaySetupAction {
    ConnectRemote,
    StartLocal,
    SaveGateway,
    DeleteGateway,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GatewayOperationSource {
    InitialSetup,
    AddGatewayDialog,
}

impl GatewayOperationSource {
    pub(super) fn close_dialog_on_success(self) -> bool {
        matches!(self, Self::AddGatewayDialog)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum GatewaySetupFormMode {
    Initial { allow_local: bool },
    AddGateway { allow_local: bool },
    EditGateway { endpoint_id: String },
}

impl GatewaySetupFormMode {
    pub(super) fn allow_local(&self) -> bool {
        match self {
            Self::Initial { allow_local } | Self::AddGateway { allow_local } => *allow_local,
            Self::EditGateway { .. } => false,
        }
    }

    pub(super) fn operation_source(&self) -> Option<GatewayOperationSource> {
        match self {
            Self::Initial { .. } => Some(GatewayOperationSource::InitialSetup),
            Self::AddGateway { .. } => Some(GatewayOperationSource::AddGatewayDialog),
            Self::EditGateway { .. } => None,
        }
    }

    pub(super) fn remote_button_id(&self) -> &'static str {
        match self {
            Self::Initial { .. } => "connect-remote-gateway",
            Self::AddGateway { .. } => "add-connect-remote-gateway",
            Self::EditGateway { .. } => "save-gateway",
        }
    }

    pub(super) fn local_button_id(&self) -> &'static str {
        match self {
            Self::Initial { .. } => "start-local-gateway",
            Self::AddGateway { .. } => "add-start-local-gateway",
            Self::EditGateway { .. } => "delete-gateway",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GatewayStatusLevel {
    Neutral,
    Connected,
    Degraded,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GatewayConnectionState {
    Idle,
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
}

#[derive(Clone, Debug)]
pub(super) struct SkillUploadProgress {
    pub label: String,
    pub sent_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MainContentView {
    Threads,
    AgentsDoc,
    Providers,
    Mcp,
    McpDetails,
    Skills,
    SkillDetails,
    Settings,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SettingsContentView {
    General,
    Memory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProviderFilter {
    All,
    Connected,
}

impl GatewayConnectionState {
    pub(super) fn is_transitioning(self) -> bool {
        matches!(self, Self::Connecting | Self::Reconnecting)
    }
}

#[derive(Default)]
pub(super) struct ThreadResumeCoordinator {
    pub(super) in_progress: bool,
    pub(super) retry_attempt: u32,
    pub(super) next_attempt_at: Option<Instant>,
}

#[derive(Default)]
pub(super) struct ThreadStartCoordinator {
    pub(super) pending_thread_id: Option<String>,
    pub(super) in_progress: bool,
    pub(super) retry_attempt: u32,
    pub(super) next_attempt_at: Option<Instant>,
}

pub(super) struct GatewayCoordinator {
    pub(super) runtime: Option<GatewayRuntime>,
    pub(super) ws_client: GatewayWsClient,
    pub(super) ws_command_sender: GatewayWsCommandSender,
    pub(super) ws_connection_id: Option<u64>,
    pub(super) connection_epoch: u64,
    pub(super) connection_state: GatewayConnectionState,
    pub(super) status: String,
    pub(super) status_level: GatewayStatusLevel,
    pub(super) error: Option<String>,
    pub(super) connecting: bool,
    pub(super) setup_action: Option<GatewaySetupAction>,
    pub(super) bootstrap_complete: bool,
    pub(super) settings: Option<GatewaySettingsSnapshot>,
    pub(super) settings_loading: bool,
    pub(super) settings_error: Option<String>,
}

#[derive(Default)]
pub(super) struct ThreadTimelineViewState {
    pub(super) active_thread_id: Option<String>,
    pub(super) item_count: usize,
    pub(super) tail_entry_id: Option<String>,
    pub(super) tail_text_len: usize,
    pub(super) measured_list_width: Pixels,
    pub(super) pending_width_probe: bool,
    pub(super) width_probe_attempts: u8,
    pub(super) entry_layout_cache: HashMap<String, CachedTimelineEntryLayout>,
    pub(super) cached_render_active_thread_id: Option<String>,
    pub(super) cached_render_width_px: i32,
    pub(super) cached_render_item_count: usize,
    pub(super) cached_render_tail_entry_id: Option<String>,
    pub(super) cached_render_tail_layout_hash: u64,
    pub(super) cached_render_model_layout_hash: u64,
    pub(super) cached_item_sizes: Option<Rc<Vec<Size<Pixels>>>>,
    pub(super) expanded_revision: u64,
    pub(super) cached_model_signature_hash: u64,
    pub(super) cached_model_rows_layout_hash: u64,
    pub(super) cached_model_rows: Option<Rc<Vec<TimelineRow>>>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CachedTimelineEntryLayout {
    pub(super) layout_hash: u64,
    pub(super) height: Pixels,
}

pub(super) struct CachedTimelineTerminal {
    pub(super) content_hash: u64,
    pub(super) view: Entity<TerminalView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ComposerAttachmentKind {
    Image,
    File,
    Audio,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ComposerAttachment {
    pub(super) path: String,
    pub(super) file_name: String,
    pub(super) kind: ComposerAttachmentKind,
    pub(super) upload_state: ComposerAttachmentUploadState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ComposerAttachmentUploadState {
    Local,
    Uploading,
    Uploaded { artifact: ArtifactRef },
    Failed { error: String },
}

#[derive(Clone, Debug, Default)]
pub(super) struct TurnTimelineRefreshState {
    pub(super) in_flight: bool,
    pub(super) dirty: bool,
    pub(super) next_generation: u64,
    pub(super) in_flight_generation: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum ThreadArtifactFilter {
    #[default]
    All,
    Uploaded,
    Generated,
    TaskOutput,
    Images,
    Documents,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ThreadArtifactCacheEntry {
    pub(super) items: Vec<ArtifactSummary>,
    pub(super) loaded: bool,
    pub(super) error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct ThreadArtifactVersionKey {
    pub(super) artifact_id: String,
    pub(super) version_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ThreadArtifactLocalFile {
    pub(super) path: PathBuf,
    pub(super) sha256: String,
    pub(super) size_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ThreadArtifactActionStatus {
    Queued,
    Downloading,
    Verifying,
    Opening,
    Revealing,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ThreadArtifactPreviewImagePaths {
    pub(super) square_path: PathBuf,
    pub(super) detail_path: PathBuf,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ThreadArtifactsState {
    pub(super) active_thread_id: Option<String>,
    pub(super) loading: bool,
    pub(super) loading_thread_id: Option<String>,
    pub(super) loading_thread_ids: HashSet<String>,
    pub(super) refresh_requested_thread_ids: HashSet<String>,
    pub(super) retry_after_by_thread: HashMap<String, Instant>,
    pub(super) transient_retry_count_by_thread: HashMap<String, u8>,
    pub(super) error: Option<String>,
    pub(super) selected_artifact_id: Option<String>,
    pub(super) filter: ThreadArtifactFilter,
    pub(super) cache_by_thread: HashMap<String, ThreadArtifactCacheEntry>,
    pub(super) local_files_by_artifact: HashMap<ThreadArtifactVersionKey, ThreadArtifactLocalFile>,
    pub(super) action_status_by_artifact:
        HashMap<ThreadArtifactVersionKey, ThreadArtifactActionStatus>,
    pub(super) preview_image_path_by_artifact:
        HashMap<ThreadArtifactVersionKey, ThreadArtifactPreviewImagePaths>,
    pub(super) preview_loading_by_artifact: HashSet<ThreadArtifactVersionKey>,
    pub(super) preview_failed_by_artifact: HashSet<ThreadArtifactVersionKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum ThreadAgentsDocSummaryKey {
    Root,
    Folder(String),
}

impl ThreadAgentsDocSummaryKey {
    pub(super) fn from_folder_id(folder_id: Option<&str>) -> Self {
        match folder_id {
            Some(folder_id) => Self::Folder(folder_id.to_owned()),
            None => Self::Root,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ThreadAgentsDocEditorScope {
    Root {
        workspace_id: String,
    },
    Folder {
        workspace_id: String,
        folder_id: String,
    },
}

impl ThreadAgentsDocEditorScope {
    pub(super) fn folder_id(&self) -> Option<&str> {
        match self {
            Self::Root { .. } => None,
            Self::Folder { folder_id, .. } => Some(folder_id.as_str()),
        }
    }

    pub(super) fn workspace_id(&self) -> &str {
        match self {
            Self::Root { workspace_id } | Self::Folder { workspace_id, .. } => {
                workspace_id.as_str()
            }
        }
    }
}

pub struct PioneerDesktop {
    pub(super) thread_coordinators: HashMap<String, ThreadCoordinator>,
    pub(super) thread_folders: HashMap<String, ThreadFolder>,
    pub(super) thread_placements: HashMap<String, ThreadPlacement>,
    pub(super) thread_agents_doc_summaries:
        HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    pub(super) active_agents_doc_editor_scope: Option<ThreadAgentsDocEditorScope>,
    pub(super) agents_doc_editor: Option<Entity<AgentsDocEditor>>,
    pub(super) thread_folder_expanded: HashMap<String, bool>,
    pub(super) thread_tree_selected_node_id: Option<String>,
    pub(super) thread_tree_state: Entity<TreeState>,
    pub(super) settings_content_view: SettingsContentView,
    pub(super) settings_tree_state: Entity<TreeState>,
    pub(super) provider_tree_state: Entity<TreeState>,
    pub(super) thread_list_loading: bool,
    pub(super) thread_list_refresh_requested: bool,
    pub(super) active_thread_id: Option<String>,
    pub(super) draft_thread_id: Option<String>,
    pub(super) preferred_workspace_id: Option<String>,
    pub(super) workspaces: Vec<Workspace>,
    pub(super) workspaces_loading: bool,
    pub(super) workspaces_error: Option<String>,
    pub(super) workspace_action_in_progress: bool,
    pub(super) last_active_thread_by_workspace: HashMap<String, String>,
    pub(super) draft_thread_by_workspace: HashMap<String, String>,
    pub(super) composer_state: Entity<InputState>,
    pub(super) composer_attachments: Vec<ComposerAttachment>,
    pub(super) composer_upload_in_progress: bool,
    pub(super) composer_upload_error: Option<String>,
    pub(super) composer_turn_mode: ThreadMode,
    pub(super) composer_selected_provider: Option<String>,
    pub(super) composer_selected_model: Option<String>,
    pub(super) composer_model_selection_manually_selected: bool,
    pub(super) main_content_view: MainContentView,
    pub(super) provider_configured_names: HashSet<String>,
    pub(super) provider_filter: ProviderFilter,
    pub(super) providers_loading: bool,
    pub(super) providers_error: Option<String>,
    pub(super) mcp_servers: Vec<McpListItem>,
    pub(super) mcp_selected_server_id: Option<String>,
    pub(super) mcp_server_details: Option<McpServerDetailsResponse>,
    pub(super) mcp_loading: bool,
    pub(super) mcp_details_loading: bool,
    pub(super) mcp_error: Option<String>,
    pub(super) mcp_refresh_requested: bool,
    pub(super) mcp_details_refresh_requested: bool,
    pub(super) mcp_poller_started: bool,
    pub(super) mcp_pending_actions: HashSet<String>,
    pub(super) mcp_list_scroll_handle: VirtualListScrollHandle,
    pub(super) mcp_details_expanded_sections: HashSet<String>,
    pub(super) mcp_audit_table_state: Entity<TableState<SkillDiagnosticsTableDelegate>>,
    pub(super) installed_skills: Vec<SkillListItem>,
    pub(super) skills_catalog: Vec<SkillListItem>,
    pub(super) skills_health_details: HashMap<String, SkillHealthItem>,
    pub(super) skills_loading: bool,
    pub(super) skills_error: Option<String>,
    pub(super) skills_upload_progress: Option<SkillUploadProgress>,
    pub(super) skills_upload_cancel_token: Option<Arc<AtomicBool>>,
    pub(super) skills_refresh_requested: bool,
    pub(super) skills_poller_started: bool,
    pub(super) skills_pending_actions: HashSet<String>,
    pub(super) selected_skill_target: Option<(String, String)>,
    pub(super) skills_list_scroll_handle: VirtualListScrollHandle,
    pub(super) skills_details_expanded_sections: HashSet<String>,
    pub(super) skills_audit_table_state: Entity<TableState<SkillDiagnosticsTableDelegate>>,
    pub(super) thread_drafts: HashMap<String, String>,
    pub(super) thread_draft_attachments: HashMap<String, Vec<ComposerAttachment>>,
    pub(super) thread_start: ThreadStartCoordinator,
    pub(super) thread_start_requested: bool,
    pub(super) thread_timeline_scroll_handle: VirtualListScrollHandle,
    pub(super) thread_timeline_view_state: RefCell<ThreadTimelineViewState>,
    pub(super) thread_timeline_item_expanded: RefCell<HashSet<String>>,
    pub(super) thread_timeline_terminal_item: RefCell<HashMap<String, CachedTimelineTerminal>>,
    pub(super) thread_artifacts: ThreadArtifactsState,
    pub(super) show_thread_artifacts_sidebar: bool,
    pub(super) thread_artifacts_sidebar_width: Pixels,
    pub(super) ready_turn_resume_threads: VecDeque<String>,
    pub(super) ready_turn_resume_thread_set: HashSet<String>,
    pub(super) turn_timeline_refresh: HashMap<(String, String), TurnTimelineRefreshState>,
    pub(super) gateway_setup_form_state: Entity<GatewaySetupFormState>,
    pub(super) gateway: GatewayCoordinator,
    pub(super) show_sidebar: bool,
    pub(super) sidebar_panel_width: Pixels,
}
