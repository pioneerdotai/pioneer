mod model_selection;
mod mutations;
mod queries;
mod state;
mod view;

use crate::{
    app::{
        conversation::Conversation,
        editor::AgentsDocEditor,
        gateway_setup::{GatewaySetupDialogState, GatewaySetupFormState},
        skills::details::table::SkillDiagnosticsTableDelegate,
        thread::{ThreadCoordinator, view::timeline::TimelineRenderModel},
    },
    audio::{
        capture::{
            DesktopVoiceCaptureErrorKind, DesktopVoiceCaptureFlow, PlatformDesktopAudioInputBackend,
        },
        microphone::DesktopMicrophoneGateReport,
    },
    gateway::{ClientRuntime, GatewayRuntime, GatewayWsCommandSender},
};
use gpui::{prelude::*, *};
use gpui_component::{
    VirtualListScrollHandle, input::InputState, table::TableState, tree::TreeState,
};
use gpui_terminal::TerminalView;
pub(super) use pioneer_client::{
    agents_doc::scope::{
        AgentsDocEditorScope as ThreadAgentsDocEditorScope, ThreadAgentsDocSummaryKey,
    },
    artifacts::actions::ArtifactActionStatus as ThreadArtifactActionStatus,
    artifacts::preview::ArtifactPreviewImagePaths as ThreadArtifactPreviewImagePaths,
    artifacts::state::{ThreadArtifactFilter, ThreadArtifactsState},
    cli_runtime::approvals::{PendingRequest, PendingRequestState},
    composer::capabilities::{ComposerCapability, ComposerCapabilityKind},
    composer::{
        attachments::{ComposerAttachment, ComposerAttachmentUploadState},
        turn_prepare::PrepareVoiceComposerSnapshotRequest,
    },
    gateway::runtime::GatewaySetupAction,
    providers::list::ProviderListState,
    providers::presentation::ProviderModelDisplayKey,
    providers::selectors::ProviderFilter,
    skills::upload::SkillUploadProgress,
    state::client_state::{GatewayConnectionState, GatewayStatusLevel},
    tasks::review::TaskReviewActionState,
    threads::{resume::ThreadResumeCoordinator, start::ThreadStartCoordinator},
    timeline::semantic::{SemanticTimelineRequestKey, SemanticTimelineState},
};
use pioneer_protocol::{
    CLIRuntimeThreadBinding, GatewaySettingsSnapshot, McpListItem, McpServerDetailsResponse,
    SkillHealthItem, SkillListItem, Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadMode,
    ThreadPlacement, TurnPermissionMode, VoiceStatus, Workspace,
};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    rc::Rc,
    sync::{Arc, atomic::AtomicBool},
};

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

pub(super) struct GatewayCoordinator {
    pub(super) runtime: Option<GatewayRuntime>,
    pub(super) client_runtime: ClientRuntime,
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
    pub(super) autoscroll_paused_by_user: bool,
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
    pub(super) cached_semantic_model_active_thread_id: Option<String>,
    pub(super) cached_semantic_model_revision: u64,
    pub(super) cached_semantic_model: Option<TimelineRenderModel>,
    pub(super) expanded_revision: u64,
    pub(super) pending_scroll_anchor: Option<TimelineScrollAnchor>,
    pub(super) semantic_prefetch_scroll_generation: u64,
    pub(super) semantic_prefetch_consumed_scroll_generation: u64,
    pub(super) running_turn_indicator_timer_active: bool,
    pub(super) running_turn_indicator_tick: u64,
    pub(super) running_turn_indicator_fallback_turn_id: Option<String>,
    pub(super) running_turn_indicator_fallback_started_at_unix_ms: Option<i64>,
}

pub(super) struct TimelineScrollAnchor {
    pub(super) thread_id: String,
    pub(super) row_key: String,
    pub(super) row_top_offset_px: Pixels,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct DesktopVoiceHoldTarget {
    pub(super) center: Point<Pixels>,
    pub(super) radius: Pixels,
}

impl DesktopVoiceHoldTarget {
    pub(super) fn contains(self, position: Point<Pixels>) -> bool {
        let dx = position.x - self.center.x;
        let dy = position.y - self.center.y;
        let dx = f32::from(dx);
        let dy = f32::from(dy);
        let radius = f32::from(self.radius);
        let distance_squared = dx * dx + dy * dy;
        distance_squared <= radius * radius
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DesktopVoiceReleaseCandidate {
    Send,
    Cancel,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum DesktopVoiceComposerState {
    Idle,
    Preparing {
        target: DesktopVoiceHoldTarget,
        candidate: DesktopVoiceReleaseCandidate,
        release_requested: bool,
    },
    Holding {
        target: DesktopVoiceHoldTarget,
        candidate: DesktopVoiceReleaseCandidate,
    },
    Finalizing,
    Error {
        kind: DesktopVoiceCaptureErrorKind,
        message: String,
    },
}

impl DesktopVoiceComposerState {
    pub(super) fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Preparing { .. } | Self::Holding { .. } | Self::Finalizing
        )
    }

    pub(super) fn error_message(&self) -> Option<&str> {
        match self {
            Self::Error { message, .. } => Some(message.as_str()),
            _ => None,
        }
    }
}

impl Default for DesktopVoiceComposerState {
    fn default() -> Self {
        Self::Idle
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TaskThreadNavigationEntry {
    pub(super) parent_thread_id: String,
    pub(super) child_thread_id: String,
    pub(super) workspace_id: String,
    pub(super) title: String,
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
    pub(super) remote_access_settings_expanded: bool,
    pub(super) remote_access_key_input_revision: u64,
    pub(super) remote_access_status_poll_generation: u64,
    pub(super) settings_tree_state: Entity<TreeState>,
    pub(super) provider_tree_state: Entity<TreeState>,
    pub(super) thread_list_loading: bool,
    pub(super) thread_list_refresh_requested: bool,
    pub(super) active_thread_id: Option<String>,
    pub(super) draft_thread_id: Option<String>,
    pub(super) task_thread_navigation_stack: Vec<TaskThreadNavigationEntry>,
    pub(super) preferred_workspace_id: Option<String>,
    pub(super) workspaces: Vec<Workspace>,
    pub(super) workspaces_loading: bool,
    pub(super) workspaces_error: Option<String>,
    pub(super) workspace_action_in_progress: bool,
    pub(super) last_active_thread_by_workspace: HashMap<String, String>,
    pub(super) draft_thread_by_workspace: HashMap<String, String>,
    pub(super) composer_state: Entity<InputState>,
    pub(super) composer_attachments: Vec<ComposerAttachment>,
    pub(super) composer_capabilities: Vec<ComposerCapability>,
    pub(super) composer_upload_in_progress: bool,
    pub(super) composer_upload_error: Option<String>,
    pub(super) composer_turn_mode: ThreadMode,
    pub(super) composer_selected_provider: Option<String>,
    pub(super) composer_selected_model: Option<String>,
    pub(super) composer_selected_reasoning_effort: Option<String>,
    pub(super) composer_permission_mode: TurnPermissionMode,
    pub(super) desktop_microphone_gate: DesktopMicrophoneGateReport,
    pub(super) desktop_voice_status: VoiceStatus,
    pub(super) desktop_voice_status_error: Option<String>,
    pub(super) desktop_voice_status_poll_generation: u64,
    pub(super) desktop_voice_composer: DesktopVoiceComposerState,
    pub(super) desktop_voice_prepare_request: Option<PrepareVoiceComposerSnapshotRequest>,
    pub(super) desktop_voice_capture:
        Option<DesktopVoiceCaptureFlow<PlatformDesktopAudioInputBackend, GatewayWsCommandSender>>,
    pub(super) composer_model_selection_manually_selected: bool,
    pub(super) composer_model_display_cache: HashMap<ProviderModelDisplayKey, Option<String>>,
    pub(super) composer_model_display_loading_key: Option<ProviderModelDisplayKey>,
    pub(super) main_content_view: MainContentView,
    pub(super) providers: ProviderListState,
    pub(super) pending_requests: PendingRequestState,
    pub(super) cli_runtime_thread_bindings: HashMap<String, CLIRuntimeThreadBinding>,
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
    pub(super) thread_draft_capabilities: HashMap<String, Vec<ComposerCapability>>,
    pub(super) thread_draft_permission_modes: HashMap<String, TurnPermissionMode>,
    pub(super) thread_start: ThreadStartCoordinator,
    pub(super) thread_start_requested: bool,
    pub(super) thread_timeline_scroll_handle: VirtualListScrollHandle,
    pub(super) thread_timeline_view_state: RefCell<ThreadTimelineViewState>,
    pub(super) thread_timeline_item_expanded: RefCell<HashSet<String>>,
    pub(super) thread_timeline_terminal_item: RefCell<HashMap<String, CachedTimelineTerminal>>,
    pub(super) semantic_timelines: SemanticTimelineState,
    pub(super) semantic_timeline_revision: u64,
    pub(super) semantic_timeline_in_flight: HashSet<SemanticTimelineRequestKey>,
    pub(super) task_review_actions: TaskReviewActionState,
    pub(super) thread_artifacts: ThreadArtifactsState,
    pub(super) show_thread_artifacts_sidebar: bool,
    pub(super) thread_artifacts_sidebar_width: Pixels,
    pub(super) ready_turn_resume_threads: VecDeque<String>,
    pub(super) ready_turn_resume_thread_set: HashSet<String>,
    pub(super) gateway_setup_form_state: Entity<GatewaySetupFormState>,
    pub(super) gateway: GatewayCoordinator,
    pub(super) show_sidebar: bool,
    pub(super) sidebar_panel_width: Pixels,
}
