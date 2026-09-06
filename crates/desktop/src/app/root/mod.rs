mod composer_domain;
mod desktop_update;
mod model_selection;
mod mutations;
mod queries;
mod state;
mod view;

use crate::{
    app::{
        editor::AgentsDocEditor,
        gateway_setup::{GatewaySetupDialogState, GatewaySetupFormState},
        invitation_join::DesktopInvitationJoinState,
        member_avatars::DesktopMemberAvatarState,
        settings::ProfileEditorState,
        skills::details::table::SkillDiagnosticsTableDelegate,
        startup::DesktopStartupCoordinator,
        thread::{
            ThreadCoordinator,
            message_revisions::DesktopMessageRevisionDialogState,
            view::timeline::{RunningIndicatorViewCache, TimelineLayoutIndex, TimelineRenderModel},
        },
    },
    audio::{
        capture::{
            DesktopVoiceCaptureErrorKind, DesktopVoiceCaptureFlow, PlatformDesktopAudioInputBackend,
        },
        microphone::DesktopMicrophoneGateReport,
    },
    code_highlight::DesktopCodeHighlightCache,
    components::member_picker::MemberPickerDelegate,
    gateway::{ClientRuntime, DesktopGatewayHttpClient, GatewayRuntime, GatewayWsCommandSender},
};
pub(super) use desktop_update::DesktopUpdateUiState;
use gpui_kit::component::{
    VirtualListScrollHandle, combobox::ComboboxState, input::TextareaState, table::TableState,
    tree::TreeState,
};
use gpui_kit::{prelude::*, *};
pub(super) use pioneer_client::{
    administration::AdministrationCache,
    agents_doc::scope::{
        AgentsDocEditorScope as ThreadAgentsDocEditorScope, ThreadAgentsDocSummaryKey,
    },
    artifacts::actions::{ArtifactActionStatus as ThreadArtifactActionStatus, ArtifactVersionKey},
    artifacts::preview::ArtifactPreviewImagePaths as ThreadArtifactPreviewImagePaths,
    artifacts::state::{ThreadArtifactFilter, ThreadArtifactsState},
    authorization::ThreadPresentationCapabilities,
    cli_runtime::approvals::PendingRequest,
    composer::capabilities::{
        ComposerCapability, ComposerCapabilityKind, ComposerCapabilityTarget,
    },
    composer::{
        attachments::{ComposerAttachment, ComposerAttachmentUploadState},
        draft::ComposerDraftLifecycleState,
        skill_selection::ComposerSkillSelection,
        state_machine::ComposerMentionCandidate,
        turn_prepare::PrepareVoiceComposerSnapshotRequest,
    },
    gateway::runtime::GatewaySetupAction,
    providers::list::ProviderListState,
    providers::presentation::ProviderModelDisplayKey,
    providers::selectors::ProviderFilter,
    skills::{catalog::SkillManagementProjection, upload::SkillUploadProgress},
    state::client_state::{GatewayConnectionState, GatewayStatusLevel},
    tasks::review::TaskReviewActionState,
    threads::scope::ThreadScopePendingAction,
    threads::start::ThreadStartCoordinator,
    timeline::rows::UserMessagePresentation,
};
use pioneer_protocol::{
    ArtifactRef, AuthMeResponse, CLIRuntimeThreadBinding, GatewaySettingsSnapshot, McpListItem,
    McpServerDetailsResponse, PrincipalId, SkillHealthItem, SkillId, SkillListItem, SkillPackId,
    TaskUserNotification, Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadMode,
    ThreadParticipantSummary, ThreadPlacement, ThreadVisibility, TurnPermissionMode, VoiceStatus,
    Workspace, WorkspaceId,
};
#[cfg(test)]
pub(crate) use queries::{
    composer_capability_target_for_provider, composer_submission_plan_for_provider,
};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::{Arc, atomic::AtomicBool},
};
use terminal::TerminalView;

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
    Initial {
        allow_local: bool,
    },
    AddGateway {
        allow_local: bool,
    },
    ReauthenticateGateway {
        endpoint_id: String,
        name: String,
        gateway_base_url: String,
        close_dialog_on_success: bool,
    },
    EditGateway {
        endpoint_id: String,
    },
}

impl GatewaySetupFormMode {
    pub(super) fn allow_local(&self) -> bool {
        match self {
            Self::Initial { allow_local } | Self::AddGateway { allow_local } => *allow_local,
            Self::ReauthenticateGateway { .. } | Self::EditGateway { .. } => false,
        }
    }

    pub(super) fn operation_source(&self) -> Option<GatewayOperationSource> {
        match self {
            Self::Initial { .. } => Some(GatewayOperationSource::InitialSetup),
            Self::AddGateway { .. } => Some(GatewayOperationSource::AddGatewayDialog),
            Self::ReauthenticateGateway { .. } | Self::EditGateway { .. } => None,
        }
    }

    pub(super) fn remote_button_id(&self) -> &'static str {
        match self {
            Self::Initial { .. } => "connect-remote-gateway",
            Self::AddGateway { .. } => "add-connect-remote-gateway",
            Self::ReauthenticateGateway { .. } => "reauthenticate-remote-gateway",
            Self::EditGateway { .. } => "save-gateway",
        }
    }

    pub(super) fn secondary_button_id(&self) -> Option<&'static str> {
        match self {
            Self::Initial { .. } => Some("start-local-gateway"),
            Self::AddGateway { .. } => Some("add-start-local-gateway"),
            Self::ReauthenticateGateway { .. } => None,
            Self::EditGateway { .. } => Some("delete-gateway"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MainContentView {
    Threads,
    AgentsDoc,
    Providers,
    Administration,
    Mcp,
    McpDetails,
    Skills,
    SkillDetails,
    Settings,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AdministrationContentView {
    Members,
    Invitations,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SettingsContentView {
    General,
    Account,
    Memory,
    SelfImprovement,
}

pub(super) struct GatewayCoordinator {
    pub(super) setup_view: Entity<crate::app::initial::InitialGatewaySetupView>,
    pub(super) switcher_view: Entity<crate::app::flow::GatewaySwitcherView>,
    pub(super) compatibility_task: Option<gpui_kit::Task<()>>,
    pub(super) settings_task: Option<gpui_kit::Task<()>>,
    pub(super) settings_binding: std::sync::Arc<crate::gateway::GatewaySettingsBinding>,
    pub(super) identity_task: Option<gpui_kit::Task<()>>,
    pub(super) session_task: Option<gpui_kit::Task<()>>,
    pub(super) transport_verification_task: Option<gpui_kit::Task<()>>,
    pub(super) transport_verification_id: Option<u64>,
    pub(super) applied_transport_revision: u64,
    pub(super) identity_binding: std::sync::Arc<crate::gateway::IdentityAuthorizationBinding>,
    pub(super) session_binding: std::sync::Arc<crate::gateway::GatewaySessionBinding>,
    pub(super) runtime: Option<GatewayRuntime>,
    pub(super) client_runtime: ClientRuntime,
    pub(super) http_client: Option<DesktopGatewayHttpClient>,
    pub(super) ws_connection_id: Option<u64>,
    pub(super) current_principal_refresh_generation: u64,

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
    pub(super) auth_session_action_error: Option<String>,
    pub(super) auth_session_action_pending: Option<pioneer_protocol::AuthSessionId>,
    pub(super) current_auth: Option<AuthMeResponse>,
    pub(super) capability_snapshot: Option<pioneer_protocol::AuthorizationCapabilitySnapshot>,
}

#[derive(Default)]
pub(super) struct ThreadTimelineViewState {
    pub(super) active_thread_id: Option<String>,
    pub(super) item_count: usize,
    pub(super) tail_entry_id: Option<String>,
    pub(super) tail_text_len: usize,
    pub(super) last_read_requested_through_turn_id: Option<String>,
    pub(super) autoscroll_paused_by_user: bool,
    pub(super) measured_list_width: Pixels,
    pub(super) pending_width_probe: bool,
    pub(super) width_probe_attempts: u8,
    pub(super) entry_layout_cache: HashMap<String, CachedTimelineEntryLayout>,
    pub(super) cached_render_active_thread_id: Option<String>,
    pub(super) cached_render_width_px: i32,
    pub(super) cached_render_item_count: usize,
    pub(super) cached_render_tail_entry_id: Option<String>,
    pub(super) cached_render_tail_fingerprint: u64,
    pub(super) cached_render_model_fingerprint: u64,
    pub(super) cached_render_principal_id: Option<String>,
    pub(super) cached_render_task_child_thread: bool,
    pub(super) cached_item_sizes: Option<Rc<Vec<Size<Pixels>>>>,
    pub(super) cached_timeline_layout_index: Option<Rc<TimelineLayoutIndex>>,
    pub(super) cached_semantic_model_active_thread_id: Option<String>,
    pub(super) cached_semantic_model_revision: u64,
    pub(super) cached_semantic_model: Option<TimelineRenderModel>,
    pub(super) expanded_revision: u64,
    pub(super) pending_scroll_anchor: Option<TimelineScrollAnchor>,
    pub(super) semantic_prefetch_scroll_generation: u64,
    pub(super) semantic_prefetch_consumed_scroll_generation: u64,
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
    pub(super) render_fingerprint: u64,
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
    Finalizing {
        thread_id: String,
    },
    Error {
        kind: DesktopVoiceCaptureErrorKind,
        message: String,
    },
}

impl DesktopVoiceComposerState {
    pub(super) fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Preparing { .. } | Self::Holding { .. } | Self::Finalizing { .. }
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

#[derive(Clone)]
pub(super) struct DesktopComposerEditTarget {
    pub(super) presentation: UserMessagePresentation,
    pub(super) preview: String,
    pub(super) artifacts: Vec<ArtifactRef>,
    pub(super) mention_selections: Vec<(PrincipalId, String)>,
    pub(super) error: Option<String>,
    pub(super) conflicted: bool,
}

pub struct PioneerDesktop {
    pub(super) startup: DesktopStartupCoordinator,
    pub(super) invitation_join: Option<Entity<DesktopInvitationJoinState>>,
    pub(super) invitation_join_input_subscriptions: Vec<Subscription>,
    /// Authoritative per-thread counts from `thread/tree`; never derived from
    /// the locally loaded timeline window.
    pub(super) thread_unread: HashMap<String, u64>,
    pub(super) thread_folders: HashMap<String, ThreadFolder>,
    pub(super) thread_placements: HashMap<String, ThreadPlacement>,
    pub(super) thread_agents_doc_summaries:
        HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    pub(super) active_agents_doc_editor_scope: Option<ThreadAgentsDocEditorScope>,
    pub(super) agents_doc_editor: Option<Entity<AgentsDocEditor>>,
    pub(super) thread_folder_expanded: HashMap<String, bool>,
    pub(super) thread_tree_selected_node_id: Option<String>,
    pub(super) thread_tree_state: Entity<TreeState>,
    pub(super) administration_content_view: AdministrationContentView,
    pub(super) settings_content_view: SettingsContentView,
    pub(super) profile_editor: Option<Entity<ProfileEditorState>>,
    pub(super) profile_editor_input_subscriptions: Vec<Subscription>,
    pub(super) administration: AdministrationCache,
    pub(super) workspace_members_loading: HashSet<WorkspaceId>,
    pub(super) thread_scope_pending: ThreadScopePendingAction,
    pub(super) thread_scope_error: Option<String>,
    pub(super) message_revision_dialog: Option<Entity<DesktopMessageRevisionDialogState>>,
    pub(crate) open_model_selector_cli_runtime_binding:
        Option<crate::components::model_selector::OpenModelSelectorCliRuntimeBinding>,
    pub(super) message_revision_loading: bool,
    pub(super) message_mutation_pending: bool,
    pub(super) invitations_loading: bool,
    pub(super) invitations_error: Option<String>,
    pub(super) members_loading: bool,
    pub(super) member_workspaces_saving: bool,
    pub(super) members_error: Option<String>,
    pub(super) member_avatar_state: DesktopMemberAvatarState,
    pub(super) voice_input_action_error: Option<String>,
    pub(super) voice_input_action_generation: u64,
    pub(super) pending_voice_input_enabled: Option<bool>,
    pub(super) remote_access_settings_expanded: bool,
    pub(super) remote_access_key_input_revision: u64,
    pub(super) remote_access_status_poll_generation: u64,
    pub(super) self_improvement_status_poll: Option<Task<()>>,
    pub(super) settings_tree_state: Entity<TreeState>,
    pub(super) administration_tree_state: Entity<TreeState>,
    pub(super) provider_tree_state: Entity<TreeState>,
    pub(super) thread_list_loading: bool,
    pub(super) thread_list_refresh_requested: bool,
    pub(super) active_thread_id: Option<String>,
    pub(super) active_thread_resubscribe_pending: bool,
    pub(super) task_thread_navigation_stack: Vec<TaskThreadNavigationEntry>,
    pub(super) preferred_workspace_id: Option<String>,
    pub(super) workspaces: Vec<Workspace>,
    pub(super) workspaces_loading: bool,
    pub(super) workspaces_error: Option<String>,
    pub(super) workspace_action_in_progress: bool,
    pub(super) composer_state: Entity<TextareaState>,
    pub(super) composer_input_subscription: Option<Subscription>,
    pub(super) composer_mention_select: Entity<ComboboxState<MemberPickerDelegate>>,
    pub(super) composer_mention_select_subscription: Option<Subscription>,
    pub(super) composer_mention_items: Vec<ComposerMentionCandidate>,
    pub(super) thread_member_select: Entity<ComboboxState<MemberPickerDelegate>>,
    pub(super) thread_member_select_subscription: Option<Subscription>,
    pub(super) thread_member_items: Vec<ComposerMentionCandidate>,
    pub(super) thread_members_thread_id: Option<String>,
    pub(super) thread_scope_capabilities_thread_id: Option<String>,
    pub(super) thread_scope_capabilities_loading_thread_id: Option<String>,
    pub(super) thread_scope_capabilities_refresh_generation: u64,
    pub(super) thread_scope_capabilities: ThreadPresentationCapabilities,
    pub(super) thread_members: Vec<ThreadParticipantSummary>,
    pub(super) thread_members_loading: bool,
    pub(super) composer_attachments: Vec<ComposerAttachment>,
    pub(super) composer_capabilities: Vec<ComposerCapability>,
    pub(super) composer_skill_selections: Vec<ComposerSkillSelection>,
    pub(super) composer_authorization_fingerprint: Option<String>,
    pub(super) composer_upload_in_progress: bool,
    pub(super) composer_upload_error: Option<String>,
    pub(super) composer_turn_mode: ThreadMode,
    pub(super) composer_hovered_mode: Option<ThreadMode>,
    pub(super) composer_mode_manually_selected: bool,
    pub(super) composer_reply_target:
        Option<pioneer_client::composer::state_machine::ComposerReplyTarget>,
    pub(super) composer_edit_target: Option<DesktopComposerEditTarget>,
    pub(super) composer_selected_mentions:
        Vec<pioneer_client::composer::state_machine::ComposerMentionSelection>,
    pub(super) composer_selected_provider: Option<String>,
    pub(super) composer_capability_target: ComposerCapabilityTarget,
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
    pub(super) desktop_update: DesktopUpdateUiState,
    pub(super) composer_model_selection_manually_selected: bool,
    pub(super) composer_model_display_cache: HashMap<ProviderModelDisplayKey, Option<String>>,
    pub(super) composer_model_display_loading_key: Option<ProviderModelDisplayKey>,
    pub(super) main_content_view: MainContentView,
    pub(super) providers: ProviderListState,
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
    pub(super) skills_management: SkillManagementProjection,
    pub(super) skills_expanded_pack_ids: HashSet<SkillPackId>,
    pub(super) skills_pending_pack_actions: HashSet<SkillPackId>,
    pub(super) skills_health_details: HashMap<SkillId, SkillHealthItem>,
    pub(super) skills_loading: bool,
    pub(super) skills_error: Option<String>,
    pub(super) skills_upload_progress: Option<SkillUploadProgress>,
    pub(super) skills_upload_cancel_token: Option<Arc<AtomicBool>>,
    pub(super) skills_refresh_requested: bool,
    pub(super) skills_poller_started: bool,
    pub(super) skills_pending_actions: HashSet<SkillId>,
    pub(super) selected_skill_target: Option<SkillId>,
    pub(super) skills_list_scroll_handle: VirtualListScrollHandle,
    pub(super) skills_details_expanded_sections: HashSet<String>,
    pub(super) skills_audit_table_state: Entity<TableState<SkillDiagnosticsTableDelegate>>,
    pub(super) composer_draft_lifecycle: ComposerDraftLifecycleState,
    pub(super) pending_thread_create_visibility: ThreadVisibility,
    pub(super) thread_bindings: std::sync::Arc<crate::app::thread::binding::ThreadBindings>,
    pub(super) thread_binding_task: Option<gpui_kit::Task<()>>,
    pub(super) thread_timeline_scroll_handle: VirtualListScrollHandle,
    pub(super) thread_timeline_view_state: RefCell<ThreadTimelineViewState>,
    pub(super) running_indicator_views: RefCell<RunningIndicatorViewCache>,
    pub(super) thread_timeline_item_expanded: RefCell<HashSet<String>>,
    pub(super) thread_timeline_terminal_item: RefCell<HashMap<String, CachedTimelineTerminal>>,
    pub(super) code_highlight_cache: RefCell<DesktopCodeHighlightCache>,
    pub(super) task_review_actions: TaskReviewActionState,
    pub(super) task_user_notifications_workspace_id: Option<String>,
    pub(super) task_user_notifications: Vec<TaskUserNotification>,
    pub(super) task_user_notifications_next_cursor: Option<String>,
    pub(super) task_user_notifications_loading: bool,
    pub(super) task_user_notifications_refresh_requested: bool,
    pub(super) task_user_notifications_refresh_generation: u64,
    pub(super) task_user_notifications_error: Option<String>,
    pub(super) thread_artifacts: ThreadArtifactsState,
    pub(super) artifact_download_cancellations:
        HashMap<ArtifactVersionKey, tokio_util::sync::CancellationToken>,
    pub(super) show_thread_artifacts_sidebar: bool,
    pub(super) show_thread_members_sidebar: bool,
    pub(super) thread_artifacts_sidebar_width: Pixels,
    pub(super) gateway_setup_form_state: Entity<GatewaySetupFormState>,
    pub(super) gateway: GatewayCoordinator,
    pub(super) show_sidebar: bool,
    pub(super) sidebar_panel_width: Pixels,
}
