mod composer_domain;
mod mutations;
mod presentation_events;
mod route_lifecycle;
pub(crate) use presentation_events::{FrameChanged, SidebarChanged};
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
        startup::DesktopStartupCoordinator,
        thread::ThreadCoordinator,
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
use gpui_kit::component::{combobox::ComboboxState, input::TextareaState, tree::TreeState};
use gpui_kit::{prelude::*, *};
pub(super) use pioneer_client::{
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
        skill_selection::ComposerSkillSelection,
        state_machine::ComposerMentionCandidate,
    },
    gateway::runtime::GatewaySetupAction,
    providers::presentation::ProviderModelDisplayKey,
    providers::selectors::ProviderFilter,
    state::client_state::{GatewayConnectionState, GatewayStatusLevel},
    threads::scope::ThreadScopePendingAction,
    threads::start::ThreadStartCoordinator,
};
use pioneer_protocol::{
    AuthMeResponse, CLIRuntimeThreadBinding, GatewaySettingsSnapshot, McpListItem,
    McpServerDetailsResponse, SkillHealthItem, SkillId, SkillListItem, SkillPackId, Thread,
    ThreadAgentsDocSummary, ThreadFolder, ThreadMode, ThreadParticipantSummary, ThreadPlacement,
    ThreadVisibility, TurnPermissionMode, Workspace, WorkspaceId,
};
#[cfg(test)]
pub(crate) use queries::{
    composer_capability_target_for_provider, composer_submission_plan_for_provider,
};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
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

pub(super) use crate::desktop_navigation::MainRoute as MainContentView;
pub(super) use pioneer_client::navigation::{
    AdministrationRoute as AdministrationContentView, SettingsRoute as SettingsContentView,
    TaskThreadLineage as TaskThreadNavigationEntry,
};

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

pub(crate) struct LegacyScreenAdapter {
    pub(super) window_active: bool,
    frame_presentation: Option<presentation_events::FramePresentation>,
    pub(super) navigation_input: std::sync::Arc<pioneer_client::navigation::ClientNavigationState>,
    pub(super) navigation: Arc<crate::desktop_navigation::DesktopNavigationStore>,
    pub(super) shell_state: Entity<crate::shell_state::ShellStateStore>,
    pub(super) startup: DesktopStartupCoordinator,
    pub(super) invitation_join: Option<Entity<DesktopInvitationJoinState>>,
    pub(super) invitation_join_input_subscriptions: Vec<Subscription>,
    pub(super) active_agents_doc_editor_scope: Option<ThreadAgentsDocEditorScope>,
    pub(super) agents_doc_editor: Option<Entity<AgentsDocEditor>>,
    pub(super) profile_editor: Option<Entity<ProfileEditorState>>,
    pub(super) profile_editor_input_subscriptions: Vec<Subscription>,
    pub(super) administration_view: Entity<pioneer_desktop_administration::AdministrationView>,
    pub(super) member_avatar_state: DesktopMemberAvatarState,
    pub(super) voice_input_action_error: Option<String>,
    pub(super) voice_input_action_generation: u64,
    pub(super) pending_voice_input_enabled: Option<bool>,
    pub(super) remote_access_settings_expanded: bool,
    pub(super) remote_access_key_input_revision: u64,
    pub(super) remote_access_status_poll_generation: u64,
    pub(super) self_improvement_status_poll: Option<Task<()>>,
    pub(super) settings_tree_state: Entity<TreeState>,

    pub(super) active_thread_resubscribe_pending: bool,
    pub(crate) task_notification_surface: Option<AnyView>,
    pub(super) workspace_catalog_input:
        Arc<pioneer_client::workspaces::catalog::WorkspaceCatalogPublication>,
    pub(super) providers_view: Entity<pioneer_desktop_providers::ProviderCatalogView>,
    _providers_layout_subscription: Subscription,
    pub(super) mcp_view: Entity<pioneer_desktop_mcp::McpCatalogView>,
    pub(super) skills_view: Entity<pioneer_desktop_skills::SkillsCatalogView>,
    _catalog_layout_subscription: Subscription,
    pub(super) pending_thread_create_visibility: ThreadVisibility,
    pub(super) gateway_setup_form_state: Entity<GatewaySetupFormState>,
    pub(super) gateway: GatewayCoordinator,
}

// Internal compatibility name refers to the same retained owner.
pub(super) use LegacyScreenAdapter as PioneerDesktop;
