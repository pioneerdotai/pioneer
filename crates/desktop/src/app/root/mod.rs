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
        member_avatars::DesktopMemberAvatarState, startup::DesktopStartupCoordinator,
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
    gateway::{ClientRuntime, DesktopGatewayHttpClient, GatewayWsCommandSender},
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

pub(super) use crate::desktop_navigation::MainRoute as MainContentView;
pub(super) use pioneer_client::navigation::{
    AdministrationRoute as AdministrationContentView, SettingsRoute as SettingsContentView,
    TaskThreadLineage as TaskThreadNavigationEntry,
};

pub(super) struct GatewayCoordinator {
    pub(super) compatibility_task: Option<gpui_kit::Task<()>>,
    pub(super) identity_task: Option<gpui_kit::Task<()>>,
    pub(super) session_task: Option<gpui_kit::Task<()>>,
    pub(super) transport_verification_task: Option<gpui_kit::Task<()>>,
    pub(super) transport_verification_id: Option<u64>,
    pub(super) applied_transport_revision: u64,
    pub(super) identity_binding: std::sync::Arc<crate::gateway::IdentityAuthorizationBinding>,
    pub(super) session_binding: std::sync::Arc<crate::gateway::GatewaySessionBinding>,
    pub(super) client_runtime: ClientRuntime,
    pub(super) http_client: Option<DesktopGatewayHttpClient>,
    pub(super) ws_connection_id: Option<u64>,
    pub(super) current_principal_refresh_generation: u64,

    pub(super) connection_state: GatewayConnectionState,
    pub(super) status: String,
    pub(super) status_level: GatewayStatusLevel,
    pub(super) error: Option<String>,
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
    pub(super) onboarding_view: Entity<pioneer_desktop_onboarding::OnboardingView>,
    pub(super) onboarding_subscription: Subscription,
    pub(super) onboarding_route: crate::desktop_navigation::WindowRoute,
    pub(super) agents_doc_editor: Option<Entity<pioneer_desktop_agents_doc::AgentsDocumentEditor>>,
    pub(super) settings_view: Entity<pioneer_desktop_settings::SettingsView>,
    pub(super) administration_view: Entity<pioneer_desktop_administration::AdministrationView>,
    pub(super) member_avatar_state: DesktopMemberAvatarState,

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
    pub(super) gateway: GatewayCoordinator,
}

// Internal compatibility name refers to the same retained owner.
pub(super) use LegacyScreenAdapter as PioneerDesktop;
