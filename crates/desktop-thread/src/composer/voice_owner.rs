use super::{
    GatewayConnectionState,
    voice::{DesktopVoiceComposerState, DesktopVoiceEntryAvailability},
};
use crate::{binding::ThreadBindings, ports::*};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    composer::{state_machine::ComposerDomainState, store::*},
    core::{ClientCore, ClientScope},
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::Arc;

pub(super) enum VoiceInputEvent {
    PresentationChanged,
    UploadError(Option<String>),
}
#[derive(Clone, PartialEq)]
struct VoicePresentation {
    hold: bool,
    processing: bool,
    error: Option<String>,
    availability: DesktopVoiceEntryAvailability,
}

pub(super) struct VoiceInputView {
    pub(super) client: Arc<ClientCore>,
    pub(super) thread_id: String,
    binding: Arc<ThreadBindings>,
    pub(super) composer_input: Option<Arc<ComposerPublication>>,
    pub(super) identity_input: Option<
        Arc<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>,
    >,
    pub(super) thread_capability_input:
        Option<Arc<pioneer_client::threads::capabilities::ThreadCapabilityPublication>>,
    pub(super) settings_input:
        Option<Arc<pioneer_client::gateway::settings_store::GatewaySettingsStore>>,
    pub(super) connection_state: GatewayConnectionState,
    pub(super) window_active: bool,
    pub(super) audio: Arc<dyn ThreadAudioPort>,
    pub(super) files: Arc<dyn ThreadFilePort>,
    pub(super) mount: u64,
    pub(super) native_generation: u64,
    pub(super) desktop_voice_composer: DesktopVoiceComposerState,
    pub(super) desktop_voice_operation: Option<ComposerOperationIdentity>,
    pub(super) desktop_voice_request: Option<ThreadAudioRequest>,
    pub(super) desktop_voice_prepare_task: Option<Task<()>>,
    published_presentation: Option<VoicePresentation>,
    _changes: Task<()>,
}
impl EventEmitter<VoiceInputEvent> for VoiceInputView {}
impl Render for VoiceInputView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_desktop_voice_idle_button(cx)
    }
}
impl VoiceInputView {
    pub(super) fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        registrar: Arc<dyn ClientBindingRegistrar>,
        files: Arc<dyn ThreadFilePort>,
        audio: Arc<dyn ThreadAudioPort>,
        mount: u64,
        active: bool,
        cx: &mut App,
    ) -> Entity<Self> {
        let scopes = vec![
            ClientScope::Composer {
                thread_id: thread_id.clone(),
            },
            ClientScope::Thread {
                thread_id: thread_id.clone(),
            },
            ClientScope::ThreadCapability {
                thread_id: thread_id.clone(),
            },
            ClientScope::Administration { workspace_id: None },
            ClientScope::Settings,
            ClientScope::Session,
        ];
        let initial = scopes
            .iter()
            .filter_map(|scope| client.snapshot(scope))
            .collect();
        let binding = ThreadBindings::scoped(registrar, scopes, initial);
        cx.new(|cx: &mut Context<Self>| {
            let input = binding.clone();
            let mut changes = input.watch();
            let task = cx.spawn(async move |view, cx| {
                while changes.changed().await.is_ok() {
                    if input.drain().is_empty() {
                        continue;
                    }
                    if view.update(cx, |view, cx| view.synchronize(cx)).is_err() {
                        break;
                    }
                }
            });
            let mut view = Self {
                client,
                thread_id,
                binding,
                files,
                audio,
                mount,
                window_active: active,
                composer_input: None,
                identity_input: None,
                thread_capability_input: None,
                settings_input: None,
                connection_state: GatewayConnectionState::Disconnected,
                native_generation: 0,
                desktop_voice_composer: Default::default(),
                desktop_voice_operation: None,
                desktop_voice_request: None,
                desktop_voice_prepare_task: None,
                published_presentation: None,
                _changes: task,
            };
            view.synchronize(cx);
            view
        })
    }
    fn synchronize(&mut self, cx: &mut Context<Self>) {
        self.composer_input = self.client.composer_snapshot(&self.thread_id);
        self.thread_capability_input = self.client.thread_capability_snapshot(&self.thread_id);
        self.identity_input = self
            .binding
            .publication(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.snapshot().payload());
        self.settings_input = self
            .binding
            .publication(&ClientScope::Settings)
            .and_then(|p| p.snapshot().payload());
        self.connection_state = self.binding.publication(&ClientScope::Session)
            .and_then(|p| p.snapshot().payload::<pioneer_client::gateway::session_controller::GatewaySessionPublication>())
            .and_then(|p| p.status.as_ref().map(|status| status.connection_state)).unwrap_or(GatewayConnectionState::Disconnected);
        if let Some(operation) = &self.desktop_voice_operation {
            if self.composer_input.as_ref().is_none_or(|input| {
                input.operation().is_none_or(|current| {
                    current.identity != *operation
                        || current.status == ComposerOperationStatus::Cancelled
                })
            }) {
                self.cancel_desktop_voice_operation("voice_operation_cancelled");
            }
        }
        self.refresh_desktop_voice_status(cx);
        self.present_desktop_voice_publication(cx);
        self.publish_presentation(cx);
    }
    pub(super) fn publish_presentation(&mut self, cx: &mut Context<Self>) {
        let presentation = VoicePresentation {
            hold: self.desktop_voice_hold_ui_active(),
            processing: self.desktop_voice_send_processing(),
            error: self.desktop_voice_error_message().map(str::to_owned),
            availability: self.desktop_voice_entry_availability(),
        };
        if self.published_presentation.as_ref() != Some(&presentation) {
            self.published_presentation = Some(presentation);
            cx.emit(VoiceInputEvent::PresentationChanged);
        }
        cx.notify();
    }
    pub(super) fn set_visible(&mut self, visible: bool, active: bool, cx: &mut Context<Self>) {
        self.binding.set_active(visible);
        self.set_window_active(visible && active, cx);
    }
    pub(super) fn set_window_active(&mut self, active: bool, cx: &mut Context<Self>) {
        if self.window_active == active {
            return;
        }
        self.window_active = active;
        if !active {
            self.cancel_desktop_voice_operation("route_inactive");
        }
        self.refresh_desktop_voice_status(cx);
    }
    pub(super) fn current_active_thread_id(&self) -> Option<&str> {
        Some(&self.thread_id)
    }
    pub(super) fn composer_domain(&self) -> &ComposerDomainState {
        static EMPTY: std::sync::LazyLock<ComposerDomainState> =
            std::sync::LazyLock::new(ComposerDomainState::default);
        self.composer_input
            .as_ref()
            .map_or(&EMPTY, |input| input.domain())
    }
    pub(super) fn composer_authorization_fingerprint(&self) -> Option<&str> {
        self.composer_input
            .as_ref()
            .and_then(|input| input.authorization_fingerprint())
    }
    pub(super) fn active_thread_conversation(
        &self,
    ) -> Option<pioneer_client::threads::registry::ConversationSnapshot> {
        self.client
            .thread_snapshot(&self.thread_id)
            .map(|snapshot| snapshot.conversation())
    }
    pub(super) fn active_workspace_id(&self) -> Option<String> {
        self.client
            .thread_coordinator_snapshot(&self.thread_id)
            .map(|thread| thread.workspace_id.clone())
    }
    fn is_draft(&self) -> bool {
        self.active_workspace_id().is_some_and(|workspace| {
            self.client.thread_workspace_draft(&workspace).as_deref() == Some(&self.thread_id)
        })
    }
    fn authorization(
        &self,
    ) -> Option<pioneer_client::authorization::AuthorizationCapabilitySnapshot> {
        self.identity_input
            .as_ref()?
            .capabilities
            .snapshot(self.active_workspace_id().as_deref(), None)
    }
    pub(super) fn thread_presentation_capabilities(
        &self,
        thread: &str,
    ) -> Option<pioneer_client::authorization::ThreadPresentationCapabilities> {
        let input = self
            .thread_capability_input
            .as_ref()
            .filter(|p| p.thread_id == thread)?;
        Some(
            pioneer_client::authorization::thread_presentation_capabilities(
                input
                    .snapshot
                    .as_ref()?
                    .thread
                    .as_ref()
                    .map(|scope| &scope.capabilities),
            ),
        )
    }
    pub(super) fn can_write_active_thread_presentation(&self) -> bool {
        if self.is_draft() {
            return self
                .authorization()
                .and_then(|p| p.workspace)
                .is_some_and(|w| w.capabilities.can_create_thread);
        }
        self.thread_presentation_capabilities(&self.thread_id)
            .is_some_and(|c| c.can_write)
    }
    pub(super) fn can_start_active_thread_agent_presentation(&self) -> bool {
        if self.is_draft() {
            return self
                .authorization()
                .and_then(|p| p.workspace)
                .is_some_and(|w| w.capabilities.can_create_thread);
        }
        self.thread_presentation_capabilities(&self.thread_id)
            .is_some_and(|c| c.can_start_turn)
    }
    pub(super) fn has_complete_composer_model_selection(&self) -> bool {
        pioneer_client::composer::model_selection::has_complete_composer_model_selection(
            self.composer_domain().selected_provider.as_deref(),
            self.composer_domain().selected_model.as_deref(),
        ) && self
            .composer_input
            .as_ref()
            .is_some_and(|input| input.selected_provider_ready())
    }
    pub(super) fn composer_upload_in_progress(&self) -> bool {
        use pioneer_client::composer::store::ComposerOperationStatus;
        self.composer_input
            .as_ref()
            .and_then(|input| input.operation())
            .is_some_and(|operation| match operation.kind {
                ComposerOperationKind::Send => operation.pending(),
                ComposerOperationKind::Voice => matches!(
                    operation.status,
                    ComposerOperationStatus::Preparing | ComposerOperationStatus::Uploading
                ),
                _ => false,
            })
    }
}
impl Drop for VoiceInputView {
    fn drop(&mut self) {
        self.cancel_desktop_voice_operation("voice_input_unmounted");
        self.audio.retire_mount(&self.thread_id, self.mount);
        self.binding.clear();
    }
}
