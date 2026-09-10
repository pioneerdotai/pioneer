use super::{
    capabilities::CapabilityPickerState,
    voice_owner::{VoiceInputEvent, VoiceInputView},
    *,
};
use crate::{binding::ThreadBindings, member_picker::*, ports::*, screen::TimelineView};
use gpui_kit::{
    component::input::{InputEvent, TextareaState},
    prelude::*,
    *,
};
use pioneer_client::{
    composer::{state_machine::ComposerMentionCandidate, store::*},
    core::{ClientCore, ClientScope},
};
use std::sync::Arc;

pub(crate) struct ComposerView {
    pub(super) client: Arc<ClientCore>,
    pub(super) thread_id: String,
    pub(super) thread_bindings: Arc<ThreadBindings>,
    pub(super) screen: WeakEntity<TimelineView>,
    pub(super) composer_input: Option<Arc<ComposerPublication>>,
    pub(super) identity_input: Option<
        Arc<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>,
    >,
    pub(super) thread_member_input:
        Option<Arc<pioneer_client::threads::members::ThreadMemberPublication>>,
    pub(super) thread_capability_input:
        Option<Arc<pioneer_client::threads::capabilities::ThreadCapabilityPublication>>,
    pub(super) connection_state: GatewayConnectionState,
    pub(super) composer_state: Entity<TextareaState>,
    pub(super) composer_editor_draft: Option<DraftId>,
    pub(super) composer_mention_select: MemberPickerState,
    pub(super) composer_mention_items: Vec<ComposerMentionCandidate>,
    pub(super) composer_hovered_mode: Option<pioneer_client::timeline::types::ThreadMode>,
    pub(super) capability_picker: Option<Entity<CapabilityPickerState>>,
    pub(super) composer_model_picker: Option<Entity<ComposerModelPickerView>>,
    pub(super) composer_upload_error: Option<String>,
    pub(super) composer_policy_notice: Option<(DraftId, String)>,
    pub(super) voice: Entity<VoiceInputView>,
    pub(super) files: Arc<dyn ThreadFilePort>,
    pub(super) mount: u64,
    pub(super) native_generation: u64,
    pub(super) file_operation: Option<ComposerOperationIdentity>,
    pub(super) send_operation: Option<ComposerOperationIdentity>,
    pub(super) file_task: Option<Task<()>>,
    pub(super) send_task: Option<Task<()>>,
    _release: Subscription,
    _subscriptions: Vec<Subscription>,
    _binding_task: Task<()>,
}
impl Render for ComposerView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_composer(cx)
    }
}
impl ComposerView {
    pub(crate) fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        thread_bindings: Arc<ThreadBindings>,
        screen: WeakEntity<TimelineView>,
        files: Arc<dyn ThreadFilePort>,
        audio: Arc<dyn ThreadAudioPort>,
        mount: u64,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let voice = VoiceInputView::new(
            client.clone(),
            thread_id.clone(),
            thread_bindings.registrar(),
            files.clone(),
            audio,
            mount,
            window.is_window_active(),
            cx,
        );
        cx.new(|cx: &mut Context<Self>| {
            let voice_events = cx.subscribe(&voice, |view, _, event: &VoiceInputEvent, cx| {
                if let VoiceInputEvent::UploadError(error) = event {
                    view.composer_upload_error = error.clone();
                }
                cx.notify();
            });
            let composer_state = cx.new(|cx| {
                TextareaState::new(window, cx)
                    .auto_grow(2, 13)
                    .placeholder(t!("chat.composer.placeholder").to_string())
            });
            let composer_mention_select = cx.new(|cx| new_member_picker_state(window, cx));
            let input_subscription =
                cx.subscribe(&composer_state, |view, input, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change)
                        && view.composer_text_intent(input.read(cx).value().to_string())
                    {
                        cx.notify();
                    }
                });
            let mention_subscription = cx.subscribe_in(
                &composer_mention_select,
                window,
                |view,
                 select,
                 event: &gpui_kit::component::combobox::ComboboxEvent<MemberPickerDelegate>,
                 window,
                 cx| {
                    let gpui_kit::component::combobox::ComboboxEvent::Confirm(candidates) = event
                    else {
                        return;
                    };
                    let Some(candidate) = candidates.first().cloned() else {
                        return;
                    };
                    let Some(input) = &view.composer_input else {
                        return;
                    };
                    let draft = input.draft_id();
                    let root = cx.entity().downgrade();
                    let select = select.downgrade();
                    window.defer(cx, move |window, cx| {
                        let applied = root
                            .update(cx, |view, cx| {
                                if view
                                    .composer_input
                                    .as_ref()
                                    .is_none_or(|input| input.draft_id() != draft)
                                {
                                    return false;
                                }
                                view.insert_composer_mention(candidate, window, cx);
                                true
                            })
                            .unwrap_or(false);
                        if applied {
                            let _ = select.update(cx, |state, cx| state.clear_selection(cx));
                        }
                    });
                },
            );
            let binding = thread_bindings.clone();
            let mut changes = binding.watch();
            let binding_task = cx.spawn_in(window, async move |view, cx| {
                while changes.changed().await.is_ok() {
                    if binding.drain().is_empty() {
                        continue;
                    }
                    if view
                        .update_in(cx, |view, window, cx| view.synchronize_inputs(window, cx))
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let release = cx.on_release_in(window, |view, window, cx| {
                if let Some(picker) = view.capability_picker.take() {
                    picker.update(cx, |picker, cx| picker.close(window, cx));
                }
                if let Some(picker) = view.composer_model_picker.take() {
                    picker.update(cx, |picker, cx| picker.close(window, cx));
                }
                if view
                    .composer_state
                    .read(cx)
                    .focus_handle(cx)
                    .is_focused(window)
                {
                    window.blur(cx);
                }
                // Kit lazily removes registrations for inputs unmounted while focused.
                use gpui_kit::component::WindowExt;
                let _ = window.focused_input(cx);
            });
            let mut view = Self {
                client,
                thread_id,
                thread_bindings,
                screen,
                files,
                voice,
                mount,
                native_generation: 0,
                composer_input: None,
                identity_input: None,
                thread_member_input: None,
                thread_capability_input: None,
                connection_state: GatewayConnectionState::Disconnected,
                composer_state,
                composer_editor_draft: None,
                composer_mention_select,
                composer_mention_items: Vec::new(),
                composer_hovered_mode: None,
                capability_picker: None,
                composer_model_picker: None,
                composer_upload_error: None,
                composer_policy_notice: None,
                file_operation: None,
                send_operation: None,
                file_task: None,
                send_task: None,
                _release: release,
                _subscriptions: vec![input_subscription, mention_subscription, voice_events],
                _binding_task: binding_task,
            };
            view.synchronize_inputs(window, cx);
            view
        })
    }
    pub(super) fn synchronize_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let payload = |scope: ClientScope| self.thread_bindings.publication(&scope);
        self.composer_input = payload(ClientScope::Composer {
            thread_id: self.thread_id.clone(),
        })
        .and_then(|p| p.typed::<ComposerPublication>())
        .map(|p| p.payload());
        self.thread_member_input = payload(ClientScope::ThreadMember {
            thread_id: self.thread_id.clone(),
        })
        .and_then(|p| p.typed::<pioneer_client::threads::members::ThreadMemberPublication>())
        .map(|p| p.payload());
        self.thread_capability_input = payload(ClientScope::ThreadCapability {
            thread_id: self.thread_id.clone(),
        })
        .and_then(|p| {
            p.typed::<pioneer_client::threads::capabilities::ThreadCapabilityPublication>()
        })
        .map(|p| p.payload());
        self.identity_input = payload(ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.typed::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>()).map(|p| p.payload());
        self.connection_state = payload(ClientScope::Session)
            .and_then(|p| {
                p.typed::<pioneer_client::gateway::session_controller::GatewaySessionPublication>()
            })
            .and_then(|p| {
                p.payload()
                    .status
                    .as_ref()
                    .map(|status| status.connection_state)
            })
            .unwrap_or(GatewayConnectionState::Disconnected);
        // The startup draft can be mounted before the first authorization epoch.
        // A session fence clears its publication, but the reserved creation ID
        // still belongs to this composer. Reopen an empty draft in the new epoch.
        if self.composer_input.is_none()
            && self.client.composer_snapshot(&self.thread_id).is_none()
            && self
                .client
                .thread_start_snapshot()
                .pending_thread_id
                .as_deref()
                == Some(self.thread_id.as_str())
        {
            self.client.composer_intent(ComposerIntent::Activate {
                thread_id: self.thread_id.clone(),
            });
            self.composer_input = self.client.composer_snapshot(&self.thread_id);
        }
        if let Some(input) = &self.composer_input {
            self.client.composer_catalog_intent(
                pioneer_client::composer::catalog::ComposerCatalogIntent::Observe {
                    thread_id: self.thread_id.clone(),
                    draft_id: input.draft_id(),
                    catalog: pioneer_client::composer::catalog::ComposerCatalogKind::Skills,
                },
            );
        }
        self.present_composer_authorization_notice();
        self.controlled_text(window, cx);
        self.synchronize_mention_items(window, cx);
        if self
            .identity_input
            .as_ref()
            .is_none_or(|identity| identity.current_auth.is_none())
        {
            self.file_task.take();
            self.send_task.take();
            if let Some(picker) = self.capability_picker.take() {
                picker.update(cx, |picker, cx| picker.close(window, cx));
            }
            if let Some(picker) = self.composer_model_picker.take() {
                picker.update(cx, |picker, cx| picker.close(window, cx));
            }
            self.files.retire_mount(&self.thread_id, self.mount);
        }
        cx.notify();
    }
    pub(super) fn controlled_text(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.composer_editor_draft = self.composer_input.as_ref().map(|input| input.draft_id());
        let text = self
            .composer_input
            .as_ref()
            .map(|input| input.draft().text.clone())
            .unwrap_or_default();
        self.composer_state.update(cx, |state, cx| {
            if state.value().as_str() != text {
                state.set_value(text, window, cx);
            }
        });
    }
    pub(crate) fn set_visible(
        &mut self,
        visible: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.thread_bindings.set_active(visible);
        self.voice.update(cx, |voice, cx| {
            voice.set_visible(visible, window.is_window_active(), cx)
        });
        if !visible {
            for identity in [self.file_operation.take(), self.send_operation.take()]
                .into_iter()
                .flatten()
            {
                self.client
                    .complete_composer_operation(identity, ComposerOperationCompletion::Cancelled);
            }
            self.file_task.take();
            self.send_task.take();
            self.files.retire_mount(&self.thread_id, self.mount);
            if let Some(picker) = self.capability_picker.take() {
                picker.update(cx, |picker, cx| picker.close(window, cx));
            }
            if let Some(picker) = self.composer_model_picker.take() {
                picker.update(cx, |picker, cx| picker.close(window, cx));
            }
        }
    }
    pub(crate) fn set_window_active(&mut self, active: bool, cx: &mut Context<Self>) {
        self.voice
            .update(cx, |voice, cx| voice.set_window_active(active, cx));
    }
    pub(crate) fn focus(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.composer_state
            .update(cx, |state, cx| state.focus(window, cx));
    }
}
impl Drop for ComposerView {
    fn drop(&mut self) {
        for identity in [self.file_operation.take(), self.send_operation.take()]
            .into_iter()
            .flatten()
        {
            self.client
                .complete_composer_operation(identity, ComposerOperationCompletion::Cancelled);
        }
        self.files.retire_mount(&self.thread_id, self.mount);
        self.thread_bindings.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Arc, ClientCore, ClientScope, ComposerIntent, ComposerView, ThreadAudioCompletion,
        ThreadAudioError, ThreadAudioPort, ThreadAudioRequest, ThreadBindings, TimelineView,
    };
    use gpui_kit::component::Root;
    use gpui_kit::prelude::*;
    use gpui_kit::{
        App, AppContext, Context, Entity, IntoElement, Render, Task, TestAppContext, Window, div,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Audio(AtomicUsize);
    impl ThreadAudioPort for Audio {
        fn start_capture(
            &self,
            _: ThreadAudioRequest,
            _: &mut App,
        ) -> Task<Result<ThreadAudioCompletion, ThreadAudioError>> {
            panic!("mount and controlled publication must not capture audio")
        }
        fn stop_recording(
            &self,
            _: &ThreadAudioRequest,
        ) -> Result<ThreadAudioCompletion, ThreadAudioError> {
            panic!("no capture was started")
        }
        fn finalize_capture(
            &self,
            _: ThreadAudioRequest,
            _: pioneer_client::composer::turn_prepare::PreparedVoiceComposerSnapshot,
            _: &mut App,
        ) -> Task<Result<ThreadAudioCompletion, ThreadAudioError>> {
            panic!("no capture was started")
        }
        fn cancel_capture(&self, _: &ThreadAudioRequest) {
            panic!("no capture was started")
        }
        fn retire_mount(&self, _: &str, _: u64) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Host {
        composer: Option<Entity<ComposerView>>,
        screen: Entity<TimelineView>,
    }
    impl Render for Host {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().size_full().children(self.composer.clone())
        }
    }
    fn workspace_capabilities(
        workspace: &str,
    ) -> pioneer_client::authorization::AuthorizationCapabilitySnapshot {
        use serde_json::json;
        let resources = json!({
            "fingerprint": "synthetic-resources", "providers": {"all": true},
            "provider_models_all": true, "cli_runtimes": {"all": true},
            "cli_models_all": true, "skills": {"all": true}, "mcp_servers": {"all": true}
        });
        pioneer_client::authorization::AuthorizationCapabilitySnapshot {
            schema_version: 7,
            authorization_revision: 1,
            principal_id: "P00000000000000000001".try_into().unwrap(),
            role_key: "member".into(),
            role: serde_json::from_value(json!({"key": "member", "display_name": "Member", "description": "", "built_in": true})).unwrap(),
            global: Default::default(),
            workspace: Some(serde_json::from_value(json!({
                "workspace_id": workspace,
                "capabilities": {
                    "can_read": true, "can_create_thread": true, "can_manage": false,
                    "can_read_own_notifications": false, "can_acknowledge_own_notifications": false,
                    "can_use_providers": true, "can_use_cli_runtimes": true, "can_use_skills": true,
                    "can_use_mcp": true, "can_run_tasks": true, "can_read_artifacts": true,
                    "can_write_artifacts": true,
                    "execution_limits": {"max_active_executions": 1, "max_queued_tasks": 1, "max_scheduled_tasks": 1},
                    "agent_permission_options": [], "can_list_members": false, "can_add_member": false,
                    "can_remove_member": false, "thread_visibility_options": []
                },
                "operational_resources": resources,
                "execution_draft_policy": {
                    "fingerprint": "synthetic-policy", "resources": resources,
                    "permission_options": [], "can_attach_artifacts": true,
                    "mcp_invocation_limits": {"profile_version": 5, "max_arguments_bytes": 1024,
                        "max_queue_wait_ms": 1000, "max_concurrent_calls": 1, "max_queued_calls": 1}
                }
            })).unwrap()),
            thread: None,
        }
    }

    #[gpui_kit::test]
    fn workspace_publications_enable_draft_and_existing_composer_actions(cx: &mut TestAppContext) {
        use pioneer_client::{
            authorization::AuthorizationProjectionAcceptance,
            core::ClientMutationAuthority,
            gateway::{
                identity_authorization::IdentityAuthorizationPublication,
                session_controller::GatewaySessionPublication,
            },
            state::{
                client_state::{GatewayConnectionState, GatewayStatusLevel},
                reducers::{GatewayStatusProjection, GatewayStatusTextUpdate},
            },
        };
        fn publish<T: serde::Serialize + Send + Sync + 'static>(
            client: &ClientCore,
            authority: &ClientMutationAuthority,
            scope: ClientScope,
            payload: T,
        ) {
            use pioneer_client::core::{ClientRevisions, ClientTransitionOutcome, ScopedRevision};
            let previous = client
                .snapshot(&scope)
                .map(|p| p.revisions())
                .unwrap_or_default();
            let revisions = ClientRevisions::new(
                previous.domain(),
                previous.presentation(),
                previous.content(),
                ScopedRevision::new(previous.scoped().get() + 1),
            );
            assert_eq!(
                client
                    .publish(authority, scope, revisions, Arc::new(payload), vec![])
                    .outcome(),
                ClientTransitionOutcome::Changed
            );
        }
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        crate::test_support::install_thread_timeline(&client, "a", "row");
        client.remember_thread_draft("workspace", Some("a".into()));
        assert_eq!(
            client.thread_workspace_draft("workspace").as_deref(),
            Some("a")
        );
        client.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: Default::default(),
        });
        let (registrar, deliver) = crate::test_support::binding_router(client.clone());
        let screen_binding = ThreadBindings::new(registrar.clone(), "a", vec![]);
        let composer_binding = ThreadBindings::new(registrar, "a", vec![]);
        deliver();
        let (root, cx) = cx.add_window_view(|window, cx| {
            let ports = Arc::new(crate::test_support::ThreadPorts);
            let screen = TimelineView::new(
                client.clone(),
                "a".into(),
                screen_binding.clone(),
                ports.clone(),
                ports.clone(),
                1,
                window,
                cx,
            );
            let composer = ComposerView::new(
                client.clone(),
                "a".into(),
                composer_binding.clone(),
                screen.downgrade(),
                ports,
                Arc::new(Audio(AtomicUsize::new(0))),
                1,
                window,
                cx,
            );
            let host = cx.new(|_| Host {
                composer: Some(composer),
                screen,
            });
            Root::new(host, window, cx)
        });
        cx.run_until_parked();
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<Host>().unwrap()
        });
        let composer = host.read_with(cx, |host, _| host.composer.clone().unwrap());
        let authority = ClientMutationAuthority::for_test();
        let publish_identity = |snapshot| {
            let mut identity = IdentityAuthorizationPublication::default();
            assert_eq!(
                identity.capabilities.accept(snapshot),
                AuthorizationProjectionAcceptance::Accepted
            );
            publish(
                &client,
                &authority,
                ClientScope::Administration { workspace_id: None },
                identity,
            );
        };
        let mut session = GatewaySessionPublication::default();
        session.status = Some(GatewayStatusProjection {
            status: GatewayStatusTextUpdate::KeepExisting,
            status_level: GatewayStatusLevel::Neutral,
            connection_state: GatewayConnectionState::Connected,
            clear_gateway_error: false,
        });
        publish(&client, &authority, ClientScope::Session, session);
        // Another workspace must not grant rights to the mounted thread.
        publish_identity(workspace_capabilities("other"));
        deliver();
        cx.run_until_parked();
        composer.read_with(cx, |view, _| {
            assert!(!view.principal_presentation_capabilities().can_use_providers);
            assert!(!view.can_start_active_thread_agent_presentation());
            assert!(!view.active_artifact_presentation_policy().can_attach);
        });
        publish_identity(workspace_capabilities("workspace"));
        deliver();
        cx.run_until_parked();
        composer.read_with(cx, |view, _| {
            let rights = view.principal_presentation_capabilities();
            assert!(rights.can_use_providers && rights.can_use_cli_runtimes);
            assert!(rights.can_use_skills && rights.can_use_mcp);
            assert!(view.can_start_active_thread_agent_presentation());
            assert!(view.active_artifact_presentation_policy().can_attach);
        });
        host.read_with(cx, |host, cx| {
            assert!(
                host.screen
                    .read(cx)
                    .principal_presentation_capabilities()
                    .can_use_mcp
            )
        });
        // Once promoted, attachment/start rights come from the thread scope.
        client.remember_thread_draft("workspace", None);
        let mut snapshot = workspace_capabilities("workspace");
        snapshot.thread = Some(serde_json::from_value(serde_json::json!({
            "workspace_id": "workspace", "thread_id": "a",
            "capabilities": {
                "can_read": true, "can_write": true, "can_edit_own_message": false,
                "can_delete_own_message": false, "can_start_turn": true,
                "can_observe_agent_execution": false, "can_cancel_agent_execution": false,
                "can_resume_agent_execution": false, "can_steer_agent_execution": false,
                "can_observe_agent_requests": false, "can_respond_to_agent_requests": false,
                "can_control_cli_runtime": false, "can_create_task": false, "can_review_tasks": false,
                "can_cancel_tasks": false, "can_read_artifacts": true, "can_write_artifacts": true,
                "can_bind_artifacts": true, "can_read_agents_document": false,
                "can_manage_agents_document": false, "can_manage": false,
                "can_manage_private_participants": false, "can_move": false
            }
        })).unwrap());
        authority.accept_thread_capabilities_for_test(&client, snapshot.clone());
        deliver();
        cx.run_until_parked();
        composer.read_with(cx, |view, _| {
            assert!(view.principal_presentation_capabilities().can_use_skills);
            assert!(view.can_start_active_thread_agent_presentation());
            assert!(view.active_artifact_presentation_policy().can_attach);
        });
        let scope = snapshot.thread.as_mut().unwrap();
        scope.capabilities.can_start_turn = false;
        scope.capabilities.can_bind_artifacts = false;
        authority.accept_thread_capabilities_for_test(&client, snapshot);
        deliver();
        cx.run_until_parked();
        composer.read_with(cx, |view, _| {
            assert!(!view.can_start_active_thread_agent_presentation());
            assert!(!view.active_artifact_presentation_policy().can_attach);
        });
        publish(
            &client,
            &authority,
            ClientScope::Administration { workspace_id: None },
            IdentityAuthorizationPublication::default(),
        );
        deliver();
        cx.run_until_parked();
        composer.read_with(cx, |view, _| {
            assert!(!view.principal_presentation_capabilities().can_use_providers);
            assert!(!view.principal_presentation_capabilities().can_use_skills);
            assert!(!view.principal_presentation_capabilities().can_use_mcp);
        });
    }

    #[gpui_kit::test]
    fn retained_input_publishes_one_edit_accepts_controlled_updates_without_echo_and_drops(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        crate::test_support::install_thread_timeline(&client, "a", "row");
        client.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: Default::default(),
        });
        let (registrar, deliver) = crate::test_support::binding_router(client.clone());
        let screen_binding = ThreadBindings::new(registrar.clone(), "a", vec![]);
        let composer_binding = ThreadBindings::new(registrar, "a", vec![]);
        deliver();
        let audio = Arc::new(Audio(AtomicUsize::new(0)));
        let (root, cx) = cx.add_window_view(|window, cx| {
            let ports = Arc::new(crate::test_support::ThreadPorts);
            let screen = TimelineView::new(
                client.clone(),
                "a".into(),
                screen_binding.clone(),
                ports.clone(),
                ports.clone(),
                1,
                window,
                cx,
            );
            let composer = ComposerView::new(
                client.clone(),
                "a".into(),
                composer_binding.clone(),
                screen.downgrade(),
                ports,
                audio.clone(),
                1,
                window,
                cx,
            );
            let host = cx.new(|_| Host {
                composer: Some(composer),
                screen,
            });
            Root::new(host, window, cx)
        });
        cx.run_until_parked();
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<Host>().unwrap()
        });
        let composer = host.read_with(cx, |host, _| host.composer.clone().unwrap());
        let input = composer.read_with(cx, |view, _| view.composer_state.clone());
        let before = client.composer_snapshot("a").unwrap();
        cx.update(|window, cx| input.update(cx, |state, cx| state.focus(window, cx)));
        cx.run_until_parked();
        cx.simulate_input("x");
        cx.run_until_parked();
        let typed = client.composer_snapshot("a").unwrap();
        assert_eq!(typed.draft().text, "x");
        assert_eq!(typed.revision(), before.revision() + 1);
        deliver();
        cx.run_until_parked();
        assert!(Arc::ptr_eq(&typed, &client.composer_snapshot("a").unwrap()));
        client.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: typed.draft_id(),
            text: "controlled".into(),
        });
        let controlled = client.composer_snapshot("a").unwrap();
        deliver();
        cx.run_until_parked();
        assert_eq!(
            input.read_with(cx, |input, _| input.value().to_string()),
            "controlled"
        );
        assert!(Arc::ptr_eq(
            &controlled,
            &client.composer_snapshot("a").unwrap()
        ));
        let file = composer
            .update(cx, |view, _| {
                view.begin_operation(
                    pioneer_client::composer::store::ComposerOperationKind::PickFiles,
                )
            })
            .unwrap();
        let weak_input = input.downgrade();
        let weak_composer = composer.downgrade();
        drop(input);
        drop(composer);
        let retirements = audio.0.load(Ordering::SeqCst);
        host.update(cx, |host, cx| {
            host.composer.take();
            cx.notify();
        });
        cx.run_until_parked();
        assert!(weak_composer.upgrade().is_none());
        // The platform text handler is replaced at the next framework frame.
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        assert!(weak_input.upgrade().is_none());
        assert!(audio.0.load(Ordering::SeqCst) > retirements);
        let retired = client.composer_snapshot("a").unwrap();
        assert_eq!(retired.operation().unwrap().identity, file.identity);
        assert_eq!(
            retired.operation().unwrap().status,
            pioneer_client::composer::store::ComposerOperationStatus::Cancelled
        );
        assert!(!client.complete_composer_operation(
            file.identity,
            pioneer_client::composer::store::ComposerOperationCompletion::FilesSelected {
                attachments: vec![]
            }
        ));
        assert!(
            composer_binding
                .publication(&ClientScope::Composer {
                    thread_id: "a".into()
                })
                .is_none()
        );
        deliver();
        cx.run_until_parked();
        assert!(weak_composer.upgrade().is_none());
        assert!(
            host.read_with(cx, |host, _| host.screen.downgrade())
                .upgrade()
                .is_some()
        );
    }
}
