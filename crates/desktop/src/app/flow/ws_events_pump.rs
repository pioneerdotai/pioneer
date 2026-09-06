use super::helpers::desktop_session_terminal_message;
use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop},
    gateway::GatewayRuntime,
};
use gpui_kit::{AsyncApp, Context, WeakEntity, prelude::*};
use pioneer_client::runtime::ClientRuntimePostEventSink;
use pioneer_client::transport::ws::GatewayWsEvent;
use pioneer_protocol::GatewayNotification;

impl PioneerDesktop {
    pub(crate) fn start_gateway_ws_event_pump(&mut self, cx: &mut Context<Self>) {
        let threads = self.thread_bindings.clone();
        let mut thread_changes = threads.watch();
        self.thread_binding_task = Some(cx.spawn(async move |view, cx| {
            while thread_changes.changed().await.is_ok() {
                let _ = *thread_changes.borrow_and_update();
                let changes = threads.drain();
                if changes.is_empty() { continue; }
                if view.update(cx, |view, cx| {
                    let mut visible_changed = false;
                    let mut summary_changed = false;
                    for change in changes {
                        match change.scope() {
                            pioneer_client::core::ClientScope::Thread {thread_id} => visible_changed |= view.current_active_thread_id() == Some(thread_id.as_str()),
                            pioneer_client::core::ClientScope::SidebarSummary {workspace_id, ..} => {
                                summary_changed |= view.active_workspace_id() == Some(workspace_id.as_str());
                                if let Some(summary) = change.typed::<pioneer_client::threads::registry::SidebarSummaryChanged>() {
                                    if let Some(placement) = &summary.payload().placement { view.thread_placements.insert(placement.thread_id.clone(),placement.clone()); }
                                }
                            },
                            _ => {}
                        }
                    }

                    if summary_changed { view.rebuild_sidebar_tree_state(cx); }
                    if visible_changed {
                        if view.current_active_thread_id().is_some_and(|id|view.gateway.client_runtime.client_core().thread_snapshot(id).is_none()) {view.set_active_thread_id(None);}
                        if view.active_thread_resubscribe_pending {
                            let domain=view.current_active_thread_id().and_then(|id|view.gateway.client_runtime.client_core().thread_snapshot(id));
                            if let Some(domain)=domain {if !domain.coordinator().history_loading {
                                view.active_thread_resubscribe_pending=false;
                                if domain.subscription_failed() {view.startup.fail(pioneer_observability::DesktopStartupStage::ActiveThreadSubscribe);} else {
                                    view.startup.succeed(pioneer_observability::DesktopStartupStage::ActiveThreadSubscribe);
                                    view.reconcile_semantic_timeline_after_reconnect(cx);view.refresh_desktop_voice_status(cx);
                                }
                            }}
                        }
                        view.sync_composer_model_selection_for_active_thread();
                        view.request_thread_start_if_needed();
                        view.ensure_active_thread_capabilities_loaded(false, cx);
                        cx.notify();
                    }
                }).is_err() { break; }
            }
        }));
        let client_runtime = self.gateway.client_runtime.clone();
        let session = self.gateway.session_binding.clone();
        let mut session_publications = session.watch();
        self.gateway.session_task = Some(cx.spawn(async move |view, cx| {
            let mut terminal_delivery = None;
            let mut refresh_delivery = None;
            while session_publications.changed().await.is_ok() {
                let _ = *session_publications.borrow_and_update();
                let Some(publication) = session
                    .publication()
                    .map(|publication| publication.payload())
                else {
                    continue;
                };
                if view
                    .update(cx, |view, cx| {
                        view.apply_gateway_session_publication(&publication, cx);
                        let Some(endpoint) = view
                            .gateway
                            .runtime
                            .as_ref()
                            .and_then(GatewayRuntime::active_gateway_id)
                            .map(str::to_owned)
                        else {
                            return;
                        };
                        if let Some(reason) = publication.terminal_reason(&endpoint) {
                            let delivery = (endpoint.clone(), reason);
                            if terminal_delivery.as_ref() != Some(&delivery) {
                                terminal_delivery = Some(delivery);
                                refresh_delivery = None;
                                view.apply_gateway_session_terminal(reason, cx);
                            }
                            return;
                        }
                        terminal_delivery = None;
                        let Some(connection) = publication.connections.get(&endpoint) else {
                            return;
                        };
                        if !connection.refresh_requested {
                            refresh_delivery = None;
                            return;
                        }
                        let delivery = (
                            endpoint,
                            connection.epoch,
                            connection
                                .connected
                                .as_ref()
                                .map(|connected| connected.connection_id),
                        );
                        if refresh_delivery.as_ref() == Some(&delivery) {
                            return;
                        }
                        refresh_delivery = Some(delivery);
                        if connection.connected.is_some() {
                            view.refresh_gateway_session_now(cx);
                        } else {
                            view.recover_gateway_session_now(cx);
                        }
                    })
                    .is_err()
                {
                    break;
                }
            }
        }));
        let settings = self.gateway.settings_binding.clone();
        let mut settings_publications = settings.watch();
        self.gateway.settings_task = Some(cx.spawn(async move |view, cx| {
            let mut voice = None;
            while settings_publications.changed().await.is_ok() {
                let _ = *settings_publications.borrow_and_update();
                let Some(publication) = settings.publication() else {
                    continue;
                };
                if view
                    .update(cx, |view, cx| {
                        view.gateway.settings = publication.settings.clone();
                        view.gateway.settings_loading = publication.loading;
                        view.gateway.settings_error = publication.error.clone();
                        let voice_changed = voice != publication.voice_input;
                        voice = publication.voice_input.clone();
                        if voice_changed {
                            if let Some(settings) = &voice {
                                view.desktop_voice_status =
                                    settings.runtime.phase.coarse_voice_status();
                                view.desktop_voice_status_error = settings.runtime.error.clone();
                                view.desktop_voice_status_poll_generation =
                                    view.desktop_voice_status_poll_generation.saturating_add(1);
                            }
                        }
                        if (publication.vector_refill_refresh_requested
                            || (voice_changed && voice.is_some() && publication.settings.is_none()))
                            && !publication.loading
                            && !publication.saving
                        {
                            view.refresh_gateway_settings(cx);
                        }
                        // Settings and voice presentation still belong to their legacy feature root.
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        }));
        let identity = self.gateway.identity_binding.clone();
        let mut publications = identity.watch();
        self.gateway.identity_task = Some(cx.spawn(async move |view, cx| {
            let mut connection_generation = None;
            let mut change_sequence: u64 = 0;
            while publications.changed().await.is_ok() {
                let _ = *publications.borrow_and_update();
                let Some(publication) = identity.publication() else {
                    continue;
                };
                let changed_epoch = connection_generation
                    .is_some_and(|generation| generation != publication.connection_generation);
                connection_generation = Some(publication.connection_generation);
                let missed =
                    publication.authorization_change_sequence > change_sequence.saturating_add(1);
                let apply_change = publication.authorization_change_sequence > change_sequence;
                change_sequence = publication.authorization_change_sequence;
                if view
                    .update(cx, |view, cx| {
                        if changed_epoch || missed {
                            view.clear_authorization_epoch_cache();
                        }
                        view.gateway.current_auth = publication.current_auth.clone();
                        view.gateway.capability_snapshot = publication
                            .capabilities
                            .snapshot(view.active_workspace_id(), None)
                            .or_else(|| publication.capabilities.snapshot(None, None));
                        if apply_change {
                            if let Some(change) = &publication.access_change {
                                view.apply_gateway_notification(
                                    GatewayNotification::AccessChanged(change.clone()),
                                    cx,
                                );
                            }
                            if let Some(change) = &publication.policy_change {
                                view.apply_gateway_notification(
                                    GatewayNotification::AuthorizationProjectionChanged(
                                        change.clone(),
                                    ),
                                    cx,
                                );
                            }
                        }
                    })
                    .is_err()
                {
                    break;
                }
            }
        }));

        self.gateway.compatibility_task =
            Some(cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                let mut cx = cx.clone();
                let client_runtime = client_runtime.clone();

                async move {
                    loop {
                        let first_event = client_runtime
                            .client_core()
                            .next_gateway_compatibility_event()
                            .await
                            .map(|event| event.into_event());
                        let should_break = first_event.is_none();

                        let updated = this.update(&mut cx, |view, cx| {
                            view.handle_gateway_ws_events(first_event, cx);
                        });

                        if updated.is_err() {
                            break;
                        }

                        if should_break {
                            break;
                        }
                    }
                }
            }));
    }

    pub(crate) fn handle_gateway_ws_events(
        &mut self,
        first_event: Option<GatewayWsEvent>,
        cx: &mut Context<Self>,
    ) {
        let events = first_event
            .into_iter()
            .chain(self.gateway.client_runtime.drain_ws_events());
        let applicable = self
            .gateway
            .client_runtime
            .client_core()
            .partition_gateway_compatibility_events(
                self.gateway.ws_connection_id,
                self.gateway.connecting
                    || self
                        .gateway
                        .client_runtime
                        .client_core()
                        .gateway_refresh_in_flight(),
                events,
            );
        self.apply_gateway_ws_event_batch(applicable, cx);
    }

    fn apply_gateway_ws_event_batch(
        &mut self,
        events: impl IntoIterator<Item = GatewayWsEvent>,
        cx: &mut Context<Self>,
    ) {
        let mut events_applied = false;
        for event in events {
            if !matches!(&event, GatewayWsEvent::Notification { .. }) {
                continue;
            }
            if matches!(
                &event,
                GatewayWsEvent::Notification {
                    notification: GatewayNotification::AccessChanged(_)
                        | GatewayNotification::AuthorizationProjectionChanged(_)
                        | GatewayNotification::AuthSessionRevoked(_)
                        | GatewayNotification::AuthAccessExpiring(_)
                        | GatewayNotification::GatewayRemoteAccessStatusChanged(_)
                        | GatewayNotification::GatewayThreadEpisodicVectorRefillStatusChanged(_)
                        | GatewayNotification::GatewayVoiceInputStatusChanged(_),
                    ..
                }
            ) {
                continue;
            }
            pioneer_observability::record_qualification_diagnostic!(record_client_delivery(
                pioneer_observability::Shell::Desktop,
                pioneer_observability::DeliveryLayer::DesktopEventPump,
                pioneer_observability::ClientScope::Other,
                pioneer_observability::DiagnosticAction::Delivered,
                pioneer_observability::Visibility::NotApplicable,
            ));
            pioneer_observability::record_qualification_diagnostic!(record_client_delivery(
                pioneer_observability::Shell::Desktop,
                pioneer_observability::DeliveryLayer::DesktopRootReducer,
                pioneer_observability::ClientScope::Other,
                pioneer_observability::DiagnosticAction::Attempted,
                pioneer_observability::Visibility::NotApplicable,
            ));
            self.apply_gateway_ws_event(event, cx);
            pioneer_observability::record_qualification_diagnostic!(record_client_delivery(
                pioneer_observability::Shell::Desktop,
                pioneer_observability::DeliveryLayer::DesktopRootReducer,
                pioneer_observability::ClientScope::Other,
                pioneer_observability::DiagnosticAction::Completed,
                pioneer_observability::Visibility::NotApplicable,
            ));
            events_applied = true;
        }
        let outcome = {
            let client_runtime = self.gateway.client_runtime.clone();
            let mut sink = DesktopPostEventSink { app: self, cx };
            client_runtime.drive_post_event_batch(events_applied, &mut sink)
        };

        if outcome.should_notify() {
            cx.notify();
        }
    }

    pub(in crate::app::flow) fn replay_deferred_gateway_ws_events(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let publication = self.gateway.client_runtime.client_core().gateway_session();
        self.apply_gateway_session_publication(&publication, cx);
        let active_connection_id = self.gateway.ws_connection_id;
        let applicable = self
            .gateway
            .client_runtime
            .client_core()
            .replay_gateway_compatibility_events(active_connection_id, false);
        self.apply_gateway_ws_event_batch(applicable, cx);
    }

    pub(in crate::app::flow) fn replay_deferred_gateway_ws_notifications(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let active_connection_id = self.gateway.ws_connection_id;
        let applicable = self
            .gateway
            .client_runtime
            .client_core()
            .replay_gateway_compatibility_events(active_connection_id, true);
        self.apply_gateway_ws_event_batch(applicable, cx);
    }

    pub(in crate::app::flow) fn discard_deferred_gateway_ws_events(&mut self) {
        self.gateway
            .client_runtime
            .client_core()
            .discard_gateway_compatibility_events();
    }

    pub(in crate::app::flow) fn next_gateway_connection_epoch(&mut self) -> u64 {
        // A user-initiated Gateway operation supersedes any scheduled or
        // in-flight UI refresh result. The durable refresh transaction may
        // still finish under its per-endpoint mutation lock, but it must not
        // replace the runtime selected by the newer operation.
        self.discard_deferred_gateway_ws_events();
        self.gateway
            .client_runtime
            .client_core()
            .begin_gateway_operation()
    }

    pub(in crate::app::flow) fn apply_gateway_ws_event(
        &mut self,
        event: GatewayWsEvent,
        cx: &mut Context<Self>,
    ) {
        if let GatewayWsEvent::Notification { notification, .. } = event {
            self.apply_gateway_notification(notification, cx);
        }
    }

    fn apply_gateway_session_publication(
        &mut self,
        publication: &pioneer_client::gateway::session_controller::GatewaySessionPublication,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connecting {
            return;
        }
        let Some(endpoint_id) = publication.startup.endpoint_id.as_deref() else {
            return;
        };
        if self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_gateway_id)
            != Some(endpoint_id)
        {
            return;
        }
        let Some(connection_id) = publication.startup.connection_id else {
            return;
        };
        if publication.startup.identity_pending {
            if self.gateway.transport_verification_id == Some(connection_id) {
                return;
            }
            self.gateway.transport_verification_id = Some(connection_id);
            self.startup.gateway_session_transport_connected();
            let endpoint_id = endpoint_id.to_owned();
            let client_core = self.gateway.client_runtime.client_core().clone();
            let sender = self.gateway.client_runtime.ws_command_sender().clone();
            self.gateway.transport_verification_task = Some(cx.spawn(async move |_view, cx| {
                let _ = cx.background_spawn(async move {
                    match GatewayRuntime::load(client_core.clone()) {
                        Ok(runtime) => runtime.verify_gateway_session_identity(&endpoint_id, &sender).map(|_| ()),
                        Err(error) => {
                            client_core.reject_gateway_session_identity(&endpoint_id, connection_id,
                                pioneer_client::gateway::session_connection::GatewaySessionConnectionFailure::Unavailable { code: format!("{error:#}") });
                            Err(error)
                        }
                    }
                }).await;
            }));
            return;
        }
        self.gateway.transport_verification_id = None;
        self.gateway.transport_verification_task.take();
        if publication
            .connections
            .get(endpoint_id)
            .is_some_and(|connection| connection.failure.is_some())
        {
            self.startup.gateway_session_identity_failed();
        }
        if publication.startup.transport_revision <= self.gateway.applied_transport_revision {
            return;
        }
        let context = pioneer_client::runtime::ClientRuntimeWsEventContext {
            queue_skills_refresh: matches!(
                self.main_content_view,
                MainContentView::Skills | MainContentView::SkillDetails
            ),
            should_resume_in_flight_turn: self.should_resume_in_flight_turn(),
        };
        let Some(reduction) = self
            .gateway
            .client_runtime
            .client_core()
            .gateway_feature_connection_projection(context)
        else {
            return;
        };
        if publication.startup.transport_revision
            > self.gateway.applied_transport_revision.saturating_add(1)
        {
            self.gateway.connection_state = GatewayConnectionState::Disconnected;
        }
        self.gateway.applied_transport_revision = publication.startup.transport_revision;
        self.gateway.ws_connection_id = Some(connection_id);
        match reduction.connection_state {
            GatewayConnectionState::Connecting => self.startup.gateway_session_attempt_started(),
            GatewayConnectionState::Reconnecting => self.startup.gateway_session_retry_scheduled(
                publication.startup.retry_attempt,
                publication.startup.retry_delay_ms,
                publication.gateway_error.as_deref().unwrap_or_default(),
            ),
            GatewayConnectionState::Disconnected => self.startup.gateway_session_transport_failed(
                publication.gateway_error.as_deref().unwrap_or_default(),
            ),
            _ => {}
        }
        self.apply_gateway_connection_reduction(reduction, Some(cx));
    }

    fn apply_gateway_session_terminal(
        &mut self,
        reason: pioneer_client::gateway::session_lifecycle::SessionTerminalReason,
        cx: &mut Context<Self>,
    ) {
        self.gateway
            .client_runtime
            .client_core()
            .cancel_gateway_refresh();
        self.gateway.auth_session_action_pending = None;
        self.discard_deferred_gateway_ws_events();
        self.clear_authorization_epoch_cache();
        self.gateway.current_auth = None;
        self.gateway.capability_snapshot = None;
        self.clear_task_user_notification_inbox();
        self.administration.clear_for_session_termination();
        self.member_avatar_state.clear();
        self.member_workspaces_saving = false;
        self.gateway.ws_connection_id = None;
        self.gateway.connection_state = GatewayConnectionState::Disconnected;
        self.gateway.error = Some(desktop_session_terminal_message(reason));
        let _ = self.gateway.client_runtime.ws_command_sender().disconnect();
        cx.notify();
    }
}

struct DesktopPostEventSink<'a, 'cx> {
    app: &'a mut PioneerDesktop,
    cx: &'a mut Context<'cx, PioneerDesktop>,
}

impl ClientRuntimePostEventSink for DesktopPostEventSink<'_, '_> {
    fn refresh_thread_list_if_requested(&mut self) -> bool {
        if !self.app.take_thread_list_refresh_request() {
            return false;
        }
        self.app.refresh_thread_list(self.cx);
        true
    }

    fn refresh_skills_if_requested(&mut self) -> bool {
        if !self.app.take_skills_refresh_request() {
            return false;
        }
        self.app.refresh_installed_skills(self.cx);
        true
    }

    fn refresh_mcp_if_requested(&mut self) -> bool {
        if !self.app.take_mcp_refresh_request() {
            return false;
        }
        self.app.refresh_mcp_servers(self.cx);
        true
    }

    fn refresh_mcp_details_if_requested(&mut self) -> bool {
        if !self.app.take_mcp_details_refresh_request() {
            return false;
        }
        self.app.refresh_mcp_server_details(self.cx);
        true
    }

    fn drive_thread_start_queue(&mut self) -> bool {
        self.app.drive_thread_start_queue(self.cx)
    }

    fn drive_turn_resume_queue(&mut self) -> bool {
        self.app.drive_turn_resume_queue(self.cx)
    }

    fn tick_thread_conversations(&mut self) -> bool {
        self.app.tick_thread_conversations()
    }
}
