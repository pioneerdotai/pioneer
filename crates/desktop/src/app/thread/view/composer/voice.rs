use crate::{
    app::root::{
        DesktopVoiceComposerState, DesktopVoiceHoldTarget, DesktopVoiceReleaseCandidate,
        GatewayConnectionState, PioneerDesktop,
    },
    assets::PioneerIconName,
    audio::{
        capture::{
            DesktopVoiceCaptureConfig, DesktopVoiceCaptureError, DesktopVoiceCaptureErrorKind,
            DesktopVoiceCaptureFlow, PlatformDesktopAudioInputBackend,
        },
        microphone::{
            DesktopMicrophoneFormatRequest, PlatformDesktopMicrophoneDeviceProbe,
            verify_desktop_microphone_ready,
        },
    },
    gateway::DesktopGatewayWsCommandSenderExt,
};
use gpui::{prelude::*, *};
use gpui_component::{theme::ActiveTheme, *};
use pioneer_client::{
    composer::{
        state_machine::ComposerDomainAction,
        turn_prepare::{self, PrepareVoiceComposerSnapshotRequest},
    },
    providers::list as provider_list,
    threads::start as thread_start,
    turns::start as turn_start,
    voice::{VoiceFinalizeUiAction, reduce_voice_session_finalize_response},
};
use pioneer_protocol::{ThreadMode, VoiceSessionStartContext, VoiceStatus, VoiceStatusParams};
use std::time::Duration;
use tracing::warn;

const DESKTOP_VOICE_HOLD_RADIUS: Pixels = px(16.);
const DESKTOP_VOICE_STATUS_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const DESKTOP_VOICE_SESSION_READY_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DesktopVoiceEntryAvailability {
    Hidden,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DesktopVoiceEntryContext {
    voice_composer_idle: bool,
    gateway_connected: bool,
    active_thread: bool,
    model_selected: bool,
    conversation_can_submit: bool,
    upload_idle: bool,
}

impl DesktopVoiceEntryContext {
    fn allows_voice(self) -> bool {
        self.voice_composer_idle
            && self.gateway_connected
            && self.active_thread
            && self.model_selected
            && self.conversation_can_submit
            && self.upload_idle
    }
}

impl PioneerDesktop {
    pub(in crate::app) fn desktop_voice_hold_ui_active(&self) -> bool {
        matches!(
            self.desktop_voice_composer,
            DesktopVoiceComposerState::Preparing {
                release_requested: false,
                ..
            } | DesktopVoiceComposerState::Holding { .. }
        )
    }

    pub(in crate::app) fn desktop_voice_send_processing(&self) -> bool {
        matches!(
            self.desktop_voice_composer,
            DesktopVoiceComposerState::Preparing {
                release_requested: true,
                ..
            } | DesktopVoiceComposerState::Finalizing { .. }
        )
    }

    pub(in crate::app) fn desktop_voice_context_locked(&self) -> bool {
        self.desktop_voice_composer.is_active()
    }

    pub(in crate::app) fn desktop_voice_error_message(&self) -> Option<&str> {
        self.desktop_voice_composer.error_message()
    }

    pub(in crate::app) fn refresh_desktop_voice_status(&mut self, cx: &mut Context<Self>) {
        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.desktop_voice_status = VoiceStatus::Unavailable;
            self.desktop_voice_status_error = None;
            cx.notify();
            return;
        };
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.desktop_voice_status = VoiceStatus::Unavailable;
            self.desktop_voice_status_error = None;
            cx.notify();
            return;
        }

        self.desktop_voice_status_poll_generation =
            self.desktop_voice_status_poll_generation.saturating_add(1);
        let generation = self.desktop_voice_status_poll_generation;
        let workspace_id = self.active_workspace_id().map(str::to_owned);
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.voice_status(VoiceStatusParams { workspace_id })
                    })
                    .await;

                let should_retry = this
                    .update(&mut cx, |view, cx| {
                        if view.gateway.ws_connection_id != Some(connection_id)
                            || view.desktop_voice_status_poll_generation != generation
                        {
                            return false;
                        }

                        match result {
                            Ok(response) => {
                                view.desktop_voice_status = response.status;
                                view.desktop_voice_status_error =
                                    response.error.map(|error| error.message);
                            }
                            Err(error) => {
                                view.desktop_voice_status = VoiceStatus::Unavailable;
                                let details = format!("{error:#}");
                                view.desktop_voice_status_error = Some(
                                    t!(
                                        "chat.composer.voice.status_load_failed",
                                        error = details.as_str()
                                    )
                                    .to_string(),
                                );
                                warn!(
                                    error = %format!("{error:#}"),
                                    "failed to load desktop voice status"
                                );
                            }
                        }

                        cx.notify();
                        view.gateway.connection_state == GatewayConnectionState::Connected
                            && !matches!(view.desktop_voice_status, VoiceStatus::Ready)
                    })
                    .unwrap_or(false);

                if !should_retry {
                    return;
                }

                cx.background_executor()
                    .timer(DESKTOP_VOICE_STATUS_RETRY_INTERVAL)
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id == Some(connection_id)
                        && view.gateway.connection_state == GatewayConnectionState::Connected
                    {
                        view.refresh_desktop_voice_status(cx);
                    }
                });
            }
        })
        .detach();
    }

    pub(super) fn desktop_voice_entry_availability(&self) -> DesktopVoiceEntryAvailability {
        let voice_input_enabled = effective_voice_input_enabled(
            self.pending_voice_input_enabled,
            self.gateway
                .settings
                .as_ref()
                .map(|settings| settings.voice_input.enabled),
            self.desktop_voice_status,
        );
        desktop_voice_entry_availability_for_context(
            DesktopVoiceEntryContext {
                voice_composer_idle: !self.desktop_voice_composer.is_active(),
                gateway_connected: super::desktop_composer_transport_ready(
                    self.gateway.connection_state,
                ),
                active_thread: self.current_active_thread_id().is_some(),
                model_selected: self.composer_turn_mode == ThreadMode::Message
                    || self.has_complete_composer_model_selection(),
                conversation_can_submit: if self.composer_turn_mode == ThreadMode::Message {
                    self.can_write_active_thread_presentation()
                } else {
                    self.can_start_active_thread_agent_presentation()
                        && self
                            .active_thread_conversation()
                            .is_some_and(|conversation| conversation.can_submit_message())
                },
                upload_idle: !self.composer_upload_in_progress,
            },
            voice_input_enabled,
            self.desktop_voice_status,
        )
    }

    pub(super) fn start_desktop_voice_hold(
        &mut self,
        pointer_position: Point<Pixels>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.reconcile_composer_draft_with_capabilities();
        let Some(authorization_fingerprint) = self.composer_authorization_fingerprint.clone()
        else {
            return;
        };
        if self.desktop_voice_composer.is_active()
            || self.desktop_voice_status != VoiceStatus::Ready
            || self.desktop_voice_entry_availability() != DesktopVoiceEntryAvailability::Ready
        {
            return;
        }

        let target = DesktopVoiceHoldTarget {
            center: pointer_position,
            radius: DESKTOP_VOICE_HOLD_RADIUS,
        };

        self.desktop_voice_composer = DesktopVoiceComposerState::Preparing {
            target,
            candidate: DesktopVoiceReleaseCandidate::Send,
            release_requested: false,
        };
        self.composer_upload_error = None;
        self.desktop_microphone_gate = verify_desktop_microphone_ready(
            &PlatformDesktopMicrophoneDeviceProbe,
            DesktopMicrophoneFormatRequest::default(),
        );
        cx.notify();

        if !self
            .desktop_microphone_gate
            .can_open_gateway_voice_session()
        {
            let message = self
                .desktop_microphone_gate
                .composer_error_message()
                .unwrap_or_else(|| t!("chat.composer.voice.microphone_not_ready").to_string());
            self.desktop_voice_composer = DesktopVoiceComposerState::Error {
                kind: DesktopVoiceCaptureErrorKind::PermissionDenied,
                message,
            };
            cx.notify();
            return;
        }

        let Some(thread_id) = self.active_thread_id.clone() else {
            self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
            cx.notify();
            return;
        };
        let workspace_id = self
            .thread_workspace_id(thread_id.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| self.default_thread_start_scope());
        let selected_mode = self.composer_turn_mode;
        let selected_permission_mode = self.composer_permission_mode;
        let selected_model = self.composer_selected_model.clone();
        let selected_provider = self.composer_selected_provider.clone();
        let selected_reasoning_effort = self.composer_selected_reasoning_effort.clone();
        let selected_cli_runtime_backend =
            match provider_list::resolve_cli_runtime_execution_backend(
                selected_provider.as_deref(),
                self.providers.cli_runtimes(),
                self.gateway
                    .settings
                    .as_ref()
                    .map(|settings| &settings.cli_runtimes),
            ) {
                Ok(backend) => backend,
                Err(error) => {
                    self.desktop_voice_composer = DesktopVoiceComposerState::Error {
                        kind: DesktopVoiceCaptureErrorKind::GatewaySession,
                        message: error,
                    };
                    cx.notify();
                    return;
                }
            };
        let cli_runtime_selected = selected_cli_runtime_backend.is_some();
        let turn_model_provider = if cli_runtime_selected {
            None
        } else {
            selected_provider.clone()
        };
        let composer_attachments = self.composer_attachments.clone();
        let composer_capabilities = self.effective_composer_capabilities();
        let composer_skill_selections = self.composer_skill_selections.clone();
        let composer_skill_picker = self.composer_skill_picker_projection("");
        let endpoint_kind = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().map(|gateway| gateway.kind));
        let turn_start_ids = turn_start::plan_turn_start_ids();
        let turn_id = turn_start_ids.turn_id;
        let gateway_sender = self.gateway.ws_command_sender.clone();

        let prepare_request = PrepareVoiceComposerSnapshotRequest {
            authorization_fingerprint,
            workspace_id: workspace_id.clone(),
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            endpoint_kind,
            attachments: composer_attachments,
            capabilities: composer_capabilities,
            skill_selections: composer_skill_selections,
            skill_picker: composer_skill_picker,
            selected_model,
            selected_provider,
            turn_model_provider,
            selected_mode: Some(selected_mode),
            permission_mode: selected_permission_mode,
            execution_backend: selected_cli_runtime_backend,
            selected_reasoning_effort,
            cli_runtime_options: None,
        };

        let preflight_sender = gateway_sender.clone();
        let microphone_gate = self.desktop_microphone_gate.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                while this
                    .update(&mut cx, |view, _| view.gateway.session_refresh_in_flight)
                    .unwrap_or(false)
                {
                    cx.background_executor()
                        .timer(DESKTOP_VOICE_SESSION_READY_RETRY_DELAY)
                        .await;
                }
                let preflight_result = cx
                    .background_spawn({
                        let thread_id = thread_id.clone();
                        let workspace_id = workspace_id.clone();
                        async move {
                            preflight_sender.thread_start(thread_start::thread_start_params(
                                thread_id,
                                workspace_id,
                            ))
                        }
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    let (candidate, release_requested) = match view.desktop_voice_composer {
                        DesktopVoiceComposerState::Preparing {
                            candidate,
                            release_requested,
                            ..
                        } => (candidate, release_requested),
                        _ => return,
                    };

                    if let Err(error) = preflight_result {
                        view.desktop_voice_composer = DesktopVoiceComposerState::Error {
                            kind: DesktopVoiceCaptureErrorKind::GatewaySession,
                            message: format!("{error:#}"),
                        };
                        cx.notify();
                        return;
                    }

                    if view.current_active_thread_id() != Some(thread_id.as_str())
                        || view.thread_workspace_id(thread_id.as_str())
                            != Some(workspace_id.as_str())
                    {
                        view.desktop_voice_composer = DesktopVoiceComposerState::Idle;
                        cx.notify();
                        return;
                    }

                    if release_requested && candidate == DesktopVoiceReleaseCandidate::Cancel {
                        view.desktop_voice_composer = DesktopVoiceComposerState::Idle;
                        cx.notify();
                        return;
                    }

                    let mut flow = DesktopVoiceCaptureFlow::new(
                        PlatformDesktopAudioInputBackend,
                        gateway_sender,
                    );
                    if let Err(error) = flow.start(
                        &microphone_gate,
                        DesktopVoiceCaptureConfig::default(),
                        VoiceSessionStartContext {
                            workspace_id: workspace_id.clone(),
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                        },
                    ) {
                        view.desktop_voice_composer =
                            desktop_voice_error_state_from_capture_error(error);
                        cx.notify();
                        return;
                    }

                    view.desktop_voice_prepare_request = Some(prepare_request);
                    view.desktop_voice_capture = Some(flow);
                    view.desktop_voice_composer =
                        DesktopVoiceComposerState::Holding { target, candidate };
                    if release_requested {
                        view.finish_desktop_voice_hold_send(cx);
                    } else {
                        cx.notify();
                    }
                });
            }
        })
        .detach();
    }

    pub(super) fn update_desktop_voice_hold_pointer(
        &mut self,
        position: Point<Pixels>,
        cx: &mut Context<Self>,
    ) {
        let next_candidate = self.desktop_voice_release_candidate_at(position);
        match &mut self.desktop_voice_composer {
            DesktopVoiceComposerState::Preparing { candidate, .. }
            | DesktopVoiceComposerState::Holding { candidate, .. } => {
                if *candidate != next_candidate {
                    *candidate = next_candidate;
                    cx.notify();
                }
            }
            _ => {}
        }
    }

    pub(super) fn release_desktop_voice_hold_at(
        &mut self,
        position: Point<Pixels>,
        cx: &mut Context<Self>,
    ) {
        let candidate = self.desktop_voice_release_candidate_at(position);
        match &mut self.desktop_voice_composer {
            DesktopVoiceComposerState::Preparing {
                candidate: current_candidate,
                release_requested,
                ..
            } => {
                *current_candidate = candidate;
                *release_requested = true;
                cx.notify();
            }
            DesktopVoiceComposerState::Holding { .. } => match candidate {
                DesktopVoiceReleaseCandidate::Send => self.finish_desktop_voice_hold_send(cx),
                DesktopVoiceReleaseCandidate::Cancel => {
                    self.cancel_desktop_voice_hold("desktop_release_outside_button", cx)
                }
            },
            _ => {}
        }
    }

    pub(super) fn finish_desktop_voice_hold_send(&mut self, cx: &mut Context<Self>) {
        if !matches!(
            self.desktop_voice_composer,
            DesktopVoiceComposerState::Holding {
                candidate: DesktopVoiceReleaseCandidate::Send,
                ..
            }
        ) {
            self.cancel_desktop_voice_hold("desktop_release_outside_button", cx);
            return;
        }

        let Some(mut flow) = self.desktop_voice_capture.take() else {
            self.desktop_voice_prepare_request = None;
            self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
            cx.notify();
            return;
        };
        let Some(prepare_request) = self.desktop_voice_prepare_request.take() else {
            let _ = flow.release_cancel();
            self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
            cx.notify();
            return;
        };
        self.reconcile_composer_draft_with_capabilities();
        if self.composer_authorization_fingerprint.as_deref()
            != Some(prepare_request.authorization_fingerprint.as_str())
        {
            let _ = flow.release_cancel();
            self.desktop_voice_composer = DesktopVoiceComposerState::Error {
                kind: DesktopVoiceCaptureErrorKind::GatewaySession,
                message: "Voice draft policy changed; review the updated selections".to_owned(),
            };
            cx.notify();
            return;
        }

        self.desktop_voice_composer = DesktopVoiceComposerState::Finalizing {
            thread_id: prepare_request.thread_id.clone(),
        };
        if let Err(error) = flow.stop_recording() {
            let _ = flow.release_cancel();
            self.desktop_voice_composer = desktop_voice_error_state_from_capture_error(error);
            cx.notify();
            return;
        }

        let upload_sender = self.gateway.ws_command_sender.clone();
        self.composer_upload_in_progress = true;
        self.composer_upload_error = None;
        self.reduce_composer_domain(ComposerDomainAction::MarkAttachmentsUploading);
        cx.notify();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let prepare_result = cx
                    .background_spawn(async move {
                        upload_sender.prepare_voice_composer_snapshot(prepare_request)
                    })
                    .await;

                let snapshot = match prepare_result {
                    Ok(snapshot) => {
                        let uploaded_artifacts = snapshot.uploaded_attachment_artifacts.clone();
                        let _ = this.update(&mut cx, move |view, cx| {
                            view.composer_upload_in_progress = false;
                            view.composer_upload_error = None;
                            view.reduce_composer_domain(
                                ComposerDomainAction::ApplyUploadedAttachments {
                                    artifacts: uploaded_artifacts,
                                },
                            );
                            cx.notify();
                        });
                        snapshot
                    }
                    Err(error) => {
                        let _ = flow.release_cancel();
                        let message = format!("{error:#}");
                        let _ = this.update(&mut cx, move |view, cx| {
                            let reduction =
                                turn_prepare::reduce_prepare_composer_turn_failure(message);
                            view.composer_upload_in_progress =
                                reduction.composer_upload_in_progress;
                            view.composer_upload_error =
                                Some(reduction.composer_upload_error.clone());
                            view.reduce_composer_domain(
                                ComposerDomainAction::MarkAttachmentsFailed {
                                    error: reduction
                                        .mark_uploading_attachments_failed_error
                                        .clone(),
                                },
                            );
                            if matches!(
                                view.desktop_voice_composer,
                                DesktopVoiceComposerState::Finalizing { .. }
                            ) {
                                view.desktop_voice_composer = DesktopVoiceComposerState::Error {
                                    kind: DesktopVoiceCaptureErrorKind::GatewaySession,
                                    message: t!(
                                        "chat.composer.voice.prepare_failed",
                                        error = reduction.composer_upload_error.as_str()
                                    )
                                    .to_string(),
                                };
                            }
                            cx.notify();
                        });
                        return;
                    }
                };

                let result = cx
                    .background_spawn(async move { flow.finalize_send(snapshot.context) })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    match result {
                        Ok(ack) => {
                            if matches!(
                                view.desktop_voice_composer,
                                DesktopVoiceComposerState::Finalizing { .. }
                            ) {
                                let reduction = reduce_voice_session_finalize_response(
                                    ack.session_id,
                                    &ack.response,
                                );
                                if matches!(
                                    reduction.action,
                                    VoiceFinalizeUiAction::ClearFinalizing
                                ) {
                                    view.desktop_voice_composer = DesktopVoiceComposerState::Idle;
                                }
                            }
                        }
                        Err(error) => {
                            if matches!(
                                view.desktop_voice_composer,
                                DesktopVoiceComposerState::Finalizing { .. }
                            ) {
                                view.desktop_voice_composer =
                                    desktop_voice_error_state_from_capture_error(error);
                            }
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn cancel_desktop_voice_hold(&mut self, reason: &str, cx: &mut Context<Self>) {
        if let Some(mut flow) = self.desktop_voice_capture.take()
            && let Err(error) = flow.release_cancel()
        {
            warn!(
                reason,
                error = %format!("{error:#}"),
                "failed to cancel desktop voice session"
            );
        }
        self.desktop_voice_prepare_request = None;
        self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
        cx.notify();
    }

    pub(super) fn render_desktop_voice_idle_button(&self, cx: &mut Context<Self>) -> AnyElement {
        let candidate = match self.desktop_voice_composer {
            DesktopVoiceComposerState::Preparing { candidate, .. } => candidate,
            DesktopVoiceComposerState::Holding { candidate, .. } => candidate,
            _ => DesktopVoiceReleaseCandidate::Send,
        };
        let bg = match self.desktop_voice_composer {
            DesktopVoiceComposerState::Preparing { .. }
            | DesktopVoiceComposerState::Holding { .. } => match candidate {
                DesktopVoiceReleaseCandidate::Send => cx.theme().blue,
                DesktopVoiceReleaseCandidate::Cancel => cx.theme().red,
            },
            _ => cx.theme().primary,
        };

        div()
            .id("desktop-voice-idle-button")
            .size(px(32.))
            .rounded_full()
            .flex()
            .items_center()
            .justify_center()
            .cursor_pointer()
            .bg(bg)
            .text_color(cx.theme().primary_foreground)
            .child(Icon::new(PioneerIconName::Microphone).size_4())
            .on_mouse_move(cx.listener(|view, event: &MouseMoveEvent, _, cx| {
                view.update_desktop_voice_hold_pointer(event.position, cx);
            }))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|view, event: &MouseDownEvent, window, cx| {
                    view.start_desktop_voice_hold(event.position, window, cx);
                }),
            )
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|view, event: &MouseUpEvent, _, cx| {
                    view.release_desktop_voice_hold_at(event.position, cx);
                }),
            )
            .on_mouse_up_out(
                MouseButton::Left,
                cx.listener(|view, event: &MouseUpEvent, _, cx| {
                    view.release_desktop_voice_hold_at(event.position, cx);
                }),
            )
            .into_any_element()
    }

    pub(super) fn render_desktop_voice_hold_prompt(&self, _: &mut Context<Self>) -> AnyElement {
        div()
            .id("desktop-voice-hold-prompt")
            .w_full()
            .h(px(56.))
            .px_3()
            .pt_3()
            .flex()
            .justify_center()
            .items_center()
            .text_sm()
            .font_medium()
            .child(t!("chat.composer.voice.desktop_hold_prompt").to_string())
            .into_any_element()
    }
}

impl PioneerDesktop {
    fn desktop_voice_release_candidate_at(
        &self,
        position: Point<Pixels>,
    ) -> DesktopVoiceReleaseCandidate {
        match self.desktop_voice_composer {
            DesktopVoiceComposerState::Preparing { target, .. }
            | DesktopVoiceComposerState::Holding { target, .. } => {
                if target.contains(position) {
                    DesktopVoiceReleaseCandidate::Send
                } else {
                    DesktopVoiceReleaseCandidate::Cancel
                }
            }
            _ => DesktopVoiceReleaseCandidate::Send,
        }
    }
}

fn desktop_voice_entry_availability_for_context(
    context: DesktopVoiceEntryContext,
    voice_input_enabled: bool,
    status: VoiceStatus,
) -> DesktopVoiceEntryAvailability {
    if !context.allows_voice() || !voice_input_enabled {
        return DesktopVoiceEntryAvailability::Hidden;
    }

    match status {
        VoiceStatus::Ready => DesktopVoiceEntryAvailability::Ready,
        VoiceStatus::Disabled
        | VoiceStatus::ModelDownloading
        | VoiceStatus::ModelLoading
        | VoiceStatus::Busy
        | VoiceStatus::Recording
        | VoiceStatus::Transcribing
        | VoiceStatus::Unavailable
        | VoiceStatus::Error => DesktopVoiceEntryAvailability::Hidden,
    }
}

fn effective_voice_input_enabled(
    pending: Option<bool>,
    authoritative: Option<bool>,
    status: VoiceStatus,
) -> bool {
    // Gateway settings are a management projection and are intentionally not
    // available to Members. The operational voice/status endpoint is the
    // authoritative use-time projection for them. Preserve immediate admin
    // toggle feedback when a settings value is locally available.
    pending
        .or(authoritative)
        .unwrap_or(status == VoiceStatus::Ready)
}

fn desktop_voice_error_state_from_capture_error(
    error: DesktopVoiceCaptureError,
) -> DesktopVoiceComposerState {
    DesktopVoiceComposerState::Error {
        kind: error.kind,
        message: error.composer_message().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const READY_CONTEXT: DesktopVoiceEntryContext = DesktopVoiceEntryContext {
        voice_composer_idle: true,
        gateway_connected: true,
        active_thread: true,
        model_selected: true,
        conversation_can_submit: true,
        upload_idle: true,
    };

    #[::core::prelude::v1::test]
    fn pending_voice_disable_hides_microphone_before_gateway_ack() {
        assert!(!effective_voice_input_enabled(
            Some(false),
            Some(true),
            VoiceStatus::Ready
        ));
        assert!(effective_voice_input_enabled(
            Some(true),
            Some(false),
            VoiceStatus::Unavailable
        ));
        assert!(effective_voice_input_enabled(
            None,
            Some(true),
            VoiceStatus::Ready
        ));
        assert!(!effective_voice_input_enabled(
            None,
            Some(false),
            VoiceStatus::Ready
        ));
        assert!(effective_voice_input_enabled(
            None,
            None,
            VoiceStatus::Ready
        ));
        assert!(!effective_voice_input_enabled(
            None,
            None,
            VoiceStatus::Unavailable
        ));

        assert_eq!(
            desktop_voice_entry_availability_for_context(
                READY_CONTEXT,
                effective_voice_input_enabled(Some(false), Some(true), VoiceStatus::Unavailable,),
                VoiceStatus::Unavailable,
            ),
            DesktopVoiceEntryAvailability::Hidden
        );
    }

    #[::core::prelude::v1::test]
    fn composer_voice_is_hidden_for_every_blocked_context() {
        let blocked_contexts = [
            DesktopVoiceEntryContext {
                voice_composer_idle: false,
                ..READY_CONTEXT
            },
            DesktopVoiceEntryContext {
                gateway_connected: false,
                ..READY_CONTEXT
            },
            DesktopVoiceEntryContext {
                active_thread: false,
                ..READY_CONTEXT
            },
            DesktopVoiceEntryContext {
                model_selected: false,
                ..READY_CONTEXT
            },
            DesktopVoiceEntryContext {
                conversation_can_submit: false,
                ..READY_CONTEXT
            },
            DesktopVoiceEntryContext {
                upload_idle: false,
                ..READY_CONTEXT
            },
        ];

        for context in blocked_contexts {
            assert_eq!(
                desktop_voice_entry_availability_for_context(context, true, VoiceStatus::Ready),
                DesktopVoiceEntryAvailability::Hidden
            );
        }
    }

    #[::core::prelude::v1::test]
    fn composer_voice_readiness_matches_gateway_status_matrix() {
        for status in [
            VoiceStatus::Disabled,
            VoiceStatus::Ready,
            VoiceStatus::ModelDownloading,
            VoiceStatus::ModelLoading,
            VoiceStatus::Busy,
            VoiceStatus::Recording,
            VoiceStatus::Transcribing,
            VoiceStatus::Unavailable,
            VoiceStatus::Error,
        ] {
            assert_eq!(
                desktop_voice_entry_availability_for_context(READY_CONTEXT, false, status),
                DesktopVoiceEntryAvailability::Hidden
            );
        }

        assert_eq!(
            desktop_voice_entry_availability_for_context(
                READY_CONTEXT,
                true,
                VoiceStatus::Disabled,
            ),
            DesktopVoiceEntryAvailability::Hidden
        );
        assert_eq!(
            desktop_voice_entry_availability_for_context(READY_CONTEXT, true, VoiceStatus::Ready,),
            DesktopVoiceEntryAvailability::Ready
        );
        for status in [
            VoiceStatus::ModelDownloading,
            VoiceStatus::ModelLoading,
            VoiceStatus::Busy,
            VoiceStatus::Recording,
            VoiceStatus::Transcribing,
            VoiceStatus::Unavailable,
            VoiceStatus::Error,
        ] {
            assert_eq!(
                desktop_voice_entry_availability_for_context(READY_CONTEXT, true, status),
                DesktopVoiceEntryAvailability::Hidden
            );
        }
    }

    #[::core::prelude::v1::test]
    fn composer_voice_ready_path_keeps_capture_and_session_transport_unchanged() {
        let source = include_str!("voice.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source exists");
        let start = source
            .split("pub(super) fn start_desktop_voice_hold")
            .nth(1)
            .expect("voice start function exists")
            .split("pub(super) fn update_desktop_voice_hold_pointer")
            .next()
            .expect("voice start function body exists");
        assert!(start.contains("self.desktop_voice_status != VoiceStatus::Ready"));
        assert!(start.contains("DesktopVoiceCaptureFlow::new"));
        assert!(start.contains("flow.start("));
        assert!(start.contains("VoiceSessionStartContext"));
    }

    #[::core::prelude::v1::test]
    fn composer_voice_status_event_converges_without_restart_or_stale_poll_override() {
        let source = include_str!("../../../flow/ws_events_notifications.rs");
        let handler = source
            .split("fn apply_voice_input_status_changed")
            .nth(1)
            .expect("Voice Input status handler exists")
            .split("fn apply_thread_started_reduction")
            .next()
            .expect("Voice Input status handler body exists");
        assert!(handler.contains("settings.runtime.phase.coarse_voice_status()"));
        assert!(handler.contains("desktop_voice_status_poll_generation.saturating_add(1)"));
        assert!(handler.contains("current.voice_input = settings"));
    }
}
