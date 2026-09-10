use super::voice_owner::{VoiceInputEvent, VoiceInputView};
use crate::assets::PioneerIconName;
use crate::composer::GatewayConnectionState;
use crate::ports::{
    ThreadAudioCompletion, ThreadAudioError as DesktopVoiceCaptureError,
    ThreadAudioErrorKind as DesktopVoiceCaptureErrorKind, ThreadAudioRequest,
    ThreadPresentationOperation,
};
use gpui_kit::component::theme::ActiveTheme;
use gpui_kit::component::*;
use gpui_kit::prelude::*;
use gpui_kit::*;
use pioneer_client::composer::store::ComposerIntent;
use pioneer_client::composer::store::ComposerOperationCompletion;
use pioneer_client::composer::store::ComposerOperationKind;
use pioneer_client::timeline::types::ThreadMode;
use pioneer_client::voice::VoiceError;
use pioneer_client::voice::VoiceFinalizeUiAction;
use pioneer_client::voice::VoiceStatus;

const DESKTOP_VOICE_HOLD_RADIUS: Pixels = px(16.);

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

impl VoiceInputView {
    pub(crate) fn present_desktop_voice_publication(&mut self, cx: &mut Context<Self>) {
        use pioneer_client::composer::store::ComposerOperationStatus;
        let Some(operation) = self
            .composer_input
            .as_ref()
            .and_then(|input| input.operation())
            .filter(|operation| {
                self.desktop_voice_operation.as_ref() == Some(&operation.identity)
                    && operation.kind == ComposerOperationKind::Voice
            })
        else {
            return;
        };
        if let Some(result) = &operation.voice_result {
            match result.action {
                VoiceFinalizeUiAction::KeepFinalizing => {}
                VoiceFinalizeUiAction::ClearFinalizing => {
                    self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
                    self.desktop_voice_operation = None;
                }
                VoiceFinalizeUiAction::ShowNoSpeechError => {
                    self.desktop_voice_composer = DesktopVoiceComposerState::Error {
                        kind: DesktopVoiceCaptureErrorKind::NoSpeech,
                        message: desktop_voice_no_speech_message(result.error.as_ref()),
                    };
                }
                VoiceFinalizeUiAction::ShowFinalizeError => {
                    self.desktop_voice_composer = DesktopVoiceComposerState::Error {
                        kind: DesktopVoiceCaptureErrorKind::GatewayFinalize,
                        message: desktop_voice_transcription_failed_message(result.error.as_ref()),
                    };
                }
            }
        } else if operation.status == ComposerOperationStatus::Completed {
            self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
            self.desktop_voice_operation = None;
        } else if operation
            .voice_finalize
            .as_ref()
            .is_some_and(|response| response.action == VoiceFinalizeUiAction::ClearFinalizing)
        {
            self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
        }
        self.publish_presentation(cx);
    }

    pub(crate) fn desktop_voice_hold_ui_active(&self) -> bool {
        matches!(
            self.desktop_voice_composer,
            DesktopVoiceComposerState::Preparing {
                release_requested: false,
                ..
            } | DesktopVoiceComposerState::Holding { .. }
        )
    }

    pub(crate) fn desktop_voice_send_processing(&self) -> bool {
        matches!(
            self.desktop_voice_composer,
            DesktopVoiceComposerState::Preparing {
                release_requested: true,
                ..
            } | DesktopVoiceComposerState::Finalizing { .. }
        )
    }

    pub(crate) fn desktop_voice_error_message(&self) -> Option<&str> {
        self.desktop_voice_composer.error_message()
    }

    pub(crate) fn refresh_desktop_voice_status(&mut self, cx: &mut Context<Self>) {
        let Some(input) = self.composer_input.as_ref() else {
            return;
        };
        let core = self.client.clone();
        core.composer_intent(ComposerIntent::SetVoiceReadinessDemand {
            thread_id: input.thread_id().into(),
            draft_id: input.draft_id(),
            demand: if self.window_active
                && self.connection_state == GatewayConnectionState::Connected
            {
                pioneer_client::composer::voice_readiness::ComposerVoiceReadinessDemand::UntilReady
            } else {
                pioneer_client::composer::voice_readiness::ComposerVoiceReadinessDemand::Suspended
            },
        });
        self.composer_input = core.composer_snapshot(input.thread_id());
        self.publish_presentation(cx);
    }

    fn desktop_voice_status(&self) -> VoiceStatus {
        self.composer_input
            .as_ref()
            .and_then(|p| p.voice_readiness())
            .and_then(|p| p.response.as_ref())
            .map_or(VoiceStatus::Unavailable, |p| p.status)
    }

    pub(super) fn desktop_voice_entry_availability(&self) -> DesktopVoiceEntryAvailability {
        let voice_input_enabled = effective_voice_input_enabled(
            None,
            self.settings_input
                .as_ref()
                .and_then(|input| input.settings.as_ref())
                .map(|settings| settings.voice_input.enabled),
            self.desktop_voice_status(),
        );
        desktop_voice_entry_availability_for_context(
            DesktopVoiceEntryContext {
                voice_composer_idle: !self.desktop_voice_composer.is_active(),
                gateway_connected: super::desktop_composer_transport_ready(self.connection_state),
                active_thread: self.current_active_thread_id().is_some(),
                model_selected: self.composer_domain().selected_mode == ThreadMode::Message
                    || self.has_complete_composer_model_selection(),
                conversation_can_submit: if self.composer_domain().selected_mode
                    == ThreadMode::Message
                {
                    self.can_write_active_thread_presentation()
                } else {
                    self.can_start_active_thread_agent_presentation()
                        && self
                            .active_thread_conversation()
                            .is_some_and(|conversation| conversation.can_submit_message())
                },
                upload_idle: !self.composer_upload_in_progress(),
            },
            voice_input_enabled,
            self.desktop_voice_status(),
        )
    }

    pub(super) fn start_desktop_voice_hold(
        &mut self,
        pointer_position: Point<Pixels>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.composer_authorization_fingerprint().is_none() {
            return;
        };
        if self.desktop_voice_composer.is_active()
            || self.desktop_voice_status() != VoiceStatus::Ready
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
        cx.emit(VoiceInputEvent::UploadError(None));
        let thread_id = self.thread_id.clone();
        let client = self.client.clone();
        let Some(input) = client.composer_snapshot(&thread_id) else {
            return;
        };
        if client
            .composer_intent(ComposerIntent::BeginOperation {
                thread_id: thread_id.clone(),
                draft_id: input.draft_id(),
                operation: ComposerOperationKind::Voice,
            })
            .outcome()
            != pioneer_client::core::ClientTransitionOutcome::Changed
        {
            self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
            self.publish_presentation(cx);
            return;
        }
        let operation = client
            .composer_snapshot(&thread_id)
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        let Some(plan) = client
            .composer_operation_plan(&operation)
            .filter(|plan| plan.voice_start.is_some())
        else {
            client.complete_composer_operation(operation, ComposerOperationCompletion::Cancelled);
            self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
            return;
        };
        self.native_generation = self
            .native_generation
            .checked_add(1)
            .expect("native operation generation exhausted");
        let request = ThreadAudioRequest::new(
            ThreadPresentationOperation::new(thread_id, self.mount, self.native_generation),
            plan,
        );
        self.desktop_voice_operation = Some(operation.clone());
        self.desktop_voice_request = Some(request.clone());
        let start = self.audio.start_capture(request, cx);
        self.desktop_voice_prepare_task = Some(cx.spawn(async move |view, cx| {
            let result = start.await;
            let _ = view.update(cx, |view, cx| {
                if view.desktop_voice_operation.as_ref() != Some(&operation) {
                    return;
                }
                let (candidate, release_requested) = match view.desktop_voice_composer {
                    DesktopVoiceComposerState::Preparing {
                        candidate,
                        release_requested,
                        ..
                    } => (candidate, release_requested),
                    _ => return,
                };
                match result {
                    Ok(ThreadAudioCompletion::CaptureReady)
                        if client.composer_operation_plan(&operation).is_some() =>
                    {
                        view.desktop_voice_composer =
                            DesktopVoiceComposerState::Holding { target, candidate };
                        if release_requested {
                            view.finish_desktop_voice_hold_send(cx);
                        }
                    }
                    Err(error) => {
                        client.complete_composer_operation(
                            operation.clone(),
                            ComposerOperationCompletion::Failed {
                                message: error.message().into(),
                            },
                        );
                        view.desktop_voice_composer =
                            desktop_voice_error_state_from_capture_error(error);
                    }
                    _ => view.cancel_desktop_voice_operation("capture_cancelled"),
                }
                view.publish_presentation(cx);
            });
        }));
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
                    self.publish_presentation(cx);
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
                if candidate == DesktopVoiceReleaseCandidate::Cancel {
                    self.cancel_desktop_voice_hold("desktop_release_outside_button", cx);
                } else {
                    self.publish_presentation(cx);
                }
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
        let Some(request) = self.desktop_voice_request.clone() else {
            self.cancel_desktop_voice_hold("capture_unavailable", cx);
            return;
        };
        let operation = request.plan().identity.clone();
        let client = self.client.clone();
        if client
            .composer_intent(ComposerIntent::CommitVoiceCapture {
                identity: operation.clone(),
            })
            .outcome()
            != pioneer_client::core::ClientTransitionOutcome::Changed
        {
            self.cancel_desktop_voice_hold("voice_commit_cancelled", cx);
            return;
        }
        self.desktop_voice_composer = DesktopVoiceComposerState::Finalizing {
            thread_id: operation.thread_id.clone(),
        };
        match self.audio.stop_recording(&request) {
            Ok(ThreadAudioCompletion::RecordingStopped) => {}
            Err(error) => {
                client.complete_composer_operation(
                    operation,
                    ComposerOperationCompletion::Failed {
                        message: error.message().into(),
                    },
                );
                self.audio.cancel_capture(&request);
                self.desktop_voice_composer = desktop_voice_error_state_from_capture_error(error);
                self.publish_presentation(cx);
                return;
            }
            _ => {
                self.cancel_desktop_voice_hold("capture_cancelled", cx);
                return;
            }
        }
        let files = self.files.clone();
        let endpoint_kind = self.client.connected_gateway_endpoint_kind();
        cx.emit(VoiceInputEvent::UploadError(None));
        let prepare_client = client.clone();
        let prepare_identity = operation.clone();
        let prepare = cx.background_spawn(async move {
            prepare_client.prepare_composer_voice(&prepare_identity, files.as_ref(), endpoint_kind)
        });
        self.desktop_voice_prepare_task = Some(cx.spawn(async move |view, cx| {
            let prepared = prepare.await;
            let finalization = view
                .update(cx, |view, cx| {
                    if view.desktop_voice_operation.as_ref() != Some(&operation) {
                        return None;
                    }
                    match prepared {
                        Ok(prepared) if client.composer_operation_plan(&operation).is_some() => {
                            Some(view.audio.finalize_capture(request.clone(), prepared, cx))
                        }
                        Err(error) => {
                            view.audio.cancel_capture(&request);
                            let message = format!("{error:#}");
                            cx.emit(VoiceInputEvent::UploadError(Some(message.clone())));
                            view.desktop_voice_composer = DesktopVoiceComposerState::Error {
                                kind: DesktopVoiceCaptureErrorKind::GatewaySession,
                                message: t!(
                                    "chat.composer.voice.prepare_failed",
                                    error = message.as_str()
                                )
                                .to_string(),
                            };
                            view.publish_presentation(cx);
                            None
                        }
                        _ => {
                            view.cancel_desktop_voice_operation("voice_prepare_cancelled");
                            None
                        }
                    }
                })
                .ok()
                .flatten();
            if let Some(finalization) = finalization {
                let result = finalization.await;
                let _ = view.update(cx, |view, cx| {
                    if view.desktop_voice_operation.as_ref() != Some(&operation) {
                        return;
                    }
                    if let Err(error) = result {
                        client.complete_composer_operation(
                            operation,
                            ComposerOperationCompletion::Failed {
                                message: error.message().into(),
                            },
                        );
                        view.desktop_voice_composer =
                            desktop_voice_error_state_from_capture_error(error);
                    }
                    view.publish_presentation(cx);
                });
            }
        }));
        self.publish_presentation(cx);
    }

    pub(crate) fn cancel_desktop_voice_hold(&mut self, reason: &str, cx: &mut Context<Self>) {
        self.cancel_desktop_voice_operation(reason);
        self.publish_presentation(cx);
    }

    pub(crate) fn cancel_desktop_voice_operation(&mut self, _reason: &str) {
        if let Some(request) = self.desktop_voice_request.take() {
            self.audio.cancel_capture(&request);
        }
        self.desktop_voice_prepare_task.take();
        if let Some(operation) = self.desktop_voice_operation.take() {
            self.client
                .complete_composer_operation(operation, ComposerOperationCompletion::Cancelled);
        }
        self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
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

impl VoiceInputView {
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
        kind: error.kind(),
        message: error.message().to_owned(),
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
    fn composer_voice_ready_path_uses_the_native_port_with_a_captured_plan() {
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
        assert!(start.contains("self.desktop_voice_status() != VoiceStatus::Ready"));
        assert!(start.contains("ThreadAudioRequest::new"));
        assert!(start.contains("self.audio.start_capture(request, cx)"));
        assert!(!source.contains("crate::audio::"));
    }
}

fn desktop_voice_no_speech_message(error: Option<&VoiceError>) -> String {
    let Some(details) = error.and_then(|error| desktop_voice_error_details(error.message.as_str()))
    else {
        return t!("chat.composer.voice.no_speech").to_string();
    };

    t!(
        "chat.composer.voice.no_speech_with_details",
        details = details.as_str()
    )
    .to_string()
}

fn desktop_voice_transcription_failed_message(error: Option<&VoiceError>) -> String {
    let Some(error) = error else {
        return t!("chat.composer.voice.transcription_failed").to_string();
    };

    t!(
        "chat.composer.voice.transcription_failed_with_details",
        error = error.message.as_str()
    )
    .to_string()
}

fn desktop_voice_error_details(message: &str) -> Option<String> {
    let (_, details) = message.split_once("reason=")?;
    Some(format!("reason={details}"))
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
