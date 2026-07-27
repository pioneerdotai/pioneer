use super::*;
use crate::voice::session_store::{GatewayVoiceSession, GatewayVoiceSessionState};
use crate::voice::transcription::{
    PreparedSpeechBuffer, VoiceTranscript, VoiceTranscriptionNoSpeech, VoiceTranscriptionOutcome,
    transcribe_prepared_speech_buffer,
};
use crate::voice::vad::{EnergyVoiceActivityDetector, SmoothedVoiceVad, VoiceVadConfig};
use pioneer_protocol::{
    AgentExecutionBackend, VoiceSessionOutcome, VoiceSessionResultNotification,
};

const GATEWAY_VOICE_ENERGY_VAD_THRESHOLD_FLOOR: f32 = 0.02;

impl MessageProcessor {
    pub(super) async fn voice_status(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: VoiceStatusParams,
    ) {
        let connection_id = request_context.connection_id();
        if let Some(workspace_id) = params.workspace_id.as_deref()
            && workspace_id.trim().is_empty()
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `workspace_id` must not be empty",
                        methods::VOICE_STATUS
                    ),
                ),
            )
            .await;
            return;
        }

        let settings = self
            .voice_input_supervisor
            .as_ref()
            .map(|supervisor| supervisor.settings_snapshot())
            .unwrap_or_default();
        let mut response_payload = VoiceStatusResponse {
            status: settings.runtime.phase.coarse_voice_status(),
            active_session_id: None,
            error: voice_runtime_error(&settings.runtime),
        };
        if let Some(session) = self
            .voice_sessions
            .active_session_for_connection(connection_id)
        {
            if params
                .workspace_id
                .as_deref()
                .is_none_or(|workspace_id| workspace_id == session.workspace_id)
            {
                response_payload.active_session_id = Some(session.session_id.clone());
                response_payload.status = voice_status_for_session(&session);
            }
        }

        self.send_voice_result(
            connection_id,
            request_id,
            methods::VOICE_STATUS,
            &response_payload,
        )
        .await;
    }

    pub(super) async fn voice_session_start(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: VoiceSessionStartParams,
    ) {
        let connection_id = request_context.connection_id();
        if let Err(message) = validate_voice_start_params(&params) {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: {message}",
                        methods::VOICE_SESSION_START
                    ),
                ),
            )
            .await;
            return;
        }

        let _settings_guard = self.gateway_settings_update_lock.lock().await;

        let settings = self
            .voice_input_supervisor
            .as_ref()
            .map(|supervisor| supervisor.settings_snapshot())
            .unwrap_or_default();
        let model_status = settings.runtime.phase.coarse_voice_status();
        if model_status != VoiceStatus::Ready || !settings.runtime.effective_enabled {
            self.send_voice_error(
                connection_id,
                request_id,
                INVALID_REQUEST_CODE,
                methods::VOICE_SESSION_START,
                voice_error_for_unavailable_model(
                    model_status,
                    voice_runtime_error(&settings.runtime),
                ),
            )
            .await;
            return;
        }

        if let Some(active_session) = self
            .voice_sessions
            .active_session_for_connection(connection_id)
        {
            self.send_voice_error(
                connection_id,
                request_id,
                INVALID_REQUEST_CODE,
                methods::VOICE_SESSION_START,
                VoiceError {
                    kind: VoiceErrorKind::GatewayBusy,
                    message: format!(
                        "connection already has active voice session `{}`",
                        active_session.session_id
                    ),
                },
            )
            .await;
            return;
        }

        if let Err(error) = self
            .ensure_voice_start_context_owned_by_connection(connection_id, &params.context)
            .await
        {
            self.send_voice_error(
                connection_id,
                request_id,
                INVALID_REQUEST_CODE,
                methods::VOICE_SESSION_START,
                error,
            )
            .await;
            return;
        }

        let session_id = format!("voice_{}", generate_id(21));
        let session = match self.voice_sessions.create_session(
            session_id.clone(),
            connection_id,
            params.context,
            params.audio_format,
        ) {
            Ok(session) => session,
            Err(error) => {
                self.send_voice_error(
                    connection_id,
                    request_id,
                    INVALID_REQUEST_CODE,
                    methods::VOICE_SESSION_START,
                    error.into_voice_error(),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self
            .voice_session_buffers
            .start_session(session_id.clone(), session.audio_format)
        {
            let _ = self
                .voice_sessions
                .remove_session(&session_id, connection_id);
            self.send_voice_error(
                connection_id,
                request_id,
                INVALID_REQUEST_CODE,
                methods::VOICE_SESSION_START,
                error.into_voice_error(),
            )
            .await;
            return;
        }

        if let Err(error) = self
            .voice_sessions
            .mark_recording(&session_id, connection_id)
        {
            let _ = self.voice_session_buffers.remove_session(&session_id);
            self.send_voice_error(
                connection_id,
                request_id,
                INVALID_REQUEST_CODE,
                methods::VOICE_SESSION_START,
                error.into_voice_error(),
            )
            .await;
            return;
        }

        let response_payload = VoiceSessionStartResponse {
            session_id,
            status: VoiceStatus::Recording,
        };
        self.send_voice_result(
            connection_id,
            request_id,
            methods::VOICE_SESSION_START,
            &response_payload,
        )
        .await;
    }

    pub(super) async fn voice_session_finalize(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: VoiceSessionFinalizeParams,
    ) {
        let connection_id = request_context.connection_id();
        let request_actor = request_context.persisted_actor();
        if let Err(message) = validate_voice_finalize_params(&params) {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: {message}",
                        methods::VOICE_SESSION_FINALIZE
                    ),
                ),
            )
            .await;
            return;
        }

        let pending_session = match self
            .voice_sessions
            .lookup_session(params.session_id.as_str(), connection_id)
        {
            Ok(session) => session,
            Err(error) => {
                self.send_voice_error(
                    connection_id,
                    request_id,
                    INVALID_REQUEST_CODE,
                    methods::VOICE_SESSION_FINALIZE,
                    error.into_voice_error(),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self
            .ensure_voice_context_owned_by_connection(connection_id, &params.context)
            .await
            .and_then(|_| {
                ensure_voice_finalize_context_matches_session(&pending_session, &params.context)
            })
        {
            self.send_voice_error(
                connection_id,
                request_id,
                INVALID_REQUEST_CODE,
                methods::VOICE_SESSION_FINALIZE,
                error,
            )
            .await;
            return;
        }

        if let Err(error) = self
            .voice_sessions
            .mark_finalizing(params.session_id.as_str(), connection_id)
        {
            self.send_voice_error(
                connection_id,
                request_id,
                INVALID_REQUEST_CODE,
                methods::VOICE_SESSION_FINALIZE,
                error.into_voice_error(),
            )
            .await;
            return;
        }

        let session = match self
            .voice_sessions
            .mark_transcribing(params.session_id.as_str(), connection_id)
        {
            Ok(session) => session,
            Err(error) => {
                self.send_voice_error(
                    connection_id,
                    request_id,
                    INVALID_REQUEST_CODE,
                    methods::VOICE_SESSION_FINALIZE,
                    error.into_voice_error(),
                )
                .await;
                return;
            }
        };

        self.send_voice_result(
            connection_id,
            request_id.clone(),
            methods::VOICE_SESSION_FINALIZE,
            &VoiceSessionFinalizeResponse {
                status: VoiceStatus::Transcribing,
            },
        )
        .await;

        let pipeline_outcome = self.finalize_voice_session_audio(&session).await;
        let _ = self
            .voice_sessions
            .remove_session(session.session_id.as_str(), connection_id);

        match pipeline_outcome {
            Ok(GatewayVoiceSessionPipelineOutcome::Transcript {
                transcript,
                signal_stats,
            }) => {
                let turn_params =
                    match voice_turn_start_params_from_transcript(&params.context, transcript) {
                        Ok(turn_params) => turn_params,
                        Err(no_speech) => {
                            let voice_error =
                                voice_error_for_no_speech(&no_speech, Some(signal_stats));
                            debug!(
                                connection_id,
                                session_id = %session.session_id,
                                thread_id = %session.thread_id,
                                turn_id = %session.turn_id,
                                reason = ?no_speech.reason,
                                total_samples = no_speech.total_samples,
                                signal_rms = signal_stats.rms,
                                signal_peak = signal_stats.peak,
                                non_zero_samples = signal_stats.non_zero_samples,
                                "voice session produced empty transcript; no turn will be created"
                            );
                            self.send_voice_session_result_notification(
                                connection_id,
                                VoiceSessionResultNotification {
                                    session_id: session.session_id.clone(),
                                    outcome: VoiceSessionOutcome::NoSpeech,
                                    turn_id: Some(session.turn_id.clone()),
                                    error: Some(voice_error),
                                },
                            )
                            .await;
                            return;
                        }
                    };

                let thread = match self
                    .thread_manager
                    .thread_get(turn_params.thread_id.trim())
                    .await
                {
                    Some(thread) => thread,
                    None => {
                        self.send_voice_session_result_notification(
                            connection_id,
                            VoiceSessionResultNotification {
                                session_id: session.session_id.clone(),
                                outcome: VoiceSessionOutcome::Failed,
                                turn_id: Some(session.turn_id.clone()),
                                error: Some(VoiceError {
                                    kind: VoiceErrorKind::Unknown,
                                    message: format!(
                                        "thread `{}` is not loaded",
                                        turn_params.thread_id.trim()
                                    ),
                                }),
                            },
                        )
                        .await;
                        return;
                    }
                };
                if thread.origin_kind.composer_execution_mode()
                    == pioneer_protocol::ThreadComposerExecutionMode::DetachedTask
                {
                    self.composer_detached_task_start(
                        connection_id,
                        request_id,
                        request_actor.clone(),
                        turn_params,
                        thread,
                        super::turn_handlers::TurnStartSuccessResponse::VoiceSessionFinalizeAccepted {
                            session_id: session.session_id.clone(),
                        },
                    )
                    .await;
                    return;
                }

                if let Some(backend) = turn_params.execution_backend.clone() {
                    match backend {
                        AgentExecutionBackend::ApiProvider { .. } => {}
                        AgentExecutionBackend::CLIAgentRuntime {
                            runtime_id,
                            runtime_kind,
                        } => {
                            self.turn_start_cli_runtime(
                                connection_id,
                                request_id,
                                request_actor,
                                turn_params,
                                runtime_id,
                                runtime_kind,
                                super::turn_handlers::TurnStartSuccessResponse::VoiceSessionFinalizeAccepted {
                                    session_id: session.session_id.clone(),
                                },
                            )
                            .await;
                            return;
                        }
                        AgentExecutionBackend::ACPAgentRuntime { runtime_id } => {
                            let error = VoiceError {
                                kind: VoiceErrorKind::Unknown,
                                message: format!(
                                    "ACP agent runtime `{runtime_id}` is not supported"
                                ),
                            };
                            self.send_voice_session_result_notification(
                                connection_id,
                                VoiceSessionResultNotification {
                                    session_id: session.session_id.clone(),
                                    outcome: VoiceSessionOutcome::Failed,
                                    turn_id: Some(session.turn_id.clone()),
                                    error: Some(error),
                                },
                            )
                            .await;
                            return;
                        }
                    }
                }

                let requested_reasoning_effort =
                    super::turn_handlers::requested_reasoning_effort(&turn_params);
                let prepared = match self
                    .prepare_api_provider_turn_start(
                        connection_id,
                        request_actor,
                        turn_params,
                        requested_reasoning_effort.as_deref(),
                    )
                    .await
                {
                    Ok(prepared) => prepared,
                    Err(message) => {
                        let error = VoiceError {
                            kind: VoiceErrorKind::Unknown,
                            message: format!(
                                "failed to start voice turn after transcription: {message}"
                            ),
                        };
                        self.send_voice_session_result_notification(
                            connection_id,
                            VoiceSessionResultNotification {
                                session_id: session.session_id.clone(),
                                outcome: VoiceSessionOutcome::Failed,
                                turn_id: Some(session.turn_id.clone()),
                                error: Some(error),
                            },
                        )
                        .await;
                        return;
                    }
                };

                self.finish_api_provider_turn_start_without_response(connection_id, &prepared)
                    .await;
                self.send_voice_session_result_notification(
                    connection_id,
                    VoiceSessionResultNotification {
                        session_id: session.session_id.clone(),
                        outcome: VoiceSessionOutcome::TurnStarted,
                        turn_id: Some(session.turn_id.clone()),
                        error: None,
                    },
                )
                .await;
                self.dispatch_prepared_api_provider_turn_start(prepared)
                    .await;
                return;
            }
            Ok(GatewayVoiceSessionPipelineOutcome::NoSpeech {
                no_speech,
                signal_stats,
            }) => {
                let voice_error = voice_error_for_no_speech(&no_speech, Some(signal_stats));
                debug!(
                    connection_id,
                    session_id = %session.session_id,
                    thread_id = %session.thread_id,
                    turn_id = %session.turn_id,
                    reason = ?no_speech.reason,
                    total_samples = no_speech.total_samples,
                    signal_rms = signal_stats.rms,
                    signal_peak = signal_stats.peak,
                    non_zero_samples = signal_stats.non_zero_samples,
                    "voice session finalized with no speech; no turn will be created"
                );
                self.send_voice_session_result_notification(
                    connection_id,
                    VoiceSessionResultNotification {
                        session_id: session.session_id.clone(),
                        outcome: VoiceSessionOutcome::NoSpeech,
                        turn_id: Some(session.turn_id.clone()),
                        error: Some(voice_error),
                    },
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_voice_session_result_notification(
                    connection_id,
                    VoiceSessionResultNotification {
                        session_id: session.session_id.clone(),
                        outcome: VoiceSessionOutcome::Failed,
                        turn_id: Some(session.turn_id.clone()),
                        error: Some(error),
                    },
                )
                .await;
                return;
            }
        }
    }

    pub(super) async fn voice_session_cancel(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: VoiceSessionCancelParams,
    ) {
        let connection_id = request_context.connection_id();
        if params.session_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `session_id` is required",
                        methods::VOICE_SESSION_CANCEL
                    ),
                ),
            )
            .await;
            return;
        }

        match self
            .voice_sessions
            .remove_session(params.session_id.as_str(), connection_id)
        {
            Ok(session) => {
                let _ = self
                    .voice_session_buffers
                    .remove_session(session.session_id.as_str());
                debug!(
                    connection_id,
                    session_id = %session.session_id,
                    reason = params.reason.as_deref().unwrap_or("client_cancel"),
                    "cancelled voice session"
                );
                self.send_voice_result(
                    connection_id,
                    request_id,
                    methods::VOICE_SESSION_CANCEL,
                    &VoiceSessionCancelResponse { cancelled: true },
                )
                .await;
                self.send_voice_session_result_notification(
                    connection_id,
                    VoiceSessionResultNotification {
                        session_id: session.session_id,
                        outcome: VoiceSessionOutcome::Cancelled,
                        turn_id: Some(session.turn_id),
                        error: None,
                    },
                )
                .await;
            }
            Err(error) => {
                self.send_voice_error(
                    connection_id,
                    request_id,
                    INVALID_REQUEST_CODE,
                    methods::VOICE_SESSION_CANCEL,
                    error.into_voice_error(),
                )
                .await;
            }
        }
    }

    async fn ensure_voice_start_context_owned_by_connection(
        &self,
        connection_id: ConnectionId,
        context: &pioneer_protocol::VoiceSessionStartContext,
    ) -> Result<(), VoiceError> {
        self.ensure_voice_context_scope_owned_by_connection(
            connection_id,
            context.workspace_id.as_str(),
            context.thread_id.as_str(),
        )
        .await
    }

    async fn ensure_voice_context_owned_by_connection(
        &self,
        connection_id: ConnectionId,
        context: &pioneer_protocol::VoiceTurnContext,
    ) -> Result<(), VoiceError> {
        self.ensure_voice_context_scope_owned_by_connection(
            connection_id,
            context.workspace_id.as_str(),
            context.thread_id.as_str(),
        )
        .await
    }

    async fn ensure_voice_context_scope_owned_by_connection(
        &self,
        connection_id: ConnectionId,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<(), VoiceError> {
        let Some(thread) = self.thread_manager.thread_get(thread_id).await else {
            return Err(VoiceError {
                kind: VoiceErrorKind::InvalidSession,
                message: format!(
                    "thread `{}` is not loaded for voice session start",
                    thread_id
                ),
            });
        };
        if thread.workspace_id != workspace_id {
            return Err(VoiceError {
                kind: VoiceErrorKind::InvalidSession,
                message: format!(
                    "voice context workspace `{}` does not match thread `{}` workspace `{}`",
                    workspace_id, thread_id, thread.workspace_id
                ),
            });
        }

        if let Some(connection_workspace_id) = self
            .session_manager
            .connection_workspace_id(connection_id)
            .await
            && connection_workspace_id != workspace_id
        {
            return Err(VoiceError {
                kind: VoiceErrorKind::InvalidSession,
                message: format!(
                    "connection workspace `{connection_workspace_id}` does not match voice context workspace `{}`",
                    workspace_id
                ),
            });
        }

        let subscribed = self
            .thread_manager
            .subscribed_connection_ids(thread_id)
            .await
            .contains(&connection_id);
        if !subscribed {
            return Err(VoiceError {
                kind: VoiceErrorKind::InvalidSession,
                message: format!(
                    "connection `{connection_id}` is not subscribed to thread `{}`",
                    thread_id
                ),
            });
        }

        Ok(())
    }

    async fn finalize_voice_session_audio(
        &self,
        session: &GatewayVoiceSession,
    ) -> Result<GatewayVoiceSessionPipelineOutcome, VoiceError> {
        let audio = self
            .voice_session_buffers
            .take_session_audio(session.session_id.as_str())
            .map_err(|error| error.into_voice_error())?;
        let signal_stats = VoiceSignalStats::from_samples(audio.normalized_samples.as_slice());
        debug!(
            session_id = %session.session_id,
            thread_id = %session.thread_id,
            turn_id = %session.turn_id,
            sample_rate_hz = audio.audio_format.sample_rate_hz,
            buffered_chunks = audio.chunks.len(),
            buffered_bytes = audio.buffered_bytes,
            total_samples = signal_stats.total_samples,
            signal_rms = signal_stats.rms,
            signal_peak = signal_stats.peak,
            non_zero_samples = signal_stats.non_zero_samples,
            "voice session audio signal stats"
        );
        let speech_outcome = if audio.normalized_samples.is_empty() {
            VoiceTranscriptionOutcome::NoSpeech(VoiceTranscriptionNoSpeech {
                reason: crate::voice::transcription::VoiceTranscriptionNoSpeechReason::EmptyBuffer,
                total_samples: 0,
            })
        } else {
            let detector =
                EnergyVoiceActivityDetector::new(GATEWAY_VOICE_ENERGY_VAD_THRESHOLD_FLOOR)
                    .map_err(|error| VoiceError {
                        kind: VoiceErrorKind::TranscriptionFailed,
                        message: format!("failed to initialize gateway voice VAD: {error:#}"),
                    })?;
            let mut vad =
                SmoothedVoiceVad::new(detector, VoiceVadConfig::default()).map_err(|error| {
                    VoiceError {
                        kind: VoiceErrorKind::TranscriptionFailed,
                        message: format!("failed to initialize gateway voice VAD: {error:#}"),
                    }
                })?;
            let vad_outcome = vad
                .segment_samples(audio.normalized_samples.as_slice())
                .map_err(|error| VoiceError {
                    kind: VoiceErrorKind::TranscriptionFailed,
                    message: format!("failed to segment voice audio: {error:#}"),
                })?;
            PreparedSpeechBuffer::from_vad_outcome(audio.audio_format.sample_rate_hz, vad_outcome)
        };

        let buffer = match speech_outcome {
            VoiceTranscriptionOutcome::Ready(buffer) => buffer,
            VoiceTranscriptionOutcome::NoSpeech(no_speech) => {
                return Ok(GatewayVoiceSessionPipelineOutcome::NoSpeech {
                    no_speech,
                    signal_stats,
                });
            }
        };

        let Some(supervisor) = self.voice_input_supervisor.as_ref() else {
            return Err(VoiceError {
                kind: VoiceErrorKind::ModelUnavailable,
                message: "Voice Input is not configured".to_owned(),
            });
        };
        match transcribe_prepared_speech_buffer(supervisor.as_ref(), buffer) {
            Ok(Ok(transcript)) => Ok(GatewayVoiceSessionPipelineOutcome::Transcript {
                transcript,
                signal_stats,
            }),
            Ok(Err(no_speech)) => Ok(GatewayVoiceSessionPipelineOutcome::NoSpeech {
                no_speech,
                signal_stats,
            }),
            Err(error) => Err(error.into_voice_error()),
        }
    }

    async fn send_voice_result<T>(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &'static str,
        result: &T,
    ) where
        T: Serialize,
    {
        let response = match JsonRpcResponse::from_result(request_id, result) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode `{method}` response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                method,
                error = %format!("{error:#}"),
                "failed to send voice response"
            );
        }
    }

    pub(super) async fn send_voice_session_result_notification(
        &self,
        connection_id: ConnectionId,
        notification: VoiceSessionResultNotification,
    ) {
        self.send_notification_to_connections(
            events::VOICE_SESSION_RESULT,
            &notification,
            vec![connection_id],
        )
        .await;
    }

    async fn send_voice_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        code: i64,
        method: &'static str,
        error: VoiceError,
    ) {
        let mut response = JsonRpcErrorResponse::new(
            Some(request_id),
            code,
            format!("{}: {}", voice_error_kind_code(error.kind), error.message),
        );
        response.error.data = serde_json::to_value(&error).ok();
        self.send_error(connection_id, response).await;
        warn!(
            connection_id,
            method,
            error_kind = ?error.kind,
            error = %error.message,
            "voice request failed"
        );
    }
}

fn voice_runtime_error(
    runtime: &pioneer_protocol::GatewayVoiceInputRuntimeSnapshot,
) -> Option<VoiceError> {
    runtime.error.as_ref().map(|message| VoiceError {
        kind: VoiceErrorKind::ModelUnavailable,
        message: message.clone(),
    })
}

enum GatewayVoiceSessionPipelineOutcome {
    Transcript {
        transcript: VoiceTranscript,
        signal_stats: VoiceSignalStats,
    },
    NoSpeech {
        no_speech: VoiceTranscriptionNoSpeech,
        signal_stats: VoiceSignalStats,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct VoiceSignalStats {
    total_samples: usize,
    non_zero_samples: usize,
    rms: f32,
    peak: f32,
}

impl VoiceSignalStats {
    fn from_samples(samples: &[f32]) -> Self {
        if samples.is_empty() {
            return Self {
                total_samples: 0,
                non_zero_samples: 0,
                rms: 0.0,
                peak: 0.0,
            };
        }

        let mut sum_squares = 0.0_f32;
        let mut peak = 0.0_f32;
        let mut non_zero_samples = 0_usize;
        for sample in samples {
            let abs = sample.abs();
            if abs > f32::EPSILON {
                non_zero_samples = non_zero_samples.saturating_add(1);
            }
            peak = peak.max(abs);
            sum_squares += sample * sample;
        }

        Self {
            total_samples: samples.len(),
            non_zero_samples,
            rms: (sum_squares / samples.len() as f32).sqrt(),
            peak,
        }
    }
}

fn voice_turn_start_params_from_transcript(
    context: &pioneer_protocol::VoiceTurnContext,
    transcript: VoiceTranscript,
) -> Result<TurnStartParams, VoiceTranscriptionNoSpeech> {
    let transcript_text = transcript.text.trim();
    if transcript_text.is_empty() {
        return Err(VoiceTranscriptionNoSpeech {
            reason: crate::voice::transcription::VoiceTranscriptionNoSpeechReason::EmptyTranscript,
            total_samples: transcript.diagnostics.total_samples,
        });
    }

    Ok(context
        .clone()
        .into_turn_start_params_with_transcript(transcript_text.to_owned()))
}

fn validate_voice_start_params(params: &VoiceSessionStartParams) -> Result<(), String> {
    if params.context.workspace_id.trim().is_empty() {
        return Err("`context.workspace_id` is required".to_owned());
    }
    if params.context.thread_id.trim().is_empty() {
        return Err("`context.thread_id` is required".to_owned());
    }
    if params.context.turn_id.trim().is_empty() {
        return Err("`context.turn_id` is required".to_owned());
    }
    validate_voice_streaming_audio_format(&params.audio_format)
        .map_err(|error| format!("invalid `audio_format`: {error}"))?;
    Ok(())
}

fn validate_voice_finalize_params(params: &VoiceSessionFinalizeParams) -> Result<(), String> {
    if params.session_id.trim().is_empty() {
        return Err("`session_id` is required".to_owned());
    }
    if params.context.workspace_id.trim().is_empty() {
        return Err("`context.workspace_id` is required".to_owned());
    }
    if params.context.thread_id.trim().is_empty() {
        return Err("`context.thread_id` is required".to_owned());
    }
    if params.context.turn_id.trim().is_empty() {
        return Err("`context.turn_id` is required".to_owned());
    }
    Ok(())
}

fn ensure_voice_finalize_context_matches_session(
    session: &GatewayVoiceSession,
    context: &pioneer_protocol::VoiceTurnContext,
) -> Result<(), VoiceError> {
    if session.workspace_id != context.workspace_id
        || session.thread_id != context.thread_id
        || session.turn_id != context.turn_id
    {
        return Err(VoiceError {
            kind: VoiceErrorKind::InvalidSession,
            message: format!(
                "voice finalize context does not match session `{}`",
                session.session_id
            ),
        });
    }

    Ok(())
}

fn voice_error_for_unavailable_model(status: VoiceStatus, error: Option<VoiceError>) -> VoiceError {
    if let Some(error) = error {
        return error;
    }

    let kind = if status.is_model_bootstrap() {
        VoiceErrorKind::ModelDownloading
    } else {
        VoiceErrorKind::ModelUnavailable
    };
    VoiceError {
        kind,
        message: format!("voice model is not ready; current status is {status:?}"),
    }
}

fn voice_error_for_no_speech(
    no_speech: &VoiceTranscriptionNoSpeech,
    signal_stats: Option<VoiceSignalStats>,
) -> VoiceError {
    let signal_suffix = signal_stats
        .map(|stats| {
            format!(
                ", rms={:.6}, peak={:.6}, non_zero_samples={}",
                stats.rms, stats.peak, stats.non_zero_samples
            )
        })
        .unwrap_or_default();
    VoiceError {
        kind: VoiceErrorKind::NoSpeech,
        message: format!(
            "No speech detected. Hold the microphone and try again. reason={:?}, samples={}{}",
            no_speech.reason, no_speech.total_samples, signal_suffix
        ),
    }
}

fn voice_status_for_session(session: &GatewayVoiceSession) -> VoiceStatus {
    match session.state {
        GatewayVoiceSessionState::Created | GatewayVoiceSessionState::Recording => {
            VoiceStatus::Recording
        }
        GatewayVoiceSessionState::Finalizing | GatewayVoiceSessionState::Transcribing => {
            VoiceStatus::Transcribing
        }
    }
}

fn voice_error_kind_code(kind: VoiceErrorKind) -> &'static str {
    match kind {
        VoiceErrorKind::ModelUnavailable => "model_unavailable",
        VoiceErrorKind::MicrophonePermissionBlocked => "microphone_permission_blocked",
        VoiceErrorKind::DeviceUnavailable => "device_unavailable",
        VoiceErrorKind::InvalidSession => "invalid_session",
        VoiceErrorKind::StaleChunk => "stale_chunk",
        VoiceErrorKind::SequenceGap => "sequence_gap",
        VoiceErrorKind::Cancelled => "cancelled",
        VoiceErrorKind::NoSpeech => "no_speech",
        VoiceErrorKind::TranscriptionFailed => "transcription_failed",
        VoiceErrorKind::GatewayBusy => "gateway_busy",
        VoiceErrorKind::ModelDownloading => "model_downloading",
        VoiceErrorKind::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::voice::transcription::{
        VoiceTranscriptionDiagnostics, VoiceTranscriptionNoSpeechReason, VoiceTranscriptionStrategy,
    };
    use pioneer_protocol::{
        AgentExecutionBackend, SkillId, ThreadMode, TurnCapability, TurnCapabilityKind, UserInput,
        VoiceTurnContext,
    };

    fn test_context() -> VoiceTurnContext {
        VoiceTurnContext {
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            prepared_input: vec![UserInput::LocalFile {
                path: "/tmp/prepared.txt".to_owned(),
            }],
            capabilities: vec![TurnCapability {
                id: "skill:VVVVVVVVVVVVVVVVVVVVV".to_owned(),
                kind: TurnCapabilityKind::Skill {
                    skill_id: SkillId::new("VVVVVVVVVVVVVVVVVVVVV")
                        .expect("valid voice fixture SkillId"),
                    pack_id: None,
                },
                label: Some("Demo".to_owned()),
            }],
            model: Some("model_1".to_owned()),
            model_provider: Some("provider_1".to_owned()),
            sandbox_policy: None,
            mode: Some(ThreadMode::Chat),
            execution_backend: Some(AgentExecutionBackend::ApiProvider {
                provider: "provider_1".to_owned(),
            }),
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        }
    }

    fn transcript(text: &str, total_samples: usize) -> VoiceTranscript {
        VoiceTranscript {
            text: text.to_owned(),
            diagnostics: VoiceTranscriptionDiagnostics {
                sample_rate_hz: 16_000,
                total_samples,
                speech_samples: total_samples,
                segment_count: 1,
                strategy: VoiceTranscriptionStrategy::BufferedGatewaySession,
            },
        }
    }

    #[test]
    fn voice_transcript_becomes_first_turn_input_and_preserves_context() {
        let context = test_context();
        let expected_attachment = context.prepared_input[0].clone();
        let expected_capabilities = context.capabilities.clone();
        let params = voice_turn_start_params_from_transcript(
            &context,
            transcript("  create a summary  ", 16_000),
        )
        .expect("turn params");

        assert_eq!(
            params.input[0],
            UserInput::Text {
                text: "create a summary".to_owned(),
                text_elements: Vec::new(),
            }
        );
        assert_eq!(params.input[1], expected_attachment);
        assert_eq!(params.capabilities, expected_capabilities);
        assert_eq!(params.thread_id, "thread_1");
        assert_eq!(params.turn_id, "turn_1");
        assert_eq!(params.model.as_deref(), Some("model_1"));
        assert_eq!(params.model_provider.as_deref(), Some("provider_1"));
        assert_eq!(params.mode, Some(ThreadMode::Chat));
        assert_eq!(
            params.execution_backend,
            Some(AgentExecutionBackend::ApiProvider {
                provider: "provider_1".to_owned(),
            })
        );
    }

    #[test]
    fn empty_voice_transcript_is_no_speech_before_turn_params() {
        let context = test_context();

        let error = voice_turn_start_params_from_transcript(&context, transcript("  \n\t  ", 320))
            .expect_err("blank transcript should not build turn params");

        assert_eq!(
            error.reason,
            VoiceTranscriptionNoSpeechReason::EmptyTranscript
        );
        assert_eq!(error.total_samples, 320);
    }

    #[test]
    fn voice_signal_stats_reports_peak_rms_and_non_zero_samples() {
        let stats = VoiceSignalStats::from_samples(&[0.0, 0.5, -0.25, 0.0]);

        assert_eq!(stats.total_samples, 4);
        assert_eq!(stats.non_zero_samples, 2);
        assert_eq!(stats.peak, 0.5);
        assert!((stats.rms - 0.2795085).abs() < 0.000001);
    }
}
