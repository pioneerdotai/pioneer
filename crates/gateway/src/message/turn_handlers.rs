use super::agent_runtime::TurnFailureRecoveryKind;
use super::*;
use crate::cli_runtime::config::{
    claude_account_probe_config_from_instance, codex_account_probe_config_from_instance,
};
use pioneer_cli_agent_runtime::claude::{ClaudeModelSnapshot, ClaudeProbe};
use pioneer_cli_agent_runtime::codex::{CodexModelListProbeStatus, CodexModelSnapshot, CodexProbe};
use pioneer_protocol::{
    AgentExecutionBackend, CLIAgentRuntimeKind, UserInput, VoiceError, VoiceErrorKind,
    VoiceSessionOutcome, VoiceSessionResultNotification,
};

pub(super) struct PreparedApiProviderTurnStart {
    outcome: crate::thread::TurnStartOutcome,
    workspace_skill_policies:
        HashMap<pioneer_skills::SkillPolicyKey, pioneer_agent::WorkspaceSkillPolicy>,
    resolved_artifacts: Vec<ResolvedArtifactInput>,
    runtime_environment: HashMap<String, String>,
    history: Vec<ChatMessage>,
    effective_reasoning_effort: Option<String>,
    permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
}

struct PreparedCliRuntimeNativeTurnStart {
    outcome: crate::thread::TurnStartOutcome,
    session_key: crate::cli_runtime::manager::CLIAgentRuntimeSessionKey,
    cli_session: std::sync::Arc<dyn crate::cli_runtime::manager::CLIAgentRuntimeSession>,
    native_thread_id: String,
    turn_start_params: crate::cli_runtime::manager::CLIAgentRuntimeTurnStartParams,
    request_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum TurnStartSuccessResponse {
    TurnStart,
    VoiceSessionFinalizeAccepted { session_id: String },
}

fn cli_runtime_forbidden_input_kind(input: &UserInput) -> Option<&'static str> {
    match input {
        UserInput::Text { .. } => None,
        UserInput::Image { .. }
        | UserInput::LocalImage { .. }
        | UserInput::File { .. }
        | UserInput::LocalFile { .. }
        | UserInput::Audio { .. }
        | UserInput::LocalAudio { .. }
        | UserInput::Video { .. }
        | UserInput::LocalVideo { .. }
        | UserInput::Artifact { .. } => None,
        UserInput::Mention { .. } => Some("mention"),
    }
}

fn cli_runtime_execution_disabled_message() -> String {
    "CLI agent runtime execution is disabled or no CLI runtimes are configured".to_owned()
}

impl MessageProcessor {
    pub(super) fn turn_start<'a>(
        &'a self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnStartParams,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            if params.thread_id.trim().is_empty() {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: `thread_id` is required",
                            methods::TURN_START
                        ),
                    ),
                )
                .await;
                return;
            }

            if params.turn_id.trim().is_empty() {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: `turn_id` is required",
                            methods::TURN_START
                        ),
                    ),
                )
                .await;
                return;
            }
            let requested_reasoning_effort = requested_reasoning_effort(&params);
            if let Some(effort) = requested_reasoning_effort.as_deref() {
                debug!(
                    effort,
                    turn_id = params.turn_id.as_str(),
                    thread_id = params.thread_id.as_str(),
                    "turn/start requested reasoning effort"
                );
            }
            if let Some(backend) = params.execution_backend.clone() {
                match backend {
                    AgentExecutionBackend::CLIAgentRuntime {
                        runtime_id,
                        runtime_kind,
                    } => {
                        self.turn_start_cli_runtime(
                            connection_id,
                            request_id,
                            params,
                            runtime_id,
                            runtime_kind,
                            TurnStartSuccessResponse::TurnStart,
                        )
                        .await;
                        return;
                    }
                    AgentExecutionBackend::ACPAgentRuntime { runtime_id } => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                format!("ACP agent runtime `{runtime_id}` is not supported"),
                            ),
                        )
                        .await;
                        return;
                    }
                    AgentExecutionBackend::ApiProvider { .. } => {}
                }
            }

            let prepared = match self
                .prepare_api_provider_turn_start(
                    connection_id,
                    params,
                    requested_reasoning_effort.as_deref(),
                )
                .await
            {
                Ok(prepared) => prepared,
                Err(message) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                    )
                    .await;
                    return;
                }
            };
            message_future(self.finish_turn_start_success(
                connection_id,
                request_id,
                &prepared.outcome,
            ))
            .await;
            self.dispatch_prepared_api_provider_turn_start(prepared)
                .await;
        })
    }

    pub(super) async fn prepare_api_provider_turn_start(
        &self,
        connection_id: ConnectionId,
        params: TurnStartParams,
        requested_reasoning_effort: Option<&str>,
    ) -> Result<PreparedApiProviderTurnStart, String> {
        let outcome = self
            .thread_manager
            .turn_start(connection_id, params)
            .await
            .map_err(|error| format!("failed to start turn: {error:#}"))?;
        if let Err(message) = self
            .validate_turn_reasoning_effort(
                outcome.started_notification.workspace_id.as_str(),
                ReasoningModelLookupBackend::ApiProvider {
                    provider: outcome.materialization.thread.model_provider.as_str(),
                },
                outcome.materialization.thread.model.as_str(),
                requested_reasoning_effort,
            )
            .await
        {
            self.thread_manager
                .rollback_turn_start(outcome.rollback_context.clone())
                .await;
            return Err(message);
        }

        let effective_reasoning_effort =
            requested_reasoning_effort.map(normalized_reasoning_effort_for_comparison);
        if let Err(error) = self
            .validate_artifact_user_inputs(
                outcome.started_notification.workspace_id.as_str(),
                outcome.materialization.input.as_slice(),
            )
            .await
        {
            self.thread_manager
                .rollback_turn_start(outcome.rollback_context.clone())
                .await;
            return Err(format!("failed to validate artifact input: {error:#}"));
        }

        let profile_selected_audit = match self.turn_profile_selected_audit_event(&outcome) {
            Ok(event) => event,
            Err(error) => {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;
                return Err(format!(
                    "failed to resolve turn permission profile: {error:#}"
                ));
            }
        };
        if let Err(error) = message_future(
            self.crud_store
                .materialize_turn_start_with_reasoning_effort_and_permission_audit(
                    &outcome.materialization.thread,
                    outcome.materialization.sandbox_mode,
                    &outcome.materialization.turn,
                    &outcome.materialization.input,
                    effective_reasoning_effort.as_deref(),
                    profile_selected_audit,
                ),
        )
        .await
        {
            self.thread_manager
                .rollback_turn_start(outcome.rollback_context.clone())
                .await;

            return Err(format!(
                "failed to persist turn/start state and permission audit: {error:#}"
            ));
        }

        self.ensure_hook_runtime_with_run_store().await;
        if let Err(error) = self
            .agent_manager
            .ensure_thread(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.workspace_id.as_str(),
            )
            .await
        {
            self.report_turn_failure(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                TurnFailureRecoveryKind::TurnStart,
                format!("failed to prepare agent thread runtime: {error}"),
            )
            .await;
            return Err(format!("failed to prepare agent thread runtime: {error}"));
        }

        self.ensure_agent_listener_task(outcome.started_notification.thread_id.as_str())
            .await;
        let history = self
            .load_conversation_history_for_workspace(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
            )
            .await;
        let workspace_skill_policies = match self
            .crud_store
            .list_workspace_skill_policies(outcome.started_notification.workspace_id.as_str())
            .await
        {
            Ok(records) => records
                .into_iter()
                .map(|record| {
                    (
                        pioneer_skills::SkillPolicyKey::new(record.skill_slug, record.source_kind),
                        pioneer_agent::WorkspaceSkillPolicy {
                            enabled: record.enabled,
                            allow_implicit_invocation: record.allow_implicit_invocation,
                        },
                    )
                })
                .collect::<HashMap<_, _>>(),
            Err(error) => {
                warn!(
                    workspace_id = outcome.started_notification.workspace_id,
                    error = %format!("{error:#}"),
                    "failed to load workspace skill policies; continuing with defaults"
                );
                HashMap::new()
            }
        };
        let resolved_artifacts = match self
            .resolve_provider_artifact_inputs(
                outcome.started_notification.workspace_id.as_str(),
                outcome.materialization.input.as_slice(),
            )
            .await
        {
            Ok(resolved_artifacts) => resolved_artifacts,
            Err(error) => {
                self.report_turn_failure(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    TurnFailureRecoveryKind::TurnStart,
                    format!("failed to resolve artifact input for provider: {error:#}"),
                )
                .await;
                return Err(format!(
                    "failed to resolve artifact input for provider: {error:#}"
                ));
            }
        };
        let runtime_environment = match self
            .create_artifact_output_environment(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
            )
            .await
        {
            Ok(runtime_environment) => runtime_environment.into_iter().collect::<HashMap<_, _>>(),
            Err(error) => {
                self.report_turn_failure(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    TurnFailureRecoveryKind::TurnStart,
                    format!("failed to prepare artifact output directory: {error:#}"),
                )
                .await;
                return Err(format!(
                    "failed to prepare artifact output directory: {error:#}"
                ));
            }
        };
        let hook_runtime_context = pioneer_agent::AgentTurnHookRuntimeContext::default();
        if let Err(error) = self
            .persist_turn_runtime_snapshot(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
                outcome.materialization.thread.mode,
                &hook_runtime_context,
                &outcome.materialization.thread.model,
                &outcome.materialization.thread.model_provider,
                effective_reasoning_effort.as_deref(),
                &workspace_skill_policies,
                outcome.materialization.input.as_slice(),
                outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                history.as_slice(),
            )
            .await
        {
            self.report_turn_failure(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                TurnFailureRecoveryKind::TurnStart,
                format!("failed to persist turn runtime snapshot: {error:#}"),
            )
            .await;
            return Err(format!(
                "failed to persist turn runtime snapshot: {error:#}"
            ));
        }
        let permission_profile =
            match self.materialized_turn_permission_profile(&outcome.materialization.turn) {
                Ok(permission_profile) => permission_profile,
                Err(error) => {
                    self.report_turn_failure(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        TurnFailureRecoveryKind::TurnStart,
                        format!("failed to resolve turn permission profile: {error:#}"),
                    )
                    .await;
                    return Err(format!(
                        "failed to resolve turn permission profile: {error:#}"
                    ));
                }
            };

        Ok(PreparedApiProviderTurnStart {
            outcome,
            workspace_skill_policies,
            resolved_artifacts,
            runtime_environment,
            history,
            effective_reasoning_effort,
            permission_profile,
        })
    }

    pub(super) async fn finish_api_provider_turn_start_without_response(
        &self,
        connection_id: ConnectionId,
        prepared: &PreparedApiProviderTurnStart,
    ) {
        self.session_manager
            .set_connection_workspace(
                connection_id,
                Some(prepared.outcome.started_notification.workspace_id.clone()),
            )
            .await;
        message_future(self.publish_turn_start_success(&prepared.outcome)).await;
    }

    pub(super) async fn dispatch_prepared_api_provider_turn_start(
        &self,
        prepared: PreparedApiProviderTurnStart,
    ) {
        let outcome = prepared.outcome;
        if let Err(error) = self
            .agent_manager
            .start_turn_with_resolved_artifacts_environment_reasoning_and_permission_profile(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
                outcome.materialization.thread.mode,
                &outcome.materialization.thread.model,
                &outcome.materialization.thread.model_provider,
                prepared.workspace_skill_policies,
                outcome.materialization.input.clone(),
                outcome.materialization.capabilities.clone(),
                prepared.resolved_artifacts,
                prepared.runtime_environment,
                prepared.history,
                prepared.effective_reasoning_effort.as_deref(),
                prepared.permission_profile,
            )
            .await
        {
            self.report_turn_failure(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                TurnFailureRecoveryKind::TurnDispatch,
                format!("failed to dispatch turn to agent runtime: {error}"),
            )
            .await;
        }
    }

    async fn send_cli_runtime_turn_start_failure(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        success_response: &TurnStartSuccessResponse,
        turn_id: &str,
        message: String,
    ) {
        match success_response {
            TurnStartSuccessResponse::TurnStart => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
            }
            TurnStartSuccessResponse::VoiceSessionFinalizeAccepted { session_id } => {
                let error = VoiceError {
                    kind: VoiceErrorKind::Unknown,
                    message,
                };
                warn!(
                    connection_id,
                    session_id = %session_id,
                    turn_id,
                    error = %error.message,
                    "accepted voice finalize failed while starting CLI runtime turn"
                );
                self.send_voice_session_result_notification(
                    connection_id,
                    VoiceSessionResultNotification {
                        session_id: session_id.clone(),
                        outcome: VoiceSessionOutcome::Failed,
                        turn_id: (!turn_id.trim().is_empty()).then(|| turn_id.to_owned()),
                        error: Some(error),
                    },
                )
                .await;
            }
        }
    }

    pub(super) fn turn_start_cli_runtime<'a>(
        &'a self,
        connection_id: ConnectionId,
        request_id: RequestId,
        mut params: TurnStartParams,
        runtime_id: String,
        runtime_kind: CLIAgentRuntimeKind,
        success_response: TurnStartSuccessResponse,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            let response_turn_id = params.turn_id.clone();
            macro_rules! send_turn_start_failure {
                ($message:expr) => {{
                    self.send_cli_runtime_turn_start_failure(
                        connection_id,
                        request_id.clone(),
                        &success_response,
                        response_turn_id.as_str(),
                        $message,
                    )
                    .await;
                }};
            }

            let Some(runtime_config) = self
                .validate_cli_runtime_turn_start_backend(
                    connection_id,
                    request_id.clone(),
                    runtime_id.as_str(),
                    runtime_kind,
                    &success_response,
                    response_turn_id.as_str(),
                )
                .await
            else {
                return;
            };
            params.model_provider = Some(cli_runtime_provider_key(runtime_id.as_str()));

            if !params.capabilities.is_empty() {
                send_turn_start_failure!(
                    "CLI runtime providers do not support skills, MCP capabilities, or tool attachments".to_owned()
                );
                return;
            }
            if let Some(input_kind) = params
                .input
                .iter()
                .find_map(cli_runtime_forbidden_input_kind)
            {
                send_turn_start_failure!(format!(
                    "CLI runtime providers only support text and attachment inputs; `{input_kind}` input is not supported"
                ));
                return;
            }

            let Some(thread) = self
                .thread_manager
                .thread_get(params.thread_id.trim())
                .await
            else {
                send_turn_start_failure!(format!(
                    "thread `{}` is not loaded",
                    params.thread_id.trim()
                ));
                return;
            };
            let Some(manager) = self.cli_runtime_manager.as_ref() else {
                send_turn_start_failure!(
                    "CLI runtime manager is not available for turn start".to_owned()
                );
                return;
            };
            let session_key = match crate::cli_runtime::manager::CLIAgentRuntimeSessionKey::new(
                thread.workspace_id.as_str(),
                runtime_id.as_str(),
                thread.id.as_str(),
            ) {
                Ok(session_key) => session_key,
                Err(error) => {
                    send_turn_start_failure!(format!("invalid CLI runtime session key: {error:#}"));
                    return;
                }
            };
            match self
                .cli_runtime_turn_start_blocker_for_thread(&session_key)
                .await
            {
                Ok(Some(message)) => {
                    send_turn_start_failure!(message);
                    return;
                }
                Ok(None) => {}
                Err(error) => {
                    send_turn_start_failure!(format!(
                        "failed to check active CLI runtime turns: {error:#}"
                    ));
                    return;
                }
            }
            if let Err(error) = self
                .validate_artifact_user_inputs(
                    thread.workspace_id.as_str(),
                    params.input.as_slice(),
                )
                .await
            {
                send_turn_start_failure!(format!(
                    "failed to validate CLI runtime artifact input: {error:#}"
                ));
                return;
            }
            let resolved_artifacts = match self
                .resolve_provider_artifact_inputs(
                    thread.workspace_id.as_str(),
                    params.input.as_slice(),
                )
                .await
            {
                Ok(resolved_artifacts) => resolved_artifacts,
                Err(error) => {
                    send_turn_start_failure!(format!(
                        "failed to materialize CLI runtime artifact input: {error:#}"
                    ));
                    return;
                }
            };
            let mut input_mapping = match match runtime_kind {
                CLIAgentRuntimeKind::Codex => {
                    crate::cli_runtime::input_mapping::map_codex_turn_input_from_pioneer(
                        params.input.as_slice(),
                        resolved_artifacts.as_slice(),
                    )
                }
                CLIAgentRuntimeKind::Claude => {
                    crate::cli_runtime::input_mapping::map_claude_turn_input_from_pioneer(
                        params.input.as_slice(),
                        resolved_artifacts.as_slice(),
                    )
                }
            } {
                Ok(input_mapping) => input_mapping,
                Err(error) => {
                    send_turn_start_failure!(format!("{error}"));
                    return;
                }
            };
            let permission_adapter =
                crate::cli_runtime::permissions::adapt_cli_runtime_permissions_for_turn(
                    runtime_kind,
                    params.permission_profile.as_ref(),
                    params.cli_runtime_options.take(),
                );
            debug!(
                runtime_id = runtime_id.as_str(),
                runtime_kind = cli_runtime_protocol_kind_label(runtime_kind),
                pioneer_permission_mode = permission_adapter.output.profile.mode.as_str(),
                provider_permission_mode = permission_adapter.output.provider_mode_label.as_str(),
                mapping_quality = ?permission_adapter.output.mapping_quality,
                notes = ?permission_adapter.output.notes,
                "adapted Pioneer turn permission profile for CLI runtime"
            );
            params.cli_runtime_options = Some(permission_adapter.options.clone());
            let sandbox_json = match params
                .cli_runtime_options
                .as_ref()
                .and_then(|options| options.sandbox.as_ref())
            {
                Some(sandbox) => match pioneer_crud::serialize_cli_runtime_json(&sandbox.0) {
                    Ok(sandbox_json) => Some(sandbox_json),
                    Err(error) => {
                        send_turn_start_failure!(format!(
                            "failed to serialize CLI runtime sandbox policy: {error:#}"
                        ));
                        return;
                    }
                },
                None => None,
            };
            let effective_approval_policy = permission_adapter.output.approval_policy.clone();
            let sandbox_policy_value = cli_runtime_sandbox_policy_value(&params);
            let requested_reasoning_effort = requested_reasoning_effort(&params);
            let cli_runtime_effort = cli_runtime_effort(&params);
            // Transition rule: CLI turns may carry the legacy runtime effort, the
            // top-level reasoning effort, or both when they agree. New clients use
            // the top-level field; the native runtime still receives one value.
            let effective_cli_runtime_effort = match effective_cli_runtime_effort(
                requested_reasoning_effort.as_deref(),
                cli_runtime_effort.as_deref(),
            ) {
                Ok(effort) => effort,
                Err(message) => {
                    send_turn_start_failure!(message);
                    return;
                }
            };
            let cli_runtime_personality = params
                .cli_runtime_options
                .as_ref()
                .and_then(|options| options.personality.clone());
            let cli_runtime_summary = params
                .cli_runtime_options
                .as_ref()
                .and_then(|options| options.summary.clone());

            let outcome = match self.thread_manager.turn_start(connection_id, params).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    send_turn_start_failure!(format!(
                        "failed to start CLI runtime turn: {error:#}"
                    ));
                    return;
                }
            };
            if let Err(message) = self
                .validate_turn_reasoning_effort(
                    outcome.started_notification.workspace_id.as_str(),
                    ReasoningModelLookupBackend::CliRuntime {
                        runtime_id: runtime_id.as_str(),
                        runtime_kind,
                    },
                    outcome.materialization.thread.model.as_str(),
                    effective_cli_runtime_effort.as_deref(),
                )
                .await
            {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;
                send_turn_start_failure!(message);
                return;
            }
            let profile_selected_audit = match self.turn_profile_selected_audit_event(&outcome) {
                Ok(event) => event,
                Err(error) => {
                    self.thread_manager
                        .rollback_turn_start(outcome.rollback_context.clone())
                        .await;
                    send_turn_start_failure!(format!(
                        "failed to resolve turn permission profile: {error:#}"
                    ));
                    return;
                }
            };
            if let Err(error) = message_future(
                self.crud_store
                    .materialize_turn_start_with_reasoning_effort_and_permission_audit(
                        &outcome.materialization.thread,
                        outcome.materialization.sandbox_mode,
                        &outcome.materialization.turn,
                        &outcome.materialization.input,
                        effective_cli_runtime_effort.as_deref(),
                        profile_selected_audit,
                    ),
            )
            .await
            {
                self.thread_manager
                    .rollback_turn_start(outcome.rollback_context.clone())
                    .await;

                send_turn_start_failure!(format!(
                    "failed to persist CLI runtime turn/start state and permission audit: {error:#}"
                ));
                return;
            }
            self.ensure_hook_runtime_with_run_store().await;
            let context_bundle = match self
                .compile_cli_runtime_context_bundle_for_turn(
                    runtime_id.as_str(),
                    runtime_kind,
                    &outcome,
                )
                .await
            {
                Ok(context_bundle) => context_bundle,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to compile CLI runtime context bundle: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to compile CLI runtime context bundle: {error:#}"
                    ));
                    return;
                }
            };
            crate::cli_runtime::context::prepend_cli_runtime_context_input(
                &mut input_mapping,
                &context_bundle,
                cli_runtime_context_label(runtime_kind),
            );
            let native_cwd = match crate::cli_runtime::config::current_process_cwd() {
                Ok(cwd) => cwd,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to resolve CLI runtime cwd: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to resolve CLI runtime cwd: {error:#}"
                    ));
                    return;
                }
            };
            let session_handle = match manager
                .get_or_start_with_options(
                    session_key.clone(),
                    crate::cli_runtime::manager::CLIAgentRuntimeSessionStartOptions {
                        cwd: Some(std::path::PathBuf::from(native_cwd.as_str())),
                        approval_policy: Some(effective_approval_policy.clone()),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(handle) => handle,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to start CLI runtime session: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to start CLI runtime session: {error:#}"
                    ));
                    return;
                }
            };
            let cli_session = session_handle.session();
            self.ensure_cli_runtime_session_event_pumps(
                &session_key,
                cli_session.clone(),
                runtime_config.debug_native_events,
            )
            .await;
            let native_thread =
                match crate::cli_runtime::thread_binding::open_cli_runtime_thread_binding(
                    self.crud_store.as_ref(),
                    &cli_session,
                    crate::cli_runtime::thread_binding::CLIAgentRuntimeThreadBindingOpenRequest {
                        workspace_id: outcome.started_notification.workspace_id.clone(),
                        thread_id: outcome.started_notification.thread_id.clone(),
                        runtime_id: runtime_id.clone(),
                        runtime_kind: cli_runtime_protocol_kind_label(runtime_kind).to_owned(),
                        cwd: native_cwd,
                        model: Some(outcome.materialization.thread.model.clone()),
                        approval_policy: Some(effective_approval_policy.clone()),
                        sandbox: Some(serde_json::json!(cli_runtime_thread_sandbox_label(
                            sandbox_policy_value.as_ref()
                        ))),
                        service_tier: None,
                        resume_existing: cli_runtime_supports_durable_thread_resume(runtime_kind),
                        request_timeout: std::time::Duration::from_millis(
                            runtime_config.request_timeout_ms,
                        ),
                        opened_at: chrono::Utc::now().fixed_offset(),
                    },
                )
                .await
                {
                    Ok(opened) => opened,
                    Err(error) => {
                        self.mark_turn_blocked(
                            outcome.started_notification.thread_id.clone(),
                            outcome.started_notification.turn.id.clone(),
                            format!("failed to open CLI runtime thread: {error:#}"),
                        )
                        .await;
                        send_turn_start_failure!(format!(
                            "failed to open CLI runtime thread: {error:#}"
                        ));
                        return;
                    }
                };
            let input_mapping_json = match pioneer_crud::serialize_cli_runtime_json(&input_mapping)
            {
                Ok(input_mapping_json) => input_mapping_json,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to serialize CLI runtime input mapping: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to serialize CLI runtime input mapping: {error:#}"
                    ));
                    return;
                }
            };
            if let Err(error) = self
                .persist_cli_runtime_input_mapping_if_thread_bound(
                    runtime_id.as_str(),
                    runtime_kind,
                    input_mapping_json,
                    sandbox_json,
                    Some(effective_approval_policy.clone()),
                    &outcome,
                )
                .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to persist CLI runtime input mapping: {error:#}"),
                )
                .await;
                send_turn_start_failure!(format!(
                    "failed to persist CLI runtime input mapping: {error:#}"
                ));
                return;
            }
            let native_turn_input = match serde_json::to_value(&input_mapping.input) {
                Ok(input) => input,
                Err(error) => {
                    self.mark_turn_blocked(
                        outcome.started_notification.thread_id.clone(),
                        outcome.started_notification.turn.id.clone(),
                        format!("failed to encode CLI runtime turn input: {error:#}"),
                    )
                    .await;
                    send_turn_start_failure!(format!(
                        "failed to encode CLI runtime turn input: {error:#}"
                    ));
                    return;
                }
            };
            if let Err(error) = self
                .persist_cli_runtime_prompt_manifest(
                    outcome.started_notification.thread_id.as_str(),
                    outcome.started_notification.turn.id.as_str(),
                    &context_bundle,
                )
                .await
            {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to persist CLI runtime prompt manifest: {error:#}"),
                )
                .await;
                send_turn_start_failure!(format!(
                    "failed to persist CLI runtime prompt manifest: {error:#}"
                ));
                return;
            }
            let native_thread_id = native_thread.binding.native_thread_id;
            let native_turn_start_params =
                crate::cli_runtime::manager::CLIAgentRuntimeTurnStartParams {
                    native_thread_id: native_thread_id.clone(),
                    input: native_turn_input,
                    cwd: native_thread.binding.native_cwd,
                    approval_policy: Some(effective_approval_policy),
                    sandbox: sandbox_policy_value,
                    model: Some(outcome.materialization.thread.model.clone()),
                    effort: effective_cli_runtime_effort,
                    personality: cli_runtime_personality,
                    summary: cli_runtime_summary,
                };
            let native_turn_start = PreparedCliRuntimeNativeTurnStart {
                outcome,
                session_key,
                cli_session,
                native_thread_id,
                turn_start_params: native_turn_start_params,
                request_timeout_ms: runtime_config.request_timeout_ms,
            };

            let success_sent = match success_response {
                TurnStartSuccessResponse::TurnStart => {
                    message_future(self.finish_turn_start_success(
                        connection_id,
                        request_id,
                        &native_turn_start.outcome,
                    ))
                    .await
                }
                TurnStartSuccessResponse::VoiceSessionFinalizeAccepted { session_id } => {
                    message_future(
                        self.finish_voice_session_finalize_accepted_turn_start_success(
                            connection_id,
                            &native_turn_start.outcome,
                            &session_id,
                        ),
                    )
                    .await
                }
            };
            if !success_sent {
                return;
            }
            message_future(self.start_prepared_cli_runtime_native_turn(native_turn_start)).await;
        })
    }

    async fn start_prepared_cli_runtime_native_turn(
        &self,
        prepared: PreparedCliRuntimeNativeTurnStart,
    ) {
        let PreparedCliRuntimeNativeTurnStart {
            outcome,
            session_key,
            cli_session,
            native_thread_id,
            turn_start_params,
            request_timeout_ms,
        } = prepared;
        let native_turn = match cli_session
            .start_turn(
                turn_start_params,
                std::time::Duration::from_millis(request_timeout_ms),
            )
            .await
        {
            Ok(native_turn) => native_turn,
            Err(error) => {
                self.mark_turn_blocked(
                    outcome.started_notification.thread_id.clone(),
                    outcome.started_notification.turn.id.clone(),
                    format!("failed to start CLI runtime turn: {error:#}"),
                )
                .await;
                return;
            }
        };
        let native_turn_id = native_turn.native_turn_id.clone();
        if let Err(error) =
            crate::cli_runtime::turn_binding::persist_cli_runtime_turn_binding_after_native_start(
                self.crud_store.as_ref(),
                crate::cli_runtime::turn_binding::CLIAgentRuntimeNativeTurnStarted {
                    turn_id: outcome.started_notification.turn.id.clone(),
                    native_turn_id: native_turn_id.clone(),
                    request_id: None,
                    started_at: chrono::Utc::now().fixed_offset(),
                },
            )
            .await
        {
            self.mark_turn_blocked(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                format!("failed to persist CLI runtime native turn id: {error:#}"),
            )
            .await;
            return;
        }
        self.flush_cli_runtime_events_for_native_turn(
            &session_key,
            native_thread_id.as_str(),
            native_turn_id.as_str(),
        )
        .await;
    }

    async fn validate_cli_runtime_turn_start_backend(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
        success_response: &TurnStartSuccessResponse,
        turn_id: &str,
    ) -> Option<pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig> {
        if self.cli_runtime_manager.is_none() {
            self.send_cli_runtime_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                turn_id,
                cli_runtime_execution_disabled_message(),
            )
            .await;
            return None;
        }

        let runtimes = match self.load_cli_runtime_instances() {
            Ok(runtimes) => runtimes,
            Err(error) => {
                self.send_cli_runtime_turn_start_failure(
                    connection_id,
                    request_id,
                    success_response,
                    turn_id,
                    format!("failed to load CLI runtime config: {error:#}"),
                )
                .await;
                return None;
            }
        };
        if runtimes.is_empty() {
            self.send_cli_runtime_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                turn_id,
                cli_runtime_execution_disabled_message(),
            )
            .await;
            return None;
        }

        let Some(runtime) = runtimes
            .into_iter()
            .find(|runtime| runtime.id == runtime_id)
        else {
            self.send_cli_runtime_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                turn_id,
                format!("unknown CLI runtime `{runtime_id}`"),
            )
            .await;
            return None;
        };
        if !runtime.enabled {
            self.send_cli_runtime_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                turn_id,
                format!("CLI runtime `{runtime_id}` is disabled"),
            )
            .await;
            return None;
        }
        if !cli_runtime_kind_matches_config(runtime_kind, runtime.kind) {
            self.send_cli_runtime_turn_start_failure(
                connection_id,
                request_id,
                success_response,
                turn_id,
                format!(
                    "CLI runtime `{runtime_id}` is configured as `{}` but request asked for `{}`",
                    cli_runtime_config_kind_label(runtime.kind),
                    cli_runtime_protocol_kind_label(runtime_kind)
                ),
            )
            .await;
            return None;
        }

        Some(runtime)
    }

    pub(super) async fn cli_runtime_turn_start_blocker_for_thread(
        &self,
        key: &crate::cli_runtime::manager::CLIAgentRuntimeSessionKey,
    ) -> anyhow::Result<Option<String>> {
        let bindings = self
            .crud_store
            .list_cli_runtime_turn_bindings_for_thread(key.thread_id.as_str())
            .await?;
        for binding in bindings.into_iter().rev() {
            if binding.workspace_id != key.workspace_id || binding.runtime_id != key.runtime_id {
                continue;
            }
            if binding.status != crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_STARTING
                && binding.status
                    != crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
            {
                continue;
            }

            let turn_status = if let Some((_workspace_id, turn)) = self
                .thread_manager
                .turn_get(binding.thread_id.as_str(), binding.turn_id.as_str())
                .await
            {
                Some(turn.status)
            } else {
                self.crud_store
                    .get_turn(binding.thread_id.as_str(), binding.turn_id.as_str())
                    .await?
                    .map(|(_workspace_id, turn)| turn.status)
            };
            let Some(turn_status) = turn_status else {
                continue;
            };
            if turn_status != TurnStatus::InProgress {
                self.cleanup_cli_runtime_terminal_turn_status(
                    &binding,
                    turn_status,
                    "CLI runtime turn start blocker",
                )
                .await;
                continue;
            }

            return Ok(Some(format!(
                "CLI runtime thread `{}` already has active turn `{}`; wait for it to finish or cancel it before starting another CLI runtime turn",
                key.thread_id, binding.turn_id
            )));
        }

        let mut pending_native_turn_ids = self
            .cli_runtime_pending_turn_events
            .lock()
            .await
            .keys()
            .filter(|pending| {
                pending.workspace_id == key.workspace_id
                    && pending.runtime_id == key.runtime_id
                    && pending.thread_id == key.thread_id
            })
            .map(|pending| pending.native_turn_id.clone())
            .collect::<Vec<_>>();
        pending_native_turn_ids.extend(
            self.cli_runtime_pending_turn_server_requests
                .lock()
                .await
                .keys()
                .filter(|pending| {
                    pending.workspace_id == key.workspace_id
                        && pending.runtime_id == key.runtime_id
                        && pending.thread_id == key.thread_id
                })
                .map(|pending| pending.native_turn_id.clone()),
        );
        pending_native_turn_ids.sort();
        pending_native_turn_ids.dedup();
        pending_native_turn_ids.truncate(3);
        if !pending_native_turn_ids.is_empty() {
            return Ok(Some(format!(
                "CLI runtime thread `{}` has unbound native turn activity for `{}`; wait for the native turn to finish before starting another CLI runtime turn",
                key.thread_id,
                pending_native_turn_ids.join(", ")
            )));
        }

        Ok(None)
    }

    pub(super) fn materialized_turn_permission_profile(
        &self,
        turn: &pioneer_protocol::Turn,
    ) -> anyhow::Result<pioneer_protocol::TurnPermissionProfileSnapshot> {
        Ok(turn.permission_profile.clone())
    }

    pub(super) fn turn_profile_selected_audit_event(
        &self,
        outcome: &crate::thread::TurnStartOutcome,
    ) -> anyhow::Result<pioneer_protocol::TurnPermissionAuditEvent> {
        let permission_profile =
            self.materialized_turn_permission_profile(&outcome.materialization.turn)?;
        Ok(self.turn_profile_selected_audit_event_for_turn(
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            permission_profile,
        ))
    }

    pub(super) fn turn_profile_selected_audit_event_for_turn(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot,
    ) -> pioneer_protocol::TurnPermissionAuditEvent {
        pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            event_kind: pioneer_protocol::TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: permission_profile.mode,
            profile_source: permission_profile.source,
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        }
    }

    async fn compile_cli_runtime_context_bundle_for_turn(
        &self,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
        outcome: &crate::thread::TurnStartOutcome,
    ) -> anyhow::Result<pioneer_promt::CompiledPromptBundle> {
        let native_cwd = self
            .crud_store
            .get_cli_runtime_thread_binding(outcome.started_notification.thread_id.as_str())
            .await?
            .and_then(|binding| binding.native_cwd);
        let history = self
            .load_conversation_history_for_workspace(
                outcome.started_notification.workspace_id.as_str(),
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
            )
            .await;
        let permission_profile =
            self.materialized_turn_permission_profile(&outcome.materialization.turn)?;
        crate::cli_runtime::context::compile_cli_runtime_context_bundle(
            self.artifact_runtime_home.as_path(),
            crate::cli_runtime::context::CLIRuntimeContextBuildInput {
                workspace_id: outcome.started_notification.workspace_id.as_str(),
                thread_id: outcome.started_notification.thread_id.as_str(),
                turn_id: outcome.started_notification.turn.id.as_str(),
                runtime_id,
                runtime_label: cli_runtime_context_label(runtime_kind),
                model: Some(outcome.materialization.thread.model.as_str()),
                cwd: native_cwd.as_deref(),
                permission_profile,
                history: history.as_slice(),
            },
        )
    }

    async fn persist_cli_runtime_prompt_manifest(
        &self,
        thread_id: &str,
        turn_id: &str,
        bundle: &pioneer_promt::CompiledPromptBundle,
    ) -> anyhow::Result<()> {
        let manifest = crate::cli_runtime::context::cli_runtime_prompt_manifest_from_bundle(bundle);
        self.thread_manager
            .set_turn_prompt_manifest(thread_id, turn_id, manifest.clone())
            .await;
        self.crud_store
            .update_turn_prompt_manifest(thread_id, turn_id, &manifest, now_timestamp_secs())
            .await
            .with_context(|| {
                format!("failed to update prompt manifest for CLI runtime turn `{turn_id}`")
            })?;
        Ok(())
    }

    async fn persist_cli_runtime_input_mapping_if_thread_bound(
        &self,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
        input_mapping_json: String,
        sandbox_json: Option<String>,
        approval_policy: Option<String>,
        outcome: &crate::thread::TurnStartOutcome,
    ) -> anyhow::Result<()> {
        let Some(thread_binding) = self
            .crud_store
            .get_cli_runtime_thread_binding(outcome.started_notification.thread_id.as_str())
            .await?
        else {
            return Ok(());
        };
        let created_at = cli_runtime_binding_timestamp();

        crate::cli_runtime::turn_binding::persist_cli_runtime_turn_binding_before_native_start(
            self.crud_store.as_ref(),
            crate::cli_runtime::turn_binding::CLIAgentRuntimeTurnBindingStartRequest {
                workspace_id: outcome.started_notification.workspace_id.clone(),
                thread_id: outcome.started_notification.thread_id.clone(),
                turn_id: outcome.started_notification.turn.id.clone(),
                runtime_id: runtime_id.to_owned(),
                runtime_kind: cli_runtime_protocol_kind_label(runtime_kind).to_owned(),
                native_thread_id: thread_binding.native_thread_id,
                request_id: None,
                model: Some(outcome.materialization.thread.model.clone()),
                cwd: thread_binding.native_cwd,
                sandbox_json,
                approval_policy,
                input_mapping_json,
                created_at,
            },
        )
        .await?;
        Ok(())
    }

    async fn finish_turn_start_success(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        outcome: &crate::thread::TurnStartOutcome,
    ) -> bool {
        self.session_manager
            .set_connection_workspace(
                connection_id,
                Some(outcome.started_notification.workspace_id.clone()),
            )
            .await;
        let response = match JsonRpcResponse::from_result(request_id, &outcome.response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return false;
            }
        };
        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/start response"
            );
            return false;
        }
        message_future(self.publish_turn_start_success(outcome)).await
    }

    async fn finish_voice_session_finalize_accepted_turn_start_success(
        &self,
        connection_id: ConnectionId,
        outcome: &crate::thread::TurnStartOutcome,
        session_id: &str,
    ) -> bool {
        self.session_manager
            .set_connection_workspace(
                connection_id,
                Some(outcome.started_notification.workspace_id.clone()),
            )
            .await;
        if !message_future(self.publish_turn_start_success(outcome)).await {
            self.send_voice_session_result_notification(
                connection_id,
                VoiceSessionResultNotification {
                    session_id: session_id.to_owned(),
                    outcome: VoiceSessionOutcome::Failed,
                    turn_id: Some(outcome.started_notification.turn.id.clone()),
                    error: Some(VoiceError {
                        kind: VoiceErrorKind::Unknown,
                        message: "failed to publish CLI runtime turn start".to_owned(),
                    }),
                },
            )
            .await;
            return false;
        }
        self.send_voice_session_result_notification(
            connection_id,
            VoiceSessionResultNotification {
                session_id: session_id.to_owned(),
                outcome: VoiceSessionOutcome::TurnStarted,
                turn_id: Some(outcome.started_notification.turn.id.clone()),
                error: None,
            },
        )
        .await;
        true
    }

    async fn publish_turn_start_success(&self, outcome: &crate::thread::TurnStartOutcome) -> bool {
        let notification = match JsonRpcNotification::from_params(
            events::TURN_STARTED,
            &outcome.started_notification,
        ) {
            Ok(notification) => notification,
            Err(error) => {
                error!(error = %error, "failed to encode turn/started notification");
                return false;
            }
        };
        match serde_json::to_string(&notification) {
            Ok(payload) => {
                for notification_connection_id in
                    outcome.started_notification_connection_ids.iter().copied()
                {
                    if let Err(error) = self
                        .session_manager
                        .send_text(notification_connection_id, payload.clone())
                        .await
                    {
                        warn!(
                            connection_id = notification_connection_id,
                            error = %format!("{error:#}"),
                            "failed to send turn/started notification"
                        );
                    }
                }
            }
            Err(error) => {
                error!(error = %error, "failed to serialize turn/started notification");
            }
        }
        message_future(self.emit_user_message_item_lifecycle(
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            outcome.materialization.input.as_slice(),
            outcome.materialization.capabilities.as_slice(),
        ))
        .await;

        // Spawn background title generation on first turn (fire-and-forget) only for user-origin threads.
        if outcome.materialization.thread.name.is_none()
            && outcome.materialization.thread.origin_kind
                == pioneer_protocol::ThreadOriginKind::User
        {
            self.spawn_initial_thread_title_task(
                outcome.started_notification.thread_id.clone(),
                first_user_text(outcome.materialization.input.as_slice()),
            );
        }

        true
    }

    pub(super) async fn turn_cancel(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnCancelParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_CANCEL
                    ),
                ),
            )
            .await;
            return;
        }

        let thread_id = params.thread_id.trim().to_owned();
        let turn_id = params.turn_id.trim().to_owned();
        let reason = params
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("turn cancelled by user")
            .to_owned();

        let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("turn `{turn_id}` not found in thread `{thread_id}`"),
                ),
            )
            .await;
            return;
        };

        let subscribed = self
            .thread_manager
            .subscribed_connection_ids(thread_id.as_str())
            .await
            .contains(&connection_id);
        if !subscribed {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!(
                        "connection `{connection_id}` is not subscribed to thread `{thread_id}`"
                    ),
                ),
            )
            .await;
            return;
        }

        if turn.status != TurnStatus::InProgress {
            self.send_turn_cancel_response(
                connection_id,
                request_id,
                TurnCancelResponse {
                    thread_id,
                    workspace_id,
                    turn,
                },
            )
            .await;
            return;
        }

        let cli_turn_binding = match self
            .crud_store
            .get_cli_runtime_turn_binding(turn_id.as_str())
            .await
        {
            Ok(binding) => binding,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load CLI runtime turn binding: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if let Some(cli_turn_binding) =
            cli_turn_binding.filter(|binding| binding.thread_id == thread_id)
        {
            if !self
                .mark_turn_interrupted(thread_id.clone(), turn_id.clone(), reason.clone())
                .await
            {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to interrupt turn `{turn_id}` in thread `{thread_id}`"),
                    ),
                )
                .await;
                return;
            }
            self.ensure_cli_runtime_turn_interrupted_cleanup(
                &cli_turn_binding,
                Some(reason.as_str()),
            )
            .await;

            let Some((workspace_id, turn)) = self
                .thread_manager
                .turn_get(thread_id.as_str(), turn_id.as_str())
                .await
            else {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("turn `{turn_id}` disappeared after cancellation"),
                    ),
                )
                .await;
                return;
            };

            self.send_turn_cancel_response(
                connection_id,
                request_id,
                TurnCancelResponse {
                    thread_id,
                    workspace_id,
                    turn,
                },
            )
            .await;
            return;
        }

        match self
            .agent_manager
            .cancel_turn(thread_id.as_str(), turn_id.as_str(), reason.as_str())
            .await
        {
            Ok(()) => {}
            Err(pioneer_agent::AgentControlError::ThreadNotFound)
            | Err(pioneer_agent::AgentControlError::NoActiveTurn) => {
                warn!(
                    thread_id,
                    turn_id,
                    "agent runtime had no active turn during turn/cancel; terminalizing in gateway"
                );
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to cancel turn: {error}"),
                    ),
                )
                .await;
                return;
            }
        }

        if !self
            .mark_turn_interrupted(thread_id.clone(), turn_id.clone(), reason)
            .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("failed to interrupt turn `{turn_id}` in thread `{thread_id}`"),
                ),
            )
            .await;
            return;
        }

        let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("turn `{turn_id}` disappeared after cancellation"),
                ),
            )
            .await;
            return;
        };

        self.send_turn_cancel_response(
            connection_id,
            request_id,
            TurnCancelResponse {
                thread_id,
                workspace_id,
                turn,
            },
        )
        .await;
    }

    async fn send_turn_cancel_response(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        response_payload: TurnCancelResponse,
    ) {
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/cancel response"
            );
        }
    }

    pub(super) async fn turn_resume(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnResumeParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_RESUME
                    ),
                ),
            )
            .await;
            return;
        }

        let thread_id = params.thread_id.trim().to_owned();
        let turn_id = params.turn_id.trim().to_owned();
        let recovery_job_id = params
            .recovery_job_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);

        let subscribed = self
            .thread_manager
            .subscribed_connection_ids(thread_id.as_str())
            .await
            .contains(&connection_id);
        if !subscribed {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!(
                        "connection `{connection_id}` is not subscribed to thread `{thread_id}`"
                    ),
                ),
            )
            .await;
            return;
        }

        let Some((workspace_id, turn)) = (match self
            .crud_store
            .get_turn(thread_id.as_str(), turn_id.as_str())
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to fetch turn before resume: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        }) else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("turn `{turn_id}` not found in thread `{thread_id}`"),
                ),
            )
            .await;
            return;
        };

        if turn.status != TurnStatus::Blocked {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("turn `{turn_id}` is not blocked and cannot be resumed"),
                ),
            )
            .await;
            return;
        }

        let now_unix = now_timestamp_secs();
        let resumed_job = match self
            .recovery_coordinator
            .resume_blocked_turn(
                thread_id.as_str(),
                turn_id.as_str(),
                recovery_job_id.as_deref(),
                now_unix,
            )
            .await
        {
            Ok(Some(job)) => job,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("turn `{turn_id}` has no blocked recovery job to resume"),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to resume turn `{turn_id}`: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        match self.recovery_coordinator.run_ready_jobs(now_unix, 16).await {
            Ok(events) => {
                for event in events {
                    self.handle_recovery_event(event, now_unix).await;
                }
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "turn `{turn_id}` was resumed but recovery start failed: {error:#}"
                        ),
                    ),
                )
                .await;
                return;
            }
        }

        let turn = match self
            .crud_store
            .get_turn(thread_id.as_str(), turn_id.as_str())
            .await
        {
            Ok(Some((_workspace_id, turn))) => turn,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("turn `{turn_id}` disappeared after resume"),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to fetch turn after resume: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let response = match JsonRpcResponse::from_result(
            request_id,
            &TurnResumeResponse {
                thread_id,
                workspace_id,
                turn,
                recovery_job_id: resumed_job.id,
            },
        ) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/resume response"
            );
        }
    }

    pub(super) async fn turn_get(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnGetParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_GET
                    ),
                ),
            )
            .await;
            return;
        }

        let result = if let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(params.thread_id.as_str(), params.turn_id.as_str())
            .await
        {
            Some(TurnGetResponse {
                thread_id: params.thread_id.clone(),
                workspace_id,
                turn,
            })
        } else {
            match self
                .crud_store
                .get_turn(params.thread_id.as_str(), params.turn_id.as_str())
                .await
            {
                Ok(Some((workspace_id, turn))) => Some(TurnGetResponse {
                    thread_id: params.thread_id.clone(),
                    workspace_id,
                    turn,
                }),
                Ok(None) => None,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to fetch turn: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        };

        let Some(result) = result else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!(
                        "turn `{}` in thread `{}` was not found",
                        params.turn_id, params.thread_id
                    ),
                ),
            )
            .await;
            return;
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(result.workspace_id.clone()))
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &result) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/get response"
            );
        }
    }

    pub(super) async fn turn_items(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnItemsParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_ITEMS
                    ),
                ),
            )
            .await;
            return;
        }

        let mut result = match self
            .crud_store
            .get_turn_item_events(params.thread_id.as_str(), params.turn_id.as_str())
            .await
        {
            Ok(Some(value)) => value,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "turn `{}` in thread `{}` was not found",
                            params.turn_id, params.thread_id
                        ),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to fetch turn items: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        Self::enrich_turn_item_events_markdown(result.events.as_mut_slice());

        self.session_manager
            .set_connection_workspace(connection_id, Some(result.workspace_id.clone()))
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &result) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/items response"
            );
        }
    }

    async fn lookup_reasoning_model_capabilities(
        &self,
        workspace_id: &str,
        backend: ReasoningModelLookupBackend<'_>,
        model_id: &str,
    ) -> Option<ProviderModelInfo> {
        let model_id = model_id.trim();
        if model_id.is_empty() {
            debug!(
                workspace_id,
                "skipping reasoning model capability lookup because model id is empty"
            );
            return None;
        }

        let model = match backend {
            ReasoningModelLookupBackend::ApiProvider { provider } => {
                self.lookup_api_provider_model_for_reasoning(workspace_id, provider, model_id)
                    .await
            }
            ReasoningModelLookupBackend::CliRuntime {
                runtime_id,
                runtime_kind,
            } => {
                self.lookup_cli_runtime_model_for_reasoning(
                    workspace_id,
                    runtime_id,
                    runtime_kind,
                    model_id,
                )
                .await
            }
        };

        match model
            .as_ref()
            .and_then(|model| model.capabilities.reasoning.as_ref())
        {
            Some(reasoning) => {
                debug!(
                    workspace_id,
                    model_id,
                    supported = ?reasoning.supported,
                    efforts = ?reasoning.effort_options,
                    source = reasoning_capability_source_label(reasoning.source),
                    "resolved reasoning model capability metadata"
                );
            }
            None if model.is_some() => {
                debug!(
                    workspace_id,
                    model_id,
                    source = reasoning_capability_source_label(None),
                    "resolved model but reasoning capability metadata is missing"
                );
            }
            None => {
                debug!(
                    workspace_id,
                    model_id,
                    source = reasoning_capability_source_label(None),
                    "reasoning model capability metadata is unavailable"
                );
            }
        }

        model
    }

    async fn validate_turn_reasoning_effort(
        &self,
        workspace_id: &str,
        backend: ReasoningModelLookupBackend<'_>,
        model_id: &str,
        effort: Option<&str>,
    ) -> Result<(), String> {
        let Some(effort) = effort.map(str::trim).filter(|effort| !effort.is_empty()) else {
            return Ok(());
        };
        let backend_label = reasoning_model_lookup_backend_label(backend);
        let model = self
            .lookup_reasoning_model_capabilities(workspace_id, backend, model_id)
            .await;

        let result = validate_reasoning_effort_for_model(
            backend_label.as_str(),
            model_id,
            effort,
            model.as_ref(),
        );
        let capability_source = reasoning_capability_source_for_model(model.as_ref());
        let supported_efforts = reasoning_effort_options_for_model(model.as_ref());
        match &result {
            Ok(()) => {
                debug!(
                    workspace_id,
                    backend = backend_label.as_str(),
                    model_id,
                    effort,
                    capability_source,
                    supported_efforts = ?supported_efforts,
                    "accepted reasoning effort selection"
                );
            }
            Err(message) => {
                debug!(
                    workspace_id,
                    backend = backend_label.as_str(),
                    model_id,
                    effort,
                    capability_source,
                    supported_efforts = ?supported_efforts,
                    error = message.as_str(),
                    "rejected reasoning effort selection"
                );
            }
        }
        result
    }

    async fn lookup_api_provider_model_for_reasoning(
        &self,
        workspace_id: &str,
        provider_id: &str,
        model_id: &str,
    ) -> Option<ProviderModelInfo> {
        let provider = match self
            .provider_registry
            .get_or_create_for_workspace(workspace_id, provider_id)
        {
            Ok(provider) => provider,
            Err(error) => {
                debug!(
                    workspace_id,
                    provider = provider_id,
                    model_id,
                    error = %format!("{error:#}"),
                    "failed to create provider for reasoning capability lookup"
                );
                return None;
            }
        };

        match provider.list_models().await {
            Ok(models) => models
                .into_iter()
                .find(|model| model.id == model_id)
                .or_else(|| {
                    debug!(
                        workspace_id,
                        provider = provider_id,
                        model_id,
                        "provider model list did not contain selected model for reasoning lookup"
                    );
                    None
                }),
            Err(error) => {
                debug!(
                    workspace_id,
                    provider = provider_id,
                    model_id,
                    error = %format!("{error:#}"),
                    "failed to list provider models for reasoning capability lookup"
                );
                None
            }
        }
    }

    async fn lookup_cli_runtime_model_for_reasoning(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
        model_id: &str,
    ) -> Option<ProviderModelInfo> {
        let instances = match self.load_cli_runtime_instances() {
            Ok(instances) => instances,
            Err(error) => {
                debug!(
                    workspace_id,
                    runtime_id,
                    model_id,
                    error = %format!("{error:#}"),
                    "failed to load CLI runtime config for reasoning capability lookup"
                );
                return None;
            }
        };

        let Some(instance) = instances
            .into_iter()
            .find(|instance| instance.id == runtime_id)
        else {
            debug!(
                workspace_id,
                runtime_id, model_id, "CLI runtime not found for reasoning capability lookup"
            );
            return None;
        };

        if !cli_runtime_kind_matches_config(runtime_kind, instance.kind) {
            debug!(
                workspace_id,
                runtime_id,
                requested_kind = cli_runtime_protocol_kind_label(runtime_kind),
                configured_kind = cli_runtime_config_kind_label(instance.kind),
                model_id,
                "CLI runtime kind mismatch for reasoning capability lookup"
            );
            return None;
        }

        let mut models = if instance.enabled {
            match instance.kind {
                pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex => {
                    let probe =
                        CodexProbe::model_list(codex_account_probe_config_from_instance(&instance))
                            .await;
                    if probe.status == CodexModelListProbeStatus::Ready {
                        probe
                            .models
                            .into_iter()
                            .map(runtime_model_from_codex_snapshot_for_reasoning_lookup)
                            .collect::<Vec<_>>()
                    } else {
                        debug!(
                            workspace_id,
                            runtime_id,
                            model_id,
                            status = ?probe.status,
                            "Codex CLI model metadata is unavailable for reasoning lookup"
                        );
                        Vec::new()
                    }
                }
                pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude => {
                    let probe = ClaudeProbe::model_list(
                        claude_account_probe_config_from_instance(&instance),
                        &instance.custom_models,
                    )
                    .await;
                    if let Some(error_message) = probe.error_message.as_deref() {
                        debug!(
                            workspace_id,
                            runtime_id,
                            model_id,
                            error = error_message,
                            "Claude CLI model metadata returned diagnostics for reasoning lookup"
                        );
                    }
                    probe
                        .models
                        .into_iter()
                        .map(runtime_model_from_claude_snapshot_for_reasoning_lookup)
                        .collect::<Vec<_>>()
                }
            }
        } else {
            debug!(
                workspace_id,
                runtime_id, model_id, "CLI runtime is disabled for reasoning capability lookup"
            );
            Vec::new()
        };
        append_cli_runtime_custom_models_for_reasoning_lookup(&mut models, &instance.custom_models);

        models
            .into_iter()
            .find(|model| model.id == model_id)
            .map(|model| {
                provider_model_from_runtime_model_for_reasoning_lookup(
                    cli_runtime_provider_key(runtime_id).as_str(),
                    model,
                )
            })
            .or_else(|| {
                debug!(
                    workspace_id,
                    runtime_id,
                    model_id,
                    "CLI runtime model list did not contain selected model for reasoning lookup"
                );
                None
            })
    }
}

#[derive(Clone, Copy)]
enum ReasoningModelLookupBackend<'a> {
    ApiProvider {
        provider: &'a str,
    },
    CliRuntime {
        runtime_id: &'a str,
        runtime_kind: CLIAgentRuntimeKind,
    },
}

fn reasoning_model_lookup_backend_label(backend: ReasoningModelLookupBackend<'_>) -> String {
    match backend {
        ReasoningModelLookupBackend::ApiProvider { provider } => {
            format!("provider `{provider}`")
        }
        ReasoningModelLookupBackend::CliRuntime { runtime_id, .. } => {
            format!("CLI runtime `{runtime_id}`")
        }
    }
}

fn reasoning_capability_source_label(source: Option<ReasoningCapabilitySource>) -> &'static str {
    match source {
        Some(ReasoningCapabilitySource::ProviderMetadata) => "provider_metadata",
        Some(ReasoningCapabilitySource::CliMetadata) => "cli_metadata",
        Some(ReasoningCapabilitySource::StaticRegistry) => "static_registry",
        Some(ReasoningCapabilitySource::ConfigOverride) => "config_override",
        Some(ReasoningCapabilitySource::Unknown) | None => "unknown",
    }
}

fn reasoning_capability_source_for_model(model: Option<&ProviderModelInfo>) -> &'static str {
    reasoning_capability_source_label(
        model
            .and_then(|model| model.capabilities.reasoning.as_ref())
            .and_then(|reasoning| reasoning.source),
    )
}

fn reasoning_effort_options_for_model(model: Option<&ProviderModelInfo>) -> Option<&[String]> {
    model
        .and_then(|model| model.capabilities.reasoning.as_ref())
        .map(|reasoning| reasoning.effort_options.as_slice())
}

fn supported_efforts_for_error(reasoning: &ProviderModelReasoningCapabilities) -> String {
    let mut effort_options = Vec::new();
    for effort in &reasoning.effort_options {
        let Some(effort) = pioneer_protocol::ReasoningEffort::canonical_value(effort.as_str())
        else {
            continue;
        };
        if reasoning.mandatory == Some(true) && effort == "none" {
            continue;
        }
        if !effort_options.contains(&effort) {
            effort_options.push(effort);
        }
    }

    if effort_options.is_empty() {
        "unknown".to_owned()
    } else {
        effort_options.join(", ")
    }
}

fn validate_reasoning_effort_for_model(
    backend_label: &str,
    model_id: &str,
    effort: &str,
    model: Option<&ProviderModelInfo>,
) -> Result<(), String> {
    let normalized_effort =
        pioneer_protocol::ReasoningEffort::canonical_value(effort).ok_or_else(|| {
            format!(
                "reasoning effort `{effort}` is not recognized by Pioneer for {backend_label} model `{model_id}`"
            )
        })?;
    let Some(model) = model else {
        return Err(format!(
            "reasoning effort `{effort}` cannot be used with {backend_label} model `{model_id}` because model capability metadata is unavailable; capability source: unknown"
        ));
    };

    let Some(reasoning) = model.capabilities.reasoning.as_ref() else {
        return Err(format!(
            "reasoning effort `{effort}` cannot be used with {backend_label} model `{model_id}` because reasoning capability metadata is missing; capability source: unknown"
        ));
    };
    let capability_source = reasoning_capability_source_label(reasoning.source);

    if reasoning.supported == Some(false) {
        return Err(format!(
            "reasoning effort `{effort}` is not supported by {backend_label} model `{model_id}`; supported efforts: {}; capability source: {capability_source}",
            supported_efforts_for_error(reasoning)
        ));
    }

    if reasoning.effort_options.is_empty() {
        return Err(format!(
            "reasoning effort `{effort}` cannot be used with {backend_label} model `{model_id}` because supported reasoning efforts are unknown; capability source: {capability_source}"
        ));
    }

    if reasoning.mandatory == Some(true) && normalized_effort == "none" {
        return Err(format!(
            "reasoning effort `{effort}` is not supported by {backend_label} model `{model_id}`; supported efforts: {}; capability source: {capability_source}",
            supported_efforts_for_error(reasoning)
        ));
    }

    if !reasoning
        .effort_options
        .iter()
        .filter_map(|supported_effort| {
            let supported_effort =
                pioneer_protocol::ReasoningEffort::canonical_value(supported_effort.as_str())?;
            if reasoning.mandatory == Some(true) && supported_effort == "none" {
                return None;
            }
            Some(supported_effort)
        })
        .any(|supported_effort| supported_effort == normalized_effort)
    {
        return Err(format!(
            "reasoning effort `{effort}` is not supported by {backend_label} model `{model_id}`; supported efforts: {}; capability source: {capability_source}",
            supported_efforts_for_error(reasoning)
        ));
    }

    Ok(())
}

fn effective_cli_runtime_effort(
    requested_reasoning_effort: Option<&str>,
    cli_runtime_effort: Option<&str>,
) -> Result<Option<String>, String> {
    let requested_reasoning_effort =
        requested_reasoning_effort.map(normalized_reasoning_effort_for_comparison);
    let cli_runtime_effort = cli_runtime_effort.map(normalized_reasoning_effort_for_comparison);

    match (
        requested_reasoning_effort.as_deref(),
        cli_runtime_effort.as_deref(),
    ) {
        (Some(requested), Some(cli)) if requested != cli => Err(format!(
            "CLI runtime reasoning effort conflict: top-level reasoning effort `{requested}` does not match cli_runtime_options effort `{cli}`"
        )),
        (Some(requested), _) => Ok(Some(requested.to_owned())),
        (None, Some(cli)) => Ok(Some(cli.to_owned())),
        (None, None) => Ok(None),
    }
}

fn normalized_reasoning_effort_for_comparison(value: &str) -> String {
    pioneer_protocol::ReasoningEffort::canonical_value(value)
        .map(str::to_owned)
        .unwrap_or_else(|| value.trim().to_owned())
}

fn runtime_model_from_codex_snapshot_for_reasoning_lookup(
    model: CodexModelSnapshot,
) -> RuntimeModelInfo {
    RuntimeModelInfo {
        id: model.id,
        name: model.name,
        description: model.description,
        family: model.family,
        is_custom: false,
        active: model.active,
        effort_options: model.effort_options,
        input_modalities: model.input_modalities,
        output_modalities: model.output_modalities,
        supports_reasoning: model.supports_reasoning,
        supports_vision: model.supports_vision,
        max_input_tokens: model.max_input_tokens,
        max_output_tokens: model.max_output_tokens,
    }
}

fn runtime_model_from_claude_snapshot_for_reasoning_lookup(
    model: ClaudeModelSnapshot,
) -> RuntimeModelInfo {
    RuntimeModelInfo {
        id: model.id,
        name: model.name,
        description: model.description,
        family: model.family,
        is_custom: false,
        active: model.active,
        effort_options: model.effort_options,
        input_modalities: model.input_modalities,
        output_modalities: model.output_modalities,
        supports_reasoning: model.supports_reasoning,
        supports_vision: model.supports_vision,
        max_input_tokens: model.max_input_tokens,
        max_output_tokens: model.max_output_tokens,
    }
}

fn append_cli_runtime_custom_models_for_reasoning_lookup(
    models: &mut Vec<RuntimeModelInfo>,
    custom_models: &[String],
) {
    let mut seen = models
        .iter()
        .map(|model| model.id.clone())
        .collect::<HashSet<_>>();
    for raw_model in custom_models {
        let model_id = raw_model.trim();
        if model_id.is_empty() || !seen.insert(model_id.to_owned()) {
            continue;
        }

        models.push(RuntimeModelInfo {
            id: model_id.to_owned(),
            name: Some(model_id.to_owned()),
            description: Some(
                "Configured custom CLI runtime model; capability metadata was not reported by the runtime"
                    .to_owned(),
            ),
            family: None,
            is_custom: true,
            active: None,
            effort_options: Vec::new(),
            input_modalities: Vec::new(),
            output_modalities: Vec::new(),
            supports_reasoning: None,
            supports_vision: None,
            max_input_tokens: None,
            max_output_tokens: None,
        });
    }
}

fn provider_model_from_runtime_model_for_reasoning_lookup(
    provider_key: &str,
    model: RuntimeModelInfo,
) -> ProviderModelInfo {
    let supports_reasoning = model
        .supports_reasoning
        .or_else(|| (!model.effort_options.is_empty()).then_some(true));
    let reasoning = supports_reasoning.map(|supported| ProviderModelReasoningCapabilities {
        supported: Some(supported),
        effort_options: model.effort_options.clone(),
        default_effort: None,
        mandatory: None,
        supports_token_budget: None,
        source: Some(ReasoningCapabilitySource::CliMetadata),
    });

    ProviderModelInfo {
        id: model.id,
        name: model.name,
        description: model.description,
        created: None,
        provider: provider_key.to_owned(),
        owned_by: None,
        limits: ProviderModelLimits {
            max_input_tokens: model.max_input_tokens,
            max_output_tokens: model.max_output_tokens,
            context_window: model.max_input_tokens,
        },
        capabilities: ProviderModelCapabilities {
            vision: model.supports_vision,
            tool_calling: None,
            json_output: None,
            streaming: Some(true),
            thinking: supports_reasoning,
            reasoning,
            fine_tuning: None,
            input_modalities: (!model.input_modalities.is_empty())
                .then_some(model.input_modalities),
            output_modalities: (!model.output_modalities.is_empty())
                .then_some(model.output_modalities),
        },
        pricing: None,
        active: model.active,
        family: model.family,
        lifecycle_status: None,
    }
}

pub(super) fn requested_reasoning_effort(params: &TurnStartParams) -> Option<String> {
    params
        .reasoning
        .as_ref()
        .map(|reasoning| reasoning.effort.clone())
}

fn cli_runtime_effort(params: &TurnStartParams) -> Option<String> {
    params
        .cli_runtime_options
        .as_ref()
        .and_then(|options| options.effort.clone())
}

fn cli_runtime_sandbox_policy_value(params: &TurnStartParams) -> Option<JsonValue> {
    params
        .cli_runtime_options
        .as_ref()
        .and_then(|options| options.sandbox.as_ref())
        .map(|sandbox| sandbox.0.clone())
}

fn cli_runtime_thread_sandbox_label(sandbox_policy: Option<&JsonValue>) -> String {
    let Some(sandbox_policy) = sandbox_policy else {
        return "workspace-write".to_owned();
    };
    let raw = sandbox_policy
        .as_str()
        .or_else(|| sandbox_policy.get("type").and_then(JsonValue::as_str))
        .unwrap_or("workspace-write");
    match normalize_cli_runtime_sandbox_label(raw).as_str() {
        "dangerfullaccess" | "fullaccess" | "dangerfull" => "danger-full-access".to_owned(),
        "readonly" | "read" => "read-only".to_owned(),
        "workspacewrite" | "workspace" | "write" => "workspace-write".to_owned(),
        "externalsandbox" | "external" => "external-sandbox".to_owned(),
        _ => raw.trim().to_owned(),
    }
}

fn normalize_cli_runtime_sandbox_label(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn cli_runtime_provider_key(runtime_id: &str) -> String {
    format!("cli_runtime:{}", runtime_id.trim())
}

fn cli_runtime_kind_matches_config(
    protocol_kind: CLIAgentRuntimeKind,
    config_kind: pioneer_config::GatewayCliAgentRuntimeKindConfig,
) -> bool {
    matches!(
        (protocol_kind, config_kind),
        (
            CLIAgentRuntimeKind::Codex,
            pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex
        ) | (
            CLIAgentRuntimeKind::Claude,
            pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude
        )
    )
}

fn cli_runtime_protocol_kind_label(kind: CLIAgentRuntimeKind) -> &'static str {
    match kind {
        CLIAgentRuntimeKind::Codex => "codex",
        CLIAgentRuntimeKind::Claude => "claude",
    }
}

fn cli_runtime_context_label(kind: CLIAgentRuntimeKind) -> &'static str {
    match kind {
        CLIAgentRuntimeKind::Codex => "Codex CLI",
        CLIAgentRuntimeKind::Claude => "Claude CLI",
    }
}

fn cli_runtime_supports_durable_thread_resume(kind: CLIAgentRuntimeKind) -> bool {
    match kind {
        CLIAgentRuntimeKind::Codex => true,
        CLIAgentRuntimeKind::Claude => false,
    }
}

fn cli_runtime_config_kind_label(
    kind: pioneer_config::GatewayCliAgentRuntimeKindConfig,
) -> &'static str {
    match kind {
        pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex => "codex",
        pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude => "claude",
    }
}

fn cli_runtime_binding_timestamp() -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    use chrono::{FixedOffset, TimeZone};

    FixedOffset::east_opt(0)
        .expect("UTC offset should exist")
        .timestamp_opt(now_timestamp_secs(), 0)
        .single()
        .expect("current timestamp should be valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reasoning_test_model(
        reasoning: Option<ProviderModelReasoningCapabilities>,
    ) -> ProviderModelInfo {
        ProviderModelInfo {
            id: "model-a".to_owned(),
            name: None,
            description: None,
            created: None,
            provider: "provider-a".to_owned(),
            owned_by: None,
            limits: ProviderModelLimits {
                max_input_tokens: None,
                max_output_tokens: None,
                context_window: None,
            },
            capabilities: ProviderModelCapabilities {
                vision: None,
                tool_calling: None,
                json_output: None,
                streaming: None,
                thinking: None,
                reasoning,
                fine_tuning: None,
                input_modalities: None,
                output_modalities: None,
            },
            pricing: None,
            active: None,
            family: None,
            lifecycle_status: None,
        }
    }

    fn reasoning_capabilities(
        supported: Option<bool>,
        effort_options: &[&str],
    ) -> ProviderModelReasoningCapabilities {
        ProviderModelReasoningCapabilities {
            supported,
            effort_options: effort_options
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            default_effort: None,
            mandatory: None,
            supports_token_budget: None,
            source: Some(ReasoningCapabilitySource::StaticRegistry),
        }
    }

    #[test]
    fn reasoning_effort_validation_rejects_missing_or_unknown_metadata() {
        let missing =
            validate_reasoning_effort_for_model("provider `openai`", "unknown-model", "high", None)
                .expect_err("missing model metadata should reject selected effort");
        assert!(missing.contains("metadata is unavailable"));

        let model_without_reasoning = reasoning_test_model(None);
        let absent = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&model_without_reasoning),
        )
        .expect_err("missing reasoning capability should reject selected effort");
        assert!(absent.contains("reasoning capability metadata is missing"));

        let model_without_efforts =
            reasoning_test_model(Some(reasoning_capabilities(Some(true), &[])));
        let unknown = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&model_without_efforts),
        )
        .expect_err("empty effort list should reject selected effort");
        assert!(unknown.contains("supported reasoning efforts are unknown"));
    }

    #[test]
    fn reasoning_effort_validation_rejects_unsupported_model_or_value() {
        let unsupported_model =
            reasoning_test_model(Some(reasoning_capabilities(Some(false), &["low", "high"])));
        let error = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&unsupported_model),
        )
        .expect_err("unsupported model should reject selected effort");
        assert!(error.contains("is not supported"));

        let unsupported_value =
            reasoning_test_model(Some(reasoning_capabilities(Some(true), &["low", "medium"])));
        let error = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&unsupported_value),
        )
        .expect_err("unsupported value should reject selected effort");
        assert!(error.contains("supported efforts: low, medium"));
    }

    #[test]
    fn reasoning_effort_validation_error_includes_debuggable_context() {
        let model =
            reasoning_test_model(Some(reasoning_capabilities(Some(true), &["low", "medium"])));

        let error = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "high",
            Some(&model),
        )
        .expect_err("unsupported value should reject selected effort");

        assert_eq!(
            error,
            "reasoning effort `high` is not supported by provider `openai` model `model-a`; supported efforts: low, medium; capability source: static_registry"
        );
    }

    #[test]
    fn reasoning_effort_validation_accepts_known_effort() {
        let model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["low", "medium", "high"],
        )));

        validate_reasoning_effort_for_model("provider `openai`", "model-a", "medium", Some(&model))
            .expect("known effort should pass validation");
    }

    #[test]
    fn reasoning_effort_validation_accepts_known_aliases() {
        let model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["low", "extra-high", "maximum"],
        )));

        validate_reasoning_effort_for_model("provider `openai`", "model-a", "xhigh", Some(&model))
            .expect("canonical effort should match provider alias");
        validate_reasoning_effort_for_model("provider `openai`", "model-a", "max", Some(&model))
            .expect("canonical max should match provider alias");
    }

    #[test]
    fn reasoning_effort_validation_rejects_unknown_provider_effort_values() {
        let model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["low", "turbo-high"],
        )));

        validate_reasoning_effort_for_model("provider `openai`", "model-a", "low", Some(&model))
            .expect("known effort should pass validation");
        let error = validate_reasoning_effort_for_model(
            "provider `openai`",
            "model-a",
            "turbo-high",
            Some(&model),
        )
        .expect_err("unknown effort should be rejected even if metadata reports it");

        assert_eq!(
            error,
            "reasoning effort `turbo-high` is not recognized by Pioneer for provider `openai` model `model-a`"
        );
    }

    #[test]
    fn reasoning_effort_validation_rejects_none_for_mandatory_reasoning() {
        let mut model = reasoning_test_model(Some(reasoning_capabilities(
            Some(true),
            &["none", "low", "medium"],
        )));
        model
            .capabilities
            .reasoning
            .as_mut()
            .expect("reasoning capabilities")
            .mandatory = Some(true);

        let error = validate_reasoning_effort_for_model(
            "provider `openrouter`",
            "model-a",
            "none",
            Some(&model),
        )
        .expect_err("mandatory reasoning should reject none");

        assert!(error.contains("supported efforts: low, medium"));
    }

    #[test]
    fn effective_cli_runtime_effort_accepts_legacy_top_level_or_matching_values() {
        assert_eq!(
            effective_cli_runtime_effort(None, Some("high")).expect("legacy effort"),
            Some("high".to_owned())
        );
        assert_eq!(
            effective_cli_runtime_effort(Some("medium"), None).expect("top-level effort"),
            Some("medium".to_owned())
        );
        assert_eq!(
            effective_cli_runtime_effort(Some("low"), Some("low")).expect("matching efforts"),
            Some("low".to_owned())
        );
        assert_eq!(
            effective_cli_runtime_effort(Some("Extra High"), Some("xhigh"))
                .expect("matching alias efforts"),
            Some("xhigh".to_owned())
        );
        assert_eq!(
            effective_cli_runtime_effort(None, None).expect("no effort"),
            None
        );
    }

    #[test]
    fn effective_cli_runtime_effort_rejects_conflicting_values() {
        let error = effective_cli_runtime_effort(Some("high"), Some("low"))
            .expect_err("conflicting CLI efforts should reject");
        assert!(error.contains("top-level reasoning effort `high`"));
        assert!(error.contains("cli_runtime_options effort `low`"));
    }

    #[test]
    fn runtime_model_reasoning_lookup_infers_legacy_thinking_from_efforts() {
        let model = provider_model_from_runtime_model_for_reasoning_lookup(
            "cli_runtime:codex",
            RuntimeModelInfo {
                id: "gpt-5".to_owned(),
                name: Some("GPT 5".to_owned()),
                description: None,
                family: None,
                is_custom: false,
                active: Some(true),
                effort_options: vec!["low".to_owned(), "high".to_owned()],
                input_modalities: Vec::new(),
                output_modalities: Vec::new(),
                supports_reasoning: None,
                supports_vision: None,
                max_input_tokens: None,
                max_output_tokens: None,
            },
        );

        assert_eq!(model.capabilities.thinking, Some(true));
        assert_eq!(
            model
                .capabilities
                .reasoning
                .as_ref()
                .and_then(|reasoning| reasoning.supported),
            Some(true)
        );
    }
}
