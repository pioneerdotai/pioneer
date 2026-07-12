use super::*;
use crate::cli_runtime::config::{
    claude_account_probe_config_from_instance_with_proxy,
    codex_account_probe_config_from_instance_with_proxy,
};
use crate::cli_runtime::manager::{
    CLIAgentRuntimeCodexEventReceivers, CLIAgentRuntimeObservedTurnStatus, CLIAgentRuntimeSession,
    CLIAgentRuntimeSessionHandle, CLIAgentRuntimeSessionKey, CLIAgentRuntimeThreadForkRequest,
    CLIAgentRuntimeThreadNameSetRequest, CLIAgentRuntimeTurnSteerRequest,
};
use anyhow::Context as AnyhowContext;
use pioneer_cli_agent_runtime::claude::{
    ClaudeAccountProbeSnapshot, ClaudeAccountProbeStatus, ClaudeAccountSnapshot,
    ClaudeModelListSnapshot, ClaudeModelSnapshot, ClaudeProbe, ClaudeProbeDiagnostic,
    ClaudeProbeDiagnosticLevel,
};
use pioneer_cli_agent_runtime::codex::{
    CodexAccountProbeSnapshot, CodexAccountProbeStatus, CodexAccountSnapshot,
    CodexCommandApprovalDecision, CodexCommandApprovalRequest, CodexFileChangeApprovalDecision,
    CodexFileChangeApprovalRequest, CodexJsonlRpcServerRequest, CodexLoginStartResponse,
    CodexLoginStartSnapshot, CodexLoginStartStatus, CodexModelListProbeSnapshot,
    CodexModelListProbeStatus, CodexModelSnapshot, CodexProbe, CodexProbeDiagnostic,
    CodexProbeDiagnosticLevel, CodexUserInputRequest, codex_command_approval_response,
    codex_file_change_approval_response, codex_user_input_response,
    decode_codex_command_approval_request, decode_codex_file_change_approval_request,
    decode_codex_user_input_request,
};
use pioneer_cli_agent_runtime::event::{
    RuntimeEvent, RuntimeEventMappingOptions, RuntimeRequestResolved, map_codex_notification_event,
    map_codex_server_request_event,
};
use pioneer_config::{
    EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use pioneer_crud::NewCliRuntimeNativeEvent;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

const CLI_RUNTIME_FILE_CHANGE_DIFF_PREVIEW_MAX_CHARS: usize = 16_384;
const CLI_RUNTIME_PENDING_TURN_EVENT_MAX_KEYS: usize = 128;
const CLI_RUNTIME_PENDING_TURN_EVENT_MAX_PER_TURN: usize = 512;
const CLI_RUNTIME_STALE_TURN_SCAN_LIMIT: u64 = 128;
const CLI_RUNTIME_SILENT_TURN_STALE_AFTER_MS: i64 = 150_000;
const CLI_RUNTIME_EVENTED_TURN_STALE_AFTER_MS: i64 = 15 * 60 * 1_000;
const CLI_RUNTIME_PENDING_UNBOUND_EVENT_TTL_MS: i64 = 30_000;
const CLI_RUNTIME_HUMAN_RESPONSE_TIMEOUT_MS: i64 = 24 * 60 * 60 * 1_000;
const CLI_RUNTIME_TERMINAL_NATIVE_EVENT_CLEANUP_BATCH_SIZE: u64 = 16_384;

impl MessageProcessor {
    pub(super) async fn cli_runtime_list(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeListParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_LIST,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        let runtimes = match self.load_cli_runtime_summaries(workspace_id.as_str()).await {
            Ok(runtimes) => runtimes,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load CLI runtime catalog: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_LIST,
            &CLIRuntimeListResponse { runtimes },
        )
        .await;
    }

    pub(super) async fn cli_runtime_get(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeGetParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_GET,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        let Some(runtime) = self
            .load_cli_runtime_live_summary_by_id(
                connection_id,
                request_id.clone(),
                workspace_id.as_str(),
                &params.runtime_id,
            )
            .await
        else {
            return;
        };

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_GET,
            &CLIRuntimeGetResponse { runtime },
        )
        .await;
    }

    pub(super) async fn cli_runtime_status(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeStatusParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_STATUS,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        let Some(runtime) = self
            .load_cli_runtime_summary_by_id(
                connection_id,
                request_id.clone(),
                workspace_id.as_str(),
                &params.runtime_id,
            )
            .await
        else {
            return;
        };

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_STATUS,
            &CLIRuntimeStatusResponse { runtime },
        )
        .await;
    }

    pub(super) async fn cli_runtime_refresh(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeRefreshParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_REFRESH,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        let instances = match self.load_cli_runtime_instances() {
            Ok(instances) => instances,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load CLI runtime catalog: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let instances = if let Some(runtime_id) = params.runtime_id {
            match find_cli_runtime_instance(instances, runtime_id.as_str()) {
                Some(instance) => vec![instance],
                None => {
                    self.send_unknown_cli_runtime_error(connection_id, request_id, &runtime_id)
                        .await;
                    return;
                }
            }
        } else {
            instances
        };
        let mut runtimes = Vec::with_capacity(instances.len());
        for instance in instances {
            runtimes.push(
                self.cli_runtime_live_summary_from_instance(workspace_id.as_str(), instance)
                    .await,
            );
        }

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_REFRESH,
            &CLIRuntimeRefreshResponse { runtimes },
        )
        .await;
    }

    pub(super) async fn cli_runtime_list_models(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeListModelsParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_LIST_MODELS,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        let Some(instance) = self
            .load_cli_runtime_instance_by_id(connection_id, request_id.clone(), &params.runtime_id)
            .await
        else {
            return;
        };

        let model_list = if instance.enabled {
            match instance.kind {
                GatewayCliAgentRuntimeKindConfig::Codex => {
                    let proxy_url = self
                        .cli_runtime_proxy_url(workspace_id.as_str(), instance.id.as_str())
                        .await;
                    let probe = CodexProbe::model_list(
                        codex_account_probe_config_from_instance_with_proxy(
                            &instance,
                            proxy_url.as_deref(),
                        ),
                    )
                    .await;
                    runtime_model_list_from_codex_probe(probe, &instance.custom_models)
                }
                GatewayCliAgentRuntimeKindConfig::Claude => runtime_model_list_from_claude_probe(
                    ClaudeProbe::model_list(
                        claude_account_probe_config_from_instance_with_proxy(
                            &instance,
                            self.cli_runtime_proxy_url(workspace_id.as_str(), instance.id.as_str())
                                .await
                                .as_deref(),
                        ),
                        &instance.custom_models,
                    )
                    .await,
                ),
            }
        } else {
            RuntimeModelListResult {
                models: runtime_models_with_custom_models(Vec::new(), &instance.custom_models),
                diagnostics: Vec::new(),
                error_message: Some(format!("CLI runtime `{}` is disabled", instance.id)),
            }
        };

        if model_list.models.is_empty() {
            if let Some(error_message) = model_list.error_message.as_deref() {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "failed to list models for CLI runtime `{}`: {error_message}",
                            params.runtime_id
                        ),
                    ),
                )
                .await;
                return;
            }
        }

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_LIST_MODELS,
            &CLIRuntimeListModelsResponse {
                runtime_id: params.runtime_id,
                models: model_list.models,
                diagnostics: model_list.diagnostics,
                refreshed_at_unix_ms: Some(current_unix_ms()),
            },
        )
        .await;
    }

    pub(super) async fn cli_runtime_thread_binding_get(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeThreadBindingGetParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_THREAD_BINDING_GET,
                params.workspace_id,
            )
            .await
        else {
            return;
        };
        let thread_id = params.thread_id.trim().to_owned();
        if thread_id.is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` is required",
                        methods::CLI_RUNTIME_THREAD_BINDING_GET
                    ),
                ),
            )
            .await;
            return;
        }

        let binding = match self
            .crud_store
            .get_cli_runtime_thread_binding(thread_id.as_str())
            .await
        {
            Ok(Some(binding)) => {
                if binding.workspace_id != workspace_id {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            format!(
                                "CLI runtime binding for thread `{}` belongs to workspace `{}`",
                                thread_id, binding.workspace_id
                            ),
                        ),
                    )
                    .await;
                    return;
                }
                match cli_runtime_thread_binding_from_record(binding) {
                    Ok(binding) => Some(binding),
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                format!(
                                    "failed to load CLI runtime binding for thread `{thread_id}`: {error:#}"
                                ),
                            ),
                        )
                        .await;
                        return;
                    }
                }
            }
            Ok(None) => None,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "failed to load CLI runtime binding for thread `{thread_id}`: {error:#}"
                        ),
                    ),
                )
                .await;
                return;
            }
        };

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_THREAD_BINDING_GET,
            &CLIRuntimeThreadBindingGetResponse { binding },
        )
        .await;
    }

    pub(super) async fn cli_runtime_thread_compact(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeThreadCompactParams,
    ) {
        let Some(_workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_THREAD_COMPACT,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_REQUEST_CODE,
                "CLI runtime thread compaction is not supported for managed Pioneer CLI runtime threads".to_owned(),
            ),
        )
        .await;
    }

    // Native archive/unarchive is intentionally not mirrored here. Pioneer remains
    // the owner of sidebar visibility and archival state until a dedicated product
    // decision wires native Codex archive semantics explicitly.
    pub(super) fn cli_runtime_thread_fork<'a>(
        &'a self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeThreadForkParams,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            let Some(workspace_id) = self
                .validate_cli_runtime_workspace(
                    connection_id,
                    request_id.clone(),
                    methods::CLI_RUNTIME_THREAD_FORK,
                    params.workspace_id.clone(),
                )
                .await
            else {
                return;
            };
            let params = match validate_cli_runtime_thread_fork_params(params) {
                Ok(params) => params,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(Some(request_id), INVALID_PARAMS_CODE, error),
                    )
                    .await;
                    return;
                }
            };

            let Some(source_thread) = self
                .thread_manager
                .thread_get(params.source_thread_id.as_str())
                .await
            else {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("thread `{}` is not loaded", params.source_thread_id),
                    ),
                )
                .await;
                return;
            };
            if source_thread.workspace_id != workspace_id {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "thread `{}` belongs to workspace `{}`",
                            params.source_thread_id, source_thread.workspace_id
                        ),
                    ),
                )
                .await;
                return;
            }
            if self
                .thread_manager
                .thread_get(params.fork_thread_id.as_str())
                .await
                .is_some()
            {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!("thread `{}` already exists", params.fork_thread_id),
                    ),
                )
                .await;
                return;
            }
            match self
                .crud_store
                .get_thread_model(params.fork_thread_id.as_str())
                .await
            {
                Ok(Some(_)) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            format!("thread `{}` already exists", params.fork_thread_id),
                        ),
                    )
                    .await;
                    return;
                }
                Ok(None) => {}
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!(
                                "failed to check fork thread `{}` in storage: {error:#}",
                                params.fork_thread_id
                            ),
                        ),
                    )
                    .await;
                    return;
                }
            }

            let binding = match self
                .crud_store
                .get_cli_runtime_thread_binding(params.source_thread_id.as_str())
                .await
            {
                Ok(Some(binding)) => binding,
                Ok(None) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!(
                                "thread `{}` is not bound to a CLI runtime",
                                params.source_thread_id
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
                            format!(
                                "failed to load CLI runtime binding for thread `{}`: {error:#}",
                                params.source_thread_id
                            ),
                        ),
                    )
                    .await;
                    return;
                }
            };
            if binding.workspace_id != workspace_id {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "CLI runtime binding for thread `{}` belongs to workspace `{}`",
                            params.source_thread_id, binding.workspace_id
                        ),
                    ),
                )
                .await;
                return;
            }
            if binding.runtime_id != params.runtime_id {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "thread `{}` is bound to CLI runtime `{}`",
                            params.source_thread_id, binding.runtime_id
                        ),
                    ),
                )
                .await;
                return;
            }
            let supports_fork =
                cli_runtime_capabilities_for_stored_kind(binding.runtime_kind.as_str())
                    .is_some_and(|capabilities| capabilities.supports_fork);
            if !supports_fork {
                self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!(
                        "CLI runtime fork is not supported for `{}` threads; thread `{}` is bound to `{}`",
                        binding.runtime_kind.as_str(),
                        params.source_thread_id,
                        binding.runtime_kind.as_str()
                    ),
                ),
            )
            .await;
                return;
            }

            let Some(manager) = self.cli_runtime_manager.as_ref() else {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        "CLI runtime manager is not available for thread fork".to_owned(),
                    ),
                )
                .await;
                return;
            };
            let key = match CLIAgentRuntimeSessionKey::new(
                workspace_id.as_str(),
                params.runtime_id.as_str(),
                params.source_thread_id.as_str(),
            ) {
                Ok(key) => key,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            format!("invalid CLI runtime fork key: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let proxy_url = self
                .cli_runtime_proxy_url(workspace_id.as_str(), params.runtime_id.as_str())
                .await;
            let handle = match manager
                .get_or_start_with_options(
                    key,
                    crate::cli_runtime::manager::CLIAgentRuntimeSessionStartOptions {
                        env: crate::cli_runtime::config::proxy_env(proxy_url.as_deref()),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(handle) => handle,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to start CLI runtime session for fork: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let fork = match handle
                .session()
                .fork_thread(CLIAgentRuntimeThreadForkRequest {
                    native_thread_id: binding.native_thread_id.clone(),
                })
                .await
            {
                Ok(fork) => fork,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to fork CLI runtime thread: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            };

            let outcome = match self
                .thread_manager
                .thread_start(
                    connection_id,
                    workspace_id.clone(),
                    ThreadStartParams {
                        thread_id: params.fork_thread_id.clone(),
                        workspace_id: workspace_id.clone(),
                        name: params.name.clone().or_else(|| source_thread.name.clone()),
                        model: Some(source_thread.model.clone()),
                        model_provider: Some(source_thread.model_provider.clone()),
                        sandbox: None,
                        mode: Some(source_thread.mode),
                        origin_kind: Some(ThreadOriginKind::User),
                        sidebar_visibility: Some(ThreadSidebarVisibility::Visible),
                        agent_nickname: source_thread.agent_nickname.clone(),
                        agent_role: source_thread.agent_role.clone(),
                    },
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to create fork thread: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            };

            if let Err(error) = self
                .crud_store
                .upsert_thread_model(&outcome.response.thread)
                .await
            {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to persist fork thread: {error:#}"),
                    ),
                )
                .await;
                return;
            }

            let now = chrono::Utc::now().fixed_offset();
            let resume_cursor_json = match pioneer_crud::serialize_cli_runtime_json(&json!({
                "threadId": fork.native_thread_id.as_str(),
                "forkedFromThreadId": binding.native_thread_id.as_str(),
            })) {
                Ok(resume_cursor_json) => resume_cursor_json,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to encode fork CLI runtime cursor: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            };
            if let Err(error) = self
                .crud_store
                .upsert_cli_runtime_thread_binding(NewCliRuntimeThreadBinding {
                    thread_id: params.fork_thread_id.clone(),
                    workspace_id: workspace_id.clone(),
                    runtime_id: params.runtime_id.clone(),
                    runtime_kind: binding.runtime_kind.clone(),
                    native_thread_id: fork.native_thread_id.clone(),
                    native_session_id: None,
                    native_root_thread_id: binding
                        .native_root_thread_id
                        .clone()
                        .or_else(|| Some(binding.native_thread_id.clone())),
                    native_cwd: fork.native_cwd.clone().or(binding.native_cwd.clone()),
                    native_model: fork.native_model.clone().or(binding.native_model.clone()),
                    resume_cursor_json,
                    status: "active".to_owned(),
                    created_at: now,
                    updated_at: now,
                })
                .await
            {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to persist fork CLI runtime binding: {error:#}"),
                    ),
                )
                .await;
                return;
            }

            self.send_cli_runtime_response(
                connection_id,
                request_id,
                methods::CLI_RUNTIME_THREAD_FORK,
                &CLIRuntimeThreadForkResponse {
                    workspace_id: workspace_id.clone(),
                    runtime_id: params.runtime_id,
                    source_thread_id: params.source_thread_id,
                    thread: outcome.response.thread.clone(),
                    native_thread_id: fork.native_thread_id,
                    raw: fork.raw,
                },
            )
            .await;

            let notification = match JsonRpcNotification::from_params(
                events::THREAD_STARTED,
                &outcome.started_notification,
            ) {
                Ok(notification) => notification,
                Err(error) => {
                    error!(error = %error, "failed to encode fork thread/started notification");
                    return;
                }
            };
            match serde_json::to_string(&notification) {
                Ok(payload) => {
                    for notification_connection_id in outcome.started_notification_connection_ids {
                        if let Err(error) = self
                            .session_manager
                            .send_text(notification_connection_id, payload.clone())
                            .await
                        {
                            warn!(
                                connection_id = notification_connection_id,
                                error = %format!("{error:#}"),
                                "failed to send fork thread/started notification"
                            );
                        }
                    }
                }
                Err(error) => {
                    error!(error = %error, "failed to serialize fork thread/started notification");
                }
            }
            self.notify_thread_tree_changed(workspace_id).await;
        })
    }

    pub(super) fn cli_runtime_turn_steer<'a>(
        &'a self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeTurnSteerParams,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            let Some(workspace_id) = self
                .validate_cli_runtime_workspace(
                    connection_id,
                    request_id.clone(),
                    methods::CLI_RUNTIME_TURN_STEER,
                    params.workspace_id.clone(),
                )
                .await
            else {
                return;
            };
            let params = match validate_cli_runtime_turn_steer_params(params) {
                Ok(params) => params,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(Some(request_id), INVALID_PARAMS_CODE, error),
                    )
                    .await;
                    return;
                }
            };

            let Some(thread) = self
                .thread_manager
                .thread_get(params.thread_id.as_str())
                .await
            else {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("thread `{}` is not loaded", params.thread_id),
                    ),
                )
                .await;
                return;
            };
            if thread.workspace_id != workspace_id {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "thread `{}` belongs to workspace `{}`",
                            params.thread_id, thread.workspace_id
                        ),
                    ),
                )
                .await;
                return;
            }

            let turn_binding = match self
                .crud_store
                .get_cli_runtime_turn_binding(params.turn_id.as_str())
                .await
            {
                Ok(Some(binding)) => binding,
                Ok(None) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("turn `{}` is not bound to a CLI runtime", params.turn_id),
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
                            format!(
                                "failed to load CLI runtime turn binding for `{}`: {error:#}",
                                params.turn_id
                            ),
                        ),
                    )
                    .await;
                    return;
                }
            };
            if turn_binding.workspace_id != workspace_id
                || turn_binding.thread_id != params.thread_id
            {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "turn `{}` belongs to workspace/thread `{}/{}`",
                            params.turn_id, turn_binding.workspace_id, turn_binding.thread_id
                        ),
                    ),
                )
                .await;
                return;
            }
            if turn_binding.runtime_id != params.runtime_id {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "turn `{}` is bound to CLI runtime `{}`",
                            params.turn_id, turn_binding.runtime_id
                        ),
                    ),
                )
                .await;
                return;
            }
            let supports_steer =
                cli_runtime_capabilities_for_stored_kind(turn_binding.runtime_kind.as_str())
                    .is_some_and(|capabilities| capabilities.supports_steer);
            if !supports_steer {
                self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!(
                        "CLI runtime turn steering is not supported for `{}` turns; turn `{}` is bound to `{}`",
                        turn_binding.runtime_kind.as_str(),
                        params.turn_id, turn_binding.runtime_kind.as_str()
                    ),
                ),
            )
            .await;
                return;
            }
            if turn_binding.status
                != crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
            {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "turn `{}` is not running and cannot be steered",
                            params.turn_id
                        ),
                    ),
                )
                .await;
                return;
            }
            let Some(native_turn_id) = turn_binding.native_turn_id.clone() else {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "turn `{}` does not have an active CLI runtime native turn id",
                            params.turn_id
                        ),
                    ),
                )
                .await;
                return;
            };

            let Some(manager) = self.cli_runtime_manager.as_ref() else {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        "CLI runtime manager is not available for turn steering".to_owned(),
                    ),
                )
                .await;
                return;
            };
            let key = match CLIAgentRuntimeSessionKey::new(
                workspace_id.as_str(),
                params.runtime_id.as_str(),
                params.thread_id.as_str(),
            ) {
                Ok(key) => key,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            format!("invalid CLI runtime steering key: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let Some(handle) = manager.existing_session(&key).await else {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        "CLI runtime session is not active for turn steering".to_owned(),
                    ),
                )
                .await;
                return;
            };
            let steer = match handle
                .session()
                .steer_turn(CLIAgentRuntimeTurnSteerRequest {
                    native_thread_id: turn_binding.native_thread_id.clone(),
                    native_turn_id: native_turn_id.clone(),
                    message: params.message.clone(),
                })
                .await
            {
                Ok(steer) => steer,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to steer CLI runtime turn: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            };

            self.emit_cli_runtime_steer_accepted_timeline_event_fresh_task(
                workspace_id.clone(),
                params.thread_id.clone(),
                params.turn_id.clone(),
                steer.native_turn_id.clone(),
            )
            .await;

            self.send_cli_runtime_response(
                connection_id,
                request_id,
                methods::CLI_RUNTIME_TURN_STEER,
                &CLIRuntimeTurnSteerResponse {
                    workspace_id,
                    runtime_id: params.runtime_id,
                    thread_id: params.thread_id,
                    turn_id: params.turn_id,
                    native_thread_id: steer.native_thread_id,
                    native_turn_id: steer.native_turn_id,
                    raw: steer.raw,
                },
            )
            .await;
        })
    }

    pub(super) async fn cli_runtime_review_start(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeReviewStartParams,
    ) {
        let Some(_workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_REVIEW_START,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_REQUEST_CODE,
                "CLI runtime review start is not supported for managed Pioneer CLI runtime threads"
                    .to_owned(),
            ),
        )
        .await;
    }

    pub(super) async fn cli_runtime_login_start(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeLoginStartParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_LOGIN_START,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        let Some(instance) = self
            .load_cli_runtime_instance_by_id(connection_id, request_id.clone(), &params.runtime_id)
            .await
        else {
            return;
        };

        let response = if instance.enabled {
            match instance.kind {
                GatewayCliAgentRuntimeKindConfig::Codex => {
                    let proxy_url = self
                        .cli_runtime_proxy_url(workspace_id.as_str(), instance.id.as_str())
                        .await;
                    let snapshot = CodexProbe::login_start(
                        codex_account_probe_config_from_instance_with_proxy(
                            &instance,
                            proxy_url.as_deref(),
                        ),
                        cli_runtime_login_start_type_to_codex(params.login_type),
                    )
                    .await;
                    cli_runtime_login_start_response_from_codex(
                        params.runtime_id,
                        params.login_type,
                        snapshot,
                    )
                }
                unsupported_kind => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!(
                                "CLI runtime login management is not supported for `{}` runtimes",
                                cli_runtime_protocol_kind_label(cli_runtime_kind_from_config(
                                    unsupported_kind
                                ))
                            ),
                        ),
                    )
                    .await;
                    return;
                }
            }
        } else {
            CLIRuntimeLoginStartResponse {
                runtime_id: params.runtime_id,
                login_type: params.login_type,
                status: RuntimeStatus::Disabled,
                login_id: None,
                verification_url: None,
                user_code: None,
                auth_url: None,
                message: Some("CLI runtime is disabled".to_owned()),
                raw: None,
            }
        };

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_LOGIN_START,
            &response,
        )
        .await;
    }

    pub(super) async fn cli_runtime_login_cancel(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeLoginCancelParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_LOGIN_CANCEL,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        let Some(instance) = self
            .load_cli_runtime_instance_by_id(connection_id, request_id.clone(), &params.runtime_id)
            .await
        else {
            return;
        };

        let cancelled = if instance.enabled {
            match instance.kind {
                GatewayCliAgentRuntimeKindConfig::Codex => {
                    let proxy_url = self
                        .cli_runtime_proxy_url(workspace_id.as_str(), instance.id.as_str())
                        .await;
                    CodexProbe::login_cancel(
                        codex_account_probe_config_from_instance_with_proxy(
                            &instance,
                            proxy_url.as_deref(),
                        ),
                        params.login_id.clone(),
                    )
                    .await
                    .cancelled
                }
                unsupported_kind => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!(
                                "CLI runtime login management is not supported for `{}` runtimes",
                                cli_runtime_protocol_kind_label(cli_runtime_kind_from_config(
                                    unsupported_kind
                                ))
                            ),
                        ),
                    )
                    .await;
                    return;
                }
            }
        } else {
            false
        };

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_LOGIN_CANCEL,
            &CLIRuntimeLoginCancelResponse {
                runtime_id: params.runtime_id,
                login_id: params.login_id,
                cancelled,
            },
        )
        .await;
    }

    pub(super) async fn cli_runtime_proxy_set(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeProxySetParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_PROXY_SET,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        if self
            .load_cli_runtime_instance_by_id(connection_id, request_id.clone(), &params.runtime_id)
            .await
            .is_none()
        {
            return;
        }

        let proxy_url = match pioneer_provider::validate_proxy_url(params.proxy_url.as_str()) {
            Ok(proxy_url) => proxy_url,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: {error:#}",
                            methods::CLI_RUNTIME_PROXY_SET
                        ),
                    ),
                )
                .await;
                return;
            }
        };

        let runtime_id = match self.gateway_secrets.set_workspace_cli_runtime_proxy(
            workspace_id.as_str(),
            params.runtime_id.as_str(),
            proxy_url.as_str(),
        ) {
            Ok(runtime_id) => runtime_id,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to save CLI runtime proxy: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        self.cache_cli_runtime_proxy_url(
            workspace_id.as_str(),
            runtime_id.as_str(),
            Some(proxy_url.clone()),
        )
        .await;

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_PROXY_SET,
            &CLIRuntimeProxySetResponse {
                runtime_id,
                proxy_url,
            },
        )
        .await;
    }

    pub(super) async fn cli_runtime_proxy_delete(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeProxyDeleteParams,
    ) {
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_PROXY_DELETE,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        if self
            .load_cli_runtime_instance_by_id(connection_id, request_id.clone(), &params.runtime_id)
            .await
            .is_none()
        {
            return;
        }

        let (runtime_id, deleted) = match self
            .gateway_secrets
            .delete_workspace_cli_runtime_proxy(workspace_id.as_str(), params.runtime_id.as_str())
        {
            Ok(result) => result,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to delete CLI runtime proxy: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        self.cache_cli_runtime_proxy_url(workspace_id.as_str(), runtime_id.as_str(), None)
            .await;

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_PROXY_DELETE,
            &CLIRuntimeProxyDeleteResponse {
                runtime_id,
                deleted,
            },
        )
        .await;
    }

    pub(super) async fn cli_runtime_request_respond(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: CLIRuntimeRequestRespondParams,
    ) {
        let resolution = params.resolution.clone();
        let Some(workspace_id) = self
            .validate_cli_runtime_workspace(
                connection_id,
                request_id.clone(),
                methods::CLI_RUNTIME_REQUEST_RESPOND,
                params.workspace_id.clone(),
            )
            .await
        else {
            return;
        };

        let pending = match self
            .crud_store
            .get_cli_runtime_pending_request(params.request_id.as_str())
            .await
        {
            Ok(Some(pending)) => pending,
            Ok(None) => {
                self.send_stale_cli_runtime_request_error(
                    connection_id,
                    request_id,
                    params.request_id.as_str(),
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
                        format!(
                            "failed to load CLI runtime pending request `{}`: {error:#}",
                            params.request_id
                        ),
                    ),
                )
                .await;
                return;
            }
        };

        if pending.workspace_id != workspace_id {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "CLI runtime request `{}` belongs to workspace `{}`",
                        pending.request_id, pending.workspace_id
                    ),
                ),
            )
            .await;
            return;
        }
        if pending.runtime_id != params.runtime_id {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "CLI runtime request `{}` belongs to runtime `{}`",
                        pending.request_id, pending.runtime_id
                    ),
                ),
            )
            .await;
            return;
        }
        if pending.status != StoredCliRuntimePendingRequestStatus::Pending {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "CLI runtime request `{}` is already `{}`",
                        pending.request_id,
                        pending.status.as_str()
                    ),
                ),
            )
            .await;
            return;
        }

        if let Err(error) = self
            .validate_cli_runtime_pending_request_active_turn(&pending)
            .await
        {
            if let Err(expire_error) = self
                .expire_cli_runtime_pending_request_as_stale(&pending)
                .await
            {
                warn!(
                    workspace_id = pending.workspace_id.as_str(),
                    runtime_id = pending.runtime_id.as_str(),
                    thread_id = pending.thread_id.as_str(),
                    turn_id = pending.turn_id.as_deref(),
                    request_id = pending.request_id.as_str(),
                    error = %format!("{expire_error:#}"),
                    "failed to expire stale CLI runtime pending request"
                );
            }
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "stale CLI runtime request `{}` cannot be answered: {error:#}",
                        pending.request_id
                    ),
                ),
            )
            .await;
            return;
        }

        if cli_runtime_pending_request_is_human_wait(pending.request_kind.as_str()) {
            let now_unix_ms = chrono::Utc::now().timestamp_millis();
            if now_unix_ms.saturating_sub(pending.created_at.timestamp_millis())
                >= CLI_RUNTIME_HUMAN_RESPONSE_TIMEOUT_MS
            {
                if let Some(turn_id) = pending.turn_id.as_deref() {
                    if let Err(error) =
                        message_future(self.reconcile_cli_runtime_human_wait_for_turn(
                            turn_id,
                            now_unix_ms,
                            "CLI runtime request response",
                        ))
                        .await
                    {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                format!(
                                    "failed to handle expired CLI runtime request `{}` after user response timeout: {error:#}",
                                    pending.request_id
                                ),
                            ),
                        )
                        .await;
                        return;
                    }
                } else if let Err(error) = self
                    .expire_cli_runtime_pending_request_as_stale(&pending)
                    .await
                {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!(
                                "failed to expire CLI runtime request `{}` after user response timeout: {error:#}",
                                pending.request_id
                            ),
                        ),
                    )
                    .await;
                    return;
                }
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "CLI runtime request `{}` expired after waiting for user response for {} ms",
                            pending.request_id, CLI_RUNTIME_HUMAN_RESPONSE_TIMEOUT_MS
                        ),
                    ),
                )
                .await;
                return;
            }
        }

        if let Err(error) = validate_cli_runtime_native_request_resolution(&pending, &resolution) {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid CLI runtime request `{}` resolution: {error:#}",
                        pending.request_id
                    ),
                ),
            )
            .await;
            return;
        }

        let native_response_session = match self
            .cli_runtime_existing_session_for_pending_request(&pending)
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                if let Err(expire_error) = self
                    .expire_cli_runtime_pending_request_as_stale(&pending)
                    .await
                {
                    warn!(
                        workspace_id = pending.workspace_id.as_str(),
                        runtime_id = pending.runtime_id.as_str(),
                        thread_id = pending.thread_id.as_str(),
                        turn_id = pending.turn_id.as_deref(),
                        request_id = pending.request_id.as_str(),
                        error = %format!("{expire_error:#}"),
                        "failed to expire CLI runtime pending request after missing runtime session"
                    );
                }
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "CLI runtime request `{}` cannot be answered: {error:#}",
                            pending.request_id
                        ),
                    ),
                )
                .await;
                return;
            }
        };

        let response_json = match pioneer_crud::serialize_cli_runtime_json(&resolution) {
            Ok(payload) => Some(payload),
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to encode CLI runtime request resolution: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        let now = cli_runtime_request_timestamp();
        let status = cli_runtime_request_status_for_resolution(&resolution);
        let resolved = match self
            .crud_store
            .resolve_cli_runtime_pending_request(ResolveCliRuntimePendingRequest {
                request_id: pending.request_id.clone(),
                status,
                response_json,
                updated_at: now,
                resolved_at: now,
            })
            .await
        {
            Ok(Some(resolved)) => resolved,
            Ok(None) => {
                self.send_stale_cli_runtime_request_error(
                    connection_id,
                    request_id,
                    pending.request_id.as_str(),
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
                        format!(
                            "failed to resolve CLI runtime request `{}`: {error:#}",
                            pending.request_id
                        ),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self
            .respond_to_cli_runtime_native_request(&resolved, &resolution, native_response_session)
            .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!(
                        "failed to respond to native CLI runtime request `{}`: {error:#}",
                        resolved.request_id
                    ),
                ),
            )
            .await;
            return;
        }

        if let Some(turn_id) = resolved.turn_id.as_deref()
            && let Err(error) = self
                .timeout_supervisor
                .renew_running_attempt_deadlines_for_turn(turn_id, now_timestamp_secs())
                .await
        {
            warn!(
                workspace_id = resolved.workspace_id.as_str(),
                runtime_id = resolved.runtime_id.as_str(),
                thread_id = resolved.thread_id.as_str(),
                turn_id,
                request_id = resolved.request_id.as_str(),
                error = %format!("{error:#}"),
                "failed to renew turn item deadlines after CLI runtime user response"
            );
        }

        self.emit_cli_runtime_request_resolved(resolved.clone(), resolution.clone())
            .await;

        self.send_cli_runtime_response(
            connection_id,
            request_id,
            methods::CLI_RUNTIME_REQUEST_RESPOND,
            &CLIRuntimeRequestRespondResponse {
                workspace_id: resolved.workspace_id.clone(),
                runtime_id: resolved.runtime_id.clone(),
                request_id: resolved.request_id.clone(),
                thread_id: Some(resolved.thread_id.clone()),
                turn_id: resolved.turn_id.clone(),
                item_id: resolved.native_item_id.clone(),
                status: protocol_status_from_stored_status(resolved.status),
                resolution,
            },
        )
        .await;
    }

    #[allow(dead_code)]
    pub(super) async fn open_cli_runtime_pending_request(
        &self,
        request: NewCliRuntimePendingRequest,
    ) -> anyhow::Result<CliRuntimePendingRequestRecord> {
        let opened = self
            .crud_store
            .open_cli_runtime_pending_request(request)
            .await?;
        if opened.status == StoredCliRuntimePendingRequestStatus::Pending {
            self.emit_cli_runtime_request_opened(opened.clone()).await;
        }
        Ok(opened)
    }

    #[allow(dead_code)]
    pub(super) async fn open_codex_command_approval_request(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: &str,
        thread_id: &str,
        request: CodexCommandApprovalRequest,
    ) -> anyhow::Result<CliRuntimePendingRequestRecord> {
        self.open_codex_command_approval_request_for_turn(
            workspace_id,
            runtime_id,
            runtime_kind,
            thread_id,
            request.native_turn_id.as_deref().map(str::to_owned),
            request,
        )
        .await
    }

    async fn open_codex_command_approval_request_for_turn(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: &str,
        thread_id: &str,
        turn_id: Option<String>,
        request: CodexCommandApprovalRequest,
    ) -> anyhow::Result<CliRuntimePendingRequestRecord> {
        let payload = cli_runtime_command_approval_pending_request(&request);
        let payload_json = pioneer_crud::serialize_cli_runtime_json(&payload)
            .context("failed to serialize Codex command approval pending request")?;
        let now = cli_runtime_request_timestamp();
        self.open_cli_runtime_pending_request(NewCliRuntimePendingRequest {
            request_id: format!("cli_req_{}", generate_id(21)),
            runtime_id: runtime_id.to_owned(),
            runtime_kind: runtime_kind.to_owned(),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id,
            native_thread_id: request.native_thread_id.clone(),
            native_turn_id: request.native_turn_id.clone(),
            native_item_id: request.native_item_id.clone(),
            request_kind: cli_runtime_request_kind_as_str(CLIRuntimeRequestKind::CommandApproval)
                .to_owned(),
            payload_json,
            created_at: now,
            updated_at: now,
        })
        .await
    }

    #[allow(dead_code)]
    pub(super) async fn open_codex_file_change_approval_request(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: &str,
        thread_id: &str,
        request: CodexFileChangeApprovalRequest,
    ) -> anyhow::Result<CliRuntimePendingRequestRecord> {
        self.open_codex_file_change_approval_request_for_turn(
            workspace_id,
            runtime_id,
            runtime_kind,
            thread_id,
            request.native_turn_id.as_deref().map(str::to_owned),
            request,
        )
        .await
    }

    async fn open_codex_file_change_approval_request_for_turn(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: &str,
        thread_id: &str,
        turn_id: Option<String>,
        request: CodexFileChangeApprovalRequest,
    ) -> anyhow::Result<CliRuntimePendingRequestRecord> {
        let payload = cli_runtime_file_change_approval_pending_request(&request);
        let payload_json = pioneer_crud::serialize_cli_runtime_json(&payload)
            .context("failed to serialize Codex file change approval pending request")?;
        let now = cli_runtime_request_timestamp();
        let opened = self
            .open_cli_runtime_pending_request(NewCliRuntimePendingRequest {
                request_id: format!("cli_req_{}", generate_id(21)),
                runtime_id: runtime_id.to_owned(),
                runtime_kind: runtime_kind.to_owned(),
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id,
                native_thread_id: request.native_thread_id.clone(),
                native_turn_id: request.native_turn_id.clone(),
                native_item_id: request.native_item_id.clone(),
                request_kind: cli_runtime_request_kind_as_str(
                    CLIRuntimeRequestKind::FileChangeApproval,
                )
                .to_owned(),
                payload_json,
                created_at: now,
                updated_at: now,
            })
            .await?;

        if opened.status == StoredCliRuntimePendingRequestStatus::Pending {
            self.emit_cli_runtime_file_change_approval_timeline_update(
                &opened,
                "approval_requested",
            )
            .await;
        }
        Ok(opened)
    }

    #[allow(dead_code)]
    pub(super) async fn open_codex_user_input_request(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: &str,
        thread_id: &str,
        request: CodexUserInputRequest,
    ) -> anyhow::Result<CliRuntimePendingRequestRecord> {
        self.open_codex_user_input_request_for_turn(
            workspace_id,
            runtime_id,
            runtime_kind,
            thread_id,
            request.native_turn_id.as_deref().map(str::to_owned),
            request,
        )
        .await
    }

    async fn open_codex_user_input_request_for_turn(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: &str,
        thread_id: &str,
        turn_id: Option<String>,
        request: CodexUserInputRequest,
    ) -> anyhow::Result<CliRuntimePendingRequestRecord> {
        let payload = cli_runtime_user_input_pending_request(&request);
        let payload_json = pioneer_crud::serialize_cli_runtime_json(&payload)
            .context("failed to serialize Codex user input pending request")?;
        let now = cli_runtime_request_timestamp();
        self.open_cli_runtime_pending_request(NewCliRuntimePendingRequest {
            request_id: format!("cli_req_{}", generate_id(21)),
            runtime_id: runtime_id.to_owned(),
            runtime_kind: runtime_kind.to_owned(),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id,
            native_thread_id: request.native_thread_id.clone(),
            native_turn_id: request.native_turn_id.clone(),
            native_item_id: request.native_item_id.clone(),
            request_kind: cli_runtime_request_kind_as_str(CLIRuntimeRequestKind::UserInput)
                .to_owned(),
            payload_json,
            created_at: now,
            updated_at: now,
        })
        .await
    }

    pub(crate) async fn ensure_cli_runtime_session_event_pumps(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        session: Arc<dyn CLIAgentRuntimeSession>,
        debug_native_events: bool,
    ) {
        let event_receivers = session.take_event_receivers();
        let codex_receivers = session.take_codex_event_receivers();
        if event_receivers.is_none() && codex_receivers.is_none() {
            return;
        }

        self.ensure_cli_runtime_execution_event_hub(key).await;
        if let Some(receivers) = event_receivers {
            self.spawn_cli_runtime_event_pump(key.clone(), receivers, debug_native_events);
        }
        if let Some(receivers) = codex_receivers {
            self.spawn_codex_event_pumps(key.clone(), receivers, debug_native_events);
        }
    }

    async fn ensure_cli_runtime_execution_event_hub(
        &self,
        key: &CLIAgentRuntimeSessionKey,
    ) -> Arc<ExecutionEventHub> {
        let mut hubs = self.cli_runtime_event_hubs.lock().await;
        if let Some(hub) = hubs.get(key) {
            return hub.clone();
        }

        let hub = Arc::new(ExecutionEventHub::new());
        let durable_receiver = hub
            .take_durable_receiver()
            .await
            .expect("new execution event hub must expose its durable receiver");
        let snapshot_receiver = hub
            .take_snapshot_receiver()
            .await
            .expect("new execution event hub must expose its snapshot receiver");
        let live_receiver = hub.subscribe_live();
        hubs.insert(key.clone(), hub.clone());
        drop(hubs);

        self.spawn_cli_runtime_execution_event_listener(
            key.clone(),
            durable_receiver,
            snapshot_receiver,
            live_receiver,
        );
        hub
    }

    fn spawn_cli_runtime_execution_event_listener(
        &self,
        key: CLIAgentRuntimeSessionKey,
        mut durable_receiver: pioneer_runtime_events::DurableEventReceiver,
        mut snapshot_receiver: pioneer_runtime_events::SnapshotEventReceiver,
        mut live_receiver: tokio::sync::broadcast::Receiver<AgentProgressEvent>,
    ) {
        let processor = self.clone();
        tokio::spawn(async move {
            let mut durable_open = true;
            let mut snapshot_open = true;
            let mut live_open = true;
            while durable_open || snapshot_open || live_open {
                tokio::select! {
                    biased;
                    snapshot = snapshot_receiver.recv(), if snapshot_open => {
                        match snapshot {
                            Some(event) => processor.handle_snapshot_agent_event(event).await,
                            None => snapshot_open = false,
                        }
                    }
                    live = live_receiver.recv(), if live_open => {
                        match live {
                            Ok(event) => processor.handle_progress_agent_event(event).await,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(
                                    workspace_id = key.workspace_id.as_str(),
                                    runtime_id = key.runtime_id.as_str(),
                                    thread_id = key.thread_id.as_str(),
                                    skipped,
                                    "CLI runtime live progress listener lagged"
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                live_open = false;
                            }
                        }
                    }
                    durable = durable_receiver.recv(), if durable_open => {
                        match durable {
                            Some(event) => {
                                let committed = processor.handle_durable_agent_event(event).await;
                                durable_receiver.acknowledge_last(if committed {
                                    Ok(())
                                } else {
                                    Err("gateway failed to commit durable CLI runtime event".to_owned())
                                });
                            }
                            None => durable_open = false,
                        }
                    }
                }
            }
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                "CLI runtime execution event listener closed"
            );
        });
    }

    async fn close_cli_runtime_execution_event_hub(&self, key: &CLIAgentRuntimeSessionKey) {
        let hub = self.cli_runtime_event_hubs.lock().await.remove(key);
        if let Some(hub) = hub {
            hub.shutdown_progress().await;
        }
        self.cli_runtime_turn_binding_cache
            .lock()
            .await
            .retain(|cached, _| {
                cached.workspace_id != key.workspace_id
                    || cached.runtime_id != key.runtime_id
                    || cached.thread_id != key.thread_id
            });
    }

    fn spawn_cli_runtime_event_pump(
        &self,
        key: CLIAgentRuntimeSessionKey,
        receivers: crate::cli_runtime::manager::CLIAgentRuntimeEventReceivers,
        debug_native_events: bool,
    ) {
        let processor = self.clone();
        tokio::spawn(async move {
            let mut events = receivers.events;
            let runtime_kind = receivers.runtime_kind;
            while let Some(event) = events.recv().await {
                if cli_runtime_event_requires_native_journal(&event) {
                    processor
                        .persist_cli_runtime_canonical_event(
                            &key,
                            runtime_kind.as_str(),
                            &event,
                            debug_native_events,
                        )
                        .await;
                }
                processor.handle_cli_runtime_event(&key, event).await;
            }
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                "CLI runtime canonical event pump closed"
            );
            processor.close_cli_runtime_execution_event_hub(&key).await;
            processor
                .remove_cli_runtime_command_items_for_session(&key)
                .await;
        });
    }

    fn spawn_codex_event_pumps(
        &self,
        key: CLIAgentRuntimeSessionKey,
        receivers: CLIAgentRuntimeCodexEventReceivers,
        debug_native_events: bool,
    ) {
        let options = RuntimeEventMappingOptions {
            include_redacted_native_payload: debug_native_events,
        };

        let notification_processor = self.clone();
        let notification_key = key.clone();
        tokio::spawn(async move {
            let mut notifications = receivers.notifications;
            while let Some(notification) = notifications.recv().await {
                let event = map_codex_notification_event(&notification, options);
                if cli_runtime_event_requires_native_journal(&event) {
                    notification_processor
                        .persist_codex_native_transport_event(
                            &notification_key,
                            "notification",
                            notification.method.as_str(),
                            notification.params.as_ref(),
                            debug_native_events,
                        )
                        .await;
                }
                notification_processor
                    .handle_cli_runtime_timeline_event(&notification_key, event)
                    .await;
            }
            debug!(
                workspace_id = notification_key.workspace_id.as_str(),
                runtime_id = notification_key.runtime_id.as_str(),
                thread_id = notification_key.thread_id.as_str(),
                "Codex CLI runtime notification pump closed"
            );
            notification_processor
                .close_cli_runtime_execution_event_hub(&notification_key)
                .await;
            notification_processor
                .remove_cli_runtime_command_items_for_session(&notification_key)
                .await;
        });

        let request_processor = self.clone();
        let request_key = key.clone();
        tokio::spawn(async move {
            let mut server_requests = receivers.server_requests;
            while let Some(request) = server_requests.recv().await {
                request_processor
                    .persist_codex_native_transport_event(
                        &request_key,
                        "server_request",
                        request.method.as_str(),
                        request.params.as_ref(),
                        debug_native_events,
                    )
                    .await;
                let event = map_codex_server_request_event(&request, options);
                request_processor
                    .handle_cli_runtime_codex_server_request(&request_key, request, event)
                    .await;
            }
            debug!(
                workspace_id = request_key.workspace_id.as_str(),
                runtime_id = request_key.runtime_id.as_str(),
                thread_id = request_key.thread_id.as_str(),
                "Codex CLI runtime server request pump closed"
            );
        });

        let diagnostic_key = key;
        tokio::spawn(async move {
            let mut diagnostics = receivers.diagnostics;
            while let Some(diagnostic) = diagnostics.recv().await {
                warn!(
                    workspace_id = diagnostic_key.workspace_id.as_str(),
                    runtime_id = diagnostic_key.runtime_id.as_str(),
                    thread_id = diagnostic_key.thread_id.as_str(),
                    kind = ?diagnostic.kind,
                    method = diagnostic.method.as_deref().unwrap_or("<none>"),
                    message = sanitize_runtime_diagnostic_line(diagnostic.message.as_str()),
                    "Codex CLI runtime JSONL-RPC diagnostic"
                );
            }
        });
    }

    async fn persist_codex_native_transport_event(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        transport_kind: &str,
        native_method: &str,
        params: Option<&JsonValue>,
        include_payload: bool,
    ) {
        let native_thread_id = params.and_then(|params| {
            json_string_path(params, &["threadId"])
                .or_else(|| json_string_path(params, &["thread_id"]))
        });
        let native_turn_id = params.and_then(|params| {
            json_string_path(params, &["turnId"]).or_else(|| json_string_path(params, &["turn_id"]))
        });
        let native_item_id = params.and_then(|params| {
            json_string_path(params, &["itemId"])
                .or_else(|| json_string_path(params, &["item_id"]))
                .or_else(|| json_string_path(params, &["callId"]))
                .or_else(|| json_string_path(params, &["call_id"]))
                .or_else(|| json_string_path(params, &["item", "id"]))
        });
        let turn_id = if let Some(native_turn_id) = native_turn_id.as_deref() {
            self.cli_runtime_pioneer_turn_id_for_native_turn(
                key,
                native_thread_id.as_deref(),
                Some(native_turn_id),
            )
            .await
        } else {
            None
        };
        let payload = if include_payload {
            json!({
                "transportKind": transport_kind,
                "nativeItemId": native_item_id,
                "params": params.map(redact_cli_runtime_native_payload),
            })
        } else {
            json!({
                "transportKind": transport_kind,
                "nativeItemId": native_item_id,
            })
        };
        let payload_redacted_json = match pioneer_crud::serialize_cli_runtime_json(&payload) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    method = native_method,
                    error = %format!("{error:#}"),
                    "failed to serialize Codex native event payload"
                );
                return;
            }
        };
        if let Err(error) = self
            .crud_store
            .append_cli_runtime_native_event(NewCliRuntimeNativeEvent {
                id: generate_id(24),
                runtime_id: key.runtime_id.clone(),
                runtime_kind: "codex".to_owned(),
                workspace_id: Some(key.workspace_id.clone()),
                thread_id: Some(key.thread_id.clone()),
                turn_id,
                native_thread_id,
                native_turn_id,
                native_method: native_method.to_owned(),
                payload_redacted_json,
                sequence: now_timestamp_millis(),
                created_at: chrono::Utc::now().fixed_offset(),
            })
            .await
        {
            warn!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                method = native_method,
                error = %format!("{error:#}"),
                "failed to persist Codex native event"
            );
        }
    }

    async fn persist_cli_runtime_canonical_event(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        runtime_kind: &str,
        event: &RuntimeEvent,
        include_payload: bool,
    ) {
        let native_thread_id = cli_runtime_native_thread_id_for_event(event).map(str::to_owned);
        let native_turn_id = cli_runtime_native_turn_id_for_event(event).map(str::to_owned);
        let native_item_id = cli_runtime_native_item_id_for_event(event).map(str::to_owned);
        let turn_id = self
            .cli_runtime_pioneer_turn_id_for_native_turn(
                key,
                native_thread_id.as_deref(),
                native_turn_id.as_deref(),
            )
            .await;
        let payload = if include_payload {
            serde_json::to_value(event).unwrap_or_else(|_| json!({ "event": "unserializable" }))
        } else {
            json!({
                "event": cli_runtime_event_log_label(event),
                "nativeItemId": native_item_id,
            })
        };
        let payload_redacted_json = match pioneer_crud::serialize_cli_runtime_json(&payload) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to serialize CLI runtime canonical event"
                );
                return;
            }
        };
        if let Err(error) = self
            .crud_store
            .append_cli_runtime_native_event(NewCliRuntimeNativeEvent {
                id: generate_id(24),
                runtime_id: key.runtime_id.clone(),
                runtime_kind: runtime_kind.to_owned(),
                workspace_id: Some(key.workspace_id.clone()),
                thread_id: Some(key.thread_id.clone()),
                turn_id,
                native_thread_id,
                native_turn_id,
                native_method: cli_runtime_event_log_label(event),
                payload_redacted_json,
                sequence: now_timestamp_millis(),
                created_at: chrono::Utc::now().fixed_offset(),
            })
            .await
        {
            warn!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist CLI runtime canonical event"
            );
        }
    }

    pub(super) async fn handle_cli_runtime_timeline_event(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        event: RuntimeEvent,
    ) {
        match &event {
            RuntimeEvent::RequestResolved(_) | RuntimeEvent::RequestOpened(_) => return,
            _ => {}
        }

        let Some(native_turn_id) = cli_runtime_native_turn_id_for_event(&event).map(str::to_owned)
        else {
            let Some(turn_binding) = self
                .cli_runtime_running_turn_binding_for_timeline_event(key, &event)
                .await
            else {
                return;
            };
            self.process_bound_cli_runtime_event(key, turn_binding, event)
                .await;
            return;
        };
        let Some(native_thread_id) =
            cli_runtime_native_thread_id_for_event(&event).map(str::to_owned)
        else {
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                native_turn_id = native_turn_id.as_str(),
                "ignored CLI runtime event without native thread id"
            );
            return;
        };
        let Some(turn_binding) = self
            .cli_runtime_turn_binding_for_native_turn(
                key,
                native_thread_id.as_str(),
                native_turn_id.as_str(),
            )
            .await
        else {
            if !self
                .cli_runtime_has_starting_turn_binding_without_native_turn(
                    key,
                    native_thread_id.as_str(),
                )
                .await
            {
                debug!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    native_thread_id,
                    native_turn_id = native_turn_id.as_str(),
                    "ignored CLI runtime event without matching Pioneer turn binding"
                );
                return;
            }
            self.buffer_cli_runtime_event_until_turn_binding(
                key,
                native_thread_id.as_str(),
                native_turn_id.as_str(),
                event,
            )
            .await;
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                native_turn_id = native_turn_id.as_str(),
                "buffering CLI runtime event before Pioneer turn binding is available"
            );
            return;
        };

        self.process_bound_cli_runtime_event(key, turn_binding, event)
            .await;
    }

    pub(super) async fn handle_cli_runtime_event(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        event: RuntimeEvent,
    ) {
        match event {
            RuntimeEvent::RequestOpened(request) => {
                self.handle_cli_runtime_request_opened_event(key, request)
                    .await;
            }
            RuntimeEvent::RequestResolved(resolved) => {
                self.handle_cli_runtime_request_resolved_event(key, resolved)
                    .await;
            }
            other => self.handle_cli_runtime_timeline_event(key, other).await,
        }
    }

    async fn handle_cli_runtime_request_opened_event(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        request: pioneer_cli_agent_runtime::event::RuntimeRequestOpened,
    ) {
        let Some(turn_binding) = self
            .cli_runtime_turn_binding_for_native_turn_option(
                key,
                request.native_thread_id.as_deref(),
                request.native_turn_id.as_deref(),
            )
            .await
        else {
            if let (Some(native_thread_id), Some(native_turn_id)) = (
                request.native_thread_id.clone(),
                request.native_turn_id.clone(),
            ) {
                self.buffer_cli_runtime_event_until_turn_binding(
                    key,
                    native_thread_id.as_str(),
                    native_turn_id.as_str(),
                    RuntimeEvent::RequestOpened(request),
                )
                .await;
                debug!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    native_thread_id,
                    native_turn_id,
                    "buffering CLI runtime request before Pioneer turn binding is available"
                );
                return;
            }
            warn!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                native_thread_id = request.native_thread_id.as_deref(),
                native_turn_id = request.native_turn_id.as_deref(),
                native_request_id = request.native_request_id.as_str(),
                "ignored CLI runtime request without matching Pioneer turn binding"
            );
            return;
        };
        self.open_bound_cli_runtime_request(key, turn_binding, request)
            .await;
    }

    async fn handle_cli_runtime_request_resolved_event(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        resolved: RuntimeRequestResolved,
    ) {
        let pending_requests = match self
            .crud_store
            .list_cli_runtime_pending_requests(pioneer_crud::CliRuntimePendingRequestListFilter {
                workspace_id: Some(key.workspace_id.clone()),
                runtime_id: Some(key.runtime_id.clone()),
                thread_id: Some(key.thread_id.clone()),
                status: Some(StoredCliRuntimePendingRequestStatus::Pending),
                limit: None,
                ..Default::default()
            })
            .await
        {
            Ok(requests) => requests,
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    native_request_id = resolved.native_request_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to list pending CLI runtime requests for native cancellation"
                );
                return;
            }
        };

        let Some(request) = pending_requests.into_iter().find(|request| {
            cli_runtime_pending_request_from_record(request)
                .native_request_id
                .as_deref()
                == Some(resolved.native_request_id.as_str())
        }) else {
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                native_request_id = resolved.native_request_id.as_str(),
                "ignored CLI runtime native request resolution without matching pending request"
            );
            return;
        };

        let resolution = CLIRuntimeRequestResolution::Cancelled;
        let response_json = match pioneer_crud::serialize_cli_runtime_json(&resolution) {
            Ok(response_json) => Some(response_json),
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    request_id = request.request_id.as_str(),
                    native_request_id = resolved.native_request_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to serialize cancelled CLI runtime request resolution"
                );
                return;
            }
        };
        let now = cli_runtime_request_timestamp();
        let cancelled = match self
            .crud_store
            .cancel_cli_runtime_pending_request(request.request_id.as_str(), response_json, now)
            .await
        {
            Ok(cancelled) => cancelled,
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    request_id = request.request_id.as_str(),
                    native_request_id = resolved.native_request_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to cancel CLI runtime pending request after native resolution"
                );
                return;
            }
        };

        if let Some(cancelled) = cancelled {
            self.emit_cli_runtime_request_resolved(cancelled.clone(), resolution.clone())
                .await;
            if cancelled.request_kind
                == cli_runtime_request_kind_as_str(CLIRuntimeRequestKind::FileChangeApproval)
            {
                self.emit_cli_runtime_file_change_approval_timeline_update(
                    &cancelled,
                    file_change_approval_timeline_status_for_resolution(&resolution),
                )
                .await;
            }
        }
    }

    async fn open_bound_cli_runtime_request(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        turn_binding: pioneer_crud::CliRuntimeTurnBindingRecord,
        request: pioneer_cli_agent_runtime::event::RuntimeRequestOpened,
    ) {
        if !self
            .cli_runtime_turn_binding_accepts_native_activity(
                key,
                &turn_binding,
                request.native_thread_id.as_deref(),
                request.request_kind.as_str(),
            )
            .await
        {
            return;
        }
        let request_kind = cli_runtime_request_kind_from_stored(request.request_kind.as_str());
        if request_kind == CLIRuntimeRequestKind::Other {
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                native_request_id = request.native_request_id.as_str(),
                request_kind = request.request_kind.as_str(),
                "ignored unsupported CLI runtime request kind"
            );
            return;
        }
        let payload = cli_runtime_pending_request_from_runtime_event(&request, request_kind);
        let payload_json = match pioneer_crud::serialize_cli_runtime_json(&payload) {
            Ok(payload_json) => payload_json,
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    native_request_id = request.native_request_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to serialize CLI runtime pending request"
                );
                return;
            }
        };
        let now = cli_runtime_request_timestamp();
        if let Err(error) = self
            .open_cli_runtime_pending_request(NewCliRuntimePendingRequest {
                request_id: format!("cli_req_{}", generate_id(21)),
                runtime_id: key.runtime_id.clone(),
                runtime_kind: turn_binding.runtime_kind,
                workspace_id: key.workspace_id.clone(),
                thread_id: key.thread_id.clone(),
                turn_id: Some(turn_binding.turn_id),
                native_thread_id: request.native_thread_id,
                native_turn_id: request.native_turn_id,
                native_item_id: request.native_item_id,
                request_kind: cli_runtime_request_kind_as_str(request_kind).to_owned(),
                payload_json,
                created_at: now,
                updated_at: now,
            })
            .await
        {
            warn!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                native_request_id = request.native_request_id.as_str(),
                error = %format!("{error:#}"),
                "failed to open CLI runtime pending request"
            );
        }
    }

    async fn process_bound_cli_runtime_event(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        turn_binding: pioneer_crud::CliRuntimeTurnBindingRecord,
        event: RuntimeEvent,
    ) -> bool {
        let native_turn_id = cli_runtime_native_turn_id_for_event(&event);
        let native_turn_id_label = native_turn_id.unwrap_or("<none>");
        let native_thread_id = cli_runtime_native_thread_id_for_event(&event);
        let event_label = cli_runtime_event_log_label(&event);
        let terminal_status = cli_runtime_turn_status_for_terminal_event(&event);
        if !self
            .cli_runtime_turn_binding_accepts_native_activity(
                key,
                &turn_binding,
                native_thread_id,
                event_label.as_str(),
            )
            .await
        {
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                turn_id = turn_binding.turn_id.as_str(),
                native_turn_id = native_turn_id_label,
                event = %event_label,
                binding_status = turn_binding.status.as_str(),
                "ignored CLI runtime event for non-active Pioneer turn"
            );
            return false;
        }
        let context = crate::cli_runtime::projector::CLIRuntimeProjectorContext {
            workspace_id: key.workspace_id.clone(),
            thread_id: key.thread_id.clone(),
            turn_id: turn_binding.turn_id.clone(),
        };
        let event_hub = self.ensure_cli_runtime_execution_event_hub(key).await;
        let projected = crate::cli_runtime::projector::project_cli_runtime_event(&context, &event);
        if terminal_status.is_some()
            && let Some(native_turn_id) = native_turn_id
        {
            self.commit_cli_runtime_final_diff_snapshot(key, &turn_binding, native_turn_id)
                .await;
        }
        for durable in projected.durable {
            if let Err(error) = event_hub.publish_durable_and_wait(durable).await {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    turn_id = turn_binding.turn_id.as_str(),
                    native_turn_id = native_turn_id_label,
                    event = %event_label,
                    error = %error,
                    "failed to commit CLI runtime durable event"
                );
                if let Some(status) = terminal_status {
                    self.cleanup_cli_runtime_terminal_turn_status(
                        &turn_binding,
                        status,
                        event_label.as_str(),
                    )
                    .await;
                }
                return false;
            }
        }
        for snapshot in projected.snapshot {
            if !event_hub.publish_snapshot(snapshot) {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    turn_id = turn_binding.turn_id.as_str(),
                    event = %event_label,
                    "failed to enqueue CLI runtime snapshot because execution hub is closed"
                );
                if let Some(status) = terminal_status {
                    self.cleanup_cli_runtime_terminal_turn_status(
                        &turn_binding,
                        status,
                        event_label.as_str(),
                    )
                    .await;
                }
                return false;
            }
        }
        for progress in projected.progress {
            event_hub.publish_progress(progress);
        }
        self.update_cli_runtime_command_item_registry(key, &turn_binding, &event)
            .await;
        if terminal_status.is_some() {
            match self
                .cli_runtime_turn_status_for_binding(&turn_binding)
                .await
            {
                Ok(Some(status)) if status != TurnStatus::InProgress => {
                    self.cleanup_cli_runtime_terminal_turn_status(
                        &turn_binding,
                        status,
                        event_label.as_str(),
                    )
                    .await;
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(
                        workspace_id = key.workspace_id.as_str(),
                        runtime_id = key.runtime_id.as_str(),
                        thread_id = key.thread_id.as_str(),
                        turn_id = turn_binding.turn_id.as_str(),
                        event = %event_label,
                        error = %format!("{error:#}"),
                        "failed to reconcile Pioneer turn status after CLI runtime terminal event"
                    );
                }
            }
        }
        true
    }

    async fn commit_cli_runtime_final_diff_snapshot(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        turn_binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        native_turn_id: &str,
    ) {
        let item_id =
            crate::cli_runtime::projector::agent_diff_item_id_for_native_turn_id(native_turn_id);
        let item = match self
            .crud_store
            .get_turn_item(turn_binding.turn_id.as_str(), item_id.as_str())
            .await
        {
            Ok(Some(item)) => item,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    turn_id = turn_binding.turn_id.as_str(),
                    item_id = item_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to load CLI runtime final diff snapshot item"
                );
                return;
            }
        };

        let event_timestamp = now_timestamp_secs();
        match message_future(
            self.crud_store
                .materialize_agent_diff_final_snapshot_if_changed(
                    ItemCompletedNotification {
                        workspace_id: key.workspace_id.clone(),
                        thread_id: key.thread_id.clone(),
                        turn_id: turn_binding.turn_id.clone(),
                        item,
                    },
                    event_timestamp,
                ),
        )
        .await
        {
            Ok(true) => {
                debug!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    turn_id = turn_binding.turn_id.as_str(),
                    item_id = item_id.as_str(),
                    "committed final CLI runtime diff snapshot"
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    turn_id = turn_binding.turn_id.as_str(),
                    item_id = item_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to commit final CLI runtime diff snapshot"
                );
            }
        }
    }

    pub(super) async fn flush_cli_runtime_events_for_native_turn(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: &str,
        native_turn_id: &str,
    ) {
        let pending_key = CLIRuntimePendingTurnEventKey {
            workspace_id: key.workspace_id.clone(),
            runtime_id: key.runtime_id.clone(),
            thread_id: key.thread_id.clone(),
            native_thread_id: native_thread_id.to_owned(),
            native_turn_id: native_turn_id.to_owned(),
        };
        let pending = self
            .cli_runtime_pending_turn_events
            .lock()
            .await
            .remove(&pending_key)
            .unwrap_or_default();
        let pending_requests = self
            .cli_runtime_pending_turn_server_requests
            .lock()
            .await
            .remove(&pending_key)
            .unwrap_or_default();
        if pending.is_empty() && pending_requests.is_empty() {
            return;
        }

        let Some(turn_binding) = self
            .cli_runtime_turn_binding_for_native_turn(key, native_thread_id, native_turn_id)
            .await
        else {
            if !pending.is_empty() {
                let mut buffer = self.cli_runtime_pending_turn_events.lock().await;
                buffer
                    .entry(pending_key.clone())
                    .or_default()
                    .extend(pending);
            }
            if !pending_requests.is_empty() {
                let mut buffer = self.cli_runtime_pending_turn_server_requests.lock().await;
                buffer
                    .entry(pending_key)
                    .or_default()
                    .extend(pending_requests);
            }
            return;
        };

        enum PendingTurnActivity {
            Event(CLIRuntimePendingTurnEvent),
            ServerRequest(CLIRuntimePendingTurnServerRequest),
        }

        impl PendingTurnActivity {
            fn received_sequence(&self) -> u64 {
                match self {
                    Self::Event(event) => event.received_sequence,
                    Self::ServerRequest(request) => request.received_sequence,
                }
            }
        }

        let mut activities: Vec<PendingTurnActivity> = pending
            .into_iter()
            .map(PendingTurnActivity::Event)
            .chain(
                pending_requests
                    .into_iter()
                    .map(PendingTurnActivity::ServerRequest),
            )
            .collect();
        activities.sort_by_key(PendingTurnActivity::received_sequence);

        for activity in activities {
            match activity {
                PendingTurnActivity::Event(pending_event) => {
                    debug!(
                        workspace_id = key.workspace_id.as_str(),
                        runtime_id = key.runtime_id.as_str(),
                        thread_id = key.thread_id.as_str(),
                        native_thread_id,
                        native_turn_id,
                        received_at_unix_ms = pending_event.received_at_unix_ms,
                        received_sequence = pending_event.received_sequence,
                        "flushing buffered CLI runtime event"
                    );
                    match pending_event.event {
                        RuntimeEvent::RequestOpened(request) => {
                            self.open_bound_cli_runtime_request(key, turn_binding.clone(), request)
                                .await;
                        }
                        event => {
                            self.process_bound_cli_runtime_event(key, turn_binding.clone(), event)
                                .await;
                        }
                    }
                }
                PendingTurnActivity::ServerRequest(pending_request) => {
                    debug!(
                        workspace_id = key.workspace_id.as_str(),
                        runtime_id = key.runtime_id.as_str(),
                        thread_id = key.thread_id.as_str(),
                        native_thread_id,
                        native_turn_id,
                        method = pending_request.request.method.as_str(),
                        received_at_unix_ms = pending_request.received_at_unix_ms,
                        received_sequence = pending_request.received_sequence,
                        "flushing buffered CLI runtime server request"
                    );
                    self.handle_cli_runtime_codex_server_request(
                        key,
                        pending_request.request,
                        pending_request.event,
                    )
                    .await;
                }
            }
        }
    }

    pub(super) async fn fail_stale_cli_runtime_turns(&self, now_unix_ms: i64) {
        let bindings = match self
            .crud_store
            .list_cli_runtime_turn_bindings(pioneer_crud::CliRuntimeTurnBindingListFilter {
                statuses: vec![
                    crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_STARTING.to_owned(),
                    crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING.to_owned(),
                ],
                limit: Some(CLI_RUNTIME_STALE_TURN_SCAN_LIMIT),
                ..Default::default()
            })
            .await
        {
            Ok(bindings) => bindings,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to scan stale CLI runtime turn bindings"
                );
                return;
            }
        };

        for binding in bindings {
            if let Err(error) = self
                .fail_stale_cli_runtime_turn_binding(binding, now_unix_ms)
                .await
            {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to reconcile stale CLI runtime turn binding"
                );
            }
        }
        self.prune_stale_cli_runtime_pending_turn_events(now_unix_ms)
            .await;
        self.prune_stale_cli_runtime_pending_turn_server_requests(now_unix_ms)
            .await;
    }

    pub(super) async fn reconcile_cli_runtime_human_wait_for_turn(
        &self,
        turn_id: &str,
        now_unix_ms: i64,
        source: &str,
    ) -> anyhow::Result<bool> {
        let pending_requests = self
            .crud_store
            .list_cli_runtime_pending_requests(pioneer_crud::CliRuntimePendingRequestListFilter {
                turn_id: Some(turn_id.to_owned()),
                status: Some(StoredCliRuntimePendingRequestStatus::Pending),
                limit: None,
                ..Default::default()
            })
            .await?;

        let mut human_requests = pending_requests
            .into_iter()
            .filter(|request| {
                cli_runtime_pending_request_is_human_wait(request.request_kind.as_str())
            })
            .collect::<Vec<_>>();
        if human_requests.is_empty() {
            return Ok(false);
        }

        let Some(binding) = self
            .crud_store
            .get_cli_runtime_turn_binding(turn_id)
            .await
            .with_context(|| {
                format!(
                    "failed to load CLI runtime turn binding while reconciling human wait for turn `{turn_id}`"
                )
            })?
        else {
            debug!(
                turn_id,
                source,
                request_count = human_requests.len(),
                "ignoring CLI runtime human wait for turn without CLI runtime binding"
            );
            return Ok(false);
        };
        if !cli_runtime_turn_binding_status_is_active(binding.status.as_str()) {
            self.cleanup_cli_runtime_terminal_binding_status(
                &binding,
                "CLI runtime human wait reconciliation",
            )
            .await;
            return Ok(false);
        }
        human_requests.retain(|request| {
            request.workspace_id == binding.workspace_id
                && request.runtime_id == binding.runtime_id
                && request.thread_id == binding.thread_id
                && request.turn_id.as_deref() == Some(binding.turn_id.as_str())
        });
        if human_requests.is_empty() {
            debug!(
                turn_id,
                source,
                binding_workspace_id = binding.workspace_id.as_str(),
                binding_runtime_id = binding.runtime_id.as_str(),
                binding_thread_id = binding.thread_id.as_str(),
                "ignoring CLI runtime human wait requests that do not match the active turn binding"
            );
            return Ok(false);
        }

        human_requests.sort_by_key(|request| request.created_at);
        let expired = human_requests
            .iter()
            .filter(|request| {
                now_unix_ms.saturating_sub(request.created_at.timestamp_millis())
                    >= CLI_RUNTIME_HUMAN_RESPONSE_TIMEOUT_MS
            })
            .collect::<Vec<_>>();
        if expired.is_empty() {
            debug!(
                turn_id,
                source,
                request_count = human_requests.len(),
                "deferred turn timeout while waiting for CLI runtime user response"
            );
            return Ok(true);
        }

        let first_expired = expired[0];
        let wait_ms = now_unix_ms.saturating_sub(first_expired.created_at.timestamp_millis());
        let reason = format!(
            "CLI runtime turn `{turn_id}` was blocked because pending user response request `{}` ({}) was not answered within {} ms",
            first_expired.request_id,
            first_expired.request_kind,
            CLI_RUNTIME_HUMAN_RESPONSE_TIMEOUT_MS
        );
        warn!(
            workspace_id = first_expired.workspace_id.as_str(),
            runtime_id = first_expired.runtime_id.as_str(),
            thread_id = first_expired.thread_id.as_str(),
            turn_id,
            request_id = first_expired.request_id.as_str(),
            request_kind = first_expired.request_kind.as_str(),
            wait_ms,
            source,
            "blocking CLI runtime turn after human response timeout"
        );
        if !self
            .mark_turn_blocked(first_expired.thread_id.clone(), turn_id.to_owned(), reason)
            .await
        {
            anyhow::bail!(
                "failed to mark CLI runtime turn `{turn_id}` blocked after human response timeout for request `{}`",
                first_expired.request_id
            );
        }
        Ok(true)
    }

    pub(super) async fn renew_active_cli_runtime_turn_deadlines(
        &self,
        turn_id: &str,
        now_unix: i64,
    ) -> anyhow::Result<bool> {
        let Some(binding) = self
            .crud_store
            .get_cli_runtime_turn_binding(turn_id)
            .await?
        else {
            return Ok(false);
        };
        if !cli_runtime_turn_binding_status_is_active(binding.status.as_str()) {
            return Ok(false);
        }
        let Some((workspace_id, turn)) = (if let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(binding.thread_id.as_str(), binding.turn_id.as_str())
            .await
        {
            Some((workspace_id, turn))
        } else {
            self.crud_store
                .get_turn(binding.thread_id.as_str(), binding.turn_id.as_str())
                .await?
        }) else {
            return Ok(false);
        };
        if turn.status != TurnStatus::InProgress {
            self.cleanup_cli_runtime_terminal_turn_status(
                &binding,
                turn.status,
                "timeout supervisor observed terminal Pioneer turn",
            )
            .await;
            return Ok(false);
        }

        match self
            .reconcile_cli_runtime_turn_from_runtime(
                &binding,
                now_unix.saturating_mul(1_000),
                workspace_id.as_str(),
                &turn,
            )
            .await
        {
            Ok(reconciled) => Ok(reconciled),
            Err(error) => {
                warn!(
                    workspace_id = binding.workspace_id.as_str(),
                    runtime_id = binding.runtime_id.as_str(),
                    thread_id = binding.thread_id.as_str(),
                    turn_id = binding.turn_id.as_str(),
                    error = %format!("{error:#}"),
                    "runtime observation failed while evaluating item timeout; deferring destructive timeout transition"
                );
                Ok(true)
            }
        }
    }

    async fn fail_stale_cli_runtime_turn_binding(
        &self,
        binding: pioneer_crud::CliRuntimeTurnBindingRecord,
        now_unix_ms: i64,
    ) -> anyhow::Result<()> {
        let (workspace_id, turn) = if let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(binding.thread_id.as_str(), binding.turn_id.as_str())
            .await
        {
            (workspace_id, turn)
        } else {
            let Some((workspace_id, turn)) = self
                .crud_store
                .get_turn(binding.thread_id.as_str(), binding.turn_id.as_str())
                .await?
            else {
                return Ok(());
            };
            (workspace_id, turn)
        };
        if turn.status != TurnStatus::InProgress {
            self.cleanup_cli_runtime_terminal_turn_status(
                &binding,
                turn.status,
                "stale CLI runtime turn scan",
            )
            .await;
            return Ok(());
        }

        if self
            .reconcile_cli_runtime_human_wait_for_turn(
                binding.turn_id.as_str(),
                now_unix_ms,
                "CLI runtime stale turn scan",
            )
            .await?
        {
            return Ok(());
        }

        let latest_native_event_ms = self
            .latest_cli_runtime_native_event_timestamp_ms(&binding)
            .await?;
        let turn_liveness_ms = self
            .crud_store
            .get_turn_liveness(binding.turn_id.as_str())
            .await?
            .map(|liveness| liveness.last_activity_at_unix.saturating_mul(1_000));
        let observed_activity_ms = latest_native_event_ms
            .into_iter()
            .chain(turn_liveness_ms)
            .max();
        let last_activity_ms =
            observed_activity_ms.unwrap_or(binding.updated_at.timestamp_millis());
        let stale_after_ms = if observed_activity_ms.is_some() {
            CLI_RUNTIME_EVENTED_TURN_STALE_AFTER_MS
        } else {
            CLI_RUNTIME_SILENT_TURN_STALE_AFTER_MS
        };
        if now_unix_ms.saturating_sub(last_activity_ms) < stale_after_ms {
            return Ok(());
        }

        if self
            .reconcile_cli_runtime_turn_from_runtime(
                &binding,
                now_unix_ms,
                workspace_id.as_str(),
                &turn,
            )
            .await?
        {
            return Ok(());
        }

        let message = if let Some(native_turn_id) = binding.native_turn_id.as_deref() {
            if observed_activity_ms.is_some() {
                format!(
                    "CLI runtime turn `{}` stopped emitting native events for native turn `{native_turn_id}` and was marked failed after {} ms of inactivity",
                    binding.turn_id, stale_after_ms
                )
            } else {
                format!(
                    "CLI runtime turn `{}` did not emit any native events for native turn `{native_turn_id}` and was marked failed after {} ms",
                    binding.turn_id, stale_after_ms
                )
            }
        } else {
            format!(
                "CLI runtime turn `{}` did not receive a native turn id and was marked failed after {} ms",
                binding.turn_id, stale_after_ms
            )
        };

        self.ensure_cli_runtime_turn_loaded_for_lifecycle(
            workspace_id.as_str(),
            binding.thread_id.as_str(),
            &turn,
        )
        .await?;
        let did_mark_failed = self
            .mark_turn_failed_terminal(binding.thread_id.clone(), binding.turn_id.clone(), message)
            .await;

        if did_mark_failed {
            self.cleanup_cli_runtime_terminal_turn_status(
                &binding,
                TurnStatus::Failed,
                "stale CLI runtime turn failed",
            )
            .await;
        }

        Ok(())
    }

    async fn reconcile_cli_runtime_turn_from_runtime(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        now_unix_ms: i64,
        workspace_id: &str,
        turn: &Turn,
    ) -> anyhow::Result<bool> {
        let Some(native_turn_id) = binding.native_turn_id.as_deref() else {
            return Ok(false);
        };
        let Some(manager) = self.cli_runtime_manager.as_ref() else {
            return Ok(false);
        };
        let key = CLIAgentRuntimeSessionKey::new(
            binding.workspace_id.clone(),
            binding.runtime_id.clone(),
            binding.thread_id.clone(),
        )?;
        let Some(handle) = manager.existing_session(&key).await else {
            return Ok(false);
        };
        let Some(observation) = handle
            .session()
            .observe_turn(binding.native_thread_id.as_str(), native_turn_id)
            .await?
        else {
            return Ok(false);
        };

        if observation.status == CLIAgentRuntimeObservedTurnStatus::InProgress {
            let renewed = self
                .timeout_supervisor
                .renew_running_attempt_deadlines_for_turn(
                    binding.turn_id.as_str(),
                    now_unix_ms.saturating_div(1_000),
                )
                .await?;
            debug!(
                workspace_id = binding.workspace_id.as_str(),
                runtime_id = binding.runtime_id.as_str(),
                thread_id = binding.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                native_turn_id,
                renewed,
                "runtime reconciliation confirmed stale Pioneer turn is still active"
            );
            return Ok(true);
        }

        for event in observation.reconciliation_events.iter().cloned() {
            if !self
                .process_bound_cli_runtime_event(&key, binding.clone(), event)
                .await
            {
                anyhow::bail!(
                    "failed to commit canonical runtime snapshot before terminal reconciliation"
                );
            }
        }
        self.commit_cli_runtime_final_diff_snapshot(&key, binding, native_turn_id)
            .await;
        let (event, status) = match observation.status {
            CLIAgentRuntimeObservedTurnStatus::Completed => (
                AgentDurableEvent::TurnCompleted {
                    thread_id: binding.thread_id.clone(),
                    turn_id: binding.turn_id.clone(),
                    recovery: None,
                },
                TurnStatus::Completed,
            ),
            CLIAgentRuntimeObservedTurnStatus::Failed => (
                AgentDurableEvent::TurnFailed {
                    thread_id: binding.thread_id.clone(),
                    turn_id: binding.turn_id.clone(),
                    error: observation
                        .message
                        .unwrap_or_else(|| "CLI runtime reported turn failure".to_owned()),
                    recovery: None,
                },
                TurnStatus::Failed,
            ),
            CLIAgentRuntimeObservedTurnStatus::Blocked => (
                AgentDurableEvent::TurnBlocked {
                    thread_id: binding.thread_id.clone(),
                    turn_id: binding.turn_id.clone(),
                    reason: observation
                        .message
                        .unwrap_or_else(|| "CLI runtime reported a blocked turn".to_owned()),
                    recovery: None,
                },
                TurnStatus::Blocked,
            ),
            CLIAgentRuntimeObservedTurnStatus::Interrupted => (
                AgentDurableEvent::TurnInterrupted {
                    thread_id: binding.thread_id.clone(),
                    turn_id: binding.turn_id.clone(),
                    reason: observation
                        .message
                        .unwrap_or_else(|| "CLI runtime reported turn interruption".to_owned()),
                    recovery: None,
                },
                TurnStatus::Interrupted,
            ),
            CLIAgentRuntimeObservedTurnStatus::InProgress => unreachable!(),
        };
        self.ensure_cli_runtime_turn_loaded_for_lifecycle(
            workspace_id,
            binding.thread_id.as_str(),
            turn,
        )
        .await?;
        self.ensure_cli_runtime_execution_event_hub(&key)
            .await
            .publish_durable_and_wait(event)
            .await
            .map_err(|error| {
                anyhow::anyhow!("failed to commit reconciled terminal event: {error}")
            })?;
        if let Some(actual_status) = self.cli_runtime_turn_status_for_binding(binding).await?
            && actual_status != TurnStatus::InProgress
        {
            self.cleanup_cli_runtime_terminal_turn_status(
                binding,
                actual_status,
                "runtime reconciliation observed terminal turn",
            )
            .await;
        } else {
            debug!(
                workspace_id = binding.workspace_id.as_str(),
                runtime_id = binding.runtime_id.as_str(),
                thread_id = binding.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                native_turn_id,
                observed_status = ?status,
                "runtime terminal observation entered Pioneer recovery without closing the turn binding"
            );
        }
        Ok(true)
    }

    async fn ensure_cli_runtime_turn_loaded_for_lifecycle(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn: &Turn,
    ) -> anyhow::Result<()> {
        if self
            .thread_manager
            .turn_get(thread_id, turn.id.as_str())
            .await
            .is_some()
        {
            return Ok(());
        }
        let mut thread = self
            .crud_store
            .get_thread_model(thread_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("thread `{thread_id}` not found"))?;
        if thread.workspace_id != workspace_id {
            anyhow::bail!(
                "thread `{thread_id}` belongs to workspace `{}` instead of `{workspace_id}`",
                thread.workspace_id
            );
        }
        thread.status = pioneer_protocol::ThreadStatus::Active;
        thread.turns = vec![turn.clone()];
        let sandbox = self.crud_store.get_thread_sandbox_mode(thread_id).await?;
        self.thread_manager
            .system_thread_restore_persisted(thread, sandbox)
            .await
    }

    async fn latest_cli_runtime_native_event_timestamp_ms(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
    ) -> anyhow::Result<Option<i64>> {
        let Some(native_turn_id) = binding.native_turn_id.as_ref() else {
            return Ok(None);
        };
        let event = self
            .crud_store
            .latest_cli_runtime_native_event(pioneer_crud::CliRuntimeNativeEventListFilter {
                runtime_id: Some(binding.runtime_id.clone()),
                thread_id: Some(binding.thread_id.clone()),
                turn_id: None,
                native_thread_id: Some(binding.native_thread_id.clone()),
                native_turn_id: Some(native_turn_id.clone()),
                limit: None,
            })
            .await?;
        Ok(event.map(|event| event.sequence.max(event.created_at.timestamp_millis())))
    }

    async fn prune_stale_cli_runtime_pending_turn_events(&self, now_unix_ms: i64) {
        let mut removed = Vec::new();
        {
            let mut buffer = self.cli_runtime_pending_turn_events.lock().await;
            buffer.retain(|key, events| {
                let last_received_at = events
                    .iter()
                    .map(|event| event.received_at_unix_ms)
                    .max()
                    .unwrap_or(0);
                let keep = now_unix_ms.saturating_sub(last_received_at)
                    < CLI_RUNTIME_PENDING_UNBOUND_EVENT_TTL_MS;
                if !keep {
                    removed.push((
                        key.workspace_id.clone(),
                        key.runtime_id.clone(),
                        key.thread_id.clone(),
                        key.native_thread_id.clone(),
                        key.native_turn_id.clone(),
                        events.len(),
                    ));
                }
                keep
            });
        }

        for (workspace_id, runtime_id, thread_id, native_thread_id, native_turn_id, event_count) in
            removed
        {
            warn!(
                workspace_id = workspace_id.as_str(),
                runtime_id = runtime_id.as_str(),
                thread_id = thread_id.as_str(),
                native_thread_id = native_thread_id.as_str(),
                native_turn_id = native_turn_id.as_str(),
                event_count,
                "discarded unbound CLI runtime native turn events after TTL"
            );
        }
    }

    async fn prune_stale_cli_runtime_pending_turn_server_requests(&self, now_unix_ms: i64) {
        let mut removed = Vec::new();
        {
            let mut buffer = self.cli_runtime_pending_turn_server_requests.lock().await;
            buffer.retain(|key, requests| {
                let last_received_at = requests
                    .iter()
                    .map(|request| request.received_at_unix_ms)
                    .max()
                    .unwrap_or(0);
                let keep = now_unix_ms.saturating_sub(last_received_at)
                    < CLI_RUNTIME_PENDING_UNBOUND_EVENT_TTL_MS;
                if !keep {
                    removed.push((
                        key.workspace_id.clone(),
                        key.runtime_id.clone(),
                        key.thread_id.clone(),
                        key.native_thread_id.clone(),
                        key.native_turn_id.clone(),
                        requests.len(),
                    ));
                }
                keep
            });
        }

        for (
            workspace_id,
            runtime_id,
            thread_id,
            native_thread_id,
            native_turn_id,
            request_count,
        ) in removed
        {
            warn!(
                workspace_id = workspace_id.as_str(),
                runtime_id = runtime_id.as_str(),
                thread_id = thread_id.as_str(),
                native_thread_id = native_thread_id.as_str(),
                native_turn_id = native_turn_id.as_str(),
                request_count,
                "discarded unbound CLI runtime native turn server requests after TTL"
            );
        }
    }

    async fn buffer_cli_runtime_event_until_turn_binding(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: &str,
        native_turn_id: &str,
        event: RuntimeEvent,
    ) {
        let pending_key = CLIRuntimePendingTurnEventKey {
            workspace_id: key.workspace_id.clone(),
            runtime_id: key.runtime_id.clone(),
            thread_id: key.thread_id.clone(),
            native_thread_id: native_thread_id.to_owned(),
            native_turn_id: native_turn_id.to_owned(),
        };
        let mut buffer = self.cli_runtime_pending_turn_events.lock().await;
        if !buffer.contains_key(&pending_key)
            && buffer.len() >= CLI_RUNTIME_PENDING_TURN_EVENT_MAX_KEYS
            && let Some(oldest_key) = buffer.keys().next().cloned()
        {
            buffer.remove(&oldest_key);
        }
        let events = buffer.entry(pending_key).or_default();
        if events.len() >= CLI_RUNTIME_PENDING_TURN_EVENT_MAX_PER_TURN {
            events.remove(0);
        }
        events.push(CLIRuntimePendingTurnEvent {
            event,
            received_at_unix_ms: now_timestamp_millis(),
            received_sequence: self
                .cli_runtime_pending_turn_activity_sequence
                .fetch_add(1, Ordering::Relaxed),
        });
    }

    async fn buffer_cli_runtime_codex_server_request_until_turn_binding(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: &str,
        native_turn_id: &str,
        request: CodexJsonlRpcServerRequest,
        event: RuntimeEvent,
    ) {
        let pending_key = CLIRuntimePendingTurnEventKey {
            workspace_id: key.workspace_id.clone(),
            runtime_id: key.runtime_id.clone(),
            thread_id: key.thread_id.clone(),
            native_thread_id: native_thread_id.to_owned(),
            native_turn_id: native_turn_id.to_owned(),
        };
        let mut buffer = self.cli_runtime_pending_turn_server_requests.lock().await;
        if !buffer.contains_key(&pending_key)
            && buffer.len() >= CLI_RUNTIME_PENDING_TURN_EVENT_MAX_KEYS
            && let Some(oldest_key) = buffer.keys().next().cloned()
        {
            buffer.remove(&oldest_key);
        }
        let requests = buffer.entry(pending_key).or_default();
        if requests.len() >= CLI_RUNTIME_PENDING_TURN_EVENT_MAX_PER_TURN {
            requests.remove(0);
        }
        requests.push(CLIRuntimePendingTurnServerRequest {
            request,
            event,
            received_at_unix_ms: now_timestamp_millis(),
            received_sequence: self
                .cli_runtime_pending_turn_activity_sequence
                .fetch_add(1, Ordering::Relaxed),
        });
    }

    pub(super) async fn handle_cli_runtime_codex_server_request(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        request: CodexJsonlRpcServerRequest,
        event: RuntimeEvent,
    ) {
        if matches!(event, RuntimeEvent::Raw(_)) {
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                method = request.method.as_str(),
                "ignoring unsupported Codex server request"
            );
            return;
        }

        match request.method.as_str() {
            "item/commandExecution/requestApproval" => {
                let decoded = decode_codex_command_approval_request(&request);
                let Some(turn_binding) = self
                    .cli_runtime_turn_binding_for_native_turn_option(
                        key,
                        decoded.native_thread_id.as_deref(),
                        decoded.native_turn_id.as_deref(),
                    )
                    .await
                else {
                    if self
                        .buffer_cli_runtime_codex_server_request_if_turn_binding_pending(
                            key,
                            decoded.native_thread_id.as_deref(),
                            decoded.native_turn_id.as_deref(),
                            &request,
                            &event,
                        )
                        .await
                    {
                        return;
                    }
                    self.cancel_stale_codex_command_approval_request(
                        key,
                        &decoded,
                        "missing Pioneer turn binding",
                    )
                    .await;
                    return;
                };
                if !self
                    .cli_runtime_turn_binding_accepts_native_activity(
                        key,
                        &turn_binding,
                        decoded.native_thread_id.as_deref(),
                        request.method.as_str(),
                    )
                    .await
                {
                    self.cancel_stale_codex_command_approval_request(
                        key,
                        &decoded,
                        "terminal turn",
                    )
                    .await;
                    return;
                }
                if let Err(error) = self
                    .open_codex_command_approval_request_for_turn(
                        key.workspace_id.as_str(),
                        key.runtime_id.as_str(),
                        "codex",
                        key.thread_id.as_str(),
                        Some(turn_binding.turn_id),
                        decoded.clone(),
                    )
                    .await
                {
                    warn!(
                        workspace_id = key.workspace_id.as_str(),
                        runtime_id = key.runtime_id.as_str(),
                        thread_id = key.thread_id.as_str(),
                        method = request.method.as_str(),
                        error = %format!("{error:#}"),
                        "failed to open Codex CLI runtime command approval request"
                    );
                    self.cancel_stale_codex_command_approval_request(
                        key,
                        &decoded,
                        "failed to open Pioneer pending request",
                    )
                    .await;
                }
            }
            "item/fileChange/requestApproval" => {
                let decoded = decode_codex_file_change_approval_request(&request);
                let Some(turn_binding) = self
                    .cli_runtime_turn_binding_for_native_turn_option(
                        key,
                        decoded.native_thread_id.as_deref(),
                        decoded.native_turn_id.as_deref(),
                    )
                    .await
                else {
                    if self
                        .buffer_cli_runtime_codex_server_request_if_turn_binding_pending(
                            key,
                            decoded.native_thread_id.as_deref(),
                            decoded.native_turn_id.as_deref(),
                            &request,
                            &event,
                        )
                        .await
                    {
                        return;
                    }
                    self.cancel_stale_codex_file_change_approval_request(
                        key,
                        &decoded,
                        "missing Pioneer turn binding",
                    )
                    .await;
                    return;
                };
                if !self
                    .cli_runtime_turn_binding_accepts_native_activity(
                        key,
                        &turn_binding,
                        decoded.native_thread_id.as_deref(),
                        request.method.as_str(),
                    )
                    .await
                {
                    self.cancel_stale_codex_file_change_approval_request(
                        key,
                        &decoded,
                        "terminal turn",
                    )
                    .await;
                    return;
                }
                if let Err(error) = self
                    .open_codex_file_change_approval_request_for_turn(
                        key.workspace_id.as_str(),
                        key.runtime_id.as_str(),
                        "codex",
                        key.thread_id.as_str(),
                        Some(turn_binding.turn_id),
                        decoded.clone(),
                    )
                    .await
                {
                    warn!(
                        workspace_id = key.workspace_id.as_str(),
                        runtime_id = key.runtime_id.as_str(),
                        thread_id = key.thread_id.as_str(),
                        method = request.method.as_str(),
                        error = %format!("{error:#}"),
                        "failed to open Codex CLI runtime file change approval request"
                    );
                    self.cancel_stale_codex_file_change_approval_request(
                        key,
                        &decoded,
                        "failed to open Pioneer pending request",
                    )
                    .await;
                }
            }
            "item/tool/requestUserInput" | "tool/requestUserInput" | "userInput/request" => {
                let decoded = decode_codex_user_input_request(&request);
                let Some(turn_binding) = self
                    .cli_runtime_turn_binding_for_native_turn_option(
                        key,
                        decoded.native_thread_id.as_deref(),
                        decoded.native_turn_id.as_deref(),
                    )
                    .await
                else {
                    if self
                        .buffer_cli_runtime_codex_server_request_if_turn_binding_pending(
                            key,
                            decoded.native_thread_id.as_deref(),
                            decoded.native_turn_id.as_deref(),
                            &request,
                            &event,
                        )
                        .await
                    {
                        return;
                    }
                    self.cancel_stale_codex_user_input_request(
                        key,
                        &decoded,
                        "missing Pioneer turn binding",
                    )
                    .await;
                    return;
                };
                if !self
                    .cli_runtime_turn_binding_accepts_native_activity(
                        key,
                        &turn_binding,
                        decoded.native_thread_id.as_deref(),
                        request.method.as_str(),
                    )
                    .await
                {
                    self.cancel_stale_codex_user_input_request(key, &decoded, "terminal turn")
                        .await;
                    return;
                }
                if let Err(error) = self
                    .open_codex_user_input_request_for_turn(
                        key.workspace_id.as_str(),
                        key.runtime_id.as_str(),
                        "codex",
                        key.thread_id.as_str(),
                        Some(turn_binding.turn_id),
                        decoded.clone(),
                    )
                    .await
                {
                    warn!(
                        workspace_id = key.workspace_id.as_str(),
                        runtime_id = key.runtime_id.as_str(),
                        thread_id = key.thread_id.as_str(),
                        method = request.method.as_str(),
                        error = %format!("{error:#}"),
                        "failed to open Codex CLI runtime user input request"
                    );
                    self.cancel_stale_codex_user_input_request(
                        key,
                        &decoded,
                        "failed to open Pioneer pending request",
                    )
                    .await;
                }
            }
            _ => {}
        }
    }

    async fn cli_runtime_turn_binding_for_native_turn_option(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> Option<pioneer_crud::CliRuntimeTurnBindingRecord> {
        let native_thread_id = native_thread_id?;
        let native_turn_id = native_turn_id?;
        self.cli_runtime_turn_binding_for_native_turn(key, native_thread_id, native_turn_id)
            .await
    }

    async fn buffer_cli_runtime_codex_server_request_if_turn_binding_pending(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
        request: &CodexJsonlRpcServerRequest,
        event: &RuntimeEvent,
    ) -> bool {
        let Some(native_turn_id) = native_turn_id else {
            return false;
        };
        let Some(native_thread_id) = native_thread_id else {
            return false;
        };
        if !self
            .cli_runtime_has_starting_turn_binding_without_native_turn(key, native_thread_id)
            .await
        {
            return false;
        }
        self.buffer_cli_runtime_codex_server_request_until_turn_binding(
            key,
            native_thread_id,
            native_turn_id,
            request.clone(),
            event.clone(),
        )
        .await;
        debug!(
            workspace_id = key.workspace_id.as_str(),
            runtime_id = key.runtime_id.as_str(),
            thread_id = key.thread_id.as_str(),
            native_turn_id,
            method = request.method.as_str(),
            "buffering Codex CLI runtime server request before Pioneer turn binding has native turn id"
        );
        true
    }

    async fn cli_runtime_has_starting_turn_binding_without_native_turn(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: &str,
    ) -> bool {
        match self
            .crud_store
            .list_cli_runtime_turn_bindings_for_thread(key.thread_id.as_str())
            .await
        {
            Ok(bindings) => bindings.into_iter().any(|binding| {
                binding.workspace_id == key.workspace_id
                    && binding.runtime_id == key.runtime_id
                    && binding.status
                        == crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_STARTING
                    && binding.native_turn_id.is_none()
                    && binding.native_thread_id == native_thread_id
            }),
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to check starting CLI runtime turn binding for server request buffering"
                );
                false
            }
        }
    }

    async fn cli_runtime_turn_binding_accepts_native_activity(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        native_thread_id: Option<&str>,
        source: &str,
    ) -> bool {
        if binding.workspace_id != key.workspace_id
            || binding.runtime_id != key.runtime_id
            || binding.thread_id != key.thread_id
        {
            warn!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                binding_workspace_id = binding.workspace_id.as_str(),
                binding_runtime_id = binding.runtime_id.as_str(),
                binding_thread_id = binding.thread_id.as_str(),
                source,
                "rejected Codex CLI runtime activity for mismatched turn binding"
            );
            return false;
        }

        if let Some(native_thread_id) = native_thread_id
            && binding.native_thread_id != native_thread_id
        {
            warn!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                binding_native_thread_id = binding.native_thread_id.as_str(),
                native_thread_id,
                source,
                "rejected Codex CLI runtime activity for mismatched native thread"
            );
            return false;
        }

        if !cli_runtime_turn_binding_status_is_active(binding.status.as_str()) {
            self.compact_cli_runtime_terminal_native_events_for_turn(
                binding.turn_id.as_str(),
                source,
            )
            .await;
            return false;
        }

        match self.cli_runtime_turn_status_for_binding(binding).await {
            Ok(Some(TurnStatus::InProgress)) => true,
            Ok(Some(status)) => {
                self.cleanup_cli_runtime_terminal_turn_status(binding, status, source)
                    .await;
                false
            }
            Ok(None) => {
                warn!(
                    workspace_id = binding.workspace_id.as_str(),
                    runtime_id = binding.runtime_id.as_str(),
                    thread_id = binding.thread_id.as_str(),
                    turn_id = binding.turn_id.as_str(),
                    source,
                    "rejected Codex CLI runtime activity for missing Pioneer turn"
                );
                false
            }
            Err(error) => {
                warn!(
                    workspace_id = binding.workspace_id.as_str(),
                    runtime_id = binding.runtime_id.as_str(),
                    thread_id = binding.thread_id.as_str(),
                    turn_id = binding.turn_id.as_str(),
                    source,
                    error = %format!("{error:#}"),
                    "failed to validate Pioneer turn status for Codex CLI runtime activity"
                );
                false
            }
        }
    }

    async fn cli_runtime_turn_status_for_binding(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
    ) -> anyhow::Result<Option<TurnStatus>> {
        if let Some((_workspace_id, turn)) = self
            .thread_manager
            .turn_get(binding.thread_id.as_str(), binding.turn_id.as_str())
            .await
        {
            return Ok(Some(turn.status));
        }

        Ok(self
            .crud_store
            .get_turn(binding.thread_id.as_str(), binding.turn_id.as_str())
            .await?
            .map(|(_workspace_id, turn)| turn.status))
    }

    async fn cli_runtime_pioneer_turn_id_for_native_turn(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> Option<String> {
        let native_thread_id = native_thread_id?;
        let native_turn_id = native_turn_id?;
        self.cli_runtime_turn_binding_for_native_turn(key, native_thread_id, native_turn_id)
            .await
            .map(|binding| binding.turn_id)
    }

    async fn cli_runtime_turn_binding_for_native_turn(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Option<pioneer_crud::CliRuntimeTurnBindingRecord> {
        let cache_key = CLIRuntimeNativeTurnKey {
            workspace_id: key.workspace_id.clone(),
            runtime_id: key.runtime_id.clone(),
            thread_id: key.thread_id.clone(),
            native_thread_id: native_thread_id.to_owned(),
            native_turn_id: native_turn_id.to_owned(),
        };
        if let Some(binding) = self
            .cli_runtime_turn_binding_cache
            .lock()
            .await
            .get(&cache_key)
            .cloned()
        {
            return Some(binding);
        }

        match self
            .crud_store
            .list_cli_runtime_turn_bindings_for_thread(key.thread_id.as_str())
            .await
        {
            Ok(bindings) => {
                let binding = bindings.into_iter().find(|binding| {
                    binding.workspace_id == key.workspace_id
                        && binding.runtime_id == key.runtime_id
                        && binding.native_turn_id.as_deref() == Some(native_turn_id)
                        && binding.native_thread_id == native_thread_id
                });
                if let Some(binding) = binding.as_ref() {
                    self.cli_runtime_turn_binding_cache
                        .lock()
                        .await
                        .insert(cache_key, binding.clone());
                }
                binding
            }
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    native_turn_id,
                    error = %format!("{error:#}"),
                    "failed to load CLI runtime turn binding for native turn"
                );
                None
            }
        }
    }

    async fn invalidate_cli_runtime_turn_binding_cache(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
    ) {
        let Some(native_turn_id) = binding.native_turn_id.as_deref() else {
            return;
        };
        self.cli_runtime_turn_binding_cache
            .lock()
            .await
            .remove(&CLIRuntimeNativeTurnKey {
                workspace_id: binding.workspace_id.clone(),
                runtime_id: binding.runtime_id.clone(),
                thread_id: binding.thread_id.clone(),
                native_thread_id: binding.native_thread_id.clone(),
                native_turn_id: native_turn_id.to_owned(),
            });
    }

    async fn cli_runtime_running_turn_binding_for_timeline_event(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        event: &RuntimeEvent,
    ) -> Option<pioneer_crud::CliRuntimeTurnBindingRecord> {
        if !cli_runtime_event_can_use_running_turn_fallback(event) {
            return None;
        }
        let Some(native_thread_id) = cli_runtime_native_thread_id_for_event(event) else {
            return None;
        };
        match self
            .crud_store
            .list_cli_runtime_turn_bindings_for_thread(key.thread_id.as_str())
            .await
        {
            Ok(bindings) => bindings.into_iter().rev().find(|binding| {
                binding.workspace_id == key.workspace_id
                    && binding.runtime_id == key.runtime_id
                    && binding.status
                        == crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
                    && binding.native_turn_id.is_some()
                    && binding.native_thread_id == native_thread_id
            }),
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    event = %cli_runtime_event_log_label(event),
                    error = %format!("{error:#}"),
                    "failed to load running CLI runtime turn binding for timeline event fallback"
                );
                None
            }
        }
    }

    async fn emit_cli_runtime_steer_accepted_timeline_event(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        native_turn_id: &str,
    ) {
        let item_id = format!("cli_runtime_steer_{}", generate_id(16));
        let item = TurnItem::SystemEvent {
            id: item_id.clone(),
            level: SystemEventLevel::Info,
            message: "CLI runtime steering accepted".to_owned(),
            code: Some("cli_runtime_turn_steer".to_owned()),
            details: Some(json!({
                "status": "accepted",
                "nativeTurnId": native_turn_id,
            })),
        };
        message_future(
            self.handle_durable_agent_event(AgentDurableEvent::ItemStarted {
                notification: ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: item.clone(),
                },
            }),
        )
        .await;
        message_future(
            self.handle_durable_agent_event(AgentDurableEvent::ItemCompleted {
                notification: ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item,
                },
            }),
        )
        .await;
    }

    async fn emit_cli_runtime_steer_accepted_timeline_event_fresh_task(
        &self,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        native_turn_id: String,
    ) {
        let processor = self.clone();
        let log_thread_id = thread_id.clone();
        let log_turn_id = turn_id.clone();
        let join = tokio::spawn(async move {
            processor
                .emit_cli_runtime_steer_accepted_timeline_event(
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    turn_id.as_str(),
                    native_turn_id.as_str(),
                )
                .await;
        });

        if let Err(error) = join.await {
            warn!(
                thread_id = log_thread_id.as_str(),
                turn_id = log_turn_id.as_str(),
                error = %format!("{error:#}"),
                "CLI runtime steer accepted timeline event task failed"
            );
        }
    }

    pub(super) async fn best_effort_sync_cli_runtime_thread_name(
        &self,
        workspace_id: &str,
        thread_id: &str,
        name: &str,
    ) {
        let binding = match self
            .crud_store
            .get_cli_runtime_thread_binding(thread_id)
            .await
        {
            Ok(Some(binding)) => binding,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    error = %format!("{error:#}"),
                    "failed to load CLI runtime thread binding for best-effort name sync"
                );
                return;
            }
        };
        if binding.workspace_id != workspace_id {
            return;
        }
        let Some(manager) = self.cli_runtime_manager.as_ref() else {
            debug!(
                workspace_id,
                thread_id,
                runtime_id = binding.runtime_id.as_str(),
                "skipping CLI runtime thread name sync because manager is unavailable"
            );
            return;
        };
        let key = match CLIAgentRuntimeSessionKey::new(
            workspace_id,
            binding.runtime_id.as_str(),
            thread_id,
        ) {
            Ok(key) => key,
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    error = %format!("{error:#}"),
                    "invalid CLI runtime key for best-effort name sync"
                );
                return;
            }
        };
        let Some(handle) = manager.existing_session(&key).await else {
            debug!(
                workspace_id,
                thread_id,
                runtime_id = binding.runtime_id.as_str(),
                "skipping CLI runtime thread name sync because session is not active"
            );
            return;
        };
        if !handle.session().supports_thread_name_sync() {
            debug!(
                workspace_id,
                thread_id,
                runtime_id = binding.runtime_id.as_str(),
                "skipping CLI runtime thread name sync because runtime does not support it"
            );
            return;
        }
        if let Err(error) = handle
            .session()
            .set_thread_name(CLIAgentRuntimeThreadNameSetRequest {
                native_thread_id: binding.native_thread_id,
                name: name.to_owned(),
            })
            .await
        {
            warn!(
                workspace_id,
                thread_id,
                runtime_id = binding.runtime_id.as_str(),
                error = %format!("{error:#}"),
                "failed to sync CLI runtime thread name"
            );
        }
    }

    pub(crate) async fn ensure_cli_runtime_turn_blocked_cleanup(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: Option<&str>,
    ) {
        let binding = match self.crud_store.get_cli_runtime_turn_binding(turn_id).await {
            Ok(Some(binding)) => binding,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to load CLI runtime turn binding for blocked cleanup"
                );
                return;
            }
        };

        if binding.thread_id != thread_id {
            warn!(
                thread_id,
                turn_id,
                binding_thread_id = binding.thread_id.as_str(),
                "CLI runtime turn binding thread mismatch during blocked cleanup"
            );
        }

        if binding.status != crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_BLOCKED
            && let Err(error) =
                crate::cli_runtime::turn_binding::update_cli_runtime_turn_binding_status(
                    self.crud_store.as_ref(),
                    turn_id,
                    crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_BLOCKED,
                    chrono::Utc::now().fixed_offset(),
                )
                .await
        {
            warn!(
                workspace_id = binding.workspace_id.as_str(),
                runtime_id = binding.runtime_id.as_str(),
                thread_id = binding.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                error = %format!("{error:#}"),
                "failed to mark CLI runtime turn binding blocked"
            );
        }

        self.expire_cli_runtime_pending_requests_for_turn(turn_id)
            .await;
        self.interrupt_and_close_cli_runtime_binding(&binding, reason)
            .await;
    }

    pub(crate) async fn ensure_cli_runtime_turn_interrupted_cleanup(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        reason: Option<&str>,
    ) {
        if binding.status != crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_INTERRUPTED
            && let Err(error) =
                crate::cli_runtime::turn_binding::update_cli_runtime_turn_binding_status(
                    self.crud_store.as_ref(),
                    binding.turn_id.as_str(),
                    crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_INTERRUPTED,
                    chrono::Utc::now().fixed_offset(),
                )
                .await
        {
            warn!(
                workspace_id = binding.workspace_id.as_str(),
                runtime_id = binding.runtime_id.as_str(),
                thread_id = binding.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                error = %format!("{error:#}"),
                "failed to mark CLI runtime turn binding interrupted"
            );
        }

        self.expire_cli_runtime_pending_requests_for_turn(binding.turn_id.as_str())
            .await;
        self.interrupt_and_close_cli_runtime_binding(binding, reason)
            .await;
    }

    async fn validate_cli_runtime_pending_request_active_turn(
        &self,
        pending: &CliRuntimePendingRequestRecord,
    ) -> anyhow::Result<()> {
        let request_kind = cli_runtime_request_kind_from_stored(pending.request_kind.as_str());
        let Some(turn_id) = pending.turn_id.as_deref() else {
            if cli_runtime_request_kind_requires_turn_binding(request_kind) {
                anyhow::bail!(
                    "CLI runtime request `{}` is not bound to a Pioneer turn",
                    pending.request_id
                );
            }
            return Ok(());
        };
        let binding = self
            .crud_store
            .get_cli_runtime_turn_binding(turn_id)
            .await
            .with_context(|| {
                format!("failed to load CLI runtime turn binding for turn `{turn_id}`")
            })?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Pioneer turn `{turn_id}` is not bound to CLI runtime `{}`",
                    pending.runtime_id
                )
            })?;

        if binding.workspace_id != pending.workspace_id
            || binding.runtime_id != pending.runtime_id
            || binding.thread_id != pending.thread_id
        {
            anyhow::bail!(
                "turn binding `{}` belongs to `{}/{}/{}` but request belongs to `{}/{}/{}`",
                binding.turn_id,
                binding.workspace_id,
                binding.runtime_id,
                binding.thread_id,
                pending.workspace_id,
                pending.runtime_id,
                pending.thread_id
            );
        }
        if cli_runtime_request_kind_requires_turn_binding(request_kind) {
            let native_thread_id = pending.native_thread_id.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "CLI runtime request `{}` is not bound to a native thread",
                    pending.request_id
                )
            })?;
            let native_turn_id = pending.native_turn_id.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "CLI runtime request `{}` is not bound to a native turn",
                    pending.request_id
                )
            })?;
            if binding.native_thread_id != native_thread_id {
                anyhow::bail!(
                    "request native thread `{native_thread_id}` does not match binding native thread `{}`",
                    binding.native_thread_id
                );
            }
            if binding.native_turn_id.as_deref() != Some(native_turn_id) {
                anyhow::bail!(
                    "request native turn `{native_turn_id}` does not match binding native turn `{:?}`",
                    binding.native_turn_id
                );
            }
        }
        if !cli_runtime_turn_binding_status_is_active(binding.status.as_str()) {
            self.cleanup_cli_runtime_terminal_binding_status(
                &binding,
                "stale CLI runtime request response",
            )
            .await;
            anyhow::bail!(
                "turn `{}` is no longer active for CLI runtime `{}`; binding status is `{}`",
                binding.turn_id,
                binding.runtime_id,
                binding.status
            );
        }

        match self.cli_runtime_turn_status_for_binding(&binding).await? {
            Some(TurnStatus::InProgress) => Ok(()),
            Some(status) => {
                self.cleanup_cli_runtime_terminal_turn_status(
                    &binding,
                    status,
                    "stale CLI runtime request response",
                )
                .await;
                anyhow::bail!(
                    "Pioneer turn `{}` is `{}`",
                    binding.turn_id,
                    cli_runtime_turn_status_label(status)
                )
            }
            None => anyhow::bail!("Pioneer turn `{}` is missing", binding.turn_id),
        }
    }

    pub(super) async fn cleanup_cli_runtime_terminal_turn_status(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        status: TurnStatus,
        reason: &str,
    ) {
        match status {
            TurnStatus::InProgress => {}
            TurnStatus::Completed => {
                self.update_cli_runtime_turn_binding_terminal_status(
                    binding,
                    crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_COMPLETED,
                    reason,
                )
                .await;
                self.expire_cli_runtime_pending_requests_for_turn(binding.turn_id.as_str())
                    .await;
            }
            TurnStatus::Failed => {
                self.update_cli_runtime_turn_binding_terminal_status(
                    binding,
                    crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_FAILED,
                    reason,
                )
                .await;
                self.expire_cli_runtime_pending_requests_for_turn(binding.turn_id.as_str())
                    .await;
                self.interrupt_and_close_cli_runtime_binding(binding, Some(reason))
                    .await;
            }
            TurnStatus::Interrupted => {
                self.ensure_cli_runtime_turn_interrupted_cleanup(binding, Some(reason))
                    .await;
            }
            TurnStatus::Blocked => {
                self.ensure_cli_runtime_turn_blocked_cleanup(
                    binding.thread_id.as_str(),
                    binding.turn_id.as_str(),
                    Some(reason),
                )
                .await;
            }
        }
        if status != TurnStatus::InProgress {
            self.invalidate_cli_runtime_turn_binding_cache(binding)
                .await;
            self.compact_cli_runtime_terminal_native_events_for_turn(
                binding.turn_id.as_str(),
                reason,
            )
            .await;
        }
    }

    async fn cleanup_cli_runtime_terminal_binding_status(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        reason: &str,
    ) {
        match binding.status.as_str() {
            crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_COMPLETED => {
                self.expire_cli_runtime_pending_requests_for_turn(binding.turn_id.as_str())
                    .await;
            }
            crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_FAILED => {
                self.expire_cli_runtime_pending_requests_for_turn(binding.turn_id.as_str())
                    .await;
                self.interrupt_and_close_cli_runtime_binding(binding, Some(reason))
                    .await;
            }
            crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_INTERRUPTED => {
                self.ensure_cli_runtime_turn_interrupted_cleanup(binding, Some(reason))
                    .await;
            }
            crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_BLOCKED => {
                self.ensure_cli_runtime_turn_blocked_cleanup(
                    binding.thread_id.as_str(),
                    binding.turn_id.as_str(),
                    Some(reason),
                )
                .await;
            }
            _ => {}
        }
        self.compact_cli_runtime_terminal_native_events_for_turn(binding.turn_id.as_str(), reason)
            .await;
    }

    async fn compact_cli_runtime_terminal_native_events_for_turn(
        &self,
        turn_id: &str,
        reason: &str,
    ) {
        loop {
            match self
                .crud_store
                .compact_terminal_cli_runtime_native_events_for_turn(
                    turn_id,
                    CLI_RUNTIME_TERMINAL_NATIVE_EVENT_CLEANUP_BATCH_SIZE,
                    false,
                )
                .await
            {
                Ok(summary) if summary.candidate_rows == 0 => break,
                Ok(summary) => {
                    debug!(
                        turn_id,
                        reason,
                        candidate_rows = summary.candidate_rows,
                        deleted_rows = summary.deleted_rows,
                        payload_bytes = summary.payload_bytes,
                        "compacted terminal CLI runtime native delta events for turn"
                    );
                    if summary.deleted_rows == 0 {
                        break;
                    }
                }
                Err(error) => {
                    warn!(
                        turn_id,
                        reason,
                        error = %format!("{error:#}"),
                        "failed to compact terminal CLI runtime native delta events for turn"
                    );
                    break;
                }
            }
        }
    }

    async fn update_cli_runtime_turn_binding_terminal_status(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        terminal_status: &str,
        reason: &str,
    ) {
        if binding.status == terminal_status {
            return;
        }
        if let Err(error) =
            crate::cli_runtime::turn_binding::update_cli_runtime_turn_binding_status(
                self.crud_store.as_ref(),
                binding.turn_id.as_str(),
                terminal_status,
                chrono::Utc::now().fixed_offset(),
            )
            .await
        {
            warn!(
                workspace_id = binding.workspace_id.as_str(),
                runtime_id = binding.runtime_id.as_str(),
                thread_id = binding.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                terminal_status,
                reason,
                error = %format!("{error:#}"),
                "failed to reconcile CLI runtime turn binding terminal status"
            );
        }
    }

    async fn expire_cli_runtime_pending_requests_for_turn(&self, turn_id: &str) {
        let pending_requests = match self
            .crud_store
            .list_cli_runtime_pending_requests(pioneer_crud::CliRuntimePendingRequestListFilter {
                turn_id: Some(turn_id.to_owned()),
                status: Some(StoredCliRuntimePendingRequestStatus::Pending),
                limit: None,
                ..Default::default()
            })
            .await
        {
            Ok(requests) => requests,
            Err(error) => {
                warn!(
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to list CLI runtime pending requests for terminal cleanup"
                );
                return;
            }
        };

        for request in pending_requests {
            if let Err(error) = self
                .expire_cli_runtime_pending_request_as_stale(&request)
                .await
            {
                warn!(
                    workspace_id = request.workspace_id.as_str(),
                    runtime_id = request.runtime_id.as_str(),
                    thread_id = request.thread_id.as_str(),
                    turn_id = request.turn_id.as_deref(),
                    request_id = request.request_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to expire CLI runtime pending request for terminal cleanup"
                );
            }
        }
    }

    async fn expire_cli_runtime_pending_request_as_stale(
        &self,
        request: &CliRuntimePendingRequestRecord,
    ) -> anyhow::Result<Option<CliRuntimePendingRequestRecord>> {
        if let Some(current) = self
            .crud_store
            .get_cli_runtime_pending_request(request.request_id.as_str())
            .await?
            && current.status != StoredCliRuntimePendingRequestStatus::Pending
        {
            return Ok(Some(current));
        }

        let resolution = CLIRuntimeRequestResolution::Expired;
        let response_json = Some(
            pioneer_crud::serialize_cli_runtime_json(&resolution)
                .context("failed to serialize expired CLI runtime request resolution")?,
        );
        let now = cli_runtime_request_timestamp();
        let expire_result = {
            let crud_store = self.crud_store.clone();
            let request_id = request.request_id.clone();
            message_fresh_task(async move {
                crud_store
                    .expire_cli_runtime_pending_request(request_id.as_str(), response_json, now)
                    .await
            })
            .await
        };
        let expired = match expire_result {
            Ok(result) => result,
            Err(error) => Err(anyhow::anyhow!(
                "CLI runtime pending request expiration task failed: {error}"
            )),
        }?;
        if let Some(expired) = expired.as_ref() {
            self.emit_cli_runtime_request_resolved(expired.clone(), resolution.clone())
                .await;
            if expired.request_kind
                == cli_runtime_request_kind_as_str(CLIRuntimeRequestKind::FileChangeApproval)
            {
                self.emit_cli_runtime_file_change_approval_timeline_update(
                    expired,
                    file_change_approval_timeline_status_for_resolution(&resolution),
                )
                .await;
            }
        }
        Ok(expired)
    }

    async fn interrupt_and_close_cli_runtime_binding(
        &self,
        binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
        reason: Option<&str>,
    ) {
        let Some(manager) = self.cli_runtime_manager.as_ref() else {
            return;
        };
        let key = match CLIAgentRuntimeSessionKey::new(
            binding.workspace_id.as_str(),
            binding.runtime_id.as_str(),
            binding.thread_id.as_str(),
        ) {
            Ok(key) => key,
            Err(error) => {
                warn!(
                    workspace_id = binding.workspace_id.as_str(),
                    runtime_id = binding.runtime_id.as_str(),
                    thread_id = binding.thread_id.as_str(),
                    turn_id = binding.turn_id.as_str(),
                    error = %format!("{error:#}"),
                    "invalid CLI runtime key during terminal cleanup"
                );
                return;
            }
        };

        if let Some(handle) = manager.existing_session(&key).await {
            let session = handle.session();
            if let Err(error) = session
                .interrupt_turn(
                    Some(binding.native_thread_id.as_str()),
                    binding.native_turn_id.as_deref(),
                )
                .await
            {
                debug!(
                    workspace_id = binding.workspace_id.as_str(),
                    runtime_id = binding.runtime_id.as_str(),
                    thread_id = binding.thread_id.as_str(),
                    turn_id = binding.turn_id.as_str(),
                    native_turn_id = binding.native_turn_id.as_deref(),
                    reason,
                    error = %format!("{error:#}"),
                    "failed to interrupt CLI runtime turn during terminal cleanup"
                );
            }
        }

        if self
            .cli_runtime_has_active_other_turn_binding(
                &key,
                Some(binding.native_thread_id.as_str()),
                binding.native_turn_id.as_deref(),
            )
            .await
        {
            debug!(
                workspace_id = binding.workspace_id.as_str(),
                runtime_id = binding.runtime_id.as_str(),
                thread_id = binding.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                native_turn_id = binding.native_turn_id.as_deref(),
                reason,
                "kept CLI runtime session open during terminal cleanup because another turn binding is active"
            );
            return;
        }

        if let Err(error) = manager.close_session(&key).await {
            warn!(
                workspace_id = binding.workspace_id.as_str(),
                runtime_id = binding.runtime_id.as_str(),
                thread_id = binding.thread_id.as_str(),
                turn_id = binding.turn_id.as_str(),
                reason,
                error = %format!("{error:#}"),
                "failed to close CLI runtime session during terminal cleanup"
            );
        }
    }

    async fn cancel_stale_codex_command_approval_request(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        request: &CodexCommandApprovalRequest,
        reason: &str,
    ) {
        self.cancel_stale_codex_native_request(
            key,
            "item/commandExecution/requestApproval",
            request.native_request_id_json.clone(),
            codex_command_approval_response(CodexCommandApprovalDecision::Cancel),
            request.native_thread_id.as_deref(),
            request.native_turn_id.as_deref(),
            reason,
        )
        .await;
    }

    async fn cancel_stale_codex_file_change_approval_request(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        request: &CodexFileChangeApprovalRequest,
        reason: &str,
    ) {
        self.cancel_stale_codex_native_request(
            key,
            "item/fileChange/requestApproval",
            request.native_request_id_json.clone(),
            codex_file_change_approval_response(CodexFileChangeApprovalDecision::Cancel),
            request.native_thread_id.as_deref(),
            request.native_turn_id.as_deref(),
            reason,
        )
        .await;
    }

    async fn cancel_stale_codex_user_input_request(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        request: &CodexUserInputRequest,
        reason: &str,
    ) {
        self.cancel_stale_codex_native_request(
            key,
            "item/tool/requestUserInput",
            request.native_request_id_json.clone(),
            codex_user_input_response(BTreeMap::new()),
            request.native_thread_id.as_deref(),
            request.native_turn_id.as_deref(),
            reason,
        )
        .await;
    }

    async fn cancel_stale_codex_native_request(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        method: &str,
        native_request_id: JsonValue,
        response: JsonValue,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
        reason: &str,
    ) {
        let Some(manager) = self.cli_runtime_manager.as_ref() else {
            return;
        };
        let Some(handle) = manager.existing_session(key).await else {
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                method,
                reason,
                "dropped stale Codex CLI runtime server request because session is already closed"
            );
            return;
        };
        let session = handle.session();
        if let Err(error) = session
            .respond_to_request(native_request_id, response)
            .await
        {
            warn!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                method,
                reason,
                error = %format!("{error:#}"),
                "failed to cancel stale Codex CLI runtime server request"
            );
        }
        if native_turn_id.is_some()
            && let Err(error) = session
                .interrupt_turn(native_thread_id, native_turn_id)
                .await
        {
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                method,
                reason,
                error = %format!("{error:#}"),
                "failed to interrupt stale Codex CLI runtime turn"
            );
        }
        if self
            .cli_runtime_has_active_other_turn_binding(key, native_thread_id, native_turn_id)
            .await
        {
            debug!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                method,
                reason,
                native_thread_id,
                native_turn_id,
                "kept CLI runtime session open after stale Codex server request because another turn binding is active"
            );
            return;
        }
        if let Err(error) = manager.close_session(key).await {
            warn!(
                workspace_id = key.workspace_id.as_str(),
                runtime_id = key.runtime_id.as_str(),
                thread_id = key.thread_id.as_str(),
                method,
                reason,
                error = %format!("{error:#}"),
                "failed to close CLI runtime session after stale Codex server request"
            );
        }
    }

    async fn cli_runtime_has_active_other_turn_binding(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> bool {
        match self
            .crud_store
            .list_cli_runtime_turn_bindings_for_thread(key.thread_id.as_str())
            .await
        {
            Ok(bindings) => bindings.into_iter().any(|binding| {
                binding.workspace_id == key.workspace_id
                    && binding.runtime_id == key.runtime_id
                    && cli_runtime_turn_binding_status_is_active(binding.status.as_str())
                    && !cli_runtime_turn_binding_matches_native_activity(
                        &binding,
                        native_thread_id,
                        native_turn_id,
                    )
            }),
            Err(error) => {
                warn!(
                    workspace_id = key.workspace_id.as_str(),
                    runtime_id = key.runtime_id.as_str(),
                    thread_id = key.thread_id.as_str(),
                    native_thread_id,
                    native_turn_id,
                    error = %format!("{error:#}"),
                    "failed to check active CLI runtime turn bindings before closing stale server request session"
                );
                false
            }
        }
    }

    async fn cli_runtime_existing_session_for_pending_request(
        &self,
        request: &CliRuntimePendingRequestRecord,
    ) -> anyhow::Result<Option<CLIAgentRuntimeSessionHandle>> {
        if cli_runtime_request_kind_from_stored(request.request_kind.as_str())
            == CLIRuntimeRequestKind::Other
        {
            return Ok(None);
        }

        let Some(manager) = self.cli_runtime_manager.as_ref() else {
            anyhow::bail!(
                "CLI runtime manager is not available for CLI runtime request `{}`",
                request.request_id
            );
        };
        let key = CLIAgentRuntimeSessionKey::new(
            request.workspace_id.as_str(),
            request.runtime_id.as_str(),
            request.thread_id.as_str(),
        )?;
        let Some(handle) = manager.existing_session(&key).await else {
            anyhow::bail!(
                "CLI runtime session is not active for request `{}`",
                request.request_id
            );
        };
        Ok(Some(handle))
    }

    async fn respond_to_cli_runtime_native_request(
        &self,
        request: &CliRuntimePendingRequestRecord,
        resolution: &CLIRuntimeRequestResolution,
        native_response_session: Option<CLIAgentRuntimeSessionHandle>,
    ) -> anyhow::Result<()> {
        let (response, should_interrupt) =
            match cli_runtime_kind_config_from_stored_kind(request.runtime_kind.as_str()) {
                Some(GatewayCliAgentRuntimeKindConfig::Claude) => {
                    claude_permission_response_from_resolution(request, resolution)?
                }
                Some(GatewayCliAgentRuntimeKindConfig::Codex) => {
                    match cli_runtime_request_kind_from_stored(request.request_kind.as_str()) {
                        CLIRuntimeRequestKind::CommandApproval => {
                            let decision =
                                codex_command_approval_decision_from_resolution(resolution)?;
                            (
                                codex_command_approval_response(decision.clone()),
                                decision == CodexCommandApprovalDecision::Cancel,
                            )
                        }
                        CLIRuntimeRequestKind::FileChangeApproval => {
                            let decision =
                                codex_file_change_approval_decision_from_resolution(resolution)?;
                            (
                                codex_file_change_approval_response(decision.clone()),
                                decision == CodexFileChangeApprovalDecision::Cancel,
                            )
                        }
                        CLIRuntimeRequestKind::UserInput => (
                            codex_user_input_response_from_resolution(request, resolution)?,
                            false,
                        ),
                        CLIRuntimeRequestKind::Other => return Ok(()),
                    }
                }
                None => {
                    anyhow::bail!(
                        "unsupported CLI runtime kind `{}` for request `{}`",
                        request.runtime_kind,
                        request.request_id
                    );
                }
            };
        let native_request_id = cli_runtime_native_request_id_json_from_record(request)?;
        let Some(handle) = native_response_session else {
            anyhow::bail!(
                "CLI runtime session is not active for request `{}`",
                request.request_id
            );
        };
        let session = handle.session();
        session
            .respond_to_request(native_request_id, response)
            .await?;

        if should_interrupt && request.native_turn_id.is_some() {
            session
                .interrupt_turn(
                    request.native_thread_id.as_deref(),
                    request.native_turn_id.as_deref(),
                )
                .await?;
        }

        if request.request_kind
            == cli_runtime_request_kind_as_str(CLIRuntimeRequestKind::FileChangeApproval)
        {
            self.emit_cli_runtime_file_change_approval_timeline_update(
                request,
                file_change_approval_timeline_status_for_resolution(resolution),
            )
            .await;
        }

        Ok(())
    }

    async fn emit_cli_runtime_file_change_approval_timeline_update(
        &self,
        request: &CliRuntimePendingRequestRecord,
        status: &str,
    ) {
        let (Some(turn_id), Some(item_id)) =
            (request.turn_id.as_ref(), request.native_item_id.as_ref())
        else {
            return;
        };

        self.handle_progress_agent_event(AgentProgressEvent::ItemDelta {
            notification: ItemDeltaNotification {
                workspace_id: request.workspace_id.clone(),
                thread_id: request.thread_id.clone(),
                turn_id: turn_id.clone(),
                item_id: item_id.clone(),
                delta: status.to_owned(),
                stream: Some(ItemDeltaStream::ToolProgress),
                payload: Some(json!({
                    "status": status,
                    "requestId": request.request_id.as_str(),
                    "requestKind": request.request_kind.as_str(),
                })),
                markdown: None,
                markdown_version: None,
            },
        })
        .await;
    }

    #[allow(dead_code)]
    pub(super) async fn emit_cli_runtime_request_opened(
        &self,
        request: CliRuntimePendingRequestRecord,
    ) {
        let notification = cli_runtime_request_opened_notification_from_record(&request);
        self.send_notification_to_workspace_connections(
            request.workspace_id.as_str(),
            events::CLI_RUNTIME_REQUEST_OPENED,
            &notification,
        )
        .await;
        self.notify_semantic_timeline_pending_request_changed(&request)
            .await;
    }

    pub(super) async fn emit_cli_runtime_request_resolved(
        &self,
        request: CliRuntimePendingRequestRecord,
        resolution: CLIRuntimeRequestResolution,
    ) {
        let notification =
            cli_runtime_request_resolved_notification_from_record(&request, resolution);
        self.send_notification_to_workspace_connections(
            request.workspace_id.as_str(),
            events::CLI_RUNTIME_REQUEST_RESOLVED,
            &notification,
        )
        .await;
        self.notify_semantic_timeline_pending_request_changed(&request)
            .await;
    }

    async fn validate_cli_runtime_workspace(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &str,
        workspace_id: String,
    ) -> Option<String> {
        let workspace_id = match self
            .workspace_manager
            .validate_workspace_id(workspace_id.as_str())
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!("failed to validate workspace for `{method}`: {error}"),
                    ),
                )
                .await;
                return None;
            }
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;
        Some(workspace_id)
    }

    pub(crate) fn load_cli_runtime_instances(
        &self,
    ) -> anyhow::Result<Vec<EffectiveGatewayCliAgentRuntimeInstanceConfig>> {
        crate::cli_runtime::config::load_effective_cli_runtime_instances(
            self.artifact_runtime_home.as_path(),
        )
    }

    async fn load_cli_runtime_summaries(
        &self,
        workspace_id: &str,
    ) -> anyhow::Result<Vec<RuntimeSummary>> {
        let instances = self.load_cli_runtime_instances()?;
        let mut summaries = Vec::with_capacity(instances.len());
        for instance in instances {
            let proxy_url = self
                .prepare_cli_runtime_proxy_url(workspace_id, instance.id.as_str())
                .await?;
            summaries.push(cli_runtime_summary_from_instance(instance, proxy_url));
        }
        sort_cli_runtime_summary_display_order(summaries.as_mut_slice());
        Ok(summaries)
    }

    async fn load_cli_runtime_summary_by_id(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        workspace_id: &str,
        runtime_id: &str,
    ) -> Option<RuntimeSummary> {
        let runtimes = match self.load_cli_runtime_summaries(workspace_id).await {
            Ok(runtimes) => runtimes,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load CLI runtime catalog: {error:#}"),
                    ),
                )
                .await;
                return None;
            }
        };

        match find_cli_runtime_summary(runtimes, runtime_id) {
            Some(runtime) => Some(runtime),
            None => {
                self.send_unknown_cli_runtime_error(connection_id, request_id, runtime_id)
                    .await;
                None
            }
        }
    }

    async fn load_cli_runtime_instance_by_id(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        runtime_id: &str,
    ) -> Option<EffectiveGatewayCliAgentRuntimeInstanceConfig> {
        let instances = match self.load_cli_runtime_instances() {
            Ok(instances) => instances,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load CLI runtime catalog: {error:#}"),
                    ),
                )
                .await;
                return None;
            }
        };

        match find_cli_runtime_instance(instances, runtime_id) {
            Some(instance) => Some(instance),
            None => {
                self.send_unknown_cli_runtime_error(connection_id, request_id, runtime_id)
                    .await;
                None
            }
        }
    }

    async fn load_cli_runtime_live_summary_by_id(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        workspace_id: &str,
        runtime_id: &str,
    ) -> Option<RuntimeSummary> {
        let instances = match self.load_cli_runtime_instances() {
            Ok(instances) => instances,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load CLI runtime catalog: {error:#}"),
                    ),
                )
                .await;
                return None;
            }
        };

        match find_cli_runtime_instance(instances, runtime_id) {
            Some(instance) => Some(
                self.cli_runtime_live_summary_from_instance(workspace_id, instance)
                    .await,
            ),
            None => {
                self.send_unknown_cli_runtime_error(connection_id, request_id, runtime_id)
                    .await;
                None
            }
        }
    }

    async fn cli_runtime_live_summary_from_instance(
        &self,
        workspace_id: &str,
        instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
    ) -> RuntimeSummary {
        let proxy_url = self
            .cli_runtime_proxy_url(workspace_id, instance.id.as_str())
            .await;
        let summary = cli_runtime_summary_from_instance(instance.clone(), proxy_url.clone());
        if !instance.enabled {
            return summary;
        }

        match instance.kind {
            GatewayCliAgentRuntimeKindConfig::Codex => {
                let probe =
                    CodexProbe::account_read(codex_account_probe_config_from_instance_with_proxy(
                        &instance,
                        proxy_url.as_deref(),
                    ))
                    .await;
                apply_codex_account_probe_to_summary(summary, probe)
            }
            GatewayCliAgentRuntimeKindConfig::Claude => {
                let probe = ClaudeProbe::account_read(
                    claude_account_probe_config_from_instance_with_proxy(
                        &instance,
                        proxy_url.as_deref(),
                    ),
                )
                .await;
                apply_claude_account_probe_to_summary(summary, probe)
            }
        }
    }

    pub(super) async fn prepare_cli_runtime_proxy_url(
        &self,
        workspace_id: &str,
        runtime_id: &str,
    ) -> anyhow::Result<Option<String>> {
        let key = (workspace_id.to_owned(), runtime_id.to_owned());
        if let Some(proxy_url) = self.cli_runtime_proxy_cache.lock().await.get(&key).cloned() {
            return Ok(proxy_url);
        }

        let proxy_url = self
            .gateway_secrets
            .get_workspace_cli_runtime_proxy(workspace_id, runtime_id)
            .map_err(anyhow::Error::from)?;
        self.cli_runtime_proxy_cache
            .lock()
            .await
            .insert(key, proxy_url.clone());
        Ok(proxy_url)
    }

    async fn cli_runtime_proxy_url(&self, workspace_id: &str, runtime_id: &str) -> Option<String> {
        match self
            .prepare_cli_runtime_proxy_url(workspace_id, runtime_id)
            .await
        {
            Ok(proxy_url) => proxy_url,
            Err(error) => {
                warn!(
                    workspace_id,
                    runtime_id,
                    error = %format!("{error:#}"),
                    "failed to prepare CLI runtime proxy settings"
                );
                None
            }
        }
    }

    async fn cache_cli_runtime_proxy_url(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        proxy_url: Option<String>,
    ) {
        self.cli_runtime_proxy_cache
            .lock()
            .await
            .insert((workspace_id.to_owned(), runtime_id.to_owned()), proxy_url);
    }

    async fn send_unknown_cli_runtime_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        runtime_id: &str,
    ) {
        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_PARAMS_CODE,
                format!("unknown CLI runtime `{runtime_id}`"),
            ),
        )
        .await;
    }

    async fn send_stale_cli_runtime_request_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        pending_request_id: &str,
    ) {
        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_PARAMS_CODE,
                format!("unknown or stale CLI runtime request `{pending_request_id}`"),
            ),
        )
        .await;
    }

    async fn send_cli_runtime_response<T: Serialize>(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &str,
        result: &T,
    ) {
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
                error = %format!("{error:#}"),
                method,
                "failed to send CLI runtime response"
            );
        }
    }
}

fn find_cli_runtime_summary(
    runtimes: Vec<RuntimeSummary>,
    runtime_id: &str,
) -> Option<RuntimeSummary> {
    runtimes
        .into_iter()
        .find(|runtime| runtime.runtime_id == runtime_id)
}

fn find_cli_runtime_instance(
    instances: Vec<EffectiveGatewayCliAgentRuntimeInstanceConfig>,
    runtime_id: &str,
) -> Option<EffectiveGatewayCliAgentRuntimeInstanceConfig> {
    instances
        .into_iter()
        .find(|instance| instance.id == runtime_id)
}

fn cli_runtime_thread_binding_from_record(
    binding: pioneer_crud::CliRuntimeThreadBindingRecord,
) -> anyhow::Result<CLIRuntimeThreadBinding> {
    let runtime_kind = match binding.runtime_kind.as_str() {
        "codex" => CLIAgentRuntimeKind::Codex,
        "claude" => CLIAgentRuntimeKind::Claude,
        other => anyhow::bail!("unknown CLI runtime kind `{other}`"),
    };

    Ok(CLIRuntimeThreadBinding {
        workspace_id: binding.workspace_id,
        thread_id: binding.thread_id,
        runtime_id: binding.runtime_id,
        runtime_kind,
        native_thread_id: binding.native_thread_id,
        native_cwd: binding.native_cwd,
        native_model: binding.native_model,
        status: binding.status,
    })
}

fn validate_cli_runtime_thread_fork_params(
    params: CLIRuntimeThreadForkParams,
) -> Result<CLIRuntimeThreadForkParams, String> {
    let workspace_id = normalize_cli_runtime_required_field(
        methods::CLI_RUNTIME_THREAD_FORK,
        "workspace_id",
        params.workspace_id,
        128,
    )?;
    let runtime_id = normalize_cli_runtime_required_field(
        methods::CLI_RUNTIME_THREAD_FORK,
        "runtime_id",
        params.runtime_id,
        128,
    )?;
    let source_thread_id = normalize_cli_runtime_required_field(
        methods::CLI_RUNTIME_THREAD_FORK,
        "source_thread_id",
        params.source_thread_id,
        128,
    )?;
    let fork_thread_id = normalize_cli_runtime_required_field(
        methods::CLI_RUNTIME_THREAD_FORK,
        "fork_thread_id",
        params.fork_thread_id,
        128,
    )?;
    if source_thread_id == fork_thread_id {
        return Err(format!(
            "invalid params for `{}`: `fork_thread_id` must be different from `source_thread_id`",
            methods::CLI_RUNTIME_THREAD_FORK
        ));
    }
    let name = params
        .name
        .map(|name| {
            normalize_cli_runtime_optional_field(
                methods::CLI_RUNTIME_THREAD_FORK,
                "name",
                name,
                256,
            )
        })
        .transpose()?
        .flatten();

    Ok(CLIRuntimeThreadForkParams {
        workspace_id,
        runtime_id,
        source_thread_id,
        fork_thread_id,
        name,
    })
}

fn validate_cli_runtime_turn_steer_params(
    params: CLIRuntimeTurnSteerParams,
) -> Result<CLIRuntimeTurnSteerParams, String> {
    Ok(CLIRuntimeTurnSteerParams {
        workspace_id: normalize_cli_runtime_required_field(
            methods::CLI_RUNTIME_TURN_STEER,
            "workspace_id",
            params.workspace_id,
            128,
        )?,
        runtime_id: normalize_cli_runtime_required_field(
            methods::CLI_RUNTIME_TURN_STEER,
            "runtime_id",
            params.runtime_id,
            128,
        )?,
        thread_id: normalize_cli_runtime_required_field(
            methods::CLI_RUNTIME_TURN_STEER,
            "thread_id",
            params.thread_id,
            128,
        )?,
        turn_id: normalize_cli_runtime_required_field(
            methods::CLI_RUNTIME_TURN_STEER,
            "turn_id",
            params.turn_id,
            128,
        )?,
        message: normalize_cli_runtime_required_field(
            methods::CLI_RUNTIME_TURN_STEER,
            "message",
            params.message,
            32_768,
        )?,
    })
}

fn normalize_cli_runtime_required_field(
    method: &str,
    field: &str,
    value: String,
    max_chars: usize,
) -> Result<String, String> {
    let Some(value) = normalize_cli_runtime_optional_field(method, field, value, max_chars)? else {
        return Err(format!(
            "invalid params for `{method}`: `{field}` is required"
        ));
    };
    Ok(value)
}

fn normalize_cli_runtime_optional_field(
    method: &str,
    field: &str,
    value: String,
    max_chars: usize,
) -> Result<Option<String>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > max_chars {
        return Err(format!(
            "invalid params for `{method}`: `{field}` must be at most {max_chars} characters"
        ));
    }
    Ok(Some(trimmed.to_owned()))
}

fn sort_cli_runtime_summary_display_order(summaries: &mut [RuntimeSummary]) {
    summaries.sort_by(|left, right| {
        let left_order = cli_runtime_default_display_order(left.runtime_id.as_str());
        let right_order = cli_runtime_default_display_order(right.runtime_id.as_str());
        left_order.cmp(&right_order).then_with(|| {
            if left_order == usize::MAX {
                std::cmp::Ordering::Equal
            } else {
                left.runtime_id.cmp(&right.runtime_id)
            }
        })
    });
}

fn cli_runtime_default_display_order(runtime_id: &str) -> usize {
    match runtime_id {
        "codex" => 0,
        "claude" => 1,
        _ => usize::MAX,
    }
}

fn cli_runtime_summary_from_instance(
    instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
    proxy_url: Option<String>,
) -> RuntimeSummary {
    let status = if instance.enabled {
        RuntimeStatus::Degraded {
            message: "CLI runtime has not been probed yet".to_owned(),
        }
    } else {
        RuntimeStatus::Disabled
    };
    let diagnostics = if instance.enabled {
        vec![RuntimeDiagnostic {
            level: RuntimeDiagnosticLevel::Info,
            code: "cli_runtime.unprobed".to_owned(),
            message: "Runtime status will refresh after live probing is enabled".to_owned(),
        }]
    } else {
        Vec::new()
    };

    RuntimeSummary {
        runtime_id: instance.id,
        kind: cli_runtime_kind_from_config(instance.kind),
        display_name: instance.display_name,
        enabled: instance.enabled,
        status,
        capabilities: cli_runtime_capabilities_for_kind(instance.kind),
        account: None,
        version: None,
        binary_path: Some(instance.binary_path),
        home_path: Some(instance.home_path),
        shadow_home_path: instance.shadow_home_path,
        proxy_url,
        debug_native_events_enabled: instance.debug_native_events,
        models_refreshed_at_unix_ms: None,
        diagnostics,
        recent_stderr: Vec::new(),
    }
}

fn cli_runtime_kind_from_config(kind: GatewayCliAgentRuntimeKindConfig) -> CLIAgentRuntimeKind {
    match kind {
        GatewayCliAgentRuntimeKindConfig::Codex => CLIAgentRuntimeKind::Codex,
        GatewayCliAgentRuntimeKindConfig::Claude => CLIAgentRuntimeKind::Claude,
    }
}

fn cli_runtime_protocol_kind_label(kind: CLIAgentRuntimeKind) -> &'static str {
    match kind {
        CLIAgentRuntimeKind::Codex => "codex",
        CLIAgentRuntimeKind::Claude => "claude",
    }
}

fn cli_runtime_capabilities_for_stored_kind(kind: &str) -> Option<RuntimeCapabilities> {
    cli_runtime_kind_config_from_stored_kind(kind).map(cli_runtime_capabilities_for_kind)
}

fn cli_runtime_kind_config_from_stored_kind(
    kind: &str,
) -> Option<GatewayCliAgentRuntimeKindConfig> {
    match kind {
        "codex" => Some(GatewayCliAgentRuntimeKindConfig::Codex),
        "claude" => Some(GatewayCliAgentRuntimeKindConfig::Claude),
        _ => None,
    }
}

fn cli_runtime_capabilities_for_kind(
    kind: GatewayCliAgentRuntimeKindConfig,
) -> RuntimeCapabilities {
    match kind {
        GatewayCliAgentRuntimeKindConfig::Codex => RuntimeCapabilities {
            supports_skills: true,
            supports_threads: true,
            supports_resume: true,
            supports_fork: true,
            supports_steer: true,
            supports_interrupt: true,
            supports_approvals: true,
            supports_file_change_approvals: true,
            supports_command_approvals: true,
            supports_user_input_requests: true,
            supports_model_list: true,
            supports_apps: false,
            supports_review: false,
            supports_compaction: false,
            supports_goal: false,
            supports_diff_updates: true,
            supports_history_read: true,
            supports_thread_archive: false,
            supports_auth_management: true,
            supports_generated_schema_probe: true,
        },
        GatewayCliAgentRuntimeKindConfig::Claude => RuntimeCapabilities {
            supports_skills: true,
            supports_threads: true,
            supports_resume: false,
            supports_fork: false,
            supports_steer: false,
            supports_interrupt: true,
            supports_approvals: true,
            supports_file_change_approvals: true,
            supports_command_approvals: true,
            supports_user_input_requests: false,
            supports_model_list: true,
            supports_apps: false,
            supports_review: false,
            supports_compaction: false,
            supports_goal: false,
            supports_diff_updates: false,
            supports_history_read: true,
            supports_thread_archive: false,
            supports_auth_management: false,
            supports_generated_schema_probe: false,
        },
    }
}

fn apply_claude_account_probe_to_summary(
    mut summary: RuntimeSummary,
    probe: ClaudeAccountProbeSnapshot,
) -> RuntimeSummary {
    summary.status = match probe.status {
        ClaudeAccountProbeStatus::Ready => RuntimeStatus::Ready,
        ClaudeAccountProbeStatus::NeedsAuth => RuntimeStatus::NeedsAuth,
        ClaudeAccountProbeStatus::MissingBinary => RuntimeStatus::MissingBinary {
            binary_path: summary.binary_path.clone(),
        },
        ClaudeAccountProbeStatus::SpawnFailed => RuntimeStatus::SpawnFailed {
            message: probe
                .message
                .clone()
                .unwrap_or_else(|| "failed to spawn Claude CLI".to_owned()),
        },
        ClaudeAccountProbeStatus::UnsupportedVersion => RuntimeStatus::UnsupportedVersion {
            version: probe.version.clone(),
            minimum_version: Some("2.0.0".to_owned()),
        },
        ClaudeAccountProbeStatus::Error => RuntimeStatus::Error {
            message: probe
                .message
                .clone()
                .unwrap_or_else(|| "Claude CLI probe failed".to_owned()),
        },
    };
    summary.version = probe.version.clone();
    summary.account = probe
        .account
        .as_ref()
        .map(runtime_account_from_claude_account);
    summary.diagnostics = probe
        .diagnostics
        .iter()
        .map(runtime_diagnostic_from_claude_probe)
        .collect();
    summary.recent_stderr = sanitize_runtime_diagnostic_lines(probe.stderr);
    summary
}

fn apply_codex_account_probe_to_summary(
    mut summary: RuntimeSummary,
    probe: CodexAccountProbeSnapshot,
) -> RuntimeSummary {
    summary.status = match probe.status {
        CodexAccountProbeStatus::Ready => RuntimeStatus::Ready,
        CodexAccountProbeStatus::NeedsAuth => RuntimeStatus::NeedsAuth,
        CodexAccountProbeStatus::MissingBinary => RuntimeStatus::MissingBinary {
            binary_path: summary.binary_path.clone(),
        },
        CodexAccountProbeStatus::SpawnFailed => RuntimeStatus::SpawnFailed {
            message: probe
                .message
                .clone()
                .unwrap_or_else(|| "failed to spawn Codex app-server".to_owned()),
        },
        CodexAccountProbeStatus::Error => RuntimeStatus::Error {
            message: probe
                .message
                .clone()
                .unwrap_or_else(|| "Codex account probe failed".to_owned()),
        },
    };
    summary.version = probe.version.clone();
    summary.account = runtime_account_from_codex_account_probe(&probe);
    summary.diagnostics = probe
        .diagnostics
        .iter()
        .map(runtime_diagnostic_from_codex_probe)
        .collect();
    summary.recent_stderr = sanitize_runtime_diagnostic_lines(probe.stderr);

    summary
}

fn runtime_account_from_codex_account_probe(
    probe: &CodexAccountProbeSnapshot,
) -> Option<RuntimeAccountSnapshot> {
    match probe.account.as_ref() {
        Some(account) => Some(runtime_account_from_codex_account(account)),
        None if probe.status == CodexAccountProbeStatus::NeedsAuth => {
            Some(RuntimeAccountSnapshot {
                authenticated: false,
                account_id: None,
                email: None,
                display_name: None,
                plan: None,
                auth_method: None,
            })
        }
        None => None,
    }
}

fn runtime_account_from_codex_account(account: &CodexAccountSnapshot) -> RuntimeAccountSnapshot {
    RuntimeAccountSnapshot {
        authenticated: account.authenticated,
        account_id: account.account_id.clone(),
        email: account.email.clone(),
        display_name: account.display_name.clone(),
        plan: account.plan.clone(),
        auth_method: account.auth_method.clone(),
    }
}

fn runtime_account_from_claude_account(account: &ClaudeAccountSnapshot) -> RuntimeAccountSnapshot {
    RuntimeAccountSnapshot {
        authenticated: account.authenticated,
        account_id: None,
        email: account.email.clone(),
        display_name: None,
        plan: account.plan.clone(),
        auth_method: None,
    }
}

fn runtime_diagnostic_from_codex_probe(diagnostic: &CodexProbeDiagnostic) -> RuntimeDiagnostic {
    RuntimeDiagnostic {
        level: match diagnostic.level {
            CodexProbeDiagnosticLevel::Info => RuntimeDiagnosticLevel::Info,
            CodexProbeDiagnosticLevel::Warning => RuntimeDiagnosticLevel::Warning,
            CodexProbeDiagnosticLevel::Error => RuntimeDiagnosticLevel::Error,
        },
        code: sanitize_runtime_diagnostic_line(diagnostic.code.as_str()),
        message: sanitize_runtime_diagnostic_line(diagnostic.message.as_str()),
    }
}

fn runtime_diagnostic_from_claude_probe(diagnostic: &ClaudeProbeDiagnostic) -> RuntimeDiagnostic {
    RuntimeDiagnostic {
        level: match diagnostic.level {
            ClaudeProbeDiagnosticLevel::Info => RuntimeDiagnosticLevel::Info,
            ClaudeProbeDiagnosticLevel::Warning => RuntimeDiagnosticLevel::Warning,
            ClaudeProbeDiagnosticLevel::Error => RuntimeDiagnosticLevel::Error,
        },
        code: sanitize_runtime_diagnostic_line(diagnostic.code.as_str()),
        message: sanitize_runtime_diagnostic_line(diagnostic.message.as_str()),
    }
}

fn runtime_models_from_codex_probe(
    probe: CodexModelListProbeSnapshot,
    custom_models: &[String],
) -> Vec<RuntimeModelInfo> {
    let models = match probe.status {
        CodexModelListProbeStatus::Ready => probe
            .models
            .into_iter()
            .map(runtime_model_from_codex_model)
            .collect(),
        CodexModelListProbeStatus::NeedsAuth
        | CodexModelListProbeStatus::MissingBinary
        | CodexModelListProbeStatus::SpawnFailed
        | CodexModelListProbeStatus::Error => Vec::new(),
    };

    runtime_models_with_custom_models(models, custom_models)
}

struct RuntimeModelListResult {
    models: Vec<RuntimeModelInfo>,
    diagnostics: Vec<RuntimeDiagnostic>,
    error_message: Option<String>,
}

fn runtime_model_list_from_codex_probe(
    probe: CodexModelListProbeSnapshot,
    custom_models: &[String],
) -> RuntimeModelListResult {
    let status = probe.status;
    let message = probe.message.clone();
    let diagnostics = probe
        .diagnostics
        .iter()
        .map(runtime_diagnostic_from_codex_probe)
        .collect::<Vec<_>>();
    let models = runtime_models_from_codex_probe(probe, custom_models);
    let error_message = match status {
        CodexModelListProbeStatus::Ready if models.is_empty() => {
            Some("Codex model/list returned no models".to_owned())
        }
        CodexModelListProbeStatus::Ready => None,
        CodexModelListProbeStatus::NeedsAuth => {
            Some(message.unwrap_or_else(|| "Codex CLI is not authenticated".to_owned()))
        }
        CodexModelListProbeStatus::MissingBinary => {
            Some(message.unwrap_or_else(|| "Codex CLI binary was not found".to_owned()))
        }
        CodexModelListProbeStatus::SpawnFailed => {
            Some(message.unwrap_or_else(|| "failed to spawn Codex app-server".to_owned()))
        }
        CodexModelListProbeStatus::Error => {
            Some(message.unwrap_or_else(|| "Codex app-server model probe failed".to_owned()))
        }
    };

    RuntimeModelListResult {
        models,
        diagnostics,
        error_message,
    }
}

fn runtime_model_list_from_claude_probe(probe: ClaudeModelListSnapshot) -> RuntimeModelListResult {
    let diagnostics = probe
        .diagnostics
        .iter()
        .map(runtime_diagnostic_from_claude_probe)
        .collect::<Vec<_>>();
    RuntimeModelListResult {
        models: probe
            .models
            .into_iter()
            .map(runtime_model_from_claude_model)
            .collect(),
        diagnostics,
        error_message: probe.error_message,
    }
}

fn runtime_model_from_codex_model(model: CodexModelSnapshot) -> RuntimeModelInfo {
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

fn runtime_model_from_claude_model(model: ClaudeModelSnapshot) -> RuntimeModelInfo {
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

fn runtime_models_with_custom_models(
    mut models: Vec<RuntimeModelInfo>,
    custom_models: &[String],
) -> Vec<RuntimeModelInfo> {
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

    models
}

fn cli_runtime_login_start_type_to_codex(login_type: CLIRuntimeLoginStartType) -> &'static str {
    match login_type {
        CLIRuntimeLoginStartType::ChatgptDeviceCode => "chatgptDeviceCode",
        CLIRuntimeLoginStartType::Chatgpt => "chatgpt",
    }
}

fn cli_runtime_login_start_response_from_codex(
    runtime_id: String,
    login_type: CLIRuntimeLoginStartType,
    snapshot: CodexLoginStartSnapshot,
) -> CLIRuntimeLoginStartResponse {
    let status = match snapshot.status {
        CodexLoginStartStatus::Started => RuntimeStatus::NeedsAuth,
        CodexLoginStartStatus::MissingBinary => RuntimeStatus::MissingBinary { binary_path: None },
        CodexLoginStartStatus::SpawnFailed => RuntimeStatus::SpawnFailed {
            message: snapshot
                .message
                .clone()
                .unwrap_or_else(|| "failed to spawn Codex app-server".to_owned()),
        },
        CodexLoginStartStatus::Error => RuntimeStatus::Error {
            message: snapshot
                .message
                .clone()
                .unwrap_or_else(|| "Codex login start failed".to_owned()),
        },
    };
    let response = snapshot.response.unwrap_or(CodexLoginStartResponse {
        login_type: Some(snapshot.login_type),
        login_id: None,
        verification_url: None,
        user_code: None,
        auth_url: None,
        message: snapshot.message.clone(),
        raw: JsonValue::Null,
        extra: Default::default(),
    });

    CLIRuntimeLoginStartResponse {
        runtime_id,
        login_type,
        status,
        login_id: response.login_id,
        verification_url: response.verification_url,
        user_code: response.user_code,
        auth_url: response.auth_url,
        message: response.message.or(snapshot.message),
        raw: (response.raw != JsonValue::Null).then_some(response.raw),
    }
}

#[allow(dead_code)]
fn cli_runtime_request_opened_notification_from_record(
    record: &CliRuntimePendingRequestRecord,
) -> CLIRuntimeRequestOpenedNotification {
    CLIRuntimeRequestOpenedNotification {
        workspace_id: record.workspace_id.clone(),
        runtime_id: record.runtime_id.clone(),
        request_id: record.request_id.clone(),
        thread_id: Some(record.thread_id.clone()),
        turn_id: record.turn_id.clone(),
        item_id: record.native_item_id.clone(),
        request: cli_runtime_pending_request_from_record(record),
    }
}

fn cli_runtime_request_resolved_notification_from_record(
    record: &CliRuntimePendingRequestRecord,
    resolution: CLIRuntimeRequestResolution,
) -> CLIRuntimeRequestResolvedNotification {
    CLIRuntimeRequestResolvedNotification {
        workspace_id: record.workspace_id.clone(),
        runtime_id: record.runtime_id.clone(),
        request_id: record.request_id.clone(),
        thread_id: Some(record.thread_id.clone()),
        turn_id: record.turn_id.clone(),
        item_id: record.native_item_id.clone(),
        resolution,
    }
}

fn cli_runtime_command_approval_pending_request(
    request: &CodexCommandApprovalRequest,
) -> CLIRuntimePendingRequest {
    let display_command = request.display_command();
    CLIRuntimePendingRequest {
        kind: CLIRuntimeRequestKind::CommandApproval,
        title: display_command
            .as_deref()
            .map(|command| format!("Run `{command}`")),
        message: request.reason.clone().or(display_command),
        native_request_id: Some(request.native_request_id.clone()),
        payload: Some(json!({
            "approvalId": request.approval_id,
            "command": request.command,
            "argv": request.argv,
            "commandActions": request.command_actions,
            "cwd": request.cwd,
            "reason": request.reason,
            "startedAtMs": request.started_at_ms,
            "nativeRequestId": request.native_request_id,
            "nativeRequestIdJson": request.native_request_id_json,
            "threadId": request.native_thread_id,
            "turnId": request.native_turn_id,
            "itemId": request.native_item_id,
            "raw": request.raw,
        })),
    }
}

fn cli_runtime_pending_request_from_runtime_event(
    request: &pioneer_cli_agent_runtime::event::RuntimeRequestOpened,
    kind: CLIRuntimeRequestKind,
) -> CLIRuntimePendingRequest {
    let payload = request
        .payload_redacted
        .clone()
        .unwrap_or_else(|| json!({}));
    let title = payload
        .get("title")
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .or_else(|| {
            payload
                .get("displayName")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            payload
                .get("command")
                .and_then(JsonValue::as_str)
                .map(|command| format!("Run `{command}`"))
        })
        .or_else(|| match kind {
            CLIRuntimeRequestKind::FileChangeApproval => Some("Review file changes".to_owned()),
            CLIRuntimeRequestKind::CommandApproval => Some("Approve command".to_owned()),
            CLIRuntimeRequestKind::UserInput => Some("User input required".to_owned()),
            CLIRuntimeRequestKind::Other => None,
        });
    let message = payload
        .get("description")
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .or_else(|| {
            payload
                .get("reason")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            payload
                .get("command")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        });
    CLIRuntimePendingRequest {
        kind,
        title,
        message,
        native_request_id: Some(request.native_request_id.clone()),
        payload: Some(payload),
    }
}

fn cli_runtime_file_change_approval_pending_request(
    request: &CodexFileChangeApprovalRequest,
) -> CLIRuntimePendingRequest {
    let changed_files = cli_runtime_file_change_changed_files(request);
    let summary = request.summary.clone().or_else(|| {
        (!changed_files.is_empty()).then(|| {
            format!(
                "{} changed {}",
                changed_files.len(),
                if changed_files.len() == 1 {
                    "path"
                } else {
                    "paths"
                }
            )
        })
    });
    let diff_preview = request.diff.as_ref().map(|diff| {
        let (text, truncated, original_chars) =
            truncate_string_preview(diff, CLI_RUNTIME_FILE_CHANGE_DIFF_PREVIEW_MAX_CHARS);
        json!({
            "text": text,
            "truncated": truncated,
            "originalChars": original_chars,
        })
    });

    CLIRuntimePendingRequest {
        kind: CLIRuntimeRequestKind::FileChangeApproval,
        title: Some("Review file changes".to_owned()),
        message: request
            .reason
            .clone()
            .or_else(|| summary.clone())
            .or_else(|| request.grant_root.clone()),
        native_request_id: Some(request.native_request_id.clone()),
        payload: Some(json!({
            "grantRoot": request.grant_root,
            "changedFiles": changed_files,
            "summary": summary,
            "diffPreview": diff_preview,
            "reason": request.reason,
            "startedAtMs": request.started_at_ms,
            "nativeRequestId": request.native_request_id,
            "nativeRequestIdJson": request.native_request_id_json,
            "threadId": request.native_thread_id,
            "turnId": request.native_turn_id,
            "itemId": request.native_item_id,
        })),
    }
}

fn cli_runtime_file_change_changed_files(request: &CodexFileChangeApprovalRequest) -> Vec<String> {
    if !request.changed_files.is_empty() {
        return request.changed_files.clone();
    }
    request
        .grant_root
        .as_ref()
        .map(|grant_root| vec![grant_root.clone()])
        .unwrap_or_default()
}

fn truncate_string_preview(value: &str, max_chars: usize) -> (String, bool, usize) {
    let original_chars = value.chars().count();
    if original_chars <= max_chars {
        return (value.to_owned(), false, original_chars);
    }
    (
        value.chars().take(max_chars).collect::<String>(),
        true,
        original_chars,
    )
}

fn cli_runtime_user_input_pending_request(
    request: &CodexUserInputRequest,
) -> CLIRuntimePendingRequest {
    let questions = request
        .questions
        .iter()
        .map(|question| {
            json!({
                "id": question.id,
                "header": question.header,
                "question": question.question,
                "options": question
                    .options
                    .iter()
                    .map(|option| json!({
                        "label": option.label,
                        "description": option.description,
                    }))
                    .collect::<Vec<_>>(),
                "isOther": question.is_other,
                "isSecret": question.is_secret,
            })
        })
        .collect::<Vec<_>>();
    let first_question = request
        .questions
        .first()
        .map(|question| question.question.clone())
        .filter(|question| !question.trim().is_empty());

    CLIRuntimePendingRequest {
        kind: CLIRuntimeRequestKind::UserInput,
        title: Some("Input requested".to_owned()),
        message: first_question,
        native_request_id: Some(request.native_request_id.clone()),
        payload: Some(json!({
            "questions": questions,
            "nativeRequestId": request.native_request_id,
            "nativeRequestIdJson": request.native_request_id_json,
            "threadId": request.native_thread_id,
            "turnId": request.native_turn_id,
            "itemId": request.native_item_id,
        })),
    }
}

#[allow(dead_code)]
fn cli_runtime_pending_request_from_record(
    record: &CliRuntimePendingRequestRecord,
) -> CLIRuntimePendingRequest {
    let parsed_payload = serde_json::from_str::<JsonValue>(record.payload_json.as_str())
        .unwrap_or_else(|_| JsonValue::String(record.payload_json.clone()));
    if let Ok(request) = serde_json::from_value::<CLIRuntimePendingRequest>(parsed_payload.clone())
    {
        return request;
    }

    CLIRuntimePendingRequest {
        kind: cli_runtime_request_kind_from_stored(record.request_kind.as_str()),
        title: None,
        message: None,
        native_request_id: None,
        payload: Some(parsed_payload),
    }
}

fn cli_runtime_native_request_id_json_from_record(
    record: &CliRuntimePendingRequestRecord,
) -> anyhow::Result<JsonValue> {
    let pending_request = cli_runtime_pending_request_from_record(record);
    if let Some(payload) = pending_request.payload.as_ref() {
        if let Some(value) = payload
            .get("nativeRequestIdJson")
            .or_else(|| payload.get("native_request_id_json"))
        {
            return Ok(value.clone());
        }
    }
    if let Some(native_request_id) = pending_request.native_request_id.as_ref() {
        return Ok(JsonValue::String(native_request_id.clone()));
    }
    anyhow::bail!(
        "CLI runtime request `{}` does not include a native request id",
        record.request_id
    );
}

fn validate_cli_runtime_native_request_resolution(
    record: &CliRuntimePendingRequestRecord,
    resolution: &CLIRuntimeRequestResolution,
) -> anyhow::Result<()> {
    match cli_runtime_kind_config_from_stored_kind(record.runtime_kind.as_str()) {
        Some(GatewayCliAgentRuntimeKindConfig::Claude) => {
            let _ = claude_permission_response_from_resolution(record, resolution)?;
            return Ok(());
        }
        Some(GatewayCliAgentRuntimeKindConfig::Codex) => {}
        None => {
            anyhow::bail!(
                "unsupported CLI runtime kind `{}` for request `{}`",
                record.runtime_kind,
                record.request_id
            );
        }
    }

    match cli_runtime_request_kind_from_stored(record.request_kind.as_str()) {
        CLIRuntimeRequestKind::CommandApproval => {
            let _ = codex_command_approval_decision_from_resolution(resolution)?;
        }
        CLIRuntimeRequestKind::FileChangeApproval => {
            let _ = codex_file_change_approval_decision_from_resolution(resolution)?;
        }
        CLIRuntimeRequestKind::UserInput => {
            let _ = codex_user_input_response_from_resolution(record, resolution)?;
        }
        CLIRuntimeRequestKind::Other => {}
    }
    Ok(())
}

fn json_string_path(value: &JsonValue, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn redact_cli_runtime_native_payload(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => JsonValue::Object(
            object
                .iter()
                .map(|(key, value)| {
                    if cli_runtime_native_payload_key_is_sensitive(key) {
                        (key.clone(), JsonValue::String("[redacted]".to_owned()))
                    } else {
                        (key.clone(), redact_cli_runtime_native_payload(value))
                    }
                })
                .collect(),
        ),
        JsonValue::Array(items) => JsonValue::Array(
            items
                .iter()
                .map(redact_cli_runtime_native_payload)
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn cli_runtime_native_payload_key_is_sensitive(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("token")
        || key.contains("secret")
        || key.contains("password")
        || key.contains("api_key")
        || key.contains("apikey")
        || key.contains("authorization")
        || key == "auth"
}

fn codex_command_approval_decision_from_resolution(
    resolution: &CLIRuntimeRequestResolution,
) -> anyhow::Result<CodexCommandApprovalDecision> {
    match resolution {
        CLIRuntimeRequestResolution::Approved => Ok(CodexCommandApprovalDecision::Accept),
        CLIRuntimeRequestResolution::Denied { .. } => Ok(CodexCommandApprovalDecision::Decline),
        CLIRuntimeRequestResolution::Cancelled => Ok(CodexCommandApprovalDecision::Cancel),
        CLIRuntimeRequestResolution::Expired => Ok(CodexCommandApprovalDecision::Cancel),
        CLIRuntimeRequestResolution::Error { .. } => Ok(CodexCommandApprovalDecision::Decline),
        CLIRuntimeRequestResolution::Answered { response } => {
            let Some(response) = response.as_ref() else {
                anyhow::bail!("answered command approval did not include a response payload");
            };
            codex_command_approval_decision_from_value(response).ok_or_else(|| {
                anyhow::anyhow!(
                    "unsupported command approval decision response `{}`",
                    response
                )
            })
        }
    }
}

fn codex_command_approval_decision_from_value(
    value: &JsonValue,
) -> Option<CodexCommandApprovalDecision> {
    if let Some(decision) = value.get("decision") {
        return codex_command_approval_decision_from_value(decision);
    }
    if value.get("acceptWithExecpolicyAmendment").is_some()
        || value.get("applyNetworkPolicyAmendment").is_some()
    {
        return Some(CodexCommandApprovalDecision::Other(value.clone()));
    }
    let decision = value.as_str()?;
    let normalized = decision
        .chars()
        .filter(|char| !matches!(char, '_' | '-' | ' '))
        .flat_map(char::to_lowercase)
        .collect::<String>();
    match normalized.as_str() {
        "accept" | "allow" | "approve" | "approved" => Some(CodexCommandApprovalDecision::Accept),
        "acceptforsession" | "allowforsession" | "approvedforsession" => {
            Some(CodexCommandApprovalDecision::AcceptForSession)
        }
        "decline" | "deny" | "denied" | "reject" | "rejected" => {
            Some(CodexCommandApprovalDecision::Decline)
        }
        "cancel" | "cancelled" | "abort" | "aborted" => Some(CodexCommandApprovalDecision::Cancel),
        _ => None,
    }
}

fn codex_file_change_approval_decision_from_resolution(
    resolution: &CLIRuntimeRequestResolution,
) -> anyhow::Result<CodexFileChangeApprovalDecision> {
    match resolution {
        CLIRuntimeRequestResolution::Approved => Ok(CodexFileChangeApprovalDecision::Accept),
        CLIRuntimeRequestResolution::Denied { .. } => Ok(CodexFileChangeApprovalDecision::Decline),
        CLIRuntimeRequestResolution::Cancelled => Ok(CodexFileChangeApprovalDecision::Cancel),
        CLIRuntimeRequestResolution::Expired => Ok(CodexFileChangeApprovalDecision::Cancel),
        CLIRuntimeRequestResolution::Error { .. } => Ok(CodexFileChangeApprovalDecision::Decline),
        CLIRuntimeRequestResolution::Answered { response } => {
            let Some(response) = response.as_ref() else {
                anyhow::bail!("answered file change approval did not include a response payload");
            };
            codex_file_change_approval_decision_from_value(response).ok_or_else(|| {
                anyhow::anyhow!(
                    "unsupported file change approval decision response `{}`",
                    response
                )
            })
        }
    }
}

fn claude_permission_response_from_resolution(
    request: &CliRuntimePendingRequestRecord,
    resolution: &CLIRuntimeRequestResolution,
) -> anyhow::Result<(JsonValue, bool)> {
    let payload = cli_runtime_pending_request_from_record(request)
        .payload
        .unwrap_or(JsonValue::Null);
    let original_input = payload.get("input").cloned().unwrap_or(JsonValue::Null);
    let response = match resolution {
        CLIRuntimeRequestResolution::Approved | CLIRuntimeRequestResolution::Answered { .. } => {
            json!({
                "behavior": "allow",
                "updatedInput": original_input,
            })
        }
        CLIRuntimeRequestResolution::Denied { reason } => {
            json!({
                "behavior": "deny",
                "message": reason.clone().unwrap_or_else(|| "Denied by user".to_owned()),
            })
        }
        CLIRuntimeRequestResolution::Cancelled | CLIRuntimeRequestResolution::Expired => {
            json!({
                "behavior": "deny",
                "message": "Cancelled",
                "interrupt": true,
            })
        }
        CLIRuntimeRequestResolution::Error { message } => {
            json!({
                "behavior": "deny",
                "message": message.clone(),
            })
        }
    };
    let should_interrupt = matches!(
        resolution,
        CLIRuntimeRequestResolution::Cancelled | CLIRuntimeRequestResolution::Expired
    );
    Ok((response, should_interrupt))
}

fn codex_file_change_approval_decision_from_value(
    value: &JsonValue,
) -> Option<CodexFileChangeApprovalDecision> {
    if let Some(decision) = value.get("decision") {
        return codex_file_change_approval_decision_from_value(decision);
    }
    let decision = value.as_str()?;
    let normalized = decision
        .chars()
        .filter(|char| !matches!(char, '_' | '-' | ' '))
        .flat_map(char::to_lowercase)
        .collect::<String>();
    match normalized.as_str() {
        "accept" | "allow" | "approve" | "approved" => {
            Some(CodexFileChangeApprovalDecision::Accept)
        }
        "acceptforsession" | "allowforsession" | "approvedforsession" => {
            Some(CodexFileChangeApprovalDecision::AcceptForSession)
        }
        "decline" | "deny" | "denied" | "reject" | "rejected" => {
            Some(CodexFileChangeApprovalDecision::Decline)
        }
        "cancel" | "cancelled" | "abort" | "aborted" => {
            Some(CodexFileChangeApprovalDecision::Cancel)
        }
        _ => None,
    }
}

fn file_change_approval_timeline_status_for_resolution(
    resolution: &CLIRuntimeRequestResolution,
) -> &'static str {
    match resolution {
        CLIRuntimeRequestResolution::Approved | CLIRuntimeRequestResolution::Answered { .. } => {
            "approval_accepted"
        }
        CLIRuntimeRequestResolution::Denied { .. } | CLIRuntimeRequestResolution::Error { .. } => {
            "approval_declined"
        }
        CLIRuntimeRequestResolution::Cancelled | CLIRuntimeRequestResolution::Expired => {
            "approval_cancelled"
        }
    }
}

fn codex_user_input_response_from_resolution(
    record: &CliRuntimePendingRequestRecord,
    resolution: &CLIRuntimeRequestResolution,
) -> anyhow::Result<JsonValue> {
    match resolution {
        CLIRuntimeRequestResolution::Cancelled | CLIRuntimeRequestResolution::Expired => {
            Ok(codex_user_input_response(BTreeMap::new()))
        }
        CLIRuntimeRequestResolution::Answered { response } => {
            let question_ids = cli_runtime_user_input_question_ids_from_record(record)?;
            let Some(response) = response.as_ref() else {
                anyhow::bail!("answered user input request did not include answers");
            };
            let answers = codex_user_input_answers_from_value(response, question_ids.as_slice())?;
            Ok(codex_user_input_response(answers))
        }
        CLIRuntimeRequestResolution::Approved
        | CLIRuntimeRequestResolution::Denied { .. }
        | CLIRuntimeRequestResolution::Error { .. } => {
            anyhow::bail!("user input requests must be answered or cancelled")
        }
    }
}

fn cli_runtime_user_input_question_ids_from_record(
    record: &CliRuntimePendingRequestRecord,
) -> anyhow::Result<Vec<String>> {
    let pending_request = cli_runtime_pending_request_from_record(record);
    let Some(payload) = pending_request.payload.as_ref() else {
        anyhow::bail!(
            "user input request `{}` does not include payload",
            record.request_id
        );
    };
    let question_ids = payload
        .get("questions")
        .and_then(JsonValue::as_array)
        .map(|questions| {
            questions
                .iter()
                .filter_map(|question| question.get("id").and_then(JsonValue::as_str))
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if question_ids.is_empty() {
        anyhow::bail!(
            "user input request `{}` does not include question ids",
            record.request_id
        );
    }
    Ok(question_ids)
}

fn codex_user_input_answers_from_value(
    response: &JsonValue,
    question_ids: &[String],
) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
    let answers_value = response.get("answers").unwrap_or(response);
    let single_answer = response.get("answer");
    let mut answers = BTreeMap::new();

    for question_id in question_ids {
        let answer_value = answers_value
            .get(question_id)
            .or_else(|| (question_ids.len() == 1).then_some(single_answer).flatten());
        let Some(answer_value) = answer_value else {
            anyhow::bail!("missing answer for question `{question_id}`");
        };
        answers.insert(
            question_id.clone(),
            codex_user_input_answer_strings(question_id.as_str(), answer_value)?,
        );
    }

    Ok(answers)
}

fn codex_user_input_answer_strings(
    question_id: &str,
    value: &JsonValue,
) -> anyhow::Result<Vec<String>> {
    if let Some(answer) = value.as_str() {
        return Ok(vec![answer.to_owned()]);
    }
    if let Some(array) = value.as_array() {
        return array
            .iter()
            .map(|entry| {
                entry.as_str().map(str::to_owned).ok_or_else(|| {
                    anyhow::anyhow!("answer for question `{question_id}` must be a string")
                })
            })
            .collect();
    }
    if let Some(answers) = value.get("answers").and_then(JsonValue::as_array) {
        return answers
            .iter()
            .map(|entry| {
                entry.as_str().map(str::to_owned).ok_or_else(|| {
                    anyhow::anyhow!("answer for question `{question_id}` must be a string")
                })
            })
            .collect();
    }
    anyhow::bail!(
        "answer for question `{question_id}` must be a string, string array or answers object"
    );
}

#[allow(dead_code)]
pub(super) fn cli_runtime_request_kind_as_str(kind: CLIRuntimeRequestKind) -> &'static str {
    match kind {
        CLIRuntimeRequestKind::CommandApproval => "command_approval",
        CLIRuntimeRequestKind::FileChangeApproval => "file_change_approval",
        CLIRuntimeRequestKind::UserInput => "user_input",
        CLIRuntimeRequestKind::Other => "other",
    }
}

#[allow(dead_code)]
fn cli_runtime_request_kind_from_stored(value: &str) -> CLIRuntimeRequestKind {
    match value {
        "command_approval" => CLIRuntimeRequestKind::CommandApproval,
        "file_change_approval" => CLIRuntimeRequestKind::FileChangeApproval,
        "user_input" => CLIRuntimeRequestKind::UserInput,
        _ => CLIRuntimeRequestKind::Other,
    }
}

fn cli_runtime_request_kind_requires_turn_binding(kind: CLIRuntimeRequestKind) -> bool {
    matches!(
        kind,
        CLIRuntimeRequestKind::CommandApproval
            | CLIRuntimeRequestKind::FileChangeApproval
            | CLIRuntimeRequestKind::UserInput
    )
}

fn cli_runtime_pending_request_is_human_wait(request_kind: &str) -> bool {
    cli_runtime_request_kind_requires_turn_binding(cli_runtime_request_kind_from_stored(
        request_kind,
    ))
}

fn cli_runtime_event_can_use_running_turn_fallback(event: &RuntimeEvent) -> bool {
    matches!(event, RuntimeEvent::ThreadStateChanged(_))
        || matches!(
            event,
            RuntimeEvent::Raw(raw)
                if cli_runtime_raw_method_can_use_running_turn_fallback(raw.native_method.as_str())
        )
}

fn cli_runtime_raw_method_can_use_running_turn_fallback(method: &str) -> bool {
    matches!(
        method,
        "thread/tokenUsage/updated"
            | "fuzzyFileSearch/sessionUpdated"
            | "fuzzyFileSearch/sessionCompleted"
            | "windowsSandbox/setupCompleted"
    )
}

fn cli_runtime_event_log_label(event: &RuntimeEvent) -> String {
    match event {
        RuntimeEvent::Raw(raw) => raw.native_method.clone(),
        RuntimeEvent::SessionStateChanged(_) => "session_state_changed".to_owned(),
        RuntimeEvent::ThreadStateChanged(_) => "thread_state_changed".to_owned(),
        RuntimeEvent::TurnStarted(_) => "turn_started".to_owned(),
        RuntimeEvent::TurnCompleted(_) => "turn_completed".to_owned(),
        RuntimeEvent::TurnFailed(_) => "turn_failed".to_owned(),
        RuntimeEvent::TurnInterrupted(_) => "turn_interrupted".to_owned(),
        RuntimeEvent::ItemStarted(_) => "item_started".to_owned(),
        RuntimeEvent::ItemDelta(_) => "item_delta".to_owned(),
        RuntimeEvent::ItemCompleted(_) => "item_completed".to_owned(),
        RuntimeEvent::ItemUpdated(_) => "item_updated".to_owned(),
        RuntimeEvent::PlanUpdated(_) => "plan_updated".to_owned(),
        RuntimeEvent::DiffUpdated(_) => "diff_updated".to_owned(),
        RuntimeEvent::RequestOpened(_) => "request_opened".to_owned(),
        RuntimeEvent::RequestResolved(_) => "request_resolved".to_owned(),
        RuntimeEvent::AccountUpdated(_) => "account_updated".to_owned(),
        RuntimeEvent::AppListUpdated(_) => "app_list_updated".to_owned(),
        RuntimeEvent::ReviewModeChanged(_) => "review_mode_changed".to_owned(),
        RuntimeEvent::Error(_) => "error".to_owned(),
    }
}

fn cli_runtime_native_turn_id_for_event(event: &RuntimeEvent) -> Option<&str> {
    match event {
        RuntimeEvent::TurnStarted(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::TurnCompleted(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::TurnFailed(event) => event.native_turn_id.as_deref(),
        RuntimeEvent::TurnInterrupted(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::ItemStarted(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::ItemDelta(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::ItemCompleted(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::ItemUpdated(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::PlanUpdated(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::DiffUpdated(event) => Some(event.native_turn_id.as_str()),
        RuntimeEvent::ReviewModeChanged(event) => event.native_turn_id.as_deref(),
        RuntimeEvent::Error(event) => event.native_turn_id.as_deref(),
        RuntimeEvent::Raw(event) => event.native_turn_id.as_deref(),
        RuntimeEvent::RequestOpened(event) => event.native_turn_id.as_deref(),
        RuntimeEvent::SessionStateChanged(_)
        | RuntimeEvent::ThreadStateChanged(_)
        | RuntimeEvent::RequestResolved(_)
        | RuntimeEvent::AccountUpdated(_)
        | RuntimeEvent::AppListUpdated(_) => None,
    }
}

fn cli_runtime_native_item_id_for_event(event: &RuntimeEvent) -> Option<&str> {
    match event {
        RuntimeEvent::ItemStarted(event) => Some(event.native_item_id.as_str()),
        RuntimeEvent::ItemDelta(event) => Some(event.native_item_id.as_str()),
        RuntimeEvent::ItemCompleted(event) => Some(event.native_item_id.as_str()),
        RuntimeEvent::ItemUpdated(event) => Some(event.native_item_id.as_str()),
        RuntimeEvent::RequestOpened(event) => event.native_item_id.as_deref(),
        RuntimeEvent::Raw(event) => event.native_item_id.as_deref(),
        _ => None,
    }
}

fn cli_runtime_native_thread_id_for_event(event: &RuntimeEvent) -> Option<&str> {
    match event {
        RuntimeEvent::ThreadStateChanged(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::TurnStarted(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::TurnCompleted(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::TurnFailed(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::TurnInterrupted(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::ItemStarted(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::ItemDelta(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::ItemCompleted(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::ItemUpdated(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::PlanUpdated(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::DiffUpdated(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::RequestOpened(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::ReviewModeChanged(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::Error(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::Raw(event) => event.native_thread_id.as_deref(),
        RuntimeEvent::SessionStateChanged(_)
        | RuntimeEvent::RequestResolved(_)
        | RuntimeEvent::AccountUpdated(_)
        | RuntimeEvent::AppListUpdated(_) => None,
    }
}

fn cli_runtime_turn_status_for_terminal_event(event: &RuntimeEvent) -> Option<TurnStatus> {
    match event {
        RuntimeEvent::TurnCompleted(_) => Some(TurnStatus::Completed),
        RuntimeEvent::TurnFailed(_) | RuntimeEvent::Error(_) => Some(TurnStatus::Failed),
        RuntimeEvent::TurnInterrupted(_) => Some(TurnStatus::Interrupted),
        _ => None,
    }
}

fn cli_runtime_event_requires_native_journal(event: &RuntimeEvent) -> bool {
    match event {
        RuntimeEvent::ItemDelta(_)
        | RuntimeEvent::DiffUpdated(_)
        | RuntimeEvent::AccountUpdated(_)
        | RuntimeEvent::AppListUpdated(_) => false,
        RuntimeEvent::Raw(raw) => !matches!(
            raw.native_method.as_str(),
            "thread/tokenUsage/updated"
                | "account/rateLimits/updated"
                | "item/agentMessage/delta"
                | "item/commandExecution/outputDelta"
                | "item/reasoning/textDelta"
                | "item/reasoning/summaryTextDelta"
                | "turn/diff/updated"
        ),
        _ => true,
    }
}

fn cli_runtime_turn_binding_status_is_active(status: &str) -> bool {
    matches!(
        status,
        crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_STARTING
            | crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
    )
}

fn cli_runtime_turn_binding_matches_native_activity(
    binding: &pioneer_crud::CliRuntimeTurnBindingRecord,
    native_thread_id: Option<&str>,
    native_turn_id: Option<&str>,
) -> bool {
    native_thread_id.is_some_and(|native_thread_id| binding.native_thread_id == native_thread_id)
        && native_turn_id
            .is_some_and(|native_turn_id| binding.native_turn_id.as_deref() == Some(native_turn_id))
}

fn cli_runtime_turn_status_label(status: TurnStatus) -> &'static str {
    match status {
        TurnStatus::InProgress => "in_progress",
        TurnStatus::Completed => "completed",
        TurnStatus::Failed => "failed",
        TurnStatus::Interrupted => "interrupted",
        TurnStatus::Blocked => "blocked",
    }
}

fn cli_runtime_request_status_for_resolution(
    resolution: &CLIRuntimeRequestResolution,
) -> StoredCliRuntimePendingRequestStatus {
    match resolution {
        CLIRuntimeRequestResolution::Approved
        | CLIRuntimeRequestResolution::Denied { .. }
        | CLIRuntimeRequestResolution::Answered { .. } => {
            StoredCliRuntimePendingRequestStatus::Answered
        }
        CLIRuntimeRequestResolution::Cancelled => StoredCliRuntimePendingRequestStatus::Cancelled,
        CLIRuntimeRequestResolution::Expired => StoredCliRuntimePendingRequestStatus::Expired,
        CLIRuntimeRequestResolution::Error { .. } => StoredCliRuntimePendingRequestStatus::Resolved,
    }
}

fn protocol_status_from_stored_status(
    status: StoredCliRuntimePendingRequestStatus,
) -> CLIRuntimePendingRequestStatus {
    match status {
        StoredCliRuntimePendingRequestStatus::Pending => CLIRuntimePendingRequestStatus::Pending,
        StoredCliRuntimePendingRequestStatus::Answered => CLIRuntimePendingRequestStatus::Answered,
        StoredCliRuntimePendingRequestStatus::Resolved => CLIRuntimePendingRequestStatus::Resolved,
        StoredCliRuntimePendingRequestStatus::Cancelled => {
            CLIRuntimePendingRequestStatus::Cancelled
        }
        StoredCliRuntimePendingRequestStatus::Expired => CLIRuntimePendingRequestStatus::Expired,
    }
}

fn cli_runtime_request_timestamp() -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

fn current_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::RUNTIME_DIAGNOSTIC_MAX_LINES;
    use serde_json::json;

    fn effective_instance(
        id: &str,
        enabled: bool,
    ) -> EffectiveGatewayCliAgentRuntimeInstanceConfig {
        EffectiveGatewayCliAgentRuntimeInstanceConfig {
            id: id.to_owned(),
            kind: GatewayCliAgentRuntimeKindConfig::Codex,
            display_name: format!("Codex {id}"),
            enabled,
            binary_path: "codex".to_owned(),
            home_path: "~/.codex".to_owned(),
            shadow_home_path: None,
            custom_models: Vec::new(),
            app_server_args: Vec::new(),
            startup_probe_timeout_ms: 15_000,
            request_timeout_ms: 60_000,
            idle_session_ttl_secs: 1_800,
            event_channel_capacity: 2_048,
            stderr_ring_lines: 200,
            debug_native_events: false,
        }
    }

    fn cli_runtime_summaries_from_instances(
        instances: Vec<EffectiveGatewayCliAgentRuntimeInstanceConfig>,
    ) -> Vec<RuntimeSummary> {
        let mut summaries = instances
            .into_iter()
            .map(|instance| super::cli_runtime_summary_from_instance(instance, None))
            .collect::<Vec<_>>();
        super::sort_cli_runtime_summary_display_order(summaries.as_mut_slice());
        summaries
    }

    fn cli_runtime_summary_from_instance(
        instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
    ) -> RuntimeSummary {
        super::cli_runtime_summary_from_instance(instance, None)
    }

    #[test]
    fn cli_runtime_catalog_marks_enabled_instances_as_unprobed() {
        let summaries =
            cli_runtime_summaries_from_instances(vec![effective_instance("codex", true)]);

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].runtime_id, "codex");
        assert_eq!(summaries[0].kind, CLIAgentRuntimeKind::Codex);
        assert!(summaries[0].enabled);
        assert!(matches!(
            summaries[0].status,
            RuntimeStatus::Degraded { .. }
        ));
        assert_eq!(summaries[0].diagnostics[0].code, "cli_runtime.unprobed");
        assert!(summaries[0].capabilities.supports_threads);
        assert!(summaries[0].capabilities.supports_model_list);
    }

    #[test]
    fn cli_runtime_catalog_uses_product_display_order() {
        let mut claude = effective_instance("claude", true);
        claude.kind = GatewayCliAgentRuntimeKindConfig::Claude;
        claude.display_name = "Claude CLI".to_owned();

        let summaries = cli_runtime_summaries_from_instances(vec![
            effective_instance("custom_b", true),
            claude,
            effective_instance("codex", true),
            effective_instance("custom_a", true),
        ]);

        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.runtime_id.as_str())
                .collect::<Vec<_>>(),
            vec!["codex", "claude", "custom_b", "custom_a"]
        );
    }

    #[test]
    fn cli_runtime_capability_matrix_matches_runtime_contracts() {
        let codex = cli_runtime_capabilities_for_kind(GatewayCliAgentRuntimeKindConfig::Codex);
        assert!(codex.supports_skills);
        assert!(codex.supports_resume);
        assert!(codex.supports_fork);
        assert!(codex.supports_steer);
        assert!(codex.supports_auth_management);

        let claude = cli_runtime_capabilities_for_kind(GatewayCliAgentRuntimeKindConfig::Claude);
        assert!(claude.supports_skills);
        assert!(claude.supports_threads);
        assert!(claude.supports_model_list);
        assert!(claude.supports_interrupt);
        assert!(claude.supports_approvals);
        assert!(!claude.supports_resume);
        assert!(!claude.supports_fork);
        assert!(!claude.supports_steer);
        assert!(!claude.supports_auth_management);
    }

    #[test]
    fn cli_runtime_catalog_marks_disabled_instances_as_disabled() {
        let summaries =
            cli_runtime_summaries_from_instances(vec![effective_instance("codex_work", false)]);

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].runtime_id, "codex_work");
        assert!(!summaries[0].enabled);
        assert_eq!(summaries[0].status, RuntimeStatus::Disabled);
        assert!(summaries[0].diagnostics.is_empty());
    }

    #[test]
    fn cli_runtime_summary_includes_prepared_proxy_url() {
        let summary = super::cli_runtime_summary_from_instance(
            effective_instance("codex_work", true),
            Some("socks5://user:pass@127.0.0.1:1080".to_owned()),
        );

        assert_eq!(summary.runtime_id, "codex_work");
        assert_eq!(
            summary.proxy_url.as_deref(),
            Some("socks5://user:pass@127.0.0.1:1080")
        );
    }

    #[test]
    fn cli_runtime_status_maps_ready_account_probe() {
        let summary = cli_runtime_summary_from_instance(effective_instance("codex", true));
        let runtime = apply_codex_account_probe_to_summary(
            summary,
            CodexAccountProbeSnapshot {
                status: CodexAccountProbeStatus::Ready,
                message: None,
                user_agent: Some("codex/1.2.3 darwin".to_owned()),
                version: Some("1.2.3".to_owned()),
                account: Some(CodexAccountSnapshot {
                    authenticated: true,
                    account_id: Some("acct_123".to_owned()),
                    email: Some("alex@example.com".to_owned()),
                    display_name: Some("Alex".to_owned()),
                    plan: Some("pro".to_owned()),
                    auth_method: Some("chatgpt".to_owned()),
                }),
                requires_openai_auth: false,
                diagnostics: vec![CodexProbeDiagnostic {
                    level: CodexProbeDiagnosticLevel::Info,
                    code: "codex_probe.ready".to_owned(),
                    message: "Codex app-server probe succeeded".to_owned(),
                }],
                stderr: Vec::new(),
            },
        );

        assert_eq!(runtime.status, RuntimeStatus::Ready);
        assert_eq!(runtime.version.as_deref(), Some("1.2.3"));
        assert_eq!(
            runtime
                .account
                .as_ref()
                .and_then(|account| account.email.as_deref()),
            Some("alex@example.com")
        );
        assert_eq!(runtime.diagnostics[0].code, "codex_probe.ready");
        assert!(!runtime.capabilities.supports_review);
        assert!(!runtime.capabilities.supports_compaction);
        assert!(!runtime.capabilities.supports_goal);
        assert!(!runtime.capabilities.supports_thread_archive);
        assert!(
            !runtime
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cli_runtime.unprobed")
        );
    }

    #[test]
    fn cli_runtime_diagnostics_redacts_and_bounds_account_stderr() {
        let summary = cli_runtime_summary_from_instance(effective_instance("codex", true));
        let long_blob = "x".repeat(900);
        let stderr = (0..45)
            .map(|index| {
                if index == 44 {
                    format!(
                        "headers={{Authorization: Bearer sk-proj-secret}} OPENAI_API_KEY=sk-secret stderr {long_blob}"
                    )
                } else {
                    format!("stderr line {index}")
                }
            })
            .collect::<Vec<_>>();

        let runtime = apply_codex_account_probe_to_summary(
            summary,
            CodexAccountProbeSnapshot {
                status: CodexAccountProbeStatus::Ready,
                message: None,
                user_agent: Some("codex/1.2.3 darwin".to_owned()),
                version: Some("1.2.3".to_owned()),
                account: None,
                requires_openai_auth: false,
                diagnostics: Vec::new(),
                stderr,
            },
        );

        assert_eq!(runtime.recent_stderr.len(), RUNTIME_DIAGNOSTIC_MAX_LINES);
        assert_eq!(runtime.recent_stderr[0], "stderr line 5");
        let last = runtime.recent_stderr.last().expect("last stderr line");
        assert!(last.contains("[REDACTED]"));
        assert!(!last.contains("sk-secret"));
        assert!(!last.contains("sk-proj-secret"));
        assert!(last.contains("[truncated "));
    }

    #[test]
    fn cli_runtime_status_maps_needs_auth_account_probe() {
        let summary = cli_runtime_summary_from_instance(effective_instance("codex", true));
        let runtime = apply_codex_account_probe_to_summary(
            summary,
            CodexAccountProbeSnapshot {
                status: CodexAccountProbeStatus::NeedsAuth,
                message: Some("Codex CLI is not authenticated".to_owned()),
                user_agent: Some("codex/1.2.3 darwin".to_owned()),
                version: Some("1.2.3".to_owned()),
                account: None,
                requires_openai_auth: true,
                diagnostics: vec![CodexProbeDiagnostic {
                    level: CodexProbeDiagnosticLevel::Warning,
                    code: "codex_probe.needs_auth".to_owned(),
                    message: "Codex CLI is not authenticated".to_owned(),
                }],
                stderr: Vec::new(),
            },
        );

        assert_eq!(runtime.status, RuntimeStatus::NeedsAuth);
        assert_eq!(
            runtime.account,
            Some(RuntimeAccountSnapshot {
                authenticated: false,
                account_id: None,
                email: None,
                display_name: None,
                plan: None,
                auth_method: None,
            })
        );
        assert_eq!(
            runtime.diagnostics[0].level,
            RuntimeDiagnosticLevel::Warning
        );
        assert_eq!(runtime.diagnostics[0].code, "codex_probe.needs_auth");
    }

    #[test]
    fn cli_runtime_status_maps_missing_binary_account_probe() {
        let summary = cli_runtime_summary_from_instance(effective_instance("codex", true));
        let runtime = apply_codex_account_probe_to_summary(
            summary,
            CodexAccountProbeSnapshot {
                status: CodexAccountProbeStatus::MissingBinary,
                message: Some("Codex CLI binary `codex` was not found".to_owned()),
                user_agent: None,
                version: None,
                account: None,
                requires_openai_auth: false,
                diagnostics: vec![CodexProbeDiagnostic {
                    level: CodexProbeDiagnosticLevel::Error,
                    code: "codex_probe.missing_binary".to_owned(),
                    message: "Codex CLI binary `codex` was not found".to_owned(),
                }],
                stderr: Vec::new(),
            },
        );

        assert_eq!(
            runtime.status,
            RuntimeStatus::MissingBinary {
                binary_path: Some("codex".to_owned())
            }
        );
        assert!(runtime.account.is_none());
        assert_eq!(runtime.diagnostics[0].level, RuntimeDiagnosticLevel::Error);
        assert_eq!(runtime.diagnostics[0].code, "codex_probe.missing_binary");
    }

    #[test]
    fn cli_runtime_list_models_maps_codex_model_metadata() {
        let runtime_model = runtime_model_from_codex_model(CodexModelSnapshot {
            id: "gpt-5.4".to_owned(),
            name: Some("GPT-5.4".to_owned()),
            description: Some("Flagship reasoning model".to_owned()),
            family: Some("gpt-5".to_owned()),
            active: Some(true),
            effort_options: vec!["low".to_owned(), "high".to_owned()],
            input_modalities: vec!["text".to_owned(), "image".to_owned()],
            output_modalities: vec!["text".to_owned()],
            supports_reasoning: Some(true),
            supports_vision: Some(true),
            max_input_tokens: Some(128_000),
            max_output_tokens: Some(8_192),
        });

        assert_eq!(runtime_model.id, "gpt-5.4");
        assert_eq!(runtime_model.name.as_deref(), Some("GPT-5.4"));
        assert!(!runtime_model.is_custom);
        assert_eq!(runtime_model.effort_options, vec!["low", "high"]);
        assert_eq!(runtime_model.supports_reasoning, Some(true));
        assert_eq!(runtime_model.supports_vision, Some(true));
    }

    #[test]
    fn cli_runtime_list_models_appends_custom_model_dedupe() {
        let models = runtime_models_with_custom_models(
            vec![runtime_model("gpt-5.4", false)],
            &[
                "gpt-5.4".to_owned(),
                "  gpt-custom ".to_owned(),
                "gpt-custom".to_owned(),
                " ".to_owned(),
            ],
        );

        assert_eq!(
            models
                .iter()
                .map(|model| (model.id.as_str(), model.is_custom))
                .collect::<Vec<_>>(),
            vec![("gpt-5.4", false), ("gpt-custom", true)]
        );
        assert!(
            models[1]
                .description
                .as_deref()
                .expect("custom model description")
                .contains("capability metadata")
        );
    }

    #[test]
    fn cli_runtime_list_models_needs_auth_returns_custom_models() {
        let models = runtime_models_from_codex_probe(
            CodexModelListProbeSnapshot {
                status: CodexModelListProbeStatus::NeedsAuth,
                message: Some("Codex CLI is not authenticated".to_owned()),
                user_agent: Some("codex/1.2.3".to_owned()),
                version: Some("1.2.3".to_owned()),
                models: vec![CodexModelSnapshot {
                    id: "should-not-leak".to_owned(),
                    name: None,
                    description: None,
                    family: None,
                    active: None,
                    effort_options: Vec::new(),
                    input_modalities: Vec::new(),
                    output_modalities: Vec::new(),
                    supports_reasoning: None,
                    supports_vision: None,
                    max_input_tokens: None,
                    max_output_tokens: None,
                }],
                requires_openai_auth: true,
                diagnostics: vec![CodexProbeDiagnostic {
                    level: CodexProbeDiagnosticLevel::Warning,
                    code: "codex_probe.needs_auth".to_owned(),
                    message: "Codex CLI is not authenticated".to_owned(),
                }],
                stderr: Vec::new(),
            },
            &["gpt-custom".to_owned()],
        );

        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "gpt-custom");
        assert!(models[0].is_custom);
    }

    #[test]
    fn cli_runtime_login_maps_device_code_start_response() {
        let response = cli_runtime_login_start_response_from_codex(
            "codex".to_owned(),
            CLIRuntimeLoginStartType::ChatgptDeviceCode,
            CodexLoginStartSnapshot {
                status: CodexLoginStartStatus::Started,
                login_type: "chatgptDeviceCode".to_owned(),
                message: None,
                response: Some(CodexLoginStartResponse {
                    login_type: Some("chatgptDeviceCode".to_owned()),
                    login_id: Some("login_123".to_owned()),
                    verification_url: Some("https://auth.openai.com/codex/device".to_owned()),
                    user_code: Some("ABCD-1234".to_owned()),
                    auth_url: None,
                    message: None,
                    raw: json!({
                        "type": "chatgptDeviceCode",
                        "loginId": "login_123",
                        "verificationUrl": "https://auth.openai.com/codex/device",
                        "userCode": "ABCD-1234"
                    }),
                    extra: Default::default(),
                }),
                diagnostics: Vec::new(),
                stderr: Vec::new(),
            },
        );

        assert_eq!(response.runtime_id, "codex");
        assert_eq!(
            response.login_type,
            CLIRuntimeLoginStartType::ChatgptDeviceCode
        );
        assert_eq!(response.status, RuntimeStatus::NeedsAuth);
        assert_eq!(response.login_id.as_deref(), Some("login_123"));
        assert_eq!(response.user_code.as_deref(), Some("ABCD-1234"));
        assert!(response.raw.is_some());
    }

    #[test]
    fn cli_runtime_login_maps_missing_binary_status() {
        let response = cli_runtime_login_start_response_from_codex(
            "codex".to_owned(),
            CLIRuntimeLoginStartType::ChatgptDeviceCode,
            CodexLoginStartSnapshot {
                status: CodexLoginStartStatus::MissingBinary,
                login_type: "chatgptDeviceCode".to_owned(),
                message: Some("Codex CLI binary `codex` was not found".to_owned()),
                response: None,
                diagnostics: Vec::new(),
                stderr: Vec::new(),
            },
        );

        assert_eq!(
            response.status,
            RuntimeStatus::MissingBinary { binary_path: None }
        );
        assert_eq!(
            response.message.as_deref(),
            Some("Codex CLI binary `codex` was not found")
        );
    }

    fn runtime_model(id: &str, is_custom: bool) -> RuntimeModelInfo {
        RuntimeModelInfo {
            id: id.to_owned(),
            name: Some(id.to_owned()),
            description: None,
            family: None,
            is_custom,
            active: None,
            effort_options: Vec::new(),
            input_modalities: Vec::new(),
            output_modalities: Vec::new(),
            supports_reasoning: None,
            supports_vision: None,
            max_input_tokens: None,
            max_output_tokens: None,
        }
    }
}
