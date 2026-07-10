use crate::cli_runtime::config::{
    codex_account_probe_config_from_instance, load_effective_cli_runtime_instances,
};
use crate::cli_runtime::manager::{
    CLIAgentRuntimeCodexEventReceivers, CLIAgentRuntimeManager, CLIAgentRuntimeObservedTurnStatus,
    CLIAgentRuntimeSession, CLIAgentRuntimeSessionFactory, CLIAgentRuntimeSessionKey,
    CLIAgentRuntimeSessionStartOptions, CLIAgentRuntimeThreadForkRequest,
    CLIAgentRuntimeThreadForkResult, CLIAgentRuntimeThreadNameSetRequest,
    CLIAgentRuntimeThreadNameSetResult, CLIAgentRuntimeThreadOpenParams,
    CLIAgentRuntimeThreadOpenSnapshot, CLIAgentRuntimeTurnObservation,
    CLIAgentRuntimeTurnStartParams, CLIAgentRuntimeTurnStartSnapshot,
    CLIAgentRuntimeTurnSteerRequest, CLIAgentRuntimeTurnSteerResult,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_cli_agent_runtime::codex::{
    CodexAppServerClient, CodexJsonlRpcClient, CodexJsonlRpcNotificationEvent,
    CodexThreadForkParams, CodexThreadNameSetParams, CodexThreadStartParams, CodexTurnStartParams,
    CodexTurnSteerParams, codex_app_server_process_config,
};
use pioneer_cli_agent_runtime::driver::JsonlRpcId;
use pioneer_cli_agent_runtime::event::{RuntimeEventMappingOptions, map_codex_notification_event};
use pioneer_cli_agent_runtime::process::{CLIAgentProcess, spawn_cli_agent_process};
use pioneer_config::{
    EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::BufReader;

pub(crate) fn cli_runtime_manager(
    runtime_home: PathBuf,
    idle_session_ttl: Duration,
) -> Result<Arc<CLIAgentRuntimeManager>> {
    let factory = Arc::new(DispatchingCLIAgentRuntimeSessionFactory { runtime_home });
    Ok(Arc::new(CLIAgentRuntimeManager::new(
        factory,
        idle_session_ttl,
    )?))
}

struct DispatchingCLIAgentRuntimeSessionFactory {
    runtime_home: PathBuf,
}

#[async_trait]
impl CLIAgentRuntimeSessionFactory for DispatchingCLIAgentRuntimeSessionFactory {
    async fn start_session(
        &self,
        key: &CLIAgentRuntimeSessionKey,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        self.start_session_with_options(key, &CLIAgentRuntimeSessionStartOptions::default())
            .await
    }

    async fn start_session_with_options(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        options: &CLIAgentRuntimeSessionStartOptions,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        let instance = load_effective_cli_runtime_instances(self.runtime_home.as_path())?
            .into_iter()
            .find(|instance| instance.id == key.runtime_id)
            .ok_or_else(|| anyhow!("unknown CLI runtime `{}`", key.runtime_id))?;
        match instance.kind {
            GatewayCliAgentRuntimeKindConfig::Codex => {
                CodexCLIAgentRuntimeSessionFactory {
                    runtime_home: self.runtime_home.clone(),
                }
                .start_session_with_options(key, options)
                .await
            }
            GatewayCliAgentRuntimeKindConfig::Claude => {
                crate::cli_runtime::claude_session::ClaudeCLIAgentRuntimeSessionFactory::new(
                    self.runtime_home.clone(),
                )
                .start_session_with_options(key, options)
                .await
            }
        }
    }
}

struct CodexCLIAgentRuntimeSessionFactory {
    runtime_home: PathBuf,
}

#[async_trait]
impl CLIAgentRuntimeSessionFactory for CodexCLIAgentRuntimeSessionFactory {
    async fn start_session(
        &self,
        key: &CLIAgentRuntimeSessionKey,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        self.start_session_with_options(key, &CLIAgentRuntimeSessionStartOptions::default())
            .await
    }

    async fn start_session_with_options(
        &self,
        key: &CLIAgentRuntimeSessionKey,
        options: &CLIAgentRuntimeSessionStartOptions,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        let instance = self.runtime_instance(key.runtime_id.as_str())?;
        if !instance.enabled {
            bail!("CLI runtime `{}` is disabled", instance.id);
        }
        if instance.kind != GatewayCliAgentRuntimeKindConfig::Codex {
            bail!(
                "CLI runtime `{}` is configured as unsupported kind `{:?}`",
                instance.id,
                instance.kind
            );
        }

        let mut probe_config = codex_account_probe_config_from_instance(&instance);
        probe_config.cwd = options.cwd.clone().or_else(|| std::env::current_dir().ok());
        let mut process_config = codex_app_server_process_config(&probe_config)
            .map_err(|error| anyhow!("failed to prepare Codex home layout: {error}"))?;
        process_config.args.extend(instance.app_server_args.clone());
        process_config.args.extend(options.app_server_args.clone());
        for (key, value) in &options.env {
            process_config = process_config.with_env(key, value);
        }
        let mut process = spawn_cli_agent_process(&process_config).with_context(|| {
            format!(
                "failed to spawn Codex app-server for CLI runtime `{}`",
                instance.id
            )
        })?;
        let stderr = process.stderr();
        let (stdout, stdin) = process.take_stdio()?;
        let rpc = CodexJsonlRpcClient::new_with_channel_capacity(
            BufReader::new(stdout),
            stdin,
            instance.event_channel_capacity,
            instance.event_channel_capacity,
            instance.event_channel_capacity,
        );
        let notifications = rpc
            .take_notification_receiver()
            .ok_or_else(|| anyhow!("Codex notification receiver was already taken"))?;
        let server_requests = rpc
            .take_server_request_receiver()
            .ok_or_else(|| anyhow!("Codex server request receiver was already taken"))?;
        let diagnostics = rpc
            .take_diagnostic_receiver()
            .ok_or_else(|| anyhow!("Codex diagnostic receiver was already taken"))?;
        let client = CodexAppServerClient::new(rpc);
        client
            .initialize(Duration::from_millis(instance.startup_probe_timeout_ms))
            .await
            .context("Codex initialize handshake failed")?;

        Ok(Arc::new(CodexCLIAgentRuntimeSession {
            client,
            process: tokio::sync::Mutex::new(process),
            request_timeout: Duration::from_millis(instance.request_timeout_ms),
            shutdown_grace: Duration::from_secs(2),
            event_receivers: StdMutex::new(Some(CLIAgentRuntimeCodexEventReceivers {
                notifications,
                server_requests,
                diagnostics,
            })),
            stderr,
        }))
    }
}

impl CodexCLIAgentRuntimeSessionFactory {
    fn runtime_instance(
        &self,
        runtime_id: &str,
    ) -> Result<EffectiveGatewayCliAgentRuntimeInstanceConfig> {
        load_effective_cli_runtime_instances(self.runtime_home.as_path())?
            .into_iter()
            .find(|instance| instance.id == runtime_id)
            .ok_or_else(|| anyhow!("unknown CLI runtime `{runtime_id}`"))
    }
}

struct CodexCLIAgentRuntimeSession {
    client: CodexAppServerClient,
    process: tokio::sync::Mutex<CLIAgentProcess>,
    request_timeout: Duration,
    shutdown_grace: Duration,
    event_receivers: StdMutex<Option<CLIAgentRuntimeCodexEventReceivers>>,
    #[allow(dead_code)]
    stderr: pioneer_cli_agent_runtime::process::StderrRing,
}

#[async_trait]
impl CLIAgentRuntimeSession for CodexCLIAgentRuntimeSession {
    async fn close(&self) -> Result<()> {
        if let Err(error) = self.client.rpc().shutdown().await {
            tracing::warn!(
                error = %format!("{error:#}"),
                "failed to request Codex CLI runtime shutdown"
            );
        }
        let mut process = self.process.lock().await;
        let _ = process.terminate_with_grace(self.shutdown_grace).await?;
        Ok(())
    }

    fn take_codex_event_receivers(&self) -> Option<CLIAgentRuntimeCodexEventReceivers> {
        self.event_receivers
            .lock()
            .expect("Codex event receiver mutex should not be poisoned")
            .take()
    }

    fn supports_thread_name_sync(&self) -> bool {
        true
    }

    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let opened = self
            .client
            .thread_start(
                CodexThreadStartParams {
                    cwd: params.cwd,
                    approval_policy: params
                        .approval_policy
                        .unwrap_or_else(|| "default".to_owned()),
                    sandbox: params.permissions.is_none().then(|| {
                        params
                            .sandbox
                            .as_ref()
                            .and_then(|value| value.as_str().map(str::to_owned))
                            .unwrap_or_else(|| "read-only".to_owned())
                    }),
                    permissions: params.permissions,
                    model: params.model,
                    service_tier: params.service_tier,
                },
                timeout,
            )
            .await
            .context("Codex thread/start failed")?;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: opened.native_thread_id,
            cwd: opened.cwd,
            model: opened.model,
            raw: opened.raw,
        })
    }

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let opened = self
            .client
            .thread_resume(
                native_thread_id,
                CodexThreadStartParams {
                    cwd: params.cwd,
                    approval_policy: params
                        .approval_policy
                        .unwrap_or_else(|| "default".to_owned()),
                    sandbox: params.permissions.is_none().then(|| {
                        params
                            .sandbox
                            .as_ref()
                            .and_then(|value| value.as_str().map(str::to_owned))
                            .unwrap_or_else(|| "read-only".to_owned())
                    }),
                    permissions: params.permissions,
                    model: params.model,
                    service_tier: params.service_tier,
                },
                timeout,
            )
            .await
            .context("Codex thread/resume failed")?;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: opened.native_thread_id,
            cwd: opened.cwd,
            model: opened.model,
            raw: opened.raw,
        })
    }

    async fn start_turn(
        &self,
        params: CLIAgentRuntimeTurnStartParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeTurnStartSnapshot> {
        let input = serde_json::from_value(params.input)
            .context("failed to decode generic CLI runtime input for Codex")?;
        let started = self
            .client
            .turn_start(
                CodexTurnStartParams {
                    thread_id: params.native_thread_id,
                    input,
                    cwd: params.cwd,
                    approval_policy: params.approval_policy,
                    sandbox_policy: params
                        .permissions
                        .is_none()
                        .then_some(params.sandbox)
                        .flatten(),
                    permissions: params.permissions,
                    model: params.model,
                    effort: params.effort,
                    personality: params.personality,
                    summary: params.summary,
                },
                timeout,
            )
            .await
            .context("Codex turn/start failed")?;
        Ok(CLIAgentRuntimeTurnStartSnapshot {
            native_thread_id: started.native_thread_id,
            native_turn_id: started.native_turn_id,
            raw: started.raw,
        })
    }

    async fn respond_to_request(
        &self,
        native_request_id: JsonValue,
        response: JsonValue,
    ) -> Result<()> {
        let id: JsonlRpcId = serde_json::from_value(native_request_id)
            .context("failed to decode Codex native request id")?;
        self.client
            .rpc()
            .respond_to_server_request(id, response)
            .await
            .context("failed to respond to Codex server request")
    }

    async fn interrupt_turn(
        &self,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> Result<()> {
        let native_thread_id =
            native_thread_id.ok_or_else(|| anyhow!("Codex interrupt requires native thread id"))?;
        let native_turn_id =
            native_turn_id.ok_or_else(|| anyhow!("Codex interrupt requires native turn id"))?;
        self.client
            .interrupt_turn(native_thread_id, native_turn_id, self.request_timeout)
            .await
            .context("Codex turn/interrupt failed")?;
        Ok(())
    }

    async fn observe_turn(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeTurnObservation>> {
        let snapshot = self
            .client
            .thread_read_raw(native_thread_id, true, self.request_timeout)
            .await
            .context("Codex thread/read reconciliation failed")?;
        let Some(turn) = snapshot
            .pointer("/thread/turns")
            .and_then(JsonValue::as_array)
            .and_then(|turns| {
                turns
                    .iter()
                    .find(|turn| turn.get("id").and_then(JsonValue::as_str) == Some(native_turn_id))
            })
        else {
            return Ok(None);
        };
        let status = match turn.get("status").and_then(JsonValue::as_str) {
            Some("inProgress" | "in_progress") => CLIAgentRuntimeObservedTurnStatus::InProgress,
            Some("completed") => CLIAgentRuntimeObservedTurnStatus::Completed,
            Some("failed") => CLIAgentRuntimeObservedTurnStatus::Failed,
            Some("blocked") => CLIAgentRuntimeObservedTurnStatus::Blocked,
            Some("interrupted") => CLIAgentRuntimeObservedTurnStatus::Interrupted,
            _ => return Ok(None),
        };
        let message = turn
            .pointer("/error/message")
            .and_then(JsonValue::as_str)
            .map(str::to_owned);
        let reconciliation_events = if status == CLIAgentRuntimeObservedTurnStatus::InProgress {
            Vec::new()
        } else {
            turn.get("items")
                .and_then(JsonValue::as_array)
                .into_iter()
                .flatten()
                .map(|item| {
                    let params = serde_json::json!({
                        "threadId": native_thread_id,
                        "turnId": native_turn_id,
                        "item": item,
                    });
                    map_codex_notification_event(
                        &CodexJsonlRpcNotificationEvent {
                            method: "item/completed".to_owned(),
                            params: Some(params.clone()),
                            raw: serde_json::json!({
                                "method": "item/completed",
                                "params": params,
                            }),
                        },
                        RuntimeEventMappingOptions::default(),
                    )
                })
                .collect()
        };
        Ok(Some(CLIAgentRuntimeTurnObservation {
            status,
            message,
            reconciliation_events,
        }))
    }

    async fn set_thread_name(
        &self,
        request: CLIAgentRuntimeThreadNameSetRequest,
    ) -> Result<CLIAgentRuntimeThreadNameSetResult> {
        let snapshot = self
            .client
            .thread_name_set(
                CodexThreadNameSetParams {
                    thread_id: request.native_thread_id,
                    name: request.name,
                },
                self.request_timeout,
            )
            .await
            .context("Codex thread/name/set failed")?;
        Ok(CLIAgentRuntimeThreadNameSetResult {
            native_thread_id: snapshot.native_thread_id,
            raw: Some(snapshot.raw),
        })
    }

    async fn fork_thread(
        &self,
        request: CLIAgentRuntimeThreadForkRequest,
    ) -> Result<CLIAgentRuntimeThreadForkResult> {
        let snapshot = self
            .client
            .thread_fork(
                CodexThreadForkParams {
                    thread_id: request.native_thread_id,
                },
                self.request_timeout,
            )
            .await
            .context("Codex thread/fork failed")?;
        Ok(CLIAgentRuntimeThreadForkResult {
            native_thread_id: snapshot.native_thread_id,
            native_cwd: snapshot.cwd,
            native_model: snapshot.model,
            raw: Some(snapshot.raw),
        })
    }

    async fn steer_turn(
        &self,
        request: CLIAgentRuntimeTurnSteerRequest,
    ) -> Result<CLIAgentRuntimeTurnSteerResult> {
        let snapshot = self
            .client
            .turn_steer(
                CodexTurnSteerParams {
                    thread_id: request.native_thread_id,
                    expected_turn_id: request.native_turn_id,
                    input: vec![
                        pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Text {
                            text: request.message,
                        },
                    ],
                },
                self.request_timeout,
            )
            .await
            .context("Codex turn/steer failed")?;
        Ok(CLIAgentRuntimeTurnSteerResult {
            native_thread_id: snapshot.native_thread_id,
            native_turn_id: snapshot.native_turn_id,
            raw: Some(snapshot.raw),
        })
    }
}
