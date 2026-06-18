use crate::cli_runtime::config::{
    codex_account_probe_config_from_instance, load_effective_cli_runtime_instances,
};
use crate::cli_runtime::manager::{
    CLIAgentRuntimeCodexEventReceivers, CLIAgentRuntimeManager, CLIAgentRuntimeSession,
    CLIAgentRuntimeSessionFactory, CLIAgentRuntimeSessionKey, CLIAgentRuntimeSessionStartOptions,
    CLIAgentRuntimeThreadForkRequest, CLIAgentRuntimeThreadForkResult,
    CLIAgentRuntimeThreadNameSetRequest, CLIAgentRuntimeThreadNameSetResult,
    CLIAgentRuntimeTurnSteerRequest, CLIAgentRuntimeTurnSteerResult,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_cli_agent_runtime::codex::{
    CodexAppServerClient, CodexJsonlRpcClient, CodexThreadForkParams, CodexThreadNameSetParams,
    CodexThreadOpenSnapshot, CodexThreadStartParams, CodexTurnStartParams, CodexTurnStartSnapshot,
    CodexTurnSteerParams, codex_app_server_process_config,
};
use pioneer_cli_agent_runtime::driver::JsonlRpcId;
use pioneer_cli_agent_runtime::process::{CLIAgentProcess, spawn_cli_agent_process};
use pioneer_config::{
    EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::BufReader;

pub(crate) fn codex_cli_runtime_manager(
    runtime_home: PathBuf,
    idle_session_ttl: Duration,
) -> Result<Arc<CLIAgentRuntimeManager>> {
    let factory = Arc::new(CodexCLIAgentRuntimeSessionFactory { runtime_home });
    Ok(Arc::new(CLIAgentRuntimeManager::new(
        factory,
        idle_session_ttl,
    )?))
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
        probe_config.cwd = std::env::current_dir().ok();
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

    async fn start_codex_thread(
        &self,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot> {
        self.client
            .thread_start(params, timeout)
            .await
            .context("Codex thread/start failed")
    }

    async fn resume_codex_thread(
        &self,
        native_thread_id: &str,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot> {
        self.client
            .thread_resume(native_thread_id, params, timeout)
            .await
            .context("Codex thread/resume failed")
    }

    async fn start_codex_turn(
        &self,
        params: CodexTurnStartParams,
        timeout: Duration,
    ) -> Result<CodexTurnStartSnapshot> {
        self.client
            .turn_start(params, timeout)
            .await
            .context("Codex turn/start failed")
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
                        pioneer_cli_agent_runtime::codex_input::CodexTurnInputItem::Text {
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
