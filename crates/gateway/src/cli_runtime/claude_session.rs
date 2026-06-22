use crate::cli_runtime::config::{
    claude_account_probe_config_from_instance, load_effective_cli_runtime_instances,
};
use crate::cli_runtime::manager::{
    CLIAgentRuntimeEventReceivers, CLIAgentRuntimeSession, CLIAgentRuntimeSessionFactory,
    CLIAgentRuntimeSessionKey, CLIAgentRuntimeSessionStartOptions, CLIAgentRuntimeThreadOpenParams,
    CLIAgentRuntimeThreadOpenSnapshot, CLIAgentRuntimeTurnStartParams,
    CLIAgentRuntimeTurnStartSnapshot, CLIAgentRuntimeTurnSteerRequest,
    CLIAgentRuntimeTurnSteerResult,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use base64::Engine as _;
use pioneer_cli_agent_runtime::claude::{ClaudeAccountProbeStatus, ClaudeProbe};
use pioneer_cli_agent_runtime::event::{
    RuntimeAgentMessagePhase, RuntimeErrorEvent, RuntimeEvent, RuntimeItemCompleted,
    RuntimeItemDelta, RuntimeItemDeltaKind, RuntimeItemStarted, RuntimeNativeEvent,
    RuntimeRequestOpened, RuntimeRequestResolved, RuntimeThreadStateChanged, RuntimeTurnCompleted,
    RuntimeTurnFailed, RuntimeTurnStarted,
};
use pioneer_cli_agent_runtime::process::{
    CLIAgentProcess, CLIAgentProcessSpawnConfig, expand_home_path, spawn_cli_agent_process,
};
use pioneer_config::{
    EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use serde_json::{Value as JsonValue, json};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{Mutex, mpsc, oneshot};

const CLAUDE_SDK_ENTRYPOINT: &str = "sdk-rs";
const CLAUDE_SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

pub(crate) struct ClaudeCLIAgentRuntimeSessionFactory {
    runtime_home: PathBuf,
}

impl ClaudeCLIAgentRuntimeSessionFactory {
    pub(crate) fn new(runtime_home: PathBuf) -> Self {
        Self { runtime_home }
    }

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

#[async_trait]
impl CLIAgentRuntimeSessionFactory for ClaudeCLIAgentRuntimeSessionFactory {
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
        if instance.kind != GatewayCliAgentRuntimeKindConfig::Claude {
            bail!(
                "CLI runtime `{}` is configured as unsupported kind `{:?}` for Claude session",
                instance.id,
                instance.kind
            );
        }

        let probe =
            ClaudeProbe::account_read(claude_account_probe_config_from_instance(&instance)).await;
        match probe.status {
            ClaudeAccountProbeStatus::Ready => {}
            ClaudeAccountProbeStatus::NeedsAuth => {
                bail!(
                    "{}",
                    probe
                        .message
                        .unwrap_or_else(|| "Claude CLI authentication is required".to_owned())
                );
            }
            ClaudeAccountProbeStatus::MissingBinary => {
                bail!(
                    "{}",
                    probe
                        .message
                        .unwrap_or_else(|| "Claude CLI binary was not found".to_owned())
                );
            }
            ClaudeAccountProbeStatus::SpawnFailed
            | ClaudeAccountProbeStatus::UnsupportedVersion
            | ClaudeAccountProbeStatus::Error => {
                bail!(
                    "{}",
                    probe
                        .message
                        .unwrap_or_else(|| "Claude CLI probe failed".to_owned())
                );
            }
        }

        let process_config = claude_process_config_from_instance(&instance, options)?;
        let mut process = spawn_cli_agent_process(&process_config)
            .with_context(|| format!("failed to spawn Claude CLI for runtime `{}`", instance.id))?;
        let stderr = process.stderr();
        let (stdout, stdin) = process.take_stdio()?;
        let (event_tx, event_rx) = mpsc::channel(instance.event_channel_capacity.max(1));
        let client = Arc::new(ClaudeStreamClient::new(stdin, event_tx));
        client.spawn_reader(stdout);
        client
            .initialize(Duration::from_millis(instance.startup_probe_timeout_ms))
            .await
            .context("Claude initialize handshake failed")?;

        Ok(Arc::new(ClaudeCLIAgentRuntimeSession {
            client,
            process: Mutex::new(process),
            request_timeout: Duration::from_millis(instance.request_timeout_ms),
            shutdown_grace: Duration::from_secs(5),
            native_thread_id: Mutex::new(None),
            event_receivers: std::sync::Mutex::new(Some(CLIAgentRuntimeEventReceivers {
                runtime_kind: "claude".to_owned(),
                events: event_rx,
            })),
            #[allow(dead_code)]
            stderr,
        }))
    }
}

fn claude_process_config_from_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    options: &CLIAgentRuntimeSessionStartOptions,
) -> Result<CLIAgentProcessSpawnConfig> {
    let config_dir = expand_home_path(
        instance
            .shadow_home_path
            .as_deref()
            .unwrap_or(instance.home_path.as_str()),
        None,
    )?;
    let mut env = BTreeMap::new();
    env.insert(
        "CLAUDE_CONFIG_DIR".to_owned(),
        config_dir.to_string_lossy().into_owned(),
    );
    env.insert(
        "CLAUDE_CODE_ENTRYPOINT".to_owned(),
        CLAUDE_SDK_ENTRYPOINT.to_owned(),
    );
    env.insert(
        "CLAUDE_AGENT_SDK_VERSION".to_owned(),
        CLAUDE_SDK_VERSION.to_owned(),
    );
    env.insert(
        "CLAUDE_AGENT_SDK_CLIENT_APP".to_owned(),
        "pioneer".to_owned(),
    );
    for (key, value) in &options.env {
        env.insert(key.clone(), value.clone());
    }

    let mut args = vec![
        "--output-format".to_owned(),
        "stream-json".to_owned(),
        "--verbose".to_owned(),
        "--system-prompt".to_owned(),
        String::new(),
        "--permission-prompt-tool".to_owned(),
        "stdio".to_owned(),
        "--permission-mode".to_owned(),
        "default".to_owned(),
        "--safe-mode".to_owned(),
        "--setting-sources=".to_owned(),
        "--include-partial-messages".to_owned(),
        "--input-format".to_owned(),
        "stream-json".to_owned(),
    ];
    args.extend(instance.app_server_args.clone());
    args.extend(options.app_server_args.clone());

    Ok(CLIAgentProcessSpawnConfig {
        executable: instance.binary_path.clone(),
        args,
        cwd: options.cwd.clone().or_else(|| std::env::current_dir().ok()),
        home_path: None,
        home_dir: None,
        env,
        env_remove: vec!["CLAUDECODE".to_owned()],
        stderr_ring_lines: instance.stderr_ring_lines,
        process_group: true,
    })
}

struct ClaudeCLIAgentRuntimeSession {
    client: Arc<ClaudeStreamClient>,
    process: Mutex<CLIAgentProcess>,
    request_timeout: Duration,
    shutdown_grace: Duration,
    native_thread_id: Mutex<Option<String>>,
    event_receivers: std::sync::Mutex<Option<CLIAgentRuntimeEventReceivers>>,
    #[allow(dead_code)]
    stderr: pioneer_cli_agent_runtime::process::StderrRing,
}

#[async_trait]
impl CLIAgentRuntimeSession for ClaudeCLIAgentRuntimeSession {
    async fn close(&self) -> Result<()> {
        let mut process = self.process.lock().await;
        let _ = process.terminate_with_grace(self.shutdown_grace).await?;
        Ok(())
    }

    fn take_event_receivers(&self) -> Option<CLIAgentRuntimeEventReceivers> {
        self.event_receivers
            .lock()
            .expect("Claude event receiver mutex should not be poisoned")
            .take()
    }

    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        _timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let native_thread_id = format!("claude_thread_{}", new_runtime_id());
        *self.native_thread_id.lock().await = Some(native_thread_id.clone());
        self.client
            .set_native_thread_id(native_thread_id.clone())
            .await;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id,
            cwd: Some(params.cwd),
            model: params.model,
            raw: json!({ "provider": "claude", "mode": "started" }),
        })
    }

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        _timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let native_thread_id = native_thread_id.trim().to_owned();
        if native_thread_id.is_empty() {
            bail!("Claude native thread id cannot be empty");
        }
        *self.native_thread_id.lock().await = Some(native_thread_id.clone());
        self.client
            .set_native_thread_id(native_thread_id.clone())
            .await;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id,
            cwd: Some(params.cwd),
            model: params.model,
            raw: json!({ "provider": "claude", "mode": "resumed_from_pioneer_history" }),
        })
    }

    async fn start_turn(
        &self,
        params: CLIAgentRuntimeTurnStartParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeTurnStartSnapshot> {
        let native_turn_id = format!("claude_turn_{}", new_runtime_id());
        self.client
            .start_turn(
                params.native_thread_id.clone(),
                native_turn_id.clone(),
                params.model.clone(),
                params.effort.clone(),
                params.input.clone(),
                timeout,
            )
            .await?;
        Ok(CLIAgentRuntimeTurnStartSnapshot {
            native_thread_id: params.native_thread_id,
            native_turn_id,
            raw: json!({ "provider": "claude" }),
        })
    }

    async fn respond_to_request(
        &self,
        native_request_id: JsonValue,
        response: JsonValue,
    ) -> Result<()> {
        let request_id = native_request_id
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow!("Claude native request id must be a string"))?;
        self.client
            .send_control_response(request_id, response)
            .await
            .context("failed to respond to Claude control request")
    }

    async fn interrupt_turn(
        &self,
        _native_thread_id: Option<&str>,
        _native_turn_id: Option<&str>,
    ) -> Result<()> {
        self.client
            .send_control_request(json!({ "subtype": "interrupt" }), self.request_timeout)
            .await?;
        Ok(())
    }

    async fn steer_turn(
        &self,
        request: CLIAgentRuntimeTurnSteerRequest,
    ) -> Result<CLIAgentRuntimeTurnSteerResult> {
        let _ = request;
        bail!("Claude CLI does not support in-flight turn steering")
    }
}

struct ClaudeStreamClient {
    stdin: Mutex<ChildStdin>,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<JsonValue, String>>>>,
    event_tx: mpsc::Sender<RuntimeEvent>,
    state: Mutex<ClaudeStreamState>,
    request_counter: AtomicU64,
}

#[derive(Default)]
struct ClaudeStreamState {
    native_thread_id: Option<String>,
    active_turn_id: Option<String>,
    active_text_item_id: Option<String>,
    active_reasoning_item_id: Option<String>,
    active_text_item_started: bool,
    active_reasoning_item_started: bool,
    emitted_final_text: bool,
    tool_items: HashMap<String, ClaudeToolItemState>,
}

#[derive(Debug, Clone)]
struct ClaudeToolItemState {
    item_id: String,
    item_kind: String,
    tool_name: String,
    input: JsonValue,
}

impl ClaudeStreamClient {
    fn new(stdin: ChildStdin, event_tx: mpsc::Sender<RuntimeEvent>) -> Self {
        Self {
            stdin: Mutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            event_tx,
            state: Mutex::new(ClaudeStreamState::default()),
            request_counter: AtomicU64::new(0),
        }
    }

    fn spawn_reader(self: &Arc<Self>, stdout: ChildStdout) {
        let client = self.clone();
        tokio::spawn(async move {
            client.read_loop(stdout).await;
        });
    }

    async fn initialize(&self, timeout: Duration) -> Result<()> {
        self.send_control_request(json!({ "subtype": "initialize", "hooks": null }), timeout)
            .await?;
        Ok(())
    }

    async fn set_native_thread_id(&self, native_thread_id: String) {
        let mut state = self.state.lock().await;
        state.native_thread_id = Some(native_thread_id);
    }

    async fn start_turn(
        &self,
        native_thread_id: String,
        native_turn_id: String,
        model: Option<String>,
        _effort: Option<String>,
        input: JsonValue,
        timeout: Duration,
    ) -> Result<()> {
        if let Some(model) = model.filter(|model| !model.trim().is_empty()) {
            self.send_control_request(json!({ "subtype": "set_model", "model": model }), timeout)
                .await?;
        }
        {
            let mut state = self.state.lock().await;
            state.native_thread_id = Some(native_thread_id.clone());
            state.active_turn_id = Some(native_turn_id.clone());
            state.active_text_item_id = None;
            state.active_reasoning_item_id = None;
            state.active_text_item_started = false;
            state.active_reasoning_item_started = false;
            state.emitted_final_text = false;
            state.tool_items.clear();
        }
        self.emit(RuntimeEvent::TurnStarted(RuntimeTurnStarted {
            native_thread_id: Some(native_thread_id.clone()),
            native_turn_id: native_turn_id.clone(),
            native: Some(native_event(
                "turn/started",
                json!({ "provider": "claude" }),
            )),
        }))
        .await;

        let prompt = claude_prompt_from_input(input)?;
        self.write_json_line(json!({
            "type": "user",
            "message": { "role": "user", "content": prompt },
            "parent_tool_use_id": null,
            "session_id": "default",
        }))
        .await
    }

    async fn send_control_request(
        &self,
        request: JsonValue,
        timeout_value: Duration,
    ) -> Result<JsonValue> {
        let request_id = format!(
            "req_{}_{}",
            self.request_counter.fetch_add(1, Ordering::Relaxed),
            new_runtime_id()
        );
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(request_id.clone(), tx);
        if let Err(error) = self
            .write_json_line(json!({
                "type": "control_request",
                "request_id": request_id,
                "request": request,
            }))
            .await
        {
            self.pending.lock().await.remove(&request_id);
            return Err(error);
        }
        match tokio::time::timeout(timeout_value, rx).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(error))) => {
                self.pending.lock().await.remove(&request_id);
                bail!("{error}")
            }
            Ok(Err(_)) => {
                self.pending.lock().await.remove(&request_id);
                bail!("Claude control request response channel closed")
            }
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                bail!("Claude control request timed out")
            }
        }
    }

    async fn send_control_response(&self, request_id: String, response: JsonValue) -> Result<()> {
        self.write_json_line(json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": request_id,
                "response": response,
            }
        }))
        .await
    }

    async fn write_json_line(&self, value: JsonValue) -> Result<()> {
        let mut stdin = self.stdin.lock().await;
        let line = serde_json::to_string(&value).context("failed to encode Claude JSON line")?;
        stdin
            .write_all(line.as_bytes())
            .await
            .context("failed to write Claude JSON line")?;
        stdin
            .write_all(b"\n")
            .await
            .context("failed to write Claude JSON newline")?;
        stdin.flush().await.context("failed to flush Claude stdin")
    }

    async fn read_loop(&self, stdout: ChildStdout) {
        let mut lines = BufReader::new(stdout).lines();
        let mut buffer = String::new();
        loop {
            let line = match lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => break,
                Err(error) => {
                    self.emit(RuntimeEvent::Error(RuntimeErrorEvent {
                        native_thread_id: self.state.lock().await.native_thread_id.clone(),
                        native_turn_id: self.state.lock().await.active_turn_id.clone(),
                        message: format!("Claude stdout read failed: {error}"),
                        code: Some("claude_stdout_read_failed".to_owned()),
                        retryable: false,
                        native: None,
                    }))
                    .await;
                    break;
                }
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if buffer.is_empty() && !trimmed.starts_with('{') {
                continue;
            }
            buffer.push_str(trimmed);
            let value = match serde_json::from_str::<JsonValue>(&buffer) {
                Ok(value) => {
                    buffer.clear();
                    value
                }
                Err(_) => continue,
            };
            self.handle_incoming(value).await;
        }
        self.fail_pending_requests("Claude CLI process ended".to_owned())
            .await;
        let (native_thread_id, native_turn_id) = {
            let mut state = self.state.lock().await;
            let ids = (state.native_thread_id.clone(), state.active_turn_id.clone());
            if ids.1.is_some() {
                state.active_turn_id = None;
                state.active_text_item_id = None;
                state.active_reasoning_item_id = None;
                state.active_text_item_started = false;
                state.active_reasoning_item_started = false;
                state.emitted_final_text = false;
                state.tool_items.clear();
            }
            ids
        };
        if let Some(native_turn_id) = native_turn_id {
            self.emit(RuntimeEvent::TurnFailed(RuntimeTurnFailed {
                native_thread_id,
                native_turn_id: Some(native_turn_id),
                message: "Claude CLI process ended before the turn completed".to_owned(),
                code: Some("claude_process_exited".to_owned()),
                native: Some(native_event("process/eof", json!({ "provider": "claude" }))),
            }))
            .await;
        }
    }

    async fn handle_incoming(&self, value: JsonValue) {
        match value.get("type").and_then(JsonValue::as_str) {
            Some("control_response") => {
                self.handle_control_response(value).await;
            }
            Some("control_request") => {
                self.handle_control_request(value).await;
            }
            Some("control_cancel_request") => {
                if let Some(request_id) = value.get("request_id").and_then(JsonValue::as_str) {
                    self.emit(RuntimeEvent::RequestResolved(RuntimeRequestResolved {
                        native_request_id: request_id.to_owned(),
                        native: Some(native_event("control_cancel_request", value)),
                    }))
                    .await;
                }
            }
            _ => {
                for event in self.map_message(value).await {
                    self.emit(event).await;
                }
            }
        }
    }

    async fn fail_pending_requests(&self, message: String) {
        let pending = {
            let mut pending = self.pending.lock().await;
            pending
                .drain()
                .map(|(_, sender)| sender)
                .collect::<Vec<_>>()
        };
        for sender in pending {
            let _ = sender.send(Err(message.clone()));
        }
    }

    async fn handle_control_response(&self, value: JsonValue) {
        let response = value.get("response").cloned().unwrap_or(JsonValue::Null);
        let Some(request_id) = response.get("request_id").and_then(JsonValue::as_str) else {
            return;
        };
        let result = if response.get("subtype").and_then(JsonValue::as_str) == Some("error") {
            Err(response
                .get("error")
                .and_then(JsonValue::as_str)
                .unwrap_or("Claude control request failed")
                .to_owned())
        } else {
            Ok(response.get("response").cloned().unwrap_or(JsonValue::Null))
        };
        if let Some(tx) = self.pending.lock().await.remove(request_id) {
            let _ = tx.send(result);
        }
    }

    async fn handle_control_request(&self, value: JsonValue) {
        let request_id = value
            .get("request_id")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_owned();
        if request_id.is_empty() {
            return;
        }
        let request = value.get("request").cloned().unwrap_or(JsonValue::Null);
        if request.get("subtype").and_then(JsonValue::as_str) != Some("can_use_tool") {
            self.send_control_response(
                request_id,
                json!({ "behavior": "deny", "message": "Unsupported Claude control request" }),
            )
            .await
            .ok();
            return;
        }
        let tool_name = request
            .get("tool_name")
            .and_then(JsonValue::as_str)
            .unwrap_or("tool")
            .to_owned();
        let (native_thread_id, active_turn_id, payload) = {
            let state = self.state.lock().await;
            let tool_input = request.get("input").cloned().unwrap_or(JsonValue::Null);
            let native_thread_id = state.native_thread_id.clone();
            let active_turn_id = state.active_turn_id.clone();
            let payload = json!({
                "nativeRequestId": request_id,
                "nativeRequestIdJson": request_id,
                "toolName": tool_name,
                "command": command_from_claude_tool(tool_name.as_str(), &tool_input),
                "input": tool_input,
                "title": request.get("title").cloned(),
                "displayName": request.get("display_name").cloned(),
                "description": request.get("description").cloned(),
                "reason": request.get("decision_reason").cloned(),
                "threadId": native_thread_id,
                "turnId": active_turn_id,
                "itemId": request.get("tool_use_id").and_then(JsonValue::as_str),
                "raw": request,
            });
            (native_thread_id, active_turn_id, payload)
        };
        self.emit(RuntimeEvent::RequestOpened(RuntimeRequestOpened {
            native_request_id: request_id,
            native_request_id_json: payload.get("nativeRequestIdJson").cloned(),
            request_kind: request_kind_for_claude_tool(tool_name.as_str()).to_owned(),
            native_thread_id,
            native_turn_id: active_turn_id,
            native_item_id: payload
                .get("itemId")
                .and_then(JsonValue::as_str)
                .map(str::to_owned),
            payload_redacted: Some(payload),
            native: Some(native_event("control_request/can_use_tool", value)),
        }))
        .await;
    }

    async fn map_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        match value.get("type").and_then(JsonValue::as_str) {
            Some("system") => self.map_system_message(value).await,
            Some("stream_event") => self.map_stream_event(value).await,
            Some("assistant") => self.map_assistant_message(value).await,
            Some("result") => self.map_result_message(value).await,
            Some("error") => self.map_error_message(value).await,
            _ => Vec::new(),
        }
    }

    async fn map_system_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let state = self.state.lock().await;
        let subtype = value
            .get("subtype")
            .and_then(JsonValue::as_str)
            .unwrap_or("system");
        match subtype {
            "session_state_changed" => vec![RuntimeEvent::ThreadStateChanged(
                RuntimeThreadStateChanged {
                    native_thread_id: state.native_thread_id.clone(),
                    status: value
                        .get("status")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("changed")
                        .to_owned(),
                    native: Some(native_event("system/session_state_changed", value)),
                },
            )],
            "permission_denied" => vec![RuntimeEvent::Error(RuntimeErrorEvent {
                native_thread_id: state.native_thread_id.clone(),
                native_turn_id: state.active_turn_id.clone(),
                message: value
                    .get("message")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("Claude denied a tool call")
                    .to_owned(),
                code: Some("claude_permission_denied".to_owned()),
                retryable: false,
                native: Some(native_event("system/permission_denied", value)),
            })],
            _ => Vec::new(),
        }
    }

    async fn map_stream_event(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let event = value.get("event").cloned().unwrap_or(JsonValue::Null);
        let event_type = event.get("type").and_then(JsonValue::as_str);
        let mut state = self.state.lock().await;
        let Some(native_thread_id) = state.native_thread_id.clone() else {
            return Vec::new();
        };
        let Some(native_turn_id) = state.active_turn_id.clone() else {
            return Vec::new();
        };
        match event_type {
            Some("content_block_delta") => {
                let delta = event.get("delta").cloned().unwrap_or(JsonValue::Null);
                let delta_type = delta.get("type").and_then(JsonValue::as_str);
                let text = delta
                    .get("text")
                    .or_else(|| delta.get("thinking"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("");
                if text.is_empty() {
                    return Vec::new();
                }
                let mut events = Vec::new();
                let (item_id, item_kind, delta_kind) = if delta_type == Some("thinking_delta") {
                    let item_id = state
                        .active_reasoning_item_id
                        .get_or_insert_with(|| format!("claude_reasoning_{}", new_runtime_id()))
                        .clone();
                    if !state.active_reasoning_item_started {
                        state.active_reasoning_item_started = true;
                        events.push(item_started(
                            &state,
                            item_id.as_str(),
                            "reasoning",
                            None,
                            None,
                        ));
                    }
                    (
                        item_id,
                        "reasoning".to_owned(),
                        RuntimeItemDeltaKind::ReasoningSummary,
                    )
                } else {
                    let item_id = state
                        .active_text_item_id
                        .get_or_insert_with(|| format!("claude_message_{}", new_runtime_id()))
                        .clone();
                    if !state.active_text_item_started {
                        state.active_text_item_started = true;
                        events.push(item_started(
                            &state,
                            item_id.as_str(),
                            "agentMessage",
                            None,
                            None,
                        ));
                    }
                    (
                        item_id,
                        "agentMessage".to_owned(),
                        RuntimeItemDeltaKind::AgentMessage,
                    )
                };
                events.push(RuntimeEvent::ItemDelta(RuntimeItemDelta {
                    native_thread_id: Some(native_thread_id),
                    native_turn_id,
                    native_item_id: item_id,
                    item_kind,
                    delta_kind,
                    delta: text.to_owned(),
                    metadata: None,
                    native: Some(native_event("stream_event/content_block_delta", value)),
                }));
                events
            }
            _ => Vec::new(),
        }
    }

    async fn map_assistant_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let content = value
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(JsonValue::as_array)
            .cloned()
            .unwrap_or_default();
        let mut events = Vec::new();
        for (index, block) in content.into_iter().enumerate() {
            match block.get("type").and_then(JsonValue::as_str) {
                Some("text") => {
                    let text = block
                        .get("text")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("")
                        .to_owned();
                    if text.is_empty() {
                        continue;
                    }
                    let mut state = self.state.lock().await;
                    let item_id = state
                        .active_text_item_id
                        .take()
                        .unwrap_or_else(|| claude_item_id(&value, "message", index));
                    let item_started_emitted = state.active_text_item_started;
                    state.active_text_item_started = false;
                    state.emitted_final_text = true;
                    if !item_started_emitted {
                        events.push(item_started(
                            &state,
                            item_id.as_str(),
                            "agentMessage",
                            None,
                            None,
                        ));
                    }
                    events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                        native_thread_id: state.native_thread_id.clone(),
                        native_turn_id: state.active_turn_id.clone().unwrap_or_default(),
                        native_item_id: item_id,
                        item_kind: "agentMessage".to_owned(),
                        text: Some(text),
                        summary: Vec::new(),
                        content: Vec::new(),
                        phase: RuntimeAgentMessagePhase::FinalAnswer,
                        metadata: None,
                        native_item_redacted: Some(block),
                        native: Some(native_event("assistant/text", value.clone())),
                    }));
                }
                Some("thinking") => {
                    let thinking = block
                        .get("thinking")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("")
                        .to_owned();
                    let mut state = self.state.lock().await;
                    let item_id = state
                        .active_reasoning_item_id
                        .take()
                        .unwrap_or_else(|| claude_item_id(&value, "reasoning", index));
                    let item_started_emitted = state.active_reasoning_item_started;
                    state.active_reasoning_item_started = false;
                    if !item_started_emitted {
                        events.push(item_started(
                            &state,
                            item_id.as_str(),
                            "reasoning",
                            None,
                            None,
                        ));
                    }
                    events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                        native_thread_id: state.native_thread_id.clone(),
                        native_turn_id: state.active_turn_id.clone().unwrap_or_default(),
                        native_item_id: item_id,
                        item_kind: "reasoning".to_owned(),
                        text: None,
                        summary: if thinking.is_empty() {
                            Vec::new()
                        } else {
                            vec![thinking]
                        },
                        content: Vec::new(),
                        phase: RuntimeAgentMessagePhase::FinalAnswer,
                        metadata: None,
                        native_item_redacted: Some(block),
                        native: Some(native_event("assistant/thinking", value.clone())),
                    }));
                }
                Some("tool_use") => {
                    let mut state = self.state.lock().await;
                    let tool_id = block
                        .get("id")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| claude_item_id(&value, "tool", index));
                    let tool_name = block
                        .get("name")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("tool")
                        .to_owned();
                    let input = block.get("input").cloned().unwrap_or(JsonValue::Null);
                    let item_kind = item_kind_for_claude_tool(tool_name.as_str()).to_owned();
                    let metadata = metadata_for_claude_tool(tool_name.as_str(), &input, None, None);
                    events.push(item_started(
                        &state,
                        tool_id.as_str(),
                        item_kind.as_str(),
                        Some(tool_name.clone()),
                        Some(metadata),
                    ));
                    state.tool_items.insert(
                        tool_id.clone(),
                        ClaudeToolItemState {
                            item_id: tool_id,
                            item_kind,
                            tool_name,
                            input,
                        },
                    );
                }
                Some("tool_result") => {
                    let tool_use_id = block
                        .get("tool_use_id")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("");
                    let mut state = self.state.lock().await;
                    let Some(tool) = state.tool_items.remove(tool_use_id) else {
                        continue;
                    };
                    let output = claude_tool_result_text(&block);
                    let is_error = block
                        .get("is_error")
                        .and_then(JsonValue::as_bool)
                        .unwrap_or(false);
                    let metadata = metadata_for_claude_tool(
                        tool.tool_name.as_str(),
                        &tool.input,
                        Some(output.as_str()),
                        Some(!is_error),
                    );
                    events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                        native_thread_id: state.native_thread_id.clone(),
                        native_turn_id: state.active_turn_id.clone().unwrap_or_default(),
                        native_item_id: tool.item_id,
                        item_kind: tool.item_kind,
                        text: Some(output),
                        summary: Vec::new(),
                        content: Vec::new(),
                        phase: RuntimeAgentMessagePhase::FinalAnswer,
                        metadata: Some(metadata),
                        native_item_redacted: Some(block),
                        native: Some(native_event("assistant/tool_result", value.clone())),
                    }));
                }
                _ => {}
            }
        }
        events
    }

    async fn map_result_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let mut state = self.state.lock().await;
        let Some(native_thread_id) = state.native_thread_id.clone() else {
            return Vec::new();
        };
        let Some(native_turn_id) = state.active_turn_id.clone() else {
            return Vec::new();
        };
        let is_error = value
            .get("is_error")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let mut events = Vec::new();
        if !state.emitted_final_text
            && let Some(result) = value
                .get("result")
                .and_then(JsonValue::as_str)
                .filter(|text| !text.trim().is_empty())
        {
            let item_id = state
                .active_text_item_id
                .take()
                .unwrap_or_else(|| format!("claude_result_{}", new_runtime_id()));
            let item_started_emitted = state.active_text_item_started;
            state.active_text_item_started = false;
            if !item_started_emitted {
                events.push(item_started(
                    &state,
                    item_id.as_str(),
                    "agentMessage",
                    None,
                    None,
                ));
            }
            events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                native_thread_id: Some(native_thread_id.clone()),
                native_turn_id: native_turn_id.clone(),
                native_item_id: item_id,
                item_kind: "agentMessage".to_owned(),
                text: Some(result.to_owned()),
                summary: Vec::new(),
                content: Vec::new(),
                phase: RuntimeAgentMessagePhase::FinalAnswer,
                metadata: None,
                native_item_redacted: None,
                native: Some(native_event("result/final_text", value.clone())),
            }));
        }
        if is_error {
            events.push(RuntimeEvent::TurnFailed(RuntimeTurnFailed {
                native_thread_id: Some(native_thread_id),
                native_turn_id: Some(native_turn_id.clone()),
                message: claude_result_error_message(&value),
                code: value
                    .get("subtype")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned),
                native: Some(native_event("result/error", value)),
            }));
        } else {
            events.push(RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                native_thread_id: Some(native_thread_id),
                native_turn_id: native_turn_id.clone(),
                status: "completed".to_owned(),
                native: Some(native_event("result/success", value)),
            }));
        }
        state.active_turn_id = None;
        state.active_text_item_id = None;
        state.active_reasoning_item_id = None;
        state.active_text_item_started = false;
        state.active_reasoning_item_started = false;
        state.emitted_final_text = false;
        state.tool_items.clear();
        events
    }

    async fn map_error_message(&self, value: JsonValue) -> Vec<RuntimeEvent> {
        let mut state = self.state.lock().await;
        let native_thread_id = state.native_thread_id.clone();
        let native_turn_id = state.active_turn_id.clone();
        state.active_turn_id = None;
        state.active_text_item_id = None;
        state.active_reasoning_item_id = None;
        state.active_text_item_started = false;
        state.active_reasoning_item_started = false;
        state.emitted_final_text = false;
        state.tool_items.clear();
        vec![RuntimeEvent::TurnFailed(RuntimeTurnFailed {
            native_thread_id,
            native_turn_id,
            message: value
                .get("error")
                .and_then(JsonValue::as_str)
                .unwrap_or("Claude CLI emitted an error")
                .to_owned(),
            code: Some("claude_stream_error".to_owned()),
            native: Some(native_event("error", value)),
        })]
    }

    async fn emit(&self, event: RuntimeEvent) {
        let _ = self.event_tx.send(event).await;
    }
}

fn claude_prompt_from_input(input: JsonValue) -> Result<JsonValue> {
    let items = input
        .as_array()
        .ok_or_else(|| anyhow!("Claude turn input must be an array"))?;
    let mut content = Vec::new();
    for item in items {
        match item.get("type").and_then(JsonValue::as_str) {
            Some("text") => {
                if let Some(text) = item.get("text").and_then(JsonValue::as_str) {
                    push_claude_text_block(&mut content, text);
                }
            }
            Some("localImage") | Some("local_image") => {
                if let Some(path) = item.get("path").and_then(JsonValue::as_str) {
                    content.push(claude_local_image_block(path)?);
                }
            }
            Some("image") => {
                if let Some(url) = item.get("url").and_then(JsonValue::as_str) {
                    push_claude_text_block(
                        &mut content,
                        format!(
                            "Attached image available at URL:\n{url}\nUse this URL if the task requires inspecting the image."
                        ),
                    );
                }
            }
            Some("fileReference") | Some("file_reference") => {
                if let Some(path) = item.get("path").and_then(JsonValue::as_str) {
                    push_claude_text_block(
                        &mut content,
                        format!(
                            "Attached file available at local path:\n{path}\nUse this path if the task requires inspecting the file."
                        ),
                    );
                } else if let Some(url) = item.get("url").and_then(JsonValue::as_str) {
                    push_claude_text_block(
                        &mut content,
                        format!(
                            "Attached file available at URL:\n{url}\nUse this URL if the task requires inspecting the file."
                        ),
                    );
                } else if let Some(reference) = item.get("reference").and_then(JsonValue::as_str) {
                    push_claude_text_block(
                        &mut content,
                        format!("Attached file reference:\n{reference}"),
                    );
                }
            }
            other => {
                push_claude_text_block(
                    &mut content,
                    format!("Unsupported CLI attachment input: {other:?}"),
                );
            }
        }
    }
    Ok(JsonValue::Array(content))
}

fn push_claude_text_block(content: &mut Vec<JsonValue>, text: impl Into<String>) {
    let text = text.into();
    if !text.trim().is_empty() {
        content.push(json!({ "type": "text", "text": text }));
    }
}

fn claude_local_image_block(path: &str) -> Result<JsonValue> {
    let media_type = claude_image_media_type(path)
        .ok_or_else(|| anyhow!("unsupported Claude image attachment type for `{path}`"))?;
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read Claude image attachment `{path}`"))?;
    let data = base64::engine::general_purpose::STANDARD.encode(bytes);
    Ok(json!({
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": data,
        },
    }))
}

fn claude_image_media_type(path: &str) -> Option<&'static str> {
    let mime = mime_guess::from_path(Path::new(path)).first()?;
    match (mime.type_().as_str(), mime.subtype().as_str()) {
        ("image", "png") => Some("image/png"),
        ("image", "jpeg") => Some("image/jpeg"),
        ("image", "gif") => Some("image/gif"),
        ("image", "webp") => Some("image/webp"),
        _ => None,
    }
}

fn native_event(method: &str, raw: JsonValue) -> RuntimeNativeEvent {
    RuntimeNativeEvent {
        method: method.to_owned(),
        payload_redacted: Some(redact_native(raw.clone())),
        raw_redacted: Some(redact_native(raw)),
    }
}

fn redact_native(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => JsonValue::Object(
            map.into_iter()
                .map(|(key, value)| {
                    let lowered = key.to_ascii_lowercase();
                    if lowered.contains("token")
                        || lowered.contains("secret")
                        || lowered.contains("password")
                        || lowered.contains("authorization")
                    {
                        (key, JsonValue::String("<redacted>".to_owned()))
                    } else {
                        (key, redact_native(value))
                    }
                })
                .collect(),
        ),
        JsonValue::Array(items) => JsonValue::Array(items.into_iter().map(redact_native).collect()),
        other => other,
    }
}

fn item_started(
    state: &ClaudeStreamState,
    item_id: &str,
    item_kind: &str,
    title: Option<String>,
    metadata: Option<JsonValue>,
) -> RuntimeEvent {
    RuntimeEvent::ItemStarted(RuntimeItemStarted {
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone().unwrap_or_default(),
        native_item_id: item_id.to_owned(),
        item_kind: item_kind.to_owned(),
        title,
        phase: RuntimeAgentMessagePhase::FinalAnswer,
        metadata,
        native_item_redacted: None,
        native: Some(native_event(
            "item/started",
            json!({ "provider": "claude" }),
        )),
    })
}

fn item_kind_for_claude_tool(tool_name: &str) -> &'static str {
    match tool_name {
        "Bash" => "commandExecution",
        "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => "fileChange",
        _ => "dynamicToolCall",
    }
}

fn request_kind_for_claude_tool(tool_name: &str) -> &'static str {
    match tool_name {
        "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => "file_change_approval",
        _ => "command_approval",
    }
}

fn metadata_for_claude_tool(
    tool_name: &str,
    input: &JsonValue,
    output: Option<&str>,
    success: Option<bool>,
) -> JsonValue {
    match tool_name {
        "Bash" => json!({
            "toolName": "Bash",
            "command": command_from_claude_tool(tool_name, input)
                .map(|command| vec![command])
                .unwrap_or_default(),
            "cwd": input.get("cwd").and_then(JsonValue::as_str),
            "stdout": output,
            "success": success,
        }),
        "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => json!({
            "toolName": tool_name,
            "changedFiles": changed_files_from_claude_tool(input),
            "stdout": output,
            "success": success,
            "arguments": input,
        }),
        _ => json!({
            "toolName": tool_name,
            "tool": tool_name,
            "message": output,
            "success": success,
            "arguments": input,
        }),
    }
}

fn command_from_claude_tool(tool_name: &str, input: &JsonValue) -> Option<String> {
    if tool_name == "Bash" {
        return input
            .get("command")
            .and_then(JsonValue::as_str)
            .map(str::to_owned);
    }
    input
        .get("description")
        .or_else(|| input.get("file_path"))
        .or_else(|| input.get("path"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
}

fn changed_files_from_claude_tool(input: &JsonValue) -> Vec<String> {
    ["file_path", "path", "notebook_path"]
        .into_iter()
        .filter_map(|key| {
            input
                .get(key)
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn claude_tool_result_text(block: &JsonValue) -> String {
    match block.get("content") {
        Some(JsonValue::String(text)) => text.clone(),
        Some(JsonValue::Array(items)) => items
            .iter()
            .filter_map(|item| {
                item.get("text")
                    .and_then(JsonValue::as_str)
                    .or_else(|| item.as_str())
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

fn claude_result_error_message(value: &JsonValue) -> String {
    value
        .get("errors")
        .and_then(JsonValue::as_array)
        .map(|errors| {
            errors
                .iter()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>()
                .join("; ")
        })
        .filter(|message| !message.is_empty())
        .or_else(|| {
            value
                .get("result")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "Claude CLI turn failed".to_owned())
}

fn claude_item_id(value: &JsonValue, label: &str, index: usize) -> String {
    value
        .get("uuid")
        .and_then(JsonValue::as_str)
        .map(|uuid| format!("claude_{label}_{uuid}_{index}"))
        .unwrap_or_else(|| format!("claude_{label}_{}", new_runtime_id()))
}

fn new_runtime_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{now:x}_{counter:x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_prompt_from_input_encodes_local_images_as_image_blocks() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let image_path = temp_dir.path().join("screen.png");
        std::fs::write(&image_path, b"png bytes").expect("write image");

        let content = claude_prompt_from_input(json!([
            { "type": "text", "text": "Inspect this image." },
            { "type": "localImage", "path": image_path.to_string_lossy() },
        ]))
        .expect("content");

        assert_eq!(
            content,
            json!([
                { "type": "text", "text": "Inspect this image." },
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": base64::engine::general_purpose::STANDARD.encode(b"png bytes"),
                    },
                },
            ])
        );
    }

    #[test]
    fn claude_prompt_from_input_keeps_files_as_text_references() {
        let content = claude_prompt_from_input(json!([
            { "type": "fileReference", "path": "/tmp/report.pdf" },
        ]))
        .expect("content");

        assert_eq!(
            content,
            json!([
                {
                    "type": "text",
                    "text": "Attached file available at local path:\n/tmp/report.pdf\nUse this path if the task requires inspecting the file.",
                },
            ])
        );
    }
}
