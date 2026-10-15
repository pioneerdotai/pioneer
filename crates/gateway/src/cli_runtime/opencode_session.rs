//! OpenCode server adapter.
//!
//! A Pioneer session owns one authenticated loopback `opencode serve`
//! process. Provider-native HTTP/SSE events are projected into the same
//! canonical runtime event stream used by Codex and Claude.

use crate::cli_runtime::continuation::{
    CliMcpSessionLaunch, CliProviderContinuation, CliSessionLaunchSpec,
};
use crate::cli_runtime::manager::{
    CLIAgentRuntimeEventReceivers, CLIAgentRuntimeMcpTurnMetadata,
    CLIAgentRuntimeObservedTurnStatus, CLIAgentRuntimeSession, CLIAgentRuntimeSessionFactory,
    CLIAgentRuntimeThreadCompactRequest, CLIAgentRuntimeThreadCompactResult,
    CLIAgentRuntimeThreadForkRequest, CLIAgentRuntimeThreadForkResult,
    CLIAgentRuntimeThreadNameSetRequest, CLIAgentRuntimeThreadNameSetResult,
    CLIAgentRuntimeThreadOpenParams, CLIAgentRuntimeThreadOpenSnapshot,
    CLIAgentRuntimeTurnLivenessProbe, CLIAgentRuntimeTurnObservation,
    CLIAgentRuntimeTurnStartParams, CLIAgentRuntimeTurnStartSnapshot,
};
use crate::cli_runtime::mcp::coordinator::{
    CliMcpProjectionFingerprint, CliMcpProjectionGeneration,
};
use crate::cli_runtime::mcp::facade::CliMcpFacadeProjection;
use crate::cli_runtime::mcp::grants::{CliMcpGrantScope, CliMcpManifestHash};
use crate::cli_runtime::mcp::limits::{CliMcpFacadeLimits, CliMcpRuntimeLimits};
use crate::cli_runtime::mcp::native_correlation::{
    CorrelatingTurnMcpInvoker, NativeMcpCorrelationLedger, NativeMcpItemBinding,
};
use crate::cli_runtime::mcp::server::{CliMcpBridgeFacadeHandle, CliMcpBridgeFacadeServer};
use crate::cli_runtime::mcp::supervisor::{CliMcpBridgeLaunch, CliMcpBridgeSupervisor};
use crate::cli_runtime::opencode_mcp::{
    OPENCODE_MCP_SERVER_NAME, OpenCodeMcpSessionLaunchProjection,
};
use crate::cli_runtime::session_instance::CliSessionInstanceId;
use crate::turn_mcp::invoker::TurnMcpInvoker;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use base64::Engine as _;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pioneer_cli_agent_runtime::event::{
    RuntimeAgentMessagePhase, RuntimeDiffUpdated, RuntimeErrorEvent, RuntimeEvent,
    RuntimeItemCompleted, RuntimeItemDelta, RuntimeItemDeltaKind, RuntimeItemStarted,
    RuntimeNativeEvent, RuntimePlanUpdated, RuntimeRawEvent, RuntimeRequestOpened,
    RuntimeRequestResolved, RuntimeThreadStateChanged, RuntimeTurnCompleted, RuntimeTurnFailed,
    RuntimeTurnInterrupted, RuntimeTurnRetrying, RuntimeTurnStarted,
};
use pioneer_cli_agent_runtime::process::{
    CLIAgentProcess, CLIAgentProcessSpawnConfig, SecretString, SensitiveEnvironment,
    expand_home_path, spawn_cli_agent_process,
};
use pioneer_cli_agent_runtime::reserved_args::validate_opencode_custom_args;
use pioneer_config::{
    EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use reqwest::{Method, StatusCode};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::ChildStdin;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout as tokio_timeout};
use tokio_util::sync::CancellationToken;

const OPENCODE_SERVER_USERNAME: &str = "pioneer";
const OPENCODE_SERVER_PREFIX: &str = "opencode server listening on ";
const OPENCODE_API_CONTRACT_VERSION: u32 = 2;
const OPENCODE_SUPPORTED_VERSION: &str = "1.4.14";

#[derive(Debug)]
struct UnsupportedOpenCodeVersion;
impl std::fmt::Display for UnsupportedOpenCodeVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OpenCode {} server contract is required",
            OPENCODE_SUPPORTED_VERSION
        )
    }
}
impl std::error::Error for UnsupportedOpenCodeVersion {}

const OPENCODE_BUILTIN_TOOLS: &[&str] = &[
    "bash",
    "read",
    "glob",
    "grep",
    "edit",
    "write",
    "webfetch",
    "todowrite",
    "websearch",
    "codesearch",
    "apply_patch",
    "question",
    "lsp",
    "plan_exit",
];
const OPENCODE_SAFE_TOOLS: &[&str] = &[
    "read",
    "glob",
    "grep",
    "webfetch",
    "todowrite",
    "websearch",
    "codesearch",
    "question",
];

fn opencode_config_environment_roots(config_dir: &Path) -> Result<(PathBuf, Option<PathBuf>)> {
    if config_dir.file_name().and_then(|name| name.to_str()) == Some("opencode") {
        let config_parent = config_dir
            .parent()
            .ok_or_else(|| anyhow!("OpenCode config path has no parent"))?;
        return Ok((config_parent.to_path_buf(), None));
    }

    // OpenCode always reads `$XDG_CONFIG_HOME/opencode` in addition to an
    // explicit `OPENCODE_CONFIG_DIR`. Keep that implicit location inside the
    // configured runtime home so a custom home cannot inherit sibling config.
    Ok((
        config_dir.join(".pioneer-xdg"),
        Some(config_dir.to_path_buf()),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenCodeProbeStatus {
    Ready,
    NeedsAuth,
    MissingBinary,
    SpawnFailed,
    UnsupportedVersion,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenCodeProbeDiagnostic {
    pub(crate) level: OpenCodeProbeDiagnosticLevel,
    pub(crate) code: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenCodeProbeDiagnosticLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenCodeAccountSnapshot {
    pub(crate) authenticated: bool,
    pub(crate) display_name: Option<String>,
    pub(crate) auth_method: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpenCodeProbeSnapshot {
    pub(crate) status: OpenCodeProbeStatus,
    pub(crate) message: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) account: Option<OpenCodeAccountSnapshot>,
    pub(crate) diagnostics: Vec<OpenCodeProbeDiagnostic>,
    pub(crate) stderr: Vec<String>,
    pub(crate) models: Vec<OpenCodeModelSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenCodeModelSnapshot {
    pub(crate) id: String,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) family: Option<String>,
    pub(crate) is_custom: bool,
    pub(crate) active: Option<bool>,
    pub(crate) effort_options: Vec<String>,
    pub(crate) input_modalities: Vec<String>,
    pub(crate) output_modalities: Vec<String>,
    pub(crate) supports_reasoning: Option<bool>,
    pub(crate) supports_vision: Option<bool>,
    pub(crate) max_input_tokens: Option<u64>,
    pub(crate) max_output_tokens: Option<u64>,
}

#[derive(Clone)]
struct OpenCodeApiClient {
    client: reqwest::Client,
    base_url: String,
    directory: String,
    username: String,
    password: SecretString,
}

impl std::fmt::Debug for OpenCodeApiClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenCodeApiClient")
            .field("base_url", &self.base_url)
            .field("directory", &self.directory)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

impl OpenCodeApiClient {
    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.authenticated_request(method, format!("{}{}", self.base_url, path))
    }

    fn authenticated_request(
        &self,
        method: Method,
        url: impl reqwest::IntoUrl,
    ) -> reqwest::RequestBuilder {
        self.client
            .request(method, url)
            .basic_auth(self.username.as_str(), Some(self.password.expose_secret()))
            .query(&[("directory", self.directory.as_str())])
    }

    fn url_with_segments(&self, segments: &[&str]) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(format!("{}/", self.base_url).as_str())
            .context("invalid managed OpenCode server URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("managed OpenCode server URL cannot contain path segments"))?
            .extend(segments.iter().copied());
        Ok(url)
    }

    async fn json(
        &self,
        method: Method,
        path: &str,
        body: Option<&JsonValue>,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        let request = self.request(method, path);
        self.json_request(request, path, body, request_timeout)
            .await
    }

    async fn json_url(
        &self,
        method: Method,
        url: reqwest::Url,
        label: &str,
        body: Option<&JsonValue>,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        let request = self.authenticated_request(method, url);
        self.json_request(request, label, body, request_timeout)
            .await
    }

    async fn json_request(
        &self,
        mut request: reqwest::RequestBuilder,
        label: &str,
        body: Option<&JsonValue>,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        if request_timeout.is_zero() {
            bail!("OpenCode request timeout must be non-zero");
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = tokio_timeout(request_timeout, request.send())
            .await
            .map_err(|_| anyhow!("OpenCode request to `{label}` timed out"))??;
        let status = response.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(JsonValue::Null);
        }
        let max_bytes = if status.is_success() {
            pioneer_cli_agent_runtime::DEFAULT_MAX_RECOVERY_FRAME_BYTES
        } else {
            2048
        };
        let bytes = tokio_timeout(request_timeout, async {
            let mut stream = response.bytes_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if bytes.len().saturating_add(chunk.len()) > max_bytes {
                    bail!("OpenCode HTTP response exceeds bounded capacity");
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok::<_, anyhow::Error>(bytes)
        })
        .await
        .map_err(|_| anyhow!("OpenCode response from `{label}` timed out"))??;
        if !status.is_success() {
            let detail = bounded_safe_detail(String::from_utf8_lossy(&bytes).as_ref());
            bail!("OpenCode `{label}` returned HTTP {status}: {detail}");
        }
        if bytes.is_empty() {
            return Ok(JsonValue::Null);
        }
        serde_json::from_slice(&bytes)
            .with_context(|| format!("OpenCode `{label}` returned invalid JSON"))
    }

    async fn health(&self, request_timeout: Duration) -> Result<JsonValue> {
        self.json(Method::GET, "/global/health", None, request_timeout)
            .await
    }

    async fn providers(&self, request_timeout: Duration) -> Result<JsonValue> {
        self.json(Method::GET, "/provider", None, request_timeout)
            .await
    }

    async fn tool_ids(&self, request_timeout: Duration) -> Result<Vec<String>> {
        let value = self
            .json(Method::GET, "/experimental/tool/ids", None, request_timeout)
            .await?;
        serde_json::from_value(value).context("OpenCode tool ID response is invalid")
    }

    async fn create_session(
        &self,
        permission: JsonValue,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        self.json(
            Method::POST,
            "/session",
            Some(&json!({ "permission": permission })),
            request_timeout,
        )
        .await
    }

    async fn get_session(&self, session_id: &str, request_timeout: Duration) -> Result<JsonValue> {
        validate_opencode_id(session_id, "session")?;
        self.json(
            Method::GET,
            format!("/session/{session_id}").as_str(),
            None,
            request_timeout,
        )
        .await
    }

    async fn update_session(
        &self,
        session_id: &str,
        body: &JsonValue,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        validate_opencode_id(session_id, "session")?;
        self.json(
            Method::PATCH,
            format!("/session/{session_id}").as_str(),
            Some(body),
            request_timeout,
        )
        .await
    }

    async fn fork_session(
        &self,
        session_id: &str,
        exclusive_message_id: Option<&str>,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        validate_opencode_id(session_id, "session")?;
        let body = exclusive_message_id
            .map(|id| json!({"messageID": id}))
            .unwrap_or_else(|| json!({}));
        self.json(
            Method::POST,
            format!("/session/{session_id}/fork").as_str(),
            Some(&body),
            request_timeout,
        )
        .await
    }

    async fn abort_session(&self, session_id: &str, request_timeout: Duration) -> Result<()> {
        validate_opencode_id(session_id, "session")?;
        self.json(
            Method::POST,
            format!("/session/{session_id}/abort").as_str(),
            None,
            request_timeout,
        )
        .await?;
        Ok(())
    }

    async fn session_status(&self, request_timeout: Duration) -> Result<JsonValue> {
        self.json(Method::GET, "/session/status", None, request_timeout)
            .await
    }

    async fn session_messages(
        &self,
        session_id: &str,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        validate_opencode_id(session_id, "session")?;
        self.json(
            Method::GET,
            format!("/session/{session_id}/message").as_str(),
            None,
            request_timeout,
        )
        .await
    }

    async fn prompt_async(
        &self,
        session_id: &str,
        body: &JsonValue,
        request_timeout: Duration,
    ) -> Result<()> {
        validate_opencode_id(session_id, "session")?;
        self.json(
            Method::POST,
            format!("/session/{session_id}/prompt_async").as_str(),
            Some(body),
            request_timeout,
        )
        .await?;
        Ok(())
    }

    async fn summarize(
        &self,
        session_id: &str,
        provider_id: &str,
        model_id: &str,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        validate_opencode_id(session_id, "session")?;
        self.json(
            Method::POST,
            format!("/session/{session_id}/summarize").as_str(),
            Some(&json!({
                "providerID": provider_id,
                "modelID": model_id,
                "auto": false,
            })),
            request_timeout,
        )
        .await
    }

    // Called before attaching the managed facade. No inherited name, including
    // `pioneer`, establishes ownership; summary never attaches a facade.
    async fn disconnect_inherited_mcp(&self, request_timeout: Duration) -> Result<()> {
        let statuses = self
            .json(Method::GET, "/mcp", None, request_timeout)
            .await?;
        let Some(statuses) = statuses.as_object() else {
            bail!("OpenCode MCP status response is invalid");
        };
        for name in statuses.keys() {
            let url = self.url_with_segments(&["mcp", name.as_str(), "disconnect"])?;
            self.json_url(
                Method::POST,
                url,
                "/mcp/:name/disconnect",
                None,
                request_timeout,
            )
            .await?;
        }
        Ok(())
    }

    async fn add_pioneer_mcp(
        &self,
        helper_path: &Path,
        bootstrap_path: &Path,
        request_timeout: Duration,
    ) -> Result<JsonValue> {
        let command = vec![
            helper_path.to_string_lossy().into_owned(),
            "__cli-mcp-stdio".to_owned(),
            bootstrap_path.to_string_lossy().into_owned(),
        ];
        self.json(
            Method::POST,
            "/mcp",
            Some(&json!({
                "name": OPENCODE_MCP_SERVER_NAME,
                "config": {
                    "type": "local",
                    "command": command,
                    "enabled": true,
                    "timeout": request_timeout.as_millis().min(u128::from(u32::MAX)) as u64,
                }
            })),
            request_timeout,
        )
        .await
    }

    async fn permission_reply(
        &self,
        request_id: &str,
        reply: &str,
        message: Option<&str>,
        request_timeout: Duration,
    ) -> Result<()> {
        validate_opencode_id(request_id, "permission request")?;
        let mut body = json!({ "reply": reply });
        if let Some(message) = message {
            body["message"] = JsonValue::String(message.to_owned());
        }
        self.json(
            Method::POST,
            format!("/permission/{request_id}/reply").as_str(),
            Some(&body),
            request_timeout,
        )
        .await?;
        Ok(())
    }

    async fn question_reply(
        &self,
        request_id: &str,
        answers: &[Vec<String>],
        request_timeout: Duration,
    ) -> Result<()> {
        validate_opencode_id(request_id, "question request")?;
        self.json(
            Method::POST,
            format!("/question/{request_id}/reply").as_str(),
            Some(&json!({ "answers": answers })),
            request_timeout,
        )
        .await?;
        Ok(())
    }

    async fn question_reject(&self, request_id: &str, request_timeout: Duration) -> Result<()> {
        validate_opencode_id(request_id, "question request")?;
        self.json(
            Method::POST,
            format!("/question/{request_id}/reject").as_str(),
            None,
            request_timeout,
        )
        .await?;
        Ok(())
    }
}

struct OpenCodeServerProcess {
    closed: std::sync::atomic::AtomicBool,
    api: OpenCodeApiClient,
    process: Mutex<CLIAgentProcess>,
    stdin: Mutex<Option<ChildStdin>>,
    stdout_reader: JoinHandle<()>,
    stderr: pioneer_cli_agent_runtime::process::StderrRing,
    shutdown_grace: Duration,
}

impl OpenCodeServerProcess {
    async fn close(&self) -> Result<()> {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        self.stdout_reader.abort();
        self.stdin.lock().await.take();
        let mut process = self.process.lock().await;
        process.terminate_with_grace(self.shutdown_grace).await?;
        Ok(())
    }
}

fn opencode_managed_data_home(
    runtime_home: &Path,
    workspace: &str,
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> PathBuf {
    let scope = pioneer_cli_agent_runtime::codex_attestation::sha256_json(&json!([
        workspace,
        instance.id,
        instance.home_path,
        instance.shadow_home_path
    ]))
    .expect("OpenCode managed scope is serializable");
    runtime_home
        .join("cli-runtime/opencode-data-v1")
        .join(scope)
}

async fn prepare_opencode_auth_link(
    managed_data_home: &Path,
    environment: &SensitiveEnvironment,
) -> Result<()> {
    let data_home = environment
        .expose("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("XDG_DATA_HOME").map(PathBuf::from))
        .filter(|path| path.is_absolute())
        .unwrap_or(expand_home_path("~/.local/share", None)?);
    let auth = data_home.join("opencode/auth.json");
    let link = managed_data_home.join("opencode/auth.json");
    tokio::fs::create_dir_all(link.parent().unwrap()).await?;
    // Keep auth in the ordinary store. OpenCode Auth.set/remove use writeFile
    // through this link, so OAuth refreshes remain visible to the normal CLI.
    match tokio::fs::symlink_metadata(&link).await {
        Ok(metadata) => {
            if !metadata.file_type().is_symlink() || tokio::fs::read_link(&link).await? != auth {
                bail!("OpenCode managed auth link does not target the ordinary auth store");
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(auth.parent().unwrap()).await?;
            #[cfg(unix)]
            let result = tokio::fs::symlink(&auth, &link).await;
            #[cfg(windows)]
            let result = tokio::fs::symlink_file(&auth, &link).await;
            match result {
                Ok(()) => {}
                Err(error)
                    if error.kind() == std::io::ErrorKind::AlreadyExists
                        && tokio::fs::read_link(&link).await? == auth => {}
                Err(error) => {
                    return Err(error).context("OpenCode managed auth link could not be created");
                }
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn spawn_opencode_server(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    cwd: &Path,
    environment: &SensitiveEnvironment,
    process_generation: Option<u64>,
    managed_data_home: &Path,
) -> Result<OpenCodeServerProcess> {
    validate_opencode_custom_args(&instance.app_server_args)
        .context("invalid OpenCode custom launch arguments")?;
    let password = SecretString::new(format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    ));
    let mut env = environment.clone();
    env.insert_secret("OPENCODE_SERVER_PASSWORD", password.clone());
    env.insert_plain("OPENCODE_SERVER_USERNAME", OPENCODE_SERVER_USERNAME);
    env.insert_plain("OPENCODE_CLIENT", "pioneer");
    env.insert_plain("OPENCODE_ENABLE_QUESTION_TOOL", "1");
    env.insert_plain("OPENCODE_DISABLE_AUTOUPDATE", "1");
    let config_dir = expand_home_path(
        instance
            .shadow_home_path
            .as_deref()
            .unwrap_or(instance.home_path.as_str()),
        None,
    )?;
    let (xdg_config_home, extra_config_dir) = opencode_config_environment_roots(&config_dir)?;
    env.insert_plain(
        "XDG_CONFIG_HOME",
        xdg_config_home.to_string_lossy().into_owned(),
    );
    if let Some(extra_config_dir) = extra_config_dir.as_ref() {
        env.insert_plain(
            "OPENCODE_CONFIG_DIR",
            extra_config_dir.to_string_lossy().into_owned(),
        );
    }
    // Isolate the whole provider data root as well as the supported DB path:
    // v1.4.14's CLI JSON migration reads Global.Path.data/storage even with
    // OPENCODE_DB set. A DB override alone can import remembered permissions.
    prepare_opencode_auth_link(managed_data_home, environment).await?;
    env.insert_plain(
        "XDG_DATA_HOME",
        managed_data_home.to_string_lossy().into_owned(),
    );
    env.insert_plain(
        "OPENCODE_DB",
        managed_data_home
            .join("opencode/opencode.db")
            .to_string_lossy()
            .into_owned(),
    );
    let mut env_remove = vec![
        "OPENCODE_CONFIG",
        "OPENCODE_CONFIG_CONTENT",
        "OPENCODE_PERMISSION",
        "OPENCODE_AUTH_CONTENT",
        "OPENCODE_TEST_HOME",
        "OPENCODE_SKIP_MIGRATIONS",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    if extra_config_dir.is_none() {
        env_remove.push("OPENCODE_CONFIG_DIR".into());
    }
    let http_client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .context("failed to build OpenCode loopback HTTP client")?;
    let mut process_config = CLIAgentProcessSpawnConfig {
        executable: instance.binary_path.clone(),
        args: vec![
            "serve".to_owned(),
            "--hostname".to_owned(),
            "127.0.0.1".to_owned(),
            "--port".to_owned(),
            "0".to_owned(),
            "--mdns".to_owned(),
            "false".to_owned(),
        ],
        cwd: Some(cwd.to_path_buf()),
        home_path: None,
        home_dir: None,
        env,
        env_remove,
        stderr_ring_lines: instance.stderr_ring_lines.max(1),
        process_group: true,
        process_generation,
    };
    process_config
        .args
        .extend(instance.app_server_args.iter().cloned());
    let mut process = spawn_cli_agent_process(&process_config)
        .with_context(|| format!("failed to spawn OpenCode server `{}`", instance.binary_path))?;
    let stderr = process.stderr();
    let (stdout, stdin) = match process.take_stdio() {
        Ok(stdio) => stdio,
        Err(error) => {
            let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
            return Err(error).context("failed to capture OpenCode server stdio");
        }
    };
    let mut reader = BufReader::new(stdout);
    let startup_timeout = Duration::from_millis(instance.startup_probe_timeout_ms.max(1));
    let base_url = tokio_timeout(startup_timeout, async {
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line).await?;
            if read == 0 {
                bail!("OpenCode server exited before publishing its listen address");
            }
            if let Some(url) = line.trim().strip_prefix(OPENCODE_SERVER_PREFIX) {
                let parsed = reqwest::Url::parse(url).context("invalid OpenCode listen URL")?;
                if parsed.scheme() != "http"
                    || parsed.host_str() != Some("127.0.0.1")
                    || parsed.port().is_none_or(|port| port == 0)
                    || parsed.path() != "/"
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                {
                    bail!("OpenCode server did not bind to the managed loopback boundary");
                }
                break Ok(parsed.as_str().trim_end_matches('/').to_owned());
            }
        }
    })
    .await
    .map_err(|_| anyhow!("OpenCode server startup timed out"));
    let base_url = match base_url {
        Ok(Ok(base_url)) => base_url,
        Ok(Err(error)) | Err(error) => {
            let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
            return Err(error).context("OpenCode server did not publish a usable listen address");
        }
    };
    let stdout_reader = tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });
    let api = OpenCodeApiClient {
        client: http_client,
        base_url,
        directory: cwd.to_string_lossy().into_owned(),
        username: OPENCODE_SERVER_USERNAME.to_owned(),
        password,
    };
    let health = api.health(startup_timeout).await.and_then(|health| {
        if health["healthy"] != true
            || health["version"]
                .as_str()
                .map(|v| v.trim_start_matches('v'))
                != Some(OPENCODE_SUPPORTED_VERSION)
        {
            return Err(UnsupportedOpenCodeVersion.into());
        }
        Ok(health)
    });
    if let Err(error) = health {
        stdout_reader.abort();
        let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
        return Err(error).context("OpenCode server health probe failed");
    }
    Ok(OpenCodeServerProcess {
        closed: std::sync::atomic::AtomicBool::new(false),
        api,
        process: Mutex::new(process),
        stdin: Mutex::new(Some(stdin)),
        stdout_reader,
        stderr,
        shutdown_grace: Duration::from_secs(5),
    })
}

// The shared compaction runner owns timeout/cancellation and calls cleanup,
// just as it does for Codex/Claude services. No interactive session is reused.
pub(crate) struct OpenCodeSummaryService {
    instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
    environment: SensitiveEnvironment,
    owner: Mutex<Option<(Arc<OpenCodeServerProcess>, tempfile::TempDir)>>,
}
impl OpenCodeSummaryService {
    pub(crate) fn new(
        instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
        environment: SensitiveEnvironment,
    ) -> Self {
        Self {
            instance,
            environment,
            owner: Mutex::new(None),
        }
    }
    pub(crate) async fn summarize(
        &self,
        model: String,
        effort: Option<String>,
        instructions: &str,
        input: String,
        timeout: Duration,
    ) -> Result<(String, Option<u64>, Option<u64>, JsonValue)> {
        let (provider, model_id) = parse_optional_opencode_model(Some(&model))?
            .context("OpenCode summary model is missing")?;
        let temporary = tempfile::tempdir()?;
        let server = Arc::new(
            spawn_opencode_server(
                &self.instance,
                temporary.path(),
                &self.environment,
                None,
                &temporary.path().join("data"),
            )
            .await?,
        );
        {
            let mut owner = self.owner.lock().await;
            if owner.is_some() {
                let _ = server.close().await;
                bail!("OpenCode summary service already owns a process");
            }
            *owner = Some((server.clone(), temporary));
        }
        server.api.disconnect_inherited_mcp(timeout).await?;
        let session = server
            .api
            .create_session(
                json!([{"permission":"*","pattern":"*","action":"deny"}]),
                timeout,
            )
            .await?;
        let id = session["id"]
            .as_str()
            .context("OpenCode summary session identity is missing")?;
        validate_opencode_id(id, "summary session")?;
        // v1.4.14's deprecated `tools` input replaces session.permission.
        // Omit it so the wildcard deny above remains authoritative for native,
        // custom and MCP tools throughout this tool-free summary.
        let mut prompt = json!({
            "model":{"providerID":provider,"modelID":model_id},
            "system":instructions, "parts":[{"type":"text","text":input}]
        });
        if let Some(effort) = effort {
            prompt["variant"] = json!(effort);
        }
        let result = server
            .api
            .json(
                Method::POST,
                &format!("/session/{id}/message"),
                Some(&prompt),
                timeout,
            )
            .await?;
        anyhow::ensure!(
            result["info"]["error"].is_null()
                && result["info"]["time"]["completed"].as_u64().is_some()
                && result["info"]["finish"]
                    .as_str()
                    .is_some_and(|s| !matches!(s, "tool-calls" | "unknown")),
            "OpenCode summary ended without a final completion"
        );
        anyhow::ensure!(
            result["info"]["providerID"] == provider && result["info"]["modelID"] == model_id,
            "OpenCode summary model changed"
        );
        let text = result["parts"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|p| p["type"] == "text")
            .filter_map(|p| p["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        anyhow::ensure!(!text.trim().is_empty(), "OpenCode summary returned no text");
        let tokens = &result["info"]["tokens"];
        let input_tokens = tokens["input"].as_u64();
        let output_tokens = tokens["output"].as_u64();
        Ok((
            text,
            input_tokens,
            output_tokens,
            json!({"input_tokens":input_tokens,"output_tokens":output_tokens,
            "cache_read_input_tokens":tokens["cache"]["read"].as_u64(),"cache_creation_input_tokens":tokens["cache"]["write"].as_u64()}),
        ))
    }
    pub(crate) async fn cleanup(&self) -> Result<()> {
        if let Some((server, _temporary)) = self.owner.lock().await.take() {
            server.close().await?;
        }
        Ok(())
    }
}

pub(crate) async fn probe_opencode_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    cwd: &Path,
    environment: &SensitiveEnvironment,
) -> OpenCodeProbeSnapshot {
    if !instance.enabled {
        return OpenCodeProbeSnapshot {
            status: OpenCodeProbeStatus::Error,
            message: Some("OpenCode runtime is disabled".to_owned()),
            version: None,
            account: None,
            diagnostics: Vec::new(),
            stderr: Vec::new(),
            models: Vec::new(),
        };
    }
    let managed_home = opencode_managed_data_home(cwd, "probe", instance);
    let server = match spawn_opencode_server(instance, cwd, environment, None, &managed_home).await
    {
        Ok(server) => server,
        Err(error) => {
            let missing = error.chain().any(|source| {
                source
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
            });
            let status = if error.downcast_ref::<UnsupportedOpenCodeVersion>().is_some() {
                OpenCodeProbeStatus::UnsupportedVersion
            } else if missing {
                OpenCodeProbeStatus::MissingBinary
            } else {
                OpenCodeProbeStatus::SpawnFailed
            };
            let message = bounded_safe_detail(format!("{error:#}").as_str());
            return OpenCodeProbeSnapshot {
                status,
                message: Some(message.clone()),
                version: None,
                account: None,
                diagnostics: vec![OpenCodeProbeDiagnostic {
                    level: OpenCodeProbeDiagnosticLevel::Error,
                    code: if missing {
                        "opencode_probe.missing_binary"
                    } else {
                        "opencode_probe.spawn_failed"
                    }
                    .to_owned(),
                    message,
                }],
                stderr: Vec::new(),
                models: Vec::new(),
            };
        }
    };
    let request_timeout = Duration::from_millis(instance.request_timeout_ms.max(1));
    let health = server.api.health(request_timeout).await;
    let providers = server.api.providers(request_timeout).await;
    let stderr_before_close = server.stderr.lines().await;
    let close_result = server.close().await;
    let (health, providers) = match (health, providers) {
        (Ok(health), Ok(providers)) => (health, providers),
        (health, providers) => {
            let error = health
                .err()
                .or_else(|| providers.err())
                .expect("one OpenCode probe request failed");
            let message = bounded_safe_detail(format!("{error:#}").as_str());
            return OpenCodeProbeSnapshot {
                status: OpenCodeProbeStatus::Error,
                message: Some(message.clone()),
                version: None,
                account: None,
                diagnostics: vec![OpenCodeProbeDiagnostic {
                    level: OpenCodeProbeDiagnosticLevel::Error,
                    code: "opencode_probe.server_api_failed".to_owned(),
                    message,
                }],
                stderr: stderr_before_close,
                models: Vec::new(),
            };
        }
    };
    let version = health
        .get("version")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let healthy = health
        .get("healthy")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    if !healthy
        || version.as_deref().map(|v| v.trim_start_matches('v')) != Some(OPENCODE_SUPPORTED_VERSION)
    {
        return OpenCodeProbeSnapshot {
            status: if healthy {
                OpenCodeProbeStatus::UnsupportedVersion
            } else {
                OpenCodeProbeStatus::Error
            },
            message: Some("OpenCode 1.4.14 server contract is required".to_owned()),
            version,
            account: None,
            diagnostics: vec![OpenCodeProbeDiagnostic {
                level: OpenCodeProbeDiagnosticLevel::Error,
                code: "opencode_probe.unsupported_server".to_owned(),
                message: "OpenCode 1.4.14 server contract is required".to_owned(),
            }],
            stderr: stderr_before_close,
            models: Vec::new(),
        };
    }
    let connected = providers
        .get("connected")
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::to_owned)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let models = opencode_models_from_provider_response(&providers, &connected);
    let authenticated = !connected.is_empty();
    let status = if authenticated {
        OpenCodeProbeStatus::Ready
    } else {
        OpenCodeProbeStatus::NeedsAuth
    };
    let message = (!authenticated).then(|| {
        "OpenCode has no connected model provider; authenticate one with the OpenCode CLI"
            .to_owned()
    });
    let mut diagnostics = Vec::new();
    if let Some(message) = message.as_ref() {
        diagnostics.push(OpenCodeProbeDiagnostic {
            level: OpenCodeProbeDiagnosticLevel::Warning,
            code: "opencode_probe.needs_auth".to_owned(),
            message: message.clone(),
        });
    } else {
        diagnostics.push(OpenCodeProbeDiagnostic {
            level: OpenCodeProbeDiagnosticLevel::Info,
            code: "opencode_probe.server_ready".to_owned(),
            message: format!(
                "OpenCode server API is ready with {} connected provider{}",
                connected.len(),
                if connected.len() == 1 { "" } else { "s" }
            ),
        });
    }
    if let Err(error) = close_result {
        diagnostics.push(OpenCodeProbeDiagnostic {
            level: OpenCodeProbeDiagnosticLevel::Warning,
            code: "opencode_probe.shutdown_failed".to_owned(),
            message: bounded_safe_detail(format!("{error:#}").as_str()),
        });
    }
    OpenCodeProbeSnapshot {
        status,
        message,
        version,
        account: Some(OpenCodeAccountSnapshot {
            authenticated,
            display_name: authenticated.then(|| {
                format!(
                    "{} connected provider{}",
                    connected.len(),
                    if connected.len() == 1 { "" } else { "s" }
                )
            }),
            auth_method: authenticated.then(|| "opencode_provider".to_owned()),
        }),
        diagnostics,
        stderr: stderr_before_close,
        models,
    }
}

fn opencode_models_from_provider_response(
    providers: &JsonValue,
    connected: &HashSet<String>,
) -> Vec<OpenCodeModelSnapshot> {
    let mut result = Vec::new();
    let Some(all) = providers.get("all").and_then(JsonValue::as_array) else {
        return result;
    };
    for provider in all {
        let Some(provider_id) = provider.get("id").and_then(JsonValue::as_str) else {
            continue;
        };
        if !connected.contains(provider_id) {
            continue;
        }
        let source = provider
            .get("source")
            .and_then(JsonValue::as_str)
            .unwrap_or_default();
        let Some(models) = provider.get("models").and_then(JsonValue::as_object) else {
            continue;
        };
        for (map_id, model) in models {
            let model_id = model
                .get("id")
                .and_then(JsonValue::as_str)
                .unwrap_or(map_id.as_str());
            let capabilities = model.get("capabilities").unwrap_or(&JsonValue::Null);
            let input = capabilities.get("input").unwrap_or(&JsonValue::Null);
            let output = capabilities.get("output").unwrap_or(&JsonValue::Null);
            let mut effort_options = model
                .get("variants")
                .and_then(JsonValue::as_object)
                .map(|variants| variants.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            effort_options.sort();
            let status = model
                .get("status")
                .and_then(JsonValue::as_str)
                .unwrap_or("active");
            result.push(OpenCodeModelSnapshot {
                id: format!("{provider_id}/{model_id}"),
                name: model
                    .get("name")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned),
                description: Some(format!(
                    "{} · {}",
                    provider
                        .get("name")
                        .and_then(JsonValue::as_str)
                        .unwrap_or(provider_id),
                    status
                )),
                family: model
                    .get("family")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned),
                is_custom: matches!(source, "config" | "custom"),
                active: Some(status != "deprecated"),
                effort_options,
                input_modalities: enabled_modalities(input),
                output_modalities: enabled_modalities(output),
                supports_reasoning: capabilities.get("reasoning").and_then(JsonValue::as_bool),
                supports_vision: input.get("image").and_then(JsonValue::as_bool),
                max_input_tokens: model
                    .pointer("/limit/input")
                    .or_else(|| model.pointer("/limit/context"))
                    .and_then(JsonValue::as_u64),
                max_output_tokens: model.pointer("/limit/output").and_then(JsonValue::as_u64),
            });
        }
    }
    result.sort_by(|left, right| left.id.cmp(&right.id));
    result
}

fn enabled_modalities(value: &JsonValue) -> Vec<String> {
    let mut result = value
        .as_object()
        .into_iter()
        .flat_map(|object| object.iter())
        .filter(|(_, enabled)| enabled.as_bool() == Some(true))
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    result.sort();
    result
}

pub(crate) fn opencode_provider_contract_fingerprint(version: Option<&str>) -> String {
    pioneer_cli_agent_runtime::codex_attestation::sha256_json(&json!({
        "provider": "opencode",
        "serverApiContractVersion": OPENCODE_API_CONTRACT_VERSION,
        "providerVersion": version,
        "transport": "authenticated_loopback_http_sse",
        "mcp": "dynamic_local_stdio",
    }))
    .expect("static OpenCode provider contract is serializable")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenCodePendingRequestKind {
    Permission,
    Question,
}

#[derive(Debug, Clone)]
struct OpenCodeItemState {
    kind: String,
    text: String,
    metadata: Option<JsonValue>,
    completed: bool,
}

#[derive(Debug, Default)]
struct OpenCodeRuntimeState {
    native_thread_id: Option<String>,
    active_turn_id: Option<String>,
    turn_started: bool,
    terminal_emitted: bool,
    interrupted: bool,
    last_error: Option<JsonValue>,
    last_diff: Option<JsonValue>,
    item_order: Vec<String>,
    // Native SSE prefix is separate from the prefix hydrated by REST. Queued
    // deltas advance this cursor even when REST has already published them.
    stream_offsets: HashMap<String, usize>,
    awaiting_execution: bool,
    user_observed: bool,
    execution_observed: bool,
    dispatch_deadline: Option<tokio::time::Instant>,
    assistant_message_ids: HashSet<String>,
    turn_user_ids: HashSet<String>,
    cwd: Option<String>,
    items: HashMap<String, OpenCodeItemState>,
    pending_requests: HashMap<String, OpenCodePendingRequestKind>,
    last_model: Option<(String, String)>,
    last_turn_observation: Option<CLIAgentRuntimeTurnObservation>,
}

impl OpenCodeRuntimeState {
    fn begin_turn(&mut self, native_thread_id: String, native_turn_id: String) {
        self.native_thread_id = Some(native_thread_id);
        self.turn_user_ids = HashSet::from([native_turn_id.clone()]);
        self.active_turn_id = Some(native_turn_id);
        self.turn_started = false;
        self.terminal_emitted = false;
        self.interrupted = false;
        self.last_error = None;
        self.last_diff = None;
        self.item_order.clear();
        self.stream_offsets.clear();
        self.awaiting_execution = true;
        self.dispatch_deadline = None;
        self.user_observed = false;
        self.execution_observed = false;
        self.assistant_message_ids.clear();
        self.items.clear();
        self.pending_requests.clear();
        self.last_turn_observation = Some(CLIAgentRuntimeTurnObservation {
            status: CLIAgentRuntimeObservedTurnStatus::InProgress,
            message: None,
            reconciliation_events: Vec::new(),
        });
    }

    fn snapshot_is_live(
        &mut self,
        session: &str,
        turn: &str,
        messages: &JsonValue,
        native_busy: bool,
        stable_idle: bool,
    ) -> bool {
        let mut live = native_busy || !stable_idle;
        if self.native_thread_id.as_deref() == Some(session)
            && self.active_turn_id.as_deref() == Some(turn)
        {
            let users = opencode_turn_user_ids(&messages, turn);
            let user_saved = messages
                .as_array()
                .into_iter()
                .flatten()
                .any(|message| message["info"]["id"] == turn);
            let assistant = messages.as_array().into_iter().flatten().any(|message| {
                message["info"]["role"] == "assistant"
                    && message["info"]["parentID"]
                        .as_str()
                        .is_some_and(|id| users.contains(id))
            });
            if assistant || (user_saved && native_busy) {
                self.observe_execution();
            }
            live |= self.awaiting_execution && !self.execution_observed;
        }
        live
    }

    fn observe_execution(&mut self) {
        self.execution_observed = true;
        self.awaiting_execution = false;
        self.dispatch_deadline = None;
    }
}

fn spawn_opencode_event_stream(
    server: Arc<OpenCodeServerProcess>,
    state: Arc<Mutex<OpenCodeRuntimeState>>,
    event_tx: mpsc::Sender<RuntimeEvent>,
    cancellation: CancellationToken,
    include_native: bool,
    budget: pioneer_cli_agent_runtime::NativeEventBudget,
    ready: watch::Sender<bool>,
    request_timeout: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let api = &server.api;
        let codec = pioneer_cli_agent_runtime::BoundedNativeEventCodec::new(budget);
        while !cancellation.is_cancelled()
            && !server.closed.load(std::sync::atomic::Ordering::SeqCst)
        {
            ready.send_replace(false);
            let response = tokio::select! {
                _ = cancellation.cancelled() => break,
                response = api.request(Method::GET, "/event").send() => response,
            };
            let response = match response {
                Ok(response) if response.status().is_success() => response,
                _ => {
                    emit_opencode_stream_diagnostic(
                        &state,
                        &event_tx,
                        "OpenCode event stream connection failed".into(),
                    )
                    .await;
                    if wait_or_cancel(&cancellation, Duration::from_millis(500)).await {
                        break;
                    }
                    continue;
                }
            };
            let stream = response.bytes_stream().map_err(std::io::Error::other);
            let mut reader = BufReader::new(tokio_util::io::StreamReader::new(stream));
            let mut data = Vec::new();
            loop {
                let line = tokio::select! {
                    _ = cancellation.cancelled() => return,
                    line = codec.read_frame_or_spool(&mut reader) => line,
                };
                let line = match line {
                    Ok(Some(pioneer_cli_agent_runtime::SpooledNativeFrame::Complete(line))) => line,
                    Ok(None) => break,
                    Err(error)
                        if error.kind == pioneer_cli_agent_runtime::NativeIngressErrorKind::Io =>
                    {
                        break;
                    }
                    // SSE cannot replay a discarded frame. Fail this generation once;
                    // do not reconnect forever and silently discard the tool result.
                    _ => {
                        fail_opencode_ingress(&server, &state, &event_tx, request_timeout).await;
                        ready.send_replace(false);
                        return;
                    }
                };
                let line = line.strip_suffix(b"\n").unwrap_or(&line);
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                if !line.is_empty() {
                    if let Some(value) = line.strip_prefix(b"data:") {
                        let value = value.strip_prefix(b" ").unwrap_or(value);
                        if data.len().saturating_add(value.len()).saturating_add(1)
                            > budget.max_frame_bytes
                        {
                            fail_opencode_ingress(&server, &state, &event_tx, request_timeout)
                                .await;
                            ready.send_replace(false);
                            return;
                        }
                        if !data.is_empty() {
                            data.push(b'\n');
                        }
                        data.extend_from_slice(value);
                    }
                    continue;
                }
                if data.is_empty() {
                    continue;
                }
                let raw = serde_json::from_slice::<JsonValue>(&data);
                data.clear();
                let raw = match raw {
                    Ok(raw) => raw,
                    Err(_) => {
                        let snapshot = state.lock().await;
                        let event = RuntimeEvent::Raw(RuntimeRawEvent {
                            native_method: "event/invalid_json".into(),
                            reason: "OpenCode SSE event was not valid JSON or UTF-8".into(),
                            native_thread_id: snapshot.native_thread_id.clone(),
                            native_turn_id: snapshot.active_turn_id.clone(),
                            native_item_id: None,
                            payload_redacted: None,
                            raw_redacted: None,
                        });
                        drop(snapshot);
                        if event_tx.send(event).await.is_err() {
                            return;
                        }
                        continue;
                    }
                };
                let kind = raw["type"].as_str().unwrap_or("");
                let events = if kind == "server.connected" {
                    match reconcile_opencode_turn(&server, &state, request_timeout, true).await {
                        Ok(events) => {
                            ready.send_replace(true);
                            events
                        }
                        Err(_) => break,
                    }
                } else if kind == "session.idle"
                    || (kind == "session.status" && raw["properties"]["status"]["type"] == "idle")
                {
                    if !event_belongs_to_active_session(&*state.lock().await, &raw["properties"]) {
                        continue;
                    }
                    match reconcile_opencode_turn(&server, &state, request_timeout, true).await {
                        Ok(events) => events,
                        Err(_) => break,
                    }
                } else {
                    if kind == "message.updated" && raw["properties"]["info"]["role"] == "assistant"
                    {
                        let _ = accept_opencode_assistant_info(
                            &api,
                            &state,
                            &raw["properties"]["info"],
                            request_timeout,
                        )
                        .await;
                    }
                    // A part may precede message.updated after reconnect. Fetch its
                    // authoritative parent identity before attributing it to a turn.
                    if kind.starts_with("message.part.") {
                        let properties = &raw["properties"];
                        let mid = properties["messageID"]
                            .as_str()
                            .or_else(|| properties["part"]["messageID"].as_str());
                        let session = state.lock().await.native_thread_id.clone();
                        if let (Some(mid), Some(session)) = (mid, session) {
                            let known = state.lock().await.assistant_message_ids.contains(mid);
                            if !known {
                                let url = match api
                                    .url_with_segments(&["session", &session, "message", mid])
                                {
                                    Ok(url) => url,
                                    Err(_) => continue,
                                };
                                if let Ok(message) = api
                                    .json_url(
                                        Method::GET,
                                        url,
                                        "message identity",
                                        None,
                                        request_timeout,
                                    )
                                    .await
                                {
                                    let _ = accept_opencode_assistant_info(
                                        &api,
                                        &state,
                                        &message["info"],
                                        request_timeout,
                                    )
                                    .await;
                                    map_opencode_event(&state, json!({"type":"message.updated","properties":{"info":message["info"]}}), false).await;
                                }
                            }
                        }
                    }
                    map_opencode_event(&state, raw, include_native).await
                };
                for event in events {
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        result = event_tx.send(event) => if result.is_err() { return; },
                    }
                }
            }
            ready.send_replace(false);
            emit_opencode_stream_diagnostic(
                &state,
                &event_tx,
                "OpenCode event stream disconnected; reconciling on reconnect".into(),
            )
            .await;
            if wait_or_cancel(&cancellation, Duration::from_millis(250)).await {
                break;
            }
        }
    })
}

async fn fail_opencode_ingress(
    server: &OpenCodeServerProcess,
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    tx: &mpsc::Sender<RuntimeEvent>,
    timeout: Duration,
) {
    let (session, events) = {
        let mut state = state.lock().await;
        let session = state.native_thread_id.clone();
        state.last_error = Some(
            json!({"name":"NativeEventBudgetExceeded","data":{"message":"OpenCode SSE frame exceeds the admitted native event budget"}}),
        );
        let events = terminalize_opencode_failure(&mut state, None);
        (session, events)
    };
    if let Some(session) = session {
        let _ = server.api.abort_session(&session, timeout).await;
    }
    let _ = server.close().await;
    for event in events {
        if tx.send(event).await.is_err() {
            break;
        }
    }
}

async fn wait_or_cancel(cancellation: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancellation.cancelled() => true,
        _ = sleep(duration) => false,
    }
}

#[cfg(test)]
fn take_sse_frame(buffer: &[u8]) -> Option<(&[u8], usize)> {
    if let Some(index) = buffer.windows(2).position(|window| window == b"\n\n") {
        return Some((&buffer[..index], index + 2));
    }
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (&buffer[..index], index + 4))
}

async fn emit_opencode_stream_diagnostic(
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    event_tx: &mpsc::Sender<RuntimeEvent>,
    message: String,
) {
    let state = state.lock().await;
    let event = RuntimeEvent::Error(RuntimeErrorEvent {
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone(),
        message: bounded_safe_detail(message.as_str()),
        code: Some("opencode_event_stream_disconnected".to_owned()),
        retryable: true,
        native: None,
    });
    drop(state);
    let _ = event_tx.send(event).await;
}

async fn map_opencode_event(
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    raw: JsonValue,
    include_native: bool,
) -> Vec<RuntimeEvent> {
    let event_type = raw
        .get("type")
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let properties = raw.get("properties").cloned().unwrap_or_else(|| json!({}));
    let mut state = state.lock().await;
    if !event_belongs_to_active_session(&state, &properties) {
        return Vec::new();
    }
    if state.terminal_emitted
        && !matches!(
            event_type.as_str(),
            "session.created" | "session.updated" | "server.connected" | "server.heartbeat"
        )
    {
        return Vec::new();
    }
    let native = || opencode_native_event(event_type.as_str(), &properties, &raw, include_native);
    match event_type.as_str() {
        "server.connected" | "server.heartbeat" => Vec::new(),
        "session.created" => vec![RuntimeEvent::ThreadStateChanged(
            RuntimeThreadStateChanged {
                native_thread_id: properties
                    .get("info")
                    .and_then(|info| info.get("id"))
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned),
                status: "started".to_owned(),
                native: native(),
            },
        )],
        "session.updated" => vec![RuntimeEvent::ThreadStateChanged(
            RuntimeThreadStateChanged {
                native_thread_id: session_id_from_properties(&properties),
                status: "updated".to_owned(),
                native: native(),
            },
        )],
        "message.updated" => {
            if properties["info"]["role"] == "user"
                && properties["info"]["id"].as_str() == state.active_turn_id.as_deref()
            {
                state.user_observed = true;
            }

            if let Some(info) = properties.get("info")
                && info.get("role").and_then(JsonValue::as_str) == Some("assistant")
                && !state.terminal_emitted
                && info
                    .get("parentID")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|parent| state.turn_user_ids.contains(parent))
                && let Some(message_id) = info.get("id").and_then(JsonValue::as_str)
            {
                state.assistant_message_ids.insert(message_id.to_owned());
                state.observe_execution();
            }
            if let Some(info) = properties.get("info")
                && info["id"].as_str() == state.active_turn_id.as_deref()
                && let Some(diff) = info.pointer("/summary/diffs")
            {
                return map_opencode_diff(&mut state, diff, native());
            }
            Vec::new()
        }
        "message.part.delta" => map_opencode_part_delta(&mut state, &properties, native()),
        "message.part.updated" => map_opencode_part_updated(&mut state, &properties, native()),
        "session.status" => map_opencode_session_status(&mut state, &properties, native()),
        "session.idle" => Vec::new(),
        "session.error" => fail_opencode_turn(&mut state, &properties, native()),
        "session.diff" => properties
            .get("diff")
            .map(|diff| map_opencode_diff(&mut state, diff, native()))
            .unwrap_or_default(),
        "todo.updated" => {
            let Some(native_turn_id) = state.active_turn_id.clone() else {
                return Vec::new();
            };
            vec![RuntimeEvent::PlanUpdated(RuntimePlanUpdated {
                native_thread_id: state.native_thread_id.clone(),
                native_turn_id,
                plan_redacted: properties
                    .get("todos")
                    .or_else(|| properties.get("todo"))
                    .map(redact_opencode_json),
                native: native(),
            })]
        }
        "permission.asked" => map_opencode_permission_request(&mut state, &properties, native()),
        "permission.replied" => {
            let request_id = properties
                .get("requestID")
                .or_else(|| properties.get("id"))
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            if request_id.is_empty() {
                Vec::new()
            } else {
                state.pending_requests.remove(request_id.as_str());
                vec![RuntimeEvent::RequestResolved(RuntimeRequestResolved {
                    native_request_id: request_id,
                    native: native(),
                })]
            }
        }
        "question.asked" => map_opencode_question_request(&mut state, &properties, native()),
        "question.replied" | "question.rejected" => {
            let request_id = properties
                .get("requestID")
                .or_else(|| properties.get("id"))
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            if request_id.is_empty() {
                Vec::new()
            } else {
                state.pending_requests.remove(request_id.as_str());
                vec![RuntimeEvent::RequestResolved(RuntimeRequestResolved {
                    native_request_id: request_id,
                    native: native(),
                })]
            }
        }
        _ => vec![RuntimeEvent::Raw(RuntimeRawEvent {
            native_method: event_type,
            reason: "OpenCode event has no canonical projection".to_owned(),
            native_thread_id: state.native_thread_id.clone(),
            native_turn_id: state.active_turn_id.clone(),
            native_item_id: properties
                .get("part")
                .and_then(|part| part.get("id"))
                .and_then(JsonValue::as_str)
                .map(str::to_owned),
            payload_redacted: Some(redact_opencode_json(&properties)),
            raw_redacted: include_native.then(|| redact_opencode_json(&raw)),
        })],
    }
}

fn event_belongs_to_active_session(state: &OpenCodeRuntimeState, properties: &JsonValue) -> bool {
    let Some(active) = state.native_thread_id.as_deref() else {
        return true;
    };
    session_id_from_properties(properties)
        .as_deref()
        .is_none_or(|session_id| session_id == active)
}

fn session_id_from_properties(properties: &JsonValue) -> Option<String> {
    properties
        .get("sessionID")
        .or_else(|| properties.get("sessionId"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .or_else(|| {
            properties
                .get("part")
                .and_then(|part| part.get("sessionID"))
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            properties
                .get("info")
                .and_then(|info| info.get("sessionID"))
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
}

fn map_opencode_part_delta(
    state: &mut OpenCodeRuntimeState,
    properties: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    let Some(native_thread_id) = state.native_thread_id.clone() else {
        return Vec::new();
    };
    let Some(native_turn_id) = state.active_turn_id.clone() else {
        return Vec::new();
    };
    let message_id = properties
        .get("messageID")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if state.terminal_emitted || !state.assistant_message_ids.contains(message_id) {
        return Vec::new();
    }
    let part_id = properties
        .get("partID")
        .or_else(|| properties.get("partId"))
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let delta = properties
        .get("delta")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if part_id.is_empty() || delta.is_empty() {
        return Vec::new();
    }
    if properties.get("field").and_then(JsonValue::as_str) != Some("text") {
        return Vec::new();
    }
    let newly_started = !state.items.contains_key(part_id);
    let item = state
        .items
        .entry(part_id.to_owned())
        .or_insert_with(|| OpenCodeItemState {
            kind: "agentMessage".to_owned(),
            text: String::new(),
            metadata: None,
            completed: false,
        });
    if item.completed {
        return Vec::new();
    }
    let offset = state
        .stream_offsets
        .entry(part_id.to_owned())
        .or_insert(item.text.len());
    let covered = item.text.len().saturating_sub(*offset).min(delta.len());
    // Compare only the newly covered bytes, not the whole accumulated prefix.
    // Native full part.updated establishes this cursor; REST never resets it.
    if item.text.get(*offset..offset.saturating_add(covered)) != delta.get(..covered) {
        // A native content replacement needs its next full baseline. Guessing
        // an overlap from repeated text could discard genuinely new output.
        return Vec::new();
    }
    *offset = offset.saturating_add(delta.len());
    if covered == delta.len() {
        return Vec::new();
    }
    let delta = delta[covered..].to_owned();
    item.text.push_str(&delta);
    let delta_kind = if normalize_opencode_kind(item.kind.as_str()) == "reasoning" {
        RuntimeItemDeltaKind::ReasoningText
    } else if is_tool_item_kind(item.kind.as_str()) {
        RuntimeItemDeltaKind::ToolProgress
    } else {
        RuntimeItemDeltaKind::AgentMessage
    };
    let mut events = Vec::with_capacity(2);
    if newly_started {
        state.item_order.push(part_id.to_owned());
        events.push(RuntimeEvent::ItemStarted(RuntimeItemStarted {
            native_thread_id: Some(native_thread_id.clone()),
            native_turn_id: native_turn_id.clone(),
            native_item_id: part_id.to_owned(),
            item_kind: item.kind.clone(),
            title: None,
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: item.metadata.clone(),
            native_item_redacted: None,
            native: native.clone(),
        }));
    }
    events.push(RuntimeEvent::ItemDelta(RuntimeItemDelta {
        native_thread_id: Some(native_thread_id),
        native_turn_id,
        native_item_id: part_id.to_owned(),
        item_kind: item.kind.clone(),
        delta_kind,
        delta: delta.to_owned(),
        metadata: item.metadata.clone(),
        native,
    }));
    events
}

fn map_opencode_part_updated(
    state: &mut OpenCodeRuntimeState,
    properties: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    map_opencode_part(state, properties, native, false)
}

fn map_opencode_part(
    state: &mut OpenCodeRuntimeState,
    properties: &JsonValue,
    native: Option<RuntimeNativeEvent>,
    snapshot: bool,
) -> Vec<RuntimeEvent> {
    let Some(part) = properties.get("part") else {
        return Vec::new();
    };
    let Some(native_thread_id) = state.native_thread_id.clone() else {
        return Vec::new();
    };
    let Some(native_turn_id) = state.active_turn_id.clone() else {
        return Vec::new();
    };
    let message_id = part
        .get("messageID")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if state.terminal_emitted || !state.assistant_message_ids.contains(message_id) {
        return Vec::new();
    }
    let part_id = part
        .get("id")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let part_type = part
        .get("type")
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown");
    if part_id.is_empty() {
        return Vec::new();
    }
    match part_type {
        "text" | "reasoning" => {
            let kind = if part_type == "reasoning" {
                "reasoning"
            } else {
                "agentMessage"
            };
            let text = part
                .get("text")
                .and_then(JsonValue::as_str)
                .unwrap_or_default();
            let completed = part.pointer("/time/end").is_some();
            let existing = state.items.get(part_id).cloned();
            if existing.as_ref().is_some_and(|item| item.completed) {
                return Vec::new();
            }
            if snapshot {
                // v1.4.14 persists full text at part.updated, not at each delta.
                // Preserve the native SSE cursor while REST may be ahead of it.
                state.stream_offsets.entry(part_id.to_owned()).or_default();
            } else {
                state.stream_offsets.insert(part_id.to_owned(), text.len());
            }
            if !completed
                && existing
                    .as_ref()
                    .is_some_and(|item| item.text.len() > text.len() && item.text.starts_with(text))
            {
                return Vec::new();
            }
            let previous_text = existing
                .as_ref()
                .map(|item| item.text.as_str())
                .unwrap_or("");
            let started = existing.is_some();
            // A final plugin transform can replace text. ItemCompleted carries
            // its authoritative value; appending the whole replacement would
            // duplicate the already streamed prefix.
            let suffix = text.strip_prefix(previous_text).unwrap_or("");
            let already_completed = existing.as_ref().is_some_and(|item| item.completed);
            if !started {
                state.item_order.push(part_id.to_owned());
            }
            state.items.insert(
                part_id.to_owned(),
                OpenCodeItemState {
                    kind: kind.to_owned(),
                    text: text.to_owned(),
                    metadata: None,
                    completed,
                },
            );
            let mut events = Vec::new();
            if !started {
                events.push(RuntimeEvent::ItemStarted(RuntimeItemStarted {
                    native_thread_id: Some(native_thread_id.clone()),
                    native_turn_id: native_turn_id.clone(),
                    native_item_id: part_id.to_owned(),
                    item_kind: kind.to_owned(),
                    title: None,
                    phase: RuntimeAgentMessagePhase::FinalAnswer,
                    metadata: None,
                    native_item_redacted: None,
                    native: native.clone(),
                }));
            }
            if !suffix.is_empty() {
                events.push(RuntimeEvent::ItemDelta(RuntimeItemDelta {
                    native_thread_id: Some(native_thread_id.clone()),
                    native_turn_id: native_turn_id.clone(),
                    native_item_id: part_id.to_owned(),
                    item_kind: kind.to_owned(),
                    delta_kind: if kind == "reasoning" {
                        RuntimeItemDeltaKind::ReasoningText
                    } else {
                        RuntimeItemDeltaKind::AgentMessage
                    },
                    delta: suffix.to_owned(),
                    metadata: None,
                    native: native.clone(),
                }));
            }
            if completed && !already_completed {
                events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                    native_thread_id: Some(native_thread_id),
                    native_turn_id,
                    native_item_id: part_id.to_owned(),
                    item_kind: kind.to_owned(),
                    text: (!text.is_empty()).then(|| text.to_owned()),
                    summary: Vec::new(),
                    content: if kind == "reasoning" && !text.is_empty() {
                        vec![text.to_owned()]
                    } else {
                        Vec::new()
                    },
                    phase: RuntimeAgentMessagePhase::FinalAnswer,
                    metadata: None,
                    native_item_redacted: None,
                    native,
                }));
            }
            events
        }
        "tool" => map_opencode_tool_part(
            state,
            part_id,
            part,
            native_thread_id,
            native_turn_id,
            native,
        ),
        "patch" => map_opencode_diff(state, part, native),
        _ => Vec::new(),
    }
}

fn map_opencode_diff(
    state: &mut OpenCodeRuntimeState,
    diff: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    let Some(native_turn_id) = state.active_turn_id.clone() else {
        return Vec::new();
    };
    let diff = redact_opencode_json(diff);
    if state.terminal_emitted || state.last_diff.as_ref() == Some(&diff) {
        return Vec::new();
    }
    state.last_diff = Some(diff.clone());
    vec![RuntimeEvent::DiffUpdated(RuntimeDiffUpdated {
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id,
        diff_redacted: Some(diff),
        diff_text: None,
        revision: None,
        event_id: None,
        provider_exact: Some(false),
        native,
    })]
}

fn map_opencode_tool_part(
    state: &mut OpenCodeRuntimeState,
    part_id: &str,
    part: &JsonValue,
    native_thread_id: String,
    native_turn_id: String,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    let tool_name = part
        .get("tool")
        .and_then(JsonValue::as_str)
        .unwrap_or("tool");
    let item_kind = opencode_tool_item_kind(tool_name);
    let tool_state = part.get("state").cloned().unwrap_or_else(|| json!({}));
    let status = tool_state
        .get("status")
        .and_then(JsonValue::as_str)
        .unwrap_or("pending");
    if status == "pending" {
        return Vec::new();
    }
    let mut metadata = opencode_tool_metadata(tool_name, &tool_state, status);
    metadata["nativeCallId"] = part.get("callID").cloned().unwrap_or(JsonValue::Null);
    if metadata.get("cwd").is_none() {
        metadata["cwd"] = json!(state.cwd);
    }
    let output = tool_state
        .get("output")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let existing = state.items.get(part_id).cloned();
    if existing.as_ref().is_some_and(|item| item.completed) {
        return Vec::new();
    }
    let completed = matches!(status, "completed" | "error");
    if existing.is_none() {
        state.item_order.push(part_id.to_owned());
    }
    state.items.insert(
        part_id.to_owned(),
        OpenCodeItemState {
            kind: item_kind.clone(),
            text: output.to_owned(),
            metadata: Some(metadata.clone()),
            completed,
        },
    );
    let mut events = Vec::new();
    if existing.is_none() {
        events.push(RuntimeEvent::ItemStarted(RuntimeItemStarted {
            native_thread_id: Some(native_thread_id.clone()),
            native_turn_id: native_turn_id.clone(),
            native_item_id: part_id.to_owned(),
            item_kind: item_kind.clone(),
            title: tool_state
                .get("title")
                .and_then(JsonValue::as_str)
                .map(str::to_owned),
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some(metadata.clone()),
            native_item_redacted: None,
            native: native.clone(),
        }));
    }
    if existing
        .as_ref()
        .is_some_and(|item| item.metadata.as_ref() != Some(&metadata) || item.text != output)
        && !completed
    {
        events.push(RuntimeEvent::ItemDelta(RuntimeItemDelta {
            native_thread_id: Some(native_thread_id.clone()),
            native_turn_id: native_turn_id.clone(),
            native_item_id: part_id.to_owned(),
            item_kind: item_kind.clone(),
            delta_kind: RuntimeItemDeltaKind::ToolProgress,
            delta: output
                .strip_prefix(
                    existing
                        .as_ref()
                        .map(|item| item.text.as_str())
                        .unwrap_or(""),
                )
                .unwrap_or("")
                .to_owned(),
            metadata: Some(metadata.clone()),
            native: native.clone(),
        }));
    }
    if completed && existing.as_ref().is_none_or(|item| !item.completed) {
        events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
            native_thread_id: Some(native_thread_id),
            native_turn_id,
            native_item_id: part_id.to_owned(),
            item_kind,
            text: (!output.is_empty()).then(|| output.to_owned()),
            summary: Vec::new(),
            content: Vec::new(),
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some(metadata),
            native_item_redacted: None,
            native,
        }));
    }
    if completed && tool_name == "todowrite" && tool_state["input"]["todos"].is_array() {
        events.push(RuntimeEvent::PlanUpdated(RuntimePlanUpdated {
            native_thread_id: state.native_thread_id.clone(),
            native_turn_id: state.active_turn_id.clone().unwrap(),
            plan_redacted: Some(redact_opencode_json(&tool_state["input"]["todos"])),
            native: None,
        }));
    }
    events
}

fn map_opencode_session_status(
    state: &mut OpenCodeRuntimeState,
    properties: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    let status = properties.get("status").unwrap_or(&JsonValue::Null);
    let status_type = status
        .get("type")
        .or_else(|| properties.get("type"))
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown");
    match status_type {
        "busy" => {
            if state.user_observed {
                state.observe_execution();
            }
            if state.turn_started {
                Vec::new()
            } else if let (Some(native_thread_id), Some(native_turn_id)) =
                (state.native_thread_id.clone(), state.active_turn_id.clone())
            {
                state.turn_started = true;
                vec![RuntimeEvent::TurnStarted(RuntimeTurnStarted {
                    native_thread_id: Some(native_thread_id),
                    native_turn_id,
                    native,
                })]
            } else {
                Vec::new()
            }
        }
        "retry" => vec![RuntimeEvent::TurnRetrying(RuntimeTurnRetrying {
            native_thread_id: state.native_thread_id.clone(),
            native_turn_id: state.active_turn_id.clone(),
            message: status
                .get("message")
                .and_then(JsonValue::as_str)
                .unwrap_or("OpenCode is retrying the model request")
                .to_owned(),
            code: Some("opencode_retry".to_owned()),
            native,
        })],
        "idle" => Vec::new(),
        _ => Vec::new(),
    }
}

fn complete_opencode_turn(
    state: &mut OpenCodeRuntimeState,
    status: &str,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    if state.terminal_emitted {
        return Vec::new();
    }
    let (Some(native_thread_id), Some(native_turn_id)) =
        (state.native_thread_id.clone(), state.active_turn_id.clone())
    else {
        return Vec::new();
    };
    let mut events = Vec::new();
    for item_id in &state.item_order {
        let Some(item) = state.items.get_mut(item_id) else {
            continue;
        };
        if item.completed {
            continue;
        }
        item.completed = true;
        events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
            native_thread_id: Some(native_thread_id.clone()),
            native_turn_id: native_turn_id.clone(),
            native_item_id: item_id.clone(),
            item_kind: item.kind.clone(),
            text: (!item.text.is_empty()).then(|| item.text.clone()),
            summary: Vec::new(),
            content: if item.kind == "reasoning" && !item.text.is_empty() {
                vec![item.text.clone()]
            } else {
                Vec::new()
            },
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: item.metadata.clone(),
            native_item_redacted: None,
            native: native.clone(),
        }));
    }
    let terminal = if state.interrupted {
        RuntimeEvent::TurnInterrupted(RuntimeTurnInterrupted {
            native_thread_id: Some(native_thread_id),
            native_turn_id,
            reason: "aborted".to_owned(),
            native,
        })
    } else {
        RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
            native_thread_id: Some(native_thread_id),
            native_turn_id,
            status: status.to_owned(),
            native,
        })
    };
    state.terminal_emitted = true;
    state.awaiting_execution = false;
    state.dispatch_deadline = None;
    state.last_turn_observation = Some(CLIAgentRuntimeTurnObservation {
        status: if state.interrupted {
            CLIAgentRuntimeObservedTurnStatus::Interrupted
        } else {
            CLIAgentRuntimeObservedTurnStatus::Completed
        },
        message: None,
        reconciliation_events: events.clone(),
    });
    events.push(terminal);
    events
}

fn fail_opencode_turn(
    state: &mut OpenCodeRuntimeState,
    properties: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    if state.terminal_emitted || state.active_turn_id.is_none() {
        return Vec::new();
    }
    let error = properties.get("error").unwrap_or(properties);
    let name = error["name"].as_str().unwrap_or("OpenCodeError");
    if name == "MessageAbortedError" {
        state.interrupted = true;
    }
    if name != "ContextOverflowError" {
        state.last_error = Some(error.clone());
    }
    // RuntimeEvent::Error(retryable=false) itself terminalizes in the shared
    // projector. Keep structured evidence here and emit TurnFailed only once
    // the authoritative loop status is idle.
    vec![RuntimeEvent::Raw(RuntimeRawEvent {
        native_method: "session.error".into(),
        reason: name.to_owned(),
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone(),
        native_item_id: None,
        payload_redacted: Some(redact_opencode_json(error)),
        raw_redacted: native.and_then(|native| native.raw_redacted),
    })]
}

fn terminalize_opencode_failure(
    state: &mut OpenCodeRuntimeState,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    if state.terminal_emitted || state.active_turn_id.is_none() {
        return Vec::new();
    }
    if state.interrupted {
        return complete_opencode_turn(state, "interrupted", native);
    }
    let error = state.last_error.clone().unwrap_or_else(|| json!({"name":"ExecutionLost","data":{"message":"OpenCode is idle without a completed final response"}}));
    let message = bounded_safe_detail(
        error
            .pointer("/data/message")
            .and_then(JsonValue::as_str)
            .unwrap_or("OpenCode turn failed"),
    );
    let mut events = complete_opencode_turn(state, "failed", None);
    events.pop();
    if let Some(observation) = state.last_turn_observation.as_mut() {
        observation.status = CLIAgentRuntimeObservedTurnStatus::Failed;
        observation.message = Some(message.clone());
    }
    events.push(RuntimeEvent::TurnFailed(RuntimeTurnFailed {
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone(),
        message,
        code: error["name"].as_str().map(str::to_owned),
        native,
    }));
    events
}

fn map_opencode_permission_request(
    state: &mut OpenCodeRuntimeState,
    properties: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    let request_id = properties
        .get("id")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if request_id.is_empty()
        || state.terminal_emitted
        || state.active_turn_id.is_none()
        || state.pending_requests.contains_key(request_id)
    {
        return Vec::new();
    }
    let permission = properties
        .get("permission")
        .and_then(JsonValue::as_str)
        .unwrap_or("tool");
    let tool_name = properties
        .pointer("/metadata/tool")
        .and_then(JsonValue::as_str)
        .unwrap_or(permission);
    let metadata = properties
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let input = metadata
        .get("input")
        .cloned()
        .unwrap_or_else(|| metadata.clone());
    let request_kind = opencode_permission_request_kind(tool_name);
    let command = command_from_opencode_input(&input).or_else(|| {
        matches!(
            normalize_opencode_kind(permission).as_str(),
            "bash" | "shell"
        )
        .then(|| {
            properties
                .get("patterns")
                .and_then(JsonValue::as_array)
                .into_iter()
                .flatten()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>()
                .join(" && ")
        })
        .filter(|command| !command.is_empty())
    });
    let item_id = properties
        .pointer("/tool/callID")
        .and_then(JsonValue::as_str)
        .and_then(|call| {
            state
                .items
                .iter()
                .find(|(_, item)| {
                    item.metadata
                        .as_ref()
                        .is_some_and(|m| m["nativeCallId"] == call)
                })
                .map(|(id, _)| id.clone())
        });
    let payload = json!({
        "nativeRequestId": request_id,
        "nativeRequestIdJson": request_id,
        "toolName": tool_name,
        "command": command,
        "input": redact_opencode_json(&input),
        "title": properties.pointer("/metadata/title"),
        "description": format!("OpenCode requests permission for `{tool_name}`"),
        "patterns": properties.get("patterns").map(redact_opencode_json),
        "changedFiles": changed_files_from_opencode_input(&metadata),
        "diffPreview": metadata.get("diff").and_then(JsonValue::as_str).map(opencode_diff_preview),
        "cwd": input.get("cwd").or_else(|| input.get("workdir")).or_else(|| metadata.get("cwd")).and_then(JsonValue::as_str).or(state.cwd.as_deref()),
        "reason": metadata.get("description").or_else(|| metadata.get("title")),
        "grantRoot": metadata.get("grantRoot"),
        "threadId": state.native_thread_id,
        "turnId": state.active_turn_id,
        "itemId": item_id,
    });
    state.pending_requests.insert(
        request_id.to_owned(),
        OpenCodePendingRequestKind::Permission,
    );
    vec![RuntimeEvent::RequestOpened(RuntimeRequestOpened {
        native_request_id: request_id.to_owned(),
        native_request_id_json: Some(JsonValue::String(request_id.to_owned())),
        request_kind: request_kind.to_owned(),
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone(),
        native_item_id: item_id,
        payload_redacted: Some(payload),
        native,
    })]
}

fn map_opencode_question_request(
    state: &mut OpenCodeRuntimeState,
    properties: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    let request_id = properties
        .get("id")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if request_id.is_empty()
        || state.terminal_emitted
        || state.active_turn_id.is_none()
        || state.pending_requests.contains_key(request_id)
    {
        return Vec::new();
    }
    let questions = properties
        .get("questions")
        .and_then(JsonValue::as_array)
        .map(|questions| {
            questions
                .iter()
                .enumerate()
                .map(|(index, question)| {
                    json!({
                        "id": format!("question_{index}"),
                        "header": question.get("header"),
                        "question": question.get("question"),
                        "options": question.get("options"),
                        "multiple": question.get("multiple"),
                        "isOther": question.get("custom").and_then(JsonValue::as_bool).unwrap_or(true),
                        "isSecret": false,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let item_id = properties
        .pointer("/tool/callID")
        .and_then(JsonValue::as_str)
        .and_then(|call| {
            state
                .items
                .iter()
                .find(|(_, item)| {
                    item.metadata
                        .as_ref()
                        .is_some_and(|m| m["nativeCallId"] == call)
                })
                .map(|(id, _)| id.clone())
        });
    state
        .pending_requests
        .insert(request_id.to_owned(), OpenCodePendingRequestKind::Question);
    vec![RuntimeEvent::RequestOpened(RuntimeRequestOpened {
        native_request_id: request_id.to_owned(),
        native_request_id_json: Some(JsonValue::String(request_id.to_owned())),
        request_kind: "user_input".to_owned(),
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone(),
        native_item_id: item_id.clone(),
        payload_redacted: Some(json!({
            "questions": questions,
            "nativeRequestId": request_id,
            "nativeRequestIdJson": request_id,
            "threadId": state.native_thread_id,
            "turnId": state.active_turn_id,
            "itemId": item_id,
        })),
        native,
    })]
}

fn opencode_tool_item_kind(tool_name: &str) -> String {
    let normalized = normalize_opencode_kind(tool_name);
    if tool_name.starts_with("pioneer_") {
        "mcpToolCall"
    } else if matches!(normalized.as_str(), "bash" | "shell") {
        "commandExecution"
    } else if matches!(
        normalized.as_str(),
        "edit" | "write" | "applypatch" | "patch" | "multiedit"
    ) {
        "fileChange"
    } else if matches!(normalized.as_str(), "websearch" | "webfetch" | "codesearch") {
        "webSearch"
    } else {
        "dynamicToolCall"
    }
    .to_owned()
}

fn is_tool_item_kind(kind: &str) -> bool {
    matches!(
        normalize_opencode_kind(kind).as_str(),
        "commandexecution" | "filechange" | "websearch" | "mcptoolcall" | "dynamictoolcall"
    )
}

fn opencode_tool_metadata(tool_name: &str, state: &JsonValue, status: &str) -> JsonValue {
    let input = state.get("input").cloned().unwrap_or_else(|| json!({}));
    let output = state
        .get("output")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let error = state
        .get("error")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let mut metadata = JsonMap::new();
    metadata.insert(
        "toolName".to_owned(),
        JsonValue::String(tool_name.to_owned()),
    );
    metadata.insert(
        "providerQualifiedName".to_owned(),
        JsonValue::String(tool_name.to_owned()),
    );
    metadata.insert("arguments".to_owned(), input.clone());
    metadata.insert(
        "status".to_owned(),
        JsonValue::String(
            match status {
                "completed" => "completed",
                "error" => "failed",
                _ => "inProgress",
            }
            .to_owned(),
        ),
    );
    metadata.insert("success".to_owned(), JsonValue::Bool(status == "completed"));
    if let Some(command) = command_from_opencode_input(&input) {
        metadata.insert("command".to_owned(), json!([command]));
    }
    if let Some(cwd) = input
        .get("cwd")
        .or_else(|| input.get("workdir"))
        .and_then(JsonValue::as_str)
    {
        metadata.insert("cwd".to_owned(), JsonValue::String(cwd.to_owned()));
    }
    let mut changed_files = changed_files_from_opencode_input(&input);
    changed_files.extend(changed_files_from_opencode_input(&state["metadata"]));
    changed_files.sort();
    changed_files.dedup();
    if let Some(diff) = state["metadata"]["diff"].as_str() {
        metadata.insert("diffPreview".into(), opencode_diff_preview(diff));
    }
    if !changed_files.is_empty() {
        metadata.insert("changedFiles".to_owned(), json!(changed_files));
    }
    if let Some(output) = output {
        metadata.insert("stdout".to_owned(), JsonValue::String(output));
    }
    if let Some(error) = error {
        metadata.insert("stderr".to_owned(), JsonValue::String(error));
    }
    if let Some(exit_code) = state
        .pointer("/metadata/exit")
        .or_else(|| state.pointer("/metadata/exitCode"))
        .and_then(JsonValue::as_i64)
    {
        metadata.insert("exitCode".to_owned(), exit_code.into());
    }
    if let Some(query) = input
        .get("query")
        .or_else(|| input.get("q"))
        .and_then(JsonValue::as_str)
    {
        metadata.insert("query".to_owned(), JsonValue::String(query.to_owned()));
    }
    JsonValue::Object(metadata)
}

fn opencode_permission_request_kind(tool_name: &str) -> &'static str {
    match normalize_opencode_kind(tool_name).as_str() {
        "bash" | "shell" | "task" => "command_approval",
        "edit" | "write" | "applypatch" | "patch" | "multiedit" => "file_change_approval",
        _ => "command_approval",
    }
}

fn command_from_opencode_input(input: &JsonValue) -> Option<String> {
    input
        .get("command")
        .or_else(|| input.get("cmd"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
}

fn changed_files_from_opencode_input(input: &JsonValue) -> Vec<String> {
    let mut paths = Vec::new();
    for key in ["filePath", "filepath", "path"] {
        if input
            .get("files")
            .and_then(JsonValue::as_array)
            .is_some_and(|files| !files.is_empty())
        {
            break;
        }
        if let Some(path) = input.get(key).and_then(JsonValue::as_str) {
            paths.push(path.to_owned());
        }
    }
    if let Some(files) = input.get("files").and_then(JsonValue::as_array) {
        for file in files {
            if let Some(path) = file.as_str() {
                paths.push(path.to_owned());
            } else {
                // A move changes both the source and destination paths.
                if let Some(path) = file["filePath"]
                    .as_str()
                    .or_else(|| file["relativePath"].as_str())
                {
                    paths.push(path.to_owned());
                }
                if let Some(path) = file["movePath"].as_str() {
                    paths.push(path.to_owned());
                }
            }
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

fn opencode_diff_preview(diff: &str) -> JsonValue {
    const MAX_CHARS: usize = 32_768;
    let original_chars = diff.chars().count();
    json!({"text": diff.chars().take(MAX_CHARS).collect::<String>(), "truncated": original_chars > MAX_CHARS, "originalChars": original_chars})
}

fn normalize_opencode_kind(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn opencode_native_event(
    method: &str,
    properties: &JsonValue,
    raw: &JsonValue,
    include_native: bool,
) -> Option<RuntimeNativeEvent> {
    include_native.then(|| RuntimeNativeEvent {
        method: method.to_owned(),
        payload_redacted: Some(redact_opencode_json(properties)),
        raw_redacted: Some(redact_opencode_json(raw)),
    })
}

fn redact_opencode_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => JsonValue::Object(
            object
                .iter()
                .map(|(key, value)| {
                    let normalized = key.to_ascii_lowercase();
                    let value = if [
                        "authorization",
                        "password",
                        "token",
                        "secret",
                        "cookie",
                        "apikey",
                        "api_key",
                    ]
                    .iter()
                    .any(|sensitive| normalized.contains(sensitive))
                    {
                        JsonValue::String("[REDACTED]".to_owned())
                    } else {
                        redact_opencode_json(value)
                    };
                    (key.clone(), value)
                })
                .collect(),
        ),
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().take(512).map(redact_opencode_json).collect())
        }
        // Content and diffs preserve line boundaries. Diagnostic strings use
        // bounded_safe_detail separately; event ingress has its own admitted cap.
        JsonValue::String(value) => JsonValue::String(value.clone()),
        other => other.clone(),
    }
}

struct OpenCodePreparedMcpBridge {
    launch: CliMcpBridgeLaunch,
    projection: CliMcpFacadeProjection,
    projection_generation: CliMcpProjectionGeneration,
    canonical_manifest_hash: String,
    provider_contract_fingerprint: String,
    isolation_contract_fingerprint: String,
}

struct OpenCodeActiveMcpTurn {
    pioneer_turn_id: String,
    activation_generation: crate::cli_runtime::mcp::coordinator::CliMcpActivationGeneration,
}

enum OpenCodeRequiredMcpBridgeState {
    Pending,
    Serving {
        bound_grant: crate::cli_runtime::mcp::grants::CliMcpBoundGrant,
        handle: CliMcpBridgeFacadeHandle,
        server: JoinHandle<Result<(), crate::cli_runtime::mcp::server::CliMcpBridgeServerError>>,
    },
    Ready {
        bound_grant: crate::cli_runtime::mcp::grants::CliMcpBoundGrant,
        handle: CliMcpBridgeFacadeHandle,
        server: JoinHandle<Result<(), crate::cli_runtime::mcp::server::CliMcpBridgeServerError>>,
        active_turn: Option<OpenCodeActiveMcpTurn>,
    },
    Failed,
}

struct OpenCodeRequiredMcpBridge {
    supervisor: Arc<CliMcpBridgeSupervisor>,
    process_instance: CliSessionInstanceId,
    launch: CliMcpBridgeLaunch,
    projection: CliMcpFacadeProjection,
    projection_generation: CliMcpProjectionGeneration,
    projection_fingerprint: CliMcpProjectionFingerprint,
    canonical_manifest_hash: String,
    provider_contract_fingerprint: String,
    isolation_contract_fingerprint: String,
    invoker: Arc<dyn TurnMcpInvoker>,
    native_items: Arc<NativeMcpCorrelationLedger>,
    facade_limits: CliMcpFacadeLimits,
    state: Mutex<OpenCodeRequiredMcpBridgeState>,
}

impl OpenCodeRequiredMcpBridge {
    async fn ensure_ready(&self, readiness_timeout: Duration) -> Result<()> {
        if readiness_timeout.is_zero() {
            bail!("OpenCode required MCP readiness timeout must be non-zero");
        }
        let bound = {
            let mut state = self.state.lock().await;
            match &*state {
                OpenCodeRequiredMcpBridgeState::Ready { .. } => return Ok(()),
                OpenCodeRequiredMcpBridgeState::Failed => {
                    bail!("OpenCode required MCP bridge generation is failed")
                }
                OpenCodeRequiredMcpBridgeState::Serving { bound_grant, .. } => bound_grant.clone(),
                OpenCodeRequiredMcpBridgeState::Pending => {
                    let attachment = self
                        .supervisor
                        .await_attach(&self.process_instance, readiness_timeout)
                        .await
                        .map_err(|error| anyhow!("OpenCode MCP helper attach failed: {error}"))?;
                    let transport = self
                        .supervisor
                        .take_transport(&self.process_instance)
                        .await
                        .map_err(|error| anyhow!("OpenCode MCP transport failed: {error}"))?;
                    let (handle, server) = CliMcpBridgeFacadeServer::build(
                        transport,
                        self.supervisor.coordinator(),
                        Arc::new(CorrelatingTurnMcpInvoker {
                            inner: self.invoker.clone(),
                            native_items: self.native_items.clone(),
                        }),
                        self.projection_generation,
                        self.projection.clone(),
                        self.facade_limits.clone(),
                    )
                    .map_err(|error| anyhow!("OpenCode MCP facade build failed: {error}"))?;
                    let server = tokio::spawn(server.run());
                    let bound_grant = attachment.bound_grant;
                    *state = OpenCodeRequiredMcpBridgeState::Serving {
                        bound_grant: bound_grant.clone(),
                        handle,
                        server,
                    };
                    bound_grant
                }
            }
        };
        tokio_timeout(
            readiness_timeout,
            self.supervisor.coordinator().wait_projection_ready(
                &bound,
                self.projection_generation,
                &self.projection_fingerprint,
            ),
        )
        .await
        .map_err(|_| anyhow!("OpenCode MCP tools/list readiness timed out"))?
        .map_err(|error| anyhow!("OpenCode MCP tools/list readiness failed: {error:?}"))?;
        let mut state = self.state.lock().await;
        match std::mem::replace(&mut *state, OpenCodeRequiredMcpBridgeState::Failed) {
            OpenCodeRequiredMcpBridgeState::Serving {
                bound_grant,
                handle,
                server,
            } => {
                *state = OpenCodeRequiredMcpBridgeState::Ready {
                    bound_grant,
                    handle,
                    server,
                    active_turn: None,
                };
                Ok(())
            }
            ready @ OpenCodeRequiredMcpBridgeState::Ready { .. } => {
                *state = ready;
                Ok(())
            }
            other => {
                *state = other;
                bail!("OpenCode MCP bridge changed state before readiness")
            }
        }
    }

    async fn fail_closed(&self) {
        let mut state = self.state.lock().await;
        let previous = std::mem::replace(&mut *state, OpenCodeRequiredMcpBridgeState::Failed);
        match previous {
            OpenCodeRequiredMcpBridgeState::Serving { server, .. }
            | OpenCodeRequiredMcpBridgeState::Ready { server, .. } => server.abort(),
            OpenCodeRequiredMcpBridgeState::Pending | OpenCodeRequiredMcpBridgeState::Failed => {}
        }
        drop(state);
        self.supervisor.revoke_session(&self.process_instance).await;
    }

    async fn prepare_turn(
        &self,
        pioneer_thread_id: &str,
        pioneer_turn_id: &str,
    ) -> Result<CLIAgentRuntimeMcpTurnMetadata> {
        self.ensure_ready(Duration::from_secs(30)).await?;
        let mut state = self.state.lock().await;
        let OpenCodeRequiredMcpBridgeState::Ready { active_turn, .. } = &mut *state else {
            bail!("OpenCode MCP bridge is not ready for turn reservation")
        };
        if active_turn.is_some() {
            bail!("OpenCode MCP bridge already has a non-terminal turn lease")
        }
        let reservation = self
            .supervisor
            .coordinator()
            .reserve_turn(
                self.launch.grant_ref(),
                self.projection_generation,
                pioneer_thread_id,
                pioneer_turn_id,
            )
            .await
            .map_err(|error| anyhow!("failed to reserve OpenCode MCP turn: {error:?}"))?;
        *active_turn = Some(OpenCodeActiveMcpTurn {
            pioneer_turn_id: pioneer_turn_id.to_owned(),
            activation_generation: reservation.activation_generation,
        });
        Ok(CLIAgentRuntimeMcpTurnMetadata {
            adapter_kind: "opencode_dynamic_mcp".to_owned(),
            manifest_hash: self.canonical_manifest_hash.clone(),
            projection_fingerprint: self.projection_fingerprint.as_str().to_owned(),
            provider_contract_fingerprint: self.provider_contract_fingerprint.clone(),
            isolation_contract_fingerprint: self.isolation_contract_fingerprint.clone(),
            session_generation: self.process_instance.generation(),
            projection_activation_generation: reservation.activation_generation.get(),
        })
    }

    async fn activate_turn(
        &self,
        pioneer_turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        let OpenCodeRequiredMcpBridgeState::Ready {
            bound_grant,
            handle,
            active_turn,
            ..
        } = &mut *state
        else {
            bail!("OpenCode MCP bridge is not ready for turn activation")
        };
        let turn = active_turn
            .as_ref()
            .ok_or_else(|| anyhow!("OpenCode MCP turn was not reserved"))?;
        if turn.pioneer_turn_id != pioneer_turn_id {
            bail!("OpenCode MCP turn reservation does not match the Pioneer turn")
        }
        self.supervisor
            .coordinator()
            .activate_turn(
                bound_grant,
                turn.activation_generation,
                native_thread_id,
                native_turn_id,
            )
            .await
            .map_err(|error| anyhow!("failed to activate OpenCode MCP turn: {error:?}"))?;
        self.native_items
            .activate_scope(native_thread_id, native_turn_id);
        handle
            .set_activation(Some(turn.activation_generation))
            .await;
        Ok(())
    }

    async fn terminal_turn(&self, pioneer_turn_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let OpenCodeRequiredMcpBridgeState::Ready {
            bound_grant,
            handle,
            active_turn,
            ..
        } = &mut *state
        else {
            return Ok(());
        };
        let Some(turn) = active_turn.take() else {
            return Ok(());
        };
        if turn.pioneer_turn_id != pioneer_turn_id {
            *active_turn = Some(turn);
            bail!("OpenCode MCP terminal turn does not match the active lease")
        }
        self.supervisor
            .coordinator()
            .terminal_turn(bound_grant, turn.activation_generation)
            .await
            .map_err(|error| anyhow!("failed to terminalize OpenCode MCP turn: {error:?}"))?;
        handle.set_activation(None).await;
        self.native_items.deactivate_scope();
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct PendingOpenCodePrompt {
    native_thread_id: String,
    native_turn_id: String,
    body: JsonValue,
}

pub(crate) struct OpenCodeCLIAgentRuntimeSessionFactory {
    runtime_home: PathBuf,
    bridge_supervisor: Arc<CliMcpBridgeSupervisor>,
    mcp_limits: CliMcpRuntimeLimits,
    turn_mcp_invoker: Arc<dyn TurnMcpInvoker>,
}

impl OpenCodeCLIAgentRuntimeSessionFactory {
    pub(crate) fn new(
        runtime_home: PathBuf,
        bridge_supervisor: Arc<CliMcpBridgeSupervisor>,
        mcp_limits: CliMcpRuntimeLimits,
        turn_mcp_invoker: Arc<dyn TurnMcpInvoker>,
    ) -> Self {
        Self {
            runtime_home,
            bridge_supervisor,
            mcp_limits,
            turn_mcp_invoker,
        }
    }

    fn runtime_instance(
        &self,
        runtime_id: &str,
    ) -> Result<EffectiveGatewayCliAgentRuntimeInstanceConfig> {
        crate::cli_runtime::config::load_effective_cli_runtime_instances(
            self.runtime_home.as_path(),
        )?
        .into_iter()
        .find(|instance| instance.id == runtime_id)
        .ok_or_else(|| anyhow!("unknown CLI runtime `{runtime_id}`"))
    }
}

#[async_trait]
impl CLIAgentRuntimeSessionFactory for OpenCodeCLIAgentRuntimeSessionFactory {
    async fn start_session_with_launch_spec(
        &self,
        process_instance: &CliSessionInstanceId,
        launch_spec: &CliSessionLaunchSpec,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        let native_session_id = match &launch_spec.continuation {
            CliProviderContinuation::OpenCodeServerSession { native_session_id } => {
                native_session_id.clone()
            }
            _ => bail!("OpenCode runtime requires a typed OpenCode continuation"),
        };
        let launch_projection = match &launch_spec.mcp {
            CliMcpSessionLaunch::Disabled | CliMcpSessionLaunch::ManagementOnly => None,
            CliMcpSessionLaunch::OpenCode(projection) => Some(projection.clone()),
            CliMcpSessionLaunch::Codex(_) | CliMcpSessionLaunch::Claude(_) => {
                bail!("OpenCode runtime cannot consume another provider's MCP projection")
            }
        };
        let key = process_instance.key();
        let instance = self.runtime_instance(key.runtime_id.as_str())?;
        if !instance.enabled {
            bail!("CLI runtime `{}` is disabled", instance.id);
        }
        if instance.kind != GatewayCliAgentRuntimeKindConfig::OpenCode {
            bail!(
                "CLI runtime `{}` is not configured as OpenCode",
                instance.id
            );
        }
        if matches!(launch_spec.mcp, CliMcpSessionLaunch::ManagementOnly)
            && launch_spec.options.cwd.is_none()
        {
            bail!("OpenCode management requires the durable binding working directory");
        }
        let cwd = launch_spec
            .options
            .cwd
            .clone()
            .or_else(|| std::env::current_dir().ok())
            .ok_or_else(|| anyhow!("OpenCode runtime working directory is unavailable"))?;
        let prepared_mcp = if let Some(projection) = launch_projection.as_ref() {
            if projection.canonical_projection.tools.is_empty() {
                None
            } else {
                let facade_projection = projection
                    .facade_projection(self.mcp_limits.facade_projection_limits())
                    .map_err(|error| anyhow!("failed to build OpenCode MCP facade: {error}"))?;
                let manifest_hash =
                    CliMcpManifestHash::new(projection.canonical_projection.manifest_hash.clone())
                        .map_err(|error| anyhow!("invalid OpenCode MCP manifest: {error:?}"))?;
                let scope = CliMcpGrantScope::new(process_instance.clone(), manifest_hash);
                let launch = self
                    .bridge_supervisor
                    .prepare(
                        scope,
                        opencode_mcp_bootstrap_expiry(instance.startup_probe_timeout_ms)?,
                    )
                    .await
                    .map_err(|error| anyhow!("failed to prepare OpenCode MCP bridge: {error}"))?;
                let reservation = match self
                    .bridge_supervisor
                    .coordinator()
                    .stage_projection(launch.grant_ref(), facade_projection.fingerprint().clone())
                    .await
                {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        self.bridge_supervisor
                            .revoke_session(process_instance)
                            .await;
                        return Err(anyhow!(
                            "failed to stage OpenCode MCP projection: {error:?}"
                        ));
                    }
                };
                Some(OpenCodePreparedMcpBridge {
                    launch,
                    projection: facade_projection,
                    projection_generation: reservation.generation,
                    canonical_manifest_hash: projection.canonical_projection.manifest_hash.clone(),
                    provider_contract_fingerprint: projection.provider_contract_fingerprint.clone(),
                    isolation_contract_fingerprint: projection
                        .semantic_restart_fingerprint()
                        .to_owned(),
                })
            }
        } else {
            None
        };
        let server = match spawn_opencode_server(
            &instance,
            cwd.as_path(),
            &launch_spec.options.env,
            Some(process_instance.generation()),
            &opencode_managed_data_home(&self.runtime_home, &key.workspace_id, &instance),
        )
        .await
        {
            Ok(server) => Arc::new(server),
            Err(error) => {
                if prepared_mcp.is_some() {
                    self.bridge_supervisor
                        .revoke_session(process_instance)
                        .await;
                }
                return Err(error);
            }
        };
        let request_timeout = Duration::from_millis(instance.request_timeout_ms.max(1));
        let initialized: Result<(Option<Arc<OpenCodeRequiredMcpBridge>>, Vec<String>)> = async {
            if let Some(projection) = launch_projection.as_ref() {
                let binary = instance.binary_path.clone();
                let (actual_contract, _) = tokio::task::spawn_blocking(move || {
                    crate::cli_runtime::mcp::readiness::opencode_executable_fingerprints(
                        &binary,
                        OPENCODE_SUPPORTED_VERSION,
                    )
                })
                .await
                .context("OpenCode executable attestation worker failed")??;
                anyhow::ensure!(
                    actual_contract == projection.provider_contract_fingerprint,
                    "OpenCode executable changed since the frozen MCP readiness contract"
                );
            }
            let providers = server.api.providers(request_timeout).await?;
            let connected = providers
                .get("connected")
                .and_then(JsonValue::as_array)
                .map(|providers| !providers.is_empty())
                .unwrap_or(false);
            if !connected {
                bail!("OpenCode has no authenticated model provider");
            }
            server.api.disconnect_inherited_mcp(request_timeout).await?;
            let required_mcp_bridge = if let Some(prepared) = prepared_mcp {
                let provider_process_id = server
                    .process
                    .lock()
                    .await
                    .id()
                    .filter(|process_id| *process_id != 0)
                    .ok_or_else(|| anyhow!("OpenCode provider process identity is unavailable"))?;
                self.bridge_supervisor
                    .associate_provider_process(process_instance, provider_process_id, None)
                    .await
                    .map_err(|error| anyhow!("failed to bind OpenCode MCP process: {error}"))?;
                let projection_fingerprint = prepared.projection.fingerprint().clone();
                let bridge = Arc::new(OpenCodeRequiredMcpBridge {
                    supervisor: self.bridge_supervisor.clone(),
                    process_instance: process_instance.clone(),
                    launch: prepared.launch,
                    projection: prepared.projection,
                    projection_generation: prepared.projection_generation,
                    projection_fingerprint,
                    canonical_manifest_hash: prepared.canonical_manifest_hash,
                    provider_contract_fingerprint: prepared.provider_contract_fingerprint,
                    isolation_contract_fingerprint: prepared.isolation_contract_fingerprint,
                    invoker: self.turn_mcp_invoker.clone(),
                    native_items: Arc::new(NativeMcpCorrelationLedger::scoped()),
                    facade_limits: self.mcp_limits.facade_limits(),
                    state: Mutex::new(OpenCodeRequiredMcpBridgeState::Pending),
                });
                let helper = crate::cli_runtime::config::resolve_current_pioneer_cli_mcp_helper()?;
                let add = server.api.add_pioneer_mcp(
                    helper.as_path(),
                    bridge.launch.bootstrap_path(),
                    Duration::from_millis(instance.startup_probe_timeout_ms.max(1)),
                );
                let ready = bridge.ensure_ready(Duration::from_millis(
                    instance.startup_probe_timeout_ms.max(1),
                ));
                if let Err(error) = tokio::try_join!(add, ready) {
                    bridge.fail_closed().await;
                    return Err(error).context("OpenCode MCP initialization failed");
                }
                Some(bridge)
            } else {
                None
            };
            let tool_ids = server.api.tool_ids(request_timeout).await?;
            Ok((required_mcp_bridge, tool_ids))
        }
        .await;
        let (required_mcp_bridge, tool_ids) = match initialized {
            Ok(initialized) => initialized,
            Err(error) => {
                let _ = server.close().await;
                self.bridge_supervisor
                    .revoke_session(process_instance)
                    .await;
                return Err(error);
            }
        };
        let (event_tx, event_rx) = mpsc::channel(instance.event_channel_capacity.max(1));
        let state = Arc::new(Mutex::new(OpenCodeRuntimeState {
            native_thread_id: native_session_id,
            cwd: Some(cwd.to_string_lossy().into_owned()),
            ..Default::default()
        }));
        let cancellation = CancellationToken::new();
        if !launch_spec.native_event_budget.is_valid() {
            let _ = server.close().await;
            if let Some(bridge) = &required_mcp_bridge {
                bridge.fail_closed().await;
            }
            bail!("invalid admitted OpenCode native event budget");
        }
        let (ready_tx, ready_rx) = watch::channel(false);
        let event_stream = spawn_opencode_event_stream(
            server.clone(),
            state.clone(),
            event_tx.clone(),
            cancellation.clone(),
            instance.debug_native_events,
            launch_spec.native_event_budget,
            ready_tx,
            request_timeout,
        );
        if let Err(error) =
            wait_opencode_subscription(ready_rx.clone(), &cancellation, request_timeout).await
        {
            cancellation.cancel();
            event_stream.abort();
            if let Some(bridge) = &required_mcp_bridge {
                bridge.fail_closed().await;
            }
            let _ = server.close().await;
            return Err(error);
        }
        Ok(Arc::new(OpenCodeCLIAgentRuntimeSession {
            runtime_id: key.runtime_id.clone(),
            process_instance: process_instance.clone(),
            server,
            state,
            request_timeout,
            tool_ids,
            event_tx,
            event_receivers: std::sync::Mutex::new(Some(CLIAgentRuntimeEventReceivers {
                process_instance: process_instance.clone(),
                runtime_kind: "opencode".to_owned(),
                events: event_rx,
            })),
            event_stream: std::sync::Mutex::new(Some(event_stream)),
            cancellation,
            required_mcp_bridge,
            stream_ready: ready_rx,
            mcp_launch_projection: launch_projection,
            pending_prompt: Mutex::new(None),
            turn_prepared_for_mcp: Mutex::new(false),
        }))
    }
}

struct OpenCodeCLIAgentRuntimeSession {
    runtime_id: String,
    process_instance: CliSessionInstanceId,
    server: Arc<OpenCodeServerProcess>,
    state: Arc<Mutex<OpenCodeRuntimeState>>,
    request_timeout: Duration,
    tool_ids: Vec<String>,
    event_tx: mpsc::Sender<RuntimeEvent>,
    event_receivers: std::sync::Mutex<Option<CLIAgentRuntimeEventReceivers>>,
    event_stream: std::sync::Mutex<Option<JoinHandle<()>>>,
    cancellation: CancellationToken,
    required_mcp_bridge: Option<Arc<OpenCodeRequiredMcpBridge>>,
    stream_ready: watch::Receiver<bool>,
    mcp_launch_projection: Option<OpenCodeMcpSessionLaunchProjection>,
    pending_prompt: Mutex<Option<PendingOpenCodePrompt>>,
    turn_prepared_for_mcp: Mutex<bool>,
}

#[async_trait]
impl CLIAgentRuntimeSession for OpenCodeCLIAgentRuntimeSession {
    async fn close(&self) -> Result<()> {
        self.cancellation.cancel();
        if let Some(stream) = self
            .event_stream
            .lock()
            .expect("OpenCode event stream mutex should not be poisoned")
            .take()
        {
            stream.abort();
        }
        self.pending_prompt.lock().await.take();
        if let Some(bridge) = self.required_mcp_bridge.as_ref() {
            bridge.fail_closed().await;
        }
        self.server.close().await
    }

    async fn validate_replacement_launch(&self, launch: &CliSessionLaunchSpec) -> Result<()> {
        let CliProviderContinuation::OpenCodeServerSession {
            native_session_id: Some(id),
        } = &launch.continuation
        else {
            bail!("OpenCode replacement requires its durable session identity");
        };
        if self.state.lock().await.native_thread_id.as_deref() != Some(id.as_str()) {
            bail!("OpenCode replacement cannot change its durable session identity");
        }
        Ok(())
    }

    fn is_usable(&self) -> bool {
        !self.cancellation.is_cancelled()
            && !self.server.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn prepare_for_replacement(&self) -> Result<()> {
        if self.pending_prompt.lock().await.is_some() {
            bail!("OpenCode session has a prepared prompt that has not been activated");
        }
        let state = self.state.lock().await;
        if state.active_turn_id.is_some() && !state.terminal_emitted {
            bail!("OpenCode session has an active turn");
        }
        Ok(())
    }

    fn take_event_receivers(&self) -> Option<CLIAgentRuntimeEventReceivers> {
        self.event_receivers
            .lock()
            .expect("OpenCode event receiver mutex should not be poisoned")
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
        let model = parse_optional_opencode_model(params.model.as_deref())?;
        let permissions = opencode_permission_rules(
            params.approval_policy.as_deref(),
            self.tool_ids.as_slice(),
            self.mcp_launch_projection.as_ref(),
        );
        let raw = self.server.api.create_session(permissions, timeout).await?;
        let native_thread_id = raw
            .get("id")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("OpenCode session/create response is missing its id"))?
            .to_owned();
        validate_opencode_id(native_thread_id.as_str(), "session")?;
        let mut state = self.state.lock().await;
        state.native_thread_id = Some(native_thread_id.clone());
        state.last_model = model;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id,
            cwd: raw
                .get("directory")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .or(Some(params.cwd)),
            model: params.model,
            raw,
        })
    }

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        validate_opencode_id(native_thread_id, "session")?;
        let model = parse_optional_opencode_model(params.model.as_deref())?;
        let mut raw = self
            .server
            .api
            .get_session(native_thread_id, timeout)
            .await?;
        if raw.get("id").and_then(JsonValue::as_str) != Some(native_thread_id) {
            bail!("OpenCode resumed a different native session");
        }
        let permissions = opencode_permission_rules(
            params.approval_policy.as_deref(),
            self.tool_ids.as_slice(),
            self.mcp_launch_projection.as_ref(),
        );
        raw = self
            .server
            .api
            .update_session(
                native_thread_id,
                &json!({ "permission": permissions }),
                timeout,
            )
            .await?;
        let mut state = self.state.lock().await;
        state.native_thread_id = Some(native_thread_id.to_owned());
        state.last_model = model;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: native_thread_id.to_owned(),
            cwd: raw
                .get("directory")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .or(Some(params.cwd)),
            model: params.model,
            raw,
        })
    }

    async fn start_turn(
        &self,
        params: CLIAgentRuntimeTurnStartParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeTurnStartSnapshot> {
        validate_opencode_id(params.native_thread_id.as_str(), "session")?;
        {
            let state = self.state.lock().await;
            if state.native_thread_id.as_deref() != Some(params.native_thread_id.as_str()) {
                bail!("OpenCode turn targets a session that is not open in this adapter");
            }
            if state.active_turn_id.is_some() && !state.terminal_emitted {
                bail!("OpenCode session already has an active turn");
            }
        }
        let messages = self
            .server
            .api
            .session_messages(&params.native_thread_id, timeout)
            .await?;
        let native_turn_id = next_opencode_message_id(&messages, &self.cancellation).await?;
        let model = parse_optional_opencode_model(params.model.as_deref())?;
        let parts = opencode_prompt_parts(&params.input).await?;
        if parts.is_empty() {
            bail!("OpenCode turn input is empty");
        }
        let mut body = json!({
            "messageID": native_turn_id,
            "parts": parts,
            "system": params.elevated_instructions.text(),
        });
        if let Some((provider_id, model_id)) = model.as_ref() {
            body["model"] = json!({
                "providerID": provider_id,
                "modelID": model_id,
            });
        }
        if let Some(variant) = params
            .effort
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            body["variant"] = JsonValue::String(variant.to_owned());
        }
        let defer_for_mcp = self.required_mcp_bridge.is_some();
        if defer_for_mcp && !*self.turn_prepared_for_mcp.lock().await {
            bail!("OpenCode MCP prompt requires a reserved turn before start");
        }
        if defer_for_mcp && self.pending_prompt.lock().await.is_some() {
            bail!("OpenCode already has a deferred MCP prompt");
        }
        {
            let mut state = self.state.lock().await;
            state.begin_turn(params.native_thread_id.clone(), native_turn_id.clone());
            if let Some(model) = model {
                state.last_model = Some(model);
            }
        }
        if defer_for_mcp {
            let mut pending = self.pending_prompt.lock().await;
            debug_assert!(pending.is_none());
            *pending = Some(PendingOpenCodePrompt {
                native_thread_id: params.native_thread_id.clone(),
                native_turn_id: native_turn_id.clone(),
                body: body.clone(),
            });
        } else {
            if let Err(error) = self
                .send_prompt(
                    params.native_thread_id.as_str(),
                    native_turn_id.as_str(),
                    &body,
                    timeout,
                )
                .await
            {
                self.record_prompt_delivery_failure(
                    params.native_thread_id.as_str(),
                    native_turn_id.as_str(),
                    &error,
                )
                .await;
                return Err(error);
            }
        }
        Ok(CLIAgentRuntimeTurnStartSnapshot {
            native_thread_id: params.native_thread_id,
            native_turn_id,
            raw: json!({
                "provider": "opencode",
                "deferredForMcpActivation": defer_for_mcp,
            }),
        })
    }

    async fn prepare_mcp_turn(
        &self,
        pioneer_thread_id: &str,
        pioneer_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeMcpTurnMetadata>> {
        let Some(bridge) = self.required_mcp_bridge.as_ref() else {
            return Ok(None);
        };
        let metadata = bridge
            .prepare_turn(pioneer_thread_id, pioneer_turn_id)
            .await?;
        *self.turn_prepared_for_mcp.lock().await = true;
        Ok(Some(metadata))
    }

    async fn activate_mcp_turn(
        &self,
        pioneer_turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        if self.state.lock().await.terminal_emitted {
            bail!("OpenCode turn ended before MCP prompt activation");
        }
        if let Some(bridge) = self.required_mcp_bridge.as_ref() {
            bridge
                .activate_turn(pioneer_turn_id, native_thread_id, native_turn_id)
                .await?;
        }
        let pending = self.pending_prompt.lock().await.take();
        *self.turn_prepared_for_mcp.lock().await = false;
        if let Some(pending) = pending {
            if pending.native_thread_id != native_thread_id
                || pending.native_turn_id != native_turn_id
            {
                bail!("OpenCode deferred prompt does not match the activated MCP turn");
            }
            if let Err(error) = self
                .send_prompt(
                    native_thread_id,
                    native_turn_id,
                    &pending.body,
                    self.request_timeout,
                )
                .await
            {
                self.record_prompt_delivery_failure(native_thread_id, native_turn_id, &error)
                    .await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn terminal_mcp_turn(&self, pioneer_turn_id: &str) -> Result<()> {
        self.pending_prompt.lock().await.take();
        *self.turn_prepared_for_mcp.lock().await = false;
        if let Some(bridge) = self.required_mcp_bridge.as_ref() {
            bridge.terminal_turn(pioneer_turn_id).await?;
            let state = self.state.lock().await;
            bridge.native_items.clear_turn(
                state.native_thread_id.as_deref(),
                state.active_turn_id.as_deref(),
            );
        }
        Ok(())
    }

    fn enrich_runtime_event(&self, event: &mut RuntimeEvent) -> Result<()> {
        if let Some(projection) = self.mcp_launch_projection.as_ref() {
            projection.enrich_native_event(
                self.runtime_id.as_str(),
                self.process_instance.generation(),
                event,
            )?;
            if let Some(bridge) = &self.required_mcp_bridge {
                let binding = match event {
                    RuntimeEvent::ItemStarted(item) => item.metadata.as_ref().map(|m| {
                        (
                            &item.native_thread_id,
                            &item.native_turn_id,
                            &item.native_item_id,
                            m,
                        )
                    }),
                    RuntimeEvent::ItemDelta(item) => item.metadata.as_ref().map(|m| {
                        (
                            &item.native_thread_id,
                            &item.native_turn_id,
                            &item.native_item_id,
                            m,
                        )
                    }),
                    RuntimeEvent::ItemCompleted(item) => item.metadata.as_ref().map(|m| {
                        (
                            &item.native_thread_id,
                            &item.native_turn_id,
                            &item.native_item_id,
                            m,
                        )
                    }),
                    _ => None,
                };
                if let Some((Some(thread), turn, item, metadata)) = binding
                    && let (Some(callable), Some(fingerprint)) = (
                        metadata["canonicalCallableName"].as_str(),
                        metadata["argumentsFingerprint"].as_str(),
                    )
                {
                    bridge.native_items.register(NativeMcpItemBinding {
                        native_thread_id: thread.clone(),
                        native_turn_id: turn.clone(),
                        native_item_id: item.clone(),
                        canonical_callable_name: callable.to_owned(),
                        arguments_fingerprint: fingerprint.to_owned(),
                    })?;
                }
            }
        }
        Ok(())
    }

    async fn respond_to_request(
        &self,
        native_request_id: JsonValue,
        response: JsonValue,
    ) -> Result<()> {
        let request_id = native_request_id
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("OpenCode native request id must be a string"))?;
        let kind = self
            .state
            .lock()
            .await
            .pending_requests
            .get(request_id)
            .copied()
            .or_else(|| match response.get("kind").and_then(JsonValue::as_str) {
                Some("question") => Some(OpenCodePendingRequestKind::Question),
                Some("permission") => Some(OpenCodePendingRequestKind::Permission),
                _ => None,
            })
            .ok_or_else(|| anyhow!("unknown OpenCode native request id"))?;
        match kind {
            OpenCodePendingRequestKind::Permission => {
                let reply = response
                    .get("reply")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("reject");
                if !matches!(reply, "once" | "reject") {
                    bail!("invalid OpenCode permission reply");
                }
                self.server
                    .api
                    .permission_reply(
                        request_id,
                        reply,
                        response.get("message").and_then(JsonValue::as_str),
                        self.request_timeout,
                    )
                    .await?;
            }
            OpenCodePendingRequestKind::Question => {
                if response
                    .get("reject")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false)
                {
                    self.server
                        .api
                        .question_reject(request_id, self.request_timeout)
                        .await?;
                } else {
                    let answers = response
                        .get("answers")
                        .cloned()
                        .ok_or_else(|| anyhow!("OpenCode question response is missing answers"))?;
                    let answers: Vec<Vec<String>> = serde_json::from_value(answers)
                        .context("OpenCode question answers are invalid")?;
                    self.server
                        .api
                        .question_reply(request_id, answers.as_slice(), self.request_timeout)
                        .await?;
                }
            }
        }
        {
            let mut state = self.state.lock().await;
            if state.awaiting_execution {
                state.dispatch_deadline =
                    tokio::time::Instant::now().checked_add(self.request_timeout);
            }
        }
        self.state.lock().await.pending_requests.remove(request_id);
        Ok(())
    }

    async fn fail_request(
        &self,
        native_request_id: JsonValue,
        _code: i64,
        message: String,
        _data: Option<JsonValue>,
    ) -> Result<()> {
        let request_id = native_request_id
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("OpenCode native request id must be a string"))?;
        let kind = self
            .state
            .lock()
            .await
            .pending_requests
            .get(request_id)
            .copied()
            .ok_or_else(|| anyhow!("unknown OpenCode native request id"))?;
        let result = match kind {
            OpenCodePendingRequestKind::Permission => {
                self.server
                    .api
                    .permission_reply(
                        request_id,
                        "reject",
                        Some(message.as_str()),
                        self.request_timeout,
                    )
                    .await
            }
            OpenCodePendingRequestKind::Question => {
                self.server
                    .api
                    .question_reject(request_id, self.request_timeout)
                    .await
            }
        };
        if result.is_ok() {
            self.state.lock().await.pending_requests.remove(request_id);
        }
        result
    }

    async fn interrupt_turn(
        &self,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        if native_thread_id.is_some_and(|value| state.native_thread_id.as_deref() != Some(value))
            || native_turn_id.is_some_and(|value| state.active_turn_id.as_deref() != Some(value))
        {
            bail!("OpenCode interrupt targets a non-current turn");
        }
        if state.terminal_emitted {
            return Ok(());
        }
        state.interrupted = true;
        let before_execution = state.awaiting_execution && !state.execution_observed;
        let session_id = state.native_thread_id.clone();
        drop(state);
        let deferred = self.pending_prompt.lock().await.take().is_some();
        if deferred || before_execution {
            if !deferred {
                // Abort has no loop to cancel while prompt_async is still
                // creating its user. Closing prevents a late loop after Stop.
                self.close().await?;
            }
            let events = {
                let mut state = self.state.lock().await;
                complete_opencode_turn(&mut state, "interrupted", None)
            };
            for event in events {
                let _ = self.event_tx.send(event).await;
            }
            return Ok(());
        }
        if let Some(session_id) = session_id {
            self.server
                .api
                .abort_session(session_id.as_str(), self.request_timeout)
                .await?;
        }
        Ok(())
    }

    async fn probe_turn_liveness(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<CLIAgentRuntimeTurnLivenessProbe> {
        let active = {
            let state = self.state.lock().await;
            state.native_thread_id.as_deref() == Some(native_thread_id)
                && state.active_turn_id.as_deref() == Some(native_turn_id)
                && !state.terminal_emitted
        };
        if active {
            let statuses = self.server.api.session_status(self.request_timeout).await?;
            let status = statuses
                .get(native_thread_id)
                .and_then(|status| status.get("type"))
                .and_then(JsonValue::as_str);
            return Ok(if matches!(status, Some("busy" | "retry")) {
                CLIAgentRuntimeTurnLivenessProbe::ConfirmedActive
            } else {
                CLIAgentRuntimeTurnLivenessProbe::SnapshotRequired
            });
        }
        Ok(CLIAgentRuntimeTurnLivenessProbe::SnapshotRequired)
    }

    async fn load_turn_snapshot(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeTurnObservation>> {
        if !self.is_usable() {
            // Never substitute cached partial items for authoritative native
            // history. The shared recovery acquisition can replace this owner.
            bail!("OpenCode process generation is closed; reacquire it for native history");
        }
        let (messages, live, terminal_events) = read_opencode_turn_snapshot(
            &self.server,
            &self.state,
            native_thread_id,
            native_turn_id,
            self.request_timeout,
        )
        .await?;
        let mut observation =
            opencode_observation_from_messages(native_thread_id, native_turn_id, &messages, live);
        if !terminal_events.is_empty() {
            // The messages above were read from the live server. Use their
            // normal projection plus the explicitly proved owner cancellation;
            // do not replace their history with last_turn_observation's items.
            let state = self.state.lock().await;
            if let Some(terminal) = &state.last_turn_observation {
                if let Some(observation) = observation.as_mut() {
                    observation.status = terminal.status;
                    observation.message.clone_from(&terminal.message);
                } else {
                    observation = Some(terminal.clone());
                }
            }
        }
        if let Some(observation) = observation.as_mut() {
            let state = self.state.lock().await;
            if state.active_turn_id.as_deref() == Some(native_turn_id) && state.interrupted && !live
            {
                observation.status = CLIAgentRuntimeObservedTurnStatus::Interrupted;
                observation.message = None;
            }
        }
        if let (Some(observation), Some(projection)) =
            (&mut observation, &self.mcp_launch_projection)
        {
            for event in &mut observation.reconciliation_events {
                projection.enrich_native_event(
                    &self.runtime_id,
                    self.process_instance.generation(),
                    event,
                )?;
            }
        }
        Ok(observation)
    }

    async fn thread_compact(
        &self,
        request: CLIAgentRuntimeThreadCompactRequest,
    ) -> Result<CLIAgentRuntimeThreadCompactResult> {
        let model = self
            .state
            .lock()
            .await
            .last_model
            .clone()
            .ok_or_else(|| anyhow!("OpenCode compaction requires a selected model"))?;
        let raw = self
            .server
            .api
            .summarize(
                request.native_thread_id.as_str(),
                model.0.as_str(),
                model.1.as_str(),
                self.request_timeout,
            )
            .await?;
        Ok(CLIAgentRuntimeThreadCompactResult {
            native_thread_id: request.native_thread_id,
            raw: Some(raw),
        })
    }

    async fn set_thread_name(
        &self,
        request: CLIAgentRuntimeThreadNameSetRequest,
    ) -> Result<CLIAgentRuntimeThreadNameSetResult> {
        let raw = self
            .server
            .api
            .update_session(
                request.native_thread_id.as_str(),
                &json!({ "title": request.name }),
                self.request_timeout,
            )
            .await?;
        Ok(CLIAgentRuntimeThreadNameSetResult {
            native_thread_id: request.native_thread_id,
            raw: Some(raw),
        })
    }

    async fn fork_thread(
        &self,
        request: CLIAgentRuntimeThreadForkRequest,
    ) -> Result<CLIAgentRuntimeThreadForkResult> {
        let cutoff = if let Some(boundary) = request.last_turn_id.as_deref() {
            let messages = self
                .server
                .api
                .session_messages(&request.native_thread_id, self.request_timeout)
                .await?;
            let observation = opencode_observation_from_messages(
                &request.native_thread_id,
                boundary,
                &messages,
                false,
            )
            .context("OpenCode fork boundary is missing")?;
            if observation.status != CLIAgentRuntimeObservedTurnStatus::Completed {
                bail!("OpenCode fork boundary is not a completed turn");
            }
            let users = opencode_turn_user_ids(&messages, boundary);
            let last = messages
                .as_array()
                .into_iter()
                .flatten()
                .filter(|m| {
                    m["info"]["id"]
                        .as_str()
                        .is_some_and(|id| users.contains(id))
                        || m["info"]["parentID"]
                            .as_str()
                            .is_some_and(|parent| users.contains(parent))
                })
                .filter_map(|m| m["info"]["id"].as_str())
                .max()
                .context("OpenCode fork has no native boundary messages")?;
            // Session.fork's bound is exclusive; fixed-width ascending IDs make
            // this sentinel include the final assistant and exclude later turns.
            Some(format!("{last}0"))
        } else {
            None
        };
        let raw = self
            .server
            .api
            .fork_session(
                request.native_thread_id.as_str(),
                cutoff.as_deref(),
                self.request_timeout,
            )
            .await?;
        let native_thread_id = raw
            .get("id")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("OpenCode session/fork response is missing its id"))?
            .to_owned();
        validate_opencode_id(&native_thread_id, "fork session")?;
        if let Some(marker) = request.thread_source.as_deref() {
            self.server
                .api
                .update_session(
                    &native_thread_id,
                    &json!({"title":marker}),
                    self.request_timeout,
                )
                .await?;
        }
        if native_thread_id == request.native_thread_id {
            bail!("OpenCode fork returned the source session identity");
        }
        Ok(CLIAgentRuntimeThreadForkResult {
            native_thread_id,
            native_cwd: raw
                .get("directory")
                .and_then(JsonValue::as_str)
                .map(str::to_owned),
            native_model: self
                .state
                .lock()
                .await
                .last_model
                .as_ref()
                .map(|(provider, model)| format!("{provider}/{model}")),
            raw: Some(raw),
        })
    }
    async fn find_marked_fork(
        &self,
        source_thread_id: &str,
        _boundary_turn_id: &str,
        marker: &str,
    ) -> Result<Option<CLIAgentRuntimeThreadForkResult>> {
        let mut url = self.server.api.url_with_segments(&["session"])?;
        url.query_pairs_mut()
            .append_pair("search", marker)
            .append_pair("limit", "2");
        let sessions = self
            .server
            .api
            .json_url(
                Method::GET,
                url,
                "marked fork lookup",
                None,
                self.request_timeout,
            )
            .await?;
        let matches = sessions
            .as_array()
            .context("OpenCode session list is invalid")?
            .iter()
            .filter(|s| s["title"] == marker)
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            // Create and title PATCH are separate official operations. Absence
            // cannot prove the fork was never created; retain the existing
            // durable intent instead of discarding it and duplicating a fork.
            bail!("OpenCode marked fork outcome is unknown; durable intent is retained");
        }
        let session = matches[0];
        let id = session["id"]
            .as_str()
            .context("OpenCode marked fork identity is missing")?;
        validate_opencode_id(id, "marked fork session")?;
        if id == source_thread_id {
            bail!("OpenCode marked fork points to its source");
        }
        Ok(Some(CLIAgentRuntimeThreadForkResult {
            native_thread_id: id.to_owned(),
            native_cwd: session["directory"].as_str().map(str::to_owned),
            native_model: None,
            raw: Some(session.clone()),
        }))
    }
}

impl OpenCodeCLIAgentRuntimeSession {
    async fn send_prompt(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
        body: &JsonValue,
        timeout: Duration,
    ) -> Result<()> {
        wait_opencode_subscription(self.stream_ready.clone(), &self.cancellation, timeout).await?;
        {
            let mut state = self.state.lock().await;
            state.dispatch_deadline = Some(
                tokio::time::Instant::now()
                    .checked_add(timeout)
                    .context("OpenCode dispatch deadline exceeds the clock range")?,
            );
        }
        if let Err(error) = self
            .server
            .api
            .prompt_async(native_thread_id, body, timeout)
            .await
        {
            // An uncertain response does not stop prompt_async's detached
            // future. Close before reporting failure to prevent a late loop.
            self.close()
                .await
                .context("failed to close uncertain OpenCode dispatch")?;
            return Err(error);
        }
        let emit_started = {
            let mut state = self.state.lock().await;
            if state.turn_started || state.terminal_emitted {
                false
            } else {
                state.turn_started = true;
                true
            }
        };
        if emit_started {
            self.event_tx
                .send(RuntimeEvent::TurnStarted(RuntimeTurnStarted {
                    native_thread_id: Some(native_thread_id.to_owned()),
                    native_turn_id: native_turn_id.to_owned(),
                    native: None,
                }))
                .await
                .map_err(|_| anyhow!("OpenCode runtime event channel is closed"))?;
        }
        Ok(())
    }

    async fn record_prompt_delivery_failure(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
        error: &anyhow::Error,
    ) {
        let mut state = self.state.lock().await;
        if state.native_thread_id.as_deref() != Some(native_thread_id)
            || state.active_turn_id.as_deref() != Some(native_turn_id)
            || state.terminal_emitted
        {
            return;
        }
        let message = bounded_safe_detail(format!("{error:#}").as_str());
        state.terminal_emitted = true;
        state.last_turn_observation = Some(CLIAgentRuntimeTurnObservation {
            status: CLIAgentRuntimeObservedTurnStatus::Failed,
            message: Some(message),
            reconciliation_events: Vec::new(),
        });
    }
}

fn opencode_permission_rules(
    approval_policy: Option<&str>,
    tool_ids: &[String],
    projection: Option<&OpenCodeMcpSessionLaunchProjection>,
) -> JsonValue {
    let policy = approval_policy.unwrap_or("ask");
    let full_access = matches!(policy, "allow" | "never" | "bypassPermissions");
    let auto_edits = matches!(policy, "auto_accept_edits" | "acceptEdits");
    let builtins = OPENCODE_BUILTIN_TOOLS
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let safe = OPENCODE_SAFE_TOOLS.iter().copied().collect::<HashSet<_>>();
    let mut result = vec![json!({
        "permission": "*",
        "pattern": "*",
        "action": "deny",
    })];
    for tool_id in tool_ids {
        if !builtins.contains(tool_id.as_str()) {
            continue;
        }
        let action = if full_access
            || safe.contains(tool_id.as_str())
            || (auto_edits && matches!(tool_id.as_str(), "edit" | "write" | "apply_patch"))
        {
            "allow"
        } else {
            "ask"
        };
        result.push(json!({
            "permission": tool_id,
            "pattern": "*",
            "action": action,
        }));
    }
    result.extend([
        json!({"permission":"task", "pattern":"*", "action":"deny"}),
        json!({
            "permission": "external_directory",
            "pattern": "*",
            "action": if full_access { "allow" } else { "ask" },
        }),
        json!({
            "permission": "doom_loop",
            "pattern": "*",
            "action": if full_access { "allow" } else { "ask" },
        }),
        json!({
            "permission": "question",
            "pattern": "*",
            "action": "allow",
        }),
        json!({
            "permission": "skill",
            "pattern": "*",
            "action": "deny",
        }),
    ]);
    if let Some(projection) = projection {
        for tool in &projection.canonical_projection.tools {
            result.push(json!({
                "permission": format!("pioneer_{}", tool.canonical_callable_name),
                "pattern": "*",
                "action": "allow",
            }));
        }
    }
    JsonValue::Array(result)
}

async fn opencode_prompt_parts(input: &JsonValue) -> Result<Vec<JsonValue>> {
    let items = input
        .as_array()
        .ok_or_else(|| anyhow!("OpenCode turn input must be an array"))?;
    let mut result = Vec::with_capacity(items.len());
    for item in items {
        match item.get("type").and_then(JsonValue::as_str) {
            Some("text") => {
                if let Some(text) = item.get("text").and_then(JsonValue::as_str) {
                    result.push(json!({ "type": "text", "text": text }));
                }
            }
            Some("image") => {
                let url = item
                    .get("url")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("OpenCode image input is missing its URL"))?;
                result.push(json!({
                    "type": "file",
                    "mime": mime_for_path_or_url(url, "image/png"),
                    "url": url,
                }));
            }
            Some("localImage") => {
                let path = item
                    .get("path")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("OpenCode local image input is missing its path"))?;
                if tokio::fs::metadata(path).await?.len() > 25 * 1024 * 1024 {
                    bail!("OpenCode local image exceeds the 25 MiB adapter limit");
                }
                let bytes = tokio::fs::read(path)
                    .await
                    .with_context(|| format!("failed to read OpenCode image `{path}`"))?;
                if bytes.len() > 25 * 1024 * 1024 {
                    bail!("OpenCode local image exceeds the 25 MiB adapter limit");
                }
                let mime = mime_for_path_or_url(path, "image/png");
                let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                result.push(json!({
                    "type": "file",
                    "mime": mime,
                    "url": format!("data:{mime};base64,{encoded}"),
                    "filename": Path::new(path).file_name().and_then(|name| name.to_str()),
                }));
            }
            Some("skill") => {
                let name = item
                    .get("name")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("selected skill");
                let path = item
                    .get("path")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("OpenCode skill input is missing its path"))?;
                result.push(json!({
                    "type": "text",
                    "text": format!(
                        "Pioneer selected the skill `{name}` for this turn. Read and follow its SKILL.md at `{path}` before using it."
                    ),
                }));
            }
            Some(other) => bail!("unsupported OpenCode input item type `{other}`"),
            None => bail!("OpenCode input item is missing its type"),
        }
    }
    Ok(result)
}

fn mime_for_path_or_url(value: &str, fallback: &str) -> String {
    let path = value.split(['?', '#']).next().unwrap_or(value);
    mime_guess::from_path(path)
        .first_raw()
        .unwrap_or(fallback)
        .to_owned()
}

fn split_opencode_model(value: &str) -> Option<(String, String)> {
    let (provider, model) = value.trim().split_once('/')?;
    (!provider.is_empty() && !model.is_empty()).then(|| (provider.to_owned(), model.to_owned()))
}

fn parse_optional_opencode_model(value: Option<&str>) -> Result<Option<(String, String)>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    split_opencode_model(value).map(Some).ok_or_else(|| {
        anyhow!("OpenCode model `{value}` must use the `provider/model` identifier format")
    })
}

// Compaction creates a user message and can then replay the original user or
// add a synthetic continuation. These are one managed prompt's runLoop, not a
// new Pioneer turn. Stop at the next adapter-authored (counter-zero) user ID.
fn opencode_turn_user_ids(messages: &JsonValue, root: &str) -> HashSet<String> {
    let mut users = HashSet::from([root.to_owned()]);
    let mut inside = false;
    let mut compaction_followup = false;
    for message in messages.as_array().into_iter().flatten() {
        let Some(id) = message["info"]["id"].as_str() else {
            continue;
        };
        if id == root {
            inside = true;
            continue;
        }
        if !inside || message["info"]["role"] != "user" {
            continue;
        }
        let adapter_authored = id
            .strip_prefix("msg_")
            .and_then(|s| s.get(..12))
            .and_then(|s| u64::from_str_radix(s, 16).ok())
            .is_some_and(|n| n & 0xfff == 0);
        if adapter_authored {
            break;
        }
        let compaction = message["parts"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|p| p["type"] == "compaction" && p["auto"] == true);
        if compaction {
            users.insert(id.to_owned());
            compaction_followup = true;
        } else if compaction_followup {
            users.insert(id.to_owned());
            compaction_followup = false;
        } else {
            break;
        }
    }
    users
}

async fn accept_opencode_assistant_info(
    api: &OpenCodeApiClient,
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    info: &JsonValue,
    timeout: Duration,
) -> Result<()> {
    let Some(parent) = info["parentID"].as_str() else {
        return Ok(());
    };
    let (session, turn, known) = {
        let state = state.lock().await;
        (
            state.native_thread_id.clone(),
            state.active_turn_id.clone(),
            state.turn_user_ids.contains(parent),
        )
    };
    if known {
        return Ok(());
    }
    if let (Some(session), Some(turn)) = (session, turn) {
        if info["sessionID"] != session {
            return Ok(());
        }
        let messages = api.session_messages(&session, timeout).await?;
        let users = opencode_turn_user_ids(&messages, &turn);
        let mut state = state.lock().await;
        if state.active_turn_id.as_deref() == Some(&turn) {
            state.turn_user_ids = users;
        }
    }
    Ok(())
}

fn project_opencode_messages(
    state: &mut OpenCodeRuntimeState,
    messages: &JsonValue,
) -> Vec<RuntimeEvent> {
    let mut events = Vec::new();
    if let Some(root) = state.active_turn_id.as_deref() {
        state.turn_user_ids = opencode_turn_user_ids(messages, root);
    }
    for message in messages.as_array().into_iter().flatten() {
        let info = &message["info"];
        if info["role"] != "assistant"
            || !info["parentID"]
                .as_str()
                .is_some_and(|parent| state.turn_user_ids.contains(parent))
        {
            continue;
        }
        let Some(mid) = info["id"].as_str() else {
            continue;
        };
        state.assistant_message_ids.insert(mid.to_owned());
        for part in message["parts"].as_array().into_iter().flatten() {
            // The REST part has the same identity as message.part.updated.
            events.extend(map_opencode_part(state, &json!({"part":part}), None, true));
        }
    }
    // User.summary.diffs is the persisted per-prompt diff; session.diff is an
    // ephemeral SSE notification and cannot be replayed after a disconnect.
    if let Some(diff) = messages
        .as_array()
        .into_iter()
        .flatten()
        .find(|message| message["info"]["id"].as_str() == state.active_turn_id.as_deref())
        .and_then(|message| message.pointer("/info/summary/diffs"))
    {
        events.extend(map_opencode_diff(state, diff, None));
    }
    events
}

fn opencode_snapshot_terminal(
    state: &mut OpenCodeRuntimeState,
    messages: &JsonValue,
    live: bool,
) -> Vec<RuntimeEvent> {
    if live || state.terminal_emitted {
        return Vec::new();
    }
    let last = messages.as_array().into_iter().flatten().rev().find(|m| {
        m["info"]["role"] == "assistant"
            && m["info"]["parentID"]
                .as_str()
                .is_some_and(|parent| state.turn_user_ids.contains(parent))
    });
    if let Some(error) = last
        .and_then(|m| m["info"].get("error"))
        .filter(|e| !e.is_null())
    {
        if error["name"] == "MessageAbortedError" {
            state.interrupted = true;
        }
        state.last_error = Some(error.clone());
        return terminalize_opencode_failure(state, None);
    }
    let final_answer = last.is_some_and(|m| {
        !m["parts"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|p| p["type"] == "tool" && p["metadata"]["providerExecuted"] != true)
            && m["info"]["time"]["completed"].as_u64().is_some()
            && m["info"]["finish"]
                .as_str()
                .is_some_and(|s| !matches!(s, "tool-calls" | "unknown"))
    });
    if state.interrupted {
        complete_opencode_turn(state, "interrupted", None)
    } else if final_answer {
        complete_opencode_turn(state, "completed", None)
    } else {
        terminalize_opencode_failure(state, None)
    }
}

fn opencode_observation_from_messages(
    native_thread_id: &str,
    native_turn_id: &str,
    messages: &JsonValue,
    live: bool,
) -> Option<CLIAgentRuntimeTurnObservation> {
    if !messages
        .as_array()?
        .iter()
        .any(|m| m["info"]["id"] == native_turn_id && m["info"]["role"] == "user")
    {
        return None;
    }
    let mut state = OpenCodeRuntimeState::default();
    state.begin_turn(native_thread_id.to_owned(), native_turn_id.to_owned());
    let mut events = project_opencode_messages(&mut state, messages);
    events.extend(opencode_snapshot_terminal(&mut state, messages, live));
    let mut observation = state.last_turn_observation.unwrap();
    // Reconciliation can recreate the entire supported item lifecycle, including
    // tool steps already completed before a disconnect, in provider order.
    observation.reconciliation_events = events
        .into_iter()
        .filter(|e| {
            !matches!(
                e,
                RuntimeEvent::ItemDelta(_)
                    | RuntimeEvent::TurnCompleted(_)
                    | RuntimeEvent::TurnFailed(_)
                    | RuntimeEvent::TurnInterrupted(_)
            )
        })
        .collect();
    Some(observation)
}

// An idle read only brackets already-observed progress. In particular, saving
// the user precedes runLoop/busy in v1.4.14; an owned dispatch waiting for its
// first execution evidence is never classified as ExecutionLost by this reader.
async fn read_opencode_turn_snapshot(
    server: &OpenCodeServerProcess,
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    session: &str,
    turn: &str,
    timeout: Duration,
) -> Result<(JsonValue, bool, Vec<RuntimeEvent>)> {
    let api = &server.api;
    let mut messages = api.session_messages(session, timeout).await?;
    let mut statuses = api.session_status(timeout).await?;
    let busy = |statuses: &JsonValue| {
        statuses[session]["type"]
            .as_str()
            .is_some_and(|status| matches!(status, "busy" | "retry"))
    };
    let mut live = busy(&statuses);
    if !live {
        // A loop may have completed after the first message read. Require a
        // stable history across the idle observation, then confirm status again.
        let confirmed = api.session_messages(session, timeout).await?;
        statuses = api.session_status(timeout).await?;
        live = busy(&statuses) || messages != confirmed;
        messages = confirmed;
    }
    let expired_dispatch = {
        let mut state = state.lock().await;
        live = state.snapshot_is_live(session, turn, &messages, busy(&statuses), !live);
        state.native_thread_id.as_deref() == Some(session)
            && state.active_turn_id.as_deref() == Some(turn)
            && state.awaiting_execution
            && !state.execution_observed
            && state
                .dispatch_deadline
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            && !state.terminal_emitted
    };
    let mut terminal_events = Vec::new();
    if expired_dispatch {
        // A prompt can wait for input before entering runLoop. This is progress,
        // not a lost execution. Restore such requests instead of killing it.
        let permissions = api.json(Method::GET, "/permission", None, timeout).await?;
        let questions = api.json(Method::GET, "/question", None, timeout).await?;
        let waiting = permissions
            .as_array()
            .into_iter()
            .flatten()
            .chain(questions.as_array().into_iter().flatten())
            .any(|request| request["sessionID"] == session);
        if !waiting {
            // Recheck progress after pending API reads. This is an ordinary
            // adapter mutex, not a database lock; serialize the close and its
            // terminal evidence so no other snapshot declares loss mid-close.
            let mut state = state.lock().await;
            if state.active_turn_id.as_deref() == Some(turn)
                && state.awaiting_execution
                && !state.execution_observed
                && !state.terminal_emitted
                && state.pending_requests.is_empty()
                && state
                    .dispatch_deadline
                    .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            {
                server.close().await?;
                state.last_error.get_or_insert_with(|| json!({"name":"PromptStartupTimeout","data":{"message":"OpenCode did not enter execution before its dispatch deadline"}}));
                terminal_events = terminalize_opencode_failure(&mut state, None);
                live = false;
            }
        }
    }

    Ok((messages, live, terminal_events))
}

async fn reconcile_opencode_turn(
    server: &OpenCodeServerProcess,
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    timeout: Duration,
    pending: bool,
) -> Result<Vec<RuntimeEvent>> {
    let api = &server.api;
    let (Some(session), Some(turn)) = ({
        let state = state.lock().await;
        (state.native_thread_id.clone(), state.active_turn_id.clone())
    }) else {
        return Ok(Vec::new());
    };
    let (messages, live, terminal_events) =
        read_opencode_turn_snapshot(server, state, &session, &turn, timeout).await?;
    if !terminal_events.is_empty() {
        return Ok(terminal_events);
    }
    let permissions = if pending && live {
        api.json(Method::GET, "/permission", None, timeout).await?
    } else {
        json!([])
    };
    let questions = if pending && live {
        api.json(Method::GET, "/question", None, timeout).await?
    } else {
        json!([])
    };
    let mut state = state.lock().await;
    if state.native_thread_id.as_deref() != Some(&session)
        || state.active_turn_id.as_deref() != Some(&turn)
        || state.terminal_emitted
    {
        return Ok(Vec::new());
    }
    // A reconnect can race a prompt which has not yet reached the server.
    if !messages
        .as_array()
        .into_iter()
        .flatten()
        .any(|m| m["info"]["id"] == turn)
    {
        return Ok(if !live && state.last_error.is_some() {
            terminalize_opencode_failure(&mut state, None)
        } else {
            Vec::new()
        });
    }
    let mut events = project_opencode_messages(&mut state, &messages);
    let current_requests = permissions
        .as_array()
        .into_iter()
        .flatten()
        .chain(questions.as_array().into_iter().flatten())
        .filter(|r| r["sessionID"] == session)
        .filter_map(|r| r["id"].as_str())
        .collect::<HashSet<_>>();
    let resolved = state
        .pending_requests
        .keys()
        .filter(|id| !current_requests.contains(id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for id in resolved {
        state.pending_requests.remove(&id);
        events.push(RuntimeEvent::RequestResolved(RuntimeRequestResolved {
            native_request_id: id,
            native: None,
        }));
    }
    for request in permissions
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| r["sessionID"] == session)
    {
        events.extend(map_opencode_permission_request(&mut state, request, None));
    }
    for request in questions
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| r["sessionID"] == session)
    {
        events.extend(map_opencode_question_request(&mut state, request, None));
    }
    events.extend(opencode_snapshot_terminal(&mut state, &messages, live));
    Ok(events)
}

async fn wait_opencode_subscription(
    mut ready: watch::Receiver<bool>,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> Result<()> {
    tokio_timeout(timeout, async {
        loop {
            if *ready.borrow_and_update() {
                return Ok(());
            }
            tokio::select! {
                _ = cancellation.cancelled() => bail!("OpenCode subscription cancelled"),
                result = ready.changed() => result.context("OpenCode event reader stopped")?,
            }
        }
    })
    .await
    .context("OpenCode event subscription readiness timed out")?
}

// v1.4.14 Identifier.create: 6 low bytes of (Date.now() * 0x1000 + counter),
// then 14 base62 characters. Reserve counter zero: the server's first ID in the
// same millisecond uses counter one, so its assistant sorts after this user.
async fn next_opencode_message_id(
    messages: &JsonValue,
    cancellation: &CancellationToken,
) -> Result<String> {
    static LAST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let latest = messages
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m.pointer("/info/id").and_then(JsonValue::as_str))
        .filter_map(|id| id.strip_prefix("msg_").and_then(|s| s.get(..12)))
        .filter_map(|prefix| u64::from_str_radix(prefix, 16).ok())
        .max()
        .unwrap_or(0);
    tokio_timeout(Duration::from_secs(2), async {
        loop {
            let millis = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
            let encoded = (millis << 12) & 0xffffffffffff;
            let previous = LAST.load(std::sync::atomic::Ordering::SeqCst);
            if encoded > latest
                && encoded > previous
                && LAST
                    .compare_exchange(
                        previous,
                        encoded,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
            {
                return Ok(format!(
                    "msg_{encoded:012x}{}",
                    &uuid::Uuid::new_v4().simple().to_string()[..14]
                ));
            }
            tokio::select! {
                _ = cancellation.cancelled() => bail!("OpenCode message ID allocation cancelled"),
                _ = sleep(Duration::from_millis(1)) => {},
            }
        }
    })
    .await
    .context("OpenCode clock cannot allocate an ordered message ID")?
}

fn opencode_mcp_bootstrap_expiry(startup_timeout_ms: u64) -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis();
    let lifetime = u128::from(startup_timeout_ms.max(1)).saturating_add(30_000);
    u64::try_from(now.saturating_add(lifetime))
        .context("OpenCode MCP bootstrap expiry exceeds supported range")
}

fn validate_opencode_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || value
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')))
    {
        bail!("invalid OpenCode {label} id");
    }
    Ok(())
}

fn bounded_safe_detail(value: &str) -> String {
    const MAX_BYTES: usize = 2_048;
    let value = value.replace(['\r', '\n'], " ");
    if value.len() <= MAX_BYTES {
        return value;
    }
    let mut end = MAX_BYTES;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn effective_permission_action<'a>(rules: &'a JsonValue, permission: &str) -> &'a str {
        rules
            .as_array()
            .expect("permission rules should be an array")
            .iter()
            .rev()
            .find(|rule| {
                rule.get("permission").and_then(JsonValue::as_str) == Some(permission)
                    || rule.get("permission").and_then(JsonValue::as_str) == Some("*")
            })
            .and_then(|rule| rule.get("action"))
            .and_then(JsonValue::as_str)
            .expect("permission should have an effective action")
    }

    fn available_tool_ids() -> Vec<String> {
        OPENCODE_BUILTIN_TOOLS
            .iter()
            .map(|value| (*value).to_owned())
            .chain(std::iter::once("plugin_danger".to_owned()))
            .collect()
    }

    #[test]
    fn config_environment_keeps_custom_home_isolated() {
        let standard = Path::new("/home/pioneer/.config/opencode");
        let (xdg_config_home, extra_config_dir) =
            opencode_config_environment_roots(standard).unwrap();
        assert_eq!(xdg_config_home, Path::new("/home/pioneer/.config"));
        assert_eq!(extra_config_dir, None);

        let custom = Path::new("/tmp/pioneer-opencode-home");
        let (xdg_config_home, extra_config_dir) =
            opencode_config_environment_roots(custom).unwrap();
        assert_eq!(
            xdg_config_home,
            Path::new("/tmp/pioneer-opencode-home/.pioneer-xdg")
        );
        assert_eq!(extra_config_dir.as_deref(), Some(custom));
    }

    #[test]
    fn provider_models_include_only_connected_providers_and_preserve_capabilities() {
        let providers = json!({
            "all": [
                {
                    "id": "anthropic",
                    "name": "Anthropic",
                    "source": "config",
                    "models": {
                        "claude-sonnet": {
                            "id": "claude-sonnet-4",
                            "name": "Claude Sonnet 4",
                            "family": "claude",
                            "status": "active",
                            "variants": {"high": {}, "low": {}},
                            "capabilities": {
                                "reasoning": true,
                                "input": {"text": true, "image": true, "audio": false},
                                "output": {"text": true}
                            },
                            "limit": {"context": 200000, "output": 32000}
                        }
                    }
                },
                {
                    "id": "disconnected",
                    "name": "Disconnected",
                    "models": {"hidden": {"id": "hidden"}}
                }
            ]
        });
        let connected = HashSet::from(["anthropic".to_owned()]);

        let models = opencode_models_from_provider_response(&providers, &connected);

        assert_eq!(models.len(), 1);
        let model = &models[0];
        assert_eq!(model.id, "anthropic/claude-sonnet-4");
        assert_eq!(model.name.as_deref(), Some("Claude Sonnet 4"));
        assert_eq!(model.family.as_deref(), Some("claude"));
        assert!(model.is_custom);
        assert_eq!(model.active, Some(true));
        assert_eq!(model.effort_options, ["high", "low"]);
        assert_eq!(model.input_modalities, ["image", "text"]);
        assert_eq!(model.output_modalities, ["text"]);
        assert_eq!(model.supports_reasoning, Some(true));
        assert_eq!(model.supports_vision, Some(true));
        assert_eq!(model.max_input_tokens, Some(200_000));
        assert_eq!(model.max_output_tokens, Some(32_000));
    }

    #[test]
    fn permission_rules_are_fail_closed_and_preserve_pioneer_modes() {
        let tools = available_tool_ids();
        let supervised = opencode_permission_rules(Some("ask"), &tools, None);
        assert_eq!(effective_permission_action(&supervised, "read"), "allow");
        assert_eq!(effective_permission_action(&supervised, "bash"), "ask");
        assert_eq!(effective_permission_action(&supervised, "edit"), "ask");
        assert_eq!(
            effective_permission_action(&supervised, "plugin_danger"),
            "deny"
        );
        assert_eq!(effective_permission_action(&supervised, "skill"), "deny");
        assert_eq!(
            effective_permission_action(&supervised, "question"),
            "allow"
        );

        let auto_edits = opencode_permission_rules(Some("auto_accept_edits"), &tools, None);
        assert_eq!(effective_permission_action(&auto_edits, "edit"), "allow");
        assert_eq!(effective_permission_action(&auto_edits, "write"), "allow");
        assert_eq!(
            effective_permission_action(&auto_edits, "apply_patch"),
            "allow"
        );
        assert_eq!(effective_permission_action(&auto_edits, "bash"), "ask");

        let full_access = opencode_permission_rules(Some("never"), &tools, None);
        assert_eq!(effective_permission_action(&full_access, "bash"), "allow");
        assert_eq!(
            effective_permission_action(&full_access, "external_directory"),
            "allow"
        );
        assert_eq!(
            effective_permission_action(&full_access, "plugin_danger"),
            "deny"
        );
        assert_eq!(effective_permission_action(&full_access, "skill"), "deny");
    }

    #[test]
    fn sse_frame_parser_handles_lf_and_crlf_boundaries() {
        let lf = "data: {\"type\":\"server.heartbeat\"}\n\nremaining";
        let (frame, consumed) = take_sse_frame(lf.as_bytes()).expect("LF frame");
        assert_eq!(frame, b"data: {\"type\":\"server.heartbeat\"}");
        assert_eq!(&lf[consumed..], "remaining");

        let crlf = "data: first\r\ndata: second\r\n\r\nremaining";
        let (frame, consumed) = take_sse_frame(crlf.as_bytes()).expect("CRLF frame");
        assert_eq!(frame, b"data: first\r\ndata: second");
        assert_eq!(&crlf[consumed..], "remaining");
        assert!(take_sse_frame(b"data: incomplete").is_none());
    }

    #[test]
    fn sse_frame_parser_preserves_utf8_split_across_transport_chunks() {
        let encoded = "data: {\"text\":\"Привет\"}\n\n".as_bytes();
        let split = encoded
            .windows(2)
            .position(|window| window[0] >= 0xC0 && (window[1] & 0xC0) == 0x80)
            .expect("fixture should contain a multibyte code point")
            + 1;
        let mut pending = Vec::new();
        pending.extend_from_slice(&encoded[..split]);
        assert!(take_sse_frame(pending.as_slice()).is_none());
        pending.extend_from_slice(&encoded[split..]);
        let (frame, consumed) = take_sse_frame(pending.as_slice()).expect("complete UTF-8 frame");
        assert_eq!(
            std::str::from_utf8(frame).unwrap(),
            "data: {\"text\":\"Привет\"}"
        );
        assert_eq!(consumed, pending.len());
    }

    #[tokio::test]
    async fn event_projection_emits_turn_and_text_lifecycle_once() {
        let state = Arc::new(Mutex::new(OpenCodeRuntimeState::default()));
        state
            .lock()
            .await
            .begin_turn("session-1".to_owned(), "user-message-1".to_owned());

        let started = map_opencode_event(
            &state,
            json!({
                "type": "session.status",
                "properties": {"sessionID": "session-1", "status": {"type": "busy"}}
            }),
            false,
        )
        .await;
        assert!(matches!(started.as_slice(), [RuntimeEvent::TurnStarted(_)]));

        map_opencode_event(&state, json!({"type":"message.updated","properties":{"info":{"id":"assistant-message-1","role":"assistant","sessionID":"session-1","parentID":"user-message-1"}}}), false).await;
        let item = map_opencode_event(
            &state,
            json!({
                "type": "message.part.updated",
                "properties": {
                    "part": {
                        "id": "part-1",
                        "sessionID": "session-1",
                        "messageID": "assistant-message-1",
                        "type": "text",
                        "text": "hello",
                        "time": {"start": 1, "end": 2}
                    }
                }
            }),
            false,
        )
        .await;
        assert!(matches!(
            item.as_slice(),
            [
                RuntimeEvent::ItemStarted(_),
                RuntimeEvent::ItemDelta(_),
                RuntimeEvent::ItemCompleted(_)
            ]
        ));

        let completed = complete_opencode_turn(&mut *state.lock().await, "completed", None);
        assert!(matches!(
            completed.as_slice(),
            [RuntimeEvent::TurnCompleted(_)]
        ));
        let duplicate = map_opencode_event(
            &state,
            json!({"type": "session.idle", "properties": {"sessionID": "session-1"}}),
            false,
        )
        .await;
        assert!(duplicate.is_empty());
    }

    #[test]
    fn permission_and_question_requests_keep_native_correlation() {
        let mut state = OpenCodeRuntimeState::default();
        state.begin_turn("session-1".to_owned(), "turn-1".to_owned());

        let permission = map_opencode_permission_request(
            &mut state,
            &json!({
                "id": "permission-1",
                "sessionID": "session-1",
                "permission": "edit",
                "metadata": {
                    "tool": "edit",
                    "title": "Edit file",
                    "input": {"filePath": "/tmp/example.rs", "newString": "changed"}
                },
                "tool": {"callID": "call-1"}
            }),
            None,
        );
        let RuntimeEvent::RequestOpened(permission) = &permission[0] else {
            panic!("permission should open a canonical request");
        };
        assert_eq!(permission.native_request_id, "permission-1");
        assert_eq!(permission.request_kind, "file_change_approval");
        assert_eq!(permission.native_item_id, None); // Unknown callID is not a timeline part ID.
        assert_eq!(
            permission
                .payload_redacted
                .as_ref()
                .and_then(|payload| payload.pointer("/input/filePath"))
                .and_then(JsonValue::as_str),
            Some("/tmp/example.rs")
        );

        let question = map_opencode_question_request(
            &mut state,
            &json!({
                "id": "question-1",
                "sessionID": "session-1",
                "questions": [{
                    "header": "Choice",
                    "question": "Continue?",
                    "options": [{"label": "Yes", "description": "Continue"}],
                    "multiple": false,
                    "custom": false
                }]
            }),
            None,
        );
        let RuntimeEvent::RequestOpened(question) = &question[0] else {
            panic!("question should open a canonical request");
        };
        assert_eq!(question.native_request_id, "question-1");
        assert_eq!(question.request_kind, "user_input");
        assert_eq!(
            question
                .payload_redacted
                .as_ref()
                .and_then(|payload| payload.pointer("/questions/0/question"))
                .and_then(JsonValue::as_str),
            Some("Continue?")
        );
    }

    #[test]
    fn model_identifiers_mime_and_message_observation_are_strict() {
        assert_eq!(
            split_opencode_model("anthropic/claude-sonnet"),
            Some(("anthropic".to_owned(), "claude-sonnet".to_owned()))
        );
        assert!(parse_optional_opencode_model(Some("claude-sonnet")).is_err());
        assert_eq!(
            mime_for_path_or_url(
                "https://example.test/image.webp?token=secret#view",
                "image/png"
            ),
            "image/webp"
        );

        let observation = opencode_observation_from_messages(
            "session-1",
            "user-message-1",
            &json!([
                {"info": {"id": "user-message-1", "role": "user"}, "parts": []},
                {
                    "info": {
                        "id": "assistant-message-1",
                        "role": "assistant",
                        "parentID": "user-message-1",
                        "time": {"completed": 2}, "finish":"stop"
                    },
                    "parts": [{"id": "part-1", "messageID":"assistant-message-1", "sessionID":"session-1", "type": "text", "text": "done", "time":{"end":2}}]
                }
            ]),
            false,
        )
        .expect("turn should be observable");
        assert_eq!(
            observation.status,
            CLIAgentRuntimeObservedTurnStatus::Completed
        );
        assert!(matches!(
            observation.reconciliation_events.as_slice(),
            [RuntimeEvent::ItemStarted(_), RuntimeEvent::ItemCompleted(item)] if item.text.as_deref() == Some("done")
        ));
    }
    #[tokio::test]
    async fn message_ids_sort_between_previous_messages_and_server_counter_one() {
        let cancellation = CancellationToken::new();
        let user = next_opencode_message_id(&json!([]), &cancellation)
            .await
            .unwrap();
        let time_counter = u64::from_str_radix(&user[4..16], 16).unwrap();
        assert_eq!(time_counter & 0xfff, 0);
        let assistant = format!("msg_{:012x}00000000000000", time_counter + 1);
        assert!(user < assistant);
        let next = next_opencode_message_id(&json!([{"info":{"id":assistant}}]), &cancellation)
            .await
            .unwrap();
        assert!(assistant < next);
    }

    fn recovery_messages(final_answer: bool) -> JsonValue {
        json!([
            {"info":{"id":"user","role":"user"},"parts":[]},
            {"info":{"id":"assistant-tool","role":"assistant","parentID":"user","time":{"completed":2},"finish":"tool-calls"},
             "parts":[{"id":"tool-part","messageID":"assistant-tool","sessionID":"session","type":"tool","tool":"bash","callID":"model-call",
                 "state":{"status":"completed","input":{"command":"pwd"},"output":"/workspace","time":{"start":1,"end":2}}}]},
            {"info":{"id":"assistant-final","role":"assistant","parentID":"user","time":{"completed":if final_answer {json!(3)} else {JsonValue::Null}},"finish":if final_answer {json!("stop")} else {JsonValue::Null}},
             "parts":[{"id":"text-part","messageID":"assistant-final","sessionID":"session","type":"text","text":"answer"}]}
        ])
    }

    #[test]
    fn authoritative_snapshot_requires_idle_final_and_retains_all_tool_steps() {
        let messages = recovery_messages(false);
        assert_eq!(
            opencode_observation_from_messages("session", "user", &messages, true)
                .unwrap()
                .status,
            CLIAgentRuntimeObservedTurnStatus::InProgress
        );
        assert_eq!(
            opencode_observation_from_messages("session", "user", &messages, false)
                .unwrap()
                .status,
            CLIAgentRuntimeObservedTurnStatus::Failed
        );
        let observation =
            opencode_observation_from_messages("session", "user", &recovery_messages(true), false)
                .unwrap();
        assert_eq!(
            observation.status,
            CLIAgentRuntimeObservedTurnStatus::Completed
        );
        assert!(observation.reconciliation_events.iter().any(|event| matches!(event, RuntimeEvent::ItemCompleted(item) if item.native_item_id == "tool-part" && item.metadata.as_ref().unwrap()["stdout"] == "/workspace")));
        assert!(
            !observation
                .reconciliation_events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::ItemDelta(_)))
        );
    }

    #[test]
    fn snapshot_recovers_persisted_turn_diff_through_the_live_mapper() {
        let mut messages = recovery_messages(true);
        messages[0]["info"]["summary"] = json!({"diffs":[{"file":"a.rs","before":"a\nb","after":"a\nc","additions":1,"deletions":1}]});
        let observation =
            opencode_observation_from_messages("session", "user", &messages, false).unwrap();
        assert!(observation.reconciliation_events.iter().any(|event|
            matches!(event, RuntimeEvent::DiffUpdated(diff) if diff.diff_redacted.as_ref().is_some_and(|value| value[0]["before"] == "a\nb"))));
    }

    #[test]
    fn abort_and_compaction_errors_wait_for_authoritative_lifecycle() {
        let mut state = OpenCodeRuntimeState::default();
        state.begin_turn("session".into(), "user".into());
        let error =
            json!({"error":{"name":"ContextOverflowError","data":{"message":"context full"}}});
        assert!(matches!(
            fail_opencode_turn(&mut state, &error, None).as_slice(),
            [RuntimeEvent::Raw(_)]
        ));
        assert!(!state.terminal_emitted);
        let error = json!({"error":{"name":"MessageAbortedError","data":{"message":"aborted"}}});
        fail_opencode_turn(&mut state, &error, None);
        assert!(!state.terminal_emitted);
        let terminal = opencode_snapshot_terminal(&mut state, &recovery_messages(false), false);
        assert!(matches!(
            terminal.last(),
            Some(RuntimeEvent::TurnInterrupted(_))
        ));
        assert!(
            opencode_snapshot_terminal(&mut state, &recovery_messages(false), false).is_empty()
        );
    }

    #[test]
    fn empty_text_start_delta_fields_and_completed_turn_attribution_are_independent() {
        let mut state = OpenCodeRuntimeState::default();
        state.begin_turn("session".into(), "user".into());
        state.assistant_message_ids.insert("assistant".into());
        let part = json!({"part":{"id":"part","messageID":"assistant","type":"text","text":""}});
        assert!(matches!(
            map_opencode_part_updated(&mut state, &part, None).as_slice(),
            [RuntimeEvent::ItemStarted(_)]
        ));
        let delta = json!({"partID":"part","messageID":"assistant","field":"text","delta":"hello"});
        assert!(matches!(
            map_opencode_part_delta(&mut state, &delta, None).as_slice(),
            [RuntimeEvent::ItemDelta(_)]
        ));
        let mut invalid = delta.clone();
        invalid["field"] = json!("other");
        assert!(map_opencode_part_delta(&mut state, &invalid, None).is_empty());
        invalid["field"] = json!("text");
        invalid["messageID"] = json!("previous-assistant");
        assert!(map_opencode_part_delta(&mut state, &invalid, None).is_empty());
        state.terminal_emitted = true;
        assert!(map_opencode_part_delta(&mut state, &delta, None).is_empty());
    }

    #[test]
    fn snapshot_idle_requires_progress_and_a_stable_post_idle_history() {
        let mut state = OpenCodeRuntimeState::default();
        state.begin_turn("session".into(), "user".into());
        // Before dispatch and between saving user and entering busy, idle is
        // not a completion acknowledgement of this adapter-owned prompt.
        assert!(state.snapshot_is_live("session", "user", &json!([]), false, true));
        let user = json!([{"info":{"id":"user","role":"user"},"parts":[]}]);
        assert!(state.snapshot_is_live("session", "user", &user, false, true));
        assert!(state.snapshot_is_live("session", "user", &user, true, false));
        state.awaiting_execution = false;
        // Changed history across idle still requires another observation.
        assert!(state.snapshot_is_live("session", "user", &recovery_messages(true), false, false));
        assert!(!state.snapshot_is_live("session", "user", &recovery_messages(true), false, true));
        assert!(!state.snapshot_is_live("session", "user", &user, false, true));
        // A replacement process never owns the former prompt future. Stable
        // idle with a saved user and no assistant is genuinely lost execution.
        let mut replacement = OpenCodeRuntimeState::default();
        assert!(!replacement.snapshot_is_live("session", "user", &user, false, true));
        assert_eq!(
            opencode_observation_from_messages("session", "user", &user, false)
                .unwrap()
                .status,
            CLIAgentRuntimeObservedTurnStatus::Failed
        );
    }

    #[test]
    fn restored_prefix_deduplicates_queued_deltas_then_resumes_text_and_reasoning() {
        for kind in ["text", "reasoning"] {
            let mut state = OpenCodeRuntimeState::default();
            state.begin_turn("session".into(), "user".into());
            state.assistant_message_ids.insert("assistant".into());
            let part = |text: &str, end: bool| {
                let mut value = json!({"part":{"id":"part","type":kind,"messageID":"assistant","sessionID":"session","text":text,"time":{"start":1}}});
                if end {
                    value["part"]["time"]["end"] = json!(2);
                }
                value
            };
            let initial = map_opencode_part_updated(&mut state, &part("prefix ", false), None);
            assert_eq!(
                initial
                    .iter()
                    .filter(|e| matches!(e, RuntimeEvent::ItemStarted(_)))
                    .count(),
                1
            );
            let restored = map_opencode_part(&mut state, &part("prefix old ", false), None, true);
            assert!(
                !restored
                    .iter()
                    .any(|e| matches!(e, RuntimeEvent::ItemStarted(_)))
            );
            let delta =
                |text| json!({"messageID":"assistant","partID":"part","field":"text","delta":text});
            assert!(map_opencode_part_delta(&mut state, &delta("old "), None).is_empty());
            let next = map_opencode_part_delta(&mut state, &delta("new"), None);
            assert!(
                matches!(next.as_slice(), [RuntimeEvent::ItemDelta(value)] if value.delta == "new")
            );
            let final_events =
                map_opencode_part_updated(&mut state, &part("prefix old new", true), None);
            assert!(
                matches!(final_events.as_slice(), [RuntimeEvent::ItemCompleted(value)] if value.text.as_deref() == Some("prefix old new"))
            );
            if kind == "reasoning" {
                assert!(
                    matches!(final_events.as_slice(), [RuntimeEvent::ItemCompleted(value)] if value.content == vec!["prefix old new"])
                );
            }
            assert!(map_opencode_part_delta(&mut state, &delta("late"), None).is_empty());
            let mut trim_start = part("tail ", false);
            trim_start["part"]["id"] = json!("trim");
            map_opencode_part_updated(&mut state, &trim_start, None);
            let mut trim_end = part("tail", true);
            trim_end["part"]["id"] = json!("trim");
            assert!(
                matches!(map_opencode_part_updated(&mut state, &trim_end, None).as_slice(),
                [RuntimeEvent::ItemCompleted(value)] if value.text.as_deref() == Some("tail"))
            );
            let mut next_part = part("", false);
            next_part["part"]["id"] = json!("next");
            assert!(matches!(
                map_opencode_part_updated(&mut state, &next_part, None).as_slice(),
                [RuntimeEvent::ItemStarted(_)]
            ));
        }
    }

    #[test]
    fn native_task_is_denied_and_patch_metadata_preserves_files_and_diff_lines() {
        let rules = opencode_permission_rules(Some("allow"), &available_tool_ids(), None);
        assert_eq!(effective_permission_action(&rules, "task"), "deny");
        let metadata = opencode_tool_metadata(
            "apply_patch",
            &json!({"input":{"patchText":"patch"},"metadata":{"files":[{"filePath":"a.rs"},{"filePath":"b.rs","movePath":"c.rs"}],"diff":"--- a\n+++ b\n"}}),
            "completed",
        );
        assert_eq!(metadata["changedFiles"], json!(["a.rs", "b.rs", "c.rs"]));
        assert_eq!(metadata["diffPreview"]["text"], "--- a\n+++ b\n");
        assert_eq!(metadata["diffPreview"]["truncated"], false);
    }
    #[test]
    fn compaction_followups_stay_in_the_root_turn_but_a_later_prompt_does_not() {
        let messages = json!([
            {"info":{"id":"root","role":"user"}},
            {"info":{"id":"compact-user","role":"user"},"parts":[{"type":"compaction","auto":true}]},
            {"info":{"id":"compact-assistant","role":"assistant","parentID":"compact-user","time":{"completed":2},"finish":"stop"},"parts":[]},
            {"info":{"id":"followup","role":"user"},"parts":[{"type":"text","synthetic":true,"metadata":{"compaction_continue":true}}]},
            {"info":{"id":"final-assistant","role":"assistant","parentID":"followup","time":{"completed":3},"finish":"stop"},"parts":[{"id":"answer","messageID":"final-assistant","sessionID":"session","type":"text","text":"continued"}]},
            {"info":{"id":"msg_10000000100000000000000000","role":"user"},"parts":[]},
            {"info":{"id":"later-assistant","role":"assistant","parentID":"msg_10000000100000000000000000","time":{"completed":4},"finish":"stop"},"parts":[]}
        ]);
        let observation =
            opencode_observation_from_messages("session", "root", &messages, false).unwrap();
        assert_eq!(
            observation.status,
            CLIAgentRuntimeObservedTurnStatus::Completed
        );
        assert!(observation.reconciliation_events.iter().any(|e| matches!(e, RuntimeEvent::ItemCompleted(item) if item.text.as_deref() == Some("continued"))));
        let users = opencode_turn_user_ids(&messages, "root");
        assert!(users.contains("followup"));
        assert!(!users.contains("msg_10000000100000000000000000"));
    }
}
