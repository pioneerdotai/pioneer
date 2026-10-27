//! OpenCode server adapter.
//!
//! A Pioneer session owns one authenticated loopback `opencode serve --stdio`
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
    RuntimeNativeEvent, RuntimeRequestOpened, RuntimeRequestResolved, RuntimeTurnCompleted,
    RuntimeTurnFailed, RuntimeTurnInterrupted, RuntimeTurnRetrying, RuntimeTurnStarted,
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
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::ChildStdin;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout as tokio_timeout, timeout_at};
use tokio_util::sync::CancellationToken;

const OPENCODE_SERVER_USERNAME: &str = "opencode";
// Fingerprint of Pioneer's native V2 projection, independent of binary release.
const OPENCODE_ADAPTER_CONTRACT: &str = "pioneer.opencode.server-v2";
// Permission actions from V2 built-in registrations. edit/write/patch share
// the `edit` action. Unknown/plugin/MCP tools stay denied by default.
const OPENCODE_BUILTIN_ACTIONS: &[&str] = &[
    "shell",
    "read",
    "glob",
    "grep",
    "edit",
    "webfetch",
    "websearch",
    "question",
];
const OPENCODE_SAFE_ACTIONS: &[&str] =
    &["read", "glob", "grep", "webfetch", "websearch", "question"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenCodeProbeStatus {
    Ready,
    NeedsAuth,
    MissingBinary,
    SpawnFailed,
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
    catalog_rpc_id: String,
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
        self.authenticated_request(method, format!("{}/api{}", self.base_url, path))
    }

    fn authenticated_request(
        &self,
        method: Method,
        url: impl reqwest::IntoUrl,
    ) -> reqwest::RequestBuilder {
        self.client
            .request(method, url)
            .basic_auth(self.username.as_str(), Some(self.password.expose_secret()))
            .query(&[("location[directory]", self.directory.as_str())])
    }

    fn url_with_segments(&self, segments: &[&str]) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(format!("{}/", self.base_url).as_str())
            .context("invalid managed OpenCode server URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("managed OpenCode server URL cannot contain path segments"))?
            .push("api")
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
        let response = tokio_timeout(request_timeout, request.timeout(request_timeout).send())
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
            let error = serde_json::from_slice::<JsonValue>(&bytes).ok();
            let tag = error
                .as_ref()
                .and_then(|e| e["_tag"].as_str())
                .unwrap_or("InvalidResponse");
            let message = error
                .as_ref()
                .and_then(|e| e["message"].as_str())
                .unwrap_or("The server did not return a V2 error object");
            bail!(
                "OpenCode V2 `{label}` returned HTTP {status} ({tag}): {}",
                bounded_safe_detail(message)
            );
        }
        if bytes.is_empty() {
            return Ok(JsonValue::Null);
        }
        serde_json::from_slice(&bytes)
            .with_context(|| format!("OpenCode `{label}` returned invalid JSON"))
    }

    async fn data(
        &self,
        method: Method,
        path: &str,
        body: Option<&JsonValue>,
        timeout: Duration,
    ) -> Result<JsonValue> {
        let response = self.json(method, path, body, timeout).await?;
        response
            .get("data")
            .cloned()
            .with_context(|| format!("OpenCode V2 `{path}` response is missing data"))
    }

    async fn health(&self, timeout: Duration) -> Result<JsonValue> {
        self.json(Method::GET, "/info", None, timeout).await
    }

    async fn providers(&self, timeout: Duration) -> Result<JsonValue> {
        self.data(Method::GET, "/provider", None, timeout).await
    }

    async fn models(&self, timeout: Duration) -> Result<JsonValue> {
        self.data(Method::GET, "/model", None, timeout).await
    }

    async fn create_session(
        &self,
        permissions: JsonValue,
        model: Option<JsonValue>,
        timeout: Duration,
    ) -> Result<JsonValue> {
        self.data(
            Method::POST,
            "/session",
            Some(&json!({
                "permissions": permissions, "location": {"directory": self.directory}, "model":model
            })),
            timeout,
        )
        .await
    }

    async fn get_session(&self, id: &str, timeout: Duration) -> Result<JsonValue> {
        validate_opencode_id(id, "session")?;
        self.data(Method::GET, &format!("/session/{id}"), None, timeout)
            .await
    }

    async fn update_session(
        &self,
        id: &str,
        body: &JsonValue,
        timeout: Duration,
    ) -> Result<JsonValue> {
        validate_opencode_id(id, "session")?;
        // PATCH is 204 in V2. The authoritative snapshot is a separate GET.
        self.json(
            Method::PATCH,
            &format!("/session/{id}"),
            Some(body),
            timeout,
        )
        .await?;
        self.get_session(id, timeout).await
    }

    async fn fork_session(
        &self,
        id: &str,
        before: Option<&str>,
        timeout: Duration,
    ) -> Result<JsonValue> {
        validate_opencode_id(id, "session")?;
        let body = before
            .map(|id| json!({"before":id}))
            .unwrap_or_else(|| json!({}));
        self.data(
            Method::POST,
            &format!("/session/{id}/fork"),
            Some(&body),
            timeout,
        )
        .await
    }

    async fn abort_session(&self, id: &str, timeout: Duration) -> Result<()> {
        validate_opencode_id(id, "session")?;
        let response = self
            .json(
                Method::POST,
                &format!("/session/{id}/interrupt?resume=false"),
                None,
                timeout,
            )
            .await?;
        anyhow::ensure!(
            response["interrupted"].is_boolean(),
            "OpenCode V2 interrupt response is invalid"
        );
        Ok(())
    }

    async fn session_status(&self, timeout: Duration) -> Result<JsonValue> {
        self.data(Method::GET, "/session/active", None, timeout)
            .await
    }

    async fn session_messages(&self, id: &str, timeout: Duration) -> Result<JsonValue> {
        validate_opencode_id(id, "session")?;
        let deadline = tokio::time::Instant::now() + timeout;
        let mut messages = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        let mut bytes = 0usize;
        loop {
            let mut url = self.url_with_segments(&["session", id, "message"])?;
            url.query_pairs_mut().append_pair("limit", "200");
            if let Some(cursor) = &cursor {
                url.query_pairs_mut().append_pair("cursor", cursor);
            } else {
                url.query_pairs_mut().append_pair("order", "asc");
            }
            let page = self
                .json_url(
                    Method::GET,
                    url,
                    "session messages",
                    None,
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await?;
            let data = page["data"]
                .as_array()
                .context("OpenCode V2 message page is invalid")?;
            bytes = bytes.saturating_add(serde_json::to_vec(data)?.len());
            anyhow::ensure!(
                bytes <= pioneer_cli_agent_runtime::DEFAULT_MAX_RECOVERY_FRAME_BYTES,
                "OpenCode V2 history exceeds the admitted recovery capacity"
            );
            messages.extend(data.iter().cloned());
            cursor = page
                .pointer("/cursor/next")
                .and_then(JsonValue::as_str)
                .map(str::to_owned);
            let Some(next) = &cursor else {
                break;
            };
            anyhow::ensure!(
                seen.insert(next.clone()),
                "OpenCode V2 returned a repeated history cursor"
            );
        }
        Ok(json!(messages))
    }

    async fn prompt(&self, id: &str, body: &JsonValue, timeout: Duration) -> Result<()> {
        validate_opencode_id(id, "session")?;
        let admitted = self
            .data(
                Method::POST,
                &format!("/session/{id}/prompt"),
                Some(body),
                timeout,
            )
            .await?;
        anyhow::ensure!(
            admitted["id"] == body["id"],
            "OpenCode V2 admitted a different prompt identity"
        );
        Ok(())
    }

    async fn configure_turn(
        &self,
        id: &str,
        instructions: &str,
        model: Option<&JsonValue>,
        timeout: Duration,
    ) -> Result<()> {
        validate_opencode_id(id, "session")?;
        if let Some(model) = model {
            self.json(
                Method::POST,
                &format!("/session/{id}/model"),
                Some(&json!({"model":model})),
                timeout,
            )
            .await?;
        }
        // V2 host instruction entries are rendered in the system context and
        // replaced by key on later turns, rather than appended to user text.
        self.json(
            Method::PUT,
            &format!("/experimental/session/{id}/instructions/entries/pioneer"),
            Some(&json!({"value":format!("{instructions}\n\nRun native OpenCode shell commands in the foreground. Use Pioneer task tools for background work and delegation.")})),
            timeout,
        )
        .await?;
        Ok(())
    }

    async fn inbox(&self, id: &str, timeout: Duration) -> Result<JsonValue> {
        self.data(Method::GET, &format!("/session/{id}/inbox"), None, timeout)
            .await
    }

    async fn cancel_input(&self, session: &str, id: &str, timeout: Duration) -> Result<()> {
        let inbox = self.inbox(session, timeout).await?;
        for item in inbox
            .as_array()
            .context("OpenCode V2 inbox response is invalid")?
        {
            if item["id"] == id {
                self.json(
                    Method::DELETE,
                    &format!("/session/{session}/inbox/{id}"),
                    None,
                    timeout,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn disconnect_inherited_mcp(&self, timeout: Duration) -> Result<()> {
        let servers = self.data(Method::GET, "/mcp", None, timeout).await?;
        for server in servers
            .as_array()
            .context("OpenCode V2 MCP list is invalid")?
        {
            let name = server["name"]
                .as_str()
                .context("OpenCode V2 MCP name is missing")?;
            // Remove the runtime entry, not just its connection; config reload
            // must not quietly re-enable an inherited ambient catalog.
            let url = self.url_with_segments(&["experimental", "mcp", name])?;
            self.json_url(Method::DELETE, url, "remove ambient MCP", None, timeout)
                .await?;
        }
        Ok(())
    }

    async fn add_pioneer_mcp(
        &self,
        helper: &Path,
        bootstrap: &Path,
        timeout: Duration,
    ) -> Result<JsonValue> {
        let url = self.url_with_segments(&["experimental", "mcp", OPENCODE_MCP_SERVER_NAME])?;
        self.json_url(Method::PUT, url, "attach Pioneer MCP", Some(&json!({"config":{
            "type":"local", "command":[helper.to_string_lossy(),
                pioneer_cli_mcp_bridge::helper::HIDDEN_HELPER_COMMAND,
                pioneer_cli_mcp_bridge::helper::BOOTSTRAP_FILE_FLAG, bootstrap.to_string_lossy()],
            "disabled":false, "codemode":false,
            "timeout":{"startup":timeout.as_millis().min(u128::from(u32::MAX)) as u64,
                "catalog":timeout.as_millis().min(u128::from(u32::MAX)) as u64,
                "execution":crate::cli_runtime::mcp::limits::MANAGED_DURABLE_TOOL_TIMEOUT_SECS * 1_000}
        }})), timeout).await?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let servers = self
                .data(
                    Method::GET,
                    "/mcp",
                    None,
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await?;
            let values = servers
                .as_array()
                .context("OpenCode V2 MCP list is invalid")?;
            anyhow::ensure!(
                values.iter().all(|s| s["name"] == OPENCODE_MCP_SERVER_NAME),
                "OpenCode V2 ambient MCP catalog reappeared"
            );
            if let Some(server) = values
                .iter()
                .find(|s| s["name"] == OPENCODE_MCP_SERVER_NAME)
            {
                match server["status"]["status"].as_str() {
                    Some("connected") => return Ok(servers),
                    Some("failed" | "needs_auth" | "disabled") => bail!(
                        "OpenCode V2 Pioneer MCP failed to connect: {}",
                        server["status"]
                    ),
                    _ => {}
                }
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "OpenCode V2 Pioneer MCP catalog connection timed out"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn ensure_mcp_isolation(&self, required: bool, timeout: Duration) -> Result<()> {
        let servers = self.data(Method::GET, "/mcp", None, timeout).await?;
        let mut owned = false;
        for server in servers
            .as_array()
            .context("OpenCode V2 MCP list is invalid")?
        {
            let name = server["name"]
                .as_str()
                .context("OpenCode V2 MCP server has no name")?;
            if required && name == OPENCODE_MCP_SERVER_NAME {
                anyhow::ensure!(
                    server["status"]["status"] == "connected",
                    "OpenCode V2 Pioneer MCP catalog connection was lost"
                );
                owned = true;
            } else {
                self.json_url(
                    Method::DELETE,
                    self.url_with_segments(&["experimental", "mcp", name])?,
                    "remove ambient MCP",
                    None,
                    timeout,
                )
                .await?;
            }
        }
        anyhow::ensure!(
            !required || owned,
            "OpenCode V2 Pioneer MCP server is missing"
        );
        Ok(())
    }

    async fn catalog(&self, timeout: Duration) -> Result<JsonValue> {
        let url = self.url_with_segments(&["rpc", &self.catalog_rpc_id, "catalog"])?;
        let response = self
            .json_url(
                Method::POST,
                url,
                "Pioneer V2 catalog attestation",
                Some(&json!({"input":{}})),
                timeout,
            )
            .await?;
        response
            .get("output")
            .filter(|v| v.is_array())
            .cloned()
            .context("OpenCode V2 catalog attestation response is invalid")
    }

    async fn verify_catalog(
        &self,
        projection: Option<&OpenCodeMcpSessionLaunchProjection>,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let catalog = self
                .catalog(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await?;
            if opencode_catalog_matches(&catalog, projection)? {
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "OpenCode V2 effective tool catalog does not match the frozen Pioneer projection (names, schemas or CodeMode)"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn permission_reply(
        &self,
        session: &str,
        id: &str,
        reply: &str,
        message: Option<&str>,
        timeout: Duration,
    ) -> Result<()> {
        validate_opencode_id(session, "session")?;
        validate_opencode_id(id, "permission request")?;
        let mut body = json!({"decision":reply});
        if let Some(message) = message {
            body["message"] = json!(message);
        }
        self.json(
            Method::POST,
            &format!("/session/{session}/permission/{id}/reply"),
            Some(&body),
            timeout,
        )
        .await?;
        Ok(())
    }

    async fn form_reply(
        &self,
        session: &str,
        id: &str,
        answer: &JsonValue,
        timeout: Duration,
    ) -> Result<()> {
        validate_opencode_id(session, "session")?;
        validate_opencode_id(id, "form")?;
        self.json(
            Method::POST,
            &format!("/session/{session}/form/{id}/reply"),
            Some(&json!({"answer":answer})),
            timeout,
        )
        .await?;
        Ok(())
    }

    async fn form_cancel(&self, session: &str, id: &str, timeout: Duration) -> Result<()> {
        validate_opencode_id(session, "session")?;
        validate_opencode_id(id, "form")?;
        self.json(
            Method::DELETE,
            &format!("/session/{session}/form/{id}"),
            None,
            timeout,
        )
        .await?;
        Ok(())
    }
}

struct OpenCodeServerProcess {
    closed: std::sync::atomic::AtomicBool,
    api: OpenCodeApiClient,
    version: Option<String>,
    process: Mutex<CLIAgentProcess>,
    stdin: Mutex<Option<ChildStdin>>,
    stdout_reader: JoinHandle<()>,
    stderr: pioneer_cli_agent_runtime::process::StderrRing,
    shutdown_grace: Duration,
    _catalog_plugin: tempfile::TempDir,
}

impl OpenCodeServerProcess {
    async fn close(&self) -> Result<()> {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        self.stdout_reader.abort();
        self.stdin.lock().await.take();
        let mut process = self.process.lock().await;
        terminate_opencode_process(&mut process, self.shutdown_grace).await
    }
}

impl Drop for OpenCodeServerProcess {
    fn drop(&mut self) {
        self.stdout_reader.abort();
        self.process.get_mut().abort_service();
    }
}

// Startup owns the process before it can be published to a session/service.
// Dropping a caller's future must kill its group and stop both pipe readers.
struct OpenCodeStartupOwner {
    process: Option<CLIAgentProcess>,
    stdout_reader: Option<JoinHandle<()>>,
}

impl Drop for OpenCodeStartupOwner {
    fn drop(&mut self) {
        if let Some(reader) = &self.stdout_reader {
            reader.abort();
        }
        if let Some(process) = &mut self.process {
            process.abort_service();
        }
    }
}

const OPENCODE_STARTUP_MAX_ATTEMPTS: usize = 3;
const OPENCODE_STARTUP_EXIT_DRAIN: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenCodeStartupStage {
    Stdio,
    AddressEof,
    AddressRead,
    AddressInvalid,
    Timeout,
    Health,
}

#[derive(Debug)]
struct OpenCodeStartupFailure {
    stage: OpenCodeStartupStage,
    detail: String,
    status: Option<ExitStatus>,
    stderr: Vec<String>,
    sqlite_busy: bool,
    cleanup_error: Option<String>,
}

impl std::fmt::Display for OpenCodeStartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = self
            .status
            .map(|s| s.to_string())
            .unwrap_or_else(|| "exit status unavailable".into());
        let busy = if self.sqlite_busy {
            "; SQLite database contention"
        } else {
            ""
        };
        let cleanup = self.cleanup_error.as_deref().unwrap_or("complete");
        f.write_str(&bounded_safe_detail(&format!(
            "OpenCode server startup failed ({:?}; {status}{busy}); {}; cleanup: {cleanup}; stderr: {}",
            self.stage, self.detail, self.stderr.join(" | ")
        )))
    }
}

impl std::error::Error for OpenCodeStartupFailure {}

#[derive(Debug)]
struct OpenCodeStartupPreparationTimeout(&'static str);

impl std::fmt::Display for OpenCodeStartupPreparationTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OpenCode startup deadline exhausted {}; no process was spawned for this attempt",
            self.0
        )
    }
}

impl std::error::Error for OpenCodeStartupPreparationTimeout {}

fn opencode_sqlite_startup_conflict(
    stage: OpenCodeStartupStage,
    natural_status: Option<ExitStatus>,
    stderr: &[String],
) -> bool {
    // Require an unsuccessful natural exit, before any usable endpoint. A
    // timeout/health/boundary failure must never become retryable from stderr.
    stage == OpenCodeStartupStage::AddressEof
        && natural_status.is_some_and(|status| !status.success() && status.code().is_some())
        && stderr
            .iter()
            .any(|line| line.trim() == "SQLiteError: database is locked")
}

fn opencode_startup_safe_detail(value: &str, env: &SensitiveEnvironment) -> String {
    // In addition to StderrRing's exact secret redactions, hide launch values
    // if the child echoes its environment/config. Never attach raw error chains.
    let mut value = value.to_owned();
    let mut values = env
        .expose_iter()
        .map(|(_, value)| value)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    values.sort_unstable_by_key(|value| std::cmp::Reverse(value.len()));
    for secret in values {
        value = value.replace(secret, "[REDACTED]");
    }
    pioneer_protocol::sanitize_runtime_diagnostic_line(&value)
}

async fn opencode_startup_failure(
    process: &mut CLIAgentProcess,
    stage: OpenCodeStartupStage,
    error: anyhow::Error,
    env: &SensitiveEnvironment,
    deadline: Instant,
) -> OpenCodeStartupFailure {
    // stdout EOF can win the race against the stderr reader and child reaping.
    // wait() joins stderr; a descendant holding that pipe cannot block forever.
    let mut natural_status = process.try_wait().ok().flatten();
    if stage == OpenCodeStartupStage::AddressEof {
        let drain_deadline = deadline.min(Instant::now() + OPENCODE_STARTUP_EXIT_DRAIN);
        if let Ok(Ok(status)) = timeout_at(drain_deadline, process.wait()).await {
            natural_status = Some(status);
        } else if natural_status.is_none() {
            natural_status = process.try_wait().ok().flatten();
        }
    }
    let cleanup_error = match timeout_at(deadline, async {
        terminate_opencode_process(process, Duration::from_secs(2)).await?;
        // Parent exit and drained pipes are insufficient: descendants may
        // have closed all their channels and still hold the shared store.
        process.terminate_service_group(deadline).await?;
        #[cfg(all(test, unix))]
        startup_tests::pause_at(startup_tests::StartupPhase::GroupCleanupConfirmed).await;
        Ok::<(), anyhow::Error>(())
    })
    .await
    {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(opencode_startup_safe_detail(&format!("{error:#}"), env)),
        Err(_) => {
            process.abort_service();
            Some("startup deadline exhausted during process cleanup; owned process group force-termination requested".into())
        }
    };
    let stderr = process.stderr().lines().await;
    let sqlite_busy = opencode_sqlite_startup_conflict(stage, natural_status, &stderr);
    let status = natural_status.or_else(|| process.try_wait().ok().flatten());
    OpenCodeStartupFailure {
        stage,
        detail: opencode_startup_safe_detail(&format!("{error:#}"), env),
        status,
        stderr: pioneer_protocol::sanitize_runtime_diagnostic_lines(
            stderr
                .into_iter()
                .map(|line| opencode_startup_safe_detail(&line, env))
                .collect(),
        ),
        sqlite_busy,
        cleanup_error,
    }
}

async fn terminate_opencode_process(process: &mut CLIAgentProcess, grace: Duration) -> Result<()> {
    match tokio_timeout(
        grace + Duration::from_secs(2),
        process.terminate_with_grace(grace),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => {
            process.abort_service();
            Err(error)
        }
        Err(_) => {
            // A descendant may retain stderr after its parent exits. Bound that
            // drain too and use the shared process-group hard-kill path.
            process.abort_service();
            let _ = tokio_timeout(
                Duration::from_secs(2),
                process.terminate_with_grace(Duration::from_millis(100)),
            )
            .await;
            bail!(
                "OpenCode V2 shutdown exceeded its deadline; its process group was force-terminated"
            )
        }
    }
}

pub(crate) fn opencode_managed_data_home(
    runtime_home: &Path,
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> PathBuf {
    let scope = pioneer_cli_agent_runtime::codex_attestation::sha256_json(&json!([
        instance.id,
        instance.home_path,
        instance.shadow_home_path
    ]))
    .expect("OpenCode managed scope is serializable");
    runtime_home
        .join("cli-runtime/opencode-data-v2")
        .join(scope)
}

async fn spawn_opencode_server(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    cwd: &Path,
    environment: &SensitiveEnvironment,
    process_generation: Option<u64>,
    managed_data_home: &Path,
) -> Result<OpenCodeServerProcess> {
    // One budget covers preparation, all attempts, health, draining/cleanup and
    // backoff. Outer readiness (60s) and compaction deadlines may cancel sooner.
    let deadline = Instant::now() + Duration::from_millis(instance.startup_probe_timeout_ms.max(1));
    let mut last_confirmed_error: Option<anyhow::Error> = None;
    for attempt in 1..=OPENCODE_STARTUP_MAX_ATTEMPTS {
        let error = match spawn_opencode_server_attempt(
            instance,
            cwd,
            environment,
            process_generation,
            managed_data_home,
            deadline,
        )
        .await
        {
            Ok(server) => return Ok(server),
            Err(error) => error,
        };
        if error.is::<OpenCodeStartupPreparationTimeout>()
            && let Some(previous) = last_confirmed_error.take()
        {
            // Keep just the last completed process failure, explicitly scoped
            // to its attempt. Preparation itself has neither exit nor stderr.
            return Err(previous).with_context(|| {
                format!(
                    "{error} (attempt {attempt}); last completed process attempt {} follows",
                    attempt - 1
                )
            });
        }
        let retry = error
            .downcast_ref::<OpenCodeStartupFailure>()
            .is_some_and(|failure| failure.sqlite_busy && failure.cleanup_error.is_none());
        if !retry || attempt == OPENCODE_STARTUP_MAX_ATTEMPTS {
            return Err(error)
                .with_context(|| format!("OpenCode startup failed after {attempt} attempt(s)"));
        }
        // Independent jitter prevents simultaneous probes repeating in lockstep.
        let delay = Duration::from_millis(rand::random_range(100..=250) * attempt as u64);
        if Instant::now() + delay >= deadline {
            return Err(error).context("OpenCode startup deadline exhausted before SQLite retry");
        }
        if timeout_at(deadline, async {
            let delay = sleep(delay);
            #[cfg(all(test, unix))]
            startup_tests::pause_at(startup_tests::StartupPhase::RetryDelay).await;
            delay.await;
        })
        .await
        .is_err()
            || Instant::now() >= deadline
        {
            return Err(error)
                .context("OpenCode startup deadline exhausted during SQLite retry delay");
        }
        last_confirmed_error = Some(error);
    }
    unreachable!("OpenCode startup attempt limit is nonzero")
}

async fn spawn_opencode_server_attempt(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    cwd: &Path,
    environment: &SensitiveEnvironment,
    process_generation: Option<u64>,
    managed_data_home: &Path,
    deadline: Instant,
) -> Result<OpenCodeServerProcess> {
    if Instant::now() >= deadline {
        return Err(OpenCodeStartupPreparationTimeout("before preparation").into());
    }
    validate_opencode_custom_args(&instance.app_server_args)
        .context("invalid OpenCode custom launch arguments")?;
    let password = SecretString::new(format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    ));
    let mut env = environment.clone();
    env.insert_secret("OPENCODE_PASSWORD", password.clone());
    env.insert_plain("OPENCODE_CLIENT", "pioneer");

    let config_dir = expand_home_path(
        instance
            .shadow_home_path
            .as_deref()
            .unwrap_or(instance.home_path.as_str()),
        None,
    )?;
    env.insert_plain(
        "OPENCODE_CONFIG_DIR",
        config_dir.to_string_lossy().into_owned(),
    );
    // V2 credentials, sessions and saved grants live in its database. Use a
    // native V2 store, never the unpublished adapter's experimental store.
    // Configure providers through this home/environment or authenticate this
    // managed store with the native V2 CLI (reported by readiness).
    for (key, path) in [
        ("XDG_DATA_HOME", managed_data_home.to_path_buf()),
        ("XDG_CACHE_HOME", managed_data_home.join("cache")),
        ("XDG_STATE_HOME", managed_data_home.join("state")),
    ] {
        env.insert_plain(key, path.to_string_lossy().into_owned());
    }
    env.insert_plain(
        "OPENCODE_DB",
        managed_data_home
            .join("opencode/opencode.db")
            .to_string_lossy()
            .into_owned(),
    );
    let catalog_plugin = tempfile::Builder::new()
        .prefix("pioneer-opencode-catalog-")
        .tempdir()?;
    let catalog_rpc_id = format!("pioneer.catalog.{}", uuid::Uuid::new_v4().simple());
    timeout_at(deadline, async {
        #[cfg(all(test, unix))]
        startup_tests::pause_at(startup_tests::StartupPhase::PreparingPlugin).await;
        tokio::fs::write(
            catalog_plugin.path().join("package.json"),
            r#"{"name":"pioneer-opencode-catalog","type":"module","main":"server.js"}"#,
        )
        .await?;
        tokio::fs::write(
            catalog_plugin.path().join("server.js"),
            include_str!("opencode_catalog.js").replace("__PIONEER_CATALOG_ID__", &catalog_rpc_id),
        )
        .await
    })
    .await
    .map_err(|_| OpenCodeStartupPreparationTimeout("preparing catalog plugin"))??;
    let plugin_url = reqwest::Url::from_directory_path(catalog_plugin.path())
        .map_err(|_| anyhow!("OpenCode V2 catalog plugin path is invalid"))?;
    // Native V2 inline config. Global/project MCP entries are removed before
    // admission as well; wildcard-denied unknown actions cannot expand tools.
    env.insert_plain(
        "OPENCODE_CONFIG_CONTENT",
        json!({"update":"disable","plugins":[plugin_url.as_str()]}).to_string(),
    );
    let env_remove = [
        "OPENCODE_CONFIG",
        "OPENCODE_TEST_HOME",
        "OPENCODE_SERVER_PASSWORD",
        "OPENCODE_PTY_HANDOFF",
        "OPENCODE_SIMULATE",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let http_client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_millis(instance.request_timeout_ms.max(1)))
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
            "--stdio".to_owned(),
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
    if Instant::now() >= deadline {
        return Err(OpenCodeStartupPreparationTimeout("before spawn").into());
    }
    let process = spawn_cli_agent_process(&process_config)
        .with_context(|| format!("failed to spawn OpenCode server `{}`", instance.binary_path))?;
    let mut owner = OpenCodeStartupOwner {
        process: Some(process),
        stdout_reader: None,
    };
    let process = owner.process.as_mut().expect("startup owns process");
    let stderr = process.stderr();
    let (stdout, stdin) = match process.take_stdio() {
        Ok(stdio) => stdio,
        Err(error) => {
            return Err(opencode_startup_failure(
                process,
                OpenCodeStartupStage::Stdio,
                error,
                &process_config.env,
                deadline,
            )
            .await
            .into());
        }
    };
    let mut reader = BufReader::new(stdout);
    let mut failure_stage = OpenCodeStartupStage::AddressRead;
    let base_url = timeout_at(deadline, async {
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line);
            #[cfg(all(test, unix))]
            let read = startup_tests::observe_address_read(read).await?;
            #[cfg(not(all(test, unix)))]
            let read = read.await?;
            if read == 0 {
                failure_stage = OpenCodeStartupStage::AddressEof;
                bail!("OpenCode server exited before publishing its listen address");
            }
            if let Ok(address) = serde_json::from_str::<JsonValue>(line.trim())
                && let Some(url) = address["url"].as_str()
            {
                failure_stage = OpenCodeStartupStage::AddressInvalid;
                let parsed =
                    reqwest::Url::parse(url).context("invalid OpenCode V2 stdio listen URL")?;
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
    .map_err(|_| {
        failure_stage = OpenCodeStartupStage::Timeout;
        anyhow!("OpenCode server startup timed out waiting for listen address")
    });
    let base_url = match base_url {
        Ok(Ok(base_url)) => base_url,
        Ok(Err(error)) | Err(error) => {
            return Err(opencode_startup_failure(
                process,
                failure_stage,
                error,
                &process_config.env,
                deadline,
            )
            .await
            .into());
        }
    };
    owner.stdout_reader = Some(tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    }));
    let api = OpenCodeApiClient {
        client: http_client,
        base_url,
        directory: cwd.to_string_lossy().into_owned(),
        username: OPENCODE_SERVER_USERNAME.to_owned(),
        password,
        catalog_rpc_id,
    };
    let health = timeout_at(
        deadline,
        api.health(deadline.saturating_duration_since(Instant::now())),
    )
    .await;
    let (health, health_stage) = match health {
        Ok(health) if Instant::now() < deadline => (health, OpenCodeStartupStage::Health),
        _ => (
            Err(anyhow!(
                "OpenCode server startup timed out during health probe"
            )),
            OpenCodeStartupStage::Timeout,
        ),
    };
    let version = match health.and_then(|health| {
        anyhow::ensure!(
            health["pid"].as_u64() == process.id().map(u64::from),
            "OpenCode V2 info belongs to another process"
        );
        opencode_server_version(&health)
    }) {
        Ok(version) => version,
        Err(error) => {
            owner
                .stdout_reader
                .as_ref()
                .expect("startup stdout reader")
                .abort();
            return Err(opencode_startup_failure(
                process,
                health_stage,
                error,
                &process_config.env,
                deadline,
            )
            .await
            .into());
        }
    };
    Ok(OpenCodeServerProcess {
        closed: std::sync::atomic::AtomicBool::new(false),
        api,
        version,
        process: Mutex::new(owner.process.take().expect("startup owns process")),
        stdin: Mutex::new(Some(stdin)),
        stdout_reader: owner.stdout_reader.take().expect("startup stdout reader"),
        stderr,
        shutdown_grace: Duration::from_secs(5),
        _catalog_plugin: catalog_plugin,
    })
}

// Version is diagnostic/cache metadata, not a condition for using the API.
fn opencode_server_version(health: &JsonValue) -> Result<Option<String>> {
    anyhow::ensure!(
        health["pid"].as_u64().is_some_and(|pid| pid > 0)
            && health["urls"].is_array()
            && health["paths"]["tmp"].is_string(),
        "OpenCode V2 /api/info response is invalid"
    );
    Ok(health
        .get("version")
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|version| !version.is_empty())
        .map(str::to_owned))
}

// The shared compaction runner owns timeout/cancellation and calls cleanup,
// just as it does for Codex/Claude services. No interactive session is reused.
pub(crate) struct OpenCodeSummaryService {
    instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
    environment: SensitiveEnvironment,
    owner: Mutex<Option<(Arc<OpenCodeServerProcess>, tempfile::TempDir)>>,
    data_home: PathBuf,
}
impl OpenCodeSummaryService {
    pub(crate) fn new(
        instance: EffectiveGatewayCliAgentRuntimeInstanceConfig,
        environment: SensitiveEnvironment,
        data_home: PathBuf,
    ) -> Self {
        Self {
            instance,
            environment,
            data_home,
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
                &self.data_home,
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
        let deadline = tokio::time::Instant::now() + timeout;
        let session = server
            .api
            .create_session(
                json!([{"action":"*","resource":"*","effect":"deny"}]),
                None,
                timeout,
            )
            .await?;
        let id = session["id"]
            .as_str()
            .context("OpenCode summary session identity is missing")?;
        let selected = opencode_model_ref(&provider, &model_id, effort.as_deref())?;
        server
            .api
            .configure_turn(id, instructions, Some(&selected), timeout)
            .await?;
        let message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
        server
            .api
            .prompt(id, &json!({"id":message_id,"text":input}), timeout)
            .await?;
        loop {
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "OpenCode V2 summary timed out"
            );
            let messages = server
                .api
                .session_messages(
                    id,
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await?;
            let turn = opencode_turn_messages(&messages, &message_id);
            if turn.iter().any(|m| m["type"] == "idle") {
                return opencode_summary_result(&selected, &messages, &message_id);
            }
            sleep(Duration::from_millis(100)).await;
        }
    }
    pub(crate) async fn cleanup(&self) -> Result<()> {
        if let Some((server, _temporary)) = self.owner.lock().await.take() {
            server.close().await?;
        }
        Ok(())
    }
}

// The summary service's terminal boundary is shared with its offline
// regressions: select this input's history, validate its outcome and expose
// normalized totals to the consumer/journal, including usage on failure.
pub(crate) fn opencode_summary_result(
    selected: &JsonValue,
    messages: &JsonValue,
    input_id: &str,
) -> Result<(String, Option<u64>, Option<u64>, JsonValue)> {
    let turn = opencode_turn_messages(messages, input_id);
    let usage = opencode_usage_metadata(turn.iter().copied());
    let result = (|| {
        let idle = turn
            .iter()
            .rev()
            .find(|m| m["type"] == "idle")
            .context("OpenCode V2 summary has no terminal outcome")?;
        anyhow::ensure!(
            idle["outcome"] == "succeeded",
            "OpenCode V2 summary failed: {}",
            idle["error"]
        );
        let mut text = Vec::new();
        for message in turn.iter().filter(|m| m["type"] == "assistant") {
            anyhow::ensure!(
                &message["model"] == selected,
                "OpenCode V2 summary model changed"
            );
            text.extend(
                message["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|c| c["type"] == "text")
                    .filter_map(|c| c["text"].as_str()),
            );
        }
        let text = text.join("\n");
        anyhow::ensure!(
            !text.trim().is_empty(),
            "OpenCode V2 summary returned no text"
        );
        Ok((
            text,
            usage["input_tokens"].as_u64(),
            usage["output_tokens"].as_u64(),
            usage.clone(),
        ))
    })();
    result.map_err(|error: anyhow::Error| {
        if usage["native_tokens"].is_object() {
            error.context(pioneer_cli_agent_runtime::service::ObservedServiceUsage(
                usage,
            ))
        } else {
            error
        }
    })
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
    let managed_home = opencode_managed_data_home(cwd, instance);
    let server = match spawn_opencode_server(instance, cwd, environment, None, &managed_home).await
    {
        Ok(server) => server,
        Err(error) => {
            let missing = error.chain().any(|source| {
                source
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
            });
            let status = if missing {
                OpenCodeProbeStatus::MissingBinary
            } else {
                OpenCodeProbeStatus::SpawnFailed
            };
            let message = bounded_safe_detail(format!("{error:#}").as_str());
            let stderr = error
                .downcast_ref::<OpenCodeStartupFailure>()
                .map(|failure| failure.stderr.clone())
                .unwrap_or_default();
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
                stderr,
                models: Vec::new(),
            };
        }
    };
    let request_timeout = Duration::from_millis(instance.request_timeout_ms.max(1));
    let version = server.version.clone();
    let catalog: Result<(JsonValue, JsonValue)> = async {
        let providers = server.api.providers(request_timeout).await?;
        let models = server.api.models(request_timeout).await?;
        // Health alone must not claim readiness for an unsupported surface.
        server
            .api
            .data(Method::GET, "/skill", None, request_timeout)
            .await?;
        server
            .api
            .data(Method::GET, "/mcp", None, request_timeout)
            .await?;
        server.api.session_status(request_timeout).await?;
        server.api.catalog(request_timeout).await?;
        Ok((providers, models))
    }
    .await;
    let stderr_before_close = server.stderr.lines().await;
    let close_result = server.close().await;
    let (providers, catalog_models) = match catalog {
        Ok(catalog) => catalog,
        Err(error) => {
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
    let models = opencode_models_from_response(&catalog_models, &providers);
    let connected = models
        .iter()
        .filter_map(|m| m.id.split_once('/').map(|(p, _)| p.to_owned()))
        .collect::<HashSet<_>>();
    let authenticated = !models.is_empty();
    let status = if authenticated {
        OpenCodeProbeStatus::Ready
    } else {
        OpenCodeProbeStatus::NeedsAuth
    };
    let message=(!authenticated).then(|| {
        let configured=instance.shadow_home_path.as_deref().unwrap_or(&instance.home_path);
        let config=expand_home_path(configured,None).map(|p|p.to_string_lossy().into_owned()).unwrap_or_else(|_|configured.to_owned());
        let quote=|value:&str|format!("'{}'",value.replace('\'',"'\"'\"'"));
        let binary=expand_home_path(&instance.binary_path,None).map(|p|p.to_string_lossy().into_owned()).unwrap_or_else(|_|instance.binary_path.clone());
        format!("OpenCode V2 has no available model provider. Authenticate its private store with these runtime paths: XDG_DATA_HOME={} XDG_CACHE_HOME={} XDG_STATE_HOME={} OPENCODE_DB={} OPENCODE_CONFIG_DIR={} {} auth login --standalone",
            quote(&managed_home.to_string_lossy()),quote(&managed_home.join("cache").to_string_lossy()),quote(&managed_home.join("state").to_string_lossy()),
            quote(&managed_home.join("opencode/opencode.db").to_string_lossy()),quote(&config),quote(&binary))
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

fn opencode_models_from_response(
    models: &JsonValue,
    providers: &JsonValue,
) -> Vec<OpenCodeModelSnapshot> {
    let mut result = Vec::new();
    for model in models
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| m["enabled"] == true)
    {
        let (Some(provider), Some(id)) = (model["providerID"].as_str(), model["id"].as_str())
        else {
            continue;
        };
        let provider_name = providers
            .as_array()
            .into_iter()
            .flatten()
            .find(|p| p["id"] == provider)
            .and_then(|p| p["name"].as_str())
            .unwrap_or(provider);
        let modalities = |key: &str| {
            model["capabilities"][key]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(JsonValue::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let input_modalities = modalities("input");
        result.push(OpenCodeModelSnapshot {
            id: format!("{provider}/{id}"),
            name: model["name"].as_str().map(str::to_owned),
            description: Some(provider_name.to_owned()),
            family: model["family"].as_str().map(str::to_owned),
            // V2 does not expose the old source=config classification.
            is_custom: model["canonical"]
                .as_str()
                .is_some_and(|canonical| canonical != provider),
            active: Some(true),
            effort_options: model["variants"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v["id"].as_str())
                .map(str::to_owned)
                .collect(),
            supports_reasoning: None,
            supports_vision: Some(input_modalities.iter().any(|m| m == "image")),
            input_modalities,
            output_modalities: modalities("output"),
            max_input_tokens: model
                .pointer("/limit/input")
                .or_else(|| model.pointer("/limit/context"))
                .and_then(JsonValue::as_u64),
            max_output_tokens: model.pointer("/limit/output").and_then(JsonValue::as_u64),
        });
    }
    result.sort_by(|a, b| a.id.cmp(&b.id));
    result
}

pub(crate) fn opencode_provider_contract_fingerprint() -> String {
    pioneer_cli_agent_runtime::codex_attestation::sha256_json(&json!({
        "provider": "opencode",
        "adapterContract": OPENCODE_ADAPTER_CONTRACT,
        "transport": "authenticated_loopback_http_sse",
        "mcp": "dynamic_local_stdio",
        "catalogAttestation":pioneer_cli_agent_runtime::codex_attestation::sha256_json(&json!(include_str!("opencode_catalog.js"))).expect("catalog plugin fingerprint"),
    }))
    .expect("static OpenCode provider contract is serializable")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenCodePendingRequestKind {
    Permission,
    Form,
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
    awaiting_execution: bool,
    user_observed: bool,
    execution_observed: bool,
    dispatch_deadline: Option<tokio::time::Instant>,
    assistant_message_ids: HashSet<String>,
    streaming_items: HashSet<String>,
    tool_contents: HashMap<String, JsonValue>,
    cwd: Option<String>,
    items: HashMap<String, OpenCodeItemState>,
    pending_requests: HashMap<String, OpenCodePendingRequestKind>,
    last_model: Option<(String, String)>,
    last_usage: Option<JsonValue>,
    event_ids: HashSet<String>,
    event_order: VecDeque<String>,
    last_turn_observation: Option<CLIAgentRuntimeTurnObservation>,
}

impl OpenCodeRuntimeState {
    fn begin_turn(&mut self, native_thread_id: String, native_turn_id: String) {
        self.native_thread_id = Some(native_thread_id);
        self.streaming_items.clear();
        self.tool_contents.clear();
        self.active_turn_id = Some(native_turn_id);
        self.turn_started = false;
        self.terminal_emitted = false;
        self.interrupted = false;
        self.last_error = None;
        self.last_diff = None;
        self.last_usage = None;
        self.event_ids.clear();
        self.event_order.clear();
        self.item_order.clear();
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

    fn observe_execution(&mut self) {
        self.execution_observed = true;
        self.awaiting_execution = false;
        self.dispatch_deadline = None;
    }
}

fn spawn_opencode_event_stream(
    server: Arc<OpenCodeServerProcess>,
    state: Arc<Mutex<OpenCodeRuntimeState>>,
    event_tx: mpsc::Sender<pioneer_observability::work::WorkItem<RuntimeEvent>>,
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
                response = tokio_timeout(request_timeout, api.request(Method::GET, "/event").send()) => response,
            };
            let response = match response {
                Ok(Ok(response)) if response.status().is_success() => response,
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
                    // V2 sends a heartbeat every 15s. A silent connection must
                    // reconnect and hydrate history rather than wait forever.
                    line = tokio_timeout(Duration::from_secs(45), codec.read_frame_or_spool(&mut reader)) => match line {
                        Ok(line) => line,
                        Err(_) => break,
                    },
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
                if line.starts_with(b":") {
                    let active = {
                        let snapshot = state.lock().await;
                        snapshot.active_turn_id.is_some() && !snapshot.terminal_emitted
                    };
                    if active {
                        match reconcile_opencode_turn(&server, &state, request_timeout, true).await
                        {
                            Ok(events) => {
                                for event in events {
                                    tokio::select! { _=cancellation.cancelled()=>return, result=event_tx.send(pioneer_observability::work::WorkItem::new(event,pioneer_observability::work::Boundary::EventLane,pioneer_observability::work::WorkKind::Event,pioneer_observability::work::ExecutorRole::Event))=>if result.is_err(){return;} }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    continue;
                }
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
                    Err(_) => break,
                };
                let kind = raw["type"].as_str().unwrap_or("");
                let data = &raw["data"];
                let events = if kind == "server.connected" {
                    // The feed is volatile: REST is the recovery authority.
                    state.lock().await.streaming_items.clear();
                    match reconcile_opencode_turn(&server, &state, request_timeout, true).await {
                        Ok(events) => {
                            ready.send_replace(true);
                            events
                        }
                        Err(_) => break,
                    }
                } else {
                    let matches_session =
                        event_belongs_to_active_session(&*state.lock().await, data);
                    if kind.starts_with("session.") && !matches_session {
                        continue;
                    }
                    let message_id = data["assistantMessageID"]
                        .as_str()
                        .or_else(|| {
                            data.pointer("/source/messageID")
                                .and_then(JsonValue::as_str)
                        })
                        .or_else(|| {
                            data.pointer("/form/metadata/tool/messageID")
                                .and_then(JsonValue::as_str)
                        });
                    let unknown_message = message_id.is_some_and(|mid| {
                        !state
                            .try_lock()
                            .is_ok_and(|s| s.assistant_message_ids.contains(mid))
                    });
                    if unknown_message {
                        // V2 has no assistant.parentID. Establish ownership from
                        // ordered history bounded by this prompt and the next input.
                        let session = state.lock().await.native_thread_id.clone();
                        if let Some(session) = session {
                            if let Ok(messages) =
                                api.session_messages(&session, request_timeout).await
                            {
                                let mut snapshot = state.lock().await;
                                refresh_opencode_message_ownership(&mut snapshot, &messages);
                            }
                        }
                    }
                    if kind == "form.created"
                        && matches_session
                        && data
                            .pointer("/form/metadata/kind")
                            .and_then(JsonValue::as_str)
                            == Some("websearch.provider")
                    {
                        match reconcile_opencode_turn(&server, &state, request_timeout, true).await
                        {
                            Ok(events) => events,
                            Err(_) => break,
                        }
                    } else if kind == "form.created"
                        && opencode_form_belongs_to_turn(&*state.lock().await, &data["form"])
                        && !opencode_form_supported(&data["form"])
                    {
                        let form = &data["form"];
                        if let (Some(session), Some(id)) =
                            (form["sessionID"].as_str(), form["id"].as_str())
                        {
                            let _ = api.form_cancel(session, id, request_timeout).await;
                            let _ = api.abort_session(session, request_timeout).await;
                        }
                        server.close().await.ok();
                        let mut snapshot = state.lock().await;
                        snapshot.last_error = Some(
                            json!({"type":"UnsupportedForm","message":"OpenCode V2 requested a form Pioneer cannot display or correlate to its tool call."}),
                        );
                        terminalize_opencode_failure(&mut snapshot, None)
                    } else if matches_session
                        && matches!(
                            kind,
                            "session.execution.started"
                                | "session.execution.succeeded"
                                | "session.execution.failed"
                                | "session.execution.interrupted"
                                | "session.step.ended"
                                | "session.step.failed"
                        )
                    {
                        match reconcile_opencode_turn(&server, &state, request_timeout, true).await
                        {
                            Ok(events) => events,
                            Err(_) => break,
                        }
                    } else {
                        map_opencode_event(&state, raw, include_native).await
                    }
                };
                for event in events {
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        result = event_tx.send(pioneer_observability::work::WorkItem::new(event,pioneer_observability::work::Boundary::EventLane,pioneer_observability::work::WorkKind::Event,pioneer_observability::work::ExecutorRole::Event)) => if result.is_err() { return; },
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
    tx: &mpsc::Sender<pioneer_observability::work::WorkItem<RuntimeEvent>>,
    timeout: Duration,
) {
    let (session, events) = {
        let mut state = state.lock().await;
        let session = state.native_thread_id.clone();
        state.last_error = Some(
            json!({"type":"NativeEventBudgetExceeded","message":"OpenCode SSE frame exceeds the admitted native event budget"}),
        );
        let events = terminalize_opencode_failure(&mut state, None);
        (session, events)
    };
    if let Some(session) = session {
        let _ = server.api.abort_session(&session, timeout).await;
    }
    let _ = server.close().await;
    for event in events {
        if tx
            .send(pioneer_observability::work::WorkItem::new(
                event,
                pioneer_observability::work::Boundary::EventLane,
                pioneer_observability::work::WorkKind::Event,
                pioneer_observability::work::ExecutorRole::Event,
            ))
            .await
            .is_err()
        {
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

async fn emit_opencode_stream_diagnostic(
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    event_tx: &mpsc::Sender<pioneer_observability::work::WorkItem<RuntimeEvent>>,
    message: String,
) {
    let state = state.lock().await;
    let event = RuntimeEvent::Error(RuntimeErrorEvent {
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: (!state.terminal_emitted)
            .then(|| state.active_turn_id.clone())
            .flatten(),
        message: bounded_safe_detail(message.as_str()),
        code: Some("opencode_event_stream_disconnected".to_owned()),
        retryable: true,
        native: None,
    });
    drop(state);
    let _ = event_tx
        .send(pioneer_observability::work::WorkItem::new(
            event,
            pioneer_observability::work::Boundary::EventLane,
            pioneer_observability::work::WorkKind::Event,
            pioneer_observability::work::ExecutorRole::Event,
        ))
        .await;
}

async fn map_opencode_event(
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    raw: JsonValue,
    include_native: bool,
) -> Vec<RuntimeEvent> {
    let Some(kind) = raw["type"].as_str() else {
        return Vec::new();
    };
    let data = &raw["data"];
    let mut state = state.lock().await;
    let native = opencode_native_event(kind, data, &raw, include_native);
    if !event_belongs_to_active_session(&state, data) {
        // The V2 feed spans every location. Global activity has no turn owner.
        return Vec::new();
    }
    if state.terminal_emitted || state.active_turn_id.is_none() {
        return Vec::new();
    }
    if let Some(id) = raw["id"].as_str() {
        if !state.event_ids.insert(id.to_owned()) {
            return Vec::new();
        }
        state.event_order.push_back(id.to_owned());
        if state.event_order.len() > 8192
            && let Some(expired) = state.event_order.pop_front()
        {
            state.event_ids.remove(&expired);
        }
    }
    let mid = data["assistantMessageID"].as_str();
    if mid.is_some_and(|mid| !state.assistant_message_ids.contains(mid)) {
        return Vec::new();
    }
    if kind.starts_with("session.text.") || kind.starts_with("session.reasoning.") {
        let Some(mid) = mid.filter(|mid| state.assistant_message_ids.contains(*mid)) else {
            return Vec::new();
        };
        let content_type = if kind.starts_with("session.reasoning.") {
            "reasoning"
        } else {
            "text"
        };
        let Some(ordinal) = data["ordinal"].as_u64() else {
            return Vec::new();
        };
        let id = format!("{mid}:{content_type}:{ordinal}");
        if kind.ends_with(".started") {
            state.streaming_items.insert(id.clone());
            return project_opencode_content(
                &mut state,
                &id,
                &json!({"type":content_type,"text":""}),
                false,
                native,
            );
        }
        if kind.ends_with(".delta") {
            // Never guess the missing prefix of a volatile stream after a gap.
            // Ended carries the authoritative full value even for empty text.
            if !state.streaming_items.contains(&id)
                || state.items.get(&id).is_some_and(|i| i.completed)
            {
                return Vec::new();
            }
            let text = format!(
                "{}{}",
                state.items.get(&id).map(|i| i.text.as_str()).unwrap_or(""),
                data["delta"].as_str().unwrap_or("")
            );
            return project_opencode_content(
                &mut state,
                &id,
                &json!({"type":content_type,"text":text}),
                false,
                native,
            );
        }
        if kind.ends_with(".ended") {
            state.streaming_items.remove(&id);
            return project_opencode_content(
                &mut state,
                &id,
                &json!({"type":content_type,"text":data["text"]}),
                true,
                native,
            );
        }
    }
    if kind.starts_with("session.tool.") {
        let Some(mid) = mid.filter(|mid| state.assistant_message_ids.contains(*mid)) else {
            return Vec::new();
        };
        let Some(call) = data["id"].as_str() else {
            return Vec::new();
        };
        let id = format!("{mid}:tool:{call}");
        let mut content = state
            .tool_contents
            .get(&id)
            .cloned()
            .unwrap_or_else(|| json!({"type":"tool","id":call}));
        match kind {
            "session.tool.input.started" => {
                content["name"] = data["name"].clone();
                content["state"] = json!({"status":"streaming","input":""});
            }
            "session.tool.input.delta" => {
                let text = format!(
                    "{}{}",
                    content["state"]["input"].as_str().unwrap_or(""),
                    data["delta"].as_str().unwrap_or("")
                );
                if content["state"]["status"] == "streaming" {
                    content["state"]["input"] = json!(text);
                }
            }
            "session.tool.input.ended" => {
                if content["state"]["status"] == "streaming" {
                    content["state"]["input"] = data["text"].clone();
                }
            }
            "session.tool.called" => {
                content["state"] = json!({"status":"running","input":data["input"],"metadata":{}});
            }
            "session.tool.progress" => {
                content["state"]["metadata"] = data["metadata"].clone();
            }
            "session.tool.success" | "session.tool.failed" => {
                content["state"]["status"] = json!(if kind.ends_with("success") {
                    "completed"
                } else {
                    "error"
                });
                content["state"]["content"] = data["content"].clone();
                content["state"]["metadata"] = data["metadata"].clone();
                content["state"]["error"] = data["error"].clone();
            }
            _ => return Vec::new(),
        }
        // If a start was missed, REST restores name/input from the same call ID.
        if !content["name"].is_string() {
            return Vec::new();
        }
        state.tool_contents.insert(id.clone(), content.clone());
        return project_opencode_content(&mut state, &id, &content, false, native);
    }
    match kind {
        "permission.asked" => map_opencode_permission_request(&mut state, data, native),
        "form.created" => map_opencode_form_request(&mut state, &data["form"], native),
        "permission.replied" | "form.replied" | "form.cancelled" => {
            let id = data["requestID"]
                .as_str()
                .or_else(|| data["id"].as_str())
                .unwrap_or("");
            if state.pending_requests.remove(id).is_none() {
                return Vec::new();
            }
            vec![RuntimeEvent::RequestResolved(RuntimeRequestResolved {
                native_request_id: id.to_owned(),
                native,
            })]
        }
        "session.retry.scheduled" => {
            state.observe_execution();
            vec![RuntimeEvent::TurnRetrying(RuntimeTurnRetrying {
                native_thread_id: state.native_thread_id.clone(),
                native_turn_id: state.active_turn_id.clone(),
                message: data["error"]["message"]
                    .as_str()
                    .unwrap_or("OpenCode is retrying")
                    .to_owned(),
                code: None,
                native,
            })]
        }
        _ => Vec::new(),
    }
}

fn event_belongs_to_active_session(state: &OpenCodeRuntimeState, data: &JsonValue) -> bool {
    let session = data["sessionID"]
        .as_str()
        .or_else(|| data["form"]["sessionID"].as_str());
    session.is_some() && session == state.native_thread_id.as_deref()
}

fn opencode_content_id(message: &str, content: &JsonValue, ordinal: usize) -> String {
    if content["type"] == "tool" {
        format!("{message}:tool:{}", content["id"].as_str().unwrap_or(""))
    } else {
        format!(
            "{message}:{}:{ordinal}",
            content["type"].as_str().unwrap_or("unknown")
        )
    }
}

fn project_opencode_content(
    state: &mut OpenCodeRuntimeState,
    id: &str,
    content: &JsonValue,
    message_completed: bool,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    let (Some(session), Some(turn)) =
        (state.native_thread_id.clone(), state.active_turn_id.clone())
    else {
        return Vec::new();
    };
    if state.terminal_emitted {
        return Vec::new();
    }
    let tool = content["type"] == "tool";
    let reasoning = content["type"] == "reasoning";
    let name = content["name"].as_str().unwrap_or("tool");
    let kind = if tool {
        opencode_tool_item_kind(name)
    } else if reasoning {
        "reasoning".into()
    } else {
        "agentMessage".into()
    };
    let status = content["state"]["status"].as_str().unwrap_or("streaming");
    let completed = if tool {
        matches!(status, "completed" | "error")
    } else {
        message_completed
    };
    let text = if tool {
        opencode_tool_text(&content["state"])
    } else {
        content["text"].as_str().unwrap_or("").to_owned()
    };
    if tool && status == "streaming" {
        return Vec::new();
    }
    let metadata = if tool {
        let mut metadata = opencode_tool_metadata(name, &content["state"], status);
        metadata["nativeCallId"] = content["id"].clone();
        metadata["nativeMessageId"] = json!(id.split_once(":tool:").map(|(mid, _)| mid));
        if let (Some(start), Some(end)) = (
            content["time"]["ran"].as_u64(),
            content["time"]["completed"].as_u64(),
        ) {
            metadata["durationMs"] = json!(end.saturating_sub(start));
        }
        if metadata.get("cwd").is_none() {
            metadata["cwd"] = json!(state.cwd);
        }
        Some(metadata)
    } else {
        None
    };
    let previous = state.items.get(id).cloned();
    if previous.as_ref().is_some_and(|i| i.completed) {
        return Vec::new();
    }
    if tool {
        state.tool_contents.insert(id.to_owned(), content.clone());
    }
    let mut events = Vec::new();
    if !state.turn_started {
        state.turn_started = true;
        events.push(RuntimeEvent::TurnStarted(RuntimeTurnStarted {
            native_thread_id: Some(session.clone()),
            native_turn_id: turn.clone(),
            native: None,
        }));
    }
    if previous.is_none() {
        state.item_order.push(id.to_owned());
        events.push(RuntimeEvent::ItemStarted(RuntimeItemStarted {
            native_thread_id: Some(session.clone()),
            native_turn_id: turn.clone(),
            native_item_id: id.to_owned(),
            item_kind: kind.clone(),
            title: tool.then(|| name.to_owned()),
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: metadata.clone(),
            native_item_redacted: None,
            native: native.clone(),
        }));
    }
    let old = previous.as_ref().map(|i| i.text.as_str()).unwrap_or("");
    let suffix = text.strip_prefix(old).unwrap_or("");
    let changed_metadata = previous.as_ref().and_then(|i| i.metadata.as_ref()) != metadata.as_ref();
    if !suffix.is_empty() || (tool && changed_metadata) {
        events.push(RuntimeEvent::ItemDelta(RuntimeItemDelta {
            native_thread_id: Some(session.clone()),
            native_turn_id: turn.clone(),
            native_item_id: id.to_owned(),
            item_kind: kind.clone(),
            delta_kind: if tool {
                RuntimeItemDeltaKind::ToolProgress
            } else if reasoning {
                RuntimeItemDeltaKind::ReasoningText
            } else {
                RuntimeItemDeltaKind::AgentMessage
            },
            delta: suffix.to_owned(),
            metadata: metadata.clone(),
            native: native.clone(),
        }));
    }
    if completed {
        events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
            native_thread_id: Some(session),
            native_turn_id: turn,
            native_item_id: id.to_owned(),
            item_kind: kind.clone(),
            text: Some(text.clone()),
            summary: Vec::new(),
            content: if reasoning {
                vec![text.clone()]
            } else {
                Vec::new()
            },
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: metadata.clone(),
            native_item_redacted: None,
            native,
        }));
    }
    state.items.insert(
        id.to_owned(),
        OpenCodeItemState {
            kind,
            text,
            metadata,
            completed,
        },
    );
    events
}

fn opencode_tool_text(state: &JsonValue) -> String {
    state["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["type"] == "text")
        .filter_map(|c| c["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn map_opencode_diff(
    state: &mut OpenCodeRuntimeState,
    diff: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    if state.terminal_emitted || state.last_diff.as_ref() == Some(diff) {
        return Vec::new();
    }
    let Some(turn) = &state.active_turn_id else {
        return Vec::new();
    };
    state.last_diff = Some(diff.clone());
    vec![RuntimeEvent::DiffUpdated(RuntimeDiffUpdated {
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: turn.clone(),
        diff_redacted: Some(diff.clone()),
        diff_text: None,
        revision: None,
        event_id: None,
        provider_exact: Some(false),
        native,
    })]
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
        if let Some(metadata) = item.metadata.as_mut() {
            metadata["status"] = json!(if state.interrupted {
                "interrupted"
            } else {
                "failed"
            });
            metadata["success"] = json!(false);
        }
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
    if let Some(usage) = &state.last_usage {
        events.push(RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
            native_thread_id: Some(native_thread_id.clone()),
            native_turn_id: native_turn_id.clone(),
            native_item_id: format!("{native_turn_id}:usage"),
            item_kind: "tokenUsage".into(),
            text: Some(format!(
                "OpenCode usage: {} input, {} output tokens",
                usage["input_tokens"], usage["output_tokens"]
            )),
            summary: Vec::new(),
            content: Vec::new(),
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some(usage.clone()),
            native_item_redacted: None,
            native: None,
        }));
    }
    for id in state
        .pending_requests
        .drain()
        .map(|(id, _)| id)
        .collect::<Vec<_>>()
    {
        events.push(RuntimeEvent::RequestResolved(RuntimeRequestResolved {
            native_request_id: id,
            native: None,
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
    let error = state.last_error.clone().unwrap_or_else(|| json!({"type":"ExecutionLost","message":"OpenCode V2 has no terminal outcome for this turn"}));
    let message = bounded_safe_detail(
        error
            .get("message")
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
        code:error["status"].as_u64().map(|s|s.to_string()).or_else(||error["type"].as_str().map(str::to_owned)),
        native:native.or_else(||Some(RuntimeNativeEvent {method:"session.execution.failed".into(),
            payload_redacted:Some(json!({"type":error["type"],"message":error["message"],"status":error["status"]})),raw_redacted:None})),
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
    if properties["sessionID"].as_str() != state.native_thread_id.as_deref()
        || !properties
            .pointer("/source/messageID")
            .and_then(JsonValue::as_str)
            .is_some_and(|mid| state.assistant_message_ids.contains(mid))
        || request_id.is_empty()
        || state.terminal_emitted
        || state.active_turn_id.is_none()
        || state.pending_requests.contains_key(request_id)
    {
        return Vec::new();
    }
    let permission = properties
        .get("action")
        .and_then(JsonValue::as_str)
        .unwrap_or("tool");
    let item_id = properties
        .pointer("/source/messageID")
        .and_then(JsonValue::as_str)
        .zip(properties.pointer("/source/id").and_then(JsonValue::as_str))
        .map(|(mid, call)| format!("{mid}:tool:{call}"));
    let content = item_id.as_ref().and_then(|id| state.tool_contents.get(id));
    let tool_name = content
        .and_then(|c| c["name"].as_str())
        .unwrap_or(permission);
    let metadata = properties
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let input = content
        .and_then(|c| c.pointer("/state/input"))
        .filter(|i| i.is_object())
        .cloned()
        .unwrap_or_else(|| metadata.clone());
    let request_kind = opencode_permission_request_kind(tool_name);
    let command = command_from_opencode_input(&input).or_else(|| {
        matches!(normalize_opencode_kind(permission).as_str(), "shell")
            .then(|| {
                properties
                    .get("resources")
                    .and_then(JsonValue::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(JsonValue::as_str)
                    .collect::<Vec<_>>()
                    .join(" && ")
            })
            .filter(|command| !command.is_empty())
    });
    let payload = json!({
        "nativeRequestId": request_id,
        "nativeRequestIdJson": request_id,
        "toolName": tool_name,
        "command": command,
        "input": redact_opencode_json(&input),
        "title": properties.pointer("/metadata/title"),
        "description": format!("OpenCode requests permission for `{tool_name}`"),
        "patterns": properties.get("resources").map(redact_opencode_json),
        "changedFiles": changed_files_from_opencode_input(&metadata),
        "diffPreview": opencode_file_diff_text(&metadata).map(|d| opencode_diff_preview(&d)),
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

fn opencode_form_belongs_to_turn(state: &OpenCodeRuntimeState, form: &JsonValue) -> bool {
    if form["sessionID"].as_str() != state.native_thread_id.as_deref()
        || state.active_turn_id.is_none()
        || state.terminal_emitted
        || state.interrupted
    {
        return false;
    }
    if let Some(tool) = form.pointer("/metadata/tool") {
        return tool["id"].as_str().is_some()
            && tool["messageID"]
                .as_str()
                .is_some_and(|mid| state.assistant_message_ids.contains(mid));
    }
    // Native websearch's provider-selection form has no tool call metadata.
    // Only its documented kind, within this owner's executing current input,
    // may omit the item ID. Other session/global plugin forms stay unowned.
    form.pointer("/metadata/kind").and_then(JsonValue::as_str) == Some("websearch.provider")
        && state.user_observed
        && state.execution_observed
}

fn opencode_form_supported(form: &JsonValue) -> bool {
    form["fields"].as_array().is_some_and(|fields| {
        fields.iter().any(|f| f["hidden"] != true)
            && fields.iter().all(|field| {
                matches!(
                    field["type"].as_str(),
                    Some("string" | "multiselect" | "number" | "integer" | "boolean")
                ) && !(field["hidden"] == true
                    && field["required"] == true
                    && field.get("default").is_none())
            })
    })
}

fn map_opencode_form_request(
    state: &mut OpenCodeRuntimeState,
    form: &JsonValue,
    native: Option<RuntimeNativeEvent>,
) -> Vec<RuntimeEvent> {
    let Some(id) = form["id"].as_str() else {
        return Vec::new();
    };
    if !opencode_form_belongs_to_turn(state, form)
        || !opencode_form_supported(form)
        || state.pending_requests.contains_key(id)
    {
        return Vec::new();
    }
    let mid = form
        .pointer("/metadata/tool/messageID")
        .and_then(JsonValue::as_str);
    let Some(fields) = form["fields"].as_array() else {
        return Vec::new();
    };
    let questions = fields.iter().filter(|f| f["hidden"] != true).map(|field| {
        let options = field["options"].as_array().map(|options| options.iter().map(|o| json!({"label":o["label"],"description":o["description"]})).collect::<Vec<_>>());
        json!({"id":field["key"], "header":field["title"], "question":field["description"].as_str().or_else(|| field["title"].as_str()).unwrap_or("Answer"),
            "multiple":field["type"] == "multiselect", "isOther":field["custom"].as_bool().unwrap_or(options.is_none()),
            "options":options, "isSecret":false})
    }).collect::<Vec<_>>();
    let item = form
        .pointer("/metadata/tool/id")
        .and_then(JsonValue::as_str)
        .zip(mid)
        .map(|(call, mid)| format!("{mid}:tool:{call}"));
    state
        .pending_requests
        .insert(id.to_owned(), OpenCodePendingRequestKind::Form);
    vec![RuntimeEvent::RequestOpened(RuntimeRequestOpened {
        native_request_id: id.to_owned(),
        native_request_id_json: Some(json!(id)),
        request_kind: "user_input".into(),
        native_thread_id: state.native_thread_id.clone(),
        native_turn_id: state.active_turn_id.clone(),
        native_item_id: item.clone(),
        payload_redacted: Some(
            json!({"questions":questions,"formFields":fields,"nativeRequestId":id,"nativeRequestIdJson":id,
            "threadId":state.native_thread_id,"turnId":state.active_turn_id,"itemId":item}),
        ),
        native,
    })]
}

// Keep the existing Pioneer question UI and durable answer contract. V2 gets
// typed answers keyed by the real form field keys, never positional V1 arrays.
pub(crate) fn opencode_form_answer(
    fields: &JsonValue,
    answers: &[Vec<String>],
) -> Result<JsonValue> {
    let fields = fields
        .as_array()
        .context("OpenCode V2 request is missing form fields")?;
    let mut result = JsonMap::new();
    let mut index = 0;
    for field in fields {
        let key = field["key"]
            .as_str()
            .context("OpenCode V2 field key is missing")?;
        let visible = field["when"]
            .as_array()
            .into_iter()
            .flatten()
            .all(|condition| {
                let current = result.get(condition["key"].as_str().unwrap_or(""));
                let matches = current.is_some_and(|v| {
                    if let Some(values) = v.as_array() {
                        values.contains(&condition["value"])
                    } else {
                        v == &condition["value"]
                    }
                });
                current.is_some()
                    && if condition["op"] == "neq" {
                        !matches
                    } else {
                        matches
                    }
            });
        if field["hidden"] == true {
            if visible && let Some(default) = field.get("default") {
                result.insert(key.to_owned(), default.clone());
            }
            continue;
        }
        let values = answers
            .get(index)
            .context("OpenCode V2 form answer is missing")?;
        index += 1;
        if !visible {
            continue;
        }
        let values = values
            .iter()
            .map(|answer| {
                field["options"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|o| o["label"] == *answer)
                    .and_then(|o| o["value"].as_str())
                    .unwrap_or(answer)
                    .to_owned()
            })
            .collect::<Vec<_>>();
        if values.is_empty() {
            if let Some(default) = field.get("default") {
                result.insert(key.to_owned(), default.clone());
                continue;
            }
            anyhow::ensure!(
                field["required"] != true,
                "OpenCode V2 required field `{key}` has no answer"
            );
            continue;
        }
        let value = match field["type"].as_str() {
            Some("string") => {
                anyhow::ensure!(
                    values.len() == 1,
                    "OpenCode V2 string field requires one answer"
                );
                json!(values[0])
            }
            Some("multiselect") => json!(values),
            Some("boolean") => {
                anyhow::ensure!(
                    values.len() == 1,
                    "OpenCode V2 boolean field requires one answer"
                );
                json!(
                    values[0]
                        .parse::<bool>()
                        .context("Enter true or false for the OpenCode form field")?
                )
            }
            Some("integer") => {
                anyhow::ensure!(
                    values.len() == 1,
                    "OpenCode V2 integer field requires one answer"
                );
                json!(
                    values[0]
                        .parse::<i64>()
                        .context("Enter an integer for the OpenCode form field")?
                )
            }
            Some("number") => {
                anyhow::ensure!(
                    values.len() == 1,
                    "OpenCode V2 number field requires one answer"
                );
                let n = values[0]
                    .parse::<f64>()
                    .context("Enter a number for the OpenCode form field")?;
                anyhow::ensure!(n.is_finite(), "OpenCode form requires a finite number");
                json!(n)
            }
            _ => bail!("OpenCode V2 form field type is not supported by Pioneer"),
        };
        if let Some(text) = value.as_str() {
            let length = text.encode_utf16().count() as u64;
            anyhow::ensure!(
                field["required"] != true || !text.is_empty(),
                "OpenCode V2 required field `{key}` is empty"
            );
            anyhow::ensure!(
                field["minLength"].as_u64().is_none_or(|n| length >= n),
                "OpenCode V2 field `{key}` is too short"
            );
            anyhow::ensure!(
                field["maxLength"].as_u64().is_none_or(|n| length <= n),
                "OpenCode V2 field `{key}` is too long"
            );
        }
        if let Some(number) = value.as_f64() {
            anyhow::ensure!(
                field["minimum"].as_f64().is_none_or(|n| number >= n),
                "OpenCode V2 field `{key}` is below its minimum"
            );
            anyhow::ensure!(
                field["maximum"].as_f64().is_none_or(|n| number <= n),
                "OpenCode V2 field `{key}` exceeds its maximum"
            );
        }
        if let Some(values) = value.as_array() {
            anyhow::ensure!(
                field["required"] != true || !values.is_empty(),
                "OpenCode V2 required field `{key}` is empty"
            );
            anyhow::ensure!(
                field["minItems"]
                    .as_u64()
                    .is_none_or(|n| values.len() as u64 >= n),
                "OpenCode V2 field `{key}` has too few selections"
            );
            anyhow::ensure!(
                field["maxItems"]
                    .as_u64()
                    .is_none_or(|n| values.len() as u64 <= n),
                "OpenCode V2 field `{key}` has too many selections"
            );
        }
        result.insert(key.to_owned(), value);
    }
    anyhow::ensure!(
        index == answers.len(),
        "OpenCode V2 form answer count differs from its fields"
    );
    Ok(JsonValue::Object(result))
}

fn opencode_tool_item_kind(tool_name: &str) -> String {
    let normalized = normalize_opencode_kind(tool_name);
    if tool_name.starts_with("pioneer_") {
        "mcpToolCall"
    } else if matches!(normalized.as_str(), "shell") {
        "commandExecution"
    } else if matches!(normalized.as_str(), "edit" | "write" | "patch") {
        "fileChange"
    } else if matches!(normalized.as_str(), "websearch" | "webfetch") {
        "webSearch"
    } else {
        "dynamicToolCall"
    }
    .to_owned()
}

fn opencode_tool_metadata(tool_name: &str, state: &JsonValue, status: &str) -> JsonValue {
    let input = state.get("input").cloned().unwrap_or_else(|| json!({}));
    let output = Some(opencode_tool_text(state));
    let error = state["error"]["message"].as_str().map(str::to_owned);
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
    metadata.insert("result".to_owned(), state["content"].clone());
    metadata.insert("message".to_owned(), json!(opencode_tool_text(state)));
    if let Some(error) = state.get("error") {
        metadata.insert("error".into(), error.clone());
    }
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
    if matches!(status, "completed" | "error") {
        let shell_failed = tool_name == "shell"
            && (state["metadata"]["exit"]
                .as_i64()
                .is_some_and(|exit| exit != 0)
                || state["metadata"]["timeout"] == true
                || state["metadata"]["signal"].is_string());
        metadata.insert(
            "success".to_owned(),
            json!(status == "completed" && !shell_failed),
        );
    }
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
    if let Some(diff) = opencode_file_diff_text(&state["metadata"]) {
        metadata.insert("diffPreview".into(), opencode_diff_preview(&diff));
    }
    if !changed_files.is_empty() {
        metadata.insert("changedFiles".to_owned(), json!(changed_files));
    }
    if let Some(output) = output {
        metadata.insert("aggregatedOutput".to_owned(), JsonValue::String(output));
    }
    if let Some(error) = error {
        metadata.insert("message".to_owned(), JsonValue::String(error));
    }
    if let Some(raw) = state["metadata"].as_object() {
        for key in [
            "signal",
            "truncated",
            "timeout",
            "shellID",
            "stdout",
            "stderr",
            "pioneerBackgroundShellRejected",
        ] {
            if let Some(value) = raw.get(key) {
                metadata.insert(key.to_owned(), value.clone());
            }
        }
    }
    if let Some(timed_out) = state.pointer("/metadata/timeout") {
        metadata.insert("timedOut".into(), timed_out.clone());
    }
    if let Some(exit_code) = state.pointer("/metadata/exit").and_then(JsonValue::as_i64) {
        metadata.insert("exitCode".to_owned(), exit_code.into());
    }
    if let Some(query) = input.get("query").and_then(JsonValue::as_str) {
        metadata.insert("query".to_owned(), JsonValue::String(query.to_owned()));
    }
    JsonValue::Object(metadata)
}

fn opencode_permission_request_kind(tool_name: &str) -> &'static str {
    match normalize_opencode_kind(tool_name).as_str() {
        "shell" | "subagent" => "command_approval",
        "edit" | "write" | "patch" => "file_change_approval",
        _ => "command_approval",
    }
}

fn command_from_opencode_input(input: &JsonValue) -> Option<String> {
    input
        .get("command")
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
}

fn changed_files_from_opencode_input(input: &JsonValue) -> Vec<String> {
    let mut paths = Vec::new();
    for key in ["filepath", "path"] {
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
                if let Some(path) = file["file"].as_str() {
                    paths.push(path.to_owned());
                }
            }
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

fn opencode_file_diff_text(metadata: &JsonValue) -> Option<String> {
    if let Some(diff) = metadata["diff"].as_str() {
        return Some(diff.to_owned());
    }
    let patches = metadata["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| f["patch"].as_str())
        .collect::<Vec<_>>();
    (!patches.is_empty()).then(|| patches.join("\n"))
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
                            codex_binding: None,
                            opencode_binding: Some((
                                self.supervisor.coordinator(),
                                attachment.bound_grant.clone(),
                            )),
                        }),
                        self.projection_generation,
                        self.projection.clone(),
                        self.facade_limits.clone(),
                        crate::cli_runtime::mcp::facade::CliMcpCallContract::OpenCode,
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
        self.native_items.clear();
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
            &opencode_managed_data_home(&self.runtime_home, &instance),
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
        let initialized: Result<Option<Arc<OpenCodeRequiredMcpBridge>>> = async {
            if let Some(projection) = launch_projection.as_ref() {
                let binary = instance.binary_path.clone();
                let version = server.version.clone();
                let (actual_contract, _) = pioneer_observability::work::spawn_blocking(move || {
                    crate::cli_runtime::mcp::readiness::opencode_executable_fingerprints(
                        &binary,
                        version.as_deref(),
                    )
                })
                .await
                .context("OpenCode executable attestation worker failed")??;
                anyhow::ensure!(
                    actual_contract == projection.provider_contract_fingerprint,
                    "OpenCode executable changed since the frozen MCP readiness contract"
                );
            }
            let models = server.api.models(request_timeout).await?;
            anyhow::ensure!(
                models.as_array().is_some_and(|m| !m.is_empty()),
                "OpenCode V2 has no available model provider"
            );
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
                    native_items: Arc::new(NativeMcpCorrelationLedger::default()),
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
            server
                .api
                .verify_catalog(launch_projection.as_ref(), request_timeout)
                .await?;
            Ok(required_mcp_bridge)
        }
        .await;
        let required_mcp_bridge = match initialized {
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
            dispatch: Mutex::new(()),
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
    event_tx: mpsc::Sender<pioneer_observability::work::WorkItem<RuntimeEvent>>,
    event_receivers: std::sync::Mutex<Option<CLIAgentRuntimeEventReceivers>>,
    event_stream: std::sync::Mutex<Option<JoinHandle<()>>>,
    cancellation: CancellationToken,
    required_mcp_bridge: Option<Arc<OpenCodeRequiredMcpBridge>>,
    stream_ready: watch::Receiver<bool>,
    mcp_launch_projection: Option<OpenCodeMcpSessionLaunchProjection>,
    pending_prompt: Mutex<Option<PendingOpenCodePrompt>>,
    dispatch: Mutex<()>,
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
            self.mcp_launch_projection.as_ref(),
        );
        let selected = model
            .as_ref()
            .map(|(provider, model)| opencode_model_ref(provider, model, None))
            .transpose()?;
        let raw = self
            .server
            .api
            .create_session(permissions, selected, timeout)
            .await?;
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
                .pointer("/location/directory")
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
        if self
            .server
            .api
            .session_status(timeout)
            .await?
            .get(native_thread_id)
            .is_none()
        {
            let inbox = self.server.api.inbox(native_thread_id, timeout).await?;
            for item in inbox.as_array().context("OpenCode V2 inbox is invalid")? {
                if item["type"] == "user"
                    && item.pointer("/payload/metadata/pioneer") == Some(&json!(true))
                {
                    let id = item["id"]
                        .as_str()
                        .context("OpenCode V2 inbox input has no identity")?;
                    self.server
                        .api
                        .cancel_input(native_thread_id, id, timeout)
                        .await?;
                }
            }
        }
        let permissions = opencode_permission_rules(
            params.approval_policy.as_deref(),
            self.mcp_launch_projection.as_ref(),
        );
        raw = self
            .server
            .api
            .update_session(
                native_thread_id,
                &json!({ "permissions": permissions }),
                timeout,
            )
            .await?;
        if let Some((provider, model)) = &model {
            self.server
                .api
                .json(
                    Method::POST,
                    &format!("/session/{native_thread_id}/model"),
                    Some(&json!({"model":opencode_model_ref(provider,model,None)?})),
                    timeout,
                )
                .await?;
            raw = self
                .server
                .api
                .get_session(native_thread_id, timeout)
                .await?;
        }
        let mut state = self.state.lock().await;
        state.native_thread_id = Some(native_thread_id.to_owned());
        state.last_model = model;
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: native_thread_id.to_owned(),
            cwd: raw
                .pointer("/location/directory")
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
        let native_turn_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
        let mut model = parse_optional_opencode_model(params.model.as_deref())?;
        let defer_for_mcp = self.required_mcp_bridge.is_some();
        {
            let mut state = self.state.lock().await;
            anyhow::ensure!(
                state.native_thread_id.as_deref() == Some(params.native_thread_id.as_str()),
                "OpenCode turn targets a session that is not open in this adapter"
            );
            anyhow::ensure!(
                state.active_turn_id.is_none() || state.terminal_emitted,
                "OpenCode session already has an active turn"
            );
            if model.is_none() {
                model = state.last_model.clone();
            }
            state.begin_turn(params.native_thread_id.clone(), native_turn_id.clone());
            if let Some(model) = model.as_ref() {
                state.last_model = Some(model.clone());
            }
        }
        let preparation = async {
            if defer_for_mcp {
                anyhow::ensure!(
                    *self.turn_prepared_for_mcp.lock().await,
                    "OpenCode MCP prompt requires a reserved turn before start"
                );
                anyhow::ensure!(
                    self.pending_prompt.lock().await.is_none(),
                    "OpenCode already has a deferred MCP prompt"
                );
            }
            let mut body = opencode_prompt_body(&self.server.api, &params.input, timeout).await?;
            body["id"] = json!(native_turn_id);
            body["delivery"] = json!("queue");
            body["metadata"] = json!({"pioneer":true});
            body["pioneerInstructions"] = json!(params.elevated_instructions.text());
            if model.is_none() && params.effort.as_deref().is_some_and(|v| !v.is_empty()) {
                let default = self
                    .server
                    .api
                    .data(Method::GET, "/model/default", None, timeout)
                    .await?;
                model = default["providerID"]
                    .as_str()
                    .zip(default["id"].as_str())
                    .map(|(p, m)| (p.to_owned(), m.to_owned()));
                anyhow::ensure!(
                    model.is_some(),
                    "OpenCode V2 cannot apply reasoning without an available model"
                );
            }
            body["pioneerModel"] = model
                .as_ref()
                .map(|(provider, model)| {
                    opencode_model_ref(provider, model, params.effort.as_deref())
                })
                .transpose()?
                .unwrap_or(JsonValue::Null);
            Ok::<_, anyhow::Error>(body)
        };
        let body = match tokio::select! {
            _=self.cancellation.cancelled()=>Err(anyhow!("OpenCode V2 preparation cancelled")),
            result=tokio_timeout(timeout,preparation)=>result.context("OpenCode V2 input preparation timed out").and_then(|r|r),
        } {
            Ok(body) => body,
            Err(error) => {
                self.record_prompt_delivery_failure(
                    &params.native_thread_id,
                    &native_turn_id,
                    &error,
                )
                .await;
                return Err(error);
            }
        };
        if defer_for_mcp {
            let mut pending = self.pending_prompt.lock().await;
            let state = self.state.lock().await;
            anyhow::ensure!(
                !state.interrupted && !state.terminal_emitted && self.is_usable(),
                "OpenCode V2 prompt cancelled before MCP activation"
            );
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
        let _dispatch = self.dispatch.lock().await;
        let id = native_request_id
            .as_str()
            .context("OpenCode V2 request ID must be a string")?;
        let (session, kind) = {
            let state = self.state.lock().await;
            anyhow::ensure!(
                !state.interrupted && !state.terminal_emitted,
                "OpenCode V2 interaction belongs to a stopped turn"
            );
            (
                state
                    .native_thread_id
                    .clone()
                    .context("OpenCode V2 session is not open")?,
                *state
                    .pending_requests
                    .get(id)
                    .context("Unknown or resolved OpenCode V2 request")?,
            )
        };
        let delivery: Result<()> = async {
            match kind {
                OpenCodePendingRequestKind::Permission => {
                    let reply = response["reply"].as_str().unwrap_or("reject");
                    anyhow::ensure!(
                        matches!(reply, "once" | "reject"),
                        "OpenCode V2 only accepts request-scoped permission decisions"
                    );
                    self.server
                        .api
                        .permission_reply(
                            &session,
                            id,
                            reply,
                            response["message"].as_str(),
                            self.request_timeout,
                        )
                        .await?;
                }
                OpenCodePendingRequestKind::Form => {
                    if response["reject"] == true {
                        self.server
                            .api
                            .form_cancel(&session, id, self.request_timeout)
                            .await?;
                    } else {
                        self.server
                            .api
                            .form_reply(
                                &session,
                                id,
                                response
                                    .get("answer")
                                    .context("OpenCode V2 form answer is missing")?,
                                self.request_timeout,
                            )
                            .await?;
                    }
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = delivery {
            // A rejected/uncertain answer cannot leave a native request that
            // the durable Pioneer UI already considers answered waiting forever.
            let _ = self
                .server
                .api
                .abort_session(&session, self.request_timeout)
                .await;
            let shutdown = self.close().await;
            let events = {
                let mut state = self.state.lock().await;
                state.last_error = Some(
                    json!({"type":"InteractionDeliveryFailed","message":bounded_safe_detail(&error.to_string())}),
                );
                terminalize_opencode_failure(&mut state, None)
            };
            for event in events {
                let _ = self
                    .event_tx
                    .send(pioneer_observability::work::WorkItem::new(
                        event,
                        pioneer_observability::work::Boundary::EventLane,
                        pioneer_observability::work::WorkKind::Event,
                        pioneer_observability::work::ExecutorRole::Event,
                    ))
                    .await;
            }
            if let Err(shutdown) = shutdown {
                return Err(error.context(format!(
                    "OpenCode V2 interaction cleanup failed: {}",
                    bounded_safe_detail(&shutdown.to_string())
                )));
            }
            return Err(error);
        }
        self.state.lock().await.pending_requests.remove(id);
        Ok(())
    }

    async fn fail_request(
        &self,
        id: JsonValue,
        _code: i64,
        message: String,
        _data: Option<JsonValue>,
    ) -> Result<()> {
        self.respond_to_request(
            id,
            json!({"reply":"reject","reject":true,"message":message}),
        )
        .await
    }

    async fn interrupt_turn(
        &self,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> Result<()> {
        {
            let mut state = self.state.lock().await;
            if native_thread_id.is_some_and(|id| state.native_thread_id.as_deref() != Some(id))
                || native_turn_id.is_some_and(|id| state.active_turn_id.as_deref() != Some(id))
            {
                bail!("OpenCode interrupt targets a non-current turn");
            }
            if state.terminal_emitted {
                return Ok(());
            }
            // Set before waiting for dispatch: an in-flight sender must observe Stop.
            state.interrupted = true;
        }
        let _dispatch = self.dispatch.lock().await;
        self.pending_prompt.lock().await.take();
        let (session, turn) = {
            let state = self.state.lock().await;
            (state.native_thread_id.clone(), state.active_turn_id.clone())
        };
        let stopped: Result<()> = async {
            if let Some(session) = &session {
                self.server
                    .api
                    .abort_session(session, self.request_timeout)
                    .await?;
                if let Some(turn) = &turn {
                    self.server
                        .api
                        .cancel_input(session, turn, self.request_timeout)
                        .await?;
                }
                self.cancel_pending_requests(session).await?;
                let deadline = tokio::time::Instant::now() + self.request_timeout;
                loop {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    anyhow::ensure!(
                        !remaining.is_zero(),
                        "OpenCode V2 execution did not settle after interrupt"
                    );
                    if self
                        .server
                        .api
                        .session_status(remaining)
                        .await?
                        .get(session)
                        .is_none()
                    {
                        break;
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            }
            Ok(())
        }
        .await;
        // Close the isolated owner after interrupt as well: background shell
        // jobs may otherwise schedule a later synthetic continuation.
        let shutdown = self
            .close()
            .await
            .context("OpenCode V2 Stop could not terminate its process owner");
        if let Err(error) = stopped {
            tracing::debug!(error = %bounded_safe_detail(&error.to_string()), "OpenCode V2 Stop used process termination after API cleanup failed");
        }
        let events = {
            let mut state = self.state.lock().await;
            complete_opencode_turn(&mut state, "interrupted", None)
        };
        for event in events {
            let _ = self
                .event_tx
                .send(pioneer_observability::work::WorkItem::new(
                    event,
                    pioneer_observability::work::Boundary::EventLane,
                    pioneer_observability::work::WorkKind::Event,
                    pioneer_observability::work::ExecutorRole::Event,
                ))
                .await;
        }
        // Resolve the durable waiters even when shutdown itself reports an
        // error. The caller still receives that failure instead of success.
        shutdown
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
            return Ok(if matches!(status, Some("running")) {
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
        {
            let mut state = self.state.lock().await;
            if state.active_turn_id.is_none() {
                state.begin_turn(native_thread_id.to_owned(), native_turn_id.to_owned());
            }
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
                    let mut restored = terminal.clone();
                    restored.reconciliation_events.extend(
                        terminal_events
                            .iter()
                            .filter(|e| {
                                !matches!(
                                    e,
                                    RuntimeEvent::TurnCompleted(_)
                                        | RuntimeEvent::TurnFailed(_)
                                        | RuntimeEvent::TurnInterrupted(_)
                                        | RuntimeEvent::ItemDelta(_)
                                )
                            })
                            .cloned(),
                    );
                    observation = Some(restored);
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
        let current = {
            let state = self.state.lock().await;
            state.native_thread_id.as_deref() == Some(native_thread_id)
                && state.active_turn_id.as_deref() == Some(native_turn_id)
        };
        if current && !live {
            let mut state = self.state.lock().await;
            let _ = project_opencode_messages(&mut state, &messages);
            let _ = opencode_snapshot_terminal(&mut state, &messages, false);
        }
        if live
            && current
            && let Some(observation) = observation.as_mut()
        {
            let events =
                reconcile_opencode_turn(&self.server, &self.state, self.request_timeout, true)
                    .await?;
            observation
                .reconciliation_events
                .extend(events.into_iter().filter(|e| {
                    matches!(
                        e,
                        RuntimeEvent::RequestOpened(_) | RuntimeEvent::RequestResolved(_)
                    )
                }));
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
        let session = &request.native_thread_id;
        validate_opencode_id(session, "session")?;
        let id = format!("msg_{}", uuid::Uuid::new_v4().simple());
        let raw = self
            .server
            .api
            .data(
                Method::POST,
                &format!("/session/{session}/compact"),
                Some(&json!({"id":id})),
                self.request_timeout,
            )
            .await?;
        let completed = tokio_timeout(self.request_timeout, async {
            loop {
                let messages=self.server.api.session_messages(session,self.request_timeout).await?;
                if let Some(compaction)=messages.as_array().into_iter().flatten().find(|m| m["id"]==id) {
                    match compaction["status"].as_str() {
                        Some("completed")=>return Ok::<_,anyhow::Error>(compaction.clone()),
                        Some("failed")=>bail!("OpenCode V2 compaction failed: {}",compaction["error"]["message"]),
                        _=>{},
                    }
                }
                tokio::select! { _=self.cancellation.cancelled()=>bail!("OpenCode V2 compaction cancelled"), _=sleep(Duration::from_millis(100))=>{} }
            }
        }).await.context("OpenCode V2 compaction timed out").and_then(|r| r);
        let completed = match completed {
            Ok(completed) => completed,
            Err(error) => {
                let _ = self
                    .server
                    .api
                    .abort_session(session, self.request_timeout)
                    .await;
                let _ = self
                    .server
                    .api
                    .cancel_input(session, &id, self.request_timeout)
                    .await;
                self.close().await?;
                return Err(error);
            }
        };
        Ok(CLIAgentRuntimeThreadCompactResult {
            native_thread_id: request.native_thread_id,
            raw: Some(
                json!({"admission":raw,"usage":opencode_usage_metadata([&completed]),"compaction":completed}),
            ),
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
            let turn_messages = opencode_turn_messages(&messages, boundary);
            let last = turn_messages
                .last()
                .and_then(|m| m["id"].as_str())
                .context("OpenCode V2 fork boundary is missing")?;
            let all = messages
                .as_array()
                .context("OpenCode V2 history is invalid")?;
            let index = all
                .iter()
                .position(|m| m["id"] == last)
                .context("OpenCode V2 fork boundary is missing")?;
            all.get(index + 1)
                .and_then(|m| m["id"].as_str())
                .map(str::to_owned)
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
                .pointer("/location/directory")
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
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let mut cursor: Option<String> = None;
        let mut cursors = HashSet::new();
        let mut matches = Vec::new();
        loop {
            let mut url = self.server.api.url_with_segments(&["session"])?;
            url.query_pairs_mut()
                .append_pair("search", marker)
                .append_pair("limit", "100")
                .append_pair("directory", &self.server.api.directory);
            if let Some(cursor) = &cursor {
                url.query_pairs_mut().append_pair("cursor", cursor);
            }
            let page = self
                .server
                .api
                .json_url(
                    Method::GET,
                    url,
                    "marked fork lookup",
                    None,
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await?;
            matches.extend(
                page["data"]
                    .as_array()
                    .context("OpenCode V2 session page is invalid")?
                    .iter()
                    .filter(|s| s["title"] == marker && s["fork"]["sessionID"] == source_thread_id)
                    .cloned(),
            );
            anyhow::ensure!(matches.len() <= 1, "OpenCode V2 fork marker is ambiguous");
            cursor = page
                .pointer("/cursor/next")
                .and_then(JsonValue::as_str)
                .map(str::to_owned);
            let Some(next) = &cursor else {
                break;
            };
            anyhow::ensure!(
                cursors.insert(next.clone()),
                "OpenCode V2 returned a repeated session cursor"
            );
        }
        if matches.len() != 1 {
            // Create and title PATCH are separate official operations. Absence
            // cannot prove the fork was never created; retain the existing
            // durable intent instead of discarding it and duplicating a fork.
            bail!("OpenCode marked fork outcome is unknown; durable intent is retained");
        }
        let session = &matches[0];
        let id = session["id"]
            .as_str()
            .context("OpenCode marked fork identity is missing")?;
        validate_opencode_id(id, "marked fork session")?;
        if id == source_thread_id {
            bail!("OpenCode marked fork points to its source");
        }
        Ok(Some(CLIAgentRuntimeThreadForkResult {
            native_thread_id: id.to_owned(),
            native_cwd: session["location"]["directory"].as_str().map(str::to_owned),
            native_model: None,
            raw: Some(session.clone()),
        }))
    }
}

impl OpenCodeCLIAgentRuntimeSession {
    async fn send_prompt(
        &self,
        session: &str,
        turn: &str,
        body: &JsonValue,
        timeout: Duration,
    ) -> Result<()> {
        let _dispatch = self.dispatch.lock().await;
        wait_opencode_subscription(self.stream_ready.clone(), &self.cancellation, timeout).await?;
        {
            let mut state = self.state.lock().await;
            anyhow::ensure!(
                !state.interrupted && !state.terminal_emitted,
                "OpenCode V2 prompt cancelled before admission"
            );
            state.dispatch_deadline = tokio::time::Instant::now().checked_add(timeout);
        }
        let mut prompt = body.clone();
        let instructions = prompt
            .as_object_mut()
            .unwrap()
            .remove("pioneerInstructions")
            .unwrap_or(JsonValue::Null);
        let model = prompt
            .as_object_mut()
            .unwrap()
            .remove("pioneerModel")
            .unwrap_or(JsonValue::Null);
        self.server
            .api
            .ensure_mcp_isolation(self.required_mcp_bridge.is_some(), timeout)
            .await?;
        self.server
            .api
            .verify_catalog(self.mcp_launch_projection.as_ref(), timeout)
            .await?;
        self.server
            .api
            .configure_turn(
                session,
                instructions.as_str().unwrap_or(""),
                (!model.is_null()).then_some(&model),
                timeout,
            )
            .await?;
        if self.state.lock().await.interrupted {
            bail!("OpenCode V2 prompt cancelled before admission");
        }
        if let Err(error) = self.server.api.prompt(session, &prompt, timeout).await {
            // Never resend on an uncertain response. Stop the owning process;
            // subsequent recovery reads this exact message ID from V2 history.
            self.close()
                .await
                .context("failed to close uncertain OpenCode V2 dispatch")?;
            return Err(error);
        }
        let mut state = self.state.lock().await;
        state.user_observed = true;
        let emit = !state.turn_started && !state.terminal_emitted;
        state.turn_started = true;
        drop(state);
        if emit {
            self.event_tx
                .send(pioneer_observability::work::WorkItem::new(
                    RuntimeEvent::TurnStarted(RuntimeTurnStarted {
                        native_thread_id: Some(session.to_owned()),
                        native_turn_id: turn.to_owned(),
                        native: None,
                    }),
                    pioneer_observability::work::Boundary::EventLane,
                    pioneer_observability::work::WorkKind::Event,
                    pioneer_observability::work::ExecutorRole::Event,
                ))
                .await
                .map_err(|_| anyhow!("OpenCode event channel is closed"))?;
        }
        Ok(())
    }

    async fn cancel_pending_requests(&self, session: &str) -> Result<()> {
        let permissions = self
            .server
            .api
            .data(
                Method::GET,
                &format!("/session/{session}/permission"),
                None,
                self.request_timeout,
            )
            .await?;
        let forms = self
            .server
            .api
            .data(
                Method::GET,
                &format!("/session/{session}/form"),
                None,
                self.request_timeout,
            )
            .await?;
        for request in permissions
            .as_array()
            .context("OpenCode V2 permission list is invalid")?
        {
            let id = request["id"]
                .as_str()
                .context("OpenCode V2 permission ID is missing")?;
            self.server
                .api
                .permission_reply(
                    session,
                    id,
                    "reject",
                    Some("Stopped by Pioneer"),
                    self.request_timeout,
                )
                .await?;
        }
        for form in forms
            .as_array()
            .context("OpenCode V2 form list is invalid")?
        {
            let id = form["id"]
                .as_str()
                .context("OpenCode V2 form ID is missing")?;
            self.server
                .api
                .form_cancel(session, id, self.request_timeout)
                .await?;
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
            || state.interrupted
        {
            return;
        }
        let message = bounded_safe_detail(format!("{error:#}").as_str());
        state.terminal_emitted = true;
        state.last_turn_observation = Some(CLIAgentRuntimeTurnObservation {
            status: if state.interrupted {
                CLIAgentRuntimeObservedTurnStatus::Interrupted
            } else {
                CLIAgentRuntimeObservedTurnStatus::Failed
            },
            message: Some(message),
            reconciliation_events: Vec::new(),
        });
    }
}

pub(super) fn opencode_catalog_matches(
    catalog: &JsonValue,
    projection: Option<&OpenCodeMcpSessionLaunchProjection>,
) -> Result<bool> {
    let items = catalog
        .as_array()
        .context("OpenCode V2 catalog must be an array")?;
    let expected = projection
        .map(|p| p.canonical_projection.tools.as_slice())
        .unwrap_or_default();
    if items.len() != expected.len() {
        return Ok(false);
    }
    let mut seen = HashSet::new();
    for item in items {
        let Some(id) = item["id"].as_str() else {
            return Ok(false);
        };
        if !seen.insert(id)
            || item["namespace"] != OPENCODE_MCP_SERVER_NAME
            || item["codemode"] != false
        {
            return Ok(false);
        }
        let Some(tool) = expected
            .iter()
            .find(|t| format!("pioneer_{}", t.canonical_callable_name) == id)
        else {
            return Ok(false);
        };
        if crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&item["input"])?
            != crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&tool.input_schema)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn opencode_permission_rules(
    approval_policy: Option<&str>,
    projection: Option<&OpenCodeMcpSessionLaunchProjection>,
) -> JsonValue {
    let policy = approval_policy.unwrap_or("ask");
    let full_access = matches!(policy, "allow" | "never" | "bypassPermissions");
    let auto_edits = matches!(policy, "auto_accept_edits" | "acceptEdits");
    let mut rules = vec![json!({"action":"*","resource":"*","effect":"deny"})];
    for action in OPENCODE_BUILTIN_ACTIONS {
        rules.push(json!({"action":action,"resource":"*","effect":
            if full_access || OPENCODE_SAFE_ACTIONS.contains(action) || (auto_edits && *action == "edit") { "allow" } else { "ask" }}));
    }
    for action in ["external_directory", "doom_loop"] {
        rules.push(json!({"action":action,"resource":"*","effect":if full_access { "allow" } else { "ask" }}));
    }
    // Native subagents bypass Pioneer's child-task lifecycle. Only the frozen
    // facade names below may delegate work. CodeMode stays denied as well.
    rules.push(json!({"action":"subagent","resource":"*","effect":"deny"}));
    rules.push(json!({"action":"skill","resource":"*","effect":"deny"}));
    if let Some(projection) = projection {
        for tool in &projection.canonical_projection.tools {
            rules.push(json!({"action":format!("pioneer_{}", tool.canonical_callable_name),"resource":"*","effect":"allow"}));
        }
    }
    json!(rules)
}

fn opencode_model_ref(provider: &str, model: &str, effort: Option<&str>) -> Result<JsonValue> {
    let (id, embedded_variant) = model
        .split_once('#')
        .map(|(id, variant)| (id, Some(variant)))
        .unwrap_or((model, None));
    anyhow::ensure!(
        !provider.is_empty() && !provider.contains('#') && !id.is_empty(),
        "Invalid OpenCode V2 model reference"
    );
    let mut result = json!({"providerID":provider,"id":id});
    if let Some(variant) = effort.filter(|v| !v.trim().is_empty()).or(embedded_variant) {
        anyhow::ensure!(
            !variant.is_empty() && !variant.contains('#'),
            "Invalid OpenCode V2 model variant"
        );
        result["variant"] = json!(variant);
    }
    Ok(result)
}

async fn opencode_prompt_body(
    api: &OpenCodeApiClient,
    input: &JsonValue,
    timeout: Duration,
) -> Result<JsonValue> {
    let items = input
        .as_array()
        .context("OpenCode input must be an array")?;
    let mut text = Vec::new();
    let mut files = Vec::new();
    let mut skills = Vec::new();
    let mut catalog = None;
    for item in items {
        match item["type"].as_str() {
            Some("text") => text.push(
                item["text"]
                    .as_str()
                    .context("OpenCode text is missing")?
                    .to_owned(),
            ),
            Some("image") => {
                let url = item["url"]
                    .as_str()
                    .context("OpenCode image URL is missing")?;
                files.push(json!({"uri":opencode_attachment_uri(url, timeout).await?}));
            }
            Some("localImage") => {
                let path = item["path"]
                    .as_str()
                    .context("OpenCode image path is missing")?;
                let path = tokio::fs::canonicalize(path).await?;
                let uri = reqwest::Url::from_file_path(path)
                    .map_err(|_| anyhow!("OpenCode attachment path is invalid"))?;
                files.push(json!({"uri":uri.as_str()}));
            }
            Some("skill") => {
                if catalog.is_none() {
                    catalog = Some(api.data(Method::GET, "/skill", None, timeout).await?);
                }
                let path = item["path"]
                    .as_str()
                    .context("OpenCode selected skill path is missing")?;
                let path = PathBuf::from(path);
                let path = if path.file_name().is_some_and(|n| n == "SKILL.md") {
                    path
                } else {
                    path.join("SKILL.md")
                };
                let canonical = tokio::fs::canonicalize(&path)
                    .await
                    .context("OpenCode selected SKILL.md is not available")?;
                let mut matches = Vec::new();
                for skill in catalog
                    .as_ref()
                    .unwrap()
                    .as_array()
                    .context("OpenCode V2 skill catalog is invalid")?
                {
                    if let Some(path) = skill["path"].as_str()
                        && tokio::fs::canonicalize(path).await.ok().as_ref() == Some(&canonical)
                    {
                        matches.push(skill);
                    }
                }
                anyhow::ensure!(
                    matches.len() == 1,
                    "OpenCode V2 did not discover the exact selected skill; restart its managed session after installation"
                );
                let skill = matches[0];
                anyhow::ensure!(
                    skill["content"]
                        .as_str()
                        .is_some_and(|c| !c.trim().is_empty()),
                    "OpenCode V2 selected skill has no usable content"
                );
                skills.push(json!({"id":skill["id"]}));
            }
            Some(kind) => bail!("Unsupported OpenCode input item `{kind}`"),
            None => bail!("OpenCode input item is missing its type"),
        }
    }
    anyhow::ensure!(
        !text.is_empty() || !files.is_empty() || !skills.is_empty(),
        "OpenCode turn input is empty"
    );
    Ok(json!({"text":text.join("\n"),"files":files,"skills":skills}))
}

async fn opencode_attachment_uri(value: &str, timeout: Duration) -> Result<String> {
    const MAX_BYTES: usize = 25 * 1024 * 1024;
    let url = reqwest::Url::parse(value).context("OpenCode attachment URI is invalid")?;
    match url.scheme() {
        "data" | "file" => Ok(value.to_owned()),
        "http" | "https" => {
            // V2 accepts data/file URIs, not remote URLs. Materialize the
            // existing Pioneer remote-image input without asking the model.
            let response = reqwest::Client::new()
                .get(url)
                .timeout(timeout)
                .send()
                .await?
                .error_for_status()?;
            let mime = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/png")
                .to_owned();
            let mut bytes = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                anyhow::ensure!(
                    bytes.len().saturating_add(chunk.len()) <= MAX_BYTES,
                    "OpenCode image exceeds the 25 MiB adapter limit"
                );
                bytes.extend_from_slice(&chunk);
            }
            Ok(format!(
                "data:{mime};base64,{}",
                base64::engine::general_purpose::STANDARD.encode(bytes)
            ))
        }
        _ => bail!("OpenCode V2 attachment URI scheme is unsupported"),
    }
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

// V2 history is ordered by durable projection sequence, not lexical IDs.
// The root input owns messages up to the next user input; automatic compaction
// and synthetic continuations stay inside that boundary. The first idle is the
// execution's explicit outcome and ends its ownership (including shutdown recovery).
fn opencode_turn_messages<'a>(messages: &'a JsonValue, root: &str) -> Vec<&'a JsonValue> {
    let Some(all) = messages.as_array() else {
        return Vec::new();
    };
    let Some(start) = all
        .iter()
        .position(|m| m["id"] == root && m["type"] == "user")
    else {
        return Vec::new();
    };
    let mut result = Vec::new();
    for message in &all[start..] {
        if !result.is_empty() && message["type"] == "user" {
            break;
        }
        result.push(message);
        if message["type"] == "idle" {
            break;
        }
    }
    result
}

fn refresh_opencode_message_ownership(state: &mut OpenCodeRuntimeState, messages: &JsonValue) {
    let Some(root) = state.active_turn_id.clone() else {
        return;
    };
    let owned = opencode_turn_messages(messages, &root);
    state.user_observed = !owned.is_empty();
    for message in owned.iter().filter(|m| m["type"] == "assistant") {
        if let Some(id) = message["id"].as_str() {
            state.assistant_message_ids.insert(id.to_owned());
            for content in message["content"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|c| c["type"] == "tool")
            {
                let key = opencode_content_id(id, content, 0);
                // Live progress is ephemeral and may be ahead of REST; only
                // hydrate absent calls or authoritative terminal snapshots.
                if !state.tool_contents.contains_key(&key)
                    || matches!(
                        content["state"]["status"].as_str(),
                        Some("completed" | "error")
                    )
                {
                    state.tool_contents.insert(key, content.clone());
                }
            }
        }
    }
    if owned
        .iter()
        .any(|m| matches!(m["type"].as_str(), Some("assistant" | "idle")))
    {
        state.observe_execution();
    }
}

// V2 categories are exclusive: cache is separate from input and reasoning
// separate from visible output. Pioneer totals include each category once.
pub(crate) fn opencode_usage_metadata<'a>(
    messages: impl IntoIterator<Item = &'a JsonValue>,
) -> JsonValue {
    let mut input = None;
    let mut output = None;
    let mut reasoning = None;
    let mut read = None;
    let mut write = None;
    let mut reported = false;
    fn add(total: &mut Option<u64>, value: Option<u64>) {
        if let Some(value) = value {
            *total = Some(total.unwrap_or(0).saturating_add(value));
        }
    }
    fn sum(values: &[Option<u64>]) -> Option<u64> {
        values.iter().flatten().copied().reduce(u64::saturating_add)
    }
    for message in messages {
        if !matches!(message["type"].as_str(), Some("assistant" | "compaction")) {
            continue;
        }
        let tokens = &message["tokens"];
        reported |= tokens.is_object();
        add(&mut input, tokens["input"].as_u64());
        add(&mut output, tokens["output"].as_u64());
        add(&mut reasoning, tokens["reasoning"].as_u64());
        add(&mut read, tokens["cache"]["read"].as_u64());
        add(&mut write, tokens["cache"]["write"].as_u64());
    }
    json!({
        "input_tokens":sum(&[input, read, write]),
        "output_tokens":sum(&[output, reasoning]),
        "uncached_input_tokens":input,
        "visible_output_tokens":output,
        "reasoning_tokens":reasoning,
        "cache_read_input_tokens":read,
        "cache_creation_input_tokens":write,
        "semantics":"opencode_v2_exclusive_cache_separate_reasoning",
        "native_tokens":reported.then(||json!({"input":input,"output":output,"reasoning":reasoning,"cache":{"read":read,"write":write}})),
    })
}

fn project_opencode_messages(
    state: &mut OpenCodeRuntimeState,
    messages: &JsonValue,
) -> Vec<RuntimeEvent> {
    refresh_opencode_message_ownership(state, messages);
    let Some(root) = state.active_turn_id.clone() else {
        return Vec::new();
    };
    let mut events = Vec::new();
    for message in opencode_turn_messages(messages, &root) {
        if message["type"] != "assistant" {
            continue;
        }
        let Some(mid) = message["id"].as_str() else {
            continue;
        };
        let completed = message["time"]["completed"].is_number();
        let mut text_ordinal = 0;
        let mut reasoning_ordinal = 0;
        for content in message["content"].as_array().into_iter().flatten() {
            let ordinal = if content["type"] == "text" {
                let n = text_ordinal;
                text_ordinal += 1;
                n
            } else if content["type"] == "reasoning" {
                let n = reasoning_ordinal;
                reasoning_ordinal += 1;
                n
            } else {
                0
            };
            let id = opencode_content_id(mid, content, ordinal);
            // Never overwrite a streamed prefix with REST's in-flight placeholder.
            if !completed
                && content["type"] != "tool"
                && state
                    .items
                    .get(&id)
                    .is_some_and(|i| i.text.len() > content["text"].as_str().unwrap_or("").len())
            {
                continue;
            }
            events.extend(project_opencode_content(
                state, &id, content, completed, None,
            ));
        }
    }
    let usage = opencode_usage_metadata(opencode_turn_messages(messages, &root));
    if usage["native_tokens"].is_object() {
        state.last_usage = Some(usage);
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
    let Some(root) = state.active_turn_id.clone() else {
        return Vec::new();
    };
    let owned = opencode_turn_messages(messages, &root);
    let idle = owned.iter().rev().find(|m| m["type"] == "idle");
    if let Some(idle) = idle {
        match idle["outcome"].as_str() {
            Some("succeeded") => complete_opencode_turn(state, "completed", None),
            Some("interrupted") => {
                state.interrupted = true;
                complete_opencode_turn(state, "interrupted", None)
            }
            _ => {
                state.last_error = Some(idle["error"].clone());
                terminalize_opencode_failure(state, None)
            }
        }
    } else if state.interrupted {
        complete_opencode_turn(state, "interrupted", None)
    } else {
        if let Some(error) = owned
            .iter()
            .rev()
            .find_map(|m| m.get("error"))
            .filter(|e| !e.is_null())
        {
            state.last_error = Some(error.clone());
        }
        terminalize_opencode_failure(state, None)
    }
}

fn opencode_observation_from_messages(
    session: &str,
    turn: &str,
    messages: &JsonValue,
    live: bool,
) -> Option<CLIAgentRuntimeTurnObservation> {
    if opencode_turn_messages(messages, turn).is_empty() {
        return live.then(|| CLIAgentRuntimeTurnObservation {
            status: CLIAgentRuntimeObservedTurnStatus::InProgress,
            message: None,
            reconciliation_events: Vec::new(),
        });
    }
    let mut state = OpenCodeRuntimeState::default();
    state.begin_turn(session.to_owned(), turn.to_owned());
    let mut events = project_opencode_messages(&mut state, messages);
    events.extend(opencode_snapshot_terminal(&mut state, messages, live));
    let mut observation = state.last_turn_observation.unwrap();
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

struct OpenCodeTurnActivity {
    messages: JsonValue,
    native_running: bool,
    parked: bool,
    history_changed: bool,
}

impl OpenCodeTurnActivity {
    fn has_later_input(&self, turn: &str) -> bool {
        self.messages.as_array().is_some_and(|all| {
            all.iter()
                .position(|m| m["id"] == turn)
                .is_some_and(|i| all[i + 1..].iter().any(|m| m["type"] == "user"))
        })
    }

    fn assess(&self, state: &mut OpenCodeRuntimeState, session: &str, turn: &str) -> (bool, bool) {
        let owned = opencode_turn_messages(&self.messages, turn);
        let terminal = owned.iter().any(|m| m["type"] == "idle");
        let current_input = !owned.is_empty() && !terminal && !self.has_later_input(turn);
        let mut live = (self.native_running && (current_input || self.parked))
            || (self.history_changed && current_input);
        if state.native_thread_id.as_deref() != Some(session)
            || state.active_turn_id.as_deref() != Some(turn)
        {
            return (live, false);
        }
        refresh_opencode_message_ownership(state, &self.messages);
        // The coordinator runs before the first assistant step exists. A
        // promoted current input + native running is execution evidence; a
        // parked input behind another operation is not.
        if self.native_running && current_input && !self.parked {
            state.observe_execution();
        }
        let before_deadline = state.awaiting_execution
            && state
                .dispatch_deadline
                .is_some_and(|d| tokio::time::Instant::now() < d);
        live |= !terminal
            && !self.has_later_input(turn)
            && (before_deadline || (self.parked && state.dispatch_deadline.is_some()));
        let expired = state.awaiting_execution
            && !state.execution_observed
            && !state.terminal_emitted
            && !state.interrupted
            && !terminal
            && !self.has_later_input(turn)
            // Never interrupt the owner while it reports native activity,
            // including activity not yet attributable to this parked input.
            && !self.native_running
            && !self.history_changed
            && state.pending_requests.is_empty()
            && state.dispatch_deadline.is_some_and(|d| tokio::time::Instant::now() >= d);
        (live, expired)
    }
}

async fn read_opencode_activity(
    api: &OpenCodeApiClient,
    session: &str,
    turn: &str,
    timeout: Duration,
) -> Result<OpenCodeTurnActivity> {
    let before = api.session_messages(session, timeout).await?;
    let inbox = api.inbox(session, timeout).await?;
    let parked = inbox
        .as_array()
        .context("OpenCode V2 inbox is invalid")?
        .iter()
        .any(|i| i["id"] == turn);
    let mut native_running = api.session_status(timeout).await?.get(session).is_some();
    let mut history_changed = false;
    let messages = if native_running {
        before
    } else {
        let messages = api.session_messages(session, timeout).await?;
        history_changed = messages != before;
        // Last HTTP observation: do not close based on an active-map read
        // taken before the inbox/history awaits. Changing history defers a
        // timeout decision even if this busy period has already ended.
        native_running = api.session_status(timeout).await?.get(session).is_some();
        messages
    };
    Ok(OpenCodeTurnActivity {
        messages,
        native_running,
        parked,
        history_changed,
    })
}

async fn read_opencode_turn_snapshot(
    server: &OpenCodeServerProcess,
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    session: &str,
    turn: &str,
    timeout: Duration,
) -> Result<(JsonValue, bool, Vec<RuntimeEvent>)> {
    let api = &server.api;
    let mut activity = read_opencode_activity(api, session, turn, timeout).await?;
    let (mut live, expired) = activity.assess(&mut *state.lock().await, session, turn);
    if expired {
        let permissions = api
            .data(
                Method::GET,
                &format!("/session/{session}/permission"),
                None,
                timeout,
            )
            .await?;
        let forms = api
            .data(
                Method::GET,
                &format!("/session/{session}/form"),
                None,
                timeout,
            )
            .await?;
        let requests_empty = permissions
            .as_array()
            .context("OpenCode V2 permissions are invalid")?
            .is_empty()
            && forms
                .as_array()
                .context("OpenCode V2 forms are invalid")?
                .is_empty();
        // HTTP checks may have yielded to the provider, SSE, another turn or
        // Stop. Refresh native evidence, then revalidate local ownership too.
        activity = read_opencode_activity(api, session, turn, timeout).await?;
        let assessed = activity.assess(&mut *state.lock().await, session, turn);
        live = assessed.0;
        if requests_empty && assessed.1 {
            // Cancel only this admission first. Do not interrupt a session on
            // an old timeout decision. Promotion racing DELETE is resolved by
            // the final native/local read, not by resending the prompt.
            let _ = api.cancel_input(session, turn, timeout).await;
            activity = read_opencode_activity(api, session, turn, timeout).await?;
            let events = {
                let mut snapshot = state.lock().await;
                let assessed = activity.assess(&mut snapshot, session, turn);
                live = assessed.0;
                if assessed.1 {
                    // Commit the decision under the same lock as progress and
                    // Stop; make this owner unavailable before releasing it.
                    server
                        .closed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    let mut events = project_opencode_messages(&mut snapshot, &activity.messages);
                    snapshot.last_error = Some(
                        json!({"type":"PromptStartupTimeout","message":"OpenCode V2 admitted no execution before the dispatch deadline"}),
                    );
                    events.extend(terminalize_opencode_failure(&mut snapshot, None));
                    Some(events)
                } else {
                    None
                }
            };
            if let Some(events) = events {
                if let Err(error) = server.close().await {
                    tracing::error!(error = %bounded_safe_detail(&error.to_string()), "OpenCode V2 startup timeout owner cleanup failed");
                }
                return Ok((activity.messages, false, events));
            }
        }
    }
    let owned = opencode_turn_messages(&activity.messages, turn);
    if owned.iter().any(|m| m["type"] == "idle") || activity.has_later_input(turn) {
        live = false;
    }
    Ok((activity.messages, live, Vec::new()))
}

async fn reconcile_opencode_turn(
    server: &OpenCodeServerProcess,
    state: &Arc<Mutex<OpenCodeRuntimeState>>,
    timeout: Duration,
    pending: bool,
) -> Result<Vec<RuntimeEvent>> {
    let (Some(session), Some(turn)) = ({
        let s = state.lock().await;
        (s.native_thread_id.clone(), s.active_turn_id.clone())
    }) else {
        return Ok(Vec::new());
    };
    let (messages, live, terminal_events) =
        read_opencode_turn_snapshot(server, state, &session, &turn, timeout).await?;
    if !terminal_events.is_empty() {
        return Ok(terminal_events);
    }
    let permissions = if pending && live {
        server
            .api
            .data(
                Method::GET,
                &format!("/session/{session}/permission"),
                None,
                timeout,
            )
            .await?
    } else {
        json!([])
    };
    let forms = if pending && live {
        server
            .api
            .data(
                Method::GET,
                &format!("/session/{session}/form"),
                None,
                timeout,
            )
            .await?
    } else {
        json!([])
    };
    let unsupported_form = {
        let snapshot = state.lock().await;
        forms.as_array().into_iter().flatten().any(|form| {
            opencode_form_belongs_to_turn(&snapshot, form) && !opencode_form_supported(form)
        })
    };
    if unsupported_form {
        let ids = {
            let snapshot = state.lock().await;
            forms
                .as_array()
                .into_iter()
                .flatten()
                .filter(|form| opencode_form_belongs_to_turn(&snapshot, form))
                .filter_map(|form| form["id"].as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        };
        for id in ids {
            let _ = server.api.form_cancel(&session, &id, timeout).await;
        }
        let _ = server.api.abort_session(&session, timeout).await;
        server.close().await?;
        let mut snapshot = state.lock().await;
        let mut events = project_opencode_messages(&mut snapshot, &messages);
        snapshot.last_error = Some(
            json!({"type":"UnsupportedForm","message":"OpenCode V2 requested a form Pioneer cannot display or correlate to its tool call."}),
        );
        events.extend(terminalize_opencode_failure(&mut snapshot, None));
        return Ok(events);
    }
    let diff = if opencode_turn_messages(&messages, &turn).is_empty() {
        json!([])
    } else {
        server
            .api
            .data(
                Method::GET,
                &format!("/session/{session}/diff?from={turn}"),
                None,
                timeout,
            )
            .await?
    };
    let mut snapshot = state.lock().await;
    if snapshot.active_turn_id.as_deref() != Some(&turn) || snapshot.terminal_emitted {
        return Ok(Vec::new());
    }
    let mut events = project_opencode_messages(&mut snapshot, &messages);
    events.extend(map_opencode_diff(&mut snapshot, &diff, None));
    let current = permissions
        .as_array()
        .into_iter()
        .flatten()
        .chain(forms.as_array().into_iter().flatten())
        .filter_map(|r| r["id"].as_str())
        .collect::<HashSet<_>>();
    let resolved = snapshot
        .pending_requests
        .keys()
        .filter(|id| !current.contains(id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for id in resolved {
        snapshot.pending_requests.remove(&id);
        events.push(RuntimeEvent::RequestResolved(RuntimeRequestResolved {
            native_request_id: id,
            native: None,
        }));
    }
    for request in permissions.as_array().into_iter().flatten() {
        events.extend(map_opencode_permission_request(
            &mut snapshot,
            request,
            None,
        ));
    }
    for form in forms.as_array().into_iter().flatten() {
        events.extend(map_opencode_form_request(&mut snapshot, form, None));
    }
    // Do not complete an unadmitted deferred prompt just because SSE connected.
    if snapshot.user_observed || snapshot.dispatch_deadline.is_some() {
        events.extend(opencode_snapshot_terminal(&mut snapshot, &messages, live));
    }
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

#[cfg(all(test, unix))]
#[path = "opencode_startup_tests.rs"]
pub(crate) mod startup_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn history(outcome: &str) -> JsonValue {
        json!([
            {"id":"msg_user","type":"user","text":"go","time":{"created":1}},
            {"id":"msg_step","type":"assistant","agent":"build","model":{"providerID":"local","id":"model"},"time":{"created":2,"completed":20},"content":[
                {"type":"reasoning","text":"thinking"},
                {"type":"tool","id":"call_shell","name":"shell","time":{"created":3,"ran":4,"completed":5},"state":{"status":"completed","input":{"command":"echo hi"},"content":[{"type":"text","text":"hi\n"}],"metadata":{"exit":0}}},
                {"type":"text","text":"done"}],"tokens":{"input":10,"output":2,"reasoning":0,"cache":{"read":0,"write":0}}},
            {"id":"msg_idle","type":"idle","time":{"created":21},"outcome":outcome},
            {"id":"msg_later","type":"user","time":{"created":22},"text":"other"},
            {"id":"msg_other","type":"assistant","agent":"build","model":{"providerID":"local","id":"model"},"time":{"created":23,"completed":30},"content":[{"type":"text","text":"unrelated"}]}
        ])
    }

    #[test]
    fn v2_recovery_projects_shell_results_and_bounds_ownership_by_input_and_idle() {
        let messages = history("succeeded");
        let result =
            opencode_observation_from_messages("ses_test", "msg_user", &messages, false).unwrap();
        assert_eq!(result.status, CLIAgentRuntimeObservedTurnStatus::Completed);
        let completed = result
            .reconciliation_events
            .iter()
            .filter_map(|e| {
                if let RuntimeEvent::ItemCompleted(i) = e {
                    Some(i)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(completed.len(), 4);
        assert!(
            completed.iter().any(|i| i.item_kind == "tokenUsage"
                && i.metadata.as_ref().unwrap()["input_tokens"] == 10)
        );
        let shell = completed
            .iter()
            .find(|i| i.item_kind == "commandExecution")
            .unwrap();
        assert_eq!(shell.metadata.as_ref().unwrap()["exitCode"], 0);
        assert_eq!(shell.metadata.as_ref().unwrap()["aggregatedOutput"], "hi\n");
        assert!(
            completed
                .iter()
                .all(|i| i.text.as_deref() != Some("unrelated"))
        );
        assert_eq!(
            opencode_observation_from_messages(
                "ses_test",
                "msg_user",
                &history("interrupted"),
                false
            )
            .unwrap()
            .status,
            CLIAgentRuntimeObservedTurnStatus::Interrupted
        );
    }

    #[test]
    fn v2_success_requires_idle_outcome_and_tools_keep_failure_content() {
        let mut messages = history("succeeded");
        messages.as_array_mut().unwrap().truncate(2);
        assert_eq!(
            opencode_observation_from_messages("ses_test", "msg_user", &messages, false)
                .unwrap()
                .status,
            CLIAgentRuntimeObservedTurnStatus::Failed
        );
        let tool = json!({"status":"error","input":{"command":"bad"},"content":[{"type":"text","text":"partial\n"}],"metadata":{"exit":7},"error":{"type":"ToolFailure","message":"failed"}});
        let metadata = opencode_tool_metadata("shell", &tool, "error");
        assert_eq!(metadata["aggregatedOutput"], "partial\n");
        assert_eq!(metadata["error"]["message"], "failed");
        assert_eq!(metadata["exitCode"], 7);
    }

    #[test]
    fn v2_forms_preserve_field_keys_option_values_and_numeric_types() {
        let fields = json!([
            {"key":"choice","type":"string","options":[{"value":"native_value","label":"Visible choice"}],"custom":false},
            {"key":"count","type":"integer"},{"key":"enabled","type":"boolean"},
            {"key":"multi","type":"multiselect","options":[{"value":"a","label":"A"}]}
        ]);
        let answer = opencode_form_answer(
            &fields,
            &[
                vec!["Visible choice".into()],
                vec!["3".into()],
                vec!["true".into()],
                vec!["A".into()],
            ],
        )
        .unwrap();
        assert_eq!(
            answer,
            json!({"choice":"native_value","count":3,"enabled":true,"multi":["a"]})
        );
        assert!(opencode_form_answer(&fields, &[]).is_err());
    }

    #[test]
    fn v2_permissions_do_not_expose_native_subagents_or_ambient_tools() {
        let rules = opencode_permission_rules(Some("never"), None);
        let effect = |action: &str| {
            rules
                .as_array()
                .unwrap()
                .iter()
                .rev()
                .find(|r| r["action"] == action || r["action"] == "*")
                .unwrap()["effect"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        assert_eq!(effect("shell"), "allow");
        assert_eq!(effect("subagent"), "deny");
        assert_eq!(effect("execute"), "deny");
        assert_eq!(effect("ambient_tool"), "deny");
        assert_eq!(
            opencode_model_ref("provider", "model#high", None).unwrap(),
            json!({"providerID":"provider","id":"model","variant":"high"})
        );
    }

    #[tokio::test]
    async fn v2_stream_ignores_global_events_and_rest_deduplicates_completed_content() {
        let state = Arc::new(Mutex::new(OpenCodeRuntimeState::default()));
        state
            .lock()
            .await
            .begin_turn("ses_test".into(), "msg_user".into());
        assert!(
            map_opencode_event(
                &state,
                json!({"type":"permission.asked","data":{"id":"per_global"}}),
                false
            )
            .await
            .is_empty()
        );
        let messages = history("succeeded");
        let first = project_opencode_messages(&mut *state.lock().await, &messages);
        assert!(!first.is_empty());
        assert!(project_opencode_messages(&mut *state.lock().await, &messages).is_empty());
        let delayed = json!({"type":"session.text.ended","data":{"sessionID":"ses_test","assistantMessageID":"msg_step","ordinal":0,"text":"done"}});
        assert!(map_opencode_event(&state, delayed, false).await.is_empty());
    }
    #[test]
    fn v2_info_and_model_catalog_do_not_require_semver_metadata() {
        let info = json!({"pid":123,"urls":["http://127.0.0.1:4567"],"paths":{"tmp":"/tmp/opencode"},"version":"my-local-v2-build"});
        assert_eq!(
            opencode_server_version(&info).unwrap().as_deref(),
            Some("my-local-v2-build")
        );
        assert!(
            opencode_server_version(
                &json!({"pid":"invalid","urls":[],"paths":{"tmp":"/tmp/opencode"}})
            )
            .is_err()
        );
        let models = json!([{"providerID":"local","id":"model","modelID":"model","name":"Local","enabled":true,"status":"active","cost":[],"time":{"released":0},"capabilities":{"tools":true,"input":["text","image"],"output":["text"]},"variants":[{"id":"high"}],"limit":{"context":10000,"output":1000}}]);
        let catalog = opencode_models_from_response(
            &models,
            &json!([{"id":"local","name":"Local provider"}]),
        );
        assert_eq!(catalog[0].id, "local/model");
        assert_eq!(catalog[0].effort_options, vec!["high"]);
        assert_eq!(catalog[0].supports_vision, Some(true));
    }

    #[tokio::test]
    async fn v2_duplicate_deltas_and_disconnected_fragments_reconcile_to_full_values() {
        let state = Arc::new(Mutex::new(OpenCodeRuntimeState::default()));
        {
            let mut s = state.lock().await;
            s.begin_turn("ses_test".into(), "msg_user".into());
            s.assistant_message_ids.insert("msg_step".into());
        }
        let event =
            |id: &str, kind: &str, data: JsonValue| json!({"id":id,"type":kind,"data":data});
        let base = json!({"sessionID":"ses_test","assistantMessageID":"msg_step","ordinal":0});
        map_opencode_event(
            &state,
            event("evt_start", "session.text.started", base.clone()),
            false,
        )
        .await;
        let mut delta = base.clone();
        delta["delta"] = json!("Привет ");
        let raw = event("evt_delta", "session.text.delta", delta.clone());
        assert!(
            !map_opencode_event(&state, raw.clone(), false)
                .await
                .is_empty()
        );
        assert!(map_opencode_event(&state, raw, false).await.is_empty());
        state.lock().await.streaming_items.clear();
        assert!(
            map_opencode_event(&state, event("evt_gap", "session.text.delta", delta), false)
                .await
                .is_empty()
        );
        let mut ended = base;
        ended["text"] = json!("Привет после разрыва");
        let events =
            map_opencode_event(&state, event("evt_end", "session.text.ended", ended), false).await;
        assert!(events.iter().any(|e| matches!(e,RuntimeEvent::ItemCompleted(i) if i.text.as_deref()==Some("Привет после разрыва"))));
    }

    #[test]
    fn stop_resolves_waiters_and_never_completes_a_running_tool_successfully() {
        let mut state = OpenCodeRuntimeState::default();
        state.begin_turn("ses_test".into(), "msg_user".into());
        project_opencode_content(
            &mut state,
            "msg_step:tool:call",
            &json!({"type":"tool","id":"call","name":"shell","state":{"status":"running","input":{"command":"sleep 10"},"metadata":{}}}),
            false,
            None,
        );
        state
            .pending_requests
            .insert("per_waiting".into(), OpenCodePendingRequestKind::Permission);
        state.interrupted = true;
        let events = complete_opencode_turn(&mut state, "interrupted", None);
        assert!(events.iter().any(
            |e| matches!(e,RuntimeEvent::RequestResolved(r) if r.native_request_id=="per_waiting")
        ));
        assert!(events.iter().any(|e|matches!(e,RuntimeEvent::ItemCompleted(i) if i.metadata.as_ref().is_some_and(|m|m["status"]=="interrupted" && m["success"]==false))));
        assert!(matches!(
            events.last(),
            Some(RuntimeEvent::TurnInterrupted(_))
        ));
        assert!(complete_opencode_turn(&mut state, "interrupted", None).is_empty());
    }
    #[test]
    fn v2_shell_projection_uses_the_shared_durable_shell_output_policy() {
        use crate::cli_runtime::projector::{
            CLIRuntimeProjectorContext, project_cli_runtime_event,
        };
        let observation = opencode_observation_from_messages(
            "ses_test",
            "msg_user",
            &history("succeeded"),
            false,
        )
        .unwrap();
        let context = CLIRuntimeProjectorContext {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            recovery: None,
            codex_retry_attempt_id: None,
        };
        let shell = observation
            .reconciliation_events
            .iter()
            .find(|e| matches!(e,RuntimeEvent::ItemCompleted(i) if i.item_kind=="commandExecution"))
            .unwrap();
        let projected = project_cli_runtime_event(&context, shell);
        let Some(pioneer_protocol::AgentDurableEvent::ItemCompleted { notification }) =
            projected.durable.first()
        else {
            panic!("shell must commit through durable ItemCompleted");
        };
        let pioneer_protocol::TurnItem::CommandExecution {
            output_policy,
            storage,
            display,
            ..
        } = &notification.item
        else {
            panic!("shell must remain a command execution");
        };
        assert_eq!(
            output_policy,
            &pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name("exec_command")
        );
        assert!(
            matches!(storage,pioneer_protocol::ToolStoragePayload::Shell{aggregated_output:Some(text),exit_code:Some(0),..} if text=="hi\n")
        );
        assert!(
            matches!(display,pioneer_protocol::ToolDisplayPayload::Shell{aggregated_output:Some(text),exit_code:Some(0),..} if text=="hi\n")
        );
    }

    fn awaiting_startup() -> OpenCodeRuntimeState {
        let mut state = OpenCodeRuntimeState::default();
        state.begin_turn("ses_test".into(), "msg_user".into());
        state.dispatch_deadline = Some(tokio::time::Instant::now() - Duration::from_secs(1));
        state
    }

    #[test]
    fn expired_admission_with_promoted_input_and_native_running_needs_no_assistant() {
        let mut state = awaiting_startup();
        let activity = OpenCodeTurnActivity {
            messages: json!([{"id":"msg_user","type":"user","text":"go"}]),
            native_running: true,
            parked: false,
            history_changed: false,
        };
        assert_eq!(
            activity.assess(&mut state, "ses_test", "msg_user"),
            (true, false)
        );
        assert!(state.execution_observed);
        assert!(!state.awaiting_execution);
        assert!(state.dispatch_deadline.is_none());
        // A different busy input must not confirm execution of our parked one,
        // nor may expiry interrupt that other operation's owner.
        let mut state = awaiting_startup();
        let other = OpenCodeTurnActivity {
            messages: json!([]),
            native_running: true,
            parked: true,
            history_changed: false,
        };
        assert_eq!(
            other.assess(&mut state, "ses_test", "msg_user"),
            (true, false)
        );
        assert!(!state.execution_observed);
    }

    #[test]
    fn lost_startup_revalidates_progress_terminal_ownership_and_stop() {
        let lost = OpenCodeTurnActivity {
            messages: json!([]),
            native_running: false,
            parked: false,
            history_changed: false,
        };
        let mut state = awaiting_startup();
        assert_eq!(
            lost.assess(&mut state, "ses_test", "msg_user"),
            (false, true)
        );
        let unstable = OpenCodeTurnActivity {
            messages: json!([{"id":"msg_user","type":"user"}]),
            native_running: false,
            parked: false,
            history_changed: true,
        };
        assert_eq!(
            unstable.assess(&mut state, "ses_test", "msg_user"),
            (true, false)
        );
        // Progress arrives while the caller is checking pending requests.
        let started = OpenCodeTurnActivity {
            messages: json!([{"id":"msg_user","type":"user"},{"id":"msg_step","type":"assistant","content":[]}]),
            native_running: true,
            parked: false,
            history_changed: false,
        };
        assert_eq!(
            started.assess(&mut state, "ses_test", "msg_user"),
            (true, false)
        );
        // Even an older HTTP read cannot undo execution already seen by SSE.
        assert!(!lost.assess(&mut state, "ses_test", "msg_user").1);
        let mut state = awaiting_startup();
        let done = OpenCodeTurnActivity {
            messages: history("succeeded"),
            native_running: false,
            parked: false,
            history_changed: false,
        };
        assert!(!done.assess(&mut state, "ses_test", "msg_user").1);
        for change in ["stop", "terminal", "new_turn", "new_session", "pending"] {
            let mut state = awaiting_startup();
            assert!(lost.assess(&mut state, "ses_test", "msg_user").1);
            match change {
                "stop" => state.interrupted = true,
                "terminal" => state.terminal_emitted = true,
                "new_turn" => state.begin_turn("ses_test".into(), "msg_new".into()),
                "new_session" => state.begin_turn("ses_new".into(), "msg_user".into()),
                "pending" => {
                    state
                        .pending_requests
                        .insert("frm_pending".into(), OpenCodePendingRequestKind::Form);
                }
                _ => unreachable!(),
            }
            assert!(
                !lost.assess(&mut state, "ses_test", "msg_user").1,
                "{change}"
            );
        }
    }

    fn websearch_form() -> JsonValue {
        json!({"id":"frm_web","sessionID":"ses_test","title":"Web Search",
            "metadata":{"kind":"websearch.provider"},"fields":[{"key":"choice","description":"Allow OpenCode to search the web for up-to-date information?","type":"string","required":true,"custom":false,
            "options":[{"value":"allow","label":"Allow search via Provider"},{"value":"choose","label":"Choose another provider"},{"value":"disable","label":"Disable web search"}]}]})
    }

    #[tokio::test]
    async fn websearch_and_question_forms_keep_native_ids_and_ignore_foreign_forms() {
        let state = Arc::new(Mutex::new(awaiting_startup()));
        let running = OpenCodeTurnActivity {
            messages: json!([{"id":"msg_user","type":"user"}]),
            native_running: true,
            parked: false,
            history_changed: false,
        };
        running.assess(&mut *state.lock().await, "ses_test", "msg_user");
        let form = websearch_form();
        let mut unstarted = awaiting_startup();
        assert!(map_opencode_form_request(&mut unstarted, &form, None).is_empty());
        let mut unsupported = form.clone();
        unsupported["fields"][0]["type"] = json!("external");
        assert!(opencode_form_belongs_to_turn(
            &*state.lock().await,
            &unsupported
        ));
        assert!(!opencode_form_supported(&unsupported));
        let events = map_opencode_event(
            &state,
            json!({"type":"form.created","data":{"form":form}}),
            false,
        )
        .await;
        let RuntimeEvent::RequestOpened(request) = &events[0] else {
            panic!("expected a question");
        };
        assert_eq!(request.native_request_id, "frm_web");
        assert_eq!(request.native_turn_id.as_deref(), Some("msg_user"));
        assert_eq!(request.native_item_id, None);
        assert_eq!(
            opencode_form_answer(
                &request.payload_redacted.as_ref().unwrap()["formFields"],
                &[vec!["Choose another provider".into()]]
            )
            .unwrap(),
            json!({"choice":"choose"})
        );
        assert!(
            map_opencode_event(
                &state,
                json!({"type":"form.created","data":{"form":form}}),
                false
            )
            .await
            .is_empty()
        );
        for foreign in [
            {
                let mut f = form.clone();
                f["id"] = json!("frm_foreign");
                f["sessionID"] = json!("ses_foreign");
                f
            },
            {
                let mut f = form.clone();
                f["id"] = json!("frm_global");
                f["sessionID"] = json!("global");
                f
            },
            {
                let mut f = form.clone();
                f["id"] = json!("frm_plugin");
                f["metadata"] = json!({"kind":"other.plugin"});
                f
            },
        ] {
            assert!(map_opencode_form_request(&mut *state.lock().await, &foreign, None).is_empty());
        }
        let mut question = form.clone();
        question["id"] = json!("frm_question");
        question["metadata"] =
            json!({"kind":"question","tool":{"messageID":"msg_step","id":"call_question"}});
        state
            .lock()
            .await
            .assistant_message_ids
            .insert("msg_step".into());
        let mapped = map_opencode_form_request(&mut *state.lock().await, &question, None);
        assert!(
            matches!(&mapped[0],RuntimeEvent::RequestOpened(r) if r.native_item_id.as_deref()==Some("msg_step:tool:call_question"))
        );
        let cancelled = map_opencode_event(
            &state,
            json!({"type":"form.cancelled","data":{"sessionID":"ses_test","id":"frm_web"}}),
            false,
        )
        .await;
        assert!(
            matches!(&cancelled[0],RuntimeEvent::RequestResolved(r) if r.native_request_id=="frm_web")
        );
        let mut snapshot = state.lock().await;
        snapshot.interrupted = true;
        let events = complete_opencode_turn(&mut snapshot, "interrupted", None);
        assert!(events.iter().any(
            |e| matches!(e,RuntimeEvent::RequestResolved(r) if r.native_request_id=="frm_question")
        ));
        assert!(map_opencode_form_request(&mut snapshot, &form, None).is_empty());
    }

    #[test]
    fn refused_background_shell_is_a_failed_call_and_recovery_can_finish_the_turn() {
        let mut messages = history("succeeded");
        let shell = &mut messages[1]["content"][1];
        shell["state"]["input"]["background"] = json!(true);
        shell["state"]["status"] = json!("error");
        shell["state"]["metadata"] = json!({"pioneerBackgroundShellRejected":true});
        shell["state"]["error"] =
            json!({"type":"Tool.Error","message":"Native background shell is disabled"});
        let observed =
            opencode_observation_from_messages("ses_test", "msg_user", &messages, false).unwrap();
        assert_eq!(
            observed.status,
            CLIAgentRuntimeObservedTurnStatus::Completed
        );
        assert!(observed.reconciliation_events.iter().any(|e|matches!(e,RuntimeEvent::ItemCompleted(i) if i.item_kind=="commandExecution" && i.metadata.as_ref().is_some_and(|m|m["success"]==false && m["pioneerBackgroundShellRejected"]==true))));
    }

    #[test]
    fn v2_usage_normalizes_exclusive_categories_once_and_preserves_missing_zero() {
        let messages = json!([
            {"type":"assistant","tokens":{"input":10,"output":3,"reasoning":7,"cache":{"read":80,"write":20}}},
            {"type":"compaction","tokens":{"input":2,"output":1,"reasoning":3,"cache":{"read":4,"write":5}}}
        ]);
        let first = opencode_usage_metadata([&messages[0]]);
        assert_eq!(first["input_tokens"], 110);
        assert_eq!(first["output_tokens"], 10);
        assert_eq!(first["uncached_input_tokens"], 10);
        assert_eq!(first["visible_output_tokens"], 3);
        assert_eq!(first["native_tokens"]["cache"]["read"], 80);
        let sum = opencode_usage_metadata(messages.as_array().unwrap());
        assert_eq!(sum["input_tokens"], 121);
        assert_eq!(sum["output_tokens"], 14);
        assert_eq!(sum["reasoning_tokens"], 10);
        let mut native_history = history("succeeded");
        native_history[1]["tokens"] = messages[0]["tokens"].clone();
        native_history
            .as_array_mut()
            .unwrap()
            .insert(2, messages[1].clone());
        let observation =
            opencode_observation_from_messages("ses_test", "msg_user", &native_history, false)
                .unwrap();
        assert!(observation.reconciliation_events.iter().any(|event|matches!(event,RuntimeEvent::ItemCompleted(item) if item.item_kind=="tokenUsage" && item.metadata.as_ref().is_some_and(|usage| usage["input_tokens"]==121 && usage["output_tokens"]==14))));
        let empty = opencode_usage_metadata([&json!({"type":"assistant","tokens":{}})]);
        assert!(empty["input_tokens"].is_null());
        assert!(empty["output_tokens"].is_null());
        let zero = opencode_usage_metadata([
            &json!({"type":"assistant","tokens":{"input":0,"output":0,"cache":{"read":0}}}),
        ]);
        assert_eq!(zero["input_tokens"], 0);
        assert_eq!(zero["output_tokens"], 0);
        assert_eq!(zero["uncached_input_tokens"], 0);
        assert!(zero["reasoning_tokens"].is_null());
        assert!(zero["cache_creation_input_tokens"].is_null());
    }

    #[tokio::test]
    async fn websearch_typed_answer_and_cancel_use_session_scoped_native_form_routes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let receiver = tokio::spawn(async move {
            let mut requests = Vec::new();
            for _ in 0..2 {
                let (mut connection, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buffer = [0u8; 1024];
                    let count = connection.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                    assert!(bytes.len() < 8192);
                    if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|n| n.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                connection
                    .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
            }
            requests
        });
        let api = OpenCodeApiClient {
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            base_url: format!("http://{address}"),
            directory: "/workspace".into(),
            username: OPENCODE_SERVER_USERNAME.into(),
            password: SecretString::new("test-secret"),
            catalog_rpc_id: "test-catalog".into(),
        };
        let form = websearch_form();
        let answer =
            opencode_form_answer(&form["fields"], &[vec!["Allow search via Provider".into()]])
                .unwrap();
        api.form_reply("ses_test", "frm_web", &answer, Duration::from_secs(2))
            .await
            .unwrap();
        api.form_cancel("ses_test", "frm_web", Duration::from_secs(2))
            .await
            .unwrap();
        let requests = receiver.await.unwrap();
        assert!(requests[0].starts_with("POST /api/session/ses_test/form/frm_web/reply?"));
        assert_eq!(
            serde_json::from_str::<JsonValue>(requests[0].split_once("\r\n\r\n").unwrap().1)
                .unwrap(),
            json!({"answer":{"choice":"allow"}})
        );
        assert!(requests[1].starts_with("DELETE /api/session/ses_test/form/frm_web?"));
    }
}
