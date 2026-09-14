//! One nonpersistent service attempt on the existing Codex App Server codec.
//! Supported profile: openai/codex rust-v0.154.0. Empty environments remove
//! working tools independently of ephemeral persistence (see upstream
//! app-server-protocol/v2/thread.rs and core/tools/spec_plan.rs).
use super::*;
use crate::event::{
    RuntimeEventMappingOptions, classify_runtime_provider_failure, map_codex_notification_event,
};
use crate::process::{CLIAgentProcess, SensitiveEnvironment};
use crate::service::ServiceFailure;
use crate::service::ServiceStage;
use anyhow::{Context, Result, bail, ensure};
use pioneer_protocol::ProviderFailureClass;
use tokio::sync::Mutex;

const RELEASE: &str = "0.154.0";
const MAX_SERVICE_BYTES: usize = CODEX_MAX_MATERIALIZED_FRAME_BYTES;
const DISABLED_FEATURES: &[&str] = &[
    "shell_tool",
    "unified_exec",
    "view_image",
    "request_permissions_tool",
    "multi_agent",
    "multi_agent_v2",
    "plugins",
    "remote_plugin",
    "apps",
    "enable_mcp_apps",
    "hooks",
    "plugin_hooks",
    "skill_mcp_dependency_install",
    "skill_search",
    "skill_env_var_dependency_prompt",
    "token_budget",
    "context_management",
    "tool_suggest",
    "request_rule",
    "deferred_executor",
    "sleep_tool",
    "web_search_request",
    "browser_use",
    "browser_use_external",
    "computer_use",
    "image_generation",
    "in_app_local_automation",
    "remote_control",
];

#[derive(Clone)]
pub struct CodexServiceConfig {
    pub executable: String,
    /// The selected instance's normal CLI authorization. No OAuth extraction,
    /// direct API substitution, or copying of its saved conversations.
    pub home_path: String,
    pub environment: SensitiveEnvironment,
}
pub struct CodexServiceRequest {
    pub model: String,
    pub effort: Option<String>,
    pub instructions: String,
    pub input: String,
}
pub struct CodexServiceCompletion {
    pub text: String,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}
pub struct CodexService {
    config: CodexServiceConfig,
    attempt: Mutex<Option<ServiceAttempt>>,
}
struct ServiceAttempt {
    client: CodexAppServerClient,
    owner: Option<CodexJsonlRpcOwner>,
    process: CLIAgentProcess,
    directory: tempfile::TempDir,
}
impl Drop for ServiceAttempt {
    fn drop(&mut self) {
        self.process.abort_service();
        // The RPC owner aborts its bounded, in-memory tasks on Drop.
    }
}
impl CodexService {
    pub fn new(config: CodexServiceConfig) -> Self {
        Self {
            config,
            attempt: Mutex::new(None),
        }
    }
    pub async fn summarize(
        &self,
        request: CodexServiceRequest,
        timeout: Duration,
    ) -> Result<CodexServiceCompletion> {
        let deadline = Instant::now() + timeout;
        ensure!(!request.model.trim().is_empty(), "missing service model");
        ensure!(
            request
                .input
                .len()
                .saturating_add(request.instructions.len())
                <= MAX_SERVICE_BYTES,
            "service input exceeds transport capacity"
        );
        let result = {
            let mut attempt = self.attempt.lock().await;
            ensure!(attempt.is_none(), "previous service attempt needs cleanup");
            ensure!(Instant::now() < deadline, "service deadline exceeded");
            let directory = tempfile::tempdir().context(ServiceStage("cli_directory"))?;
            let config = process_config(&self.config, directory.path())
                .context(ServiceStage("cli_configuration"))?;
            let mut process =
                spawn_cli_agent_process(&config).context(ServiceStage("cli_spawn"))?;
            let (stdout, stdin) = process.take_stdio().context(ServiceStage("cli_stdio"))?;
            let (rpc, owner) = CodexJsonlRpcClient::new_owned(BufReader::new(stdout), stdin);
            *attempt = Some(ServiceAttempt {
                client: CodexAppServerClient::new(rpc),
                owner: Some(owner),
                process,
                directory,
            });
            let state = attempt.as_mut().unwrap();
            match tokio::time::timeout_at(
                deadline,
                exchange(&state.client, state.directory.path(), request, deadline),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("codex service deadline exceeded")),
            }
        };
        self.cleanup().await?;
        result
    }
    /// Retains ownership across a cancelled summarize or cleanup future.
    pub async fn cleanup(&self) -> Result<()> {
        let mut attempt = self.attempt.lock().await;
        if let Some(state) = attempt.as_mut() {
            let process_result = state
                .process
                .terminate_with_grace(Duration::from_secs(1))
                .await;
            if let Some(owner) = state.owner.as_mut() {
                owner.abort_and_join().await;
            }
            state.owner.take();
            // Even a termination error must release all other resources.
            attempt.take();
            process_result?;
        }
        Ok(())
    }
}

fn profile() -> JsonValue {
    let mut features = serde_json::Map::new();
    for name in DISABLED_FEATURES {
        features.insert((*name).into(), json!(false));
    }
    features.insert("skip_host_skill_discovery".into(), json!(true));
    json!({
        "features":features, "mcp_servers":{}, "plugins":{}, "marketplaces":{},
        "apps":{}, "projects":{}, "agents":{"enabled":false},
        "web_search":"disabled", "project_doc_max_bytes":0,
        "include_environment_context":false, "include_collaboration_mode_instructions":false,
        "history":{"persistence":"none"}
    })
}
fn process_config(config: &CodexServiceConfig, cwd: &Path) -> Result<CLIAgentProcessSpawnConfig> {
    let mut process =
        CLIAgentProcessSpawnConfig::codex_app_server(&config.executable, &config.home_path)
            .with_cwd(cwd)
            .with_environment(&config.environment)
            .with_stderr_ring_lines(0);
    for (key, value) in profile().as_object().unwrap() {
        let value = toml::Value::try_from(value)?;
        process
            .args
            .extend(["--config".into(), format!("{key}={value}")]);
    }
    Ok(process)
}
fn verify_profile(actual: &JsonValue, expected: &JsonValue) -> bool {
    match expected {
        JsonValue::Object(expected) if !expected.is_empty() => {
            actual.as_object().is_some_and(|actual| {
                expected.iter().all(|(key, value)| {
                    actual
                        .get(key)
                        .is_some_and(|actual| verify_profile(actual, value))
                })
            })
        }
        // config/read uses typed maps: e.g. an empty apps table is returned
        // as {"_default":null}. Null defaults do not enable an integration.
        JsonValue::Object(expected) if expected.is_empty() => actual
            .as_object()
            .is_some_and(|actual| actual.values().all(JsonValue::is_null)),
        _ => actual == expected,
    }
}
fn supports_release(initialize: &CodexInitializeSnapshot) -> bool {
    if let Some(version) = &initialize.version {
        return version == RELEASE;
    }
    initialize.user_agent.as_deref().is_some_and(|agent| {
        agent
            .split(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+' | '_')))
            .any(|part| part == RELEASE)
    })
}

fn remaining(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    ensure!(!remaining.is_zero(), "service deadline exceeded");
    Ok(remaining)
}
async fn exchange(
    client: &CodexAppServerClient,
    cwd: &Path,
    request: CodexServiceRequest,
    deadline: Instant,
) -> Result<CodexServiceCompletion> {
    let mut notifications = client
        .rpc
        .take_notification_receiver()
        .ok_or_else(|| anyhow::anyhow!("service notification receiver already used"))?;
    let mut interactions = client
        .rpc
        .take_server_request_receiver()
        .ok_or_else(|| anyhow::anyhow!("service interaction receiver already used"))?;
    let mut diagnostics = client
        .rpc
        .take_diagnostic_receiver()
        .ok_or_else(|| anyhow::anyhow!("service diagnostic receiver already used"))?;
    let initialize = client
        .initialize(remaining(deadline)?)
        .await
        .context(ServiceStage("cli_initialize"))?;
    ensure!(
        supports_release(&initialize),
        "unsupported Codex service capability version"
    );
    let config = client
        .rpc
        .request_value(
            "config/read",
            Some(json!({"cwd":cwd,"includeLayers":false})),
            remaining(deadline)?,
        )
        .await
        .context(ServiceStage("cli_config_read"))?;
    ensure!(
        verify_profile(&config["config"], &profile()),
        "Codex service isolation was not applied"
    );
    let opened = client.rpc.request_thread_open_value("thread/start", Some(json!({
        "model":request.model, "cwd":cwd, "approvalPolicy":"never", "sandbox":"read-only",
        "ephemeral":true, "environments":[], "dynamicTools":[], "selectedCapabilityRoots":[],
        "allowProviderModelFallback":false, "baseInstructions":request.instructions,
        "developerInstructions":"", "personality":"none"
    })), remaining(deadline)?).await.context(ServiceStage("cli_thread_start"))?;
    let opened = decode_codex_thread_open_response("thread/start", opened)
        .context(ServiceStage("cli_thread_decode"))?;
    ensure!(
        opened.model.as_deref() == Some(request.model.as_str()),
        "Codex service model changed"
    );
    ensure!(
        opened
            .raw
            .pointer("/thread/path")
            .is_some_and(JsonValue::is_null),
        "Codex service opened a persistent thread"
    );
    let started = client
        .rpc
        .request_value(
            "turn/start",
            Some(json!({
                "threadId":opened.native_thread_id, "input":[{"type":"text","text":request.input}],
                "model":request.model, "effort":request.effort, "environments":[]
            })),
            remaining(deadline)?,
        )
        .await
        .context(ServiceStage("cli_turn_start"))?;
    let started = decode_codex_turn_start_response("turn/start", &opened.native_thread_id, started)
        .context(ServiceStage("cli_turn_decode"))?;
    let mut completion = CodexServiceCompletion {
        text: String::new(),
        input_tokens: None,
        output_tokens: None,
    };
    loop {
        let event = tokio::select! { biased;
            _ = tokio::time::sleep_until(deadline) => bail!("Codex service deadline exceeded"),
            Some(_) = interactions.recv() => bail!("Codex service requested interaction or transport closed"),
            Some(_) = diagnostics.recv() => bail!("Codex service transport lost alignment"),
            event = notifications.recv() => event.ok_or_else(|| anyhow::anyhow!("Codex service ended without completion"))?,
        };
        let params = event.params.as_ref().unwrap_or(&JsonValue::Null);
        if params.get("threadId").and_then(JsonValue::as_str) != Some(&opened.native_thread_id) {
            continue;
        }
        if let Some(turn) = params.get("turnId").and_then(JsonValue::as_str)
            && turn != started.native_turn_id
        {
            continue;
        }
        if (event.method == "turn/completed"
            && params["turn"]["id"].as_str() == Some(&started.native_turn_id)
            && params["turn"]["status"].as_str() != Some("completed"))
            || (event.method == "error" && params["willRetry"].as_bool() != Some(true))
        {
            return Err(terminal_failure(&event).into());
        }
        match event.method.as_str() {
            "item/completed" => record_item(&params["item"], &mut completion)?,
            "thread/tokenUsage/updated" => {
                let last = &params["tokenUsage"]["last"];
                completion.input_tokens = last["inputTokens"].as_u64();
                completion.output_tokens = last["outputTokens"].as_u64();
            }
            "turn/completed" if params["turn"]["id"].as_str() == Some(&started.native_turn_id) => {
                let turn = &params["turn"];
                ensure!(
                    turn["status"].as_str() == Some("completed")
                        && turn.get("error").is_none_or(JsonValue::is_null),
                    "Codex service did not complete successfully"
                );
                if let Some(items) = turn["items"].as_array() {
                    for item in items {
                        record_item(item, &mut completion)?;
                    }
                }
                ensure!(
                    !completion.text.trim().is_empty(),
                    "Codex service returned no final answer"
                );
                return Ok(completion);
            }
            _ => {}
        }
    }
}
/// Decode only the pinned release's structured error contract. Raw provider
/// messages never escape this boundary; the existing mapper supplies cooldown
/// compatibility for older, message-only rate limits.
fn terminal_failure(event: &CodexJsonlRpcNotificationEvent) -> ServiceFailure {
    let params = event.params.as_ref().unwrap_or(&JsonValue::Null);
    let error = if event.method == "turn/completed" {
        &params["turn"]["error"]
    } else {
        &params["error"]
    };
    let info = &error["codexErrorInfo"];
    let name = info.as_str().or_else(|| {
        info.as_object()
            .filter(|m| m.len() == 1)
            .and_then(|m| m.keys().next().map(String::as_str))
    });
    let status = name.and_then(|name| info[name]["httpStatusCode"].as_u64());
    let class = match name {
        Some("contextWindowExceeded") => Some(ProviderFailureClass::ContextTooLarge),
        Some("rateLimitExceeded") => Some(ProviderFailureClass::RateLimit),
        // Account quota is not a transient request-rate limit.
        Some("usageLimitExceeded") => Some(ProviderFailureClass::ProviderRejected),
        Some("serverOverloaded" | "internalServerError") => Some(ProviderFailureClass::Provider5xx),
        Some("unauthorized") => Some(ProviderFailureClass::AuthOrPermission),
        Some("badRequest") => Some(ProviderFailureClass::InvalidRequest),
        Some("sessionBudgetExceeded" | "cyberPolicy" | "misalignmentPolicyViolation") => {
            Some(ProviderFailureClass::ProviderRejected)
        }
        Some(
            "httpConnectionFailed"
            | "responseStreamConnectionFailed"
            | "responseStreamDisconnected"
            | "responseTooManyFailedAttempts",
        ) => Some(match status {
            Some(429) => ProviderFailureClass::RateLimit,
            Some(500..=599) => ProviderFailureClass::Provider5xx,
            Some(401 | 403) => ProviderFailureClass::AuthOrPermission,
            Some(400..=499) => ProviderFailureClass::InvalidRequest,
            None => ProviderFailureClass::NetworkTransient,
            _ => ProviderFailureClass::Unknown,
        }),
        _ => None,
    };
    let mapped = map_codex_notification_event(
        event,
        RuntimeEventMappingOptions {
            include_redacted_native_payload: true,
        },
    );
    let legacy = classify_runtime_provider_failure(&mapped, chrono::Utc::now().fixed_offset());
    let class = class
        .or_else(|| legacy.as_ref().map(|failure| failure.class))
        .unwrap_or(ProviderFailureClass::Unknown);
    let retry_after_ms = if class == ProviderFailureClass::RateLimit {
        crate::event::find_retry_after_ms(error)
            .or_else(|| legacy.and_then(|failure| failure.retry_after_ms))
    } else {
        None
    };
    ServiceFailure {
        class,
        retry_after_ms,
    }
}

fn record_item(item: &JsonValue, completion: &mut CodexServiceCompletion) -> Result<()> {
    match item["type"].as_str() {
        Some("agentMessage") if item["phase"].as_str() != Some("commentary") => {
            let text = item["text"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("invalid service answer"))?;
            ensure!(
                text.len() <= MAX_SERVICE_BYTES,
                "service answer exceeds capacity"
            );
            completion.text = text.to_owned();
        }
        Some(
            "commandExecution"
            | "fileChange"
            | "webSearch"
            | "mcpToolCall"
            | "collabAgentToolCall"
            | "imageGeneration",
        ) => bail!("Codex service attempted a working action"),
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    #[test]
    fn terminal_failures_preserve_machine_class_and_cooldown_without_provider_text() {
        for (info, expected) in [
            (json!("rateLimitExceeded"), ProviderFailureClass::RateLimit),
            (
                json!("usageLimitExceeded"),
                ProviderFailureClass::ProviderRejected,
            ),
            (
                json!("contextWindowExceeded"),
                ProviderFailureClass::ContextTooLarge,
            ),
            (
                json!({"httpConnectionFailed":{"httpStatusCode":503}}),
                ProviderFailureClass::Provider5xx,
            ),
            (
                json!({"responseStreamDisconnected":{"httpStatusCode":null}}),
                ProviderFailureClass::NetworkTransient,
            ),
            (
                json!({"responseStreamConnectionFailed":{"httpStatusCode":401}}),
                ProviderFailureClass::AuthOrPermission,
            ),
            (
                json!({"httpConnectionFailed":{"httpStatusCode":400}}),
                ProviderFailureClass::InvalidRequest,
            ),
            (json!("badRequest"), ProviderFailureClass::InvalidRequest),
        ] {
            // Misleading message must not override an explicit permanent error.
            let params = json!({"threadId":"service", "turn":{"id":"attempt","status":"failed","error":{
                "codexErrorInfo":info,"message":"private source text: rate limit", "retryAfterMs":4321
            }}});
            let event = CodexJsonlRpcNotificationEvent {
                method: "turn/completed".into(),
                params: Some(params.clone()),
                raw: params,
            };
            let failure = terminal_failure(&event);
            assert_eq!(failure.class, expected);
            assert_eq!(
                failure.retry_after_ms,
                (expected == ProviderFailureClass::RateLimit).then_some(4321)
            );
            assert!(!format!("{failure:?} {failure}").contains("private source"));
            let erased = anyhow::Error::new(failure);
            assert_eq!(
                erased.downcast_ref::<ServiceFailure>().unwrap().class,
                expected
            );
        }
    }

    #[test]
    fn service_process_uses_selected_auth_and_parsable_restrictive_overrides() {
        let config = CodexServiceConfig {
            executable: "fixture-codex".into(),
            home_path: "/fixture/selected-auth".into(),
            environment: SensitiveEnvironment::new(),
        };
        let command = process_config(&config, Path::new("/fixture/empty-cwd"))
            .unwrap()
            .prepare()
            .unwrap();
        assert_eq!(command.args[0], "app-server");
        assert_eq!(
            command.env.expose("CODEX_HOME"),
            Some("/fixture/selected-auth")
        );
        let mut parsed = toml::Table::new();
        for pair in command.args[1..].chunks_exact(2) {
            assert_eq!(pair[0], "--config");
            parsed.extend(toml::from_str::<toml::Table>(&pair[1]).unwrap());
        }
        let actual = serde_json::to_value(parsed).unwrap();
        assert_eq!(actual["agents"]["enabled"], false);
        assert_eq!(actual["features"]["multi_agent_v2"], false);
        assert_eq!(actual["features"]["hooks"], false);
        assert_eq!(actual["features"]["skip_host_skill_discovery"], true);
        assert_eq!(actual["mcp_servers"], json!({}));
        assert_eq!(actual["history"]["persistence"], "none");
        assert!(verify_profile(&actual, &profile()));
    }

    #[test]
    fn service_profile_accepts_normalized_0154_config_and_rejects_enabled_integrations() {
        // Shape observed from the supported executable's real config/read.
        // tools.update_plan/experimental_request_user_input are not serialized
        // config fields in this version; working actions are isolated by the
        // feature gates plus empty environments in thread/start and turn/start.
        let mut actual = profile();
        actual["apps"] = json!({"_default":null});
        actual["agents"]["max_depth"] = JsonValue::Null;
        actual["history"]["max_bytes"] = JsonValue::Null;
        actual["tools"] = json!({"web_search":null});
        assert!(verify_profile(&actual, &profile()));
        actual["apps"]["_default"] = json!({"enabled":true});
        assert!(!verify_profile(&actual, &profile()));
        actual["apps"] = json!({"_default":null});
        actual["mcp_servers"] = json!({"unexpected":{"command":"tool"}});
        assert!(!verify_profile(&actual, &profile()));
        actual["mcp_servers"] = json!({});
        actual["features"]["shell_tool"] = json!(true);
        assert!(!verify_profile(&actual, &profile()));
    }

    #[tokio::test]
    async fn service_wire_is_ephemeral_for_each_fresh_attempt_and_rejects_unknown_version() {
        for attempt in 0..3 {
            let (io, server) = tokio::io::duplex(65536);
            let (reader, writer) = tokio::io::split(io);
            let (rpc, owner) = CodexJsonlRpcClient::new_owned(BufReader::new(reader), writer);
            let client = CodexAppServerClient::new(rpc);
            let version = if attempt == 2 {
                "0.154.0-canary"
            } else {
                RELEASE
            };
            let server = tokio::spawn(async move {
                let (reader, mut writer) = tokio::io::split(server);
                let mut reader = BufReader::new(reader);
                let mut seen = Vec::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap() == 0 {
                        break;
                    }
                    let value: JsonValue = serde_json::from_str(&line).unwrap();
                    let method = value["method"].as_str().unwrap();
                    seen.push(method.to_owned());
                    let result = match method {
                        "initialize" => json!({"version":version}),
                        "initialized" => continue,
                        "config/read" => {
                            let mut config = profile();
                            config["apps"] = json!({"_default":null});
                            config["tools"] = json!({"web_search":null});
                            json!({"config":config})
                        }
                        "thread/start" => {
                            let p = &value["params"];
                            assert_eq!(p["ephemeral"], true);
                            assert_eq!(p["environments"], json!([]));
                            assert_eq!(p["dynamicTools"], json!([]));
                            assert_eq!(p["allowProviderModelFallback"], false);
                            assert_eq!(p["model"], "selected-model");
                            assert_eq!(p["baseInstructions"], "service instructions");
                            json!({"thread":{"id":format!("fresh-{attempt}"),"path":null},"model":"selected-model"})
                        }
                        "turn/start" => {
                            let p = &value["params"];
                            assert_eq!(p["threadId"], format!("fresh-{attempt}"));
                            assert_eq!(p["environments"], json!([]));
                            assert_eq!(p["effort"], "high");
                            assert_eq!(p["input"][0]["text"], format!("portion-{attempt}"));
                            json!({"turn":{"id":"service-turn","status":"inProgress"}})
                        }
                        _ => panic!("unexpected service method: {method}"),
                    };
                    let response = json!({"jsonrpc":"2.0","id":value["id"],"result":result});
                    writer
                        .write_all(format!("{response}\n").as_bytes())
                        .await
                        .unwrap();
                    if method == "turn/start" {
                        let event = json!({"jsonrpc":"2.0","method":"turn/completed","params":{
                            "threadId":format!("fresh-{attempt}"),"turn":{"id":"service-turn","status":"completed","error":null,
                            "items":[{"type":"agentMessage","phase":"final_answer","text":"summary"}]}}});
                        writer
                            .write_all(format!("{event}\n").as_bytes())
                            .await
                            .unwrap();
                    }
                }
                seen
            });
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                exchange(
                    &client,
                    Path::new("/fixture/service"),
                    CodexServiceRequest {
                        model: "selected-model".into(),
                        effort: Some("high".into()),
                        instructions: "service instructions".into(),
                        input: format!("portion-{attempt}"),
                    },
                    Instant::now() + Duration::from_secs(5),
                ),
            )
            .await
            .unwrap();
            owner.shutdown().await;
            let seen = server.await.unwrap();
            if attempt == 2 {
                assert!(result.is_err());
                assert!(!seen.iter().any(|method| method == "thread/start"));
            } else {
                assert_eq!(result.unwrap().text, "summary");
                assert_eq!(
                    seen.iter()
                        .filter(|method| *method == "thread/start")
                        .count(),
                    1
                );
                assert_eq!(
                    seen.iter().filter(|method| *method == "turn/start").count(),
                    1
                );
            }
            assert!(
                !seen
                    .iter()
                    .any(|method| method.contains("resume") || method.contains("fork"))
            );
        }
    }
}

#[cfg(all(test, unix))]
mod process_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn request() -> CodexServiceRequest {
        CodexServiceRequest {
            model: "selected-model".into(),
            effort: Some("high".into()),
            instructions: "service instructions".into(),
            input: "fixture portion".into(),
        }
    }

    #[tokio::test]
    async fn service_process_cleanup_covers_success_timeout_and_dropped_call() {
        let fixture = tempfile::tempdir().unwrap();
        let home = fixture.path().join("auth");
        std::fs::create_dir(&home).unwrap();
        let executable = fixture.path().join("mock-codex");
        let script = r#"#!/usr/bin/env python3
import json, os, pathlib, sys, time
home = pathlib.Path(os.environ['CODEX_HOME'])
with (home / 'trace').open('a') as out:
    out.write(json.dumps({'pid': os.getpid(), 'cwd': os.getcwd()}) + '\n')
profile = json.loads('__PROFILE__')
thread = 'temporary-' + str(os.getpid())
for line in sys.stdin:
    call = json.loads(line)
    method = call['method']
    if method == 'initialized':
        continue
    if method == 'initialize':
        result = {'version': '0.154.0'}
    elif method == 'config/read':
        result = {'config': profile}
    elif method == 'thread/start':
        assert call['params']['ephemeral'] is True
        assert call['params']['environments'] == []
        result = {'thread': {'id': thread, 'path': None}, 'model': 'selected-model'}
    elif method == 'turn/start':
        assert call['params']['environments'] == []
        result = {'turn': {'id': 'turn', 'status': 'inProgress'}}
    else:
        raise RuntimeError('unexpected service method')
    print(json.dumps({'jsonrpc': '2.0', 'id': call['id'], 'result': result}), flush=True)
    if method == 'turn/start':
        (home / 'turn-started').touch()
        if (home / 'hang').exists():
            time.sleep(60)
        print(json.dumps({'jsonrpc': '2.0', 'method': 'turn/completed', 'params': {
            'threadId': thread, 'turn': {'id': 'turn', 'status': 'completed', 'error': None,
            'items': [{'type': 'agentMessage', 'phase': 'final_answer', 'text': 'summary'}]}}}), flush=True)
"#.replace("__PROFILE__", &profile().to_string());
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let service = Arc::new(CodexService::new(CodexServiceConfig {
            executable: executable.to_string_lossy().into_owned(),
            home_path: home.to_string_lossy().into_owned(),
            environment: SensitiveEnvironment::new(),
        }));
        for _ in 0..2 {
            assert_eq!(
                service
                    .summarize(request(), Duration::from_secs(5))
                    .await
                    .unwrap()
                    .text,
                "summary"
            );
            assert!(service.attempt.lock().await.is_none());
        }
        std::fs::write(home.join("hang"), "").unwrap();
        assert!(
            service
                .summarize(request(), Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(service.attempt.lock().await.is_none());
        std::fs::remove_file(home.join("turn-started")).unwrap();
        let work = tokio::spawn({
            let service = service.clone();
            async move { service.summarize(request(), Duration::from_secs(60)).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !home.join("turn-started").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        work.abort();
        assert!(matches!(work.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(5), service.cleanup())
            .await
            .unwrap()
            .unwrap();
        assert!(service.attempt.lock().await.is_none());
        let trace = std::fs::read_to_string(home.join("trace")).unwrap();
        let rows = trace
            .lines()
            .map(|line| serde_json::from_str::<JsonValue>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 4);
        let pids = rows
            .iter()
            .map(|row| row["pid"].as_u64().unwrap())
            .collect::<HashSet<_>>();
        assert_eq!(pids.len(), 4, "each attempt must own a fresh process");
        for row in rows {
            assert!(
                !Path::new(row["cwd"].as_str().unwrap()).exists(),
                "owned service directory must be removed after cleanup"
            );
        }
        assert!(!home.join("sessions").exists());
        assert!(!trace.contains("fixture portion"));
    }
}
