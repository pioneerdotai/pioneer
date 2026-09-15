//! Isolated, nonpersistent Codex exec service. Each portion gets a fresh
//! ephemeral context and uses the selected authorization home without loading
//! its user config, MCP servers, plugins, rules, or conversation history.
use super::*;
use crate::event::{
    RuntimeEventMappingOptions, classify_runtime_provider_failure, map_codex_notification_event,
};
use crate::process::{CLIAgentProcess, SensitiveEnvironment};
use crate::service::{ServiceFailure, ServiceStage};
use anyhow::{Context, Result, bail, ensure};
use pioneer_protocol::ProviderFailureClass;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

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
    process: CLIAgentProcess,
    _directory: tempfile::TempDir,
}
impl Drop for ServiceAttempt {
    fn drop(&mut self) {
        self.process.abort_service();
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
        let run = async {
            let (version, success) = self.invoke(None, deadline, 4096).await?;
            ensure!(
                success && std::str::from_utf8(&version)?.trim() == format!("codex-cli {RELEASE}"),
                "unsupported Codex service capability version"
            );
            let (output, success) = self
                .invoke(Some(&request), deadline, MAX_SERVICE_BYTES)
                .await?;
            let completion =
                decode_exec_completion(&output).context(ServiceStage("cli_exec_decode"))?;
            ensure!(success, "Codex service process failed");
            Ok(completion)
        };
        let result = match tokio::time::timeout_at(deadline, run).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("Codex service deadline exceeded")),
        };
        self.cleanup().await?;
        result
    }
    async fn invoke(
        &self,
        request: Option<&CodexServiceRequest>,
        deadline: Instant,
        cap: usize,
    ) -> Result<(Vec<u8>, bool)> {
        let result = {
            let mut owner = self.attempt.lock().await;
            ensure!(owner.is_none(), "previous service attempt needs cleanup");
            ensure!(Instant::now() < deadline, "Codex service deadline exceeded");
            let directory = tempfile::tempdir().context(ServiceStage("cli_directory"))?;
            if let Some(request) = request {
                // Instructions and transcript never go into process arguments.
                // The private file is removed together with the owned attempt.
                std::fs::write(
                    directory.path().join("instructions.txt"),
                    &request.instructions,
                )
                .context(ServiceStage("cli_configuration"))?;
            }
            let config = process_config(&self.config, directory.path(), request)
                .context(ServiceStage("cli_configuration"))?;
            let mut process =
                spawn_cli_agent_process(&config).context(ServiceStage("cli_spawn"))?;
            let (stdout, mut stdin) = process.take_stdio().context(ServiceStage("cli_stdio"))?;
            *owner = Some(ServiceAttempt {
                process,
                _directory: directory,
            });
            let write = async move {
                stdin
                    .write_all(request.map_or(&[][..], |r| r.input.as_bytes()))
                    .await?;
                stdin.shutdown().await
            };
            let read = async {
                let mut output = Vec::new();
                stdout.take(cap as u64 + 1).read_to_end(&mut output).await?;
                ensure!(output.len() <= cap, "Codex service output exceeds capacity");
                Ok::<_, anyhow::Error>(output)
            };
            let exchange = async {
                let (_, output) =
                    tokio::try_join!(async { write.await.map_err(anyhow::Error::from) }, read)?;
                let status = owner.as_mut().unwrap().process.wait().await?;
                Ok((output, status.success()))
            };
            exchange.await
        };
        self.cleanup().await?;
        result
    }
    /// The owner survives cancellation until cleanup or Drop kills the process.
    pub async fn cleanup(&self) -> Result<()> {
        let mut owner = self.attempt.lock().await;
        if let Some(attempt) = owner.as_mut() {
            let result = attempt
                .process
                .terminate_with_grace(Duration::from_secs(1))
                .await;
            owner.take();
            result?;
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
    json!({"features":features, "agents":{"enabled":false},
        "web_search":"disabled", "project_doc_max_bytes":0,
        "include_environment_context":false, "include_collaboration_mode_instructions":false,
        "history":{"persistence":"none"}, "suppress_unstable_features_warning":true})
}
fn process_config(
    config: &CodexServiceConfig,
    cwd: &Path,
    request: Option<&CodexServiceRequest>,
) -> Result<CLIAgentProcessSpawnConfig> {
    let mut process =
        CLIAgentProcessSpawnConfig::codex_app_server(&config.executable, &config.home_path)
            .with_cwd(cwd)
            .with_environment(&config.environment)
            .with_stderr_ring_lines(0);
    process.args = vec!["--version".into()];
    if let Some(request) = request {
        process.args = [
            "exec",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--json",
            "--model",
            &request.model,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        for (key, value) in profile().as_object().unwrap() {
            process.args.extend([
                "--config".into(),
                format!("{key}={}", toml::Value::try_from(value)?),
            ]);
        }
        process.args.extend([
            "--config".into(),
            format!(
                "model_instructions_file={}",
                toml::Value::String(cwd.join("instructions.txt").to_string_lossy().into_owned())
            ),
        ]);
        if let Some(effort) = &request.effort {
            process.args.extend([
                "--config".into(),
                format!(
                    "model_reasoning_effort={}",
                    toml::Value::String(effort.clone())
                ),
            ]);
        }
        process.args.push("-".into());
    }
    Ok(process)
}

fn decode_exec_completion(output: &[u8]) -> Result<CodexServiceCompletion> {
    let mut completion = CodexServiceCompletion {
        text: String::new(),
        input_tokens: None,
        output_tokens: None,
    };
    let (mut thread, mut started, mut completed) = (false, false, false);
    for line in output
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event: JsonValue =
            serde_json::from_slice(line).context("invalid Codex service event")?;
        ensure!(!completed, "Codex service emitted events after completion");
        match event["type"].as_str() {
            Some("thread.started") => {
                ensure!(
                    !thread && event["thread_id"].as_str().is_some_and(|id| !id.is_empty()),
                    "invalid Codex service thread start"
                );
                thread = true;
            }
            Some("turn.started") => {
                ensure!(thread && !started, "invalid Codex service turn start");
                started = true;
            }
            Some("item.started" | "item.updated" | "item.completed") => {
                let item = &event["item"];
                match item["type"].as_str() {
                    Some("agent_message") => {
                        ensure!(started, "Codex service answer preceded turn start");
                        if event["type"] == "item.completed"
                            && item["phase"].as_str() != Some("commentary")
                        {
                            completion.text = item["text"]
                                .as_str()
                                .ok_or_else(|| anyhow::anyhow!("invalid service answer"))?
                                .to_owned();
                        }
                    }
                    Some("reasoning" | "error") => {} // CLI configuration warnings are item errors, not failed turns.
                    _ => bail!("Codex service attempted a working action"),
                }
            }
            Some("turn.failed") => {
                let params = json!({"error":event["error"]});
                return Err(terminal_failure(&CodexJsonlRpcNotificationEvent {
                    method: "error".into(),
                    params: Some(params.clone()),
                    raw: params,
                })
                .into());
            }
            Some("turn.completed") => {
                ensure!(
                    started && !completion.text.trim().is_empty(),
                    "Codex service returned no final answer"
                );
                completion.input_tokens = event["usage"]["input_tokens"].as_u64();
                completion.output_tokens = event["usage"]["output_tokens"].as_u64();
                completed = true;
            }
            Some("error") => {} // Retriable stream notices; success still requires a terminal completion.
            _ => bail!("invalid Codex service event"),
        }
    }
    ensure!(completed, "Codex service ended without completion");
    Ok(completion)
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

#[cfg(test)]
mod tests {
    use super::*;
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
}

#[cfg(all(test, unix))]
mod exec_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn request() -> CodexServiceRequest {
        CodexServiceRequest {
            model: "gpt-5.6-luna".into(),
            effort: Some("low".into()),
            instructions: "service instructions".into(),
            input: "private fixture portion".into(),
        }
    }
    fn events() -> Vec<JsonValue> {
        vec![
            json!({"type":"thread.started","thread_id":"temporary"}),
            json!({"type":"item.completed","item":{"type":"error","message":"config warning"}}),
            json!({"type":"turn.started"}),
            json!({"type":"item.completed","item":{"type":"agent_message","text":"summary"}}),
            json!({"type":"turn.completed","usage":{"input_tokens":123,"output_tokens":7}}),
        ]
    }
    fn encode(events: &[JsonValue]) -> Vec<u8> {
        events
            .iter()
            .map(|e| format!("{e}\n"))
            .collect::<String>()
            .into_bytes()
    }

    #[test]
    fn exec_completion_requires_final_success_and_rejects_working_actions() {
        let valid = events();
        let completion = decode_exec_completion(&encode(&valid)).unwrap();
        assert_eq!(completion.text, "summary");
        assert_eq!(completion.input_tokens, Some(123));
        assert_eq!(completion.output_tokens, Some(7));
        assert!(decode_exec_completion(&encode(&valid[..4])).is_err());
        let mut failed = valid.clone();
        failed[4] = json!({"type":"turn.failed","error":{"message":"private source text","codexErrorInfo":"contextWindowExceeded"}});
        let error = decode_exec_completion(&encode(&failed)).err().unwrap();
        assert_eq!(
            error.downcast_ref::<ServiceFailure>().unwrap().class,
            ProviderFailureClass::ContextTooLarge
        );
        assert!(!format!("{error:?}").contains("private source"));
        for kind in [
            "command_execution",
            "file_change",
            "mcp_tool_call",
            "web_search",
            "todo_list",
            "unexpected",
        ] {
            let mut malicious = valid.clone();
            malicious.insert(3, json!({"type":"item.started","item":{"type":kind}}));
            assert!(decode_exec_completion(&encode(&malicious)).is_err());
        }
        let mut duplicate = valid.clone();
        duplicate.extend(valid);
        assert!(decode_exec_completion(&encode(&duplicate)).is_err());
        assert!(decode_exec_completion(b"invalid JSON").is_err());
    }

    #[tokio::test]
    async fn exec_ignores_populated_user_config_and_cleans_up_success_timeout_and_cancellation() {
        let fixture = tempfile::tempdir().unwrap();
        let home = fixture.path().join("auth");
        std::fs::create_dir(&home).unwrap();
        let user_config = "[mcp_servers.unexpected]\ncommand='never-run'\n[plugins.unexpected]\nenabled=true\n[projects.fixture]\ntrust_level='trusted'\n";
        std::fs::write(home.join("config.toml"), user_config).unwrap();
        let executable = fixture.path().join("mock-codex");
        let script=r#"#!/usr/bin/env python3
import json, os, pathlib, sys, time
home=pathlib.Path(os.environ['CODEX_HOME'])
if sys.argv[1:] == ['--version']:
    print('codex-cli 0.154.0-canary' if (home/'unsupported').exists() else 'codex-cli 0.154.0');sys.exit(0)
a=sys.argv[1:]
assert a[0]=='exec' and a[-1]=='-'
assert all(flag in a for flag in ['--ephemeral','--ignore-user-config','--ignore-rules','--skip-git-repo-check','--json'])
assert a[a.index('--model')+1]=='gpt-5.6-luna'
assert a[a.index('--sandbox')+1]=='read-only'
assert 'model_reasoning_effort="low"' in a
assert 'private fixture portion' not in str(a)
assert 'service instructions' not in str(a)
assert pathlib.Path('instructions.txt').read_text()=='service instructions'
assert '[mcp_servers.unexpected]' in (home/'config.toml').read_text()
assert sys.stdin.read()=='private fixture portion'
with (home/'trace').open('a') as f: f.write(json.dumps({'pid':os.getpid(),'cwd':os.getcwd()})+'\n')
(home/'started').touch()
if (home/'hang').exists(): time.sleep(60)
for e in __EVENTS__: print(json.dumps(e),flush=True)
if (home/'bad-exit').exists(): sys.exit(1)
"#.replace("__EVENTS__", &serde_json::to_string(&events()).unwrap());
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let service = Arc::new(CodexService::new(CodexServiceConfig {
            executable: executable.to_string_lossy().into(),
            home_path: home.to_string_lossy().into(),
            environment: SensitiveEnvironment::new(),
        }));
        std::fs::write(home.join("unsupported"), "").unwrap();
        assert!(
            service
                .summarize(request(), Duration::from_secs(5))
                .await
                .is_err()
        );
        assert!(!home.join("trace").exists());
        std::fs::remove_file(home.join("unsupported")).unwrap();
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
        std::fs::write(home.join("bad-exit"), "").unwrap();
        assert!(
            service
                .summarize(request(), Duration::from_secs(5))
                .await
                .is_err()
        );
        std::fs::remove_file(home.join("bad-exit")).unwrap();
        std::fs::write(home.join("hang"), "").unwrap();
        assert!(
            service
                .summarize(request(), Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(service.attempt.lock().await.is_none());
        std::fs::remove_file(home.join("started")).unwrap();
        let work = tokio::spawn({
            let service = service.clone();
            async move { service.summarize(request(), Duration::from_secs(60)).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !home.join("started").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        work.abort();
        assert!(matches!(work.await, Err(error) if error.is_cancelled()));
        service.cleanup().await.unwrap();
        assert!(service.attempt.lock().await.is_none());
        let rows = std::fs::read_to_string(home.join("trace"))
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str::<JsonValue>(s).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 5);
        for row in rows {
            assert!(!Path::new(row["cwd"].as_str().unwrap()).exists());
            let pid = row["pid"].as_i64().unwrap() as i32;
            assert_eq!(
                unsafe { libc::kill(pid, 0) },
                -1,
                "owned process still alive"
            );
        }
        assert!(!home.join("sessions").exists());
        assert_eq!(
            std::fs::read_to_string(home.join("config.toml")).unwrap(),
            user_config
        );
    }

    /// Explicit opt-in: one real model request using the installed CLI's normal auth.
    #[tokio::test]
    #[ignore = "requires explicit authorization for a real Luna Low request"]
    async fn live_exec_service_with_existing_user_config() {
        assert_eq!(
            std::env::var("PIONEER_CODEX_SERVICE_LIVE").as_deref(),
            Ok("1")
        );
        let home = std::env::var("HOME").unwrap();
        let service = CodexService::new(CodexServiceConfig {
            executable: "codex".into(),
            home_path: format!("{home}/.codex"),
            environment: SensitiveEnvironment::new(),
        });
        let response=service.summarize(CodexServiceRequest {model:"gpt-5.6-luna".into(),effort:Some("low".into()),instructions:"You are a text-only test service. Reply with exactly the text requested by the user. Do not use tools.".into(),input:"Reply exactly PIONEER_SERVICE_OK".into()},Duration::from_secs(90)).await.unwrap();
        assert_eq!(response.text.trim(), "PIONEER_SERVICE_OK");
        assert!(response.input_tokens.is_some());
        assert!(service.attempt.lock().await.is_none());
    }
}
