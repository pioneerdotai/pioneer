//! Nonpersistent Claude print service, independently owned for each portion.
//! The restricted profile is enforced by explicit CLI controls and validated
//! initialization/result frames, never by a release string. SIMPLE preserves
//! native OAuth/keychain authentication; `--bare` is not an auth-equivalent mode.
use crate::process::{
    CLIAgentProcess, CLIAgentProcessSpawnConfig, SensitiveEnvironment, spawn_cli_agent_process,
};
use crate::service::ServiceFailure;
use anyhow::{Result, ensure};
use pioneer_protocol::ProviderFailureClass;
use serde_json::Value;
use std::{path::Path, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
    time::Instant,
};

const MAX_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct ClaudeServiceConfig {
    pub executable: String,
    pub config_dir: String,
    pub environment: SensitiveEnvironment,
}
pub struct ClaudeServiceRequest {
    pub model: String,
    pub effort: Option<String>,
    pub instructions: String,
    pub input: String,
    pub output_cap: u64,
}
pub struct ClaudeServiceCompletion {
    pub observed_usage: serde_json::Value,
    pub text: String,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}
pub struct ClaudeService {
    config: ClaudeServiceConfig,
    attempt: Mutex<Option<Attempt>>,
}
struct Attempt {
    process: CLIAgentProcess,
    _directory: tempfile::TempDir,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        self.process.abort_service();
    }
}
impl ClaudeService {
    pub fn new(config: ClaudeServiceConfig) -> Self {
        Self {
            config,
            attempt: Mutex::new(None),
        }
    }
    pub async fn summarize(
        &self,
        request: ClaudeServiceRequest,
        timeout: Duration,
    ) -> Result<ClaudeServiceCompletion> {
        let deadline = Instant::now() + timeout;
        ensure!(!request.model.trim().is_empty(), "missing service model");
        ensure!(
            request
                .input
                .len()
                .saturating_add(request.instructions.len())
                <= MAX_BYTES,
            // Local hard payload overflow is typed, just like a confirmed
            // provider context rejection. Callers must not parse raw messages.
            ServiceFailure {
                class: ProviderFailureClass::ContextTooLarge,
                retry_after_ms: None,
            }
        );
        ensure!(request.output_cap > 0, "missing service output cap");
        ensure!(
            request
                .effort
                .as_deref()
                .is_none_or(|value| matches!(value, "low" | "medium" | "high" | "max")),
            "unsupported Claude service effort"
        );
        let run = async {
            let output = self
                .invoke(&request, request.input.as_bytes(), deadline, MAX_BYTES)
                .await?;
            let value = serde_json::from_slice::<Value>(&output)?;
            confirmed_completion(&value, &request.model).map_err(|error| {
                error.context(crate::service::ObservedServiceUsage(
                    crate::service::bounded_usage(terminal_value(&value)),
                ))
            })
        };
        let result = match tokio::time::timeout_at(deadline, run).await {
            Ok(result) => result,
            Err(_) => Err(crate::service::ClaudeServiceDeadline.into()),
        };
        self.cleanup().await?;
        result
    }
    async fn invoke(
        &self,
        request: &ClaudeServiceRequest,
        input: &[u8],
        deadline: Instant,
        cap: usize,
    ) -> Result<Vec<u8>> {
        let result = {
            let mut owner = self.attempt.lock().await;
            ensure!(owner.is_none(), "previous service attempt needs cleanup");
            ensure!(
                Instant::now() < deadline,
                "Claude service deadline exceeded"
            );
            let directory = tempfile::tempdir()?;
            let config = process_config(&self.config, directory.path(), request)?;
            let mut process = spawn_cli_agent_process(&config)?;
            let (stdout, mut stdin) = process.take_stdio()?;
            *owner = Some(Attempt {
                process,
                _directory: directory,
            });
            let write = async move {
                stdin.write_all(input).await?;
                stdin.shutdown().await
            };
            let read = async {
                let mut output = Vec::new();
                stdout.take(cap as u64 + 1).read_to_end(&mut output).await?;
                anyhow::ensure!(
                    output.len() <= cap,
                    "Claude service output exceeds capacity"
                );
                Ok::<_, anyhow::Error>(output)
            };
            let exchange = async {
                let (written, output) = tokio::join!(write, read);
                let output = output?;
                let process = &mut owner.as_mut().unwrap().process;
                let status = process.wait().await?;
                if !status.success() || written.is_err() {
                    let stderr = process.stderr().lines().await;
                    let metadata =
                        crate::service::ServiceProcessFailure::from_status(status, &stderr);
                    let error = ensure_process_outcome(&output, false)
                        .err()
                        .unwrap_or_else(|| anyhow::anyhow!("Claude service process failed"));
                    return Err(error.context(metadata));
                }
                Ok(output)
            };
            exchange.await
        };
        self.cleanup().await?;
        result
    }
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
fn process_config(
    config: &ClaudeServiceConfig,
    cwd: &Path,
    request: &ClaudeServiceRequest,
) -> Result<CLIAgentProcessSpawnConfig> {
    let config_dir = crate::process::expand_home_path(&config.config_dir, None)?;
    let mut spawn = CLIAgentProcessSpawnConfig::codex_app_server(&config.executable, "");
    spawn.home_path = None;
    spawn = spawn
        .with_cwd(cwd)
        .with_environment(&config.environment)
        .with_stderr_ring_lines(8)
        .with_env("CLAUDE_CONFIG_DIR", config_dir.to_string_lossy())
        .with_env("CLAUDE_CODE_SIMPLE", "1")
        .with_env_removed("CLAUDECODE");
    spawn.args = [
        "-p",
        "--no-session-persistence",
        "--output-format",
        "json",
        "--verbose",
        "--tools",
        "",
        "--disallowedTools",
        "*",
        "--strict-mcp-config",
        "--mcp-config",
        "{\"mcpServers\":{}}",
        "--setting-sources",
        "",
        "--disable-slash-commands",
        "--no-chrome",
        "--permission-mode",
        "dontAsk",
        "--settings",
        "{\"disableAllHooks\":true,\"enabledPlugins\":{},\"autoMemoryEnabled\":false}",
        "--model",
        &request.model,
        "--system-prompt",
        &request.instructions,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    if let Some(effort) = &request.effort {
        spawn.args.extend(["--effort".into(), effort.clone()]);
    }
    spawn = spawn.with_env(
        "CLAUDE_CODE_MAX_OUTPUT_TOKENS",
        request.output_cap.to_string(),
    );
    Ok(spawn)
}
/// Production nonzero-exit rejection boundary. Only a parsed native terminal
/// result contributes metrics; malformed/incomplete transcripts are not mined.
/// Preserve the original typed failure and never persist raw result/error text.
pub fn ensure_process_outcome(output: &[u8], success: bool) -> Result<()> {
    if success {
        return Ok(());
    }
    if let Ok(value) = serde_json::from_slice::<Value>(output) {
        let terminal = terminal_value(&value);
        let error: anyhow::Error = failure(terminal).into();
        return Err(if terminal["type"] == "result" {
            crate::service::with_observed_usage(error, crate::service::bounded_usage(terminal))
        } else {
            error
        });
    }
    anyhow::bail!("Claude service process failed")
}

// Verbose JSON must include the init frame carrying the
// model resolved by the CLI itself. This preserves native alias/environment
// resolution without guessing a model or accepting a fallback during generation.
fn terminal_value(value: &Value) -> &Value {
    value
        .as_array()
        .and_then(|frames| frames.last())
        .unwrap_or(value)
}
fn confirmed_completion(value: &Value, requested: &str) -> Result<ClaudeServiceCompletion> {
    let frames = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Claude service model initialization missing"))?;
    let mut initializations = frames
        .iter()
        .filter(|frame| frame["type"] == "system" && frame["subtype"] == "init");
    let init = initializations
        .next()
        .ok_or_else(|| anyhow::anyhow!("Claude service model initialization missing"))?;
    ensure!(
        initializations.next().is_none(),
        "Claude service model initialized twice"
    );
    let resolved = init["model"]
        .as_str()
        .filter(|model| !model.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("Claude service resolved model missing"))?;
    let base = |model: &str| model.strip_suffix("[1m]").unwrap_or(model).to_owned();
    let alias = matches!(
        base(requested).as_str(),
        "default" | "opus" | "sonnet" | "haiku"
    );
    ensure!(
        alias || base(requested) == base(resolved),
        "Claude service selected model changed"
    );
    ensure!(
        frames
            .iter()
            .filter(|frame| frame["type"] == "result")
            .count()
            == 1,
        "Claude service terminal result missing or repeated"
    );
    completion(terminal_value(value), &base(resolved))
}
fn completion(value: &Value, model: &str) -> Result<ClaudeServiceCompletion> {
    if value["type"] != "result" || value["subtype"] != "success" || value["is_error"] != false {
        return Err(failure(value).into());
    }
    ensure!(
        value["permission_denials"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "Claude service requested a working action"
    );
    ensure!(
        value["stop_reason"].as_str() == Some("end_turn"),
        "Claude service answer was interrupted or truncated"
    );
    let models = value["modelUsage"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("Claude service model confirmation missing"))?;
    ensure!(
        models.len() == 1 && models.contains_key(model),
        "Claude service model changed"
    );
    let text = value["result"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("Claude service returned no final answer"))?;
    let usage = &value["usage"];
    let input_tokens = usage["input_tokens"].as_u64().and_then(|input| {
        input
            .checked_add(usage["cache_read_input_tokens"].as_u64()?)?
            .checked_add(usage["cache_creation_input_tokens"].as_u64()?)
    });
    Ok(ClaudeServiceCompletion {
        observed_usage: crate::service::bounded_usage(value),
        text: text.to_owned(),
        input_tokens,
        output_tokens: usage["output_tokens"].as_u64(),
    })
}
fn failure(value: &Value) -> ServiceFailure {
    let class = match value["error"]
        .as_str()
        .or_else(|| value["error"]["type"].as_str())
    {
        Some("rate_limit_error") => ProviderFailureClass::RateLimit,
        Some("overloaded_error" | "api_error") => ProviderFailureClass::Provider5xx,
        Some("authentication_error" | "permission_error") => ProviderFailureClass::AuthOrPermission,
        Some("invalid_request_error") => ProviderFailureClass::InvalidRequest,
        _ if value["subtype"] == "error_max_turns" => ProviderFailureClass::MaxOutputTokens,
        _ => ProviderFailureClass::Unknown,
    };
    let legacy = if class == ProviderFailureClass::Unknown {
        // Print result errors in this release can be message-only. Reuse the
        // ordinary adapter's rate-limit/clock parser, then discard the message.
        let message = value["errors"]
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .or_else(|| value["result"].as_str().map(str::to_owned))
            .unwrap_or_default();
        crate::event::classify_runtime_provider_failure(
            &crate::event::RuntimeEvent::Error(crate::event::RuntimeErrorEvent {
                native_thread_id: None,
                native_turn_id: None,
                message,
                code: None,
                retryable: false,
                native: None,
            }),
            chrono::Utc::now().fixed_offset(),
        )
    } else {
        None
    };
    let class = legacy.as_ref().map_or(class, |failure| failure.class);
    ServiceFailure {
        class,
        retry_after_ms: (class == ProviderFailureClass::RateLimit)
            .then(|| {
                crate::event::find_retry_after_ms(value)
                    .or_else(|| legacy.and_then(|failure| failure.retry_after_ms))
            })
            .flatten(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{collections::HashSet, os::unix::fs::PermissionsExt, sync::Arc};
    #[test]
    fn process_exit_boundary_keeps_terminal_metrics_and_typed_failure_without_secrets() {
        for count in [Some(12_u64), Some(0), None] {
            let mut terminal = json!({"type":"result","subtype":"error_during_execution","error":{"type":"rate_limit_error"},"retry_after_ms":3000,"result":"SECRET_TRANSCRIPT","session_id":"SECRET_SESSION","usage":{"prompt":"SECRET","reasoning_output_tokens":42}});
            if let Some(count) = count {
                terminal["usage"]["input_tokens"] = count.into();
                terminal["usage"]["output_tokens"] = count.into();
                terminal["usage"]["cache_read_input_tokens"] = count.into();
                terminal["usage"]["cache_creation_input_tokens"] = 0.into();
                terminal["total_cost_usd"] = json!(0.2);
            }
            let output = json!([{"type":"system","subtype":"init"}, terminal]).to_string();
            let error = ensure_process_outcome(output.as_bytes(), false)
                .err()
                .unwrap();
            let typed = error.downcast_ref::<ServiceFailure>().unwrap();
            assert_eq!(typed.class, ProviderFailureClass::RateLimit);
            assert_eq!(typed.retry_after_ms, Some(3000));
            let metadata = error.downcast_ref::<crate::service::ObservedServiceUsage>();
            assert_eq!(metadata.is_some(), count.is_some());
            if let Some(metadata) = metadata {
                assert_eq!(metadata.0["input_tokens"].as_u64(), count);
                assert_eq!(metadata.0["output_tokens"].as_u64(), count);
                assert_eq!(metadata.0["cache_read_input_tokens"].as_u64(), count);
                assert_eq!(metadata.0["cache_creation_input_tokens"], 0);
                assert_eq!(metadata.0["total_cost_usd"], 0.2);
                assert!(metadata.0["reasoning_output_tokens"].is_null());
                assert!(!metadata.0.to_string().contains("SECRET"));
            }
            assert!(!format!("{error:#?}").contains("SECRET"));
            assert!(ensure_process_outcome(output.as_bytes(), true).is_ok());
        }
        for output in [
            b"invalid SECRET".as_slice(),
            br#"{"type":"system","usage":{"input_tokens":999},"result":"SECRET"}"#.as_slice(),
        ] {
            let error = ensure_process_outcome(output, false).err().unwrap();
            assert!(
                error
                    .downcast_ref::<crate::service::ObservedServiceUsage>()
                    .is_none()
            );
            assert!(!format!("{error:#}").contains("SECRET"));
        }
    }

    fn request() -> ClaudeServiceRequest {
        ClaudeServiceRequest {
            model: "claude-sonnet-4-6".into(),
            effort: Some("high".into()),
            instructions: "service instructions".into(),
            input: "fixture portion".into(),
            output_cap: 1024,
        }
    }
    #[test]
    fn rejects_incomplete_or_changed_model_and_counts_cached_input() {
        let value = json!({"type":"result","subtype":"success","is_error":false,"stop_reason":"end_turn","result":"summary",
            "modelUsage":{"claude-sonnet-4-6":{}},"usage":{"input_tokens":1,"cache_read_input_tokens":2,"cache_creation_input_tokens":3,"output_tokens":4}});
        let result = completion(&value, "claude-sonnet-4-6").unwrap();
        assert_eq!(result.input_tokens, Some(6));
        assert_eq!(result.output_tokens, Some(4));
        for missing in [
            "input_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ] {
            let mut partial = value.clone();
            partial["usage"].as_object_mut().unwrap().remove(missing);
            let observed = completion(&partial, "claude-sonnet-4-6").unwrap();
            assert_eq!(observed.input_tokens, None);
            assert_eq!(observed.output_tokens, Some(4));
        }
        let mut overflow = value.clone();
        overflow["usage"]["input_tokens"] = json!(u64::MAX);
        assert_eq!(
            completion(&overflow, "claude-sonnet-4-6")
                .unwrap()
                .input_tokens,
            None
        );
        for (key, changed) in [
            ("stop_reason", json!("max_tokens")),
            ("is_error", json!(true)),
            ("modelUsage", json!({"different-model":{}})),
            ("permission_denials", json!([{"tool_name":"Bash"}])),
        ] {
            let mut invalid = value.clone();
            invalid[key] = changed;
            assert!(completion(&invalid, "claude-sonnet-4-6").is_err());
        }
    }
    #[test]
    fn aliases_use_initialized_model_and_reject_mid_attempt_fallback() {
        let init = json!({"type":"system","subtype":"init","model":"claude-sonnet-4-6"});
        let result = json!({"type":"result","subtype":"success","is_error":false,
            "stop_reason":"end_turn","result":"summary","modelUsage":{"claude-sonnet-4-6":{}},"usage":{}});
        for requested in ["sonnet", "default", "sonnet[1m]", "claude-sonnet-4-6"] {
            assert!(confirmed_completion(&json!([init, result]), requested).is_ok());
        }
        assert!(confirmed_completion(&json!([init, result]), "claude-opus-4-6").is_err());
        assert!(confirmed_completion(&result, "sonnet").is_err());
        assert!(confirmed_completion(&json!([init, init, result]), "sonnet").is_err());
        let mut changed = result.clone();
        changed["modelUsage"] = json!({"claude-opus-4-6":{}});
        assert!(confirmed_completion(&json!([init, changed]), "sonnet").is_err());
    }

    #[test]
    fn print_error_rate_limit_uses_existing_retry_parser_without_retaining_text() {
        let result = failure(
            &json!({"type":"result","subtype":"error_during_execution","errors":["private: usage limit exceeded"],"retry_after_ms":1234}),
        );
        assert_eq!(result.class, ProviderFailureClass::RateLimit);
        assert_eq!(result.retry_after_ms, Some(1234));
        assert!(!format!("{result:?}").contains("private"));
        let permanent = failure(
            &json!({"error":{"type":"authentication_error"},"errors":["rate limit"],"retry_after_ms":1234}),
        );
        assert_eq!(permanent.class, ProviderFailureClass::AuthOrPermission);
        assert_eq!(permanent.retry_after_ms, None);
    }

    #[tokio::test]
    async fn fresh_print_expands_auth_without_version_gate_and_cleans_up() {
        let user_home = std::path::PathBuf::from(std::env::var_os("HOME").unwrap());
        let fixture = tempfile::tempdir_in(&user_home).unwrap();
        let home = fixture.path().join("auth");
        std::fs::create_dir(&home).unwrap();
        let executable = fixture.path().join("mock-claude");
        std::fs::write(&executable, r#"#!/usr/bin/env python3
import json, os, pathlib, sys, time
home = pathlib.Path(os.environ['CLAUDE_CONFIG_DIR'])
assert os.environ['CLAUDE_CODE_SIMPLE'] == '1'
args = sys.argv[1:]
kind = 'version' if args == ['--version'] else 'print'
with (home / 'trace').open('a') as out:
    out.write(json.dumps({'pid':os.getpid(),'cwd':os.getcwd(),'kind':kind}) + '\n')
if kind == 'version':
    raise AssertionError('print service must not gate on CLI release')
assert args[0] == '-p'
for flag in ['--no-session-persistence','--strict-mcp-config','--disable-slash-commands','--no-chrome']:
    assert flag in args
for flag in ['--resume','--continue','--fork-session','--session-id','--fallback-model','--bare']:
    assert flag not in args
for flag, expected in [('--tools',''),('--disallowedTools','*'),('--setting-sources',''),('--effort','high'),('--model','claude-sonnet-4-6'),('--system-prompt','service instructions')]:
    assert args[args.index(flag)+1] == expected
assert json.loads(args[args.index('--mcp-config')+1]) == {'mcpServers':{}}
assert json.loads(args[args.index('--settings')+1])['disableAllHooks'] is True
assert os.environ['CLAUDE_CODE_MAX_OUTPUT_TOKENS'] == '1024'
assert sys.stdin.read() == 'fixture portion'
(home / 'started').write_text('yes')
if (home / 'hang').exists(): time.sleep(60)
assert '--verbose' in args
print(json.dumps([{'type':'system','subtype':'init','model':'claude-sonnet-4-6'}, {'type':'result','subtype':'success','is_error':False,'stop_reason':'end_turn','result':'summary','modelUsage':{'claude-sonnet-4-6':{}},'usage':{'input_tokens':3,'output_tokens':4}}]))
"#).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let service = Arc::new(ClaudeService::new(ClaudeServiceConfig {
            executable: executable.to_string_lossy().into_owned(),
            config_dir: format!("~/{}", home.strip_prefix(&user_home).unwrap().display()),
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
        std::fs::write(home.join("hang"), "yes").unwrap();
        assert!(
            service
                .summarize(request(), Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(service.attempt.lock().await.is_none());
        std::fs::remove_file(home.join("started")).unwrap();
        let task = tokio::spawn({
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
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(5), service.cleanup())
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_file(home.join("hang")).unwrap();
        assert!(
            service
                .summarize(request(), Duration::from_secs(5))
                .await
                .is_ok()
        );
        let trace = std::fs::read_to_string(home.join("trace")).unwrap();
        let rows = trace
            .lines()
            .map(|s| serde_json::from_str::<Value>(s).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 5); // only the five owned print attempts
        assert_eq!(
            rows.iter()
                .map(|v| v["pid"].as_u64().unwrap())
                .collect::<HashSet<_>>()
                .len(),
            5
        );
        assert_eq!(rows.iter().filter(|v| v["kind"] == "print").count(), 5);
        for row in rows {
            assert!(!Path::new(row["cwd"].as_str().unwrap()).exists());
        }
        assert!(!trace.contains("fixture portion"));
        assert!(!home.join("projects").exists());
    }
}
