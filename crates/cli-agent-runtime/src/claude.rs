//! Claude CLI streaming runtime primitives.

use crate::process::expand_home_path;
use pioneer_protocol::normalize_metadata_reasoning_effort;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

const MINIMUM_CLAUDE_CODE_VERSION: &str = "2.0.0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeAccountProbeConfig {
    pub executable: String,
    pub config_dir_path: String,
    pub home_dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub request_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeAccountProbeSnapshot {
    pub status: ClaudeAccountProbeStatus,
    pub message: Option<String>,
    pub version: Option<String>,
    pub account: Option<ClaudeAccountSnapshot>,
    pub diagnostics: Vec<ClaudeProbeDiagnostic>,
    pub stderr: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeAccountProbeStatus {
    Ready,
    NeedsAuth,
    MissingBinary,
    SpawnFailed,
    UnsupportedVersion,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeAccountSnapshot {
    pub authenticated: bool,
    pub email: Option<String>,
    pub plan: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeProbeDiagnosticLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeProbeDiagnostic {
    pub level: ClaudeProbeDiagnosticLevel,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeModelListSnapshot {
    pub models: Vec<ClaudeModelSnapshot>,
    pub diagnostics: Vec<ClaudeProbeDiagnostic>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeModelSnapshot {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub family: Option<String>,
    pub active: Option<bool>,
    pub effort_options: Vec<String>,
    pub input_modalities: Vec<String>,
    pub output_modalities: Vec<String>,
    pub supports_reasoning: Option<bool>,
    pub supports_vision: Option<bool>,
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
}

pub struct ClaudeProbe;

impl ClaudeProbe {
    pub async fn account_read(config: ClaudeAccountProbeConfig) -> ClaudeAccountProbeSnapshot {
        let config_dir =
            match expand_home_path(config.config_dir_path.as_str(), config.home_dir.as_deref()) {
                Ok(path) => path,
                Err(error) => {
                    return ClaudeAccountProbeSnapshot {
                        status: ClaudeAccountProbeStatus::Error,
                        message: Some(format!("failed to resolve Claude config dir: {error:#}")),
                        version: None,
                        account: None,
                        diagnostics: vec![ClaudeProbeDiagnostic {
                            level: ClaudeProbeDiagnosticLevel::Error,
                            code: "claude_probe.config_dir_invalid".to_owned(),
                            message: format!("failed to resolve Claude config dir: {error:#}"),
                        }],
                        stderr: Vec::new(),
                    };
                }
            };

        let mut command = Command::new(config.executable.as_str());
        command.arg("--version");
        command.env("CLAUDE_CONFIG_DIR", &config_dir);
        command.env("CLAUDE_CODE_ENTRYPOINT", "sdk-rs");
        command.env("CLAUDE_AGENT_SDK_CLIENT_APP", "pioneer");
        command.envs(
            config
                .env
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        command.env_remove("CLAUDECODE");

        let output = match timeout(config.request_timeout, command.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => return spawn_error_probe_snapshot(&config, error),
            Err(_) => {
                let message = format!(
                    "Claude CLI version probe timed out after {} ms",
                    config.request_timeout.as_millis()
                );
                return ClaudeAccountProbeSnapshot {
                    status: ClaudeAccountProbeStatus::Error,
                    message: Some(message.clone()),
                    version: None,
                    account: None,
                    diagnostics: vec![ClaudeProbeDiagnostic {
                        level: ClaudeProbeDiagnosticLevel::Error,
                        code: "claude_probe.version_timeout".to_owned(),
                        message,
                    }],
                    stderr: Vec::new(),
                };
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = stderr_lines(&output.stderr);
        if !output.status.success() {
            let message = stderr.first().cloned().unwrap_or_else(|| {
                format!("Claude CLI version probe exited with {}", output.status)
            });
            return ClaudeAccountProbeSnapshot {
                status: ClaudeAccountProbeStatus::Error,
                message: Some(message.clone()),
                version: None,
                account: None,
                diagnostics: vec![ClaudeProbeDiagnostic {
                    level: ClaudeProbeDiagnosticLevel::Error,
                    code: "claude_probe.version_failed".to_owned(),
                    message,
                }],
                stderr,
            };
        }

        let version = parse_semver(stdout.as_str())
            .or_else(|| stderr.iter().find_map(|line| parse_semver(line.as_str())));
        let Some(version) = version else {
            let message = "Claude CLI version probe did not return a parseable version".to_owned();
            return ClaudeAccountProbeSnapshot {
                status: ClaudeAccountProbeStatus::Error,
                message: Some(message.clone()),
                version: None,
                account: None,
                diagnostics: vec![ClaudeProbeDiagnostic {
                    level: ClaudeProbeDiagnosticLevel::Error,
                    code: "claude_probe.version_unparseable".to_owned(),
                    message,
                }],
                stderr,
            };
        };

        if semver_lt(version.as_str(), MINIMUM_CLAUDE_CODE_VERSION) {
            let message = format!(
                "Claude CLI version {version} is older than required {MINIMUM_CLAUDE_CODE_VERSION}"
            );
            return ClaudeAccountProbeSnapshot {
                status: ClaudeAccountProbeStatus::UnsupportedVersion,
                message: Some(message.clone()),
                version: Some(version),
                account: None,
                diagnostics: vec![ClaudeProbeDiagnostic {
                    level: ClaudeProbeDiagnosticLevel::Error,
                    code: "claude_probe.unsupported_version".to_owned(),
                    message,
                }],
                stderr,
            };
        }

        match run_initialize_probe(&config, config_dir.as_path()).await {
            Ok(initialize) => {
                let mut diagnostics = vec![ClaudeProbeDiagnostic {
                    level: ClaudeProbeDiagnosticLevel::Info,
                    code: "claude_probe.ready".to_owned(),
                    message: "Claude CLI initialize probe succeeded".to_owned(),
                }];
                diagnostics.extend(initialize.diagnostics.clone());
                let mut stderr = stderr;
                stderr.extend(initialize.stderr);
                ClaudeAccountProbeSnapshot {
                    status: ClaudeAccountProbeStatus::Ready,
                    message: None,
                    version: Some(version),
                    account: initialize.account,
                    diagnostics,
                    stderr,
                }
            }
            Err(error) => error.into_account_snapshot(Some(version), stderr),
        }
    }

    pub async fn model_list(
        config: ClaudeAccountProbeConfig,
        custom_models: &[String],
    ) -> ClaudeModelListSnapshot {
        let config_dir =
            match expand_home_path(config.config_dir_path.as_str(), config.home_dir.as_deref()) {
                Ok(path) => path,
                Err(error) => {
                    return ClaudeModelListSnapshot {
                        models: Vec::new(),
                        diagnostics: vec![ClaudeProbeDiagnostic {
                            level: ClaudeProbeDiagnosticLevel::Error,
                            code: "claude_probe.config_dir_invalid".to_owned(),
                            message: format!("failed to resolve Claude config dir: {error:#}"),
                        }],
                        error_message: Some(format!(
                            "failed to resolve Claude config dir: {error:#}"
                        )),
                    };
                }
            };

        let initialize = match run_initialize_probe(&config, config_dir.as_path()).await {
            Ok(initialize) => initialize,
            Err(error) => {
                return ClaudeModelListSnapshot {
                    models: Vec::new(),
                    diagnostics: error.diagnostics(),
                    error_message: Some(error.message()),
                };
            }
        };
        let mut models = if initialize.models.is_empty() {
            default_claude_models()
        } else {
            initialize.models
        };
        append_custom_models(&mut models, custom_models);
        ClaudeModelListSnapshot {
            models,
            diagnostics: initialize.diagnostics,
            error_message: None,
        }
    }
}

#[derive(Debug)]
struct ClaudeInitializeProbeSnapshot {
    account: Option<ClaudeAccountSnapshot>,
    models: Vec<ClaudeModelSnapshot>,
    diagnostics: Vec<ClaudeProbeDiagnostic>,
    stderr: Vec<String>,
}

#[derive(Debug)]
enum ClaudeInitializeProbeError {
    NeedsAuth {
        message: String,
        stderr: Vec<String>,
    },
    SpawnFailed {
        message: String,
        stderr: Vec<String>,
    },
    Timeout {
        timeout: Duration,
    },
    Protocol {
        message: String,
        stderr: Vec<String>,
    },
}

impl ClaudeInitializeProbeError {
    fn message(&self) -> String {
        match self {
            Self::NeedsAuth { message, .. }
            | Self::SpawnFailed { message, .. }
            | Self::Protocol { message, .. } => message.clone(),
            Self::Timeout { timeout } => {
                format!(
                    "Claude CLI initialize probe timed out after {} ms",
                    timeout.as_millis()
                )
            }
        }
    }

    fn diagnostics(&self) -> Vec<ClaudeProbeDiagnostic> {
        let (level, code) = match self {
            Self::NeedsAuth { .. } => (
                ClaudeProbeDiagnosticLevel::Warning,
                "claude_probe.needs_auth",
            ),
            Self::SpawnFailed { .. } => (
                ClaudeProbeDiagnosticLevel::Error,
                "claude_probe.initialize_failed",
            ),
            Self::Timeout { .. } => (
                ClaudeProbeDiagnosticLevel::Error,
                "claude_probe.initialize_timeout",
            ),
            Self::Protocol { .. } => (
                ClaudeProbeDiagnosticLevel::Error,
                "claude_probe.initialize_invalid",
            ),
        };
        vec![ClaudeProbeDiagnostic {
            level,
            code: code.to_owned(),
            message: self.message(),
        }]
    }

    fn stderr(self) -> Vec<String> {
        match self {
            Self::NeedsAuth { stderr, .. }
            | Self::SpawnFailed { stderr, .. }
            | Self::Protocol { stderr, .. } => stderr,
            Self::Timeout { .. } => Vec::new(),
        }
    }

    fn status(&self) -> ClaudeAccountProbeStatus {
        match self {
            Self::NeedsAuth { .. } => ClaudeAccountProbeStatus::NeedsAuth,
            Self::SpawnFailed { .. } => ClaudeAccountProbeStatus::SpawnFailed,
            Self::Timeout { .. } | Self::Protocol { .. } => ClaudeAccountProbeStatus::Error,
        }
    }

    fn into_account_snapshot(
        self,
        version: Option<String>,
        mut prior_stderr: Vec<String>,
    ) -> ClaudeAccountProbeSnapshot {
        let status = self.status();
        let message = self.message();
        let diagnostics = self.diagnostics();
        prior_stderr.extend(self.stderr());
        ClaudeAccountProbeSnapshot {
            status,
            message: Some(message),
            version,
            account: None,
            diagnostics,
            stderr: prior_stderr,
        }
    }
}

async fn run_initialize_probe(
    config: &ClaudeAccountProbeConfig,
    config_dir: &std::path::Path,
) -> Result<ClaudeInitializeProbeSnapshot, ClaudeInitializeProbeError> {
    let mut command = Command::new(config.executable.as_str());
    command
        .args([
            "--output-format",
            "stream-json",
            "--verbose",
            "--system-prompt",
            "",
            "--permission-prompt-tool",
            "stdio",
            "--permission-mode",
            "default",
            "--safe-mode",
            "--setting-sources=",
            "--include-partial-messages",
            "--input-format",
            "stream-json",
        ])
        .env("CLAUDE_CONFIG_DIR", config_dir)
        .env("CLAUDE_CODE_ENTRYPOINT", "sdk-rs")
        .env("CLAUDE_AGENT_SDK_CLIENT_APP", "pioneer")
        .env("CLAUDE_AGENT_SDK_VERSION", env!("CARGO_PKG_VERSION"))
        .envs(
            config
                .env
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .env_remove("CLAUDECODE")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command
        .spawn()
        .map_err(|error| ClaudeInitializeProbeError::SpawnFailed {
            message: format!("failed to spawn Claude CLI initialize probe: {error}"),
            stderr: Vec::new(),
        })?;
    if let Some(mut stdin) = child.stdin.take() {
        let initialize_line = serde_json::json!({
            "type": "control_request",
            "request_id": "pioneer_probe_initialize",
            "request": { "subtype": "initialize", "hooks": null },
        })
        .to_string();
        if let Err(error) = stdin.write_all(initialize_line.as_bytes()).await {
            return Err(ClaudeInitializeProbeError::SpawnFailed {
                message: format!("failed to write Claude initialize probe: {error}"),
                stderr: Vec::new(),
            });
        }
        if let Err(error) = stdin.write_all(b"\n").await {
            return Err(ClaudeInitializeProbeError::SpawnFailed {
                message: format!("failed to finish Claude initialize probe: {error}"),
                stderr: Vec::new(),
            });
        }
    }

    let output = match timeout(config.request_timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return Err(ClaudeInitializeProbeError::SpawnFailed {
                message: format!("Claude CLI initialize probe failed: {error}"),
                stderr: Vec::new(),
            });
        }
        Err(_) => {
            return Err(ClaudeInitializeProbeError::Timeout {
                timeout: config.request_timeout,
            });
        }
    };
    let stderr = stderr_lines(&output.stderr);
    if !output.status.success() {
        let message = stderr.first().cloned().unwrap_or_else(|| {
            format!("Claude CLI initialize probe exited with {}", output.status)
        });
        return Err(classify_initialize_error(message, stderr));
    }

    parse_initialize_probe_stdout(&output.stdout, stderr)
}

fn parse_initialize_probe_stdout(
    stdout: &[u8],
    stderr: Vec<String>,
) -> Result<ClaudeInitializeProbeSnapshot, ClaudeInitializeProbeError> {
    for line in String::from_utf8_lossy(stdout).lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<JsonValue>(trimmed) else {
            continue;
        };
        match value.get("type").and_then(JsonValue::as_str) {
            Some("control_response") => {
                let response = value.get("response").cloned().unwrap_or(JsonValue::Null);
                if response.get("request_id").and_then(JsonValue::as_str)
                    != Some("pioneer_probe_initialize")
                {
                    continue;
                }
                if response.get("subtype").and_then(JsonValue::as_str) == Some("error") {
                    let message = response
                        .get("error")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("Claude initialize probe failed")
                        .to_owned();
                    return Err(classify_initialize_error(message, stderr));
                }
                let body = response.get("response").cloned().unwrap_or(JsonValue::Null);
                let models = body
                    .get("models")
                    .and_then(JsonValue::as_array)
                    .map(|models| {
                        models
                            .iter()
                            .filter_map(claude_model_from_initialize_model)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let diagnostics = if models.is_empty() {
                    vec![ClaudeProbeDiagnostic {
                        level: ClaudeProbeDiagnosticLevel::Warning,
                        code: "claude_probe.models_missing".to_owned(),
                        message: "Claude initialize probe did not return model metadata; using built-in defaults".to_owned(),
                    }]
                } else {
                    Vec::new()
                };
                return Ok(ClaudeInitializeProbeSnapshot {
                    account: body
                        .get("account")
                        .and_then(claude_account_from_initialize_account),
                    models,
                    diagnostics,
                    stderr,
                });
            }
            Some("auth_status") => {
                let message = value
                    .get("error")
                    .or_else(|| value.get("output"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("Claude CLI authentication is required")
                    .to_owned();
                return Err(ClaudeInitializeProbeError::NeedsAuth { message, stderr });
            }
            Some("error") => {
                let message = value
                    .get("error")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("Claude initialize probe failed")
                    .to_owned();
                return Err(classify_initialize_error(message, stderr));
            }
            _ => {}
        }
    }
    Err(ClaudeInitializeProbeError::Protocol {
        message: "Claude initialize probe did not return an initialize response".to_owned(),
        stderr,
    })
}

fn classify_initialize_error(message: String, stderr: Vec<String>) -> ClaudeInitializeProbeError {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("auth")
        || lowered.contains("login")
        || lowered.contains("oauth")
        || lowered.contains("credential")
    {
        ClaudeInitializeProbeError::NeedsAuth { message, stderr }
    } else {
        ClaudeInitializeProbeError::Protocol { message, stderr }
    }
}

fn spawn_error_probe_snapshot(
    config: &ClaudeAccountProbeConfig,
    error: std::io::Error,
) -> ClaudeAccountProbeSnapshot {
    let is_missing_binary = error.kind() == ErrorKind::NotFound;
    let status = if is_missing_binary {
        ClaudeAccountProbeStatus::MissingBinary
    } else {
        ClaudeAccountProbeStatus::SpawnFailed
    };
    let code = if is_missing_binary {
        "claude_probe.missing_binary"
    } else {
        "claude_probe.spawn_failed"
    };
    let message = if is_missing_binary {
        format!("Claude CLI binary `{}` was not found", config.executable)
    } else {
        format!("failed to spawn Claude CLI: {error}")
    };
    ClaudeAccountProbeSnapshot {
        status,
        message: Some(message.clone()),
        version: None,
        account: None,
        diagnostics: vec![ClaudeProbeDiagnostic {
            level: ClaudeProbeDiagnosticLevel::Error,
            code: code.to_owned(),
            message,
        }],
        stderr: Vec::new(),
    }
}

fn default_claude_models() -> Vec<ClaudeModelSnapshot> {
    [
        (
            "default",
            "Default (recommended)",
            "Claude CLI default model",
        ),
        ("opus", "Opus", "Claude Opus"),
        (
            "opus[1m]",
            "Opus (1M context)",
            "Claude Opus with 1M context",
        ),
        ("sonnet", "Sonnet", "Claude Sonnet"),
        (
            "sonnet[1m]",
            "Sonnet (1M context)",
            "Claude Sonnet with 1M context",
        ),
        ("haiku", "Haiku", "Claude Haiku"),
    ]
    .into_iter()
    .map(|(id, name, description)| ClaudeModelSnapshot {
        id: id.to_owned(),
        name: Some(name.to_owned()),
        description: Some(description.to_owned()),
        family: Some("claude".to_owned()),
        active: (id == "default").then_some(true),
        effort_options: Vec::new(),
        input_modalities: vec!["text".to_owned(), "image".to_owned()],
        output_modalities: vec!["text".to_owned()],
        supports_reasoning: Some(true),
        supports_vision: Some(true),
        max_input_tokens: None,
        max_output_tokens: None,
    })
    .collect()
}

fn append_custom_models(models: &mut Vec<ClaudeModelSnapshot>, custom_models: &[String]) {
    for custom in custom_models {
        let id = custom.trim();
        if id.is_empty() || models.iter().any(|model| model.id == id) {
            continue;
        }
        models.push(ClaudeModelSnapshot {
            id: id.to_owned(),
            name: Some(id.to_owned()),
            description: Some("Custom Claude CLI model".to_owned()),
            family: Some("claude".to_owned()),
            active: None,
            effort_options: Vec::new(),
            input_modalities: vec!["text".to_owned(), "image".to_owned()],
            output_modalities: vec!["text".to_owned()],
            supports_reasoning: Some(true),
            supports_vision: Some(true),
            max_input_tokens: None,
            max_output_tokens: None,
        });
    }
}

fn claude_account_from_initialize_account(value: &JsonValue) -> Option<ClaudeAccountSnapshot> {
    let email = value
        .get("email")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let plan = value
        .get("subscriptionType")
        .or_else(|| value.get("subscription_type"))
        .or_else(|| value.get("apiProvider"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let authenticated = email.is_some()
        || value
            .get("apiProvider")
            .and_then(JsonValue::as_str)
            .is_some()
        || value
            .get("apiKeySource")
            .and_then(JsonValue::as_str)
            .is_some()
        || value
            .get("tokenSource")
            .and_then(JsonValue::as_str)
            .is_some();
    if !authenticated && plan.is_none() {
        return None;
    }
    Some(ClaudeAccountSnapshot {
        authenticated,
        email,
        plan,
    })
}

fn claude_model_from_initialize_model(value: &JsonValue) -> Option<ClaudeModelSnapshot> {
    let id = value
        .get("value")
        .or_else(|| value.get("id"))
        .and_then(JsonValue::as_str)?
        .trim();
    if id.is_empty() {
        return None;
    }
    let supports_effort = value
        .get("supportsEffort")
        .or_else(|| value.get("supports_effort"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    let effort_options = value
        .get("supportedEffortLevels")
        .or_else(|| value.get("supported_effort_levels"))
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(JsonValue::as_str)
                .filter_map(normalize_metadata_reasoning_effort)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            if supports_effort {
                ["low", "medium", "high", "max"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            } else {
                Vec::new()
            }
        });
    let supports_adaptive_thinking = value
        .get("supportsAdaptiveThinking")
        .or_else(|| value.get("supports_adaptive_thinking"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    Some(ClaudeModelSnapshot {
        id: id.to_owned(),
        name: value
            .get("displayName")
            .or_else(|| value.get("display_name"))
            .and_then(JsonValue::as_str)
            .map(str::to_owned),
        description: value
            .get("description")
            .and_then(JsonValue::as_str)
            .map(str::to_owned),
        family: Some("claude".to_owned()),
        active: (id == "default").then_some(true),
        effort_options,
        input_modalities: vec!["text".to_owned(), "image".to_owned()],
        output_modalities: vec!["text".to_owned()],
        supports_reasoning: Some(supports_effort || supports_adaptive_thinking),
        supports_vision: Some(true),
        max_input_tokens: None,
        max_output_tokens: None,
    })
}

fn stderr_lines(bytes: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_semver(value: &str) -> Option<String> {
    value
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '+'))
        .find(|part| {
            let mut pieces = part.split('.');
            pieces
                .next()
                .is_some_and(|major| major.chars().all(|ch| ch.is_ascii_digit()))
                && pieces
                    .next()
                    .is_some_and(|minor| minor.chars().all(|ch| ch.is_ascii_digit()))
                && pieces
                    .next()
                    .is_some_and(|patch| patch.chars().next().is_some_and(|ch| ch.is_ascii_digit()))
        })
        .map(str::to_owned)
}

fn semver_lt(left: &str, right: &str) -> bool {
    let left = semver_core(left);
    let right = semver_core(right);
    left < right
}

fn semver_core(value: &str) -> (u64, u64, u64) {
    let mut parts = value.split('.');
    let major = parts.next().and_then(parse_semver_part).unwrap_or(0);
    let minor = parts.next().and_then(parse_semver_part).unwrap_or(0);
    let patch = parts.next().and_then(parse_semver_part).unwrap_or(0);
    (major, minor, patch)
}

fn parse_semver_part(value: &str) -> Option<u64> {
    let digits = value
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse().ok()
}

pub fn claude_redacted_native_event(method: impl Into<String>, raw: JsonValue) -> JsonValue {
    serde_json::json!({
        "method": method.into(),
        "raw": redact_claude_native_payload(raw),
    })
}

pub fn redact_claude_native_payload(value: JsonValue) -> JsonValue {
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
                        (key, redact_claude_native_payload(value))
                    }
                })
                .collect(),
        ),
        JsonValue::Array(items) => JsonValue::Array(
            items
                .into_iter()
                .map(redact_claude_native_payload)
                .collect(),
        ),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn claude_model_parser_preserves_camel_case_effort_metadata() {
        let model = claude_model_from_initialize_model(&json!({
            "value": "claude-sonnet-4-6",
            "displayName": "Claude Sonnet 4.6",
            "supportsEffort": true,
            "supportedEffortLevels": ["low", "medium", "high", "max"]
        }))
        .expect("model metadata");

        assert_eq!(model.id, "claude-sonnet-4-6");
        assert_eq!(model.name.as_deref(), Some("Claude Sonnet 4.6"));
        assert_eq!(model.effort_options, vec!["low", "medium", "high", "max"]);
        assert_eq!(model.supports_reasoning, Some(true));
    }

    #[test]
    fn claude_model_parser_preserves_snake_case_effort_metadata() {
        let model = claude_model_from_initialize_model(&json!({
            "id": "claude-opus-4-8",
            "display_name": "Claude Opus 4.8",
            "supports_effort": true,
            "supported_effort_levels": ["low", "medium", "high", "extra-high", "maximum"]
        }))
        .expect("model metadata");

        assert_eq!(model.id, "claude-opus-4-8");
        assert_eq!(
            model.effort_options,
            vec!["low", "medium", "high", "xhigh", "max"]
        );
        assert_eq!(model.supports_reasoning, Some(true));
    }

    #[test]
    fn claude_model_parser_preserves_runtime_defined_effort_metadata() {
        let model = claude_model_from_initialize_model(&json!({
            "id": "claude-future",
            "supportsEffort": true,
            "supportedEffortLevels": ["low", "future-level"]
        }))
        .expect("model metadata");

        assert_eq!(model.effort_options, vec!["low", "future-level"]);
    }

    #[test]
    fn claude_model_parser_defaults_effort_levels_when_only_support_flag_exists() {
        let model = claude_model_from_initialize_model(&json!({
            "id": "claude-custom",
            "supportsEffort": true
        }))
        .expect("model metadata");

        assert_eq!(model.effort_options, vec!["low", "medium", "high", "max"]);
        assert_eq!(model.supports_reasoning, Some(true));
    }
}
