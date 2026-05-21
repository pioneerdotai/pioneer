use crate::context::{ToolErrorClass, ToolInvocation, ToolOutcome, ToolOutcomeStatus, ToolPayload};
use crate::error::ToolError;
use crate::shell_format::ExecModelPayload;
use crate::spec::ToolRetryClass;
use crate::web::{DownloadModelPayload, WebFetchModelPayload, WebSearchModelPayload};
use serde_json::Value as JsonValue;

pub trait ErrorClassifier: Send + Sync {
    fn classify_result(
        &self,
        invocation: &ToolInvocation,
        raw_output_json: &JsonValue,
        success: bool,
    ) -> ToolOutcome;
    fn classify_error(&self, invocation: &ToolInvocation, error: &ToolError) -> ToolOutcome;
}

#[derive(Default)]
pub struct DefaultErrorClassifier;

impl ErrorClassifier for DefaultErrorClassifier {
    fn classify_result(
        &self,
        invocation: &ToolInvocation,
        raw_output_json: &JsonValue,
        success: bool,
    ) -> ToolOutcome {
        if is_shell_tool(invocation.tool_name.as_str()) {
            return classify_shell_result(raw_output_json, success);
        }

        if is_web_tool(invocation.tool_name.as_str()) {
            return classify_web_result(invocation.tool_name.as_str(), raw_output_json, success);
        }

        if is_computer_use_tool(invocation.tool_name.as_str()) {
            return classify_computer_use_result(raw_output_json, success);
        }

        if invocation.tool_name == "grep_files"
            && raw_output_json
                .get("errorClass")
                .and_then(JsonValue::as_str)
                == Some("needs_narrowing")
        {
            return ToolOutcome::recoverable(
                ToolErrorClass::NeedsNarrowing,
                "grep_files was too broad. Retry only with a narrower path or glob; do not repeat the same arguments.",
                false,
                None,
            );
        }

        if is_mcp_invocation(invocation) {
            return classify_mcp_result(raw_output_json, success);
        }

        if output_is_truncated(raw_output_json) {
            return ToolOutcome::partial(
                "tool output was truncated",
                "Run the tool again with narrower scope or a larger result limit.",
            );
        }

        if success {
            ToolOutcome::ok()
        } else {
            ToolOutcome::recoverable(
                ToolErrorClass::ExecutionFailed,
                "Tool returned an unsuccessful result. Diagnose the output and retry with corrected arguments.",
                false,
                None,
            )
        }
    }

    fn classify_error(&self, invocation: &ToolInvocation, error: &ToolError) -> ToolOutcome {
        if is_shell_tool(invocation.tool_name.as_str()) {
            return classify_shell_error(error);
        }

        if is_web_tool(invocation.tool_name.as_str()) {
            return classify_web_error(error);
        }

        if is_mcp_invocation(invocation) {
            return classify_mcp_error(invocation, error);
        }

        match error {
            ToolError::InvalidArguments(_) => ToolOutcome::recoverable(
                ToolErrorClass::InvalidArguments,
                "Fix tool arguments and call the tool again.",
                false,
                None,
            ),
            ToolError::NotFound(_) => ToolOutcome::recoverable(
                ToolErrorClass::NotFound,
                "Requested resource was not found. Adjust path/name and retry.",
                false,
                None,
            ),
            ToolError::NotVisible(_) => ToolOutcome::recoverable(
                ToolErrorClass::ToolNotVisible,
                "Tool is registered but hidden in this provider round. Use a visible tool or request the needed domain before retrying.",
                false,
                None,
            ),
            ToolError::Cancelled(_) => ToolOutcome::recoverable(
                ToolErrorClass::Cancelled,
                "Tool call was cancelled. Retry if still needed.",
                false,
                None,
            ),
            ToolError::ExecutionFailed(_) => ToolOutcome::recoverable(
                ToolErrorClass::ExecutionFailed,
                "Tool execution failed. Inspect the error and retry with corrected input.",
                false,
                None,
            ),
            ToolError::Rejected(_) => ToolOutcome::fatal(
                ToolErrorClass::Unknown,
                Some("Tool call was rejected by policy.".to_owned()),
            ),
            ToolError::Internal(_) => ToolOutcome::fatal(
                ToolErrorClass::Internal,
                Some("Tool failed with an internal error.".to_owned()),
            ),
        }
    }
}

pub fn classify_tool_error(tool_name: &str, error: &ToolError) -> ToolOutcome {
    let classifier = DefaultErrorClassifier;
    let invocation = ToolInvocation {
        call_id: "error".to_owned(),
        tool_name: tool_name.to_owned(),
        source: crate::context::ToolCallSource::System,
        payload: crate::context::ToolPayload::Function {
            arguments: serde_json::json!({}),
        },
        workdir: std::path::PathBuf::from("."),
        environment: Default::default(),
        attempt_id: 1,
        idempotency_key: None,
        recovery: crate::spec::ToolRecoveryMetadata::default(),
        cancellation: tokio_util::sync::CancellationToken::new(),
    };
    classifier.classify_error(&invocation, error)
}

fn is_shell_tool(tool_name: &str) -> bool {
    matches!(tool_name, "exec_command" | "write_stdin")
}

fn is_web_tool(tool_name: &str) -> bool {
    matches!(tool_name, "web_search" | "web_fetch" | "download_url")
}

fn is_computer_use_tool(tool_name: &str) -> bool {
    tool_name == "computer_use"
}

fn is_mcp_invocation(invocation: &ToolInvocation) -> bool {
    matches!(invocation.payload, ToolPayload::Mcp { .. })
}

fn classify_mcp_result(raw_output_json: &JsonValue, success: bool) -> ToolOutcome {
    if output_is_truncated(raw_output_json) {
        return ToolOutcome::partial(
            "MCP tool output was truncated",
            "Retry the MCP tool with narrower scope or follow up with a more specific request.",
        );
    }

    if success {
        ToolOutcome::ok()
    } else {
        ToolOutcome::fatal(
            ToolErrorClass::ExecutionFailed,
            Some("MCP tool returned an application-level error result.".to_owned()),
        )
    }
}

fn classify_mcp_error(invocation: &ToolInvocation, error: &ToolError) -> ToolOutcome {
    match error {
        ToolError::InvalidArguments(_) => ToolOutcome::fatal(
            ToolErrorClass::InvalidArguments,
            Some("MCP tool arguments were invalid for the materialized schema.".to_owned()),
        ),
        ToolError::NotFound(_) => ToolOutcome::fatal(
            ToolErrorClass::NotFound,
            Some("MCP server or raw tool was not available at call time.".to_owned()),
        ),
        ToolError::NotVisible(_) => ToolOutcome::recoverable(
            ToolErrorClass::ToolNotVisible,
            "MCP tool is registered but hidden in this provider round. Use a visible tool or request the needed domain before retrying.",
            false,
            None,
        ),
        ToolError::Rejected(_) => ToolOutcome::fatal(
            ToolErrorClass::PermissionDenied,
            Some("MCP tool call was rejected before execution.".to_owned()),
        ),
        ToolError::Cancelled(_) => ToolOutcome::fatal(
            ToolErrorClass::Cancelled,
            Some("MCP tool call was cancelled.".to_owned()),
        ),
        ToolError::ExecutionFailed(_) | ToolError::Internal(_) => {
            let class = match error {
                ToolError::Internal(_) => ToolErrorClass::Internal,
                _ => ToolErrorClass::ExecutionFailed,
            };
            if invocation.recovery.retry_class != ToolRetryClass::Never
                && invocation.recovery.max_attempts > 1
            {
                ToolOutcome::recoverable(
                    class,
                    "MCP transport/session call failed. Retry if the server is still live.",
                    false,
                    None,
                )
            } else {
                ToolOutcome::fatal(
                    class,
                    Some(
                        "MCP transport/session call failed and this tool is not retryable."
                            .to_owned(),
                    ),
                )
            }
        }
    }
}

fn classify_shell_result(raw_output_json: &JsonValue, success: bool) -> ToolOutcome {
    let payload = match serde_json::from_value::<ExecModelPayload>(raw_output_json.clone()) {
        Ok(payload) => payload,
        Err(_) => {
            if success {
                return ToolOutcome::ok();
            }
            return ToolOutcome::recoverable(
                ToolErrorClass::ExecutionFailed,
                "Shell command failed. Inspect stderr and run a corrected command.",
                false,
                None,
            );
        }
    };

    if payload.timed_out {
        return ToolOutcome::recoverable(
            ToolErrorClass::Timeout,
            "Command timed out. Retry with a shorter command, polling via write_stdin, or a higher timeout.",
            true,
            Some("timeout".to_owned()),
        );
    }

    if payload.truncated.stdout || payload.truncated.stderr || payload.truncated.aggregated_output {
        return ToolOutcome::partial(
            "shell output was truncated",
            "Output is incomplete. Retry with narrower command scope or higher max_output_tokens.",
        );
    }

    let stderr_lower = payload.stderr.to_lowercase();
    let stdout_empty = payload.stdout.trim().is_empty();
    let exit_code = payload.exit_code.unwrap_or(0);
    let has_error_stderr = looks_like_shell_error(stderr_lower.as_str());

    if exit_code != 0 {
        return ToolOutcome::recoverable(
            classify_shell_error_class(stderr_lower.as_str()),
            "Command failed. Diagnose stderr and run a corrected shell command.",
            false,
            None,
        );
    }

    if has_error_stderr && stdout_empty {
        return ToolOutcome::recoverable(
            classify_shell_error_class(stderr_lower.as_str()),
            "stderr indicates command failure despite zero exit code. Run corrected command and re-check output.",
            false,
            None,
        );
    }

    if has_error_stderr {
        return ToolOutcome {
            status: ToolOutcomeStatus::PartialSuccess,
            error_class: Some(classify_shell_error_class(stderr_lower.as_str())),
            should_retry: true,
            retry_hint: Some(
                "Command produced suspicious stderr. Verify results and run follow-up shell command if needed."
                    .to_owned(),
            ),
            incomplete: true,
            incomplete_reason: Some("stderr_contains_error_pattern".to_owned()),
        };
    }

    if success {
        ToolOutcome::ok()
    } else {
        ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "Shell command returned an unsuccessful result. Retry with corrected command.",
            false,
            None,
        )
    }
}

fn classify_web_result(tool_name: &str, raw_output_json: &JsonValue, success: bool) -> ToolOutcome {
    match tool_name {
        "web_search" => {
            if let Ok(payload) =
                serde_json::from_value::<WebSearchModelPayload>(raw_output_json.clone())
            {
                if payload.truncated {
                    return ToolOutcome::partial(
                        "web_search results were truncated",
                        "Retry web_search with a narrower query or higher max_results.",
                    );
                }

                if payload.result_count == 0 {
                    return ToolOutcome::recoverable(
                        ToolErrorClass::NotFound,
                        "Search returned no results. Refine query and retry web_search.",
                        false,
                        None,
                    );
                }

                return ToolOutcome::ok();
            }
        }
        "web_fetch" => {
            if let Ok(payload) =
                serde_json::from_value::<WebFetchModelPayload>(raw_output_json.clone())
            {
                if payload.truncated.network || payload.truncated.output {
                    return ToolOutcome::partial(
                        "web_fetch output was truncated",
                        "Output may be incomplete. Retry with narrower scope or follow-up fetch calls.",
                    );
                }
                return classify_status_code(payload.status_code, payload.success);
            }
        }
        "download_url" => {
            if let Ok(payload) =
                serde_json::from_value::<DownloadModelPayload>(raw_output_json.clone())
            {
                if payload.truncated {
                    return ToolOutcome::partial(
                        "download output was truncated",
                        "Retry download with higher max_bytes or narrower source artifact.",
                    );
                }
                return classify_status_code(payload.status_code, payload.success);
            }
        }
        _ => {}
    }

    if output_is_truncated(raw_output_json) {
        return ToolOutcome::partial(
            "web tool output was truncated",
            "Output may be incomplete. Retry with narrower scope, smaller page, or follow-up tool calls.",
        );
    }

    let status_code = raw_output_json
        .get("status_code")
        .and_then(JsonValue::as_u64)
        .and_then(|value| u16::try_from(value).ok());

    if let Some(status_code) = status_code {
        return classify_status_code(status_code, success);
    }

    if success {
        ToolOutcome::ok()
    } else {
        ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "Web tool returned an unsuccessful result. Inspect output and retry with corrected input.",
            false,
            None,
        )
    }
}

fn classify_status_code(status_code: u16, success: bool) -> ToolOutcome {
    if status_code >= 500 {
        return ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "Remote server returned 5xx. Retry later or use another source.",
            false,
            None,
        );
    }
    if status_code == 429 {
        return ToolOutcome::recoverable(
            ToolErrorClass::Timeout,
            "Remote service rate-limited the request. Retry later or reduce request volume.",
            false,
            None,
        );
    }
    if status_code == 404 {
        return ToolOutcome::recoverable(
            ToolErrorClass::NotFound,
            "Resource was not found (HTTP 404). Verify URL and retry.",
            false,
            None,
        );
    }
    if status_code >= 400 {
        return ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "HTTP request failed. Inspect status code/output and retry with corrected URL or parameters.",
            false,
            None,
        );
    }

    if success {
        ToolOutcome::ok()
    } else {
        ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "Web tool returned an unsuccessful result. Inspect output and retry with corrected input.",
            false,
            None,
        )
    }
}

fn classify_computer_use_result(raw_output_json: &JsonValue, success: bool) -> ToolOutcome {
    let action = raw_output_json
        .get("action")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let status = raw_output_json
        .get("status")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let loop_state = raw_output_json
        .get("loop_state")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let failure_class = raw_output_json
        .get("failure_class")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();

    if action == "snapshot" {
        let has_attachment = raw_output_json
            .get("llm_context")
            .and_then(|value| value.get("attachment"))
            .and_then(|value| value.get("path"))
            .and_then(JsonValue::as_str)
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        if status == "running" && !has_attachment {
            return ToolOutcome::recoverable(
                ToolErrorClass::ExecutionFailed,
                "computer_use snapshot result is missing llm_context attachment path. Retry snapshot.",
                true,
                Some("attachment_transport_failure".to_owned()),
            );
        }
    }

    if status == "stopped" && loop_state == "completed" {
        return ToolOutcome::ok();
    }

    if status == "stopped" {
        if action == "stop" && (failure_class.is_empty() || loop_state == "stopped") {
            return ToolOutcome::ok();
        }
        return match failure_class {
            "provider_timeout" | "provider_rate_limit" => ToolOutcome::recoverable(
                ToolErrorClass::Timeout,
                "computer_use stopped due provider timeout/rate-limit. Start a new session and retry.",
                true,
                Some(failure_class.to_owned()),
            ),
            "attachment_transport_failure" | "expected_effect_mismatch" => {
                ToolOutcome::recoverable(
                    ToolErrorClass::ExecutionFailed,
                    "computer_use stopped due recoverable transport/perception issue. Start a new session and retry with refined instructions.",
                    true,
                    Some(failure_class.to_owned()),
                )
            }
            "loop_guard_triggered" | "recovery_budget_exceeded" => ToolOutcome::fatal(
                ToolErrorClass::ExecutionFailed,
                Some(
                    "computer_use stopped by loop/recovery guard. Adjust task plan or start a fresh session."
                        .to_owned(),
                ),
            ),
            "policy_blocked" => ToolOutcome::fatal(
                ToolErrorClass::Unknown,
                Some(
                    "computer_use action was blocked by policy. Adjust policy profile or task plan."
                        .to_owned(),
                ),
            ),
            _ => ToolOutcome::recoverable(
                ToolErrorClass::ExecutionFailed,
                "computer_use session stopped. Inspect stop_reason and decide whether to start a new session.",
                false,
                None,
            ),
        };
    }

    if success {
        ToolOutcome::ok()
    } else {
        ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "computer_use returned unsuccessful result. Diagnose output and retry.",
            false,
            None,
        )
    }
}

fn classify_shell_error(error: &ToolError) -> ToolOutcome {
    match error {
        ToolError::InvalidArguments(_) => ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "Shell tool arguments are invalid. Fix args and retry.",
            false,
            None,
        ),
        ToolError::NotFound(_) => ToolOutcome::recoverable(
            ToolErrorClass::NotFound,
            "Shell session/resource not found. Recreate session or fix identifiers.",
            false,
            None,
        ),
        ToolError::NotVisible(_) => ToolOutcome::recoverable(
            ToolErrorClass::ToolNotVisible,
            "Shell tool is registered but hidden in this provider round. Use a visible tool or request the needed domain before retrying.",
            false,
            None,
        ),
        ToolError::Cancelled(_) => ToolOutcome::recoverable(
            ToolErrorClass::Cancelled,
            "Shell execution was cancelled. Retry command if still needed.",
            false,
            None,
        ),
        ToolError::ExecutionFailed(message) => {
            let lower = message.to_lowercase();
            if lower.contains("timed out") {
                return ToolOutcome::recoverable(
                    ToolErrorClass::Timeout,
                    "Shell command timed out. Retry with adjusted timeout/scope.",
                    true,
                    Some("timeout".to_owned()),
                );
            }
            ToolOutcome::recoverable(
                classify_shell_error_class(lower.as_str()),
                "Shell command failed. Inspect error and run corrected command.",
                false,
                None,
            )
        }
        ToolError::Rejected(_) => ToolOutcome::fatal(
            ToolErrorClass::Unknown,
            Some("Shell tool call was rejected by policy.".to_owned()),
        ),
        ToolError::Internal(_) => ToolOutcome::fatal(
            ToolErrorClass::Internal,
            Some("Shell tool failed with internal error.".to_owned()),
        ),
    }
}

fn classify_web_error(error: &ToolError) -> ToolOutcome {
    match error {
        ToolError::InvalidArguments(_) => ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "Web tool arguments are invalid. Fix parameters and retry.",
            false,
            None,
        ),
        ToolError::NotFound(_) => ToolOutcome::recoverable(
            ToolErrorClass::NotFound,
            "Requested web resource/session was not found. Fix identifiers and retry.",
            false,
            None,
        ),
        ToolError::NotVisible(_) => ToolOutcome::recoverable(
            ToolErrorClass::ToolNotVisible,
            "Web tool is registered but hidden in this provider round. Use a visible tool or request the needed domain before retrying.",
            false,
            None,
        ),
        ToolError::Cancelled(_) => ToolOutcome::recoverable(
            ToolErrorClass::Cancelled,
            "Web tool execution was cancelled. Retry if still needed.",
            false,
            None,
        ),
        ToolError::ExecutionFailed(message) => {
            let lower = message.to_lowercase();
            if lower.contains("timed out") {
                return ToolOutcome::recoverable(
                    ToolErrorClass::Timeout,
                    "Web request timed out. Retry with adjusted timeout or narrower scope.",
                    true,
                    Some("timeout".to_owned()),
                );
            }
            if lower.contains("not found") {
                return ToolOutcome::recoverable(
                    ToolErrorClass::NotFound,
                    "Web resource not found. Verify URL and retry.",
                    false,
                    None,
                );
            }
            ToolOutcome::recoverable(
                ToolErrorClass::ExecutionFailed,
                "Web tool execution failed. Diagnose output and retry with corrected parameters.",
                false,
                None,
            )
        }
        ToolError::Rejected(_) => ToolOutcome::fatal(
            ToolErrorClass::Unknown,
            Some("Web tool call was rejected by policy.".to_owned()),
        ),
        ToolError::Internal(_) => ToolOutcome::fatal(
            ToolErrorClass::Internal,
            Some("Web tool failed with internal error.".to_owned()),
        ),
    }
}

fn output_is_truncated(raw_output_json: &JsonValue) -> bool {
    raw_output_json
        .get("truncated")
        .map(|value| match value {
            JsonValue::Bool(b) => *b,
            JsonValue::Object(map) => map.values().any(|field| field.as_bool().unwrap_or(false)),
            _ => false,
        })
        .unwrap_or(false)
}

fn looks_like_shell_error(stderr_lower: &str) -> bool {
    let patterns = [
        "invalid option",
        "usage:",
        "command not found",
        "permission denied",
        "operation not permitted",
        "no such file or directory",
        "broken pipe",
        "not recognized as an internal or external command",
    ];

    patterns
        .iter()
        .any(|pattern| stderr_lower.contains(pattern))
}

fn classify_shell_error_class(stderr_lower: &str) -> ToolErrorClass {
    if stderr_lower.contains("command not found")
        || stderr_lower.contains("not recognized as an internal or external command")
    {
        return ToolErrorClass::CommandNotFound;
    }
    if stderr_lower.contains("permission denied")
        || stderr_lower.contains("operation not permitted")
    {
        return ToolErrorClass::PermissionDenied;
    }
    if stderr_lower.contains("timed out") {
        return ToolErrorClass::Timeout;
    }
    ToolErrorClass::ExecutionFailed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shell_format::{ExecPayloadInput, build_exec_model_payload};

    #[test]
    fn grep_needs_narrowing_result_uses_dedicated_error_class() {
        let classifier = DefaultErrorClassifier;
        let invocation = ToolInvocation {
            call_id: "call".to_owned(),
            tool_name: "grep_files".to_owned(),
            source: crate::context::ToolCallSource::Model,
            payload: crate::context::ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: std::path::PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        };
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "ok": false,
                "errorClass": "needs_narrowing",
            }),
            false,
        );

        assert_eq!(outcome.error_class, Some(ToolErrorClass::NeedsNarrowing));
        assert!(outcome.should_retry);
    }

    #[test]
    fn shell_classifier_marks_zero_exit_with_usage_stderr_as_recoverable() {
        let payload = build_exec_model_payload(ExecPayloadInput {
            exit_code: Some(0),
            timed_out: false,
            duration_ms: 12,
            stdout: String::new(),
            stderr: "du: invalid option -- b\nusage: du ...".to_owned(),
            session_id: None,
            command: vec!["/bin/sh".to_owned(), "-c".to_owned(), "du -bc".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });

        let classifier = DefaultErrorClassifier;
        let invocation = ToolInvocation {
            call_id: "call".to_owned(),
            tool_name: "exec_command".to_owned(),
            source: crate::context::ToolCallSource::Model,
            payload: crate::context::ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: std::path::PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        };
        let outcome = classifier.classify_result(&invocation, &serde_json::json!(payload), true);
        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert!(outcome.should_retry);
    }

    #[test]
    fn computer_use_snapshot_without_attachment_is_recoverable() {
        let classifier = DefaultErrorClassifier;
        let invocation = ToolInvocation {
            call_id: "call".to_owned(),
            tool_name: "computer_use".to_owned(),
            source: crate::context::ToolCallSource::Model,
            payload: crate::context::ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: std::path::PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        };
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "action": "snapshot",
                "status": "running",
                "loop_state": "planner_request_built"
            }),
            true,
        );
        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(
            outcome.incomplete_reason.as_deref(),
            Some("attachment_transport_failure")
        );
    }

    #[test]
    fn computer_use_completed_outcome_is_ok() {
        let classifier = DefaultErrorClassifier;
        let invocation = ToolInvocation {
            call_id: "call".to_owned(),
            tool_name: "computer_use".to_owned(),
            source: crate::context::ToolCallSource::Model,
            payload: crate::context::ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: std::path::PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        };
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "action": "stop",
                "status": "stopped",
                "loop_state": "completed"
            }),
            true,
        );
        assert_eq!(outcome.status, ToolOutcomeStatus::Ok);
    }
}
