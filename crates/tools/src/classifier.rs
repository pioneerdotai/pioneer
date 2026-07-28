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

const PERMISSION_DENIED_HINT: &str = "The requested operation is not permitted in the current execution context. Do not retry the same denied operation; continue with another permitted path or tool.";

fn permission_denied_outcome() -> ToolOutcome {
    ToolOutcome::fatal(
        ToolErrorClass::PermissionDenied,
        Some(PERMISSION_DENIED_HINT.to_owned()),
    )
}

fn partial_permission_denied_outcome(reason: impl Into<String>) -> ToolOutcome {
    ToolOutcome {
        status: ToolOutcomeStatus::PartialSuccess,
        error_class: Some(ToolErrorClass::PermissionDenied),
        should_retry: false,
        retry_hint: Some(PERMISSION_DENIED_HINT.to_owned()),
        incomplete: true,
        incomplete_reason: Some(reason.into()),
    }
}

fn message_indicates_permission_denied(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("permission denied")
        || lower.contains("operation not permitted")
        || lower.contains("access denied")
        || lower.contains("access is denied")
}

fn error_is_permission_denied(error: &ToolError) -> bool {
    match error {
        ToolError::Rejected(_) => true,
        ToolError::ExecutionFailed(message) => message_indicates_permission_denied(message),
        _ => false,
    }
}

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

        if invocation.tool_name == "write_file"
            && !success
            && let Some(outcome) = classify_write_file_result(raw_output_json)
        {
            return outcome;
        }

        if invocation.tool_name == "edit_file"
            && !success
            && let Some(outcome) = classify_edit_file_result(raw_output_json)
        {
            return outcome;
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
        if error_is_permission_denied(error) {
            return permission_denied_outcome();
        }

        if is_shell_tool(invocation.tool_name.as_str()) {
            return classify_shell_error(error);
        }

        if is_computer_use_tool(invocation.tool_name.as_str()) {
            return classify_computer_use_error(error);
        }

        if is_web_tool(invocation.tool_name.as_str()) {
            return classify_web_error(error);
        }

        if invocation.tool_name == "write_file"
            && let Some(outcome) = classify_write_file_error(error)
        {
            return outcome;
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
            ToolError::Rejected(_) => permission_denied_outcome(),
            ToolError::Internal(_) => ToolOutcome::fatal(
                ToolErrorClass::Internal,
                Some("Tool failed with an internal error.".to_owned()),
            ),
        }
    }
}

fn classify_write_file_result(raw_output_json: &JsonValue) -> Option<ToolOutcome> {
    let status = raw_output_json
        .get("status")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let error_class = raw_output_json
        .get("errorClass")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();

    match (status, error_class) {
        ("read_required", _) => Some(ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "write_file needs current file state before overwrite. Call read_file for the complete file, then retry write_file with updated content.",
            false,
            None,
        )),
        ("precondition_failed", _) | (_, "precondition_failed") => Some(ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "The target changed before write_file could overwrite it. Call read_file again for the complete current file, then retry write_file with updated content.",
            false,
            None,
        )),
        _ => None,
    }
}

fn classify_edit_file_result(raw_output_json: &JsonValue) -> Option<ToolOutcome> {
    let status = raw_output_json
        .get("status")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let error_class = raw_output_json
        .get("errorClass")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();

    match (status, error_class) {
        ("read_required", _) => Some(ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "edit_file needs current file state before editing. Call read_file for the complete file, then retry edit_file with exact old_string text copied without line-number prefixes.",
            false,
            None,
        )),
        ("precondition_failed", _) | (_, "precondition_failed") => Some(ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "The target changed before edit_file could modify it. Call read_file again for the complete current file, then retry edit_file with updated exact old_string text.",
            false,
            None,
        )),
        ("not_found", _) => Some(ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "edit_file could not find old_string. Call read_file again, copy the exact current file text without line-number prefixes, and retry with a corrected old_string.",
            false,
            None,
        )),
        ("ambiguous_match", _) => Some(ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "edit_file old_string matched multiple locations. Retry with more surrounding context for a unique match, or set replace_all=true only if every exact occurrence should change.",
            false,
            None,
        )),
        ("not_utf8", _) => Some(ToolOutcome::fatal(
            ToolErrorClass::InvalidArguments,
            Some("edit_file only supports UTF-8 text files. Use an appropriate binary-safe workflow instead of retrying edit_file with the same target.".to_owned()),
        )),
        ("no_change", _) => Some(ToolOutcome::fatal(
            ToolErrorClass::InvalidArguments,
            Some("edit_file computed no content change. Do not retry the same edit_file arguments; inspect the current file and choose a different edit only if needed.".to_owned()),
        )),
        _ => None,
    }
}

fn classify_write_file_error(error: &ToolError) -> Option<ToolOutcome> {
    let ToolError::InvalidArguments(message) = error else {
        return None;
    };

    if message.contains("missing field `path`")
        || message.contains("missing field `content`")
        || message.contains("unknown field")
    {
        return Some(ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            "Call write_file with canonical JSON fields `path` and `content`. Use write_stdin only for an existing exec_command session, not for file creation.",
            false,
            None,
        ));
    }

    None
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
        permission_metadata: crate::spec::ToolPermissionMetadata::default(),
        execution_security_snapshot: None,
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
        ToolError::Rejected(_) => permission_denied_outcome(),
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

    let stderr_lower = payload.stderr.to_lowercase();
    let stdout_empty = payload.stdout.trim().is_empty();
    let exit_code = payload.exit_code.unwrap_or(0);
    let has_error_stderr = looks_like_shell_error(stderr_lower.as_str());
    let error_class = classify_shell_error_class(stderr_lower.as_str());

    if has_error_stderr && error_class == ToolErrorClass::PermissionDenied {
        if stdout_empty {
            return permission_denied_outcome();
        }
        return partial_permission_denied_outcome("stderr_contains_permission_denied");
    }

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

    if exit_code != 0 {
        return ToolOutcome::recoverable(
            error_class,
            "Command failed. Diagnose stderr and run a corrected shell command.",
            false,
            None,
        );
    }

    if has_error_stderr && stdout_empty {
        return ToolOutcome::recoverable(
            error_class,
            "stderr indicates command failure despite zero exit code. Run corrected command and re-check output.",
            false,
            None,
        );
    }

    if has_error_stderr {
        return ToolOutcome {
            status: ToolOutcomeStatus::PartialSuccess,
            error_class: Some(error_class),
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
    if matches!(status_code, 401 | 403) {
        return permission_denied_outcome();
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

    if action == "act" && computer_use_act_failed(raw_output_json) {
        if let Some(failure_class) = computer_use_result_failure_class(raw_output_json) {
            return computer_use_failure_outcome(failure_class, false);
        }
        return ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "computer_use action failed without a structured failure_class. Request a fresh snapshot and choose a corrected action.",
            true,
            Some("runtime_action_error".to_owned()),
        );
    }

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
        if let Some(failure_class) = canonical_computer_use_failure_class(failure_class) {
            return computer_use_failure_outcome(failure_class, true);
        }
        return ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "computer_use session stopped. Inspect stop_reason and decide whether to start a new session.",
            false,
            None,
        );
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

fn classify_computer_use_error(error: &ToolError) -> ToolOutcome {
    match error {
        ToolError::InvalidArguments(message) => {
            let lower = message.to_lowercase();
            if lower.contains("session_id") {
                return ToolOutcome::fatal(
                    ToolErrorClass::InvalidArguments,
                    Some(
                        "computer_use requires a valid session_id from a successful start call before snapshot/act/verify/status/stop."
                            .to_owned(),
                    ),
                );
            }
            ToolOutcome::recoverable(
                ToolErrorClass::InvalidArguments,
                "Fix computer_use arguments according to the schema and retry.",
                false,
                None,
            )
        }
        ToolError::NotFound(message) => {
            let lower = message.to_lowercase();
            if lower.contains("computer_use session") && lower.contains("not found") {
                return ToolOutcome::fatal(
                    ToolErrorClass::InvalidArguments,
                    Some(
                        "computer_use session_id is invalid or stale. Do not call snapshot/act/verify/status/stop until start returns a new successful session_id."
                            .to_owned(),
                    ),
                );
            }
            if lower.contains("app target")
                || lower.contains("target app")
                || lower.contains("app_not_found")
            {
                return ToolOutcome::fatal(
                    ToolErrorClass::NotFound,
                    Some(
                        "computer_use target app was not found. Do not retry the same target blindly; use list_apps or provide a stable bundle_id/executable_path/launch_command."
                            .to_owned(),
                    ),
                );
            }
            ToolOutcome::fatal(
                ToolErrorClass::NotFound,
                Some(
                    "computer_use resource was not found. Correct the target before retrying."
                        .to_owned(),
                ),
            )
        }
        ToolError::ExecutionFailed(message) => {
            let lower = message.to_lowercase();
            if lower.contains("failed to activate computer_use target")
                || lower.contains("failed to launch computer_use target")
            {
                return ToolOutcome::fatal(
                    ToolErrorClass::ExecutionFailed,
                    Some(
                        "computer_use could not launch or activate the requested app target. Do not retry the same target blindly; resolve a stable app identity first."
                            .to_owned(),
                    ),
                );
            }
            ToolOutcome::recoverable(
                ToolErrorClass::ExecutionFailed,
                "computer_use execution failed. Inspect the structured error and retry with corrected state.",
                false,
                None,
            )
        }
        ToolError::NotVisible(_) => ToolOutcome::recoverable(
            ToolErrorClass::ToolNotVisible,
            "computer_use is registered but hidden in this provider round. Request the computer_use domain before retrying.",
            false,
            None,
        ),
        ToolError::Cancelled(_) => ToolOutcome::recoverable(
            ToolErrorClass::Cancelled,
            "computer_use call was cancelled. Retry only if the desktop task is still needed.",
            false,
            None,
        ),
        ToolError::Rejected(_) => permission_denied_outcome(),
        ToolError::Internal(_) => ToolOutcome::fatal(
            ToolErrorClass::Internal,
            Some("computer_use failed with an internal error.".to_owned()),
        ),
    }
}

fn computer_use_act_failed(raw_output_json: &JsonValue) -> bool {
    raw_output_json
        .pointer("/result/status")
        .and_then(JsonValue::as_str)
        == Some("failed")
        || raw_output_json
            .pointer("/result/execution/status")
            .and_then(JsonValue::as_str)
            == Some("failed")
}

fn computer_use_result_failure_class(raw_output_json: &JsonValue) -> Option<&'static str> {
    raw_output_json
        .pointer("/result/execution/failure_class")
        .and_then(JsonValue::as_str)
        .or_else(|| {
            raw_output_json
                .pointer("/result/failure_class")
                .and_then(JsonValue::as_str)
        })
        .or_else(|| {
            raw_output_json
                .get("failure_class")
                .and_then(JsonValue::as_str)
        })
        .and_then(canonical_computer_use_failure_class)
}

fn canonical_computer_use_failure_class(value: &str) -> Option<&'static str> {
    match normalize_computer_use_failure_class(value).as_str() {
        "permissiondenied" => Some("permission_denied"),
        "accessibilityunavailable" => Some("accessibility_unavailable"),
        "accessibilitynotenabled" => Some("accessibility_not_enabled"),
        "appnotfound" => Some("app_not_found"),
        "elementnotfound" => Some("element_not_found"),
        "elementstale" => Some("element_stale"),
        "actionnotsupported" => Some("action_not_supported"),
        "inputsimulationunavailable" => Some("input_simulation_unavailable"),
        "screenshotunavailable" => Some("screenshot_unavailable"),
        "attachmenttransportfailure" => Some("attachment_transport_failure"),
        "providertimeout" => Some("provider_timeout"),
        "providerratelimit" => Some("provider_rate_limit"),
        "loopguardtriggered" => Some("loop_guard_triggered"),
        "recoverybudgetexceeded" => Some("recovery_budget_exceeded"),
        "runtimeactionerror" => Some("runtime_action_error"),
        _ => None,
    }
}

fn normalize_computer_use_failure_class(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .flat_map(char::to_lowercase)
        .collect()
}

fn computer_use_failure_outcome(failure_class: &'static str, stopped: bool) -> ToolOutcome {
    match failure_class {
        "provider_timeout" | "provider_rate_limit" => ToolOutcome::recoverable(
            ToolErrorClass::Timeout,
            if stopped {
                "computer_use stopped due provider timeout/rate-limit. Start a new session and retry."
            } else {
                "computer_use hit a provider timeout/rate-limit. Retry after a fresh snapshot."
            },
            true,
            Some(failure_class.to_owned()),
        ),
        "attachment_transport_failure" | "screenshot_unavailable" => ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "computer_use hit a recoverable screenshot/attachment transport issue. Retry snapshot before acting again.",
            true,
            Some(failure_class.to_owned()),
        ),
        "element_not_found" | "element_stale" => ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "computer_use target is missing or stale. Request a new snapshot, re-resolve the target, and do not retry the stale node_id blindly.",
            true,
            Some(failure_class.to_owned()),
        ),
        "permission_denied" | "accessibility_not_enabled" => permission_denied_outcome(),
        "accessibility_unavailable" => ToolOutcome::fatal(
            ToolErrorClass::ExecutionFailed,
            Some("computer_use accessibility backend is unavailable on this host.".to_owned()),
        ),
        "app_not_found" => ToolOutcome::fatal(
            ToolErrorClass::NotFound,
            Some(
                "computer_use target app was not found. Retry only after providing launch_if_missing/launch_command or starting the app."
                    .to_owned(),
            ),
        ),
        "action_not_supported" => ToolOutcome::fatal(
            ToolErrorClass::ExecutionFailed,
            Some(
                "computer_use action is unsupported for the target. Do not retry the same action."
                    .to_owned(),
            ),
        ),
        "input_simulation_unavailable" => ToolOutcome::fatal(
            ToolErrorClass::ExecutionFailed,
            Some(
                "computer_use input simulation is unavailable. Use semantic accessibility actions instead of input_*."
                    .to_owned(),
            ),
        ),
        "loop_guard_triggered" | "recovery_budget_exceeded" => ToolOutcome::fatal(
            ToolErrorClass::ExecutionFailed,
            Some(
                "computer_use stopped by loop/recovery guard. Adjust task plan or start a fresh session."
                    .to_owned(),
            ),
        ),
        "runtime_action_error" => ToolOutcome::fatal(
            ToolErrorClass::ExecutionFailed,
            Some(
                "computer_use action failed at runtime. Inspect execution details before starting a new plan."
                    .to_owned(),
            ),
        ),
        _ => ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "computer_use failed. Inspect failure_class and retry with corrected state.",
            false,
            Some(failure_class.to_owned()),
        ),
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
        ToolError::Rejected(_) => permission_denied_outcome(),
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
        ToolError::Rejected(_) => permission_denied_outcome(),
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
        || stderr_lower.contains("access denied")
        || stderr_lower.contains("access is denied")
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

    fn invocation_for(tool_name: &str) -> ToolInvocation {
        ToolInvocation {
            call_id: "call".to_owned(),
            tool_name: tool_name.to_owned(),
            source: crate::context::ToolCallSource::Model,
            payload: crate::context::ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: std::path::PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn computer_use_invocation() -> ToolInvocation {
        ToolInvocation {
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
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn write_file_invocation() -> ToolInvocation {
        ToolInvocation {
            call_id: "call".to_owned(),
            tool_name: "write_file".to_owned(),
            source: crate::context::ToolCallSource::Model,
            payload: crate::context::ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: std::path::PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn edit_file_invocation() -> ToolInvocation {
        ToolInvocation {
            call_id: "call".to_owned(),
            tool_name: "edit_file".to_owned(),
            source: crate::context::ToolCallSource::Model,
            payload: crate::context::ToolPayload::Function {
                arguments: serde_json::json!({}),
            },
            workdir: std::path::PathBuf::from("."),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

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
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
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
    fn write_file_read_required_result_points_to_complete_read_then_retry() {
        let classifier = DefaultErrorClassifier;
        let invocation = write_file_invocation();
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "ok": false,
                "status": "read_required",
                "errorClass": "invalid_arguments"
            }),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
        assert!(outcome.should_retry);
        let hint = outcome.retry_hint.expect("retry hint should exist");
        assert!(hint.contains("read_file for the complete file"));
        assert!(hint.contains("retry write_file"));
    }

    #[test]
    fn write_file_precondition_failed_result_points_to_fresh_read() {
        let classifier = DefaultErrorClassifier;
        let invocation = write_file_invocation();
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "ok": false,
                "status": "precondition_failed",
                "errorClass": "precondition_failed"
            }),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::ExecutionFailed));
        assert!(outcome.should_retry);
        let hint = outcome.retry_hint.expect("retry hint should exist");
        assert!(hint.contains("Call read_file again"));
        assert!(hint.contains("retry write_file"));
    }

    #[test]
    fn write_file_malformed_arguments_get_write_specific_hint() {
        let classifier = DefaultErrorClassifier;
        let invocation = write_file_invocation();
        let outcome = classifier.classify_error(
            &invocation,
            &ToolError::invalid_arguments("invalid arguments: missing field `content`"),
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
        let hint = outcome.retry_hint.expect("retry hint should exist");
        assert!(hint.contains("`path` and `content`"));
        assert!(hint.contains("write_stdin only"));
    }

    #[test]
    fn edit_file_read_required_result_points_to_complete_read_then_retry() {
        let classifier = DefaultErrorClassifier;
        let invocation = edit_file_invocation();
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "ok": false,
                "status": "read_required",
                "errorClass": "invalid_arguments"
            }),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
        assert!(outcome.should_retry);
        let hint = outcome.retry_hint.expect("retry hint should exist");
        assert!(hint.contains("read_file for the complete file"));
        assert!(hint.contains("retry edit_file"));
        assert!(hint.contains("without line-number prefixes"));
    }

    #[test]
    fn edit_file_precondition_failed_result_points_to_fresh_read() {
        let classifier = DefaultErrorClassifier;
        let invocation = edit_file_invocation();
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "ok": false,
                "status": "precondition_failed",
                "errorClass": "precondition_failed"
            }),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::ExecutionFailed));
        assert!(outcome.should_retry);
        let hint = outcome.retry_hint.expect("retry hint should exist");
        assert!(hint.contains("Call read_file again"));
        assert!(hint.contains("retry edit_file"));
    }

    #[test]
    fn edit_file_not_found_result_points_to_exact_current_text() {
        let classifier = DefaultErrorClassifier;
        let invocation = edit_file_invocation();
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "ok": false,
                "status": "not_found",
                "errorClass": "invalid_arguments"
            }),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
        assert!(outcome.should_retry);
        let hint = outcome.retry_hint.expect("retry hint should exist");
        assert!(hint.contains("could not find old_string"));
        assert!(hint.contains("copy the exact current file text"));
        assert!(hint.contains("without line-number prefixes"));
    }

    #[test]
    fn edit_file_ambiguous_match_result_points_to_unique_context_or_replace_all() {
        let classifier = DefaultErrorClassifier;
        let invocation = edit_file_invocation();
        let outcome = classifier.classify_result(
            &invocation,
            &serde_json::json!({
                "ok": false,
                "status": "ambiguous_match",
                "errorClass": "invalid_arguments",
                "matches": 2
            }),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
        assert!(outcome.should_retry);
        let hint = outcome.retry_hint.expect("retry hint should exist");
        assert!(hint.contains("more surrounding context"));
        assert!(hint.contains("replace_all=true"));
    }

    #[test]
    fn edit_file_terminal_statuses_do_not_retry_same_arguments() {
        let classifier = DefaultErrorClassifier;
        let invocation = edit_file_invocation();

        for (status, expected_hint) in [
            ("not_utf8", "only supports UTF-8"),
            ("no_change", "Do not retry the same edit_file arguments"),
        ] {
            let outcome = classifier.classify_result(
                &invocation,
                &serde_json::json!({
                    "ok": false,
                    "status": status,
                    "errorClass": "invalid_arguments"
                }),
                false,
            );

            assert_eq!(outcome.status, ToolOutcomeStatus::FatalError);
            assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
            assert!(!outcome.should_retry);
            let hint = outcome.retry_hint.expect("retry hint should exist");
            assert!(hint.contains(expected_hint));
        }
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
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        };
        let outcome = classifier.classify_result(&invocation, &serde_json::json!(payload), true);
        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert!(outcome.should_retry);
    }

    #[test]
    fn rejected_tool_error_is_a_local_non_retryable_permission_denial() {
        let outcome = DefaultErrorClassifier.classify_error(
            &invocation_for("read_file"),
            &ToolError::Rejected("path is outside allowed roots".to_owned()),
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::FatalError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::PermissionDenied));
        assert!(!outcome.should_retry);
    }

    #[test]
    fn wrapped_filesystem_permission_error_is_not_retryable() {
        let outcome = DefaultErrorClassifier.classify_error(
            &invocation_for("read_file"),
            &ToolError::ExecutionFailed(
                "failed to read `/restricted`: Permission denied (os error 13)".to_owned(),
            ),
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::FatalError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::PermissionDenied));
        assert!(!outcome.should_retry);
    }

    #[test]
    fn shell_permission_denial_is_not_retryable() {
        let payload = build_exec_model_payload(ExecPayloadInput {
            exit_code: Some(1),
            timed_out: false,
            duration_ms: 12,
            stdout: String::new(),
            stderr: "cat: /restricted: Permission denied".to_owned(),
            session_id: None,
            command: vec!["cat".to_owned(), "/restricted".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });

        let outcome = DefaultErrorClassifier.classify_result(
            &invocation_for("exec_command"),
            &serde_json::json!(payload),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::FatalError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::PermissionDenied));
        assert!(!outcome.should_retry);
    }

    #[test]
    fn shell_partial_output_with_permission_denial_keeps_output_without_retrying() {
        let payload = build_exec_model_payload(ExecPayloadInput {
            exit_code: Some(1),
            timed_out: false,
            duration_ms: 12,
            stdout: "/allowed/result\n".to_owned(),
            stderr: "find: /restricted: Operation not permitted".to_owned(),
            session_id: None,
            command: vec!["find".to_owned(), "/".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });

        let outcome = DefaultErrorClassifier.classify_result(
            &invocation_for("exec_command"),
            &serde_json::json!(payload),
            true,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::PartialSuccess);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::PermissionDenied));
        assert!(!outcome.should_retry);
        assert!(outcome.incomplete);
    }

    #[test]
    fn web_forbidden_response_is_not_retryable() {
        let outcome = classify_status_code(403, false);

        assert_eq!(outcome.status, ToolOutcomeStatus::FatalError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::PermissionDenied));
        assert!(!outcome.should_retry);
    }

    #[test]
    fn computer_use_snapshot_without_attachment_is_recoverable() {
        let classifier = DefaultErrorClassifier;
        let invocation = computer_use_invocation();
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
        let invocation = computer_use_invocation();
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

    #[test]
    fn computer_use_stale_session_error_is_terminal() {
        let classifier = DefaultErrorClassifier;
        let invocation = computer_use_invocation();
        let outcome = classifier.classify_error(
            &invocation,
            &ToolError::NotFound("computer_use session 2 not found".to_owned()),
        );
        assert_eq!(outcome.status, ToolOutcomeStatus::FatalError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
        assert!(!outcome.should_retry);
    }

    #[test]
    fn computer_use_app_target_not_found_error_is_terminal() {
        let classifier = DefaultErrorClassifier;
        let invocation = computer_use_invocation();
        let outcome = classifier.classify_error(
            &invocation,
            &ToolError::NotFound(
                "computer_use app target not found; failure_class=app_not_found".to_owned(),
            ),
        );
        assert_eq!(outcome.status, ToolOutcomeStatus::FatalError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::NotFound));
        assert!(!outcome.should_retry);
    }

    #[test]
    fn computer_use_classifier_covers_every_failure_class_for_act_failures() {
        let classifier = DefaultErrorClassifier;
        let invocation = computer_use_invocation();
        let expected = [
            ("permission_denied", ToolOutcomeStatus::FatalError),
            ("accessibility_unavailable", ToolOutcomeStatus::FatalError),
            ("accessibility_not_enabled", ToolOutcomeStatus::FatalError),
            ("app_not_found", ToolOutcomeStatus::FatalError),
            ("element_not_found", ToolOutcomeStatus::RecoverableError),
            ("element_stale", ToolOutcomeStatus::RecoverableError),
            ("action_not_supported", ToolOutcomeStatus::FatalError),
            (
                "input_simulation_unavailable",
                ToolOutcomeStatus::FatalError,
            ),
            (
                "screenshot_unavailable",
                ToolOutcomeStatus::RecoverableError,
            ),
            (
                "attachment_transport_failure",
                ToolOutcomeStatus::RecoverableError,
            ),
            ("provider_timeout", ToolOutcomeStatus::RecoverableError),
            ("provider_rate_limit", ToolOutcomeStatus::RecoverableError),
            ("loop_guard_triggered", ToolOutcomeStatus::FatalError),
            ("recovery_budget_exceeded", ToolOutcomeStatus::FatalError),
            ("runtime_action_error", ToolOutcomeStatus::FatalError),
        ];

        for (failure_class, expected_status) in expected {
            let outcome = classifier.classify_result(
                &invocation,
                &serde_json::json!({
                    "action": "act",
                    "status": "running",
                    "loop_state": "post_action_result_reported",
                    "result": {
                        "status": "failed",
                        "execution": {
                            "status": "failed",
                            "failure_class": failure_class
                        }
                    }
                }),
                true,
            );
            assert_eq!(
                outcome.status, expected_status,
                "unexpected outcome for {failure_class}"
            );
            assert_eq!(outcome.incomplete_reason.as_deref(), {
                match expected_status {
                    ToolOutcomeStatus::RecoverableError => Some(failure_class),
                    _ => None,
                }
            });
        }
    }

    #[test]
    fn computer_use_classifier_covers_every_failure_class_for_stopped_sessions() {
        let classifier = DefaultErrorClassifier;
        let invocation = computer_use_invocation();
        let expected = [
            ("permission_denied", ToolOutcomeStatus::FatalError),
            ("accessibility_unavailable", ToolOutcomeStatus::FatalError),
            ("accessibility_not_enabled", ToolOutcomeStatus::FatalError),
            ("app_not_found", ToolOutcomeStatus::FatalError),
            ("element_not_found", ToolOutcomeStatus::RecoverableError),
            ("element_stale", ToolOutcomeStatus::RecoverableError),
            ("action_not_supported", ToolOutcomeStatus::FatalError),
            (
                "input_simulation_unavailable",
                ToolOutcomeStatus::FatalError,
            ),
            (
                "screenshot_unavailable",
                ToolOutcomeStatus::RecoverableError,
            ),
            (
                "attachment_transport_failure",
                ToolOutcomeStatus::RecoverableError,
            ),
            ("provider_timeout", ToolOutcomeStatus::RecoverableError),
            ("provider_rate_limit", ToolOutcomeStatus::RecoverableError),
            ("loop_guard_triggered", ToolOutcomeStatus::FatalError),
            ("recovery_budget_exceeded", ToolOutcomeStatus::FatalError),
            ("runtime_action_error", ToolOutcomeStatus::FatalError),
        ];

        for (failure_class, expected_status) in expected {
            let outcome = classifier.classify_result(
                &invocation,
                &serde_json::json!({
                    "action": "act",
                    "status": "stopped",
                    "loop_state": "failed",
                    "failure_class": failure_class
                }),
                true,
            );
            assert_eq!(
                outcome.status, expected_status,
                "unexpected outcome for {failure_class}"
            );
        }
    }
}
