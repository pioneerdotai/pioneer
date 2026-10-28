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

        if is_computer_use_tool(invocation.tool_name.as_str())
            || invocation
                .permission_metadata
                .dynamic_skill
                .as_ref()
                .is_some_and(|metadata| {
                    metadata.kind == crate::spec::DynamicSkillPermissionKind::FunctionProxy
                        && metadata
                            .target_tool
                            .as_deref()
                            .is_some_and(crate::is_computer_use_tool)
                })
        {
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

        if invocation.tool_name == "apply_patch" {
            return classify_apply_patch_result(raw_output_json, success);
        }

        // A bounded directory page is a successful discovery result. The
        // truncated/has_more fields tell the model to narrow or continue;
        // they must not turn a valid list_dir call into a failed tool attempt.
        if invocation.tool_name == "list_dir" && success {
            return ToolOutcome::ok();
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
        if is_native_file_tool(invocation.tool_name.as_str()) {
            return classify_native_file_tool_error(invocation.tool_name.as_str(), error);
        }

        if error_is_permission_denied(error) {
            return permission_denied_outcome();
        }

        if is_shell_tool(invocation.tool_name.as_str()) {
            return classify_shell_error(error);
        }

        if is_computer_use_tool(invocation.tool_name.as_str())
            || crate::runtime::is_active_computer_use_dispatch(&invocation.tool_name)
            || invocation
                .permission_metadata
                .dynamic_skill
                .as_ref()
                .is_some_and(|metadata| {
                    metadata.kind == crate::spec::DynamicSkillPermissionKind::FunctionProxy
                        && metadata
                            .target_tool
                            .as_deref()
                            .is_some_and(crate::is_computer_use_tool)
                })
        {
            return classify_computer_use_error(error);
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
            ToolError::Rejected(_) => permission_denied_outcome(),
            ToolError::Internal(_) => ToolOutcome::fatal(
                ToolErrorClass::Internal,
                Some("Tool failed with an internal error.".to_owned()),
            ),
        }
    }
}

fn is_native_file_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "read_file" | "list_dir" | "grep_files" | "apply_patch"
    )
}

fn classify_native_file_tool_error(tool_name: &str, error: &ToolError) -> ToolOutcome {
    if let ToolError::ExecutionFailed(message) = error
        && message_indicates_permission_denied(message)
    {
        return ToolOutcome::fatal(
            ToolErrorClass::PermissionDenied,
            Some(format!(
                "{tool_name} was denied by the filesystem: {message} Do not retry the same denied path; use an authorized path or report the blocker."
            )),
        );
    }

    match error {
        ToolError::InvalidArguments(message) => ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            format!(
                "{tool_name} rejected the input: {message} Follow that diagnostic and submit corrected arguments; do not repeat the same call unchanged."
            ),
            false,
            None,
        ),
        ToolError::NotFound(message) => ToolOutcome::recoverable(
            ToolErrorClass::NotFound,
            format!(
                "{tool_name} could not find the requested path: {message} Use list_dir on the nearest existing parent and retry with an exact returned path."
            ),
            false,
            None,
        ),
        ToolError::Rejected(message) => ToolOutcome::fatal(
            ToolErrorClass::PermissionDenied,
            Some(format!(
                "{tool_name} was rejected: {message} Use only the current working directory and authorized roots reported by the tool; do not repeat the denied call unchanged."
            )),
        ),
        ToolError::ExecutionFailed(message) => ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            format!(
                "{tool_name} failed: {message} Fix the reported filesystem condition or path, then submit a corrected call."
            ),
            false,
            None,
        ),
        ToolError::Cancelled(message) => ToolOutcome::recoverable(
            ToolErrorClass::Cancelled,
            format!(
                "{tool_name} was cancelled: {message} Retry only if the operation is still needed."
            ),
            false,
            None,
        ),
        ToolError::NotVisible(message) => ToolOutcome::recoverable(
            ToolErrorClass::ToolNotVisible,
            format!("{tool_name} is not visible: {message} Use a currently visible tool."),
            false,
            None,
        ),
        ToolError::Internal(message) => ToolOutcome::fatal(
            ToolErrorClass::Internal,
            Some(format!(
                "{tool_name} failed internally: {message} Do not retry the same call; report the blocker."
            )),
        ),
    }
}

fn classify_apply_patch_result(raw_output_json: &JsonValue, success: bool) -> ToolOutcome {
    if success {
        return ToolOutcome::ok();
    }

    let status = raw_output_json
        .get("status")
        .and_then(JsonValue::as_str)
        .unwrap_or("failed");
    let error = raw_output_json.get("error");
    let code = error
        .and_then(|error| error.get("code"))
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown");
    let next_action = error
        .and_then(|error| error.get("next_action"))
        .and_then(JsonValue::as_str)
        .filter(|action| !action.trim().is_empty())
        .unwrap_or(
            "Inspect the reported failure, read the affected path if necessary, and submit a corrected new patch. Do not repeat the same patch unchanged.",
        )
        .to_owned();

    if let Some(validation) = raw_output_json.get("validation") {
        let syntax_complete = validation
            .get("syntax_complete")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let reason = (!syntax_complete).then(|| {
            let from_line = validation
                .get("unchecked_from_line")
                .and_then(JsonValue::as_u64)
                .map(|line| format!("scan stopped at patch line {line}"));
            let skipped = validation
                .get("unverified_ranges")
                .and_then(JsonValue::as_array)
                .and_then(|ranges| ranges.first())
                .and_then(|range| range.get("start"))
                .and_then(JsonValue::as_u64)
                .map(|line| {
                    format!("operation body starting at patch line {line} was not checked")
                });
            let detail = from_line
                .or(skipped)
                .unwrap_or_else(|| "some patch lines were not checked".to_owned());
            format!(
                "apply_patch syntax validation incomplete: {detail}; later stages were not checked"
            )
        });
        return ToolOutcome::recoverable(
            ToolErrorClass::InvalidArguments,
            next_action,
            !syntax_complete,
            reason,
        );
    }

    if matches!(status, "partial" | "commit_state_uncertain") {
        return ToolOutcome {
            status: ToolOutcomeStatus::PartialSuccess,
            error_class: Some(ToolErrorClass::ExecutionFailed),
            should_retry: false,
            retry_hint: Some(next_action),
            incomplete: true,
            incomplete_reason: Some(format!("apply_patch finished with status `{status}`")),
        };
    }

    if code == "permission_denied" {
        return ToolOutcome::fatal(ToolErrorClass::PermissionDenied, Some(next_action));
    }

    let class = if matches!(
        code,
        "patch_syntax_error"
            | "patch_empty"
            | "invalid_payload"
            | "invalid_request"
            | "invalid_path"
            | "path_outside_allowed_root"
            | "unauthorized_path"
            | "context_not_found"
            | "ambiguous_context"
            | "source_missing"
            | "destination_exists"
            | "destination_missing"
            | "stale_file"
    ) {
        ToolErrorClass::InvalidArguments
    } else {
        ToolErrorClass::ExecutionFailed
    };
    ToolOutcome::recoverable(class, next_action, false, None)
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
        apply_patch_preflight: None,
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
    crate::is_computer_use_tool(tool_name)
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
            "Command reached its hard timeout and Pioneer terminated the process. Do not repeat the same command with only a larger timeout; materially change the strategy by decomposing the work, using a session with write_stdin polling, changing execution mode, or using another executable/tool.",
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

fn classify_computer_use_result(raw: &JsonValue, success: bool) -> ToolOutcome {
    let structured = raw.get("structuredContent").unwrap_or(raw);
    if !success
        || raw.get("isError").and_then(JsonValue::as_bool) == Some(true)
        || structured.get("status").and_then(JsonValue::as_str) == Some("refused")
    {
        return ToolOutcome::fatal(ToolErrorClass::ExecutionFailed, Some("CUA refused or failed. Observe fresh state before choosing a new action; never replay an unknown mutation.".to_owned()));
    }
    let effect = structured
        .pointer("/action/effect")
        .or_else(|| structured.get("effect"))
        .and_then(JsonValue::as_str);
    if effect == Some("refused") {
        return ToolOutcome::fatal(
            ToolErrorClass::ExecutionFailed,
            Some(
                "CUA refused the action. Choose a permitted action after fresh observation."
                    .to_owned(),
            ),
        );
    }
    if matches!(
        effect,
        Some("unknown" | "partial" | "unverifiable" | "suspected_noop")
    ) {
        return ToolOutcome { status: ToolOutcomeStatus::PartialSuccess, error_class: Some(ToolErrorClass::Unknown), should_retry: false, retry_hint: Some("Input delivery does not prove completion. Obtain a fresh observation before a new action.".to_owned()), incomplete: true, incomplete_reason: Some("action effect is not confirmed".to_owned()) };
    }
    ToolOutcome::ok()
}
fn classify_computer_use_error(error: &ToolError) -> ToolOutcome {
    let class = match error {
        ToolError::InvalidArguments(_) => ToolErrorClass::InvalidArguments,
        ToolError::Rejected(_) => ToolErrorClass::PermissionDenied,
        ToolError::Cancelled(_) => ToolErrorClass::Cancelled,
        _ => ToolErrorClass::Unknown,
    };
    ToolOutcome::fatal(class, Some("CUA execution was not confirmed. Never replay the operation; obtain a fresh observation before choosing a new action.".to_owned()))
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
                    "Shell command reached its hard timeout and the process was terminated. Do not retry the same command with only a larger timeout; materially change the execution strategy.",
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
            apply_patch_preflight: None,
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
            apply_patch_preflight: None,
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
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            apply_patch_preflight: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        };
        let outcome = classifier.classify_result(&invocation, &serde_json::json!(payload), true);
        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert!(outcome.should_retry);
    }

    #[test]
    fn shell_timeout_requires_a_material_strategy_change() {
        let payload = build_exec_model_payload(ExecPayloadInput {
            exit_code: None,
            timed_out: true,
            duration_ms: 30_000,
            stdout: String::new(),
            stderr: String::new(),
            session_id: None,
            command: vec!["slow-command".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });

        let outcome = DefaultErrorClassifier.classify_result(
            &invocation_for("exec_command"),
            &serde_json::json!(payload),
            false,
        );

        assert_eq!(outcome.error_class, Some(ToolErrorClass::Timeout));
        assert!(outcome.should_retry);
        let hint = outcome.retry_hint.expect("timeout hint should exist");
        assert!(hint.contains("hard timeout"));
        assert!(hint.contains("terminated the process"));
        assert!(hint.contains("only a larger timeout"));
        assert!(hint.contains("materially change the strategy"));
    }

    #[test]
    fn shell_timeout_error_does_not_recommend_only_a_larger_timeout() {
        let outcome = DefaultErrorClassifier.classify_error(
            &invocation_for("exec_command"),
            &ToolError::ExecutionFailed("command timed out".to_owned()),
        );

        assert_eq!(outcome.error_class, Some(ToolErrorClass::Timeout));
        let hint = outcome.retry_hint.expect("timeout hint should exist");
        assert!(hint.contains("process was terminated"));
        assert!(hint.contains("only a larger timeout"));
        assert!(hint.contains("materially change the execution strategy"));
    }

    #[test]
    fn bounded_list_dir_page_is_success_even_when_more_entries_exist() {
        let outcome = DefaultErrorClassifier.classify_result(
            &invocation_for("list_dir"),
            &serde_json::json!({
                "root": "/workspace",
                "truncated": true,
                "has_more": true,
                "entries": []
            }),
            true,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::Ok);
        assert!(!outcome.should_retry);
        assert!(!outcome.incomplete);
    }

    #[test]
    fn apply_patch_retry_uses_the_concrete_next_action() {
        let outcome = DefaultErrorClassifier.classify_result(
            &invocation_for("apply_patch"),
            &serde_json::json!({
                "status": "rejected",
                "success": false,
                "error": {
                    "code": "context_not_found",
                    "next_action": "Read `/workspace/file.txt` and build a new hunk from its current contents.",
                    "retry_same_patch": false
                }
            }),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::RecoverableError);
        assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
        assert_eq!(
            outcome.retry_hint.as_deref(),
            Some("Read `/workspace/file.txt` and build a new hunk from its current contents.")
        );
        assert!(outcome.should_retry);
    }

    #[test]
    fn apply_patch_validation_completeness_is_distinct_from_display_truncation() {
        let complete = DefaultErrorClassifier.classify_result(
            &invocation_for("apply_patch"),
            &serde_json::json!({
                "status": "rejected", "error": {"code": "patch_syntax_error", "next_action": "Fix lines 3-4."},
                "validation": {"stage": "syntax", "syntax_complete": true, "display_truncated": true}
            }), false,
        );
        assert_eq!(complete.status, ToolOutcomeStatus::RecoverableError);
        assert!(!complete.incomplete);
        assert_eq!(complete.retry_hint.as_deref(), Some("Fix lines 3-4."));

        let stopped = DefaultErrorClassifier.classify_result(
            &invocation_for("apply_patch"),
            &serde_json::json!({
                "status": "rejected", "error": {"code": "patch_syntax_error", "next_action": "Fix the boundary."},
                "validation": {"stage": "syntax", "syntax_complete": false, "unchecked_from_line": 4}
            }), false,
        );
        assert!(stopped.incomplete);
        assert!(stopped.incomplete_reason.unwrap().contains("line 4"));

        let skipped = DefaultErrorClassifier.classify_result(
            &invocation_for("apply_patch"),
            &serde_json::json!({
                "status": "rejected", "error": {"code": "invalid_path", "next_action": "Fix the path."},
                "validation": {"stage": "syntax", "syntax_complete": false,
                    "unverified_ranges": [{"start": 3, "end": 3}]}
            }), false,
        );
        assert!(skipped.incomplete);
        assert!(skipped.incomplete_reason.unwrap().contains("line 3"));
    }

    #[test]
    fn partial_apply_patch_never_schedules_an_automatic_retry() {
        let outcome = DefaultErrorClassifier.classify_result(
            &invocation_for("apply_patch"),
            &serde_json::json!({
                "status": "partial",
                "success": false,
                "changed_files": ["/workspace/committed.txt"],
                "error": {
                    "code": "partial_commit",
                    "next_action": "Inspect changed_files before making only the remaining changes.",
                    "retry_same_patch": false
                }
            }),
            false,
        );

        assert_eq!(outcome.status, ToolOutcomeStatus::PartialSuccess);
        assert!(!outcome.should_retry);
        assert!(outcome.incomplete);
        assert_eq!(
            outcome.retry_hint.as_deref(),
            Some("Inspect changed_files before making only the remaining changes.")
        );
    }

    #[test]
    fn native_file_tool_errors_preserve_the_actionable_diagnostic() {
        let outcome = DefaultErrorClassifier.classify_error(
            &invocation_for("read_file"),
            &ToolError::InvalidArguments(
                "input `Downloads/file.txt` resolved to `/Downloads/file.txt`; cwd is `/Users/alexander`"
                    .to_owned(),
            ),
        );

        assert_eq!(outcome.error_class, Some(ToolErrorClass::InvalidArguments));
        assert!(outcome.should_retry);
        let hint = outcome.retry_hint.unwrap();
        assert!(hint.contains("Downloads/file.txt"));
        assert!(hint.contains("/Downloads/file.txt"));
        assert!(hint.contains("/Users/alexander"));
        assert!(hint.contains("do not repeat"));
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
    fn context_sensitive_tool_errors_are_fatal_and_non_retryable() {
        for (case, invocation, error, expected_class) in [
            (
                "rejected_tool_error_is_a_local_non_retryable_permission_denial",
                invocation_for("read_file"),
                ToolError::Rejected("path is outside allowed roots".to_owned()),
                ToolErrorClass::PermissionDenied,
            ),
            (
                "wrapped_filesystem_permission_error_is_not_retryable",
                invocation_for("read_file"),
                ToolError::ExecutionFailed(
                    "failed to read `/restricted`: Permission denied (os error 13)".to_owned(),
                ),
                ToolErrorClass::PermissionDenied,
            ),
        ] {
            let outcome = DefaultErrorClassifier.classify_error(&invocation, &error);
            assert_eq!(outcome.status, ToolOutcomeStatus::FatalError, "{case}");
            assert_eq!(outcome.error_class, Some(expected_class), "{case}");
            assert!(!outcome.should_retry, "{case}");
        }
    }
}
