use super::*;

pub(super) fn hook_diagnostics_from_strings(messages: &[String]) -> Vec<HookDiagnostic> {
    messages
        .iter()
        .map(|message| {
            memory_hook_diagnostic("memory.diagnostic", safe_memory_policy_diagnostic(message))
        })
        .collect()
}

pub(super) fn memory_missing_state_response(hook: &'static str) -> HookHandlerResponse {
    let mut response = HookHandlerResponse::default();
    response.diagnostics.push(memory_safe_warning_diagnostic(
        "memory.missing_state",
        format!("{hook} skipped because memory turn policy state was unavailable"),
    ));
    response
}

pub(super) fn memory_missing_policy_response(hook: &'static str) -> HookHandlerResponse {
    let mut response = HookHandlerResponse::default();
    response.diagnostics.push(memory_safe_warning_diagnostic(
        "memory.missing_policy",
        format!("{hook} skipped because memory hook policy was unavailable"),
    ));
    response
}

pub(super) fn memory_hook_diagnostic(
    code: &'static str,
    message: impl Into<String>,
) -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(message.into())
            .expect("diagnostic message should be non-empty"),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user: false,
        metadata: HookMetadata::default(),
    }
}

pub(super) fn memory_safe_info_diagnostic(
    code: &'static str,
    message: impl Into<String>,
) -> HookDiagnostic {
    memory_safe_diagnostic(code, message, HookDiagnosticSeverity::Info)
}

pub(super) fn memory_safe_warning_diagnostic(
    code: &'static str,
    message: impl Into<String>,
) -> HookDiagnostic {
    memory_safe_diagnostic(code, message, HookDiagnosticSeverity::Warning)
}

pub(super) fn memory_safe_diagnostic(
    code: &'static str,
    message: impl Into<String>,
    severity: HookDiagnosticSeverity,
) -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(safe_memory_policy_diagnostic(message.into().as_str()))
            .expect("safe diagnostic message should be non-empty"),
        severity,
        safe_for_user: true,
        metadata: HookMetadata::default(),
    }
}

pub(super) fn memory_hook_error(code: &'static str, message: impl Into<String>) -> HookError {
    HookError::new(
        HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        HookDiagnosticMessage::new(message.into()).expect("hook error message should be non-empty"),
    )
}

pub(super) fn memory_retryable_safe_hook_error(
    code: &'static str,
    message: impl Into<String>,
) -> HookError {
    memory_hook_error(code, safe_memory_policy_diagnostic(message.into().as_str()))
        .with_retryable(true)
        .with_safe_for_user(true)
}
