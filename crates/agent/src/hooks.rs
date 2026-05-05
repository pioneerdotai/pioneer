use pioneer_hooks::{
    HookActor, HookActorKind, HookContext, HookContextMode, HookDiagnostic, HookIdError, HookInput,
    HookInputKind, HookPhase, HookPhaseRequest, HookRuntime, HookRuntimeError, HookThreadId,
    HookTurnId, HookValue, HookWorkspaceId,
};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

#[derive(Debug, Clone)]
pub(super) struct AgentTurnHookContext {
    workspace_id: String,
    thread_id: String,
    turn_id: String,
}

impl AgentTurnHookContext {
    pub(super) fn new(workspace_id: &str, thread_id: &str, turn_id: &str) -> Self {
        Self {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        }
    }
}

pub(super) async fn run_noop_agent_turn_hook_phase(
    runtime: Option<&Arc<HookRuntime>>,
    context: &AgentTurnHookContext,
    phase: HookPhase,
) {
    let Some(runtime) = runtime else {
        return;
    };

    let request = match build_phase_request(context, phase) {
        Ok(request) => request,
        Err(error) => {
            warn!(
                phase = %phase,
                error = %error,
                "skipping agent turn hook phase because context could not be built"
            );
            return;
        }
    };

    match runtime.run_phase(request).await {
        Ok(response) => {
            for diagnostic in response.diagnostics {
                warn_hook_diagnostic(phase, &diagnostic);
            }
        }
        Err(error) => warn_hook_runtime_error(phase, &error),
    }
}

fn build_phase_request(
    context: &AgentTurnHookContext,
    phase: HookPhase,
) -> Result<HookPhaseRequest, HookIdError> {
    Ok(HookPhaseRequest::new(
        phase,
        HookContext {
            workspace_id: Some(HookWorkspaceId::new(context.workspace_id.clone())?),
            thread_id: Some(HookThreadId::new(context.thread_id.clone())?),
            turn_id: Some(HookTurnId::new(context.turn_id.clone())?),
            mode: Some(HookContextMode::Agent),
            actor: Some(HookActor {
                kind: HookActorKind::Agent,
                id: None,
            }),
            now_unix: Some(current_unix_timestamp()),
            ..HookContext::default()
        },
        HookInput {
            kind: HookInputKind::from(phase),
            payload: HookValue::Null,
        },
    ))
}

fn current_unix_timestamp() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

fn warn_hook_diagnostic(phase: HookPhase, diagnostic: &HookDiagnostic) {
    warn!(
        phase = %phase,
        code = %diagnostic.code,
        severity = ?diagnostic.severity,
        safe_for_user = diagnostic.safe_for_user,
        "agent turn hook diagnostic reported; continuing"
    );
}

fn warn_hook_runtime_error(phase: HookPhase, error: &HookRuntimeError) {
    match error {
        HookRuntimeError::Registry(_) => {
            warn!(
                phase = %phase,
                error_kind = "registry",
                "agent turn hook phase failed; continuing"
            );
        }
        HookRuntimeError::MissingHandler {
            subscription_id,
            hook_id,
            phase: error_phase,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                error_kind = "missing_handler",
                "agent turn hook phase failed; continuing"
            );
        }
        HookRuntimeError::HookFailed {
            subscription_id,
            hook_id,
            phase: error_phase,
            error,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                hook_error_code = %error.code,
                retryable = error.retryable,
                safe_for_user = error.safe_for_user,
                error_kind = "hook_failed",
                "agent turn hook phase failed; continuing"
            );
        }
        HookRuntimeError::HookTimedOut {
            subscription_id,
            hook_id,
            phase: error_phase,
            timeout_ms,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                timeout_ms = *timeout_ms,
                error_kind = "hook_timed_out",
                "agent turn hook phase failed; continuing"
            );
        }
        HookRuntimeError::HookFailedClosed {
            subscription_id,
            hook_id,
            phase: error_phase,
            error,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                hook_error_code = %error.code,
                retryable = error.retryable,
                safe_for_user = error.safe_for_user,
                error_kind = "hook_failed_closed",
                "agent turn hook phase failed; continuing"
            );
        }
        HookRuntimeError::MissingFallbackContribution {
            subscription_id,
            hook_id,
            phase: error_phase,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                error_kind = "missing_fallback_contribution",
                "agent turn hook phase failed; continuing"
            );
        }
        HookRuntimeError::MissingDependency {
            subscription_id,
            dependency_id,
            phase: error_phase,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                dependency_id = %dependency_id,
                error_kind = "missing_dependency",
                "agent turn hook phase failed; continuing"
            );
        }
        HookRuntimeError::DependencyCycle {
            phase: error_phase,
            subscription_ids,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_count = subscription_ids.len(),
                error_kind = "dependency_cycle",
                "agent turn hook phase failed; continuing"
            );
        }
    }
}
