use pioneer_hooks::{
    HookActor, HookActorKind, HookContext, HookContextMode, HookContribution, HookDiagnostic,
    HookIdError, HookInput, HookInputKind, HookPhase, HookPhaseRequest, HookPolicySet,
    HookPromptContextLimits, HookPromptContextSet, HookPromptSectionLimits, HookPromptSectionSet,
    HookRuntime, HookRuntimeError, HookThreadId, HookTurnId, HookValue, HookWorkspaceId,
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

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct EffectiveTurnPolicySet {
    policies: HookPolicySet,
}

impl EffectiveTurnPolicySet {
    pub(super) fn empty() -> Self {
        Self::default()
    }

    pub(super) fn from_hook_policy_set(policies: HookPolicySet) -> Self {
        Self { policies }
    }

    pub(super) fn clone_hook_policy_set(&self) -> HookPolicySet {
        self.policies.clone()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct EffectiveTurnPromptContextSet {
    contexts: HookPromptContextSet,
}

impl EffectiveTurnPromptContextSet {
    pub(super) fn empty() -> Self {
        Self::default()
    }

    pub(super) fn from_hook_prompt_context_set(contexts: HookPromptContextSet) -> Self {
        Self { contexts }
    }

    pub(super) fn clone_hook_prompt_context_set(&self) -> HookPromptContextSet {
        self.contexts.clone()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct EffectiveTurnPromptSectionSet {
    sections: HookPromptSectionSet,
}

impl EffectiveTurnPromptSectionSet {
    pub(super) fn empty() -> Self {
        Self::default()
    }

    pub(super) fn from_hook_prompt_section_set(sections: HookPromptSectionSet) -> Self {
        Self { sections }
    }

    pub(super) fn clone_hook_prompt_section_set(&self) -> HookPromptSectionSet {
        self.sections.clone()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.sections.is_empty()
    }
}

#[derive(Debug)]
pub(super) enum AgentTurnHookError {
    InvalidContext(HookIdError),
    Runtime(HookRuntimeError),
}

impl AgentTurnHookError {
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Self::InvalidContext(error) => {
                let _ = error;
                "invalid_context"
            }
            Self::Runtime(error) => {
                let _ = error;
                "runtime"
            }
        }
    }

    pub(super) fn safe_message(&self) -> &'static str {
        "turn policy hook failed"
    }
}

pub(super) async fn run_agent_turn_policy_hook_phase(
    runtime: Option<&Arc<HookRuntime>>,
    context: &AgentTurnHookContext,
) -> Result<EffectiveTurnPolicySet, AgentTurnHookError> {
    let Some(runtime) = runtime else {
        return Ok(EffectiveTurnPolicySet::empty());
    };

    let empty_policy_set = EffectiveTurnPolicySet::empty();
    let empty_prompt_context_set = EffectiveTurnPromptContextSet::empty();
    let request = build_phase_request(
        context,
        HookPhase::TurnPrePolicy,
        &empty_policy_set,
        &empty_prompt_context_set,
    )
    .map_err(AgentTurnHookError::InvalidContext)?;

    match runtime.run_phase(request).await {
        Ok(response) => {
            for diagnostic in &response.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePolicy, diagnostic);
            }
            let policy_set = policy_set_from_contributions(response.contributions);
            for diagnostic in &policy_set.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePolicy, diagnostic);
            }
            Ok(EffectiveTurnPolicySet::from_hook_policy_set(policy_set))
        }
        Err(error) => {
            warn_hook_policy_runtime_error(HookPhase::TurnPrePolicy, &error);
            Err(AgentTurnHookError::Runtime(error))
        }
    }
}

pub(super) async fn run_agent_turn_prompt_context_hook_phase(
    runtime: Option<&Arc<HookRuntime>>,
    context: &AgentTurnHookContext,
    policy_set: &EffectiveTurnPolicySet,
) -> EffectiveTurnPromptContextSet {
    let Some(runtime) = runtime else {
        return EffectiveTurnPromptContextSet::empty();
    };

    let empty_prompt_context_set = EffectiveTurnPromptContextSet::empty();
    let request = match build_phase_request(
        context,
        HookPhase::TurnPrePromptContext,
        policy_set,
        &empty_prompt_context_set,
    ) {
        Ok(request) => request,
        Err(error) => {
            warn!(
                phase = %HookPhase::TurnPrePromptContext,
                error = %error,
                "agent turn prompt context hook phase failed to build request; continuing with empty context"
            );
            return runtime_failed_prompt_context_set();
        }
    };

    match runtime.run_phase(request).await {
        Ok(mut response) => {
            for diagnostic in &response.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePromptContext, diagnostic);
            }
            let phase_diagnostics = std::mem::take(&mut response.diagnostics);
            let mut prompt_context_set =
                prompt_context_set_from_contributions(response.contributions);
            prompt_context_set.diagnostics.extend(phase_diagnostics);
            for diagnostic in &prompt_context_set.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePromptContext, diagnostic);
            }
            EffectiveTurnPromptContextSet::from_hook_prompt_context_set(prompt_context_set)
        }
        Err(error) => {
            warn_hook_prompt_context_runtime_error(HookPhase::TurnPrePromptContext, &error);
            runtime_failed_prompt_context_set()
        }
    }
}

pub(super) async fn run_agent_turn_prompt_compile_hook_phase(
    runtime: Option<&Arc<HookRuntime>>,
    context: &AgentTurnHookContext,
    policy_set: &EffectiveTurnPolicySet,
    prompt_context_set: &EffectiveTurnPromptContextSet,
) -> Result<EffectiveTurnPromptSectionSet, AgentTurnHookError> {
    let Some(runtime) = runtime else {
        return Ok(EffectiveTurnPromptSectionSet::empty());
    };

    let request = build_phase_request(
        context,
        HookPhase::TurnPrePromptCompile,
        policy_set,
        prompt_context_set,
    )
    .map_err(AgentTurnHookError::InvalidContext)?;

    match runtime.run_phase(request).await {
        Ok(mut response) => {
            for diagnostic in &response.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePromptCompile, diagnostic);
            }
            let phase_diagnostics = std::mem::take(&mut response.diagnostics);
            let mut prompt_section_set =
                prompt_section_set_from_contributions(response.contributions);
            prompt_section_set.diagnostics.extend(phase_diagnostics);
            for diagnostic in &prompt_section_set.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePromptCompile, diagnostic);
            }
            Ok(EffectiveTurnPromptSectionSet::from_hook_prompt_section_set(
                prompt_section_set,
            ))
        }
        Err(error) => {
            warn_hook_prompt_section_runtime_error(HookPhase::TurnPrePromptCompile, &error);
            Err(AgentTurnHookError::Runtime(error))
        }
    }
}

pub(super) async fn run_noop_agent_turn_hook_phase(
    runtime: Option<&Arc<HookRuntime>>,
    context: &AgentTurnHookContext,
    phase: HookPhase,
    policy_set: &EffectiveTurnPolicySet,
    prompt_context_set: &EffectiveTurnPromptContextSet,
) {
    let Some(runtime) = runtime else {
        return;
    };

    let request = match build_phase_request(context, phase, policy_set, prompt_context_set) {
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
    policy_set: &EffectiveTurnPolicySet,
    prompt_context_set: &EffectiveTurnPromptContextSet,
) -> Result<HookPhaseRequest, HookIdError> {
    let request = HookPhaseRequest::new(
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
    )
    .with_policy_set(policy_set.clone_hook_policy_set())
    .with_prompt_context_set(prompt_context_set.clone_hook_prompt_context_set());
    Ok(request)
}

fn policy_set_from_contributions(contributions: Vec<HookContribution>) -> HookPolicySet {
    HookPolicySet::merge_hook_contributions(contributions)
}

fn prompt_context_set_from_contributions(
    contributions: Vec<HookContribution>,
) -> HookPromptContextSet {
    HookPromptContextSet::aggregate_hook_contributions(
        contributions,
        HookPromptContextLimits::default(),
    )
}

fn prompt_section_set_from_contributions(
    contributions: Vec<HookContribution>,
) -> HookPromptSectionSet {
    HookPromptSectionSet::aggregate_hook_contributions(
        contributions,
        HookPromptSectionLimits::default(),
    )
}

fn runtime_failed_prompt_context_set() -> EffectiveTurnPromptContextSet {
    EffectiveTurnPromptContextSet::from_hook_prompt_context_set(
        HookPromptContextSet::runtime_failed(),
    )
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

fn warn_hook_policy_runtime_error(phase: HookPhase, error: &HookRuntimeError) {
    match error {
        HookRuntimeError::Registry(_) => {
            warn!(
                phase = %phase,
                error_kind = "registry",
                "agent turn policy hook phase failed; failing turn"
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
                "agent turn policy hook phase failed; failing turn"
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
                "agent turn policy hook phase failed; failing turn"
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
                "agent turn policy hook phase failed; failing turn"
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
                "agent turn policy hook phase failed; failing turn"
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
                "agent turn policy hook phase failed; failing turn"
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
                "agent turn policy hook phase failed; failing turn"
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
                "agent turn policy hook phase failed; failing turn"
            );
        }
    }
}

fn warn_hook_prompt_context_runtime_error(phase: HookPhase, error: &HookRuntimeError) {
    match error {
        HookRuntimeError::Registry(_) => {
            warn!(
                phase = %phase,
                error_kind = "registry",
                "agent turn prompt context hook phase failed; continuing with empty context"
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
                "agent turn prompt context hook phase failed; continuing with empty context"
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
                "agent turn prompt context hook phase failed; continuing with empty context"
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
                "agent turn prompt context hook phase failed; continuing with empty context"
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
                "agent turn prompt context hook phase failed; continuing with empty context"
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
                "agent turn prompt context hook phase failed; continuing with empty context"
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
                "agent turn prompt context hook phase failed; continuing with empty context"
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
                "agent turn prompt context hook phase failed; continuing with empty context"
            );
        }
    }
}

fn warn_hook_prompt_section_runtime_error(phase: HookPhase, error: &HookRuntimeError) {
    match error {
        HookRuntimeError::Registry(_) => {
            warn!(
                phase = %phase,
                error_kind = "registry",
                "agent turn prompt section hook phase failed; failing turn"
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
                "agent turn prompt section hook phase failed; failing turn"
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
                "agent turn prompt section hook phase failed; failing turn"
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
                "agent turn prompt section hook phase failed; failing turn"
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
                "agent turn prompt section hook phase failed; failing turn"
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
                "agent turn prompt section hook phase failed; failing turn"
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
                "agent turn prompt section hook phase failed; failing turn"
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
                "agent turn prompt section hook phase failed; failing turn"
            );
        }
    }
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
