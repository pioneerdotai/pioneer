use pioneer_hooks::{
    HookActor, HookActorKind, HookContext, HookContextMode, HookContribution, HookContributionHash,
    HookDiagnostic, HookId, HookIdError, HookInput, HookInputKind, HookPhase, HookPhaseRequest,
    HookPolicySet, HookPromptContextLimits, HookPromptContextSet, HookPromptSectionLimits,
    HookPromptSectionSet, HookRunStatus, HookRunSummary, HookRuntime, HookRuntimeError,
    HookSectionId, HookSubscriptionId, HookThreadId, HookTurnId, HookValue, HookWorkspaceId,
    PromptManifestDiagnosticContribution,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

const HOOK_MANIFEST_MESSAGE_MAX_CHARS: usize = 512;
const REDACTED_HOOK_DIAGNOSTIC_MESSAGE: &str = "Hook diagnostic redacted.";
const HOOK_BEST_EFFORT_FAILED_MESSAGE: &str = "Best-effort hook failed before prompt compilation.";

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
    manifest: EffectiveTurnPromptManifestHookMetadata,
}

impl EffectiveTurnPromptSectionSet {
    pub(super) fn empty() -> Self {
        Self::default()
    }

    pub(super) fn from_hook_prompt_section_set_and_manifest(
        sections: HookPromptSectionSet,
        manifest: EffectiveTurnPromptManifestHookMetadata,
    ) -> Self {
        Self { sections, manifest }
    }

    pub(super) fn clone_hook_prompt_section_set(&self) -> HookPromptSectionSet {
        self.sections.clone()
    }

    pub(super) fn manifest_metadata(&self) -> &EffectiveTurnPromptManifestHookMetadata {
        &self.manifest
    }

    pub(super) fn is_empty(&self) -> bool {
        self.sections.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct EffectiveTurnPromptManifestHookMetadata {
    pub(super) sources: Vec<EffectiveTurnPromptManifestHookSourceEntry>,
    pub(super) diagnostics: Vec<EffectiveTurnPromptManifestHookDiagnostic>,
}

impl EffectiveTurnPromptManifestHookMetadata {
    pub(super) fn empty() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EffectiveTurnPromptManifestHookSourceEntry {
    pub(super) source: EffectiveTurnPromptManifestHookSource,
    pub(super) section_id: Option<HookSectionId>,
    pub(super) contribution_kind: EffectiveTurnPromptManifestHookContributionKind,
    pub(super) hook_truncated: bool,
    pub(super) hook_content_chars: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EffectiveTurnPromptManifestHookSource {
    pub(super) hook_id: HookId,
    pub(super) subscription_id: HookSubscriptionId,
    pub(super) phase: HookPhase,
    pub(super) contribution_hash: Option<HookContributionHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EffectiveTurnPromptManifestHookContributionKind {
    PromptSection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EffectiveTurnPromptManifestHookDiagnostic {
    pub(super) code: EffectiveTurnPromptManifestHookDiagnosticCode,
    pub(super) message: String,
    pub(super) source: Option<EffectiveTurnPromptManifestHookSource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EffectiveTurnPromptManifestHookDiagnosticCode {
    HookDiagnostic,
    HookBestEffortFailed,
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
            let contributions = response.contributions;
            let phase_diagnostics = std::mem::take(&mut response.diagnostics);
            let mut prompt_section_set =
                prompt_section_set_from_contributions(contributions.clone());
            prompt_section_set.diagnostics.extend(phase_diagnostics);
            for diagnostic in &prompt_section_set.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePromptCompile, diagnostic);
            }
            let manifest = prompt_manifest_hook_metadata_from_phase_response(
                &contributions,
                &response.runs,
                &prompt_section_set,
            );
            Ok(
                EffectiveTurnPromptSectionSet::from_hook_prompt_section_set_and_manifest(
                    prompt_section_set,
                    manifest,
                ),
            )
        }
        Err(error) => {
            warn_hook_prompt_section_runtime_error(HookPhase::TurnPrePromptCompile, &error);
            Err(AgentTurnHookError::Runtime(error))
        }
    }
}

fn prompt_manifest_hook_metadata_from_phase_response(
    contributions: &[HookContribution],
    runs: &[HookRunSummary],
    prompt_section_set: &HookPromptSectionSet,
) -> EffectiveTurnPromptManifestHookMetadata {
    let run_sources_by_hash = run_sources_by_hash(runs);
    let mut metadata = EffectiveTurnPromptManifestHookMetadata::empty();
    let section_entries = prompt_section_entries_by_id(prompt_section_set);

    for contribution in contributions {
        match contribution {
            HookContribution::PromptSection(section) => {
                let hash = HookContributionHash::from_contribution(contribution);
                let Some(hash) = hash else {
                    continue;
                };
                let Some(run_sources) = run_sources_by_hash.get(&hash) else {
                    continue;
                };
                let entry = section_entries.get(&section.section_id);
                let hook_truncated =
                    section.truncated || entry.is_none() || entry.is_some_and(|entry| entry.0);
                let hook_content_chars = entry.map(|entry| entry.1);
                for run_source in run_sources {
                    metadata
                        .sources
                        .push(EffectiveTurnPromptManifestHookSourceEntry {
                            source: run_source.clone(),
                            section_id: Some(section.section_id.clone()),
                            contribution_kind:
                                EffectiveTurnPromptManifestHookContributionKind::PromptSection,
                            hook_truncated,
                            hook_content_chars,
                        });
                }
            }
            HookContribution::PromptManifestDiagnostic(diagnostic) => {
                let hash = HookContributionHash::from_contribution(contribution);
                let sources = hash
                    .as_ref()
                    .and_then(|hash| run_sources_by_hash.get(hash))
                    .cloned()
                    .unwrap_or_else(|| {
                        prompt_manifest_diagnostic_contribution_source(diagnostic, hash)
                            .into_iter()
                            .collect()
                    });
                let message = safe_prompt_manifest_diagnostic_message(diagnostic);
                if sources.is_empty() {
                    metadata
                        .diagnostics
                        .push(EffectiveTurnPromptManifestHookDiagnostic {
                            code: EffectiveTurnPromptManifestHookDiagnosticCode::HookDiagnostic,
                            message,
                            source: None,
                        });
                } else {
                    for source in sources {
                        metadata
                            .diagnostics
                            .push(EffectiveTurnPromptManifestHookDiagnostic {
                                code: EffectiveTurnPromptManifestHookDiagnosticCode::HookDiagnostic,
                                message: message.clone(),
                                source: Some(source),
                            });
                    }
                }
            }
            HookContribution::Policy(_)
            | HookContribution::PromptContext(_)
            | HookContribution::Audit(_)
            | HookContribution::Noop => {}
        }
    }

    for run in runs {
        if is_failed_prompt_compile_run(run.status) {
            metadata
                .diagnostics
                .push(EffectiveTurnPromptManifestHookDiagnostic {
                    code: EffectiveTurnPromptManifestHookDiagnosticCode::HookBestEffortFailed,
                    message: best_effort_failure_message(run),
                    source: Some(run_source_without_contribution(run)),
                });
        } else if run.status == HookRunStatus::Succeeded {
            for preview in &run.diagnostic_previews {
                metadata
                    .diagnostics
                    .push(EffectiveTurnPromptManifestHookDiagnostic {
                        code: EffectiveTurnPromptManifestHookDiagnosticCode::HookDiagnostic,
                        message: bounded_hook_manifest_message(preview.message.as_str()),
                        source: Some(run_source_without_contribution(run)),
                    });
            }
        }
    }

    metadata.sources.sort_by(|left, right| {
        left.section_id
            .cmp(&right.section_id)
            .then_with(|| left.source.hook_id.cmp(&right.source.hook_id))
            .then_with(|| {
                left.source
                    .subscription_id
                    .cmp(&right.source.subscription_id)
            })
            .then_with(|| left.source.phase.cmp(&right.source.phase))
            .then_with(|| {
                left.source
                    .contribution_hash
                    .cmp(&right.source.contribution_hash)
            })
            .then_with(|| {
                prompt_manifest_hook_contribution_kind_order(left.contribution_kind).cmp(
                    &prompt_manifest_hook_contribution_kind_order(right.contribution_kind),
                )
            })
    });
    metadata.diagnostics.sort_by(|left, right| {
        prompt_manifest_hook_diagnostic_code_order(left.code)
            .cmp(&prompt_manifest_hook_diagnostic_code_order(right.code))
            .then_with(|| left.message.cmp(&right.message))
            .then_with(|| {
                left.source
                    .as_ref()
                    .map(prompt_manifest_hook_source_sort_key)
                    .cmp(
                        &right
                            .source
                            .as_ref()
                            .map(prompt_manifest_hook_source_sort_key),
                    )
            })
    });

    metadata
}

fn run_sources_by_hash(
    runs: &[HookRunSummary],
) -> BTreeMap<HookContributionHash, Vec<EffectiveTurnPromptManifestHookSource>> {
    let mut sources =
        BTreeMap::<HookContributionHash, Vec<EffectiveTurnPromptManifestHookSource>>::new();
    for run in runs {
        for hash in &run.contribution_hashes {
            sources
                .entry(hash.clone())
                .or_default()
                .push(EffectiveTurnPromptManifestHookSource {
                    hook_id: run.hook_id.clone(),
                    subscription_id: run.subscription_id.clone(),
                    phase: run.phase,
                    contribution_hash: Some(hash.clone()),
                });
        }
    }
    for values in sources.values_mut() {
        values.sort_by(|left, right| {
            left.hook_id
                .cmp(&right.hook_id)
                .then_with(|| left.subscription_id.cmp(&right.subscription_id))
                .then_with(|| left.phase.cmp(&right.phase))
                .then_with(|| left.contribution_hash.cmp(&right.contribution_hash))
        });
    }
    sources
}

fn prompt_section_entries_by_id(
    prompt_section_set: &HookPromptSectionSet,
) -> BTreeMap<HookSectionId, (bool, usize)> {
    prompt_section_set
        .entries()
        .map(|entry| {
            (
                entry.section_id.clone(),
                (entry.truncated, entry.content.as_str().chars().count()),
            )
        })
        .collect()
}

fn prompt_manifest_diagnostic_contribution_source(
    diagnostic: &PromptManifestDiagnosticContribution,
    contribution_hash: Option<HookContributionHash>,
) -> Option<EffectiveTurnPromptManifestHookSource> {
    Some(EffectiveTurnPromptManifestHookSource {
        hook_id: diagnostic.hook_id.clone()?,
        subscription_id: diagnostic.subscription_id.clone()?,
        phase: HookPhase::TurnPrePromptCompile,
        contribution_hash,
    })
}

fn safe_prompt_manifest_diagnostic_message(
    diagnostic: &PromptManifestDiagnosticContribution,
) -> String {
    if diagnostic.safe_for_user {
        bounded_hook_manifest_message(diagnostic.message.as_str())
    } else {
        REDACTED_HOOK_DIAGNOSTIC_MESSAGE.to_owned()
    }
}

fn best_effort_failure_message(run: &HookRunSummary) -> String {
    if let Some(error) = run.error.as_ref() {
        return bounded_hook_manifest_message(error.message.as_str());
    }
    if let Some(preview) = run.diagnostic_previews.first() {
        return bounded_hook_manifest_message(preview.message.as_str());
    }
    HOOK_BEST_EFFORT_FAILED_MESSAGE.to_owned()
}

fn run_source_without_contribution(run: &HookRunSummary) -> EffectiveTurnPromptManifestHookSource {
    EffectiveTurnPromptManifestHookSource {
        hook_id: run.hook_id.clone(),
        subscription_id: run.subscription_id.clone(),
        phase: run.phase,
        contribution_hash: None,
    }
}

fn is_failed_prompt_compile_run(status: HookRunStatus) -> bool {
    matches!(status, HookRunStatus::Failed | HookRunStatus::TimedOut)
}

fn bounded_hook_manifest_message(message: &str) -> String {
    let mut chars = message.chars();
    let bounded = chars
        .by_ref()
        .take(HOOK_MANIFEST_MESSAGE_MAX_CHARS)
        .collect::<String>();
    if chars.next().is_some() {
        format!("{bounded}...")
    } else {
        bounded
    }
}

fn prompt_manifest_hook_source_sort_key(
    source: &EffectiveTurnPromptManifestHookSource,
) -> (
    HookId,
    HookSubscriptionId,
    HookPhase,
    Option<HookContributionHash>,
) {
    (
        source.hook_id.clone(),
        source.subscription_id.clone(),
        source.phase,
        source.contribution_hash.clone(),
    )
}

fn prompt_manifest_hook_contribution_kind_order(
    kind: EffectiveTurnPromptManifestHookContributionKind,
) -> u8 {
    match kind {
        EffectiveTurnPromptManifestHookContributionKind::PromptSection => 0,
    }
}

fn prompt_manifest_hook_diagnostic_code_order(
    code: EffectiveTurnPromptManifestHookDiagnosticCode,
) -> u8 {
    match code {
        EffectiveTurnPromptManifestHookDiagnosticCode::HookDiagnostic => 0,
        EffectiveTurnPromptManifestHookDiagnosticCode::HookBestEffortFailed => 1,
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
