use pioneer_hooks::{
    HookActor, HookActorKind, HookContext, HookContextMode, HookContribution, HookContributionHash,
    HookContributionId, HookDiagnostic, HookId, HookIdError, HookInput, HookInputKind, HookPhase,
    HookPhaseRequest, HookPolicySet, HookPromptContextLimits, HookPromptContextSet,
    HookPromptSectionLimits, HookPromptSectionSet, HookRunStatus, HookRunSummary, HookRuntime,
    HookRuntimeError, HookSectionId, HookSubscriptionId, HookThreadId, HookToolBundleId,
    HookToolBundleSet, HookToolName, HookTurnId, HookWorkspaceId,
    PromptManifestDiagnosticContribution, ToolBundleContribution, TurnPostTurnDomainEventSummary,
    TurnPostTurnHookInput, TurnPostTurnHookInputLimits, TurnPostTurnStatus,
    TurnPostTurnToolEventSummary, TurnPrePolicyHookInput, TurnPrePromptCompileHookInput,
    TurnPrePromptContextHookInput, TurnPreToolMaterializationHookInput,
};
use pioneer_tools::ToolExtensionBundle;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

const HOOK_MANIFEST_MESSAGE_MAX_CHARS: usize = 512;
const REDACTED_HOOK_DIAGNOSTIC_MESSAGE: &str = "Hook diagnostic redacted.";
const HOOK_BEST_EFFORT_FAILED_MESSAGE: &str = "Best-effort hook failed before prompt compilation.";
const TOOL_BUNDLE_MISSING_ARTIFACT_DIAGNOSTIC_CODE: &str = "tool_bundle.missing_artifact";

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentPostTurnHookDispatchPolicy {
    pub on_success: bool,
    pub on_terminal_failure: bool,
    pub on_provider_failure: bool,
    pub on_interrupted: bool,
}

impl Default for AgentPostTurnHookDispatchPolicy {
    fn default() -> Self {
        Self {
            on_success: true,
            on_terminal_failure: false,
            on_provider_failure: false,
            on_interrupted: false,
        }
    }
}

impl AgentPostTurnHookDispatchPolicy {
    pub fn success_only() -> Self {
        Self::default()
    }

    pub fn include_failures(mut self) -> Self {
        self.on_terminal_failure = true;
        self.on_provider_failure = true;
        self
    }

    pub(crate) fn should_dispatch(self, status: TurnPostTurnStatus) -> bool {
        match status {
            TurnPostTurnStatus::Succeeded => self.on_success,
            TurnPostTurnStatus::Failed => self.on_terminal_failure,
            TurnPostTurnStatus::ProviderFailure => self.on_provider_failure,
            TurnPostTurnStatus::Interrupted => self.on_interrupted,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct AgentTurnPostTurnSummary {
    status: TurnPostTurnStatus,
    model: Option<String>,
    model_provider: Option<String>,
    user_text: String,
    assistant_text: String,
    error: Option<String>,
    tool_events: Vec<TurnPostTurnToolEventSummary>,
    domain_events: Vec<TurnPostTurnDomainEventSummary>,
}

impl AgentTurnPostTurnSummary {
    pub(super) fn succeeded_with_model(
        model: Option<String>,
        model_provider: Option<String>,
        user_text: String,
        assistant_text: String,
        tool_events: Vec<TurnPostTurnToolEventSummary>,
        domain_events: Vec<TurnPostTurnDomainEventSummary>,
    ) -> Self {
        Self {
            status: TurnPostTurnStatus::Succeeded,
            model,
            model_provider,
            user_text,
            assistant_text,
            error: None,
            tool_events,
            domain_events,
        }
    }

    pub(super) fn failed(status: TurnPostTurnStatus, user_text: String, error: String) -> Self {
        Self::failed_with_events(
            status,
            user_text,
            String::new(),
            error,
            Vec::new(),
            Vec::new(),
        )
    }

    pub(super) fn failed_with_events(
        status: TurnPostTurnStatus,
        user_text: String,
        assistant_text: String,
        error: String,
        tool_events: Vec<TurnPostTurnToolEventSummary>,
        domain_events: Vec<TurnPostTurnDomainEventSummary>,
    ) -> Self {
        debug_assert!(matches!(
            status,
            TurnPostTurnStatus::Failed
                | TurnPostTurnStatus::ProviderFailure
                | TurnPostTurnStatus::Interrupted
        ));
        Self {
            status,
            model: None,
            model_provider: None,
            user_text,
            assistant_text,
            error: Some(error),
            tool_events,
            domain_events,
        }
    }

    fn status(&self) -> TurnPostTurnStatus {
        self.status
    }

    fn into_hook_input(self) -> TurnPostTurnHookInput {
        TurnPostTurnHookInput::from_parts_with_model(
            self.status,
            self.model.as_deref(),
            self.model_provider.as_deref(),
            Some(self.user_text.as_str()),
            Some(self.assistant_text.as_str()),
            self.error.as_deref(),
            self.tool_events,
            self.domain_events,
            TurnPostTurnHookInputLimits::default(),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct AgentTurnPostTurnHookDispatch {
    context: AgentTurnHookContext,
    policy_set: EffectiveTurnPolicySet,
    prompt_context_set: EffectiveTurnPromptContextSet,
    summary: AgentTurnPostTurnSummary,
}

impl AgentTurnPostTurnHookDispatch {
    pub(super) fn new(
        context: AgentTurnHookContext,
        policy_set: EffectiveTurnPolicySet,
        prompt_context_set: EffectiveTurnPromptContextSet,
        summary: AgentTurnPostTurnSummary,
    ) -> Self {
        Self {
            context,
            policy_set,
            prompt_context_set,
            summary,
        }
    }

    pub(super) fn status(&self) -> TurnPostTurnStatus {
        self.summary.status()
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
    manifest: EffectiveTurnPromptManifestHookMetadata,
}

impl EffectiveTurnPromptContextSet {
    pub(super) fn empty() -> Self {
        Self::default()
    }

    pub(super) fn from_hook_prompt_context_set(contexts: HookPromptContextSet) -> Self {
        Self {
            contexts,
            manifest: EffectiveTurnPromptManifestHookMetadata::empty(),
        }
    }

    pub(super) fn from_hook_prompt_context_set_and_manifest(
        contexts: HookPromptContextSet,
        manifest: EffectiveTurnPromptManifestHookMetadata,
    ) -> Self {
        Self { contexts, manifest }
    }

    pub(super) fn clone_hook_prompt_context_set(&self) -> HookPromptContextSet {
        self.contexts.clone()
    }

    pub(super) fn manifest_metadata(&self) -> &EffectiveTurnPromptManifestHookMetadata {
        &self.manifest
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct EffectiveTurnPromptSectionSet {
    sections: HookPromptSectionSet,
    manifest: EffectiveTurnPromptManifestHookMetadata,
}

impl EffectiveTurnPromptSectionSet {
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

    pub(super) fn combined(
        first: &EffectiveTurnPromptManifestHookMetadata,
        second: &EffectiveTurnPromptManifestHookMetadata,
    ) -> Self {
        let mut combined = first.clone();
        combined.sources.extend(second.sources.iter().cloned());
        combined
            .diagnostics
            .extend(second.diagnostics.iter().cloned());
        combined
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EffectiveTurnPromptManifestHookSourceEntry {
    pub(super) source: EffectiveTurnPromptManifestHookSource,
    pub(super) section_id: Option<HookSectionId>,
    pub(super) contribution_kind: EffectiveTurnPromptManifestHookContributionKind,
    pub(super) contribution_id: Option<HookContributionId>,
    pub(super) priority: Option<i32>,
    pub(super) source_count: Option<usize>,
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
    PromptContext,
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

#[derive(Clone)]
pub(super) struct AgentTurnToolBundleContribution {
    contribution: ToolBundleContribution,
    bundle: ToolExtensionBundle,
}

impl AgentTurnToolBundleContribution {
    fn new(contribution: ToolBundleContribution, bundle: ToolExtensionBundle) -> Self {
        Self {
            contribution,
            bundle,
        }
    }

    fn contribution(&self) -> &ToolBundleContribution {
        &self.contribution
    }

    fn bundle(&self) -> &ToolExtensionBundle {
        &self.bundle
    }
}

#[derive(Default)]
pub(crate) struct AgentToolBundleArtifactStore {
    bundles: Mutex<BTreeMap<(String, HookToolBundleId), ToolExtensionBundle>>,
}

impl AgentToolBundleArtifactStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(
        &self,
        turn_id: impl Into<String>,
        bundle_id: HookToolBundleId,
        bundle: ToolExtensionBundle,
    ) {
        if let Ok(mut bundles) = self.bundles.lock() {
            bundles.insert((turn_id.into(), bundle_id), bundle);
        }
    }

    fn get(&self, turn_id: &str, bundle_id: &HookToolBundleId) -> Option<ToolExtensionBundle> {
        self.bundles.lock().ok().and_then(|bundles| {
            bundles
                .get(&(turn_id.to_owned(), bundle_id.clone()))
                .cloned()
        })
    }

    pub(crate) fn clear_turn(&self, turn_id: &str) {
        if let Ok(mut bundles) = self.bundles.lock() {
            bundles.retain(|(bundle_turn_id, _), _| bundle_turn_id != turn_id);
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct EffectiveTurnToolBundleSet {
    bundles: Vec<ToolExtensionBundle>,
    metadata: HookToolBundleSet,
}

impl EffectiveTurnToolBundleSet {
    pub(super) fn bundles(&self) -> &[ToolExtensionBundle] {
        &self.bundles
    }

    pub(super) fn diagnostics(&self) -> &[HookDiagnostic] {
        &self.metadata.diagnostics
    }

    fn from_local(local_contributions: Vec<AgentTurnToolBundleContribution>) -> Self {
        Self::from_local_and_hook_contributions(local_contributions, Vec::new(), None, "")
    }

    fn from_local_and_hook_contributions(
        local_contributions: Vec<AgentTurnToolBundleContribution>,
        hook_contributions: Vec<HookContribution>,
        artifact_store: Option<&AgentToolBundleArtifactStore>,
        turn_id: &str,
    ) -> Self {
        let local_artifacts = local_tool_bundle_artifacts_by_id(&local_contributions);
        let mut contributions = local_contributions
            .into_iter()
            .map(|local| HookContribution::ToolBundle(local.contribution))
            .collect::<Vec<_>>();
        contributions.extend(hook_contributions);

        let mut metadata = HookToolBundleSet::aggregate_hook_contributions(contributions);
        let mut bundles = Vec::new();
        let mut missing_artifact_ids = Vec::new();
        for entry in metadata.entries() {
            if let Some(bundle) = local_artifacts.get(&entry.bundle_id) {
                bundles.push(bundle.clone());
            } else if let Some(bundle) =
                artifact_store.and_then(|store| store.get(turn_id, &entry.bundle_id))
            {
                bundles.push(bundle);
            } else {
                missing_artifact_ids.push(entry.bundle_id.as_str().to_owned());
            }
        }
        metadata.diagnostics.extend(
            missing_artifact_ids
                .into_iter()
                .map(|bundle_id| missing_tool_bundle_artifact_diagnostic(bundle_id.as_str())),
        );

        Self { bundles, metadata }
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
    input: TurnPrePolicyHookInput,
) -> Result<EffectiveTurnPolicySet, AgentTurnHookError> {
    let Some(runtime) = runtime else {
        return Ok(EffectiveTurnPolicySet::empty());
    };

    let empty_policy_set = EffectiveTurnPolicySet::empty();
    let empty_prompt_context_set = EffectiveTurnPromptContextSet::empty();
    let request = build_phase_request_with_input(
        context,
        HookPhase::TurnPrePolicy,
        &empty_policy_set,
        &empty_prompt_context_set,
        HookInput::turn_pre_policy(input),
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
    input: TurnPrePromptContextHookInput,
) -> EffectiveTurnPromptContextSet {
    let Some(runtime) = runtime else {
        return EffectiveTurnPromptContextSet::empty();
    };

    let empty_prompt_context_set = EffectiveTurnPromptContextSet::empty();
    let request = match build_phase_request_with_input(
        context,
        HookPhase::TurnPrePromptContext,
        policy_set,
        &empty_prompt_context_set,
        HookInput::turn_pre_prompt_context(input),
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
            let hook_contributions = response.contributions;
            let phase_diagnostics = std::mem::take(&mut response.diagnostics);
            let mut prompt_context_set =
                prompt_context_set_from_contributions(hook_contributions.clone());
            prompt_context_set.diagnostics.extend(phase_diagnostics);
            for diagnostic in &prompt_context_set.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePromptContext, diagnostic);
            }
            let manifest = prompt_context_manifest_hook_metadata_from_phase_response(
                &hook_contributions,
                &response.runs,
                &prompt_context_set,
            );
            EffectiveTurnPromptContextSet::from_hook_prompt_context_set_and_manifest(
                prompt_context_set,
                manifest,
            )
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
    base_contributions: Vec<HookContribution>,
    input: TurnPrePromptCompileHookInput,
) -> Result<EffectiveTurnPromptSectionSet, AgentTurnHookError> {
    let Some(runtime) = runtime else {
        let prompt_section_set = prompt_section_set_from_contributions(base_contributions);
        return Ok(
            EffectiveTurnPromptSectionSet::from_hook_prompt_section_set_and_manifest(
                prompt_section_set,
                EffectiveTurnPromptManifestHookMetadata::empty(),
            ),
        );
    };

    let request = build_phase_request_with_input(
        context,
        HookPhase::TurnPrePromptCompile,
        policy_set,
        prompt_context_set,
        HookInput::turn_pre_prompt_compile(input),
    )
    .map_err(AgentTurnHookError::InvalidContext)?;

    match runtime.run_phase(request).await {
        Ok(mut response) => {
            for diagnostic in &response.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePromptCompile, diagnostic);
            }
            let hook_contributions = response.contributions;
            let phase_diagnostics = std::mem::take(&mut response.diagnostics);
            let mut contributions = base_contributions;
            contributions.extend(hook_contributions.clone());
            let mut prompt_section_set = prompt_section_set_from_contributions(contributions);
            prompt_section_set.diagnostics.extend(phase_diagnostics);
            for diagnostic in &prompt_section_set.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPrePromptCompile, diagnostic);
            }
            let manifest = prompt_manifest_hook_metadata_from_phase_response(
                &hook_contributions,
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

pub(super) async fn run_agent_turn_tool_materialization_hook_phase(
    runtime: Option<&Arc<HookRuntime>>,
    context: &AgentTurnHookContext,
    policy_set: &EffectiveTurnPolicySet,
    prompt_context_set: &EffectiveTurnPromptContextSet,
    local_contributions: Vec<AgentTurnToolBundleContribution>,
    artifact_store: Option<&Arc<AgentToolBundleArtifactStore>>,
    provider_tool_calling: bool,
) -> Result<EffectiveTurnToolBundleSet, AgentTurnHookError> {
    let Some(runtime) = runtime else {
        return Ok(EffectiveTurnToolBundleSet::from_local(local_contributions));
    };

    let input = TurnPreToolMaterializationHookInput::from_parts(
        provider_tool_calling,
        existing_tool_names_from_local_contributions(&local_contributions),
    );
    let request = build_phase_request_with_input(
        context,
        HookPhase::TurnPreToolMaterialization,
        policy_set,
        prompt_context_set,
        HookInput::turn_pre_tool_materialization(input),
    )
    .map_err(AgentTurnHookError::InvalidContext)?;

    match runtime.run_phase(request).await {
        Ok(mut response) => {
            for diagnostic in &response.diagnostics {
                warn_hook_diagnostic(HookPhase::TurnPreToolMaterialization, diagnostic);
            }
            let hook_contributions = response.contributions;
            let phase_diagnostics = std::mem::take(&mut response.diagnostics);
            let mut set = EffectiveTurnToolBundleSet::from_local_and_hook_contributions(
                local_contributions,
                hook_contributions,
                artifact_store.map(Arc::as_ref),
                context.turn_id.as_str(),
            );
            if let Some(store) = artifact_store {
                store.clear_turn(context.turn_id.as_str());
            }
            set.metadata.diagnostics.extend(phase_diagnostics);
            for diagnostic in set.diagnostics() {
                warn_hook_diagnostic(HookPhase::TurnPreToolMaterialization, diagnostic);
            }
            Ok(set)
        }
        Err(error) => {
            warn_hook_runtime_error(HookPhase::TurnPreToolMaterialization, &error);
            Err(AgentTurnHookError::Runtime(error))
        }
    }
}

fn prompt_context_manifest_hook_metadata_from_phase_response(
    contributions: &[HookContribution],
    runs: &[HookRunSummary],
    prompt_context_set: &HookPromptContextSet,
) -> EffectiveTurnPromptManifestHookMetadata {
    let run_sources_by_hash = run_sources_by_hash(runs);
    let mut metadata = EffectiveTurnPromptManifestHookMetadata::empty();
    let context_entries = prompt_context_entries_by_id(prompt_context_set);

    for contribution in contributions {
        if let HookContribution::PromptContext(context) = contribution {
            let hash = HookContributionHash::from_contribution(contribution);
            let Some(hash) = hash else {
                continue;
            };
            let Some(run_sources) = run_sources_by_hash.get(&hash) else {
                continue;
            };
            let entry = context_entries.get(&context.contribution_id);
            let hook_truncated =
                context.truncated || entry.is_none() || entry.is_some_and(|entry| entry.0);
            let hook_content_chars = entry.map(|entry| entry.1);
            for run_source in run_sources {
                metadata
                    .sources
                    .push(EffectiveTurnPromptManifestHookSourceEntry {
                        source: run_source.clone(),
                        section_id: None,
                        contribution_kind:
                            EffectiveTurnPromptManifestHookContributionKind::PromptContext,
                        contribution_id: Some(context.contribution_id.clone()),
                        priority: Some(context.priority),
                        source_count: Some(context.source_refs.len()),
                        hook_truncated,
                        hook_content_chars,
                    });
            }
        }
    }

    metadata
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
                            contribution_id: Some(section.contribution_id.clone()),
                            priority: Some(section.priority),
                            source_count: Some(0),
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
            | HookContribution::ToolBundle(_)
            | HookContribution::PromptContext(_)
            | HookContribution::Audit(_)
            | HookContribution::BackgroundJob(_)
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
            .then_with(|| left.contribution_id.cmp(&right.contribution_id))
            .then_with(|| left.priority.cmp(&right.priority))
            .then_with(|| left.source_count.cmp(&right.source_count))
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

fn prompt_context_entries_by_id(
    prompt_context_set: &HookPromptContextSet,
) -> BTreeMap<HookContributionId, (bool, usize)> {
    prompt_context_set
        .entries()
        .map(|entry| {
            (
                entry.contribution_id.clone(),
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
        EffectiveTurnPromptManifestHookContributionKind::PromptContext => 0,
        EffectiveTurnPromptManifestHookContributionKind::PromptSection => 1,
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

pub(super) async fn run_agent_turn_post_turn_hook_phase(
    runtime: Option<&Arc<HookRuntime>>,
    dispatch: AgentTurnPostTurnHookDispatch,
) {
    let Some(runtime) = runtime else {
        return;
    };

    let phase = HookPhase::TurnPostTurn;
    let request = match build_phase_request_with_input(
        &dispatch.context,
        phase,
        &dispatch.policy_set,
        &dispatch.prompt_context_set,
        HookInput::turn_post_turn(dispatch.summary.into_hook_input()),
    ) {
        Ok(request) => request,
        Err(error) => {
            warn!(
                phase = %phase,
                error = %error,
                "skipping agent turn post-turn hook because context could not be built"
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
    build_phase_request_with_input(
        context,
        phase,
        policy_set,
        prompt_context_set,
        HookInput::empty(HookInputKind::from(phase)),
    )
}

fn build_phase_request_with_input(
    context: &AgentTurnHookContext,
    phase: HookPhase,
    policy_set: &EffectiveTurnPolicySet,
    prompt_context_set: &EffectiveTurnPromptContextSet,
    input: HookInput,
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
        input,
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

pub(super) fn tool_bundle_contributions_from_bundles(
    domain: &'static str,
    id_prefix: &'static str,
    priority: i32,
    bundles: Vec<ToolExtensionBundle>,
) -> Vec<AgentTurnToolBundleContribution> {
    bundles
        .into_iter()
        .enumerate()
        .filter_map(|(index, bundle)| {
            if bundle.specs.is_empty() && bundle.handlers.is_empty() {
                return None;
            }

            let contribution_id =
                pioneer_hooks::HookContributionId::new(format!("{id_prefix}.contribution.{index}"))
                    .expect("static tool bundle contribution id prefix is valid");
            let bundle_id = HookToolBundleId::new(format!("{id_prefix}.bundle.{index}"))
                .expect("static tool bundle id prefix is valid");
            let domain =
                pioneer_hooks::HookDomain::new(domain).expect("static tool bundle domain is valid");
            let tool_names = bundle
                .specs
                .iter()
                .filter_map(|configured| HookToolName::new(configured.spec.name.clone()).ok())
                .collect::<Vec<_>>();

            Some(AgentTurnToolBundleContribution::new(
                ToolBundleContribution {
                    contribution_id,
                    bundle_id,
                    domain,
                    priority,
                    tool_names,
                    diagnostics: Vec::new(),
                },
                bundle,
            ))
        })
        .collect()
}

fn existing_tool_names_from_local_contributions(
    contributions: &[AgentTurnToolBundleContribution],
) -> Vec<HookToolName> {
    let mut names = contributions
        .iter()
        .flat_map(|contribution| contribution.contribution().tool_names.iter().cloned())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn local_tool_bundle_artifacts_by_id(
    contributions: &[AgentTurnToolBundleContribution],
) -> BTreeMap<HookToolBundleId, ToolExtensionBundle> {
    contributions
        .iter()
        .map(|contribution| {
            (
                contribution.contribution().bundle_id.clone(),
                contribution.bundle().clone(),
            )
        })
        .collect()
}

fn missing_tool_bundle_artifact_diagnostic(bundle_id: &str) -> HookDiagnostic {
    let mut metadata = pioneer_hooks::HookMetadata::default();
    metadata.insert(
        pioneer_hooks::HookMetadataKey::new("bundle_id").expect("static metadata key is valid"),
        pioneer_hooks::HookValue::Text(bundle_id.to_owned()),
    );
    HookDiagnostic {
        code: pioneer_hooks::HookDiagnosticCode::new(TOOL_BUNDLE_MISSING_ARTIFACT_DIAGNOSTIC_CODE)
            .expect("static diagnostic code is valid"),
        message: pioneer_hooks::HookDiagnosticMessage::new(
            "tool bundle contribution was ignored because no local bundle artifact was registered",
        )
        .expect("static diagnostic message is valid"),
        severity: pioneer_hooks::HookDiagnosticSeverity::Warning,
        safe_for_user: false,
        metadata,
    }
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
        HookRuntimeError::InvalidExecutionPolicy {
            subscription_id,
            hook_id,
            phase: error_phase,
            reason,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                reason = %reason,
                error_kind = "invalid_execution_policy",
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
        HookRuntimeError::InvalidExecutionPolicy {
            subscription_id,
            hook_id,
            phase: error_phase,
            reason,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                reason = %reason,
                error_kind = "invalid_execution_policy",
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
        HookRuntimeError::InvalidExecutionPolicy {
            subscription_id,
            hook_id,
            phase: error_phase,
            reason,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                reason = %reason,
                error_kind = "invalid_execution_policy",
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
        HookRuntimeError::InvalidExecutionPolicy {
            subscription_id,
            hook_id,
            phase: error_phase,
            reason,
        } => {
            warn!(
                phase = %phase,
                error_phase = %error_phase,
                subscription_id = %subscription_id,
                hook_id = %hook_id,
                reason = %reason,
                error_kind = "invalid_execution_policy",
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
