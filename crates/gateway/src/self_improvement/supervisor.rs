use std::collections::{BTreeMap, HashSet};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use pioneer_config::{GatewaySelfImprovementConfig, GatewaySelfImprovementModelSelectionConfig};
use pioneer_crud::{
    CrudStore, FinalizeSelfImprovementRunInput, FinalizeSelfImprovementRunResult,
    NewSelfImprovementRun, SelfImprovementFinalOutcome, SelfImprovementFinalizationAuthority,
    SelfImprovementFrozenSourceRange, SelfImprovementNoChangeReason, SelfImprovementRunFence,
    SelfImprovementRunMutationResult, SelfImprovementRunRecord,
};
use pioneer_protocol::{ProviderFailureClass, SKILL_ID_LEN, SkillId, generate_id};
use pioneer_provider::ProviderRegistry;
use pioneer_skills::AgentSkillRuntimeEntry;
use pioneer_sqlite::is_anyhow_sqlite_lock;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::resilience::provider_failure_class_name;
use crate::workspace::WorkspaceManager;

use super::analysis_checkpoint::ResumableHistoryAnalysis;
use super::history::{
    CHUNK_ANALYSIS_MAX_TOKEN_UPPER_BOUND, HistoryChunkLimits,
    build_model_safe_full_thread_snapshot, plan_history_chunks,
};
use super::learner::{
    ActiveSkillModelInput, LearnerReviewerClient, MAX_CHUNK_CONTRACT_ATTEMPTS,
    MAX_LIFECYCLE_CONTRACT_ATTEMPTS, MAX_MODEL_INPUT_BYTES, MAX_MODEL_OUTPUT_TOKENS,
    ModelCallResult, ModelCallUsage, ModelContractError, ModelContractErrorKind, ReviewDecision,
    no_candidate_final_outcome, pre_review_skill_candidate_policy, reviewed_skill_final_outcome,
};
use super::settings::{
    AuthoritativeSelfImprovementSettings, resolve_authoritative_settings_for_workspace,
};
use super::validation::AuthorizedAgentSkillTarget;

const PIPELINE_CONTRACT_VERSION: &str = "self-improvement-v3";
const RUN_STATUS_PENDING: &str = "pending";
const RUN_STATUS_RUNNING: &str = "running";
const RUN_STATUS_FAILED: &str = "failed";
const MAX_NEW_SOURCE_TURNS_PER_RUN: u64 = 1_024;
const RUN_LEASE_SECONDS: i64 = 60 * 60;
const PROVIDER_CALL_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const WAKE_WALL_CLOCK_BUDGET: Duration = Duration::from_secs(45 * 60);
const MAX_CHUNK_STEPS_PER_WAKE: u32 = 2;
const MAX_PROVIDER_CALLS_PER_WAKE: u32 = 6;
const MAX_INPUT_TOKENS_PER_WAKE: u64 =
    MAX_PROVIDER_CALLS_PER_WAKE as u64 * MAX_MODEL_INPUT_BYTES as u64;
const MAX_OUTPUT_TOKENS_PER_WAKE: u64 =
    MAX_PROVIDER_CALLS_PER_WAKE as u64 * MAX_MODEL_OUTPUT_TOKENS as u64;
const BUDGET_YIELD_RETRY_SECONDS: i64 = 1;
const OVERDUE_RETRY_POLL_DELAY: Duration = Duration::from_secs(1);
const MAX_CONCURRENT_WORKSPACES: usize = 4;
const MAX_INFRASTRUCTURE_ATTEMPTS: i64 = 3;
const RETRY_BACKOFF_BASE_SECONDS: i64 = 30;
const RETRY_BACKOFF_MAX_SECONDS: i64 = 15 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunFailureDisposition {
    RetryInfrastructure,
    FailWithoutInfrastructureRetry,
    LostAuthority,
}

#[derive(Debug, Clone, Copy)]
struct RunExecutionFailure {
    disposition: RunFailureDisposition,
    error_class: &'static str,
    reason_code: &'static str,
}

impl Display for RunExecutionFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.error_class, self.reason_code)
    }
}

impl StdError for RunExecutionFailure {}

fn run_execution_error(
    disposition: RunFailureDisposition,
    error_class: &'static str,
    reason_code: &'static str,
) -> anyhow::Error {
    anyhow::Error::new(RunExecutionFailure {
        disposition,
        error_class,
        reason_code,
    })
}

struct RunLeaseClock {
    base_unix: i64,
    started_at: Instant,
}

impl RunLeaseClock {
    fn new(base_unix: i64) -> Self {
        Self {
            base_unix,
            started_at: Instant::now(),
        }
    }

    fn now_unix(&self) -> i64 {
        self.base_unix
            .saturating_add(i64::try_from(self.started_at.elapsed().as_secs()).unwrap_or(i64::MAX))
    }
}

#[derive(Debug)]
struct WakeBudget {
    started_at: Instant,
    chunk_steps: u32,
    usage: ModelCallUsage,
}

impl WakeBudget {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            chunk_steps: 0,
            usage: ModelCallUsage::default(),
        }
    }

    fn can_start_chunk(&self) -> bool {
        self.chunk_steps < MAX_CHUNK_STEPS_PER_WAKE
            && self.can_reserve(
                MAX_CHUNK_CONTRACT_ATTEMPTS,
                CHUNK_ANALYSIS_MAX_TOKEN_UPPER_BOUND as u64,
                MAX_MODEL_OUTPUT_TOKENS as u64,
            )
            && self.remaining_wall_clock()
                >= PROVIDER_CALL_TIMEOUT.saturating_mul(MAX_CHUNK_CONTRACT_ATTEMPTS)
    }

    fn can_start_synthesis_and_review(&self) -> bool {
        self.can_reserve(
            MAX_LIFECYCLE_CONTRACT_ATTEMPTS.saturating_mul(2),
            MAX_MODEL_INPUT_BYTES as u64,
            MAX_MODEL_OUTPUT_TOKENS as u64,
        ) && self.remaining_wall_clock()
            >= PROVIDER_CALL_TIMEOUT
                .saturating_mul(MAX_LIFECYCLE_CONTRACT_ATTEMPTS.saturating_mul(2))
    }

    fn record_chunk_step(&mut self, usage: ModelCallUsage) -> Result<()> {
        self.chunk_steps = self.chunk_steps.saturating_add(1);
        self.record_usage(
            usage,
            CHUNK_ANALYSIS_MAX_TOKEN_UPPER_BOUND as u64,
            MAX_MODEL_OUTPUT_TOKENS as u64,
        )
    }

    fn record_general_call(&mut self, usage: ModelCallUsage) -> Result<()> {
        self.record_usage(
            usage,
            MAX_MODEL_INPUT_BYTES as u64,
            MAX_MODEL_OUTPUT_TOKENS as u64,
        )
    }

    fn record_usage(
        &mut self,
        usage: ModelCallUsage,
        input_tokens_per_call: u64,
        output_tokens_per_call: u64,
    ) -> Result<()> {
        let charged_input = usage.input_tokens.unwrap_or_else(|| {
            input_tokens_per_call.saturating_mul(u64::from(usage.provider_calls))
        });
        let charged_output = usage.output_tokens.unwrap_or_else(|| {
            output_tokens_per_call.saturating_mul(u64::from(usage.provider_calls))
        });
        self.usage.provider_calls = self
            .usage
            .provider_calls
            .saturating_add(usage.provider_calls);
        self.usage.input_tokens = Some(
            self.usage
                .input_tokens
                .unwrap_or_default()
                .saturating_add(charged_input),
        );
        self.usage.output_tokens = Some(
            self.usage
                .output_tokens
                .unwrap_or_default()
                .saturating_add(charged_output),
        );
        if self.usage.provider_calls > MAX_PROVIDER_CALLS_PER_WAKE
            || self.usage.input_tokens.unwrap_or(u64::MAX) > MAX_INPUT_TOKENS_PER_WAKE
            || self.usage.output_tokens.unwrap_or(u64::MAX) > MAX_OUTPUT_TOKENS_PER_WAKE
        {
            bail!("self-improvement provider usage exceeded the reserved wake budget");
        }
        Ok(())
    }

    fn can_reserve(
        &self,
        provider_calls: u32,
        input_tokens_per_call: u64,
        output_tokens_per_call: u64,
    ) -> bool {
        self.usage
            .provider_calls
            .checked_add(provider_calls)
            .is_some_and(|calls| calls <= MAX_PROVIDER_CALLS_PER_WAKE)
            && self
                .usage
                .input_tokens
                .unwrap_or_default()
                .checked_add(input_tokens_per_call.saturating_mul(u64::from(provider_calls)))
                .is_some_and(|tokens| tokens <= MAX_INPUT_TOKENS_PER_WAKE)
            && self
                .usage
                .output_tokens
                .unwrap_or_default()
                .checked_add(output_tokens_per_call.saturating_mul(u64::from(provider_calls)))
                .is_some_and(|tokens| tokens <= MAX_OUTPUT_TOKENS_PER_WAKE)
    }

    fn remaining_wall_clock(&self) -> Duration {
        WAKE_WALL_CLOCK_BUDGET.saturating_sub(self.started_at.elapsed())
    }
}

/// Owns the single in-process Proposal 58 self-improvement execution path.
///
/// The supervisor deliberately is not a generic job runner: scheduling,
/// resumable history processing and terminal finalization remain one
/// production `wake_once` path.
pub(crate) struct SelfImprovementSupervisor {
    store: Arc<CrudStore>,
    provider_registry: Arc<ProviderRegistry>,
    workspace_manager: Arc<WorkspaceManager>,
    desired: StdRwLock<BTreeMap<String, GatewaySelfImprovementConfig>>,
    max_skill_markdown_bytes: usize,
    worker_id: String,
    cancellation: CancellationToken,
    execution_cancellations: StdMutex<BTreeMap<String, CancellationToken>>,
    transition_gate: RwLock<()>,
    dispatch_gate: Mutex<()>,
    wake: Notify,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl SelfImprovementSupervisor {
    pub(crate) fn new(
        store: Arc<CrudStore>,
        provider_registry: Arc<ProviderRegistry>,
        workspace_manager: Arc<WorkspaceManager>,
        workspace_configs: BTreeMap<String, GatewaySelfImprovementConfig>,
        max_skill_markdown_bytes: usize,
    ) -> Self {
        Self {
            store,
            provider_registry,
            workspace_manager,
            desired: StdRwLock::new(workspace_configs),
            max_skill_markdown_bytes: max_skill_markdown_bytes.max(1),
            worker_id: format!("gateway-self-improvement-{}", generate_id(SKILL_ID_LEN)),
            cancellation: CancellationToken::new(),
            execution_cancellations: StdMutex::new(BTreeMap::new()),
            transition_gate: RwLock::new(()),
            dispatch_gate: Mutex::new(()),
            wake: Notify::new(),
            task: Mutex::new(None),
        }
    }

    /// Reconciles the durable enabled state before exposing the supervisor to
    /// turns, then starts exactly one tracked task.
    pub(crate) async fn start(self: &Arc<Self>) -> Result<()> {
        {
            // The listener may already be reachable while Gateway finishes
            // startup. Serialize startup reconciliation with both live
            // Settings updates and new-turn overlay materialization so a
            // stale startup snapshot cannot overwrite a newer desired state.
            let _transition = self.transition_gate.write().await;
            self.reconcile_all(Utc::now().timestamp()).await?;
        }

        let mut task = self.task.lock().await;
        if task.is_some() {
            bail!("self-improvement supervisor is already started");
        }
        let supervisor = self.clone();
        *task = Some(tokio::spawn(async move {
            supervisor.run().await;
        }));
        Ok(())
    }

    pub(crate) async fn shutdown(&self) {
        self.cancellation.cancel();
        self.cancel_all_executions();
        if let Some(task) = self.task.lock().await.take() {
            if let Err(error) = task.await {
                error!(error = %error, "self-improvement supervisor task failed to join");
            }
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            if let Err(error) = self.wake_once(Utc::now().timestamp()).await {
                let failure = classify_execution_failure(&error);
                error!(
                    error_class = failure.error_class,
                    reason_code = failure.reason_code,
                    "self-improvement supervisor wake failed"
                );
            }
            let now = Utc::now();
            let retry_at_unix = match self.store.get_next_self_improvement_retry_at().await {
                Ok(retry_at) => retry_at,
                Err(error) => {
                    let failure = classify_execution_failure(&error);
                    warn!(
                        error_class = failure.error_class,
                        reason_code = failure.reason_code,
                        "self-improvement retry timer lookup failed"
                    );
                    None
                }
            };
            let timer = tokio::time::sleep(next_supervisor_delay(now, retry_at_unix));
            tokio::pin!(timer);
            tokio::select! {
                () = self.cancellation.cancelled() => return,
                () = self.wake.notified() => {},
                () = &mut timer => {},
            }
        }
    }

    /// Applies one validated workspace Settings snapshot and waits until that
    /// workspace has reached the matching durable effective state.
    pub(crate) async fn apply_desired_for_workspace(
        &self,
        workspace_id: &str,
        desired: GatewaySelfImprovementConfig,
        now_unix: i64,
    ) -> Result<()> {
        let workspace_id = self
            .workspace_manager
            .validate_workspace_id(workspace_id)
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "self-improvement settings workspace `{workspace_id}` is unavailable: {error}"
                )
            })?;
        let _transition = self.transition_gate.write().await;
        let invalidates_execution = {
            let mut current = self
                .desired
                .write()
                .expect("self-improvement desired settings lock poisoned");
            let previous = current
                .get(workspace_id.as_str())
                .cloned()
                .unwrap_or_default();
            let invalidates_execution = settings_change_invalidates_execution(&previous, &desired);
            current.insert(workspace_id.clone(), desired.clone());
            invalidates_execution
        };
        if invalidates_execution {
            self.cancel_workspace_execution(workspace_id.as_str());
        }

        let reconciliation = self
            .reconcile_workspace_with(workspace_id.as_str(), &desired, true, now_unix)
            .await;
        if invalidates_execution {
            self.replace_workspace_execution_cancellation(workspace_id.as_str());
        }
        reconciliation?;
        self.wake.notify_one();
        Ok(())
    }

    /// Materializes the overlay for a newly starting turn under the same
    /// desired/effective transition gate used by Settings reconciliation.
    pub(crate) async fn load_overlay_for_new_turn(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<AgentSkillRuntimeEntry>> {
        let _transition = self.transition_gate.read().await;
        self.workspace_manager
            .validate_workspace_id(workspace_id)
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "self-improvement overlay workspace `{workspace_id}` is unavailable: {error}"
                )
            })?;
        let desired = self.desired_settings_for_workspace(workspace_id);
        let authoritative = self.authoritative_settings_from(&desired, Some(workspace_id));
        if !authoritative.effective_enabled {
            return Ok(Vec::new());
        }

        self.store
            .activate_self_improvement_workspace(workspace_id, Utc::now().timestamp())
            .await
            .with_context(|| {
                format!(
                    "failed to reconcile self-improvement before overlay for workspace \
                     `{workspace_id}`"
                )
            })?;
        super::overlay::load_active_agent_skill_overlay(self.store.as_ref(), workspace_id).await
    }

    /// Executes the real production wake. Tests call this method only to
    /// control time; no alternate orchestration exists.
    pub(crate) async fn wake_once(&self, now_unix: i64) -> Result<()> {
        let _dispatch = self.dispatch_gate.lock().await;
        let workspaces = {
            let _transition = self.transition_gate.read().await;
            self.reconcile_all(now_unix).await?
        };
        let mut executions = stream::iter(workspaces.into_iter().map(|workspace_id| {
            let execution_cancellation =
                self.workspace_execution_cancellation(workspace_id.as_str());
            async move {
                let result = tokio::select! {
                    biased;
                    () = self.cancellation.cancelled() => Ok(()),
                    () = execution_cancellation.cancelled() => Ok(()),
                    result = self.process_workspace(workspace_id.as_str(), now_unix) => result,
                };
                (workspace_id, result)
            }
        }))
        .buffer_unordered(MAX_CONCURRENT_WORKSPACES);
        while let Some((workspace_id, result)) = executions.next().await {
            if let Err(error) = result {
                let failure = classify_execution_failure(&error);
                warn!(
                    workspace_id,
                    error_class = failure.error_class,
                    reason_code = failure.reason_code,
                    "self-improvement workspace wake failed"
                );
            }
            if self.cancellation.is_cancelled() {
                break;
            }
        }
        Ok(())
    }

    async fn reconcile_all(&self, now_unix: i64) -> Result<Vec<String>> {
        let desired = self.desired_settings();
        self.reconcile_all_with(&desired, now_unix).await
    }

    async fn reconcile_all_with(
        &self,
        desired: &BTreeMap<String, GatewaySelfImprovementConfig>,
        now_unix: i64,
    ) -> Result<Vec<String>> {
        let workspaces = self
            .workspace_manager
            .list_workspaces()
            .await
            .map_err(|error| {
                anyhow::anyhow!("failed to list self-improvement workspaces: {error}")
            })?;
        let mut runnable = Vec::new();
        for workspace in workspaces {
            let workspace_desired = desired
                .get(workspace.id.as_str())
                .cloned()
                .unwrap_or_default();
            if self
                .reconcile_workspace_with(
                    workspace.id.as_str(),
                    &workspace_desired,
                    workspace.is_active,
                    now_unix,
                )
                .await?
            {
                runnable.push(workspace.id);
            }
        }
        Ok(runnable)
    }

    async fn reconcile_workspace_with(
        &self,
        workspace_id: &str,
        desired: &GatewaySelfImprovementConfig,
        workspace_is_active: bool,
        now_unix: i64,
    ) -> Result<bool> {
        let authoritative = self.authoritative_settings_from(desired, Some(workspace_id));
        if authoritative.effective_enabled && workspace_is_active {
            let state = self
                .store
                .activate_self_improvement_workspace(workspace_id, now_unix)
                .await
                .with_context(|| {
                    format!(
                        "failed to reconcile enabled self-improvement workspace `{workspace_id}`"
                    )
                })?;
            self.reconcile_unfinished_authority(workspace_id, &state, &authoritative, now_unix)
                .await?;
            return Ok(true);
        }

        self.store
            .deactivate_self_improvement_workspace(workspace_id, now_unix)
            .await
            .with_context(|| {
                format!("failed to reconcile disabled self-improvement workspace `{workspace_id}`")
            })?;
        Ok(false)
    }

    async fn reconcile_unfinished_authority(
        &self,
        workspace_id: &str,
        state: &pioneer_crud::SelfImprovementWorkspaceStateRecord,
        authoritative: &AuthoritativeSelfImprovementSettings,
        now_unix: i64,
    ) -> Result<()> {
        let Some(run) = self
            .store
            .get_oldest_unresolved_self_improvement_run(workspace_id, state.activation_epoch)
            .await?
        else {
            return Ok(());
        };
        if run.source_lower_exclusive != state.cursor_source_id {
            bail!(
                "oldest self-improvement run `{}` no longer starts at workspace `{workspace_id}` \
                 cursor during authority reconciliation",
                run.id
            );
        }
        let default_model = authoritative
            .default_model
            .as_ref()
            .context("effective self-improvement config lost its default model")?;
        let reviewer_model = authoritative
            .reviewer_model
            .as_ref()
            .context("effective self-improvement config lost its reviewer model")?;
        let authority = SelfImprovementFinalizationAuthority {
            effective_enabled: true,
            learner_provider: default_model.provider.clone(),
            learner_model: default_model.model.clone(),
            reviewer_provider: reviewer_model.provider.clone(),
            reviewer_model: reviewer_model.model.clone(),
            pipeline_contract_version: PIPELINE_CONTRACT_VERSION.to_owned(),
        };
        if run.learner_provider == authority.learner_provider
            && run.learner_model == authority.learner_model
            && run.reviewer_provider == authority.reviewer_provider
            && run.reviewer_model == authority.reviewer_model
            && run.pipeline_contract_version == authority.pipeline_contract_version
        {
            return Ok(());
        }

        match self
            .store
            .reset_unfinished_self_improvement_run_authority(&run, &authority, now_unix)
            .await?
        {
            SelfImprovementRunMutationResult::Applied => Ok(()),
            SelfImprovementRunMutationResult::LostAuthority => {
                let current = self
                    .store
                    .get_self_improvement_run(workspace_id, run.id.as_str())
                    .await?;
                if current.as_ref().is_none_or(|current| {
                    matches!(current.status.as_str(), "completed" | "cancelled")
                        || (current.learner_provider == authority.learner_provider
                            && current.learner_model == authority.learner_model
                            && current.reviewer_provider == authority.reviewer_provider
                            && current.reviewer_model == authority.reviewer_model
                            && current.pipeline_contract_version
                                == authority.pipeline_contract_version)
                }) {
                    return Ok(());
                }
                bail!(
                    "self-improvement run `{}` authority changed concurrently and remains stale",
                    run.id
                )
            }
        }
    }

    fn desired_settings(&self) -> BTreeMap<String, GatewaySelfImprovementConfig> {
        self.desired
            .read()
            .expect("self-improvement desired settings lock poisoned")
            .clone()
    }

    fn desired_settings_for_workspace(&self, workspace_id: &str) -> GatewaySelfImprovementConfig {
        self.desired
            .read()
            .expect("self-improvement desired settings lock poisoned")
            .get(workspace_id)
            .cloned()
            .unwrap_or_default()
    }

    fn authoritative_settings(
        &self,
        workspace_id: Option<&str>,
    ) -> AuthoritativeSelfImprovementSettings {
        let desired = workspace_id
            .map(|workspace_id| self.desired_settings_for_workspace(workspace_id))
            .unwrap_or_default();
        self.authoritative_settings_from(&desired, workspace_id)
    }

    fn authoritative_settings_from(
        &self,
        desired: &GatewaySelfImprovementConfig,
        workspace_id: Option<&str>,
    ) -> AuthoritativeSelfImprovementSettings {
        resolve_authoritative_settings_for_workspace(
            desired,
            self.provider_registry.as_ref(),
            workspace_id,
        )
    }

    fn cancel_all_executions(&self) {
        for cancellation in self
            .execution_cancellations
            .lock()
            .expect("self-improvement execution cancellations lock poisoned")
            .values()
        {
            cancellation.cancel();
        }
    }

    fn cancel_workspace_execution(&self, workspace_id: &str) {
        if let Some(cancellation) = self
            .execution_cancellations
            .lock()
            .expect("self-improvement execution cancellations lock poisoned")
            .get(workspace_id)
        {
            cancellation.cancel();
        }
    }

    fn replace_workspace_execution_cancellation(&self, workspace_id: &str) {
        self.execution_cancellations
            .lock()
            .expect("self-improvement execution cancellations lock poisoned")
            .insert(workspace_id.to_owned(), CancellationToken::new());
    }

    fn workspace_execution_cancellation(&self, workspace_id: &str) -> CancellationToken {
        self.execution_cancellations
            .lock()
            .expect("self-improvement execution cancellations lock poisoned")
            .entry(workspace_id.to_owned())
            .or_default()
            .clone()
    }

    async fn process_workspace(&self, workspace_id: &str, now_unix: i64) -> Result<()> {
        let state = self
            .store
            .get_self_improvement_workspace_state(workspace_id)
            .await?
            .with_context(|| {
                format!("self-improvement state is missing for workspace `{workspace_id}`")
            })?;
        let effective_enabled_at = match state.effective_enabled_at_unix {
            Some(value) => value,
            None => return Ok(()),
        };

        if let Some(oldest) = self
            .store
            .get_oldest_unresolved_self_improvement_run(workspace_id, state.activation_epoch)
            .await?
        {
            if oldest.source_lower_exclusive != state.cursor_source_id {
                bail!(
                    "oldest self-improvement run `{}` no longer starts at workspace `{}` cursor",
                    oldest.id,
                    workspace_id
                );
            }
            return self
                .process_available_run(oldest, effective_enabled_at, now_unix)
                .await;
        }

        let selected_sources = self
            .store
            .list_self_improvement_source_turns_after(
                workspace_id,
                state.cursor_source_id,
                effective_enabled_at,
                MAX_NEW_SOURCE_TURNS_PER_RUN,
            )
            .await?;
        let Some(source_upper_inclusive) = selected_sources.last().map(|source| source.id) else {
            return Ok(());
        };

        let settings = self.authoritative_settings(Some(workspace_id));
        let default_model = settings
            .default_model
            .as_ref()
            .context("effective self-improvement config lost its default model")?;
        let reviewer_model = settings
            .reviewer_model
            .as_ref()
            .context("effective self-improvement config lost its reviewer model")?;
        let scheduled_date_utc = DateTime::<Utc>::from_timestamp(now_unix, 0)
            .context("self-improvement wake timestamp is outside the supported range")?
            .format("%Y-%m-%d")
            .to_string();
        let run = self
            .store
            .create_or_get_self_improvement_run(
                NewSelfImprovementRun {
                    workspace_id: workspace_id.to_owned(),
                    activation_epoch: state.activation_epoch,
                    scheduled_date_utc,
                    source_lower_exclusive: state.cursor_source_id,
                    source_upper_inclusive,
                    learner_provider: default_model.provider.clone(),
                    learner_model: default_model.model.clone(),
                    reviewer_provider: reviewer_model.provider.clone(),
                    reviewer_model: reviewer_model.model.clone(),
                    pipeline_contract_version: PIPELINE_CONTRACT_VERSION.to_owned(),
                },
                now_unix,
            )
            .await?;
        if run.status != RUN_STATUS_PENDING {
            return Ok(());
        }
        self.claim_and_execute(run, effective_enabled_at, now_unix)
            .await
    }

    async fn process_available_run(
        &self,
        mut run: SelfImprovementRunRecord,
        effective_enabled_at: i64,
        now_unix: i64,
    ) -> Result<()> {
        if run.status == RUN_STATUS_FAILED {
            if !is_later_utc_date(run.updated_at_unix, now_unix)? {
                return Ok(());
            }
            match self
                .store
                .requeue_failed_self_improvement_run(&run, now_unix)
                .await?
            {
                SelfImprovementRunMutationResult::Applied => {
                    run = self
                        .store
                        .get_self_improvement_run(run.workspace_id.as_str(), run.id.as_str())
                        .await?
                        .context("requeued self-improvement run disappeared")?;
                }
                SelfImprovementRunMutationResult::LostAuthority => return Ok(()),
            }
        }
        if run.status == RUN_STATUS_PENDING
            && run
                .next_attempt_at_unix
                .is_some_and(|retry_at| retry_at > now_unix)
        {
            return Ok(());
        }
        if run.status == RUN_STATUS_RUNNING
            && run
                .lease_expires_at_unix
                .is_some_and(|lease_expires_at| lease_expires_at > now_unix)
        {
            return Ok(());
        }
        if run.status != RUN_STATUS_PENDING && run.status != RUN_STATUS_RUNNING {
            return Ok(());
        }

        self.claim_and_execute(run, effective_enabled_at, now_unix)
            .await
    }

    async fn load_frozen_source_range(
        &self,
        run: &SelfImprovementRunRecord,
        effective_enabled_at: i64,
    ) -> Result<SelfImprovementFrozenSourceRange> {
        let anchors = self
            .store
            .list_frozen_self_improvement_source_range(
                run.workspace_id.as_str(),
                run.source_lower_exclusive,
                run.source_upper_inclusive,
                effective_enabled_at,
            )
            .await?;
        SelfImprovementFrozenSourceRange::new(
            run.workspace_id.clone(),
            run.source_lower_exclusive,
            run.source_upper_inclusive,
            anchors,
        )
    }

    async fn claim_and_execute(
        &self,
        run: SelfImprovementRunRecord,
        effective_enabled_at: i64,
        now_unix: i64,
    ) -> Result<()> {
        let Some(claimed) = self
            .store
            .claim_available_self_improvement_run(
                run.workspace_id.as_str(),
                run.id.as_str(),
                run.activation_epoch,
                self.worker_id.as_str(),
                now_unix,
                now_unix.saturating_add(RUN_LEASE_SECONDS),
            )
            .await?
        else {
            return Ok(());
        };
        let started_at = Instant::now();
        let execution = async {
            let frozen_range = self
                .load_frozen_source_range(&claimed, effective_enabled_at)
                .await?;
            self.execute_claimed_run(claimed.clone(), frozen_range, now_unix)
                .await
        }
        .await;
        match execution {
            Ok(()) => {
                info!(
                    run_id = %claimed.id,
                    workspace_id = %claimed.workspace_id,
                    source_lower_exclusive = claimed.source_lower_exclusive,
                    source_upper_inclusive = claimed.source_upper_inclusive,
                    learner_provider = %claimed.learner_provider,
                    learner_model = %claimed.learner_model,
                    reviewer_provider = %claimed.reviewer_provider,
                    reviewer_model = %claimed.reviewer_model,
                    attempt = claimed.attempt_count,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "self-improvement run attempt finished"
                );
                Ok(())
            }
            Err(error) => {
                self.handle_execution_failure(&claimed, error, now_unix, started_at.elapsed())
                    .await
            }
        }
    }

    async fn handle_execution_failure(
        &self,
        run: &SelfImprovementRunRecord,
        error: anyhow::Error,
        base_now_unix: i64,
        elapsed: Duration,
    ) -> Result<()> {
        let failure = classify_execution_failure(&error);
        let now_unix =
            base_now_unix.saturating_add(i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX));
        warn!(
            run_id = %run.id,
            workspace_id = %run.workspace_id,
            source_lower_exclusive = run.source_lower_exclusive,
            source_upper_inclusive = run.source_upper_inclusive,
            learner_provider = %run.learner_provider,
            learner_model = %run.learner_model,
            reviewer_provider = %run.reviewer_provider,
            reviewer_model = %run.reviewer_model,
            attempt = run.attempt_count,
            elapsed_ms = elapsed.as_millis(),
            error_class = failure.error_class,
            reason_code = failure.reason_code,
            "self-improvement run attempt stopped"
        );
        if failure.disposition == RunFailureDisposition::LostAuthority {
            return Ok(());
        }
        let fence = run
            .fence()
            .with_context(|| format!("claimed run `{}` lost its execution fence", run.id))?;
        let safe_error = format!("{}:{}", failure.error_class, failure.reason_code);
        let mutation = if failure.disposition == RunFailureDisposition::RetryInfrastructure
            && run.attempt_count < MAX_INFRASTRUCTURE_ATTEMPTS
        {
            let retry_at = now_unix.saturating_add(retry_backoff_seconds(run.attempt_count));
            self.store
                .return_self_improvement_run_to_pending(
                    &fence,
                    now_unix,
                    retry_at,
                    safe_error.as_str(),
                )
                .await?
        } else {
            self.store
                .fail_claimed_self_improvement_run(&fence, now_unix, safe_error.as_str())
                .await?
        };
        match mutation {
            SelfImprovementRunMutationResult::Applied => Ok(()),
            SelfImprovementRunMutationResult::LostAuthority => Ok(()),
        }
    }

    async fn execute_claimed_run(
        &self,
        run: SelfImprovementRunRecord,
        frozen_range: SelfImprovementFrozenSourceRange,
        now_unix: i64,
    ) -> Result<()> {
        let mut wake_budget = WakeBudget::new();
        let fence = run
            .fence()
            .with_context(|| format!("claimed run `{}` has no execution fence", run.id))?;
        let lease_clock = RunLeaseClock::new(now_unix);
        let canonical = self
            .store
            .list_canonical_turn_events_for_self_improvement(&frozen_range)
            .await?;
        let snapshot = build_model_safe_full_thread_snapshot(&frozen_range, canonical.as_slice())?;
        let chunks = plan_history_chunks(&snapshot, HistoryChunkLimits::default())?;
        let mut analysis = ResumableHistoryAnalysis::restore(&run, chunks.as_slice())?;
        info!(
            run_id = %run.id,
            workspace_id = %run.workspace_id,
            source_lower_exclusive = run.source_lower_exclusive,
            source_upper_inclusive = run.source_upper_inclusive,
            anchor_count = frozen_range.anchors.len(),
            thread_count = snapshot.threads.len(),
            chunk_count = chunks.len(),
            "self-improvement frozen history plan loaded"
        );

        let default_model = GatewaySelfImprovementModelSelectionConfig {
            provider: run.learner_provider.clone(),
            model: run.learner_model.clone(),
        };
        let reviewer_model = GatewaySelfImprovementModelSelectionConfig {
            provider: run.reviewer_provider.clone(),
            model: run.reviewer_model.clone(),
        };
        let client = LearnerReviewerClient::new(
            self.provider_registry.as_ref(),
            run.workspace_id.as_str(),
            &default_model,
            Some(&reviewer_model),
        );

        while !analysis.is_complete() {
            if !wake_budget.can_start_chunk() {
                return self.yield_after_wake_budget(&fence, &lease_clock).await;
            }
            let chunk_index = usize::try_from(analysis.next_chunk_index())
                .context("analysis cursor exceeds platform usize")?;
            let chunk = chunks
                .get(chunk_index)
                .context("analysis cursor references an unknown history chunk")?;
            let result = self
                .provider_call_with_lease(
                    &fence,
                    &lease_clock,
                    "chunk analysis",
                    client.analyze_chunk(chunk, analysis.validated_digest()),
                )
                .await?;
            match result {
                Ok(result) => {
                    log_model_usage(&run, "chunk_analysis", result.usage);
                    wake_budget.record_chunk_step(result.usage)?;
                    analysis.record_validated(chunk, result.value)?;
                }
                Err(error) if is_chunk_contract_exhaustion(&error) => {
                    log_model_usage(&run, "chunk_analysis", error.usage);
                    wake_budget.record_chunk_step(error.usage)?;
                    analysis.record_contract_rejected(chunk, error.reason_code)?;
                    info!(
                        run_id = %run.id,
                        workspace_id = %run.workspace_id,
                        chunk_index = chunk.chunk_index,
                        chunk_fingerprint = %chunk.fingerprint,
                        reason_code = error.reason_code,
                        "self-improvement chunk contract exhausted"
                    );
                }
                Err(error) => {
                    log_model_usage(&run, "chunk_analysis", error.usage);
                    wake_budget.record_chunk_step(error.usage)?;
                    return Err(model_contract_error(error));
                }
            }
            let (cursor, digest) = analysis.encode()?;
            match self
                .store
                .save_self_improvement_run_checkpoint(
                    &fence,
                    cursor.as_str(),
                    digest.as_str(),
                    lease_clock.now_unix(),
                )
                .await?
            {
                SelfImprovementRunMutationResult::Applied => {}
                SelfImprovementRunMutationResult::LostAuthority => {
                    return Err(run_execution_error(
                        RunFailureDisposition::LostAuthority,
                        "lost_authority",
                        "analysis_checkpoint_fence_changed",
                    ));
                }
            }
        }

        let Some(digest) = analysis.validated_digest().cloned() else {
            return self
                .finalize(&run, no_candidate_final_outcome(), lease_clock.now_unix())
                .await;
        };
        if digest.observations.is_empty() {
            return self
                .finalize(&run, no_candidate_final_outcome(), lease_clock.now_unix())
                .await;
        }
        if !wake_budget.can_start_synthesis_and_review() {
            return self.yield_after_wake_budget(&fence, &lease_clock).await;
        }

        let active = self
            .store
            .list_active_agent_skill_versions(run.workspace_id.as_str())
            .await?;
        let mut authorized_targets = Vec::with_capacity(active.len());
        for snapshot in &active {
            let rollback_parent = match snapshot.version.parent_version_id.as_deref() {
                Some(parent_version_id) => {
                    let parent = self
                        .store
                        .get_agent_skill_version(run.workspace_id.as_str(), parent_version_id)
                        .await?
                        .with_context(|| {
                            format!(
                                "active Agent skill `{}` references missing rollback parent \
                                 `{parent_version_id}`",
                                snapshot.skill_id
                            )
                        })?;
                    if parent.skill_id != snapshot.skill_id
                        || parent.workspace_id != snapshot.workspace_id
                    {
                        bail!(
                            "active Agent skill `{}` has a cross-skill or cross-workspace parent",
                            snapshot.skill_id
                        );
                    }
                    Some(parent)
                }
                None => None,
            };
            authorized_targets.push(AuthorizedAgentSkillTarget {
                active: snapshot.clone(),
                rollback_parent,
                next_version_number: self
                    .store
                    .get_next_agent_skill_version_number(
                        run.workspace_id.as_str(),
                        &snapshot.skill_id,
                    )
                    .await?
                    .with_context(|| {
                        format!(
                            "active Agent skill `{}` disappeared while authorizing its update",
                            snapshot.skill_id
                        )
                    })?,
            });
        }
        let active_inputs = active
            .iter()
            .map(|snapshot| ActiveSkillModelInput {
                skill_id: snapshot.skill_id.to_string(),
                version_id: snapshot.version.id.clone(),
                rollback_parent_version_id: snapshot.version.parent_version_id.clone(),
                slug: snapshot.slug.clone(),
                display_name: snapshot.version.display_name.clone(),
                when_to_use: snapshot.version.when_to_use.clone(),
                when_not_to_use: snapshot.version.when_not_to_use.clone(),
                instruction_body: snapshot.version.instruction_body.clone(),
            })
            .collect::<Vec<_>>();
        let existing_agent_entries = active
            .iter()
            .cloned()
            .map(super::overlay::agent_skill_runtime_entry)
            .collect::<Vec<_>>();
        let existing_fingerprints = self
            .store
            .list_agent_skill_version_fingerprints(run.workspace_id.as_str())
            .await?
            .into_iter()
            .collect::<HashSet<_>>();
        let prospective_skill_id = SkillId::new(generate_id(SKILL_ID_LEN))
            .context("generated an invalid Agent skill ID")?;
        let prospective_version_id = generate_id(SKILL_ID_LEN);
        let candidate = match self
            .lifecycle_contract_call_with_lease(&fence, &lease_clock, "synthesis", || {
                client.synthesize_validated_candidate(
                    &digest,
                    &frozen_range,
                    chunks.as_slice(),
                    authorized_targets.as_slice(),
                    active_inputs.as_slice(),
                    self.max_skill_markdown_bytes,
                )
            })
            .await?
        {
            Ok(candidate) => {
                log_model_usage(&run, "synthesis", candidate.usage);
                wake_budget.record_general_call(candidate.usage)?;
                info!(
                    run_id = %run.id,
                    workspace_id = %run.workspace_id,
                    candidate_present = candidate.value.is_some(),
                    "self-improvement synthesis completed"
                );
                candidate.value
            }
            Err(error) if is_lifecycle_host_rejection(&error) => {
                log_model_usage(&run, "synthesis", error.usage);
                wake_budget.record_general_call(error.usage)?;
                info!(
                    run_id = %run.id,
                    workspace_id = %run.workspace_id,
                    candidate_present = error.kind
                        == ModelContractErrorKind::HostValidationRejected,
                    reason_code = error.reason_code,
                    "self-improvement synthesis rejected by host"
                );
                return self
                    .finalize(
                        &run,
                        SelfImprovementFinalOutcome::NoChange {
                            reason: SelfImprovementNoChangeReason::HostValidationRejected,
                            reason_codes: vec![error.reason_code.to_owned()],
                        },
                        lease_clock.now_unix(),
                    )
                    .await;
            }
            Err(error) if is_retryable_lifecycle_contract_error(&error) => {
                log_model_usage(&run, "synthesis", error.usage);
                wake_budget.record_general_call(error.usage)?;
                info!(
                    run_id = %run.id,
                    workspace_id = %run.workspace_id,
                    candidate_present = false,
                    reason_code = error.reason_code,
                    "self-improvement synthesis contract exhausted"
                );
                return self
                    .finalize(
                        &run,
                        SelfImprovementFinalOutcome::NoChange {
                            reason: SelfImprovementNoChangeReason::ModelContractRejected,
                            reason_codes: vec![error.reason_code.to_owned()],
                        },
                        lease_clock.now_unix(),
                    )
                    .await;
            }
            Err(error) => {
                log_model_usage(&run, "synthesis", error.usage);
                wake_budget.record_general_call(error.usage)?;
                return Err(model_contract_error(error));
            }
        };
        let outcome = match candidate {
            None => no_candidate_final_outcome(),
            Some(candidate) => {
                if let Some(outcome) = pre_review_skill_candidate_policy(
                    &candidate,
                    &prospective_skill_id,
                    prospective_version_id.as_str(),
                    existing_agent_entries.as_slice(),
                    &existing_fingerprints,
                ) {
                    return self.finalize(&run, outcome, lease_clock.now_unix()).await;
                }
                let reviewed = match self
                    .lifecycle_contract_call_with_lease(&fence, &lease_clock, "review", || {
                        client.review_skill_candidate(
                            candidate.clone(),
                            self.max_skill_markdown_bytes,
                        )
                    })
                    .await?
                {
                    Ok(reviewed) => {
                        log_model_usage(&run, "review", reviewed.usage);
                        wake_budget.record_general_call(reviewed.usage)?;
                        info!(
                            run_id = %run.id,
                            workspace_id = %run.workspace_id,
                            reviewer_decision = review_decision_name(reviewed.value.decision),
                            reason_codes = ?reviewed.value.reason_codes,
                            "self-improvement review completed"
                        );
                        reviewed
                    }
                    Err(error) if is_lifecycle_host_rejection(&error) => {
                        log_model_usage(&run, "review", error.usage);
                        wake_budget.record_general_call(error.usage)?;
                        return self
                            .finalize(
                                &run,
                                SelfImprovementFinalOutcome::NoChange {
                                    reason: SelfImprovementNoChangeReason::HostValidationRejected,
                                    reason_codes: vec![error.reason_code.to_owned()],
                                },
                                lease_clock.now_unix(),
                            )
                            .await;
                    }
                    Err(error) if is_retryable_lifecycle_contract_error(&error) => {
                        log_model_usage(&run, "review", error.usage);
                        wake_budget.record_general_call(error.usage)?;
                        info!(
                            run_id = %run.id,
                            workspace_id = %run.workspace_id,
                            reason_code = error.reason_code,
                            "self-improvement review contract exhausted"
                        );
                        return self
                            .finalize(
                                &run,
                                SelfImprovementFinalOutcome::NoChange {
                                    reason: SelfImprovementNoChangeReason::ModelContractRejected,
                                    reason_codes: vec![error.reason_code.to_owned()],
                                },
                                lease_clock.now_unix(),
                            )
                            .await;
                    }
                    Err(error) => {
                        log_model_usage(&run, "review", error.usage);
                        wake_budget.record_general_call(error.usage)?;
                        return Err(model_contract_error(error));
                    }
                };
                reviewed_skill_final_outcome(
                    reviewed.value,
                    prospective_skill_id,
                    prospective_version_id,
                    existing_agent_entries.as_slice(),
                    &existing_fingerprints,
                    self.max_skill_markdown_bytes,
                )
            }
        };
        self.refresh_lease(&fence, lease_clock.now_unix()).await?;
        self.finalize(&run, outcome, lease_clock.now_unix()).await
    }

    async fn yield_after_wake_budget(
        &self,
        fence: &SelfImprovementRunFence,
        lease_clock: &RunLeaseClock,
    ) -> Result<()> {
        let now_unix = lease_clock.now_unix();
        match self
            .store
            .yield_self_improvement_run_after_budget(
                fence,
                now_unix,
                now_unix.saturating_add(BUDGET_YIELD_RETRY_SECONDS),
            )
            .await?
        {
            SelfImprovementRunMutationResult::Applied
            | SelfImprovementRunMutationResult::LostAuthority => Ok(()),
        }
    }

    async fn provider_call_with_lease<T, E, F>(
        &self,
        fence: &SelfImprovementRunFence,
        lease_clock: &RunLeaseClock,
        stage: &str,
        call: F,
    ) -> Result<std::result::Result<T, E>>
    where
        F: Future<Output = std::result::Result<T, E>>,
    {
        self.refresh_lease(fence, lease_clock.now_unix()).await?;
        let output = tokio::time::timeout(PROVIDER_CALL_TIMEOUT, call)
            .await
            .map_err(|_| {
                run_execution_error(
                    RunFailureDisposition::RetryInfrastructure,
                    "provider_timeout",
                    provider_stage_reason_code(stage),
                )
            })?;
        self.refresh_lease(fence, lease_clock.now_unix()).await?;
        Ok(output)
    }

    async fn lifecycle_contract_call_with_lease<T, F, Fut>(
        &self,
        fence: &SelfImprovementRunFence,
        lease_clock: &RunLeaseClock,
        stage: &str,
        mut call: F,
    ) -> Result<std::result::Result<ModelCallResult<T>, ModelContractError>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<ModelCallResult<T>, ModelContractError>>,
    {
        let mut usage = ModelCallUsage::default();
        let mut last_contract_error = None;
        for _attempt in 1..=MAX_LIFECYCLE_CONTRACT_ATTEMPTS {
            match self
                .provider_call_with_lease(fence, lease_clock, stage, call())
                .await?
            {
                Ok(mut result) => {
                    usage.accumulate(result.usage);
                    result.usage = usage;
                    return Ok(Ok(result));
                }
                Err(mut error) if is_retryable_lifecycle_contract_error(&error) => {
                    usage.accumulate(error.usage);
                    error.usage = usage;
                    last_contract_error = Some(error);
                }
                Err(mut error) => {
                    usage.accumulate(error.usage);
                    error.usage = usage;
                    return Ok(Err(error));
                }
            }
        }
        Ok(Err(last_contract_error.expect(
            "positive lifecycle contract attempt limit must produce a final error",
        )))
    }

    async fn refresh_lease(&self, fence: &SelfImprovementRunFence, now_unix: i64) -> Result<()> {
        match self
            .store
            .heartbeat_self_improvement_run(
                fence,
                now_unix,
                now_unix.saturating_add(RUN_LEASE_SECONDS),
            )
            .await?
        {
            SelfImprovementRunMutationResult::Applied => Ok(()),
            SelfImprovementRunMutationResult::LostAuthority => Err(run_execution_error(
                RunFailureDisposition::LostAuthority,
                "lost_authority",
                "lease_fence_changed",
            )),
        }
    }

    async fn finalize(
        &self,
        run: &SelfImprovementRunRecord,
        outcome: SelfImprovementFinalOutcome,
        now_unix: i64,
    ) -> Result<()> {
        // Linearize the authoritative Settings snapshot and terminal
        // transaction against disable/model-change reconciliation. Provider
        // calls deliberately never hold this gate.
        let _transition = self.transition_gate.read().await;
        let fence = run
            .fence()
            .with_context(|| format!("claimed run `{}` has no execution fence", run.id))?;
        let (requested_action, requested_reason, reason_codes) = match &outcome {
            SelfImprovementFinalOutcome::AcceptedCreate(_) => (Some("create"), None, Vec::new()),
            SelfImprovementFinalOutcome::AcceptedUpdate(_) => (Some("update"), None, Vec::new()),
            SelfImprovementFinalOutcome::AcceptedRollback(_) => {
                (Some("rollback"), None, Vec::new())
            }
            SelfImprovementFinalOutcome::NoChange {
                reason,
                reason_codes,
            } => (None, Some(reason.as_str()), reason_codes.clone()),
        };
        let settings = self.authoritative_settings(Some(run.workspace_id.as_str()));
        let current_default = settings.default_model.as_ref();
        let current_reviewer = settings.reviewer_model.as_ref();
        let authority = SelfImprovementFinalizationAuthority {
            effective_enabled: settings.effective_enabled,
            learner_provider: current_default
                .map(|model| model.provider.clone())
                .unwrap_or_default(),
            learner_model: current_default
                .map(|model| model.model.clone())
                .unwrap_or_default(),
            reviewer_provider: current_reviewer
                .map(|model| model.provider.clone())
                .unwrap_or_default(),
            reviewer_model: current_reviewer
                .map(|model| model.model.clone())
                .unwrap_or_default(),
            pipeline_contract_version: PIPELINE_CONTRACT_VERSION.to_owned(),
        };
        let result = self
            .store
            .finalize_self_improvement_run(
                FinalizeSelfImprovementRunInput {
                    fence,
                    authority,
                    outcome,
                },
                now_unix,
            )
            .await?;
        match result {
            FinalizeSelfImprovementRunResult::Applied { .. } => {
                info!(
                    run_id = %run.id,
                    workspace_id = %run.workspace_id,
                    terminal_outcome = "applied",
                    applied_action = ?requested_action,
                    reason_codes = ?reason_codes,
                    "self-improvement run finalized"
                );
                Ok(())
            }
            FinalizeSelfImprovementRunResult::NoChange { reason } => {
                info!(
                    run_id = %run.id,
                    workspace_id = %run.workspace_id,
                    terminal_outcome = "no_change",
                    no_change_reason = reason.as_str(),
                    requested_reason = ?requested_reason,
                    reason_codes = ?reason_codes,
                    "self-improvement run finalized"
                );
                Ok(())
            }
            FinalizeSelfImprovementRunResult::AlreadyFinalized => {
                info!(
                    run_id = %run.id,
                    workspace_id = %run.workspace_id,
                    terminal_outcome = "already_finalized",
                    applied_action = ?requested_action,
                    requested_reason = ?requested_reason,
                    reason_codes = ?reason_codes,
                    "self-improvement finalization replay matched"
                );
                Ok(())
            }
            FinalizeSelfImprovementRunResult::Stale => Err(run_execution_error(
                RunFailureDisposition::LostAuthority,
                "lost_authority",
                "terminal_fence_changed",
            )),
            FinalizeSelfImprovementRunResult::Conflict(_) => Err(run_execution_error(
                RunFailureDisposition::FailWithoutInfrastructureRetry,
                "finalization_conflict",
                "terminal_identity_conflict",
            )),
        }
    }
}

fn log_model_usage(run: &SelfImprovementRunRecord, stage: &'static str, usage: ModelCallUsage) {
    info!(
        run_id = %run.id,
        workspace_id = %run.workspace_id,
        stage,
        provider_calls = usage.provider_calls,
        input_tokens = ?usage.input_tokens,
        output_tokens = ?usage.output_tokens,
        "self-improvement model usage"
    );
}

fn review_decision_name(decision: ReviewDecision) -> &'static str {
    match decision {
        ReviewDecision::Accept => "accept",
        ReviewDecision::Reject => "reject",
    }
}

fn model_contract_error(error: ModelContractError) -> anyhow::Error {
    if error.kind == ModelContractErrorKind::Transport {
        let provider_class = error
            .provider_failure_class
            .unwrap_or(ProviderFailureClass::Unknown);
        let disposition = if retryable_provider_failure(provider_class) {
            RunFailureDisposition::RetryInfrastructure
        } else {
            RunFailureDisposition::FailWithoutInfrastructureRetry
        };
        return run_execution_error(
            disposition,
            provider_failure_class_name(provider_class),
            error.reason_code,
        );
    }
    if error.kind == ModelContractErrorKind::ProviderUnavailable {
        return run_execution_error(
            RunFailureDisposition::FailWithoutInfrastructureRetry,
            "provider_unavailable",
            error.reason_code,
        );
    }
    run_execution_error(
        RunFailureDisposition::FailWithoutInfrastructureRetry,
        "model_contract",
        error.reason_code,
    )
}

fn is_chunk_contract_exhaustion(error: &ModelContractError) -> bool {
    error.stage == super::learner::ModelContractStage::ChunkAnalysis
        && matches!(
            error.kind,
            ModelContractErrorKind::OutputTooLarge
                | ModelContractErrorKind::MalformedJson
                | ModelContractErrorKind::ContractRejected
        )
}

fn is_retryable_lifecycle_contract_error(error: &ModelContractError) -> bool {
    matches!(
        error.kind,
        ModelContractErrorKind::OutputTooLarge
            | ModelContractErrorKind::MalformedJson
            | ModelContractErrorKind::ContractRejected
    )
}

fn is_lifecycle_host_rejection(error: &ModelContractError) -> bool {
    matches!(
        error.kind,
        ModelContractErrorKind::InputTooLarge | ModelContractErrorKind::HostValidationRejected
    )
}

fn classify_execution_failure(error: &anyhow::Error) -> RunExecutionFailure {
    if let Some(failure) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<RunExecutionFailure>())
    {
        return *failure;
    }
    if is_anyhow_sqlite_lock(error) {
        return RunExecutionFailure {
            disposition: RunFailureDisposition::RetryInfrastructure,
            error_class: "sqlite_busy",
            reason_code: "sqlite_retry_exhausted",
        };
    }
    RunExecutionFailure {
        disposition: RunFailureDisposition::RetryInfrastructure,
        error_class: "infrastructure",
        reason_code: "internal_operation_failed",
    }
}

fn retryable_provider_failure(class: ProviderFailureClass) -> bool {
    matches!(
        class,
        ProviderFailureClass::NetworkTransient
            | ProviderFailureClass::RateLimit
            | ProviderFailureClass::Provider5xx
            | ProviderFailureClass::AuthExpired
            | ProviderFailureClass::StreamStall
            | ProviderFailureClass::StreamTruncated
            | ProviderFailureClass::EmptyResponse
            | ProviderFailureClass::Unknown
    )
}

fn provider_stage_reason_code(stage: &str) -> &'static str {
    match stage {
        "chunk analysis" => "chunk_analysis_timeout",
        "synthesis" => "synthesis_timeout",
        "review" => "review_timeout",
        _ => "provider_call_timeout",
    }
}

fn retry_backoff_seconds(attempt_count: i64) -> i64 {
    let exponent = u32::try_from(attempt_count.saturating_sub(1).clamp(0, 6)).unwrap_or_default();
    RETRY_BACKOFF_BASE_SECONDS
        .saturating_mul(4_i64.saturating_pow(exponent))
        .min(RETRY_BACKOFF_MAX_SECONDS)
}

fn settings_change_invalidates_execution(
    current: &GatewaySelfImprovementConfig,
    desired: &GatewaySelfImprovementConfig,
) -> bool {
    current.enabled != desired.enabled
        || current.default_model != desired.default_model
        || current
            .reviewer_model
            .as_ref()
            .or(current.default_model.as_ref())
            != desired
                .reviewer_model
                .as_ref()
                .or(desired.default_model.as_ref())
}

fn is_later_utc_date(previous_unix: i64, now_unix: i64) -> Result<bool> {
    let previous = DateTime::<Utc>::from_timestamp(previous_unix, 0)
        .context("self-improvement previous timestamp is outside the supported range")?;
    let now = DateTime::<Utc>::from_timestamp(now_unix, 0)
        .context("self-improvement current timestamp is outside the supported range")?;
    Ok(now.date_naive() > previous.date_naive())
}

fn next_supervisor_delay(now: DateTime<Utc>, retry_at_unix: Option<i64>) -> Duration {
    let daily = next_daily_utc_delay(now);
    let Some(retry_at_unix) = retry_at_unix else {
        return daily;
    };
    let retry = if retry_at_unix <= now.timestamp() {
        // An overdue durable row remains the next priority, but a previous
        // wake may have failed before it could claim or reschedule that row.
        // Avoid a zero-delay loop that can otherwise spin until the external
        // failure clears.
        OVERDUE_RETRY_POLL_DELAY
    } else {
        Duration::from_secs(
            u64::try_from(retry_at_unix.saturating_sub(now.timestamp())).unwrap_or(u64::MAX),
        )
    };
    daily.min(retry)
}

fn next_daily_utc_delay(now: DateTime<Utc>) -> Duration {
    let Some(next_date) = now.date_naive().succ_opt() else {
        return Duration::from_secs(24 * 60 * 60);
    };
    let Some(next_midnight) = next_date.and_hms_opt(0, 0, 0) else {
        return Duration::from_secs(24 * 60 * 60);
    };
    (next_midnight.and_utc() - now)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::{Result as AnyhowResult, bail};
    use async_trait::async_trait;
    use futures_util::{StreamExt, stream::BoxStream};
    use migration::{Migrator, MigratorTrait};
    use pioneer_agent::{
        AgentManager, PreflightLoopConfig, SkillsDependenciesLoopConfig, SkillsLoopConfig,
        SkillsRuntimeLoopConfig, SkillsSecurityLoopConfig, SkillsValidationLoopConfig,
        ToolLoopConfig,
    };
    use pioneer_entity::turn;
    use pioneer_memory::hooks::MemoryLoopConfig;
    use pioneer_protocol::{
        AgentDurableEvent, SandboxMode, Thread, ThreadMode, ThreadOriginKind,
        ThreadSidebarVisibility, ThreadStatus, Turn, TurnCompletedNotification,
        TurnExecutionSecuritySnapshot, TurnKind, TurnOrigin, TurnStatus, UserInput,
        default_turn_permission_profile_snapshot,
    };
    use pioneer_provider::{
        ChatRequest, ChatResponse, Provider, ProviderCapabilities, ProviderInputCapabilities,
        ProviderToolCall, Role, StreamChunk,
    };
    use pioneer_skills::{SkillCatalogSnapshot, SkillTrustLevel};
    use pioneer_tools::{
        ComputerUseToolsConfig, ExecutionWindowsConfig, ToolLoopBudgetConfig,
        ToolRetryBudgetConfig, WebToolsConfig,
    };
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, EntityTrait, Statement};
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    use super::super::overlay::load_active_agent_skill_overlay;
    use super::*;

    const WORKSPACE: &str = "ws_self_improvement_e2e";
    const ENABLED_AT: i64 = 1_900_500_000;
    const FIRST_TEXT: &str = "Always verify the checksum before publishing build one.";
    const SECOND_TEXT: &str = "Always verify the checksum before publishing build two.";
    const SKILL_BODY: &str =
        "Before publishing a release, calculate and verify the artifact checksum.";

    trait IntoTestWorkspaceConfigs {
        fn into_test_workspace_configs(self) -> BTreeMap<String, GatewaySelfImprovementConfig>;
    }

    impl IntoTestWorkspaceConfigs for GatewaySelfImprovementConfig {
        fn into_test_workspace_configs(self) -> BTreeMap<String, GatewaySelfImprovementConfig> {
            BTreeMap::from([(WORKSPACE.to_owned(), self)])
        }
    }

    impl IntoTestWorkspaceConfigs for BTreeMap<String, GatewaySelfImprovementConfig> {
        fn into_test_workspace_configs(self) -> BTreeMap<String, GatewaySelfImprovementConfig> {
            self
        }
    }

    fn test_supervisor(
        store: Arc<CrudStore>,
        provider_registry: Arc<ProviderRegistry>,
        workspace_manager: Arc<WorkspaceManager>,
        configs: impl IntoTestWorkspaceConfigs,
        max_skill_markdown_bytes: usize,
    ) -> SelfImprovementSupervisor {
        SelfImprovementSupervisor::new(
            store,
            provider_registry,
            workspace_manager,
            configs.into_test_workspace_configs(),
            max_skill_markdown_bytes,
        )
    }

    struct ScriptedLearningProvider {
        responses: StdMutex<VecDeque<String>>,
        requests: StdMutex<Vec<ChatRequest>>,
    }

    struct BlockingLearningProvider {
        started: Notify,
        requests: StdMutex<Vec<ChatRequest>>,
    }

    struct WorkspaceSelectiveLearningProvider {
        requests: StdMutex<Vec<ChatRequest>>,
    }

    struct ReleasableLearningProvider {
        started: Notify,
        release: Notify,
        requests: StdMutex<Vec<ChatRequest>>,
    }

    struct PinnedOverlayNativeProvider {
        skill_id: SkillId,
        started: Notify,
        release: Notify,
        requests: StdMutex<Vec<ChatRequest>>,
        round: AtomicUsize,
    }

    impl BlockingLearningProvider {
        fn new() -> Self {
            Self {
                started: Notify::new(),
                requests: StdMutex::new(Vec::new()),
            }
        }

        fn request_count(&self) -> usize {
            self.requests
                .lock()
                .expect("blocking learning request lock")
                .len()
        }
    }

    impl WorkspaceSelectiveLearningProvider {
        fn new() -> Self {
            Self {
                requests: StdMutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<ChatRequest> {
            self.requests
                .lock()
                .expect("workspace-selective request lock")
                .clone()
        }
    }

    impl ScriptedLearningProvider {
        fn new() -> Self {
            Self {
                responses: StdMutex::new(VecDeque::new()),
                requests: StdMutex::new(Vec::new()),
            }
        }

        fn set_responses(&self, responses: impl IntoIterator<Item = String>) {
            *self.responses.lock().expect("learning response lock") =
                responses.into_iter().collect();
        }

        fn requests(&self) -> Vec<ChatRequest> {
            self.requests.lock().expect("learning request lock").clone()
        }
    }

    impl ReleasableLearningProvider {
        fn new() -> Self {
            Self {
                started: Notify::new(),
                release: Notify::new(),
                requests: StdMutex::new(Vec::new()),
            }
        }

        fn request_count(&self) -> usize {
            self.requests
                .lock()
                .expect("releasable learning request lock")
                .len()
        }
    }

    impl PinnedOverlayNativeProvider {
        fn new(skill_id: SkillId) -> Self {
            Self {
                skill_id,
                started: Notify::new(),
                release: Notify::new(),
                requests: StdMutex::new(Vec::new()),
                round: AtomicUsize::new(0),
            }
        }

        fn requests(&self) -> Vec<ChatRequest> {
            self.requests
                .lock()
                .expect("pinned overlay request lock")
                .clone()
        }
    }

    #[async_trait]
    impl Provider for ScriptedLearningProvider {
        fn name(&self) -> &str {
            "learning"
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities::default()
        }

        async fn chat(&self, request: ChatRequest) -> AnyhowResult<ChatResponse> {
            self.requests
                .lock()
                .expect("learning request lock")
                .push(request);
            let Some(text) = self
                .responses
                .lock()
                .expect("learning response lock")
                .pop_front()
            else {
                bail!("learning response script exhausted");
            };
            Ok(ChatResponse {
                text,
                usage: None,
                reasoning_content: None,
                provider_replay_state: None,
                termination: pioneer_provider::ProviderTermination::Complete,
                tool_calls: Vec::new(),
            })
        }

        async fn stream_chat(
            &self,
            _request: ChatRequest,
        ) -> AnyhowResult<BoxStream<'static, AnyhowResult<StreamChunk>>> {
            bail!("learner/reviewer contracts must use non-streaming chat")
        }
    }

    #[async_trait]
    impl Provider for WorkspaceSelectiveLearningProvider {
        fn name(&self) -> &str {
            "workspace-selective-learning"
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities::default()
        }

        async fn chat(&self, request: ChatRequest) -> AnyhowResult<ChatResponse> {
            let should_fail = request
                .messages
                .iter()
                .any(|message| message.content.contains("FAIL_WORKSPACE_SENTINEL"));
            self.requests
                .lock()
                .expect("workspace-selective request lock")
                .push(request);
            if should_fail {
                bail!("workspace-scoped provider failure");
            }
            Ok(ChatResponse {
                text: serde_json::json!({
                    "digestRevision": 1,
                    "observations": []
                })
                .to_string(),
                usage: None,
                reasoning_content: None,
                provider_replay_state: None,
                termination: pioneer_provider::ProviderTermination::Complete,
                tool_calls: Vec::new(),
            })
        }

        async fn stream_chat(
            &self,
            _request: ChatRequest,
        ) -> AnyhowResult<BoxStream<'static, AnyhowResult<StreamChunk>>> {
            bail!("learner/reviewer contracts must use non-streaming chat")
        }
    }

    #[async_trait]
    impl Provider for BlockingLearningProvider {
        fn name(&self) -> &str {
            "blocking-learning"
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities::default()
        }

        async fn chat(&self, request: ChatRequest) -> AnyhowResult<ChatResponse> {
            self.requests
                .lock()
                .expect("blocking learning request lock")
                .push(request);
            self.started.notify_one();
            std::future::pending::<AnyhowResult<ChatResponse>>().await
        }

        async fn stream_chat(
            &self,
            _request: ChatRequest,
        ) -> AnyhowResult<BoxStream<'static, AnyhowResult<StreamChunk>>> {
            bail!("learner/reviewer contracts must use non-streaming chat")
        }
    }

    #[async_trait]
    impl Provider for ReleasableLearningProvider {
        fn name(&self) -> &str {
            "releasable-learning"
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities::default()
        }

        async fn chat(&self, request: ChatRequest) -> AnyhowResult<ChatResponse> {
            self.requests
                .lock()
                .expect("releasable learning request lock")
                .push(request);
            self.started.notify_one();
            self.release.notified().await;
            Ok(ChatResponse {
                text: serde_json::json!({
                    "digestRevision": 1,
                    "observations": []
                })
                .to_string(),
                usage: None,
                reasoning_content: None,
                provider_replay_state: None,
                termination: pioneer_provider::ProviderTermination::Complete,
                tool_calls: Vec::new(),
            })
        }

        async fn stream_chat(
            &self,
            _request: ChatRequest,
        ) -> AnyhowResult<BoxStream<'static, AnyhowResult<StreamChunk>>> {
            bail!("learner/reviewer contracts must use non-streaming chat")
        }
    }

    #[async_trait]
    impl Provider for PinnedOverlayNativeProvider {
        fn name(&self) -> &str {
            "pinned-native"
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                streaming: false,
                vision: false,
                tool_calling: true,
                embeddings: false,
                transcription: false,
                input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
            }
        }

        async fn chat(&self, request: ChatRequest) -> AnyhowResult<ChatResponse> {
            let is_preflight = request.compiled_prompt.is_none();
            self.requests
                .lock()
                .expect("pinned overlay request lock")
                .push(request);
            if is_preflight {
                return Ok(ChatResponse {
                    text: r#"{"tools":{"visibleTools":[]}}"#.to_owned(),
                    usage: None,
                    reasoning_content: None,
                    provider_replay_state: None,
                    termination: pioneer_provider::ProviderTermination::Complete,
                    tool_calls: Vec::new(),
                });
            }
            if self.round.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.notify_one();
                self.release.notified().await;
                return Ok(ChatResponse {
                    text: String::new(),
                    usage: None,
                    reasoning_content: None,
                    provider_replay_state: None,
                    termination: pioneer_provider::ProviderTermination::ToolCalls,
                    tool_calls: vec![ProviderToolCall {
                        id: "read_pinned_agent_skill".to_owned(),
                        name: "read_skill".to_owned(),
                        arguments: format!(r#"{{"skill_id":"skill:{}"}}"#, self.skill_id),
                    }],
                });
            }
            Ok(ChatResponse {
                text: "The pinned learned procedure was read.".to_owned(),
                usage: None,
                reasoning_content: None,
                provider_replay_state: None,
                termination: pioneer_provider::ProviderTermination::Complete,
                tool_calls: Vec::new(),
            })
        }

        async fn stream_chat(
            &self,
            request: ChatRequest,
        ) -> AnyhowResult<BoxStream<'static, AnyhowResult<StreamChunk>>> {
            let response = self.chat(request).await?;
            let mut chunks = Vec::new();
            if !response.text.is_empty() {
                chunks.push(Ok(StreamChunk::delta(response.text)));
            }
            if !response.tool_calls.is_empty() {
                chunks.push(Ok(StreamChunk::tool_calls(response.tool_calls)));
            }
            chunks.push(Ok(StreamChunk::final_chunk_with(
                pioneer_provider::ProviderTermination::Complete,
            )));
            Ok(futures_util::stream::iter(chunks).boxed())
        }
    }

    struct NativeReadSkillProvider {
        skill_id: SkillId,
        requests: StdMutex<Vec<ChatRequest>>,
        round: AtomicUsize,
    }

    impl NativeReadSkillProvider {
        fn new(skill_id: SkillId) -> Self {
            Self {
                skill_id,
                requests: StdMutex::new(Vec::new()),
                round: AtomicUsize::new(0),
            }
        }

        fn requests(&self) -> Vec<ChatRequest> {
            self.requests.lock().expect("native request lock").clone()
        }
    }

    #[async_trait]
    impl Provider for NativeReadSkillProvider {
        fn name(&self) -> &str {
            "native"
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                streaming: false,
                vision: false,
                tool_calling: true,
                embeddings: false,
                transcription: false,
                input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
            }
        }

        async fn chat(&self, request: ChatRequest) -> AnyhowResult<ChatResponse> {
            let is_preflight = request.compiled_prompt.is_none();
            self.requests
                .lock()
                .expect("native request lock")
                .push(request);
            if is_preflight {
                return Ok(ChatResponse {
                    text: r#"{"tools":{"visibleTools":[]}}"#.to_owned(),
                    usage: None,
                    reasoning_content: None,
                    provider_replay_state: None,
                    termination: pioneer_provider::ProviderTermination::Complete,
                    tool_calls: Vec::new(),
                });
            }
            if self.round.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(ChatResponse {
                    text: String::new(),
                    usage: None,
                    reasoning_content: None,
                    provider_replay_state: None,
                    termination: pioneer_provider::ProviderTermination::ToolCalls,
                    tool_calls: vec![ProviderToolCall {
                        id: "read_active_agent_skill".to_owned(),
                        name: "read_skill".to_owned(),
                        arguments: format!(r#"{{"skill_id":"skill:{}"}}"#, self.skill_id),
                    }],
                });
            }
            Ok(ChatResponse {
                text: "The learned procedure was read.".to_owned(),
                usage: None,
                reasoning_content: None,
                provider_replay_state: None,
                termination: pioneer_provider::ProviderTermination::Complete,
                tool_calls: Vec::new(),
            })
        }

        async fn stream_chat(
            &self,
            request: ChatRequest,
        ) -> AnyhowResult<BoxStream<'static, AnyhowResult<StreamChunk>>> {
            let response = self.chat(request).await?;
            let mut chunks = Vec::new();
            if !response.text.is_empty() {
                chunks.push(Ok(StreamChunk::delta(response.text)));
            }
            if !response.tool_calls.is_empty() {
                chunks.push(Ok(StreamChunk::tool_calls(response.tool_calls)));
            }
            chunks.push(Ok(StreamChunk::final_chunk_with(
                pioneer_provider::ProviderTermination::Complete,
            )));
            Ok(futures_util::stream::iter(chunks).boxed())
        }
    }

    async fn test_store() -> (
        sea_orm::DatabaseConnection,
        Arc<CrudStore>,
        Arc<WorkspaceManager>,
    ) {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite must open");
        Migrator::up(&database, None)
            .await
            .expect("migrations must apply");
        database
            .execute_unprepared(
                "INSERT INTO workspace (id, name, is_active, is_current) VALUES \
                 ('ws_self_improvement_e2e', 'Self improvement E2E', 1, 1)",
            )
            .await
            .expect("workspace fixture must insert");
        (
            database.clone(),
            Arc::new(CrudStore::new(database.clone())),
            Arc::new(WorkspaceManager::new(database)),
        )
    }

    fn source_thread_for_workspace(workspace_id: &str, thread_id: &str, created_at: i64) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "native-model".to_owned(),
            model_provider: "native".to_owned(),
            reasoning_effort: None,
            created_at,
            updated_at: created_at,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        }
    }

    fn source_turn(turn_id: &str) -> Turn {
        Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: default_turn_permission_profile_snapshot(),
        }
    }

    async fn project_completed_source_turn(
        store: &CrudStore,
        thread_id: &str,
        turn_id: &str,
        text: &str,
        terminal_at: i64,
    ) {
        project_completed_source_turn_for_workspace(
            store,
            WORKSPACE,
            thread_id,
            turn_id,
            text,
            terminal_at,
        )
        .await;
    }

    async fn project_completed_source_turn_for_workspace(
        store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        text: &str,
        terminal_at: i64,
    ) {
        let thread = source_thread_for_workspace(workspace_id, thread_id, terminal_at - 1);
        let turn = source_turn(turn_id);
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[UserInput::Text {
                    text: text.to_owned(),
                    text_elements: Vec::new(),
                }],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("source turn start must project");
        store
            .database_connection()
            .execute_unprepared(
                format!(
                    "UPDATE thread SET access_class = 'workspace' WHERE id = '{}'",
                    thread_id
                )
                .as_str(),
            )
            .await
            .expect("self-improvement source fixture must be workspace-visible");
        let persisted = turn::Entity::find_by_id(turn_id)
            .one(&store.database_connection())
            .await
            .expect("self-improvement source turn provenance query should succeed")
            .expect("self-improvement source turn should exist");
        assert_eq!(
            persisted.initiated_by_actor_kind.as_deref(),
            Some("system"),
            "supervisor work must not be attributed to the settings author"
        );
        assert_eq!(persisted.initiated_by_actor_id, None);
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: Turn {
                        status: TurnStatus::Completed,
                        ..turn
                    },
                },
                terminal_at,
            )
            .await
            .expect("source turn completion must project");
    }

    #[tokio::test]
    async fn frozen_range_keeps_exact_terminal_boundary_per_thread() {
        let (_database, store, _workspace_manager) = test_store().await;
        let state = store
            .activate_self_improvement_workspace(WORKSPACE, ENABLED_AT)
            .await
            .expect("workspace must activate");
        project_completed_source_turn(
            store.as_ref(),
            "thread_boundary_a",
            "turn_boundary_a_one",
            FIRST_TEXT,
            ENABLED_AT + 1,
        )
        .await;
        project_completed_source_turn(
            store.as_ref(),
            "thread_boundary_b",
            "turn_boundary_b",
            SECOND_TEXT,
            ENABLED_AT + 2,
        )
        .await;
        project_completed_source_turn(
            store.as_ref(),
            "thread_boundary_a",
            "turn_boundary_a_two",
            SECOND_TEXT,
            ENABLED_AT + 3,
        )
        .await;
        let anchors = store
            .list_self_improvement_source_turns_after(
                WORKSPACE,
                state.cursor_source_id,
                state.effective_enabled_at_unix.unwrap(),
                MAX_NEW_SOURCE_TURNS_PER_RUN,
            )
            .await
            .expect("source anchors must load");
        let run = store
            .create_or_get_self_improvement_run(
                NewSelfImprovementRun {
                    workspace_id: WORKSPACE.to_owned(),
                    activation_epoch: state.activation_epoch,
                    scheduled_date_utc: "2030-03-19".to_owned(),
                    source_lower_exclusive: state.cursor_source_id,
                    source_upper_inclusive: anchors.last().unwrap().id,
                    learner_provider: "learning".to_owned(),
                    learner_model: "learner".to_owned(),
                    reviewer_provider: "learning".to_owned(),
                    reviewer_model: "reviewer".to_owned(),
                    pipeline_contract_version: PIPELINE_CONTRACT_VERSION.to_owned(),
                },
                ENABLED_AT + 4,
            )
            .await
            .expect("run must freeze the selected range");
        let frozen = SelfImprovementFrozenSourceRange::new(
            run.workspace_id.clone(),
            run.source_lower_exclusive,
            run.source_upper_inclusive,
            anchors.clone(),
        )
        .expect("range must validate");

        assert_eq!(frozen.anchors, anchors);
        assert_eq!(frozen.source_lower_exclusive, state.cursor_source_id);
        assert_eq!(frozen.source_upper_inclusive, run.source_upper_inclusive);
        assert_eq!(
            frozen
                .thread_terminal_boundaries
                .iter()
                .map(|boundary| (
                    boundary.thread_id.as_str(),
                    boundary.turn_id.as_str(),
                    boundary.terminal_event_id.as_str(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    "thread_boundary_a",
                    "turn_boundary_a_two",
                    anchors[2].terminal_event_id.as_str(),
                ),
                (
                    "thread_boundary_b",
                    "turn_boundary_b",
                    anchors[1].terminal_event_id.as_str(),
                ),
            ]
        );
    }

    fn agent_tool_loop_config() -> ToolLoopConfig {
        ToolLoopConfig {
            provider: pioneer_provider::ProviderTimeoutPolicy::default(),
            preflight: PreflightLoopConfig::default(),
            web: WebToolsConfig::default(),
            computer_use: ComputerUseToolsConfig::default(),
            skills: SkillsLoopConfig {
                enabled: false,
                max_skills_per_source: 1,
                max_skill_file_bytes: 1024 * 1024,
                prompt_max_chars: 24_000,
                allow_implicit_invocation: false,
                system_roots: Vec::new(),
                user_roots: Vec::new(),
                registry_roots: Vec::new(),
                system_import_roots: Vec::new(),
                user_import_roots: Vec::new(),
                registry_import_roots: Vec::new(),
                validation: SkillsValidationLoopConfig {
                    strict_agentskills: true,
                    accept_openclaw_profile: false,
                },
                security: SkillsSecurityLoopConfig {
                    allow_untrusted_install: false,
                    min_trust_for_shell_tools: SkillTrustLevel::Verified,
                    min_trust_for_http_tools: SkillTrustLevel::Verified,
                    min_trust_for_function_proxy_tools: SkillTrustLevel::Verified,
                    max_install_archive_bytes: 1024 * 1024,
                    max_install_archive_compressed_bytes: 1024 * 1024,
                    max_install_archive_uncompressed_bytes: 1024 * 1024,
                    max_install_archive_entries: 32,
                    max_install_file_bytes: 1024 * 1024,
                    upload_ttl_secs: 60,
                    upload_recommended_chunk_size_bytes: 1024,
                    upload_max_chunk_size_bytes: 4096,
                },
                dependencies: SkillsDependenciesLoopConfig {
                    preflight_on_resolve: true,
                    runtime_recheck_on_tool_call: true,
                },
                runtime: SkillsRuntimeLoopConfig {
                    enable_dynamic_tools: false,
                    enable_read_skill: false,
                    max_dynamic_tools_per_skill: 1,
                    read_skill_max_chars: 1,
                    compact_mode_threshold: 1,
                    allow_shell_tools: false,
                    allow_http_tools: false,
                    allow_function_proxy_tools: false,
                },
            },
            memory: MemoryLoopConfig::default(),
            budget: ToolLoopBudgetConfig::default(),
            execution_windows: ExecutionWindowsConfig::default(),
            retry: ToolRetryBudgetConfig::default(),
        }
    }

    #[test]
    fn daily_schedule_is_fixed_to_next_utc_midnight() {
        use chrono::TimeZone;

        let now = Utc
            .with_ymd_and_hms(2030, 3, 1, 23, 59, 30)
            .single()
            .expect("fixture timestamp must exist");
        assert_eq!(next_daily_utc_delay(now), Duration::from_secs(30));

        let morning = Utc
            .with_ymd_and_hms(2030, 3, 1, 8, 0, 0)
            .single()
            .expect("fixture timestamp must exist");
        let morning_timestamp = morning.timestamp();
        assert_eq!(
            next_daily_utc_delay(morning.clone()),
            Duration::from_secs(16 * 60 * 60)
        );
        assert!(
            PROVIDER_CALL_TIMEOUT < Duration::from_secs(RUN_LEASE_SECONDS as u64),
            "one provider call timeout must remain below the lease TTL"
        );
        assert_eq!(
            next_supervisor_delay(morning, Some(morning_timestamp + 45)),
            Duration::from_secs(45)
        );
        assert_eq!(
            next_supervisor_delay(morning, Some(morning_timestamp - 1)),
            OVERDUE_RETRY_POLL_DELAY
        );
        assert_eq!(retry_backoff_seconds(1), 30);
        assert_eq!(retry_backoff_seconds(2), 120);
        assert_eq!(retry_backoff_seconds(3), 480);
        assert_eq!(retry_backoff_seconds(i64::MAX), 900);
    }

    #[test]
    fn retry_classification_keeps_infrastructure_contract_and_authority_distinct() {
        for class in [
            ProviderFailureClass::NetworkTransient,
            ProviderFailureClass::RateLimit,
            ProviderFailureClass::Provider5xx,
            ProviderFailureClass::AuthExpired,
            ProviderFailureClass::StreamStall,
            ProviderFailureClass::StreamTruncated,
            ProviderFailureClass::EmptyResponse,
            ProviderFailureClass::Unknown,
        ] {
            assert!(retryable_provider_failure(class), "{class:?}");
        }
        for class in [
            ProviderFailureClass::AuthOrPermission,
            ProviderFailureClass::ModelNotFound,
            ProviderFailureClass::ContextTooLarge,
            ProviderFailureClass::MaxOutputTokens,
            ProviderFailureClass::ProviderRejected,
            ProviderFailureClass::UnsupportedParameter,
            ProviderFailureClass::InvalidRequest,
            ProviderFailureClass::PermissionDenied,
        ] {
            assert!(!retryable_provider_failure(class), "{class:?}");
        }

        let timeout = run_execution_error(
            RunFailureDisposition::RetryInfrastructure,
            "provider_timeout",
            "review_timeout",
        );
        assert_eq!(
            classify_execution_failure(&timeout).disposition,
            RunFailureDisposition::RetryInfrastructure
        );

        let sqlite_busy = anyhow::anyhow!("database is locked");
        let sqlite_failure = classify_execution_failure(&sqlite_busy);
        assert_eq!(
            sqlite_failure.disposition,
            RunFailureDisposition::RetryInfrastructure
        );
        assert_eq!(sqlite_failure.error_class, "sqlite_busy");

        let contract = run_execution_error(
            RunFailureDisposition::FailWithoutInfrastructureRetry,
            "model_contract",
            "malformed_model_json",
        );
        assert_eq!(
            classify_execution_failure(&contract).disposition,
            RunFailureDisposition::FailWithoutInfrastructureRetry
        );

        let stale = run_execution_error(
            RunFailureDisposition::LostAuthority,
            "lost_authority",
            "lease_fence_changed",
        );
        assert_eq!(
            classify_execution_failure(&stale).disposition,
            RunFailureDisposition::LostAuthority
        );

        for kind in [
            ModelContractErrorKind::InputTooLarge,
            ModelContractErrorKind::HostValidationRejected,
        ] {
            assert!(is_lifecycle_host_rejection(&ModelContractError {
                stage: super::super::learner::ModelContractStage::Synthesis,
                kind,
                reason_code: "host_capacity_rejected",
                provider_failure_class: None,
                usage: ModelCallUsage::default(),
            }));
        }
        assert!(!is_lifecycle_host_rejection(&ModelContractError {
            stage: super::super::learner::ModelContractStage::Synthesis,
            kind: ModelContractErrorKind::MalformedJson,
            reason_code: "malformed_model_json",
            provider_failure_class: None,
            usage: ModelCallUsage::default(),
        }));
    }

    #[test]
    fn wake_budget_reserves_worst_case_calls_and_unknown_tokens() {
        let mut budget = WakeBudget::new();
        assert!(budget.can_start_chunk());
        budget
            .record_chunk_step(ModelCallUsage {
                provider_calls: MAX_CHUNK_CONTRACT_ATTEMPTS,
                input_tokens: None,
                output_tokens: None,
            })
            .expect("first worst-case chunk step must fit");
        assert!(budget.can_start_chunk());
        budget
            .record_chunk_step(ModelCallUsage {
                provider_calls: MAX_CHUNK_CONTRACT_ATTEMPTS,
                input_tokens: None,
                output_tokens: None,
            })
            .expect("second worst-case chunk step must fit");
        assert!(!budget.can_start_chunk());
        assert!(!budget.can_start_synthesis_and_review());
        assert_eq!(
            budget.usage.input_tokens,
            Some(
                u64::from(MAX_CHUNK_CONTRACT_ATTEMPTS)
                    .saturating_mul(2)
                    .saturating_mul(CHUNK_ANALYSIS_MAX_TOKEN_UPPER_BOUND as u64)
            )
        );

        let mut short = WakeBudget::new();
        short
            .record_chunk_step(ModelCallUsage {
                provider_calls: 1,
                input_tokens: Some(100),
                output_tokens: Some(10),
            })
            .expect("small successful chunk call must fit");
        assert!(short.can_start_synthesis_and_review());

        let mut input_exhausted = WakeBudget::new();
        input_exhausted.usage.input_tokens = Some(MAX_INPUT_TOKENS_PER_WAKE);
        assert!(
            !input_exhausted.can_start_chunk(),
            "input-token exhaustion must prevent another provider call"
        );

        let mut output_exhausted = WakeBudget::new();
        output_exhausted.usage.output_tokens = Some(MAX_OUTPUT_TOKENS_PER_WAKE);
        assert!(
            !output_exhausted.can_start_chunk(),
            "output-token exhaustion must prevent another provider call"
        );

        let mut wall_clock_exhausted = WakeBudget::new();
        wall_clock_exhausted.started_at = Instant::now()
            .checked_sub(WAKE_WALL_CLOCK_BUDGET.saturating_add(Duration::from_secs(1)))
            .expect("test wake budget must fit the platform monotonic clock");
        assert!(
            !wall_clock_exhausted.can_start_chunk(),
            "wall-clock exhaustion must prevent another provider call"
        );

        let chunk_attempt_reservation =
            PROVIDER_CALL_TIMEOUT.saturating_mul(MAX_CHUNK_CONTRACT_ATTEMPTS);
        let mut only_one_call_fits = WakeBudget::new();
        only_one_call_fits.started_at = Instant::now()
            .checked_sub(
                WAKE_WALL_CLOCK_BUDGET
                    .saturating_sub(chunk_attempt_reservation)
                    .saturating_add(Duration::from_secs(1)),
            )
            .expect("test wake budget must fit the platform monotonic clock");
        assert!(
            only_one_call_fits.remaining_wall_clock() > PROVIDER_CALL_TIMEOUT,
            "fixture must still have time for one provider call"
        );
        assert!(
            !only_one_call_fits.can_start_chunk(),
            "a chunk must reserve every possible contract attempt before it starts"
        );
    }

    #[test]
    fn settings_invalidation_uses_effective_model_selections() {
        let default_model = GatewaySelfImprovementModelSelectionConfig {
            provider: "learning".to_owned(),
            model: "model-a".to_owned(),
        };
        let current = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(default_model.clone()),
            reviewer_model: None,
        };
        let explicit_same_reviewer = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(default_model.clone()),
            reviewer_model: Some(default_model.clone()),
        };
        assert!(
            !settings_change_invalidates_execution(&current, &explicit_same_reviewer),
            "equivalent explicit reviewer inheritance must not strand a live claim"
        );
        assert!(settings_change_invalidates_execution(
            &current,
            &GatewaySelfImprovementConfig {
                enabled: false,
                ..current.clone()
            }
        ));
        assert!(settings_change_invalidates_execution(
            &current,
            &GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "model-b".to_owned(),
                }),
                reviewer_model: None,
            }
        ));
        assert!(settings_change_invalidates_execution(
            &current,
            &GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(default_model),
                reviewer_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "reviewer".to_owned(),
                    model: "reviewer-model".to_owned(),
                }),
            }
        ));
    }

    #[test]
    fn production_owner_constructs_starts_and_joins_before_database_close() {
        let source = include_str!("../lib.rs");
        let saved_workspace_settings = source
            .find("gateway_settings.workspace_self_improvement_configs()")
            .expect("production supervisor must load workspace-scoped saved Settings");
        let constructed = source
            .find("SelfImprovementSupervisor::new(")
            .expect("production Gateway must construct the supervisor");
        let started = source[constructed..]
            .find(".start()")
            .map(|offset| constructed + offset)
            .expect("production Gateway must start the supervisor");
        let shutdown = source[started..]
            .find("self_improvement_supervisor.shutdown().await")
            .map(|offset| started + offset)
            .expect("production Gateway must join the supervisor");
        let database_close = source[shutdown..]
            .find("database\n        .close()")
            .map(|offset| shutdown + offset)
            .expect("production Gateway must close the database");
        assert!(
            saved_workspace_settings < constructed
                && constructed < started
                && started < shutdown
                && shutdown < database_close
        );
        assert!(!source.contains("config.gateway.self_improvement"));
        assert!(!source.contains("database::startup::spawn_self_improvement"));
    }

    #[tokio::test]
    async fn supervisor_start_and_shutdown_own_exactly_one_joined_task() {
        let (_database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider("learning", provider));
        let supervisor = Arc::new(test_supervisor(
            store,
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig::default(),
            1024 * 1024,
        ));

        supervisor
            .start()
            .await
            .expect("owned supervisor task must start");
        assert!(supervisor.task.lock().await.is_some());
        assert!(
            supervisor.start().await.is_err(),
            "a second owned task must be rejected"
        );
        timeout(Duration::from_secs(2), supervisor.shutdown())
            .await
            .expect("shutdown must join the owned task");
        assert!(supervisor.task.lock().await.is_none());
        assert!(supervisor.cancellation.is_cancelled());
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn production_walking_skeleton_reaches_native_prompt_and_exact_read_skill() {
        let (database, store, workspace_manager) = test_store().await;
        let learning_provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            learning_provider.clone(),
        ));
        let supervisor = test_supervisor(
            store.clone(),
            registry.clone(),
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner-model".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );

        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("enabled setting must establish the source baseline");
        project_completed_source_turn(
            store.as_ref(),
            "thread_source_one",
            "turn_source_one",
            FIRST_TEXT,
            ENABLED_AT + 1,
        )
        .await;
        project_completed_source_turn(
            store.as_ref(),
            "thread_source_two",
            "turn_source_two",
            SECOND_TEXT,
            ENABLED_AT + 2,
        )
        .await;

        let sources = store
            .list_self_improvement_source_turns_after(WORKSPACE, 0, ENABLED_AT, 10)
            .await
            .expect("projected source range must load");
        assert_eq!(sources.len(), 2);
        let frozen_range = SelfImprovementFrozenSourceRange::new(
            WORKSPACE,
            0,
            sources.last().unwrap().id,
            sources.clone(),
        )
        .expect("source range must freeze");
        let canonical = store
            .list_canonical_turn_events_for_self_improvement(&frozen_range)
            .await
            .expect("canonical source history must load");
        let first_event_id = canonical
            .iter()
            .find(|record| record.turn_id == "turn_source_one")
            .expect("first turn event must exist")
            .event_id
            .clone();
        let second_event_id = canonical
            .iter()
            .find(|record| record.turn_id == "turn_source_two")
            .expect("second turn event must exist")
            .event_id
            .clone();
        learning_provider.set_responses([
            serde_json::json!({
                "digestRevision": 1,
                "observations": [{
                    "observationKey": "verify-checksum",
                    "summary": "Successful releases verify their artifact checksum.",
                    "evidence": [
                        {
                            "turnId": "turn_source_one",
                            "eventId": first_event_id,
                            "excerpt": "Always verify the checksum"
                        },
                        {
                            "turnId": "turn_source_two",
                            "eventId": second_event_id,
                            "excerpt": "Always verify the checksum"
                        }
                    ],
                    "kind": "success_pattern"
                }]
            })
            .to_string(),
            serde_json::json!({
                "candidate": {
                    "action": "create",
                    "candidateKey": "verify-checksum-before-publish",
                    "observationKeys": ["verify-checksum"],
                    "name": "Verify release checksum",
                    "slug": "verify-release-checksum",
                    "whenToUse": "Publishing a release build",
                    "whenNotToUse": "No build artifact is being published",
                    "instructions": SKILL_BODY
                }
            })
            .to_string(),
            serde_json::json!({
                "candidateKey": "candidate-1feca80ff4895b7c3aa4adda2212300c4b88adbe0de96686ab4569c9500b9bd1",
                "decision": "accept",
                "reasonCodes": []
            })
            .to_string(),
        ]);

        supervisor
            .wake_once(ENABLED_AT + 10)
            .await
            .expect("production supervisor wake must complete");
        supervisor
            .wake_once(ENABLED_AT + 11)
            .await
            .expect("a repeated same-day wake must be idempotent");

        let learning_requests = learning_provider.requests();
        assert_eq!(learning_requests.len(), 3, "exactly three typed calls");
        for request in &learning_requests {
            let input = request
                .messages
                .iter()
                .find(|message| message.role == Role::User)
                .expect("typed call must contain its untrusted data message");
            assert!(input.content.contains("turn_source_one"));
            assert!(input.content.contains("turn_source_two"));
        }
        let state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("state query must succeed")
            .expect("state must exist");
        assert_eq!(state.cursor_source_id, sources[1].id);
        let run_row = database
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT status, learner_provider, learner_model, reviewer_provider, \
                 reviewer_model FROM self_improvement_run"
                    .to_owned(),
            ))
            .await
            .expect("run status query must execute")
            .expect("one run must exist");
        let run_status = run_row
            .try_get::<String>("", "status")
            .expect("run status must decode");
        assert_eq!(run_status, "completed");
        assert_eq!(
            run_row
                .try_get::<String>("", "learner_provider")
                .expect("frozen learner provider must decode"),
            "learning"
        );
        assert_eq!(
            run_row
                .try_get::<String>("", "learner_model")
                .expect("frozen learner model must decode"),
            "learner-model"
        );
        assert_eq!(
            run_row
                .try_get::<String>("", "reviewer_provider")
                .expect("frozen reviewer provider must decode"),
            "learning"
        );
        assert_eq!(
            run_row
                .try_get::<String>("", "reviewer_model")
                .expect("frozen reviewer model must decode"),
            "learner-model"
        );
        let run_count = database
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) AS value FROM self_improvement_run".to_owned(),
            ))
            .await
            .expect("run count query must execute")
            .expect("run count must exist")
            .try_get::<i64>("", "value")
            .expect("run count must decode");
        assert_eq!(run_count, 1, "repeated wakes must reuse one logical row");

        let active = store
            .list_active_agent_skill_versions(WORKSPACE)
            .await
            .expect("active skill must load");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].version.instruction_body, SKILL_BODY);
        assert_eq!(
            active[0].version.source_turn_ids,
            vec!["turn_source_one", "turn_source_two"]
        );
        let overlay = load_active_agent_skill_overlay(store.as_ref(), WORKSPACE)
            .await
            .expect("production overlay must load");
        assert_eq!(overlay.len(), 1);
        let skill_id = overlay[0].skill_id.clone();
        let version_id = overlay[0].version_id.clone();

        let native_provider = Arc::new(NativeReadSkillProvider::new(skill_id.clone()));
        registry.insert("native", native_provider.clone());
        let manager = AgentManager::new(registry, agent_tool_loop_config());
        manager
            .ensure_thread("thread_native", WORKSPACE)
            .await
            .expect("native thread must initialize");
        let mut events = manager
            .take_durable_receiver("thread_native")
            .await
            .expect("native durable receiver must exist");
        manager
            .start_turn_with_resolved_artifacts_environment_reasoning_permission_profile_security_snapshot_and_agent_skill_overlay(
                "thread_native",
                "turn_native",
                ThreadMode::Agent,
                "native-model",
                "native",
                HashMap::new(),
                SkillCatalogSnapshot {
                    version: 0,
                    generated_at_unix: ENABLED_AT + 11,
                    skills: Vec::new(),
                },
                overlay,
                vec![UserInput::Text {
                    text: "Apply the learned release procedure.".to_owned(),
                    text_elements: Vec::new(),
                }],
                Vec::new(),
                Vec::new(),
                HashMap::new(),
                Vec::new(),
                None,
                default_turn_permission_profile_snapshot(),
                TurnExecutionSecuritySnapshot::unrestricted_full_access(
                    "/workspace",
                    (ENABLED_AT + 11) * 1000,
                ),
            )
            .await
            .expect("native Agent turn must start");
        timeout(Duration::from_secs(5), async {
            loop {
                let Some(event) = events.recv().await else {
                    panic!("native durable event lane closed");
                };
                events.acknowledge_last(Ok(()));
                match event {
                    AgentDurableEvent::TurnCompleted { .. } => return,
                    AgentDurableEvent::TurnFailed { error, .. } => {
                        panic!("native Agent turn failed: {error}")
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("native Agent turn must terminate");

        let native_requests = native_provider
            .requests()
            .into_iter()
            .filter(|request| request.compiled_prompt.is_some())
            .collect::<Vec<_>>();
        assert!(native_requests.len() >= 2);
        let first = &native_requests[0];
        let compiled = first
            .compiled_prompt
            .as_ref()
            .expect("native request must use production prompt compiler");
        assert!(
            compiled
                .full_system_text
                .contains(&format!("skill:{skill_id}"))
        );
        assert!(compiled.full_system_text.contains(version_id.as_str()));
        assert!(
            compiled
                .full_system_text
                .contains("Verify release checksum")
        );
        assert!(
            first
                .tools
                .as_ref()
                .is_some_and(|tools| tools.iter().any(|tool| tool.name == "read_skill"))
        );
        let read_result = native_requests[1]
            .messages
            .iter()
            .find(|message| {
                message.role == Role::Tool && message.name.as_deref() == Some("read_skill")
            })
            .expect("second native request must contain real read_skill output");
        assert!(read_result.content.contains(SKILL_BODY));
        assert!(read_result.content.contains(version_id.as_str()));
        assert!(
            read_result
                .content
                .contains(&format!("\"skill_id\":\"{skill_id}\""))
        );
        assert!(!read_result.content.contains("skill_asset_root"));
    }

    #[tokio::test]
    async fn production_supervisor_executes_create_update_and_exact_parent_rollback() {
        async fn range_events(
            store: &CrudStore,
            lower: i64,
            turn_ids: &[&str],
        ) -> (i64, Vec<String>) {
            let sources = store
                .list_self_improvement_source_turns_after(
                    WORKSPACE,
                    lower,
                    ENABLED_AT,
                    MAX_NEW_SOURCE_TURNS_PER_RUN,
                )
                .await
                .expect("lifecycle source range");
            let upper = sources.last().expect("lifecycle source").id;
            let range = SelfImprovementFrozenSourceRange::new(WORKSPACE, lower, upper, sources)
                .expect("lifecycle frozen range");
            let canonical = store
                .list_canonical_turn_events_for_self_improvement(&range)
                .await
                .expect("lifecycle canonical history");
            let event_ids = turn_ids
                .iter()
                .map(|turn_id| {
                    canonical
                        .iter()
                        .find(|record| record.turn_id == *turn_id)
                        .unwrap_or_else(|| panic!("missing canonical event for {turn_id}"))
                        .event_id
                        .clone()
                })
                .collect();
            (upper, event_ids)
        }

        let (_database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            provider.clone(),
        ));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("workspace activation");

        project_completed_source_turn(
            store.as_ref(),
            "thread_lifecycle_create_one",
            "turn_lifecycle_create_one",
            "Verify checksum before the first release.",
            ENABLED_AT + 1,
        )
        .await;
        project_completed_source_turn(
            store.as_ref(),
            "thread_lifecycle_create_two",
            "turn_lifecycle_create_two",
            "Verify checksum before the second release.",
            ENABLED_AT + 2,
        )
        .await;
        let (create_upper, create_events) = range_events(
            store.as_ref(),
            0,
            &["turn_lifecycle_create_one", "turn_lifecycle_create_two"],
        )
        .await;
        provider.set_responses([
            serde_json::json!({
                "digestRevision": 1,
                "observations": [{
                    "observationKey": "checksum-procedure",
                    "summary": "Releases verify checksums.",
                    "evidence": [
                        {
                            "turnId": "turn_lifecycle_create_one",
                            "eventId": create_events[0],
                            "excerpt": "Verify checksum"
                        },
                        {
                            "turnId": "turn_lifecycle_create_two",
                            "eventId": create_events[1],
                            "excerpt": "Verify checksum"
                        }
                    ],
                    "kind": "success_pattern"
                }]
            })
            .to_string(),
            serde_json::json!({
                "candidate": {
                    "action": "create",
                    "candidateKey": "checksum-create",
                    "observationKeys": ["checksum-procedure"],
                    "name": "Verify checksum",
                    "slug": "verify-checksum",
                    "whenToUse": "Publishing a release",
                    "whenNotToUse": "No artifact is published",
                    "instructions": "Verify the artifact checksum before publishing."
                }
            })
            .to_string(),
            serde_json::json!({
                "candidateKey": "candidate-f493585583e0cfa5700aadd9a60c5cc50f25c37ed62c0067f00b8886a743e967",
                "decision": "accept",
                "reasonCodes": []
            })
            .to_string(),
        ]);
        supervisor
            .wake_once(ENABLED_AT + 10)
            .await
            .expect("production create wake");
        let created = store
            .list_active_agent_skill_versions(WORKSPACE)
            .await
            .expect("created active skill");
        assert_eq!(created.len(), 1);
        let skill_id = created[0].skill_id.clone();
        let version_one = created[0].version.id.clone();

        let update_day = ENABLED_AT + 24 * 60 * 60;
        project_completed_source_turn(
            store.as_ref(),
            "thread_lifecycle_update",
            "turn_lifecycle_update",
            "Double-check the checksum before publishing.",
            update_day + 1,
        )
        .await;
        let (update_upper, update_events) =
            range_events(store.as_ref(), create_upper, &["turn_lifecycle_update"]).await;
        provider.set_responses([
            serde_json::json!({
                "digestRevision": 1,
                "observations": [{
                    "observationKey": "checksum-update",
                    "summary": "The procedure now double-checks the checksum.",
                    "evidence": [{
                        "turnId": "turn_lifecycle_update",
                        "eventId": update_events[0],
                        "excerpt": "Double-check the checksum"
                    }],
                    "kind": "correction"
                }]
            })
            .to_string(),
            serde_json::json!({
                "candidate": {
                    "action": "update",
                    "candidateKey": "checksum-update",
                    "targetSkillId": skill_id.to_string(),
                    "observationKeys": ["checksum-update"],
                    "name": "Double-check checksum",
                    "slug": "verify-checksum",
                    "whenToUse": "Publishing a release",
                    "whenNotToUse": "No artifact is published",
                    "instructions": "Calculate and double-check the artifact checksum before publishing."
                }
            })
            .to_string(),
            serde_json::json!({
                "candidateKey": "candidate-a693ee600f72ff7d800b648629d3c21d99dfe9f181544a3cc9a5a6c255eb2d62",
                "decision": "accept",
                "reasonCodes": []
            })
            .to_string(),
        ]);
        supervisor
            .wake_once(update_day + 10)
            .await
            .expect("production update wake");
        let updated = store
            .list_active_agent_skill_versions(WORKSPACE)
            .await
            .expect("updated active skill");
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].skill_id, skill_id);
        assert_eq!(
            updated[0].version.parent_version_id.as_deref(),
            Some(version_one.as_str())
        );
        assert_eq!(updated[0].version.display_name, "Double-check checksum");
        let version_two = updated[0].version.id.clone();

        let rollback_day = update_day + 24 * 60 * 60;
        project_completed_source_turn(
            store.as_ref(),
            "thread_lifecycle_rollback",
            "turn_lifecycle_rollback",
            "Return to the earlier verified checksum procedure.",
            rollback_day + 1,
        )
        .await;
        let (_rollback_upper, rollback_events) =
            range_events(store.as_ref(), update_upper, &["turn_lifecycle_rollback"]).await;
        provider.set_responses([
            serde_json::json!({
                "digestRevision": 1,
                "observations": [{
                    "observationKey": "checksum-rollback",
                    "summary": "The earlier procedure is required again.",
                    "evidence": [{
                        "turnId": "turn_lifecycle_rollback",
                        "eventId": rollback_events[0],
                        "excerpt": "earlier verified checksum procedure"
                    }],
                    "kind": "correction"
                }]
            })
            .to_string(),
            serde_json::json!({
                "candidate": {
                    "action": "rollback",
                    "candidateKey": "checksum-rollback",
                    "targetSkillId": skill_id.to_string(),
                    "targetVersionId": version_one,
                    "observationKeys": ["checksum-rollback"]
                }
            })
            .to_string(),
            serde_json::json!({
                "candidateKey": "candidate-84562f8e4aaa754d15d288d1ad02b3de5de582f61f170440ddc5732c7cda48f1",
                "decision": "accept",
                "reasonCodes": []
            })
            .to_string(),
        ]);
        supervisor
            .wake_once(rollback_day + 10)
            .await
            .expect("production rollback wake");
        let restored = store
            .list_active_agent_skill_versions(WORKSPACE)
            .await
            .expect("restored active skill");
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].skill_id, skill_id);
        assert_eq!(
            restored[0].version.id, created[0].version.id,
            "rollback must restore the exact immutable parent"
        );
        assert_eq!(restored[0].version.display_name, "Verify checksum");
        assert_eq!(
            store
                .get_agent_skill_version(WORKSPACE, version_two.as_str())
                .await
                .expect("updated version lookup")
                .expect("updated version remains historical")
                .version
                .id,
            version_two
        );
        assert_eq!(provider.requests().len(), 9);
    }

    #[tokio::test]
    async fn bounded_dispatch_starts_other_workspaces_without_detached_workers() {
        let (database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(BlockingLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "blocking-learning",
            provider.clone(),
        ));
        let enabled = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                provider: "blocking-learning".to_owned(),
                model: "learner".to_owned(),
            }),
            reviewer_model: None,
        };
        let mut workspace_ids = vec![WORKSPACE.to_owned()];
        for index in 1..(MAX_CONCURRENT_WORKSPACES + 2) {
            let workspace_id = format!("ws_bounded_{index}");
            let workspace_name = format!("Bounded {index}");
            let workspace = workspace_manager
                .create_workspace(workspace_id.as_str(), Some(workspace_name.as_str()))
                .await
                .expect("bounded workspace fixture must create");
            workspace_ids.push(workspace.id);
        }
        let workspace_configs = workspace_ids
            .iter()
            .map(|workspace_id| (workspace_id.clone(), enabled.clone()))
            .collect::<BTreeMap<_, _>>();
        let supervisor = Arc::new(test_supervisor(
            store.clone(),
            registry,
            workspace_manager.clone(),
            workspace_configs,
            1024 * 1024,
        ));
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("all bounded workspace fixtures must activate");
        for (index, workspace_id) in workspace_ids.iter().enumerate() {
            let thread_id = format!("thread_bounded_{index}");
            let turn_id = format!("turn_bounded_{index}");
            project_completed_source_turn_for_workspace(
                store.as_ref(),
                workspace_id,
                thread_id.as_str(),
                turn_id.as_str(),
                "A repeated procedure should be considered.",
                ENABLED_AT + 1,
            )
            .await;
        }

        let wake = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.wake_once(ENABLED_AT + 2).await })
        };
        timeout(Duration::from_secs(2), async {
            while provider.request_count() < MAX_CONCURRENT_WORKSPACES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bounded peers must begin while the first provider call is blocked");
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            provider.request_count(),
            MAX_CONCURRENT_WORKSPACES,
            "the owned stream must not poll more than the configured workspace bound"
        );

        supervisor
            .apply_desired_for_workspace(
                WORKSPACE,
                GatewaySelfImprovementConfig {
                    enabled: false,
                    ..enabled.clone()
                },
                ENABLED_AT + 3,
            )
            .await
            .expect("one workspace disable must cancel only its dispatch branch");
        timeout(Duration::from_secs(2), async {
            while provider.request_count() == MAX_CONCURRENT_WORKSPACES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelling one branch must let the next workspace enter the bounded stream");
        assert_eq!(
            provider.request_count(),
            MAX_CONCURRENT_WORKSPACES + 1,
            "one freed slot must start exactly one queued workspace"
        );
        assert!(
            !wake.is_finished(),
            "changing one workspace must not cancel peer workspace executions"
        );

        for workspace_id in workspace_ids.iter().filter(|id| id.as_str() != WORKSPACE) {
            supervisor
                .apply_desired_for_workspace(
                    workspace_id,
                    GatewaySelfImprovementConfig {
                        enabled: false,
                        ..enabled.clone()
                    },
                    ENABLED_AT + 3,
                )
                .await
                .expect("workspace disable must cancel its owned dispatch branch");
        }
        timeout(Duration::from_secs(2), wake)
            .await
            .expect("bounded dispatch must join after cancellation")
            .expect("bounded dispatch task must join")
            .expect("bounded dispatch wake must finish");
        let cancelled_count = database
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) AS value FROM self_improvement_run WHERE status = 'cancelled'"
                    .to_owned(),
            ))
            .await
            .expect("cancelled run count must query")
            .expect("cancelled run count must exist")
            .try_get::<i64>("", "value")
            .expect("cancelled run count must decode");
        assert_eq!(
            cancelled_count,
            workspace_ids.len() as i64,
            "every workspace must own and cancel exactly its logical run as bounded slots drain"
        );
    }

    #[tokio::test]
    async fn oldest_unresolved_run_blocks_only_its_workspace() {
        let (database, store, workspace_manager) = test_store().await;
        let other_workspace = workspace_manager
            .create_workspace("ws_unblocked_peer", Some("Unblocked peer"))
            .await
            .expect("peer workspace must create");
        let provider = Arc::new(ScriptedLearningProvider::new());
        provider.set_responses([
            serde_json::json!({
                "digestRevision": 1,
                "observations": []
            })
            .to_string(),
            serde_json::json!({ "candidate": null }).to_string(),
        ]);
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            provider.clone(),
        ));
        let enabled = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                provider: "learning".to_owned(),
                model: "learner".to_owned(),
            }),
            reviewer_model: None,
        };
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            BTreeMap::from([
                (WORKSPACE.to_owned(), enabled.clone()),
                (other_workspace.id.clone(), enabled),
            ]),
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("both workspaces must activate");
        project_completed_source_turn_for_workspace(
            store.as_ref(),
            WORKSPACE,
            "thread_blocked_workspace",
            "turn_blocked_workspace",
            "Blocked workspace evidence.",
            ENABLED_AT + 1,
        )
        .await;
        project_completed_source_turn_for_workspace(
            store.as_ref(),
            other_workspace.id.as_str(),
            "thread_unblocked_workspace",
            "turn_unblocked_workspace",
            "Unblocked workspace evidence.",
            ENABLED_AT + 1,
        )
        .await;

        let blocked_state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("blocked workspace state must query")
            .expect("blocked workspace state must exist");
        let blocked_sources = store
            .list_self_improvement_source_turns_after(WORKSPACE, 0, ENABLED_AT, 10)
            .await
            .expect("blocked workspace sources must query");
        let scheduled_date_utc = DateTime::<Utc>::from_timestamp(ENABLED_AT + 2, 0)
            .expect("fixture timestamp must be valid")
            .format("%Y-%m-%d")
            .to_string();
        let blocked_run = store
            .create_or_get_self_improvement_run(
                NewSelfImprovementRun {
                    workspace_id: WORKSPACE.to_owned(),
                    activation_epoch: blocked_state.activation_epoch,
                    scheduled_date_utc,
                    source_lower_exclusive: blocked_state.cursor_source_id,
                    source_upper_inclusive: blocked_sources[0].id,
                    learner_provider: "learning".to_owned(),
                    learner_model: "learner".to_owned(),
                    reviewer_provider: "learning".to_owned(),
                    reviewer_model: "learner".to_owned(),
                    pipeline_contract_version: PIPELINE_CONTRACT_VERSION.to_owned(),
                },
                ENABLED_AT + 2,
            )
            .await
            .expect("blocked workspace run must freeze");
        store
            .claim_available_self_improvement_run(
                WORKSPACE,
                blocked_run.id.as_str(),
                blocked_state.activation_epoch,
                "other-live-owner",
                ENABLED_AT + 2,
                ENABLED_AT + 600,
            )
            .await
            .expect("blocked workspace claim must execute")
            .expect("blocked workspace claim must win");

        supervisor
            .wake_once(ENABLED_AT + 3)
            .await
            .expect("peer workspace must progress despite the unresolved run");
        assert_eq!(
            provider.requests().len(),
            1,
            "only the unblocked peer chunk analyzer should run; an empty digest skips synthesis"
        );
        let blocked_after = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("blocked state must query")
            .expect("blocked state must exist");
        assert_eq!(blocked_after.cursor_source_id, 0);
        let peer_after = store
            .get_self_improvement_workspace_state(other_workspace.id.as_str())
            .await
            .expect("peer state must query")
            .expect("peer state must exist");
        assert!(
            peer_after.cursor_source_id > 0,
            "the peer range must complete independently"
        );
        let blocked_run_count = database
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!(
                    "SELECT COUNT(*) AS value FROM self_improvement_run WHERE workspace_id = \
                     '{WORKSPACE}'"
                ),
            ))
            .await
            .expect("blocked run count must query")
            .expect("blocked run count must exist")
            .try_get::<i64>("", "value")
            .expect("blocked run count must decode");
        assert_eq!(
            blocked_run_count, 1,
            "the unresolved workspace must not freeze another source range"
        );
    }

    #[tokio::test]
    async fn provider_failure_and_history_remain_isolated_to_one_workspace() {
        let (_database, store, workspace_manager) = test_store().await;
        let peer = workspace_manager
            .create_workspace("ws_provider_peer", Some("Provider peer"))
            .await
            .expect("peer workspace must create");
        let provider = Arc::new(WorkspaceSelectiveLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "workspace-selective-learning",
            provider.clone(),
        ));
        let enabled = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                provider: "workspace-selective-learning".to_owned(),
                model: "learner".to_owned(),
            }),
            reviewer_model: None,
        };
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            BTreeMap::from([
                (WORKSPACE.to_owned(), enabled.clone()),
                (peer.id.clone(), enabled),
            ]),
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("both workspace baselines must reconcile");
        project_completed_source_turn_for_workspace(
            store.as_ref(),
            WORKSPACE,
            "thread_provider_failure",
            "turn_provider_failure",
            "FAIL_WORKSPACE_SENTINEL",
            ENABLED_AT + 1,
        )
        .await;
        project_completed_source_turn_for_workspace(
            store.as_ref(),
            peer.id.as_str(),
            "thread_provider_peer",
            "turn_provider_peer",
            "READY_WORKSPACE_SENTINEL",
            ENABLED_AT + 1,
        )
        .await;

        supervisor
            .wake_once(ENABLED_AT + 2)
            .await
            .expect("one workspace failure must not fail the owned wake");

        let failed_state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("failed workspace state must query")
            .expect("failed workspace state must exist");
        assert_eq!(
            failed_state.cursor_source_id, 0,
            "provider failure must not advance its workspace cursor"
        );
        let failed_run = store
            .get_oldest_unresolved_self_improvement_run(WORKSPACE, failed_state.activation_epoch)
            .await
            .expect("failed workspace run must query")
            .expect("failed workspace run must remain retryable");
        assert_eq!(failed_run.status, RUN_STATUS_PENDING);
        assert!(failed_run.next_attempt_at_unix.is_some());

        let peer_state = store
            .get_self_improvement_workspace_state(peer.id.as_str())
            .await
            .expect("peer workspace state must query")
            .expect("peer workspace state must exist");
        assert!(
            peer_state.cursor_source_id > 0,
            "ready peer must complete independently"
        );
        assert!(
            store
                .get_oldest_unresolved_self_improvement_run(
                    peer.id.as_str(),
                    peer_state.activation_epoch,
                )
                .await
                .expect("peer unresolved run lookup must succeed")
                .is_none()
        );

        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        let failed_payload = requests
            .iter()
            .find(|request| {
                request
                    .messages
                    .iter()
                    .any(|message| message.content.contains("FAIL_WORKSPACE_SENTINEL"))
            })
            .expect("failing workspace request must be observed");
        assert!(
            !failed_payload
                .messages
                .iter()
                .any(|message| message.content.contains("READY_WORKSPACE_SENTINEL")),
            "peer history must not enter the failing workspace request"
        );
        let peer_payload = requests
            .iter()
            .find(|request| {
                request
                    .messages
                    .iter()
                    .any(|message| message.content.contains("READY_WORKSPACE_SENTINEL"))
            })
            .expect("peer workspace request must be observed");
        assert!(
            !peer_payload
                .messages
                .iter()
                .any(|message| message.content.contains("FAIL_WORKSPACE_SENTINEL")),
            "failing workspace history must not enter the peer request"
        );
        assert!(
            store
                .list_active_agent_skill_versions(WORKSPACE)
                .await
                .expect("failed workspace skill lookup must succeed")
                .is_empty()
        );
        assert!(
            store
                .list_active_agent_skill_versions(peer.id.as_str())
                .await
                .expect("peer workspace skill lookup must succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn provider_output_after_lease_takeover_is_discarded() {
        let (database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ReleasableLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "releasable-learning",
            provider.clone(),
        ));
        let supervisor = Arc::new(test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "releasable-learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        ));
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("workspace must activate");
        project_completed_source_turn(
            store.as_ref(),
            "thread_stale_post_call",
            "turn_stale_post_call",
            "Repeated evidence must not outlive lease authority.",
            ENABLED_AT + 1,
        )
        .await;

        let started = provider.started.notified();
        let wake = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.wake_once(ENABLED_AT + 2).await })
        };
        timeout(Duration::from_secs(2), started)
            .await
            .expect("provider call must begin");
        let running = store
            .get_oldest_unresolved_self_improvement_run(WORKSPACE, 1)
            .await
            .expect("running row must query")
            .expect("running row must exist");
        let stale_fence = running.fence().expect("running row must have a fence");

        database
            .execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE self_improvement_run SET lease_expires_at = datetime(?, 'unixepoch') \
                 WHERE id = ?",
                [(ENABLED_AT + 2).into(), running.id.clone().into()],
            ))
            .await
            .expect("lease takeover fixture must expire the first owner");
        let takeover_store = CrudStore::new(database.clone());
        let takeover = takeover_store
            .claim_available_self_improvement_run(
                WORKSPACE,
                running.id.as_str(),
                running.activation_epoch,
                "gateway-takeover",
                ENABLED_AT + 3,
                ENABLED_AT + RUN_LEASE_SECONDS,
            )
            .await
            .expect("takeover claim must execute")
            .expect("expired provider call owner must be reclaimed");
        assert_ne!(
            takeover.claim_token.as_deref(),
            Some(stale_fence.claim_token.as_str())
        );

        provider.release.notify_one();
        timeout(Duration::from_secs(2), wake)
            .await
            .expect("stale wake must terminate")
            .expect("stale wake task must join")
            .expect("workspace dispatch reports failures without failing the wake");
        assert_eq!(
            provider.request_count(),
            1,
            "stale output must be rejected before synthesis"
        );
        let persisted = takeover_store
            .get_self_improvement_run(WORKSPACE, running.id.as_str())
            .await
            .expect("takeover run must query")
            .expect("takeover run must exist");
        assert_eq!(persisted.status, "running");
        assert_eq!(persisted.claimed_by.as_deref(), Some("gateway-takeover"));
        assert_eq!(persisted.analysis_cursor_json, None);
        assert_eq!(
            store
                .get_self_improvement_workspace_state(WORKSPACE)
                .await
                .expect("workspace state must query")
                .expect("workspace state must exist")
                .cursor_source_id,
            0
        );
        assert!(
            store
                .list_active_agent_skill_versions(WORKSPACE)
                .await
                .expect("skill rows must query")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn frozen_range_load_failure_enters_bounded_retry_instead_of_timer_spin() {
        let (database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            provider.clone(),
        ));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );
        let state = supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("workspace must activate");
        assert_eq!(state, vec![WORKSPACE.to_owned()]);
        project_completed_source_turn(
            store.as_ref(),
            "thread_missing_frozen_anchor",
            "turn_missing_frozen_anchor",
            "Evidence whose source ledger row becomes unavailable.",
            ENABLED_AT + 1,
        )
        .await;
        let workspace_state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("workspace state must query")
            .expect("workspace state must exist");
        let sources = store
            .list_self_improvement_source_turns_after(
                WORKSPACE,
                workspace_state.cursor_source_id,
                ENABLED_AT,
                10,
            )
            .await
            .expect("source range must query");
        let source_upper = sources.last().expect("source anchor must exist").id;
        let wake_at = ENABLED_AT + 2;
        let scheduled_date_utc = DateTime::<Utc>::from_timestamp(wake_at, 0)
            .expect("fixture timestamp")
            .format("%Y-%m-%d")
            .to_string();
        let run = store
            .create_or_get_self_improvement_run(
                NewSelfImprovementRun {
                    workspace_id: WORKSPACE.to_owned(),
                    activation_epoch: workspace_state.activation_epoch,
                    scheduled_date_utc,
                    source_lower_exclusive: workspace_state.cursor_source_id,
                    source_upper_inclusive: source_upper,
                    learner_provider: "learning".to_owned(),
                    learner_model: "learner".to_owned(),
                    reviewer_provider: "learning".to_owned(),
                    reviewer_model: "learner".to_owned(),
                    pipeline_contract_version: PIPELINE_CONTRACT_VERSION.to_owned(),
                },
                wake_at,
            )
            .await
            .expect("run must freeze");
        database
            .execute_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("DELETE FROM self_improvement_source_turn WHERE id = {source_upper}"),
            ))
            .await
            .expect("corrupt frozen anchor fixture must delete");

        supervisor
            .wake_once(wake_at)
            .await
            .expect("frozen-range failure must be handled by the claimed retry path");

        let persisted = store
            .get_self_improvement_run(WORKSPACE, run.id.as_str())
            .await
            .expect("run must query")
            .expect("run must remain durable");
        assert_eq!(persisted.status, RUN_STATUS_PENDING);
        assert_eq!(persisted.attempt_count, 1);
        assert_eq!(
            persisted.next_attempt_at_unix,
            Some(wake_at + RETRY_BACKOFF_BASE_SECONDS)
        );
        assert_eq!(
            persisted.last_error.as_deref(),
            Some("infrastructure:internal_operation_failed")
        );
        assert_eq!(provider.requests().len(), 0);
    }

    #[tokio::test]
    async fn infrastructure_retries_exhaust_and_next_daily_wake_reuses_the_same_range() {
        let (database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            provider.clone(),
        ));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("workspace must activate");
        project_completed_source_turn(
            store.as_ref(),
            "thread_retry_initial",
            "turn_retry_initial",
            "Initial retry range evidence.",
            ENABLED_AT + 1,
        )
        .await;

        supervisor
            .wake_once(ENABLED_AT + 2)
            .await
            .expect("first infrastructure failure must be handled");
        let first = store
            .get_oldest_unresolved_self_improvement_run(WORKSPACE, 1)
            .await
            .expect("first retry row must query")
            .expect("first retry row must exist");
        assert_eq!(first.status, RUN_STATUS_PENDING);
        assert_eq!(first.attempt_count, 1);
        assert_eq!(
            first.next_attempt_at_unix,
            Some(ENABLED_AT + 2 + RETRY_BACKOFF_BASE_SECONDS)
        );
        assert_eq!(
            first.last_error.as_deref(),
            Some("unknown:provider_transport_failed")
        );
        assert_eq!(
            store
                .get_next_self_improvement_retry_at()
                .await
                .expect("retry timer must query"),
            first.next_attempt_at_unix
        );
        let frozen_run_id = first.id.clone();
        let frozen_upper = first.source_upper_inclusive;

        project_completed_source_turn(
            store.as_ref(),
            "thread_retry_later",
            "turn_retry_later",
            "Later evidence must not change the frozen retry range.",
            ENABLED_AT + 3,
        )
        .await;
        supervisor
            .wake_once(ENABLED_AT + 4)
            .await
            .expect("wake before next_attempt_at must be harmless");
        assert_eq!(
            provider.requests().len(),
            1,
            "a pending run must not claim before its durable due time"
        );

        let second_attempt_at = ENABLED_AT + 2 + RETRY_BACKOFF_BASE_SECONDS;
        supervisor
            .wake_once(second_attempt_at)
            .await
            .expect("second infrastructure failure must be handled");
        let second = store
            .get_self_improvement_run(WORKSPACE, frozen_run_id.as_str())
            .await
            .expect("second retry row must query")
            .expect("second retry row must exist");
        assert_eq!(second.status, RUN_STATUS_PENDING);
        assert_eq!(second.attempt_count, 2);
        assert_eq!(second.source_upper_inclusive, frozen_upper);

        let third_attempt_at = second
            .next_attempt_at_unix
            .expect("second failure must schedule a third attempt");
        supervisor
            .wake_once(third_attempt_at)
            .await
            .expect("retry exhaustion must be handled");
        let failed = store
            .get_self_improvement_run(WORKSPACE, frozen_run_id.as_str())
            .await
            .expect("failed row must query")
            .expect("failed row must exist");
        assert_eq!(failed.status, RUN_STATUS_FAILED);
        assert_eq!(failed.attempt_count, MAX_INFRASTRUCTURE_ATTEMPTS);
        assert_eq!(failed.source_upper_inclusive, frozen_upper);
        assert_eq!(failed.next_attempt_at_unix, None);
        assert_eq!(
            store
                .get_self_improvement_workspace_state(WORKSPACE)
                .await
                .expect("retry workspace state must query")
                .expect("retry workspace state must exist")
                .cursor_source_id,
            0
        );
        assert!(
            store
                .list_active_agent_skill_versions(WORKSPACE)
                .await
                .expect("retry skill rows must query")
                .is_empty()
        );

        supervisor
            .wake_once(third_attempt_at + 60)
            .await
            .expect("same-day failed wake must remain blocked");
        assert_eq!(
            provider.requests().len(),
            MAX_INFRASTRUCTURE_ATTEMPTS as usize
        );

        provider.set_responses([
            serde_json::json!({
                "digestRevision": 1,
                "observations": []
            })
            .to_string(),
            serde_json::json!({ "candidate": null }).to_string(),
        ]);
        let next_day = third_attempt_at + 24 * 60 * 60;
        supervisor
            .wake_once(next_day)
            .await
            .expect("next daily wake must requeue and finish the same row");
        let completed = store
            .get_self_improvement_run(WORKSPACE, frozen_run_id.as_str())
            .await
            .expect("completed retry row must query")
            .expect("completed retry row must exist");
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.id, frozen_run_id);
        assert_eq!(completed.source_upper_inclusive, frozen_upper);
        assert_eq!(completed.attempt_count, 1);
        assert_eq!(
            database
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT COUNT(*) AS value FROM self_improvement_run".to_owned(),
                ))
                .await
                .expect("run count must query")
                .expect("run count must exist")
                .try_get::<i64>("", "value")
                .expect("run count must decode"),
            1
        );
        assert_eq!(
            store
                .get_self_improvement_workspace_state(WORKSPACE)
                .await
                .expect("completed retry state must query")
                .expect("completed retry state must exist")
                .cursor_source_id,
            frozen_upper
        );
    }

    #[tokio::test]
    async fn malformed_chunk_exhaustion_becomes_a_terminal_no_change_range() {
        let (database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        provider.set_responses([
            "{malformed".to_owned(),
            "{malformed".to_owned(),
            "{malformed".to_owned(),
        ]);
        let registry = Arc::new(ProviderRegistry::with_provider("learning", provider));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("workspace must activate");
        project_completed_source_turn(
            store.as_ref(),
            "thread_contract_failure",
            "turn_contract_failure",
            "Contract failures remain distinct from infrastructure.",
            ENABLED_AT + 1,
        )
        .await;
        supervisor
            .wake_once(ENABLED_AT + 2)
            .await
            .expect("contract failure must be durably classified");

        let completed = database
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT status, outcome, analysis_cursor_json, analysis_digest_json \
                 FROM self_improvement_run"
                    .to_owned(),
            ))
            .await
            .expect("contract-exhausted run must query")
            .expect("contract-exhausted run must exist");
        assert_eq!(
            completed
                .try_get::<String>("", "status")
                .expect("status must decode"),
            "completed"
        );
        assert_eq!(
            completed
                .try_get::<String>("", "outcome")
                .expect("outcome must decode"),
            "no_change"
        );
        assert!(
            completed
                .try_get::<Option<String>>("", "analysis_cursor_json")
                .expect("cursor must decode")
                .is_none()
        );
        assert!(
            completed
                .try_get::<Option<String>>("", "analysis_digest_json")
                .expect("digest must decode")
                .is_none()
        );
        assert_eq!(
            store
                .get_self_improvement_workspace_state(WORKSPACE)
                .await
                .expect("contract-exhausted state must query")
                .expect("contract-exhausted state must exist")
                .cursor_source_id,
            1
        );
    }

    #[tokio::test]
    async fn synthesis_review_contract_exhaustion_and_reviewer_rejection_are_terminal_no_change() {
        for mode in ["synthesis_contract", "review_contract", "reviewer_reject"] {
            let (database, store, workspace_manager) = test_store().await;
            let provider = Arc::new(ScriptedLearningProvider::new());
            let registry = Arc::new(ProviderRegistry::with_provider(
                "learning",
                provider.clone(),
            ));
            let supervisor = test_supervisor(
                store.clone(),
                registry,
                workspace_manager,
                GatewaySelfImprovementConfig {
                    enabled: true,
                    default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                        provider: "learning".to_owned(),
                        model: "learner".to_owned(),
                    }),
                    reviewer_model: None,
                },
                1024 * 1024,
            );
            supervisor
                .reconcile_all(ENABLED_AT)
                .await
                .expect("workspace activation");
            project_completed_source_turn(
                store.as_ref(),
                "thread_contract_one",
                "turn_contract_one",
                "Verify checksum before publishing one.",
                ENABLED_AT + 1,
            )
            .await;
            project_completed_source_turn(
                store.as_ref(),
                "thread_contract_two",
                "turn_contract_two",
                "Verify checksum before publishing two.",
                ENABLED_AT + 2,
            )
            .await;
            let sources = store
                .list_self_improvement_source_turns_after(
                    WORKSPACE,
                    0,
                    ENABLED_AT,
                    MAX_NEW_SOURCE_TURNS_PER_RUN,
                )
                .await
                .expect("contract source range");
            let upper = sources.last().expect("contract source").id;
            let frozen = SelfImprovementFrozenSourceRange::new(WORKSPACE, 0, upper, sources)
                .expect("contract frozen range");
            let canonical = store
                .list_canonical_turn_events_for_self_improvement(&frozen)
                .await
                .expect("contract canonical history");
            let event = |turn_id: &str| {
                canonical
                    .iter()
                    .find(|record| record.turn_id == turn_id)
                    .unwrap_or_else(|| panic!("missing contract event for {turn_id}"))
                    .event_id
                    .clone()
            };
            let analysis = serde_json::json!({
                "digestRevision": 1,
                "observations": [{
                    "observationKey": "contract-observation",
                    "summary": "Releases verify checksums.",
                    "evidence": [
                        {
                            "turnId": "turn_contract_one",
                            "eventId": event("turn_contract_one"),
                            "excerpt": "Verify checksum"
                        },
                        {
                            "turnId": "turn_contract_two",
                            "eventId": event("turn_contract_two"),
                            "excerpt": "Verify checksum"
                        }
                    ],
                    "kind": "success_pattern"
                }]
            })
            .to_string();
            let candidate = serde_json::json!({
                "candidate": {
                    "action": "create",
                    "candidateKey": "contract-candidate",
                    "observationKeys": ["contract-observation"],
                    "name": "Verify checksum",
                    "slug": "verify-checksum",
                    "whenToUse": "Publishing a release",
                    "whenNotToUse": "No artifact is published",
                    "instructions": "Verify the checksum before publishing."
                }
            })
            .to_string();
            let (responses, expected_reason, expected_requests) = match mode {
                "synthesis_contract" => (
                    vec![analysis, "{malformed".to_owned(), "{malformed".to_owned()],
                    "model_contract_rejected",
                    3,
                ),
                "review_contract" => (
                    vec![
                        analysis,
                        candidate,
                        "{malformed".to_owned(),
                        "{malformed".to_owned(),
                    ],
                    "model_contract_rejected",
                    4,
                ),
                "reviewer_reject" => (
                    vec![
                        analysis,
                        candidate,
                        serde_json::json!({
                            "candidateKey": "candidate-733ae3bf97038400036ad29d7f210cfb801016d0f5598c90b0bc2e0286479ab7",
                            "decision": "reject",
                            "reasonCodes": ["not_general_enough"]
                        })
                        .to_string(),
                    ],
                    "reviewer_rejected",
                    3,
                ),
                _ => unreachable!(),
            };
            provider.set_responses(responses);
            supervisor
                .wake_once(ENABLED_AT + 10)
                .await
                .expect("terminal no-change wake");

            assert_eq!(provider.requests().len(), expected_requests);
            assert_eq!(
                store
                    .get_self_improvement_workspace_state(WORKSPACE)
                    .await
                    .expect("contract workspace state")
                    .expect("contract workspace row")
                    .cursor_source_id,
                upper
            );
            assert!(
                store
                    .list_active_agent_skill_versions(WORKSPACE)
                    .await
                    .expect("contract skill rows")
                    .is_empty()
            );
            let row = database
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT status, outcome, result_summary FROM self_improvement_run".to_owned(),
                ))
                .await
                .expect("contract run query")
                .expect("contract run row");
            assert_eq!(
                row.try_get::<String>("", "status").expect("status"),
                "completed"
            );
            assert_eq!(
                row.try_get::<String>("", "outcome").expect("outcome"),
                "no_change"
            );
            let summary: serde_json::Value = serde_json::from_str(
                row.try_get::<String>("", "result_summary")
                    .expect("result summary")
                    .as_str(),
            )
            .expect("summary JSON");
            assert_eq!(summary["reason"], expected_reason);
        }
    }

    #[tokio::test]
    async fn multi_chunk_driver_resumes_after_rejected_middle_chunk_without_tail_loss() {
        let (_database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            provider.clone(),
        ));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("workspace must activate");

        const FINAL_TAIL: &str = "VERY_LONG_FINAL_FRAGMENT_SENTINEL";
        let long_history = format!(
            "{}{FINAL_TAIL}",
            "x".repeat(super::super::history::HISTORY_CHUNK_MAX_SERIALIZED_BYTES.saturating_mul(4))
        );
        project_completed_source_turn(
            store.as_ref(),
            "thread_long_resume",
            "turn_long_resume",
            long_history.as_str(),
            ENABLED_AT + 1,
        )
        .await;

        let state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("workspace state must query")
            .expect("workspace state must exist");
        let sources = store
            .list_self_improvement_source_turns_after(WORKSPACE, 0, ENABLED_AT, 10)
            .await
            .expect("long source range must query");
        let upper = sources.last().expect("long source must exist").id;
        let frozen = SelfImprovementFrozenSourceRange::new(WORKSPACE, 0, upper, sources)
            .expect("long source range must freeze");
        let canonical = store
            .list_canonical_turn_events_for_self_improvement(&frozen)
            .await
            .expect("long canonical history must load");
        let snapshot = build_model_safe_full_thread_snapshot(&frozen, canonical.as_slice())
            .expect("long canonical snapshot must build");
        let chunks = plan_history_chunks(&snapshot, HistoryChunkLimits::default())
            .expect("long history must plan");
        assert!(
            chunks.len() > usize::try_from(MAX_CHUNK_STEPS_PER_WAKE).unwrap_or(usize::MAX),
            "fixture must require more than one bounded wake"
        );
        assert!(
            chunks
                .last()
                .and_then(|chunk| serde_json::to_string(chunk).ok())
                .is_some_and(|chunk| chunk.contains(FINAL_TAIL)),
            "the final planned fragment must contain the tail sentinel"
        );

        let mut responses = Vec::new();
        responses.push(serde_json::json!({"digestRevision": 1, "observations": []}).to_string());
        responses.extend([
            "{malformed-middle".to_owned(),
            "{malformed-middle".to_owned(),
            "{malformed-middle".to_owned(),
        ]);
        for chunk_index in 2..chunks.len() {
            responses.push(
                serde_json::json!({
                    "digestRevision": u32::try_from(chunk_index)
                        .expect("fixture revision must fit u32"),
                    "observations": []
                })
                .to_string(),
            );
        }
        provider.set_responses(responses);

        supervisor
            .wake_once(ENABLED_AT + 2)
            .await
            .expect("first bounded wake must checkpoint its exact prefix");
        let pending = store
            .get_oldest_unresolved_self_improvement_run(WORKSPACE, state.activation_epoch)
            .await
            .expect("pending run must query")
            .expect("pending run must remain");
        assert_eq!(pending.status, RUN_STATUS_PENDING);
        assert_eq!(pending.attempt_count, 0, "budget yield is not a failure");
        let cursor = pending
            .analysis_cursor_json
            .as_deref()
            .expect("budget yield must preserve its exact cursor");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(cursor).expect("cursor must be valid JSON")["nextChunkIndex"],
            serde_json::json!(2)
        );
        let digest = pending
            .analysis_digest_json
            .as_deref()
            .expect("budget yield must preserve its bounded digest");
        let checkpoint = ResumableHistoryAnalysis::restore(&pending, chunks.as_slice())
            .expect("bounded checkpoint and exact deterministic plan must restore");
        assert_eq!(checkpoint.next_chunk_index(), 2);
        assert_eq!(
            checkpoint
                .chunk_rejection_reason_code(1)
                .expect("middle chunk terminal marker must decode"),
            Some("malformed_model_json")
        );
        assert!(!digest.contains("malformed-middle"));
        assert!(!digest.contains(FINAL_TAIL));
        assert_eq!(
            provider.requests().len(),
            1 + MAX_CHUNK_CONTRACT_ATTEMPTS as usize
        );

        let run_id = pending.id.clone();
        let mut wake_at = pending
            .next_attempt_at_unix
            .expect("budget yield must schedule the nearest wake");
        for _ in 0..chunks.len() {
            supervisor
                .wake_once(wake_at)
                .await
                .expect("resumed bounded wake must execute");
            let current = store
                .get_self_improvement_run(WORKSPACE, run_id.as_str())
                .await
                .expect("resumed run must query")
                .expect("resumed run must exist");
            if current.status == "completed" {
                assert_eq!(current.analysis_cursor_json, None);
                assert_eq!(current.analysis_digest_json, None);
                break;
            }
            assert_eq!(current.status, RUN_STATUS_PENDING);
            wake_at = current
                .next_attempt_at_unix
                .expect("continued run must schedule its next bounded wake");
        }

        let completed = store
            .get_self_improvement_run(WORKSPACE, run_id.as_str())
            .await
            .expect("completed long run must query")
            .expect("completed long run must exist");
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.outcome.as_deref(), Some("no_change"));
        assert_eq!(
            store
                .get_self_improvement_workspace_state(WORKSPACE)
                .await
                .expect("completed state must query")
                .expect("completed state must exist")
                .cursor_source_id,
            upper
        );
        let requests = provider.requests();
        assert_eq!(requests.len(), chunks.len() + 2);
        let requested_chunk_indexes = requests
            .iter()
            .map(|request| {
                let data = serde_json::from_str::<serde_json::Value>(
                    request
                        .messages
                        .get(1)
                        .expect("chunk request must contain data")
                        .content
                        .as_str(),
                )
                .expect("chunk request data must be JSON");
                data["history"]["chunk_index"]
                    .as_u64()
                    .expect("chunk index must be present")
            })
            .collect::<Vec<_>>();
        let mut expected_indexes = vec![0, 1, 1, 1];
        expected_indexes.extend(
            (2..chunks.len()).map(|index| u64::try_from(index).expect("fixture index must fit")),
        );
        assert_eq!(
            requested_chunk_indexes, expected_indexes,
            "resume may only repeat the contract-retried chunk, never a committed prefix"
        );
        let last_input = requests
            .last()
            .and_then(|request| request.messages.get(1))
            .expect("last chunk request must contain untrusted data");
        assert!(
            last_input.content.contains(FINAL_TAIL),
            "the final fragment must reach the provider after resume"
        );
    }

    #[tokio::test]
    async fn restart_after_checkpoint_repeats_only_the_uncommitted_chunk() {
        let (database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            provider.clone(),
        ));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("workspace must activate");
        let long_history = "checkpoint-crash ".repeat(
            super::super::history::HISTORY_CHUNK_MAX_SERIALIZED_BYTES.saturating_mul(3) / 17,
        );
        project_completed_source_turn(
            store.as_ref(),
            "thread_checkpoint_crash",
            "turn_checkpoint_crash",
            long_history.as_str(),
            ENABLED_AT + 1,
        )
        .await;

        let state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("workspace state must query")
            .expect("workspace state must exist");
        let sources = store
            .list_self_improvement_source_turns_after(WORKSPACE, 0, ENABLED_AT, 10)
            .await
            .expect("checkpoint sources must query");
        let upper = sources.last().expect("checkpoint source must exist").id;
        let frozen = SelfImprovementFrozenSourceRange::new(WORKSPACE, 0, upper, sources)
            .expect("checkpoint range must freeze");
        let canonical = store
            .list_canonical_turn_events_for_self_improvement(&frozen)
            .await
            .expect("checkpoint canonical history must load");
        let snapshot = build_model_safe_full_thread_snapshot(&frozen, canonical.as_slice())
            .expect("checkpoint snapshot must build");
        let chunks = plan_history_chunks(&snapshot, HistoryChunkLimits::default())
            .expect("checkpoint chunks must plan");
        assert!(chunks.len() > 1, "fixture must create multiple chunks");

        provider.set_responses([serde_json::json!({
            "digestRevision": 1,
            "observations": []
        })
        .to_string()]);
        supervisor
            .wake_once(ENABLED_AT + 2)
            .await
            .expect("provider interruption must remain contained to the workspace");
        let pending = store
            .get_oldest_unresolved_self_improvement_run(WORKSPACE, state.activation_epoch)
            .await
            .expect("interrupted run must query")
            .expect("interrupted run must remain retryable");
        assert_eq!(pending.status, RUN_STATUS_PENDING);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                pending
                    .analysis_cursor_json
                    .as_deref()
                    .expect("first committed chunk must be checkpointed")
            )
            .expect("checkpoint cursor must decode")["nextChunkIndex"],
            serde_json::json!(1)
        );
        let first_attempt = provider.requests();
        assert_eq!(
            first_attempt.len(),
            2,
            "the second provider call is the simulated uncommitted interruption"
        );
        let request_payload = |request: &ChatRequest| {
            request
                .messages
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        };
        let committed_chunk_payload = request_payload(&first_attempt[0]);
        let interrupted_chunk_payload = request_payload(&first_attempt[1]);
        assert_ne!(committed_chunk_payload, interrupted_chunk_payload);

        provider.set_responses((1..chunks.len()).map(|revision| {
            serde_json::json!({
                "digestRevision": u32::try_from(revision + 1)
                    .expect("fixture revision must fit u32"),
                "observations": []
            })
            .to_string()
        }));
        let run_id = pending.id.clone();
        for _ in 0..=chunks.len() {
            let current = store
                .get_self_improvement_run(WORKSPACE, run_id.as_str())
                .await
                .expect("resumed run must query")
                .expect("resumed run must exist");
            if current.status == "completed" {
                break;
            }
            supervisor
                .wake_once(
                    current
                        .next_attempt_at_unix
                        .unwrap_or(current.updated_at_unix.saturating_add(1)),
                )
                .await
                .expect("resumed checkpoint wake must execute");
        }

        let completed = store
            .get_self_improvement_run(WORKSPACE, run_id.as_str())
            .await
            .expect("completed run must query")
            .expect("completed run must exist");
        assert_eq!(completed.status, "completed");
        let all_requests = provider.requests();
        assert_eq!(
            all_requests.len(),
            chunks.len() + 1,
            "only the uncommitted provider call may repeat"
        );
        let all_payloads = all_requests.iter().map(request_payload).collect::<Vec<_>>();
        assert_eq!(
            all_payloads
                .iter()
                .filter(|payload| **payload == committed_chunk_payload)
                .count(),
            1,
            "the committed prefix must never replay"
        );
        assert_eq!(
            all_payloads
                .iter()
                .filter(|payload| **payload == interrupted_chunk_payload)
                .count(),
            2,
            "only the last uncommitted chunk may be retried"
        );
        assert_eq!(
            store
                .get_self_improvement_workspace_state(WORKSPACE)
                .await
                .expect("completed state must query")
                .expect("completed state must exist")
                .cursor_source_id,
            upper
        );
        assert_eq!(
            database
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT COUNT(*) AS value FROM self_improvement_run".to_owned(),
                ))
                .await
                .expect("run count must query")
                .expect("run count must exist")
                .try_get::<i64>("", "value")
                .expect("run count must decode"),
            1,
            "restart must reuse the same frozen daily run"
        );
    }

    #[tokio::test]
    async fn workspace_scoped_reconciliation_is_idempotent_and_reenable_baselines_disabled_history()
    {
        let (_database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider("learning", provider));
        let workspace_manager_for_new_workspace = workspace_manager.clone();
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig::default(),
            1024 * 1024,
        );

        project_completed_source_turn(
            store.as_ref(),
            "thread_before_enable",
            "turn_before_enable",
            FIRST_TEXT,
            ENABLED_AT - 1,
        )
        .await;
        supervisor
            .wake_once(ENABLED_AT)
            .await
            .expect("disabled startup reconciliation must succeed");
        let inactive = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("inactive state lookup must succeed")
            .expect("inactive state must exist");
        assert_eq!(inactive.activation_epoch, 0);
        assert_eq!(inactive.cursor_source_id, 0);
        assert!(inactive.effective_enabled_at_unix.is_none());

        let enabled = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                provider: "learning".to_owned(),
                model: "learner".to_owned(),
            }),
            reviewer_model: None,
        };
        supervisor
            .apply_desired_for_workspace(WORKSPACE, enabled.clone(), ENABLED_AT + 1)
            .await
            .expect("live activation must reconcile before returning");
        let first_epoch = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("active state lookup must succeed")
            .expect("active state must exist");
        assert_eq!(first_epoch.activation_epoch, 1);
        assert_eq!(first_epoch.cursor_source_id, 1);
        assert_eq!(first_epoch.effective_enabled_at_unix, Some(ENABLED_AT + 1));

        let new_workspace = workspace_manager_for_new_workspace
            .create_workspace("ws_created_while_enabled", Some("Created while enabled"))
            .await
            .expect("workspace creation must succeed");
        assert!(
            supervisor
                .load_overlay_for_new_turn(new_workspace.id.as_str())
                .await
                .expect("first-turn reconciliation must succeed")
                .is_empty()
        );
        assert!(
            store
                .get_self_improvement_workspace_state(new_workspace.id.as_str())
                .await
                .expect("new workspace state query must succeed")
                .is_none(),
            "a workspace without its own setting must remain untouched"
        );
        supervisor
            .apply_desired_for_workspace(new_workspace.id.as_str(), enabled.clone(), ENABLED_AT + 2)
            .await
            .expect("the new workspace must be independently activatable");
        let new_workspace_state = store
            .get_self_improvement_workspace_state(new_workspace.id.as_str())
            .await
            .expect("new workspace state query must succeed")
            .expect("new workspace must activate after its own setting changes");
        assert_eq!(new_workspace_state.activation_epoch, 1);
        assert_eq!(new_workspace_state.cursor_source_id, 0);
        assert!(new_workspace_state.effective_enabled_at_unix.is_some());

        project_completed_source_turn(
            store.as_ref(),
            "thread_active",
            "turn_active",
            SECOND_TEXT,
            ENABLED_AT + 2,
        )
        .await;
        supervisor
            .apply_desired_for_workspace(WORKSPACE, enabled.clone(), ENABLED_AT + 3)
            .await
            .expect("idempotent activation must reconcile");
        let still_first_epoch = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("idempotent state lookup must succeed")
            .expect("idempotent state must exist");
        assert_eq!(still_first_epoch.activation_epoch, 1);
        assert_eq!(
            still_first_epoch.cursor_source_id, 1,
            "already-active reconciliation must not move the cursor"
        );

        supervisor
            .apply_desired_for_workspace(
                WORKSPACE,
                GatewaySelfImprovementConfig {
                    enabled: false,
                    ..enabled.clone()
                },
                ENABLED_AT + 4,
            )
            .await
            .expect("disable must reconcile before returning");
        let disabled = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("disabled state lookup must succeed")
            .expect("disabled state must exist");
        assert_eq!(disabled.activation_epoch, 1);
        assert!(disabled.effective_enabled_at_unix.is_none());

        project_completed_source_turn(
            store.as_ref(),
            "thread_while_disabled",
            "turn_while_disabled",
            FIRST_TEXT,
            ENABLED_AT + 5,
        )
        .await;
        supervisor
            .apply_desired_for_workspace(WORKSPACE, enabled, ENABLED_AT + 6)
            .await
            .expect("re-enable must reconcile before returning");
        let second_epoch = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("re-enabled state lookup must succeed")
            .expect("re-enabled state must exist");
        assert_eq!(second_epoch.activation_epoch, 2);
        assert_eq!(
            second_epoch.cursor_source_id, 3,
            "active and disabled-window history must be baselined on re-enable"
        );

        project_completed_source_turn(
            store.as_ref(),
            "thread_projected_late",
            "turn_projected_late",
            "This turn belongs to the disabled window despite late projection.",
            ENABLED_AT + 5,
        )
        .await;
        assert!(
            store
                .list_self_improvement_source_turns_after(
                    WORKSPACE,
                    second_epoch.cursor_source_id,
                    second_epoch
                        .effective_enabled_at_unix
                        .expect("re-enabled state must carry an effective timestamp"),
                    10,
                )
                .await
                .expect("late-projected source lookup must succeed")
                .is_empty(),
            "a turn completed while disabled must stay excluded even if projected after re-enable"
        );

        supervisor
            .apply_desired_for_workspace(
                WORKSPACE,
                GatewaySelfImprovementConfig {
                    enabled: true,
                    default_model: None,
                    reviewer_model: None,
                },
                ENABLED_AT + 7,
            )
            .await
            .expect("incomplete desired state must reconcile inactive");
        let incomplete = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("incomplete state lookup must succeed")
            .expect("incomplete state must exist");
        assert_eq!(incomplete.activation_epoch, 2);
        assert!(incomplete.effective_enabled_at_unix.is_none());
    }

    #[tokio::test]
    async fn enabled_startup_reconciliation_baselines_the_uncertain_window() {
        let (_database, store, workspace_manager) = test_store().await;
        project_completed_source_turn(
            store.as_ref(),
            "thread_uncertain_startup",
            "turn_uncertain_startup",
            "Persisted Settings may have committed before the previous process stopped.",
            ENABLED_AT,
        )
        .await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            provider.clone(),
        ));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );

        supervisor
            .reconcile_all(ENABLED_AT + 1)
            .await
            .expect("enabled startup reconciliation must complete before dispatch");
        let state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("startup state must query")
            .expect("startup state must exist");
        assert_eq!(state.activation_epoch, 1);
        assert_eq!(
            state.cursor_source_id, 1,
            "uncertain-window history must enter the privacy-safe startup baseline"
        );
        assert_eq!(state.effective_enabled_at_unix, Some(ENABLED_AT + 1));

        supervisor
            .wake_once(ENABLED_AT + 2)
            .await
            .expect("post-reconciliation wake must be safe");
        assert!(
            provider.requests().is_empty(),
            "startup-baselined history must never be sent retroactively"
        );
        assert!(
            store
                .get_oldest_unresolved_self_improvement_run(WORKSPACE, state.activation_epoch)
                .await
                .expect("startup run lookup must succeed")
                .is_none(),
            "startup baseline must not create a run"
        );
        assert!(
            supervisor
                .load_overlay_for_new_turn(WORKSPACE)
                .await
                .expect("startup-gated overlay lookup must succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn startup_reconciliation_uses_the_live_settings_transition_gate() {
        let (_database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider("learning", provider));
        let supervisor = Arc::new(test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig::default(),
            1024 * 1024,
        ));

        let overlay_transition = supervisor.transition_gate.read().await;
        let mut startup = Box::pin(supervisor.start());
        assert!(
            timeout(Duration::from_secs(1), &mut startup).await.is_err(),
            "startup must remain blocked while a new-turn transition is active"
        );
        assert!(
            store
                .get_self_improvement_workspace_state(WORKSPACE)
                .await
                .expect("workspace state query must succeed")
                .is_none(),
            "startup reconciliation must wait for the same transition gate as new-turn overlays"
        );

        drop(overlay_transition);
        timeout(Duration::from_secs(5), startup)
            .await
            .expect("startup must resume after the transition finishes")
            .expect("startup reconciliation must succeed");
        assert!(
            store
                .get_self_improvement_workspace_state(WORKSPACE)
                .await
                .expect("reconciled workspace state query must succeed")
                .is_some()
        );
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn disabled_startup_reconciliation_closes_a_stale_active_db_state() {
        let (_database, store, workspace_manager) = test_store().await;
        let stale_active = store
            .activate_self_improvement_workspace(WORKSPACE, ENABLED_AT - 1)
            .await
            .expect("stale active state fixture must commit");
        assert_eq!(stale_active.activation_epoch, 1);
        assert!(stale_active.effective_enabled_at_unix.is_some());

        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider("learning", provider));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig::default(),
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("disabled startup reconciliation must succeed");

        let reconciled = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("reconciled state lookup must succeed")
            .expect("reconciled state must exist");
        assert_eq!(
            reconciled.activation_epoch, 1,
            "deactivation must not invent an activation epoch"
        );
        assert!(reconciled.effective_enabled_at_unix.is_none());
        assert!(
            load_active_agent_skill_overlay(store.as_ref(), WORKSPACE)
                .await
                .expect("startup-gated overlay lookup must succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn disable_cancels_an_inflight_provider_call_and_its_durable_run() {
        let (database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(BlockingLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "blocking-learning",
            provider.clone(),
        ));
        let enabled = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                provider: "blocking-learning".to_owned(),
                model: "learner".to_owned(),
            }),
            reviewer_model: None,
        };
        let supervisor = Arc::new(test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            enabled.clone(),
            1024 * 1024,
        ));
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("initial activation must reconcile");
        project_completed_source_turn(
            store.as_ref(),
            "thread_inflight",
            "turn_inflight",
            FIRST_TEXT,
            ENABLED_AT + 1,
        )
        .await;

        let started = provider.started.notified();
        let wake = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.wake_once(ENABLED_AT + 2).await })
        };
        timeout(Duration::from_secs(5), started)
            .await
            .expect("learner provider call must start");
        let old_fence = store
            .get_oldest_unresolved_self_improvement_run(WORKSPACE, 1)
            .await
            .expect("inflight run must query")
            .expect("inflight run must exist")
            .fence()
            .expect("inflight run must expose its old fence");

        supervisor
            .apply_desired_for_workspace(
                WORKSPACE,
                GatewaySelfImprovementConfig {
                    enabled: false,
                    ..enabled
                },
                ENABLED_AT + 3,
            )
            .await
            .expect("disable must cancel and reconcile");
        timeout(Duration::from_secs(5), wake)
            .await
            .expect("cancelled wake must terminate")
            .expect("wake task must join")
            .expect("cancelled wake must exit cleanly");

        let state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("disabled state must load")
            .expect("disabled state must exist");
        assert!(state.effective_enabled_at_unix.is_none());
        assert_eq!(
            state.cursor_source_id, 0,
            "disable during a provider call must not advance the source cursor"
        );
        assert!(
            supervisor
                .load_overlay_for_new_turn(WORKSPACE)
                .await
                .expect("disabled overlay lookup must succeed")
                .is_empty()
        );
        let run = database
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT status, claim_token, lease_expires_at, last_error \
                 FROM self_improvement_run"
                    .to_owned(),
            ))
            .await
            .expect("cancelled run query must execute")
            .expect("cancelled run must exist");
        assert_eq!(
            run.try_get::<String>("", "status")
                .expect("cancelled status must decode"),
            "cancelled"
        );
        assert!(
            run.try_get::<Option<String>>("", "claim_token")
                .expect("cancelled claim token must decode")
                .is_none()
        );
        assert!(
            run.try_get::<Option<String>>("", "lease_expires_at")
                .expect("cancelled lease must decode")
                .is_none()
        );
        assert_eq!(
            run.try_get::<Option<String>>("", "last_error")
                .expect("cancel reason must decode")
                .as_deref(),
            Some("self_improvement_disabled")
        );
        assert_eq!(
            store
                .heartbeat_self_improvement_run(
                    &old_fence,
                    ENABLED_AT + 4,
                    ENABLED_AT + 4 + RUN_LEASE_SECONDS,
                )
                .await
                .expect("old worker heartbeat must report authority"),
            SelfImprovementRunMutationResult::LostAuthority
        );
        assert_eq!(
            store
                .save_self_improvement_run_checkpoint(
                    &old_fence,
                    r#"{"nextChunkIndex":1}"#,
                    r#"{"digestRevision":1,"observations":[]}"#,
                    ENABLED_AT + 4,
                )
                .await
                .expect("old worker checkpoint must report authority"),
            SelfImprovementRunMutationResult::LostAuthority
        );
        assert_eq!(
            store
                .return_self_improvement_run_to_pending(
                    &old_fence,
                    ENABLED_AT + 4,
                    ENABLED_AT + 5,
                    "stale_after_disable",
                )
                .await
                .expect("old worker status transition must report authority"),
            SelfImprovementRunMutationResult::LostAuthority
        );
        assert!(
            store
                .list_active_agent_skill_versions(WORKSPACE)
                .await
                .expect("disabled skill lookup must succeed")
                .is_empty(),
            "stale provider output must not create an Agent version"
        );
    }

    #[tokio::test]
    async fn same_day_model_change_reuses_the_exact_run_and_rejects_the_old_fence() {
        let (database, store, workspace_manager) = test_store().await;
        let old_provider = Arc::new(ReleasableLearningProvider::new());
        let new_provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning-old",
            old_provider.clone(),
        ));
        registry.insert("learning-new", new_provider.clone());
        let old_config = GatewaySelfImprovementConfig {
            enabled: true,
            default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                provider: "learning-old".to_owned(),
                model: "old-model".to_owned(),
            }),
            reviewer_model: None,
        };
        let supervisor = Arc::new(test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            old_config,
            1024 * 1024,
        ));
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("old model state must activate");
        project_completed_source_turn(
            store.as_ref(),
            "thread_model_change",
            "turn_model_change",
            "The frozen range must survive a same-day model replacement.",
            ENABLED_AT + 1,
        )
        .await;

        let started = old_provider.started.notified();
        let old_wake = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.wake_once(ENABLED_AT + 2).await })
        };
        timeout(Duration::from_secs(5), started)
            .await
            .expect("old model call must start");
        let running = store
            .get_oldest_unresolved_self_improvement_run(WORKSPACE, 1)
            .await
            .expect("old-model run must query")
            .expect("old-model run must exist");
        let old_fence = running
            .fence()
            .expect("old-model run must expose its fence");
        let frozen_run_id = running.id.clone();
        let frozen_lower = running.source_lower_exclusive;
        let frozen_upper = running.source_upper_inclusive;
        let state_before = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("pre-change state must query")
            .expect("pre-change state must exist");

        supervisor
            .apply_desired_for_workspace(
                WORKSPACE,
                GatewaySelfImprovementConfig {
                    enabled: true,
                    default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                        provider: "learning-new".to_owned(),
                        model: "new-model".to_owned(),
                    }),
                    reviewer_model: None,
                },
                ENABLED_AT + 3,
            )
            .await
            .expect("model replacement must reconcile");
        timeout(Duration::from_secs(5), old_wake)
            .await
            .expect("old-model wake must stop")
            .expect("old-model wake task must join")
            .expect("old-model wake must exit cleanly");

        let reset = store
            .get_self_improvement_run(WORKSPACE, frozen_run_id.as_str())
            .await
            .expect("reset run must query")
            .expect("reset run must exist");
        assert_eq!(reset.status, RUN_STATUS_PENDING);
        assert_eq!(reset.id, frozen_run_id);
        assert_eq!(reset.source_lower_exclusive, frozen_lower);
        assert_eq!(reset.source_upper_inclusive, frozen_upper);
        assert_eq!(reset.activation_epoch, state_before.activation_epoch);
        assert_eq!(reset.learner_provider, "learning-new");
        assert_eq!(reset.learner_model, "new-model");
        assert_eq!(reset.reviewer_provider, "learning-new");
        assert_eq!(reset.reviewer_model, "new-model");
        assert_eq!(reset.pipeline_contract_version, PIPELINE_CONTRACT_VERSION);
        assert_eq!(reset.attempt_count, 0);
        assert_eq!(reset.claim_token, None);
        assert_eq!(reset.analysis_cursor_json, None);
        assert_eq!(reset.analysis_digest_json, None);
        assert_eq!(
            store
                .heartbeat_self_improvement_run(
                    &old_fence,
                    ENABLED_AT + 4,
                    ENABLED_AT + 4 + RUN_LEASE_SECONDS,
                )
                .await
                .expect("old-model heartbeat must report authority"),
            SelfImprovementRunMutationResult::LostAuthority
        );
        let state_after_reset = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("post-change state must query")
            .expect("post-change state must exist");
        assert_eq!(
            state_after_reset.activation_epoch,
            state_before.activation_epoch
        );
        assert_eq!(
            state_after_reset.cursor_source_id,
            state_before.cursor_source_id
        );

        new_provider.set_responses([
            serde_json::json!({
                "digestRevision": 1,
                "observations": []
            })
            .to_string(),
            serde_json::json!({ "candidate": null }).to_string(),
        ]);
        supervisor
            .wake_once(ENABLED_AT + 5)
            .await
            .expect("new model must finish the reset run");
        let completed = store
            .get_self_improvement_run(WORKSPACE, frozen_run_id.as_str())
            .await
            .expect("completed model-change run must query")
            .expect("completed model-change run must exist");
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.id, frozen_run_id);
        assert_eq!(completed.source_upper_inclusive, frozen_upper);
        assert_eq!(
            database
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT COUNT(*) AS value FROM self_improvement_run".to_owned(),
                ))
                .await
                .expect("model-change run count must query")
                .expect("model-change run count must exist")
                .try_get::<i64>("", "value")
                .expect("model-change run count must decode"),
            1
        );
        assert!(
            store
                .list_active_agent_skill_versions(WORKSPACE)
                .await
                .expect("model-change skill rows must query")
                .is_empty(),
            "discarded old output and no-change new output must create no version"
        );
    }

    #[tokio::test]
    async fn deploy_contract_mismatch_resets_checkpoint_and_reuses_the_frozen_range() {
        let (database, store, workspace_manager) = test_store().await;
        let provider = Arc::new(ScriptedLearningProvider::new());
        let registry = Arc::new(ProviderRegistry::with_provider(
            "learning",
            provider.clone(),
        ));
        let supervisor = test_supervisor(
            store.clone(),
            registry,
            workspace_manager,
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "learning".to_owned(),
                    model: "learner".to_owned(),
                }),
                reviewer_model: None,
            },
            1024 * 1024,
        );
        supervisor
            .reconcile_all(ENABLED_AT)
            .await
            .expect("contract fixture must activate");
        project_completed_source_turn(
            store.as_ref(),
            "thread_contract_reset",
            "turn_contract_reset",
            "A deploy contract change must restart the same frozen range.",
            ENABLED_AT + 1,
        )
        .await;
        let state = store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("contract state must query")
            .expect("contract state must exist");
        let sources = store
            .list_self_improvement_source_turns_after(
                WORKSPACE,
                state.cursor_source_id,
                state
                    .effective_enabled_at_unix
                    .expect("state must be active"),
                10,
            )
            .await
            .expect("contract sources must query");
        let old_contract_run = store
            .create_or_get_self_improvement_run(
                NewSelfImprovementRun {
                    workspace_id: WORKSPACE.to_owned(),
                    activation_epoch: state.activation_epoch,
                    scheduled_date_utc: DateTime::<Utc>::from_timestamp(ENABLED_AT + 2, 0)
                        .expect("fixture timestamp must be valid")
                        .format("%Y-%m-%d")
                        .to_string(),
                    source_lower_exclusive: state.cursor_source_id,
                    source_upper_inclusive: sources[0].id,
                    learner_provider: "learning".to_owned(),
                    learner_model: "learner".to_owned(),
                    reviewer_provider: "learning".to_owned(),
                    reviewer_model: "learner".to_owned(),
                    pipeline_contract_version: "self-improvement-v0".to_owned(),
                },
                ENABLED_AT + 2,
            )
            .await
            .expect("old-contract run must freeze");
        let claimed = store
            .claim_available_self_improvement_run(
                WORKSPACE,
                old_contract_run.id.as_str(),
                state.activation_epoch,
                "old-deploy",
                ENABLED_AT + 3,
                ENABLED_AT + 600,
            )
            .await
            .expect("old-contract run claim must execute")
            .expect("old-contract run claim must win");
        let old_fence = claimed
            .fence()
            .expect("old-contract claim must expose a fence");
        assert_eq!(
            store
                .save_self_improvement_run_checkpoint(
                    &old_fence,
                    r#"{"nextChunk":1}"#,
                    r#"{"digestRevision":1,"observations":[]}"#,
                    ENABLED_AT + 4,
                )
                .await
                .expect("old-contract checkpoint must save"),
            SelfImprovementRunMutationResult::Applied
        );

        supervisor
            .reconcile_all(ENABLED_AT + 5)
            .await
            .expect("new deploy contract must reconcile");
        let reset = store
            .get_self_improvement_run(WORKSPACE, old_contract_run.id.as_str())
            .await
            .expect("contract-reset run must query")
            .expect("contract-reset run must exist");
        assert_eq!(reset.status, RUN_STATUS_PENDING);
        assert_eq!(reset.id, old_contract_run.id);
        assert_eq!(
            reset.source_upper_inclusive,
            old_contract_run.source_upper_inclusive
        );
        assert_eq!(reset.activation_epoch, state.activation_epoch);
        assert_eq!(reset.pipeline_contract_version, PIPELINE_CONTRACT_VERSION);
        assert_eq!(reset.analysis_cursor_json, None);
        assert_eq!(reset.analysis_digest_json, None);
        assert_eq!(reset.claim_token, None);
        assert_eq!(reset.attempt_count, 0);
        assert_eq!(
            store
                .save_self_improvement_run_checkpoint(
                    &old_fence,
                    r#"{"nextChunk":2}"#,
                    r#"{"digestRevision":2,"observations":[]}"#,
                    ENABLED_AT + 6,
                )
                .await
                .expect("old-contract checkpoint must report authority"),
            SelfImprovementRunMutationResult::LostAuthority
        );

        provider.set_responses([
            serde_json::json!({
                "digestRevision": 1,
                "observations": []
            })
            .to_string(),
            serde_json::json!({ "candidate": null }).to_string(),
        ]);
        supervisor
            .wake_once(ENABLED_AT + 7)
            .await
            .expect("current contract must finish the reset range");
        let completed = store
            .get_self_improvement_run(WORKSPACE, old_contract_run.id.as_str())
            .await
            .expect("completed contract run must query")
            .expect("completed contract run must exist");
        assert_eq!(completed.status, "completed");
        assert_eq!(
            completed.pipeline_contract_version,
            PIPELINE_CONTRACT_VERSION
        );
        assert_eq!(
            database
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT COUNT(*) AS value FROM self_improvement_run".to_owned(),
                ))
                .await
                .expect("contract run count must query")
                .expect("contract run count must exist")
                .try_get::<i64>("", "value")
                .expect("contract run count must decode"),
            1
        );
    }

    #[tokio::test]
    async fn started_ordinary_turn_keeps_its_owned_agent_overlay_snapshot() {
        let skill_id =
            SkillId::new("PPPPPPPPPPPPPPPPPPPPP").expect("pinned skill ID must be valid");
        let provider = Arc::new(PinnedOverlayNativeProvider::new(skill_id.clone()));
        let registry = Arc::new(ProviderRegistry::with_provider(
            "pinned-native",
            provider.clone(),
        ));
        let manager = AgentManager::new(registry, agent_tool_loop_config());
        manager
            .ensure_thread("thread_pinned_overlay", WORKSPACE)
            .await
            .expect("pinned overlay thread must initialize");
        let mut events = manager
            .take_durable_receiver("thread_pinned_overlay")
            .await
            .expect("pinned overlay durable receiver must exist");
        let original_body = "Original instructions pinned when the turn starts.".to_owned();
        let mut external_entry = AgentSkillRuntimeEntry {
            skill_id: skill_id.clone(),
            slug: "pinned-overlay".to_owned(),
            version_id: "QQQQQQQQQQQQQQQQQQQQQ".to_owned(),
            version_number: 1,
            display_name: "Pinned overlay".to_owned(),
            runtime_description: "Use this pinned learned procedure.".to_owned(),
            body: original_body.clone(),
            fingerprint: "pinned-overlay-fingerprint".to_owned(),
        };
        let started = provider.started.notified();
        tokio::pin!(started);
        manager
            .start_turn_with_resolved_artifacts_environment_reasoning_permission_profile_security_snapshot_and_agent_skill_overlay(
                "thread_pinned_overlay",
                "turn_pinned_overlay",
                ThreadMode::Agent,
                "pinned-model",
                "pinned-native",
                HashMap::new(),
                SkillCatalogSnapshot {
                    version: 0,
                    generated_at_unix: ENABLED_AT,
                    skills: Vec::new(),
                },
                vec![external_entry.clone()],
                vec![UserInput::Text {
                    text: "Use the procedure pinned at turn start.".to_owned(),
                    text_elements: Vec::new(),
                }],
                Vec::new(),
                Vec::new(),
                HashMap::new(),
                Vec::new(),
                None,
                default_turn_permission_profile_snapshot(),
                TurnExecutionSecuritySnapshot::unrestricted_full_access(
                    "/workspace",
                    ENABLED_AT * 1000,
                ),
            )
            .await
            .expect("pinned overlay turn must start");
        timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    _ = &mut started => return,
                    event = events.recv() => {
                        let Some(event) = event else {
                            panic!("pinned overlay durable event lane closed");
                        };
                        events.acknowledge_last(Ok(()));
                        match event {
                            AgentDurableEvent::TurnFailed { error, .. } => {
                                panic!("pinned overlay turn failed before provider call: {error}")
                            }
                            AgentDurableEvent::TurnCompleted { .. } => {
                                panic!("pinned overlay turn completed before provider call")
                            }
                            _ => {}
                        }
                    }
                }
            }
        })
        .await
        .expect("pinned overlay provider call must start");

        external_entry.body = "Replacement instructions from later Settings state.".to_owned();
        external_entry.version_id = "RRRRRRRRRRRRRRRRRRRRR".to_owned();
        provider.release.notify_one();
        timeout(Duration::from_secs(5), async {
            loop {
                let Some(event) = events.recv().await else {
                    panic!("pinned overlay durable event lane closed");
                };
                events.acknowledge_last(Ok(()));
                match event {
                    AgentDurableEvent::TurnCompleted { .. } => return,
                    AgentDurableEvent::TurnFailed { error, .. } => {
                        panic!("pinned overlay turn failed: {error}")
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("pinned overlay turn must terminate");

        let requests = provider
            .requests()
            .into_iter()
            .filter(|request| request.compiled_prompt.is_some())
            .collect::<Vec<_>>();
        assert!(requests.len() >= 2);
        let read_result = requests[1]
            .messages
            .iter()
            .find(|message| {
                message.role == Role::Tool && message.name.as_deref() == Some("read_skill")
            })
            .expect("pinned turn must receive read_skill output");
        assert!(read_result.content.contains(original_body.as_str()));
        assert!(read_result.content.contains("QQQQQQQQQQQQQQQQQQQQQ"));
        assert!(!read_result.content.contains(external_entry.body.as_str()));
        assert!(
            !read_result
                .content
                .contains(external_entry.version_id.as_str())
        );
    }

    #[tokio::test]
    async fn disabled_or_incomplete_settings_cannot_call_models_create_runs_or_load_overlay() {
        for config in [
            GatewaySelfImprovementConfig::default(),
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: None,
                reviewer_model: None,
            },
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "local".to_owned(),
                    model: "local-model".to_owned(),
                }),
                reviewer_model: None,
            },
            GatewaySelfImprovementConfig {
                enabled: true,
                default_model: Some(GatewaySelfImprovementModelSelectionConfig {
                    provider: "cli_runtime:codex".to_owned(),
                    model: "codex-model".to_owned(),
                }),
                reviewer_model: None,
            },
        ] {
            let (database, store, workspace_manager) = test_store().await;
            project_completed_source_turn(
                store.as_ref(),
                "thread_disabled",
                "turn_disabled",
                FIRST_TEXT,
                ENABLED_AT + 1,
            )
            .await;
            let provider = Arc::new(ScriptedLearningProvider::new());
            let registry = Arc::new(ProviderRegistry::with_provider(
                "learning",
                provider.clone(),
            ));
            let supervisor = test_supervisor(
                store.clone(),
                registry,
                workspace_manager,
                config,
                1024 * 1024,
            );
            supervisor
                .wake_once(ENABLED_AT + 2)
                .await
                .expect("disabled reconciliation must succeed");

            assert!(provider.requests().is_empty());
            assert!(
                load_active_agent_skill_overlay(store.as_ref(), WORKSPACE)
                    .await
                    .expect("disabled overlay lookup must succeed")
                    .is_empty()
            );
            let run_count = database
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT COUNT(*) AS count FROM self_improvement_run".to_owned(),
                ))
                .await
                .expect("run count query must execute")
                .expect("run count row must exist")
                .try_get::<i64>("", "count")
                .expect("run count must decode");
            assert_eq!(run_count, 0);
        }
    }
}
