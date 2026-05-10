use crate::{
    HookAwaitPolicy, HookCapability, HookContext, HookContribution, HookContributionHash,
    HookDiagnostic, HookDiagnosticCode, HookDiagnosticMessage, HookDiagnosticPreview,
    HookDiagnosticRedactionPolicy, HookDiagnosticSeverity, HookError, HookFailurePolicy,
    HookHandler, HookHandlerRequest, HookHandlerResponse, HookId, HookInput, HookInputPayload,
    HookMetadata, HookMetadataKey, HookPhase, HookPolicySet, HookPromptContextSet,
    HookRecoverableRunRecord, HookRecoveryScan, HookRegistry, HookRegistryError, HookRetryBackoff,
    HookRetrySchedule, HookRunAttemptStoreCompletion, HookRunAttemptStoreRecord,
    HookRunIdempotencyKey, HookRunInputSnapshot, HookRunResumePayload, HookRunResumeState,
    HookRunScope, HookRunScopeKind, HookRunStore, HookRunStoreCompletion, HookRunStoreRecord,
    HookSubscription, HookSubscriptionId, HookSubscriptionRegistry, HookValue,
    NewHookAuditEventStoreRecord, NewHookRunAttemptStoreRecord, NewHookRunStoreRecord,
};
use futures_timer::Delay;
use futures_util::future::{Either, join_all, select};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub type HookRuntimeResult<T> = Result<T, HookRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookRuntimeError {
    Registry(HookRegistryError),
    MissingHandler {
        subscription_id: HookSubscriptionId,
        hook_id: HookId,
        phase: HookPhase,
    },
    HookFailed {
        subscription_id: HookSubscriptionId,
        hook_id: HookId,
        phase: HookPhase,
        error: HookError,
    },
    HookTimedOut {
        subscription_id: HookSubscriptionId,
        hook_id: HookId,
        phase: HookPhase,
        timeout_ms: u64,
    },
    HookFailedClosed {
        subscription_id: HookSubscriptionId,
        hook_id: HookId,
        phase: HookPhase,
        error: HookError,
    },
    MissingFallbackContribution {
        subscription_id: HookSubscriptionId,
        hook_id: HookId,
        phase: HookPhase,
    },
    InvalidExecutionPolicy {
        subscription_id: HookSubscriptionId,
        hook_id: HookId,
        phase: HookPhase,
        reason: String,
    },
    MissingDependency {
        subscription_id: HookSubscriptionId,
        dependency_id: HookSubscriptionId,
        phase: HookPhase,
    },
    DependencyCycle {
        phase: HookPhase,
        subscription_ids: Vec<HookSubscriptionId>,
    },
}

impl fmt::Display for HookRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => write!(formatter, "hook registry error: {}", error),
            Self::MissingHandler {
                subscription_id,
                hook_id,
                phase,
            } => write!(
                formatter,
                "hook subscription `{}` references missing handler `{}` for phase `{}`",
                subscription_id, hook_id, phase
            ),
            Self::HookFailed {
                subscription_id,
                hook_id,
                phase,
                error,
            } => write!(
                formatter,
                "hook subscription `{}` handler `{}` failed for phase `{}`: {}",
                subscription_id, hook_id, phase, error
            ),
            Self::HookTimedOut {
                subscription_id,
                hook_id,
                phase,
                timeout_ms,
            } => write!(
                formatter,
                "hook subscription `{}` handler `{}` timed out after {} ms for phase `{}`",
                subscription_id, hook_id, timeout_ms, phase
            ),
            Self::HookFailedClosed {
                subscription_id,
                hook_id,
                phase,
                error,
            } => write!(
                formatter,
                "hook subscription `{}` handler `{}` failed closed for phase `{}`: {}",
                subscription_id, hook_id, phase, error
            ),
            Self::MissingFallbackContribution {
                subscription_id,
                hook_id,
                phase,
            } => write!(
                formatter,
                "hook subscription `{}` handler `{}` requires fallback contributions for phase `{}`",
                subscription_id, hook_id, phase
            ),
            Self::InvalidExecutionPolicy {
                subscription_id,
                hook_id,
                phase,
                reason,
            } => write!(
                formatter,
                "hook subscription `{}` handler `{}` has invalid execution policy for phase `{}`: {}",
                subscription_id, hook_id, phase, reason
            ),
            Self::MissingDependency {
                subscription_id,
                dependency_id,
                phase,
            } => write!(
                formatter,
                "hook subscription `{}` references missing dependency `{}` for phase `{}`",
                subscription_id, dependency_id, phase
            ),
            Self::DependencyCycle {
                phase,
                subscription_ids,
            } => {
                write!(formatter, "hook dependency cycle for phase `{}`: ", phase)?;
                for (index, subscription_id) in subscription_ids.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{}", subscription_id)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for HookRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Registry(error) => Some(error),
            Self::HookFailed { error, .. } | Self::HookFailedClosed { error, .. } => Some(error),
            Self::MissingHandler { .. }
            | Self::HookTimedOut { .. }
            | Self::MissingFallbackContribution { .. }
            | Self::InvalidExecutionPolicy { .. }
            | Self::MissingDependency { .. }
            | Self::DependencyCycle { .. } => None,
        }
    }
}

impl From<HookRegistryError> for HookRuntimeError {
    fn from(error: HookRegistryError) -> Self {
        Self::Registry(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookRunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    TimedOut,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRunErrorSummary {
    pub code: HookDiagnosticCode,
    pub message: HookDiagnosticMessage,
    pub retryable: bool,
    pub safe_for_user: bool,
}

impl HookRunErrorSummary {
    pub fn from_error(error: &HookError, policy: &HookDiagnosticRedactionPolicy) -> Self {
        let diagnostic = HookDiagnostic {
            code: error.code.clone(),
            message: error.message.clone(),
            severity: HookDiagnosticSeverity::Error,
            safe_for_user: error.safe_for_user,
            metadata: HookMetadata::default(),
        };
        let preview = diagnostic.preview(policy);
        Self {
            code: error.code.clone(),
            message: preview.message,
            retryable: error.retryable,
            safe_for_user: preview.safe_for_user,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookAttemptSummary {
    pub attempt_number: u16,
    pub status: HookRunStatus,
    pub contribution_count: usize,
    pub diagnostic_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contribution_hashes: Vec<HookContributionHash>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<HookRunErrorSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRunSummary {
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub status: HookRunStatus,
    pub attempt_count: u16,
    pub contribution_count: usize,
    pub diagnostic_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contribution_hashes: Vec<HookContributionHash>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostic_previews: Vec<HookDiagnosticPreview>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<HookAttemptSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<HookRunErrorSummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookPhaseRequest {
    pub phase: HookPhase,
    pub context: HookContext,
    pub input: HookInput,
    #[serde(default, skip_serializing_if = "HookPolicySet::is_empty")]
    pub policy_set: HookPolicySet,
    #[serde(default, skip_serializing_if = "HookPromptContextSet::is_empty")]
    pub prompt_context_set: HookPromptContextSet,
}

impl HookPhaseRequest {
    pub fn new(phase: HookPhase, context: HookContext, input: HookInput) -> Self {
        Self {
            phase,
            context,
            input,
            policy_set: HookPolicySet::empty(),
            prompt_context_set: HookPromptContextSet::empty(),
        }
    }

    pub fn with_policy_set(mut self, policy_set: HookPolicySet) -> Self {
        self.policy_set = policy_set;
        self
    }

    pub fn with_prompt_context_set(mut self, prompt_context_set: HookPromptContextSet) -> Self {
        self.prompt_context_set = prompt_context_set;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookPhaseResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contributions: Vec<HookContribution>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<HookRunSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookBackgroundRunSummary {
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub await_policy: HookAwaitPolicy,
    pub status: HookRunStatus,
    pub contribution_count: usize,
    pub diagnostic_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookBackgroundDrainSummary {
    pub executed_count: usize,
    pub succeeded_count: usize,
    pub failed_count: usize,
    pub timed_out_count: usize,
    pub skipped_count: usize,
}

impl HookBackgroundDrainSummary {
    fn record(&mut self, run: &HookBackgroundRunSummary) {
        self.executed_count += 1;
        match run.status {
            HookRunStatus::Succeeded => self.succeeded_count += 1,
            HookRunStatus::Failed => self.failed_count += 1,
            HookRunStatus::TimedOut => self.timed_out_count += 1,
            HookRunStatus::Skipped => self.skipped_count += 1,
            HookRunStatus::Queued | HookRunStatus::Running => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRecoveryOptions {
    pub now_unix_ms: i64,
    pub batch_size: usize,
    pub max_concurrent: usize,
    pub stale_running_after_ms: u64,
    pub strict_debug: bool,
}

impl Default for HookRecoveryOptions {
    fn default() -> Self {
        Self {
            now_unix_ms: current_unix_ms(),
            batch_size: 64,
            max_concurrent: 4,
            stale_running_after_ms: 120_000,
            strict_debug: false,
        }
    }
}

impl HookRecoveryOptions {
    fn normalized(mut self) -> Self {
        self.batch_size = self.batch_size.max(1);
        self.max_concurrent = self.max_concurrent.max(1);
        self.stale_running_after_ms = self.stale_running_after_ms.max(1);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookRecoverySummary {
    pub scanned_count: usize,
    pub recovered_count: usize,
    pub executed_count: usize,
    pub retried_count: usize,
    pub timed_out_count: usize,
    pub unrecoverable_count: usize,
    pub skipped_count: usize,
}

impl HookRecoverySummary {
    fn record(&mut self, run: &HookRecoveredRunSummary) {
        match run.status {
            HookRunStatus::Succeeded => {
                self.recovered_count += 1;
                self.executed_count += 1;
            }
            HookRunStatus::Failed => {
                self.executed_count += usize::from(run.executed);
                if run.unrecoverable {
                    self.unrecoverable_count += 1;
                }
            }
            HookRunStatus::TimedOut => {
                self.timed_out_count += 1;
                self.executed_count += usize::from(run.executed);
            }
            HookRunStatus::Skipped => self.skipped_count += 1,
            HookRunStatus::Queued => {
                if run.retried {
                    self.retried_count += 1;
                }
            }
            HookRunStatus::Running => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRecoveredRunSummary {
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub status: HookRunStatus,
    pub executed: bool,
    pub retried: bool,
    pub unrecoverable: bool,
    pub diagnostic_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRuntimeOptions {
    pub default_deadline_timeout_ms: u64,
    pub error_preview_max_chars: usize,
}

impl Default for HookRuntimeOptions {
    fn default() -> Self {
        Self {
            default_deadline_timeout_ms: 1_000,
            error_preview_max_chars: 512,
        }
    }
}

impl HookRuntimeOptions {
    fn normalized(mut self) -> Self {
        self.error_preview_max_chars = self.error_preview_max_chars.max(3);
        self
    }

    fn diagnostic_redaction_policy(&self) -> HookDiagnosticRedactionPolicy {
        HookDiagnosticRedactionPolicy::new(self.error_preview_max_chars, false)
    }
}

#[derive(Clone)]
pub struct HookRuntime {
    handlers: Arc<HookRegistry>,
    subscriptions: Arc<HookSubscriptionRegistry>,
    options: HookRuntimeOptions,
    queued_background: HookBackgroundQueue,
    run_store: Option<Arc<dyn HookRunStore>>,
}

#[derive(Clone)]
struct HookBackgroundQueue {
    inner: Arc<Mutex<VecDeque<HookQueuedBackgroundRun>>>,
    drain_state: Arc<Mutex<HookBackgroundDrainState>>,
}

#[derive(Default)]
struct HookBackgroundDrainState {
    draining: bool,
}

struct HookBackgroundDrainGuard {
    queue: HookBackgroundQueue,
}

impl HookBackgroundQueue {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
            drain_state: Arc::new(Mutex::new(HookBackgroundDrainState::default())),
        }
    }

    fn len(&self) -> HookRuntimeResult<usize> {
        self.inner
            .lock()
            .map(|queue| queue.len())
            .map_err(|_| HookRegistryError::LockPoisoned("hook runtime background queue").into())
    }

    fn push(&self, run: HookQueuedBackgroundRun) -> HookRuntimeResult<()> {
        self.inner
            .lock()
            .map_err(|_| HookRegistryError::LockPoisoned("hook runtime background queue"))?
            .push_back(run);
        Ok(())
    }

    fn pop_front(&self) -> HookRuntimeResult<Option<HookQueuedBackgroundRun>> {
        self.inner
            .lock()
            .map(|mut queue| queue.pop_front())
            .map_err(|_| HookRegistryError::LockPoisoned("hook runtime background queue").into())
    }

    fn try_acquire_drain(&self) -> HookRuntimeResult<Option<HookBackgroundDrainGuard>> {
        let mut state = self
            .drain_state
            .lock()
            .map_err(|_| HookRegistryError::LockPoisoned("hook runtime background drain state"))?;
        if state.draining {
            return Ok(None);
        }
        state.draining = true;
        Ok(Some(HookBackgroundDrainGuard {
            queue: self.clone(),
        }))
    }

    fn release_drain(&self) {
        if let Ok(mut state) = self.drain_state.lock() {
            state.draining = false;
        }
    }
}

impl Drop for HookBackgroundDrainGuard {
    fn drop(&mut self) {
        self.queue.release_drain();
    }
}

impl HookRuntime {
    pub fn new(handlers: Arc<HookRegistry>, subscriptions: Arc<HookSubscriptionRegistry>) -> Self {
        Self::with_options(handlers, subscriptions, HookRuntimeOptions::default())
    }

    pub fn with_options(
        handlers: Arc<HookRegistry>,
        subscriptions: Arc<HookSubscriptionRegistry>,
        options: HookRuntimeOptions,
    ) -> Self {
        Self::with_options_and_optional_run_store(handlers, subscriptions, options, None)
    }

    pub fn with_run_store(
        handlers: Arc<HookRegistry>,
        subscriptions: Arc<HookSubscriptionRegistry>,
        run_store: Arc<dyn HookRunStore>,
    ) -> Self {
        Self::with_options_and_optional_run_store(
            handlers,
            subscriptions,
            HookRuntimeOptions::default(),
            Some(run_store),
        )
    }

    pub fn with_options_and_run_store(
        handlers: Arc<HookRegistry>,
        subscriptions: Arc<HookSubscriptionRegistry>,
        options: HookRuntimeOptions,
        run_store: Arc<dyn HookRunStore>,
    ) -> Self {
        Self::with_options_and_optional_run_store(handlers, subscriptions, options, Some(run_store))
    }

    pub fn with_options_and_optional_run_store(
        handlers: Arc<HookRegistry>,
        subscriptions: Arc<HookSubscriptionRegistry>,
        options: HookRuntimeOptions,
        run_store: Option<Arc<dyn HookRunStore>>,
    ) -> Self {
        Self {
            handlers,
            subscriptions,
            options: options.normalized(),
            queued_background: HookBackgroundQueue::new(),
            run_store,
        }
    }

    pub fn clone_with_optional_run_store(&self, run_store: Option<Arc<dyn HookRunStore>>) -> Self {
        Self {
            handlers: self.handlers.clone(),
            subscriptions: self.subscriptions.clone(),
            options: self.options.clone(),
            queued_background: self.queued_background.clone(),
            run_store,
        }
    }

    pub fn clone_with_run_store(&self, run_store: Arc<dyn HookRunStore>) -> Self {
        self.clone_with_optional_run_store(Some(run_store))
    }

    pub fn handlers(&self) -> &Arc<HookRegistry> {
        &self.handlers
    }

    pub fn subscriptions(&self) -> &Arc<HookSubscriptionRegistry> {
        &self.subscriptions
    }

    pub fn options(&self) -> &HookRuntimeOptions {
        &self.options
    }

    pub fn run_store(&self) -> Option<&Arc<dyn HookRunStore>> {
        self.run_store.as_ref()
    }

    pub fn has_run_store(&self) -> bool {
        self.run_store.is_some()
    }

    pub fn queued_background_len(&self) -> HookRuntimeResult<usize> {
        self.queued_background.len()
    }

    pub async fn run_phase(
        &self,
        request: HookPhaseRequest,
    ) -> HookRuntimeResult<HookPhaseResponse> {
        let subscriptions = self.subscriptions.subscriptions_for_phase(request.phase)?;
        let plan = build_execution_plan(request.phase, subscriptions, self.handlers.as_ref())?;
        let mut response = HookPhaseResponse::default();

        for batch in plan.batches {
            let chunk_size = batch_parallelism(&batch);
            for chunk in batch.chunks(chunk_size) {
                let mut results = join_all(chunk.iter().cloned().map(|node| {
                    execute_node(
                        node,
                        request.clone(),
                        self.queued_background.clone(),
                        self.options.clone(),
                        self.run_store.clone(),
                    )
                }))
                .await;
                results.sort_by_key(|result| result.order_index);

                for result in results {
                    append_node_result(&mut response, result, request.phase, &self.options)?;
                }
            }
        }

        Ok(response)
    }

    pub async fn run_queued_background_once(
        &self,
    ) -> HookRuntimeResult<Option<HookBackgroundRunSummary>> {
        let Some(_guard) = self.queued_background.try_acquire_drain()? else {
            return Ok(None);
        };
        let Some(run) = self.queued_background.pop_front()? else {
            return Ok(None);
        };
        execute_queued_background_run(run, &self.options)
            .await
            .map(Some)
    }

    pub async fn drain_queued_background(&self) -> HookRuntimeResult<HookBackgroundDrainSummary> {
        let Some(_guard) = self.queued_background.try_acquire_drain()? else {
            return Ok(HookBackgroundDrainSummary::default());
        };
        let mut summary = HookBackgroundDrainSummary::default();
        while let Some(run) = self.queued_background.pop_front()? {
            let run_summary = execute_queued_background_run(run, &self.options).await?;
            summary.record(&run_summary);
        }
        Ok(summary)
    }

    pub async fn recover_background_runs_once(
        &self,
        options: HookRecoveryOptions,
    ) -> HookRuntimeResult<HookRecoverySummary> {
        let options = options.normalized();
        let Some(store) = self.run_store.as_ref() else {
            return Ok(HookRecoverySummary::default());
        };
        let records = store
            .list_recoverable_runs(HookRecoveryScan {
                now_unix_ms: options.now_unix_ms,
                batch_size: options.batch_size,
                stale_running_after_ms: options.stale_running_after_ms,
                phases: None,
            })
            .await
            .unwrap_or_default();
        let mut summary = HookRecoverySummary {
            scanned_count: records.len(),
            ..HookRecoverySummary::default()
        };
        for chunk in records.chunks(options.max_concurrent) {
            for record in chunk {
                let run_summary = self
                    .recover_background_run(record.clone(), options.clone())
                    .await?;
                summary.record(&run_summary);
            }
        }
        Ok(summary)
    }

    pub async fn recover_background_run(
        &self,
        record: HookRecoverableRunRecord,
        options: HookRecoveryOptions,
    ) -> HookRuntimeResult<HookRecoveredRunSummary> {
        recover_background_run(self, record, options.normalized()).await
    }
}

#[derive(Clone)]
struct HookExecutionPlan {
    batches: Vec<Vec<HookExecutionNode>>,
}

#[derive(Clone)]
struct HookExecutionNode {
    order_index: usize,
    subscription: HookSubscription,
    handler: Arc<dyn HookHandler>,
}

struct NodeExecutionResult {
    order_index: usize,
    subscription: HookSubscription,
    outcome: HookRuntimeResult<HookNodeOutcome>,
}

enum HookNodeOutcome {
    Succeeded(HookHandlerResponse),
    Failed(HookError),
    TimedOut { timeout_ms: u64 },
    Skipped,
    Queued,
}

struct HookQueuedBackgroundRun {
    await_policy: HookQueuedBackgroundPolicy,
    node: HookExecutionNode,
    request: HookPhaseRequest,
    persistence: HookRunPersistence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HookQueuedBackgroundPolicy {
    Background,
    FireAndRecord,
}

impl HookQueuedBackgroundPolicy {
    fn from_await_policy(await_policy: HookAwaitPolicy) -> Option<Self> {
        match await_policy {
            HookAwaitPolicy::Background => Some(Self::Background),
            HookAwaitPolicy::FireAndRecord => Some(Self::FireAndRecord),
            HookAwaitPolicy::Blocking | HookAwaitPolicy::Deadline => None,
        }
    }

    fn as_await_policy(self) -> HookAwaitPolicy {
        match self {
            Self::Background => HookAwaitPolicy::Background,
            Self::FireAndRecord => HookAwaitPolicy::FireAndRecord,
        }
    }
}

fn build_execution_plan(
    phase: HookPhase,
    subscriptions: Vec<HookSubscription>,
    handlers: &HookRegistry,
) -> HookRuntimeResult<HookExecutionPlan> {
    let subscription_indexes = subscriptions
        .iter()
        .enumerate()
        .map(|(index, subscription)| (subscription.subscription_id.clone(), index))
        .collect::<BTreeMap<_, _>>();
    validate_dependencies(phase, &subscriptions, &subscription_indexes)?;
    validate_policy_configuration(phase, &subscriptions)?;
    let batches = build_topological_batches(phase, &subscriptions, &subscription_indexes)?;
    let mut nodes = Vec::with_capacity(subscriptions.len());
    for (order_index, subscription) in subscriptions.into_iter().enumerate() {
        let handler = handlers
            .get_handler(&subscription.hook_id)?
            .ok_or_else(|| HookRuntimeError::MissingHandler {
                subscription_id: subscription.subscription_id.clone(),
                hook_id: subscription.hook_id.clone(),
                phase,
            })?;
        nodes.push(HookExecutionNode {
            order_index,
            subscription,
            handler,
        });
    }

    Ok(HookExecutionPlan {
        batches: batches
            .into_iter()
            .map(|batch| {
                batch
                    .into_iter()
                    .map(|node_index| nodes[node_index].clone())
                    .collect()
            })
            .collect(),
    })
}

fn validate_dependencies(
    phase: HookPhase,
    subscriptions: &[HookSubscription],
    subscription_indexes: &BTreeMap<HookSubscriptionId, usize>,
) -> HookRuntimeResult<()> {
    for subscription in subscriptions {
        for dependency_id in &subscription.dependencies.after {
            if !subscription_indexes.contains_key(dependency_id) {
                return Err(HookRuntimeError::MissingDependency {
                    subscription_id: subscription.subscription_id.clone(),
                    dependency_id: dependency_id.clone(),
                    phase,
                });
            }
            let dependency = &subscriptions[subscription_indexes[dependency_id]];
            validate_dependency_execution_policy(phase, subscription, dependency)?;
        }
        for dependency_id in &subscription.dependencies.before {
            if !subscription_indexes.contains_key(dependency_id) {
                return Err(HookRuntimeError::MissingDependency {
                    subscription_id: subscription.subscription_id.clone(),
                    dependency_id: dependency_id.clone(),
                    phase,
                });
            }
            let target = &subscriptions[subscription_indexes[dependency_id]];
            validate_dependency_execution_policy(phase, target, subscription)?;
        }
    }
    Ok(())
}

fn validate_dependency_execution_policy(
    phase: HookPhase,
    subscription: &HookSubscription,
    dependency: &HookSubscription,
) -> HookRuntimeResult<()> {
    let subscription_background = is_background_like(subscription.execution_policy.await_policy);
    let dependency_background = is_background_like(dependency.execution_policy.await_policy);
    if !subscription_background && dependency_background {
        return Err(HookRuntimeError::InvalidExecutionPolicy {
            subscription_id: subscription.subscription_id.clone(),
            hook_id: subscription.hook_id.clone(),
            phase,
            reason: format!(
                "inline hook cannot depend on background-like subscription `{}` in the same phase",
                dependency.subscription_id
            ),
        });
    }
    if subscription_background && dependency_background {
        return Err(HookRuntimeError::InvalidExecutionPolicy {
            subscription_id: subscription.subscription_id.clone(),
            hook_id: subscription.hook_id.clone(),
            phase,
            reason: format!(
                "background-like hook cannot depend on background-like subscription `{}` in phase 20",
                dependency.subscription_id
            ),
        });
    }
    Ok(())
}

fn validate_policy_configuration(
    phase: HookPhase,
    subscriptions: &[HookSubscription],
) -> HookRuntimeResult<()> {
    for subscription in subscriptions {
        if subscription.failure_policy == HookFailurePolicy::Fallback
            && subscription.fallback_contributions.is_empty()
        {
            return Err(HookRuntimeError::MissingFallbackContribution {
                subscription_id: subscription.subscription_id.clone(),
                hook_id: subscription.hook_id.clone(),
                phase,
            });
        }
        if subscription.execution_policy.max_parallelism == Some(0) {
            return Err(HookRuntimeError::InvalidExecutionPolicy {
                subscription_id: subscription.subscription_id.clone(),
                hook_id: subscription.hook_id.clone(),
                phase,
                reason: "max_parallelism must be greater than zero".to_owned(),
            });
        }
        if is_background_like(subscription.execution_policy.await_policy) {
            match subscription.failure_policy {
                HookFailurePolicy::Required | HookFailurePolicy::FailClosed => {
                    return Err(HookRuntimeError::InvalidExecutionPolicy {
                        subscription_id: subscription.subscription_id.clone(),
                        hook_id: subscription.hook_id.clone(),
                        phase,
                        reason: format!(
                            "{:?} failure policy cannot be used with background-like await policy",
                            subscription.failure_policy
                        ),
                    });
                }
                HookFailurePolicy::Fallback => {
                    if subscription
                        .fallback_contributions
                        .iter()
                        .any(|contribution| !background_contribution_allowed(contribution))
                    {
                        return Err(HookRuntimeError::InvalidExecutionPolicy {
                            subscription_id: subscription.subscription_id.clone(),
                            hook_id: subscription.hook_id.clone(),
                            phase,
                            reason:
                                "background-like fallback contributions must be side-effect-only"
                                    .to_owned(),
                        });
                    }
                }
                HookFailurePolicy::BestEffort | HookFailurePolicy::Skip => {}
            }
        }
    }
    Ok(())
}

fn is_background_like(await_policy: HookAwaitPolicy) -> bool {
    matches!(
        await_policy,
        HookAwaitPolicy::Background | HookAwaitPolicy::FireAndRecord
    )
}

fn build_topological_batches(
    phase: HookPhase,
    subscriptions: &[HookSubscription],
    subscription_indexes: &BTreeMap<HookSubscriptionId, usize>,
) -> HookRuntimeResult<Vec<Vec<usize>>> {
    let node_count = subscriptions.len();
    let mut successors = vec![BTreeSet::new(); node_count];
    let mut indegrees = vec![0usize; node_count];

    for (current_index, subscription) in subscriptions.iter().enumerate() {
        for dependency_id in &subscription.dependencies.after {
            let dependency_index = subscription_indexes[dependency_id];
            add_edge(
                dependency_index,
                current_index,
                &mut successors,
                &mut indegrees,
            );
        }
        for dependency_id in &subscription.dependencies.before {
            let target_index = subscription_indexes[dependency_id];
            add_edge(current_index, target_index, &mut successors, &mut indegrees);
        }
    }

    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(index, indegree)| (*indegree == 0).then_some(index))
        .collect::<Vec<_>>();
    let mut processed = vec![false; node_count];
    let mut processed_count = 0usize;
    let mut batches = Vec::new();

    while !ready.is_empty() {
        ready.sort_unstable();
        let batch = ready;
        ready = Vec::new();

        for &node_index in &batch {
            if processed[node_index] {
                continue;
            }
            processed[node_index] = true;
            processed_count += 1;
        }

        for &node_index in &batch {
            for &successor_index in &successors[node_index] {
                indegrees[successor_index] -= 1;
                if indegrees[successor_index] == 0 {
                    ready.push(successor_index);
                }
            }
        }

        batches.push(batch);
    }

    if processed_count != node_count {
        let subscription_ids = subscriptions
            .iter()
            .enumerate()
            .filter_map(|(index, subscription)| {
                (!processed[index]).then_some(subscription.subscription_id.clone())
            })
            .collect();
        return Err(HookRuntimeError::DependencyCycle {
            phase,
            subscription_ids,
        });
    }

    Ok(batches)
}

fn add_edge(from: usize, to: usize, successors: &mut [BTreeSet<usize>], indegrees: &mut [usize]) {
    if successors[from].insert(to) {
        indegrees[to] += 1;
    }
}

fn batch_parallelism(batch: &[HookExecutionNode]) -> usize {
    let batch_len = batch.len().max(1);
    batch
        .iter()
        .filter_map(|node| node.subscription.execution_policy.max_parallelism)
        .map(usize::from)
        .min()
        .map(|limit| limit.clamp(1, batch_len))
        .unwrap_or(batch_len)
}

fn append_node_result(
    response: &mut HookPhaseResponse,
    result: NodeExecutionResult,
    phase: HookPhase,
    options: &HookRuntimeOptions,
) -> HookRuntimeResult<()> {
    let NodeExecutionResult {
        subscription,
        outcome,
        ..
    } = result;
    match outcome? {
        HookNodeOutcome::Succeeded(handler_response) => {
            append_success(
                response,
                subscription.subscription_id,
                subscription.hook_id,
                phase,
                handler_response,
                options,
            );
        }
        HookNodeOutcome::Failed(error) => match subscription.failure_policy {
            HookFailurePolicy::Required => {
                return Err(HookRuntimeError::HookFailed {
                    subscription_id: subscription.subscription_id,
                    hook_id: subscription.hook_id,
                    phase,
                    error,
                });
            }
            HookFailurePolicy::Fallback => {
                append_fallback_failure(
                    response,
                    &subscription,
                    phase,
                    HookRunStatus::Failed,
                    error,
                    options,
                );
            }
            HookFailurePolicy::BestEffort => {
                append_best_effort_failure(
                    response,
                    subscription.subscription_id,
                    subscription.hook_id,
                    phase,
                    HookRunStatus::Failed,
                    error,
                    options,
                );
            }
            HookFailurePolicy::Skip => {
                append_skipped(response, subscription, phase);
            }
            HookFailurePolicy::FailClosed => {
                return Err(HookRuntimeError::HookFailedClosed {
                    subscription_id: subscription.subscription_id,
                    hook_id: subscription.hook_id,
                    phase,
                    error,
                });
            }
        },
        HookNodeOutcome::TimedOut { timeout_ms } => match subscription.failure_policy {
            HookFailurePolicy::Required => {
                return Err(HookRuntimeError::HookTimedOut {
                    subscription_id: subscription.subscription_id,
                    hook_id: subscription.hook_id,
                    phase,
                    timeout_ms,
                });
            }
            HookFailurePolicy::Fallback => {
                append_fallback_failure(
                    response,
                    &subscription,
                    phase,
                    HookRunStatus::TimedOut,
                    timeout_error(timeout_ms),
                    options,
                );
            }
            HookFailurePolicy::BestEffort => {
                append_best_effort_failure(
                    response,
                    subscription.subscription_id,
                    subscription.hook_id,
                    phase,
                    HookRunStatus::TimedOut,
                    timeout_error(timeout_ms),
                    options,
                );
            }
            HookFailurePolicy::Skip => {
                append_skipped(response, subscription, phase);
            }
            HookFailurePolicy::FailClosed => {
                return Err(HookRuntimeError::HookFailedClosed {
                    subscription_id: subscription.subscription_id,
                    hook_id: subscription.hook_id,
                    phase,
                    error: timeout_error(timeout_ms),
                });
            }
        },
        HookNodeOutcome::Skipped => {
            append_skipped(response, subscription, phase);
        }
        HookNodeOutcome::Queued => {
            append_queued(
                response,
                subscription.subscription_id,
                subscription.hook_id,
                phase,
            );
        }
    }
    Ok(())
}

async fn execute_node(
    node: HookExecutionNode,
    request: HookPhaseRequest,
    queued_background: HookBackgroundQueue,
    options: HookRuntimeOptions,
    run_store: Option<Arc<dyn HookRunStore>>,
) -> NodeExecutionResult {
    let outcome =
        execute_node_with_persistence(&node, request, queued_background, &options, run_store).await;
    NodeExecutionResult {
        order_index: node.order_index,
        subscription: node.subscription,
        outcome,
    }
}

async fn execute_node_with_persistence(
    node: &HookExecutionNode,
    request: HookPhaseRequest,
    queued_background: HookBackgroundQueue,
    options: &HookRuntimeOptions,
    run_store: Option<Arc<dyn HookRunStore>>,
) -> HookRuntimeResult<HookNodeOutcome> {
    let mut persistence = HookRunPersistence::start(run_store, node, &request).await;

    if node.subscription.failure_policy == HookFailurePolicy::Skip {
        persistence
            .complete_run(
                HookRunStatus::Skipped,
                Vec::new(),
                Vec::new(),
                None,
                current_unix_ms(),
            )
            .await;
        return Ok(HookNodeOutcome::Skipped);
    }

    let await_policy = node.subscription.execution_policy.await_policy;
    if let Some(background_policy) = HookQueuedBackgroundPolicy::from_await_policy(await_policy) {
        queued_background.push(HookQueuedBackgroundRun {
            await_policy: background_policy,
            node: node.clone(),
            request,
            persistence,
        })?;
        return Ok(HookNodeOutcome::Queued);
    }

    persistence.start_attempt().await;
    let outcome = execute_node_with_policy(node, request.clone(), options).await;
    if let Ok(outcome) = &outcome {
        persistence
            .complete_for_outcome(&node.subscription, request.phase, outcome, options)
            .await;
    }
    outcome
}

async fn execute_queued_background_run(
    mut run: HookQueuedBackgroundRun,
    options: &HookRuntimeOptions,
) -> HookRuntimeResult<HookBackgroundRunSummary> {
    run.persistence.start_attempt().await;
    let mut outcome = execute_node_with_policy(&run.node, run.request.clone(), options).await?;
    if let HookNodeOutcome::Succeeded(response) = outcome {
        outcome = match validate_background_response(response) {
            Ok(response) => HookNodeOutcome::Succeeded(response),
            Err(error) => HookNodeOutcome::Failed(error),
        };
    }
    let status = run
        .persistence
        .complete_background_for_outcome(&run.node, run.request.phase, &outcome, options)
        .await;
    Ok(background_run_summary(&run, &outcome, status))
}

async fn recover_background_run(
    runtime: &HookRuntime,
    mut record: HookRecoverableRunRecord,
    options: HookRecoveryOptions,
) -> HookRuntimeResult<HookRecoveredRunSummary> {
    let Some(store) = runtime.run_store.clone() else {
        return Ok(recovered_run_summary(
            &record.run,
            HookRunStatus::Skipped,
            false,
            false,
            false,
            0,
        ));
    };

    let resolved = resolve_recovered_background_run(runtime, &record);
    let (node, request, await_policy) = match resolved {
        Ok(resolved) => resolved,
        Err(error) => {
            mark_recovered_run_unrecoverable(store.as_ref(), &record.run, error, &runtime.options)
                .await;
            return Ok(recovered_run_summary(
                &record.run,
                HookRunStatus::Failed,
                false,
                false,
                true,
                1,
            ));
        }
    };

    if record.run.status == HookRunStatus::Running {
        let stale_error = timeout_error(
            node.subscription
                .execution_policy
                .timeout_ms
                .unwrap_or(runtime.options.default_deadline_timeout_ms),
        );
        let current_attempt = record.run.attempt_count.max(
            record
                .attempts
                .iter()
                .map(|attempt| attempt.attempt_number)
                .max()
                .unwrap_or(0),
        );
        let can_retry = should_retry_background_outcome(
            &node,
            current_attempt,
            HookRunStatus::TimedOut,
            &stale_error,
        );
        let stale_completion = error_completion(
            HookRunStatus::TimedOut,
            stale_error.clone(),
            &runtime.options,
            options.now_unix_ms,
        );
        let _ = store
            .mark_stale_run_timed_out(&record.run.id, stale_completion)
            .await;
        if !can_retry {
            return Ok(recovered_run_summary(
                &record.run,
                HookRunStatus::TimedOut,
                false,
                false,
                false,
                1,
            ));
        }
        let queued_at_unix_ms = options.now_unix_ms.saturating_add(retry_delay_ms(
            &node.subscription.retry_policy,
            current_attempt,
        ));
        let deadline_at_unix_ms = node
            .subscription
            .execution_policy
            .timeout_ms
            .map(|timeout_ms| queued_at_unix_ms.saturating_add(u64_to_i64_saturating(timeout_ms)));
        if let Ok(updated) = store
            .schedule_run_retry(
                &record.run.id,
                HookRetrySchedule {
                    queued_at_unix_ms,
                    deadline_at_unix_ms,
                    diagnostic_previews: vec![
                        diagnostic_from_error(&stale_error, HookRunStatus::TimedOut)
                            .preview(&runtime.options.diagnostic_redaction_policy()),
                    ],
                },
            )
            .await
        {
            record.run = updated;
        }
    }

    let run = HookQueuedBackgroundRun {
        await_policy,
        node,
        request,
        persistence: HookRunPersistence::from_existing(store, record.run.clone()),
    };
    let summary = execute_queued_background_run(run, &runtime.options).await?;
    Ok(HookRecoveredRunSummary {
        subscription_id: summary.subscription_id,
        hook_id: summary.hook_id,
        phase: summary.phase,
        status: summary.status,
        executed: true,
        retried: summary.status == HookRunStatus::Queued,
        unrecoverable: false,
        diagnostic_count: summary.diagnostic_count,
    })
}

fn resolve_recovered_background_run(
    runtime: &HookRuntime,
    record: &HookRecoverableRunRecord,
) -> Result<
    (
        HookExecutionNode,
        HookPhaseRequest,
        HookQueuedBackgroundPolicy,
    ),
    HookError,
> {
    let resume_state = record
        .resume_state
        .as_ref()
        .or(record.run.resume_state.as_ref())
        .ok_or_else(|| {
            recovery_error(
                "hook.recovery_resume_missing",
                "hook recovery resume state is missing",
            )
        })?;
    if resume_state.schema_version != crate::HOOK_RUN_RESUME_SCHEMA_VERSION {
        return Err(recovery_error(
            "hook.recovery_resume_unsupported",
            "hook recovery resume schema version is unsupported",
        ));
    }
    let await_policy =
        HookQueuedBackgroundPolicy::from_await_policy(resume_state.execution_policy.await_policy)
            .ok_or_else(|| {
            recovery_error(
                "hook.recovery_not_background",
                "hook recovery requires a background-like await policy",
            )
        })?;
    let mut subscription = runtime
        .subscriptions
        .get_subscription(&record.run.subscription_id)
        .map_err(|_| {
            recovery_error(
                "hook.recovery_subscription_failed",
                "hook recovery could not read subscription",
            )
        })?
        .ok_or_else(|| {
            recovery_error(
                "hook.recovery_subscription_missing",
                "hook recovery subscription is missing",
            )
        })?;
    if subscription.hook_id != record.run.hook_id || subscription.phase != record.run.phase {
        return Err(recovery_error(
            "hook.recovery_subscription_mismatch",
            "hook recovery subscription no longer matches persisted run",
        ));
    }
    if !subscription.enabled {
        return Err(recovery_error(
            "hook.recovery_subscription_disabled",
            "hook recovery subscription is disabled",
        ));
    }
    subscription.execution_policy = resume_state.execution_policy.clone();
    subscription.failure_policy = resume_state.failure_policy;
    subscription.retry_policy = resume_state.retry_policy.clone();
    let handler = runtime
        .handlers
        .get_handler(&record.run.hook_id)
        .map_err(|_| {
            recovery_error(
                "hook.recovery_handler_failed",
                "hook recovery could not read handler",
            )
        })?
        .ok_or_else(|| {
            recovery_error(
                "hook.recovery_handler_missing",
                "hook recovery handler is missing",
            )
        })?;
    if !handler.supported_phases().contains(&record.run.phase) {
        return Err(recovery_error(
            "hook.recovery_phase_unsupported",
            "hook recovery handler no longer supports persisted phase",
        ));
    }
    if handler.version() != resume_state.handler_version
        || handler.input_contract_version() != resume_state.input_contract_version
        || handler.output_contract_version() != resume_state.output_contract_version
    {
        return Err(recovery_error(
            "hook.recovery_contract_mismatch",
            "hook recovery handler contract version no longer matches persisted run",
        ));
    }
    let request = match &resume_state.payload {
        HookRunResumePayload::InputSnapshot(snapshot) => request_from_snapshot(snapshot)?,
        HookRunResumePayload::Reference(_) => {
            return Err(recovery_error(
                "hook.recovery_reference_unsupported",
                "hook recovery reference reconstruction is not available",
            ));
        }
    };
    Ok((
        HookExecutionNode {
            order_index: 0,
            subscription,
            handler,
        },
        request,
        await_policy,
    ))
}

fn request_from_snapshot(snapshot: &HookRunInputSnapshot) -> Result<HookPhaseRequest, HookError> {
    let expected_hash = HookRunInputSnapshot::hash_parts(
        snapshot.phase,
        &snapshot.context,
        &snapshot.input,
        &snapshot.policy_set,
        &snapshot.prompt_context_set,
    );
    if expected_hash != snapshot.snapshot_hash {
        return Err(recovery_error(
            "hook.recovery_snapshot_hash_mismatch",
            "hook recovery input snapshot hash does not match",
        ));
    }
    Ok(HookPhaseRequest::new(
        snapshot.phase,
        snapshot.context.clone(),
        snapshot.input.clone(),
    )
    .with_policy_set(snapshot.policy_set.clone())
    .with_prompt_context_set(snapshot.prompt_context_set.clone()))
}

async fn mark_recovered_run_unrecoverable(
    store: &dyn HookRunStore,
    run: &HookRunStoreRecord,
    error: HookError,
    options: &HookRuntimeOptions,
) {
    let _ = store
        .mark_run_unrecoverable(
            &run.id,
            error_completion(HookRunStatus::Failed, error, options, current_unix_ms()),
        )
        .await;
}

fn error_completion(
    status: HookRunStatus,
    error: HookError,
    options: &HookRuntimeOptions,
    completed_at_unix_ms: i64,
) -> HookRunStoreCompletion {
    let policy = options.diagnostic_redaction_policy();
    let diagnostic = diagnostic_from_error(&error, status);
    let diagnostic_preview = diagnostic.preview(&policy);
    let error_summary = HookRunErrorSummary::from_error(&error, &policy);
    HookRunStoreCompletion {
        status,
        contribution_hashes: Vec::new(),
        diagnostic_previews: vec![diagnostic_preview],
        error: Some(error_summary),
        completed_at_unix_ms,
    }
}

fn recovery_error(code: &'static str, message: &'static str) -> HookError {
    HookError::new(
        HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        HookDiagnosticMessage::new(message).expect("static diagnostic message is valid"),
    )
    .with_safe_for_user(true)
}

fn recovered_run_summary(
    run: &HookRunStoreRecord,
    status: HookRunStatus,
    executed: bool,
    retried: bool,
    unrecoverable: bool,
    diagnostic_count: usize,
) -> HookRecoveredRunSummary {
    HookRecoveredRunSummary {
        subscription_id: run.subscription_id.clone(),
        hook_id: run.hook_id.clone(),
        phase: run.phase,
        status,
        executed,
        retried,
        unrecoverable,
        diagnostic_count,
    }
}

struct HookRunPersistence {
    store: Option<Arc<dyn HookRunStore>>,
    run: Option<HookRunStoreRecord>,
    attempt: Option<HookRunAttemptStoreRecord>,
    attempt_started_instant: Option<Instant>,
}

impl HookRunPersistence {
    async fn start(
        store: Option<Arc<dyn HookRunStore>>,
        node: &HookExecutionNode,
        request: &HookPhaseRequest,
    ) -> Self {
        let Some(store) = store else {
            return Self::disabled();
        };
        let queued_at_unix_ms = current_unix_ms();
        let run = store
            .create_or_load_run(NewHookRunStoreRecord {
                idempotency_key: hook_run_idempotency_key(
                    request.phase,
                    &request.context,
                    &request.input,
                    &node.subscription,
                ),
                subscription_id: node.subscription.subscription_id.clone(),
                hook_id: node.subscription.hook_id.clone(),
                phase: request.phase,
                status: HookRunStatus::Queued,
                scope: hook_run_scope_from_context(&request.context),
                context: request.context.clone(),
                contribution_hashes: Vec::new(),
                diagnostic_previews: Vec::new(),
                error: None,
                queued_at_unix_ms: Some(queued_at_unix_ms),
                started_at_unix_ms: None,
                completed_at_unix_ms: None,
                deadline_at_unix_ms: node.subscription.execution_policy.timeout_ms.map(
                    |timeout_ms| {
                        queued_at_unix_ms.saturating_add(u64_to_i64_saturating(timeout_ms))
                    },
                ),
                resume_state: background_resume_state(node, request),
            })
            .await
            .ok();

        Self {
            store: Some(store),
            run,
            attempt: None,
            attempt_started_instant: None,
        }
    }

    fn disabled() -> Self {
        Self {
            store: None,
            run: None,
            attempt: None,
            attempt_started_instant: None,
        }
    }

    fn from_existing(store: Arc<dyn HookRunStore>, run: HookRunStoreRecord) -> Self {
        Self {
            store: Some(store),
            run: Some(run),
            attempt: None,
            attempt_started_instant: None,
        }
    }

    async fn start_attempt(&mut self) {
        let (Some(store), Some(run)) = (self.store.as_ref(), self.run.as_ref()) else {
            return;
        };
        let started_at_unix_ms = current_unix_ms();
        if let Ok(updated) = store.mark_run_running(&run.id, started_at_unix_ms).await {
            self.run = Some(updated);
        }

        let Some(run) = self.run.as_ref() else {
            return;
        };
        let run_id = run.id.clone();
        let attempt_number = run.attempt_count.saturating_add(1).max(1);
        self.attempt_started_instant = Some(Instant::now());
        let new_attempt = |run: &HookRunStoreRecord, attempt_number| NewHookRunAttemptStoreRecord {
            hook_run_id: run.id.clone(),
            attempt_number,
            status: HookRunStatus::Running,
            contribution_hashes: Vec::new(),
            diagnostic_previews: Vec::new(),
            error: None,
            started_at_unix_ms: Some(started_at_unix_ms),
            completed_at_unix_ms: None,
            duration_ms: None,
        };
        match store.append_attempt(new_attempt(run, attempt_number)).await {
            Ok(attempt) => {
                self.attempt = Some(attempt);
            }
            Err(crate::HookRunStoreError::Conflict { .. }) => {
                if let Ok(updated) = store.mark_run_running(&run_id, started_at_unix_ms).await {
                    self.run = Some(updated);
                }
                let Some(run) = self.run.as_ref() else {
                    return;
                };
                let retry_attempt_number = run
                    .attempt_count
                    .saturating_add(1)
                    .max(attempt_number.saturating_add(1));
                if retry_attempt_number != attempt_number {
                    if let Ok(attempt) = store
                        .append_attempt(new_attempt(run, retry_attempt_number))
                        .await
                    {
                        self.attempt = Some(attempt);
                    }
                }
            }
            Err(_) => {}
        }
    }

    async fn complete_for_outcome(
        &mut self,
        subscription: &HookSubscription,
        phase: HookPhase,
        outcome: &HookNodeOutcome,
        options: &HookRuntimeOptions,
    ) {
        match outcome {
            HookNodeOutcome::Succeeded(response) => {
                let policy = options.diagnostic_redaction_policy();
                let contribution_hashes = hash_contributions(&response.contributions);
                let diagnostic_previews = diagnostic_previews(&response.diagnostics, &policy);
                self.complete_attempt_and_run(
                    HookRunStatus::Succeeded,
                    contribution_hashes,
                    diagnostic_previews,
                    None,
                    current_unix_ms(),
                )
                .await;
                self.persist_audit_contributions(
                    subscription,
                    phase,
                    response.contributions.as_slice(),
                    current_unix_ms(),
                )
                .await;
            }
            HookNodeOutcome::Failed(error) => {
                self.complete_error_outcome(
                    subscription,
                    phase,
                    HookRunStatus::Failed,
                    error.clone(),
                    options,
                )
                .await;
            }
            HookNodeOutcome::TimedOut { timeout_ms } => {
                self.complete_error_outcome(
                    subscription,
                    phase,
                    HookRunStatus::TimedOut,
                    timeout_error(*timeout_ms),
                    options,
                )
                .await;
            }
            HookNodeOutcome::Skipped => {
                self.complete_run(
                    HookRunStatus::Skipped,
                    Vec::new(),
                    Vec::new(),
                    None,
                    current_unix_ms(),
                )
                .await;
            }
            HookNodeOutcome::Queued => {}
        }
    }

    async fn complete_background_for_outcome(
        &mut self,
        node: &HookExecutionNode,
        phase: HookPhase,
        outcome: &HookNodeOutcome,
        options: &HookRuntimeOptions,
    ) -> HookRunStatus {
        match outcome {
            HookNodeOutcome::Succeeded(response) => {
                let policy = options.diagnostic_redaction_policy();
                let contribution_hashes = hash_contributions(&response.contributions);
                let diagnostic_previews = diagnostic_previews(&response.diagnostics, &policy);
                self.complete_attempt_and_run(
                    HookRunStatus::Succeeded,
                    contribution_hashes,
                    diagnostic_previews,
                    None,
                    current_unix_ms(),
                )
                .await;
                self.persist_audit_contributions(
                    &node.subscription,
                    phase,
                    response.contributions.as_slice(),
                    current_unix_ms(),
                )
                .await;
                HookRunStatus::Succeeded
            }
            HookNodeOutcome::Failed(error) => {
                self.complete_background_error_or_retry(
                    node,
                    phase,
                    HookRunStatus::Failed,
                    error.clone(),
                    options,
                )
                .await
            }
            HookNodeOutcome::TimedOut { timeout_ms } => {
                self.complete_background_error_or_retry(
                    node,
                    phase,
                    HookRunStatus::TimedOut,
                    timeout_error(*timeout_ms),
                    options,
                )
                .await
            }
            HookNodeOutcome::Skipped => {
                self.complete_run(
                    HookRunStatus::Skipped,
                    Vec::new(),
                    Vec::new(),
                    None,
                    current_unix_ms(),
                )
                .await;
                HookRunStatus::Skipped
            }
            HookNodeOutcome::Queued => HookRunStatus::Queued,
        }
    }

    async fn complete_background_error_or_retry(
        &mut self,
        node: &HookExecutionNode,
        phase: HookPhase,
        status: HookRunStatus,
        error: HookError,
        options: &HookRuntimeOptions,
    ) -> HookRunStatus {
        let policy = options.diagnostic_redaction_policy();
        let diagnostic = diagnostic_from_error(&error, status);
        let diagnostic_preview = diagnostic.preview(&policy);
        let error_summary = HookRunErrorSummary::from_error(&error, &policy);
        let completed_at_unix_ms = current_unix_ms();
        self.complete_attempt(
            status,
            Vec::new(),
            vec![diagnostic_preview.clone()],
            Some(error_summary.clone()),
            completed_at_unix_ms,
        )
        .await;

        if should_retry_background_outcome(node, self.current_attempt_number(), status, &error) {
            let queued_at_unix_ms = completed_at_unix_ms.saturating_add(retry_delay_ms(
                &node.subscription.retry_policy,
                self.current_attempt_number(),
            ));
            let deadline_at_unix_ms =
                node.subscription
                    .execution_policy
                    .timeout_ms
                    .map(|timeout_ms| {
                        queued_at_unix_ms.saturating_add(u64_to_i64_saturating(timeout_ms))
                    });
            if let (Some(store), Some(run)) = (self.store.as_ref(), self.run.as_ref()) {
                if let Ok(updated) = store
                    .schedule_run_retry(
                        &run.id,
                        HookRetrySchedule {
                            queued_at_unix_ms,
                            deadline_at_unix_ms,
                            diagnostic_previews: vec![diagnostic_preview.clone()],
                        },
                    )
                    .await
                {
                    self.run = Some(updated);
                    return HookRunStatus::Queued;
                }
            }
        }

        self.complete_run(
            status,
            Vec::new(),
            vec![diagnostic_preview],
            Some(error_summary),
            completed_at_unix_ms,
        )
        .await;
        if node.subscription.failure_policy == HookFailurePolicy::Fallback {
            self.persist_audit_contributions(
                &node.subscription,
                phase,
                node.subscription.fallback_contributions.as_slice(),
                current_unix_ms(),
            )
            .await;
        }
        status
    }

    fn current_attempt_number(&self) -> u16 {
        self.attempt
            .as_ref()
            .map(|attempt| attempt.attempt_number)
            .or_else(|| self.run.as_ref().map(|run| run.attempt_count))
            .unwrap_or(0)
    }

    async fn complete_error_outcome(
        &mut self,
        subscription: &HookSubscription,
        phase: HookPhase,
        status: HookRunStatus,
        error: HookError,
        options: &HookRuntimeOptions,
    ) {
        let policy = options.diagnostic_redaction_policy();
        let diagnostic = diagnostic_from_error(&error, status);
        let diagnostic_preview = diagnostic.preview(&policy);
        let error_summary = HookRunErrorSummary::from_error(&error, &policy);
        let run_contribution_hashes = if subscription.failure_policy == HookFailurePolicy::Fallback
        {
            hash_contributions(&subscription.fallback_contributions)
        } else {
            Vec::new()
        };
        self.complete_attempt(
            status,
            Vec::new(),
            vec![diagnostic_preview.clone()],
            Some(error_summary.clone()),
            current_unix_ms(),
        )
        .await;
        self.complete_run(
            status,
            run_contribution_hashes,
            vec![diagnostic_preview],
            Some(error_summary),
            current_unix_ms(),
        )
        .await;
        if subscription.failure_policy == HookFailurePolicy::Fallback {
            self.persist_audit_contributions(
                subscription,
                phase,
                subscription.fallback_contributions.as_slice(),
                current_unix_ms(),
            )
            .await;
        }
    }

    async fn complete_attempt_and_run(
        &mut self,
        status: HookRunStatus,
        contribution_hashes: Vec<HookContributionHash>,
        diagnostic_previews: Vec<HookDiagnosticPreview>,
        error: Option<HookRunErrorSummary>,
        completed_at_unix_ms: i64,
    ) {
        self.complete_attempt(
            status,
            contribution_hashes.clone(),
            diagnostic_previews.clone(),
            error.clone(),
            completed_at_unix_ms,
        )
        .await;
        self.complete_run(
            status,
            contribution_hashes,
            diagnostic_previews,
            error,
            completed_at_unix_ms,
        )
        .await;
    }

    async fn complete_attempt(
        &mut self,
        status: HookRunStatus,
        contribution_hashes: Vec<HookContributionHash>,
        diagnostic_previews: Vec<HookDiagnosticPreview>,
        error: Option<HookRunErrorSummary>,
        completed_at_unix_ms: i64,
    ) {
        let (Some(store), Some(attempt)) = (self.store.as_ref(), self.attempt.as_ref()) else {
            return;
        };
        let duration_ms = self
            .attempt_started_instant
            .map(|instant| u128_to_i64_saturating(instant.elapsed().as_millis()));
        if let Ok(updated) = store
            .complete_attempt(
                &attempt.id,
                HookRunAttemptStoreCompletion {
                    status,
                    contribution_hashes,
                    diagnostic_previews,
                    error,
                    completed_at_unix_ms,
                    duration_ms,
                },
            )
            .await
        {
            self.attempt = Some(updated);
        }
    }

    async fn complete_run(
        &mut self,
        status: HookRunStatus,
        contribution_hashes: Vec<HookContributionHash>,
        diagnostic_previews: Vec<HookDiagnosticPreview>,
        error: Option<HookRunErrorSummary>,
        completed_at_unix_ms: i64,
    ) {
        let (Some(store), Some(run)) = (self.store.as_ref(), self.run.as_ref()) else {
            return;
        };
        if let Ok(updated) = store
            .complete_run(
                &run.id,
                HookRunStoreCompletion {
                    status,
                    contribution_hashes,
                    diagnostic_previews,
                    error,
                    completed_at_unix_ms,
                },
            )
            .await
        {
            self.run = Some(updated);
        }
    }

    async fn persist_audit_contributions(
        &self,
        subscription: &HookSubscription,
        phase: HookPhase,
        contributions: &[HookContribution],
        created_at_unix_ms: i64,
    ) {
        let (Some(store), Some(run)) = (self.store.as_ref(), self.run.as_ref()) else {
            return;
        };
        let attempt_id = self.attempt.as_ref().map(|attempt| attempt.id.clone());
        let events = contributions
            .iter()
            .filter_map(|contribution| {
                let HookContribution::Audit(audit) = contribution else {
                    return None;
                };
                Some(NewHookAuditEventStoreRecord {
                    hook_run_id: run.id.clone(),
                    hook_run_attempt_id: attempt_id.clone(),
                    subscription_id: subscription.subscription_id.clone(),
                    hook_id: subscription.hook_id.clone(),
                    phase,
                    context: run.context.clone(),
                    event_kind: audit.event_kind.clone(),
                    contribution_hash: HookContributionHash::from_contribution(contribution),
                    details: audit.details.clone(),
                    safe_for_user: audit.safe_for_user,
                    created_at_unix_ms: Some(created_at_unix_ms),
                })
            })
            .collect::<Vec<_>>();
        if !events.is_empty() {
            let _ = store.append_audit_events(events).await;
        }
    }
}

fn hook_run_idempotency_key(
    phase: HookPhase,
    context: &HookContext,
    input: &HookInput,
    subscription: &HookSubscription,
) -> HookRunIdempotencyKey {
    let mut hasher = Sha256::new();
    hash_segment(&mut hasher, "phase", phase.as_str());
    hash_segment(&mut hasher, "input_kind", input.kind.as_str());
    if let Ok(input_payload) = serde_json::to_string(&input.payload) {
        hash_segment(&mut hasher, "input_payload", input_payload.as_str());
    }
    hash_segment(
        &mut hasher,
        "subscription_id",
        subscription.subscription_id.as_str(),
    );
    hash_segment(&mut hasher, "hook_id", subscription.hook_id.as_str());
    if let Some(workspace_id) = &context.workspace_id {
        hash_segment(&mut hasher, "workspace_id", workspace_id.as_str());
    }
    if let Some(thread_id) = &context.thread_id {
        hash_segment(&mut hasher, "thread_id", thread_id.as_str());
    }
    if let Some(turn_id) = &context.turn_id {
        hash_segment(&mut hasher, "turn_id", turn_id.as_str());
    }
    if let Some(task_id) = &context.task_id {
        hash_segment(&mut hasher, "task_id", task_id.as_str());
    }
    if let Some(agent_id) = &context.agent_id {
        hash_segment(&mut hasher, "agent_id", agent_id.as_str());
    }
    if let Some(mode) = &context.mode {
        hash_segment(&mut hasher, "context_mode", mode.as_str());
    }
    if let Some(actor) = &context.actor {
        hash_segment(&mut hasher, "actor_kind", actor.kind.as_str());
        if let Some(actor_id) = &actor.id {
            hash_segment(&mut hasher, "actor_id", actor_id.as_str());
        }
    }
    if let HookInputPayload::TurnPreCompaction(payload) = &input.payload {
        hash_segment(&mut hasher, "compaction_id", payload.compaction_id.as_str());
    }
    let key = format!("sha256:{}", hex::encode(hasher.finalize()));
    HookRunIdempotencyKey::new(key).expect("sha256 hook run idempotency key is valid")
}

fn background_resume_state(
    node: &HookExecutionNode,
    request: &HookPhaseRequest,
) -> Option<HookRunResumeState> {
    if !is_background_like(node.subscription.execution_policy.await_policy) {
        return None;
    }
    let snapshot = recoverable_background_input_snapshot(request)?;
    Some(HookRunResumeState::input_snapshot(
        node.subscription.execution_policy.clone(),
        node.subscription.failure_policy,
        node.subscription.retry_policy.clone(),
        node.handler.version(),
        node.handler.input_contract_version(),
        node.handler.output_contract_version(),
        snapshot,
    ))
}

fn recoverable_background_input_snapshot(
    request: &HookPhaseRequest,
) -> Option<HookRunInputSnapshot> {
    match &request.input.payload {
        HookInputPayload::TurnPostTurn(_) | HookInputPayload::Empty => {
            Some(HookRunInputSnapshot::new(
                request.phase,
                request.context.clone(),
                request.input.clone(),
                request.policy_set.clone(),
                request.prompt_context_set.clone(),
            ))
        }
        HookInputPayload::TurnPrePolicy(_)
        | HookInputPayload::TurnPrePromptContext(_)
        | HookInputPayload::TurnPreToolMaterialization(_)
        | HookInputPayload::TurnPrePromptCompile(_)
        | HookInputPayload::TurnPreCompaction(_)
        | HookInputPayload::Custom(_) => None,
    }
}

fn hash_segment(hasher: &mut Sha256, key: &str, value: &str) {
    hasher.update(key.as_bytes());
    hasher.update([0]);
    hasher.update(value.as_bytes());
    hasher.update([0xff]);
}

fn hook_run_scope_from_context(context: &HookContext) -> Option<HookRunScope> {
    if let Some(turn_id) = &context.turn_id {
        return hook_run_scope(HookRunScopeKind::Turn, turn_id.as_str());
    }
    if let Some(task_id) = &context.task_id {
        return hook_run_scope(HookRunScopeKind::Task, task_id.as_str());
    }
    if let Some(thread_id) = &context.thread_id {
        return hook_run_scope(HookRunScopeKind::Thread, thread_id.as_str());
    }
    if let Some(workspace_id) = &context.workspace_id {
        return hook_run_scope(HookRunScopeKind::Workspace, workspace_id.as_str());
    }
    if let Some(agent_id) = &context.agent_id {
        return hook_run_scope(HookRunScopeKind::Agent, agent_id.as_str());
    }
    None
}

fn hook_run_scope(kind: HookRunScopeKind, id: &str) -> Option<HookRunScope> {
    Some(HookRunScope {
        kind,
        id: crate::HookRunScopeId::new(id.to_owned()).ok()?,
    })
}

fn current_unix_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u128_to_i64_saturating(duration.as_millis()),
        Err(_) => 0,
    }
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn u128_to_i64_saturating(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

async fn execute_node_with_policy(
    node: &HookExecutionNode,
    request: HookPhaseRequest,
    options: &HookRuntimeOptions,
) -> HookRuntimeResult<HookNodeOutcome> {
    match node.subscription.failure_policy {
        HookFailurePolicy::Skip => return Ok(HookNodeOutcome::Skipped),
        HookFailurePolicy::Required
        | HookFailurePolicy::Fallback
        | HookFailurePolicy::BestEffort
        | HookFailurePolicy::FailClosed => {}
    }

    match node.subscription.execution_policy.await_policy {
        HookAwaitPolicy::Blocking => {
            let handler_request = handler_request(node, request);
            Ok(match node.handler.execute(handler_request).await {
                Ok(response) => validated_success(node.handler.as_ref(), response),
                Err(error) => HookNodeOutcome::Failed(error),
            })
        }
        HookAwaitPolicy::Deadline
        | HookAwaitPolicy::Background
        | HookAwaitPolicy::FireAndRecord => {
            let timeout_ms = node
                .subscription
                .execution_policy
                .timeout_ms
                .unwrap_or(options.default_deadline_timeout_ms);
            let handler_request = handler_request(node, request);
            let handler_future = node.handler.execute(handler_request);
            let timeout_future = Delay::new(Duration::from_millis(timeout_ms));
            match select(handler_future, timeout_future).await {
                Either::Left((Ok(response), _timeout_future)) => {
                    Ok(validated_success(node.handler.as_ref(), response))
                }
                Either::Left((Err(error), _timeout_future)) => Ok(HookNodeOutcome::Failed(error)),
                Either::Right((_elapsed, _handler_future)) => {
                    Ok(HookNodeOutcome::TimedOut { timeout_ms })
                }
            }
        }
    }
}

fn validated_success(handler: &dyn HookHandler, response: HookHandlerResponse) -> HookNodeOutcome {
    match missing_contribution_capability(handler, response.contributions.as_slice()) {
        Some(error) => HookNodeOutcome::Failed(error),
        None => HookNodeOutcome::Succeeded(response),
    }
}

fn validate_background_response(
    response: HookHandlerResponse,
) -> Result<HookHandlerResponse, HookError> {
    if let Some(contribution) = response
        .contributions
        .iter()
        .find(|contribution| !background_contribution_allowed(contribution))
    {
        return Err(HookError::new(
            HookDiagnosticCode::new("hook.background_contribution_not_allowed")
                .expect("static diagnostic code is valid"),
            HookDiagnosticMessage::new(format!(
                "background hook returned `{}` contribution that cannot mutate a completed phase",
                contribution.kind_name()
            ))
            .expect("static diagnostic message is valid"),
        )
        .with_safe_for_user(true));
    }
    Ok(response)
}

fn background_contribution_allowed(contribution: &HookContribution) -> bool {
    matches!(
        contribution,
        HookContribution::Audit(_) | HookContribution::BackgroundJob(_) | HookContribution::Noop
    )
}

fn should_retry_background_outcome(
    node: &HookExecutionNode,
    attempt_number: u16,
    status: HookRunStatus,
    error: &HookError,
) -> bool {
    if !is_background_like(node.subscription.execution_policy.await_policy) {
        return false;
    }
    if attempt_number >= node.subscription.retry_policy.max_attempts {
        return false;
    }
    match status {
        HookRunStatus::Failed if !error.retryable => return false,
        HookRunStatus::Failed | HookRunStatus::TimedOut => {}
        HookRunStatus::Queued
        | HookRunStatus::Running
        | HookRunStatus::Succeeded
        | HookRunStatus::Skipped => {
            return false;
        }
    }
    if node.subscription.retry_policy.idempotency_required && !retry_idempotency_satisfied(node) {
        return false;
    }
    true
}

fn retry_idempotency_satisfied(node: &HookExecutionNode) -> bool {
    let capability =
        HookCapability::new("idempotent_side_effect").expect("static hook capability is valid");
    if node.handler.capabilities().contains(&capability) {
        return true;
    }
    let key =
        HookMetadataKey::new("idempotent_side_effect").expect("static hook metadata key is valid");
    matches!(
        node.subscription.metadata.get(&key),
        Some(HookValue::Bool(true))
    )
}

fn retry_delay_ms(policy: &crate::HookRetryPolicy, attempt_number: u16) -> i64 {
    let base = u64_to_i64_saturating(policy.initial_delay_ms.unwrap_or(1_000));
    match policy.backoff {
        HookRetryBackoff::None => 0,
        HookRetryBackoff::Fixed => base,
        HookRetryBackoff::Exponential => {
            let exponent = u32::from(attempt_number.saturating_sub(1)).min(6);
            base.saturating_mul(1_i64 << exponent).min(60_000)
        }
    }
}

fn background_run_summary(
    run: &HookQueuedBackgroundRun,
    outcome: &HookNodeOutcome,
    status: HookRunStatus,
) -> HookBackgroundRunSummary {
    let (contribution_count, diagnostic_count) = match outcome {
        HookNodeOutcome::Succeeded(response) => {
            (response.contributions.len(), response.diagnostics.len())
        }
        HookNodeOutcome::Failed(_) | HookNodeOutcome::TimedOut { .. } => (0, 1),
        HookNodeOutcome::Skipped | HookNodeOutcome::Queued => (0, 0),
    };
    HookBackgroundRunSummary {
        subscription_id: run.node.subscription.subscription_id.clone(),
        hook_id: run.node.subscription.hook_id.clone(),
        phase: run.request.phase,
        await_policy: run.await_policy.as_await_policy(),
        status,
        contribution_count,
        diagnostic_count,
    }
}

fn missing_contribution_capability(
    handler: &dyn HookHandler,
    contributions: &[HookContribution],
) -> Option<HookError> {
    let capabilities = handler.capabilities();
    contributions.iter().find_map(|contribution| {
        let required = contribution.required_capability()?;
        if capabilities.contains(&required) {
            None
        } else {
            Some(HookError::new(
                HookDiagnosticCode::new("hook.capability_missing")
                    .expect("static diagnostic code is valid"),
                HookDiagnosticMessage::new(format!(
                    "hook `{}` returned `{}` contribution without `{}` capability",
                    handler.id(),
                    contribution.kind_name(),
                    required
                ))
                .expect("static diagnostic message is valid"),
            ))
        }
    })
}

impl HookContributionHash {
    pub fn from_contribution(contribution: &HookContribution) -> Option<Self> {
        let bytes = serde_json::to_vec(contribution).ok()?;
        let digest = Sha256::digest(bytes);
        Self::new(format!("sha256:{}", hex::encode(digest))).ok()
    }
}

fn handler_request(node: &HookExecutionNode, request: HookPhaseRequest) -> HookHandlerRequest {
    HookHandlerRequest {
        hook_id: node.subscription.hook_id.clone(),
        phase: request.phase,
        context: request.context,
        input: request.input,
        policy_set: request.policy_set,
        prompt_context_set: request.prompt_context_set,
    }
}

fn append_success(
    phase_response: &mut HookPhaseResponse,
    subscription_id: HookSubscriptionId,
    hook_id: HookId,
    phase: HookPhase,
    handler_response: HookHandlerResponse,
    options: &HookRuntimeOptions,
) {
    let HookHandlerResponse {
        contributions,
        diagnostics,
        ..
    } = handler_response;
    let policy = options.diagnostic_redaction_policy();
    let contribution_hashes = hash_contributions(&contributions);
    let diagnostic_previews = diagnostic_previews(&diagnostics, &policy);
    let redacted_diagnostics = redacted_diagnostics(diagnostics, &policy);
    let contribution_count = contributions.len();
    let diagnostic_count = redacted_diagnostics.len();
    let attempt = HookAttemptSummary {
        attempt_number: 1,
        status: HookRunStatus::Succeeded,
        contribution_count,
        diagnostic_count,
        contribution_hashes: contribution_hashes.clone(),
        diagnostic_previews: diagnostic_previews.clone(),
        error: None,
    };
    phase_response.contributions.extend(contributions);
    phase_response.diagnostics.extend(redacted_diagnostics);
    phase_response.runs.push(HookRunSummary {
        subscription_id,
        hook_id,
        phase,
        status: HookRunStatus::Succeeded,
        attempt_count: 1,
        contribution_count,
        diagnostic_count,
        contribution_hashes,
        diagnostic_previews,
        attempts: vec![attempt],
        error: None,
    });
}

fn append_best_effort_failure(
    phase_response: &mut HookPhaseResponse,
    subscription_id: HookSubscriptionId,
    hook_id: HookId,
    phase: HookPhase,
    status: HookRunStatus,
    error: HookError,
    options: &HookRuntimeOptions,
) {
    let policy = options.diagnostic_redaction_policy();
    let diagnostic = diagnostic_from_error(&error, status);
    let redacted_diagnostic = diagnostic.redacted(&policy);
    let diagnostic_preview = diagnostic.preview(&policy);
    let error_summary = HookRunErrorSummary::from_error(&error, &policy);
    let attempt = HookAttemptSummary {
        attempt_number: 1,
        status,
        contribution_count: 0,
        diagnostic_count: 1,
        contribution_hashes: Vec::new(),
        diagnostic_previews: vec![diagnostic_preview.clone()],
        error: Some(error_summary.clone()),
    };
    phase_response.diagnostics.push(redacted_diagnostic);
    phase_response.runs.push(HookRunSummary {
        subscription_id,
        hook_id,
        phase,
        status,
        attempt_count: 1,
        contribution_count: 0,
        diagnostic_count: 1,
        contribution_hashes: Vec::new(),
        diagnostic_previews: vec![diagnostic_preview],
        attempts: vec![attempt],
        error: Some(error_summary),
    });
}

fn append_fallback_failure(
    phase_response: &mut HookPhaseResponse,
    subscription: &HookSubscription,
    phase: HookPhase,
    status: HookRunStatus,
    error: HookError,
    options: &HookRuntimeOptions,
) {
    let policy = options.diagnostic_redaction_policy();
    let contribution_hashes = hash_contributions(&subscription.fallback_contributions);
    let contribution_count = subscription.fallback_contributions.len();
    let diagnostic = diagnostic_from_error(&error, status);
    let redacted_diagnostic = diagnostic.redacted(&policy);
    let diagnostic_preview = diagnostic.preview(&policy);
    let error_summary = HookRunErrorSummary::from_error(&error, &policy);
    let attempt = HookAttemptSummary {
        attempt_number: 1,
        status,
        contribution_count: 0,
        diagnostic_count: 1,
        contribution_hashes: Vec::new(),
        diagnostic_previews: vec![diagnostic_preview.clone()],
        error: Some(error_summary.clone()),
    };
    phase_response
        .contributions
        .extend(subscription.fallback_contributions.clone());
    phase_response.diagnostics.push(redacted_diagnostic);
    phase_response.runs.push(HookRunSummary {
        subscription_id: subscription.subscription_id.clone(),
        hook_id: subscription.hook_id.clone(),
        phase,
        status,
        attempt_count: 1,
        contribution_count,
        diagnostic_count: 1,
        contribution_hashes,
        diagnostic_previews: vec![diagnostic_preview],
        attempts: vec![attempt],
        error: Some(error_summary),
    });
}

fn append_skipped(
    phase_response: &mut HookPhaseResponse,
    subscription: HookSubscription,
    phase: HookPhase,
) {
    phase_response.runs.push(HookRunSummary {
        subscription_id: subscription.subscription_id,
        hook_id: subscription.hook_id,
        phase,
        status: HookRunStatus::Skipped,
        attempt_count: 0,
        contribution_count: 0,
        diagnostic_count: 0,
        contribution_hashes: Vec::new(),
        diagnostic_previews: Vec::new(),
        attempts: Vec::new(),
        error: None,
    });
}

fn append_queued(
    phase_response: &mut HookPhaseResponse,
    subscription_id: HookSubscriptionId,
    hook_id: HookId,
    phase: HookPhase,
) {
    phase_response.runs.push(HookRunSummary {
        subscription_id,
        hook_id,
        phase,
        status: HookRunStatus::Queued,
        attempt_count: 0,
        contribution_count: 0,
        diagnostic_count: 0,
        contribution_hashes: Vec::new(),
        diagnostic_previews: Vec::new(),
        attempts: Vec::new(),
        error: None,
    });
}

fn timeout_error(timeout_ms: u64) -> HookError {
    HookError::new(
        HookDiagnosticCode::new("hook.timeout").expect("static diagnostic code is valid"),
        HookDiagnosticMessage::new(format!("hook exceeded deadline of {} ms", timeout_ms))
            .expect("static diagnostic message is valid"),
    )
}

fn diagnostic_from_error(error: &HookError, status: HookRunStatus) -> HookDiagnostic {
    HookDiagnostic {
        code: error.code.clone(),
        message: error.message.clone(),
        severity: match status {
            HookRunStatus::Queued
            | HookRunStatus::Running
            | HookRunStatus::Succeeded
            | HookRunStatus::Skipped => HookDiagnosticSeverity::Info,
            HookRunStatus::Failed | HookRunStatus::TimedOut => HookDiagnosticSeverity::Warning,
        },
        safe_for_user: error.safe_for_user,
        metadata: HookMetadata::default(),
    }
}

fn redacted_diagnostics(
    diagnostics: Vec<HookDiagnostic>,
    policy: &HookDiagnosticRedactionPolicy,
) -> Vec<HookDiagnostic> {
    diagnostics
        .into_iter()
        .map(|diagnostic| diagnostic.redacted(policy))
        .collect()
}

fn diagnostic_previews(
    diagnostics: &[HookDiagnostic],
    policy: &HookDiagnosticRedactionPolicy,
) -> Vec<HookDiagnosticPreview> {
    diagnostics
        .iter()
        .map(|diagnostic| diagnostic.preview(policy))
        .collect()
}

fn hash_contributions(contributions: &[HookContribution]) -> Vec<HookContributionHash> {
    contributions.iter().filter_map(hash_contribution).collect()
}

fn hash_contribution(contribution: &HookContribution) -> Option<HookContributionHash> {
    HookContributionHash::from_contribution(contribution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HookAuditEventStoreRecord, HookCapabilities, HookCapability, HookDiagnosticMessage,
        HookDomain, HookError, HookExecutionPolicy, HookFilterKey, HookHandler, HookKind,
        HookPromptContent, HookPromptSectionTitle, HookResult, HookRunAttemptId, HookRunId,
        HookRunStoreResult, HookSectionId, HookSubscription, HookSubscriptionDependencies,
        HookValue, NewHookAuditEventStoreRecord,
    };
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    use std::time::Duration;
    use tokio::sync::Barrier;

    struct RecordingHookHandler {
        id: HookId,
        phases: Vec<HookPhase>,
        calls: Arc<Mutex<Vec<HookId>>>,
        responses: Mutex<VecDeque<HookResult<HookHandlerResponse>>>,
        capabilities: HookCapabilities,
    }

    #[async_trait]
    impl HookHandler for RecordingHookHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            self.phases.clone()
        }

        fn capabilities(&self) -> HookCapabilities {
            self.capabilities.clone()
        }

        async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
            assert_eq!(request.hook_id, self.id);
            assert_eq!(request.phase, HookPhase::TurnPrePromptCompile);
            assert_eq!(request.context, HookContext::default());
            assert_eq!(
                request.input.kind,
                crate::HookInputKind::TurnPrePromptCompile
            );
            self.calls.lock().expect("calls lock").push(self.id.clone());
            self.responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .expect("test response exists")
        }
    }

    struct BarrierHookHandler {
        id: HookId,
        barrier: Arc<Barrier>,
        started_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HookHandler for BarrierHookHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPrePromptCompile]
        }

        async fn execute(&self, _request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
            self.started_count.fetch_add(1, Ordering::SeqCst);
            self.barrier.wait().await;
            Ok(HookHandlerResponse::default())
        }
    }

    struct ConcurrencyTrackingHookHandler {
        id: HookId,
        active_count: Arc<AtomicUsize>,
        max_active_count: Arc<AtomicUsize>,
        delay: Duration,
    }

    #[async_trait]
    impl HookHandler for ConcurrencyTrackingHookHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPrePromptCompile]
        }

        async fn execute(&self, _request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
            let active = self.active_count.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active_count
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    (active > current).then_some(active)
                })
                .ok();
            tokio::time::sleep(self.delay).await;
            self.active_count.fetch_sub(1, Ordering::SeqCst);
            Ok(HookHandlerResponse::default())
        }
    }

    struct DelayedHookHandler {
        id: HookId,
        delay: Duration,
        contribution: HookContribution,
    }

    #[async_trait]
    impl HookHandler for DelayedHookHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPrePromptCompile]
        }

        fn capabilities(&self) -> HookCapabilities {
            test_output_capabilities()
        }

        async fn execute(&self, _request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
            tokio::time::sleep(self.delay).await;
            Ok(HookHandlerResponse {
                contributions: vec![self.contribution.clone()],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })
        }
    }

    struct PolicySetRecordingHookHandler {
        id: HookId,
        captured_policy_sets: Arc<Mutex<Vec<HookPolicySet>>>,
        captured_prompt_context_sets: Arc<Mutex<Vec<HookPromptContextSet>>>,
    }

    #[async_trait]
    impl HookHandler for PolicySetRecordingHookHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPrePromptCompile]
        }

        async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
            self.captured_policy_sets
                .lock()
                .expect("policy sets lock")
                .push(request.policy_set);
            self.captured_prompt_context_sets
                .lock()
                .expect("prompt context sets lock")
                .push(request.prompt_context_set);
            Ok(HookHandlerResponse::default())
        }
    }

    fn block_on_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future unexpectedly pending"),
        }
    }

    fn noop_waker() -> Waker {
        unsafe fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe fn wake(_: *const ()) {}
        unsafe fn wake_by_ref(_: *const ()) {}
        unsafe fn drop(_: *const ()) {}

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        let raw = RawWaker::new(std::ptr::null(), &VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    fn hook_id(value: &str) -> HookId {
        HookId::new(value).expect("valid hook id")
    }

    fn subscription_id(value: &str) -> HookSubscriptionId {
        HookSubscriptionId::new(value).expect("valid subscription id")
    }

    fn test_output_capabilities() -> HookCapabilities {
        HookCapabilities::new([
            HookCapability::new("contribute_policy").expect("valid capability"),
            HookCapability::new("contribute_prompt_context").expect("valid capability"),
            HookCapability::new("contribute_prompt_section").expect("valid capability"),
            HookCapability::new("contribute_tool_bundle").expect("valid capability"),
            HookCapability::new("contribute_prompt_manifest_diagnostic").expect("valid capability"),
            HookCapability::new("emit_audit").expect("valid capability"),
            HookCapability::new("schedule_background_job").expect("valid capability"),
        ])
    }

    fn phase_request() -> HookPhaseRequest {
        HookPhaseRequest::new(
            HookPhase::TurnPrePromptCompile,
            HookContext::default(),
            HookInput::empty(crate::HookInputKind::TurnPrePromptCompile),
        )
    }

    fn contribution(section_id: &str, content: &str) -> HookContribution {
        HookContribution::PromptSection(crate::PromptSectionContribution {
            contribution_id: crate::HookContributionId::new(section_id)
                .expect("valid contribution id"),
            section_id: HookSectionId::new(section_id).expect("valid section id"),
            title: Some(HookPromptSectionTitle::new("Test").expect("valid title")),
            domain: HookDomain::new("test").expect("valid domain"),
            priority: 0,
            content: HookPromptContent::new(content).expect("valid content"),
            max_chars: None,
            diagnostics: Vec::new(),
            truncated: false,
        })
    }

    fn audit_contribution(event_kind: &str) -> HookContribution {
        HookContribution::Audit(crate::AuditContribution {
            event_kind: crate::HookAuditEventKind::new(event_kind).expect("valid event kind"),
            details: HookValue::Text("audit details".to_owned()),
            safe_for_user: false,
        })
    }

    fn policy_contribution(key: &str) -> HookContribution {
        HookContribution::Policy(crate::PolicyContribution {
            domain: HookDomain::new("test").expect("valid domain"),
            key: crate::HookPolicyKey::new(key).expect("valid policy key"),
            value: HookValue::Bool(true),
            priority: 0,
            diagnostics: Vec::new(),
        })
    }

    fn prompt_context_contribution(id: &str) -> HookContribution {
        HookContribution::PromptContext(crate::PromptContextContribution {
            contribution_id: crate::HookContributionId::new(id).expect("valid contribution id"),
            domain: HookDomain::new("test").expect("valid domain"),
            priority: 0,
            content: HookPromptContent::new("context").expect("valid content"),
            max_chars: None,
            source_refs: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
        })
    }

    fn tool_bundle_contribution(id: &str) -> HookContribution {
        HookContribution::ToolBundle(crate::ToolBundleContribution {
            contribution_id: crate::HookContributionId::new(id).expect("valid contribution id"),
            bundle_id: crate::HookToolBundleId::new(format!("{id}.bundle"))
                .expect("valid bundle id"),
            domain: HookDomain::new("test").expect("valid domain"),
            priority: 0,
            tool_names: vec![crate::HookToolName::new("test_tool").expect("valid tool name")],
            diagnostics: Vec::new(),
        })
    }

    fn prompt_manifest_diagnostic_contribution(code: &str) -> HookContribution {
        HookContribution::PromptManifestDiagnostic(crate::PromptManifestDiagnosticContribution {
            code: HookDiagnosticCode::new(code).expect("valid diagnostic code"),
            message: HookDiagnosticMessage::new("manifest diagnostic").expect("valid message"),
            severity: HookDiagnosticSeverity::Info,
            safe_for_user: true,
            hook_id: None,
            subscription_id: None,
        })
    }

    fn contribution_section_ids(response: &HookPhaseResponse) -> Vec<HookSectionId> {
        response
            .contributions
            .iter()
            .map(|contribution| match contribution {
                HookContribution::PromptSection(section) => section.section_id.clone(),
                _ => panic!("expected prompt section contribution"),
            })
            .collect()
    }

    fn diagnostic(code: &str, message: &str) -> HookDiagnostic {
        HookDiagnostic {
            code: HookDiagnosticCode::new(code).expect("valid code"),
            message: HookDiagnosticMessage::new(message).expect("valid message"),
            severity: HookDiagnosticSeverity::Info,
            safe_for_user: false,
            metadata: HookMetadata::default(),
        }
    }

    fn hook_error(code: &str, message: &str) -> HookError {
        HookError::new(
            HookDiagnosticCode::new(code).expect("valid code"),
            HookDiagnosticMessage::new(message).expect("valid message"),
        )
    }

    fn handler(
        id: &str,
        calls: Arc<Mutex<Vec<HookId>>>,
        responses: Vec<HookResult<HookHandlerResponse>>,
    ) -> Arc<dyn HookHandler> {
        Arc::new(RecordingHookHandler {
            id: hook_id(id),
            phases: vec![HookPhase::TurnPrePromptCompile],
            calls,
            responses: Mutex::new(VecDeque::from(responses)),
            capabilities: test_output_capabilities(),
        })
    }

    fn register_handler(
        registry: &HookRegistry,
        id: &str,
        calls: Arc<Mutex<Vec<HookId>>>,
        responses: Vec<HookResult<HookHandlerResponse>>,
    ) {
        registry
            .register_handler(handler(id, calls, responses))
            .expect("handler registers");
    }

    fn register_subscription(
        handlers: &HookRegistry,
        subscriptions: &HookSubscriptionRegistry,
        subscription_id: &str,
        hook_id: &str,
        priority: i32,
        failure_policy: HookFailurePolicy,
    ) {
        subscriptions
            .register_subscription(
                handlers,
                HookSubscription::new(
                    self::subscription_id(subscription_id),
                    self::hook_id(hook_id),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_priority(priority)
                .with_failure_policy(failure_policy),
            )
            .expect("subscription registers");
    }

    fn register_subscription_with_dependencies(
        handlers: &HookRegistry,
        subscriptions: &HookSubscriptionRegistry,
        subscription_id: &str,
        hook_id: &str,
        priority: i32,
        dependencies: HookSubscriptionDependencies,
    ) {
        subscriptions
            .register_subscription(
                handlers,
                HookSubscription::new(
                    self::subscription_id(subscription_id),
                    self::hook_id(hook_id),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_priority(priority)
                .with_dependencies(dependencies),
            )
            .expect("subscription registers");
    }

    fn register_subscription_with_policy(
        handlers: &HookRegistry,
        subscriptions: &HookSubscriptionRegistry,
        subscription_id: &str,
        hook_id: &str,
        priority: i32,
        await_policy: HookAwaitPolicy,
        timeout_ms: Option<u64>,
        failure_policy: HookFailurePolicy,
        fallback_contributions: impl IntoIterator<Item = HookContribution>,
    ) {
        subscriptions
            .register_subscription(
                handlers,
                HookSubscription::new(
                    self::subscription_id(subscription_id),
                    self::hook_id(hook_id),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_priority(priority)
                .with_execution_policy(crate::HookExecutionPolicy {
                    await_policy,
                    timeout_ms,
                    max_parallelism: None,
                })
                .with_failure_policy(failure_policy)
                .with_fallback_contributions(fallback_contributions),
            )
            .expect("subscription registers");
    }

    fn runtime(
        handlers: Arc<HookRegistry>,
        subscriptions: Arc<HookSubscriptionRegistry>,
    ) -> HookRuntime {
        HookRuntime::new(handlers, subscriptions)
    }

    #[test]
    fn no_subscriptions_returns_empty_response() {
        let runtime = runtime(
            Arc::new(HookRegistry::new()),
            Arc::new(HookSubscriptionRegistry::new()),
        );

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response, HookPhaseResponse::default());
    }

    #[test]
    fn one_hook_returns_contribution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.one",
            calls.clone(),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.one", "context")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.one",
            "test.one",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.contributions.len(), 1);
        assert!(response.diagnostics.is_empty());
        assert_eq!(response.runs.len(), 1);
        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);
        assert_eq!(response.runs[0].contribution_count, 1);
        assert_eq!(response.runs[0].attempt_count, 1);
        assert_eq!(response.runs[0].attempts.len(), 1);
        assert_eq!(response.runs[0].attempts[0].attempt_number, 1);
        assert_eq!(
            response.runs[0].attempts[0].status,
            HookRunStatus::Succeeded
        );
        assert_eq!(response.runs[0].contribution_hashes.len(), 1);
        assert!(
            response.runs[0].contribution_hashes[0]
                .as_str()
                .starts_with("sha256:")
        );
        assert_eq!(
            response.runs[0].attempts[0].contribution_hashes,
            response.runs[0].contribution_hashes
        );
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.one")]
        );
    }

    #[test]
    fn contribution_without_required_capability_is_rejected() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let hook_id = hook_id("test.no_capability");
        handlers
            .register_handler(Arc::new(RecordingHookHandler {
                id: hook_id.clone(),
                phases: vec![HookPhase::TurnPrePromptCompile],
                calls: calls.clone(),
                responses: Mutex::new(VecDeque::from([Ok(HookHandlerResponse {
                    contributions: vec![contribution("section.no_capability", "context")],
                    diagnostics: Vec::new(),
                    metadata: HookMetadata::default(),
                })])),
                capabilities: HookCapabilities::default(),
            }))
            .expect("handler registers");
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.no_capability",
            "test.no_capability",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(*calls.lock().expect("calls lock"), vec![hook_id]);
        assert!(response.contributions.is_empty());
        assert_eq!(response.runs[0].status, HookRunStatus::Failed);
        assert_eq!(
            response.diagnostics[0].code,
            HookDiagnosticCode::new("hook.capability_missing").expect("valid code")
        );
    }

    #[test]
    fn multiple_hooks_run_in_deterministic_order() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        for id in ["test.c", "test.b", "test.a"] {
            register_handler(
                &handlers,
                id,
                calls.clone(),
                vec![Ok(HookHandlerResponse {
                    contributions: vec![contribution(id, id)],
                    diagnostics: Vec::new(),
                    metadata: HookMetadata::default(),
                })],
            );
        }
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.c",
            "test.c",
            20,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.b",
            "test.b",
            10,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.a",
            "test.a",
            10,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.a"), hook_id("test.b"), hook_id("test.c")]
        );
        assert_eq!(
            response
                .runs
                .into_iter()
                .map(|run| run.subscription_id)
                .collect::<Vec<_>>(),
            vec![
                subscription_id("sub.a"),
                subscription_id("sub.b"),
                subscription_id("sub.c")
            ]
        );
    }

    #[test]
    fn best_effort_failure_records_diagnostic_and_continues() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.fail",
            calls.clone(),
            vec![Err(
                hook_error("hook.failed", "failed").with_safe_for_user(true)
            )],
        );
        register_handler(
            &handlers,
            "test.ok",
            calls.clone(),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.ok", "ok")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.fail",
            "test.fail",
            0,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.ok",
            "test.ok",
            1,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response = block_on_ready(runtime.run_phase(phase_request()))
            .expect("best effort failure continues");

        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.fail"), hook_id("test.ok")]
        );
        assert_eq!(response.contributions.len(), 1);
        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.diagnostics[0].code.as_str(), "hook.failed");
        assert_eq!(
            response.diagnostics[0].severity,
            HookDiagnosticSeverity::Warning
        );
        assert!(response.diagnostics[0].safe_for_user);
        assert_eq!(response.runs[0].status, HookRunStatus::Failed);
        assert_eq!(response.runs[0].diagnostic_count, 1);
        assert_eq!(response.runs[0].attempts.len(), 1);
        assert_eq!(response.runs[0].attempts[0].status, HookRunStatus::Failed);
        assert_eq!(
            response.runs[0]
                .error
                .as_ref()
                .expect("run error")
                .message
                .as_str(),
            "failed"
        );
        assert_eq!(response.runs[1].status, HookRunStatus::Succeeded);
    }

    #[test]
    fn non_best_effort_failure_returns_runtime_error() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.fail",
            calls.clone(),
            vec![Err(hook_error("hook.failed", "failed"))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.fail",
            "test.fail",
            0,
            HookFailurePolicy::Required,
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request())).expect_err("runtime fails");

        assert!(matches!(
            error,
            HookRuntimeError::HookFailed { subscription_id, hook_id, phase, .. }
                if subscription_id == self::subscription_id("sub.fail")
                    && hook_id == self::hook_id("test.fail")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
    }

    #[test]
    fn disabled_subscription_is_not_executed() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.one",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.one",
            "test.one",
            0,
            HookFailurePolicy::BestEffort,
        );
        subscriptions
            .disable_subscription(&subscription_id("sub.one"))
            .expect("disable succeeds");
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response, HookPhaseResponse::default());
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[test]
    fn runtime_uses_subscription_lookup_order_for_runs() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        for id in ["test.second", "test.first"] {
            register_handler(
                &handlers,
                id,
                calls.clone(),
                vec![Ok(HookHandlerResponse::default())],
            );
        }
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.second",
            "test.second",
            2,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.first",
            "test.first",
            1,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            response
                .runs
                .into_iter()
                .map(|run| run.subscription_id)
                .collect::<Vec<_>>(),
            vec![subscription_id("sub.first"), subscription_id("sub.second")]
        );
    }

    #[test]
    fn missing_handler_during_runtime_returns_error() {
        let registration_handlers = Arc::new(HookRegistry::new());
        let runtime_handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &registration_handlers,
            "test.missing",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription(
            &registration_handlers,
            &subscriptions,
            "sub.missing",
            "test.missing",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(runtime_handlers, subscriptions);

        let error =
            block_on_ready(runtime.run_phase(phase_request())).expect_err("handler missing");

        assert!(matches!(
            error,
            HookRuntimeError::MissingHandler { subscription_id, hook_id, phase }
                if subscription_id == self::subscription_id("sub.missing")
                    && hook_id == self::hook_id("test.missing")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
    }

    #[test]
    fn handler_response_diagnostics_are_aggregated() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.one",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: Vec::new(),
                diagnostics: vec![diagnostic("diag.one", "diagnostic")],
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.one",
            "test.one",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.runs[0].diagnostic_count, 1);
    }

    #[test]
    fn phase_request_roundtrips() {
        let request = phase_request();
        let value = serde_json::to_value(&request).expect("request serializes");
        let decoded: HookPhaseRequest =
            serde_json::from_value(value).expect("request deserializes");

        assert_eq!(decoded, request);
    }

    #[test]
    fn hook_phase_request_defaults_to_empty_policy_set() {
        assert!(phase_request().policy_set.is_empty());
    }

    #[test]
    fn hook_phase_request_defaults_to_empty_prompt_context_set() {
        assert!(phase_request().prompt_context_set.is_empty());
    }

    #[test]
    fn hook_phase_request_with_policy_set_reaches_handler_request() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let captured_policy_sets = Arc::new(Mutex::new(Vec::new()));
        let captured_prompt_context_sets = Arc::new(Mutex::new(Vec::new()));
        let hook_id = hook_id("test.policy_receiver");
        handlers
            .register_handler(Arc::new(PolicySetRecordingHookHandler {
                id: hook_id.clone(),
                captured_policy_sets: captured_policy_sets.clone(),
                captured_prompt_context_sets: captured_prompt_context_sets.clone(),
            }))
            .expect("handler registers");
        subscriptions
            .register_subscription(
                &handlers,
                HookSubscription::new(
                    subscription_id("sub.policy_receiver"),
                    hook_id,
                    HookPhase::TurnPrePromptCompile,
                ),
            )
            .expect("subscription registers");
        let runtime = runtime(handlers, subscriptions);
        let policy_set = HookPolicySet::merge_contributions([crate::PolicyContribution {
            domain: HookDomain::new("test").expect("valid domain"),
            key: crate::HookPolicyKey::new("mode").expect("valid key"),
            value: HookValue::Text("strict".to_owned()),
            priority: 10,
            diagnostics: Vec::new(),
        }]);

        block_on_ready(runtime.run_phase(phase_request().with_policy_set(policy_set.clone())))
            .expect("phase execution succeeds");

        assert_eq!(
            *captured_policy_sets.lock().expect("policy sets lock"),
            vec![policy_set]
        );
        assert_eq!(
            *captured_prompt_context_sets
                .lock()
                .expect("prompt context sets lock"),
            vec![HookPromptContextSet::empty()]
        );
    }

    #[test]
    fn hook_phase_request_with_prompt_context_set_reaches_handler_request() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let captured_policy_sets = Arc::new(Mutex::new(Vec::new()));
        let captured_prompt_context_sets = Arc::new(Mutex::new(Vec::new()));
        let hook_id = hook_id("test.prompt_context_receiver");
        handlers
            .register_handler(Arc::new(PolicySetRecordingHookHandler {
                id: hook_id.clone(),
                captured_policy_sets: captured_policy_sets.clone(),
                captured_prompt_context_sets: captured_prompt_context_sets.clone(),
            }))
            .expect("handler registers");
        subscriptions
            .register_subscription(
                &handlers,
                HookSubscription::new(
                    subscription_id("sub.prompt_context_receiver"),
                    hook_id,
                    HookPhase::TurnPrePromptCompile,
                ),
            )
            .expect("subscription registers");
        let runtime = runtime(handlers, subscriptions);
        let prompt_context_set = HookPromptContextSet::aggregate_contributions(
            [crate::PromptContextContribution {
                contribution_id: crate::HookContributionId::new("test.context.one")
                    .expect("valid contribution id"),
                domain: HookDomain::new("test").expect("valid domain"),
                priority: 10,
                content: crate::HookPromptContent::new("context").expect("valid content"),
                max_chars: None,
                source_refs: Vec::new(),
                diagnostics: Vec::new(),
                truncated: false,
            }],
            crate::HookPromptContextLimits::default(),
        );

        block_on_ready(
            runtime.run_phase(phase_request().with_prompt_context_set(prompt_context_set.clone())),
        )
        .expect("phase execution succeeds");

        assert_eq!(
            *captured_policy_sets.lock().expect("policy sets lock"),
            vec![HookPolicySet::empty()]
        );
        assert_eq!(
            *captured_prompt_context_sets
                .lock()
                .expect("prompt context sets lock"),
            vec![prompt_context_set]
        );
    }

    #[test]
    fn phase_response_default_is_empty() {
        assert_eq!(HookPhaseResponse::default().runs.len(), 0);
        assert!(HookPhaseResponse::default().contributions.is_empty());
        assert!(HookPhaseResponse::default().diagnostics.is_empty());
    }

    #[test]
    fn run_status_serializes_stably() {
        for (status, expected) in [
            (HookRunStatus::Queued, "queued"),
            (HookRunStatus::Running, "running"),
            (HookRunStatus::Succeeded, "succeeded"),
            (HookRunStatus::Failed, "failed"),
            (HookRunStatus::TimedOut, "timed_out"),
            (HookRunStatus::Skipped, "skipped"),
        ] {
            assert_eq!(
                serde_json::to_value(status).expect("status serializes"),
                serde_json::json!(expected)
            );
        }
    }

    #[test]
    fn runtime_error_display_is_stable() {
        let error = HookRuntimeError::MissingHandler {
            subscription_id: subscription_id("sub.missing"),
            hook_id: hook_id("test.missing"),
            phase: HookPhase::TurnPrePromptCompile,
        };

        assert_eq!(
            error.to_string(),
            "hook subscription `sub.missing` references missing handler `test.missing` for phase `turn.pre_prompt_compile`"
        );
    }

    #[test]
    fn filters_still_are_not_evaluated_in_phase_04() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.filtered",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        subscriptions
            .register_subscription(
                &handlers,
                HookSubscription::new(
                    subscription_id("sub.filtered"),
                    hook_id("test.filtered"),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_filters(BTreeMap::from([(
                    HookFilterKey::new("would.not.match").expect("valid filter key"),
                    HookValue::Bool(false),
                )])),
            )
            .expect("subscription registers");
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs.len(), 1);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.filtered")]
        );
    }

    #[test]
    fn dependency_after_order_is_respected() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        for id in ["test.early", "test.late"] {
            register_handler(
                &handlers,
                id,
                calls.clone(),
                vec![Ok(HookHandlerResponse::default())],
            );
        }
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.early",
            "test.early",
            0,
            HookSubscriptionDependencies::new([subscription_id("sub.late")], []),
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.late",
            "test.late",
            1,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            response
                .runs
                .into_iter()
                .map(|run| run.subscription_id)
                .collect::<Vec<_>>(),
            vec![subscription_id("sub.late"), subscription_id("sub.early")]
        );
    }

    #[test]
    fn dependency_before_order_is_respected() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        for id in ["test.before", "test.after"] {
            register_handler(
                &handlers,
                id,
                calls.clone(),
                vec![Ok(HookHandlerResponse::default())],
            );
        }
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.before",
            "test.before",
            10,
            HookSubscriptionDependencies::new([], [subscription_id("sub.after")]),
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.after",
            "test.after",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            response
                .runs
                .into_iter()
                .map(|run| run.subscription_id)
                .collect::<Vec<_>>(),
            vec![subscription_id("sub.before"), subscription_id("sub.after")]
        );
    }

    #[test]
    fn dependency_cycle_is_rejected() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        for id in ["test.a", "test.b"] {
            register_handler(
                &handlers,
                id,
                calls.clone(),
                vec![Ok(HookHandlerResponse::default())],
            );
        }
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.a",
            "test.a",
            0,
            HookSubscriptionDependencies::new([subscription_id("sub.b")], []),
        );
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.b",
            "test.b",
            1,
            HookSubscriptionDependencies::new([subscription_id("sub.a")], []),
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request()))
            .expect_err("cycle should be rejected");

        assert!(matches!(
            error,
            HookRuntimeError::DependencyCycle { phase, subscription_ids }
                if phase == HookPhase::TurnPrePromptCompile
                    && subscription_ids == vec![subscription_id("sub.a"), subscription_id("sub.b")]
        ));
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[test]
    fn self_dependency_is_rejected_as_cycle() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.self",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.self",
            "test.self",
            0,
            HookSubscriptionDependencies::new([subscription_id("sub.self")], []),
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request()))
            .expect_err("self dependency should be rejected");

        assert!(matches!(
            error,
            HookRuntimeError::DependencyCycle { phase, subscription_ids }
                if phase == HookPhase::TurnPrePromptCompile
                    && subscription_ids == vec![subscription_id("sub.self")]
        ));
    }

    #[test]
    fn missing_dependency_is_rejected() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.one",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.one",
            "test.one",
            0,
            HookSubscriptionDependencies::new([subscription_id("sub.missing")], []),
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request()))
            .expect_err("missing dependency should be rejected");

        assert!(matches!(
            error,
            HookRuntimeError::MissingDependency { subscription_id, dependency_id, phase }
                if subscription_id == self::subscription_id("sub.one")
                    && dependency_id == self::subscription_id("sub.missing")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[test]
    fn disabled_dependency_is_reported_missing() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        for id in ["test.enabled", "test.disabled"] {
            register_handler(
                &handlers,
                id,
                Arc::new(Mutex::new(Vec::new())),
                vec![Ok(HookHandlerResponse::default())],
            );
        }
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.enabled",
            "test.enabled",
            0,
            HookSubscriptionDependencies::new([subscription_id("sub.disabled")], []),
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.disabled",
            "test.disabled",
            1,
            HookFailurePolicy::BestEffort,
        );
        subscriptions
            .disable_subscription(&subscription_id("sub.disabled"))
            .expect("disable succeeds");
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request()))
            .expect_err("disabled dependency should be missing");

        assert!(matches!(
            error,
            HookRuntimeError::MissingDependency { subscription_id, dependency_id, phase }
                if subscription_id == self::subscription_id("sub.enabled")
                    && dependency_id == self::subscription_id("sub.disabled")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
    }

    #[test]
    fn missing_handler_is_resolved_before_any_handler_executes() {
        let registration_handlers = Arc::new(HookRegistry::new());
        let runtime_handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &registration_handlers,
            "test.ok",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_handler(
            &registration_handlers,
            "test.missing",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        runtime_handlers
            .register_handler(handler(
                "test.ok",
                calls.clone(),
                vec![Ok(HookHandlerResponse::default())],
            ))
            .expect("handler registers");
        register_subscription(
            &registration_handlers,
            &subscriptions,
            "sub.ok",
            "test.ok",
            0,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &registration_handlers,
            &subscriptions,
            "sub.missing",
            "test.missing",
            1,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(runtime_handlers, subscriptions);

        let error =
            block_on_ready(runtime.run_phase(phase_request())).expect_err("handler missing");

        assert!(matches!(
            error,
            HookRuntimeError::MissingHandler { subscription_id, hook_id, phase }
                if subscription_id == self::subscription_id("sub.missing")
                    && hook_id == self::hook_id("test.missing")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_hooks_run_concurrently() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let barrier = Arc::new(Barrier::new(2));
        let started_count = Arc::new(AtomicUsize::new(0));
        for id in ["test.one", "test.two"] {
            handlers
                .register_handler(Arc::new(BarrierHookHandler {
                    id: hook_id(id),
                    barrier: barrier.clone(),
                    started_count: started_count.clone(),
                }))
                .expect("handler registers");
        }
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.one",
            "test.one",
            0,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.two",
            "test.two",
            1,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("parallel execution should not hang")
                .expect("phase execution succeeds");

        assert_eq!(started_count.load(Ordering::SeqCst), 2);
        assert_eq!(response.runs.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn max_parallelism_limits_ready_batch_width() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let active_count = Arc::new(AtomicUsize::new(0));
        let max_active_count = Arc::new(AtomicUsize::new(0));

        for id in ["test.limit.one", "test.limit.two", "test.limit.three"] {
            handlers
                .register_handler(Arc::new(ConcurrencyTrackingHookHandler {
                    id: hook_id(id),
                    active_count: active_count.clone(),
                    max_active_count: max_active_count.clone(),
                    delay: Duration::from_millis(10),
                }))
                .expect("handler registers");
            subscriptions
                .register_subscription(
                    handlers.as_ref(),
                    HookSubscription::new(
                        subscription_id(format!("sub.{id}").as_str()),
                        hook_id(id),
                        HookPhase::TurnPrePromptCompile,
                    )
                    .with_execution_policy(crate::HookExecutionPolicy {
                        await_policy: HookAwaitPolicy::Blocking,
                        timeout_ms: None,
                        max_parallelism: Some(1),
                    }),
                )
                .expect("subscription registers");
        }
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("limited execution should not hang")
                .expect("phase execution succeeds");

        assert_eq!(response.runs.len(), 3);
        assert_eq!(max_active_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn zero_max_parallelism_is_invalid() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.invalid.parallelism",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        subscriptions
            .register_subscription(
                handlers.as_ref(),
                HookSubscription::new(
                    subscription_id("sub.invalid.parallelism"),
                    hook_id("test.invalid.parallelism"),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_execution_policy(crate::HookExecutionPolicy {
                    await_policy: HookAwaitPolicy::Blocking,
                    timeout_ms: None,
                    max_parallelism: Some(0),
                }),
            )
            .expect("subscription registers");
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request()))
            .expect_err("zero max_parallelism should be rejected");

        assert!(matches!(
            error,
            HookRuntimeError::InvalidExecutionPolicy { .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_contribution_order_is_deterministic_when_completion_order_differs() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.slow"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.slow", "slow"),
            }))
            .expect("handler registers");
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.fast"),
                delay: Duration::from_millis(1),
                contribution: contribution("section.fast", "fast"),
            }))
            .expect("handler registers");
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.slow",
            "test.slow",
            0,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.fast",
            "test.fast",
            10,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("phase should complete")
                .expect("phase execution succeeds");

        assert_eq!(
            contribution_section_ids(&response),
            vec![
                HookSectionId::new("section.slow").expect("valid section id"),
                HookSectionId::new("section.fast").expect("valid section id")
            ]
        );
        assert_eq!(response.runs[0].contribution_hashes.len(), 1);
        assert_eq!(response.runs[1].contribution_hashes.len(), 1);
        assert_ne!(
            response.runs[0].contribution_hashes,
            response.runs[1].contribution_hashes
        );
    }

    #[test]
    fn best_effort_failure_in_parallel_batch_records_diagnostic_and_continues() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.fail",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error("hook.failed", "failed"))],
        );
        register_handler(
            &handlers,
            "test.ok",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.ok", "ok")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.fail",
            "test.fail",
            0,
            HookFailurePolicy::BestEffort,
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.ok",
            "test.ok",
            1,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.runs[0].status, HookRunStatus::Failed);
        assert_eq!(response.runs[1].status, HookRunStatus::Succeeded);
        assert_eq!(response.contributions.len(), 1);
    }

    #[test]
    fn non_best_effort_failure_stops_later_batches() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.fail",
            calls.clone(),
            vec![Err(hook_error("hook.failed", "failed"))],
        );
        register_handler(
            &handlers,
            "test.later",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.fail",
            "test.fail",
            0,
            HookFailurePolicy::Required,
        );
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.later",
            "test.later",
            1,
            HookSubscriptionDependencies::new([subscription_id("sub.fail")], []),
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request())).expect_err("runtime fails");

        assert!(matches!(error, HookRuntimeError::HookFailed { .. }));
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.fail")]
        );
    }

    #[test]
    fn required_failure_fails_phase() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.required",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error("hook.failed", "failed"))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.required",
            "test.required",
            0,
            HookFailurePolicy::Required,
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request())).expect_err("runtime fails");

        assert!(matches!(
            error,
            HookRuntimeError::HookFailed { subscription_id, hook_id, phase, .. }
                if subscription_id == self::subscription_id("sub.required")
                    && hook_id == self::hook_id("test.required")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
    }

    #[test]
    fn fallback_failure_returns_fallback_contribution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.fallback",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error("hook.failed", "failed"))],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.fallback",
            "test.fallback",
            0,
            HookAwaitPolicy::Deadline,
            Some(1_000),
            HookFailurePolicy::Fallback,
            [contribution("section.fallback", "fallback")],
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("fallback succeeds");

        assert_eq!(
            contribution_section_ids(&response),
            vec![HookSectionId::new("section.fallback").expect("valid section id")]
        );
        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.runs[0].status, HookRunStatus::Failed);
        assert_eq!(response.runs[0].contribution_count, 1);
        assert_eq!(response.runs[0].contribution_hashes.len(), 1);
        assert_eq!(response.runs[0].attempts.len(), 1);
        assert_eq!(response.runs[0].attempts[0].status, HookRunStatus::Failed);
        assert!(response.runs[0].attempts[0].contribution_hashes.is_empty());
    }

    #[test]
    fn fallback_without_contribution_is_rejected_before_execution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.ok",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_handler(
            &handlers,
            "test.fallback",
            calls.clone(),
            vec![Err(hook_error("hook.failed", "failed"))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.ok",
            "test.ok",
            0,
            HookFailurePolicy::BestEffort,
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.fallback",
            "test.fallback",
            1,
            HookAwaitPolicy::Deadline,
            Some(1_000),
            HookFailurePolicy::Fallback,
            [],
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request()))
            .expect_err("missing fallback should fail before execution");

        assert!(matches!(
            error,
            HookRuntimeError::MissingFallbackContribution { subscription_id, hook_id, phase }
                if subscription_id == self::subscription_id("sub.fallback")
                    && hook_id == self::hook_id("test.fallback")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_policy_waits_for_completion() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.blocking"),
                delay: Duration::from_millis(25),
                contribution: contribution("section.blocking", "blocking"),
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.blocking",
            "test.blocking",
            0,
            HookAwaitPolicy::Blocking,
            Some(1),
            HookFailurePolicy::BestEffort,
            [],
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("blocking hook should complete")
                .expect("phase execution succeeds");

        assert_eq!(
            contribution_section_ids(&response),
            vec![HookSectionId::new("section.blocking").expect("valid section id")]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_timeout_records_diagnostic() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.timeout"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.late", "late"),
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.timeout",
            "test.timeout",
            0,
            HookAwaitPolicy::Deadline,
            Some(5),
            HookFailurePolicy::BestEffort,
            [],
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("deadline hook should return")
                .expect("phase execution succeeds");

        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.diagnostics[0].code.as_str(), "hook.timeout");
        assert_eq!(response.runs[0].status, HookRunStatus::TimedOut);
        assert_eq!(response.runs[0].attempts.len(), 1);
        assert_eq!(response.runs[0].attempts[0].status, HookRunStatus::TimedOut);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_timeout_does_not_include_late_contribution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.late"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.late", "late"),
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.late",
            "test.late",
            0,
            HookAwaitPolicy::Deadline,
            Some(5),
            HookFailurePolicy::BestEffort,
            [],
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("deadline hook should return")
                .expect("phase execution succeeds");

        assert!(response.contributions.is_empty());
        assert_eq!(response.runs[0].contribution_count, 0);
        assert!(response.runs[0].contribution_hashes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_timeout_with_required_fails_phase() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let later_calls = Arc::new(Mutex::new(Vec::new()));
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.timeout"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.timeout", "timeout"),
            }))
            .expect("handler registers");
        register_handler(
            &handlers,
            "test.later",
            later_calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.timeout",
            "test.timeout",
            0,
            HookAwaitPolicy::Deadline,
            Some(5),
            HookFailurePolicy::Required,
            [],
        );
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.later",
            "test.later",
            1,
            HookSubscriptionDependencies::new([subscription_id("sub.timeout")], []),
        );
        let runtime = runtime(handlers, subscriptions);

        let error =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("deadline hook should return")
                .expect_err("required timeout should fail phase");

        assert!(matches!(
            error,
            HookRuntimeError::HookTimedOut { subscription_id, hook_id, phase, timeout_ms }
                if subscription_id == self::subscription_id("sub.timeout")
                    && hook_id == self::hook_id("test.timeout")
                    && phase == HookPhase::TurnPrePromptCompile
                    && timeout_ms == 5
        ));
        assert!(later_calls.lock().expect("calls lock").is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_timeout_with_fallback_returns_fallback_contribution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.timeout"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.timeout", "timeout"),
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.timeout",
            "test.timeout",
            0,
            HookAwaitPolicy::Deadline,
            Some(5),
            HookFailurePolicy::Fallback,
            [contribution("section.fallback", "fallback")],
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("deadline hook should return")
                .expect("fallback timeout should continue");

        assert_eq!(
            contribution_section_ids(&response),
            vec![HookSectionId::new("section.fallback").expect("valid section id")]
        );
        assert_eq!(response.runs[0].status, HookRunStatus::TimedOut);
        assert_eq!(response.runs[0].contribution_count, 1);
        assert_eq!(response.runs[0].contribution_hashes.len(), 1);
        assert_eq!(response.runs[0].attempts.len(), 1);
        assert_eq!(response.runs[0].attempts[0].status, HookRunStatus::TimedOut);
        assert!(response.runs[0].attempts[0].contribution_hashes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_timeout_uses_runtime_default_when_subscription_timeout_missing() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.default.timeout"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.default.timeout", "timeout"),
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.default.timeout",
            "test.default.timeout",
            0,
            HookAwaitPolicy::Deadline,
            None,
            HookFailurePolicy::BestEffort,
            [],
        );
        let runtime = HookRuntime::with_options(
            handlers,
            subscriptions,
            HookRuntimeOptions {
                default_deadline_timeout_ms: 5,
                error_preview_max_chars: 512,
            },
        );

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("deadline hook should return")
                .expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::TimedOut);
        assert_eq!(
            response.runs[0]
                .error
                .as_ref()
                .expect("run error")
                .message
                .as_str(),
            "diagnostic redacted"
        );
        assert_eq!(response.runs[0].attempts.len(), 1);
        assert_eq!(response.runs[0].attempts[0].status, HookRunStatus::TimedOut);
    }

    #[test]
    fn skip_policy_does_not_execute_handler() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.skip",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.skip",
            "test.skip",
            0,
            HookFailurePolicy::Skip,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert!(calls.lock().expect("calls lock").is_empty());
        assert_eq!(response.runs[0].status, HookRunStatus::Skipped);
        assert_eq!(response.runs[0].attempt_count, 0);
        assert!(response.runs[0].attempts.is_empty());
    }

    #[test]
    fn fail_closed_failure_returns_typed_error() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.fail.closed",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error("hook.failed", "failed"))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.fail.closed",
            "test.fail.closed",
            0,
            HookFailurePolicy::FailClosed,
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request()))
            .expect_err("fail closed should fail phase");

        assert!(matches!(
            error,
            HookRuntimeError::HookFailedClosed { subscription_id, hook_id, phase, .. }
                if subscription_id == self::subscription_id("sub.fail.closed")
                    && hook_id == self::hook_id("test.fail.closed")
                    && phase == HookPhase::TurnPrePromptCompile
        ));
    }

    #[test]
    fn background_policy_returns_without_inline_execution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.background",
            calls.clone(),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.background", "background")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.background",
            "test.background",
            0,
            HookAwaitPolicy::Background,
            Some(1),
            HookFailurePolicy::BestEffort,
            [],
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert!(calls.lock().expect("calls lock").is_empty());
        assert!(response.contributions.is_empty());
        assert_eq!(response.runs[0].status, HookRunStatus::Queued);
        assert!(response.runs[0].attempts.is_empty());
        assert_eq!(runtime.queued_background_len().expect("queue length"), 1);
    }

    #[test]
    fn fire_and_record_returns_no_contribution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.fire",
            calls.clone(),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.fire", "fire")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.fire",
            "test.fire",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1),
            HookFailurePolicy::BestEffort,
            [],
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert!(calls.lock().expect("calls lock").is_empty());
        assert!(response.contributions.is_empty());
        assert_eq!(response.runs[0].status, HookRunStatus::Queued);
        assert!(response.runs[0].attempts.is_empty());
        assert_eq!(runtime.queued_background_len().expect("queue length"), 1);
    }

    #[tokio::test]
    async fn phase_20_fire_and_record_executes_through_runtime_drain() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.phase20.fire",
            calls.clone(),
            vec![Ok(HookHandlerResponse {
                contributions: vec![HookContribution::Noop],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.fire",
            "test.phase20.fire",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let runtime = runtime(handlers, subscriptions);

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");

        assert!(response.contributions.is_empty());
        assert!(calls.lock().expect("calls lock").is_empty());
        assert_eq!(response.runs[0].status, HookRunStatus::Queued);
        assert_eq!(runtime.queued_background_len().expect("queue length"), 1);

        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");

        assert_eq!(drain.executed_count, 1);
        assert_eq!(drain.succeeded_count, 1);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.phase20.fire")]
        );
        assert_eq!(runtime.queued_background_len().expect("queue length"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn phase_20_caller_receives_queued_before_slow_handler_completion() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let active_count = Arc::new(AtomicUsize::new(0));
        let max_active_count = Arc::new(AtomicUsize::new(0));
        handlers
            .register_handler(Arc::new(ConcurrencyTrackingHookHandler {
                id: hook_id("test.phase20.slow"),
                active_count: active_count.clone(),
                max_active_count: max_active_count.clone(),
                delay: Duration::from_millis(25),
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.slow",
            "test.phase20.slow",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let runtime = runtime(handlers, subscriptions);

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Queued);
        assert_eq!(max_active_count.load(Ordering::SeqCst), 0);
        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");
        assert_eq!(drain.succeeded_count, 1);
        assert_eq!(max_active_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn phase_20_fire_and_record_persists_success_lifecycle() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase20.persist.success",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![HookContribution::Noop],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.persist.success",
            "test.phase20.persist.success",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");
        assert_eq!(response.runs[0].status, HookRunStatus::Queued);

        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");

        assert_eq!(drain.succeeded_count, 1);
        let events = store.events();
        assert_eq!(
            events
                .iter()
                .map(|event| match event {
                    StoreEvent::CreateRun { status, .. }
                    | StoreEvent::AppendAttempt { status, .. }
                    | StoreEvent::CompleteAttempt { status, .. }
                    | StoreEvent::CompleteRun { status, .. } => Some(*status),
                    StoreEvent::MarkRunRunning
                    | StoreEvent::AppendAudit { .. }
                    | StoreEvent::ScheduleRetry { .. }
                    | StoreEvent::MarkUnrecoverable { .. } => None,
                })
                .collect::<Vec<_>>(),
            vec![
                Some(HookRunStatus::Queued),
                None,
                Some(HookRunStatus::Running),
                Some(HookRunStatus::Succeeded),
                Some(HookRunStatus::Succeeded),
            ]
        );
    }

    #[tokio::test]
    async fn phase_20_background_failure_persists_failed_lifecycle() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase20.failure",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error("hook.failed", "failure"))],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.failure",
            "test.phase20.failure",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");
        assert_eq!(response.runs[0].status, HookRunStatus::Queued);

        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");

        assert_eq!(drain.failed_count, 1);
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::CompleteAttempt {
                status: HookRunStatus::Failed,
                ..
            }
        )));
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::CompleteRun {
                status: HookRunStatus::Failed,
                ..
            }
        )));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn phase_20_background_timeout_persists_timed_out_lifecycle() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.phase20.timeout"),
                delay: Duration::from_millis(50),
                contribution: HookContribution::Noop,
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.timeout",
            "test.phase20.timeout",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");
        assert_eq!(response.runs[0].status, HookRunStatus::Queued);

        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");

        assert_eq!(drain.timed_out_count, 1);
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::CompleteRun {
                status: HookRunStatus::TimedOut,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn phase_20_background_prompt_contributions_do_not_mutate_returned_phase_output() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase20.forbidden",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.phase20.forbidden", "forbidden")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.forbidden",
            "test.phase20.forbidden",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let runtime = runtime(handlers, subscriptions);

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");
        assert!(response.contributions.is_empty());

        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");

        assert_eq!(drain.failed_count, 1);
        assert_eq!(runtime.queued_background_len().expect("queue length"), 0);
    }

    #[tokio::test]
    async fn phase_20_all_prompt_mutating_background_contributions_are_rejected() {
        let forbidden = vec![
            policy_contribution("phase20.policy"),
            prompt_context_contribution("phase20.prompt_context"),
            contribution("phase20.prompt_section", "forbidden"),
            tool_bundle_contribution("phase20.tool_bundle"),
            prompt_manifest_diagnostic_contribution("phase20.manifest"),
        ];

        for (index, forbidden_contribution) in forbidden.into_iter().enumerate() {
            let handlers = Arc::new(HookRegistry::new());
            let subscriptions = Arc::new(HookSubscriptionRegistry::new());
            let hook_id_value = format!("test.phase20.forbidden.{index}");
            let subscription_id_value = format!("sub.phase20.forbidden.{index}");
            register_handler(
                &handlers,
                hook_id_value.as_str(),
                Arc::new(Mutex::new(Vec::new())),
                vec![Ok(HookHandlerResponse {
                    contributions: vec![forbidden_contribution],
                    diagnostics: Vec::new(),
                    metadata: HookMetadata::default(),
                })],
            );
            register_subscription_with_policy(
                &handlers,
                &subscriptions,
                subscription_id_value.as_str(),
                hook_id_value.as_str(),
                0,
                HookAwaitPolicy::FireAndRecord,
                Some(1_000),
                HookFailurePolicy::BestEffort,
                Vec::new(),
            );
            let runtime = runtime(handlers, subscriptions);

            let response = runtime
                .run_phase(phase_request())
                .await
                .expect("phase execution succeeds");
            assert!(response.contributions.is_empty());

            let drain = runtime
                .drain_queued_background()
                .await
                .expect("background drain succeeds");

            assert_eq!(drain.failed_count, 1, "index {index}");
        }
    }

    #[tokio::test]
    async fn phase_20_background_audit_contribution_persists() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let audit = audit_contribution("test.phase20.audit");
        register_handler(
            &handlers,
            "test.phase20.audit",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![audit],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.audit",
            "test.phase20.audit",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");
        assert!(response.contributions.is_empty());
        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");

        assert_eq!(drain.succeeded_count, 1);
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::AppendAudit { event_kinds, .. }
                if event_kinds == &vec!["test.phase20.audit".to_owned()]
        )));
    }

    #[test]
    fn phase_20_inline_dependency_on_background_hook_is_rejected() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase20.background.dep",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_handler(
            &handlers,
            "test.phase20.inline.dep",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.background.dep",
            "test.phase20.background.dep",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.phase20.inline.dep",
            "test.phase20.inline.dep",
            1,
            HookSubscriptionDependencies::new([subscription_id("sub.phase20.background.dep")], []),
        );
        let runtime = runtime(handlers, subscriptions);

        let error = block_on_ready(runtime.run_phase(phase_request()))
            .expect_err("inline dependency on background should be rejected");

        assert!(matches!(
            error,
            HookRuntimeError::InvalidExecutionPolicy {
                subscription_id: invalid_subscription_id,
                ..
            } if invalid_subscription_id == subscription_id("sub.phase20.inline.dep")
        ));
    }

    #[tokio::test]
    async fn phase_20_background_after_inline_dependency_is_allowed() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.phase20.inline.first",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_handler(
            &handlers,
            "test.phase20.background.after",
            calls.clone(),
            vec![
                Ok(HookHandlerResponse {
                    contributions: vec![HookContribution::Noop],
                    diagnostics: Vec::new(),
                    metadata: HookMetadata::default(),
                }),
                Ok(HookHandlerResponse {
                    contributions: vec![HookContribution::Noop],
                    diagnostics: Vec::new(),
                    metadata: HookMetadata::default(),
                }),
            ],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.phase20.inline.first",
            "test.phase20.inline.first",
            0,
            HookFailurePolicy::BestEffort,
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.background.after",
            "test.phase20.background.after",
            1,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        subscriptions
            .register_subscription(
                handlers.as_ref(),
                HookSubscription::new(
                    subscription_id("sub.phase20.background.after.dep"),
                    hook_id("test.phase20.background.after"),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_priority(2)
                .with_execution_policy(crate::HookExecutionPolicy {
                    await_policy: HookAwaitPolicy::FireAndRecord,
                    timeout_ms: Some(1_000),
                    max_parallelism: None,
                })
                .with_dependencies(HookSubscriptionDependencies::new(
                    [subscription_id("sub.phase20.inline.first")],
                    [],
                )),
            )
            .expect("subscription registers");
        let runtime = runtime(handlers, subscriptions);

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);
        assert_eq!(response.runs[1].status, HookRunStatus::Queued);
        assert_eq!(response.runs[2].status, HookRunStatus::Queued);
        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");
        assert_eq!(drain.executed_count, 2);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![
                hook_id("test.phase20.inline.first"),
                hook_id("test.phase20.background.after"),
                hook_id("test.phase20.background.after"),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn phase_20_background_drain_respects_max_parallelism_upper_bound() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let active_count = Arc::new(AtomicUsize::new(0));
        let max_active_count = Arc::new(AtomicUsize::new(0));
        handlers
            .register_handler(Arc::new(ConcurrencyTrackingHookHandler {
                id: hook_id("test.phase20.max.parallel"),
                active_count: active_count.clone(),
                max_active_count: max_active_count.clone(),
                delay: Duration::from_millis(10),
            }))
            .expect("handler registers");
        subscriptions
            .register_subscription(
                handlers.as_ref(),
                HookSubscription::new(
                    subscription_id("sub.phase20.max.parallel"),
                    hook_id("test.phase20.max.parallel"),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_execution_policy(crate::HookExecutionPolicy {
                    await_policy: HookAwaitPolicy::FireAndRecord,
                    timeout_ms: Some(1_000),
                    max_parallelism: Some(1),
                }),
            )
            .expect("subscription registers");
        let runtime = runtime(handlers, subscriptions);

        runtime
            .run_phase(phase_request())
            .await
            .expect("first phase execution succeeds");
        runtime
            .run_phase(phase_request())
            .await
            .expect("second phase execution succeeds");
        assert_eq!(runtime.queued_background_len().expect("queue length"), 2);

        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds");

        assert_eq!(drain.executed_count, 2);
        assert_eq!(max_active_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn phase_20_empty_background_drain_is_idempotent() {
        let runtime = runtime(
            Arc::new(HookRegistry::new()),
            Arc::new(HookSubscriptionRegistry::new()),
        );

        let first = runtime
            .drain_queued_background()
            .await
            .expect("empty drain succeeds");
        let second = runtime
            .drain_queued_background()
            .await
            .expect("empty drain succeeds again");

        assert_eq!(first.executed_count, 0);
        assert_eq!(second.executed_count, 0);
        assert_eq!(runtime.queued_background_len().expect("queue length"), 0);
    }

    #[tokio::test]
    async fn phase_20_store_failure_does_not_prevent_best_effort_background_execution() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.phase20.store.failure",
            calls.clone(),
            vec![Ok(HookHandlerResponse {
                contributions: vec![HookContribution::Noop],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase20.store.failure",
            "test.phase20.store.failure",
            0,
            HookAwaitPolicy::FireAndRecord,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore {
            events: Mutex::new(Vec::new()),
            recoverable_runs: Mutex::new(Vec::new()),
            fail_all: true,
            append_attempt_conflicts_remaining: AtomicUsize::new(0),
        });
        let runtime = runtime_with_recording_store(handlers, subscriptions, store);

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds despite store failure");
        assert_eq!(response.runs[0].status, HookRunStatus::Queued);
        let drain = runtime
            .drain_queued_background()
            .await
            .expect("background drain succeeds despite store failure");

        assert_eq!(drain.succeeded_count, 1);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![hook_id("test.phase20.store.failure")]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completion_order_still_does_not_affect_aggregation_with_timeouts() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.timeout"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.timeout", "timeout"),
            }))
            .expect("handler registers");
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.fast"),
                delay: Duration::from_millis(1),
                contribution: contribution("section.fast", "fast"),
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.timeout",
            "test.timeout",
            0,
            HookAwaitPolicy::Deadline,
            Some(5),
            HookFailurePolicy::BestEffort,
            [],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.fast",
            "test.fast",
            10,
            HookAwaitPolicy::Deadline,
            Some(1_000),
            HookFailurePolicy::BestEffort,
            [],
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("phase should complete")
                .expect("phase execution succeeds");

        assert_eq!(
            response
                .runs
                .iter()
                .map(|run| run.status)
                .collect::<Vec<_>>(),
            vec![HookRunStatus::TimedOut, HookRunStatus::Succeeded]
        );
        assert_eq!(
            contribution_section_ids(&response),
            vec![HookSectionId::new("section.fast").expect("valid section id")]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dependencies_still_respected_with_policy_outcomes() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.timeout"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.timeout", "timeout"),
            }))
            .expect("handler registers");
        register_handler(
            &handlers,
            "test.after",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.after", "after")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.timeout",
            "test.timeout",
            0,
            HookAwaitPolicy::Deadline,
            Some(5),
            HookFailurePolicy::BestEffort,
            [],
        );
        register_subscription_with_dependencies(
            &handlers,
            &subscriptions,
            "sub.after",
            "test.after",
            1,
            HookSubscriptionDependencies::new([subscription_id("sub.timeout")], []),
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            tokio::time::timeout(Duration::from_secs(1), runtime.run_phase(phase_request()))
                .await
                .expect("phase should complete")
                .expect("phase execution succeeds");

        assert_eq!(
            response
                .runs
                .iter()
                .map(|run| run.subscription_id.clone())
                .collect::<Vec<_>>(),
            vec![subscription_id("sub.timeout"), subscription_id("sub.after")]
        );
        assert_eq!(
            response
                .runs
                .iter()
                .map(|run| run.status)
                .collect::<Vec<_>>(),
            vec![HookRunStatus::TimedOut, HookRunStatus::Succeeded]
        );
    }

    #[test]
    fn runtime_generated_error_previews_are_bounded() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.long.error",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error(
                "hook.failed",
                "this diagnostic message is intentionally long",
            )
            .with_safe_for_user(true))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.long.error",
            "test.long.error",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = HookRuntime::with_options(
            handlers,
            subscriptions,
            HookRuntimeOptions {
                default_deadline_timeout_ms: 1_000,
                error_preview_max_chars: 12,
            },
        );

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.diagnostics[0].message.as_str(), "this diag...");
        assert_eq!(
            response.runs[0]
                .error
                .as_ref()
                .expect("run error summary")
                .message
                .as_str(),
            "this diag..."
        );
    }

    #[test]
    fn diagnostics_do_not_include_raw_sensitive_payload_in_summary() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let sensitive_payload = "password=super-secret-token";
        register_handler(
            &handlers,
            "test.sensitive",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: Vec::new(),
                diagnostics: vec![diagnostic("hook.sensitive", sensitive_payload)],
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.sensitive",
            "test.sensitive",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            response.diagnostics[0].message.as_str(),
            "diagnostic redacted"
        );
        assert!(
            !serde_json::to_string(&response.runs[0])
                .expect("run summary serializes")
                .contains(sensitive_payload)
        );
        assert_eq!(
            response.runs[0].diagnostic_previews[0].message.as_str(),
            "diagnostic redacted"
        );
        assert_eq!(
            response.runs[0].attempts[0].diagnostic_previews[0]
                .message
                .as_str(),
            "diagnostic redacted"
        );
    }

    #[test]
    fn failed_hook_returns_safe_error_preview() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let sensitive_payload = "password=super-secret-token";
        register_handler(
            &handlers,
            "test.unsafe.error",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error("hook.failed", sensitive_payload))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.unsafe.error",
            "test.unsafe.error",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");
        let run = &response.runs[0];

        assert_eq!(run.status, HookRunStatus::Failed);
        assert_eq!(
            run.error.as_ref().expect("run error").message.as_str(),
            "diagnostic redacted"
        );
        assert_eq!(
            run.diagnostic_previews[0].message.as_str(),
            "diagnostic redacted"
        );
        assert_eq!(
            run.attempts[0]
                .error
                .as_ref()
                .expect("attempt error")
                .message
                .as_str(),
            "diagnostic redacted"
        );
        assert!(
            !serde_json::to_string(run)
                .expect("run summary serializes")
                .contains(sensitive_payload)
        );
    }

    #[test]
    fn contribution_hash_is_stable_for_same_contribution() {
        let first = contribution("section.hash", "hash me");
        let second = contribution("section.hash", "hash me");
        let different = contribution("section.hash", "different");

        let first_hash =
            HookContributionHash::from_contribution(&first).expect("hash should be produced");
        let second_hash =
            HookContributionHash::from_contribution(&second).expect("hash should be produced");
        let different_hash =
            HookContributionHash::from_contribution(&different).expect("hash should be produced");

        assert_eq!(first_hash, second_hash);
        assert_ne!(first_hash, different_hash);
        assert!(first_hash.as_str().starts_with("sha256:"));
    }

    #[test]
    fn phase_11_contribution_hash_helper_matches_runtime_summary() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let contribution = contribution("section.hash_summary", "hash summary");
        let expected_hash = HookContributionHash::from_contribution(&contribution)
            .expect("hash should be produced");
        register_handler(
            &handlers,
            "test.hash.summary",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.hash.summary",
            "test.hash.summary",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs[0].contribution_hashes, vec![expected_hash]);
    }

    #[test]
    fn audit_contribution_is_appended_to_run_store() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let audit_contribution = HookContribution::Audit(crate::AuditContribution {
            event_kind: crate::HookAuditEventKind::new("test.audit.persisted")
                .expect("valid audit event kind"),
            details: HookValue::Text("audit details".to_owned()),
            safe_for_user: false,
        });
        let expected_hash = HookContributionHash::from_contribution(&audit_contribution)
            .expect("audit contribution hash should be produced");
        register_handler(
            &handlers,
            "test.audit.persisted",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![audit_contribution],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.audit.persisted",
            "test.audit.persisted",
            0,
            HookFailurePolicy::BestEffort,
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = HookRuntime::with_run_store(handlers, subscriptions, store.clone());

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);
        assert!(store.events().iter().any(|event| {
            matches!(
                event,
                StoreEvent::AppendAudit {
                    event_kinds,
                    contribution_hashes
                } if event_kinds == &vec!["test.audit.persisted".to_owned()]
                    && contribution_hashes == &vec![Some(expected_hash.clone())]
            )
        }));
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum StoreEvent {
        CreateRun {
            status: HookRunStatus,
            contribution_hashes: Vec<HookContributionHash>,
        },
        MarkRunRunning,
        AppendAttempt {
            status: HookRunStatus,
            attempt_number: u16,
            contribution_hashes: Vec<HookContributionHash>,
        },
        CompleteAttempt {
            status: HookRunStatus,
            contribution_hashes: Vec<HookContributionHash>,
        },
        CompleteRun {
            status: HookRunStatus,
            contribution_hashes: Vec<HookContributionHash>,
        },
        ScheduleRetry {
            queued_at_unix_ms: i64,
        },
        MarkUnrecoverable {
            status: HookRunStatus,
        },
        AppendAudit {
            event_kinds: Vec<String>,
            contribution_hashes: Vec<Option<HookContributionHash>>,
        },
    }

    #[derive(Default)]
    struct RecordingHookRunStore {
        events: Mutex<Vec<StoreEvent>>,
        recoverable_runs: Mutex<Vec<crate::HookRecoverableRunRecord>>,
        fail_all: bool,
        append_attempt_conflicts_remaining: AtomicUsize,
    }

    impl RecordingHookRunStore {
        fn events(&self) -> Vec<StoreEvent> {
            self.events.lock().expect("events lock").clone()
        }

        fn with_append_attempt_conflicts(count: usize) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                recoverable_runs: Mutex::new(Vec::new()),
                fail_all: false,
                append_attempt_conflicts_remaining: AtomicUsize::new(count),
            }
        }

        fn with_recoverable_runs(records: Vec<crate::HookRecoverableRunRecord>) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                recoverable_runs: Mutex::new(records),
                fail_all: false,
                append_attempt_conflicts_remaining: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl HookRunStore for RecordingHookRunStore {
        async fn create_or_load_run(
            &self,
            run: NewHookRunStoreRecord,
        ) -> HookRunStoreResult<HookRunStoreRecord> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(StoreEvent::CreateRun {
                    status: run.status,
                    contribution_hashes: run.contribution_hashes.clone(),
                });
            Ok(HookRunStoreRecord {
                id: HookRunId::new("run.phase15").expect("valid run id"),
                idempotency_key: run.idempotency_key,
                subscription_id: run.subscription_id,
                hook_id: run.hook_id,
                phase: run.phase,
                status: run.status,
                scope: run.scope,
                context: run.context,
                attempt_count: 0,
                contribution_count: run.contribution_hashes.len(),
                diagnostic_count: run.diagnostic_previews.len(),
                contribution_hashes: run.contribution_hashes,
                diagnostic_previews: run.diagnostic_previews,
                error: run.error,
                queued_at_unix_ms: run.queued_at_unix_ms,
                started_at_unix_ms: run.started_at_unix_ms,
                completed_at_unix_ms: run.completed_at_unix_ms,
                deadline_at_unix_ms: run.deadline_at_unix_ms,
                resume_state: run.resume_state,
            })
        }

        async fn mark_run_running(
            &self,
            run_id: &HookRunId,
            started_at_unix_ms: i64,
        ) -> HookRunStoreResult<HookRunStoreRecord> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(StoreEvent::MarkRunRunning);
            Ok(HookRunStoreRecord {
                id: run_id.clone(),
                idempotency_key: HookRunIdempotencyKey::new("phase15.running").expect("valid key"),
                subscription_id: subscription_id("sub.persistence"),
                hook_id: hook_id("test.persistence"),
                phase: HookPhase::TurnPrePromptCompile,
                status: HookRunStatus::Running,
                scope: None,
                context: HookContext::default(),
                attempt_count: 0,
                contribution_count: 0,
                diagnostic_count: 0,
                contribution_hashes: Vec::new(),
                diagnostic_previews: Vec::new(),
                error: None,
                queued_at_unix_ms: None,
                started_at_unix_ms: Some(started_at_unix_ms),
                completed_at_unix_ms: None,
                deadline_at_unix_ms: None,
                resume_state: None,
            })
        }

        async fn complete_run(
            &self,
            run_id: &HookRunId,
            completion: HookRunStoreCompletion,
        ) -> HookRunStoreResult<HookRunStoreRecord> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(StoreEvent::CompleteRun {
                    status: completion.status,
                    contribution_hashes: completion.contribution_hashes.clone(),
                });
            Ok(HookRunStoreRecord {
                id: run_id.clone(),
                idempotency_key: HookRunIdempotencyKey::new("phase15.completed")
                    .expect("valid key"),
                subscription_id: subscription_id("sub.persistence"),
                hook_id: hook_id("test.persistence"),
                phase: HookPhase::TurnPrePromptCompile,
                status: completion.status,
                scope: None,
                context: HookContext::default(),
                attempt_count: 1,
                contribution_count: completion.contribution_hashes.len(),
                diagnostic_count: completion.diagnostic_previews.len(),
                contribution_hashes: completion.contribution_hashes,
                diagnostic_previews: completion.diagnostic_previews,
                error: completion.error,
                queued_at_unix_ms: None,
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(completion.completed_at_unix_ms),
                deadline_at_unix_ms: None,
                resume_state: None,
            })
        }

        async fn append_attempt(
            &self,
            attempt: NewHookRunAttemptStoreRecord,
        ) -> HookRunStoreResult<HookRunAttemptStoreRecord> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(StoreEvent::AppendAttempt {
                    status: attempt.status,
                    attempt_number: attempt.attempt_number,
                    contribution_hashes: attempt.contribution_hashes.clone(),
                });
            if self
                .append_attempt_conflicts_remaining
                .load(Ordering::SeqCst)
                > 0
            {
                self.append_attempt_conflicts_remaining
                    .fetch_sub(1, Ordering::SeqCst);
                return Err(crate::HookRunStoreError::conflict(
                    "attempt number already exists",
                ));
            }
            Ok(HookRunAttemptStoreRecord {
                id: HookRunAttemptId::new("attempt.phase15").expect("valid attempt id"),
                hook_run_id: attempt.hook_run_id,
                attempt_number: attempt.attempt_number,
                status: attempt.status,
                contribution_count: attempt.contribution_hashes.len(),
                diagnostic_count: attempt.diagnostic_previews.len(),
                contribution_hashes: attempt.contribution_hashes,
                diagnostic_previews: attempt.diagnostic_previews,
                error: attempt.error,
                started_at_unix_ms: attempt.started_at_unix_ms,
                completed_at_unix_ms: attempt.completed_at_unix_ms,
                duration_ms: attempt.duration_ms,
            })
        }

        async fn complete_attempt(
            &self,
            attempt_id: &HookRunAttemptId,
            completion: HookRunAttemptStoreCompletion,
        ) -> HookRunStoreResult<HookRunAttemptStoreRecord> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(StoreEvent::CompleteAttempt {
                    status: completion.status,
                    contribution_hashes: completion.contribution_hashes.clone(),
                });
            Ok(HookRunAttemptStoreRecord {
                id: attempt_id.clone(),
                hook_run_id: HookRunId::new("run.phase15").expect("valid run id"),
                attempt_number: 1,
                status: completion.status,
                contribution_count: completion.contribution_hashes.len(),
                diagnostic_count: completion.diagnostic_previews.len(),
                contribution_hashes: completion.contribution_hashes,
                diagnostic_previews: completion.diagnostic_previews,
                error: completion.error,
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(completion.completed_at_unix_ms),
                duration_ms: completion.duration_ms,
            })
        }

        async fn append_audit_events(
            &self,
            events: Vec<NewHookAuditEventStoreRecord>,
        ) -> HookRunStoreResult<Vec<HookAuditEventStoreRecord>> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(StoreEvent::AppendAudit {
                    event_kinds: events
                        .iter()
                        .map(|event| event.event_kind.as_str().to_owned())
                        .collect(),
                    contribution_hashes: events
                        .iter()
                        .map(|event| event.contribution_hash.clone())
                        .collect(),
                });
            Ok(events
                .into_iter()
                .enumerate()
                .map(|(index, event)| HookAuditEventStoreRecord {
                    id: format!("audit.{index}"),
                    hook_run_id: event.hook_run_id,
                    hook_run_attempt_id: event.hook_run_attempt_id,
                    subscription_id: event.subscription_id,
                    hook_id: event.hook_id,
                    phase: event.phase,
                    context: event.context,
                    event_kind: event.event_kind,
                    contribution_hash: event.contribution_hash,
                    details: event.details,
                    safe_for_user: event.safe_for_user,
                    created_at_unix_ms: event.created_at_unix_ms.unwrap_or(0),
                })
                .collect())
        }

        async fn list_recoverable_runs(
            &self,
            _scan: crate::HookRecoveryScan,
        ) -> HookRunStoreResult<Vec<crate::HookRecoverableRunRecord>> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            Ok(self
                .recoverable_runs
                .lock()
                .expect("recoverable runs lock")
                .clone())
        }

        async fn schedule_run_retry(
            &self,
            run_id: &HookRunId,
            schedule: crate::HookRetrySchedule,
        ) -> HookRunStoreResult<HookRunStoreRecord> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(StoreEvent::ScheduleRetry {
                    queued_at_unix_ms: schedule.queued_at_unix_ms,
                });
            Ok(HookRunStoreRecord {
                id: run_id.clone(),
                idempotency_key: HookRunIdempotencyKey::new("phase21.retry").expect("valid key"),
                subscription_id: subscription_id("sub.persistence"),
                hook_id: hook_id("test.persistence"),
                phase: HookPhase::TurnPrePromptCompile,
                status: HookRunStatus::Queued,
                scope: None,
                context: HookContext::default(),
                attempt_count: 1,
                contribution_count: 0,
                diagnostic_count: schedule.diagnostic_previews.len(),
                contribution_hashes: Vec::new(),
                diagnostic_previews: schedule.diagnostic_previews,
                error: None,
                queued_at_unix_ms: Some(schedule.queued_at_unix_ms),
                started_at_unix_ms: None,
                completed_at_unix_ms: None,
                deadline_at_unix_ms: schedule.deadline_at_unix_ms,
                resume_state: None,
            })
        }

        async fn mark_stale_run_timed_out(
            &self,
            run_id: &HookRunId,
            completion: HookRunStoreCompletion,
        ) -> HookRunStoreResult<HookRunStoreRecord> {
            self.mark_run_unrecoverable(run_id, completion).await
        }

        async fn mark_run_unrecoverable(
            &self,
            run_id: &HookRunId,
            completion: HookRunStoreCompletion,
        ) -> HookRunStoreResult<HookRunStoreRecord> {
            if self.fail_all {
                return Err(crate::HookRunStoreError::internal("store unavailable"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(StoreEvent::MarkUnrecoverable {
                    status: completion.status,
                });
            Ok(HookRunStoreRecord {
                id: run_id.clone(),
                idempotency_key: HookRunIdempotencyKey::new("phase21.unrecoverable")
                    .expect("valid key"),
                subscription_id: subscription_id("sub.persistence"),
                hook_id: hook_id("test.persistence"),
                phase: HookPhase::TurnPrePromptCompile,
                status: completion.status,
                scope: None,
                context: HookContext::default(),
                attempt_count: 1,
                contribution_count: completion.contribution_hashes.len(),
                diagnostic_count: completion.diagnostic_previews.len(),
                contribution_hashes: completion.contribution_hashes,
                diagnostic_previews: completion.diagnostic_previews,
                error: completion.error,
                queued_at_unix_ms: None,
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(completion.completed_at_unix_ms),
                deadline_at_unix_ms: None,
                resume_state: None,
            })
        }
    }

    fn runtime_with_recording_store(
        handlers: Arc<HookRegistry>,
        subscriptions: Arc<HookSubscriptionRegistry>,
        store: Arc<RecordingHookRunStore>,
    ) -> HookRuntime {
        HookRuntime::with_run_store(handlers, subscriptions, store)
    }

    fn phase_21_background_policy() -> HookExecutionPolicy {
        HookExecutionPolicy {
            await_policy: HookAwaitPolicy::Background,
            timeout_ms: Some(10_000),
            max_parallelism: None,
        }
    }

    fn phase_21_retry_policy(
        max_attempts: u16,
        idempotency_required: bool,
    ) -> crate::HookRetryPolicy {
        crate::HookRetryPolicy {
            max_attempts,
            backoff: HookRetryBackoff::Fixed,
            initial_delay_ms: Some(250),
            idempotency_required,
        }
    }

    fn phase_21_resume_state(retry_policy: crate::HookRetryPolicy) -> HookRunResumeState {
        HookRunResumeState::input_snapshot(
            phase_21_background_policy(),
            HookFailurePolicy::BestEffort,
            retry_policy,
            1,
            1,
            1,
            HookRunInputSnapshot::new(
                HookPhase::TurnPrePromptCompile,
                HookContext::default(),
                HookInput::empty(crate::HookInputKind::TurnPrePromptCompile),
                HookPolicySet::empty(),
                HookPromptContextSet::empty(),
            ),
        )
    }

    fn phase_21_recoverable_record(
        subscription_id: &str,
        hook_id: &str,
        status: HookRunStatus,
        attempt_count: u16,
        resume_state: HookRunResumeState,
        attempts: Vec<HookRunAttemptStoreRecord>,
    ) -> HookRecoverableRunRecord {
        HookRecoverableRunRecord {
            run: HookRunStoreRecord {
                id: HookRunId::new("run.phase21").expect("valid run id"),
                idempotency_key: HookRunIdempotencyKey::new("phase21.recovery")
                    .expect("valid idempotency key"),
                subscription_id: self::subscription_id(subscription_id),
                hook_id: self::hook_id(hook_id),
                phase: HookPhase::TurnPrePromptCompile,
                status,
                scope: None,
                context: HookContext::default(),
                attempt_count,
                contribution_count: 0,
                diagnostic_count: 0,
                contribution_hashes: Vec::new(),
                diagnostic_previews: Vec::new(),
                error: None,
                queued_at_unix_ms: Some(1_000),
                started_at_unix_ms: None,
                completed_at_unix_ms: None,
                deadline_at_unix_ms: None,
                resume_state: Some(resume_state.clone()),
            },
            resume_state: Some(resume_state),
            attempts,
        }
    }

    fn register_phase_21_background_subscription(
        handlers: &HookRegistry,
        subscriptions: &HookSubscriptionRegistry,
        subscription_id: &str,
        hook_id: &str,
        retry_policy: crate::HookRetryPolicy,
    ) {
        subscriptions
            .register_subscription(
                handlers,
                HookSubscription::new(
                    self::subscription_id(subscription_id),
                    self::hook_id(hook_id),
                    HookPhase::TurnPrePromptCompile,
                )
                .with_execution_policy(phase_21_background_policy())
                .with_failure_policy(HookFailurePolicy::BestEffort)
                .with_retry_policy(retry_policy),
            )
            .expect("subscription registers");
    }

    #[tokio::test]
    async fn phase_21_recover_background_runs_once_executes_due_queued_run() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.phase21.recover",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        let retry_policy = phase_21_retry_policy(1, true);
        register_phase_21_background_subscription(
            &handlers,
            &subscriptions,
            "sub.phase21.recover",
            "test.phase21.recover",
            retry_policy.clone(),
        );
        let record = phase_21_recoverable_record(
            "sub.phase21.recover",
            "test.phase21.recover",
            HookRunStatus::Queued,
            0,
            phase_21_resume_state(retry_policy),
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::with_recoverable_runs(vec![record]));
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let summary = runtime
            .recover_background_runs_once(HookRecoveryOptions {
                now_unix_ms: 2_000,
                batch_size: 10,
                max_concurrent: 2,
                stale_running_after_ms: 1_000,
                strict_debug: true,
            })
            .await
            .expect("recovery succeeds");

        assert_eq!(summary.scanned_count, 1);
        assert_eq!(summary.recovered_count, 1);
        assert_eq!(summary.executed_count, 1);
        assert_eq!(calls.lock().expect("calls lock").len(), 1);
        assert!(store.events().iter().any(|event| {
            matches!(
                event,
                StoreEvent::CompleteRun {
                    status: HookRunStatus::Succeeded,
                    ..
                }
            )
        }));
    }

    #[tokio::test]
    async fn phase_21_recovery_marks_missing_subscription_unrecoverable() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let retry_policy = phase_21_retry_policy(1, true);
        let record = phase_21_recoverable_record(
            "sub.phase21.missing_subscription",
            "test.phase21.missing_subscription",
            HookRunStatus::Queued,
            0,
            phase_21_resume_state(retry_policy),
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::with_recoverable_runs(vec![record]));
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let summary = runtime
            .recover_background_runs_once(HookRecoveryOptions::default())
            .await
            .expect("recovery handles unrecoverable run");

        assert_eq!(summary.unrecoverable_count, 1);
        assert!(store.events().iter().any(|event| {
            matches!(
                event,
                StoreEvent::MarkUnrecoverable {
                    status: HookRunStatus::Failed
                }
            )
        }));
    }

    #[tokio::test]
    async fn phase_21_retryable_failure_schedules_retry_with_idempotency_proof() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        handlers
            .register_handler(Arc::new(RecordingHookHandler {
                id: hook_id("test.phase21.retry"),
                phases: vec![HookPhase::TurnPrePromptCompile],
                calls: calls.clone(),
                responses: Mutex::new(VecDeque::from([Err(HookError::new(
                    HookDiagnosticCode::new("test.retryable").expect("valid code"),
                    HookDiagnosticMessage::new("retryable failure").expect("valid message"),
                )
                .with_retryable(true))])),
                capabilities: HookCapabilities::new([HookCapability::new(
                    "idempotent_side_effect",
                )
                .expect("valid capability")]),
            }))
            .expect("handler registers");
        let retry_policy = phase_21_retry_policy(2, true);
        register_phase_21_background_subscription(
            &handlers,
            &subscriptions,
            "sub.phase21.retry",
            "test.phase21.retry",
            retry_policy.clone(),
        );
        let record = phase_21_recoverable_record(
            "sub.phase21.retry",
            "test.phase21.retry",
            HookRunStatus::Queued,
            0,
            phase_21_resume_state(retry_policy),
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::with_recoverable_runs(vec![record]));
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let summary = runtime
            .recover_background_runs_once(HookRecoveryOptions {
                now_unix_ms: 2_000,
                batch_size: 10,
                max_concurrent: 1,
                stale_running_after_ms: 1_000,
                strict_debug: true,
            })
            .await
            .expect("recovery succeeds");

        assert_eq!(summary.retried_count, 1);
        assert!(store.events().iter().any(|event| {
            matches!(
                event,
                StoreEvent::ScheduleRetry {
                    queued_at_unix_ms
                } if *queued_at_unix_ms > 0
            )
        }));
        assert!(!store.events().iter().any(|event| {
            matches!(
                event,
                StoreEvent::CompleteRun {
                    status: HookRunStatus::Failed,
                    ..
                }
            )
        }));
    }

    #[tokio::test]
    async fn phase_21_retryable_failure_without_idempotency_proof_is_terminal() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.phase21.no_idempotency",
            calls,
            vec![Err(HookError::new(
                HookDiagnosticCode::new("test.retryable").expect("valid code"),
                HookDiagnosticMessage::new("retryable failure").expect("valid message"),
            )
            .with_retryable(true))],
        );
        let retry_policy = phase_21_retry_policy(2, true);
        register_phase_21_background_subscription(
            &handlers,
            &subscriptions,
            "sub.phase21.no_idempotency",
            "test.phase21.no_idempotency",
            retry_policy.clone(),
        );
        let record = phase_21_recoverable_record(
            "sub.phase21.no_idempotency",
            "test.phase21.no_idempotency",
            HookRunStatus::Queued,
            0,
            phase_21_resume_state(retry_policy),
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::with_recoverable_runs(vec![record]));
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let summary = runtime
            .recover_background_runs_once(HookRecoveryOptions::default())
            .await
            .expect("recovery succeeds");

        assert_eq!(summary.retried_count, 0);
        assert!(store.events().iter().any(|event| {
            matches!(
                event,
                StoreEvent::CompleteRun {
                    status: HookRunStatus::Failed,
                    ..
                }
            )
        }));
        assert!(
            !store
                .events()
                .iter()
                .any(|event| matches!(event, StoreEvent::ScheduleRetry { .. }))
        );
    }

    #[tokio::test]
    async fn phase_21_stale_running_recovery_marks_timed_out_without_retry_budget() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.phase21.stale",
            calls.clone(),
            vec![Ok(HookHandlerResponse::default())],
        );
        let retry_policy = phase_21_retry_policy(1, true);
        register_phase_21_background_subscription(
            &handlers,
            &subscriptions,
            "sub.phase21.stale",
            "test.phase21.stale",
            retry_policy.clone(),
        );
        let attempt = HookRunAttemptStoreRecord {
            id: HookRunAttemptId::new("attempt.phase21.stale").expect("valid attempt id"),
            hook_run_id: HookRunId::new("run.phase21").expect("valid run id"),
            attempt_number: 1,
            status: HookRunStatus::Running,
            contribution_count: 0,
            diagnostic_count: 0,
            contribution_hashes: Vec::new(),
            diagnostic_previews: Vec::new(),
            error: None,
            started_at_unix_ms: Some(1_000),
            completed_at_unix_ms: None,
            duration_ms: None,
        };
        let record = phase_21_recoverable_record(
            "sub.phase21.stale",
            "test.phase21.stale",
            HookRunStatus::Running,
            1,
            phase_21_resume_state(retry_policy),
            vec![attempt],
        );
        let store = Arc::new(RecordingHookRunStore::with_recoverable_runs(vec![record]));
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let summary = runtime
            .recover_background_runs_once(HookRecoveryOptions {
                now_unix_ms: 3_000,
                batch_size: 10,
                max_concurrent: 1,
                stale_running_after_ms: 1_000,
                strict_debug: true,
            })
            .await
            .expect("recovery succeeds");

        assert_eq!(summary.timed_out_count, 1);
        assert!(calls.lock().expect("calls lock").is_empty());
        assert!(store.events().iter().any(|event| {
            matches!(
                event,
                StoreEvent::MarkUnrecoverable {
                    status: HookRunStatus::TimedOut
                }
            )
        }));
    }

    #[tokio::test]
    async fn phase_21_recovered_forbidden_background_contribution_fails_terminal() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_handler(
            &handlers,
            "test.phase21.forbidden",
            calls,
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.phase21.forbidden", "forbidden")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        let retry_policy = phase_21_retry_policy(1, true);
        register_phase_21_background_subscription(
            &handlers,
            &subscriptions,
            "sub.phase21.forbidden",
            "test.phase21.forbidden",
            retry_policy.clone(),
        );
        let record = phase_21_recoverable_record(
            "sub.phase21.forbidden",
            "test.phase21.forbidden",
            HookRunStatus::Queued,
            0,
            phase_21_resume_state(retry_policy),
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::with_recoverable_runs(vec![record]));
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let summary = runtime
            .recover_background_runs_once(HookRecoveryOptions::default())
            .await
            .expect("recovery succeeds");

        assert_eq!(summary.executed_count, 1);
        assert!(store.events().iter().any(|event| {
            matches!(
                event,
                StoreEvent::CompleteRun {
                    status: HookRunStatus::Failed,
                    ..
                }
            )
        }));
    }

    #[test]
    fn phase_15_runtime_without_store_preserves_current_behavior() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase15.no_store",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.phase15.no_store", "context")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.phase15.no_store",
            "test.phase15.no_store",
            0,
            HookFailurePolicy::BestEffort,
        );
        let runtime = runtime(handlers, subscriptions);

        assert!(!runtime.has_run_store());
        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);
        assert_eq!(
            contribution_section_ids(&response),
            vec![HookSectionId::new("section.phase15.no_store").expect("valid section id")]
        );
    }

    #[test]
    fn phase_15_run_store_records_success_lifecycle_without_raw_output() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        let secret = "SECRET_PROMPT_SECTION_SHOULD_NOT_BE_STORED";
        let prompt_contribution = contribution("section.phase15.success", secret);
        let expected_hash = HookContributionHash::from_contribution(&prompt_contribution)
            .expect("hash should be produced");
        register_handler(
            &handlers,
            "test.phase15.success",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![prompt_contribution],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.phase15.success",
            "test.phase15.success",
            0,
            HookFailurePolicy::BestEffort,
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);
        let events = store.events();
        assert_eq!(events.len(), 5);
        assert_eq!(
            events[0],
            StoreEvent::CreateRun {
                status: HookRunStatus::Queued,
                contribution_hashes: Vec::new(),
            }
        );
        assert_eq!(events[1], StoreEvent::MarkRunRunning);
        assert_eq!(
            events[2],
            StoreEvent::AppendAttempt {
                status: HookRunStatus::Running,
                attempt_number: 1,
                contribution_hashes: Vec::new(),
            }
        );
        assert_eq!(
            events[3],
            StoreEvent::CompleteAttempt {
                status: HookRunStatus::Succeeded,
                contribution_hashes: vec![expected_hash.clone()],
            }
        );
        assert_eq!(
            events[4],
            StoreEvent::CompleteRun {
                status: HookRunStatus::Succeeded,
                contribution_hashes: vec![expected_hash],
            }
        );
        assert!(
            !format!("{events:?}").contains(secret),
            "store events must not carry raw prompt-critical content"
        );
    }

    #[test]
    fn phase_15_run_store_retries_attempt_conflict_once() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase15.conflict",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.phase15.conflict", "ok")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.phase15.conflict",
            "test.phase15.conflict",
            0,
            HookFailurePolicy::BestEffort,
        );
        let store = Arc::new(RecordingHookRunStore::with_append_attempt_conflicts(1));
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);
        let attempt_numbers = store
            .events()
            .into_iter()
            .filter_map(|event| match event {
                StoreEvent::AppendAttempt { attempt_number, .. } => Some(attempt_number),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(attempt_numbers, vec![1, 2]);
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::CompleteAttempt {
                status: HookRunStatus::Succeeded,
                ..
            }
        )));
    }

    #[test]
    fn phase_15_run_store_records_best_effort_failure_lifecycle() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase15.failure",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error("hook.failed", "failure"))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.phase15.failure",
            "test.phase15.failure",
            0,
            HookFailurePolicy::BestEffort,
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Failed);
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::CompleteRun {
                status: HookRunStatus::Failed,
                ..
            }
        )));
    }

    #[test]
    fn phase_15_run_store_records_required_failure_before_returning_error() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase15.required",
            Arc::new(Mutex::new(Vec::new())),
            vec![Err(hook_error("hook.required_failed", "required failure"))],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.phase15.required",
            "test.phase15.required",
            0,
            HookFailurePolicy::Required,
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let error = block_on_ready(runtime.run_phase(phase_request())).unwrap_err();

        assert!(matches!(error, HookRuntimeError::HookFailed { .. }));
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::CompleteRun {
                status: HookRunStatus::Failed,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn phase_15_run_store_records_deadline_timeout_lifecycle() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        handlers
            .register_handler(Arc::new(DelayedHookHandler {
                id: hook_id("test.phase15.timeout"),
                delay: Duration::from_millis(50),
                contribution: contribution("section.phase15.timeout", "late"),
            }))
            .expect("handler registers");
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase15.timeout",
            "test.phase15.timeout",
            0,
            HookAwaitPolicy::Deadline,
            Some(1),
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = HookRuntime::with_options_and_run_store(
            handlers,
            subscriptions,
            HookRuntimeOptions {
                default_deadline_timeout_ms: 1,
                ..HookRuntimeOptions::default()
            },
            store.clone(),
        );

        let response = runtime
            .run_phase(phase_request())
            .await
            .expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::TimedOut);
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::CompleteRun {
                status: HookRunStatus::TimedOut,
                ..
            }
        )));
    }

    #[test]
    fn phase_15_run_store_records_skip_and_background_without_attempt() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase15.skip",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.phase15.skip",
            "test.phase15.skip",
            0,
            HookFailurePolicy::Skip,
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Skipped);
        assert!(store.events().iter().any(|event| matches!(
            event,
            StoreEvent::CompleteRun {
                status: HookRunStatus::Skipped,
                ..
            }
        )));
        assert!(
            !store
                .events()
                .iter()
                .any(|event| matches!(event, StoreEvent::AppendAttempt { .. }))
        );

        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase15.background",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse::default())],
        );
        register_subscription_with_policy(
            &handlers,
            &subscriptions,
            "sub.phase15.background",
            "test.phase15.background",
            0,
            HookAwaitPolicy::Background,
            None,
            HookFailurePolicy::BestEffort,
            Vec::new(),
        );
        let store = Arc::new(RecordingHookRunStore::default());
        let runtime = runtime_with_recording_store(handlers, subscriptions, store.clone());

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(response.runs[0].status, HookRunStatus::Queued);
        assert_eq!(store.events().len(), 1);
        assert!(matches!(store.events()[0], StoreEvent::CreateRun { .. }));
    }

    #[test]
    fn phase_15_store_failure_does_not_change_hook_result() {
        let handlers = Arc::new(HookRegistry::new());
        let subscriptions = Arc::new(HookSubscriptionRegistry::new());
        register_handler(
            &handlers,
            "test.phase15.store_failure",
            Arc::new(Mutex::new(Vec::new())),
            vec![Ok(HookHandlerResponse {
                contributions: vec![contribution("section.phase15.store_failure", "context")],
                diagnostics: Vec::new(),
                metadata: HookMetadata::default(),
            })],
        );
        register_subscription(
            &handlers,
            &subscriptions,
            "sub.phase15.store_failure",
            "test.phase15.store_failure",
            0,
            HookFailurePolicy::BestEffort,
        );
        let store = Arc::new(RecordingHookRunStore {
            events: Mutex::new(Vec::new()),
            recoverable_runs: Mutex::new(Vec::new()),
            fail_all: true,
            append_attempt_conflicts_remaining: AtomicUsize::new(0),
        });
        let runtime = runtime_with_recording_store(handlers, subscriptions, store);

        let response =
            block_on_ready(runtime.run_phase(phase_request())).expect("phase execution succeeds");

        assert_eq!(
            contribution_section_ids(&response),
            vec![HookSectionId::new("section.phase15.store_failure").expect("valid section id")]
        );
        assert_eq!(response.runs[0].status, HookRunStatus::Succeeded);
    }
}
