use crate::{
    HookAwaitPolicy, HookContext, HookContribution, HookContributionHash, HookDiagnostic,
    HookDiagnosticCode, HookDiagnosticMessage, HookDiagnosticPreview,
    HookDiagnosticRedactionPolicy, HookDiagnosticSeverity, HookError, HookFailurePolicy,
    HookHandler, HookHandlerRequest, HookHandlerResponse, HookId, HookInput, HookMetadata,
    HookPhase, HookPolicySet, HookPromptContextSet, HookRegistry, HookRegistryError,
    HookSubscription, HookSubscriptionId, HookSubscriptionRegistry,
};
use futures_timer::Delay;
use futures_util::future::{Either, join_all, select};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

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
    queued_background: Arc<Mutex<VecDeque<HookQueuedBackgroundRun>>>,
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
        Self {
            handlers,
            subscriptions,
            options: options.normalized(),
            queued_background: Arc::new(Mutex::new(VecDeque::new())),
        }
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

    pub fn queued_background_len(&self) -> HookRuntimeResult<usize> {
        self.queued_background
            .lock()
            .map(|queue| queue.len())
            .map_err(|_| HookRegistryError::LockPoisoned("hook runtime background queue").into())
    }

    pub async fn run_phase(
        &self,
        request: HookPhaseRequest,
    ) -> HookRuntimeResult<HookPhaseResponse> {
        let subscriptions = self.subscriptions.subscriptions_for_phase(request.phase)?;
        let plan = build_execution_plan(request.phase, subscriptions, self.handlers.as_ref())?;
        let mut response = HookPhaseResponse::default();

        for batch in plan.batches {
            let mut results = join_all(batch.into_iter().map(|node| {
                execute_node(
                    node,
                    request.clone(),
                    self.queued_background.clone(),
                    self.options.clone(),
                )
            }))
            .await;
            results.sort_by_key(|result| result.order_index);

            for result in results {
                let NodeExecutionResult {
                    subscription,
                    outcome,
                    ..
                } = result;
                match outcome? {
                    HookNodeOutcome::Succeeded(handler_response) => {
                        append_success(
                            &mut response,
                            subscription.subscription_id,
                            subscription.hook_id,
                            request.phase,
                            handler_response,
                            &self.options,
                        );
                    }
                    HookNodeOutcome::Failed(error) => match subscription.failure_policy {
                        HookFailurePolicy::Required => {
                            return Err(HookRuntimeError::HookFailed {
                                subscription_id: subscription.subscription_id,
                                hook_id: subscription.hook_id,
                                phase: request.phase,
                                error,
                            });
                        }
                        HookFailurePolicy::Fallback => {
                            append_fallback_failure(
                                &mut response,
                                &subscription,
                                request.phase,
                                HookRunStatus::Failed,
                                error,
                                &self.options,
                            );
                        }
                        HookFailurePolicy::BestEffort => {
                            append_best_effort_failure(
                                &mut response,
                                subscription.subscription_id,
                                subscription.hook_id,
                                request.phase,
                                HookRunStatus::Failed,
                                error,
                                &self.options,
                            );
                        }
                        HookFailurePolicy::Skip => {
                            append_skipped(&mut response, subscription, request.phase);
                        }
                        HookFailurePolicy::FailClosed => {
                            return Err(HookRuntimeError::HookFailedClosed {
                                subscription_id: subscription.subscription_id,
                                hook_id: subscription.hook_id,
                                phase: request.phase,
                                error,
                            });
                        }
                    },
                    HookNodeOutcome::TimedOut { timeout_ms } => match subscription.failure_policy {
                        HookFailurePolicy::Required => {
                            return Err(HookRuntimeError::HookTimedOut {
                                subscription_id: subscription.subscription_id,
                                hook_id: subscription.hook_id,
                                phase: request.phase,
                                timeout_ms,
                            });
                        }
                        HookFailurePolicy::Fallback => {
                            append_fallback_failure(
                                &mut response,
                                &subscription,
                                request.phase,
                                HookRunStatus::TimedOut,
                                timeout_error(timeout_ms),
                                &self.options,
                            );
                        }
                        HookFailurePolicy::BestEffort => {
                            append_best_effort_failure(
                                &mut response,
                                subscription.subscription_id,
                                subscription.hook_id,
                                request.phase,
                                HookRunStatus::TimedOut,
                                timeout_error(timeout_ms),
                                &self.options,
                            );
                        }
                        HookFailurePolicy::Skip => {
                            append_skipped(&mut response, subscription, request.phase);
                        }
                        HookFailurePolicy::FailClosed => {
                            return Err(HookRuntimeError::HookFailedClosed {
                                subscription_id: subscription.subscription_id,
                                hook_id: subscription.hook_id,
                                phase: request.phase,
                                error: timeout_error(timeout_ms),
                            });
                        }
                    },
                    HookNodeOutcome::Skipped => {
                        append_skipped(&mut response, subscription, request.phase);
                    }
                    HookNodeOutcome::Queued => {
                        append_queued(
                            &mut response,
                            subscription.subscription_id,
                            subscription.hook_id,
                            request.phase,
                        );
                    }
                }
            }
        }

        Ok(response)
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

#[derive(Clone)]
enum HookQueuedBackgroundRun {
    Background,
    FireAndRecord,
}

impl HookQueuedBackgroundRun {
    fn from_await_policy(await_policy: HookAwaitPolicy) -> Option<Self> {
        match await_policy {
            HookAwaitPolicy::Background => Some(Self::Background),
            HookAwaitPolicy::FireAndRecord => Some(Self::FireAndRecord),
            HookAwaitPolicy::Blocking | HookAwaitPolicy::Deadline => None,
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
        }
        for dependency_id in &subscription.dependencies.before {
            if !subscription_indexes.contains_key(dependency_id) {
                return Err(HookRuntimeError::MissingDependency {
                    subscription_id: subscription.subscription_id.clone(),
                    dependency_id: dependency_id.clone(),
                    phase,
                });
            }
        }
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
    }
    Ok(())
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

async fn execute_node(
    node: HookExecutionNode,
    request: HookPhaseRequest,
    queued_background: Arc<Mutex<VecDeque<HookQueuedBackgroundRun>>>,
    options: HookRuntimeOptions,
) -> NodeExecutionResult {
    let outcome = execute_node_with_policy(&node, request, queued_background, &options).await;
    NodeExecutionResult {
        order_index: node.order_index,
        subscription: node.subscription,
        outcome,
    }
}

async fn execute_node_with_policy(
    node: &HookExecutionNode,
    request: HookPhaseRequest,
    queued_background: Arc<Mutex<VecDeque<HookQueuedBackgroundRun>>>,
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
                Ok(response) => HookNodeOutcome::Succeeded(response),
                Err(error) => HookNodeOutcome::Failed(error),
            })
        }
        HookAwaitPolicy::Deadline => {
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
                    Ok(HookNodeOutcome::Succeeded(response))
                }
                Either::Left((Err(error), _timeout_future)) => Ok(HookNodeOutcome::Failed(error)),
                Either::Right((_elapsed, _handler_future)) => {
                    Ok(HookNodeOutcome::TimedOut { timeout_ms })
                }
            }
        }
        HookAwaitPolicy::Background | HookAwaitPolicy::FireAndRecord => {
            let await_policy = node.subscription.execution_policy.await_policy;
            queued_background
                .lock()
                .map_err(|_| HookRegistryError::LockPoisoned("hook runtime background queue"))?
                .push_back(
                    HookQueuedBackgroundRun::from_await_policy(await_policy)
                        .expect("queued await policy is background-like"),
                );
            Ok(HookNodeOutcome::Queued)
        }
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
    let bytes = serde_json::to_vec(contribution).ok()?;
    let digest = Sha256::digest(bytes);
    HookContributionHash::new(format!("sha256:{}", hex::encode(digest))).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HookDiagnosticMessage, HookDomain, HookError, HookFilterKey, HookHandler, HookKind,
        HookPromptContent, HookPromptSectionTitle, HookResult, HookSectionId, HookSubscription,
        HookSubscriptionDependencies, HookValue,
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

    fn phase_request() -> HookPhaseRequest {
        HookPhaseRequest::new(
            HookPhase::TurnPrePromptCompile,
            HookContext::default(),
            HookInput {
                kind: crate::HookInputKind::TurnPrePromptCompile,
                payload: crate::HookValue::Null,
            },
        )
    }

    fn contribution(section_id: &str, content: &str) -> HookContribution {
        HookContribution::PromptSection(crate::PromptSectionContribution {
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

        let first_hash = hash_contribution(&first).expect("hash should be produced");
        let second_hash = hash_contribution(&second).expect("hash should be produced");
        let different_hash = hash_contribution(&different).expect("hash should be produced");

        assert_eq!(first_hash, second_hash);
        assert_ne!(first_hash, different_hash);
        assert!(first_hash.as_str().starts_with("sha256:"));
    }
}
