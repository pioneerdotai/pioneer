use super::{
    ActiveTurnRequest, AgentCommand, AgentEventHub, AgentMcpToolProvider, AgentStartError,
    ExecutionCheckpointContext, TaskToolProvider, TaskTurnContext, ToolLoopConfig,
    TurnExecutionControl, TurnExecutionUsageCounters, TurnFinalizationProvider, TurnTaskCompletion,
    TurnTaskFailure, TurnTaskSuccess, TurnToolProvider,
};
use crate::chat;
use crate::hooks::{
    AgentPostTurnHookDispatchPolicy, AgentToolBundleArtifactStore, AgentTurnHookContext,
    AgentTurnPostTurnHookDispatch, AgentTurnPostTurnSummary, DeferredTaskPostTurnDispatchStore,
    EffectiveTurnPolicySet, EffectiveTurnPromptContextSet,
};
use futures_util::FutureExt;
use pioneer_hooks::{HookRuntime, TurnPostTurnStatus};
use pioneer_protocol::{
    AgentDurableEvent, ExecutionWindowStatus, RecoveryAttemptContext, ThreadMode, TurnCapability,
    TurnExecutionSecuritySnapshot, TurnExecutionWindowBlockedNotification,
    TurnExecutionWindowContinuedNotification, TurnPermissionProfileSnapshot, UserInput,
};
use pioneer_provider::{ChatMessage, Provider, ProviderRegistry};
use pioneer_tools::{ExecutionWindowAdmissionDecision, decide_execution_window_admission};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};
use tracing::{debug, error};

const TURN_CANCEL_GRACE_MS: u64 = 750;

type TurnFlowFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionWindowTotalBudgetBlockKind {
    MaxWindows,
    MaxToolCalls,
    MaxWallClockMs,
    MaxProviderTokens,
    MaxConsecutiveNoProgressWindows,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutionWindowTotalBudgetBlock {
    kind: ExecutionWindowTotalBudgetBlockKind,
    total_windows: u32,
    total_tool_calls: u32,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExecutionWindowTotalBudgetDecision {
    Continue { next_window_index: u32 },
    Block(ExecutionWindowTotalBudgetBlock),
}

fn decide_execution_window_total_budget(
    usage: &TurnExecutionUsageCounters,
    total_budget: &pioneer_tools::ExecutionWindowTotalBudgetConfig,
) -> ExecutionWindowTotalBudgetDecision {
    let next_window_index = match decide_execution_window_admission(
        Some(usage.total_windows.max(1)),
        total_budget.max_windows_per_turn,
    ) {
        ExecutionWindowAdmissionDecision::Open { window_index } => window_index,
        ExecutionWindowAdmissionDecision::Block {
            total_windows,
            max_windows_per_turn,
        } => {
            return ExecutionWindowTotalBudgetDecision::Block(ExecutionWindowTotalBudgetBlock {
                kind: ExecutionWindowTotalBudgetBlockKind::MaxWindows,
                total_windows,
                total_tool_calls: saturating_u32_from_u64(usage.total_tool_calls),
                reason: format!(
                    "max execution windows per turn reached: limit={}, observed={total_windows}",
                    max_windows_per_turn
                ),
            });
        }
    };

    if let Some(max_tool_calls) = total_budget.max_tool_calls_per_turn.map(u64::from)
        && usage.total_tool_calls >= max_tool_calls
    {
        return ExecutionWindowTotalBudgetDecision::Block(ExecutionWindowTotalBudgetBlock {
            kind: ExecutionWindowTotalBudgetBlockKind::MaxToolCalls,
            total_windows: usage.total_windows,
            total_tool_calls: saturating_u32_from_u64(usage.total_tool_calls),
            reason: format!(
                "max_total_tool_calls_per_turn reached: limit={max_tool_calls}, observed={}",
                usage.total_tool_calls
            ),
        });
    }

    if let Some(max_wall_clock_ms) = total_budget.max_wall_clock_ms_per_turn
        && usage.total_wall_clock_ms >= max_wall_clock_ms
    {
        return ExecutionWindowTotalBudgetDecision::Block(ExecutionWindowTotalBudgetBlock {
            kind: ExecutionWindowTotalBudgetBlockKind::MaxWallClockMs,
            total_windows: usage.total_windows,
            total_tool_calls: saturating_u32_from_u64(usage.total_tool_calls),
            reason: format!(
                "max_total_wall_clock_ms_per_turn reached: limit={max_wall_clock_ms}, observed={}",
                usage.total_wall_clock_ms
            ),
        });
    }

    if let Some(max_provider_tokens) = total_budget.max_provider_tokens_per_turn
        && !usage.provider_token_usage_unknown
        && usage.total_provider_tokens >= max_provider_tokens
    {
        return ExecutionWindowTotalBudgetDecision::Block(ExecutionWindowTotalBudgetBlock {
            kind: ExecutionWindowTotalBudgetBlockKind::MaxProviderTokens,
            total_windows: usage.total_windows,
            total_tool_calls: saturating_u32_from_u64(usage.total_tool_calls),
            reason: format!(
                "max_total_provider_tokens_per_turn reached: limit={max_provider_tokens}, observed={}",
                usage.total_provider_tokens
            ),
        });
    }

    let max_consecutive_no_progress_windows =
        total_budget.max_consecutive_no_progress_windows.max(1);
    if usage.consecutive_no_progress_windows >= max_consecutive_no_progress_windows {
        return ExecutionWindowTotalBudgetDecision::Block(ExecutionWindowTotalBudgetBlock {
            kind: ExecutionWindowTotalBudgetBlockKind::MaxConsecutiveNoProgressWindows,
            total_windows: usage.total_windows,
            total_tool_calls: saturating_u32_from_u64(usage.total_tool_calls),
            reason: format!(
                "max_consecutive_no_progress_windows reached: limit={max_consecutive_no_progress_windows}, observed={}",
                usage.consecutive_no_progress_windows
            ),
        });
    }

    ExecutionWindowTotalBudgetDecision::Continue { next_window_index }
}

fn total_budget_block_exhaustion_reason(
    kind: ExecutionWindowTotalBudgetBlockKind,
    fallback: pioneer_protocol::ExecutionWindowExhaustionReason,
) -> pioneer_protocol::ExecutionWindowExhaustionReason {
    match kind {
        ExecutionWindowTotalBudgetBlockKind::MaxWindows => fallback,
        ExecutionWindowTotalBudgetBlockKind::MaxToolCalls => {
            pioneer_protocol::ExecutionWindowExhaustionReason::MaxToolCallsPerWindow
        }
        ExecutionWindowTotalBudgetBlockKind::MaxWallClockMs => {
            pioneer_protocol::ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow
        }
        ExecutionWindowTotalBudgetBlockKind::MaxProviderTokens => {
            pioneer_protocol::ExecutionWindowExhaustionReason::MaxProviderTokensPerWindow
        }
        ExecutionWindowTotalBudgetBlockKind::MaxConsecutiveNoProgressWindows => fallback,
    }
}

fn saturating_u32_from_u64(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn recovery_execution_window_admission_error(
    turn_request: &ActiveTurnRequest,
    tool_loop_config: &ToolLoopConfig,
) -> Option<super::AgentControlError> {
    turn_request.execution_checkpoint_context.as_ref()?;
    match decide_execution_window_total_budget(
        &turn_request.execution_usage,
        &tool_loop_config.execution_windows.total.normalized(),
    ) {
        ExecutionWindowTotalBudgetDecision::Continue { .. } => None,
        ExecutionWindowTotalBudgetDecision::Block(block) => Some(
            super::AgentControlError::ExecutionWindowContinuationBlocked {
                reason: block.reason,
            },
        ),
    }
}

// Agent turn execution composes prompt, hook, provider, and tool-loop futures; keep it off worker stacks.
fn turn_flow_future<'a, F, T>(future: F) -> TurnFlowFuture<'a, T>
where
    F: Future<Output = T> + Send + 'a,
{
    Box::pin(future)
}

pub(super) async fn run_agent_loop(
    thread_id: String,
    workspace_id: String,
    provider_registry: Arc<ProviderRegistry>,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    permission_approval_broker: Arc<RwLock<Arc<dyn pioneer_tools::PermissionApprovalBroker>>>,
    post_turn_hook_dispatch_policy: AgentPostTurnHookDispatchPolicy,
    deferred_task_post_turn_dispatches: Arc<DeferredTaskPostTurnDispatchStore>,
    command_tx: mpsc::Sender<AgentCommand>,
    mut command_rx: mpsc::Receiver<AgentCommand>,
    event_hub: Arc<AgentEventHub>,
) {
    let mut active_turn_id: Option<String> = None;
    let mut active_turn_task: Option<JoinHandle<()>> = None;
    let mut active_turn_control: Option<TurnExecutionControl> = None;
    let mut active_turn_request: Option<ActiveTurnRequest> = None;
    let mut last_turn_request: Option<ActiveTurnRequest> = None;
    let mut active_turn_run_id: Option<u64> = None;
    let mut active_recovery: Option<RecoveryAttemptContext> = None;
    let mut last_turn_observation: Option<(String, super::ExecutionTurnObservation)> = None;
    let mut next_turn_run_id: u64 = 1;

    macro_rules! commit_or_stop {
        ($event:expr $(,)?) => {
            if !publish_loop_durable_event(event_hub.as_ref(), $event).await {
                error!(
                    thread_id = %thread_id,
                    "stopping agent loop after durable event commit was exhausted"
                );
                return;
            }
        };
    }

    while let Some(command) = command_rx.recv().await {
        match command {
            AgentCommand::StartTurn {
                turn_id,
                mode,
                hook_runtime_context,
                model,
                provider_name,
                reasoning,
                workspace_skill_policies,
                skill_catalog,
                agent_skill_overlay,
                input,
                capabilities,
                resolved_artifacts,
                runtime_environment,
                history,
                execution_checkpoint_context,
                permission_profile,
                execution_security_snapshot,
                ack,
            } => {
                if active_turn_id.is_some() {
                    let _ = ack.send(Err(AgentStartError::TurnAlreadyRunning));
                    continue;
                }

                let execution_window_index = execution_checkpoint_context
                    .as_ref()
                    .map(ExecutionCheckpointContext::next_window_index)
                    .unwrap_or(1);
                let execution_usage = execution_checkpoint_context
                    .as_ref()
                    .map(|context| super::TurnExecutionUsageCounters::from_snapshot(context.usage))
                    .unwrap_or_default();

                let turn_request = ActiveTurnRequest {
                    turn_id: turn_id.clone(),
                    execution_window_index,
                    mode,
                    hook_runtime_context,
                    model,
                    provider_name: provider_name.clone(),
                    reasoning,
                    workspace_skill_policies,
                    skill_catalog,
                    agent_skill_overlay,
                    input,
                    capabilities,
                    resolved_artifacts,
                    runtime_environment,
                    history,
                    retained_provider_history: Vec::new(),
                    execution_checkpoint_context,
                    execution_usage,
                    execution_options: super::TurnExecutionOptions::default(),
                    permission_profile,
                    execution_security_snapshot,
                };

                let provider = match provider_registry
                    .get_or_create_for_workspace(workspace_id.as_str(), &provider_name)
                {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = ack.send(Err(AgentStartError::Internal(format!(
                            "failed to create provider `{provider_name}`: {e}"
                        ))));
                        continue;
                    }
                };

                // A direct start carrying an execution checkpoint is a native
                // recovery start (task recovery uses this path instead of the
                // explicit recovery command).  The durable state machine must
                // observe the continuation before the chat flow emits the
                // Started event for the next window; otherwise that Started
                // event is rejected because the checkpointed predecessor has
                // not been advanced yet and the provider loop waits forever
                // for a commit that can never become valid.
                if let Err(error) = publish_recovery_execution_window_continued(
                    event_hub.as_ref(),
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    &turn_request,
                )
                .await
                {
                    let _ = ack.send(Err(AgentStartError::Internal(format!(
                        "failed to publish execution window continuation for direct start: {error}"
                    ))));
                    continue;
                }

                active_turn_id = Some(turn_id.clone());
                active_turn_request = Some(turn_request.clone());
                last_turn_request = Some(turn_request.clone());
                active_recovery = None;
                last_turn_observation = None;
                let task_recovery = None;

                let run_id = next_turn_run_id;
                next_turn_run_id = next_turn_run_id.saturating_add(1);

                let turn_control = TurnExecutionControl::new(command_tx.clone(), run_id);
                active_turn_control = Some(turn_control.clone());

                active_turn_run_id = Some(run_id);

                active_turn_task = Some(spawn_turn_task(
                    command_tx.clone(),
                    event_hub.clone(),
                    thread_id.clone(),
                    workspace_id.clone(),
                    provider_registry.clone(),
                    tool_loop_config.clone(),
                    mcp_tool_provider.clone(),
                    turn_tool_provider.clone(),
                    turn_finalization_provider.clone(),
                    task_tool_provider.clone(),
                    hook_runtime.clone(),
                    tool_bundle_artifacts.clone(),
                    permission_approval_broker.clone(),
                    provider,
                    turn_request,
                    turn_control,
                    task_recovery,
                    run_id,
                ));

                let _ = ack.send(Ok(()));
            }
            AgentCommand::TurnTaskFinished {
                turn_id,
                run_id,
                completion,
            } => {
                if active_turn_id.as_deref() != Some(turn_id.as_str()) {
                    continue;
                }
                if active_turn_run_id != Some(run_id) {
                    continue;
                }

                let turn_request_snapshot = active_turn_request.clone();
                let recovery = active_recovery.clone();
                active_turn_task = None;
                active_turn_control = None;
                active_turn_run_id = None;

                let TurnTaskCompletion {
                    result,
                    post_turn_dispatch,
                } = completion;

                match result {
                    Ok(TurnTaskSuccess::Completed) => {
                        last_turn_observation = Some((
                            turn_id.clone(),
                            super::ExecutionTurnObservation {
                                status: super::ExecutionTurnStatus::Completed,
                                message: None,
                            },
                        ));
                        commit_or_stop!(AgentDurableEvent::TurnCompleted {
                            thread_id: thread_id.clone(),
                            turn_id,
                            recovery,
                        },);
                        active_turn_id = None;
                        active_turn_request = None;
                        active_recovery = None;
                        maybe_dispatch_post_turn_hook(
                            hook_runtime.clone(),
                            post_turn_hook_dispatch_policy,
                            deferred_task_post_turn_dispatches.as_ref(),
                            post_turn_dispatch,
                        )
                        .await;
                    }
                    Ok(TurnTaskSuccess::NeedsContinuation(continuation)) => {
                        debug!(
                            turn_id = %turn_id,
                            reason = ?continuation.reason,
                            checkpoint_schema_version =
                                continuation.checkpoint_payload.schema_version,
                            "turn execution window needs continuation"
                        );
                        let Some(mut next_turn_request) = turn_request_snapshot.clone() else {
                            let blocked_reason =
                                "execution window continuation could not resume: active turn request missing"
                                    .to_owned();
                            last_turn_observation = Some((
                                turn_id.clone(),
                                super::ExecutionTurnObservation {
                                    status: super::ExecutionTurnStatus::Blocked,
                                    message: Some(blocked_reason.clone()),
                                },
                            ));
                            commit_or_stop!(AgentDurableEvent::TurnExecutionWindowBlocked {
                                notification: TurnExecutionWindowBlockedNotification {
                                    workspace_id: workspace_id.clone(),
                                    thread_id: thread_id.clone(),
                                    turn_id: turn_id.clone(),
                                    window_id: continuation.exhausted_window_id.clone(),
                                    window_index: continuation
                                        .checkpoint_payload
                                        .window
                                        .window_index,
                                    status: ExecutionWindowStatus::Blocked,
                                    exhaustion_reason: Some(continuation.reason),
                                    checkpoint_id: Some(continuation.checkpoint_id.clone()),
                                    total_windows: continuation
                                        .checkpoint_payload
                                        .window
                                        .window_index,
                                    total_tool_calls: continuation
                                        .checkpoint_payload
                                        .tools
                                        .total_count,
                                    reason: blocked_reason.clone(),
                                    blocked_at_unix_ms: chrono::Local::now().timestamp_millis(),
                                },
                            },);
                            commit_or_stop!(AgentDurableEvent::TurnBlocked {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                reason: blocked_reason,
                                recovery,
                            },);
                            active_turn_id = None;
                            active_turn_run_id = None;
                            active_turn_control = None;
                            active_turn_request = None;
                            active_recovery = None;
                            continue;
                        };
                        let total_budget = tool_loop_config.execution_windows.total.normalized();
                        next_turn_request
                            .execution_usage
                            .observe_checkpoint_payload(&continuation.checkpoint_payload);
                        let next_window_index = match decide_execution_window_total_budget(
                            &next_turn_request.execution_usage,
                            &total_budget,
                        ) {
                            ExecutionWindowTotalBudgetDecision::Continue { next_window_index } => {
                                next_window_index
                            }
                            ExecutionWindowTotalBudgetDecision::Block(block) => {
                                let blocked_reason = block.reason.clone();
                                last_turn_observation = Some((
                                    turn_id.clone(),
                                    super::ExecutionTurnObservation {
                                        status: super::ExecutionTurnStatus::Blocked,
                                        message: Some(blocked_reason.clone()),
                                    },
                                ));
                                commit_or_stop!(AgentDurableEvent::TurnExecutionWindowBlocked {
                                    notification: TurnExecutionWindowBlockedNotification {
                                        workspace_id: workspace_id.clone(),
                                        thread_id: thread_id.clone(),
                                        turn_id: turn_id.clone(),
                                        window_id: continuation.exhausted_window_id.clone(),
                                        window_index: continuation
                                            .checkpoint_payload
                                            .window
                                            .window_index,
                                        status: ExecutionWindowStatus::Blocked,
                                        exhaustion_reason: Some(
                                            total_budget_block_exhaustion_reason(
                                                block.kind,
                                                continuation.reason,
                                            ),
                                        ),
                                        checkpoint_id: Some(continuation.checkpoint_id.clone()),
                                        total_windows: block.total_windows,
                                        total_tool_calls: block.total_tool_calls,
                                        reason: block.reason,
                                        blocked_at_unix_ms: chrono::Local::now().timestamp_millis(),
                                    },
                                },);
                                commit_or_stop!(AgentDurableEvent::TurnBlocked {
                                    thread_id: thread_id.clone(),
                                    turn_id: turn_id.clone(),
                                    reason: blocked_reason,
                                    recovery,
                                },);
                                active_turn_id = None;
                                active_turn_run_id = None;
                                active_turn_control = None;
                                active_turn_request = None;
                                active_recovery = None;
                                continue;
                            }
                        };
                        next_turn_request.execution_window_index = next_window_index;
                        next_turn_request.retained_provider_history =
                            continuation.provider_history.clone();
                        next_turn_request.execution_options.continue_generation_hint = true;
                        next_turn_request.execution_checkpoint_context =
                            Some(ExecutionCheckpointContext {
                                window_id: continuation.exhausted_window_id.clone(),
                                window_index: continuation.checkpoint_payload.window.window_index,
                                checkpoint_id: continuation.checkpoint_id.clone(),
                                checkpoint_kind: "execution_window_exhausted".to_owned(),
                                payload: continuation.checkpoint_payload.clone(),
                                usage: next_turn_request.execution_usage.snapshot(),
                            });

                        let provider = match provider_registry.get_or_create_for_workspace(
                            workspace_id.as_str(),
                            next_turn_request.provider_name.as_str(),
                        ) {
                            Ok(provider) => provider,
                            Err(error) => {
                                let failure_message = format!(
                                    "failed to recreate provider `{}` for execution window continuation: {error}",
                                    next_turn_request.provider_name
                                );
                                last_turn_observation = Some((
                                    turn_id.clone(),
                                    super::ExecutionTurnObservation {
                                        status: super::ExecutionTurnStatus::Failed,
                                        message: Some(failure_message.clone()),
                                    },
                                ));
                                commit_or_stop!(AgentDurableEvent::TurnFailed {
                                    thread_id: thread_id.clone(),
                                    turn_id: turn_id.clone(),
                                    error: failure_message,
                                    recovery,
                                },);
                                active_turn_id = None;
                                active_turn_request = None;
                                active_recovery = None;
                                continue;
                            }
                        };

                        let run_id = next_turn_run_id;
                        next_turn_run_id = next_turn_run_id.saturating_add(1);
                        let turn_control = TurnExecutionControl::new(command_tx.clone(), run_id);
                        let next_window_id = format!("{turn_id}:window:{next_window_index}");

                        commit_or_stop!(AgentDurableEvent::TurnExecutionWindowContinued {
                            notification: TurnExecutionWindowContinuedNotification {
                                workspace_id: workspace_id.clone(),
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                window_id: next_window_id,
                                window_index: next_window_index,
                                status: ExecutionWindowStatus::Continued,
                                previous_window_id: continuation.exhausted_window_id.clone(),
                                previous_window_index: continuation
                                    .checkpoint_payload
                                    .window
                                    .window_index,
                                checkpoint_id: continuation.checkpoint_id.clone(),
                                continued_at_unix_ms: chrono::Local::now().timestamp_millis(),
                            },
                        },);

                        active_turn_id = Some(turn_id.clone());
                        active_turn_run_id = Some(run_id);
                        active_turn_control = Some(turn_control.clone());
                        active_turn_request = Some(next_turn_request.clone());
                        last_turn_request = Some(next_turn_request.clone());
                        active_recovery = recovery.clone();

                        active_turn_task = Some(spawn_turn_task(
                            command_tx.clone(),
                            event_hub.clone(),
                            thread_id.clone(),
                            workspace_id.clone(),
                            provider_registry.clone(),
                            tool_loop_config.clone(),
                            mcp_tool_provider.clone(),
                            turn_tool_provider.clone(),
                            turn_finalization_provider.clone(),
                            task_tool_provider.clone(),
                            hook_runtime.clone(),
                            tool_bundle_artifacts.clone(),
                            permission_approval_broker.clone(),
                            provider,
                            next_turn_request,
                            turn_control,
                            recovery,
                            run_id,
                        ));
                    }
                    Err(TurnTaskFailure::Terminal(error)) => {
                        last_turn_observation = Some((
                            turn_id.clone(),
                            super::ExecutionTurnObservation {
                                status: super::ExecutionTurnStatus::Failed,
                                message: Some(error.clone()),
                            },
                        ));
                        let error_for_dispatch = error.clone();
                        commit_or_stop!(AgentDurableEvent::TurnFailed {
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            error,
                            recovery,
                        },);
                        active_turn_id = None;
                        active_turn_request = None;
                        active_recovery = None;
                        cleanup_attached_tasks(
                            task_tool_provider.as_ref(),
                            workspace_id.as_str(),
                            thread_id.as_str(),
                            turn_id.as_str(),
                            format!("parent turn failed: {error_for_dispatch}"),
                        )
                        .await;
                        let failure_dispatch = post_turn_dispatch.or_else(|| {
                            synthesize_post_turn_failure_dispatch(
                                workspace_id.as_str(),
                                thread_id.as_str(),
                                turn_id.as_str(),
                                turn_request_snapshot.as_ref(),
                                TurnPostTurnStatus::Failed,
                                error_for_dispatch,
                            )
                        });
                        maybe_dispatch_post_turn_hook(
                            hook_runtime.clone(),
                            post_turn_hook_dispatch_policy,
                            deferred_task_post_turn_dispatches.as_ref(),
                            failure_dispatch,
                        )
                        .await;
                    }
                    Err(TurnTaskFailure::Blocked(reason)) => {
                        last_turn_observation = Some((
                            turn_id.clone(),
                            super::ExecutionTurnObservation {
                                status: super::ExecutionTurnStatus::Blocked,
                                message: Some(reason.clone()),
                            },
                        ));
                        let reason_for_dispatch = reason.clone();
                        commit_or_stop!(AgentDurableEvent::TurnBlocked {
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            reason,
                            recovery,
                        },);
                        active_turn_id = None;
                        active_turn_request = None;
                        active_recovery = None;
                        let blocked_dispatch = post_turn_dispatch.or_else(|| {
                            synthesize_post_turn_failure_dispatch(
                                workspace_id.as_str(),
                                thread_id.as_str(),
                                turn_id.as_str(),
                                turn_request_snapshot.as_ref(),
                                TurnPostTurnStatus::Blocked,
                                reason_for_dispatch,
                            )
                        });
                        maybe_dispatch_post_turn_hook(
                            hook_runtime.clone(),
                            post_turn_hook_dispatch_policy,
                            deferred_task_post_turn_dispatches.as_ref(),
                            blocked_dispatch,
                        )
                        .await;
                    }
                    Err(TurnTaskFailure::ProviderFailure {
                        item_id,
                        item_type,
                        failure,
                    }) => {
                        last_turn_request = turn_request_snapshot.clone();
                        let failure_message = failure.message.clone().unwrap_or_else(|| {
                            format!(
                                "provider failure: {:?} during {:?}",
                                failure.class, failure.stage
                            )
                        });
                        commit_or_stop!(AgentDurableEvent::ProviderFailureDetected {
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            item_id,
                            item_type,
                            failure,
                            recovery,
                        },);
                        active_turn_id = None;
                        active_turn_request = None;
                        active_recovery = None;
                        let failure_dispatch = post_turn_dispatch.or_else(|| {
                            synthesize_post_turn_failure_dispatch(
                                workspace_id.as_str(),
                                thread_id.as_str(),
                                turn_id.as_str(),
                                turn_request_snapshot.as_ref(),
                                TurnPostTurnStatus::ProviderFailure,
                                failure_message,
                            )
                        });
                        maybe_dispatch_post_turn_hook(
                            hook_runtime.clone(),
                            post_turn_hook_dispatch_policy,
                            deferred_task_post_turn_dispatches.as_ref(),
                            failure_dispatch,
                        )
                        .await;
                    }
                }
            }
            AgentCommand::RecoveryAttemptSucceeded {
                turn_id,
                run_id,
                recovery,
            } => {
                if active_turn_id.as_deref() != Some(turn_id.as_str()) {
                    continue;
                }
                if active_turn_run_id != Some(run_id) {
                    continue;
                }
                if active_recovery.as_ref() != Some(&recovery) {
                    continue;
                }

                commit_or_stop!(AgentDurableEvent::RecoveryAttemptSucceeded {
                    thread_id: thread_id.clone(),
                    turn_id,
                    recovery,
                },);
                active_recovery = None;
            }
            AgentCommand::CancelAttempt {
                turn_id,
                item_id,
                ack,
            } => {
                if active_turn_id.is_none() {
                    let _ = ack.send(Err(super::AgentControlError::NoActiveTurn));
                    continue;
                }
                if active_turn_id.as_deref() != Some(turn_id.as_str()) {
                    let _ = ack.send(Err(super::AgentControlError::TurnMismatch));
                    continue;
                }

                let Some(control) = active_turn_control.clone() else {
                    let _ = ack.send(Err(super::AgentControlError::NoActiveTurn));
                    continue;
                };

                let cancelled = control.cancel_attempt(item_id.as_str()).await;

                if cancelled {
                    let _ = ack.send(Ok(()));
                } else {
                    let _ = ack.send(Err(super::AgentControlError::AttemptNotRunning));
                }
            }
            AgentCommand::CancelTurn {
                turn_id,
                reason,
                ack,
            } => {
                if active_turn_id.is_none() {
                    let _ = ack.send(Err(super::AgentControlError::NoActiveTurn));
                    continue;
                }
                if active_turn_id.as_deref() != Some(turn_id.as_str()) {
                    let _ = ack.send(Err(super::AgentControlError::TurnMismatch));
                    continue;
                }

                if let Some(control) = active_turn_control.as_ref() {
                    control.cancel_all_attempts().await;
                }

                if let Some(task) = active_turn_task.take() {
                    sleep(Duration::from_millis(TURN_CANCEL_GRACE_MS)).await;
                    if task.is_finished() {
                        if let Err(error) = task.await
                            && !error.is_cancelled()
                        {
                            error!(error = %error, "active turn task failed during cancellation");
                        }
                    } else {
                        task.abort();
                        if let Err(error) = task.await
                            && !error.is_cancelled()
                        {
                            error!(error = %error, "active turn task failed after abort");
                        }
                    }
                }
                let turn_request_snapshot = active_turn_request.clone();
                let recovery = active_recovery.clone();
                last_turn_observation = Some((
                    turn_id.clone(),
                    super::ExecutionTurnObservation {
                        status: super::ExecutionTurnStatus::Interrupted,
                        message: Some(reason.clone()),
                    },
                ));
                let reason_for_dispatch = reason.clone();
                commit_or_stop!(AgentDurableEvent::TurnInterrupted {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    reason,
                    recovery,
                },);
                active_turn_id = None;
                active_turn_control = None;
                active_turn_request = None;
                active_turn_run_id = None;
                active_recovery = None;
                cleanup_attached_tasks(
                    task_tool_provider.as_ref(),
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    turn_id.as_str(),
                    format!("parent turn cancelled: {reason_for_dispatch}"),
                )
                .await;
                let _ = ack.send(Ok(()));
                let interrupted_dispatch = synthesize_post_turn_failure_dispatch(
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    turn_id.as_str(),
                    turn_request_snapshot.as_ref(),
                    TurnPostTurnStatus::Interrupted,
                    reason_for_dispatch,
                );
                maybe_dispatch_post_turn_hook(
                    hook_runtime.clone(),
                    post_turn_hook_dispatch_policy,
                    deferred_task_post_turn_dispatches.as_ref(),
                    interrupted_dispatch,
                )
                .await;
            }
            AgentCommand::ObserveTurn { turn_id, ack } => {
                let observation = if active_turn_id.as_deref() == Some(turn_id.as_str()) {
                    Some(super::ExecutionTurnObservation {
                        status: super::ExecutionTurnStatus::InProgress,
                        message: None,
                    })
                } else {
                    last_turn_observation
                        .as_ref()
                        .and_then(|(observed_turn_id, observation)| {
                            (observed_turn_id == &turn_id).then(|| observation.clone())
                        })
                };
                let _ = ack.send(observation);
            }
            AgentCommand::StartRecoveryAttempt { request, ack } => {
                if active_turn_id.is_none()
                    && !request.item_type.is_tool_item()
                    && let Some(turn_request) = last_turn_request.clone()
                    && turn_request.turn_id == request.turn_id
                {
                    let mut turn_request = turn_request;

                    super::apply_recovery_adjustments(&mut turn_request, &request);
                    if let Some(error) =
                        recovery_execution_window_admission_error(&turn_request, &tool_loop_config)
                    {
                        let _ = ack.send(Err(error));
                        continue;
                    }

                    if request.refresh_provider_auth {
                        provider_registry.invalidate(turn_request.provider_name.as_str());
                    }

                    let provider = match provider_registry.get_or_create_for_workspace(
                        workspace_id.as_str(),
                        turn_request.provider_name.as_str(),
                    ) {
                        Ok(provider) => provider,
                        Err(error) => {
                            let _ = ack.send(Err(super::AgentControlError::Internal(format!(
                                "failed to recreate provider `{}` for recovery: {error}",
                                turn_request.provider_name
                            ))));
                            continue;
                        }
                    };
                    if let Err(error) = publish_recovery_execution_window_continued(
                        event_hub.as_ref(),
                        workspace_id.as_str(),
                        thread_id.as_str(),
                        &turn_request,
                    )
                    .await
                    {
                        let _ = ack.send(Err(error));
                        continue;
                    }

                    let run_id = next_turn_run_id;
                    next_turn_run_id = next_turn_run_id.saturating_add(1);

                    active_turn_run_id = Some(run_id);
                    active_turn_id = Some(request.turn_id.clone());
                    last_turn_observation = None;

                    let turn_control = TurnExecutionControl::new(command_tx.clone(), run_id);
                    active_turn_control = Some(turn_control.clone());

                    active_turn_request = Some(turn_request.clone());
                    last_turn_request = Some(turn_request.clone());
                    let recovery = recovery_context(&request);
                    active_recovery = Some(recovery.clone());

                    active_turn_task = Some(spawn_turn_task(
                        command_tx.clone(),
                        event_hub.clone(),
                        thread_id.clone(),
                        workspace_id.clone(),
                        provider_registry.clone(),
                        tool_loop_config.clone(),
                        mcp_tool_provider.clone(),
                        turn_tool_provider.clone(),
                        turn_finalization_provider.clone(),
                        task_tool_provider.clone(),
                        hook_runtime.clone(),
                        tool_bundle_artifacts.clone(),
                        permission_approval_broker.clone(),
                        provider,
                        turn_request,
                        turn_control,
                        Some(recovery),
                        run_id,
                    ));

                    let _ = ack.send(Ok(()));
                    continue;
                }

                if active_turn_id.is_none() {
                    let _ = ack.send(Err(super::AgentControlError::NoActiveTurn));
                    continue;
                }
                if active_turn_id.as_deref() != Some(request.turn_id.as_str()) {
                    let _ = ack.send(Err(super::AgentControlError::TurnMismatch));
                    continue;
                }

                if request.item_type.is_tool_item() {
                    let Some(control) = active_turn_control.clone() else {
                        let _ = ack.send(Err(super::AgentControlError::NoActiveTurn));
                        continue;
                    };

                    let recovery = recovery_context(&request);
                    let cancelled = control
                        .cancel_attempt_for_recovery(request.item_id.as_str(), recovery.clone())
                        .await;

                    if cancelled {
                        active_recovery = Some(recovery);
                        let _ = ack.send(Ok(()));
                    } else {
                        let _ = ack.send(Err(super::AgentControlError::AttemptNotRunning));
                    }
                    continue;
                }

                let Some(turn_request) = active_turn_request.clone() else {
                    let _ = ack.send(Err(super::AgentControlError::NoActiveTurn));
                    continue;
                };
                let mut turn_request = turn_request;

                super::apply_recovery_adjustments(&mut turn_request, &request);
                if let Some(error) =
                    recovery_execution_window_admission_error(&turn_request, &tool_loop_config)
                {
                    let _ = ack.send(Err(error));
                    continue;
                }

                if let Some(task) = active_turn_task.take() {
                    task.abort();
                }

                if request.refresh_provider_auth {
                    provider_registry.invalidate(turn_request.provider_name.as_str());
                }

                let provider = match provider_registry.get_or_create_for_workspace(
                    workspace_id.as_str(),
                    turn_request.provider_name.as_str(),
                ) {
                    Ok(provider) => provider,
                    Err(error) => {
                        let _ = ack.send(Err(super::AgentControlError::Internal(format!(
                            "failed to recreate provider `{}` for recovery: {error}",
                            turn_request.provider_name
                        ))));
                        continue;
                    }
                };
                if let Err(error) = publish_recovery_execution_window_continued(
                    event_hub.as_ref(),
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    &turn_request,
                )
                .await
                {
                    active_turn_id = None;
                    active_turn_run_id = None;
                    active_turn_control = None;
                    active_turn_request = None;
                    active_recovery = None;
                    let _ = ack.send(Err(error));
                    continue;
                }

                let run_id = next_turn_run_id;
                next_turn_run_id = next_turn_run_id.saturating_add(1);
                active_turn_run_id = Some(run_id);
                last_turn_observation = None;
                active_turn_request = Some(turn_request.clone());
                last_turn_request = Some(turn_request.clone());
                let recovery = recovery_context(&request);
                active_recovery = Some(recovery.clone());

                let turn_control = TurnExecutionControl::new(command_tx.clone(), run_id);
                active_turn_control = Some(turn_control.clone());

                active_turn_task = Some(spawn_turn_task(
                    command_tx.clone(),
                    event_hub.clone(),
                    thread_id.clone(),
                    workspace_id.clone(),
                    provider_registry.clone(),
                    tool_loop_config.clone(),
                    mcp_tool_provider.clone(),
                    turn_tool_provider.clone(),
                    turn_finalization_provider.clone(),
                    task_tool_provider.clone(),
                    hook_runtime.clone(),
                    tool_bundle_artifacts.clone(),
                    permission_approval_broker.clone(),
                    provider,
                    turn_request,
                    turn_control,
                    Some(recovery),
                    run_id,
                ));

                let _ = ack.send(Ok(()));
            }
            AgentCommand::StartRestoredRecoveryTurn {
                turn_request,
                recovery_request,
                ack,
            } => {
                if active_turn_id.is_some() {
                    let _ = ack.send(Err(super::AgentControlError::TurnAlreadyRunning));
                    continue;
                }
                if turn_request.turn_id != recovery_request.turn_id {
                    let _ = ack.send(Err(super::AgentControlError::TurnMismatch));
                    continue;
                }

                let mut active_request = ActiveTurnRequest {
                    turn_id: turn_request.turn_id.clone(),
                    execution_window_index: turn_request.execution_window_index.max(1),
                    mode: turn_request.mode,
                    hook_runtime_context: turn_request.hook_runtime_context,
                    model: turn_request.model,
                    provider_name: turn_request.provider_name,
                    reasoning: turn_request.reasoning,
                    workspace_skill_policies: turn_request.workspace_skill_policies,
                    skill_catalog: turn_request.skill_catalog,
                    agent_skill_overlay: turn_request.agent_skill_overlay,
                    input: turn_request.input,
                    capabilities: turn_request.capabilities,
                    resolved_artifacts: turn_request.resolved_artifacts,
                    runtime_environment: turn_request.runtime_environment,
                    history: turn_request.history,
                    retained_provider_history: Vec::new(),
                    execution_checkpoint_context: None,
                    execution_usage: super::TurnExecutionUsageCounters::default(),
                    execution_options: super::TurnExecutionOptions::default(),
                    permission_profile: turn_request.permission_profile,
                    execution_security_snapshot: turn_request.execution_security_snapshot,
                };

                super::apply_recovery_adjustments(&mut active_request, &recovery_request);
                if let Some(error) =
                    recovery_execution_window_admission_error(&active_request, &tool_loop_config)
                {
                    let _ = ack.send(Err(error));
                    continue;
                }

                if recovery_request.refresh_provider_auth {
                    provider_registry.invalidate(active_request.provider_name.as_str());
                }

                let provider = match provider_registry.get_or_create_for_workspace(
                    workspace_id.as_str(),
                    active_request.provider_name.as_str(),
                ) {
                    Ok(provider) => provider,
                    Err(error) => {
                        let _ = ack.send(Err(super::AgentControlError::Internal(format!(
                            "failed to recreate provider `{}` for restored recovery: {error}",
                            active_request.provider_name
                        ))));
                        continue;
                    }
                };
                if let Err(error) = publish_recovery_execution_window_continued(
                    event_hub.as_ref(),
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    &active_request,
                )
                .await
                {
                    let _ = ack.send(Err(error));
                    continue;
                }

                let run_id = next_turn_run_id;
                next_turn_run_id = next_turn_run_id.saturating_add(1);
                let turn_control = TurnExecutionControl::new(command_tx.clone(), run_id);
                let recovery = recovery_context(&recovery_request);

                active_turn_id = Some(active_request.turn_id.clone());
                active_turn_run_id = Some(run_id);
                active_turn_control = Some(turn_control.clone());
                active_turn_request = Some(active_request.clone());
                last_turn_request = Some(active_request.clone());
                active_recovery = Some(recovery.clone());

                active_turn_task = Some(spawn_turn_task(
                    command_tx.clone(),
                    event_hub.clone(),
                    thread_id.clone(),
                    workspace_id.clone(),
                    provider_registry.clone(),
                    tool_loop_config.clone(),
                    mcp_tool_provider.clone(),
                    turn_tool_provider.clone(),
                    turn_finalization_provider.clone(),
                    task_tool_provider.clone(),
                    hook_runtime.clone(),
                    tool_bundle_artifacts.clone(),
                    permission_approval_broker.clone(),
                    provider,
                    active_request,
                    turn_control,
                    Some(recovery),
                    run_id,
                ));

                let _ = ack.send(Ok(()));
            }
            AgentCommand::Shutdown => {
                if let Some(task) = active_turn_task.take() {
                    task.abort();
                }
                break;
            }
        }
    }
}

fn spawn_turn_task(
    command_tx: mpsc::Sender<AgentCommand>,
    event_hub: Arc<AgentEventHub>,
    thread_id: String,
    workspace_id: String,
    provider_registry: Arc<ProviderRegistry>,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    permission_approval_broker: Arc<RwLock<Arc<dyn pioneer_tools::PermissionApprovalBroker>>>,
    provider: Arc<dyn Provider>,
    turn_request: ActiveTurnRequest,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    run_id: u64,
) -> JoinHandle<()> {
    tokio::spawn(turn_flow_future(async move {
        let permission_approval_broker = permission_approval_broker.read().await.clone();
        let result = AssertUnwindSafe(turn_flow_future(execute_turn_flow(
            thread_id.clone(),
            turn_request.turn_id.clone(),
            workspace_id,
            turn_request.mode,
            provider_registry,
            provider,
            turn_request.model,
            turn_request.hook_runtime_context,
            turn_request.reasoning,
            turn_request.workspace_skill_policies,
            turn_request.skill_catalog,
            turn_request.agent_skill_overlay,
            turn_request.input,
            turn_request.capabilities,
            turn_request.resolved_artifacts,
            turn_request.runtime_environment,
            turn_request.history,
            turn_request.retained_provider_history,
            turn_request.execution_window_index,
            turn_request.execution_checkpoint_context,
            turn_request.permission_profile,
            turn_request.execution_security_snapshot,
            turn_request.execution_options.force_non_stream,
            turn_request.execution_options.disable_tool_calling,
            turn_request.execution_options.continue_generation_hint,
            tool_loop_config,
            mcp_tool_provider,
            turn_tool_provider,
            turn_finalization_provider,
            task_tool_provider,
            hook_runtime,
            tool_bundle_artifacts,
            permission_approval_broker,
            turn_control,
            recovery,
            event_hub,
        )))
        .catch_unwind()
        .await
        .unwrap_or_else(|panic| {
            let message = panic_payload_message(panic.as_ref());
            error!(
                thread_id = %thread_id,
                turn_id = %turn_request.turn_id,
                message,
                "agent turn task panicked"
            );
            TurnTaskCompletion {
                result: Err(TurnTaskFailure::Terminal(format!(
                    "agent turn task panicked: {message}"
                ))),
                post_turn_dispatch: None,
            }
        });

        let _ = command_tx
            .send(AgentCommand::TurnTaskFinished {
                turn_id: turn_request.turn_id,
                run_id,
                completion: result,
            })
            .await;
    }))
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "non-string panic payload".to_owned()
}

fn recovery_context(request: &super::RecoveryAttemptRequest) -> RecoveryAttemptContext {
    RecoveryAttemptContext {
        job_id: request.recovery_job_id.clone(),
        attempt_id: request.recovery_attempt_id.clone(),
    }
}

async fn execute_turn_flow(
    thread_id: String,
    turn_id: String,
    workspace_id: String,
    mode: ThreadMode,
    provider_registry: Arc<ProviderRegistry>,
    provider: Arc<dyn Provider>,
    model: String,
    hook_runtime_context: super::AgentTurnHookRuntimeContext,
    reasoning: Option<pioneer_provider::ReasoningConfig>,
    workspace_skill_policies: std::collections::HashMap<
        pioneer_skills::SkillPolicyKey,
        super::WorkspaceSkillPolicy,
    >,
    skill_catalog: pioneer_skills::SkillCatalogSnapshot,
    agent_skill_overlay: Vec<pioneer_skills::AgentSkillRuntimeEntry>,
    input: Vec<UserInput>,
    capabilities: Vec<TurnCapability>,
    resolved_artifacts: Vec<super::ResolvedArtifactInput>,
    runtime_environment: std::collections::HashMap<String, String>,
    history: Vec<ChatMessage>,
    retained_provider_history: Vec<super::RetainedProviderHistoryMessage>,
    execution_window_index: u32,
    execution_checkpoint_context: Option<super::ExecutionCheckpointContext>,
    permission_profile: TurnPermissionProfileSnapshot,
    execution_security_snapshot: Option<TurnExecutionSecuritySnapshot>,
    force_non_stream: bool,
    disable_tool_calling: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    turn_tool_provider: Option<Arc<dyn TurnToolProvider>>,
    turn_finalization_provider: Option<Arc<dyn TurnFinalizationProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    permission_approval_broker: Arc<dyn pioneer_tools::PermissionApprovalBroker>,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    event_hub: Arc<AgentEventHub>,
) -> TurnTaskCompletion {
    match mode {
        ThreadMode::Message => TurnTaskCompletion {
            result: Err(TurnTaskFailure::Terminal(
                "message turns must complete in gateway admission without entering agent execution"
                    .to_owned(),
            )),
            post_turn_dispatch: None,
        },
        ThreadMode::Chat | ThreadMode::Agent => turn_flow_future(chat::execute_chat_turn_flow(
            thread_id,
            turn_id,
            workspace_id,
            mode,
            provider_registry,
            provider,
            model,
            hook_runtime_context,
            reasoning,
            workspace_skill_policies,
            skill_catalog,
            agent_skill_overlay,
            input,
            capabilities,
            resolved_artifacts,
            runtime_environment,
            history,
            retained_provider_history,
            execution_window_index,
            execution_checkpoint_context,
            permission_profile,
            execution_security_snapshot,
            force_non_stream,
            disable_tool_calling,
            continue_generation_hint,
            tool_loop_config,
            mcp_tool_provider,
            turn_tool_provider,
            turn_finalization_provider,
            task_tool_provider,
            hook_runtime,
            tool_bundle_artifacts,
            permission_approval_broker,
            turn_control,
            recovery,
            event_hub,
        ))
        .await
        .map(|outcome| match outcome {
            chat::ChatTurnOutcome::Completed { post_turn_dispatch } => TurnTaskCompletion {
                result: Ok(TurnTaskSuccess::Completed),
                post_turn_dispatch,
            },
            chat::ChatTurnOutcome::NeedsContinuation(continuation) => TurnTaskCompletion {
                result: Ok(TurnTaskSuccess::NeedsContinuation(continuation)),
                post_turn_dispatch: None,
            },
        })
        .unwrap_or_else(|error| {
            let (error, post_turn_dispatch) = error.into_parts();
            TurnTaskCompletion {
                result: Err(match error {
                    chat::ChatTurnError::Terminal(message) => TurnTaskFailure::Terminal(message),
                    chat::ChatTurnError::Blocked(message) => TurnTaskFailure::Blocked(message),
                    chat::ChatTurnError::ProviderFailure {
                        item_id,
                        item_type,
                        failure,
                    } => TurnTaskFailure::ProviderFailure {
                        item_id,
                        item_type,
                        failure,
                    },
                    chat::ChatTurnError::WithPostTurnDispatch { .. } => {
                        unreachable!("post-turn dispatch wrapper should be unwrapped")
                    }
                }),
                post_turn_dispatch,
            }
        }),
    }
}

async fn publish_loop_durable_event(event_hub: &AgentEventHub, event: AgentDurableEvent) -> bool {
    const COMMIT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
    let mut attempt = 0_u32;
    loop {
        attempt = attempt.saturating_add(1);
        let result = timeout(
            COMMIT_ATTEMPT_TIMEOUT,
            event_hub.publish_durable_and_wait(event.clone()),
        )
        .await;
        match result {
            Ok(Ok(())) => return true,
            Ok(Err(error)) => error!(
                attempt,
                error = %error,
                "durable agent-loop event remains pending after rejected commit"
            ),
            Err(_) => error!(
                attempt,
                timeout_seconds = COMMIT_ATTEMPT_TIMEOUT.as_secs(),
                "durable agent-loop event remains pending after commit attempt timed out"
            ),
        }

        // A terminal/checkpoint boundary cannot be abandoned or converted to
        // process-local success. Retry indefinitely across transient listener
        // and database outages, but cap the cadence so an unavailable sink does
        // not create a hot no-progress loop. Each retry has stable event
        // identity and is safe after an unknown commit result.
        let delay_ms = 100_u64
            .saturating_mul(1_u64 << attempt.saturating_sub(1).min(8))
            .min(30_000);
        sleep(Duration::from_millis(delay_ms)).await;
    }
}

async fn publish_recovery_execution_window_continued(
    event_hub: &AgentEventHub,
    workspace_id: &str,
    thread_id: &str,
    turn_request: &ActiveTurnRequest,
) -> Result<(), super::AgentControlError> {
    let Some(context) = turn_request.execution_checkpoint_context.as_ref() else {
        return Ok(());
    };
    let next_window_index = turn_request
        .execution_window_index
        .max(context.next_window_index());

    event_hub
        .publish_durable_and_wait(AgentDurableEvent::TurnExecutionWindowContinued {
            notification: TurnExecutionWindowContinuedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_request.turn_id.clone(),
                window_id: format!("{}:window:{next_window_index}", turn_request.turn_id),
                window_index: next_window_index,
                status: ExecutionWindowStatus::Continued,
                previous_window_id: context.window_id.clone(),
                previous_window_index: context.window_index,
                checkpoint_id: context.checkpoint_id.clone(),
                continued_at_unix_ms: chrono::Local::now().timestamp_millis(),
            },
        })
        .await
        .map_err(|error| {
            super::AgentControlError::Internal(format!(
                "failed to publish execution window continuation for recovery: {error}"
            ))
        })
}

async fn maybe_dispatch_post_turn_hook(
    hook_runtime: Option<Arc<HookRuntime>>,
    policy: AgentPostTurnHookDispatchPolicy,
    deferred_task_post_turn_dispatches: &DeferredTaskPostTurnDispatchStore,
    dispatch: Option<AgentTurnPostTurnHookDispatch>,
) {
    let Some(dispatch) = dispatch else {
        return;
    };
    if !policy.should_dispatch(dispatch.status()) {
        return;
    }

    deferred_task_post_turn_dispatches
        .defer(hook_runtime, dispatch)
        .await;
}

fn synthesize_post_turn_failure_dispatch(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    turn_request: Option<&ActiveTurnRequest>,
    status: TurnPostTurnStatus,
    error: String,
) -> Option<AgentTurnPostTurnHookDispatch> {
    let Some(turn_request) = turn_request else {
        return None;
    };
    if turn_request.mode != ThreadMode::Agent {
        return None;
    }

    let summary = AgentTurnPostTurnSummary::failed(
        status,
        turn_request_user_text(turn_request.input.as_slice()),
        error,
    );
    Some(AgentTurnPostTurnHookDispatch::new(
        AgentTurnHookContext::with_runtime_context(
            workspace_id,
            thread_id,
            turn_id,
            turn_request.hook_runtime_context.clone(),
        ),
        EffectiveTurnPolicySet::empty(),
        EffectiveTurnPromptContextSet::empty(),
        summary,
    ))
}

fn turn_request_user_text(input: &[UserInput]) -> String {
    let mut parts = Vec::new();

    for item in input {
        if let UserInput::Text { text, .. } = item
            && !text.is_empty()
        {
            parts.push(text.clone());
        }
    }

    parts.join("\n")
}

async fn cleanup_attached_tasks(
    provider: Option<&Arc<dyn TaskToolProvider>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    reason: String,
) {
    let Some(provider) = provider else {
        return;
    };
    let _ = provider
        .cleanup_attached_tasks(
            TaskTurnContext {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
            },
            reason,
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionWindowTotalBudgetBlockKind, ExecutionWindowTotalBudgetDecision,
        TurnExecutionUsageCounters, decide_execution_window_total_budget,
    };

    fn total_budget(
        max_windows_per_turn: u32,
        max_tool_calls_per_turn: u32,
        max_wall_clock_ms_per_turn: Option<u64>,
        max_provider_tokens_per_turn: Option<u64>,
    ) -> pioneer_tools::ExecutionWindowTotalBudgetConfig {
        pioneer_tools::ExecutionWindowTotalBudgetConfig {
            max_windows_per_turn: Some(max_windows_per_turn),
            max_tool_calls_per_turn: Some(max_tool_calls_per_turn),
            max_wall_clock_ms_per_turn,
            max_provider_tokens_per_turn,
            max_consecutive_no_progress_windows: 3,
        }
    }

    fn checkpoint_payload(
        window_index: u32,
        agent_round_count: u32,
        succeeded_count: u32,
        failed_count: u32,
        exhaustion_reason: pioneer_protocol::ExecutionWindowExhaustionReason,
    ) -> pioneer_protocol::ExecutionCheckpointPayload {
        let executed_count = succeeded_count.saturating_add(failed_count);
        pioneer_protocol::build_execution_checkpoint_payload(
            "ws",
            "thr",
            "turn",
            pioneer_protocol::ExecutionCheckpointOriginalRequestSummary {
                input_count: 1,
                text_preview: Some("continue".to_owned()),
                text_truncated: false,
                attachment_count: 0,
                attachment_kinds: Vec::new(),
            },
            pioneer_protocol::ExecutionCheckpointWindowSummary {
                window_id: Some(format!("window_{window_index}")),
                window_index,
                started_at_unix_ms: Some(i64::from(window_index) * 1_000),
                completed_at_unix_ms: Some(i64::from(window_index) * 1_000 + 100),
                agent_round_count,
                tool_call_count: executed_count,
                provider_token_count: Some(10),
                exhaustion_reason: Some(exhaustion_reason),
            },
            pioneer_protocol::ExecutionCheckpointProviderBudgetSummary {
                model: Some("model".to_owned()),
                model_provider: Some("provider".to_owned()),
                agent_round_count,
                tool_call_count: executed_count,
                provider_token_count: Some(10),
                provider_usage_available: true,
                exhaustion_reason: Some(exhaustion_reason),
                exhausted_limit: Some(u64::from(executed_count)),
                exhausted_observed: Some(u64::from(executed_count)),
            },
            pioneer_protocol::ExecutionCheckpointToolSummary {
                requested_count: executed_count,
                executed_count,
                unexecuted_count: 0,
                total_count: executed_count,
                succeeded_count,
                failed_count,
                in_progress_count: 0,
                detail_limit: 0,
                details_truncated: false,
                details: Vec::new(),
            },
            Vec::new(),
        )
    }

    #[test]
    fn total_budget_decision_blocks_at_max_windows() {
        let usage = TurnExecutionUsageCounters {
            total_windows: 3,
            total_tool_calls: 2,
            total_wall_clock_ms: 100,
            total_provider_tokens: 0,
            provider_token_usage_unknown: false,
            consecutive_no_progress_windows: 0,
        };

        let decision =
            decide_execution_window_total_budget(&usage, &total_budget(3, 12, Some(10_000), None));

        let ExecutionWindowTotalBudgetDecision::Block(block) = decision else {
            panic!("total window budget should block continuation");
        };
        assert_eq!(block.kind, ExecutionWindowTotalBudgetBlockKind::MaxWindows);
        assert_eq!(block.total_windows, 3);
        assert!(block.reason.contains("limit=3, observed=3"));
    }

    #[test]
    fn total_budget_decision_blocks_on_total_tool_calls() {
        let usage = TurnExecutionUsageCounters {
            total_windows: 1,
            total_tool_calls: 12,
            total_wall_clock_ms: 100,
            total_provider_tokens: 0,
            provider_token_usage_unknown: false,
            consecutive_no_progress_windows: 0,
        };

        let decision =
            decide_execution_window_total_budget(&usage, &total_budget(4, 12, Some(10_000), None));

        let ExecutionWindowTotalBudgetDecision::Block(block) = decision else {
            panic!("total tool-call budget should block continuation");
        };
        assert_eq!(
            block.kind,
            ExecutionWindowTotalBudgetBlockKind::MaxToolCalls
        );
        assert_eq!(block.total_windows, 1);
        assert_eq!(block.total_tool_calls, 12);
        assert!(block.reason.contains("max_total_tool_calls_per_turn"));
    }

    #[test]
    fn total_budget_decision_blocks_on_total_wall_clock() {
        let usage = TurnExecutionUsageCounters {
            total_windows: 2,
            total_tool_calls: 4,
            total_wall_clock_ms: 5_000,
            total_provider_tokens: 0,
            provider_token_usage_unknown: false,
            consecutive_no_progress_windows: 0,
        };

        let decision =
            decide_execution_window_total_budget(&usage, &total_budget(4, 100, Some(5_000), None));

        let ExecutionWindowTotalBudgetDecision::Block(block) = decision else {
            panic!("total wall-clock budget should block continuation");
        };
        assert_eq!(
            block.kind,
            ExecutionWindowTotalBudgetBlockKind::MaxWallClockMs
        );
        assert_eq!(block.total_windows, 2);
        assert_eq!(block.total_tool_calls, 4);
        assert!(block.reason.contains("max_total_wall_clock_ms_per_turn"));
    }

    #[test]
    fn total_budget_decision_blocks_on_known_provider_tokens() {
        let usage = TurnExecutionUsageCounters {
            total_windows: 2,
            total_tool_calls: 4,
            total_wall_clock_ms: 500,
            total_provider_tokens: 10_000,
            provider_token_usage_unknown: false,
            consecutive_no_progress_windows: 0,
        };

        let decision = decide_execution_window_total_budget(
            &usage,
            &total_budget(4, 100, Some(50_000), Some(10_000)),
        );

        let ExecutionWindowTotalBudgetDecision::Block(block) = decision else {
            panic!("known total provider-token budget should block continuation");
        };
        assert_eq!(
            block.kind,
            ExecutionWindowTotalBudgetBlockKind::MaxProviderTokens
        );
        assert_eq!(block.total_windows, 2);
        assert_eq!(block.total_tool_calls, 4);
        assert!(block.reason.contains("max_total_provider_tokens_per_turn"));
    }

    #[test]
    fn total_budget_decision_does_not_block_on_unknown_provider_tokens() {
        let usage = TurnExecutionUsageCounters {
            total_windows: 1,
            total_tool_calls: 4,
            total_wall_clock_ms: 500,
            total_provider_tokens: 10_000,
            provider_token_usage_unknown: true,
            consecutive_no_progress_windows: 0,
        };

        assert_eq!(
            decide_execution_window_total_budget(
                &usage,
                &total_budget(4, 100, Some(50_000), Some(10))
            ),
            ExecutionWindowTotalBudgetDecision::Continue {
                next_window_index: 2
            }
        );
    }

    #[test]
    fn default_total_budget_allows_long_running_turns() {
        let usage = TurnExecutionUsageCounters {
            total_windows: 96,
            total_tool_calls: 12_000,
            total_wall_clock_ms: 259_200_000,
            total_provider_tokens: 2_000_000,
            provider_token_usage_unknown: false,
            consecutive_no_progress_windows: 0,
        };

        assert_eq!(
            decide_execution_window_total_budget(
                &usage,
                &pioneer_tools::ExecutionWindowTotalBudgetConfig::default()
            ),
            ExecutionWindowTotalBudgetDecision::Continue {
                next_window_index: 97
            }
        );
    }

    #[test]
    fn no_progress_accounting_increments_only_for_empty_recovery_windows() {
        let mut usage = TurnExecutionUsageCounters::default();

        usage.observe_checkpoint_payload(&checkpoint_payload(
            1,
            0,
            0,
            0,
            pioneer_protocol::ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
        ));

        assert_eq!(usage.total_windows, 1);
        assert_eq!(usage.consecutive_no_progress_windows, 1);
    }

    #[test]
    fn durable_progress_resets_no_progress_accounting_even_when_tools_fail() {
        let mut usage = TurnExecutionUsageCounters::default();

        usage.observe_checkpoint_payload(&checkpoint_payload(
            1,
            0,
            0,
            0,
            pioneer_protocol::ExecutionWindowExhaustionReason::ProviderFailureContinuation,
        ));
        usage.observe_checkpoint_payload(&checkpoint_payload(
            2,
            1,
            0,
            3,
            pioneer_protocol::ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
        ));

        assert_eq!(usage.total_windows, 2);
        assert_eq!(usage.consecutive_no_progress_windows, 0);
    }

    #[test]
    fn total_budget_decision_blocks_on_consecutive_no_progress_windows() {
        let usage = TurnExecutionUsageCounters {
            total_windows: 3,
            total_tool_calls: 0,
            total_wall_clock_ms: 300,
            total_provider_tokens: 0,
            provider_token_usage_unknown: true,
            consecutive_no_progress_windows: 3,
        };

        let mut budget = total_budget(8, 100, Some(10_000), None);
        budget.max_consecutive_no_progress_windows = 3;
        let decision = decide_execution_window_total_budget(&usage, &budget);

        let ExecutionWindowTotalBudgetDecision::Block(block) = decision else {
            panic!("consecutive no-progress budget should block continuation");
        };
        assert_eq!(
            block.kind,
            ExecutionWindowTotalBudgetBlockKind::MaxConsecutiveNoProgressWindows
        );
        assert_eq!(block.total_windows, 3);
        assert_eq!(block.total_tool_calls, 0);
        assert!(block.reason.contains("max_consecutive_no_progress_windows"));
    }
}
