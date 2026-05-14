use super::{
    ActiveTurnRequest, AgentCommand, AgentEventHub, AgentMcpToolProvider, AgentStartError,
    TaskToolProvider, TaskTurnContext, ToolLoopConfig, TurnExecutionControl, TurnTaskCompletion,
    TurnTaskFailure,
};
use crate::chat;
use crate::hooks::{
    AgentPostTurnHookDispatchPolicy, AgentToolBundleArtifactStore, AgentTurnHookContext,
    AgentTurnPostTurnHookDispatch, AgentTurnPostTurnSummary, EffectiveTurnPolicySet,
    EffectiveTurnPromptContextSet, run_agent_turn_post_turn_hook_phase,
};
use pioneer_hooks::{HookRuntime, TurnPostTurnStatus};
use pioneer_protocol::{AgentDurableEvent, RecoveryAttemptContext, ThreadMode, UserInput};
use pioneer_provider::{ChatMessage, Provider, ProviderRegistry};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::error;

const TURN_CANCEL_GRACE_MS: u64 = 750;

type TurnFlowFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    post_turn_hook_dispatch_policy: AgentPostTurnHookDispatchPolicy,
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
    let mut next_turn_run_id: u64 = 1;

    while let Some(command) = command_rx.recv().await {
        match command {
            AgentCommand::StartTurn {
                turn_id,
                mode,
                model,
                provider_name,
                workspace_skill_policies,
                input,
                resolved_artifacts,
                history,
                ack,
            } => {
                if active_turn_id.is_some() {
                    let _ = ack.send(Err(AgentStartError::TurnAlreadyRunning));
                    continue;
                }

                let turn_request = ActiveTurnRequest {
                    turn_id: turn_id.clone(),
                    mode,
                    model,
                    provider_name: provider_name.clone(),
                    workspace_skill_policies,
                    input,
                    resolved_artifacts,
                    history,
                    retained_llm_context: Vec::new(),
                    execution_options: super::TurnExecutionOptions::default(),
                };

                let provider = match provider_registry.get_or_create(&provider_name) {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = ack.send(Err(AgentStartError::Internal(format!(
                            "failed to create provider `{provider_name}`: {e}"
                        ))));
                        continue;
                    }
                };

                active_turn_id = Some(turn_id.clone());
                active_turn_request = Some(turn_request.clone());
                last_turn_request = Some(turn_request.clone());
                active_recovery = None;
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
                    tool_loop_config.clone(),
                    mcp_tool_provider.clone(),
                    task_tool_provider.clone(),
                    hook_runtime.clone(),
                    tool_bundle_artifacts.clone(),
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
                active_recovery = None;

                let TurnTaskCompletion {
                    result,
                    post_turn_dispatch,
                } = completion;

                match result {
                    Ok(()) => {
                        active_turn_id = None;
                        active_turn_request = None;
                        publish_loop_durable_event(
                            event_hub.as_ref(),
                            AgentDurableEvent::TurnCompleted {
                                thread_id: thread_id.clone(),
                                turn_id,
                                recovery,
                            },
                        )
                        .await;
                        maybe_spawn_post_turn_hook_dispatch(
                            hook_runtime.clone(),
                            post_turn_hook_dispatch_policy,
                            post_turn_dispatch,
                        );
                    }
                    Err(TurnTaskFailure::Terminal(error)) => {
                        active_turn_id = None;
                        active_turn_request = None;
                        cleanup_attached_tasks(
                            task_tool_provider.as_ref(),
                            workspace_id.as_str(),
                            thread_id.as_str(),
                            turn_id.as_str(),
                            format!("parent turn failed: {error}"),
                        )
                        .await;
                        let error_for_dispatch = error.clone();
                        publish_loop_durable_event(
                            event_hub.as_ref(),
                            AgentDurableEvent::TurnFailed {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                error,
                                recovery,
                            },
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
                        maybe_spawn_post_turn_hook_dispatch(
                            hook_runtime.clone(),
                            post_turn_hook_dispatch_policy,
                            failure_dispatch,
                        );
                    }
                    Err(TurnTaskFailure::ProviderFailure {
                        item_id,
                        item_type,
                        failure,
                    }) => {
                        last_turn_request = turn_request_snapshot.clone();
                        active_turn_id = None;
                        active_turn_request = None;
                        let failure_message = failure.message.clone().unwrap_or_else(|| {
                            format!(
                                "provider failure: {:?} during {:?}",
                                failure.class, failure.stage
                            )
                        });
                        publish_loop_durable_event(
                            event_hub.as_ref(),
                            AgentDurableEvent::ProviderFailureDetected {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                item_id,
                                item_type,
                                failure,
                                recovery,
                            },
                        )
                        .await;
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
                        maybe_spawn_post_turn_hook_dispatch(
                            hook_runtime.clone(),
                            post_turn_hook_dispatch_policy,
                            failure_dispatch,
                        );
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

                active_recovery = None;
                publish_loop_durable_event(
                    event_hub.as_ref(),
                    AgentDurableEvent::RecoveryAttemptSucceeded {
                        thread_id: thread_id.clone(),
                        turn_id,
                        recovery,
                    },
                )
                .await;
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
                        let _ = task.await;
                    } else {
                        task.abort();
                        let _ = task.await;
                    }
                }
                let turn_request_snapshot = active_turn_request.clone();
                let recovery = active_recovery.take();
                active_turn_id = None;
                active_turn_control = None;
                active_turn_request = None;
                active_turn_run_id = None;

                let _ = ack.send(Ok(()));
                cleanup_attached_tasks(
                    task_tool_provider.as_ref(),
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    turn_id.as_str(),
                    format!("parent turn cancelled: {reason}"),
                )
                .await;
                let reason_for_dispatch = reason.clone();
                publish_loop_durable_event(
                    event_hub.as_ref(),
                    AgentDurableEvent::TurnInterrupted {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        reason,
                        recovery,
                    },
                )
                .await;
                let interrupted_dispatch = synthesize_post_turn_failure_dispatch(
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    turn_id.as_str(),
                    turn_request_snapshot.as_ref(),
                    TurnPostTurnStatus::Interrupted,
                    reason_for_dispatch,
                );
                maybe_spawn_post_turn_hook_dispatch(
                    hook_runtime.clone(),
                    post_turn_hook_dispatch_policy,
                    interrupted_dispatch,
                );
            }
            AgentCommand::StartRecoveryAttempt { request, ack } => {
                if active_turn_id.is_none()
                    && !request.item_type.is_tool_item()
                    && let Some(turn_request) = last_turn_request.clone()
                    && turn_request.turn_id == request.turn_id
                {
                    let mut turn_request = turn_request;

                    super::apply_recovery_adjustments(&mut turn_request, &request);

                    if request.refresh_provider_auth {
                        provider_registry.invalidate(turn_request.provider_name.as_str());
                    }

                    let provider = match provider_registry
                        .get_or_create(turn_request.provider_name.as_str())
                    {
                        Ok(provider) => provider,
                        Err(error) => {
                            let _ = ack.send(Err(super::AgentControlError::Internal(format!(
                                "failed to recreate provider `{}` for recovery: {error}",
                                turn_request.provider_name
                            ))));
                            continue;
                        }
                    };

                    let run_id = next_turn_run_id;
                    next_turn_run_id = next_turn_run_id.saturating_add(1);

                    active_turn_run_id = Some(run_id);
                    active_turn_id = Some(request.turn_id.clone());

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
                        tool_loop_config.clone(),
                        mcp_tool_provider.clone(),
                        task_tool_provider.clone(),
                        hook_runtime.clone(),
                        tool_bundle_artifacts.clone(),
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

                if let Some(task) = active_turn_task.take() {
                    task.abort();
                }

                if request.refresh_provider_auth {
                    provider_registry.invalidate(turn_request.provider_name.as_str());
                }

                let provider =
                    match provider_registry.get_or_create(turn_request.provider_name.as_str()) {
                        Ok(provider) => provider,
                        Err(error) => {
                            let _ = ack.send(Err(super::AgentControlError::Internal(format!(
                                "failed to recreate provider `{}` for recovery: {error}",
                                turn_request.provider_name
                            ))));
                            continue;
                        }
                    };

                let run_id = next_turn_run_id;
                next_turn_run_id = next_turn_run_id.saturating_add(1);
                active_turn_run_id = Some(run_id);
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
                    tool_loop_config.clone(),
                    mcp_tool_provider.clone(),
                    task_tool_provider.clone(),
                    hook_runtime.clone(),
                    tool_bundle_artifacts.clone(),
                    provider,
                    turn_request,
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
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    provider: Arc<dyn Provider>,
    turn_request: ActiveTurnRequest,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    run_id: u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = turn_flow_future(execute_turn_flow(
            thread_id,
            turn_request.turn_id.clone(),
            workspace_id,
            turn_request.mode,
            provider,
            turn_request.model,
            turn_request.workspace_skill_policies,
            turn_request.input,
            turn_request.resolved_artifacts,
            turn_request.history,
            turn_request.retained_llm_context,
            turn_request.execution_options.force_non_stream,
            turn_request.execution_options.continue_generation_hint,
            tool_loop_config,
            mcp_tool_provider,
            task_tool_provider,
            hook_runtime,
            tool_bundle_artifacts,
            turn_control,
            recovery,
            event_hub,
        ))
        .await;

        let _ = command_tx
            .send(AgentCommand::TurnTaskFinished {
                turn_id: turn_request.turn_id,
                run_id,
                completion: result,
            })
            .await;
    })
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
    provider: Arc<dyn Provider>,
    model: String,
    workspace_skill_policies: std::collections::HashMap<
        pioneer_skills::SkillPolicyKey,
        super::WorkspaceSkillPolicy,
    >,
    input: Vec<UserInput>,
    resolved_artifacts: Vec<super::ResolvedArtifactInput>,
    history: Vec<ChatMessage>,
    retained_llm_context: Vec<super::RetainedToolLlmContext>,
    force_non_stream: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    hook_runtime: Option<Arc<HookRuntime>>,
    tool_bundle_artifacts: Option<Arc<AgentToolBundleArtifactStore>>,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    event_hub: Arc<AgentEventHub>,
) -> TurnTaskCompletion {
    match mode {
        ThreadMode::Chat | ThreadMode::Agent => turn_flow_future(chat::execute_chat_turn_flow(
            thread_id,
            turn_id,
            workspace_id,
            mode,
            provider,
            model,
            workspace_skill_policies,
            input,
            resolved_artifacts,
            history,
            retained_llm_context,
            force_non_stream,
            continue_generation_hint,
            tool_loop_config,
            mcp_tool_provider,
            task_tool_provider,
            hook_runtime,
            tool_bundle_artifacts,
            turn_control,
            recovery,
            event_hub,
        ))
        .await
        .map(|outcome| TurnTaskCompletion {
            result: Ok(()),
            post_turn_dispatch: outcome.post_turn_dispatch,
        })
        .unwrap_or_else(|error| {
            let (error, post_turn_dispatch) = error.into_parts();
            TurnTaskCompletion {
                result: Err(match error {
                    chat::ChatTurnError::Terminal(message) => TurnTaskFailure::Terminal(message),
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

async fn publish_loop_durable_event(event_hub: &AgentEventHub, event: AgentDurableEvent) {
    if let Err(error) = event_hub.publish_durable(event).await {
        error!(error = %error, "failed to publish durable agent loop event");
    }
}

fn maybe_spawn_post_turn_hook_dispatch(
    hook_runtime: Option<Arc<HookRuntime>>,
    policy: AgentPostTurnHookDispatchPolicy,
    dispatch: Option<AgentTurnPostTurnHookDispatch>,
) {
    let Some(dispatch) = dispatch else {
        return;
    };
    if !policy.should_dispatch(dispatch.status()) {
        return;
    }

    tokio::spawn(async move {
        run_agent_turn_post_turn_hook_phase(hook_runtime.as_ref(), dispatch).await;
    });
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
        AgentTurnHookContext::new(workspace_id, thread_id, turn_id),
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
