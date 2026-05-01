use super::{
    ActiveTurnRequest, AgentCommand, AgentEventHub, AgentMcpToolProvider, AgentStartError,
    TaskToolProvider, TaskTurnContext, ToolLoopConfig, TurnExecutionControl, TurnTaskFailure,
};
use crate::chat;
use pioneer_protocol::{AgentDurableEvent, RecoveryAttemptContext, ThreadMode, UserInput};
use pioneer_provider::{ChatMessage, Provider, ProviderRegistry};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::error;

pub(super) async fn run_agent_loop(
    thread_id: String,
    workspace_id: String,
    provider_registry: Arc<ProviderRegistry>,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
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
                result,
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
                        publish_loop_durable_event(
                            event_hub.as_ref(),
                            AgentDurableEvent::TurnFailed {
                                thread_id: thread_id.clone(),
                                turn_id,
                                error,
                                recovery,
                            },
                        )
                        .await;
                    }
                    Err(TurnTaskFailure::ProviderFailure {
                        item_id,
                        item_type,
                        failure,
                    }) => {
                        last_turn_request = turn_request_snapshot;
                        active_turn_id = None;
                        active_turn_request = None;
                        publish_loop_durable_event(
                            event_hub.as_ref(),
                            AgentDurableEvent::ProviderFailureDetected {
                                thread_id: thread_id.clone(),
                                turn_id,
                                item_id,
                                item_type,
                                failure,
                                recovery,
                            },
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

                if let Some(task) = active_turn_task.take() {
                    task.abort();
                }
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
                publish_loop_durable_event(
                    event_hub.as_ref(),
                    AgentDurableEvent::TurnFailed {
                        thread_id: thread_id.clone(),
                        turn_id,
                        error: reason,
                        recovery,
                    },
                )
                .await;
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
    provider: Arc<dyn Provider>,
    turn_request: ActiveTurnRequest,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    run_id: u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = execute_turn_flow(
            thread_id,
            turn_request.turn_id.clone(),
            workspace_id,
            turn_request.mode,
            provider,
            turn_request.model,
            turn_request.workspace_skill_policies,
            turn_request.input,
            turn_request.history,
            turn_request.retained_llm_context,
            turn_request.execution_options.force_non_stream,
            turn_request.execution_options.continue_generation_hint,
            tool_loop_config,
            mcp_tool_provider,
            task_tool_provider,
            turn_control,
            recovery,
            event_hub,
        )
        .await;

        let _ = command_tx
            .send(AgentCommand::TurnTaskFinished {
                turn_id: turn_request.turn_id,
                run_id,
                result,
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
    history: Vec<ChatMessage>,
    retained_llm_context: Vec<super::RetainedToolLlmContext>,
    force_non_stream: bool,
    continue_generation_hint: bool,
    tool_loop_config: ToolLoopConfig,
    mcp_tool_provider: Option<Arc<dyn AgentMcpToolProvider>>,
    task_tool_provider: Option<Arc<dyn TaskToolProvider>>,
    turn_control: TurnExecutionControl,
    recovery: Option<RecoveryAttemptContext>,
    event_hub: Arc<AgentEventHub>,
) -> Result<(), TurnTaskFailure> {
    match mode {
        ThreadMode::Chat | ThreadMode::Agent => chat::execute_chat_turn_flow(
            thread_id,
            turn_id,
            workspace_id,
            mode,
            provider,
            model,
            workspace_skill_policies,
            input,
            history,
            retained_llm_context,
            force_non_stream,
            continue_generation_hint,
            tool_loop_config,
            mcp_tool_provider,
            task_tool_provider,
            turn_control,
            recovery,
            event_hub,
        )
        .await
        .map_err(|error| match error {
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
        }),
    }
}

async fn publish_loop_durable_event(event_hub: &AgentEventHub, event: AgentDurableEvent) {
    if let Err(error) = event_hub.publish_durable(event).await {
        error!(error = %error, "failed to publish durable agent loop event");
    }
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
