use crate::{AgentEventHub, AgentEventHubError};
use pioneer_protocol::{
    AgentDurableEvent, ItemToolRetryExhaustedNotification, ItemToolRetryResolvedNotification,
    ItemToolRetryScheduledNotification, ToolLoopBudgetAction, ToolLoopBudgetLimitKind,
    ToolRetryBudgetKind, ToolRetryBudgetUsage, ToolRetryErrorClass, ToolRetryExhaustionKind,
    ToolRetryResolution, TurnItemType, TurnToolLoopBudgetExceededNotification,
};
use pioneer_tools::{ToolLoopBudgetExceeded, ToolLoopBudgetReason, ToolRetryEventDraft};

#[derive(Debug, Default)]
pub(super) struct ToolRetryLifecycleTracker {
    active_episode_id: Option<String>,
    next_episode_index: u32,
}

impl ToolRetryLifecycleTracker {
    fn episode_id_for_scheduled(&mut self, turn_id: &str) -> String {
        if let Some(id) = self.active_episode_id.clone() {
            return id;
        }

        self.next_episode_index = self.next_episode_index.saturating_add(1);
        let id = format!("tool_retry_{turn_id}_{}", self.next_episode_index);
        self.active_episode_id = Some(id.clone());
        id
    }

    fn active_episode_id(&self) -> Option<String> {
        self.active_episode_id.clone()
    }

    fn close(&mut self) {
        self.active_episode_id = None;
    }
}

pub(super) fn turn_item_type_code(item_type: TurnItemType) -> &'static str {
    match item_type {
        TurnItemType::UserMessage => "user_message",
        TurnItemType::AgentMessage => "agent_message",
        TurnItemType::Reasoning => "reasoning",
        TurnItemType::SystemEvent => "system_event",
        TurnItemType::Task => "task",
        TurnItemType::CommandExecution => "command_execution",
        TurnItemType::FileChange => "file_change",
        TurnItemType::WebSearch => "web_search",
        TurnItemType::WebFetch => "web_fetch",
        TurnItemType::Download => "download",
        TurnItemType::DynamicToolCall => "dynamic_tool_call",
    }
}

fn turn_item_type_from_code(code: &str) -> TurnItemType {
    match code {
        "user_message" => TurnItemType::UserMessage,
        "agent_message" => TurnItemType::AgentMessage,
        "reasoning" => TurnItemType::Reasoning,
        "system_event" => TurnItemType::SystemEvent,
        "task" => TurnItemType::Task,
        "command_execution" => TurnItemType::CommandExecution,
        "file_change" => TurnItemType::FileChange,
        "web_search" => TurnItemType::WebSearch,
        "web_fetch" => TurnItemType::WebFetch,
        "download" => TurnItemType::Download,
        "dynamic_tool_call" => TurnItemType::DynamicToolCall,
        _ => TurnItemType::DynamicToolCall,
    }
}

fn protocol_retry_error_class(error_class: pioneer_tools::ToolErrorClass) -> ToolRetryErrorClass {
    match error_class {
        pioneer_tools::ToolErrorClass::InvalidArguments => ToolRetryErrorClass::InvalidArguments,
        pioneer_tools::ToolErrorClass::NotFound => ToolRetryErrorClass::NotFound,
        pioneer_tools::ToolErrorClass::ToolNotVisible => ToolRetryErrorClass::ToolNotVisible,
        pioneer_tools::ToolErrorClass::PermissionDenied => ToolRetryErrorClass::PermissionDenied,
        pioneer_tools::ToolErrorClass::CommandNotFound => ToolRetryErrorClass::CommandNotFound,
        pioneer_tools::ToolErrorClass::Timeout => ToolRetryErrorClass::Timeout,
        pioneer_tools::ToolErrorClass::Cancelled => ToolRetryErrorClass::Cancelled,
        pioneer_tools::ToolErrorClass::ExecutionFailed => ToolRetryErrorClass::ExecutionFailed,
        pioneer_tools::ToolErrorClass::NeedsNarrowing => ToolRetryErrorClass::NeedsNarrowing,
        pioneer_tools::ToolErrorClass::Internal => ToolRetryErrorClass::Internal,
        pioneer_tools::ToolErrorClass::OutputTruncated => ToolRetryErrorClass::OutputTruncated,
        pioneer_tools::ToolErrorClass::Unknown => ToolRetryErrorClass::Unknown,
    }
}

fn protocol_retry_budget_kind(kind: pioneer_tools::ToolRetryBudgetKind) -> ToolRetryBudgetKind {
    match kind {
        pioneer_tools::ToolRetryBudgetKind::Episode => ToolRetryBudgetKind::Episode,
        pioneer_tools::ToolRetryBudgetKind::ErrorClass => ToolRetryBudgetKind::ErrorClass,
        pioneer_tools::ToolRetryBudgetKind::ToolName => ToolRetryBudgetKind::ToolName,
        pioneer_tools::ToolRetryBudgetKind::FailureSignature => {
            ToolRetryBudgetKind::FailureSignature
        }
    }
}

fn protocol_retry_budget_usage(
    usage: &pioneer_tools::ToolRetryBudgetUsage,
) -> ToolRetryBudgetUsage {
    ToolRetryBudgetUsage {
        kind: protocol_retry_budget_kind(usage.kind),
        used: usage.used,
        limit: usage.limit,
    }
}

fn protocol_retry_resolution(
    resolution: pioneer_tools::ToolRetryResolution,
) -> ToolRetryResolution {
    match resolution {
        pioneer_tools::ToolRetryResolution::Succeeded => ToolRetryResolution::Succeeded,
        pioneer_tools::ToolRetryResolution::NonRetryable => ToolRetryResolution::NonRetryable,
    }
}

fn protocol_retry_exhaustion_kind(
    kind: pioneer_tools::ToolRetryBudgetKind,
) -> ToolRetryExhaustionKind {
    match kind {
        pioneer_tools::ToolRetryBudgetKind::Episode => ToolRetryExhaustionKind::TotalRetryRounds,
        pioneer_tools::ToolRetryBudgetKind::ErrorClass => ToolRetryExhaustionKind::ErrorClass,
        pioneer_tools::ToolRetryBudgetKind::ToolName => ToolRetryExhaustionKind::ToolName,
        pioneer_tools::ToolRetryBudgetKind::FailureSignature => {
            ToolRetryExhaustionKind::FailureSignature
        }
    }
}

fn protocol_loop_limit_kind(reason: ToolLoopBudgetReason) -> ToolLoopBudgetLimitKind {
    match reason {
        ToolLoopBudgetReason::AgentRoundsExceeded => ToolLoopBudgetLimitKind::AgentRounds,
        ToolLoopBudgetReason::ToolCallsExceeded => ToolLoopBudgetLimitKind::ToolCalls,
        ToolLoopBudgetReason::ProviderReturnedToolsAfterToolsDisabled => {
            ToolLoopBudgetLimitKind::ProviderReturnedToolsAfterToolsDisabled
        }
    }
}

fn protocol_loop_action(action: pioneer_tools::ToolLoopBudgetAction) -> ToolLoopBudgetAction {
    match action {
        pioneer_tools::ToolLoopBudgetAction::ContinueInNextWindow => {
            ToolLoopBudgetAction::ContinueInNextWindow
        }
    }
}

pub(super) async fn emit_tool_loop_budget_exceeded(
    budget_exceeded: &ToolLoopBudgetExceeded,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    event_tx: &AgentEventHub,
) -> Result<(), AgentEventHubError> {
    event_tx
        .publish_durable(AgentDurableEvent::TurnToolLoopBudgetExceeded {
            notification: TurnToolLoopBudgetExceededNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                limit_kind: protocol_loop_limit_kind(budget_exceeded.reason),
                limit: budget_exceeded.limit,
                observed: budget_exceeded.observed,
                action: protocol_loop_action(budget_exceeded.action),
                reason: budget_exceeded.reason.code().to_owned(),
            },
        })
        .await
}

pub(super) async fn emit_tool_retry_drafts(
    drafts: &[ToolRetryEventDraft],
    lifecycle: &mut ToolRetryLifecycleTracker,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    event_tx: &AgentEventHub,
) -> Result<(), AgentEventHubError> {
    for draft in drafts {
        match draft {
            ToolRetryEventDraft::Scheduled { entries } => {
                let episode_id = lifecycle.episode_id_for_scheduled(turn_id);
                for entry in entries {
                    event_tx
                        .publish_durable(AgentDurableEvent::ItemToolRetryScheduled {
                            notification: ItemToolRetryScheduledNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item_id: entry.item_id.clone(),
                                item_type: turn_item_type_from_code(entry.item_type.as_str()),
                                tool_retry_episode_id: episode_id.clone(),
                                tool_name: entry.tool_name.clone(),
                                attempt_number: entry.attempt_number,
                                error_class: protocol_retry_error_class(entry.error_class),
                                retry_hint: entry.retry_hint.clone(),
                                budgets: entry
                                    .budget_usages()
                                    .iter()
                                    .map(protocol_retry_budget_usage)
                                    .collect(),
                                failure_signature_fingerprint: entry
                                    .failure_signature_fingerprint
                                    .clone(),
                                reason: "recoverable_tool_output".to_owned(),
                            },
                        })
                        .await?;
                }
            }
            ToolRetryEventDraft::Resolved { entries, .. } => {
                let Some(episode_id) = lifecycle.active_episode_id() else {
                    continue;
                };
                for entry in entries {
                    event_tx
                        .publish_durable(AgentDurableEvent::ItemToolRetryResolved {
                            notification: ItemToolRetryResolvedNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item_id: entry.item_id.clone(),
                                item_type: turn_item_type_from_code(entry.item_type.as_str()),
                                tool_retry_episode_id: episode_id.clone(),
                                tool_name: entry.tool_name.clone(),
                                attempt_number: entry.attempt_number,
                                resolution: protocol_retry_resolution(entry.resolution),
                                budgets: entry
                                    .budgets
                                    .iter()
                                    .map(protocol_retry_budget_usage)
                                    .collect(),
                                reason: entry.reason.clone(),
                            },
                        })
                        .await?;
                }
                lifecycle.close();
            }
            ToolRetryEventDraft::Exhausted { entries, reason } => {
                let episode_id = lifecycle.episode_id_for_scheduled(turn_id);
                for entry in entries {
                    event_tx
                        .publish_durable(AgentDurableEvent::ItemToolRetryExhausted {
                            notification: ItemToolRetryExhaustedNotification {
                                workspace_id: workspace_id.to_owned(),
                                thread_id: thread_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                item_id: entry.item_id.clone(),
                                item_type: turn_item_type_from_code(entry.item_type.as_str()),
                                tool_retry_episode_id: episode_id.clone(),
                                tool_name: entry.tool_name.clone(),
                                attempt_number: entry.attempt_number,
                                error_class: protocol_retry_error_class(entry.error_class),
                                exhaustion_kind: protocol_retry_exhaustion_kind(reason.kind()),
                                budgets: entry
                                    .budget_usages()
                                    .iter()
                                    .map(protocol_retry_budget_usage)
                                    .collect(),
                                failure_signature_fingerprint: entry
                                    .failure_signature_fingerprint
                                    .clone(),
                                reason: reason.fact_value(),
                            },
                        })
                        .await?;
                }
                lifecycle.close();
            }
        }
    }
    Ok(())
}
