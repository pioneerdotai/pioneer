use super::PendingToolUiState;
use crate::{AgentEventHub, AgentEventHubError};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemCompletedNotification, ItemDeltaNotification,
    ItemDeltaStream, ItemStartedNotification, LlmRetentionPolicy, StorageOutputPolicy,
    TimelineOutputPolicy, ToolCallStatus, ToolDisplayPayload, ToolMetadata, ToolMetadataValue,
    ToolObservation, ToolOutputPolicySnapshot, ToolOutputSummary, ToolRecoveryPolicySnapshot,
    ToolRecoveryView, ToolStoragePayload, TurnItem, TurnItemExecutionClass, TurnItemType,
};
use pioneer_tools::{ToolDeltaPayload, ToolEvent, ToolEventPayload, ToolOutcome, ToolResultView};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(super) async fn forward_tool_event_to_agent(
    event: ToolEvent,
    event_tx: &AgentEventHub,
    pending_tool_ui: Arc<Mutex<HashMap<String, PendingToolUiState>>>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<(), AgentEventHubError> {
    let observation = Some(protocol_observation(event.observation.clone()));
    match event.payload {
        ToolEventPayload::CallStarted(started) => {
            let (
                tool_name,
                arguments,
                recovery_policy,
                turn_item_execution_class,
                output_policy,
                should_emit_started,
                latest_observation,
            ) = {
                let mut pending = pending_tool_ui.lock().await;
                let state = pending.entry(event.call_id.clone()).or_default();
                if state.tool_name.is_empty() {
                    state.tool_name = event.tool_name.clone();
                }
                if state.arguments.is_empty()
                    && let Some(arguments) = started.arguments.clone()
                {
                    state.arguments =
                        serde_json::to_string(&arguments).unwrap_or_else(|_| "{}".to_owned());
                }
                if state.recovery_policy.is_none() {
                    state.recovery_policy = started.recovery_policy.clone();
                }
                state.turn_item_execution_class = started.turn_item_execution_class;
                state.output_policy = Some(started.output_policy.clone());
                state.latest_observation = observation.clone();
                let should_emit_started = if state.started_sent {
                    false
                } else {
                    state.started_sent = true;
                    true
                };
                let arguments = if state.arguments.is_empty() {
                    "{}".to_owned()
                } else {
                    state.arguments.clone()
                };
                (
                    state.tool_name.clone(),
                    parse_arguments_json(arguments.as_str()),
                    state.recovery_policy.clone(),
                    state.turn_item_execution_class,
                    started.output_policy,
                    should_emit_started,
                    state.latest_observation.clone(),
                )
            };

            if should_emit_started {
                event_tx
                    .publish_durable_and_wait(AgentDurableEvent::ItemStarted {
                        notification: ItemStartedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: build_tool_turn_item(ProjectedToolItemInput::started(
                                event.call_id,
                                tool_name,
                                arguments,
                                recovery_policy,
                                turn_item_execution_class,
                                output_policy,
                                latest_observation,
                            )),
                        },
                    })
                    .await?;
            }
        }
        ToolEventPayload::Heartbeat => {
            let should_forward = {
                let pending = pending_tool_ui.lock().await;
                pending
                    .get(event.call_id.as_str())
                    .is_some_and(|state| state.started_sent)
            };
            if should_forward {
                event_tx.publish_confirmed_activity(
                    workspace_id.to_owned(),
                    thread_id.to_owned(),
                    turn_id.to_owned(),
                    event.call_id,
                    tool_item_type_from_name(event.tool_name.as_str()),
                );
            }
        }
        ToolEventPayload::PermissionAudit(audit_event) => {
            event_tx
                .publish_durable_and_wait(AgentDurableEvent::TurnPermissionAudit {
                    event: audit_event,
                })
                .await?;
        }
        ToolEventPayload::OutputDelta(delta_event) => {
            let should_forward = {
                let mut pending = pending_tool_ui.lock().await;
                let Some(state) = pending.get_mut(event.call_id.as_str()) else {
                    // Progress is deliberately lossy. It must never synthesize
                    // durable lifecycle state when CallStarted has not yet
                    // crossed the acknowledged lane or when CallCompleted has
                    // already removed that state.
                    return Ok(());
                };
                state.latest_observation = observation.clone();
                state.started_sent
            };
            if !should_forward {
                return Ok(());
            }

            if let Some((delta, stream, payload)) =
                protocol_delta_from_tool_delta(delta_event.delta)
                && !delta.is_empty()
            {
                event_tx.publish_progress(AgentProgressEvent::ItemDelta {
                    notification: ItemDeltaNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item_id: event.call_id,
                        delta,
                        stream: Some(stream),
                        payload,
                        markdown: None,
                        markdown_version: None,
                    },
                });
            }
        }
        ToolEventPayload::CallCompleted(completed) => {
            let (
                tool_name,
                arguments,
                recovery_policy,
                turn_item_execution_class,
                should_emit_started,
                state_observation,
            ) = {
                let mut pending = pending_tool_ui.lock().await;
                let mut state = pending.remove(event.call_id.as_str()).unwrap_or_default();
                if state.tool_name.is_empty() {
                    state.tool_name = event.tool_name.clone();
                }
                state.latest_observation = observation.clone().or(state.latest_observation.clone());
                let should_emit_started = !state.started_sent;
                let arguments = if state.arguments.is_empty() {
                    "{}".to_owned()
                } else {
                    state.arguments
                };
                (
                    state.tool_name,
                    parse_arguments_json(arguments.as_str()),
                    state.recovery_policy,
                    state.turn_item_execution_class,
                    should_emit_started,
                    state.latest_observation,
                )
            };

            if should_emit_started {
                event_tx
                    .publish_durable_and_wait(AgentDurableEvent::ItemStarted {
                        notification: ItemStartedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: build_tool_turn_item(ProjectedToolItemInput::started(
                                event.call_id.clone(),
                                tool_name.clone(),
                                arguments.clone(),
                                recovery_policy.clone(),
                                turn_item_execution_class,
                                completed.output_policy.clone(),
                                state_observation.clone(),
                            )),
                        },
                    })
                    .await?;
            }

            let outcome = protocol_outcome_from_tool_outcome(&completed.outcome);
            let storage = storage_with_native_patch_history(
                event.tool_name.as_str(),
                &completed.llm_view,
                completed.storage,
            );

            event_tx
                .publish_durable_and_wait(AgentDurableEvent::ItemCompleted {
                    notification: ItemCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item: build_tool_turn_item(ProjectedToolItemInput {
                            id: event.call_id,
                            tool_name,
                            arguments,
                            status: ToolCallStatus::Completed,
                            success: Some(completed.success),
                            outcome: Some(outcome),
                            output_policy: completed.output_policy,
                            recovery_policy,
                            turn_item_execution_class,
                            display: completed.display,
                            storage,
                            recovery: completed.recovery,
                            observation: state_observation,
                        }),
                    },
                })
                .await?;
        }
        ToolEventPayload::CallFailed(failed) => {
            let (
                tool_name,
                arguments,
                recovery_policy,
                turn_item_execution_class,
                should_emit_started,
                state_observation,
            ) = {
                let mut pending = pending_tool_ui.lock().await;
                let mut state = pending.remove(event.call_id.as_str()).unwrap_or_default();
                if state.tool_name.is_empty() {
                    state.tool_name = event.tool_name.clone();
                }
                state.latest_observation = observation.clone().or(state.latest_observation.clone());
                let should_emit_started = !state.started_sent;
                let arguments = if state.arguments.is_empty() {
                    "{}".to_owned()
                } else {
                    state.arguments
                };
                (
                    state.tool_name,
                    parse_arguments_json(arguments.as_str()),
                    state.recovery_policy,
                    state.turn_item_execution_class,
                    should_emit_started,
                    state.latest_observation,
                )
            };

            if should_emit_started {
                event_tx
                    .publish_durable_and_wait(AgentDurableEvent::ItemStarted {
                        notification: ItemStartedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: build_tool_turn_item(ProjectedToolItemInput::started(
                                event.call_id.clone(),
                                tool_name.clone(),
                                arguments.clone(),
                                recovery_policy.clone(),
                                turn_item_execution_class,
                                failed.output_policy.clone(),
                                state_observation.clone(),
                            )),
                        },
                    })
                    .await?;
            }

            let outcome = protocol_outcome_from_tool_outcome(&failed.outcome);

            event_tx
                .publish_durable_and_wait(AgentDurableEvent::ItemCompleted {
                    notification: ItemCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item: build_tool_turn_item(ProjectedToolItemInput {
                            id: event.call_id,
                            tool_name,
                            arguments,
                            status: ToolCallStatus::Failed,
                            success: Some(false),
                            outcome: Some(outcome),
                            output_policy: failed.output_policy,
                            recovery_policy,
                            turn_item_execution_class,
                            display: failed.display,
                            storage: failed.storage,
                            recovery: failed.recovery,
                            observation: state_observation,
                        }),
                    },
                })
                .await?;
        }
    }
    Ok(())
}

/// The native patch executor includes a typed, trusted history envelope in
/// the structured tool result. Keep that envelope on the durable file-change
/// item even when the normal tool output policy is metadata-only; the gateway
/// consumes it to append the immutable record and then projects the aggregate.
fn storage_with_native_patch_history(
    tool_name: &str,
    llm_view: &ToolResultView,
    storage: ToolStoragePayload,
) -> ToolStoragePayload {
    if tool_name != "apply_patch" {
        return storage;
    }
    let history_from_storage = match &storage {
        ToolStoragePayload::Metadata { metadata } => {
            metadata.to_json().get("patchHistory").cloned()
        }
        ToolStoragePayload::Summary(summary) => {
            summary.metadata.to_json().get("patchHistory").cloned()
        }
        ToolStoragePayload::Shell { .. } | ToolStoragePayload::None => None,
    };
    let history = history_from_storage.or_else(|| match llm_view {
        ToolResultView::Json { value, .. } => value.get("history").cloned(),
        ToolResultView::Text { text, .. } => serde_json::from_str::<JsonValue>(text)
            .ok()
            .and_then(|value| value.get("history").cloned()),
        ToolResultView::Empty => None,
    });
    let Some(history) = history else {
        return storage;
    };

    let mut metadata = match storage {
        ToolStoragePayload::Summary(summary) => summary.metadata,
        ToolStoragePayload::Metadata { metadata } => metadata,
        ToolStoragePayload::Shell { .. } | ToolStoragePayload::None => ToolMetadata::empty(),
    };
    metadata.insert("patchHistory", ToolMetadataValue::from_json(history));
    ToolStoragePayload::Metadata { metadata }
}

fn protocol_observation(
    observation: pioneer_tools::ObservationContext,
) -> pioneer_protocol::ToolObservation {
    pioneer_protocol::ToolObservation {
        trace_id: observation.trace_id,
        turn_id: observation.turn_id,
        tool_call_id: observation.tool_call_id,
        attempt_id: observation.attempt_id,
        pipeline_stage: observation.pipeline_stage,
        ts_unix_ms: observation.ts_unix_ms,
        mono_ns: observation.mono_ns,
        event_seq: observation.event_seq,
    }
}

fn protocol_delta_from_tool_delta(
    delta: ToolDeltaPayload,
) -> Option<(String, ItemDeltaStream, Option<JsonValue>)> {
    match delta {
        ToolDeltaPayload::OutputChunk {
            stream,
            text,
            truncated,
        } => Some((
            text,
            stream,
            Some(serde_json::json!({
                "kind": "output_chunk",
                "truncated": truncated,
            })),
        )),
        ToolDeltaPayload::Progress { stage, metadata } => Some((
            stage.clone(),
            ItemDeltaStream::ToolProgress,
            Some(serde_json::json!({
                "kind": "progress",
                "stage": stage,
                "metadata": metadata,
            })),
        )),
        ToolDeltaPayload::ArtifactRef {
            label,
            uri,
            metadata,
        } => Some((
            label.clone(),
            ItemDeltaStream::ToolProgress,
            Some(serde_json::json!({
                "kind": "artifact_ref",
                "label": label,
                "uri": uri,
                "metadata": metadata,
            })),
        )),
        ToolDeltaPayload::Diagnostic {
            message,
            error_class,
            metadata,
        } => Some((
            message.clone(),
            ItemDeltaStream::ToolProgress,
            Some(serde_json::json!({
                "kind": "diagnostic",
                "message": message,
                "errorClass": error_class,
                "metadata": metadata,
            })),
        )),
    }
}

pub(super) fn tool_item_type_from_name(tool_name: &str) -> TurnItemType {
    match tool_name {
        "exec_command" | "write_stdin" => TurnItemType::CommandExecution,
        "apply_patch" => TurnItemType::FileChange,
        "web_search" => TurnItemType::WebSearch,
        "web_fetch" => TurnItemType::WebFetch,
        "download_url" => TurnItemType::Download,
        _ => TurnItemType::DynamicToolCall,
    }
}

fn parse_arguments_json(arguments: &str) -> JsonValue {
    if arguments.trim().is_empty() {
        return serde_json::json!({});
    }
    serde_json::from_str(arguments).unwrap_or_else(|_| serde_json::json!({ "raw": arguments }))
}

fn argument_cwd(arguments: &JsonValue) -> Option<String> {
    arguments
        .as_object()
        .and_then(|map| map.get("cwd").or_else(|| map.get("workdir")))
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

fn argument_command(arguments: &JsonValue) -> Vec<String> {
    let from_array = arguments
        .get("command")
        .and_then(JsonValue::as_array)
        .map(|array| {
            array
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !from_array.is_empty() {
        return from_array;
    }

    Vec::new()
}

pub(super) fn protocol_outcome_from_tool_outcome(
    outcome: &ToolOutcome,
) -> pioneer_protocol::ToolOutcome {
    pioneer_protocol::ToolOutcome {
        status: match outcome.status {
            pioneer_tools::ToolOutcomeStatus::Ok => pioneer_protocol::ToolOutcomeStatus::Ok,
            pioneer_tools::ToolOutcomeStatus::RecoverableError => {
                pioneer_protocol::ToolOutcomeStatus::RecoverableError
            }
            pioneer_tools::ToolOutcomeStatus::FatalError => {
                pioneer_protocol::ToolOutcomeStatus::FatalError
            }
            pioneer_tools::ToolOutcomeStatus::PartialSuccess => {
                pioneer_protocol::ToolOutcomeStatus::PartialSuccess
            }
        },
        error_class: outcome.error_class.map(|error_class| match error_class {
            pioneer_tools::ToolErrorClass::InvalidArguments => {
                pioneer_protocol::ToolErrorClass::InvalidArguments
            }
            pioneer_tools::ToolErrorClass::NotFound => pioneer_protocol::ToolErrorClass::NotFound,
            pioneer_tools::ToolErrorClass::ToolNotVisible => {
                pioneer_protocol::ToolErrorClass::ToolNotVisible
            }
            pioneer_tools::ToolErrorClass::PermissionDenied => {
                pioneer_protocol::ToolErrorClass::PermissionDenied
            }
            pioneer_tools::ToolErrorClass::CommandNotFound => {
                pioneer_protocol::ToolErrorClass::CommandNotFound
            }
            pioneer_tools::ToolErrorClass::Timeout => pioneer_protocol::ToolErrorClass::Timeout,
            pioneer_tools::ToolErrorClass::Cancelled => pioneer_protocol::ToolErrorClass::Cancelled,
            pioneer_tools::ToolErrorClass::ExecutionFailed => {
                pioneer_protocol::ToolErrorClass::ExecutionFailed
            }
            pioneer_tools::ToolErrorClass::NeedsNarrowing => {
                pioneer_protocol::ToolErrorClass::NeedsNarrowing
            }
            pioneer_tools::ToolErrorClass::Internal => pioneer_protocol::ToolErrorClass::Internal,
            pioneer_tools::ToolErrorClass::OutputTruncated => {
                pioneer_protocol::ToolErrorClass::OutputTruncated
            }
            pioneer_tools::ToolErrorClass::Unknown => pioneer_protocol::ToolErrorClass::Unknown,
        }),
        should_retry: outcome.should_retry,
        retry_hint: outcome.retry_hint.clone(),
        incomplete: outcome.incomplete,
        incomplete_reason: outcome.incomplete_reason.clone(),
    }
}

fn summary_payload(
    title: impl Into<String>,
    lines: Vec<String>,
    metadata: ToolMetadata,
    truncated: bool,
) -> ToolOutputSummary {
    ToolOutputSummary {
        title: title.into(),
        lines,
        metadata,
        truncated,
    }
}

fn bounded_summary_for_chars(summary: ToolOutputSummary, max_chars: usize) -> ToolOutputSummary {
    let mut remaining = max_chars;
    let mut truncated = summary.truncated;
    let (title, title_truncated) = take_summary_chars(summary.title.as_str(), remaining);
    truncated |= title_truncated;
    remaining = remaining.saturating_sub(title.chars().count());

    let mut lines = Vec::new();
    for line in summary.lines {
        if remaining == 0 {
            truncated = true;
            break;
        }
        let (bounded, line_truncated) = take_summary_chars(line.as_str(), remaining);
        remaining = remaining.saturating_sub(bounded.chars().count());
        lines.push(bounded);
        if line_truncated {
            truncated = true;
            break;
        }
    }

    ToolOutputSummary {
        title,
        lines,
        metadata: summary.metadata,
        truncated,
    }
}

fn take_summary_chars(value: &str, max_chars: usize) -> (String, bool) {
    let count = value.chars().count();
    if count <= max_chars {
        return (value.to_owned(), false);
    }
    (value.chars().take(max_chars).collect(), true)
}

fn summary_display(summary: ToolOutputSummary) -> ToolDisplayPayload {
    ToolDisplayPayload::Summary(summary)
}

fn summary_storage(summary: ToolOutputSummary) -> ToolStoragePayload {
    ToolStoragePayload::Summary(summary)
}

fn metadata_storage(metadata: ToolMetadata) -> ToolStoragePayload {
    ToolStoragePayload::Metadata { metadata }
}

fn recovery_view_from_outcome(
    outcome: Option<&pioneer_protocol::ToolOutcome>,
) -> Option<ToolRecoveryView> {
    outcome.map(|outcome| ToolRecoveryView {
        error_class: outcome.error_class.map(|value| format!("{value:?}")),
        retry_hint: outcome.retry_hint.clone(),
        incomplete_reason: outcome.incomplete_reason.clone(),
        diagnostic_summary: outcome.retry_hint.clone(),
        diagnostic_excerpt: None,
        output_fingerprint: None,
        content_fingerprint: None,
        was_truncated: outcome.incomplete,
        continuation: None,
    })
}

pub(super) fn retained_llm_context_view(
    policy: &ToolOutputPolicySnapshot,
    llm_view: &ToolResultView,
) -> Option<ToolResultView> {
    match policy.llm_retention {
        LlmRetentionPolicy::UntilTurnTerminal { max_bytes } => {
            Some(llm_view.bounded_to_bytes(max_bytes))
        }
        LlmRetentionPolicy::DoNotRetain => None,
    }
}

fn storage_metadata_from_payload(storage: Option<&ToolStoragePayload>) -> Option<ToolMetadata> {
    match storage? {
        ToolStoragePayload::Metadata { metadata } => Some(metadata.clone()),
        ToolStoragePayload::Summary(summary) => Some(summary.metadata.clone()),
        _ => None,
    }
}

fn storage_summary_from_payload(storage: Option<&ToolStoragePayload>) -> Option<ToolOutputSummary> {
    match storage? {
        ToolStoragePayload::Summary(summary) => Some(summary.clone()),
        _ => None,
    }
}

#[derive(Debug, Clone, Default)]
struct ShellProjection {
    stdout: Option<String>,
    stderr: Option<String>,
    aggregated_output: Option<String>,
    exit_code: Option<i32>,
    duration_ms: Option<u64>,
    timed_out: Option<bool>,
    truncated: bool,
}

fn shell_from_display(display: Option<&ToolDisplayPayload>) -> Option<ShellProjection> {
    match display? {
        ToolDisplayPayload::Shell {
            stdout,
            stderr,
            aggregated_output,
            exit_code,
            duration_ms,
            timed_out,
            truncated,
        } => Some(ShellProjection {
            stdout: stdout.clone(),
            stderr: stderr.clone(),
            aggregated_output: aggregated_output.clone(),
            exit_code: *exit_code,
            duration_ms: *duration_ms,
            timed_out: *timed_out,
            truncated: *truncated,
        }),
        _ => None,
    }
}

fn shell_from_storage(storage: Option<&ToolStoragePayload>) -> Option<ShellProjection> {
    match storage? {
        ToolStoragePayload::Shell {
            stdout,
            stderr,
            aggregated_output,
            exit_code,
            duration_ms,
            timed_out,
            truncated,
        } => Some(ShellProjection {
            stdout: stdout.clone(),
            stderr: stderr.clone(),
            aggregated_output: aggregated_output.clone(),
            exit_code: *exit_code,
            duration_ms: *duration_ms,
            timed_out: *timed_out,
            truncated: *truncated,
        }),
        _ => None,
    }
}

fn metadata_string(metadata: &ToolMetadata, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(ToolMetadataValue::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn file_change_title(tool_name: &str, operation: Option<&str>, file_count: usize) -> String {
    let verb = match operation {
        Some("created") => "created",
        Some("overwritten") => "overwrote",
        Some("edited") => "edited",
        _ => "changed",
    };
    format!("{tool_name} {verb} {file_count} file(s)")
}

fn metadata_u16(metadata: &ToolMetadata, key: &str) -> Option<u16> {
    metadata
        .get(key)
        .and_then(ToolMetadataValue::as_u64)
        .and_then(|value| u16::try_from(value).ok())
}

fn metadata_u64(metadata: &ToolMetadata, key: &str) -> Option<u64> {
    metadata.get(key).and_then(ToolMetadataValue::as_u64)
}

fn metadata_usize(metadata: &ToolMetadata, key: &str) -> Option<usize> {
    metadata
        .get(key)
        .and_then(ToolMetadataValue::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

fn metadata_bool(metadata: &ToolMetadata, key: &str) -> Option<bool> {
    metadata.get(key).and_then(ToolMetadataValue::as_bool)
}

fn metadata_i32(metadata: &ToolMetadata, key: &str) -> Option<i32> {
    metadata
        .get(key)
        .and_then(ToolMetadataValue::as_i64)
        .and_then(|value| i32::try_from(value).ok())
}

fn metadata_string_array(metadata: &ToolMetadata, key: &str) -> Vec<String> {
    metadata
        .get(key)
        .and_then(ToolMetadataValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(ToolMetadataValue::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn web_search_results_from_metadata(
    metadata: &ToolMetadata,
) -> Vec<pioneer_protocol::WebSearchResultItem> {
    metadata
        .get("results")
        .and_then(ToolMetadataValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    serde_json::from_value::<WebSearchModelPayloadResult>(item.to_json()).ok()
                })
                .map(|result| pioneer_protocol::WebSearchResultItem {
                    rank: result.rank,
                    title: result.title,
                    url: result.url,
                    snippet: result.snippet,
                    source: result.source,
                    published_at: result.published_at,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

#[derive(serde::Deserialize)]
struct WebSearchModelPayloadResult {
    rank: usize,
    title: String,
    url: String,
    #[serde(default)]
    snippet: String,
    source: String,
    #[serde(default)]
    published_at: Option<String>,
}

fn web_fetch_links_from_metadata(metadata: &ToolMetadata) -> Vec<pioneer_protocol::WebFetchLink> {
    metadata
        .get("links")
        .and_then(ToolMetadataValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let fields = item.as_object()?;
                    let text = fields.get("text").and_then(ToolMetadataValue::as_str)?;
                    let url = fields
                        .get("url")
                        .or_else(|| fields.get("href"))
                        .and_then(ToolMetadataValue::as_str)?;
                    Some(pioneer_protocol::WebFetchLink {
                        text: text.to_owned(),
                        url: url.to_owned(),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

#[derive(Debug, Clone)]
pub(super) struct ProjectedToolItemInput {
    pub id: String,
    pub tool_name: String,
    pub arguments: JsonValue,
    pub status: ToolCallStatus,
    pub success: Option<bool>,
    pub outcome: Option<pioneer_protocol::ToolOutcome>,
    pub output_policy: ToolOutputPolicySnapshot,
    pub recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    pub turn_item_execution_class: TurnItemExecutionClass,
    pub display: ToolDisplayPayload,
    pub storage: ToolStoragePayload,
    pub recovery: Option<ToolRecoveryView>,
    pub observation: Option<ToolObservation>,
}

impl ProjectedToolItemInput {
    pub fn started(
        id: String,
        tool_name: String,
        arguments: JsonValue,
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        turn_item_execution_class: TurnItemExecutionClass,
        output_policy: ToolOutputPolicySnapshot,
        observation: Option<ToolObservation>,
    ) -> Self {
        Self {
            id,
            tool_name,
            arguments,
            status: ToolCallStatus::InProgress,
            success: None,
            outcome: None,
            output_policy,
            recovery_policy,
            turn_item_execution_class,
            display: ToolDisplayPayload::Progress {
                stage: "running".to_owned(),
                metadata: ToolMetadata::empty(),
            },
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({ "state": "running" })),
            },
            recovery: None,
            observation,
        }
    }

    pub fn failed(
        id: String,
        tool_name: String,
        arguments: JsonValue,
        error: String,
        outcome: pioneer_protocol::ToolOutcome,
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        turn_item_execution_class: TurnItemExecutionClass,
        output_policy: ToolOutputPolicySnapshot,
        observation: Option<ToolObservation>,
    ) -> Self {
        let summary = summary_payload(
            format!("{tool_name} failed"),
            vec![error.clone()],
            ToolMetadata::from_json(serde_json::json!({
                "errorClass": outcome.error_class.map(|class| format!("{class:?}")),
            })),
            false,
        );
        let display = display_for_policy(&output_policy, summary.clone());
        let storage = storage_for_policy(&output_policy, summary);
        Self {
            id,
            tool_name,
            arguments,
            status: ToolCallStatus::Failed,
            success: Some(false),
            outcome: Some(outcome.clone()),
            output_policy,
            recovery_policy,
            turn_item_execution_class,
            display,
            storage,
            recovery: recovery_view_from_outcome(Some(&outcome)),
            observation,
        }
    }
}

pub(super) fn build_started_tool_turn_item(
    id: String,
    tool_name: String,
    arguments: String,
    recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    turn_item_execution_class: TurnItemExecutionClass,
    output_policy: Option<ToolOutputPolicySnapshot>,
    observation: Option<ToolObservation>,
) -> TurnItem {
    let output_policy = output_policy
        .unwrap_or_else(|| ToolOutputPolicySnapshot::for_tool_name(tool_name.as_str()));
    build_tool_turn_item(ProjectedToolItemInput::started(
        id,
        tool_name,
        parse_arguments_json(arguments.as_str()),
        recovery_policy,
        turn_item_execution_class,
        output_policy,
        observation,
    ))
}

pub(super) fn build_failed_tool_turn_item(
    id: String,
    tool_name: String,
    arguments: String,
    error: String,
    outcome: ToolOutcome,
    recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    turn_item_execution_class: TurnItemExecutionClass,
    output_policy: Option<ToolOutputPolicySnapshot>,
    observation: Option<ToolObservation>,
) -> TurnItem {
    let output_policy = output_policy
        .unwrap_or_else(|| ToolOutputPolicySnapshot::for_tool_name(tool_name.as_str()));
    build_tool_turn_item(ProjectedToolItemInput::failed(
        id,
        tool_name,
        parse_arguments_json(arguments.as_str()),
        error,
        protocol_outcome_from_tool_outcome(&outcome),
        recovery_policy,
        turn_item_execution_class,
        output_policy,
        observation,
    ))
}

pub(super) fn build_completed_tool_turn_item(
    id: String,
    tool_name: String,
    arguments: String,
    recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    turn_item_execution_class: TurnItemExecutionClass,
    output_policy: Option<ToolOutputPolicySnapshot>,
    observation: Option<ToolObservation>,
) -> TurnItem {
    let output_policy = output_policy
        .unwrap_or_else(|| ToolOutputPolicySnapshot::for_tool_name(tool_name.as_str()));
    let summary = summary_payload(
        format!("{tool_name} completed"),
        Vec::new(),
        ToolMetadata::from_json(serde_json::json!({ "state": "completed" })),
        false,
    );
    let display = display_for_policy(&output_policy, summary.clone());
    let storage = storage_for_policy(&output_policy, summary);
    build_tool_turn_item(ProjectedToolItemInput {
        id,
        tool_name,
        arguments: parse_arguments_json(arguments.as_str()),
        status: ToolCallStatus::Completed,
        success: Some(true),
        outcome: None,
        output_policy,
        recovery_policy,
        turn_item_execution_class,
        display,
        storage,
        recovery: None,
        observation,
    })
}

fn display_for_policy(
    output_policy: &ToolOutputPolicySnapshot,
    summary: ToolOutputSummary,
) -> ToolDisplayPayload {
    match output_policy.timeline {
        TimelineOutputPolicy::Full { .. } => ToolDisplayPayload::Summary(summary),
        TimelineOutputPolicy::Summary { max_chars } => {
            ToolDisplayPayload::Summary(bounded_summary_for_chars(summary, max_chars))
        }
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {
            ToolDisplayPayload::Hidden
        }
    }
}

fn storage_for_policy(
    output_policy: &ToolOutputPolicySnapshot,
    summary: ToolOutputSummary,
) -> ToolStoragePayload {
    match output_policy.storage {
        StorageOutputPolicy::Full { .. } => ToolStoragePayload::Summary(summary),
        StorageOutputPolicy::Summary { max_chars } => {
            ToolStoragePayload::Summary(bounded_summary_for_chars(summary, max_chars))
        }
        StorageOutputPolicy::MetadataOnly => ToolStoragePayload::Metadata {
            metadata: summary.metadata,
        },
        StorageOutputPolicy::None => ToolStoragePayload::None,
    }
}

pub(super) fn build_tool_turn_item(input: ProjectedToolItemInput) -> TurnItem {
    let ProjectedToolItemInput {
        id,
        tool_name,
        arguments: arguments_json,
        status,
        success,
        outcome,
        output_policy,
        recovery_policy,
        turn_item_execution_class,
        display,
        storage,
        recovery,
        observation,
    } = input;
    let display_projection = Some(display);
    let storage_projection = Some(storage);
    let storage_metadata = storage_metadata_from_payload(storage_projection.as_ref());
    let storage_summary = storage_summary_from_payload(storage_projection.as_ref());

    match tool_item_type_from_name(tool_name.as_str()) {
        TurnItemType::CommandExecution => {
            let shell = shell_from_storage(storage_projection.as_ref())
                .or_else(|| shell_from_display(display_projection.as_ref()))
                .unwrap_or_default();
            let command = argument_command(&arguments_json);
            let cwd = argument_cwd(&arguments_json);
            let display = display_projection
                .clone()
                .unwrap_or_else(|| ToolDisplayPayload::Shell {
                    stdout: shell.stdout.clone(),
                    stderr: shell.stderr.clone(),
                    aggregated_output: shell.aggregated_output.clone(),
                    exit_code: shell.exit_code,
                    duration_ms: shell.duration_ms,
                    timed_out: shell.timed_out,
                    truncated: shell.truncated,
                });
            let storage = storage_projection
                .clone()
                .unwrap_or_else(|| ToolStoragePayload::Shell {
                    stdout: shell.stdout.clone(),
                    stderr: shell.stderr.clone(),
                    aggregated_output: shell.aggregated_output.clone(),
                    exit_code: shell.exit_code,
                    duration_ms: shell.duration_ms,
                    timed_out: shell.timed_out,
                    truncated: shell.truncated,
                });
            TurnItem::CommandExecution {
                id,
                tool_name,
                arguments: arguments_json,
                status,
                recovery_policy,
                output_policy,
                display,
                storage,
                recovery,
                command,
                cwd,
                success,
                outcome,
                observation,
            }
        }
        TurnItemType::FileChange => {
            let metadata = storage_metadata.clone().unwrap_or_else(ToolMetadata::empty);
            let changed_files = metadata_string_array(&metadata, "changedFiles");
            let exit_code = metadata_i32(&metadata, "exitCode");
            let title = if changed_files.is_empty() {
                format!("{} completed", tool_name)
            } else {
                file_change_title(
                    tool_name.as_str(),
                    metadata_string(&metadata, "operation").as_deref(),
                    changed_files.len(),
                )
            };
            let summary = storage_summary.clone().unwrap_or_else(|| {
                summary_payload(
                    title,
                    changed_files.clone(),
                    metadata.clone(),
                    metadata_bool(&metadata, "truncated").unwrap_or(false),
                )
            });
            TurnItem::FileChange {
                id,
                tool_name,
                arguments: arguments_json,
                status,
                recovery_policy,
                output_policy,
                display: display_projection
                    .clone()
                    .unwrap_or_else(|| summary_display(summary.clone())),
                storage: storage_projection
                    .clone()
                    .unwrap_or_else(|| summary_storage(summary)),
                recovery,
                changed_files,
                exit_code,
                stdout: None,
                stderr: None,
                success,
                outcome,
                observation,
            }
        }
        TurnItemType::WebSearch => {
            let metadata = storage_metadata.clone().unwrap_or_else(ToolMetadata::empty);
            let query = metadata_string(&metadata, "query").or_else(|| {
                arguments_json
                    .get("query")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)
            });
            let provider = metadata_string(&metadata, "provider");
            let took_ms = metadata_u64(&metadata, "tookMs");
            let results = web_search_results_from_metadata(&metadata);
            let result_count = metadata_usize(&metadata, "resultCount").or(Some(results.len()));
            let summary = storage_summary.clone().unwrap_or_else(|| {
                summary_payload(
                    query
                        .as_ref()
                        .map(|query| {
                            format!(
                                "web_search found {} result(s) for {query}",
                                result_count.unwrap_or(results.len())
                            )
                        })
                        .unwrap_or_else(|| {
                            format!(
                                "web_search found {} result(s)",
                                result_count.unwrap_or(results.len())
                            )
                        }),
                    Vec::new(),
                    metadata.clone(),
                    metadata_bool(&metadata, "truncated").unwrap_or(false),
                )
            });
            TurnItem::WebSearch {
                id,
                tool_name,
                arguments: arguments_json,
                status,
                recovery_policy,
                output_policy,
                display: display_projection
                    .clone()
                    .unwrap_or_else(|| summary_display(summary.clone())),
                storage: storage_projection
                    .clone()
                    .unwrap_or_else(|| summary_storage(summary)),
                recovery,
                query,
                provider,
                took_ms,
                result_count,
                results,
                success,
                outcome,
                observation,
            }
        }
        TurnItemType::WebFetch => {
            let metadata = storage_metadata.clone().unwrap_or_else(ToolMetadata::empty);
            let url = metadata_string(&metadata, "url").or_else(|| {
                arguments_json
                    .get("url")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)
            });
            let final_url = metadata_string(&metadata, "finalUrl");
            let status_code = metadata_u16(&metadata, "statusCode");
            let content_type = metadata_string(&metadata, "contentType");
            let extract_mode = metadata_string(&metadata, "extractMode");
            let resolved_mode = metadata_string(&metadata, "resolvedMode");
            let bytes_received = metadata_usize(&metadata, "bytesReceived");
            let elapsed_ms = metadata_u64(&metadata, "elapsedMs");
            let truncated_bool = metadata_bool(&metadata, "truncated").unwrap_or(false);
            let truncated = truncated_bool.then_some(JsonValue::Bool(true));
            let title = metadata_string(&metadata, "title");
            let word_count = metadata_usize(&metadata, "wordCount");
            let links = web_fetch_links_from_metadata(&metadata);
            let summary = storage_summary.clone().unwrap_or_else(|| {
                summary_payload(
                    url.as_ref()
                        .map(|url| format!("Fetched {url}"))
                        .unwrap_or_else(|| "web_fetch completed".to_owned()),
                    vec![
                        status_code
                            .map(|code| format!("HTTP {code}"))
                            .unwrap_or_else(|| "HTTP status unavailable".to_owned()),
                        content_type
                            .clone()
                            .unwrap_or_else(|| "content type unavailable".to_owned()),
                    ],
                    metadata.clone(),
                    truncated_bool,
                )
            });
            TurnItem::WebFetch {
                id,
                tool_name,
                arguments: arguments_json,
                status,
                recovery_policy,
                output_policy,
                display: display_projection
                    .clone()
                    .unwrap_or_else(|| summary_display(summary.clone())),
                storage: storage_projection
                    .clone()
                    .unwrap_or_else(|| metadata_storage(summary.metadata.clone())),
                recovery,
                url,
                final_url,
                status_code,
                content_type,
                extract_mode,
                resolved_mode,
                bytes_received,
                elapsed_ms,
                truncated,
                title,
                word_count,
                links,
                success,
                outcome,
                observation,
            }
        }
        TurnItemType::Download => {
            let metadata = storage_metadata.clone().unwrap_or_else(ToolMetadata::empty);
            let url = metadata_string(&metadata, "url").or_else(|| {
                arguments_json
                    .get("url")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)
            });
            let final_url = metadata_string(&metadata, "finalUrl");
            let status_code = metadata_u16(&metadata, "statusCode");
            let path = metadata_string(&metadata, "path");
            let bytes_written = metadata_u64(&metadata, "bytesWritten");
            let sha256 = metadata_string(&metadata, "sha256");
            let content_type = metadata_string(&metadata, "contentType");
            let elapsed_ms = metadata_u64(&metadata, "elapsedMs");
            let truncated = metadata_bool(&metadata, "truncated");
            let summary = storage_summary.clone().unwrap_or_else(|| {
                summary_payload(
                    url.as_ref()
                        .map(|url| format!("Downloaded {url}"))
                        .unwrap_or_else(|| "download completed".to_owned()),
                    path.iter().cloned().collect::<Vec<_>>(),
                    metadata.clone(),
                    truncated.unwrap_or(false),
                )
            });
            TurnItem::Download {
                id,
                tool_name,
                arguments: arguments_json,
                status,
                recovery_policy,
                output_policy,
                display: display_projection
                    .clone()
                    .unwrap_or_else(|| summary_display(summary.clone())),
                storage: storage_projection
                    .clone()
                    .unwrap_or_else(|| metadata_storage(summary.metadata.clone())),
                recovery,
                url,
                final_url,
                status_code,
                path,
                bytes_written,
                sha256,
                content_type,
                elapsed_ms,
                truncated,
                success,
                outcome,
                observation,
            }
        }
        TurnItemType::DynamicToolCall
        | TurnItemType::UserMessage
        | TurnItemType::AgentMessage
        | TurnItemType::Reasoning
        | TurnItemType::SystemEvent
        | TurnItemType::Task => {
            let metadata = storage_metadata.clone().unwrap_or_else(|| {
                ToolMetadata::from_json(serde_json::json!({
                    "toolName": tool_name.clone(),
                    "storageProjection": "metadata_only",
                }))
            });
            let summary = storage_summary.clone().unwrap_or_else(|| {
                summary_payload(
                    format!("{tool_name} {status:?}"),
                    Vec::new(),
                    metadata.clone(),
                    metadata_bool(&metadata, "truncated").unwrap_or(false),
                )
            });
            TurnItem::DynamicToolCall {
                id,
                tool_name,
                arguments: arguments_json,
                status,
                execution_class: turn_item_execution_class,
                recovery_policy,
                output_policy,
                display: display_projection
                    .clone()
                    .unwrap_or_else(|| summary_display(summary.clone())),
                storage: storage_projection
                    .clone()
                    .unwrap_or_else(|| metadata_storage(summary.metadata.clone())),
                recovery,
                success,
                outcome,
                observation,
            }
        }
    }
}

pub(super) fn build_tool_error_message(
    tool_call_id: String,
    tool_name: String,
    error_message: String,
    outcome: ToolOutcome,
) -> pioneer_provider::ChatMessage {
    let payload = serde_json::json!({
        "error": error_message.clone(),
        "tool_outcome": outcome.clone(),
        "partial_output": {
            "is_partial": outcome.incomplete || matches!(outcome.status, pioneer_tools::ToolOutcomeStatus::PartialSuccess),
            "reason": outcome.incomplete_reason,
            "continuation_available": outcome.should_retry,
            "truncated": false,
        },
    });
    pioneer_provider::ModelInputItem::tool_result(
        tool_call_id,
        tool_name,
        error_message,
        Some(payload),
    )
    .into_chat_message()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::AgentDurableEvent;
    use pioneer_tools::{
        ObservationContext, ToolCallCompletedEvent, ToolEventPayload, ToolResultView,
    };
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn lossy_delta_cannot_synthesize_missing_durable_tool_start() {
        let event_tx = AgentEventHub::with_capacity(8, 8);
        let mut durable_rx = event_tx
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available");
        let pending_tool_ui = Arc::new(Mutex::new(HashMap::new()));
        let event = ToolEvent {
            schema_version: 1,
            call_id: "call_late_delta".to_owned(),
            tool_name: "exec_command".to_owned(),
            ts_unix_ms: 1,
            observation: ObservationContext {
                trace_id: "trace_late_delta".to_owned(),
                turn_id: "turn_1".to_owned(),
                tool_call_id: "call_late_delta".to_owned(),
                attempt_id: 1,
                pipeline_stage: "runtime.call.delta".to_owned(),
                ts_unix_ms: 1,
                mono_ns: 1,
                event_seq: 2,
            },
            payload: ToolEventPayload::OutputDelta(pioneer_tools::ToolOutputDeltaEvent {
                delta: ToolDeltaPayload::OutputChunk {
                    stream: ItemDeltaStream::Stdout,
                    text: "late".to_owned(),
                    truncated: false,
                },
            }),
        };

        forward_tool_event_to_agent(
            event,
            &event_tx,
            pending_tool_ui,
            "workspace_1",
            "thread_1",
            "turn_1",
        )
        .await
        .expect("lossy delta should be safely ignored");

        assert!(
            timeout(Duration::from_millis(20), durable_rx.recv())
                .await
                .is_err(),
            "a progress delta must not invent an ItemStarted durable event"
        );
    }

    #[test]
    fn tool_item_type_maps_apply_patch_to_file_change() {
        assert_eq!(
            tool_item_type_from_name("apply_patch"),
            TurnItemType::FileChange
        );
        assert_eq!(
            tool_item_type_from_name("exec_command"),
            TurnItemType::CommandExecution
        );
        assert_eq!(
            tool_item_type_from_name("write_stdin"),
            TurnItemType::CommandExecution
        );
        assert_eq!(
            tool_item_type_from_name("unknown_tool"),
            TurnItemType::DynamicToolCall
        );
    }

    #[test]
    fn file_change_title_uses_operation_when_available() {
        assert_eq!(
            file_change_title("apply_patch", Some("update"), 1),
            "apply_patch changed 1 file(s)"
        );
        assert_eq!(
            file_change_title("apply_patch", None, 2),
            "apply_patch changed 2 file(s)"
        );
    }

    #[tokio::test]
    async fn completed_tool_event_keeps_llm_context_out_of_async_event_forwarder() {
        let event_tx = Arc::new(AgentEventHub::with_capacity(8, 8));
        let mut durable_rx = event_tx
            .take_durable_receiver()
            .await
            .expect("durable receiver should be available");
        let pending_tool_ui = Arc::new(Mutex::new(HashMap::new()));
        let mut output_policy = ToolOutputPolicySnapshot::for_tool_name("read_file");
        output_policy.llm_retention = LlmRetentionPolicy::UntilTurnTerminal { max_bytes: 256 };
        let event = ToolEvent {
            schema_version: 1,
            call_id: "call_read".to_owned(),
            tool_name: "read_file".to_owned(),
            ts_unix_ms: 1,
            observation: ObservationContext {
                trace_id: "trace_read".to_owned(),
                turn_id: "turn_1".to_owned(),
                tool_call_id: "call_read".to_owned(),
                attempt_id: 1,
                pipeline_stage: "runtime.call.completed".to_owned(),
                ts_unix_ms: 1,
                mono_ns: 1,
                event_seq: 7,
            },
            payload: ToolEventPayload::CallCompleted(ToolCallCompletedEvent {
                success: true,
                outcome: ToolOutcome::ok(),
                llm_view: ToolResultView::Json {
                    value: serde_json::json!({
                        "path": "/tmp/secret.txt",
                        "output": format!("SECRET_LLM_ONLY_SENTINEL{}", "x".repeat(5_000))
                    }),
                    truncated: false,
                },
                display: ToolDisplayPayload::Summary(ToolOutputSummary {
                    title: "Read /tmp/secret.txt".to_owned(),
                    lines: Vec::new(),
                    metadata: ToolMetadata::from_json(serde_json::json!({
                        "path": "/tmp/secret.txt",
                        "contentHash": "sha256:test"
                    })),
                    truncated: false,
                }),
                storage: ToolStoragePayload::Metadata {
                    metadata: ToolMetadata::from_json(serde_json::json!({
                        "path": "/tmp/secret.txt",
                        "contentHash": "sha256:test"
                    })),
                },
                recovery: None,
                output_policy,
            }),
        };

        let forward_event_tx = event_tx.clone();
        let forwarder = tokio::spawn(async move {
            forward_tool_event_to_agent(
                event,
                forward_event_tx.as_ref(),
                pending_tool_ui,
                "workspace_1",
                "thread_1",
                "turn_1",
            )
            .await
        });

        let mut saw_completed = false;
        for _ in 0..2 {
            let event = tokio::time::timeout(std::time::Duration::from_secs(1), durable_rx.recv())
                .await
                .expect("agent event should be emitted before timeout")
                .expect("durable lane should stay open");
            match event {
                AgentDurableEvent::TurnLlmContextAppended { .. } => {
                    panic!("async tool forwarding must not race provider-history persistence");
                }
                AgentDurableEvent::ItemCompleted { notification } => {
                    saw_completed = true;
                    let item_json =
                        serde_json::to_string(&notification.item).expect("item should serialize");
                    assert!(!item_json.contains("SECRET_LLM_ONLY_SENTINEL"));
                }
                _ => {}
            }
            durable_rx.acknowledge_last(Ok(()));
        }

        forwarder
            .await
            .expect("tool forwarder should not panic")
            .expect("tool event should publish after commit acknowledgements");
        assert!(saw_completed);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), durable_rx.recv())
                .await
                .is_err(),
            "tool event forwarder must not publish retained provider history"
        );
    }

    #[test]
    fn hidden_tool_failed_item_preserves_tool_not_visible_outcome() {
        let outcome = pioneer_tools::ToolOutcome::recoverable(
            pioneer_tools::ToolErrorClass::ToolNotVisible,
            "Tool is registered but hidden in this provider round.",
            false,
            None,
        );

        let item = build_failed_tool_turn_item(
            "call_hidden".to_owned(),
            "hidden_tool".to_owned(),
            "{}".to_owned(),
            "tool not visible: hidden_tool".to_owned(),
            outcome,
            None,
            TurnItemExecutionClass::Standard,
            None,
            None,
        );

        let TurnItem::DynamicToolCall {
            status,
            success,
            outcome,
            recovery,
            ..
        } = item
        else {
            panic!("hidden tool failure should use normal dynamic tool call item");
        };

        assert_eq!(status, ToolCallStatus::Failed);
        assert_eq!(success, Some(false));
        let outcome = outcome.expect("failed hidden tool item should carry outcome");
        assert_eq!(
            outcome.status,
            pioneer_protocol::ToolOutcomeStatus::RecoverableError
        );
        assert_eq!(
            outcome.error_class,
            Some(pioneer_protocol::ToolErrorClass::ToolNotVisible)
        );
        assert!(
            recovery.is_some(),
            "failed hidden tool item should expose normal recovery metadata"
        );
    }

    #[test]
    fn failed_tool_item_display_summary_is_bounded_by_policy() {
        let item = build_failed_tool_turn_item(
            "call_computer_use".to_owned(),
            "computer_use".to_owned(),
            serde_json::json!({"action": "start"}).to_string(),
            "computer_use app target not found ".repeat(300),
            pioneer_tools::ToolOutcome::fatal(
                pioneer_tools::ToolErrorClass::NotFound,
                Some("target not found".to_owned()),
            ),
            None,
            TurnItemExecutionClass::Standard,
            Some(ToolOutputPolicySnapshot::for_tool_name("computer_use")),
            None,
        );

        let TurnItem::DynamicToolCall { display, .. } = item else {
            panic!("computer_use failure should be a dynamic tool call");
        };
        let ToolDisplayPayload::Summary(summary) = display else {
            panic!("computer_use failure should render a summary display");
        };
        let visible_chars = summary.title.chars().count()
            + summary
                .lines
                .iter()
                .map(|line| line.chars().count())
                .sum::<usize>();
        assert!(visible_chars <= 2_000, "visible_chars={visible_chars}");
        assert!(summary.truncated);
    }
}
