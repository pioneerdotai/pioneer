//! Projection from canonical CLI runtime events into Pioneer agent events.

#![allow(dead_code)]

use pioneer_cli_agent_runtime::event::{
    RuntimeAgentMessagePhase, RuntimeDiffUpdated, RuntimeErrorEvent, RuntimeEvent,
    RuntimeItemCompleted, RuntimeItemDelta, RuntimeItemDeltaKind, RuntimeItemStarted,
    RuntimeItemUpdated, RuntimePlanUpdated, RuntimeRawEvent, RuntimeReviewModeChanged,
    RuntimeThreadStateChanged,
};
use pioneer_protocol::{
    AgentDurableEvent, AgentMessagePhase, AgentProgressEvent, ItemCompletedNotification,
    ItemDeltaNotification, ItemDeltaStream, ItemStartedNotification, ItemUpdatedNotification,
    StorageOutputPolicy, SystemEventLevel, TimelineOutputPolicy, ToolCallStatus,
    ToolDisplayPayload, ToolMetadata, ToolOutputPolicySnapshot, ToolOutputSummary,
    ToolStoragePayload, TurnItem,
};
pub(crate) use pioneer_runtime_events::ExecutionSnapshotEvent as AgentSnapshotEvent;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIRuntimeProjectorContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct CLIRuntimeProjectedEvents {
    pub durable: Vec<AgentDurableEvent>,
    pub snapshot: Vec<AgentSnapshotEvent>,
    pub progress: Vec<AgentProgressEvent>,
    pub ignored: Vec<String>,
}

impl CLIRuntimeProjectedEvents {
    fn durable(event: AgentDurableEvent) -> Self {
        Self {
            durable: vec![event],
            snapshot: Vec::new(),
            progress: Vec::new(),
            ignored: Vec::new(),
        }
    }

    fn snapshot(event: AgentSnapshotEvent) -> Self {
        Self {
            durable: Vec::new(),
            snapshot: vec![event],
            progress: Vec::new(),
            ignored: Vec::new(),
        }
    }

    fn progress(event: AgentProgressEvent) -> Self {
        Self {
            durable: Vec::new(),
            snapshot: Vec::new(),
            progress: vec![event],
            ignored: Vec::new(),
        }
    }

    fn ignored(reason: impl Into<String>) -> Self {
        Self {
            durable: Vec::new(),
            snapshot: Vec::new(),
            progress: Vec::new(),
            ignored: vec![reason.into()],
        }
    }
}

pub(crate) fn project_cli_runtime_event(
    context: &CLIRuntimeProjectorContext,
    event: &RuntimeEvent,
) -> CLIRuntimeProjectedEvents {
    match event {
        RuntimeEvent::TurnCompleted(_) => {
            CLIRuntimeProjectedEvents::durable(AgentDurableEvent::TurnCompleted {
                thread_id: context.thread_id.clone(),
                turn_id: context.turn_id.clone(),
                recovery: context.recovery.clone(),
            })
        }
        RuntimeEvent::TurnFailed(failed) => {
            CLIRuntimeProjectedEvents::durable(AgentDurableEvent::TurnFailed {
                thread_id: context.thread_id.clone(),
                turn_id: context.turn_id.clone(),
                error: failed.message.clone(),
                recovery: context.recovery.clone(),
            })
        }
        RuntimeEvent::TurnInterrupted(interrupted) => {
            CLIRuntimeProjectedEvents::durable(AgentDurableEvent::TurnInterrupted {
                thread_id: context.thread_id.clone(),
                turn_id: context.turn_id.clone(),
                reason: interrupted.reason.clone(),
                recovery: context.recovery.clone(),
            })
        }
        RuntimeEvent::TurnRetrying(_) => CLIRuntimeProjectedEvents::ignored("turn_retrying"),
        RuntimeEvent::Error(error) => project_runtime_error(context, error),
        RuntimeEvent::ReviewModeChanged(review) => project_review_mode_changed(context, review),
        RuntimeEvent::ItemStarted(started) => project_item_started(context, started),
        RuntimeEvent::ItemDelta(delta) => project_item_delta(context, delta),
        RuntimeEvent::ItemCompleted(completed) => project_item_completed(context, completed),
        RuntimeEvent::ItemUpdated(updated) => project_item_updated(context, updated),
        RuntimeEvent::PlanUpdated(plan) => project_plan_updated(context, plan),
        RuntimeEvent::DiffUpdated(diff) => project_diff_updated(context, diff),
        RuntimeEvent::ThreadStateChanged(thread) => project_thread_state_changed(context, thread),
        RuntimeEvent::Raw(raw) => project_raw_event(context, raw),
        RuntimeEvent::SessionStateChanged(_)
        | RuntimeEvent::ThreadGoalUpdated(_)
        | RuntimeEvent::ThreadGoalCleared(_)
        | RuntimeEvent::TurnStarted(_)
        | RuntimeEvent::RequestOpened(_)
        | RuntimeEvent::RequestResolved(_)
        | RuntimeEvent::AccountUpdated(_)
        | RuntimeEvent::AppListUpdated(_) => {
            CLIRuntimeProjectedEvents::ignored(event_kind_name(event).to_owned())
        }
    }
}

fn project_runtime_error(
    context: &CLIRuntimeProjectorContext,
    error: &RuntimeErrorEvent,
) -> CLIRuntimeProjectedEvents {
    if error.retryable {
        return CLIRuntimeProjectedEvents::ignored("retryable runtime error");
    }
    if error.native_turn_id.is_none() {
        return CLIRuntimeProjectedEvents::ignored("runtime error without turn id");
    }

    CLIRuntimeProjectedEvents::durable(AgentDurableEvent::TurnFailed {
        thread_id: context.thread_id.clone(),
        turn_id: context.turn_id.clone(),
        error: error.message.clone(),
        recovery: context.recovery.clone(),
    })
}

fn project_item_started(
    context: &CLIRuntimeProjectorContext,
    started: &RuntimeItemStarted,
) -> CLIRuntimeProjectedEvents {
    if is_user_message_kind(started.item_kind.as_str()) {
        return CLIRuntimeProjectedEvents::ignored("native userMessage echo");
    }

    let item = started_turn_item(
        started.item_kind.as_str(),
        started.native_item_id.as_str(),
        started.title.as_deref(),
        started.phase,
        started.metadata.as_ref(),
    );

    CLIRuntimeProjectedEvents::durable(AgentDurableEvent::ItemStarted {
        notification: ItemStartedNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item,
        },
    })
}

fn project_review_mode_changed(
    context: &CLIRuntimeProjectorContext,
    review: &RuntimeReviewModeChanged,
) -> CLIRuntimeProjectedEvents {
    let item = review_system_event_item(context, review);
    if review.status == "entered" || review.status == "started" {
        return CLIRuntimeProjectedEvents::durable(AgentDurableEvent::ItemStarted {
            notification: ItemStartedNotification {
                workspace_id: context.workspace_id.clone(),
                thread_id: context.thread_id.clone(),
                turn_id: context.turn_id.clone(),
                item,
            },
        });
    }

    CLIRuntimeProjectedEvents::durable(AgentDurableEvent::ItemCompleted {
        notification: ItemCompletedNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item,
        },
    })
}

fn project_item_delta(
    context: &CLIRuntimeProjectorContext,
    delta: &RuntimeItemDelta,
) -> CLIRuntimeProjectedEvents {
    let (stream, runtime_delta_kind) = match delta.delta_kind {
        RuntimeItemDeltaKind::AgentMessage => (ItemDeltaStream::AgentMessage, "agent_message"),
        RuntimeItemDeltaKind::ReasoningText => (ItemDeltaStream::Generic, "reasoning_text"),
        RuntimeItemDeltaKind::ReasoningSummary => (ItemDeltaStream::Generic, "reasoning_summary"),
        RuntimeItemDeltaKind::Plan => (ItemDeltaStream::Generic, "plan"),
        RuntimeItemDeltaKind::Generic => (ItemDeltaStream::Generic, "generic"),
        RuntimeItemDeltaKind::FileChange => (ItemDeltaStream::FileChange, "file_change"),
        RuntimeItemDeltaKind::Stdout | RuntimeItemDeltaKind::Stderr => {
            return project_tool_output_delta(context, delta);
        }
        RuntimeItemDeltaKind::ToolProgress => (ItemDeltaStream::ToolProgress, "tool_progress"),
    };

    CLIRuntimeProjectedEvents::progress(AgentProgressEvent::ItemDelta {
        notification: ItemDeltaNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item_id: delta.native_item_id.clone(),
            delta: delta.delta.clone(),
            stream: Some(stream),
            payload: Some(item_delta_metadata(delta, runtime_delta_kind)),
            markdown: None,
            markdown_version: None,
        },
    })
}

fn project_tool_output_delta(
    context: &CLIRuntimeProjectorContext,
    delta: &RuntimeItemDelta,
) -> CLIRuntimeProjectedEvents {
    let stream = match delta.delta_kind {
        RuntimeItemDeltaKind::Stderr => ItemDeltaStream::Stderr,
        _ => ItemDeltaStream::Stdout,
    };
    CLIRuntimeProjectedEvents::progress(AgentProgressEvent::ToolOutputDelta {
        workspace_id: context.workspace_id.clone(),
        thread_id: context.thread_id.clone(),
        turn_id: context.turn_id.clone(),
        item_id: delta.native_item_id.clone(),
        stream,
        delta: delta.delta.clone(),
        payload: Some(item_delta_metadata(delta, "command_output")),
    })
}

fn project_item_completed(
    context: &CLIRuntimeProjectorContext,
    completed: &RuntimeItemCompleted,
) -> CLIRuntimeProjectedEvents {
    if is_user_message_kind(completed.item_kind.as_str()) {
        return CLIRuntimeProjectedEvents::ignored("native userMessage echo");
    }

    let item = completed_turn_item(completed);

    CLIRuntimeProjectedEvents::durable(AgentDurableEvent::ItemCompleted {
        notification: ItemCompletedNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item,
        },
    })
}

fn project_item_updated(
    context: &CLIRuntimeProjectorContext,
    updated: &RuntimeItemUpdated,
) -> CLIRuntimeProjectedEvents {
    let item = generic_agent_system_item(
        updated.native_item_id.as_str(),
        updated.item_kind.as_str(),
        "Agent item updated",
        "updated",
        SystemEventLevel::Info,
        "agent_runtime_item_updated",
        None,
    );

    CLIRuntimeProjectedEvents::durable(AgentDurableEvent::ItemCompleted {
        notification: ItemCompletedNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item,
        },
    })
}

fn project_plan_updated(
    context: &CLIRuntimeProjectorContext,
    plan: &RuntimePlanUpdated,
) -> CLIRuntimeProjectedEvents {
    let item_id = format!(
        "agent_plan_{}",
        normalize_kind(plan.native_turn_id.as_str())
    );
    let item = generic_agent_system_item(
        item_id.as_str(),
        "turn/plan/updated",
        "Plan updated",
        "updated",
        SystemEventLevel::Info,
        "agent_plan_updated",
        plan.plan_redacted.as_ref(),
    );

    CLIRuntimeProjectedEvents::durable(AgentDurableEvent::ItemCompleted {
        notification: ItemCompletedNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item,
        },
    })
}

fn project_diff_updated(
    context: &CLIRuntimeProjectorContext,
    diff: &RuntimeDiffUpdated,
) -> CLIRuntimeProjectedEvents {
    let item_id = agent_diff_item_id_for_native_turn_id(diff.native_turn_id.as_str());
    let item = generic_agent_system_item(
        item_id.as_str(),
        "turn/diff/updated",
        "Diff updated",
        "updated",
        SystemEventLevel::Info,
        "agent_diff_updated",
        diff.diff_redacted.as_ref(),
    );

    CLIRuntimeProjectedEvents::snapshot(AgentSnapshotEvent::ItemUpdated {
        notification: ItemUpdatedNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item,
        },
    })
}

pub(crate) fn agent_diff_item_id_for_native_turn_id(native_turn_id: &str) -> String {
    format!("agent_diff_{}", normalize_kind(native_turn_id))
}

fn project_thread_state_changed(
    context: &CLIRuntimeProjectorContext,
    thread: &RuntimeThreadStateChanged,
) -> CLIRuntimeProjectedEvents {
    let status = thread.status.trim();
    let status = if status.is_empty() { "changed" } else { status };
    let item_id = format!(
        "agent_thread_{}_{}",
        normalize_kind(context.turn_id.as_str()),
        normalize_kind(status)
    );
    let item = TurnItem::SystemEvent {
        id: item_id,
        level: SystemEventLevel::Info,
        message: format!("Thread status changed: {status}"),
        code: Some("agent_thread_status_changed".to_owned()),
        details: Some(json!({
            "status": status,
            "nativeThreadId": thread.native_thread_id.as_deref(),
        })),
    };

    CLIRuntimeProjectedEvents::durable(AgentDurableEvent::ItemCompleted {
        notification: ItemCompletedNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item,
        },
    })
}

fn project_raw_event(
    context: &CLIRuntimeProjectorContext,
    raw: &RuntimeRawEvent,
) -> CLIRuntimeProjectedEvents {
    let native_turn_id = if let Some(native_turn_id) = raw.native_turn_id.as_deref() {
        Some(native_turn_id)
    } else if runtime_raw_event_is_timeline_worthy(raw.native_method.as_str()) {
        None
    } else {
        return CLIRuntimeProjectedEvents::ignored(format!(
            "unsupported raw event {}",
            raw.native_method
        ));
    };
    let native_item_or_method = raw
        .native_item_id
        .as_deref()
        .unwrap_or(raw.native_method.as_str());
    let item_id = format!(
        "agent_event_{}_{}",
        normalize_kind(native_turn_id.unwrap_or(context.turn_id.as_str())),
        normalize_kind(native_item_or_method)
    );
    let mut details = JsonMap::new();
    details.insert("nativeMethod".to_owned(), json!(raw.native_method));
    details.insert("reason".to_owned(), json!(raw.reason));
    if let Some(native_turn_id) = native_turn_id {
        details.insert("nativeTurnId".to_owned(), json!(native_turn_id));
    }
    if let Some(native_thread_id) = raw.native_thread_id.as_ref() {
        details.insert("nativeThreadId".to_owned(), json!(native_thread_id));
    }
    if let Some(native_item_id) = raw.native_item_id.as_ref() {
        details.insert("nativeItemId".to_owned(), json!(native_item_id));
    }
    if let Some(payload) = raw.payload_redacted.as_ref() {
        details.insert("payload".to_owned(), payload.clone());
    }
    let item = TurnItem::SystemEvent {
        id: item_id,
        level: SystemEventLevel::Info,
        message: format!("Runtime event: {}", raw.native_method),
        code: Some("agent_runtime_event".to_owned()),
        details: Some(JsonValue::Object(details)),
    };

    CLIRuntimeProjectedEvents::durable(AgentDurableEvent::ItemCompleted {
        notification: ItemCompletedNotification {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            item,
        },
    })
}

fn started_turn_item(
    kind: &str,
    item_id: &str,
    title: Option<&str>,
    phase: RuntimeAgentMessagePhase,
    metadata: Option<&JsonValue>,
) -> TurnItem {
    if is_agent_message_kind(kind) {
        return TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: title.unwrap_or_default().to_owned(),
            phase: agent_message_phase(phase),
            markdown: None,
            markdown_version: None,
        };
    }
    if is_reasoning_kind(kind) {
        return TurnItem::Reasoning {
            id: item_id.to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        };
    }
    if is_command_kind(kind) {
        return command_execution_item(item_id, metadata, ToolCallStatus::InProgress);
    }
    if is_file_change_kind(kind) {
        return file_change_item(item_id, metadata, ToolCallStatus::InProgress);
    }
    if is_web_search_kind(kind) {
        return web_search_item(item_id, metadata, ToolCallStatus::InProgress);
    }
    if is_mcp_tool_kind(kind) {
        return mcp_tool_item(item_id, metadata, ToolCallStatus::InProgress);
    }
    if is_dynamic_tool_kind(kind) {
        return dynamic_tool_item(item_id, metadata, ToolCallStatus::InProgress);
    }
    if is_plan_kind(kind) {
        return generic_agent_system_item(
            item_id,
            kind,
            title.unwrap_or("Plan started"),
            "started",
            SystemEventLevel::Info,
            "agent_plan",
            metadata,
        );
    }
    if is_context_compaction_kind(kind) {
        return context_compaction_item(
            item_id,
            SystemEventLevel::Info,
            "Context compaction started",
            "started",
            metadata,
        );
    }
    if is_collab_tool_kind(kind) {
        return collab_tool_item(item_id, metadata, ToolCallStatus::InProgress);
    }
    if is_image_view_kind(kind) {
        return image_view_item(item_id, metadata, ToolCallStatus::InProgress);
    }
    generic_agent_system_item(
        item_id,
        kind,
        title.unwrap_or("Agent item started"),
        "started",
        SystemEventLevel::Info,
        "agent_runtime_item",
        metadata,
    )
}

fn completed_turn_item(completed: &RuntimeItemCompleted) -> TurnItem {
    if is_agent_message_kind(completed.item_kind.as_str()) {
        return TurnItem::AgentMessage {
            id: completed.native_item_id.clone(),
            text: completed.text.clone().unwrap_or_default(),
            phase: agent_message_phase(completed.phase),
            markdown: None,
            markdown_version: None,
        };
    }
    if is_reasoning_kind(completed.item_kind.as_str()) {
        return TurnItem::Reasoning {
            id: completed.native_item_id.clone(),
            summary: completed.summary.clone(),
            content: completed.content.clone(),
        };
    }
    if is_command_kind(completed.item_kind.as_str()) {
        return command_execution_item(
            completed.native_item_id.as_str(),
            completed.metadata.as_ref(),
            terminal_tool_status(completed.metadata.as_ref()),
        );
    }
    if is_file_change_kind(completed.item_kind.as_str()) {
        return file_change_item(
            completed.native_item_id.as_str(),
            completed.metadata.as_ref(),
            terminal_tool_status(completed.metadata.as_ref()),
        );
    }
    if is_web_search_kind(completed.item_kind.as_str()) {
        return web_search_item(
            completed.native_item_id.as_str(),
            completed.metadata.as_ref(),
            terminal_tool_status(completed.metadata.as_ref()),
        );
    }
    if is_mcp_tool_kind(completed.item_kind.as_str()) {
        return mcp_tool_item(
            completed.native_item_id.as_str(),
            completed.metadata.as_ref(),
            terminal_tool_status(completed.metadata.as_ref()),
        );
    }
    if is_dynamic_tool_kind(completed.item_kind.as_str()) {
        return dynamic_tool_item(
            completed.native_item_id.as_str(),
            completed.metadata.as_ref(),
            terminal_tool_status(completed.metadata.as_ref()),
        );
    }
    if is_plan_kind(completed.item_kind.as_str()) {
        let message = completed
            .text
            .clone()
            .or_else(|| metadata_string(completed.metadata.as_ref(), "message"))
            .unwrap_or_else(|| "Plan completed".to_owned());
        return generic_agent_system_item(
            completed.native_item_id.as_str(),
            completed.item_kind.as_str(),
            message.as_str(),
            "completed",
            SystemEventLevel::Info,
            "agent_plan",
            completed.metadata.as_ref(),
        );
    }
    if is_context_compaction_kind(completed.item_kind.as_str()) {
        let failed = metadata_failure(completed.metadata.as_ref());
        return context_compaction_item(
            completed.native_item_id.as_str(),
            if failed {
                SystemEventLevel::Error
            } else {
                SystemEventLevel::Info
            },
            completed.text.as_deref().unwrap_or(if failed {
                "Context compaction failed"
            } else {
                "Context compaction completed"
            }),
            if failed { "failed" } else { "completed" },
            completed.metadata.as_ref(),
        );
    }
    if is_collab_tool_kind(completed.item_kind.as_str()) {
        return collab_tool_item(
            completed.native_item_id.as_str(),
            completed.metadata.as_ref(),
            terminal_tool_status(completed.metadata.as_ref()),
        );
    }
    if is_image_view_kind(completed.item_kind.as_str()) {
        return image_view_item(
            completed.native_item_id.as_str(),
            completed.metadata.as_ref(),
            terminal_tool_status(completed.metadata.as_ref()),
        );
    }
    generic_agent_system_item(
        completed.native_item_id.as_str(),
        completed.item_kind.as_str(),
        completed.text.as_deref().unwrap_or("Agent item completed"),
        "completed",
        if metadata_failure(completed.metadata.as_ref()) {
            SystemEventLevel::Error
        } else {
            SystemEventLevel::Info
        },
        "agent_runtime_item",
        completed.metadata.as_ref(),
    )
}

fn context_compaction_item(
    item_id: &str,
    level: SystemEventLevel,
    message: &str,
    status: &str,
    metadata: Option<&JsonValue>,
) -> TurnItem {
    let mut details = JsonMap::new();
    details.insert("status".to_owned(), json!(status));
    details.insert("nativeItemKind".to_owned(), json!("contextCompaction"));
    if let Some(metadata) = metadata
        && let Some(object) = metadata.as_object()
    {
        for (key, value) in object {
            details.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    TurnItem::SystemEvent {
        id: item_id.to_owned(),
        level,
        message: message.to_owned(),
        code: Some("agent_context_compaction".to_owned()),
        details: Some(JsonValue::Object(details)),
    }
}

fn command_execution_item(
    item_id: &str,
    metadata: Option<&JsonValue>,
    status: ToolCallStatus,
) -> TurnItem {
    let tool_name =
        metadata_string(metadata, "toolName").unwrap_or_else(|| "exec_command".to_owned());
    let command = metadata_string_array(metadata, "command");
    let cwd = metadata_string(metadata, "cwd");
    let stdout = metadata_string(metadata, "stdout");
    let stderr = metadata_string(metadata, "stderr");
    let aggregated_output = aggregate_shell_output(stdout.as_deref(), stderr.as_deref());
    let exit_code = metadata_i32(metadata, "exitCode");
    let success = metadata_success(metadata);
    let display = ToolDisplayPayload::Shell {
        stdout: stdout.clone(),
        stderr: stderr.clone(),
        aggregated_output: aggregated_output.clone(),
        exit_code,
        duration_ms: None,
        timed_out: None,
        truncated: false,
    };
    let storage = ToolStoragePayload::Shell {
        stdout,
        stderr,
        aggregated_output,
        exit_code,
        duration_ms: None,
        timed_out: None,
        truncated: false,
    };

    TurnItem::CommandExecution {
        id: item_id.to_owned(),
        tool_name: tool_name.clone(),
        arguments: tool_arguments(
            item_id,
            json!({
                "command": command,
                "cwd": cwd,
            }),
        ),
        status,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name(tool_name.as_str()),
        display,
        storage,
        recovery: None,
        command,
        cwd,
        success,
        outcome: None,
        observation: None,
    }
}

fn file_change_item(
    item_id: &str,
    metadata: Option<&JsonValue>,
    status: ToolCallStatus,
) -> TurnItem {
    let tool_name =
        metadata_string(metadata, "toolName").unwrap_or_else(|| "apply_patch".to_owned());
    let changed_files = metadata_string_array(metadata, "changedFiles");
    let exit_code = metadata_i32(metadata, "exitCode");
    let success = metadata_success(metadata);
    let metadata_json = metadata.cloned().unwrap_or_else(|| json!({}));
    let summary = ToolOutputSummary {
        title: file_change_title(changed_files.len()),
        lines: changed_files.clone(),
        metadata: ToolMetadata::from_json(metadata_json.clone()),
        truncated: false,
    };
    let output_policy = ToolOutputPolicySnapshot::for_tool_name(tool_name.as_str());
    let (display, storage) = summary_payloads_for_policy(&output_policy, &summary);

    TurnItem::FileChange {
        id: item_id.to_owned(),
        tool_name: tool_name.clone(),
        arguments: tool_arguments(item_id, metadata_json),
        status,
        recovery_policy: None,
        output_policy,
        display,
        storage,
        recovery: None,
        changed_files,
        exit_code,
        stdout: metadata_string(metadata, "stdout"),
        stderr: metadata_string(metadata, "stderr"),
        success,
        outcome: None,
        observation: None,
    }
}

fn web_search_item(
    item_id: &str,
    metadata: Option<&JsonValue>,
    status: ToolCallStatus,
) -> TurnItem {
    let metadata_json = metadata.cloned().unwrap_or_else(|| json!({}));
    let query = metadata_string(metadata, "query").or_else(|| {
        metadata_string_array(metadata, "queries")
            .into_iter()
            .find(|query| !query.is_empty())
    });
    let result_count = metadata_usize(metadata, "resultCount");
    let took_ms = metadata_u64(metadata, "durationMs");
    let mut lines = Vec::new();
    if let Some(query) = query.as_ref() {
        lines.push(query.clone());
    }
    if let Some(result_count) = result_count {
        lines.push(format!("{result_count} result(s)"));
    }
    let summary = tool_summary(
        if status == ToolCallStatus::InProgress {
            "Web search"
        } else {
            "Web search completed"
        },
        lines,
        metadata_json.clone(),
    );

    TurnItem::WebSearch {
        id: item_id.to_owned(),
        tool_name: "web_search".to_owned(),
        arguments: tool_arguments(item_id, metadata_json),
        status,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("web_search"),
        display: ToolDisplayPayload::Summary(summary.clone()),
        storage: ToolStoragePayload::Summary(summary),
        recovery: None,
        query,
        provider: metadata_string(metadata, "provider"),
        took_ms,
        result_count,
        results: Vec::new(),
        success: metadata_success(metadata),
        outcome: None,
        observation: None,
    }
}

fn dynamic_tool_item(
    item_id: &str,
    metadata: Option<&JsonValue>,
    status: ToolCallStatus,
) -> TurnItem {
    let metadata_json = metadata.cloned().unwrap_or_else(|| json!({}));
    let tool_name =
        normalized_dynamic_tool_name(metadata).unwrap_or_else(|| "dynamic_tool_call".to_owned());
    let arguments = metadata_json
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| metadata_json.clone());
    let lines = metadata_string(metadata, "message").into_iter().collect();
    let summary = tool_summary(
        format!("Dynamic tool: {tool_name}"),
        lines,
        metadata_json.clone(),
    );
    let output_policy =
        ToolOutputPolicySnapshot::for_external_runtime_tool_name(tool_name.as_str());
    let (display, storage) = summary_payloads_for_policy(&output_policy, &summary);

    TurnItem::DynamicToolCall {
        id: item_id.to_owned(),
        tool_name: tool_name.clone(),
        arguments: tool_arguments(item_id, arguments),
        status,
        recovery_policy: None,
        output_policy,
        display,
        storage,
        recovery: None,
        success: metadata_success(metadata),
        outcome: None,
        observation: None,
    }
}

fn mcp_tool_item(item_id: &str, metadata: Option<&JsonValue>, status: ToolCallStatus) -> TurnItem {
    let metadata_json = metadata.cloned().unwrap_or_else(|| json!({}));
    let tool_name = metadata_string(metadata, "canonicalCallableName")
        .unwrap_or_else(|| "mcp_tool_call".to_owned());
    let server_name = metadata_string(metadata, "serverName").unwrap_or_else(|| "MCP".to_owned());
    let raw_tool_name =
        metadata_string(metadata, "rawToolName").unwrap_or_else(|| tool_name.clone());
    let arguments = metadata_json
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let mut lines = Vec::new();
    if let Some(message) = metadata_string(metadata, "message") {
        lines.push(message);
    }
    if let Some(error) = metadata_json
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(JsonValue::as_str)
    {
        lines.push(error.to_owned());
    }
    let summary = tool_summary(
        format!("MCP: {server_name} / {raw_tool_name}"),
        lines,
        metadata_json.clone(),
    );
    let output_policy =
        ToolOutputPolicySnapshot::for_external_runtime_tool_name(tool_name.as_str());
    let (display, storage) = summary_payloads_for_policy(&output_policy, &summary);
    let success = metadata_success(metadata).or_else(|| match status {
        ToolCallStatus::Completed => Some(true),
        ToolCallStatus::Failed => Some(false),
        _ => None,
    });

    TurnItem::DynamicToolCall {
        id: item_id.to_owned(),
        tool_name: tool_name.clone(),
        arguments: tool_arguments(item_id, arguments),
        status,
        recovery_policy: None,
        output_policy,
        display,
        storage,
        recovery: None,
        success,
        outcome: None,
        observation: None,
    }
}

fn collab_tool_item(
    item_id: &str,
    metadata: Option<&JsonValue>,
    status: ToolCallStatus,
) -> TurnItem {
    let tool = metadata_string(metadata, "tool").unwrap_or_else(|| "collaboration".to_owned());
    dynamic_tool_item_with_name(item_id, format!("collab:{tool}").as_str(), metadata, status)
}

fn image_view_item(
    item_id: &str,
    metadata: Option<&JsonValue>,
    status: ToolCallStatus,
) -> TurnItem {
    dynamic_tool_item_with_name(item_id, "image_view", metadata, status)
}

fn dynamic_tool_item_with_name(
    item_id: &str,
    tool_name: &str,
    metadata: Option<&JsonValue>,
    status: ToolCallStatus,
) -> TurnItem {
    let metadata_json = metadata.cloned().unwrap_or_else(|| json!({}));
    let arguments = metadata_json
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| metadata_json.clone());
    let lines = metadata_string(metadata, "message")
        .or_else(|| metadata_string(metadata, "path"))
        .into_iter()
        .collect();
    let summary = tool_summary(format!("Tool: {tool_name}"), lines, metadata_json.clone());
    let output_policy = ToolOutputPolicySnapshot::for_external_runtime_tool_name(tool_name);
    let (display, storage) = summary_payloads_for_policy(&output_policy, &summary);

    TurnItem::DynamicToolCall {
        id: item_id.to_owned(),
        tool_name: tool_name.to_owned(),
        arguments: tool_arguments(item_id, arguments),
        status,
        recovery_policy: None,
        output_policy,
        display,
        storage,
        recovery: None,
        success: metadata_success(metadata),
        outcome: None,
        observation: None,
    }
}

fn normalized_dynamic_tool_name(metadata: Option<&JsonValue>) -> Option<String> {
    let tool =
        metadata_string(metadata, "toolName").or_else(|| metadata_string(metadata, "tool"))?;
    let tool = tool.trim();
    if tool.is_empty() {
        return None;
    }
    if let Some(namespace) = metadata_string(metadata, "namespace")
        .map(|namespace| namespace.trim().to_owned())
        .filter(|namespace| !namespace.is_empty())
    {
        return Some(format!("{namespace}:{tool}"));
    }
    Some(tool.to_owned())
}

fn summary_payloads_for_policy(
    output_policy: &ToolOutputPolicySnapshot,
    summary: &ToolOutputSummary,
) -> (ToolDisplayPayload, ToolStoragePayload) {
    let display = match &output_policy.timeline {
        TimelineOutputPolicy::Full { .. } | TimelineOutputPolicy::Summary { .. } => {
            ToolDisplayPayload::Summary(summary.clone())
        }
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {
            ToolDisplayPayload::Hidden
        }
    };
    let storage = match &output_policy.storage {
        StorageOutputPolicy::Full { .. } | StorageOutputPolicy::Summary { .. } => {
            ToolStoragePayload::Summary(summary.clone())
        }
        StorageOutputPolicy::MetadataOnly => ToolStoragePayload::Metadata {
            metadata: summary.metadata.clone(),
        },
        StorageOutputPolicy::None => ToolStoragePayload::None,
    };
    (display, storage)
}

fn generic_agent_system_item(
    item_id: &str,
    native_kind: &str,
    message: &str,
    status: &str,
    level: SystemEventLevel,
    code: &str,
    metadata: Option<&JsonValue>,
) -> TurnItem {
    let mut details = JsonMap::new();
    details.insert("status".to_owned(), json!(status));
    details.insert("nativeItemKind".to_owned(), json!(native_kind));
    if let Some(metadata) = metadata {
        if let Some(object) = metadata.as_object() {
            for (key, value) in object {
                details.entry(key.clone()).or_insert_with(|| value.clone());
            }
        } else {
            details.insert("payload".to_owned(), metadata.clone());
        }
    }

    TurnItem::SystemEvent {
        id: item_id.to_owned(),
        level,
        message: if message.trim().is_empty() {
            "Agent event".to_owned()
        } else {
            message.to_owned()
        },
        code: Some(code.to_owned()),
        details: Some(JsonValue::Object(details)),
    }
}

fn tool_summary(
    title: impl Into<String>,
    lines: Vec<String>,
    metadata_json: JsonValue,
) -> ToolOutputSummary {
    ToolOutputSummary {
        title: title.into(),
        lines,
        metadata: ToolMetadata::from_json(metadata_json),
        truncated: false,
    }
}

fn tool_arguments(item_id: &str, value: JsonValue) -> JsonValue {
    let mut object = value.as_object().cloned().unwrap_or_default();
    object.insert("nativeItemId".to_owned(), json!(item_id));
    JsonValue::Object(object)
}

fn aggregate_shell_output(stdout: Option<&str>, stderr: Option<&str>) -> Option<String> {
    let mut output = String::new();
    if let Some(stdout) = stdout
        && !stdout.is_empty()
    {
        output.push_str(stdout);
    }
    if let Some(stderr) = stderr
        && !stderr.is_empty()
    {
        if !output.is_empty() && !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str(stderr);
    }
    (!output.is_empty()).then_some(output)
}

fn terminal_tool_status(metadata: Option<&JsonValue>) -> ToolCallStatus {
    if matches!(metadata_success(metadata), Some(false)) {
        return ToolCallStatus::Failed;
    }
    match metadata_string(metadata, "status")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "failed" | "error" | "cancelled" | "canceled" | "interrupted" => ToolCallStatus::Failed,
        _ => ToolCallStatus::Completed,
    }
}

fn metadata_success(metadata: Option<&JsonValue>) -> Option<bool> {
    metadata_bool(metadata, "success")
        .or_else(|| metadata_i32(metadata, "exitCode").map(|code| code == 0))
}

fn metadata_failure(metadata: Option<&JsonValue>) -> bool {
    if matches!(metadata_success(metadata), Some(false)) {
        return true;
    }
    matches!(
        metadata_string(metadata, "status")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "failed" | "error" | "cancelled" | "canceled" | "interrupted"
    )
}

fn file_change_title(changed_file_count: usize) -> String {
    match changed_file_count {
        0 => "File changes".to_owned(),
        1 => "Changed 1 file".to_owned(),
        count => format!("Changed {count} files"),
    }
}

fn review_system_event_item(
    context: &CLIRuntimeProjectorContext,
    review: &RuntimeReviewModeChanged,
) -> TurnItem {
    let native_turn_or_context = review
        .native_turn_id
        .as_deref()
        .unwrap_or(context.turn_id.as_str());
    let item_id = format!("agent_review_{}", normalize_kind(native_turn_or_context));
    let message = if review.status == "entered" || review.status == "started" {
        review
            .message
            .clone()
            .unwrap_or_else(|| "Review started".to_owned())
    } else {
        review
            .message
            .clone()
            .unwrap_or_else(|| "Review completed".to_owned())
    };
    TurnItem::SystemEvent {
        id: item_id,
        level: SystemEventLevel::Info,
        message,
        code: Some("agent_review".to_owned()),
        details: Some(json!({
            "status": review.status.as_str(),
            "nativeThreadId": review.native_thread_id.as_deref(),
            "nativeTurnId": review.native_turn_id.as_deref(),
        })),
    }
}

fn is_agent_message_kind(kind: &str) -> bool {
    matches!(
        normalize_kind(kind).as_str(),
        "agentmessage" | "assistantmessage" | "message"
    )
}

fn agent_message_phase(phase: RuntimeAgentMessagePhase) -> AgentMessagePhase {
    match phase {
        RuntimeAgentMessagePhase::Commentary => AgentMessagePhase::Commentary,
        RuntimeAgentMessagePhase::FinalAnswer => AgentMessagePhase::FinalAnswer,
    }
}

fn is_user_message_kind(kind: &str) -> bool {
    matches!(normalize_kind(kind).as_str(), "usermessage" | "user")
}

fn is_reasoning_kind(kind: &str) -> bool {
    normalize_kind(kind) == "reasoning"
}

fn is_command_kind(kind: &str) -> bool {
    matches!(
        normalize_kind(kind).as_str(),
        "commandexecution" | "command" | "exec" | "shell" | "terminal"
    )
}

fn is_file_change_kind(kind: &str) -> bool {
    matches!(
        normalize_kind(kind).as_str(),
        "filechange" | "filechanges" | "patch" | "applypatch"
    )
}

fn is_web_search_kind(kind: &str) -> bool {
    matches!(normalize_kind(kind).as_str(), "websearch" | "search")
}

fn is_dynamic_tool_kind(kind: &str) -> bool {
    matches!(
        normalize_kind(kind).as_str(),
        "dynamictoolcall" | "dynamictool" | "toolcall"
    )
}

fn is_mcp_tool_kind(kind: &str) -> bool {
    normalize_kind(kind) == "mcptoolcall"
}

fn is_collab_tool_kind(kind: &str) -> bool {
    matches!(
        normalize_kind(kind).as_str(),
        "collabtoolcall" | "collabtool" | "collaborationtoolcall"
    )
}

fn is_image_view_kind(kind: &str) -> bool {
    matches!(
        normalize_kind(kind).as_str(),
        "imageview" | "viewimage" | "image"
    )
}

fn is_plan_kind(kind: &str) -> bool {
    normalize_kind(kind) == "plan"
}

fn is_context_compaction_kind(kind: &str) -> bool {
    matches!(
        normalize_kind(kind).as_str(),
        "contextcompaction" | "contextcompact" | "compaction"
    )
}

fn runtime_raw_event_is_timeline_worthy(method: &str) -> bool {
    matches!(
        method,
        "thread/tokenUsage/updated"
            | "fuzzyFileSearch/sessionUpdated"
            | "fuzzyFileSearch/sessionCompleted"
            | "windowsSandbox/setupCompleted"
    )
}

fn normalize_kind(kind: &str) -> String {
    kind.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn item_delta_metadata(delta: &RuntimeItemDelta, runtime_delta_kind: &str) -> JsonValue {
    let mut metadata = JsonMap::new();
    metadata.insert("runtimeDeltaKind".to_owned(), json!(runtime_delta_kind));
    metadata.insert("nativeTurnId".to_owned(), json!(delta.native_turn_id));
    metadata.insert("nativeItemId".to_owned(), json!(delta.native_item_id));
    metadata.insert("nativeItemKind".to_owned(), json!(delta.item_kind));
    if let Some(native_thread_id) = delta.native_thread_id.as_ref() {
        metadata.insert("nativeThreadId".to_owned(), json!(native_thread_id));
    }
    if let Some(runtime_metadata) = delta.metadata.as_ref()
        && let Some(object) = runtime_metadata.as_object()
    {
        for (key, value) in object {
            metadata.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    JsonValue::Object(metadata)
}

fn metadata_string(metadata: Option<&JsonValue>, key: &str) -> Option<String> {
    metadata
        .and_then(|metadata| metadata.get(key))
        .and_then(|value| match value {
            JsonValue::String(value) => Some(value.clone()),
            JsonValue::Number(value) => Some(value.to_string()),
            JsonValue::Bool(value) => Some(value.to_string()),
            _ => None,
        })
}

fn metadata_bool(metadata: Option<&JsonValue>, key: &str) -> Option<bool> {
    metadata
        .and_then(|metadata| metadata.get(key))
        .and_then(JsonValue::as_bool)
}

fn metadata_i32(metadata: Option<&JsonValue>, key: &str) -> Option<i32> {
    metadata
        .and_then(|metadata| metadata.get(key))
        .and_then(|value| match value {
            JsonValue::Number(value) => value.as_i64().and_then(|value| i32::try_from(value).ok()),
            JsonValue::String(value) => value.parse::<i32>().ok(),
            _ => None,
        })
}

fn metadata_u64(metadata: Option<&JsonValue>, key: &str) -> Option<u64> {
    metadata
        .and_then(|metadata| metadata.get(key))
        .and_then(|value| match value {
            JsonValue::Number(value) => value
                .as_u64()
                .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok())),
            JsonValue::String(value) => value.parse::<u64>().ok(),
            _ => None,
        })
}

fn metadata_usize(metadata: Option<&JsonValue>, key: &str) -> Option<usize> {
    metadata_u64(metadata, key).and_then(|value| usize::try_from(value).ok())
}

fn metadata_string_array(metadata: Option<&JsonValue>, key: &str) -> Vec<String> {
    let Some(value) = metadata.and_then(|metadata| metadata.get(key)) else {
        return Vec::new();
    };
    match value {
        JsonValue::Array(values) => values
            .iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        JsonValue::String(value) if !value.is_empty() => vec![value.clone()],
        _ => Vec::new(),
    }
}

fn tool_status_label(status: ToolCallStatus) -> &'static str {
    match status {
        ToolCallStatus::InProgress => "started",
        ToolCallStatus::Completed => "completed",
        ToolCallStatus::Failed => "failed",
    }
}

fn system_level_for_tool_status(status: ToolCallStatus) -> SystemEventLevel {
    match status {
        ToolCallStatus::Failed => SystemEventLevel::Error,
        ToolCallStatus::InProgress | ToolCallStatus::Completed => SystemEventLevel::Info,
    }
}

fn event_kind_name(event: &RuntimeEvent) -> &'static str {
    match event {
        RuntimeEvent::SessionStateChanged(_) => "session_state_changed",
        RuntimeEvent::ThreadStateChanged(_) => "thread_state_changed",
        RuntimeEvent::ThreadGoalUpdated(_) => "thread_goal_updated",
        RuntimeEvent::ThreadGoalCleared(_) => "thread_goal_cleared",
        RuntimeEvent::TurnStarted(_) => "turn_started",
        RuntimeEvent::TurnCompleted(_) => "turn_completed",
        RuntimeEvent::TurnFailed(_) => "turn_failed",
        RuntimeEvent::TurnInterrupted(_) => "turn_interrupted",
        RuntimeEvent::TurnRetrying(_) => "turn_retrying",
        RuntimeEvent::ItemStarted(_) => "item_started",
        RuntimeEvent::ItemDelta(_) => "item_delta",
        RuntimeEvent::ItemCompleted(_) => "item_completed",
        RuntimeEvent::ItemUpdated(_) => "item_updated",
        RuntimeEvent::PlanUpdated(_) => "plan_updated",
        RuntimeEvent::DiffUpdated(_) => "diff_updated",
        RuntimeEvent::RequestOpened(_) => "request_opened",
        RuntimeEvent::RequestResolved(_) => "request_resolved",
        RuntimeEvent::AccountUpdated(_) => "account_updated",
        RuntimeEvent::AppListUpdated(_) => "app_list_updated",
        RuntimeEvent::ReviewModeChanged(_) => "review_mode_changed",
        RuntimeEvent::Error(_) => "error",
        RuntimeEvent::Raw(_) => "raw",
    }
}

#[cfg(test)]
mod tests {
    use super::{CLIRuntimeProjectorContext, project_cli_runtime_event};
    use pioneer_cli_agent_runtime::codex::CodexJsonlRpcNotificationEvent;
    use pioneer_cli_agent_runtime::event::{
        RuntimeAgentMessagePhase, RuntimeEvent, RuntimeEventMappingOptions, RuntimeItemCompleted,
        RuntimeItemDelta, RuntimeItemDeltaKind, RuntimeItemStarted, RuntimeRawEvent,
        RuntimeTurnCompleted, map_codex_notification_event,
    };
    use pioneer_protocol::{
        AgentDurableEvent, AgentMessagePhase, AgentProgressEvent, ItemDeltaStream,
        StorageOutputPolicy, SystemEventLevel, TimelineOutputPolicy, ToolDisplayPayload,
        ToolStoragePayload, TurnItem,
    };
    use serde_json::{Value as JsonValue, json};

    fn context() -> CLIRuntimeProjectorContext {
        CLIRuntimeProjectorContext {
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            recovery: None,
        }
    }

    fn project_codex_notification(
        method: &str,
        params: JsonValue,
    ) -> super::CLIRuntimeProjectedEvents {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: method.to_owned(),
                params: Some(params),
                raw: json!({"method": method}),
            },
            RuntimeEventMappingOptions::default(),
        );
        project_cli_runtime_event(&context(), &event)
    }

    #[test]
    fn cli_runtime_projector_projects_turn_completed() {
        let projected = project_cli_runtime_event(
            &context(),
            &RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                native_thread_id: Some("native_thread_1".to_owned()),
                native_turn_id: "native_turn_1".to_owned(),
                status: "completed".to_owned(),
                native: None,
            }),
        );

        assert!(projected.progress.is_empty());
        assert!(projected.ignored.is_empty());
        assert_eq!(
            projected.durable,
            vec![AgentDurableEvent::TurnCompleted {
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                recovery: None,
            }]
        );
    }

    #[test]
    fn cli_runtime_projector_projects_agent_message_delta() {
        let projected = project_cli_runtime_event(
            &context(),
            &RuntimeEvent::ItemDelta(RuntimeItemDelta {
                native_thread_id: Some("native_thread_1".to_owned()),
                native_turn_id: "native_turn_1".to_owned(),
                native_item_id: "item_1".to_owned(),
                item_kind: "agentMessage".to_owned(),
                delta_kind: RuntimeItemDeltaKind::AgentMessage,
                delta: "hello".to_owned(),
                metadata: None,
                native: None,
            }),
        );

        assert!(projected.durable.is_empty());
        assert!(projected.ignored.is_empty());
        assert_eq!(projected.progress.len(), 1);
        let AgentProgressEvent::ItemDelta { notification } = &projected.progress[0] else {
            panic!("expected item delta");
        };
        assert_eq!(notification.workspace_id, "workspace_1");
        assert_eq!(notification.thread_id, "thread_1");
        assert_eq!(notification.turn_id, "turn_1");
        assert_eq!(notification.item_id, "item_1");
        assert_eq!(notification.delta, "hello");
        assert_eq!(notification.stream, Some(ItemDeltaStream::AgentMessage));
        assert_eq!(
            notification
                .payload
                .as_ref()
                .and_then(|payload| payload.get("nativeItemId")),
            Some(&json!("item_1"))
        );
        assert_eq!(
            notification
                .payload
                .as_ref()
                .and_then(|payload| payload.get("runtimeDeltaKind")),
            Some(&json!("agent_message"))
        );
    }

    #[test]
    fn cli_runtime_projector_ignores_raw_unknown_event() {
        let projected = project_cli_runtime_event(
            &context(),
            &RuntimeEvent::Raw(RuntimeRawEvent {
                native_method: "future/event".to_owned(),
                reason: "unsupported".to_owned(),
                native_thread_id: None,
                native_turn_id: None,
                native_item_id: None,
                payload_redacted: None,
                raw_redacted: None,
            }),
        );

        assert!(projected.durable.is_empty());
        assert!(projected.progress.is_empty());
        assert_eq!(
            projected.ignored,
            vec!["unsupported raw event future/event"]
        );
    }

    #[test]
    fn cli_runtime_projector_projects_review_mode_events_to_system_item() {
        let entered = project_codex_notification(
            "enteredReviewMode",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_review_turn_1"
            }),
        );
        let exited = project_codex_notification(
            "exitedReviewMode",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_review_turn_1",
                "status": "completed"
            }),
        );

        let AgentDurableEvent::ItemStarted {
            notification: started,
        } = &entered.durable[0]
        else {
            panic!("expected review system item start");
        };
        let TurnItem::SystemEvent {
            id,
            code,
            message,
            details,
            ..
        } = &started.item
        else {
            panic!("expected system event item");
        };
        assert_eq!(id, "agent_review_nativereviewturn1");
        assert_eq!(code.as_deref(), Some("agent_review"));
        assert_eq!(message, "Review started");
        assert_eq!(
            details.as_ref().and_then(|details| details.get("status")),
            Some(&json!("entered"))
        );

        let AgentDurableEvent::ItemCompleted {
            notification: completed,
        } = &exited.durable[0]
        else {
            panic!("expected review system item completion");
        };
        let TurnItem::SystemEvent {
            id,
            code,
            message,
            details,
            ..
        } = &completed.item
        else {
            panic!("expected completed system event item");
        };
        assert_eq!(id, "agent_review_nativereviewturn1");
        assert_eq!(code.as_deref(), Some("agent_review"));
        assert_eq!(message, "Review completed");
        assert_eq!(
            details.as_ref().and_then(|details| details.get("status")),
            Some(&json!("completed"))
        );
    }

    #[test]
    fn cli_runtime_projector_projects_context_compaction_to_system_item() {
        let started = project_codex_notification(
            "item/started",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_compaction_1",
                    "type": "contextCompaction",
                    "compressedTokens": 1200
                }
            }),
        );
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_compaction_1",
                    "type": "contextCompaction",
                    "text": "Context compacted",
                    "status": "completed",
                    "compressedTokens": 1200
                }
            }),
        );
        let failed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_compaction_2",
                    "type": "contextCompaction",
                    "message": "Compaction failed",
                    "status": "failed"
                }
            }),
        );

        let AgentDurableEvent::ItemStarted {
            notification: started,
        } = &started.durable[0]
        else {
            panic!("expected compaction system item start");
        };
        let TurnItem::SystemEvent {
            id,
            level,
            code,
            message,
            details,
        } = &started.item
        else {
            panic!("expected started system event item");
        };
        assert_eq!(id, "native_compaction_1");
        assert_eq!(*level, SystemEventLevel::Info);
        assert_eq!(code.as_deref(), Some("agent_context_compaction"));
        assert_eq!(message, "Context compaction started");
        assert_eq!(
            details.as_ref().and_then(|details| details.get("status")),
            Some(&json!("started"))
        );
        assert_eq!(
            details
                .as_ref()
                .and_then(|details| details.get("compressedTokens")),
            Some(&json!(1200))
        );

        let AgentDurableEvent::ItemCompleted {
            notification: completed,
        } = &completed.durable[0]
        else {
            panic!("expected compaction system item completion");
        };
        let TurnItem::SystemEvent {
            level,
            code,
            message,
            details,
            ..
        } = &completed.item
        else {
            panic!("expected completed system event item");
        };
        assert_eq!(*level, SystemEventLevel::Info);
        assert_eq!(code.as_deref(), Some("agent_context_compaction"));
        assert_eq!(message, "Context compacted");
        assert_eq!(
            details.as_ref().and_then(|details| details.get("status")),
            Some(&json!("completed"))
        );

        let AgentDurableEvent::ItemCompleted {
            notification: failed,
        } = &failed.durable[0]
        else {
            panic!("expected failed compaction system item completion");
        };
        let TurnItem::SystemEvent {
            level,
            code,
            message,
            details,
            ..
        } = &failed.item
        else {
            panic!("expected failed system event item");
        };
        assert_eq!(*level, SystemEventLevel::Error);
        assert_eq!(code.as_deref(), Some("agent_context_compaction"));
        assert_eq!(message, "Compaction failed");
        assert_eq!(
            details.as_ref().and_then(|details| details.get("status")),
            Some(&json!("failed"))
        );
    }

    #[test]
    fn codex_message_projection_full_assistant_message_projects_existing_events() {
        let started = project_codex_notification(
            "item/started",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {"id": "native_item_message", "type": "agentMessage"}
            }),
        );
        let delta_one = project_codex_notification(
            "item/agentMessage/delta",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "itemId": "native_item_message",
                "delta": "Hel"
            }),
        );
        let delta_two = project_codex_notification(
            "item/agentMessage/delta",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "itemId": "native_item_message",
                "delta": "lo"
            }),
        );
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_item_message",
                    "type": "agentMessage",
                    "content": [{"text": "Hello"}]
                }
            }),
        );
        let turn_completed = project_codex_notification(
            "turn/completed",
            json!({
                "threadId": "native_thread_1",
                "turn": {"id": "native_turn_1", "status": "completed"}
            }),
        );

        let AgentDurableEvent::ItemStarted { notification } = &started.durable[0] else {
            panic!("expected item started");
        };
        assert_eq!(notification.workspace_id, "workspace_1");
        let TurnItem::AgentMessage { id, text, .. } = &notification.item else {
            panic!("expected agent message item");
        };
        assert_eq!(id, "native_item_message");
        assert!(text.is_empty());

        let AgentProgressEvent::ItemDelta { notification } = &delta_one.progress[0] else {
            panic!("expected first item delta");
        };
        assert_eq!(notification.item_id, "native_item_message");
        assert_eq!(notification.delta, "Hel");
        assert_eq!(notification.stream, Some(ItemDeltaStream::AgentMessage));
        assert_eq!(
            notification
                .payload
                .as_ref()
                .and_then(|payload| payload.get("nativeTurnId")),
            Some(&json!("native_turn_1"))
        );
        let AgentProgressEvent::ItemDelta { notification } = &delta_two.progress[0] else {
            panic!("expected second item delta");
        };
        assert_eq!(notification.delta, "lo");

        let AgentDurableEvent::ItemCompleted { notification } = &completed.durable[0] else {
            panic!("expected item completed");
        };
        let TurnItem::AgentMessage { id, text, .. } = &notification.item else {
            panic!("expected completed agent message item");
        };
        assert_eq!(id, "native_item_message");
        assert_eq!(text, "Hello");

        assert_eq!(
            turn_completed.durable,
            vec![AgentDurableEvent::TurnCompleted {
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                recovery: None
            }]
        );
    }

    #[test]
    fn codex_projection_ignores_native_user_message_echo() {
        let started = project_codex_notification(
            "item/started",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_user_1",
                    "type": "userMessage",
                    "content": [{"text": "## Pioneer Context\ninternal prompt"}]
                }
            }),
        );
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_user_1",
                    "type": "userMessage",
                    "content": [{"text": "## Pioneer Context\ninternal prompt"}]
                }
            }),
        );

        for projected in [started, completed] {
            assert!(projected.durable.is_empty());
            assert!(projected.progress.is_empty());
            assert_eq!(projected.ignored, vec!["native userMessage echo"]);
        }
    }

    #[test]
    fn codex_projection_commentary_agent_message_preserves_phase() {
        let started = project_codex_notification(
            "item/started",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_commentary_1",
                    "type": "agentMessage",
                    "phase": "commentary"
                }
            }),
        );
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_commentary_1",
                    "type": "agentMessage",
                    "phase": "commentary",
                    "text": "I will inspect the project."
                }
            }),
        );
        let final_answer = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_final_1",
                    "type": "agentMessage",
                    "phase": "final_answer",
                    "text": "Done."
                }
            }),
        );

        let AgentDurableEvent::ItemStarted { notification } = &started.durable[0] else {
            panic!("expected commentary item started");
        };
        let TurnItem::AgentMessage {
            id, text, phase, ..
        } = &notification.item
        else {
            panic!("expected commentary agent message");
        };
        assert_eq!(id, "native_commentary_1");
        assert!(text.is_empty());
        assert_eq!(*phase, AgentMessagePhase::Commentary);

        let AgentDurableEvent::ItemCompleted { notification } = &completed.durable[0] else {
            panic!("expected commentary item completed");
        };
        let TurnItem::AgentMessage {
            id, text, phase, ..
        } = &notification.item
        else {
            panic!("expected completed commentary agent message");
        };
        assert_eq!(id, "native_commentary_1");
        assert_eq!(text, "I will inspect the project.");
        assert_eq!(*phase, AgentMessagePhase::Commentary);

        let AgentDurableEvent::ItemCompleted { notification } = &final_answer.durable[0] else {
            panic!("expected final answer item completed");
        };
        let TurnItem::AgentMessage {
            id, text, phase, ..
        } = &notification.item
        else {
            panic!("expected final answer to remain agent message");
        };
        assert_eq!(id, "native_final_1");
        assert_eq!(text, "Done.");
        assert_eq!(*phase, AgentMessagePhase::FinalAnswer);
    }

    #[test]
    fn codex_message_projection_reasoning_summary_does_not_emit_agent_message_stream() {
        let reasoning_delta = project_codex_notification(
            "item/reasoning/textDelta",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "itemId": "native_item_reasoning",
                "delta": "Working through it."
            }),
        );
        let summary_delta = project_codex_notification(
            "item/reasoning/summaryTextDelta",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "itemId": "native_item_reasoning",
                "delta": "Short summary"
            }),
        );
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_item_reasoning",
                    "type": "reasoning",
                    "summary": ["Short summary"],
                    "content": ["Working through it."]
                }
            }),
        );

        for projected in [reasoning_delta, summary_delta] {
            let AgentProgressEvent::ItemDelta { notification } = &projected.progress[0] else {
                panic!("expected reasoning item delta");
            };
            assert_eq!(notification.item_id, "native_item_reasoning");
            assert_eq!(notification.stream, Some(ItemDeltaStream::Generic));
            assert_ne!(notification.stream, Some(ItemDeltaStream::AgentMessage));
            assert_eq!(
                notification
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("nativeItemId")),
                Some(&json!("native_item_reasoning"))
            );
        }

        let AgentDurableEvent::ItemCompleted { notification } = &completed.durable[0] else {
            panic!("expected reasoning item completed");
        };
        let TurnItem::Reasoning {
            id,
            summary,
            content,
        } = &notification.item
        else {
            panic!("expected reasoning item");
        };
        assert_eq!(id, "native_item_reasoning");
        assert_eq!(summary, &vec!["Short summary".to_owned()]);
        assert_eq!(content, &vec!["Working through it.".to_owned()]);
    }

    #[test]
    fn codex_tool_projection_command_output_projects_tool_item_and_stdout_stderr() {
        let started = project_codex_notification(
            "item/started",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_cmd_1",
                    "type": "commandExecution",
                    "command": ["cargo", "test"],
                    "cwd": "/repo"
                }
            }),
        );
        let stdout = project_codex_notification(
            "item/commandExecution/outputDelta",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "itemId": "native_cmd_1",
                "stream": "stdout",
                "delta": "ok\n"
            }),
        );
        let stderr = project_codex_notification(
            "item/commandExecution/outputDelta",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "itemId": "native_cmd_1",
                "stream": "stderr",
                "delta": "warn\n"
            }),
        );
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_cmd_1",
                    "type": "commandExecution",
                    "command": ["cargo", "test"],
                    "cwd": "/repo",
                    "status": "completed",
                    "exitCode": 0,
                    "success": true,
                    "stdout": "ok\n",
                    "stderr": "warn\n"
                }
            }),
        );

        let AgentDurableEvent::ItemStarted { notification } = &started.durable[0] else {
            panic!("expected command item started");
        };
        let TurnItem::CommandExecution {
            id,
            status,
            command,
            cwd,
            ..
        } = &notification.item
        else {
            panic!("expected command execution item");
        };
        assert_eq!(id, "native_cmd_1");
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::InProgress);
        assert_eq!(command, &vec!["cargo".to_owned(), "test".to_owned()]);
        assert_eq!(cwd.as_deref(), Some("/repo"));

        let AgentProgressEvent::ToolOutputDelta {
            item_id,
            stream,
            delta,
            payload,
            ..
        } = &stdout.progress[0]
        else {
            panic!("expected stdout tool output delta");
        };
        assert_eq!(item_id, "native_cmd_1");
        assert_eq!(*stream, ItemDeltaStream::Stdout);
        assert_eq!(delta, "ok\n");
        assert_eq!(
            payload
                .as_ref()
                .and_then(|payload| payload.get("nativeItemId")),
            Some(&json!("native_cmd_1"))
        );

        let AgentProgressEvent::ToolOutputDelta { stream, delta, .. } = &stderr.progress[0] else {
            panic!("expected stderr tool output delta");
        };
        assert_eq!(*stream, ItemDeltaStream::Stderr);
        assert_eq!(delta, "warn\n");

        let AgentDurableEvent::ItemCompleted { notification } = &completed.durable[0] else {
            panic!("expected command item completed");
        };
        let TurnItem::CommandExecution {
            status,
            success,
            command,
            ..
        } = &notification.item
        else {
            panic!("expected completed command execution item");
        };
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::Completed);
        assert_eq!(*success, Some(true));
        assert_eq!(command, &vec!["cargo".to_owned(), "test".to_owned()]);
    }

    #[test]
    fn codex_tool_projection_file_change_projects_changed_paths_and_patch_metadata() {
        let started = project_codex_notification(
            "item/started",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_file_1",
                    "type": "fileChange",
                    "changedFiles": ["src/lib.rs"]
                }
            }),
        );
        let patch = project_codex_notification(
            "item/fileChange/patchUpdated",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "itemId": "native_file_1",
                "changedFiles": ["src/lib.rs"],
                "patch": "diff --git a/src/lib.rs b/src/lib.rs"
            }),
        );
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_file_1",
                    "type": "fileChange",
                    "changedFiles": ["src/lib.rs"],
                    "status": "completed",
                    "exitCode": 0,
                    "success": true
                }
            }),
        );

        let AgentDurableEvent::ItemStarted { notification } = &started.durable[0] else {
            panic!("expected file item started");
        };
        let TurnItem::FileChange {
            id,
            status,
            changed_files,
            ..
        } = &notification.item
        else {
            panic!("expected file change item");
        };
        assert_eq!(id, "native_file_1");
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::InProgress);
        assert_eq!(changed_files, &vec!["src/lib.rs".to_owned()]);

        let AgentProgressEvent::ItemDelta { notification } = &patch.progress[0] else {
            panic!("expected file change item delta");
        };
        assert_eq!(notification.item_id, "native_file_1");
        assert_eq!(notification.stream, Some(ItemDeltaStream::FileChange));
        assert_eq!(
            notification
                .payload
                .as_ref()
                .and_then(|payload| payload.get("changedFiles")),
            Some(&json!(["src/lib.rs"]))
        );
        assert_eq!(
            notification
                .payload
                .as_ref()
                .and_then(|payload| payload.get("patch")),
            Some(&json!("diff --git a/src/lib.rs b/src/lib.rs"))
        );

        let AgentDurableEvent::ItemCompleted { notification } = &completed.durable[0] else {
            panic!("expected file item completed");
        };
        let TurnItem::FileChange {
            status,
            changed_files,
            success,
            ..
        } = &notification.item
        else {
            panic!("expected completed file change item");
        };
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::Completed);
        assert_eq!(changed_files, &vec!["src/lib.rs".to_owned()]);
        assert_eq!(*success, Some(true));
    }

    #[test]
    fn codex_projection_turn_plan_and_diff_updates_are_visible_timeline_items() {
        let plan = project_codex_notification(
            "turn/plan/updated",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "plan": [{"step": "Inspect state", "status": "inProgress"}]
            }),
        );
        let diff = project_codex_notification(
            "turn/diff/updated",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "diff": "diff --git a/src/lib.rs b/src/lib.rs"
            }),
        );

        let AgentDurableEvent::ItemCompleted { notification: plan } = &plan.durable[0] else {
            panic!("expected plan system item");
        };
        let TurnItem::SystemEvent {
            id, code, details, ..
        } = &plan.item
        else {
            panic!("expected plan system event");
        };
        assert_eq!(id, "agent_plan_nativeturn1");
        assert_eq!(code.as_deref(), Some("agent_plan_updated"));
        assert_eq!(
            details.as_ref().and_then(|details| details.get("payload")),
            Some(&json!([{"step": "Inspect state", "status": "inProgress"}]))
        );

        assert!(
            diff.durable.is_empty(),
            "diff snapshots should not be appended to the durable turn_event log"
        );
        let super::AgentSnapshotEvent::ItemUpdated { notification: diff } = &diff.snapshot[0];
        let TurnItem::SystemEvent {
            id, code, details, ..
        } = &diff.item
        else {
            panic!("expected diff system event");
        };
        assert_eq!(id, "agent_diff_nativeturn1");
        assert_eq!(code.as_deref(), Some("agent_diff_updated"));
        assert_eq!(
            details.as_ref().and_then(|details| details.get("payload")),
            Some(&json!("diff --git a/src/lib.rs b/src/lib.rs"))
        );
    }

    #[test]
    fn codex_projection_reasoning_summary_part_added_streams_generic_delta() {
        let projected = project_codex_notification(
            "item/reasoning/summaryPartAdded",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "itemId": "native_reasoning_1",
                "summaryIndex": 2
            }),
        );

        assert!(projected.durable.is_empty());
        assert!(projected.ignored.is_empty());
        let AgentProgressEvent::ItemDelta { notification } = &projected.progress[0] else {
            panic!("expected reasoning summary boundary delta");
        };
        assert_eq!(notification.item_id, "native_reasoning_1");
        assert_eq!(notification.delta, "\n");
        assert_eq!(notification.stream, Some(ItemDeltaStream::Generic));
        assert_eq!(
            notification
                .payload
                .as_ref()
                .and_then(|payload| payload.get("summaryIndex")),
            Some(&json!(2))
        );
    }

    #[test]
    fn codex_projection_web_search_uses_native_timeline_item() {
        let started = project_codex_notification(
            "item/started",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_search_1",
                    "type": "webSearch",
                    "action": {"type": "search", "query": "codex app-server"},
                    "provider": "openai"
                }
            }),
        );
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_search_1",
                    "type": "webSearch",
                    "query": "codex app-server",
                    "provider": "openai",
                    "resultCount": 3,
                    "durationMs": 42,
                    "status": "completed",
                    "success": true
                }
            }),
        );

        let AgentDurableEvent::ItemStarted {
            notification: started,
        } = &started.durable[0]
        else {
            panic!("expected web search start");
        };
        let TurnItem::WebSearch {
            id,
            status,
            query,
            provider,
            ..
        } = &started.item
        else {
            panic!("expected web search item");
        };
        assert_eq!(id, "native_search_1");
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::InProgress);
        assert_eq!(query.as_deref(), Some("codex app-server"));
        assert_eq!(provider.as_deref(), Some("openai"));

        let AgentDurableEvent::ItemCompleted {
            notification: completed,
        } = &completed.durable[0]
        else {
            panic!("expected web search completed");
        };
        let TurnItem::WebSearch {
            status,
            result_count,
            took_ms,
            success,
            ..
        } = &completed.item
        else {
            panic!("expected completed web search item");
        };
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::Completed);
        assert_eq!(*result_count, Some(3));
        assert_eq!(*took_ms, Some(42));
        assert_eq!(*success, Some(true));
    }

    #[test]
    fn codex_projection_dynamic_tool_call_uses_native_timeline_item() {
        let completed = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_dynamic_1",
                    "type": "dynamicToolCall",
                    "tool": "browser.click",
                    "arguments": {"selector": "#send"},
                    "status": "completed",
                    "success": true
                }
            }),
        );

        let AgentDurableEvent::ItemCompleted { notification } = &completed.durable[0] else {
            panic!("expected dynamic tool completion");
        };
        let TurnItem::DynamicToolCall {
            id,
            tool_name,
            arguments,
            status,
            output_policy,
            display,
            storage,
            success,
            ..
        } = &notification.item
        else {
            panic!("expected dynamic tool item");
        };
        assert_eq!(id, "native_dynamic_1");
        assert_eq!(tool_name, "browser.click");
        assert_eq!(arguments.get("selector"), Some(&json!("#send")));
        assert_eq!(
            arguments.get("nativeItemId"),
            Some(&json!("native_dynamic_1"))
        );
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::Completed);
        assert!(matches!(
            &output_policy.timeline,
            TimelineOutputPolicy::Summary { .. }
        ));
        assert!(matches!(
            &output_policy.storage,
            StorageOutputPolicy::MetadataOnly
        ));
        assert!(matches!(display, ToolDisplayPayload::Summary(_)));
        assert!(matches!(storage, ToolStoragePayload::Metadata { .. }));
        assert_eq!(*success, Some(true));
    }

    #[test]
    fn codex_mcp_timeline_uses_one_enriched_native_item_for_start_terminal_and_replay() {
        let metadata = json!({
            "server": "pioneer",
            "tool": "mcp__server__tool",
            "canonicalCallableName": "mcp__server__tool",
            "serverInstallationId": "installation-real",
            "serverName": "Real Server",
            "rawToolName": "real_tool",
            "capabilityId": "capability-1",
            "selectionReason": "explicit_tool",
            "manifestHash": "a".repeat(64),
            "providerManifestHash": "b".repeat(64),
            "providerCallId": "native_mcp_1",
            "invocationCorrelationId": "correlation-1",
            "sessionGeneration": 7,
            "arguments": {"value": 7},
            "status": "inProgress"
        });
        let started_event = RuntimeEvent::ItemStarted(RuntimeItemStarted {
            native_thread_id: Some("native_thread_1".to_owned()),
            native_turn_id: "native_turn_1".to_owned(),
            native_item_id: "native_mcp_1".to_owned(),
            item_kind: "mcpToolCall".to_owned(),
            title: None,
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some(metadata.clone()),
            native_item_redacted: None,
            native: None,
        });
        let completed_event = RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
            native_thread_id: Some("native_thread_1".to_owned()),
            native_turn_id: "native_turn_1".to_owned(),
            native_item_id: "native_mcp_1".to_owned(),
            item_kind: "mcpToolCall".to_owned(),
            text: None,
            summary: Vec::new(),
            content: Vec::new(),
            phase: RuntimeAgentMessagePhase::FinalAnswer,
            metadata: Some({
                let mut terminal = metadata;
                terminal["status"] = json!("failed");
                terminal["error"] = json!({"message": "upstream failed"});
                terminal
            }),
            native_item_redacted: None,
            native: None,
        });

        let started = project_cli_runtime_event(&context(), &started_event);
        let replayed = project_cli_runtime_event(&context(), &started_event);
        let completed = project_cli_runtime_event(&context(), &completed_event);
        assert_eq!(started, replayed, "replay must upsert the same identity");
        assert_eq!(started.durable.len(), 1);
        assert_eq!(completed.durable.len(), 1);

        let AgentDurableEvent::ItemStarted { notification } = &started.durable[0] else {
            panic!("expected MCP item start");
        };
        let TurnItem::DynamicToolCall {
            id,
            tool_name,
            arguments,
            status,
            display,
            success,
            ..
        } = &notification.item
        else {
            panic!("expected canonical MCP tool item");
        };
        assert_eq!(id, "native_mcp_1");
        assert_eq!(tool_name, "mcp__server__tool");
        assert_eq!(arguments.get("value"), Some(&json!(7)));
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::InProgress);
        let ToolDisplayPayload::Summary(summary) = display else {
            panic!("expected MCP summary");
        };
        assert_eq!(summary.title, "MCP: Real Server / real_tool");
        assert_eq!(*success, None);

        let AgentDurableEvent::ItemCompleted { notification } = &completed.durable[0] else {
            panic!("expected MCP item terminal");
        };
        let TurnItem::DynamicToolCall {
            id,
            tool_name,
            status,
            success,
            display,
            ..
        } = &notification.item
        else {
            panic!("expected terminal canonical MCP tool item");
        };
        assert_eq!(id, "native_mcp_1");
        assert_eq!(tool_name, "mcp__server__tool");
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::Failed);
        assert_eq!(*success, Some(false));
        let ToolDisplayPayload::Summary(summary) = display else {
            panic!("expected terminal MCP summary");
        };
        assert_eq!(summary.lines, vec!["upstream failed".to_owned()]);
    }

    #[test]
    fn codex_projection_collab_image_and_unknown_items_do_not_disappear() {
        let collab = project_codex_notification(
            "item/started",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_collab_1",
                    "type": "collabToolCall",
                    "tool": "handoff",
                    "senderThreadId": "native_thread_1"
                }
            }),
        );
        let image = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_image_1",
                    "type": "imageView",
                    "path": "/tmp/screenshot.png",
                    "status": "completed"
                }
            }),
        );
        let unknown = project_codex_notification(
            "item/completed",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "item": {
                    "id": "native_future_1",
                    "type": "futureItem",
                    "message": "future payload",
                    "status": "completed"
                }
            }),
        );

        let AgentDurableEvent::ItemStarted { notification } = &collab.durable[0] else {
            panic!("expected collab dynamic tool start");
        };
        let TurnItem::DynamicToolCall {
            tool_name,
            arguments,
            status,
            ..
        } = &notification.item
        else {
            panic!("expected collab dynamic tool item");
        };
        assert_eq!(tool_name, "collab:handoff");
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::InProgress);
        assert_eq!(
            arguments.get("senderThreadId"),
            Some(&json!("native_thread_1"))
        );

        let AgentDurableEvent::ItemCompleted { notification } = &image.durable[0] else {
            panic!("expected image dynamic tool event");
        };
        let TurnItem::DynamicToolCall {
            tool_name,
            arguments,
            status,
            ..
        } = &notification.item
        else {
            panic!("expected image dynamic tool item");
        };
        assert_eq!(tool_name, "image_view");
        assert_eq!(*status, pioneer_protocol::ToolCallStatus::Completed);
        assert_eq!(arguments.get("path"), Some(&json!("/tmp/screenshot.png")));

        let AgentDurableEvent::ItemCompleted { notification } = &unknown.durable[0] else {
            panic!("expected unknown system event");
        };
        let TurnItem::SystemEvent {
            code,
            details,
            message,
            ..
        } = &notification.item
        else {
            panic!("expected unknown system item");
        };
        assert_eq!(code.as_deref(), Some("agent_runtime_item"));
        assert_eq!(message, "future payload");
        assert_eq!(
            details
                .as_ref()
                .and_then(|details| details.get("nativeItemKind")),
            Some(&json!("futureItem"))
        );
    }

    #[test]
    fn codex_projection_raw_event_with_turn_id_is_visible() {
        let projected = project_cli_runtime_event(
            &context(),
            &RuntimeEvent::Raw(RuntimeRawEvent {
                native_method: "fuzzyFileSearch/sessionUpdated".to_owned(),
                reason: "unsupported codex notification".to_owned(),
                native_thread_id: Some("native_thread_1".to_owned()),
                native_turn_id: Some("native_turn_1".to_owned()),
                native_item_id: None,
                payload_redacted: Some(json!({"sessionId": "s1", "query": "main.rs"})),
                raw_redacted: None,
            }),
        );

        assert!(projected.progress.is_empty());
        assert!(projected.ignored.is_empty());
        let AgentDurableEvent::ItemCompleted { notification } = &projected.durable[0] else {
            panic!("expected raw system item");
        };
        let TurnItem::SystemEvent {
            code,
            message,
            details,
            ..
        } = &notification.item
        else {
            panic!("expected raw system event");
        };
        assert_eq!(code.as_deref(), Some("agent_runtime_event"));
        assert_eq!(message, "Runtime event: fuzzyFileSearch/sessionUpdated");
        assert_eq!(
            details.as_ref().and_then(|details| details.get("payload")),
            Some(&json!({"sessionId": "s1", "query": "main.rs"}))
        );
    }

    #[test]
    fn runtime_raw_event_without_turn_id_is_visible_when_timeline_worthy() {
        let projected = project_cli_runtime_event(
            &context(),
            &RuntimeEvent::Raw(RuntimeRawEvent {
                native_method: "thread/tokenUsage/updated".to_owned(),
                reason: "runtime notification without required turn id".to_owned(),
                native_thread_id: Some("native_thread_1".to_owned()),
                native_turn_id: None,
                native_item_id: None,
                payload_redacted: Some(json!({"inputTokens": 10, "outputTokens": 2})),
                raw_redacted: None,
            }),
        );

        assert!(projected.progress.is_empty());
        assert!(projected.ignored.is_empty());
        let AgentDurableEvent::ItemCompleted { notification } = &projected.durable[0] else {
            panic!("expected timeline-worthy raw system item");
        };
        let TurnItem::SystemEvent {
            id, code, details, ..
        } = &notification.item
        else {
            panic!("expected raw system event");
        };
        assert_eq!(id, "agent_event_turn1_threadtokenusageupdated");
        assert_eq!(code.as_deref(), Some("agent_runtime_event"));
        assert_eq!(
            details.as_ref().and_then(|details| details.get("payload")),
            Some(&json!({"inputTokens": 10, "outputTokens": 2}))
        );
    }

    #[test]
    fn thread_state_change_projects_to_universal_system_event() {
        let projected = project_codex_notification(
            "thread/status/changed",
            json!({
                "threadId": "native_thread_1",
                "status": "archived"
            }),
        );

        assert!(projected.progress.is_empty());
        assert!(projected.ignored.is_empty());
        let AgentDurableEvent::ItemCompleted { notification } = &projected.durable[0] else {
            panic!("expected thread status system item");
        };
        let TurnItem::SystemEvent {
            id,
            code,
            message,
            details,
            ..
        } = &notification.item
        else {
            panic!("expected thread status system event");
        };
        assert_eq!(id, "agent_thread_turn1_archived");
        assert_eq!(code.as_deref(), Some("agent_thread_status_changed"));
        assert_eq!(message, "Thread status changed: archived");
        assert_eq!(
            details.as_ref().and_then(|details| details.get("status")),
            Some(&json!("archived"))
        );
    }

    #[test]
    fn codex_tool_projection_native_failure_maps_to_turn_failed() {
        let failed = project_codex_notification(
            "turn/completed",
            json!({
                "threadId": "native_thread_1",
                "turn": {
                    "id": "native_turn_1",
                    "status": "failed",
                    "error": {"message": "native failure", "code": "boom"}
                }
            }),
        );

        assert_eq!(
            failed.durable,
            vec![AgentDurableEvent::TurnFailed {
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                error: "native failure".to_owned(),
                recovery: None
            }]
        );
    }

    #[test]
    fn codex_retrying_turn_does_not_project_terminal_or_timeline_events() {
        let retrying = project_codex_notification(
            "error",
            json!({
                "threadId": "native_thread_1",
                "turnId": "native_turn_1",
                "error": {
                    "message": "Reconnecting... 2/5",
                    "code": "stream_disconnected"
                },
                "willRetry": true
            }),
        );

        assert!(retrying.progress.is_empty());
        assert!(retrying.durable.is_empty());
        assert_eq!(retrying.ignored, vec!["turn_retrying"]);
    }

    #[test]
    fn codex_tool_projection_native_interruption_maps_to_turn_interrupted() {
        let interrupted = project_codex_notification(
            "turn/completed",
            json!({
                "threadId": "native_thread_1",
                "turn": {
                    "id": "native_turn_1",
                    "status": "interrupted"
                }
            }),
        );

        assert_eq!(
            interrupted.durable,
            vec![AgentDurableEvent::TurnInterrupted {
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                reason: "interrupted".to_owned(),
                recovery: None
            }]
        );
    }
}
