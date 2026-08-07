use super::*;
use anyhow::{Context, Result, bail};
use futures_util::{FutureExt, StreamExt};
use sha2::{Digest, Sha256};
use std::panic::AssertUnwindSafe;
const TITLE_JOB_MAX_ATTEMPTS: u32 = 3;
const TITLE_JOB_BASE_BACKOFF_MS: u64 = 200;
const TITLE_JOB_MAX_JITTER_MS: u64 = 250;

enum TitleAttemptOutcome {
    Succeeded,
    EmptyTitle,
}

#[derive(Clone)]
pub(super) struct ParentTimelineTarget {
    workspace_id: String,
    parent_thread_id: String,
    parent_turn_id: String,
    child_turn_id: String,
}

fn tool_display_text(display: &pioneer_protocol::ToolDisplayPayload) -> Option<String> {
    match display {
        pioneer_protocol::ToolDisplayPayload::Shell {
            aggregated_output,
            stdout,
            stderr,
            ..
        } => aggregated_output
            .clone()
            .or_else(|| stdout.clone())
            .or_else(|| stderr.clone()),
        pioneer_protocol::ToolDisplayPayload::Summary(summary) => {
            let mut lines = Vec::new();
            if !summary.title.trim().is_empty() {
                lines.push(summary.title.clone());
            }
            lines.extend(
                summary
                    .lines
                    .iter()
                    .filter(|line| !line.trim().is_empty())
                    .cloned(),
            );
            (!lines.is_empty()).then(|| lines.join("\n"))
        }
        pioneer_protocol::ToolDisplayPayload::Progress { stage, .. } => Some(stage.clone()),
        pioneer_protocol::ToolDisplayPayload::Hidden => None,
    }
}

pub(super) fn thread_episodic_index_wakeup_after_commit(item: &pioneer_protocol::TurnItem) -> bool {
    !matches!(item, pioneer_protocol::TurnItem::UserMessage { .. })
}

fn now_db_timestamp() -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

fn db_timestamp_from_unix_ms(value: i64) -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(value)
        .map(|timestamp| timestamp.fixed_offset())
        .unwrap_or_else(now_db_timestamp)
}

fn turn_llm_context_delivery_key(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

fn execution_window_started_metadata(runtime_window_id: &str) -> serde_json::Value {
    serde_json::json!({
        "runtimeWindowId": runtime_window_id,
    })
}

fn execution_window_exhausted_metadata(
    runtime_window_id: &str,
    limit: u64,
    observed: u64,
    reason: &str,
) -> serde_json::Value {
    serde_json::json!({
        "runtimeWindowId": runtime_window_id,
        "limit": limit,
        "observed": observed,
        "reason": reason,
    })
}

fn execution_window_blocked_metadata(
    runtime_window_id: &str,
    total_windows: u32,
    total_tool_calls: u32,
    reason: &str,
) -> serde_json::Value {
    serde_json::json!({
        "runtimeWindowId": runtime_window_id,
        "totalWindows": total_windows,
        "totalToolCalls": total_tool_calls,
        "reason": reason,
    })
}

fn execution_window_terminal_metadata(
    previous: &serde_json::Value,
    status: pioneer_protocol::ExecutionWindowStatus,
    reason: Option<&str>,
) -> serde_json::Value {
    let mut metadata = previous.as_object().cloned().unwrap_or_default();
    let status_label = match status {
        pioneer_protocol::ExecutionWindowStatus::Completed => "completed",
        pioneer_protocol::ExecutionWindowStatus::Failed => "failed",
        pioneer_protocol::ExecutionWindowStatus::Running => "running",
        pioneer_protocol::ExecutionWindowStatus::Exhausted => "exhausted",
        pioneer_protocol::ExecutionWindowStatus::Checkpointed => "checkpointed",
        pioneer_protocol::ExecutionWindowStatus::Continued => "continued",
        pioneer_protocol::ExecutionWindowStatus::Interrupted => "interrupted",
        pioneer_protocol::ExecutionWindowStatus::Blocked => "blocked",
    };
    metadata.insert(
        "terminalStatus".to_owned(),
        serde_json::Value::String(status_label.to_owned()),
    );
    metadata.insert(
        "terminalSource".to_owned(),
        serde_json::Value::String("turn_terminal_event".to_owned()),
    );
    if let Some(reason) = reason
        && !reason.trim().is_empty()
    {
        metadata.insert(
            "terminalReason".to_owned(),
            serde_json::Value::String(reason.to_owned()),
        );
    }
    serde_json::Value::Object(metadata)
}

fn execution_window_is_active_for_terminal_close(
    status: pioneer_protocol::ExecutionWindowStatus,
) -> bool {
    matches!(
        status,
        pioneer_protocol::ExecutionWindowStatus::Running
            | pioneer_protocol::ExecutionWindowStatus::Checkpointed
    )
}

fn execution_checkpoint_kind_from_wire(
    value: &str,
) -> Option<pioneer_crud::TurnExecutionCheckpointKind> {
    match value {
        "window_exhausted" => Some(pioneer_crud::TurnExecutionCheckpointKind::WindowExhausted),
        "turn_blocked" => Some(pioneer_crud::TurnExecutionCheckpointKind::TurnBlocked),
        "startup_recovery" => Some(pioneer_crud::TurnExecutionCheckpointKind::StartupRecovery),
        _ => None,
    }
}

fn durable_event_thread_id(event: &AgentDurableEvent) -> Option<&str> {
    match event {
        AgentDurableEvent::PromptManifestCompiled { thread_id, .. }
        | AgentDurableEvent::TurnSkillsResolved { thread_id, .. }
        | AgentDurableEvent::TurnCapabilitiesResolved { thread_id, .. }
        | AgentDurableEvent::SkillAuditEvents { thread_id, .. }
        | AgentDurableEvent::TurnLlmContextAppended { thread_id, .. }
        | AgentDurableEvent::TurnProviderHistoryAppended { thread_id, .. }
        | AgentDurableEvent::ProviderFailureDetected { thread_id, .. }
        | AgentDurableEvent::RecoveryAttemptSucceeded { thread_id, .. }
        | AgentDurableEvent::TurnCompleted { thread_id, .. }
        | AgentDurableEvent::TurnFailed { thread_id, .. }
        | AgentDurableEvent::TurnBlocked { thread_id, .. }
        | AgentDurableEvent::TurnInterrupted { thread_id, .. } => Some(thread_id.as_str()),
        AgentDurableEvent::TurnPermissionAudit { event } => Some(event.thread_id.as_str()),
        AgentDurableEvent::ItemStarted { notification } => Some(notification.thread_id.as_str()),
        AgentDurableEvent::ItemCompleted { notification }
        | AgentDurableEvent::TurnFinalizationPrepared { notification, .. } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::ItemToolRetryScheduled { notification } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::ItemToolRetryResolved { notification } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::ItemToolRetryExhausted { notification } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::TurnToolLoopBudgetExceeded { notification } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::TurnExecutionWindowStarted { notification } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::TurnExecutionWindowExhausted { notification } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::TurnExecutionWindowCheckpointed { notification, .. } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::TurnExecutionWindowContinued { notification } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::TurnExecutionWindowBlocked { notification } => {
            Some(notification.thread_id.as_str())
        }
        AgentDurableEvent::TaskEvent { .. } | AgentDurableEvent::ThreadLineageCreated { .. } => {
            None
        }
    }
}

fn tool_result_view_from_protocol(
    payload: pioneer_protocol::ToolResultView,
) -> pioneer_tools::ToolResultView {
    match payload {
        pioneer_protocol::ToolResultView::Text { text, truncated } => {
            pioneer_tools::ToolResultView::Text { text, truncated }
        }
        pioneer_protocol::ToolResultView::Json { value, truncated } => {
            pioneer_tools::ToolResultView::Json { value, truncated }
        }
        pioneer_protocol::ToolResultView::Empty => pioneer_tools::ToolResultView::Empty,
    }
}

fn title_retry_backoff(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10);
    let base = TITLE_JOB_BASE_BACKOFF_MS.saturating_mul(1_u64 << exponent);
    let jitter = rand::random_range(0..=TITLE_JOB_MAX_JITTER_MS);
    Duration::from_millis(base.saturating_add(jitter))
}

fn normalized_titles_equal(left: &str, right: &str) -> bool {
    summary::normalize_title_for_compare(left) == summary::normalize_title_for_compare(right)
}

#[derive(Debug, Clone, Copy)]
pub(super) enum TurnFailureRecoveryKind {
    TurnDispatch,
    ExecutionWindowContinuation,
    ArtifactFinalization,
    TaskDispatch,
    RuntimeFailure,
}

impl TurnFailureRecoveryKind {
    const fn trigger(self) -> pioneer_protocol::RecoveryTrigger {
        match self {
            Self::TurnDispatch => pioneer_protocol::RecoveryTrigger::TurnDispatch,
            Self::ExecutionWindowContinuation => {
                pioneer_protocol::RecoveryTrigger::ExecutionWindowContinuation
            }
            Self::ArtifactFinalization => pioneer_protocol::RecoveryTrigger::ArtifactFinalization,
            Self::TaskDispatch => pioneer_protocol::RecoveryTrigger::TaskDispatch,
            Self::RuntimeFailure => pioneer_protocol::RecoveryTrigger::RuntimeFailure,
        }
    }

    const fn action(self) -> pioneer_protocol::RecoveryAction {
        match self {
            Self::ArtifactFinalization => {
                pioneer_protocol::RecoveryAction::RepairArtifactFinalization
            }
            Self::TurnDispatch
            | Self::ExecutionWindowContinuation
            | Self::TaskDispatch
            | Self::RuntimeFailure => pioneer_protocol::RecoveryAction::RestartTurn,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::TurnDispatch => "turn_dispatch",
            Self::ExecutionWindowContinuation => "execution_window_continuation",
            Self::ArtifactFinalization => "artifact_finalization",
            Self::TaskDispatch => "task_dispatch",
            Self::RuntimeFailure => "runtime_failure",
        }
    }
}

fn classify_legacy_turn_failure(_error_message: &str) -> TurnFailureRecoveryKind {
    TurnFailureRecoveryKind::RuntimeFailure
}

impl MessageProcessor {
    async fn close_latest_active_execution_window_for_terminal_turn(
        &self,
        turn_id: &str,
        terminal_status: pioneer_protocol::ExecutionWindowStatus,
        terminal_reason: Option<&str>,
    ) -> Result<()> {
        let Some(window) = self
            .crud_store
            .latest_turn_execution_window(turn_id)
            .await?
        else {
            return Ok(());
        };
        if !execution_window_is_active_for_terminal_close(window.status) {
            return Ok(());
        }

        let counts = self
            .crud_store
            .count_turn_execution_window_terminal_items_since(turn_id, window.started_at.clone())
            .await?;
        let now = now_db_timestamp();
        let stats = pioneer_crud::TurnExecutionWindowStatsRecord {
            agent_round_count: window.agent_round_count.max(counts.agent_round_count),
            tool_call_count: window.tool_call_count.max(counts.tool_call_count),
            provider_token_count: window.provider_token_count,
            metadata_json: execution_window_terminal_metadata(
                &window.metadata_json,
                terminal_status,
                terminal_reason,
            ),
            completed_at: now,
            updated_at: now,
        };

        match terminal_status {
            pioneer_protocol::ExecutionWindowStatus::Completed => {
                self.crud_store
                    .mark_turn_execution_window_completed(window.id.as_str(), stats)
                    .await?;
            }
            pioneer_protocol::ExecutionWindowStatus::Failed => {
                self.crud_store
                    .mark_turn_execution_window_failed(window.id.as_str(), stats)
                    .await?;
            }
            pioneer_protocol::ExecutionWindowStatus::Interrupted => {
                self.crud_store
                    .mark_turn_execution_window_interrupted(window.id.as_str(), stats)
                    .await?;
            }
            pioneer_protocol::ExecutionWindowStatus::Blocked => {
                self.crud_store
                    .mark_turn_execution_window_blocked(window.id.as_str(), None, stats)
                    .await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn parent_timeline_target_for_child_turn(
        &self,
        child_thread_id: &str,
        child_turn_id: &str,
        workspace_id: Option<&str>,
    ) -> Option<ParentTimelineTarget> {
        if let Some(cached) = self
            .parent_timeline_targets
            .lock()
            .await
            .get(child_thread_id)
            .cloned()
        {
            if cached.child_turn_id == child_turn_id {
                return Some(cached);
            }
        }

        match message_future(
            self.crud_store
                .get_task_run_turn_by_turn(child_thread_id, child_turn_id),
        )
        .await
        {
            Ok(Some(task_run_turn)) => {
                if let Ok(Some(lineage)) = message_future(
                    self.crud_store
                        .get_task_thread_lineage(task_run_turn.thread_id.as_str()),
                )
                .await
                {
                    if let Some(parent_turn_id) = lineage.created_by_turn_id {
                        let parent_thread_id = lineage
                            .created_by_thread_id
                            .unwrap_or(lineage.parent_thread_id);
                        let workspace_id = if let Some(workspace_id) = workspace_id {
                            workspace_id.to_owned()
                        } else {
                            match message_future(
                                self.crud_store.get_thread_model(parent_thread_id.as_str()),
                            )
                            .await
                            {
                                Ok(Some(thread)) => thread.workspace_id,
                                Ok(None) => {
                                    match message_future(
                                        self.crud_store
                                            .get_thread_model(task_run_turn.thread_id.as_str()),
                                    )
                                    .await
                                    {
                                        Ok(Some(thread)) => thread.workspace_id,
                                        Ok(None) => return None,
                                        Err(error) => {
                                            warn!(
                                                child_thread_id,
                                                child_turn_id,
                                                error = %format!("{error:#}"),
                                                "failed to load child thread workspace for parent timeline notification"
                                            );
                                            return None;
                                        }
                                    }
                                }
                                Err(error) => {
                                    warn!(
                                        parent_thread_id,
                                        child_thread_id,
                                        child_turn_id,
                                        error = %format!("{error:#}"),
                                        "failed to load parent thread workspace for parent timeline notification"
                                    );
                                    return None;
                                }
                            }
                        };
                        let target = ParentTimelineTarget {
                            workspace_id,
                            parent_thread_id,
                            parent_turn_id,
                            child_turn_id: task_run_turn.turn_id,
                        };
                        self.parent_timeline_targets
                            .lock()
                            .await
                            .insert(child_thread_id.to_owned(), target.clone());
                        return Some(target);
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    child_thread_id,
                    child_turn_id,
                    error = %format!("{error:#}"),
                    "failed to load task run turn for parent timeline notification"
                );
            }
        }

        None
    }

    pub(super) async fn ensure_agent_listener_task(&self, thread_id: &str) -> Result<()> {
        // Keep reservation, receiver lease, and handle publication under one
        // lock. Concurrent start/recovery/supervisor callers must observe the
        // same listener rather than letting the loser mistake an already leased
        // healthy receiver for a fatal closed lane.
        let mut listeners = self.agent_listener_tasks.lock().await;
        let has_live_listener = listeners
            .get(thread_id)
            .is_some_and(|handle| !handle.is_finished());
        // A stale actor replacement creates a fresh AgentEventHub while the
        // old listener may still be draining the old hub.  A live listener is
        // therefore not sufficient proof that the current hub is leased. If
        // the current hub offers a receiver, fence the old listener and lease
        // the new generation under the same mutex.
        let replacement_receiver = if has_live_listener {
            let receiver = self.agent_manager.take_durable_receiver(thread_id).await;
            if receiver.is_none() {
                return Ok(());
            }
            if let Some(handle) = listeners.remove(thread_id) {
                handle.abort();
            }
            receiver
        } else {
            listeners.remove(thread_id);
            None
        };

        let Some(mut durable_receiver) = (match replacement_receiver {
            Some(receiver) => Some(receiver),
            None => self.agent_manager.take_durable_receiver(thread_id).await,
        }) else {
            bail!("native durable listener receiver is already leased for thread `{thread_id}`");
        };

        let mut live_receiver = self.agent_manager.subscribe_progress(thread_id).await;

        let this = self.clone();
        let thread_id_owned = thread_id.to_owned();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    durable = durable_receiver.recv() => {
                        let Some(event) = durable else {
                            break;
                        };
                        let committed = match AssertUnwindSafe(
                            this.handle_durable_agent_event(event),
                        )
                        .catch_unwind()
                        .await
                        {
                            Ok(committed) => committed,
                            Err(_) => {
                                warn!(
                                    thread_id = %thread_id_owned,
                                    "contained panic while projecting durable agent event"
                                );
                                false
                            }
                        };
                        durable_receiver.acknowledge_last(if committed {
                            Ok(())
                        } else {
                            Err("gateway failed to commit durable agent event".to_owned())
                        });
                    }
                    live = async {
                        match live_receiver.as_mut() {
                            Some(receiver) => Some(receiver.recv().await),
                            None => None,
                        }
                    }, if live_receiver.is_some() => {
                        match live {
                            Some(Ok(event)) => {
                                if AssertUnwindSafe(this.handle_progress_agent_event(event))
                                    .catch_unwind()
                                    .await
                                    .is_err()
                                {
                                    warn!(
                                        thread_id = %thread_id_owned,
                                        "contained panic while projecting agent progress event"
                                    );
                                }
                            }
                            Some(Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped))) => {
                                warn!(
                                    thread_id = %thread_id_owned,
                                    skipped,
                                    "agent live progress listener lagged behind and dropped progress events"
                                );
                            }
                            Some(Err(tokio::sync::broadcast::error::RecvError::Closed)) | None => {
                                live_receiver = None;
                            }
                        }
                    }
                }
            }
            this.agent_listener_tasks
                .lock()
                .await
                .remove(thread_id_owned.as_str());
        });

        listeners.insert(thread_id.to_owned(), handle);
        Ok(())
    }

    pub(super) fn enrich_turn_item_events_markdown(events: &mut [TurnItemEvent]) {
        let mut buffers: HashMap<String, String> = HashMap::new();

        for event in events {
            match &mut event.payload {
                TurnItemEventPayload::ItemStarted { item, .. } => {
                    let (item_id, text) = Self::normalize_item_markdown(item);
                    buffers.insert(item_id, text);
                }
                TurnItemEventPayload::ItemDelta {
                    item_id,
                    delta,
                    markdown,
                    markdown_version,
                    ..
                } => {
                    let full_text = {
                        let text = buffers.entry(item_id.clone()).or_default();
                        text.push_str(delta.as_str());
                        text.clone()
                    };

                    if markdown.is_none() {
                        *markdown = Some(markdown::parse_markdown_document(full_text.as_str()));
                    }
                    if markdown.is_some() && markdown_version.is_none() {
                        *markdown_version = Some(MARKDOWN_AST_VERSION);
                    }
                }
                TurnItemEventPayload::ItemCompleted { item, .. }
                | TurnItemEventPayload::ItemUpdated { item, .. } => {
                    let (item_id, _) = Self::normalize_item_markdown(item);
                    buffers.remove(item_id.as_str());
                }
                _ => {}
            }
        }
    }

    pub(super) fn tool_item_output(item: &TurnItem) -> Option<String> {
        match item {
            TurnItem::CommandExecution { display, .. } => tool_display_text(display),
            TurnItem::FileChange { display, .. }
            | TurnItem::WebSearch { display, .. }
            | TurnItem::WebFetch { display, .. }
            | TurnItem::Download { display, .. }
            | TurnItem::DynamicToolCall { display, .. } => tool_display_text(display),
            _ => None,
        }
    }

    pub(super) fn force_fail_tool_item(
        mut item: TurnItem,
        error_message: &str,
    ) -> Option<TurnItem> {
        match &mut item {
            TurnItem::CommandExecution {
                status,
                success,
                display,
                storage,
                ..
            }
            | TurnItem::FileChange {
                status,
                success,
                display,
                storage,
                ..
            }
            | TurnItem::WebSearch {
                status,
                success,
                display,
                storage,
                ..
            }
            | TurnItem::WebFetch {
                status,
                success,
                display,
                storage,
                ..
            }
            | TurnItem::Download {
                status,
                success,
                display,
                storage,
                ..
            }
            | TurnItem::DynamicToolCall {
                status,
                success,
                display,
                storage,
                ..
            } => {
                if *status != ToolCallStatus::InProgress {
                    return None;
                }
                *status = ToolCallStatus::Failed;
                *success = Some(false);
                let existing = tool_display_text(display);
                let text = Self::append_recovery_error_to_output(existing, error_message);
                let summary = pioneer_protocol::ToolOutputSummary {
                    title: "Tool failed during recovery".to_owned(),
                    lines: vec![text],
                    metadata: pioneer_protocol::ToolMetadata::from_json(serde_json::json!({
                        "recoveryFailed": true
                    })),
                    truncated: false,
                };
                *display = pioneer_protocol::ToolDisplayPayload::Summary(summary.clone());
                *storage = pioneer_protocol::ToolStoragePayload::Summary(summary);
                Some(item)
            }
            TurnItem::UserMessage { .. }
            | TurnItem::AgentMessage { .. }
            | TurnItem::Reasoning { .. }
            | TurnItem::SystemEvent { .. }
            | TurnItem::Task { .. } => None,
        }
    }

    pub(super) fn append_recovery_error_to_output(
        existing: Option<String>,
        error_message: &str,
    ) -> String {
        const RECOVERY_DISPLAY_MAX_CHARS: usize = 1_800;

        let mut lines = Vec::new();
        if let Some(value) = existing
            && !value.trim().is_empty()
        {
            lines.push(value);
        }
        lines.push(format!("recovery failed: {error_message}"));
        let text = lines.join("\n");
        if text.chars().count() <= RECOVERY_DISPLAY_MAX_CHARS {
            return text;
        }
        text.chars().take(RECOVERY_DISPLAY_MAX_CHARS).collect()
    }

    pub(super) fn normalize_item_markdown(item: &mut TurnItem) -> (String, String) {
        match item {
            TurnItem::AgentMessage {
                id,
                text,
                markdown,
                markdown_version,
                ..
            } => {
                if markdown.is_none() {
                    *markdown = Some(markdown::parse_markdown_document(text.as_str()));
                }
                if markdown.is_some() && markdown_version.is_none() {
                    *markdown_version = Some(MARKDOWN_AST_VERSION);
                }

                (id.clone(), text.clone())
            }
            TurnItem::UserMessage { id, text, .. } => (id.clone(), text.clone()),
            TurnItem::Reasoning {
                id,
                summary,
                content,
            } => {
                let text = if !content.is_empty() {
                    content.join("\n")
                } else if !summary.is_empty() {
                    summary.join("\n")
                } else {
                    String::new()
                };
                (id.clone(), text)
            }
            _ => {
                let id = item.item_id().to_owned();
                let text = Self::tool_item_output(item).unwrap_or_default();
                (id, text)
            }
        }
    }

    pub(super) async fn teardown_agent_thread(&self, thread_id: &str) {
        if let Some(listener_handle) = self.agent_listener_tasks.lock().await.remove(thread_id) {
            listener_handle.abort();
        }
        self.agent_manager.remove_thread(thread_id).await;
        let prefix = format!("{thread_id}:");
        self.agent_message_buffers
            .lock()
            .await
            .retain(|key, _| !key.starts_with(prefix.as_str()));
    }

    async fn delete_turn_runtime_snapshot_for_closed_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        status: &str,
    ) {
        if let Err(error) = self.crud_store.delete_turn_runtime_snapshot(turn_id).await {
            warn!(
                thread_id,
                turn_id,
                status,
                error = %format!("{error:#}"),
                "failed to delete turn_runtime_snapshot row after turn close"
            );
        }
    }

    pub(super) fn handle_durable_agent_event<'a>(
        &'a self,
        event: AgentDurableEvent,
    ) -> MessageFuture<'a, bool> {
        match event {
            AgentDurableEvent::TurnSkillsResolved {
                thread_id,
                turn_id,
                bindings,
            } => self.handle_turn_skills_resolved_event(thread_id, turn_id, bindings),
            event => message_future(async move {
                let thread_id = durable_event_thread_id(&event).map(str::to_owned);
                let committed = self.persist_durable_agent_event(event.clone()).await;
                if committed {
                    self.kick_native_turn_event_deliveries();
                    if let Some(thread_id) = thread_id {
                        self.agent_manager
                            .publish_committed(thread_id.as_str(), event)
                            .await;
                    }
                }
                committed
            }),
        }
    }

    fn handle_turn_skills_resolved_event<'a>(
        &'a self,
        thread_id: String,
        turn_id: String,
        bindings: Vec<pioneer_protocol::TurnSkillBinding>,
    ) -> MessageFuture<'a, bool> {
        message_future(async move {
            if let Err(error) = self
                .reconcile_turn_runtime_snapshot_agent_overlay(
                    thread_id.as_str(),
                    turn_id.as_str(),
                    bindings.as_slice(),
                )
                .await
            {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to confirm Agent skill exposure before provider execution"
                );
                return false;
            }
            if let Err(error) = self
                .persist_turn_skill_projection(
                    thread_id.as_str(),
                    turn_id.as_str(),
                    bindings.as_slice(),
                )
                .await
            {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to persist and authorize exact turn skill projection"
                );
                return false;
            }
            let committed_event = AgentDurableEvent::TurnSkillsResolved {
                thread_id: thread_id.clone(),
                turn_id,
                bindings,
            };
            self.agent_manager
                .publish_committed(thread_id.as_str(), committed_event)
                .await;
            true
        })
    }

    async fn reconcile_turn_runtime_snapshot_agent_overlay(
        &self,
        thread_id: &str,
        turn_id: &str,
        bindings: &[pioneer_protocol::TurnSkillBinding],
    ) -> Result<()> {
        let agent_bindings = bindings
            .iter()
            .filter(|binding| binding.source_kind == "agent")
            .collect::<Vec<_>>();
        let Some(snapshot) = self
            .crud_store
            .get_turn_runtime_snapshot(turn_id)
            .await
            .context("failed to load the authoritative turn runtime snapshot")?
        else {
            if agent_bindings.is_empty() {
                return Ok(());
            }
            bail!("Agent skill exposure has no authoritative turn runtime snapshot");
        };
        if snapshot.thread_id != thread_id {
            bail!("Agent skill exposure does not match the authoritative snapshot thread");
        }

        let pins = crate::turn_runtime_snapshot::agent_skill_version_pins_from_snapshot(&snapshot)
            .context("authoritative Agent skill pins are invalid")?;
        if pins.is_empty() {
            if agent_bindings.is_empty() {
                return Ok(());
            }
            bail!("Agent skill exposure was not pinned in the authoritative snapshot");
        }

        if agent_bindings.is_empty() {
            let expected_json = snapshot
                .agent_skill_versions_json
                .as_deref()
                .context("nonempty Agent skill pins are missing their stored representation")?;
            let cleared = self
                .crud_store
                .clear_turn_runtime_snapshot_agent_skill_versions_if_matches(
                    thread_id,
                    turn_id,
                    expected_json,
                )
                .await
                .context("failed to persist the authoritative base-only runtime snapshot")?;
            if !cleared {
                bail!("authoritative Agent skill pins changed before base-only confirmation");
            }
            return Ok(());
        }

        if agent_bindings.iter().any(|binding| {
            binding.resolved_reason != "agent_catalog"
                || binding
                    .skill_version
                    .as_deref()
                    .and_then(|version| version.parse::<i64>().ok())
                    .is_none_or(|version| version <= 0)
        }) {
            bail!("Agent skill exposure contains invalid diagnostic metadata");
        }
        let mut expected = pins
            .iter()
            .map(|pin| (pin.skill_id.as_str().to_owned(), pin.fingerprint.clone()))
            .collect::<Vec<_>>();
        expected.sort();
        let mut actual = agent_bindings
            .iter()
            .map(|binding| {
                (
                    binding.skill_id.as_str().to_owned(),
                    binding.fingerprint.clone(),
                )
            })
            .collect::<Vec<_>>();
        actual.sort();
        actual.dedup();
        if actual.len() != agent_bindings.len() || actual != expected {
            bail!("Agent skill exposure does not match the authoritative pinned overlay");
        }
        Ok(())
    }

    async fn persist_turn_skill_projection(
        &self,
        thread_id: &str,
        turn_id: &str,
        bindings: &[pioneer_protocol::TurnSkillBinding],
    ) -> Result<()> {
        let context = self
            .load_turn_execution_authorization_context(turn_id)
            .await
            .context("failed to load execution authorization for skill projection")?;
        let context = if let Some(mut context) = context {
            let revalidated = context
                .revalidate_for_turn_scope(
                    self.crud_store.as_ref(),
                    context.workspace_id(),
                    thread_id,
                    turn_id,
                    crate::authorization::ResourceAction::ThreadWrite,
                    self.authorization_invalidation_hub.current_revision(),
                )
                .await
                .context("current execution authorization no longer permits skill use")?;
            if revalidated.principal().kind == pioneer_protocol::PrincipalKind::User {
                self.revalidate_member_skill_bindings(
                    revalidated.principal(),
                    context.workspace_id(),
                    bindings,
                )
                .await?;
            }
            let workspace_id = context.workspace_id().to_owned();
            context
                .bind_skill_projection(workspace_id.as_str(), bindings)
                .context("failed to bind exact skill projection")?;
            Some(context)
        } else {
            crate::authorization::ensure_contextless_execution_is_trusted(
                self.crud_store.as_ref(),
                turn_id,
            )
            .await
            .context("contextless skill projection is not a trusted legacy execution")?;
            None
        };

        let event_timestamp = now_timestamp_secs();
        let binding_records = bindings
            .iter()
            .map(|binding| pioneer_crud::TurnSkillBindingRecord {
                skill_id: binding.skill_id.clone(),
                skill_owner: binding.skill_owner.clone(),
                skill_slug: binding.skill_slug.clone(),
                skill_version: binding.skill_version.clone(),
                fingerprint: binding.fingerprint.clone(),
                source_kind: binding.source_kind.clone(),
                resolved_reason: binding.resolved_reason.clone(),
            })
            .collect::<Vec<_>>();
        if let Some(context) = context {
            let encoded = context
                .to_persisted_json()
                .context("failed to encode skill-bound execution authorization")?;
            self.crud_store
                .replace_turn_skill_bindings_with_authorization_context(
                    turn_id,
                    binding_records.as_slice(),
                    event_timestamp,
                    encoded.as_str(),
                )
                .await
                .context("failed to persist authorized turn skill projection")?;
        } else {
            self.crud_store
                .replace_turn_skill_bindings(turn_id, binding_records.as_slice(), event_timestamp)
                .await
                .context("failed to persist exact turn skill bindings")?;
        }
        Ok(())
    }

    async fn revalidate_member_skill_bindings(
        &self,
        principal: &crate::auth::AuthenticatedSessionPrincipal,
        workspace_id: &str,
        bindings: &[pioneer_protocol::TurnSkillBinding],
    ) -> Result<()> {
        let action_gate = crate::authorization::AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            crate::authorization::ResourceAction::SkillUse,
        );
        let resolver =
            crate::authorization::AuthorizationResolver::new(self.crud_store.as_ref().clone());
        let base_catalog = if bindings
            .iter()
            .any(|binding| binding.source_kind != "agent")
        {
            let context = self
                .skills_runtime_context(workspace_id)
                .context("failed to resolve current skills runtime context")?;
            Some(
                self.load_skills_catalog(workspace_id, &context)
                    .await
                    .context("failed to load current skills catalog")?,
            )
        } else {
            None
        };
        let installations = self
            .crud_store
            .list_skill_installations()
            .await
            .context("failed to load current skill installations")?
            .into_iter()
            .map(|installation| (installation.skill_id.clone(), installation))
            .collect::<HashMap<_, _>>();
        let active_learned = self
            .crud_store
            .list_active_agent_skill_versions(workspace_id)
            .await
            .context("failed to load active learned skill versions")?
            .into_iter()
            .map(|version| (version.skill_id.clone(), version))
            .collect::<HashMap<_, _>>();
        let database = self.crud_store.database_connection();

        for binding in bindings {
            let authorization = resolver
                .authorize_persisted_capability(
                    principal,
                    &action_gate,
                    crate::authorization::ResourceAction::SkillUse,
                    workspace_id,
                    crate::authorization::CapabilityKind::Skill,
                    binding.skill_id.as_str(),
                )
                .await
                .context("failed to resolve current workspace skill policy")?;
            if !matches!(
                authorization,
                crate::authorization::ProofResolution::Authorized(_)
            ) {
                bail!(
                    "current workspace policy no longer permits skill `{}`",
                    binding.skill_id
                );
            }

            if binding.source_kind == "agent" {
                let active = active_learned.get(&binding.skill_id).with_context(|| {
                    format!("learned skill `{}` is no longer active", binding.skill_id)
                })?;
                let active_version = active.version.version_number.to_string();
                if binding.skill_version.as_deref() != Some(active_version.as_str())
                    || binding.fingerprint != active.version.fingerprint
                {
                    bail!(
                        "learned skill `{}` no longer matches its immutable projection",
                        binding.skill_id
                    );
                }
                if pioneer_crud::derive_member_learned_version_eligibility(
                    &database,
                    workspace_id,
                    active.version.id.as_str(),
                )
                .await
                .context("failed to revalidate learned skill provenance")?
                    != pioneer_crud::MemberLearnedVersionEligibility::Eligible
                {
                    bail!(
                        "learned skill `{}` is not eligible for Member use",
                        binding.skill_id
                    );
                }
                continue;
            }

            let skill = base_catalog
                .as_ref()
                .and_then(|catalog| {
                    catalog
                        .skills
                        .iter()
                        .find(|skill| skill.identity.skill_id == binding.skill_id)
                })
                .with_context(|| format!("skill `{}` is no longer installed", binding.skill_id))?;
            if !skill.is_available()
                || skill.identity.fingerprint != binding.fingerprint
                || skill.identity.version_hint != binding.skill_version
                || skill.identity.source_kind.as_db_value() != binding.source_kind
            {
                bail!(
                    "skill `{}` no longer matches its immutable projection",
                    binding.skill_id
                );
            }
            if !matches!(
                skill.identity.source_kind,
                pioneer_skills::SkillSourceKind::System
            ) {
                let installation = installations.get(&binding.skill_id).with_context(|| {
                    format!("skill `{}` is no longer installed", binding.skill_id)
                })?;
                if installation.scope_key != workspace_id
                    || installation.fingerprint != binding.fingerprint
                    || installation.version != binding.skill_version
                    || installation.source_kind != binding.source_kind
                {
                    bail!(
                        "skill `{}` current installation differs from its projection",
                        binding.skill_id
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn revalidate_persisted_turn_skill_projection(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<()> {
        let Some(context) = self
            .load_turn_execution_authorization_context(turn_id)
            .await
            .context("failed to load persisted execution authorization for skills")?
        else {
            crate::authorization::ensure_contextless_execution_is_trusted(
                self.crud_store.as_ref(),
                turn_id,
            )
            .await
            .context("contextless skill continuation is not a trusted legacy execution")?;
            return Ok(());
        };
        let bindings = self
            .crud_store
            .find_turn_skill_bindings(turn_id)
            .await
            .context("failed to load persisted turn skill bindings")?
            .into_iter()
            .map(|binding| pioneer_protocol::TurnSkillBinding {
                skill_id: binding.skill_id,
                skill_owner: binding.skill_owner,
                skill_slug: binding.skill_slug,
                skill_version: binding.skill_version,
                fingerprint: binding.fingerprint,
                source_kind: binding.source_kind,
                resolved_reason: binding.resolved_reason,
            })
            .collect::<Vec<_>>();
        context
            .verify_skill_projection(context.workspace_id(), bindings.as_slice())
            .context("persisted turn skill projection is stale or unbound")?;
        let revalidated = context
            .revalidate_for_turn_scope(
                self.crud_store.as_ref(),
                context.workspace_id(),
                thread_id,
                turn_id,
                crate::authorization::ResourceAction::ThreadWrite,
                self.authorization_invalidation_hub.current_revision(),
            )
            .await
            .context("current execution authorization no longer permits skill continuation")?;
        if revalidated.principal().kind == pioneer_protocol::PrincipalKind::User {
            self.revalidate_member_skill_bindings(
                revalidated.principal(),
                context.workspace_id(),
                bindings.as_slice(),
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn authorize_persisted_turn_skill_continuation(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        skill_id: &pioneer_skills::SkillId,
        fingerprint: &str,
    ) -> Result<()> {
        self.revalidate_persisted_turn_skill_projection(thread_id, turn_id)
            .await?;
        let context = self
            .load_turn_execution_authorization_context(turn_id)
            .await
            .context("failed to load execution authorization for skill continuation")?
            .context("skill continuation has no execution authorization context")?;
        if context.workspace_id() != workspace_id {
            bail!("skill continuation differs from its authorized execution boundary");
        }
        let exact_binding_exists = self
            .crud_store
            .find_turn_skill_bindings(turn_id)
            .await
            .context("failed to load exact skill continuation binding")?
            .into_iter()
            .any(|binding| binding.skill_id == *skill_id && binding.fingerprint == fingerprint);
        if !exact_binding_exists {
            bail!("requested skill is absent from the immutable turn projection");
        }
        Ok(())
    }

    pub(crate) async fn revalidate_execution_authorization_for_turn(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        action: crate::authorization::ResourceAction,
    ) -> Result<crate::authorization::ExecutionAuthorizationContext> {
        let context = self
            .load_turn_execution_authorization_context(turn_id)
            .await
            .context("failed to load execution authorization context")?
            .context("turn has no initiating execution authorization context")?;
        context
            .revalidate_for_turn_scope(
                self.crud_store.as_ref(),
                workspace_id,
                thread_id,
                turn_id,
                action,
                self.authorization_invalidation_hub.current_revision(),
            )
            .await
            .context("initiating authority no longer permits turn continuation")?;
        Ok(context)
    }

    /// Revalidates the immutable authority envelope used by in-process tool
    /// providers. Legacy System/Superuser turns may have no envelope; a
    /// persisted Member initiator may never fall back to that unrestricted
    /// boundary.
    pub(crate) async fn revalidate_tool_execution_authorization(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        member_principal_hint: Option<&str>,
        action: crate::authorization::ResourceAction,
    ) -> Result<Option<crate::authorization::RevalidatedExecutionAuthorization>> {
        if let Some(context) = self
            .load_turn_execution_authorization_context(turn_id)
            .await
            .context("failed to load tool execution authorization context")?
        {
            let current = context
                .revalidate_for_turn_scope(
                    self.crud_store.as_ref(),
                    workspace_id,
                    thread_id,
                    turn_id,
                    action,
                    self.authorization_invalidation_hub.current_revision(),
                )
                .await
                .context("tool execution authority is no longer current")?;
            if let Some(principal_id) = member_principal_hint
                && current.principal().principal_id.as_str() != principal_id
            {
                anyhow::bail!("tool execution principal differs from its turn context");
            }
            return Ok(Some(current));
        }

        if member_principal_hint.is_some() {
            anyhow::bail!("Member tool execution has no authorization context");
        }

        let Some((stored_workspace_id, _)) = self.crud_store.get_turn(thread_id, turn_id).await?
        else {
            // Synthetic internal providers and pre-persistence tests retain
            // the legacy boundary only when they carry no Member identity.
            return Ok(None);
        };
        if stored_workspace_id != workspace_id {
            anyhow::bail!("tool turn is outside its declared workspace");
        }

        if let Some(pioneer_protocol::PersistedActorRef::Principal(principal_id)) =
            pioneer_crud::find_turn_initiator(&self.crud_store.database_connection(), turn_id)
                .await?
        {
            let principal = pioneer_crud::load_principal_by_id(
                &self.crud_store.database_connection(),
                &principal_id,
            )
            .await?
            .context("tool turn initiator no longer exists")?;
            if principal.kind == pioneer_protocol::PrincipalKind::User {
                anyhow::bail!("Member tool execution has no authorization context");
            }
        }

        Ok(None)
    }

    pub(super) async fn handle_snapshot_agent_event(
        &self,
        event: crate::cli_runtime::projector::AgentSnapshotEvent,
    ) {
        match event {
            crate::cli_runtime::projector::AgentSnapshotEvent::ItemUpdated { notification } => {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                let persist_snapshot = || {
                    let crud_store = self.crud_store.clone();
                    let notification = notification.clone();
                    async move {
                        super::message_fresh_task(async move {
                            crud_store
                                .materialize_item_snapshot_updated(notification, event_timestamp)
                                .await
                        })
                        .await
                        .map_err(|error| {
                            anyhow::anyhow!("item snapshot materialization task failed: {error}")
                        })?
                    }
                };
                if let Err(error) = super::retry_transient_storage_access(persist_snapshot).await {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "item snapshot update could not be materialized; keeping the native turn running"
                    );
                    return;
                }

                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_UPDATED,
                    &notification,
                )
                .await;
                self.notify_semantic_timeline_item_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    &notification.item,
                    None,
                )
                .await;
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                )
                .await;
            }
        }
    }

    pub(super) async fn ingest_committed_thread_item(
        &self,
        notification: &pioneer_protocol::ItemCompletedNotification,
    ) {
        if let Err(error) = self
            .ingest_committed_thread_item_with_result(notification)
            .await
        {
            warn!(
                error = %format!("{error:#}"),
                "thread episodic ingestion failed after committed item persistence"
            );
        }
    }

    async fn ingest_committed_thread_item_with_result(
        &self,
        notification: &pioneer_protocol::ItemCompletedNotification,
    ) -> Result<()> {
        let Some(input) = crate::thread_episodic::committed_item_ingestion_input(notification)
        else {
            debug!(
                workspace_id = notification.workspace_id,
                thread_id = notification.thread_id,
                turn_id = notification.turn_id,
                item_id = notification.item.item_id(),
                "skipping thread episodic ingestion for committed item with missing required ids"
            );
            return Ok(());
        };

        let wake_indexer = thread_episodic_index_wakeup_after_commit(&notification.item);
        let ingestor = self.thread_episodic_ingestor.read().await.clone();
        let ingestion_result =
            message_fresh_task(async move { ingestor.ingest_committed_item(input).await }).await;
        let ingestion_result = match ingestion_result {
            Ok(result) => result,
            Err(error) => Err(anyhow::anyhow!(
                "thread episodic committed-item ingestion task failed: {error}"
            )),
        };
        match ingestion_result {
            Ok(crate::thread_episodic::ThreadEpisodicIngestionOutcome::Accepted) => {
                debug!("thread episodic ingestion accepted committed item");
                if wake_indexer {
                    self.spawn_thread_episodic_index_run();
                }
            }
            Ok(crate::thread_episodic::ThreadEpisodicIngestionOutcome::Skipped { reason }) => {
                debug!(
                    reason = reason.as_str(),
                    "thread episodic ingestion skipped committed item"
                );
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    pub(super) async fn process_due_native_turn_event_deliveries(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<u64> {
        let mut processed = 0_u64;
        for consumer in [
            pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER,
            pioneer_crud::NATIVE_TURN_EVENT_EPISODIC_CONSUMER,
        ] {
            let deliveries = self
                .crud_store
                .claim_due_turn_event_deliveries(consumer, now_unix, limit)
                .await?;
            let mut by_turn = std::collections::BTreeMap::<
                String,
                Vec<pioneer_crud::ClaimedTurnEventDeliveryRecord>,
            >::new();
            for delivery in deliveries {
                by_turn
                    .entry(delivery.event.turn_id.clone())
                    .or_default()
                    .push(delivery);
            }
            processed = processed.saturating_add(
                by_turn
                    .values()
                    .map(|deliveries| deliveries.len() as u64)
                    .sum::<u64>(),
            );
            futures_util::stream::iter(by_turn.into_values())
                .for_each_concurrent(Some(16), |deliveries| async move {
                    for delivery in deliveries {
                        self.process_claimed_native_turn_event_delivery(delivery)
                            .await;
                    }
                })
                .await;
        }
        Ok(processed)
    }

    /// Wake the optional outbox after the canonical transaction commits.
    ///
    /// The durable commit ACK never waits for websocket, artifact, or episodic
    /// side effects. At the same time, delivery cannot depend solely on the
    /// process-wide resilience worker: restored/test-owned processors and a
    /// freshly admitted turn may not have reached that worker yet. Each kick
    /// drains a bounded number of batches; the durable retry schedule remains
    /// authoritative for failures and the periodic worker remains the restart
    /// safety net.
    pub(super) fn kick_native_turn_event_deliveries(&self) {
        if self
            .native_turn_event_delivery_kick_running
            .swap(true, Ordering::AcqRel)
        {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            struct KickGuard(Arc<AtomicBool>);

            impl Drop for KickGuard {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }

            let _guard = KickGuard(this.native_turn_event_delivery_kick_running.clone());
            for _ in 0..128 {
                match this
                    .process_due_native_turn_event_deliveries(now_timestamp_secs(), 64)
                    .await
                {
                    Ok(0) => break,
                    Ok(_) => tokio::task::yield_now().await,
                    Err(error) => {
                        warn!(
                            error = %format!("{error:#}"),
                            "post-commit native turn-event delivery kick failed"
                        );
                        break;
                    }
                }
            }
        });
    }

    async fn materialize_native_agent_turn_event(
        &self,
        event: pioneer_crud::CanonicalTurnEventPayload,
        event_timestamp_secs: i64,
        item_started_deadlines: Option<pioneer_crud::TurnItemAttemptDeadlines>,
    ) -> Result<()> {
        let result = self
            .crud_store
            .materialize_native_agent_turn_event(
                event,
                event_timestamp_secs,
                item_started_deadlines,
            )
            .await;
        match result {
            Ok(()) => {
                self.kick_native_turn_event_deliveries();
                Ok(())
            }
            Err(error) if pioneer_crud::turn_event_was_appended_before_error(&error) => {
                // The canonical event is the authoritative commit. A read-model
                // predecessor backlog is recoverable projection work and must
                // neither reject the producer ACK nor start a second execution.
                self.kick_native_turn_event_deliveries();
                warn!(
                    error = %format!("{error:#}"),
                    "native event committed while its ordered projection remains pending"
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn reconcile_incomplete_native_turn_admissions(
        &self,
        limit: u64,
    ) -> Result<u64> {
        let candidates = self
            .crud_store
            .list_incomplete_native_turn_admissions(limit)
            .await?;
        let mut reconciled = 0_u64;
        for candidate in candidates {
            let reason = "native turn admission was interrupted before its durable runtime snapshot; resume or retry from the preserved user message"
                .to_owned();
            if self
                .mark_turn_blocked(
                    candidate.thread_id.clone(),
                    candidate.turn_id.clone(),
                    reason,
                )
                .await
            {
                reconciled = reconciled.saturating_add(1);
            } else {
                warn!(
                    thread_id = candidate.thread_id,
                    turn_id = candidate.turn_id,
                    "incomplete native turn admission could not be reconciled"
                );
            }
        }
        Ok(reconciled)
    }

    async fn process_claimed_native_turn_event_delivery(
        &self,
        delivery: pioneer_crud::ClaimedTurnEventDeliveryRecord,
    ) {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            self.deliver_native_turn_event(&delivery),
        )
        .await
        .map_err(|_| anyhow::anyhow!("optional turn-event delivery timed out"))
        .and_then(|result| result);
        let completed_at = now_timestamp_secs();
        match result {
            Ok(()) => {
                if let Err(error) = self
                    .crud_store
                    .complete_turn_event_delivery(
                        delivery.id.as_str(),
                        delivery.claim_token.as_str(),
                        completed_at,
                    )
                    .await
                {
                    warn!(
                        delivery_id = delivery.id,
                        consumer = delivery.consumer,
                        error = %format!("{error:#}"),
                        "failed to acknowledge optional turn-event delivery"
                    );
                }
            }
            Err(error) => {
                if let Err(mark_error) = self
                    .crud_store
                    .fail_turn_event_delivery(
                        delivery.id.as_str(),
                        delivery.claim_token.as_str(),
                        delivery.attempt_count,
                        format!("{error:#}"),
                        completed_at,
                    )
                    .await
                {
                    warn!(
                        delivery_id = delivery.id,
                        consumer = delivery.consumer,
                        error = %format!("{mark_error:#}"),
                        "failed to record optional turn-event delivery failure"
                    );
                }
            }
        }
    }

    async fn deliver_native_turn_event(
        &self,
        delivery: &pioneer_crud::ClaimedTurnEventDeliveryRecord,
    ) -> Result<()> {
        match delivery.consumer.as_str() {
            pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER => {
                self.deliver_native_turn_event_live(&delivery.event.payload)
                    .await
            }
            pioneer_crud::NATIVE_TURN_EVENT_EPISODIC_CONSUMER => {
                let pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(notification) =
                    &delivery.event.payload
                else {
                    bail!(
                        "episodic delivery `{}` references non-completed event",
                        delivery.id
                    );
                };
                self.ingest_committed_thread_item_with_result(notification)
                    .await
            }
            consumer => bail!("unknown native turn-event delivery consumer `{consumer}`"),
        }
    }

    async fn deliver_native_turn_event_live(
        &self,
        payload: &pioneer_crud::CanonicalTurnEventPayload,
    ) -> Result<()> {
        use pioneer_crud::CanonicalTurnEventPayload as Event;

        match payload {
            Event::ItemStarted(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_STARTED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_item_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    &notification.item,
                    Some("in_progress"),
                )
                .await;
            }
            Event::ItemCompleted(notification) => {
                self.register_artifacts_for_completed_item(notification)
                    .await;
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_COMPLETED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_item_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    &notification.item,
                    None,
                )
                .await;
            }
            Event::ItemUpdated(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_UPDATED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_item_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    &notification.item,
                    None,
                )
                .await;
            }
            Event::ItemToolRetryScheduled(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_TOOL_RETRY_SCHEDULED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_work_item_id_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    notification.item_id.as_str(),
                )
                .await;
            }
            Event::ItemToolRetryResolved(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_TOOL_RETRY_RESOLVED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_work_item_id_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    notification.item_id.as_str(),
                )
                .await;
            }
            Event::ItemToolRetryExhausted(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_TOOL_RETRY_EXHAUSTED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_work_item_id_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    notification.item_id.as_str(),
                )
                .await;
            }
            Event::TurnToolLoopBudgetExceeded(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_TOOL_LOOP_BUDGET_EXCEEDED,
                    notification,
                )
                .await;
            }
            Event::TurnExecutionWindowStarted(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_STARTED,
                    notification,
                )
                .await;
            }
            Event::TurnExecutionWindowExhausted(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_EXHAUSTED,
                    notification,
                )
                .await;
            }
            Event::TurnExecutionWindowCheckpointed(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_CHECKPOINTED,
                    notification,
                )
                .await;
            }
            Event::TurnExecutionWindowContinued(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_CONTINUED,
                    notification,
                )
                .await;
            }
            Event::TurnExecutionWindowBlocked(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_BLOCKED,
                    notification,
                )
                .await;
            }
            Event::ItemTimeoutDetected(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_TIMEOUT_DETECTED,
                    notification,
                )
                .await;
            }
            Event::ItemRecoveryOpened(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_RECOVERY_OPENED,
                    notification,
                )
                .await;
            }
            Event::ItemRecoveryAttached(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_RECOVERY_ATTACHED,
                    notification,
                )
                .await;
            }
            Event::ItemRetryScheduled(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_RETRY_SCHEDULED,
                    notification,
                )
                .await;
            }
            Event::ItemRetryAttemptStarted(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_RETRY_ATTEMPT_STARTED,
                    notification,
                )
                .await;
            }
            Event::ItemRecoverySucceeded(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_RECOVERY_SUCCEEDED,
                    notification,
                )
                .await;
            }
            Event::ItemRecoveryExhausted(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_RECOVERY_EXHAUSTED,
                    notification,
                )
                .await;
            }
            Event::TurnCompleted(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_COMPLETED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
            }
            Event::TurnFailed(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_FAILED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
            }
            Event::TurnBlocked(notification) => {
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_BLOCKED,
                    notification,
                )
                .await;
                self.notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
            }
            Event::TurnStarted(_)
            | Event::TurnPermissionAudit(_)
            | Event::TurnMessageEdited(_)
            | Event::TurnMessageDeleted(_) => {
                bail!(
                    "event type `{}` is not a native live-delivery event",
                    payload.event_type()
                );
            }
        }

        self.notify_parent_timeline_changed_for_child_turn(
            payload.thread_id(),
            payload.turn_id(),
            Some(payload.workspace_id()),
        )
        .await;
        Ok(())
    }

    fn spawn_thread_episodic_index_run(&self) {
        let executor = self.thread_episodic_index_executor.clone();
        tokio::spawn(async move {
            let now_unix = chrono::Utc::now().timestamp();
            if let Err(error) = executor.run_once(now_unix).await {
                warn!(
                    error = %format!("{error:#}"),
                    "thread episodic index run failed"
                );
            }
        });
    }

    fn persist_durable_agent_event<'a>(
        &'a self,
        event: AgentDurableEvent,
    ) -> MessageFuture<'a, bool> {
        match event {
            AgentDurableEvent::PromptManifestCompiled {
                thread_id,
                turn_id,
                manifest,
            } => message_future(async move {
                self.thread_manager
                    .set_turn_prompt_manifest(
                        thread_id.as_str(),
                        turn_id.as_str(),
                        manifest.clone(),
                    )
                    .await;

                let event_timestamp = now_timestamp_secs();

                match self
                    .crud_store
                    .update_turn_prompt_manifest(
                        thread_id.as_str(),
                        turn_id.as_str(),
                        &manifest,
                        event_timestamp,
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(
                            thread_id,
                            turn_id, "turn was not found while persisting prompt manifest"
                        );
                        return false;
                    }
                    Err(error) => {
                        warn!(
                            thread_id,
                            turn_id,
                            error = %format!("{error:#}"),
                            "failed to persist prompt manifest metadata"
                        );
                        return false;
                    }
                }
                true
            }),
            AgentDurableEvent::TurnSkillsResolved {
                thread_id,
                turn_id,
                bindings,
            } => message_future(async move {
                if let Err(error) = self
                    .persist_turn_skill_projection(
                        thread_id.as_str(),
                        turn_id.as_str(),
                        bindings.as_slice(),
                    )
                    .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to persist durable turn skill projection"
                    );
                    return false;
                }
                true
            }),
            AgentDurableEvent::TurnCapabilitiesResolved {
                thread_id,
                turn_id,
                accepted,
                rejected,
                mcp_bindings,
            } => message_future(async move {
                debug!(
                    thread_id,
                    turn_id,
                    accepted = accepted.len(),
                    rejected = rejected.len(),
                    mcp_bindings = mcp_bindings.len(),
                    "observed durable capability result after pre-provider persistence"
                );
                if !rejected.is_empty() {
                    warn!(
                        thread_id,
                        turn_id,
                        accepted = accepted.len(),
                        rejected = rejected.len(),
                        "turn capability resolution rejected selected capabilities"
                    );
                }
                true
            }),
            AgentDurableEvent::SkillAuditEvents {
                thread_id,
                turn_id,
                events,
            } => message_future(async move {
                let mut records = Vec::new();
                let mut dependency_snapshots = Vec::new();

                for event in events {
                    let skill_id = event.skill_id;
                    let skill_owner = event.skill_owner;
                    let skill_slug = event.skill_slug;
                    let source_kind = event.source_kind;
                    let created_at_unix = event.created_at_unix;
                    let dependency_snapshot_json = event
                        .details
                        .get("dependency_diagnostics")
                        .and_then(|value| {
                            value
                                .as_array()
                                .filter(|diagnostics| !diagnostics.is_empty())
                                .map(|_| value.to_string())
                        });

                    records.push(pioneer_crud::SkillAuditEventRecord {
                        turn_id: Some(turn_id.clone()),
                        skill_id: skill_id.clone(),
                        skill_owner: skill_owner.clone(),
                        skill_slug: skill_slug.clone(),
                        source_kind: source_kind.clone(),
                        action: event.action,
                        decision: event.decision,
                        reason_code: event.reason_code,
                        details_json: event.details.to_string(),
                        created_at_unix,
                    });

                    if let Some(diagnostics_json) = dependency_snapshot_json {
                        dependency_snapshots.push(pioneer_crud::SkillDependencySnapshotRecord {
                            turn_id: Some(turn_id.clone()),
                            skill_id,
                            skill_owner,
                            skill_slug,
                            source_kind,
                            diagnostics_json,
                            created_at_unix,
                        });
                    }
                }

                if let Err(error) = self
                    .crud_store
                    .persist_native_skill_audit_bundle(
                        turn_id.as_str(),
                        records.as_slice(),
                        dependency_snapshots.as_slice(),
                    )
                    .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to persist skill audit events"
                    );
                    return false;
                }
                true
            }),
            AgentDurableEvent::TurnPermissionAudit { event } => message_future(async move {
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_turn_permission_audit(event.clone(), event_timestamp)
                    .await
                {
                    warn!(
                        thread_id = event.thread_id,
                        turn_id = event.turn_id,
                        error = %format!("{error:#}"),
                        "failed to persist turn permission audit event"
                    );
                    return false;
                }
                true
            }),
            AgentDurableEvent::TurnLlmContextAppended {
                thread_id,
                turn_id,
                item_id,
                attempt_id,
                sequence,
                source,
                tool_name,
                payload,
                output_policy_snapshot,
            } => message_future(async move {
                let created_at = now_db_timestamp();
                let payload = match serde_json::to_string(&tool_result_view_from_protocol(payload))
                {
                    Ok(payload) => payload,
                    Err(error) => {
                        warn!(turn_id = %turn_id, error = %error, "rejecting unserializable retained tool result");
                        return false;
                    }
                };
                let output_policy_snapshot = match serde_json::to_string(&output_policy_snapshot) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        warn!(turn_id = %turn_id, error = %error, "rejecting unserializable retained tool output policy");
                        return false;
                    }
                };
                let delivery_key = turn_llm_context_delivery_key(&[
                    "tool_result",
                    item_id.as_str(),
                    attempt_id.as_deref().unwrap_or(""),
                ]);
                let entry = pioneer_crud::NewTurnLlmContextEntry {
                    turn_id: turn_id.clone(),
                    item_id: Some(item_id),
                    attempt_id,
                    sequence,
                    source,
                    tool_name: Some(tool_name),
                    payload,
                    output_policy_snapshot,
                    created_at,
                    expires_at: None,
                };
                if let Err(error) = self
                    .crud_store
                    .append_turn_llm_context(entry, delivery_key.as_str())
                    .await
                {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to persist turn llm context: {error:#}"),
                    )
                    .await;
                    return false;
                }
                true
            }),
            AgentDurableEvent::TurnProviderHistoryAppended {
                thread_id,
                turn_id,
                item_id,
                sequence,
                payload,
            } => message_future(async move {
                let created_at = now_db_timestamp();
                let payload = match serde_json::to_string(&payload) {
                    Ok(payload) => payload,
                    Err(error) => {
                        self.report_legacy_turn_failure(
                            thread_id,
                            turn_id,
                            format!("failed to serialize retained provider history: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                };
                let delivery_key =
                    turn_llm_context_delivery_key(&["assistant_round", item_id.as_str()]);
                let entry = pioneer_crud::NewTurnLlmContextEntry {
                    turn_id: turn_id.clone(),
                    item_id: Some(item_id),
                    attempt_id: None,
                    sequence,
                    source: "assistant_round".to_owned(),
                    tool_name: None,
                    payload,
                    output_policy_snapshot: serde_json::json!({}).to_string(),
                    created_at,
                    expires_at: None,
                };
                if let Err(error) = self
                    .crud_store
                    .append_turn_llm_context(entry, delivery_key.as_str())
                    .await
                {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to persist retained provider history: {error:#}"),
                    )
                    .await;
                    return false;
                }
                true
            }),
            AgentDurableEvent::ItemStarted { notification } => message_future(async move {
                let mut notification = notification;
                self.enrich_item_started_markdown(&mut notification).await;
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                let item_id = notification.item.item_id().to_owned();
                let deadlines = self
                    .timeout_supervisor
                    .deadlines_for_item(&notification.item, event_timestamp);
                if let Err(error) = message_future(self.materialize_native_agent_turn_event(
                    pioneer_crud::CanonicalTurnEventPayload::ItemStarted(notification.clone()),
                    event_timestamp,
                    Some(deadlines),
                ))
                .await
                {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to persist item/started: {error:#}"),
                    )
                    .await;
                    return false;
                }
                debug!(
                    thread_id = notification.thread_id,
                    turn_id = notification.turn_id,
                    item_id,
                    item_type = ?notification.item.item_type(),
                    "registered item attempt deadlines during item/started projection"
                );
                true
            }),
            AgentDurableEvent::ItemCompleted { notification } => message_future(async move {
                let mut notification = notification;
                self.enrich_item_completed_markdown(&mut notification).await;
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = message_future(self.materialize_native_agent_turn_event(
                    pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(notification.clone()),
                    event_timestamp,
                    None,
                ))
                .await
                {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to persist item/completed: {error:#}"),
                    )
                    .await;
                    return false;
                }
                // This tiny in-process projection participates in the
                // completion guard and therefore must be visible before the
                // agent receives the canonical commit ACK. Network, artifact,
                // and episodic work remains on the optional outbox.
                self.record_final_assistant_text_for_item(&notification)
                    .await;
                true
            }),
            AgentDurableEvent::TurnFinalizationPrepared {
                notification,
                generation,
            } => message_future(async move {
                let mut notification = notification;
                self.enrich_item_completed_markdown(&mut notification).await;
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                if let Err(error) = self
                    .crud_store
                    .prepare_turn_finalization(&notification, generation, now_timestamp_secs())
                    .await
                {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to persist turn finalization intent: {error:#}"),
                    )
                    .await;
                    return false;
                }
                true
            }),
            AgentDurableEvent::ItemToolRetryScheduled { notification } => {
                message_future(async move {
                    let thread_id = notification.thread_id.clone();
                    let turn_id = notification.turn_id.clone();
                    let event_timestamp = now_timestamp_secs();
                    if let Err(error) = self
                        .materialize_native_agent_turn_event(
                            pioneer_crud::CanonicalTurnEventPayload::ItemToolRetryScheduled(
                                notification.clone(),
                            ),
                            event_timestamp,
                            None,
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id,
                            turn_id,
                            format!("failed to persist item/tool/retry_scheduled: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    true
                })
            }
            AgentDurableEvent::ItemToolRetryResolved { notification } => {
                message_future(async move {
                    let thread_id = notification.thread_id.clone();
                    let turn_id = notification.turn_id.clone();
                    let event_timestamp = now_timestamp_secs();
                    if let Err(error) = self
                        .materialize_native_agent_turn_event(
                            pioneer_crud::CanonicalTurnEventPayload::ItemToolRetryResolved(
                                notification.clone(),
                            ),
                            event_timestamp,
                            None,
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id,
                            turn_id,
                            format!("failed to persist item/tool/retry_resolved: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    true
                })
            }
            AgentDurableEvent::ItemToolRetryExhausted { notification } => {
                message_future(async move {
                    let thread_id = notification.thread_id.clone();
                    let turn_id = notification.turn_id.clone();
                    let event_timestamp = now_timestamp_secs();
                    if let Err(error) = self
                        .materialize_native_agent_turn_event(
                            pioneer_crud::CanonicalTurnEventPayload::ItemToolRetryExhausted(
                                notification.clone(),
                            ),
                            event_timestamp,
                            None,
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id,
                            turn_id,
                            format!("failed to persist item/tool/retry_exhausted: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    true
                })
            }
            AgentDurableEvent::TurnToolLoopBudgetExceeded { notification } => {
                message_future(async move {
                    let thread_id = notification.thread_id.clone();
                    let turn_id = notification.turn_id.clone();
                    let event_timestamp = now_timestamp_secs();
                    if let Err(error) = self
                        .materialize_native_agent_turn_event(
                            pioneer_crud::CanonicalTurnEventPayload::TurnToolLoopBudgetExceeded(
                                notification.clone(),
                            ),
                            event_timestamp,
                            None,
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id,
                            turn_id,
                            format!("failed to persist turn/tool_loop/budget_exceeded: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    true
                })
            }
            AgentDurableEvent::TurnExecutionWindowStarted { notification } => {
                message_future(async move {
                    let thread_id = notification.thread_id.clone();
                    let turn_id = notification.turn_id.clone();
                    let event_timestamp = now_timestamp_secs();
                    let event_time =
                        db_timestamp_from_unix_ms(event_timestamp.saturating_mul(1000));
                    if let Err(error) = self
                        .crud_store
                        .materialize_native_execution_window_transition(
                            pioneer_crud::NativeExecutionWindowTransition::Started {
                                window: pioneer_crud::NewTurnExecutionWindowRecord {
                                    workspace_id: notification.workspace_id.clone(),
                                    thread_id: notification.thread_id.clone(),
                                    turn_id: notification.turn_id.clone(),
                                    window_index: notification.window_index,
                                    status: notification.status,
                                    exhaustion_reason: None,
                                    agent_round_count: 0,
                                    tool_call_count: 0,
                                    provider_token_count: 0,
                                    metadata_json: execution_window_started_metadata(
                                        notification.window_id.as_str(),
                                    ),
                                    started_at: db_timestamp_from_unix_ms(
                                        notification.started_at_unix_ms,
                                    ),
                                },
                                notification,
                                created_at: event_time,
                                updated_at: event_time,
                            },
                            event_timestamp,
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to persist turn/execution_window/started: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    true
                })
            }
            AgentDurableEvent::TurnExecutionWindowExhausted { notification } => {
                message_future(async move {
                    let thread_id = notification.thread_id.clone();
                    let turn_id = notification.turn_id.clone();
                    let event_timestamp = now_timestamp_secs();
                    let stats = pioneer_crud::TurnExecutionWindowStatsRecord {
                        agent_round_count: notification.agent_round_count,
                        tool_call_count: notification.tool_call_count,
                        provider_token_count: notification.provider_token_count.unwrap_or(0),
                        metadata_json: execution_window_exhausted_metadata(
                            notification.window_id.as_str(),
                            notification.limit,
                            notification.observed,
                            notification.reason.as_str(),
                        ),
                        completed_at: db_timestamp_from_unix_ms(notification.exhausted_at_unix_ms),
                        updated_at: now_db_timestamp(),
                    };
                    if let Err(error) = self
                        .crud_store
                        .materialize_native_execution_window_transition(
                            pioneer_crud::NativeExecutionWindowTransition::Exhausted {
                                notification,
                                stats,
                            },
                            event_timestamp,
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to persist turn/execution_window/exhausted: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    true
                })
            }
            AgentDurableEvent::TurnExecutionWindowCheckpointed {
                notification,
                payload,
            } => message_future(async move {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                let Some(checkpoint_kind) =
                    execution_checkpoint_kind_from_wire(notification.checkpoint_kind.as_str())
                else {
                    warn!(
                        turn_id = %notification.turn_id,
                        checkpoint_kind = %notification.checkpoint_kind,
                        "rejecting execution window checkpoint with unknown kind"
                    );
                    return false;
                };
                let payload_json = match serde_json::to_value(&payload) {
                    Ok(payload_json) => payload_json,
                    Err(error) => {
                        warn!(
                            turn_id = %notification.turn_id,
                            error = %format!("{error:#}"),
                            "rejecting execution window checkpoint with unserializable payload"
                        );
                        return false;
                    }
                };
                let payload_size = match serde_json::to_vec(&payload_json) {
                    Ok(bytes) => bytes.len(),
                    Err(error) => {
                        warn!(
                            turn_id = %notification.turn_id,
                            error = %format!("{error:#}"),
                            "rejecting execution window checkpoint whose size cannot be measured"
                        );
                        return false;
                    }
                };
                if payload_size > pioneer_crud::TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES
                    || u64::try_from(payload_size).ok() != Some(notification.payload_bytes)
                {
                    warn!(
                        turn_id = %notification.turn_id,
                        payload_size,
                        declared_payload_size = notification.payload_bytes,
                        max_payload_size = pioneer_crud::TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES,
                        "rejecting invalid execution window checkpoint payload size"
                    );
                    return false;
                }
                if let Err(error) = self
                    .crud_store
                    .materialize_native_execution_window_transition(
                        pioneer_crud::NativeExecutionWindowTransition::Checkpointed {
                            checkpoint: pioneer_crud::NewTurnExecutionCheckpointRecord {
                                id: Some(notification.checkpoint_id.clone()),
                                window_id: String::new(),
                                workspace_id: notification.workspace_id.clone(),
                                thread_id: notification.thread_id.clone(),
                                turn_id: notification.turn_id.clone(),
                                checkpoint_kind,
                                payload_json,
                                created_at: db_timestamp_from_unix_ms(
                                    notification.created_at_unix_ms,
                                ),
                            },
                            notification,
                        },
                        event_timestamp,
                    )
                    .await
                {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to persist execution-window checkpoint: {error:#}"),
                    )
                    .await;
                    return false;
                }
                true
            }),
            AgentDurableEvent::TurnExecutionWindowContinued { notification } => {
                message_future(async move {
                    let event_timestamp = now_timestamp_secs();
                    let thread_id = notification.thread_id.clone();
                    let turn_id = notification.turn_id.clone();
                    let updated_at = db_timestamp_from_unix_ms(notification.continued_at_unix_ms);
                    if let Err(error) = self
                        .crud_store
                        .materialize_native_execution_window_transition(
                            pioneer_crud::NativeExecutionWindowTransition::Continued {
                                notification,
                                updated_at,
                            },
                            event_timestamp,
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id,
                            turn_id,
                            format!("failed to persist turn/execution_window/continued: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    true
                })
            }
            AgentDurableEvent::TurnExecutionWindowBlocked { notification } => {
                message_future(async move {
                    let event_timestamp = now_timestamp_secs();
                    let thread_id = notification.thread_id.clone();
                    let turn_id = notification.turn_id.clone();
                    let reason = notification.reason.clone();
                    let stats = pioneer_crud::TurnExecutionWindowStatsRecord {
                        agent_round_count: 0,
                        tool_call_count: notification.total_tool_calls,
                        provider_token_count: 0,
                        metadata_json: execution_window_blocked_metadata(
                            notification.window_id.as_str(),
                            notification.total_windows,
                            notification.total_tool_calls,
                            notification.reason.as_str(),
                        ),
                        completed_at: db_timestamp_from_unix_ms(notification.blocked_at_unix_ms),
                        updated_at: now_db_timestamp(),
                    };
                    if let Err(error) = self
                        .crud_store
                        .materialize_native_execution_window_transition(
                            pioneer_crud::NativeExecutionWindowTransition::Blocked {
                                notification,
                                stats,
                            },
                            event_timestamp,
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to persist turn/execution_window/blocked: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    if reason.contains("execution window continuation could not resume") {
                        if !self
                            .report_turn_failure(
                                thread_id,
                                turn_id,
                                TurnFailureRecoveryKind::ExecutionWindowContinuation,
                                reason,
                            )
                            .await
                        {
                            return false;
                        }
                    } else if !self.mark_turn_blocked(thread_id, turn_id, reason).await {
                        return false;
                    }
                    true
                })
            }
            AgentDurableEvent::TurnCompleted {
                thread_id,
                turn_id,
                recovery,
            } => message_future(async move {
                if !message_future(self.complete_native_turn(thread_id, turn_id, recovery)).await {
                    return false;
                }
                true
            }),
            AgentDurableEvent::ProviderFailureDetected {
                thread_id,
                turn_id,
                item_id,
                item_type,
                failure,
                recovery,
            } => message_future(async move {
                message_future(self.handle_provider_failure_detected(
                    thread_id, turn_id, item_id, item_type, failure, recovery,
                ))
                .await
            }),
            AgentDurableEvent::RecoveryAttemptSucceeded {
                thread_id,
                turn_id,
                recovery,
            } => message_future(async move {
                message_future(self.handle_recovery_attempt_succeeded(thread_id, turn_id, recovery))
                    .await
            }),
            AgentDurableEvent::TurnFailed {
                thread_id,
                turn_id,
                error,
                recovery,
            } => message_future(async move {
                if recovery.is_some() {
                    if !message_future(
                        self.mark_turn_failed_with_recovery(thread_id, turn_id, error, recovery),
                    )
                    .await
                    {
                        return false;
                    }
                } else if !message_future(self.report_turn_failure(
                    thread_id,
                    turn_id,
                    TurnFailureRecoveryKind::RuntimeFailure,
                    error,
                ))
                .await
                {
                    return false;
                }
                true
            }),
            AgentDurableEvent::TurnBlocked {
                thread_id,
                turn_id,
                reason,
                recovery,
            } => message_future(async move {
                if recovery.is_none()
                    && reason.contains("execution window continuation could not resume")
                {
                    if !message_future(self.report_turn_failure(
                        thread_id,
                        turn_id,
                        TurnFailureRecoveryKind::ExecutionWindowContinuation,
                        reason,
                    ))
                    .await
                    {
                        return false;
                    }
                } else if !message_future(
                    self.mark_turn_blocked_with_recovery(thread_id, turn_id, reason, recovery),
                )
                .await
                {
                    return false;
                }
                true
            }),
            AgentDurableEvent::TurnInterrupted {
                thread_id,
                turn_id,
                reason,
                recovery,
            } => message_future(async move {
                if !message_future(
                    self.mark_turn_interrupted_with_recovery(thread_id, turn_id, reason, recovery),
                )
                .await
                {
                    return false;
                }
                true
            }),
            AgentDurableEvent::TaskEvent { .. }
            | AgentDurableEvent::ThreadLineageCreated { .. } => message_future(async { false }),
        }
    }

    async fn record_final_assistant_text_for_item(
        &self,
        notification: &pioneer_protocol::ItemCompletedNotification,
    ) {
        if let TurnItem::AgentMessage { text, .. } = &notification.item {
            self.turn_final_assistant_texts
                .lock()
                .await
                .insert(notification.turn_id.clone(), text.clone());
        }
    }

    pub(super) async fn handle_progress_agent_event(&self, event: AgentProgressEvent) {
        match event {
            AgentProgressEvent::ItemDelta { notification } => {
                let mut notification = notification;
                self.enrich_item_delta_markdown(&mut notification).await;
                let event_timestamp = now_timestamp_secs();
                let delta_method = Self::item_delta_event_method(notification.stream);
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    delta_method,
                    &notification,
                )
                .await;

                let item_type = match self
                    .crud_store
                    .get_turn_item_type(
                        notification.turn_id.as_str(),
                        notification.item_id.as_str(),
                    )
                    .await
                {
                    Ok(Some(item_type)) => Some(item_type),
                    Ok(None) => None,
                    Err(error) => {
                        warn!(
                            thread_id = notification.thread_id,
                            turn_id = notification.turn_id,
                            item_id = notification.item_id,
                            error = %format!("{error:#}"),
                            "failed to resolve turn item type for heartbeat"
                        );
                        None
                    }
                };

                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                )
                .await;

                if matches!(item_type, Some(TurnItemType::Reasoning)) {
                    self.forward_child_reasoning_delta_to_parent_turn(&notification)
                        .await;
                }

                if let Some(item_type) = item_type
                    && let Err(error) = self
                        .timeout_supervisor
                        .heartbeat_item_attempt(
                            notification.turn_id.as_str(),
                            notification.item_id.as_str(),
                            item_type,
                            event_timestamp,
                        )
                        .await
                {
                    warn!(
                        thread_id = notification.thread_id,
                        turn_id = notification.turn_id,
                        item_id = notification.item_id,
                        error = %format!("{error:#}"),
                        "failed to heartbeat item attempt from delta"
                    );
                }
            }
            AgentProgressEvent::ItemHeartbeat {
                workspace_id: _,
                thread_id,
                turn_id,
                item_id,
                item_type,
            } => {
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .timeout_supervisor
                    .heartbeat_item_attempt(
                        turn_id.as_str(),
                        item_id.as_str(),
                        item_type,
                        event_timestamp,
                    )
                    .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        item_id,
                        error = %format!("{error:#}"),
                        "failed to heartbeat item attempt from agent heartbeat"
                    );
                }
            }
            AgentProgressEvent::ToolOutputDelta { .. }
            | AgentProgressEvent::TaskProgress { .. } => {}
        }
    }

    pub(super) async fn handle_recovery_attempt_succeeded(
        &self,
        thread_id: String,
        turn_id: String,
        recovery: pioneer_protocol::RecoveryAttemptContext,
    ) -> bool {
        let now_unix = now_timestamp_secs();
        match self
            .recovery_coordinator
            .succeed_active_recovery_attempt(turn_id.as_str(), &recovery, now_unix)
            .await
        {
            Ok(events) => {
                if events.is_empty() {
                    let job = match self
                        .crud_store
                        .get_recovery_job(recovery.job_id.as_str())
                        .await
                    {
                        Ok(Some(job))
                            if job.turn_id == turn_id
                                && job.status == pioneer_protocol::RecoveryJobStatus::Succeeded =>
                        {
                            job
                        }
                        Ok(_) => return false,
                        Err(error) => {
                            warn!(
                                thread_id,
                                turn_id,
                                recovery_job_id = recovery.job_id,
                                error = %format!("{error:#}"),
                                "failed to confirm idempotent recovery success"
                            );
                            return false;
                        }
                    };
                    let attempt_number =
                        if job.trigger == pioneer_protocol::RecoveryTrigger::ProviderError {
                            u32::try_from(job.provider_attempt_number.max(0)).unwrap_or(u32::MAX)
                        } else {
                            u32::try_from(job.run_count.max(0)).unwrap_or(u32::MAX)
                        };
                    return self
                        .handle_recovery_succeeded_event(
                            job.id,
                            job.turn_id,
                            job.item_id,
                            job.item_type,
                            attempt_number,
                            now_unix,
                        )
                        .await;
                }
                let mut committed = true;
                for event in events {
                    committed &= self.handle_recovery_event(event, now_unix).await;
                }
                committed
            }
            Err(error) => {
                self.report_legacy_turn_failure(
                    thread_id,
                    turn_id,
                    format!("failed to mark recovery attempt succeeded: {error:#}"),
                )
                .await;
                false
            }
        }
    }

    pub(super) async fn handle_cli_runtime_recovery_native_failure(
        &self,
        turn_id: String,
        recovery: pioneer_protocol::RecoveryAttemptContext,
        failure_message: String,
    ) {
        let now_unix = now_timestamp_secs();
        match self
            .recovery_coordinator
            .record_cli_runtime_attempt_failure(
                recovery.job_id.as_str(),
                recovery.attempt_id.as_str(),
                failure_message,
                now_unix,
            )
            .await
        {
            Ok(events) => {
                for event in events {
                    self.handle_recovery_event(event, now_unix).await;
                }
            }
            Err(error) => {
                warn!(
                    turn_id,
                    recovery_job_id = recovery.job_id,
                    recovery_attempt_id = recovery.attempt_id,
                    error = %format!("{error:#}"),
                    "failed to record native CLI recovery attempt failure"
                );
            }
        }
    }

    pub(super) async fn report_turn_failure(
        &self,
        thread_id: String,
        turn_id: String,
        kind: TurnFailureRecoveryKind,
        error_message: String,
    ) -> bool {
        let turn_state = match self
            .crud_store
            .get_turn(thread_id.as_str(), turn_id.as_str())
            .await
        {
            Ok(state) => state,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    failure_kind = kind.label(),
                    error = %format!("{error:#}"),
                    "failed to load turn before reporting recoverable failure"
                );
                return false;
            }
        };
        let Some((_workspace_id, turn)) = turn_state else {
            warn!(
                thread_id,
                turn_id,
                failure_kind = kind.label(),
                "turn missing before reporting recoverable failure"
            );
            return false;
        };
        if turn.status != TurnStatus::InProgress {
            return false;
        }

        let now_unix = now_timestamp_secs();
        let candidate = crate::resilience::RuntimeFailureCandidate {
            turn_id: turn_id.clone(),
            item_id: format!("runtime:{}", kind.label()),
            item_type: TurnItemType::SystemEvent,
            trigger: kind.trigger(),
            action: kind.action(),
            reason: error_message.clone(),
            base_backoff_secs: 2,
            max_attempts: 3,
            max_wall_clock_secs: crate::resilience::TURN_RECOVERY_MAX_WALL_CLOCK_SECS,
            no_progress_limit: 3,
            metadata: pioneer_protocol::ToolMetadata::from_json(serde_json::json!({
                "thread_id": thread_id,
                "turn_id": turn_id,
                "failure_kind": kind.label(),
                "error": error_message,
            })),
        };

        let outcome = match self
            .recovery_coordinator
            .enqueue_runtime_failure_job(&candidate, now_unix)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(
                    turn_id = candidate.turn_id,
                    failure_kind = kind.label(),
                    error = %format!("{error:#}"),
                    "failed to enqueue recoverable turn failure"
                );
                return false;
            }
        };

        let is_created = outcome.is_created();
        let next_attempt_number = outcome.next_attempt_number();
        let job = outcome.into_job();
        let event = if is_created {
            crate::resilience::RecoveryCoordinatorEvent::RecoveryOpened {
                job_id: job.id,
                turn_id: candidate.turn_id,
                item_id: candidate.item_id,
                item_type: candidate.item_type,
                trigger: job.trigger,
                action: job.action,
                attempt_number: next_attempt_number,
            }
        } else {
            crate::resilience::RecoveryCoordinatorEvent::RecoveryAttached {
                job_id: job.id,
                turn_id: candidate.turn_id,
                item_id: candidate.item_id,
                item_type: candidate.item_type,
                recovery_item_id: job.item_id,
                recovery_item_type: job.item_type,
                trigger: candidate.trigger,
                action: job.action,
                existing_status: job.status,
                next_attempt_number,
            }
        };
        self.handle_recovery_event(event, now_unix).await
    }

    pub(super) async fn handle_provider_failure_detected(
        &self,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        failure: pioneer_protocol::ProviderFailureDetails,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) -> bool {
        let now_unix = now_timestamp_secs();

        if let Some(recovery) = recovery {
            return match self
                .recovery_coordinator
                .record_recovery_provider_failure(
                    recovery.job_id.as_str(),
                    recovery.attempt_id.as_str(),
                    failure,
                    now_unix,
                )
                .await
            {
                Ok(events) => {
                    if events.is_empty() {
                        return self
                            .replay_applied_recovery_provider_failure(
                                turn_id.as_str(),
                                &recovery,
                                now_unix,
                            )
                            .await;
                    }
                    let mut committed = true;
                    for event in events {
                        committed &= self.handle_recovery_event(event, now_unix).await;
                    }
                    committed
                }
                Err(error) => {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to update provider recovery: {error:#}"),
                    )
                    .await;
                    false
                }
            };
        }

        let candidate = crate::resilience::ProviderFailureCandidate {
            turn_id: turn_id.clone(),
            item_id: item_id.clone(),
            item_type,
            failure,
        };

        match self
            .recovery_coordinator
            .enqueue_provider_failure_job(&candidate, now_unix)
            .await
        {
            Ok(outcome) => {
                let is_created = outcome.is_created();
                let next_attempt_number = outcome.next_attempt_number();
                let job = outcome.into_job();
                let should_terminalize = job.action == pioneer_protocol::RecoveryAction::MarkFailed;
                let workspace_id = match self.crud_store.get_turn_location(turn_id.as_str()).await {
                    Ok(Some((stored_thread_id, workspace_id))) if stored_thread_id == thread_id => {
                        workspace_id
                    }
                    Ok(_) => return false,
                    Err(error) => {
                        warn!(
                            thread_id,
                            turn_id,
                            error = %format!("{error:#}"),
                            "failed to resolve provider recovery turn location"
                        );
                        return false;
                    }
                };

                // Retrying an unknown commit result reuses the same provider
                // recovery job. Keep the opened-event identity stable instead
                // of changing it to an attached event on the retry.
                let owns_provider_failure = job.trigger
                    == pioneer_protocol::RecoveryTrigger::ProviderError
                    && job.item_id == item_id
                    && job.item_type == item_type;
                let recovery_event_committed = if is_created || owns_provider_failure {
                    self.persist_and_send_item_recovery_opened(
                        pioneer_protocol::ItemRecoveryOpenedNotification {
                            workspace_id,
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            item_id: item_id.clone(),
                            item_type,
                            recovery_job_id: job.id.clone(),
                            trigger: job.trigger,
                            action: job.action,
                            attempt_number: next_attempt_number,
                        },
                        now_unix,
                    )
                    .await
                } else {
                    self.persist_and_send_item_recovery_attached(
                        pioneer_protocol::ItemRecoveryAttachedNotification {
                            workspace_id,
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            item_id: item_id.clone(),
                            item_type,
                            recovery_job_id: job.id.clone(),
                            recovery_item_id: job.item_id.clone(),
                            recovery_item_type: job.item_type,
                            trigger: pioneer_protocol::RecoveryTrigger::ProviderError,
                            action: job.action,
                            existing_status: job.status,
                            next_attempt_number,
                        },
                        now_unix,
                    )
                    .await
                };
                if !recovery_event_committed {
                    return false;
                }

                if should_terminalize {
                    return match self
                        .recovery_coordinator
                        .terminalize_pending_mark_failed_job(job, now_unix)
                        .await
                    {
                        Ok(events) => {
                            let mut committed = !events.is_empty();
                            for event in events {
                                committed &= self.handle_recovery_event(event, now_unix).await;
                            }
                            committed
                        }
                        Err(error) => {
                            self.report_legacy_turn_failure(
                                thread_id,
                                turn_id,
                                format!("failed to apply terminal provider recovery: {error:#}"),
                            )
                            .await;
                            false
                        }
                    };
                }
                true
            }
            Err(error) => {
                self.report_legacy_turn_failure(
                    thread_id,
                    turn_id,
                    format!("failed to schedule provider recovery: {error:#}"),
                )
                .await;
                false
            }
        }
    }

    async fn replay_applied_recovery_provider_failure(
        &self,
        turn_id: &str,
        recovery: &pioneer_protocol::RecoveryAttemptContext,
        event_timestamp: i64,
    ) -> bool {
        let job = match self
            .crud_store
            .get_recovery_job(recovery.job_id.as_str())
            .await
        {
            Ok(Some(job)) if job.turn_id == turn_id => job,
            Ok(_) => return false,
            Err(error) => {
                warn!(
                    turn_id,
                    recovery_job_id = recovery.job_id,
                    error = %format!("{error:#}"),
                    "failed to reload applied provider recovery failure"
                );
                return false;
            }
        };

        let event = match job.status {
            pioneer_protocol::RecoveryJobStatus::Pending => {
                let current_attempt =
                    if job.trigger == pioneer_protocol::RecoveryTrigger::ProviderError {
                        u32::try_from(job.provider_attempt_number.max(0)).unwrap_or(u32::MAX)
                    } else {
                        u32::try_from(job.run_count.max(0)).unwrap_or(u32::MAX)
                    };
                crate::resilience::RecoveryCoordinatorEvent::RetryScheduled {
                    job_id: job.id,
                    turn_id: job.turn_id,
                    item_id: job.item_id,
                    item_type: job.item_type,
                    attempt_number: current_attempt.saturating_add(1),
                    next_run_at_unix: job.next_run_at_unix,
                    reason: job.last_error,
                }
            }
            pioneer_protocol::RecoveryJobStatus::Failed
            | pioneer_protocol::RecoveryJobStatus::Exhausted => {
                let status = job.status;
                let attempt_number =
                    if job.trigger == pioneer_protocol::RecoveryTrigger::ProviderError {
                        u32::try_from(job.provider_attempt_number.max(0)).unwrap_or(u32::MAX)
                    } else {
                        u32::try_from(job.run_count.max(0)).unwrap_or(u32::MAX)
                    };
                let persisted_error = job
                    .last_error
                    .unwrap_or_else(|| "provider recovery failed".to_owned());
                let error_message = [
                    "recovery wall-clock budget exhausted",
                    "recovery attempts exhausted",
                    "recovery no-progress guardrail exhausted",
                ]
                .into_iter()
                .find(|summary| persisted_error.starts_with(summary))
                .map(str::to_owned)
                .unwrap_or(persisted_error);
                crate::resilience::RecoveryCoordinatorEvent::RecoveryExhausted(
                    crate::resilience::RecoveryTerminalOutcome {
                        job_id: job.id,
                        turn_id: job.turn_id,
                        item_id: job.item_id,
                        item_type: job.item_type,
                        attempt_number,
                        status,
                        error_message,
                    },
                )
            }
            _ => return false,
        };
        self.handle_recovery_event(event, event_timestamp).await
    }

    pub(super) async fn handle_timeout_candidate(
        &self,
        candidate: TimeoutCandidate,
        now_unix: i64,
    ) {
        let mut active_recovery_job_id = None;
        let mut active_recovery_events = Vec::new();
        let classification = match self
            .timeout_supervisor
            .classify_timeout_candidate(&candidate)
            .await
        {
            Ok(classification) => classification,
            Err(error) => {
                warn!(
                    turn_id = %candidate.turn_id,
                    item_id = %candidate.item_id,
                    attempt_id = %candidate.attempt_id,
                    error = %format!("{error:#}"),
                    "failed to classify timeout candidate before recovery; falling back to recovery path"
                );
                crate::resilience::TimeoutRecoveryClassification::RecoverTurn
            }
        };
        let recovery_job_outcome = match classification {
            crate::resilience::TimeoutRecoveryClassification::SuppressRecoveryBecauseTurnProgressed { liveness } => {
                let context =
                    crate::resilience::timeout_recovery_suppression_context(&candidate, &liveness);
                if let Err(error) = self
                    .crud_store
                    .suppress_timeout_candidate_recovery(
                        &candidate,
                        crate::resilience::TIMEOUT_RECOVERY_SUPPRESSED_TURN_PROGRESS,
                        context,
                        now_unix,
                    )
                    .await
                {
                    warn!(
                        turn_id = %candidate.turn_id,
                        item_id = %candidate.item_id,
                        attempt_id = %candidate.attempt_id,
                        error = %format!("{error:#}"),
                        "failed to persist timeout recovery suppression for live turn"
                    );
                }
                None
            }
            crate::resilience::TimeoutRecoveryClassification::RecoverTurn => match self
                .recovery_coordinator
                .suppress_timeout_recovery_if_turn_not_in_progress(&candidate, now_unix)
                .await
            {
                Ok(true) => None,
                Ok(false) => match self
                    .recovery_coordinator
                    .record_recovery_timeout_failure(&candidate, now_unix)
                    .await
                {
                    Ok(Some((job_id, events))) => {
                        active_recovery_job_id = Some(job_id);
                        active_recovery_events = events;
                        None
                    }
                    Ok(None) => match self
                        .recovery_coordinator
                        .enqueue_timeout_job(&candidate, now_unix)
                        .await
                    {
                        Ok(job) => Some(job),
                        Err(error) => {
                            warn!(
                                turn_id = %candidate.turn_id,
                                item_id = %candidate.item_id,
                                attempt_id = %candidate.attempt_id,
                                error = %format!("{error:#}"),
                                "failed to enqueue recovery job for timed out attempt"
                            );
                            None
                        }
                    },
                    Err(error) => {
                        warn!(
                            turn_id = %candidate.turn_id,
                            item_id = %candidate.item_id,
                            attempt_id = %candidate.attempt_id,
                            error = %format!("{error:#}"),
                            "failed to update active recovery job for timed out attempt"
                        );
                        None
                    }
                },
                Err(error) => {
                    warn!(
                        turn_id = %candidate.turn_id,
                        item_id = %candidate.item_id,
                        attempt_id = %candidate.attempt_id,
                        error = %format!("{error:#}"),
                        "failed to check timeout candidate turn status before recovery"
                    );
                    None
                }
            },
        };

        let Some((thread_id, workspace_id)) = (match self
            .crud_store
            .get_turn_location(candidate.turn_id.as_str())
            .await
        {
            Ok(location) => location,
            Err(error) => {
                warn!(
                    turn_id = candidate.turn_id,
                    item_id = candidate.item_id,
                    error = %format!("{error:#}"),
                    "failed to resolve turn location for timeout notification"
                );
                None
            }
        }) else {
            return;
        };

        let notification = ItemTimeoutDetectedNotification {
            workspace_id,
            thread_id: thread_id.clone(),
            turn_id: candidate.turn_id,
            item_id: candidate.item_id,
            item_type: candidate.item_type,
            attempt_number: u32::try_from(candidate.attempt_number).unwrap_or(0),
            reason: candidate.timeout_reason,
            recovery_job_id: active_recovery_job_id.clone().or_else(|| {
                recovery_job_outcome
                    .as_ref()
                    .map(|outcome| outcome.job().id.clone())
            }),
        };

        self.persist_and_send_item_timeout_detected(notification.clone(), now_unix)
            .await;

        if let Some(outcome) = recovery_job_outcome {
            let is_created = outcome.is_created();
            let next_attempt_number = outcome.next_attempt_number();
            let job = outcome.into_job();
            if is_created {
                let opened = pioneer_protocol::ItemRecoveryOpenedNotification {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: job.id,
                    trigger: job.trigger,
                    action: job.action,
                    attempt_number: next_attempt_number,
                };

                self.persist_and_send_item_recovery_opened(opened, now_unix)
                    .await;
            } else {
                let attached = pioneer_protocol::ItemRecoveryAttachedNotification {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: job.id,
                    recovery_item_id: job.item_id,
                    recovery_item_type: job.item_type,
                    trigger: pioneer_protocol::RecoveryTrigger::Timeout,
                    action: job.action,
                    existing_status: job.status,
                    next_attempt_number,
                };

                self.persist_and_send_item_recovery_attached(attached, now_unix)
                    .await;
            }
        }

        for event in active_recovery_events {
            self.handle_recovery_event(event, now_unix).await;
        }
    }

    pub(super) async fn poll_timeouts_respecting_human_wait(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<TimeoutCandidate>> {
        let candidates = self
            .timeout_supervisor
            .list_timeout_candidates(now_unix, limit)
            .await?;
        let now_unix_ms = now_unix.saturating_mul(1_000);
        let mut timed_out = Vec::new();
        let mut cli_runtime_activity = HashMap::<String, bool>::new();
        let mut native_runtime_activity = HashMap::<String, bool>::new();
        for candidate in candidates {
            let cli_active = if let Some(active) = cli_runtime_activity.get(&candidate.turn_id) {
                *active
            } else {
                let active = self
                    .renew_active_cli_runtime_turn_deadlines(candidate.turn_id.as_str(), now_unix)
                    .await?;
                cli_runtime_activity.insert(candidate.turn_id.clone(), active);
                active
            };
            if cli_active {
                continue;
            }
            let native_active =
                if let Some(active) = native_runtime_activity.get(&candidate.turn_id) {
                    *active
                } else {
                    let active = self
                        .renew_active_native_runtime_turn_deadlines(
                            candidate.turn_id.as_str(),
                            now_unix,
                        )
                        .await?;
                    native_runtime_activity.insert(candidate.turn_id.clone(), active);
                    active
                };
            if native_active {
                continue;
            }
            if self
                .reconcile_cli_runtime_human_wait_for_turn(
                    candidate.turn_id.as_str(),
                    now_unix_ms,
                    "timeout supervisor",
                )
                .await?
            {
                continue;
            }
            if self
                .timeout_supervisor
                .transition_timeout_candidate(&candidate, now_unix)
                .await?
            {
                timed_out.push(candidate);
            }
        }
        Ok(timed_out)
    }

    async fn renew_active_native_runtime_turn_deadlines(
        &self,
        turn_id: &str,
        now_unix: i64,
    ) -> Result<bool> {
        let Some((thread_id, _workspace_id)) = self.crud_store.get_turn_location(turn_id).await?
        else {
            return Ok(false);
        };
        let Some(observation) = self
            .agent_manager
            .observe_turn(thread_id.as_str(), turn_id)
            .await
        else {
            return Ok(false);
        };
        self.ensure_agent_listener_task(thread_id.as_str()).await?;
        if observation.status != pioneer_agent::ExecutionTurnStatus::InProgress {
            debug!(
                thread_id,
                turn_id,
                status = ?observation.status,
                "native runtime has queued a terminal lifecycle event; deferring item timeout until the durable listener commits it"
            );
            return Ok(true);
        }

        let renewed = self
            .timeout_supervisor
            .renew_running_attempt_deadlines_for_turn(turn_id, now_unix)
            .await?;
        debug!(
            thread_id,
            turn_id,
            renewed,
            "renewed item deadlines from authoritative active native runtime turn"
        );
        // An in-process actor is not itself proof of liveness.  Return true
        // only when a causal durable frontier actually renewed an item lease;
        // otherwise the normal timeout/recovery path remains active for a hung
        // native Turn.
        Ok(renewed > 0)
    }

    pub(super) async fn handle_recovery_terminal_outcome(
        &self,
        outcome: RecoveryTerminalOutcome,
        event_timestamp: i64,
    ) {
        let Some((thread_id, workspace_id)) = (match self
            .crud_store
            .get_turn_location(outcome.turn_id.as_str())
            .await
        {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    job_id = outcome.job_id,
                    turn_id = outcome.turn_id,
                    item_id = outcome.item_id,
                    error = %format!("{error:#}"),
                    "failed to resolve turn location for terminal recovery outcome"
                );
                None
            }
        }) else {
            return;
        };

        let turn_state = match self
            .crud_store
            .get_turn(thread_id.as_str(), outcome.turn_id.as_str())
            .await
        {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    job_id = outcome.job_id,
                    turn_id = outcome.turn_id,
                    item_id = outcome.item_id,
                    error = %format!("{error:#}"),
                    "failed to load turn state for terminal recovery outcome"
                );
                None
            }
        };
        let Some((_workspace_id, turn)) = turn_state else {
            return;
        };
        if turn.status != TurnStatus::InProgress {
            return;
        }

        let item_snapshot = match self
            .crud_store
            .get_turn_item(outcome.turn_id.as_str(), outcome.item_id.as_str())
            .await
        {
            Ok(item) => item,
            Err(error) => {
                warn!(
                    job_id = outcome.job_id,
                    turn_id = outcome.turn_id,
                    item_id = outcome.item_id,
                    error = %format!("{error:#}"),
                    "failed to load item for terminal recovery outcome"
                );
                None
            }
        };

        let mut should_mark_turn_failed = item_snapshot.is_none();
        if let Some(item) = item_snapshot {
            if let Some(failed_item) =
                Self::force_fail_tool_item(item.clone(), outcome.error_message.as_str())
            {
                should_mark_turn_failed = true;
                let completed = pioneer_protocol::ItemCompletedNotification {
                    workspace_id: workspace_id.clone(),
                    thread_id: thread_id.clone(),
                    turn_id: outcome.turn_id.clone(),
                    item: failed_item,
                };
                match self
                    .crud_store
                    .materialize_item_completed(completed.clone(), event_timestamp)
                    .await
                {
                    Ok(()) => {
                        self.send_notification_to_thread_subscribers(
                            thread_id.as_str(),
                            events::ITEM_COMPLETED,
                            &completed,
                        )
                        .await;
                        self.notify_semantic_timeline_item_changed(
                            completed.workspace_id.as_str(),
                            completed.thread_id.as_str(),
                            completed.turn_id.as_str(),
                            &completed.item,
                            None,
                        )
                        .await;
                    }
                    Err(error) => {
                        warn!(
                            job_id = outcome.job_id,
                            turn_id = outcome.turn_id,
                            item_id = outcome.item_id,
                            error = %format!("{error:#}"),
                            "failed to persist synthetic item/completed for terminal recovery outcome"
                        );
                    }
                }
            } else if !item.is_tool_item() {
                should_mark_turn_failed = true;
            }
        }

        if should_mark_turn_failed {
            let status_label = match outcome.status {
                pioneer_protocol::RecoveryJobStatus::Exhausted => "exhausted",
                pioneer_protocol::RecoveryJobStatus::Failed => "failed",
                pioneer_protocol::RecoveryJobStatus::Blocked => "blocked",
                pioneer_protocol::RecoveryJobStatus::Pending
                | pioneer_protocol::RecoveryJobStatus::Active
                | pioneer_protocol::RecoveryJobStatus::Succeeded
                | pioneer_protocol::RecoveryJobStatus::Cancelled => "terminal",
            };
            let turn_error = format!(
                "recovery {status_label} for item `{}`: {}",
                outcome.item_id, outcome.error_message
            );
            self.mark_turn_failed_terminal(thread_id, outcome.turn_id, turn_error)
                .await;
        }
    }

    pub(super) async fn process_due_recovery_terminalizations(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<u64> {
        const CLAIM_LEASE_SECS: u64 = 45;
        let records = self
            .crud_store
            .claim_due_recovery_terminalizations(now_unix, CLAIM_LEASE_SECS, limit)
            .await?;
        let count = records.len() as u64;
        for record in records {
            let resume = async {
                if record.recovery_status != pioneer_protocol::RecoveryJobStatus::Blocked {
                    return Ok(None);
                }
                let display_reason = self
                    .recovery_blocked_display_reason(
                        record.turn_id.as_str(),
                        record.recovery_job_id.as_str(),
                        record.error_message.as_str(),
                    )
                    .await?;
                self.build_recovery_blocked_resume_metadata(
                    record.turn_id.as_str(),
                    record.recovery_job_id.as_str(),
                    display_reason.as_str(),
                )
                .await
                .map(Some)
            }
            .await;
            let applied = match resume {
                Ok(resume) => {
                    self.crud_store
                        .apply_claimed_recovery_terminalization(&record, resume, now_unix)
                        .await
                }
                Err(error) => Err(error),
            };
            match applied {
                Ok(applied) => {
                    if !applied.already_terminal {
                        if let Err(error) = self
                            .thread_manager
                            .commit_terminal_turn(applied.thread_id.as_str(), &applied.turn)
                            .await
                        {
                            warn!(
                                recovery_job_id = record.recovery_job_id,
                                turn_id = record.turn_id,
                                error = %format!("{error:#}"),
                                "authoritative recovery terminalization committed but in-memory projection could not be synchronized"
                            );
                        }
                    }
                    self.kick_native_turn_event_deliveries();
                }
                Err(error) => {
                    let exponent = record.attempt_count.min(8);
                    let delay = 1_i64.checked_shl(exponent).unwrap_or(256).min(300);
                    let retry_at = now_unix.saturating_add(delay);
                    if let Err(mark_error) = self
                        .crud_store
                        .fail_recovery_terminalization_claim(
                            &record,
                            format!("{error:#}"),
                            retry_at,
                            now_unix,
                        )
                        .await
                    {
                        warn!(
                            recovery_job_id = record.recovery_job_id,
                            turn_id = record.turn_id,
                            error = %format!("{error:#}"),
                            mark_error = %format!("{mark_error:#}"),
                            "failed recovery terminalization and could not persist retry state"
                        );
                    }
                }
            }
        }
        Ok(count)
    }

    /// Completes prepared provider outcomes and synchronizes any loaded
    /// in-memory Turn from the authoritative transaction. Bootstrap performs
    /// the same database repair before threads are loaded; this path covers a
    /// listener/agent task loss while the Gateway process keeps running.
    pub(super) async fn reconcile_prepared_native_turn_finalizations(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<u64> {
        let turn_ids = self
            .crud_store
            .list_prepared_turn_finalization_ids(limit)
            .await?;
        let mut reconciled = 0_u64;
        for turn_id in turn_ids {
            let committed = self
                .crud_store
                .commit_prepared_turn_finalization(turn_id.as_str(), now_unix)
                .await?;
            self.thread_manager
                .commit_terminal_turn(
                    committed.turn_completed.thread_id.as_str(),
                    &committed.turn_completed.turn,
                )
                .await
                .with_context(|| {
                    format!("failed to synchronize reconciled native Turn `{turn_id}` in memory")
                })?;
            reconciled = reconciled.saturating_add(1);
        }
        Ok(reconciled)
    }

    pub(super) fn handle_recovery_event<'a>(
        &'a self,
        event: crate::resilience::RecoveryCoordinatorEvent,
        event_timestamp: i64,
    ) -> MessageFuture<'a, bool> {
        message_future(async move {
            match event {
                crate::resilience::RecoveryCoordinatorEvent::RecoveryOpened {
                    job_id,
                    turn_id,
                    item_id,
                    item_type,
                    trigger,
                    action,
                    attempt_number,
                } => {
                    message_future(self.handle_recovery_opened_event(
                        job_id,
                        turn_id,
                        item_id,
                        item_type,
                        trigger,
                        action,
                        attempt_number,
                        event_timestamp,
                    ))
                    .await
                }
                crate::resilience::RecoveryCoordinatorEvent::RecoveryAttached {
                    job_id,
                    turn_id,
                    item_id,
                    item_type,
                    recovery_item_id,
                    recovery_item_type,
                    trigger,
                    action,
                    existing_status,
                    next_attempt_number,
                } => {
                    message_future(self.handle_recovery_attached_event(
                        job_id,
                        turn_id,
                        item_id,
                        item_type,
                        recovery_item_id,
                        recovery_item_type,
                        trigger,
                        action,
                        existing_status,
                        next_attempt_number,
                        event_timestamp,
                    ))
                    .await
                }
                crate::resilience::RecoveryCoordinatorEvent::RetryScheduled {
                    job_id,
                    turn_id,
                    item_id,
                    item_type,
                    attempt_number,
                    next_run_at_unix,
                    reason,
                } => {
                    message_future(self.handle_retry_scheduled_event(
                        job_id,
                        turn_id,
                        item_id,
                        item_type,
                        attempt_number,
                        next_run_at_unix,
                        reason,
                        event_timestamp,
                    ))
                    .await
                }
                crate::resilience::RecoveryCoordinatorEvent::RetryAttemptStarted {
                    job_id,
                    turn_id,
                    item_id,
                    item_type,
                    attempt_number,
                } => {
                    message_future(self.handle_retry_attempt_started_event(
                        job_id,
                        turn_id,
                        item_id,
                        item_type,
                        attempt_number,
                        event_timestamp,
                    ))
                    .await
                }
                crate::resilience::RecoveryCoordinatorEvent::CliRuntimeRetryAttemptRequested(
                    request,
                ) => {
                    message_future(
                        self.handle_cli_runtime_recovery_attempt_requested(
                            *request,
                            event_timestamp,
                        ),
                    )
                    .await;
                    true
                }
                crate::resilience::RecoveryCoordinatorEvent::RecoverySucceeded {
                    job_id,
                    turn_id,
                    item_id,
                    item_type,
                    attempt_number,
                } => {
                    message_future(self.handle_recovery_succeeded_event(
                        job_id,
                        turn_id,
                        item_id,
                        item_type,
                        attempt_number,
                        event_timestamp,
                    ))
                    .await
                }
                crate::resilience::RecoveryCoordinatorEvent::RecoveryBlocked {
                    job_id,
                    turn_id,
                    reason,
                } => {
                    message_future(self.handle_recovery_blocked_event(job_id, turn_id, reason))
                        .await
                }
                crate::resilience::RecoveryCoordinatorEvent::RecoveryExhausted(outcome) => {
                    message_future(self.handle_recovery_exhausted_event(outcome, event_timestamp))
                        .await
                }
            }
        })
    }

    async fn handle_cli_runtime_recovery_attempt_requested(
        &self,
        request: crate::resilience::CliRuntimeRecoveryAttemptRequest,
        event_timestamp: i64,
    ) {
        let started = (
            request.job_id.clone(),
            request.turn_id.clone(),
            request.item_id.clone(),
            request.item_type,
            request.attempt_number,
        );
        match self
            .start_cli_runtime_recovery_attempt(request.clone())
            .await
        {
            Ok(true) => {
                self.handle_retry_attempt_started_event(
                    started.0,
                    started.1,
                    started.2,
                    started.3,
                    started.4,
                    event_timestamp,
                )
                .await;
            }
            Ok(false) => {}
            Err(error) => {
                let failure = format!("failed to start CLI runtime recovery attempt: {error:#}");
                warn!(
                    turn_id = request.turn_id,
                    recovery_job_id = request.job_id,
                    recovery_attempt_id = request.recovery_attempt_id,
                    error = %format!("{error:#}"),
                    "CLI runtime recovery attempt start failed"
                );
                match self
                    .recovery_coordinator
                    .record_cli_runtime_attempt_failure(
                        request.job_id.as_str(),
                        request.recovery_attempt_id.as_str(),
                        failure,
                        event_timestamp,
                    )
                    .await
                {
                    Ok(events) => {
                        for event in events {
                            self.handle_recovery_event(event, event_timestamp).await;
                        }
                    }
                    Err(coordinator_error) => {
                        warn!(
                            turn_id = request.turn_id,
                            recovery_job_id = request.job_id,
                            recovery_attempt_id = request.recovery_attempt_id,
                            error = %format!("{coordinator_error:#}"),
                            "failed to record CLI runtime recovery start failure"
                        );
                    }
                }
            }
        }
    }

    async fn handle_recovery_opened_event(
        &self,
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: pioneer_protocol::TurnItemType,
        trigger: pioneer_protocol::RecoveryTrigger,
        action: pioneer_protocol::RecoveryAction,
        attempt_number: u32,
        event_timestamp: i64,
    ) -> bool {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let notification = pioneer_protocol::ItemRecoveryOpenedNotification {
            workspace_id,
            thread_id: thread_id.clone(),
            turn_id,
            item_id,
            item_type,
            recovery_job_id: job_id,
            trigger,
            action,
            attempt_number,
        };
        self.persist_and_send_item_recovery_opened(notification, event_timestamp)
            .await
    }

    async fn handle_recovery_attached_event(
        &self,
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: pioneer_protocol::TurnItemType,
        recovery_item_id: String,
        recovery_item_type: pioneer_protocol::TurnItemType,
        trigger: pioneer_protocol::RecoveryTrigger,
        action: pioneer_protocol::RecoveryAction,
        existing_status: pioneer_protocol::RecoveryJobStatus,
        next_attempt_number: u32,
        event_timestamp: i64,
    ) -> bool {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let notification = pioneer_protocol::ItemRecoveryAttachedNotification {
            workspace_id,
            thread_id: thread_id.clone(),
            turn_id,
            item_id,
            item_type,
            recovery_job_id: job_id,
            recovery_item_id,
            recovery_item_type,
            trigger,
            action,
            existing_status,
            next_attempt_number,
        };
        self.persist_and_send_item_recovery_attached(notification, event_timestamp)
            .await
    }

    async fn handle_retry_scheduled_event(
        &self,
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: pioneer_protocol::TurnItemType,
        attempt_number: u32,
        next_run_at_unix: i64,
        reason: Option<String>,
        event_timestamp: i64,
    ) -> bool {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let notification = pioneer_protocol::ItemRetryScheduledNotification {
            workspace_id,
            thread_id: thread_id.clone(),
            turn_id,
            item_id,
            item_type,
            recovery_job_id: job_id,
            attempt_number,
            next_run_at_unix,
            reason,
        };
        self.persist_and_send_item_retry_scheduled(notification, event_timestamp)
            .await
    }

    async fn handle_retry_attempt_started_event(
        &self,
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: pioneer_protocol::TurnItemType,
        attempt_number: u32,
        event_timestamp: i64,
    ) -> bool {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let notification = pioneer_protocol::ItemRetryAttemptStartedNotification {
            workspace_id,
            thread_id: thread_id.clone(),
            turn_id,
            item_id,
            item_type,
            recovery_job_id: job_id,
            attempt_number,
        };
        self.persist_and_send_item_retry_attempt_started(notification, event_timestamp)
            .await
    }

    async fn handle_recovery_succeeded_event(
        &self,
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: pioneer_protocol::TurnItemType,
        attempt_number: u32,
        event_timestamp: i64,
    ) -> bool {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let notification = pioneer_protocol::ItemRecoverySucceededNotification {
            workspace_id,
            thread_id: thread_id.clone(),
            turn_id,
            item_id,
            item_type,
            recovery_job_id: job_id,
            attempt_number,
        };
        self.persist_and_send_item_recovery_succeeded(notification, event_timestamp)
            .await
    }

    async fn handle_recovery_blocked_event(
        &self,
        job_id: String,
        turn_id: String,
        reason: String,
    ) -> bool {
        let Some((thread_id, _workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let display_reason = self
            .recovery_blocked_display_reason(turn_id.as_str(), job_id.as_str(), reason.as_str())
            .await;
        let display_reason = match display_reason {
            Ok(display_reason) => display_reason,
            Err(error) => {
                warn!(
                    recovery_job_id = %job_id,
                    turn_id = %turn_id,
                    error = %format!("{error:#}"),
                    "failed to resolve blocked recovery reason; durable outbox will retry"
                );
                return false;
            }
        };
        let resume = match self
            .build_recovery_blocked_resume_metadata(
                turn_id.as_str(),
                job_id.as_str(),
                display_reason.as_str(),
            )
            .await
        {
            Ok(resume) => resume,
            Err(error) => {
                warn!(
                    recovery_job_id = %job_id,
                    turn_id = %turn_id,
                    error = %format!("{error:#}"),
                    "failed to resolve blocked recovery resume metadata; durable outbox will retry"
                );
                return false;
            }
        };
        if !self
            .mark_turn_blocked_with_resume_metadata(
                thread_id,
                turn_id,
                format!("{display_reason} (recovery job {job_id})"),
                None,
                Some(resume),
            )
            .await
        {
            warn!(
                recovery_job_id = %job_id,
                error = %display_reason,
                "failed to mark turn blocked for blocked recovery job"
            );
            return false;
        }
        true
    }

    async fn recovery_blocked_display_reason(
        &self,
        turn_id: &str,
        recovery_job_id: &str,
        fallback_reason: &str,
    ) -> Result<String> {
        if !fallback_reason
            .to_ascii_lowercase()
            .contains("durable turn runtime snapshot is missing")
        {
            return Ok(fallback_reason.to_owned());
        }

        let is_cli_runtime_turn = self
            .crud_store
            .get_cli_runtime_turn_binding(turn_id)
            .await?
            .is_some();
        if !is_cli_runtime_turn {
            return Ok(fallback_reason.to_owned());
        }

        Ok(self
            .crud_store
            .get_recovery_job(recovery_job_id)
            .await?
            .and_then(|job| job.reason)
            .filter(|reason| !reason.trim().is_empty())
            .unwrap_or_else(|| fallback_reason.to_owned()))
    }

    async fn handle_recovery_exhausted_event(
        &self,
        outcome: RecoveryTerminalOutcome,
        event_timestamp: i64,
    ) -> bool {
        let committed =
            message_future(self.send_recovery_exhausted_notification(&outcome, event_timestamp))
                .await;
        message_future(self.handle_recovery_terminal_outcome(outcome, event_timestamp)).await;
        committed
    }

    pub(super) async fn send_recovery_exhausted_notification(
        &self,
        outcome: &RecoveryTerminalOutcome,
        event_timestamp: i64,
    ) -> bool {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(outcome.turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let notification = pioneer_protocol::ItemRecoveryExhaustedNotification {
            workspace_id,
            thread_id: thread_id.clone(),
            turn_id: outcome.turn_id.clone(),
            item_id: outcome.item_id.clone(),
            item_type: outcome.item_type,
            recovery_job_id: outcome.job_id.clone(),
            attempt_number: outcome.attempt_number,
            status: outcome.status,
            error_message: outcome.error_message.clone(),
        };
        self.persist_and_send_item_recovery_exhausted(notification, event_timestamp)
            .await
    }

    async fn persist_and_send_item_timeout_detected(
        &self,
        notification: pioneer_protocol::ItemTimeoutDetectedNotification,
        event_timestamp: i64,
    ) {
        if let Err(error) = self
            .crud_store
            .materialize_item_timeout_detected(notification.clone(), event_timestamp)
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item timeout recovery timeline event; skipping live notification"
            );
            return;
        }

        self.send_notification_to_thread_subscribers(
            notification.thread_id.as_str(),
            events::ITEM_TIMEOUT_DETECTED,
            &notification,
        )
        .await;
        self.notify_semantic_timeline_work_item_id_changed(
            notification.workspace_id.as_str(),
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
            notification.item_id.as_str(),
        )
        .await;
    }

    async fn persist_and_send_item_recovery_opened(
        &self,
        notification: pioneer_protocol::ItemRecoveryOpenedNotification,
        event_timestamp: i64,
    ) -> bool {
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::ItemRecoveryOpened(notification.clone()),
                event_timestamp,
                None,
            )
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item recovery opened timeline event"
            );
            return false;
        }
        true
    }

    async fn persist_and_send_item_recovery_attached(
        &self,
        notification: pioneer_protocol::ItemRecoveryAttachedNotification,
        event_timestamp: i64,
    ) -> bool {
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::ItemRecoveryAttached(notification.clone()),
                event_timestamp,
                None,
            )
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item recovery attached timeline event"
            );
            return false;
        }
        true
    }

    async fn persist_and_send_item_retry_scheduled(
        &self,
        notification: pioneer_protocol::ItemRetryScheduledNotification,
        event_timestamp: i64,
    ) -> bool {
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::ItemRetryScheduled(notification.clone()),
                event_timestamp,
                None,
            )
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item retry scheduled timeline event"
            );
            return false;
        }
        true
    }

    async fn persist_and_send_item_retry_attempt_started(
        &self,
        notification: pioneer_protocol::ItemRetryAttemptStartedNotification,
        event_timestamp: i64,
    ) -> bool {
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::ItemRetryAttemptStarted(
                    notification.clone(),
                ),
                event_timestamp,
                None,
            )
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item retry attempt started timeline event"
            );
            return false;
        }
        true
    }

    async fn persist_and_send_item_recovery_succeeded(
        &self,
        notification: pioneer_protocol::ItemRecoverySucceededNotification,
        event_timestamp: i64,
    ) -> bool {
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::ItemRecoverySucceeded(
                    notification.clone(),
                ),
                event_timestamp,
                None,
            )
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item recovery succeeded timeline event"
            );
            return false;
        }
        true
    }

    async fn persist_and_send_item_recovery_exhausted(
        &self,
        notification: pioneer_protocol::ItemRecoveryExhaustedNotification,
        event_timestamp: i64,
    ) -> bool {
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::ItemRecoveryExhausted(
                    notification.clone(),
                ),
                event_timestamp,
                None,
            )
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item recovery exhausted timeline event"
            );
            return false;
        }
        true
    }

    pub(super) fn item_markdown_buffer_key(thread_id: &str, item_id: &str) -> String {
        format!("{thread_id}:{item_id}")
    }

    pub(super) fn item_delta_event_method(stream: Option<ItemDeltaStream>) -> &'static str {
        match stream.unwrap_or(ItemDeltaStream::AgentMessage) {
            ItemDeltaStream::AgentMessage | ItemDeltaStream::Generic => {
                events::ITEM_AGENT_MESSAGE_DELTA
            }
            ItemDeltaStream::Stdout | ItemDeltaStream::Stderr => {
                events::ITEM_COMMAND_EXECUTION_OUTPUT_DELTA
            }
            ItemDeltaStream::FileChange => events::ITEM_FILE_CHANGE_OUTPUT_DELTA,
            ItemDeltaStream::ToolProgress => events::ITEM_TOOL_PROGRESS,
        }
    }

    pub(super) async fn enrich_item_started_markdown(
        &self,
        notification: &mut pioneer_protocol::ItemStartedNotification,
    ) {
        let (item_id, text) = Self::normalize_item_markdown(&mut notification.item);
        let key = Self::item_markdown_buffer_key(notification.thread_id.as_str(), item_id.as_str());
        self.agent_message_buffers.lock().await.insert(key, text);
    }

    pub(super) async fn enrich_item_delta_markdown(
        &self,
        notification: &mut pioneer_protocol::ItemDeltaNotification,
    ) {
        let key = Self::item_markdown_buffer_key(
            notification.thread_id.as_str(),
            notification.item_id.as_str(),
        );
        let full_text = {
            let mut buffers = self.agent_message_buffers.lock().await;
            let text = buffers.entry(key).or_default();
            text.push_str(notification.delta.as_str());
            text.clone()
        };

        notification.markdown = Some(markdown::parse_markdown_document(full_text.as_str()));
        notification.markdown_version = Some(MARKDOWN_AST_VERSION);
    }

    pub(super) async fn enrich_item_completed_markdown(
        &self,
        notification: &mut pioneer_protocol::ItemCompletedNotification,
    ) {
        let (item_id, _) = Self::normalize_item_markdown(&mut notification.item);
        let key = Self::item_markdown_buffer_key(notification.thread_id.as_str(), item_id.as_str());
        self.agent_message_buffers.lock().await.remove(key.as_str());
    }

    pub(super) fn notify_parent_timeline_changed_for_child_turn(
        &self,
        child_thread_id: &str,
        child_turn_id: &str,
        workspace_id: Option<&str>,
    ) -> MessageFuture<'static, ()> {
        let processor = self.clone();
        let child_thread_id = child_thread_id.to_owned();
        let child_turn_id = child_turn_id.to_owned();
        let workspace_id = workspace_id.map(str::to_owned);
        message_future(async move {
            let handle = tokio::spawn(async move {
                let Some(target) = processor
                    .parent_timeline_target_for_child_turn(
                        child_thread_id.as_str(),
                        child_turn_id.as_str(),
                        workspace_id.as_deref(),
                    )
                    .await
                else {
                    return;
                };

                processor
                    .notify_semantic_timeline_turn_state_changed(
                        target.workspace_id.as_str(),
                        target.parent_thread_id.as_str(),
                        target.parent_turn_id.as_str(),
                    )
                    .await;
            });
            if let Err(error) = handle.await {
                warn!(
                    error = %error,
                    "parent timeline changed notification task failed"
                );
            }
        })
    }

    async fn forward_child_reasoning_delta_to_parent_turn(
        &self,
        notification: &pioneer_protocol::ItemDeltaNotification,
    ) {
        let Some(target) = self
            .parent_timeline_target_for_child_turn(
                notification.thread_id.as_str(),
                notification.turn_id.as_str(),
                Some(notification.workspace_id.as_str()),
            )
            .await
        else {
            return;
        };

        let mut parent_delta = notification.clone();
        parent_delta.workspace_id = target.workspace_id;
        parent_delta.thread_id = target.parent_thread_id;
        parent_delta.turn_id = target.parent_turn_id;

        let delta_method = Self::item_delta_event_method(parent_delta.stream);
        self.send_notification_to_thread_subscribers(
            parent_delta.thread_id.as_str(),
            delta_method,
            &parent_delta,
        )
        .await;
    }

    async fn rollback_terminal_turn_finish_if_event_not_appended(
        &self,
        rollback_context: crate::thread::TurnFinishRollbackContext,
        error: &anyhow::Error,
        thread_id: &str,
        turn_id: &str,
        event_name: &'static str,
    ) -> bool {
        if pioneer_crud::turn_event_was_appended_before_error(error) {
            warn!(
                thread_id,
                turn_id,
                event = event_name,
                error = %format!("{error:#}"),
                "terminal turn event was appended but projection failed; preserving in-memory terminal state"
            );
            return false;
        }

        self.thread_manager
            .rollback_turn_finish(rollback_context)
            .await;
        true
    }

    pub(super) async fn complete_turn(
        &self,
        thread_id: String,
        turn_id: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) -> bool {
        if let Some((workspace_id, current_turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        {
            if current_turn.status == TurnStatus::Completed {
                let notification = TurnCompletedNotification {
                    workspace_id,
                    thread_id: thread_id.clone(),
                    turn: current_turn,
                };
                return self
                    .materialize_native_agent_turn_event(
                        pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(notification),
                        now_timestamp_secs(),
                        None,
                    )
                    .await
                    .map(|()| true)
                    .unwrap_or_else(|error| {
                        warn!(
                            thread_id,
                            turn_id,
                            error = %format!("{error:#}"),
                            "failed to confirm idempotent completed Turn commit"
                        );
                        false
                    });
            }
            if current_turn.status != TurnStatus::InProgress {
                return false;
            }
        }

        if let Some(recovery) = recovery.as_ref() {
            match self
                .recovery_coordinator
                .is_active_recovery_attempt(turn_id.as_str(), recovery)
                .await
            {
                Ok(true) => {}
                Ok(false) => return false,
                Err(error) => {
                    warn!(
                        thread_id,
                        turn_id,
                        recovery_job_id = %recovery.job_id,
                        recovery_attempt_id = %recovery.attempt_id,
                        error = %format!("{error:#}"),
                        "failed to verify recovery completion context"
                    );
                    return false;
                }
            }
        }

        if self
            .artifact_finalization_blocks_completion(thread_id.as_str(), turn_id.as_str())
            .await
        {
            return false;
        }

        let finish_outcome = match self
            .thread_manager
            .turn_finish(
                thread_id.as_str(),
                turn_id.as_str(),
                TurnStatus::Completed,
                None,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to mark turn as completed"
                );
                return false;
            }
        };

        let turn_completed = TurnCompletedNotification {
            workspace_id: finish_outcome.workspace_id.clone(),
            thread_id: finish_outcome.thread_id.clone(),
            turn: finish_outcome.turn.clone(),
        };

        let event_timestamp = now_timestamp_secs();
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(turn_completed.clone()),
                event_timestamp,
                None,
            )
            .await
        {
            let rolled_back = self
                .rollback_terminal_turn_finish_if_event_not_appended(
                    finish_outcome.rollback_context,
                    &error,
                    thread_id.as_str(),
                    turn_id.as_str(),
                    "turn/completed",
                )
                .await;
            if rolled_back {
                self.report_legacy_turn_failure(
                    thread_id,
                    turn_id,
                    format!("failed to persist turn/completed: {error:#}"),
                )
                .await;
                return false;
            }
        }

        if let Err(error) = self
            .close_latest_active_execution_window_for_terminal_turn(
                turn_id.as_str(),
                pioneer_protocol::ExecutionWindowStatus::Completed,
                None,
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to close execution window after turn completion"
            );
        }

        if let Err(error) = self
            .task_agent_executor
            .reconcile_child_turn_completed(thread_id.as_str(), turn_id.as_str())
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to reconcile completed child task turn"
            );
        }

        if let Err(error) = self
            .crud_store
            .delete_turn_llm_context_for_turn(turn_id.as_str())
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to delete turn_llm_context rows after turn completion"
            );
        }
        self.delete_turn_runtime_snapshot_for_closed_turn(
            thread_id.as_str(),
            turn_id.as_str(),
            "completed",
        )
        .await;
        self.clear_artifact_finalization_state(turn_id.as_str())
            .await;

        match self
            .recovery_coordinator
            .complete_active_recovery_for_turn(turn_id.as_str(), recovery.as_ref(), event_timestamp)
            .await
        {
            Ok(events) => {
                for event in events {
                    self.handle_recovery_event(event, event_timestamp).await;
                }
            }
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to mark active recovery jobs succeeded"
                );
            }
        }

        if self
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id.as_str())
            .await
        {
            self.agent_manager
                .retire_thread_after_terminal_commit(thread_id.as_str())
                .await;
        }
        true
    }

    /// Native provider success is acknowledged only after the previously
    /// prepared final response, final AgentMessage and Completed Turn commit in
    /// one authoritative transaction. Legacy execution-free callers retain
    /// `complete_turn`, which has no provider final response to bind.
    pub(super) async fn complete_native_turn(
        &self,
        thread_id: String,
        turn_id: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) -> bool {
        if let Some(recovery) = recovery.as_ref() {
            match self
                .recovery_coordinator
                .is_active_recovery_attempt(turn_id.as_str(), recovery)
                .await
            {
                Ok(true) => {}
                Ok(false) => return false,
                Err(error) => {
                    warn!(
                        thread_id,
                        turn_id,
                        recovery_job_id = %recovery.job_id,
                        recovery_attempt_id = %recovery.attempt_id,
                        error = %format!("{error:#}"),
                        "failed to verify native finalization recovery context"
                    );
                    return false;
                }
            }
        }

        // TurnCompleted is shared by native API providers, CLI runtimes and
        // older native workers. Only the new provider protocol emits the
        // preceding durable finalization intent. Preserve the existing
        // terminal lifecycle for intent-less producers during the rolling
        // expand/migrate/contract window; a prepared native response always
        // takes the atomic path below.
        match self
            .crud_store
            .has_turn_finalization_intent(turn_id.as_str())
            .await
        {
            Ok(true) => {}
            Ok(false) => return self.complete_turn(thread_id, turn_id, recovery).await,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to resolve native Turn finalization protocol"
                );
                return false;
            }
        }

        let committed = match self
            .crud_store
            .commit_prepared_turn_finalization(turn_id.as_str(), now_timestamp_secs())
            .await
        {
            Ok(committed) => committed,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to atomically commit native Turn finalization"
                );
                return false;
            }
        };
        if committed.turn_completed.thread_id != thread_id {
            error!(
                requested_thread_id = thread_id,
                committed_thread_id = committed.turn_completed.thread_id,
                turn_id,
                "native finalization committed for a different thread"
            );
            return false;
        }
        self.record_final_assistant_text_for_item(&committed.final_item)
            .await;
        self.kick_native_turn_event_deliveries();

        // Reuse terminal cleanup/recovery/task reconciliation. Its canonical
        // TurnCompleted write is now an exact idempotent replay of the already
        // committed terminal half of the finalization transaction.
        self.complete_turn(thread_id, turn_id, recovery).await
    }

    pub(super) async fn mark_turn_blocked(
        &self,
        thread_id: String,
        turn_id: String,
        reason: String,
    ) -> bool {
        self.mark_turn_blocked_with_resume_metadata(thread_id, turn_id, reason, None, None)
            .await
    }

    async fn build_recovery_blocked_resume_metadata(
        &self,
        turn_id: &str,
        recovery_job_id: &str,
        reason: &str,
    ) -> Result<pioneer_protocol::TurnBlockedResumeMetadata> {
        let latest_checkpoint_id = self
            .crud_store
            .latest_turn_execution_checkpoint_for_turn(turn_id)
            .await?
            .map(|checkpoint| checkpoint.id);
        let recovery_job = self.crud_store.get_recovery_job(recovery_job_id).await?;
        let has_runtime_snapshot = self
            .crud_store
            .get_turn_runtime_snapshot(turn_id)
            .await?
            .is_some();
        let can_resume_same_turn = recovery_job
            .as_ref()
            .is_some_and(|job| !job.item_type.is_tool_item() || has_runtime_snapshot);

        let normalized = reason.to_ascii_lowercase();
        let reason_class = if normalized.contains("auth")
            || normalized.contains("permission")
            || normalized.contains("credential")
            || normalized.contains("api key")
        {
            "auth_or_config"
        } else if normalized.contains("model") {
            "model_unavailable"
        } else if normalized.contains("capability") || normalized.contains("unsupported") {
            "unsupported_capability"
        } else {
            "recovery_blocked"
        };

        let mut resume_requirements = Vec::new();
        match reason_class {
            "auth_or_config" => {
                resume_requirements.push(
                    "Fix provider authentication/configuration for the selected model/provider."
                        .to_owned(),
                );
            }
            "model_unavailable" => {
                resume_requirements.push(
                    "Configure an available model or an explicit fallback policy.".to_owned(),
                );
            }
            "unsupported_capability" => {
                resume_requirements.push(
                    "Disable the unsupported capability or configure a provider/model that supports it."
                        .to_owned(),
                );
            }
            _ => {
                resume_requirements.push(
                    "Resolve the blocked recovery condition reported in the reason.".to_owned(),
                );
            }
        }
        resume_requirements
            .push("Resume the same turn after the requirement is satisfied.".to_owned());
        if !can_resume_same_turn {
            if recovery_job
                .as_ref()
                .is_some_and(|job| job.item_type.is_tool_item())
            {
                resume_requirements.push(
                    "This blocked tool recovery has no durable turn runtime snapshot; operator recovery is required."
                        .to_owned(),
                );
            } else {
                resume_requirements.push(
                    "This blocked recovery cannot be resumed automatically; operator recovery is required."
                        .to_owned(),
                );
            }
        }

        Ok(pioneer_protocol::TurnBlockedResumeMetadata {
            reason_class: reason_class.to_owned(),
            human_message: reason.to_owned(),
            resume_requirements,
            resume_command: format!("turn.resume:{turn_id}"),
            blocked_recovery_job_id: Some(recovery_job_id.to_owned()),
            latest_checkpoint_id,
            can_resume_same_turn,
        })
    }

    pub(super) async fn mark_turn_blocked_with_recovery(
        &self,
        thread_id: String,
        turn_id: String,
        reason: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) -> bool {
        self.mark_turn_blocked_with_resume_metadata(thread_id, turn_id, reason, recovery, None)
            .await
    }

    pub(super) async fn mark_turn_blocked_with_resume_metadata(
        &self,
        thread_id: String,
        turn_id: String,
        reason: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
        resume: Option<pioneer_protocol::TurnBlockedResumeMetadata>,
    ) -> bool {
        let turn_loaded_in_memory = if let Some((_workspace_id, current_turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        {
            if current_turn.status == TurnStatus::Blocked {
                let block_reason = current_turn
                    .error
                    .as_deref()
                    .unwrap_or_else(|| reason.as_str());
                let notification = TurnBlockedNotification {
                    workspace_id: _workspace_id,
                    thread_id: thread_id.clone(),
                    turn: current_turn.clone(),
                    resume: resume.clone(),
                };
                if let Err(error) = self
                    .materialize_native_agent_turn_event(
                        pioneer_crud::CanonicalTurnEventPayload::TurnBlocked(notification),
                        now_timestamp_secs(),
                        None,
                    )
                    .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to confirm idempotent blocked Turn commit"
                    );
                    return false;
                }
                self.ensure_blocked_turn_terminal_cleanup(
                    thread_id.as_str(),
                    turn_id.as_str(),
                    Some(block_reason),
                )
                .await;
                if let Err(error) = self
                    .recovery_coordinator
                    .block_active_recoveries_for_turn(
                        turn_id.as_str(),
                        recovery.as_ref(),
                        block_reason,
                        now_timestamp_secs(),
                    )
                    .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to mark active recovery jobs blocked for already blocked turn"
                    );
                }
                return true;
            }
            if current_turn.status != TurnStatus::InProgress {
                return false;
            }
            true
        } else {
            false
        };

        if let Some(recovery) = recovery.as_ref() {
            match self
                .recovery_coordinator
                .is_active_recovery_attempt(turn_id.as_str(), recovery)
                .await
            {
                Ok(true) => {}
                Ok(false) => return false,
                Err(error) => {
                    warn!(
                        thread_id,
                        turn_id,
                        recovery_job_id = %recovery.job_id,
                        recovery_attempt_id = %recovery.attempt_id,
                        error = %format!("{error:#}"),
                        "failed to verify recovery block context"
                    );
                    return false;
                }
            }
        }

        if !turn_loaded_in_memory {
            return self
                .mark_unloaded_turn_blocked_with_resume_metadata(
                    thread_id,
                    turn_id,
                    reason,
                    recovery.as_ref(),
                    resume,
                )
                .await;
        }

        let finish_outcome = match self
            .thread_manager
            .turn_finish(
                thread_id.as_str(),
                turn_id.as_str(),
                TurnStatus::Blocked,
                Some(reason.clone()),
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to mark turn as blocked"
                );
                return false;
            }
        };

        let turn_blocked = TurnBlockedNotification {
            workspace_id: finish_outcome.workspace_id.clone(),
            thread_id: finish_outcome.thread_id.clone(),
            turn: finish_outcome.turn.clone(),
            resume,
        };

        let event_timestamp = now_timestamp_secs();
        let materialize_blocked_result = {
            let processor = self.clone();
            let turn_blocked = turn_blocked.clone();
            message_fresh_task(async move {
                processor
                    .materialize_native_agent_turn_event(
                        pioneer_crud::CanonicalTurnEventPayload::TurnBlocked(turn_blocked),
                        event_timestamp,
                        None,
                    )
                    .await
            })
            .await
        };
        let materialize_blocked_result = match materialize_blocked_result {
            Ok(result) => result,
            Err(error) => Err(anyhow::anyhow!(
                "turn/blocked materialization task failed: {error}"
            )),
        };
        if let Err(error) = materialize_blocked_result {
            let rolled_back = self
                .rollback_terminal_turn_finish_if_event_not_appended(
                    finish_outcome.rollback_context,
                    &error,
                    thread_id.as_str(),
                    turn_id.as_str(),
                    "turn/blocked",
                )
                .await;
            if rolled_back {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to persist turn/blocked event"
                );
                return false;
            }
        }

        self.ensure_blocked_turn_terminal_cleanup(
            thread_id.as_str(),
            turn_id.as_str(),
            turn_blocked.turn.error.as_deref(),
        )
        .await;

        if let Err(error) = self
            .task_agent_executor
            .reconcile_child_turn_blocked(
                thread_id.as_str(),
                turn_id.as_str(),
                turn_blocked.turn.error.as_deref().unwrap_or("turn blocked"),
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to reconcile blocked child task turn"
            );
        }

        if let Err(error) = self
            .recovery_coordinator
            .block_active_recoveries_for_turn(
                turn_id.as_str(),
                recovery.as_ref(),
                turn_blocked.turn.error.as_deref().unwrap_or("turn blocked"),
                event_timestamp,
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to mark active recovery jobs blocked"
            );
        }

        if self
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id.as_str())
            .await
        {
            self.agent_manager
                .retire_thread_after_terminal_commit(thread_id.as_str())
                .await;
        }

        true
    }

    async fn mark_unloaded_turn_blocked_with_resume_metadata(
        &self,
        thread_id: String,
        turn_id: String,
        reason: String,
        recovery: Option<&pioneer_protocol::RecoveryAttemptContext>,
        resume: Option<pioneer_protocol::TurnBlockedResumeMetadata>,
    ) -> bool {
        let (workspace_id, current_turn) = match self
            .crud_store
            .get_turn(thread_id.as_str(), turn_id.as_str())
            .await
        {
            Ok(Some(turn_state)) => turn_state,
            Ok(None) => {
                warn!(
                    thread_id,
                    turn_id, "turn missing before marking unloaded turn as blocked"
                );
                return false;
            }
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to load unloaded turn before marking it as blocked"
                );
                return false;
            }
        };

        if current_turn.status == TurnStatus::Blocked {
            let block_reason = current_turn
                .error
                .as_deref()
                .unwrap_or_else(|| reason.as_str());
            self.ensure_blocked_turn_terminal_cleanup(
                thread_id.as_str(),
                turn_id.as_str(),
                Some(block_reason),
            )
            .await;
            if let Err(error) = self
                .recovery_coordinator
                .block_active_recoveries_for_turn(
                    turn_id.as_str(),
                    recovery,
                    block_reason,
                    now_timestamp_secs(),
                )
                .await
            {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to mark active recovery jobs blocked for already blocked unloaded turn"
                );
            }
            return true;
        }
        if current_turn.status != TurnStatus::InProgress {
            return false;
        }

        let turn_blocked = TurnBlockedNotification {
            workspace_id: workspace_id.clone(),
            thread_id: thread_id.clone(),
            turn: Turn {
                status: TurnStatus::Blocked,
                error: Some(reason.clone()),
                ..current_turn
            },
            resume,
        };
        let event_timestamp = now_timestamp_secs();
        let materialize_blocked_result = {
            let processor = self.clone();
            let turn_blocked = turn_blocked.clone();
            message_fresh_task(async move {
                processor
                    .materialize_native_agent_turn_event(
                        pioneer_crud::CanonicalTurnEventPayload::TurnBlocked(turn_blocked),
                        event_timestamp,
                        None,
                    )
                    .await
            })
            .await
        };
        let materialize_blocked_result = match materialize_blocked_result {
            Ok(result) => result,
            Err(error) => Err(anyhow::anyhow!(
                "unloaded turn/blocked materialization task failed: {error}"
            )),
        };
        if let Err(error) = materialize_blocked_result {
            if pioneer_crud::turn_event_was_appended_before_error(&error) {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "unloaded turn/blocked event was appended but projection failed; continuing terminal cleanup"
                );
            } else {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to persist unloaded turn/blocked event"
                );
                return false;
            }
        }

        self.ensure_blocked_turn_terminal_cleanup(
            thread_id.as_str(),
            turn_id.as_str(),
            turn_blocked.turn.error.as_deref(),
        )
        .await;

        if let Err(error) = self
            .task_agent_executor
            .reconcile_child_turn_blocked(
                thread_id.as_str(),
                turn_id.as_str(),
                turn_blocked.turn.error.as_deref().unwrap_or("turn blocked"),
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to reconcile blocked child task turn for unloaded turn"
            );
        }

        if let Err(error) = self
            .recovery_coordinator
            .block_active_recoveries_for_turn(
                turn_id.as_str(),
                recovery,
                turn_blocked.turn.error.as_deref().unwrap_or("turn blocked"),
                event_timestamp,
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to mark active recovery jobs blocked for unloaded turn"
            );
        }

        self.send_notification_to_thread_subscribers(
            thread_id.as_str(),
            events::TURN_BLOCKED,
            &turn_blocked,
        )
        .await;
        self.notify_semantic_timeline_turn_state_changed(
            turn_blocked.workspace_id.as_str(),
            turn_blocked.thread_id.as_str(),
            turn_blocked.turn.id.as_str(),
        )
        .await;
        self.notify_parent_timeline_changed_for_child_turn(
            turn_blocked.thread_id.as_str(),
            turn_blocked.turn.id.as_str(),
            Some(turn_blocked.workspace_id.as_str()),
        )
        .await;

        true
    }

    async fn ensure_blocked_turn_terminal_cleanup(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: Option<&str>,
    ) {
        if let Err(error) = self
            .close_latest_active_execution_window_for_terminal_turn(
                turn_id,
                pioneer_protocol::ExecutionWindowStatus::Blocked,
                reason,
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to close execution window after turn block"
            );
        }

        // A blocked recovery is resumable by contract.  Provider rounds and
        // tool results are the resume capsule; deleting them here makes the
        // advertised `turn.resume:<id>` path reconstruct an empty transcript.
        // Terminal completion/failure cleanup still removes the context, while
        // blocked data is reclaimed only by an explicit abandon/retention path.
        self.clear_artifact_finalization_state(turn_id).await;
        self.ensure_cli_runtime_turn_blocked_cleanup(thread_id, turn_id, reason)
            .await;
    }

    pub(super) async fn report_legacy_turn_failure(
        &self,
        thread_id: String,
        turn_id: String,
        error_message: String,
    ) {
        if !self
            .report_turn_failure(
                thread_id.clone(),
                turn_id.clone(),
                classify_legacy_turn_failure(error_message.as_str()),
                error_message.clone(),
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %error_message,
                "recoverable turn failure could not be reported"
            );
        }
    }

    pub(super) async fn mark_turn_failed_terminal(
        &self,
        thread_id: String,
        turn_id: String,
        error_message: String,
    ) -> bool {
        self.mark_turn_failed_with_recovery(thread_id, turn_id, error_message, None)
            .await
    }

    pub(super) async fn mark_turn_interrupted(
        &self,
        thread_id: String,
        turn_id: String,
        reason: String,
    ) -> bool {
        self.mark_turn_interrupted_with_recovery_disposition(thread_id, turn_id, reason, None, true)
            .await
    }

    pub(super) async fn mark_turn_interrupted_with_recovery(
        &self,
        thread_id: String,
        turn_id: String,
        reason: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) -> bool {
        let user_cancellation_reason = self
            .user_turn_cancel_intents
            .lock()
            .await
            .get(&(thread_id.clone(), turn_id.clone()))
            .cloned();
        if let Some(user_cancellation_reason) = user_cancellation_reason {
            return self
                .mark_turn_interrupted_with_recovery_disposition(
                    thread_id,
                    turn_id,
                    user_cancellation_reason,
                    recovery,
                    true,
                )
                .await;
        }
        self.mark_turn_interrupted_with_recovery_disposition(
            thread_id, turn_id, reason, recovery, false,
        )
        .await
    }

    async fn mark_turn_interrupted_with_recovery_disposition(
        &self,
        thread_id: String,
        turn_id: String,
        reason: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
        user_cancellation: bool,
    ) -> bool {
        if let Some((workspace_id, current_turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        {
            if current_turn.status == TurnStatus::Interrupted {
                let notification = TurnFailedNotification {
                    workspace_id,
                    thread_id: thread_id.clone(),
                    turn: current_turn,
                };
                if let Err(error) = self
                    .materialize_native_agent_turn_event(
                        pioneer_crud::CanonicalTurnEventPayload::TurnFailed(notification),
                        now_timestamp_secs(),
                        None,
                    )
                    .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to confirm idempotent interrupted Turn commit"
                    );
                    return false;
                }
                if user_cancellation
                    && let Err(error) = self
                        .task_agent_executor
                        .reconcile_child_turn_cancelled(
                            thread_id.as_str(),
                            turn_id.as_str(),
                            reason.as_str(),
                        )
                        .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to reconcile already-interrupted child task turn as cancelled"
                    );
                }
                return true;
            }
            if current_turn.status != TurnStatus::InProgress {
                return false;
            }
        }

        if let Some(recovery) = recovery.as_ref() {
            match self
                .recovery_coordinator
                .is_active_recovery_attempt(turn_id.as_str(), recovery)
                .await
            {
                Ok(true) => {}
                Ok(false) => return false,
                Err(error) => {
                    warn!(
                        thread_id,
                        turn_id,
                        recovery_job_id = %recovery.job_id,
                        recovery_attempt_id = %recovery.attempt_id,
                        error = %format!("{error:#}"),
                        "failed to verify recovery interruption context"
                    );
                    return false;
                }
            }
        }

        let finish_outcome = match self
            .thread_manager
            .turn_finish(
                thread_id.as_str(),
                turn_id.as_str(),
                TurnStatus::Interrupted,
                Some(reason.clone()),
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Some((_workspace_id, current_turn)) = self
                    .thread_manager
                    .turn_get(thread_id.as_str(), turn_id.as_str())
                    .await
                    && current_turn.status == TurnStatus::Interrupted
                {
                    return true;
                }
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to mark turn as interrupted"
                );
                return false;
            }
        };

        let turn_failed = TurnFailedNotification {
            workspace_id: finish_outcome.workspace_id.clone(),
            thread_id: finish_outcome.thread_id,
            turn: finish_outcome.turn,
        };

        let event_timestamp = now_timestamp_secs();
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::TurnFailed(turn_failed.clone()),
                event_timestamp,
                None,
            )
            .await
        {
            let rolled_back = self
                .rollback_terminal_turn_finish_if_event_not_appended(
                    finish_outcome.rollback_context,
                    &error,
                    thread_id.as_str(),
                    turn_id.as_str(),
                    "turn/interrupted",
                )
                .await;
            if rolled_back {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to persist turn/interrupted event"
                );
                return false;
            }
        }

        if let Err(error) = self
            .close_latest_active_execution_window_for_terminal_turn(
                turn_id.as_str(),
                pioneer_protocol::ExecutionWindowStatus::Interrupted,
                Some(reason.as_str()),
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to close execution window after turn interruption"
            );
        }

        let reconciliation = if user_cancellation {
            self.task_agent_executor
                .reconcile_child_turn_cancelled(
                    thread_id.as_str(),
                    turn_id.as_str(),
                    turn_failed
                        .turn
                        .error
                        .as_deref()
                        .unwrap_or("turn cancelled by user"),
                )
                .await
        } else {
            self.task_agent_executor
                .reconcile_child_turn_failed(
                    thread_id.as_str(),
                    turn_id.as_str(),
                    turn_failed
                        .turn
                        .error
                        .as_deref()
                        .unwrap_or("turn interrupted"),
                )
                .await
        };
        if let Err(error) = reconciliation {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                user_cancellation,
                "failed to reconcile interrupted child task turn"
            );
        }

        if let Err(error) = self
            .crud_store
            .delete_turn_llm_context_for_turn(turn_id.as_str())
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to delete turn_llm_context rows after turn interruption"
            );
        }
        self.delete_turn_runtime_snapshot_for_closed_turn(
            thread_id.as_str(),
            turn_id.as_str(),
            "interrupted",
        )
        .await;
        self.clear_artifact_finalization_state(turn_id.as_str())
            .await;

        match self
            .recovery_coordinator
            .fail_active_recoveries_for_turn(
                turn_id.as_str(),
                recovery.as_ref(),
                turn_failed
                    .turn
                    .error
                    .as_deref()
                    .unwrap_or("turn interrupted"),
                event_timestamp,
            )
            .await
        {
            Ok(events) => {
                for event in events {
                    if let crate::resilience::RecoveryCoordinatorEvent::RecoveryExhausted(outcome) =
                        event
                    {
                        self.send_recovery_exhausted_notification(&outcome, event_timestamp)
                            .await;
                    }
                }
            }
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to mark active recovery jobs interrupted"
                );
            }
        }

        if self
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id.as_str())
            .await
        {
            self.agent_manager
                .retire_thread_after_terminal_commit(thread_id.as_str())
                .await;
        }
        true
    }

    pub(super) async fn mark_turn_failed_with_recovery(
        &self,
        thread_id: String,
        turn_id: String,
        error_message: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) -> bool {
        if let Some((workspace_id, current_turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        {
            if current_turn.status == TurnStatus::Failed {
                let notification = TurnFailedNotification {
                    workspace_id,
                    thread_id: thread_id.clone(),
                    turn: current_turn,
                };
                return self
                    .materialize_native_agent_turn_event(
                        pioneer_crud::CanonicalTurnEventPayload::TurnFailed(notification),
                        now_timestamp_secs(),
                        None,
                    )
                    .await
                    .map(|()| true)
                    .unwrap_or_else(|error| {
                        warn!(
                            thread_id,
                            turn_id,
                            error = %format!("{error:#}"),
                            "failed to confirm idempotent failed Turn commit"
                        );
                        false
                    });
            }
            if current_turn.status != TurnStatus::InProgress {
                return false;
            }
        }

        if let Some(recovery) = recovery.as_ref() {
            match self
                .recovery_coordinator
                .is_active_recovery_attempt(turn_id.as_str(), recovery)
                .await
            {
                Ok(true) => {}
                Ok(false) => return false,
                Err(error) => {
                    warn!(
                        thread_id,
                        turn_id,
                        recovery_job_id = %recovery.job_id,
                        recovery_attempt_id = %recovery.attempt_id,
                        error = %format!("{error:#}"),
                        "failed to verify recovery failure context"
                    );
                    return false;
                }
            }
        }

        let finish_outcome = match self
            .thread_manager
            .turn_finish(
                thread_id.as_str(),
                turn_id.as_str(),
                TurnStatus::Failed,
                Some(error_message),
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to mark turn as failed"
                );
                return false;
            }
        };

        let turn_failed = TurnFailedNotification {
            workspace_id: finish_outcome.workspace_id.clone(),
            thread_id: finish_outcome.thread_id,
            turn: finish_outcome.turn,
        };

        let event_timestamp = now_timestamp_secs();
        if let Err(error) = self
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::TurnFailed(turn_failed.clone()),
                event_timestamp,
                None,
            )
            .await
        {
            let rolled_back = self
                .rollback_terminal_turn_finish_if_event_not_appended(
                    finish_outcome.rollback_context,
                    &error,
                    thread_id.as_str(),
                    turn_id.as_str(),
                    "turn/failed",
                )
                .await;
            if rolled_back {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to persist turn/failed event"
                );
                return false;
            }
        }

        if let Err(error) = self
            .close_latest_active_execution_window_for_terminal_turn(
                turn_id.as_str(),
                pioneer_protocol::ExecutionWindowStatus::Failed,
                turn_failed.turn.error.as_deref(),
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to close execution window after turn failure"
            );
        }

        if let Err(error) = self
            .task_agent_executor
            .reconcile_child_turn_failed(
                thread_id.as_str(),
                turn_id.as_str(),
                turn_failed.turn.error.as_deref().unwrap_or("turn failed"),
            )
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to reconcile failed child task turn"
            );
        }

        if let Err(error) = self
            .crud_store
            .delete_turn_llm_context_for_turn(turn_id.as_str())
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to delete turn_llm_context rows after turn failure"
            );
        }
        self.delete_turn_runtime_snapshot_for_closed_turn(
            thread_id.as_str(),
            turn_id.as_str(),
            "failed",
        )
        .await;
        self.clear_artifact_finalization_state(turn_id.as_str())
            .await;

        match self
            .recovery_coordinator
            .fail_active_recoveries_for_turn(
                turn_id.as_str(),
                recovery.as_ref(),
                turn_failed.turn.error.as_deref().unwrap_or("turn failed"),
                event_timestamp,
            )
            .await
        {
            Ok(events) => {
                for event in events {
                    if let crate::resilience::RecoveryCoordinatorEvent::RecoveryExhausted(outcome) =
                        event
                    {
                        self.send_recovery_exhausted_notification(&outcome, event_timestamp)
                            .await;
                    }
                }
            }
            Err(error) => {
                warn!(
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to mark active recovery jobs failed"
                );
            }
        }

        if self
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id.as_str())
            .await
        {
            self.agent_manager
                .retire_thread_after_terminal_commit(thread_id.as_str())
                .await;
        }
        true
    }

    pub(super) async fn emit_user_message_item_lifecycle(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        input: &[pioneer_protocol::UserInput],
        capability_attachments: &[pioneer_protocol::UserMessageAttachment],
    ) -> Result<()> {
        let item_id = user_message_item_id(turn_id);
        let payload = self
            .user_message_payload_from_input_resolved(
                workspace_id,
                thread_id,
                turn_id,
                &item_id,
                input,
            )
            .await
            .context("failed to materialize artifact-aware user message payload")?;
        let (text, mut attachments) = payload.unwrap_or_default();
        attachments.extend_from_slice(capability_attachments);

        if text.is_empty() && attachments.is_empty() {
            return Ok(());
        }

        let item = TurnItem::UserMessage {
            id: item_id,
            text,
            attachments,
        };

        let started = pioneer_protocol::ItemStartedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item: item.clone(),
        };

        self.materialize_native_agent_turn_event(
            pioneer_crud::CanonicalTurnEventPayload::ItemStarted(started),
            now_timestamp_secs(),
            None,
        )
        .await
        .context("failed to persist user message item/started")?;

        let completed = pioneer_protocol::ItemCompletedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item,
        };

        self.materialize_native_agent_turn_event(
            pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(completed),
            now_timestamp_secs(),
            None,
        )
        .await
        .context("failed to persist user message item/completed")?;
        Ok(())
    }

    pub(super) fn spawn_initial_thread_title_task(
        &self,
        thread_id: String,
        current_turn_first_user_text: Option<String>,
    ) {
        let processor = self.clone();
        tokio::spawn(async move {
            if !processor.try_acquire_title_job(thread_id.as_str()).await {
                debug!(
                    thread_id = thread_id.as_str(),
                    "thread title job is already active; trigger coalesced"
                );
                return;
            }

            if let Err(error) = processor
                .run_initial_thread_title_task(
                    thread_id.as_str(),
                    current_turn_first_user_text.as_deref(),
                )
                .await
            {
                warn!(
                    thread_id = thread_id.as_str(),
                    error = %format!("{error:#}"),
                    "thread title job crashed (non-fatal)"
                );
                processor
                    .record_title_job_state(
                        thread_id.as_str(),
                        ThreadTitleJobState::FailedNonRetriable,
                        TITLE_JOB_MAX_ATTEMPTS,
                    )
                    .await;
            }

            processor.release_title_job(thread_id.as_str()).await;
        });
    }

    async fn try_acquire_title_job(&self, thread_id: &str) -> bool {
        let mut runtime = self.title_job_runtime.lock().await;
        if let Some(state) = runtime.get(thread_id).copied()
            && state.is_active()
        {
            return false;
        }
        runtime.insert(thread_id.to_owned(), ThreadTitleJobState::Pending);
        true
    }

    async fn release_title_job(&self, thread_id: &str) {
        self.title_job_runtime.lock().await.remove(thread_id);
    }

    async fn record_title_job_state(
        &self,
        thread_id: &str,
        state: ThreadTitleJobState,
        _attempt: u32,
    ) {
        self.title_job_runtime
            .lock()
            .await
            .insert(thread_id.to_owned(), state);
    }

    async fn set_thread_title_if_changed(&self, thread_id: &str, title: &str) -> Result<bool> {
        self.crud_store
            .update_thread_name_if_changed(thread_id, title)
            .await
    }

    async fn run_initial_thread_title_task(
        &self,
        thread_id: &str,
        current_turn_first_user_text: Option<&str>,
    ) -> Result<()> {
        self.record_title_job_state(thread_id, ThreadTitleJobState::Pending, 0)
            .await;

        let Some(thread) = self.crud_store.get_thread_model(thread_id).await? else {
            self.record_title_job_state(thread_id, ThreadTitleJobState::FailedNonRetriable, 0)
                .await;
            return Ok(());
        };

        if !matches!(
            thread.origin_kind,
            pioneer_protocol::ThreadOriginKind::User
                | pioneer_protocol::ThreadOriginKind::Collaborative
                | pioneer_protocol::ThreadOriginKind::DirectMessage
        ) || self
            .crud_store
            .get_task_thread_lineage(thread_id)
            .await?
            .is_some()
        {
            self.record_title_job_state(thread_id, ThreadTitleJobState::Succeeded, 0)
                .await;
            return Ok(());
        }

        let user_text = match self.crud_store.get_first_thread_user_text(thread_id).await {
            Ok(Some(text)) => Some(text),
            Ok(None) => current_turn_first_user_text.map(str::to_owned),
            Err(error) => {
                warn!(
                    thread_id,
                    error = %format!("{error:#}"),
                    "failed to resolve first thread user text for title generation"
                );
                current_turn_first_user_text.map(str::to_owned)
            }
        };

        let Some(user_text) = user_text else {
            self.record_title_job_state(thread_id, ThreadTitleJobState::Succeeded, 0)
                .await;
            return Ok(());
        };

        let fallback_title = fallback_title_from_first_user_text(user_text.as_str());
        if let Some(fallback_title) = fallback_title.as_deref() {
            match self
                .set_thread_title_if_changed(thread_id, fallback_title)
                .await
            {
                Ok(true) => self.send_thread_updated_notification(thread_id).await,
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        thread_id,
                        error = %format!("{error:#}"),
                        "failed to persist fallback thread title"
                    );
                }
            }
        }

        for attempt in 1..=TITLE_JOB_MAX_ATTEMPTS {
            self.record_title_job_state(thread_id, ThreadTitleJobState::Running, attempt)
                .await;

            match self
                .run_thread_title_attempt(thread_id, user_text.as_str(), fallback_title.as_deref())
                .await
            {
                Ok(TitleAttemptOutcome::Succeeded) => {
                    self.record_title_job_state(thread_id, ThreadTitleJobState::Succeeded, attempt)
                        .await;
                    return Ok(());
                }
                Ok(TitleAttemptOutcome::EmptyTitle) => {
                    self.record_title_job_state(
                        thread_id,
                        ThreadTitleJobState::FailedNonRetriable,
                        attempt,
                    )
                    .await;
                    return Ok(());
                }
                Err(error) => {
                    if attempt >= TITLE_JOB_MAX_ATTEMPTS {
                        warn!(
                            thread_id,
                            attempt,
                            max_attempts = TITLE_JOB_MAX_ATTEMPTS,
                            error = %format!("{error:#}"),
                            "title generation failed after retry budget (non-fatal)"
                        );
                        self.record_title_job_state(
                            thread_id,
                            ThreadTitleJobState::FailedNonRetriable,
                            attempt,
                        )
                        .await;
                        return Ok(());
                    }

                    warn!(
                        thread_id,
                        attempt,
                        max_attempts = TITLE_JOB_MAX_ATTEMPTS,
                        error = %format!("{error:#}"),
                        "title generation attempt failed; retry scheduled"
                    );
                    self.record_title_job_state(
                        thread_id,
                        ThreadTitleJobState::FailedRetriable,
                        attempt,
                    )
                    .await;
                    sleep(title_retry_backoff(attempt)).await;
                }
            }
        }

        self.record_title_job_state(
            thread_id,
            ThreadTitleJobState::FailedNonRetriable,
            TITLE_JOB_MAX_ATTEMPTS,
        )
        .await;
        Ok(())
    }

    async fn run_thread_title_attempt(
        &self,
        thread_id: &str,
        user_text: &str,
        fallback_title: Option<&str>,
    ) -> Result<TitleAttemptOutcome> {
        let generated_title = summary::generate_thread_title(
            &self.crud_store,
            &self.provider_registry,
            thread_id,
            user_text,
            &self.summary_config,
        )
        .await?;

        let Some(generated_title) = generated_title else {
            return Ok(TitleAttemptOutcome::EmptyTitle);
        };

        if let Some(fallback_title) = fallback_title
            && normalized_titles_equal(fallback_title, generated_title.as_str())
        {
            return Ok(TitleAttemptOutcome::Succeeded);
        }

        if self
            .set_thread_title_if_changed(thread_id, generated_title.as_str())
            .await?
        {
            self.send_thread_updated_notification(thread_id).await;
        }

        Ok(TitleAttemptOutcome::Succeeded)
    }

    pub(super) async fn send_thread_updated_notification(&self, thread_id: &str) {
        let thread = match self.crud_store.get_thread_model(thread_id).await {
            Ok(Some(thread)) => thread,
            Ok(None) => {
                warn!(
                    thread_id,
                    "thread not found while sending thread/updated notification"
                );
                return;
            }
            Err(error) => {
                warn!(
                    thread_id,
                    error = %format!("{error:#}"),
                    "failed to load thread for thread/updated notification"
                );
                return;
            }
        };

        self.thread_manager
            .sync_thread_metadata_from_persisted(&thread)
            .await;

        let notification = ThreadUpdatedNotification { thread };

        self.send_notification_to_thread_subscribers(
            thread_id,
            events::THREAD_UPDATED,
            &notification,
        )
        .await;
    }
}

fn user_message_attachments_from_capabilities_with_lookup<'a>(
    capabilities: &[pioneer_protocol::TurnCapability],
    mut skill_lookup: impl FnMut(
        &pioneer_protocol::SkillId,
    ) -> Option<(Option<&'a str>, &'a str, &'a str)>,
    pack_names: &HashMap<pioneer_protocol::SkillPackId, String>,
) -> Result<Vec<pioneer_protocol::UserMessageAttachment>> {
    capabilities
        .iter()
        .map(|capability| {
            Ok(match &capability.kind {
                pioneer_protocol::TurnCapabilityKind::Skill { skill_id, pack_id } => {
                    let (owner, slug, source_kind) = skill_lookup(skill_id).ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing immutable presentation snapshot for selected skill `{skill_id}`"
                    )
                    })?;
                    let fallback_label = pioneer_skills::compact_skill_label(owner, slug);
                    let pack = pack_id
                        .as_ref()
                        .map(|pack_id| {
                            pack_names
                                .get(pack_id)
                                .map(|name| pioneer_protocol::TurnSkillPackPresentationSummary {
                                    pack_id: pack_id.clone(),
                                    label: name.clone(),
                                })
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "missing immutable presentation snapshot for selected skill pack `{pack_id}`"
                                    )
                                })
                        })
                        .transpose()?;
                    let label = if pack.is_some() {
                        fallback_label
                    } else {
                        capability_label(capability.label.as_deref(), fallback_label.as_str())
                    };
                    pioneer_protocol::UserMessageAttachment::Skill {
                        capability: pioneer_protocol::TurnSkillCapabilitySummary {
                            skill_id: skill_id.clone(),
                            label,
                            owner: owner.map(str::to_owned),
                            slug: slug.to_owned(),
                            source_kind: source_kind.to_owned(),
                            pack,
                        },
                    }
                }
                pioneer_protocol::TurnCapabilityKind::SkillPack { pack_id } => {
                    let name = pack_names.get(pack_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "missing immutable presentation snapshot for selected skill pack `{pack_id}`"
                        )
                    })?;
                    pioneer_protocol::UserMessageAttachment::SkillPack {
                        capability: pioneer_protocol::TurnSkillPackCapabilitySummary {
                            pack_id: pack_id.clone(),
                            label: name.clone(),
                        },
                    }
                }
                pioneer_protocol::TurnCapabilityKind::McpServer { name, scope_kind } => {
                    pioneer_protocol::UserMessageAttachment::McpServer {
                        capability: pioneer_protocol::TurnMcpServerCapabilitySummary {
                            id: capability.id.clone(),
                            label: capability_label(capability.label.as_deref(), name),
                            name: name.clone(),
                            scope_kind: *scope_kind,
                        },
                    }
                }
                pioneer_protocol::TurnCapabilityKind::McpTool {
                    server_name,
                    raw_tool_name,
                    scope_kind,
                } => {
                    let fallback = format!("{server_name} / {raw_tool_name}");
                    pioneer_protocol::UserMessageAttachment::McpTool {
                        capability: pioneer_protocol::TurnMcpToolCapabilitySummary {
                            id: capability.id.clone(),
                            label: capability_label(capability.label.as_deref(), fallback.as_str()),
                            server_name: server_name.clone(),
                            raw_tool_name: raw_tool_name.clone(),
                            scope_kind: *scope_kind,
                        },
                    }
                }
            })
        })
        .collect()
}

pub(super) fn user_message_attachments_from_capabilities_and_catalog(
    capabilities: &[pioneer_protocol::TurnCapability],
    catalog: &pioneer_skills::SkillCatalogSnapshot,
    pack_names: &HashMap<pioneer_protocol::SkillPackId, String>,
) -> Result<Vec<pioneer_protocol::UserMessageAttachment>> {
    user_message_attachments_from_capabilities_with_lookup(
        capabilities,
        |skill_id| {
            catalog
                .skills
                .iter()
                .find(|skill| &skill.identity.skill_id == skill_id)
                .map(|skill| {
                    (
                        skill.identity.owner.as_deref(),
                        skill.identity.slug.as_str(),
                        skill.identity.source_kind.as_db_value(),
                    )
                })
        },
        pack_names,
    )
}

pub(super) fn user_message_attachments_from_capabilities_and_bindings(
    capabilities: &[pioneer_protocol::TurnCapability],
    bindings: &[pioneer_protocol::TurnSkillBinding],
    pack_names: &HashMap<pioneer_protocol::SkillPackId, String>,
) -> Result<Vec<pioneer_protocol::UserMessageAttachment>> {
    user_message_attachments_from_capabilities_with_lookup(
        capabilities,
        |skill_id| {
            bindings
                .iter()
                .find(|binding| &binding.skill_id == skill_id)
                .map(|binding| {
                    (
                        binding.skill_owner.as_deref(),
                        binding.skill_slug.as_str(),
                        binding.source_kind.as_str(),
                    )
                })
        },
        pack_names,
    )
}

fn capability_label(label: Option<&str>, fallback: &str) -> String {
    label
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .unwrap_or(fallback)
        .to_owned()
}

#[cfg(test)]
mod user_message_attachment_tests {
    use super::user_message_attachments_from_capabilities_and_bindings;
    use pioneer_protocol::{
        SkillId, TurnCapability, TurnCapabilityKind, TurnSkillBinding, UserMessageAttachment,
    };
    use std::collections::HashMap;

    #[test]
    fn selected_skill_attachment_is_an_owned_binding_snapshot() {
        let skill_id = SkillId::new("AAAAAAAAAAAAAAAAAAAAA").expect("valid skill id");
        let capability = TurnCapability {
            id: format!("skill:{skill_id}"),
            kind: TurnCapabilityKind::Skill {
                skill_id: skill_id.clone(),
                pack_id: None,
            },
            label: None,
        };
        let mut bindings = vec![TurnSkillBinding {
            skill_id: skill_id.clone(),
            skill_owner: Some("owner-before".to_owned()),
            skill_slug: "slug-before".to_owned(),
            skill_version: None,
            fingerprint: "fingerprint".to_owned(),
            source_kind: "user".to_owned(),
            resolved_reason: "explicit".to_owned(),
        }];

        let attachments = user_message_attachments_from_capabilities_and_bindings(
            std::slice::from_ref(&capability),
            bindings.as_slice(),
            &HashMap::new(),
        )
        .expect("binding must snapshot");
        bindings[0].skill_owner = Some("owner-after".to_owned());
        bindings[0].skill_slug = "slug-after".to_owned();

        assert_eq!(
            attachments,
            vec![UserMessageAttachment::Skill {
                capability: pioneer_protocol::TurnSkillCapabilitySummary {
                    skill_id,
                    label: "owner-before/slug-before".to_owned(),
                    owner: Some("owner-before".to_owned()),
                    slug: "slug-before".to_owned(),
                    source_kind: "user".to_owned(),
                    pack: None,
                },
            }]
        );
    }

    #[test]
    fn selected_skill_attachment_requires_an_exact_binding_snapshot() {
        let skill_id = SkillId::new("BBBBBBBBBBBBBBBBBBBBB").expect("valid skill id");
        let capability = TurnCapability {
            id: format!("skill:{skill_id}"),
            kind: TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            },
            label: None,
        };

        let error = user_message_attachments_from_capabilities_and_bindings(
            std::slice::from_ref(&capability),
            &[],
            &HashMap::new(),
        )
        .expect_err("missing binding must fail closed");

        assert!(
            error
                .to_string()
                .contains("missing immutable presentation snapshot")
        );
    }

    #[test]
    fn pack_attachments_snapshot_authoritative_names_and_original_shape() {
        let pack_id = pioneer_protocol::SkillPackId::new("P".repeat(21)).expect("pack id");
        let skill_id = SkillId::new("S".repeat(21)).expect("skill id");
        let bindings = vec![TurnSkillBinding {
            skill_id: skill_id.clone(),
            skill_owner: Some("owner".to_owned()),
            skill_slug: "member".to_owned(),
            skill_version: None,
            fingerprint: "fingerprint".to_owned(),
            source_kind: "user".to_owned(),
            resolved_reason: "explicit".to_owned(),
        }];
        let mut pack_names = HashMap::from([(pack_id.clone(), "Authoritative Pack".to_owned())]);

        let partial = user_message_attachments_from_capabilities_and_bindings(
            &[TurnCapability {
                id: pioneer_protocol::skill_capability_key(&skill_id),
                kind: TurnCapabilityKind::Skill {
                    skill_id: skill_id.clone(),
                    pack_id: Some(pack_id.clone()),
                },
                label: Some("untrusted label".to_owned()),
            }],
            bindings.as_slice(),
            &pack_names,
        )
        .expect("partial pack snapshot");
        let full = user_message_attachments_from_capabilities_and_bindings(
            &[TurnCapability {
                id: pioneer_protocol::skill_pack_capability_key(&pack_id),
                kind: TurnCapabilityKind::SkillPack {
                    pack_id: pack_id.clone(),
                },
                label: Some("untrusted pack label".to_owned()),
            }],
            bindings.as_slice(),
            &pack_names,
        )
        .expect("full pack snapshot");

        pack_names.insert(pack_id.clone(), "Renamed Pack".to_owned());
        pack_names.remove(&pack_id);
        assert!(matches!(
            &partial[0],
            UserMessageAttachment::Skill { capability }
                if capability.label == "owner/member"
                    && matches!(
                        capability.pack.as_ref(),
                        Some(pack) if pack.pack_id == pack_id && pack.label == "Authoritative Pack"
                    )
        ));
        assert!(matches!(
            &full[0],
            UserMessageAttachment::SkillPack { capability }
                if capability.pack_id == pack_id && capability.label == "Authoritative Pack"
        ));
    }
}
