use super::*;
use anyhow::Result;

const ACTIVE_TURN_LLM_CONTEXT_TTL_SECS: i64 = 7 * 24 * 60 * 60;
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

fn llm_context_expires_at(
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    created_at + chrono::Duration::seconds(ACTIVE_TURN_LLM_CONTEXT_TTL_SECS)
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

fn execution_window_can_create_after(
    latest: Option<&pioneer_crud::TurnExecutionWindowRecord>,
    window_index: u32,
) -> bool {
    match latest {
        None => window_index == 1,
        Some(window) => window.window_index.saturating_add(1) == window_index,
    }
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
        | AgentDurableEvent::ProviderFailureDetected { thread_id, .. }
        | AgentDurableEvent::RecoveryAttemptSucceeded { thread_id, .. }
        | AgentDurableEvent::TurnCompleted { thread_id, .. }
        | AgentDurableEvent::TurnFailed { thread_id, .. }
        | AgentDurableEvent::TurnBlocked { thread_id, .. }
        | AgentDurableEvent::TurnInterrupted { thread_id, .. } => Some(thread_id.as_str()),
        AgentDurableEvent::ItemStarted { notification } => Some(notification.thread_id.as_str()),
        AgentDurableEvent::ItemCompleted { notification } => Some(notification.thread_id.as_str()),
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
    TurnStart,
    TurnDispatch,
    ProjectionFailure,
    ExecutionWindowContinuation,
    ArtifactFinalization,
    TaskDispatch,
    RuntimeFailure,
}

impl TurnFailureRecoveryKind {
    const fn trigger(self) -> pioneer_protocol::RecoveryTrigger {
        match self {
            Self::TurnStart => pioneer_protocol::RecoveryTrigger::TurnStart,
            Self::TurnDispatch => pioneer_protocol::RecoveryTrigger::TurnDispatch,
            Self::ProjectionFailure => pioneer_protocol::RecoveryTrigger::ProjectionFailure,
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
            Self::ProjectionFailure => pioneer_protocol::RecoveryAction::RetryWithBackoff,
            Self::ArtifactFinalization => {
                pioneer_protocol::RecoveryAction::RepairArtifactFinalization
            }
            Self::TurnStart
            | Self::TurnDispatch
            | Self::ExecutionWindowContinuation
            | Self::TaskDispatch
            | Self::RuntimeFailure => pioneer_protocol::RecoveryAction::RestartTurn,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::TurnStart => "turn_start",
            Self::TurnDispatch => "turn_dispatch",
            Self::ProjectionFailure => "projection_failure",
            Self::ExecutionWindowContinuation => "execution_window_continuation",
            Self::ArtifactFinalization => "artifact_finalization",
            Self::TaskDispatch => "task_dispatch",
            Self::RuntimeFailure => "runtime_failure",
        }
    }
}

fn classify_legacy_turn_failure(error_message: &str) -> TurnFailureRecoveryKind {
    let normalized = error_message.trim().to_ascii_lowercase();
    if normalized.starts_with("failed to persist")
        || normalized.starts_with("failed to load")
        || normalized.starts_with("failed to create execution window")
        || normalized.starts_with("failed to mark execution window")
        || normalized.contains("projection")
        || normalized.contains("materialize")
    {
        return TurnFailureRecoveryKind::ProjectionFailure;
    }
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
            .count_turn_execution_window_terminal_items(turn_id)
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

    pub(super) async fn ensure_agent_listener_task(&self, thread_id: &str) {
        if self
            .agent_listener_tasks
            .lock()
            .await
            .contains_key(thread_id)
        {
            return;
        }

        let Some(mut durable_receiver) = self.agent_manager.take_durable_receiver(thread_id).await
        else {
            return;
        };

        let mut live_receiver = self.agent_manager.subscribe_progress(thread_id).await;

        let this = self.clone();
        let thread_id_owned = thread_id.to_owned();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    durable = durable_receiver.recv() => {
                        let Some(event) = durable else {
                            break;
                        };
                        this.handle_durable_agent_event(event).await;
                    }
                    live = async {
                        match live_receiver.as_mut() {
                            Some(receiver) => Some(receiver.recv().await),
                            None => None,
                        }
                    }, if live_receiver.is_some() => {
                        match live {
                            Some(Ok(event)) => {
                                this.handle_progress_agent_event(event).await;
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

        self.agent_listener_tasks
            .lock()
            .await
            .insert(thread_id.to_owned(), handle);
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

    async fn next_turn_llm_context_sequence(&self, turn_id: &str, observed_sequence: i64) -> i64 {
        let mut sequences = self.turn_llm_context_sequences.lock().await;
        let current = sequences.get(turn_id).copied().unwrap_or_default();
        let next = current.max(observed_sequence).saturating_add(1);
        sequences.insert(turn_id.to_owned(), next);
        next
    }

    async fn clear_turn_llm_context_state(&self, turn_id: &str) {
        self.turn_llm_context_sequences.lock().await.remove(turn_id);
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

    pub(super) async fn handle_durable_agent_event(&self, event: AgentDurableEvent) {
        let thread_id = durable_event_thread_id(&event).map(str::to_owned);
        let committed = message_future(self.persist_durable_agent_event(event.clone())).await;
        if committed {
            if let AgentDurableEvent::ItemCompleted { notification } = &event {
                self.ingest_committed_thread_item(notification).await;
            }
            if let Some(thread_id) = thread_id {
                self.agent_manager
                    .publish_committed(thread_id.as_str(), event)
                    .await;
            }
        }
    }

    async fn ingest_committed_thread_item(
        &self,
        notification: &pioneer_protocol::ItemCompletedNotification,
    ) {
        let Some(input) = crate::thread_episodic::committed_item_ingestion_input(notification)
        else {
            debug!(
                workspace_id = notification.workspace_id,
                thread_id = notification.thread_id,
                turn_id = notification.turn_id,
                item_id = notification.item.item_id(),
                "skipping thread episodic ingestion for committed item with missing required ids"
            );
            return;
        };

        let wake_indexer = thread_episodic_index_wakeup_after_commit(&notification.item);
        let ingestor = self.thread_episodic_ingestor.read().await.clone();
        match ingestor.ingest_committed_item(input).await {
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
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "thread episodic ingestion failed after committed item persistence"
                );
            }
        }
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

    async fn persist_durable_agent_event(&self, event: AgentDurableEvent) -> bool {
        match event {
            AgentDurableEvent::PromptManifestCompiled {
                thread_id,
                turn_id,
                manifest,
            } => {
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
                    }
                    Err(error) => {
                        warn!(
                            thread_id,
                            turn_id,
                            error = %format!("{error:#}"),
                            "failed to persist prompt manifest metadata; continuing"
                        );
                    }
                }
            }
            AgentDurableEvent::TurnSkillsResolved {
                thread_id,
                turn_id,
                bindings,
            } => {
                let event_timestamp = now_timestamp_secs();
                let binding_records = bindings
                    .iter()
                    .map(|binding| pioneer_crud::TurnSkillBindingRecord {
                        skill_slug: binding.skill_slug.clone(),
                        skill_version: binding.skill_version.clone(),
                        fingerprint: binding.fingerprint.clone(),
                        source_kind: binding.source_kind.clone(),
                        resolved_reason: binding.resolved_reason.clone(),
                    })
                    .collect::<Vec<_>>();
                if let Err(error) = self
                    .crud_store
                    .replace_turn_skill_bindings(
                        turn_id.as_str(),
                        binding_records.as_slice(),
                        event_timestamp,
                    )
                    .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to persist turn skill bindings; continuing without persistence"
                    );
                }
            }
            AgentDurableEvent::TurnCapabilitiesResolved {
                thread_id,
                turn_id,
                accepted,
                rejected,
                mcp_bindings,
            } => {
                if !mcp_bindings.is_empty() {
                    let event_timestamp = now_timestamp_secs();
                    let binding_records = mcp_bindings
                        .iter()
                        .map(|binding| pioneer_crud::TurnMcpBindingRecord {
                            server_installation_id: binding.server_installation_id.clone(),
                            server_name: binding.server_name.clone(),
                            raw_tool_name: binding.raw_tool_name.clone(),
                            callable_name: binding.callable_name.clone(),
                            catalog_version: binding.catalog_version.clone(),
                            fingerprint: binding.fingerprint.clone(),
                            selection_reason: binding.selection_reason.clone(),
                            capability_id: binding.capability_id.clone(),
                        })
                        .collect::<Vec<_>>();
                    if let Err(error) = self
                        .crud_store
                        .replace_turn_mcp_bindings(
                            turn_id.as_str(),
                            binding_records.as_slice(),
                            event_timestamp,
                        )
                        .await
                    {
                        warn!(
                            thread_id,
                            turn_id,
                            error = %format!("{error:#}"),
                            "failed to persist turn MCP bindings; continuing without persistence"
                        );
                    }
                }
                if !rejected.is_empty() {
                    warn!(
                        thread_id,
                        turn_id,
                        accepted = accepted.len(),
                        rejected = rejected.len(),
                        "turn capability resolution rejected selected capabilities"
                    );
                }
            }
            AgentDurableEvent::SkillAuditEvents {
                thread_id,
                turn_id,
                events,
            } => {
                let mut records = Vec::new();
                let mut dependency_snapshots = Vec::new();

                for event in events {
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
                            skill_slug,
                            source_kind,
                            diagnostics_json,
                            created_at_unix,
                        });
                    }
                }

                if let Err(error) = self
                    .crud_store
                    .append_skill_audit_event_records(turn_id.as_str(), records.as_slice())
                    .await
                {
                    warn!(
                        thread_id,
                        turn_id,
                        error = %format!("{error:#}"),
                        "failed to persist skill audit events; continuing"
                    );
                }

                for snapshot in dependency_snapshots {
                    if let Err(error) = self
                        .crud_store
                        .insert_skill_dependency_snapshot_record(&snapshot)
                        .await
                    {
                        warn!(
                            thread_id,
                            turn_id,
                            error = %format!("{error:#}"),
                            "failed to persist skill dependency snapshot; continuing"
                        );
                    }
                }
            }
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
            } => {
                let sequence = self
                    .next_turn_llm_context_sequence(turn_id.as_str(), sequence)
                    .await;
                let created_at = now_db_timestamp();
                let entry = pioneer_crud::NewTurnLlmContextEntry {
                    turn_id: turn_id.clone(),
                    item_id: Some(item_id),
                    attempt_id,
                    sequence,
                    source,
                    tool_name: Some(tool_name),
                    payload: serde_json::to_string(&tool_result_view_from_protocol(payload))
                        .unwrap_or_else(|_| serde_json::json!({}).to_string()),
                    output_policy_snapshot: serde_json::to_string(&output_policy_snapshot)
                        .unwrap_or_else(|_| serde_json::json!({}).to_string()),
                    created_at,
                    expires_at: Some(llm_context_expires_at(created_at)),
                };
                if let Err(error) = self.crud_store.insert_turn_llm_context(entry).await {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to persist turn llm context: {error:#}"),
                    )
                    .await;
                    return false;
                }
            }
            AgentDurableEvent::ItemStarted { notification } => {
                let mut notification = notification;
                self.enrich_item_started_markdown(&mut notification).await;
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                let item_id = notification.item.item_id().to_owned();
                let deadlines = self
                    .timeout_supervisor
                    .deadlines_for_item(&notification.item, event_timestamp);
                if let Err(error) = message_future(
                    self.crud_store
                        .materialize_item_started_with_attempt_deadlines(
                            notification.clone(),
                            event_timestamp,
                            deadlines,
                        ),
                )
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
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_STARTED,
                    &notification,
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
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                )
                .await;
                debug!(
                    thread_id = notification.thread_id,
                    turn_id = notification.turn_id,
                    item_id,
                    item_type = ?notification.item.item_type(),
                    "registered item attempt deadlines during item/started projection"
                );
            }
            AgentDurableEvent::ItemCompleted { notification } => {
                let mut notification = notification;
                self.enrich_item_completed_markdown(&mut notification).await;
                self.record_final_assistant_text_for_item(&notification)
                    .await;
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = message_future(
                    self.crud_store
                        .materialize_item_completed(notification.clone(), event_timestamp),
                )
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
                self.register_artifacts_for_completed_item(&notification)
                    .await;
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_COMPLETED,
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
            AgentDurableEvent::ItemToolRetryScheduled { notification } => {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_item_tool_retry_scheduled(notification.clone(), event_timestamp)
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
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_TOOL_RETRY_SCHEDULED,
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
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                )
                .await;
            }
            AgentDurableEvent::ItemToolRetryResolved { notification } => {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_item_tool_retry_resolved(notification.clone(), event_timestamp)
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
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_TOOL_RETRY_RESOLVED,
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
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                )
                .await;
            }
            AgentDurableEvent::ItemToolRetryExhausted { notification } => {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_item_tool_retry_exhausted(notification.clone(), event_timestamp)
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
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_TOOL_RETRY_EXHAUSTED,
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
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                )
                .await;
            }
            AgentDurableEvent::TurnToolLoopBudgetExceeded { notification } => {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_turn_tool_loop_budget_exceeded(
                        notification.clone(),
                        event_timestamp,
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
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_TOOL_LOOP_BUDGET_EXCEEDED,
                    &notification,
                )
                .await;
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                )
                .await;
            }
            AgentDurableEvent::TurnExecutionWindowStarted { notification } => {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_turn_execution_window_started(
                        notification.clone(),
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
                let latest = match self
                    .crud_store
                    .latest_turn_execution_window(notification.turn_id.as_str())
                    .await
                {
                    Ok(latest) => latest,
                    Err(error) => {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to load latest execution window: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                };
                if execution_window_can_create_after(latest.as_ref(), notification.window_index) {
                    if let Err(error) = self
                        .crud_store
                        .create_turn_execution_window(
                            pioneer_crud::NewTurnExecutionWindowRecord {
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
                            db_timestamp_from_unix_ms(event_timestamp.saturating_mul(1000)),
                            db_timestamp_from_unix_ms(event_timestamp.saturating_mul(1000)),
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to create execution window: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                } else if latest
                    .as_ref()
                    .is_some_and(|window| window.window_index < notification.window_index)
                {
                    warn!(
                        turn_id = %notification.turn_id,
                        latest_window_index = latest.as_ref().map(|window| window.window_index),
                        event_window_index = notification.window_index,
                        "skipping out-of-order execution window started event"
                    );
                }
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_STARTED,
                    &notification,
                )
                .await;
            }
            AgentDurableEvent::TurnExecutionWindowExhausted { notification } => {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_turn_execution_window_exhausted(
                        notification.clone(),
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
                let latest = match self
                    .crud_store
                    .latest_turn_execution_window(notification.turn_id.as_str())
                    .await
                {
                    Ok(latest) => latest,
                    Err(error) => {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to load execution window for exhaustion: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                };
                let window = if latest
                    .as_ref()
                    .is_some_and(|window| window.window_index == notification.window_index)
                {
                    latest
                } else if execution_window_can_create_after(
                    latest.as_ref(),
                    notification.window_index,
                ) {
                    match self
                        .crud_store
                        .create_turn_execution_window(
                            pioneer_crud::NewTurnExecutionWindowRecord {
                                workspace_id: notification.workspace_id.clone(),
                                thread_id: notification.thread_id.clone(),
                                turn_id: notification.turn_id.clone(),
                                window_index: notification.window_index,
                                status: pioneer_protocol::ExecutionWindowStatus::Running,
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
                            db_timestamp_from_unix_ms(event_timestamp.saturating_mul(1000)),
                            db_timestamp_from_unix_ms(event_timestamp.saturating_mul(1000)),
                        )
                        .await
                    {
                        Ok(window) => Some(window),
                        Err(error) => {
                            self.report_legacy_turn_failure(
                                thread_id.clone(),
                                turn_id.clone(),
                                format!(
                                    "failed to create execution window for exhaustion: {error:#}"
                                ),
                            )
                            .await;
                            return false;
                        }
                    }
                } else {
                    warn!(
                        turn_id = %notification.turn_id,
                        latest_window_index = latest.as_ref().map(|window| window.window_index),
                        event_window_index = notification.window_index,
                        "skipping out-of-order execution window exhausted event"
                    );
                    None
                };
                if let Some(window) = window
                    && let Err(error) = self
                        .crud_store
                        .mark_turn_execution_window_exhausted(
                            window.id.as_str(),
                            notification.exhaustion_reason,
                            pioneer_crud::TurnExecutionWindowStatsRecord {
                                agent_round_count: notification.agent_round_count,
                                tool_call_count: notification.tool_call_count,
                                provider_token_count: notification
                                    .provider_token_count
                                    .unwrap_or(0),
                                metadata_json: execution_window_exhausted_metadata(
                                    notification.window_id.as_str(),
                                    notification.limit,
                                    notification.observed,
                                    notification.reason.as_str(),
                                ),
                                completed_at: db_timestamp_from_unix_ms(
                                    notification.exhausted_at_unix_ms,
                                ),
                                updated_at: now_db_timestamp(),
                            },
                        )
                        .await
                {
                    self.report_legacy_turn_failure(
                        thread_id.clone(),
                        turn_id.clone(),
                        format!("failed to mark execution window exhausted: {error:#}"),
                    )
                    .await;
                    return false;
                }
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_EXHAUSTED,
                    &notification,
                )
                .await;
            }
            AgentDurableEvent::TurnExecutionWindowCheckpointed {
                notification,
                payload,
            } => {
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_turn_execution_window_checkpointed(
                        notification.clone(),
                        event_timestamp,
                    )
                    .await
                {
                    self.report_legacy_turn_failure(
                        thread_id.clone(),
                        turn_id.clone(),
                        format!("failed to persist turn/execution_window/checkpointed: {error:#}"),
                    )
                    .await;
                    return false;
                }
                let latest = match self
                    .crud_store
                    .latest_turn_execution_window(notification.turn_id.as_str())
                    .await
                {
                    Ok(latest) => latest,
                    Err(error) => {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to load execution window for checkpoint: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                };
                if let Some(window) =
                    latest.filter(|window| window.window_index == notification.window_index)
                {
                    let Some(checkpoint_kind) =
                        execution_checkpoint_kind_from_wire(notification.checkpoint_kind.as_str())
                    else {
                        warn!(
                            turn_id = %notification.turn_id,
                            checkpoint_kind = %notification.checkpoint_kind,
                            "skipping execution window checkpoint with unknown kind"
                        );
                        self.send_notification_to_thread_subscribers(
                            notification.thread_id.as_str(),
                            events::TURN_EXECUTION_WINDOW_CHECKPOINTED,
                            &notification,
                        )
                        .await;
                        return true;
                    };
                    let payload_json = match serde_json::to_value(&payload) {
                        Ok(payload_json) => payload_json,
                        Err(error) => {
                            warn!(
                                turn_id = %notification.turn_id,
                                error = %format!("{error:#}"),
                                "skipping execution window checkpoint with unserializable payload"
                            );
                            self.send_notification_to_thread_subscribers(
                                notification.thread_id.as_str(),
                                events::TURN_EXECUTION_WINDOW_CHECKPOINTED,
                                &notification,
                            )
                            .await;
                            return true;
                        }
                    };
                    let payload_size = serde_json::to_vec(&payload_json)
                        .map(|bytes| bytes.len())
                        .unwrap_or(usize::MAX);
                    if payload_size > pioneer_crud::TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES {
                        warn!(
                            turn_id = %notification.turn_id,
                            payload_size,
                            max_payload_size =
                                pioneer_crud::TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES,
                            "skipping oversized execution window checkpoint payload"
                        );
                        self.send_notification_to_thread_subscribers(
                            notification.thread_id.as_str(),
                            events::TURN_EXECUTION_WINDOW_CHECKPOINTED,
                            &notification,
                        )
                        .await;
                        return true;
                    }
                    if let Err(error) = self
                        .crud_store
                        .save_turn_execution_checkpoint(
                            pioneer_crud::NewTurnExecutionCheckpointRecord {
                                window_id: window.id.clone(),
                                workspace_id: notification.workspace_id.clone(),
                                thread_id: notification.thread_id.clone(),
                                turn_id: notification.turn_id.clone(),
                                checkpoint_kind,
                                payload_json,
                                created_at: db_timestamp_from_unix_ms(
                                    notification.created_at_unix_ms,
                                ),
                            },
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to save execution window checkpoint: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                    if let Err(error) = self
                        .crud_store
                        .mark_turn_execution_window_checkpointed(
                            window.id.as_str(),
                            db_timestamp_from_unix_ms(notification.created_at_unix_ms),
                        )
                        .await
                    {
                        self.report_legacy_turn_failure(
                            thread_id.clone(),
                            turn_id.clone(),
                            format!("failed to mark execution window checkpointed: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                } else {
                    warn!(
                        turn_id = %notification.turn_id,
                        event_window_index = notification.window_index,
                        "skipping execution window checkpoint without matching stored window"
                    );
                }
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_CHECKPOINTED,
                    &notification,
                )
                .await;
            }
            AgentDurableEvent::TurnExecutionWindowContinued { notification } => {
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_turn_execution_window_continued(
                        notification.clone(),
                        event_timestamp,
                    )
                    .await
                {
                    self.report_legacy_turn_failure(
                        notification.thread_id.clone(),
                        notification.turn_id.clone(),
                        format!("failed to persist turn/execution_window/continued: {error:#}"),
                    )
                    .await;
                    return false;
                }
                match self
                    .crud_store
                    .latest_turn_execution_window(notification.turn_id.as_str())
                    .await
                {
                    Ok(Some(window))
                        if window.window_index == notification.previous_window_index =>
                    {
                        if let Err(error) = self
                            .crud_store
                            .mark_turn_execution_window_continued(
                                window.id.as_str(),
                                db_timestamp_from_unix_ms(notification.continued_at_unix_ms),
                            )
                            .await
                        {
                            self.report_legacy_turn_failure(
                                notification.thread_id.clone(),
                                notification.turn_id.clone(),
                                format!("failed to mark execution window continued: {error:#}"),
                            )
                            .await;
                            return false;
                        }
                    }
                    Ok(Some(window)) => {
                        warn!(
                            turn_id = %notification.turn_id,
                            latest_window_index = window.window_index,
                            previous_window_index = notification.previous_window_index,
                            "skipping out-of-order execution window continued state update"
                        );
                    }
                    Ok(None) => {
                        warn!(
                            turn_id = %notification.turn_id,
                            previous_window_index = notification.previous_window_index,
                            "skipping execution window continued state update without stored window"
                        );
                    }
                    Err(error) => {
                        self.report_legacy_turn_failure(
                            notification.thread_id.clone(),
                            notification.turn_id.clone(),
                            format!("failed to load execution window for continuation: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                }
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_CONTINUED,
                    &notification,
                )
                .await;
            }
            AgentDurableEvent::TurnExecutionWindowBlocked { notification } => {
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = self
                    .crud_store
                    .materialize_turn_execution_window_blocked(
                        notification.clone(),
                        event_timestamp,
                    )
                    .await
                {
                    self.report_legacy_turn_failure(
                        notification.thread_id.clone(),
                        notification.turn_id.clone(),
                        format!("failed to persist turn/execution_window/blocked: {error:#}"),
                    )
                    .await;
                    return false;
                }
                let latest = match self
                    .crud_store
                    .latest_turn_execution_window(notification.turn_id.as_str())
                    .await
                {
                    Ok(latest) => latest,
                    Err(error) => {
                        self.report_legacy_turn_failure(
                            notification.thread_id.clone(),
                            notification.turn_id.clone(),
                            format!("failed to load execution window for blocked state: {error:#}"),
                        )
                        .await;
                        return false;
                    }
                };
                let window = if latest
                    .as_ref()
                    .is_some_and(|window| window.window_index == notification.window_index)
                {
                    latest
                } else if execution_window_can_create_after(
                    latest.as_ref(),
                    notification.window_index,
                ) {
                    match self
                        .crud_store
                        .create_turn_execution_window(
                            pioneer_crud::NewTurnExecutionWindowRecord {
                                workspace_id: notification.workspace_id.clone(),
                                thread_id: notification.thread_id.clone(),
                                turn_id: notification.turn_id.clone(),
                                window_index: notification.window_index,
                                status: pioneer_protocol::ExecutionWindowStatus::Running,
                                exhaustion_reason: None,
                                agent_round_count: 0,
                                tool_call_count: 0,
                                provider_token_count: 0,
                                metadata_json: execution_window_started_metadata(
                                    notification.window_id.as_str(),
                                ),
                                started_at: db_timestamp_from_unix_ms(
                                    notification.blocked_at_unix_ms,
                                ),
                            },
                            db_timestamp_from_unix_ms(event_timestamp.saturating_mul(1000)),
                            db_timestamp_from_unix_ms(event_timestamp.saturating_mul(1000)),
                        )
                        .await
                    {
                        Ok(window) => Some(window),
                        Err(error) => {
                            self.report_legacy_turn_failure(
                                notification.thread_id.clone(),
                                notification.turn_id.clone(),
                                format!(
                                    "failed to create execution window for blocked state: {error:#}"
                                ),
                            )
                            .await;
                            return false;
                        }
                    }
                } else {
                    warn!(
                        turn_id = %notification.turn_id,
                        latest_window_index = latest.as_ref().map(|window| window.window_index),
                        event_window_index = notification.window_index,
                        "skipping out-of-order execution window blocked state update"
                    );
                    None
                };
                if let Some(window) = window
                    && let Err(error) = self
                        .crud_store
                        .mark_turn_execution_window_blocked(
                            window.id.as_str(),
                            notification.exhaustion_reason,
                            pioneer_crud::TurnExecutionWindowStatsRecord {
                                agent_round_count: 0,
                                tool_call_count: notification.total_tool_calls,
                                provider_token_count: 0,
                                metadata_json: execution_window_blocked_metadata(
                                    notification.window_id.as_str(),
                                    notification.total_windows,
                                    notification.total_tool_calls,
                                    notification.reason.as_str(),
                                ),
                                completed_at: db_timestamp_from_unix_ms(
                                    notification.blocked_at_unix_ms,
                                ),
                                updated_at: now_db_timestamp(),
                            },
                        )
                        .await
                {
                    self.report_legacy_turn_failure(
                        notification.thread_id.clone(),
                        notification.turn_id.clone(),
                        format!("failed to mark execution window blocked: {error:#}"),
                    )
                    .await;
                    return false;
                }
                if notification
                    .reason
                    .contains("execution window continuation could not resume")
                {
                    if !self
                        .report_turn_failure(
                            notification.thread_id.clone(),
                            notification.turn_id.clone(),
                            TurnFailureRecoveryKind::ExecutionWindowContinuation,
                            notification.reason.clone(),
                        )
                        .await
                    {
                        return false;
                    }
                } else if !self
                    .mark_turn_blocked(
                        notification.thread_id.clone(),
                        notification.turn_id.clone(),
                        notification.reason.clone(),
                    )
                    .await
                {
                    return false;
                }
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::TURN_EXECUTION_WINDOW_BLOCKED,
                    &notification,
                )
                .await;
            }
            AgentDurableEvent::TurnCompleted {
                thread_id,
                turn_id,
                recovery,
            } => {
                if !message_future(self.complete_turn(thread_id, turn_id, recovery)).await {
                    return false;
                }
            }
            AgentDurableEvent::ProviderFailureDetected {
                thread_id,
                turn_id,
                item_id,
                item_type,
                failure,
                recovery,
            } => {
                message_future(self.handle_provider_failure_detected(
                    thread_id, turn_id, item_id, item_type, failure, recovery,
                ))
                .await;
            }
            AgentDurableEvent::RecoveryAttemptSucceeded {
                thread_id,
                turn_id,
                recovery,
            } => {
                message_future(
                    self.handle_recovery_attempt_succeeded(thread_id, turn_id, recovery),
                )
                .await;
            }
            AgentDurableEvent::TurnFailed {
                thread_id,
                turn_id,
                error,
                recovery,
            } => {
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
            }
            AgentDurableEvent::TurnBlocked {
                thread_id,
                turn_id,
                reason,
                recovery,
            } => {
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
            }
            AgentDurableEvent::TurnInterrupted {
                thread_id,
                turn_id,
                reason,
                recovery,
            } => {
                if !message_future(
                    self.mark_turn_interrupted_with_recovery(thread_id, turn_id, reason, recovery),
                )
                .await
                {
                    return false;
                }
            }
            AgentDurableEvent::TaskEvent { .. }
            | AgentDurableEvent::ThreadLineageCreated { .. } => return false,
        }
        true
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
    ) {
        let now_unix = now_timestamp_secs();
        match self
            .recovery_coordinator
            .succeed_active_recovery_attempt(turn_id.as_str(), &recovery, now_unix)
            .await
        {
            Ok(events) => {
                for event in events {
                    self.handle_recovery_event(event, now_unix).await;
                }
            }
            Err(error) => {
                self.report_legacy_turn_failure(
                    thread_id,
                    turn_id,
                    format!("failed to mark recovery attempt succeeded: {error:#}"),
                )
                .await;
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
            max_wall_clock_secs: 180,
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
        message_future(self.handle_recovery_event(event, now_unix)).await;
        true
    }

    pub(super) async fn handle_provider_failure_detected(
        &self,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        failure: pioneer_protocol::ProviderFailureDetails,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) {
        let now_unix = now_timestamp_secs();

        if let Some(recovery) = recovery {
            match self
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
                    for event in events {
                        self.handle_recovery_event(event, now_unix).await;
                    }
                }
                Err(error) => {
                    self.report_legacy_turn_failure(
                        thread_id,
                        turn_id,
                        format!("failed to update provider recovery: {error:#}"),
                    )
                    .await;
                }
            }
            return;
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
                if let Some((_, workspace_id)) = self
                    .crud_store
                    .get_turn_location(turn_id.as_str())
                    .await
                    .ok()
                    .flatten()
                {
                    if is_created {
                        let opened = pioneer_protocol::ItemRecoveryOpenedNotification {
                            workspace_id,
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            item_id: item_id.clone(),
                            item_type,
                            recovery_job_id: job.id.clone(),
                            trigger: job.trigger,
                            action: job.action,
                            attempt_number: next_attempt_number,
                        };
                        self.persist_and_send_item_recovery_opened(opened, now_unix)
                            .await;
                    } else {
                        let attached = pioneer_protocol::ItemRecoveryAttachedNotification {
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
                        };
                        self.persist_and_send_item_recovery_attached(attached, now_unix)
                            .await;
                    }
                }

                if should_terminalize {
                    match self
                        .recovery_coordinator
                        .terminalize_pending_mark_failed_job(job, now_unix)
                        .await
                    {
                        Ok(events) => {
                            for event in events {
                                self.handle_recovery_event(event, now_unix).await;
                            }
                        }
                        Err(error) => {
                            self.report_legacy_turn_failure(
                                thread_id,
                                turn_id,
                                format!("failed to apply terminal provider recovery: {error:#}"),
                            )
                            .await;
                        }
                    }
                }
            }
            Err(error) => {
                self.report_legacy_turn_failure(
                    thread_id,
                    turn_id,
                    format!("failed to schedule provider recovery: {error:#}"),
                )
                .await;
            }
        }
    }

    pub(super) async fn handle_timeout_candidate(
        &self,
        candidate: TimeoutCandidate,
        now_unix: i64,
    ) {
        let mut active_recovery_job_id = None;
        let mut active_recovery_events = Vec::new();
        let recovery_job_outcome = match self
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
        for candidate in candidates {
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

    pub(super) async fn handle_recovery_event(
        &self,
        event: crate::resilience::RecoveryCoordinatorEvent,
        event_timestamp: i64,
    ) {
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
                .await;
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
                .await;
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
                .await;
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
                .await;
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
                .await;
            }
            crate::resilience::RecoveryCoordinatorEvent::RecoveryBlocked {
                job_id,
                turn_id,
                reason,
            } => {
                message_future(self.handle_recovery_blocked_event(job_id, turn_id, reason)).await;
            }
            crate::resilience::RecoveryCoordinatorEvent::RecoveryExhausted(outcome) => {
                message_future(self.handle_recovery_exhausted_event(outcome, event_timestamp))
                    .await;
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
    ) {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return;
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
            .await;
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
    ) {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return;
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
            .await;
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
    ) {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return;
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
            .await;
    }

    async fn handle_retry_attempt_started_event(
        &self,
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: pioneer_protocol::TurnItemType,
        attempt_number: u32,
        event_timestamp: i64,
    ) {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return;
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
            .await;
    }

    async fn handle_recovery_succeeded_event(
        &self,
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: pioneer_protocol::TurnItemType,
        attempt_number: u32,
        event_timestamp: i64,
    ) {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return;
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
            .await;
    }

    async fn handle_recovery_blocked_event(&self, job_id: String, turn_id: String, reason: String) {
        let Some((thread_id, _workspace_id)) = self
            .crud_store
            .get_turn_location(turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return;
        };
        let display_reason = self
            .recovery_blocked_display_reason(turn_id.as_str(), job_id.as_str(), reason.as_str())
            .await;
        let resume = self
            .build_recovery_blocked_resume_metadata(
                turn_id.as_str(),
                job_id.as_str(),
                display_reason.as_str(),
            )
            .await;
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
        }
    }

    async fn recovery_blocked_display_reason(
        &self,
        turn_id: &str,
        recovery_job_id: &str,
        fallback_reason: &str,
    ) -> String {
        if !fallback_reason
            .to_ascii_lowercase()
            .contains("durable turn runtime snapshot is missing")
        {
            return fallback_reason.to_owned();
        }

        let is_cli_runtime_turn = self
            .crud_store
            .get_cli_runtime_turn_binding(turn_id)
            .await
            .ok()
            .flatten()
            .is_some();
        if !is_cli_runtime_turn {
            return fallback_reason.to_owned();
        }

        self.crud_store
            .get_recovery_job(recovery_job_id)
            .await
            .ok()
            .flatten()
            .and_then(|job| job.reason)
            .filter(|reason| !reason.trim().is_empty())
            .unwrap_or_else(|| fallback_reason.to_owned())
    }

    async fn handle_recovery_exhausted_event(
        &self,
        outcome: RecoveryTerminalOutcome,
        event_timestamp: i64,
    ) {
        message_future(self.send_recovery_exhausted_notification(&outcome, event_timestamp)).await;
        message_future(self.handle_recovery_terminal_outcome(outcome, event_timestamp)).await;
    }

    pub(super) async fn send_recovery_exhausted_notification(
        &self,
        outcome: &RecoveryTerminalOutcome,
        event_timestamp: i64,
    ) {
        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(outcome.turn_id.as_str())
            .await
            .ok()
            .flatten()
        else {
            return;
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
            .await;
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
    ) {
        if let Err(error) = self
            .crud_store
            .materialize_item_recovery_opened(notification.clone(), event_timestamp)
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item recovery opened timeline event; skipping live notification"
            );
            return;
        }

        self.send_notification_to_thread_subscribers(
            notification.thread_id.as_str(),
            events::ITEM_RECOVERY_OPENED,
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

    async fn persist_and_send_item_recovery_attached(
        &self,
        notification: pioneer_protocol::ItemRecoveryAttachedNotification,
        event_timestamp: i64,
    ) {
        if let Err(error) = self
            .crud_store
            .materialize_item_recovery_attached(notification.clone(), event_timestamp)
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item recovery attached timeline event; skipping live notification"
            );
            return;
        }

        self.send_notification_to_thread_subscribers(
            notification.thread_id.as_str(),
            events::ITEM_RECOVERY_ATTACHED,
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

    async fn persist_and_send_item_retry_scheduled(
        &self,
        notification: pioneer_protocol::ItemRetryScheduledNotification,
        event_timestamp: i64,
    ) {
        if let Err(error) = self
            .crud_store
            .materialize_item_retry_scheduled(notification.clone(), event_timestamp)
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item retry scheduled timeline event; skipping live notification"
            );
            return;
        }

        self.send_notification_to_thread_subscribers(
            notification.thread_id.as_str(),
            events::ITEM_RETRY_SCHEDULED,
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

    async fn persist_and_send_item_retry_attempt_started(
        &self,
        notification: pioneer_protocol::ItemRetryAttemptStartedNotification,
        event_timestamp: i64,
    ) {
        if let Err(error) = self
            .crud_store
            .materialize_item_retry_attempt_started(notification.clone(), event_timestamp)
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item retry attempt started timeline event; skipping live notification"
            );
            return;
        }

        self.send_notification_to_thread_subscribers(
            notification.thread_id.as_str(),
            events::ITEM_RETRY_ATTEMPT_STARTED,
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

    async fn persist_and_send_item_recovery_succeeded(
        &self,
        notification: pioneer_protocol::ItemRecoverySucceededNotification,
        event_timestamp: i64,
    ) {
        if let Err(error) = self
            .crud_store
            .materialize_item_recovery_succeeded(notification.clone(), event_timestamp)
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item recovery succeeded timeline event; skipping live notification"
            );
            return;
        }

        self.send_notification_to_thread_subscribers(
            notification.thread_id.as_str(),
            events::ITEM_RECOVERY_SUCCEEDED,
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

    async fn persist_and_send_item_recovery_exhausted(
        &self,
        notification: pioneer_protocol::ItemRecoveryExhaustedNotification,
        event_timestamp: i64,
    ) {
        if let Err(error) = self
            .crud_store
            .materialize_item_recovery_exhausted(notification.clone(), event_timestamp)
            .await
        {
            warn!(
                thread_id = notification.thread_id.as_str(),
                turn_id = notification.turn_id.as_str(),
                item_id = notification.item_id.as_str(),
                recovery_job_id = notification.recovery_job_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist item recovery exhausted timeline event; skipping live notification"
            );
            return;
        }

        self.send_notification_to_thread_subscribers(
            notification.thread_id.as_str(),
            events::ITEM_RECOVERY_EXHAUSTED,
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

    pub(super) async fn complete_turn(
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
            .crud_store
            .materialize_turn_completed(turn_completed.clone(), event_timestamp)
            .await
        {
            self.thread_manager
                .rollback_turn_finish(finish_outcome.rollback_context)
                .await;

            self.report_legacy_turn_failure(
                thread_id,
                turn_id,
                format!("failed to persist turn/completed: {error:#}"),
            )
            .await;
            return false;
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
        self.clear_turn_llm_context_state(turn_id.as_str()).await;
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

        self.send_notification_to_connections(
            events::TURN_COMPLETED,
            &turn_completed,
            finish_outcome.connection_ids,
        )
        .await;
        self.notify_semantic_timeline_turn_state_changed(
            turn_completed.workspace_id.as_str(),
            turn_completed.thread_id.as_str(),
            turn_completed.turn.id.as_str(),
        )
        .await;
        self.notify_parent_timeline_changed_for_child_turn(
            turn_completed.thread_id.as_str(),
            turn_completed.turn.id.as_str(),
            Some(turn_completed.workspace_id.as_str()),
        )
        .await;

        if self
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id.as_str())
            .await
        {
            self.agent_manager.remove_thread(thread_id.as_str()).await;
        }
        true
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
    ) -> pioneer_protocol::TurnBlockedResumeMetadata {
        let latest_checkpoint_id = self
            .crud_store
            .latest_turn_execution_checkpoint_for_turn(turn_id)
            .await
            .ok()
            .flatten()
            .map(|checkpoint| checkpoint.id);
        let recovery_job = self
            .crud_store
            .get_recovery_job(recovery_job_id)
            .await
            .ok()
            .flatten();
        let has_runtime_snapshot = self
            .crud_store
            .get_turn_runtime_snapshot(turn_id)
            .await
            .ok()
            .flatten()
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

        pioneer_protocol::TurnBlockedResumeMetadata {
            reason_class: reason_class.to_owned(),
            human_message: reason.to_owned(),
            resume_requirements,
            resume_command: format!("turn.resume:{turn_id}"),
            blocked_recovery_job_id: Some(recovery_job_id.to_owned()),
            latest_checkpoint_id,
            can_resume_same_turn,
        }
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
        if let Err(error) = self
            .crud_store
            .materialize_turn_blocked(turn_blocked.clone(), event_timestamp)
            .await
        {
            self.thread_manager
                .rollback_turn_finish(finish_outcome.rollback_context)
                .await;

            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to persist turn/blocked event"
            );
            return false;
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

        self.send_notification_to_connections(
            events::TURN_BLOCKED,
            &turn_blocked,
            finish_outcome.connection_ids,
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

        if self
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id.as_str())
            .await
        {
            self.agent_manager.remove_thread(thread_id.as_str()).await;
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
        if let Err(error) = self
            .crud_store
            .materialize_turn_blocked(turn_blocked.clone(), event_timestamp)
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to persist unloaded turn/blocked event"
            );
            return false;
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

        self.send_notification_to_workspace_connections(
            workspace_id.as_str(),
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

        if let Err(error) = self
            .crud_store
            .delete_turn_llm_context_for_turn(turn_id)
            .await
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to delete turn_llm_context rows after turn block"
            );
        }
        self.clear_turn_llm_context_state(turn_id).await;
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
        self.mark_turn_interrupted_with_recovery(thread_id, turn_id, reason, None)
            .await
    }

    pub(super) async fn mark_turn_interrupted_with_recovery(
        &self,
        thread_id: String,
        turn_id: String,
        reason: String,
        recovery: Option<pioneer_protocol::RecoveryAttemptContext>,
    ) -> bool {
        if let Some((_workspace_id, current_turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        {
            if current_turn.status == TurnStatus::Interrupted {
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
            .crud_store
            .materialize_turn_failed(turn_failed.clone(), event_timestamp)
            .await
        {
            self.thread_manager
                .rollback_turn_finish(finish_outcome.rollback_context)
                .await;

            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to persist turn/interrupted event"
            );
            return false;
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

        if let Err(error) = self
            .task_agent_executor
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
        {
            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
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
        self.clear_turn_llm_context_state(turn_id.as_str()).await;
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

        self.send_notification_to_connections(
            events::TURN_FAILED,
            &turn_failed,
            finish_outcome.connection_ids,
        )
        .await;
        self.notify_semantic_timeline_turn_state_changed(
            turn_failed.workspace_id.as_str(),
            turn_failed.thread_id.as_str(),
            turn_failed.turn.id.as_str(),
        )
        .await;
        self.notify_parent_timeline_changed_for_child_turn(
            turn_failed.thread_id.as_str(),
            turn_failed.turn.id.as_str(),
            Some(turn_failed.workspace_id.as_str()),
        )
        .await;

        if self
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id.as_str())
            .await
        {
            self.agent_manager.remove_thread(thread_id.as_str()).await;
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
            .crud_store
            .materialize_turn_failed(turn_failed.clone(), event_timestamp)
            .await
        {
            self.thread_manager
                .rollback_turn_finish(finish_outcome.rollback_context)
                .await;

            warn!(
                thread_id,
                turn_id,
                error = %format!("{error:#}"),
                "failed to persist turn/failed event"
            );
            return false;
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
        self.clear_turn_llm_context_state(turn_id.as_str()).await;
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

        self.send_notification_to_connections(
            events::TURN_FAILED,
            &turn_failed,
            finish_outcome.connection_ids,
        )
        .await;
        self.notify_semantic_timeline_turn_state_changed(
            turn_failed.workspace_id.as_str(),
            turn_failed.thread_id.as_str(),
            turn_failed.turn.id.as_str(),
        )
        .await;
        self.notify_parent_timeline_changed_for_child_turn(
            turn_failed.thread_id.as_str(),
            turn_failed.turn.id.as_str(),
            Some(turn_failed.workspace_id.as_str()),
        )
        .await;

        if self
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id.as_str())
            .await
        {
            self.agent_manager.remove_thread(thread_id.as_str()).await;
        }
        true
    }

    pub(super) async fn emit_user_message_item_lifecycle(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        input: &[pioneer_protocol::UserInput],
        capabilities: &[pioneer_protocol::TurnCapability],
    ) {
        let item_id = user_message_item_id(turn_id);
        let payload = match self
            .user_message_payload_from_input_resolved(
                workspace_id,
                thread_id,
                turn_id,
                &item_id,
                input,
            )
            .await
        {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to materialize artifact-aware user message payload"
                );
                return;
            }
        };
        let capability_attachments = user_message_attachments_from_capabilities(capabilities);
        let (text, mut attachments) = payload.unwrap_or_default();
        attachments.extend(capability_attachments);

        if text.is_empty() && attachments.is_empty() {
            return;
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

        let started_timestamp = now_timestamp_secs();
        if let Err(error) = message_future(
            self.crud_store
                .materialize_item_started(started.clone(), started_timestamp),
        )
        .await
        {
            warn!(
                thread_id = thread_id,
                turn_id = turn_id,
                error = %format!("{error:#}"),
                "failed to persist user message item/started"
            );
        } else {
            self.send_notification_to_thread_subscribers(thread_id, events::ITEM_STARTED, &started)
                .await;
            self.notify_semantic_timeline_item_changed(
                started.workspace_id.as_str(),
                started.thread_id.as_str(),
                started.turn_id.as_str(),
                &started.item,
                Some("in_progress"),
            )
            .await;
        }

        let completed = pioneer_protocol::ItemCompletedNotification {
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item,
        };

        let completed_timestamp = now_timestamp_secs();
        if let Err(error) = message_future(
            self.crud_store
                .materialize_item_completed(completed.clone(), completed_timestamp),
        )
        .await
        {
            warn!(
                thread_id = thread_id,
                turn_id = turn_id,
                error = %format!("{error:#}"),
                "failed to persist user message item/completed"
            );
            return;
        }

        self.ingest_committed_thread_item(&completed).await;
        self.send_notification_to_thread_subscribers(thread_id, events::ITEM_COMPLETED, &completed)
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

        if thread.origin_kind != pioneer_protocol::ThreadOriginKind::User
            || self
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

fn user_message_attachments_from_capabilities(
    capabilities: &[pioneer_protocol::TurnCapability],
) -> Vec<pioneer_protocol::UserMessageAttachment> {
    capabilities
        .iter()
        .map(|capability| match &capability.kind {
            pioneer_protocol::TurnCapabilityKind::Skill { slug, source_kind } => {
                pioneer_protocol::UserMessageAttachment::Skill {
                    capability: pioneer_protocol::TurnSkillCapabilitySummary {
                        id: capability.id.clone(),
                        label: capability_label(capability.label.as_deref(), slug),
                        slug: slug.clone(),
                        source_kind: source_kind.clone(),
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
        .collect()
}

fn capability_label(label: Option<&str>, fallback: &str) -> String {
    label
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .unwrap_or(fallback)
        .to_owned()
}
