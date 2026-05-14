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
    task_id: String,
    run_id: String,
    child_thread_id: String,
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

fn now_db_timestamp() -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

fn llm_context_expires_at(
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    created_at + chrono::Duration::seconds(ACTIVE_TURN_LLM_CONTEXT_TTL_SECS)
}

fn durable_event_thread_id(event: &AgentDurableEvent) -> Option<&str> {
    match event {
        AgentDurableEvent::PromptManifestCompiled { thread_id, .. }
        | AgentDurableEvent::TurnSkillsResolved { thread_id, .. }
        | AgentDurableEvent::SkillAuditEvents { thread_id, .. }
        | AgentDurableEvent::TurnLlmContextAppended { thread_id, .. }
        | AgentDurableEvent::ProviderFailureDetected { thread_id, .. }
        | AgentDurableEvent::RecoveryAttemptSucceeded { thread_id, .. }
        | AgentDurableEvent::TurnCompleted { thread_id, .. }
        | AgentDurableEvent::TurnFailed { thread_id, .. }
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

impl MessageProcessor {
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

        let lineage =
            match message_future(self.crud_store.get_thread_lineage(child_thread_id)).await {
                Ok(Some(lineage)) => lineage,
                Ok(None) => return None,
                Err(error) => {
                    warn!(
                        child_thread_id,
                        child_turn_id,
                        error = %format!("{error:#}"),
                        "failed to load child thread lineage for parent timeline notification"
                    );
                    return None;
                }
            };
        if lineage.child_turn_id != child_turn_id {
            return None;
        }
        let parent_turn_id = lineage.parent_turn_id.clone()?;
        let workspace_id = if let Some(workspace_id) = workspace_id {
            workspace_id.to_owned()
        } else {
            match message_future(
                self.crud_store
                    .get_thread_model(lineage.parent_thread_id.as_str()),
            )
            .await
            {
                Ok(Some(thread)) => thread.workspace_id,
                Ok(None) => {
                    match message_future(self.crud_store.get_thread_model(child_thread_id)).await {
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
                        parent_thread_id = lineage.parent_thread_id,
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
            parent_thread_id: lineage.parent_thread_id,
            parent_turn_id,
            task_id: lineage.task_id,
            run_id: lineage.task_run_id,
            child_thread_id: lineage.child_thread_id,
            child_turn_id: lineage.child_turn_id,
        };
        self.parent_timeline_targets
            .lock()
            .await
            .insert(child_thread_id.to_owned(), target.clone());
        Some(target)
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

    pub(super) fn enrich_thread_history_markdown(
        events: &mut [pioneer_protocol::ThreadHistoryEvent],
    ) {
        let mut buffers: HashMap<String, String> = HashMap::new();

        for event in events {
            match &mut event.payload {
                pioneer_protocol::ThreadHistoryEventPayload::ItemStarted { item, .. } => {
                    let (item_id, text) = Self::normalize_item_markdown(item);
                    buffers.insert(item_id, text);
                }
                pioneer_protocol::ThreadHistoryEventPayload::ItemDelta {
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
                pioneer_protocol::ThreadHistoryEventPayload::ItemCompleted { item, .. }
                | pioneer_protocol::ThreadHistoryEventPayload::ItemUpdated { item, .. } => {
                    let (item_id, _) = Self::normalize_item_markdown(item);
                    buffers.remove(item_id.as_str());
                }
                _ => {}
            }
        }
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
        let mut lines = Vec::new();
        if let Some(value) = existing
            && !value.trim().is_empty()
        {
            lines.push(value);
        }
        lines.push(format!("recovery failed: {error_message}"));
        lines.join("\n")
    }

    pub(super) fn normalize_item_markdown(item: &mut TurnItem) -> (String, String) {
        match item {
            TurnItem::AgentMessage {
                id,
                text,
                markdown,
                markdown_version,
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

    pub(super) async fn handle_durable_agent_event(&self, event: AgentDurableEvent) {
        let thread_id = durable_event_thread_id(&event).map(str::to_owned);
        let committed = message_future(self.persist_durable_agent_event(event.clone())).await;
        if committed && let Some(thread_id) = thread_id {
            self.agent_manager
                .publish_committed(thread_id.as_str(), event)
                .await;
        }
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
                    self.mark_turn_failed(
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
                let item_type = notification.item.item_type();
                let deadlines = self
                    .timeout_supervisor
                    .deadlines_for(item_type, event_timestamp);
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
                    self.mark_turn_failed(
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
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                    TurnTimelineChangedReason::ChildTurnChanged,
                )
                .await;
                debug!(
                    thread_id = notification.thread_id,
                    turn_id = notification.turn_id,
                    item_id,
                    item_type = ?item_type,
                    "registered item attempt deadlines during item/started projection"
                );
            }
            AgentDurableEvent::ItemCompleted { notification } => {
                let mut notification = notification;
                self.enrich_item_completed_markdown(&mut notification).await;
                let thread_id = notification.thread_id.clone();
                let turn_id = notification.turn_id.clone();
                let event_timestamp = now_timestamp_secs();
                if let Err(error) = message_future(
                    self.crud_store
                        .materialize_item_completed(notification.clone(), event_timestamp),
                )
                .await
                {
                    self.mark_turn_failed(
                        thread_id,
                        turn_id,
                        format!("failed to persist item/completed: {error:#}"),
                    )
                    .await;
                    return false;
                }
                self.capture_artifacts_for_completed_item(&notification)
                    .await;
                self.send_notification_to_thread_subscribers(
                    notification.thread_id.as_str(),
                    events::ITEM_COMPLETED,
                    &notification,
                )
                .await;
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                    TurnTimelineChangedReason::ChildTurnChanged,
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
                    self.mark_turn_failed(
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
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                    TurnTimelineChangedReason::ChildTurnChanged,
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
                    self.mark_turn_failed(
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
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                    TurnTimelineChangedReason::ChildTurnChanged,
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
                    self.mark_turn_failed(
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
                self.notify_parent_timeline_changed_for_child_turn(
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                    Some(notification.workspace_id.as_str()),
                    TurnTimelineChangedReason::ChildTurnChanged,
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
                    self.mark_turn_failed(
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
                    TurnTimelineChangedReason::ChildTurnChanged,
                )
                .await;
            }
            AgentDurableEvent::TurnCompleted {
                thread_id,
                turn_id,
                recovery,
            } => {
                if !self.complete_turn(thread_id, turn_id, recovery).await {
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
                self.handle_provider_failure_detected(
                    thread_id, turn_id, item_id, item_type, failure, recovery,
                )
                .await;
            }
            AgentDurableEvent::RecoveryAttemptSucceeded {
                thread_id,
                turn_id,
                recovery,
            } => {
                self.handle_recovery_attempt_succeeded(thread_id, turn_id, recovery)
                    .await;
            }
            AgentDurableEvent::TurnFailed {
                thread_id,
                turn_id,
                error,
                recovery,
            } => {
                if !self
                    .mark_turn_failed_with_recovery(thread_id, turn_id, error, recovery)
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
                if !self
                    .mark_turn_interrupted_with_recovery(thread_id, turn_id, reason, recovery)
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
                    TurnTimelineChangedReason::ChildTurnChanged,
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
                self.mark_turn_failed(
                    thread_id,
                    turn_id,
                    format!("failed to mark recovery attempt succeeded: {error:#}"),
                )
                .await;
            }
        }
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
                    self.mark_turn_failed(
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
                let Some((_, workspace_id)) = self
                    .crud_store
                    .get_turn_location(turn_id.as_str())
                    .await
                    .ok()
                    .flatten()
                else {
                    return;
                };
                if is_created {
                    let opened = pioneer_protocol::ItemRecoveryOpenedNotification {
                        workspace_id,
                        thread_id: thread_id.clone(),
                        turn_id,
                        item_id,
                        item_type,
                        recovery_job_id: job.id,
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
                        turn_id,
                        item_id,
                        item_type,
                        recovery_job_id: job.id,
                        recovery_item_id: job.item_id,
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
            Err(error) => {
                self.mark_turn_failed(
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

        let mut should_fail_turn = item_snapshot.is_none();
        if let Some(item) = item_snapshot {
            if let Some(failed_item) =
                Self::force_fail_tool_item(item.clone(), outcome.error_message.as_str())
            {
                should_fail_turn = true;
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
                should_fail_turn = true;
            }
        }

        if should_fail_turn {
            let status_label = match outcome.status {
                pioneer_protocol::RecoveryJobStatus::Exhausted => "exhausted",
                pioneer_protocol::RecoveryJobStatus::Failed => "failed",
                pioneer_protocol::RecoveryJobStatus::Pending
                | pioneer_protocol::RecoveryJobStatus::Active
                | pioneer_protocol::RecoveryJobStatus::Succeeded
                | pioneer_protocol::RecoveryJobStatus::Cancelled => "terminal",
            };
            let turn_error = format!(
                "recovery {status_label} for item `{}`: {}",
                outcome.item_id, outcome.error_message
            );
            self.mark_turn_failed(thread_id, outcome.turn_id, turn_error)
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
            crate::resilience::RecoveryCoordinatorEvent::RetryScheduled {
                job_id,
                turn_id,
                item_id,
                item_type,
                attempt_number,
                next_run_at_unix,
                reason,
            } => {
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
            crate::resilience::RecoveryCoordinatorEvent::RetryAttemptStarted {
                job_id,
                turn_id,
                item_id,
                item_type,
                attempt_number,
            } => {
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
            crate::resilience::RecoveryCoordinatorEvent::RecoverySucceeded {
                job_id,
                turn_id,
                item_id,
                item_type,
                attempt_number,
            } => {
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
            crate::resilience::RecoveryCoordinatorEvent::RecoveryExhausted(outcome) => {
                self.send_recovery_exhausted_notification(&outcome, event_timestamp)
                    .await;
                self.handle_recovery_terminal_outcome(outcome, event_timestamp)
                    .await;
            }
        }
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
        reason: TurnTimelineChangedReason,
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

                let notification = TurnTimelineChangedNotification {
                    workspace_id: target.workspace_id,
                    thread_id: target.parent_thread_id,
                    turn_id: target.parent_turn_id,
                    task_id: Some(target.task_id),
                    run_id: Some(target.run_id),
                    child_thread_id: Some(target.child_thread_id),
                    child_turn_id: Some(target.child_turn_id),
                    reason,
                };
                processor
                    .send_notification_to_thread_subscribers(
                        notification.thread_id.as_str(),
                        events::TURN_TIMELINE_CHANGED,
                        &notification,
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

            self.mark_turn_failed(
                thread_id,
                turn_id,
                format!("failed to persist turn/completed: {error:#}"),
            )
            .await;
            return false;
        }

        self.finish_file_capture_session(
            turn_completed.workspace_id.as_str(),
            turn_completed.thread_id.as_str(),
            turn_completed.turn.id.as_str(),
        )
        .await;

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
        self.clear_turn_llm_context_state(turn_id.as_str()).await;

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
        self.notify_parent_timeline_changed_for_child_turn(
            turn_completed.thread_id.as_str(),
            turn_completed.turn.id.as_str(),
            Some(turn_completed.workspace_id.as_str()),
            TurnTimelineChangedReason::ChildTurnChanged,
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

    pub(super) async fn mark_turn_failed(
        &self,
        thread_id: String,
        turn_id: String,
        error_message: String,
    ) {
        self.mark_turn_failed_with_recovery(thread_id, turn_id, error_message, None)
            .await;
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

        self.finish_file_capture_session(
            turn_failed.workspace_id.as_str(),
            turn_failed.thread_id.as_str(),
            turn_failed.turn.id.as_str(),
        )
        .await;

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
        self.clear_turn_llm_context_state(turn_id.as_str()).await;

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
        self.notify_parent_timeline_changed_for_child_turn(
            turn_failed.thread_id.as_str(),
            turn_failed.turn.id.as_str(),
            Some(turn_failed.workspace_id.as_str()),
            TurnTimelineChangedReason::ChildTurnChanged,
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

        self.finish_file_capture_session(
            turn_failed.workspace_id.as_str(),
            turn_failed.thread_id.as_str(),
            turn_failed.turn.id.as_str(),
        )
        .await;

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
        self.clear_turn_llm_context_state(turn_id.as_str()).await;

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
        self.notify_parent_timeline_changed_for_child_turn(
            turn_failed.thread_id.as_str(),
            turn_failed.turn.id.as_str(),
            Some(turn_failed.workspace_id.as_str()),
            TurnTimelineChangedReason::ChildTurnChanged,
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
        let Some((text, attachments)) = payload else {
            return;
        };

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

        self.send_notification_to_thread_subscribers(thread_id, events::ITEM_COMPLETED, &completed)
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
                .get_thread_lineage(thread_id)
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
