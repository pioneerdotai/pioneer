use super::*;

impl Conversation {
    pub(in crate::app) fn hydrate_history(&mut self, events: &[ThreadHistoryEvent]) {
        self.reset();

        for event in events {
            self.apply_history_event(event);
        }

        self.pending_completion_turn_id = None;
        self.projector.sync_flow_state(&self.state_machine);
    }

    pub(in crate::app) fn apply_composed_turn_timeline(&mut self, timeline: &TurnTimelineResponse) {
        if timeline.thread_id != self.thread_id {
            return;
        }

        let mut changed = false;
        for item in &timeline.items {
            match &item.payload {
                TimelinePayload::TurnItemEvent { event }
                    if item.origin.kind == TimelineOriginKind::ChildTurn =>
                {
                    let item_count_before = self.projector.item_count();
                    self.apply_timeline_turn_item_event(
                        timeline.turn_id.as_str(),
                        &event.payload,
                        item.origin.occurred_at,
                    );
                    if item.origin.task_id.is_some() {
                        if let Some(item_id) = timeline_turn_item_payload_id(&event.payload) {
                            self.projector
                                .set_item_timeline_origin(item_id, item.origin.clone());
                        }
                        self.projector
                            .set_item_timeline_origin_from(item_count_before, item.origin.clone());
                    }
                    changed = true;
                }
                TimelinePayload::TaskEvent { event } => {
                    self.apply_timeline_task_event(timeline.turn_id.as_str(), &item.origin, event);
                    changed = true;
                }
                _ => {}
            }
        }

        if changed {
            self.projector.sync_flow_state(&self.state_machine);
            self.projector.bump_revision();
        }
    }

    fn apply_history_event(&mut self, event: &ThreadHistoryEvent) {
        if history_event_thread_id(&event.payload) != self.thread_id.as_str() {
            return;
        }

        match &event.payload {
            ThreadHistoryEventPayload::TurnStarted {
                thread_id, turn, ..
            } => {
                let conversation_event = ConversationEvent::TurnStarted {
                    thread_id: thread_id.clone(),
                    turn: turn.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_started(turn, event.created_at);
            }
            ThreadHistoryEventPayload::ItemStarted {
                thread_id,
                turn_id,
                item,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemStarted {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item: item.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.item_handlers.apply_started(
                    &mut self.projector,
                    turn_id.as_str(),
                    item,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemDelta {
                thread_id,
                turn_id,
                item_id,
                delta,
                stream,
                payload,
                markdown,
                markdown_version,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemDelta {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    delta: delta.clone(),
                    stream: *stream,
                    payload: payload.clone(),
                    markdown: markdown.clone(),
                    markdown_version: *markdown_version,
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.item_handlers.apply_delta(
                    &mut self.projector,
                    turn_id.as_str(),
                    item_id.as_str(),
                    delta.as_str(),
                    *stream,
                    payload.as_ref(),
                    markdown.as_ref(),
                    *markdown_version,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemCompleted {
                thread_id,
                turn_id,
                item,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemCompleted {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item: item.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.item_handlers.apply_completed(
                    &mut self.projector,
                    turn_id.as_str(),
                    item,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemUpdated {
                thread_id,
                turn_id,
                item,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemUpdated {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item: item.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.item_handlers.apply_completed(
                    &mut self.projector,
                    turn_id.as_str(),
                    item,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemTimeoutDetected {
                thread_id,
                turn_id,
                item_id,
                item_type,
                attempt_number,
                reason,
                recovery_job_id,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemTimeoutDetected {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    attempt_number: *attempt_number,
                    reason: *reason,
                    recovery_job_id: recovery_job_id.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_timeout_detected(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    *attempt_number,
                    *reason,
                    recovery_job_id.as_deref(),
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemRecoveryOpened {
                thread_id,
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemRecoveryOpened {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    recovery_job_id: recovery_job_id.clone(),
                    attempt_number: *attempt_number,
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_recovery_opened(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    recovery_job_id.as_str(),
                    *attempt_number,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemRecoveryAttached {
                thread_id,
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                recovery_item_id,
                recovery_item_type,
                existing_status,
                next_attempt_number,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemRecoveryAttached {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    recovery_job_id: recovery_job_id.clone(),
                    recovery_item_id: recovery_item_id.clone(),
                    recovery_item_type: *recovery_item_type,
                    existing_status: *existing_status,
                    next_attempt_number: *next_attempt_number,
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_recovery_attached(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    recovery_job_id.as_str(),
                    recovery_item_id.as_str(),
                    *recovery_item_type,
                    *existing_status,
                    *next_attempt_number,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemRetryScheduled {
                thread_id,
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                next_run_at_unix,
                reason,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemRetryScheduled {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    recovery_job_id: recovery_job_id.clone(),
                    attempt_number: *attempt_number,
                    next_run_at_unix: *next_run_at_unix,
                    reason: reason.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_retry_scheduled(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    recovery_job_id.as_str(),
                    *attempt_number,
                    *next_run_at_unix,
                    reason.as_deref(),
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemRetryAttemptStarted {
                thread_id,
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemRetryAttemptStarted {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    recovery_job_id: recovery_job_id.clone(),
                    attempt_number: *attempt_number,
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_retry_attempt_started(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    recovery_job_id.as_str(),
                    *attempt_number,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemRecoverySucceeded {
                thread_id,
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemRecoverySucceeded {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    recovery_job_id: recovery_job_id.clone(),
                    attempt_number: *attempt_number,
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_recovery_succeeded(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    recovery_job_id.as_str(),
                    *attempt_number,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemRecoveryExhausted {
                thread_id,
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                status,
                error_message,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemRecoveryExhausted {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    recovery_job_id: recovery_job_id.clone(),
                    attempt_number: *attempt_number,
                    status: *status,
                    error_message: error_message.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_recovery_exhausted(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    recovery_job_id.as_str(),
                    *attempt_number,
                    *status,
                    error_message.as_str(),
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemToolRetryScheduled {
                thread_id,
                turn_id,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                error_class,
                retry_hint,
                budgets,
                failure_signature_fingerprint,
                reason,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemToolRetryScheduled {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    tool_retry_episode_id: tool_retry_episode_id.clone(),
                    tool_name: tool_name.clone(),
                    attempt_number: *attempt_number,
                    error_class: *error_class,
                    retry_hint: retry_hint.clone(),
                    budgets: budgets.clone(),
                    failure_signature_fingerprint: failure_signature_fingerprint.clone(),
                    reason: reason.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_tool_retry_scheduled(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    tool_retry_episode_id.as_str(),
                    tool_name.as_str(),
                    *attempt_number,
                    *error_class,
                    retry_hint.as_str(),
                    budgets.as_slice(),
                    failure_signature_fingerprint.as_str(),
                    reason.as_str(),
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemToolRetryResolved {
                thread_id,
                turn_id,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                resolution,
                budgets,
                reason,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemToolRetryResolved {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    tool_retry_episode_id: tool_retry_episode_id.clone(),
                    tool_name: tool_name.clone(),
                    attempt_number: *attempt_number,
                    resolution: *resolution,
                    budgets: budgets.clone(),
                    reason: reason.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_tool_retry_resolved(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    tool_retry_episode_id.as_str(),
                    tool_name.as_str(),
                    *attempt_number,
                    *resolution,
                    budgets.as_slice(),
                    reason.as_str(),
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::ItemToolRetryExhausted {
                thread_id,
                turn_id,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                error_class,
                exhaustion_kind,
                budgets,
                failure_signature_fingerprint,
                reason,
                ..
            } => {
                let conversation_event = ConversationEvent::ItemToolRetryExhausted {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    item_id: item_id.clone(),
                    item_type: *item_type,
                    tool_retry_episode_id: tool_retry_episode_id.clone(),
                    tool_name: tool_name.clone(),
                    attempt_number: *attempt_number,
                    error_class: *error_class,
                    exhaustion_kind: *exhaustion_kind,
                    budgets: budgets.clone(),
                    failure_signature_fingerprint: failure_signature_fingerprint.clone(),
                    reason: reason.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_item_tool_retry_exhausted(
                    turn_id.as_str(),
                    item_id.as_str(),
                    *item_type,
                    tool_retry_episode_id.as_str(),
                    tool_name.as_str(),
                    *attempt_number,
                    *error_class,
                    *exhaustion_kind,
                    budgets.as_slice(),
                    failure_signature_fingerprint.as_str(),
                    reason.as_str(),
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::TurnToolLoopBudgetExceeded {
                thread_id,
                turn_id,
                limit_kind,
                limit,
                observed,
                action,
                reason,
                ..
            } => {
                let conversation_event = ConversationEvent::TurnToolLoopBudgetExceeded {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    limit_kind: *limit_kind,
                    limit: *limit,
                    observed: *observed,
                    action: *action,
                    reason: reason.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_tool_loop_budget_exceeded(
                    turn_id.as_str(),
                    *limit_kind,
                    *limit,
                    *observed,
                    *action,
                    reason.as_str(),
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::TurnCompleted {
                thread_id, turn, ..
            } => {
                let conversation_event = ConversationEvent::TurnCompleted {
                    thread_id: thread_id.clone(),
                    turn: turn.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_completed(turn, event.created_at);
                let _ = self
                    .state_machine
                    .finalize_completing_turn(turn.id.as_str());
                self.projector
                    .finalize_turn_completed(turn.id.as_str(), event.created_at);
            }
            ThreadHistoryEventPayload::TurnFailed {
                thread_id, turn, ..
            } => {
                let conversation_event = ConversationEvent::TurnFailed {
                    thread_id: thread_id.clone(),
                    turn: turn.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_failed(turn, event.created_at);
            }
        }
    }

    fn apply_timeline_turn_item_event(
        &mut self,
        parent_turn_id: &str,
        payload: &TurnItemEventPayload,
        ts_unix_ms: i64,
    ) {
        match payload {
            TurnItemEventPayload::ItemStarted {
                turn_id: _, item, ..
            } => {
                self.item_handlers.apply_started(
                    &mut self.projector,
                    parent_turn_id,
                    item,
                    ts_unix_ms,
                );
            }
            TurnItemEventPayload::ItemDelta {
                turn_id: _,
                item_id,
                delta,
                stream,
                payload,
                markdown,
                markdown_version,
                ..
            } => {
                self.item_handlers.apply_delta(
                    &mut self.projector,
                    parent_turn_id,
                    item_id.as_str(),
                    delta.as_str(),
                    *stream,
                    payload.as_ref(),
                    markdown.as_ref(),
                    *markdown_version,
                    ts_unix_ms,
                );
            }
            TurnItemEventPayload::ItemCompleted {
                turn_id: _, item, ..
            } => {
                self.item_handlers.apply_completed(
                    &mut self.projector,
                    parent_turn_id,
                    item,
                    ts_unix_ms,
                );
            }
            TurnItemEventPayload::ItemUpdated {
                turn_id: _, item, ..
            } => {
                self.item_handlers.apply_completed(
                    &mut self.projector,
                    parent_turn_id,
                    item,
                    ts_unix_ms,
                );
            }
            TurnItemEventPayload::ItemTimeoutDetected {
                turn_id: _,
                item_id,
                item_type,
                attempt_number,
                reason,
                recovery_job_id,
                ..
            } => self.projector.apply_item_timeout_detected(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                *attempt_number,
                *reason,
                recovery_job_id.as_deref(),
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemRecoveryOpened {
                turn_id: _,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => self.projector.apply_item_recovery_opened(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                recovery_job_id.as_str(),
                *attempt_number,
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemRecoveryAttached {
                turn_id: _,
                item_id,
                item_type,
                recovery_job_id,
                recovery_item_id,
                recovery_item_type,
                existing_status,
                next_attempt_number,
                ..
            } => self.projector.apply_item_recovery_attached(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                recovery_job_id.as_str(),
                recovery_item_id.as_str(),
                *recovery_item_type,
                *existing_status,
                *next_attempt_number,
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemRetryScheduled {
                turn_id: _,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                next_run_at_unix,
                reason,
                ..
            } => self.projector.apply_item_retry_scheduled(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                recovery_job_id.as_str(),
                *attempt_number,
                *next_run_at_unix,
                reason.as_deref(),
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemRetryAttemptStarted {
                turn_id: _,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => self.projector.apply_item_retry_attempt_started(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                recovery_job_id.as_str(),
                *attempt_number,
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemRecoverySucceeded {
                turn_id: _,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => self.projector.apply_item_recovery_succeeded(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                recovery_job_id.as_str(),
                *attempt_number,
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemRecoveryExhausted {
                turn_id: _,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                status,
                error_message,
                ..
            } => self.projector.apply_item_recovery_exhausted(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                recovery_job_id.as_str(),
                *attempt_number,
                *status,
                error_message.as_str(),
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemToolRetryScheduled {
                turn_id: _,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                error_class,
                retry_hint,
                budgets,
                failure_signature_fingerprint,
                reason,
                ..
            } => self.projector.apply_item_tool_retry_scheduled(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                tool_retry_episode_id.as_str(),
                tool_name.as_str(),
                *attempt_number,
                *error_class,
                retry_hint.as_str(),
                budgets.as_slice(),
                failure_signature_fingerprint.as_str(),
                reason.as_str(),
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemToolRetryResolved {
                turn_id: _,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                resolution,
                budgets,
                reason,
                ..
            } => self.projector.apply_item_tool_retry_resolved(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                tool_retry_episode_id.as_str(),
                tool_name.as_str(),
                *attempt_number,
                *resolution,
                budgets.as_slice(),
                reason.as_str(),
                ts_unix_ms,
            ),
            TurnItemEventPayload::ItemToolRetryExhausted {
                turn_id: _,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                error_class,
                exhaustion_kind,
                budgets,
                failure_signature_fingerprint,
                reason,
                ..
            } => self.projector.apply_item_tool_retry_exhausted(
                parent_turn_id,
                item_id.as_str(),
                *item_type,
                tool_retry_episode_id.as_str(),
                tool_name.as_str(),
                *attempt_number,
                *error_class,
                *exhaustion_kind,
                budgets.as_slice(),
                failure_signature_fingerprint.as_str(),
                reason.as_str(),
                ts_unix_ms,
            ),
            TurnItemEventPayload::TurnToolLoopBudgetExceeded {
                turn_id: _,
                limit_kind,
                limit,
                observed,
                action,
                reason,
                ..
            } => self.projector.apply_turn_tool_loop_budget_exceeded(
                parent_turn_id,
                *limit_kind,
                *limit,
                *observed,
                *action,
                reason.as_str(),
                ts_unix_ms,
            ),
        }
    }

    fn apply_timeline_task_event(
        &mut self,
        parent_turn_id: &str,
        origin: &pioneer_protocol::TimelineOrigin,
        event: &TaskEvent,
    ) {
        let grouped_task_id = origin.task_id.as_deref().unwrap_or(event.task_id.as_str());
        let message = task_event_message(event);
        let item = TurnItem::SystemEvent {
            id: format!("task_event_{}", event.id),
            level: task_event_level(event),
            message: message.clone(),
            code: Some(event.event_type.clone()),
            details: Some(serde_json::json!({
                "task_id": grouped_task_id.to_owned(),
                "source_task_id": event.task_id.clone(),
                "run_id": event.run_id.clone(),
                "event_type": event.event_type.clone(),
                "payload": event.payload.clone(),
            })),
        };
        let ts_unix_ms = task_event_timestamp_ms(event);
        self.item_handlers
            .apply_started(&mut self.projector, parent_turn_id, &item, ts_unix_ms);
        self.item_handlers
            .apply_completed(&mut self.projector, parent_turn_id, &item, ts_unix_ms);
        self.projector.set_item_timeline_origin(
            format!("task_event_{}", event.id).as_str(),
            task_event_timeline_origin(origin, event),
        );
    }

    fn apply_history_conversation_event(&mut self, event: ConversationEvent, created_at: i64) {
        self.push_event_log(&event, created_at);
        self.state_machine.apply(&event);
    }
}

fn timeline_turn_item_payload_id(payload: &TurnItemEventPayload) -> Option<&str> {
    match payload {
        TurnItemEventPayload::ItemStarted { item, .. }
        | TurnItemEventPayload::ItemCompleted { item, .. }
        | TurnItemEventPayload::ItemUpdated { item, .. } => Some(super::events::turn_item_id(item)),
        TurnItemEventPayload::ItemDelta { item_id, .. }
        | TurnItemEventPayload::ItemTimeoutDetected { item_id, .. }
        | TurnItemEventPayload::ItemRecoveryOpened { item_id, .. }
        | TurnItemEventPayload::ItemRecoveryAttached { item_id, .. }
        | TurnItemEventPayload::ItemRetryScheduled { item_id, .. }
        | TurnItemEventPayload::ItemRetryAttemptStarted { item_id, .. }
        | TurnItemEventPayload::ItemRecoverySucceeded { item_id, .. }
        | TurnItemEventPayload::ItemRecoveryExhausted { item_id, .. }
        | TurnItemEventPayload::ItemToolRetryScheduled { item_id, .. }
        | TurnItemEventPayload::ItemToolRetryResolved { item_id, .. }
        | TurnItemEventPayload::ItemToolRetryExhausted { item_id, .. } => Some(item_id.as_str()),
        TurnItemEventPayload::TurnToolLoopBudgetExceeded { .. } => None,
    }
}

fn task_event_timeline_origin(
    origin: &pioneer_protocol::TimelineOrigin,
    event: &TaskEvent,
) -> pioneer_protocol::TimelineOrigin {
    let mut timeline_origin = origin.clone();
    if timeline_origin.task_id.is_none() {
        timeline_origin.task_id = Some(event.task_id.clone());
    }
    if timeline_origin.run_id.is_none() {
        timeline_origin.run_id = event.run_id.clone();
    }
    if timeline_origin.child_thread_id.is_none() {
        timeline_origin.child_thread_id = event.thread_id.clone();
    }
    if timeline_origin.child_turn_id.is_none() {
        timeline_origin.child_turn_id = event.turn_id.clone();
    }
    timeline_origin
}

fn task_event_message(event: &TaskEvent) -> String {
    match &event.payload {
        pioneer_protocol::TaskEventPayload::Progress { message, .. } => message.clone(),
        pioneer_protocol::TaskEventPayload::TaskCreated { task } => {
            format!("Task created: {}", task.title)
        }
        pioneer_protocol::TaskEventPayload::RunCreated { run, .. } => {
            format!("Task run created: attempt {}", run.attempt_number)
        }
        pioneer_protocol::TaskEventPayload::RunRetryScheduled {
            retry_run,
            next_attempt_at,
            ..
        } => format!(
            "Task retry scheduled: attempt {} at {}",
            retry_run.attempt_number, next_attempt_at
        ),
        pioneer_protocol::TaskEventPayload::RunRetryExhausted { .. } => {
            "Task retries exhausted".to_owned()
        }
        pioneer_protocol::TaskEventPayload::ChildThreadLinked { .. } => {
            "Subagent thread linked".to_owned()
        }
        pioneer_protocol::TaskEventPayload::DepthLimitExceeded {
            depth, max_depth, ..
        } => format!("Task depth limit exceeded: {depth}/{max_depth}"),
        pioneer_protocol::TaskEventPayload::WriteLockAcquired { lock } => {
            format!("Write lock acquired: {}", lock.scope_path)
        }
        pioneer_protocol::TaskEventPayload::WriteLockReleased { lock, .. } => {
            format!("Write lock released: {}", lock.scope_path)
        }
        pioneer_protocol::TaskEventPayload::WriteLockBlocked { conflicts, .. } => {
            format!("Write lock blocked by {} active run(s)", conflicts.len())
        }
        pioneer_protocol::TaskEventPayload::WriteLockExpired { lock, .. } => {
            format!("Write lock expired: {}", lock.scope_path)
        }
        pioneer_protocol::TaskEventPayload::TaskRunThreadBindingCreated { binding } => {
            format!("Task execution thread linked: {}", binding.thread_id)
        }
        pioneer_protocol::TaskEventPayload::TaskRunTurnStarted { task_run_turn } => format!(
            "Task {} turn started: round {}",
            task_run_turn_kind_label(task_run_turn.kind),
            task_run_turn.round
        ),
        pioneer_protocol::TaskEventPayload::TaskRunTurnCompleted { task_run_turn } => format!(
            "Task {} turn completed: {}",
            task_run_turn_kind_label(task_run_turn.kind),
            task_run_turn_status_label(task_run_turn.status)
        ),
        pioneer_protocol::TaskEventPayload::TaskRunTurnFailed {
            task_run_turn,
            error,
        } => error
            .as_ref()
            .map(|error| {
                format!(
                    "Task {} turn failed: {}",
                    task_run_turn_kind_label(task_run_turn.kind),
                    error.message
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "Task {} turn failed",
                    task_run_turn_kind_label(task_run_turn.kind)
                )
            }),
        pioneer_protocol::TaskEventPayload::TaskResultCandidateCreated { candidate } => format!(
            "Task result candidate created: round {}, {}",
            candidate.round,
            task_result_candidate_status_label(candidate.status)
        ),
        pioneer_protocol::TaskEventPayload::TaskResultReviewEventRecorded { review_event } => {
            format!(
                "Review {} recorded: {} {}",
                task_review_event_kind_label(review_event.event_kind),
                task_result_reviewer_kind_label(review_event.reviewer_kind),
                task_review_decision_label(review_event.decision)
            )
        }
        pioneer_protocol::TaskEventPayload::TaskResultCandidateAccepted { candidate, .. } => {
            format!("Task result candidate accepted: {}", candidate.id)
        }
        pioneer_protocol::TaskEventPayload::TaskResultCandidateRejected { candidate, .. } => {
            format!("Task result candidate rejected: {}", candidate.id)
        }
        pioneer_protocol::TaskEventPayload::TaskResultCandidateCancelled { candidate, .. } => {
            format!("Task result candidate cancelled: {}", candidate.id)
        }
        pioneer_protocol::TaskEventPayload::TaskRevisionRequested {
            round,
            task_run_turn_id,
            ..
        } => format!("Task revision requested: round {round}, turn {task_run_turn_id}"),
        pioneer_protocol::TaskEventPayload::TaskRunEnteredReview { candidate_id, .. } => {
            format!("Task run waiting for review: candidate {candidate_id}")
        }
        _ => match event.event_type.as_str() {
            pioneer_protocol::constants::events::TASK_QUEUED => "Task queued".to_owned(),
            pioneer_protocol::constants::events::TASK_RUN_STARTED => "Task run started".to_owned(),
            pioneer_protocol::constants::events::TASK_RUN_COMPLETED => {
                "Task run completed".to_owned()
            }
            pioneer_protocol::constants::events::TASK_RUN_FAILED => "Task run failed".to_owned(),
            pioneer_protocol::constants::events::TASK_RUN_CANCELLED => {
                "Task run cancelled".to_owned()
            }
            pioneer_protocol::constants::events::TASK_COMPLETED => "Task completed".to_owned(),
            pioneer_protocol::constants::events::TASK_FAILED => "Task failed".to_owned(),
            pioneer_protocol::constants::events::TASK_CANCELLED => "Task cancelled".to_owned(),
            pioneer_protocol::constants::events::TASK_DETACHED => "Task detached".to_owned(),
            pioneer_protocol::constants::events::TASK_UPDATED => "Task updated".to_owned(),
            pioneer_protocol::constants::events::TASK_RESCHEDULED => "Task rescheduled".to_owned(),
            pioneer_protocol::constants::events::TASK_PAUSED => "Task paused".to_owned(),
            pioneer_protocol::constants::events::TASK_RESUMED => "Task resumed".to_owned(),
            pioneer_protocol::constants::events::TASK_RECOVERED => "Task recovered".to_owned(),
            pioneer_protocol::constants::events::TASK_DELIVERY_QUEUED => {
                "Task delivery queued".to_owned()
            }
            pioneer_protocol::constants::events::TASK_DELIVERY_DELIVERED => {
                "Task delivery delivered".to_owned()
            }
            pioneer_protocol::constants::events::TASK_DELIVERY_FAILED => {
                "Task delivery failed".to_owned()
            }
            _ => event.event_type.clone(),
        },
    }
}

fn task_event_level(event: &TaskEvent) -> SystemEventLevel {
    match &event.payload {
        pioneer_protocol::TaskEventPayload::TaskRunTurnFailed { .. }
        | pioneer_protocol::TaskEventPayload::TaskResultCandidateRejected { .. }
        | pioneer_protocol::TaskEventPayload::TaskResultCandidateCancelled { .. } => {
            return SystemEventLevel::Warning;
        }
        pioneer_protocol::TaskEventPayload::TaskResultReviewEventRecorded { review_event }
            if matches!(
                review_event.decision,
                pioneer_protocol::TaskResultReviewDecision::RequestChanges
                    | pioneer_protocol::TaskResultReviewDecision::Reject
                    | pioneer_protocol::TaskResultReviewDecision::Cancel
            ) =>
        {
            return SystemEventLevel::Warning;
        }
        _ => {}
    }

    match event.event_type.as_str() {
        pioneer_protocol::constants::events::TASK_FAILED
        | pioneer_protocol::constants::events::TASK_RUN_FAILED
        | pioneer_protocol::constants::events::TASK_RUN_RETRY_EXHAUSTED
        | pioneer_protocol::constants::events::TASK_WRITE_LOCK_BLOCKED
        | pioneer_protocol::constants::events::TASK_WRITE_LOCK_EXPIRED => SystemEventLevel::Warning,
        _ => SystemEventLevel::Info,
    }
}

fn task_run_turn_kind_label(kind: pioneer_protocol::TaskRunTurnKind) -> &'static str {
    match kind {
        pioneer_protocol::TaskRunTurnKind::Initial => "initial",
        pioneer_protocol::TaskRunTurnKind::Revision => "revision",
        pioneer_protocol::TaskRunTurnKind::Recovery => "recovery",
        pioneer_protocol::TaskRunTurnKind::Review => "review",
    }
}

fn task_run_turn_status_label(status: pioneer_protocol::TaskRunTurnStatus) -> &'static str {
    match status {
        pioneer_protocol::TaskRunTurnStatus::InProgress => "in progress",
        pioneer_protocol::TaskRunTurnStatus::CandidateCreated => "candidate created",
        pioneer_protocol::TaskRunTurnStatus::ReviewRecorded => "review recorded",
        pioneer_protocol::TaskRunTurnStatus::Failed => "failed",
        pioneer_protocol::TaskRunTurnStatus::Interrupted => "interrupted",
        pioneer_protocol::TaskRunTurnStatus::Cancelled => "cancelled",
    }
}

fn task_result_candidate_status_label(
    status: pioneer_protocol::TaskResultCandidateStatus,
) -> &'static str {
    match status {
        pioneer_protocol::TaskResultCandidateStatus::PendingReview => "pending review",
        pioneer_protocol::TaskResultCandidateStatus::Accepted => "accepted",
        pioneer_protocol::TaskResultCandidateStatus::Rejected => "rejected",
        pioneer_protocol::TaskResultCandidateStatus::Superseded => "superseded",
        pioneer_protocol::TaskResultCandidateStatus::Cancelled => "cancelled",
        pioneer_protocol::TaskResultCandidateStatus::ExtractionFailed => "extraction failed",
    }
}

fn task_result_reviewer_kind_label(kind: pioneer_protocol::TaskResultReviewerKind) -> &'static str {
    match kind {
        pioneer_protocol::TaskResultReviewerKind::RuntimeAuto => "runtime auto",
        pioneer_protocol::TaskResultReviewerKind::ParentAgent => "parent agent",
        pioneer_protocol::TaskResultReviewerKind::ReviewAgent => "review agent",
        pioneer_protocol::TaskResultReviewerKind::User => "user",
        pioneer_protocol::TaskResultReviewerKind::System => "system",
    }
}

fn task_review_event_kind_label(kind: pioneer_protocol::TaskResultReviewEventKind) -> &'static str {
    match kind {
        pioneer_protocol::TaskResultReviewEventKind::Advisory => "advisory",
        pioneer_protocol::TaskResultReviewEventKind::Decision => "decision",
        pioneer_protocol::TaskResultReviewEventKind::Override => "override",
        pioneer_protocol::TaskResultReviewEventKind::SystemAuto => "system auto",
    }
}

fn task_review_decision_label(
    decision: pioneer_protocol::TaskResultReviewDecision,
) -> &'static str {
    match decision {
        pioneer_protocol::TaskResultReviewDecision::Accept => "accepted",
        pioneer_protocol::TaskResultReviewDecision::RequestChanges => "requested changes",
        pioneer_protocol::TaskResultReviewDecision::Reject => "rejected",
        pioneer_protocol::TaskResultReviewDecision::Abstain => "abstained",
        pioneer_protocol::TaskResultReviewDecision::Cancel => "cancelled",
    }
}

fn task_event_timestamp_ms(event: &TaskEvent) -> i64 {
    if event.created_at > 1_000_000_000_000 {
        event.created_at
    } else {
        event.created_at.saturating_mul(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::{task_event_level, task_event_message};
    use pioneer_protocol::{
        SystemEventLevel, TaskEvent, TaskEventPayload, TaskResultCandidate,
        TaskResultCandidateStatus, TaskResultReviewDecision, TaskResultReviewEvent,
        TaskResultReviewEventKind, TaskResultReviewerKind, constants::events,
    };

    fn task_event(payload: TaskEventPayload) -> TaskEvent {
        TaskEvent {
            id: "event_review0000001".to_owned(),
            task_id: "task_review00000001".to_owned(),
            run_id: Some("run_review000000001".to_owned()),
            thread_id: Some("thread_child0000001".to_owned()),
            turn_id: Some("turn_child000000001".to_owned()),
            sequence: 1,
            event_type: payload.event_type().to_owned(),
            idempotency_key: None,
            payload,
            created_at: 20,
        }
    }

    fn candidate(status: TaskResultCandidateStatus) -> TaskResultCandidate {
        TaskResultCandidate {
            id: "candidate_review0001".to_owned(),
            task_id: "task_review00000001".to_owned(),
            run_id: "run_review000000001".to_owned(),
            task_run_turn_id: "run_turn_initial001".to_owned(),
            thread_id: "thread_child0000001".to_owned(),
            turn_id: "turn_child000000001".to_owned(),
            round: 0,
            status,
            result: None,
            extraction_error: None,
            summary: Some("child result".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: None,
            created_at: 20,
            updated_at: 20,
            resolved_at: None,
        }
    }

    fn review_event(
        event_kind: TaskResultReviewEventKind,
        decision: TaskResultReviewDecision,
    ) -> TaskResultReviewEvent {
        TaskResultReviewEvent {
            id: "review_event0000001".to_owned(),
            candidate_id: "candidate_review0001".to_owned(),
            task_id: "task_review00000001".to_owned(),
            run_id: "run_review000000001".to_owned(),
            task_run_turn_id: "run_turn_initial001".to_owned(),
            reviewer_kind: TaskResultReviewerKind::ReviewAgent,
            reviewer_thread_id: Some("thread_reviewer0001".to_owned()),
            reviewer_turn_id: Some("turn_reviewer00001".to_owned()),
            reviewer_user_id: None,
            reviewer_agent_spec_id: Some("agent_spec_review01".to_owned()),
            event_kind,
            decision,
            feedback_text: Some("tighten the result".to_owned()),
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: 21,
        }
    }

    #[test]
    fn phase_12_advisory_review_event_renders_as_review_history() {
        let event = task_event(TaskEventPayload::TaskResultReviewEventRecorded {
            review_event: review_event(
                TaskResultReviewEventKind::Advisory,
                TaskResultReviewDecision::RequestChanges,
            ),
        });

        assert_eq!(
            task_event_message(&event),
            "Review advisory recorded: review agent requested changes"
        );
        assert_eq!(task_event_level(&event), SystemEventLevel::Warning);
    }

    #[test]
    fn phase_12_candidate_and_revision_events_render_specific_history_labels() {
        let created = task_event(TaskEventPayload::TaskResultCandidateCreated {
            candidate: candidate(TaskResultCandidateStatus::PendingReview),
        });
        assert_eq!(
            task_event_message(&created),
            "Task result candidate created: round 0, pending review"
        );

        let revision = task_event(TaskEventPayload::TaskRevisionRequested {
            task_id: "task_review00000001".to_owned(),
            run_id: "run_review000000001".to_owned(),
            previous_candidate_id: "candidate_review0001".to_owned(),
            requested_by_review_event_id: "review_event0000001".to_owned(),
            task_run_turn_id: "run_turn_revision01".to_owned(),
            thread_id: "thread_child0000001".to_owned(),
            turn_id: "turn_child000000002".to_owned(),
            round: 1,
            feedback: "fix it".to_owned(),
            requested_at: 22,
        });
        assert_eq!(
            task_event_message(&revision),
            "Task revision requested: round 1, turn run_turn_revision01"
        );

        let entered = task_event(TaskEventPayload::TaskRunEnteredReview {
            task_id: "task_review00000001".to_owned(),
            run_id: "run_review000000001".to_owned(),
            candidate_id: "candidate_review0001".to_owned(),
            entered_at: 20,
        });
        assert_eq!(
            task_event_message(&entered),
            "Task run waiting for review: candidate candidate_review0001"
        );
        assert_eq!(entered.event_type, events::TASK_RUN_ENTERED_REVIEW);
    }
}
