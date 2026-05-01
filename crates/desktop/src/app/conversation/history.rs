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
                    if let Some(task_id) = item.origin.task_id.as_deref() {
                        let opaque_meta = task_timeline_meta(
                            task_id,
                            item.origin.run_id.as_deref(),
                            item.origin.child_thread_id.as_deref(),
                            item.origin.child_turn_id.as_deref(),
                        );
                        if let Some(item_id) = timeline_turn_item_payload_id(&event.payload) {
                            self.projector
                                .set_item_opaque_meta(item_id, opaque_meta.clone());
                        }
                        self.projector
                            .set_item_opaque_meta_from(item_count_before, opaque_meta);
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
        self.projector.set_item_opaque_meta(
            format!("task_event_{}", event.id).as_str(),
            task_timeline_meta(
                grouped_task_id,
                origin.run_id.as_deref().or(event.run_id.as_deref()),
                origin
                    .child_thread_id
                    .as_deref()
                    .or(event.thread_id.as_deref()),
                origin.child_turn_id.as_deref().or(event.turn_id.as_deref()),
            ),
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

fn task_timeline_meta(
    task_id: &str,
    run_id: Option<&str>,
    child_thread_id: Option<&str>,
    child_turn_id: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({
        "timeline_group": "task",
        "task_id": task_id,
        "run_id": run_id,
        "child_thread_id": child_thread_id,
        "child_turn_id": child_turn_id,
    })
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
    match event.event_type.as_str() {
        pioneer_protocol::constants::events::TASK_FAILED
        | pioneer_protocol::constants::events::TASK_RUN_FAILED
        | pioneer_protocol::constants::events::TASK_RUN_RETRY_EXHAUSTED
        | pioneer_protocol::constants::events::TASK_WRITE_LOCK_BLOCKED
        | pioneer_protocol::constants::events::TASK_WRITE_LOCK_EXPIRED => SystemEventLevel::Warning,
        _ => SystemEventLevel::Info,
    }
}

fn task_event_timestamp_ms(event: &TaskEvent) -> i64 {
    if event.created_at > 1_000_000_000_000 {
        event.created_at
    } else {
        event.created_at.saturating_mul(1000)
    }
}
