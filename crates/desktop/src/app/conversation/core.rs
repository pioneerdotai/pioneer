use super::*;

impl Conversation {
    pub(in crate::app) fn new(thread_id: impl Into<String>) -> Self {
        let mut conversation = Self {
            thread_id: thread_id.into(),
            next_sequence: 0,
            event_log: VecDeque::new(),
            pending_completion_turn_id: None,
            projector: ConversationProjector::default(),
            state_machine: TurnStateMachine::default(),
            item_handlers: TurnItemHandlerRegistry::default(),
        };
        conversation.reset();
        conversation
    }

    pub(in crate::app) fn can_submit_message(&self) -> bool {
        self.state_machine.can_start_new_turn()
    }

    pub(in crate::app) fn projection(&self) -> &ConversationViewState {
        self.projector.view_state()
    }

    pub(in crate::app) fn in_flight_turn_id(&self) -> Option<&str> {
        self.state_machine.in_flight_turn_id()
    }

    pub(in crate::app) fn status_label(&self) -> &str {
        self.state_machine.status_label()
    }

    pub(in crate::app) fn tick(&mut self) -> bool {
        let Some(turn_id) = self.pending_completion_turn_id.take() else {
            return false;
        };

        let finalized = self
            .state_machine
            .finalize_completing_turn(turn_id.as_str());
        if finalized {
            self.projector
                .finalize_turn_completed(turn_id.as_str(), now_unix_ms());
            self.projector.sync_flow_state(&self.state_machine);
            self.projector.bump_revision();
        }
        finalized
    }

    pub(in crate::app) fn apply(&mut self, event: ConversationEvent) {
        if event.thread_id() != Some(self.thread_id.as_str()) {
            return;
        }

        let ts_unix_ms = now_unix_ms();
        self.push_event_log(&event, ts_unix_ms);
        self.state_machine.apply(&event);

        match event {
            ConversationEvent::LocalTurnStartRequested { turn_id, .. } => {
                self.pending_completion_turn_id = None;
                self.projector
                    .apply_local_turn_start_requested(turn_id.as_str(), ts_unix_ms);
            }
            ConversationEvent::LocalTurnStartAccepted { turn_id, .. } => {
                self.pending_completion_turn_id = None;
                self.projector
                    .apply_local_turn_start_accepted(turn_id.as_str(), ts_unix_ms);
            }
            ConversationEvent::LocalTurnStartRejected { turn_id, error, .. } => {
                self.pending_completion_turn_id = None;
                self.projector.apply_local_turn_start_rejected(
                    turn_id.as_str(),
                    error.as_str(),
                    ts_unix_ms,
                );
            }
            ConversationEvent::TurnStarted { turn, .. } => {
                self.pending_completion_turn_id = None;
                self.projector.apply_turn_started(&turn, ts_unix_ms);
            }
            ConversationEvent::TurnCompleted { turn, .. } => {
                if self.state_machine.in_flight_turn_id() == Some(turn.id.as_str()) {
                    self.projector.apply_turn_completed(&turn, ts_unix_ms);
                    self.pending_completion_turn_id = Some(turn.id);
                }
            }
            ConversationEvent::TurnFailed { turn, .. } => {
                self.pending_completion_turn_id = None;
                self.projector.apply_turn_failed(&turn, ts_unix_ms);
            }
            ConversationEvent::ItemStarted { turn_id, item, .. } => {
                self.item_handlers.apply_started(
                    &mut self.projector,
                    turn_id.as_str(),
                    &item,
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemDelta {
                turn_id,
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
                    turn_id.as_str(),
                    item_id.as_str(),
                    delta.as_str(),
                    stream,
                    payload.as_ref(),
                    markdown.as_ref(),
                    markdown_version,
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemTimeoutDetected {
                turn_id,
                item_id,
                item_type,
                attempt_number,
                reason,
                recovery_job_id,
                ..
            } => {
                self.projector.apply_item_timeout_detected(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    attempt_number,
                    reason,
                    recovery_job_id.as_deref(),
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemRecoveryOpened {
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => {
                self.projector.apply_item_recovery_opened(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    recovery_job_id.as_str(),
                    attempt_number,
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemRecoveryAttached {
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
                self.projector.apply_item_recovery_attached(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    recovery_job_id.as_str(),
                    recovery_item_id.as_str(),
                    recovery_item_type,
                    existing_status,
                    next_attempt_number,
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemRetryScheduled {
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                next_run_at_unix,
                reason,
                ..
            } => {
                self.projector.apply_item_retry_scheduled(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    recovery_job_id.as_str(),
                    attempt_number,
                    next_run_at_unix,
                    reason.as_deref(),
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemRetryAttemptStarted {
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => {
                self.projector.apply_item_retry_attempt_started(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    recovery_job_id.as_str(),
                    attempt_number,
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemRecoverySucceeded {
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                ..
            } => {
                self.projector.apply_item_recovery_succeeded(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    recovery_job_id.as_str(),
                    attempt_number,
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemRecoveryExhausted {
                turn_id,
                item_id,
                item_type,
                recovery_job_id,
                attempt_number,
                status,
                error_message,
                ..
            } => {
                self.projector.apply_item_recovery_exhausted(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    recovery_job_id.as_str(),
                    attempt_number,
                    status,
                    error_message.as_str(),
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemToolRetryScheduled {
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
                self.projector.apply_item_tool_retry_scheduled(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    tool_retry_episode_id.as_str(),
                    tool_name.as_str(),
                    attempt_number,
                    error_class,
                    retry_hint.as_str(),
                    budgets.as_slice(),
                    failure_signature_fingerprint.as_str(),
                    reason.as_str(),
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemToolRetryResolved {
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
                self.projector.apply_item_tool_retry_resolved(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    tool_retry_episode_id.as_str(),
                    tool_name.as_str(),
                    attempt_number,
                    resolution,
                    budgets.as_slice(),
                    reason.as_str(),
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemToolRetryExhausted {
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
                self.projector.apply_item_tool_retry_exhausted(
                    turn_id.as_str(),
                    item_id.as_str(),
                    item_type,
                    tool_retry_episode_id.as_str(),
                    tool_name.as_str(),
                    attempt_number,
                    error_class,
                    exhaustion_kind,
                    budgets.as_slice(),
                    failure_signature_fingerprint.as_str(),
                    reason.as_str(),
                    ts_unix_ms,
                );
            }
            ConversationEvent::TurnToolLoopBudgetExceeded {
                turn_id,
                limit_kind,
                limit,
                observed,
                action,
                reason,
                ..
            } => {
                self.projector.apply_turn_tool_loop_budget_exceeded(
                    turn_id.as_str(),
                    limit_kind,
                    limit,
                    observed,
                    action,
                    reason.as_str(),
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemCompleted { turn_id, item, .. } => {
                self.item_handlers.apply_completed(
                    &mut self.projector,
                    turn_id.as_str(),
                    &item,
                    ts_unix_ms,
                );
            }
            ConversationEvent::ItemUpdated { turn_id, item, .. } => {
                self.item_handlers.apply_completed(
                    &mut self.projector,
                    turn_id.as_str(),
                    &item,
                    ts_unix_ms,
                );
            }
        }

        self.projector.sync_flow_state(&self.state_machine);
        self.projector.bump_revision();
    }

    pub(in crate::app::conversation) fn reset(&mut self) {
        self.next_sequence = 0;
        self.event_log.clear();
        self.pending_completion_turn_id = None;
        self.projector.reset();
        self.state_machine.reset();
        self.projector.sync_flow_state(&self.state_machine);
    }

    pub(in crate::app::conversation) fn push_event_log(
        &mut self,
        event: &ConversationEvent,
        ts_unix_ms: i64,
    ) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.event_log.push_back(EventEnvelope {
            sequence: self.next_sequence,
            thread_id: self.thread_id.clone(),
            turn_id: event.turn_id().map(str::to_owned),
            item_id: event.item_id().map(str::to_owned),
            kind: event.kind(),
            payload: event.payload_value(),
            ts_unix_ms,
        });
        while self.event_log.len() > MAX_EVENT_LOG_LEN {
            let _ = self.event_log.pop_front();
        }
    }
}
