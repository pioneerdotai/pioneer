use super::*;

impl Conversation {
    pub fn hydrate_history(&mut self, events: &[ThreadHistoryEvent]) {
        self.reset();

        for event in events {
            self.apply_history_event(event);
        }

        self.pending_completion_turn_id = None;
        self.projector.sync_flow_state(&self.state_machine);
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
            ThreadHistoryEventPayload::TurnExecutionWindowStarted(notification) => {
                let conversation_event = ConversationEvent::TurnExecutionWindowStarted {
                    notification: notification.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_execution_window_started(
                    notification.turn_id.as_str(),
                    notification,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::TurnExecutionWindowExhausted(notification) => {
                let conversation_event = ConversationEvent::TurnExecutionWindowExhausted {
                    notification: notification.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_execution_window_exhausted(
                    notification.turn_id.as_str(),
                    notification,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::TurnExecutionWindowCheckpointed(notification) => {
                let conversation_event = ConversationEvent::TurnExecutionWindowCheckpointed {
                    notification: notification.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_execution_window_checkpointed(
                    notification.turn_id.as_str(),
                    notification,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::TurnExecutionWindowContinued(notification) => {
                let conversation_event = ConversationEvent::TurnExecutionWindowContinued {
                    notification: notification.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_execution_window_continued(
                    notification.turn_id.as_str(),
                    notification,
                    event.created_at,
                );
            }
            ThreadHistoryEventPayload::TurnExecutionWindowBlocked(notification) => {
                let conversation_event = ConversationEvent::TurnExecutionWindowBlocked {
                    notification: notification.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector.apply_turn_execution_window_blocked(
                    notification.turn_id.as_str(),
                    notification,
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
            ThreadHistoryEventPayload::TurnBlocked {
                thread_id,
                turn,
                resume,
                ..
            } => {
                let conversation_event = ConversationEvent::TurnBlocked {
                    thread_id: thread_id.clone(),
                    turn: turn.clone(),
                    resume: resume.clone(),
                };
                self.apply_history_conversation_event(conversation_event, event.created_at);
                self.projector
                    .apply_turn_blocked(turn, resume.as_ref(), event.created_at);
            }
        }
    }

    fn apply_history_conversation_event(&mut self, event: ConversationEvent, created_at: i64) {
        self.push_event_log(&event, created_at);
        self.state_machine.apply(&event);
    }
}

#[cfg(test)]
mod tests {
    use super::super::events::EventKind;
    use super::super::reducer::TurnPhase;
    use super::super::{Conversation, ConversationEvent};
    use pioneer_protocol::{
        ExecutionWindowExhaustionReason, ExecutionWindowStatus, ThreadHistoryEvent,
        ThreadHistoryEventPayload, Turn, TurnExecutionWindowBlockedNotification,
        TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
        TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification, TurnItem,
        TurnStatus,
    };

    const WORKSPACE_ID: &str = "ws_000000000000000001";
    const THREAD_ID: &str = "thr_000000000000000001";
    const TURN_ID: &str = "turn_000000000000000001";

    fn turn(id: &str, status: TurnStatus, error: Option<&str>) -> Turn {
        Turn {
            id: id.to_owned(),
            status,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: error.map(str::to_owned),
            prompt_manifest: None,
        }
    }

    fn history_event(
        turn_id: &str,
        sequence: i64,
        payload: ThreadHistoryEventPayload,
    ) -> ThreadHistoryEvent {
        ThreadHistoryEvent {
            turn_id: turn_id.to_owned(),
            sequence,
            created_at: 1_000 + sequence,
            payload,
        }
    }

    fn turn_started_event(turn_id: &str, sequence: i64) -> ThreadHistoryEvent {
        history_event(
            turn_id,
            sequence,
            ThreadHistoryEventPayload::TurnStarted {
                workspace_id: WORKSPACE_ID.to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: turn(turn_id, TurnStatus::InProgress, None),
                input: Vec::new(),
            },
        )
    }

    fn window_started_notification(
        window_id: &str,
        window_index: u32,
    ) -> TurnExecutionWindowStartedNotification {
        TurnExecutionWindowStartedNotification {
            workspace_id: WORKSPACE_ID.to_owned(),
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            window_id: window_id.to_owned(),
            window_index,
            status: ExecutionWindowStatus::Running,
            started_at_unix_ms: 1_000,
        }
    }

    fn window_exhausted_notification(
        window_id: &str,
        window_index: u32,
    ) -> TurnExecutionWindowExhaustedNotification {
        TurnExecutionWindowExhaustedNotification {
            workspace_id: WORKSPACE_ID.to_owned(),
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            window_id: window_id.to_owned(),
            window_index,
            status: ExecutionWindowStatus::Exhausted,
            exhaustion_reason: ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
            limit: 128,
            observed: 129,
            agent_round_count: 64,
            tool_call_count: 129,
            provider_token_count: Some(42_000),
            started_at_unix_ms: 1_000,
            exhausted_at_unix_ms: 2_000,
            reason: "max_tool_calls_per_window".to_owned(),
        }
    }

    fn window_checkpointed_notification(
        window_id: &str,
        window_index: u32,
    ) -> TurnExecutionWindowCheckpointedNotification {
        TurnExecutionWindowCheckpointedNotification {
            workspace_id: WORKSPACE_ID.to_owned(),
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            window_id: window_id.to_owned(),
            window_index,
            status: ExecutionWindowStatus::Checkpointed,
            checkpoint_id: "chk_000000000000000001".to_owned(),
            checkpoint_kind: "execution_window_budget".to_owned(),
            payload_bytes: 4096,
            created_at_unix_ms: 2_100,
        }
    }

    fn window_continued_notification(
        window_id: &str,
        window_index: u32,
    ) -> TurnExecutionWindowContinuedNotification {
        TurnExecutionWindowContinuedNotification {
            workspace_id: WORKSPACE_ID.to_owned(),
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            window_id: window_id.to_owned(),
            window_index,
            status: ExecutionWindowStatus::Continued,
            previous_window_id: "win_000000000000000001".to_owned(),
            previous_window_index: 1,
            checkpoint_id: "chk_000000000000000001".to_owned(),
            continued_at_unix_ms: 2_200,
        }
    }

    fn window_blocked_notification() -> TurnExecutionWindowBlockedNotification {
        TurnExecutionWindowBlockedNotification {
            workspace_id: WORKSPACE_ID.to_owned(),
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            window_id: "win_000000000000000003".to_owned(),
            window_index: 3,
            status: ExecutionWindowStatus::Blocked,
            exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow),
            checkpoint_id: Some("chk_000000000000000003".to_owned()),
            total_windows: 3,
            total_tool_calls: 384,
            reason: "max_total_windows_exceeded".to_owned(),
            blocked_at_unix_ms: 3_000,
        }
    }

    #[test]
    fn hydrate_history_replays_completed_failed_and_cancelled_turns() {
        for (turn_id, status, error, expected_status, expected_phase) in [
            (
                "turn_completed",
                TurnStatus::Completed,
                None,
                "completed",
                TurnPhase::Completed,
            ),
            (
                "turn_failed",
                TurnStatus::Failed,
                Some("provider failed"),
                "failed",
                TurnPhase::Failed,
            ),
            (
                "turn_cancelled",
                TurnStatus::Interrupted,
                Some("cancelled by user"),
                "cancelled",
                TurnPhase::Cancelled,
            ),
        ] {
            let terminal_payload = match status {
                TurnStatus::Completed => ThreadHistoryEventPayload::TurnCompleted {
                    workspace_id: WORKSPACE_ID.to_owned(),
                    thread_id: THREAD_ID.to_owned(),
                    turn: turn(turn_id, status, error),
                },
                TurnStatus::Failed | TurnStatus::Interrupted => {
                    ThreadHistoryEventPayload::TurnFailed {
                        workspace_id: WORKSPACE_ID.to_owned(),
                        thread_id: THREAD_ID.to_owned(),
                        turn: turn(turn_id, status, error),
                    }
                }
                _ => unreachable!("test only covers terminal replay states"),
            };
            let mut conversation = Conversation::new(THREAD_ID);
            conversation.hydrate_history(&[
                turn_started_event(turn_id, 1),
                history_event(turn_id, 2, terminal_payload),
            ]);

            assert_eq!(conversation.status_label(), expected_status);
            assert!(conversation.can_submit_message());
            let projected_turn = conversation
                .projection()
                .turns
                .iter()
                .find(|turn| turn.id == turn_id)
                .expect("terminal turn should project");
            assert_eq!(projected_turn.phase, expected_phase);
        }
    }

    #[test]
    fn hydrate_history_replays_execution_window_events_and_keeps_log_order() {
        let mut live = Conversation::new(THREAD_ID);
        live.apply(ConversationEvent::TurnStarted {
            thread_id: THREAD_ID.to_owned(),
            turn: turn(TURN_ID, TurnStatus::InProgress, None),
        });
        live.apply(ConversationEvent::TurnExecutionWindowStarted {
            notification: window_started_notification("win_000000000000000001", 1),
        });
        live.apply(ConversationEvent::TurnExecutionWindowExhausted {
            notification: window_exhausted_notification("win_000000000000000001", 1),
        });
        live.apply(ConversationEvent::TurnExecutionWindowCheckpointed {
            notification: window_checkpointed_notification("win_000000000000000001", 1),
        });
        live.apply(ConversationEvent::TurnExecutionWindowContinued {
            notification: window_continued_notification("win_000000000000000002", 2),
        });
        live.apply(ConversationEvent::TurnExecutionWindowBlocked {
            notification: window_blocked_notification(),
        });

        let mut replay = Conversation::new(THREAD_ID);
        replay.hydrate_history(&[
            turn_started_event(TURN_ID, 1),
            history_event(
                TURN_ID,
                2,
                ThreadHistoryEventPayload::TurnExecutionWindowStarted(
                    window_started_notification("win_000000000000000001", 1),
                ),
            ),
            history_event(
                TURN_ID,
                3,
                ThreadHistoryEventPayload::TurnExecutionWindowExhausted(
                    window_exhausted_notification("win_000000000000000001", 1),
                ),
            ),
            history_event(
                TURN_ID,
                4,
                ThreadHistoryEventPayload::TurnExecutionWindowCheckpointed(
                    window_checkpointed_notification("win_000000000000000001", 1),
                ),
            ),
            history_event(
                TURN_ID,
                5,
                ThreadHistoryEventPayload::TurnExecutionWindowContinued(
                    window_continued_notification("win_000000000000000002", 2),
                ),
            ),
            history_event(
                TURN_ID,
                6,
                ThreadHistoryEventPayload::TurnExecutionWindowBlocked(window_blocked_notification()),
            ),
        ]);

        let live_kinds = live
            .event_log
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>();
        let replay_kinds = replay
            .event_log
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>();
        assert_eq!(live_kinds, replay_kinds);
        assert_eq!(
            replay_kinds,
            vec![
                EventKind::TurnStarted,
                EventKind::TurnExecutionWindowStarted,
                EventKind::TurnExecutionWindowExhausted,
                EventKind::TurnExecutionWindowCheckpointed,
                EventKind::TurnExecutionWindowContinued,
                EventKind::TurnExecutionWindowBlocked,
            ]
        );
        assert_eq!(replay.status_label(), "blocked");
        assert!(replay.projection().items.iter().any(|item| {
            matches!(
                &item.item,
                TurnItem::SystemEvent {
                    code: Some(code),
                    ..
                } if code == "turn_execution_window_blocked"
            )
        }));
    }
}
