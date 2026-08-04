use super::events::ConversationEvent;
use pioneer_protocol::{ThreadMode, TurnKind, TurnStatus};

pub const DEFAULT_TURN_FAILED_ERROR: &str = "Turn failed";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnFlowState {
    Idle,
    Starting {
        turn_id: String,
        pending_request_id: String,
    },
    Running {
        turn_id: String,
    },
    Cancelling {
        turn_id: String,
    },
    Completing {
        turn_id: String,
    },
    Completed {
        turn_id: String,
    },
    Failed {
        turn_id: String,
        error: String,
    },
    Blocked {
        turn_id: String,
    },
    Cancelled {
        turn_id: String,
        error: Option<String>,
    },
}

impl Default for TurnFlowState {
    fn default() -> Self {
        Self::Idle
    }
}

#[derive(Debug, Clone, Default)]
pub struct TurnStateMachine {
    state: TurnFlowState,
}

impl TurnStateMachine {
    pub fn reset(&mut self) {
        self.state = TurnFlowState::Idle;
    }

    pub fn state(&self) -> &TurnFlowState {
        &self.state
    }

    pub fn can_start_new_turn(&self) -> bool {
        matches!(
            self.state,
            TurnFlowState::Idle
                | TurnFlowState::Completed { .. }
                | TurnFlowState::Failed { .. }
                | TurnFlowState::Blocked { .. }
                | TurnFlowState::Cancelled { .. }
        )
    }

    pub fn is_locked(&self) -> bool {
        !self.can_start_new_turn()
    }

    pub fn in_flight_turn_id(&self) -> Option<&str> {
        match &self.state {
            TurnFlowState::Starting { turn_id, .. }
            | TurnFlowState::Running { turn_id }
            | TurnFlowState::Cancelling { turn_id }
            | TurnFlowState::Completing { turn_id } => Some(turn_id.as_str()),
            TurnFlowState::Idle
            | TurnFlowState::Completed { .. }
            | TurnFlowState::Failed { .. }
            | TurnFlowState::Blocked { .. }
            | TurnFlowState::Cancelled { .. } => None,
        }
    }

    pub fn pending_request_id(&self) -> Option<&str> {
        match &self.state {
            TurnFlowState::Starting {
                pending_request_id, ..
            } => Some(pending_request_id.as_str()),
            TurnFlowState::Idle
            | TurnFlowState::Running { .. }
            | TurnFlowState::Cancelling { .. }
            | TurnFlowState::Completing { .. }
            | TurnFlowState::Completed { .. }
            | TurnFlowState::Failed { .. }
            | TurnFlowState::Blocked { .. }
            | TurnFlowState::Cancelled { .. } => None,
        }
    }

    pub fn status_label(&self) -> &'static str {
        match self.state {
            TurnFlowState::Idle => "idle",
            TurnFlowState::Starting { .. } => "starting",
            TurnFlowState::Running { .. } => "running",
            TurnFlowState::Cancelling { .. } => "cancelling",
            TurnFlowState::Completing { .. } => "completing",
            TurnFlowState::Completed { .. } => "completed",
            TurnFlowState::Failed { .. } => "failed",
            TurnFlowState::Blocked { .. } => "blocked",
            TurnFlowState::Cancelled { .. } => "cancelled",
        }
    }

    pub fn is_blocked_turn(&self, turn_id: &str) -> bool {
        matches!(
            &self.state,
            TurnFlowState::Blocked {
                turn_id: current_turn_id,
            } if current_turn_id == turn_id
        )
    }

    pub fn apply(&mut self, event: &ConversationEvent) {
        match event {
            ConversationEvent::LocalTurnStartRequested {
                turn_id,
                pending_request_id,
                mode,
                ..
            } if *mode != ThreadMode::Message => {
                self.state = TurnFlowState::Starting {
                    turn_id: turn_id.clone(),
                    pending_request_id: pending_request_id.clone(),
                };
            }
            ConversationEvent::LocalTurnStartAccepted {
                turn_id,
                pending_request_id,
                mode,
                ..
            } if *mode != ThreadMode::Message => {
                if self.matches_starting(turn_id.as_str(), pending_request_id.as_str()) {
                    self.state = TurnFlowState::Running {
                        turn_id: turn_id.clone(),
                    };
                }
            }
            ConversationEvent::LocalTurnStartRejected {
                turn_id,
                pending_request_id,
                mode,
                error,
                ..
            } if *mode != ThreadMode::Message => {
                if self.matches_starting(turn_id.as_str(), pending_request_id.as_str())
                    || self.can_start_new_turn()
                {
                    self.state = TurnFlowState::Failed {
                        turn_id: turn_id.clone(),
                        error: error.clone(),
                    };
                }
            }
            ConversationEvent::TurnStarted { turn, .. }
                if turn.turn_kind == TurnKind::Conversation && turn.mode != ThreadMode::Message =>
            {
                if self.matches_turn(turn.id.as_str()) || self.can_start_new_turn() {
                    self.state = TurnFlowState::Running {
                        turn_id: turn.id.clone(),
                    };
                }
            }
            ConversationEvent::LocalTurnCancelRequested { turn_id, .. } => {
                if self.matches_turn(turn_id.as_str()) {
                    self.state = TurnFlowState::Cancelling {
                        turn_id: turn_id.clone(),
                    };
                }
            }
            ConversationEvent::LocalTurnCancelRejected { turn_id, .. } => {
                if matches!(
                    &self.state,
                    TurnFlowState::Cancelling {
                        turn_id: current_turn_id,
                    } if current_turn_id == turn_id
                ) {
                    self.state = TurnFlowState::Running {
                        turn_id: turn_id.clone(),
                    };
                }
            }
            ConversationEvent::TurnCompleted { turn, .. }
                if turn.turn_kind == TurnKind::Conversation && turn.mode != ThreadMode::Message =>
            {
                if matches!(
                    &self.state,
                    TurnFlowState::Completed {
                        turn_id: current_turn_id,
                    } if current_turn_id == &turn.id
                ) || matches!(
                    &self.state,
                    TurnFlowState::Failed {
                        turn_id: current_turn_id,
                        ..
                    } if current_turn_id == &turn.id
                ) || matches!(
                    &self.state,
                    TurnFlowState::Blocked {
                        turn_id: current_turn_id,
                        ..
                    } if current_turn_id == &turn.id
                ) || matches!(
                    &self.state,
                    TurnFlowState::Cancelled {
                        turn_id: current_turn_id,
                        ..
                    } if current_turn_id == &turn.id
                ) {
                    return;
                }

                if self.matches_turn(turn.id.as_str()) || self.in_flight_turn_id().is_none() {
                    self.state = TurnFlowState::Completing {
                        turn_id: turn.id.clone(),
                    };
                }
            }
            ConversationEvent::TurnFailed { turn, .. }
                if turn.turn_kind == TurnKind::Conversation && turn.mode != ThreadMode::Message =>
            {
                if matches!(
                    &self.state,
                    TurnFlowState::Blocked {
                        turn_id: current_turn_id,
                        ..
                    } if current_turn_id == &turn.id
                ) {
                    return;
                }

                if self.matches_turn(turn.id.as_str()) || self.in_flight_turn_id().is_none() {
                    if turn.status == TurnStatus::Interrupted {
                        self.state = TurnFlowState::Cancelled {
                            turn_id: turn.id.clone(),
                            error: turn.error.clone(),
                        };
                    } else {
                        self.state = TurnFlowState::Failed {
                            turn_id: turn.id.clone(),
                            error: turn
                                .error
                                .clone()
                                .unwrap_or_else(|| DEFAULT_TURN_FAILED_ERROR.to_owned()),
                        };
                    }
                }
            }
            ConversationEvent::TurnBlocked { turn, .. }
                if turn.turn_kind == TurnKind::Conversation && turn.mode != ThreadMode::Message =>
            {
                if self.matches_turn(turn.id.as_str()) || self.in_flight_turn_id().is_none() {
                    self.state = TurnFlowState::Blocked {
                        turn_id: turn.id.clone(),
                    };
                }
            }
            ConversationEvent::ItemStarted { turn_id, .. }
            | ConversationEvent::ItemDelta { turn_id, .. }
            | ConversationEvent::ItemTimeoutDetected { turn_id, .. }
            | ConversationEvent::ItemRecoveryOpened { turn_id, .. }
            | ConversationEvent::ItemRecoveryAttached { turn_id, .. }
            | ConversationEvent::ItemRetryScheduled { turn_id, .. }
            | ConversationEvent::ItemRetryAttemptStarted { turn_id, .. }
            | ConversationEvent::ItemRecoverySucceeded { turn_id, .. }
            | ConversationEvent::ItemRecoveryExhausted { turn_id, .. }
            | ConversationEvent::ItemToolRetryScheduled { turn_id, .. }
            | ConversationEvent::ItemToolRetryResolved { turn_id, .. }
            | ConversationEvent::ItemToolRetryExhausted { turn_id, .. }
            | ConversationEvent::TurnToolLoopBudgetExceeded { turn_id, .. }
            | ConversationEvent::ItemCompleted { turn_id, .. }
            | ConversationEvent::ItemUpdated { turn_id, .. } => {
                if self.matches_turn(turn_id.as_str())
                    && matches!(self.state, TurnFlowState::Starting { .. })
                {
                    self.state = TurnFlowState::Running {
                        turn_id: turn_id.clone(),
                    };
                }
            }
            ConversationEvent::TurnExecutionWindowStarted { notification } => {
                if self.matches_turn(notification.turn_id.as_str())
                    && matches!(self.state, TurnFlowState::Starting { .. })
                {
                    self.state = TurnFlowState::Running {
                        turn_id: notification.turn_id.clone(),
                    };
                }
            }
            ConversationEvent::TurnExecutionWindowExhausted { notification } => {
                if self.matches_turn(notification.turn_id.as_str())
                    && matches!(self.state, TurnFlowState::Starting { .. })
                {
                    self.state = TurnFlowState::Running {
                        turn_id: notification.turn_id.clone(),
                    };
                }
            }
            ConversationEvent::TurnExecutionWindowCheckpointed { notification } => {
                if self.matches_turn(notification.turn_id.as_str())
                    && matches!(self.state, TurnFlowState::Starting { .. })
                {
                    self.state = TurnFlowState::Running {
                        turn_id: notification.turn_id.clone(),
                    };
                }
            }
            ConversationEvent::TurnExecutionWindowContinued { notification } => {
                if self.matches_turn(notification.turn_id.as_str())
                    && matches!(self.state, TurnFlowState::Starting { .. })
                {
                    self.state = TurnFlowState::Running {
                        turn_id: notification.turn_id.clone(),
                    };
                }
            }
            ConversationEvent::TurnExecutionWindowBlocked { notification } => {
                if self.matches_turn(notification.turn_id.as_str())
                    || self.in_flight_turn_id().is_none()
                {
                    self.state = TurnFlowState::Blocked {
                        turn_id: notification.turn_id.clone(),
                    };
                }
            }
            ConversationEvent::LocalTurnStartRequested { .. }
            | ConversationEvent::LocalTurnStartAccepted { .. }
            | ConversationEvent::LocalTurnStartRejected { .. }
            | ConversationEvent::TurnStarted { .. }
            | ConversationEvent::TurnCompleted { .. }
            | ConversationEvent::TurnFailed { .. }
            | ConversationEvent::TurnBlocked { .. }
            | ConversationEvent::TurnPermissionAudit { .. } => {}
        }
    }

    pub fn sync_snapshot_turn(&mut self, turn: &pioneer_protocol::Turn) {
        if turn.turn_kind != TurnKind::Conversation || turn.mode == ThreadMode::Message {
            return;
        }
        self.state = match turn.status {
            TurnStatus::InProgress => TurnFlowState::Running {
                turn_id: turn.id.clone(),
            },
            TurnStatus::Completed => TurnFlowState::Completed {
                turn_id: turn.id.clone(),
            },
            TurnStatus::Failed => TurnFlowState::Failed {
                turn_id: turn.id.clone(),
                error: turn
                    .error
                    .clone()
                    .unwrap_or_else(|| DEFAULT_TURN_FAILED_ERROR.to_owned()),
            },
            TurnStatus::Interrupted => TurnFlowState::Cancelled {
                turn_id: turn.id.clone(),
                error: turn.error.clone(),
            },
            TurnStatus::Blocked => TurnFlowState::Blocked {
                turn_id: turn.id.clone(),
            },
        };
    }

    pub fn finalize_completing_turn(&mut self, turn_id: &str) -> bool {
        match &self.state {
            TurnFlowState::Completing {
                turn_id: current_turn_id,
            } if current_turn_id == turn_id => {
                self.state = TurnFlowState::Completed {
                    turn_id: turn_id.to_owned(),
                };
                true
            }
            _ => false,
        }
    }

    fn matches_starting(&self, turn_id: &str, pending_request_id: &str) -> bool {
        matches!(
            &self.state,
            TurnFlowState::Starting {
                turn_id: current_turn_id,
                pending_request_id: current_pending_request_id,
            } if current_turn_id == turn_id && current_pending_request_id == pending_request_id
        )
    }

    fn matches_turn(&self, turn_id: &str) -> bool {
        match &self.state {
            TurnFlowState::Starting {
                turn_id: current_turn_id,
                ..
            }
            | TurnFlowState::Running {
                turn_id: current_turn_id,
            }
            | TurnFlowState::Cancelling {
                turn_id: current_turn_id,
            }
            | TurnFlowState::Completing {
                turn_id: current_turn_id,
            }
            | TurnFlowState::Completed {
                turn_id: current_turn_id,
            }
            | TurnFlowState::Failed {
                turn_id: current_turn_id,
                ..
            }
            | TurnFlowState::Blocked {
                turn_id: current_turn_id,
                ..
            }
            | TurnFlowState::Cancelled {
                turn_id: current_turn_id,
                ..
            } => current_turn_id == turn_id,
            TurnFlowState::Idle => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ExecutionWindowStatus, Turn, TurnExecutionWindowBlockedNotification, TurnKind, TurnOrigin,
    };

    #[test]
    fn conversation_state_machine_accepts_local_start_and_tracks_pending_request() {
        let mut machine = TurnStateMachine::default();

        machine.apply(&ConversationEvent::LocalTurnStartRequested {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            pending_request_id: "request_1".to_owned(),
            mode: ThreadMode::Agent,
            user_text: "hello".to_owned(),
            attachments: Vec::new(),
        });

        assert_eq!(machine.status_label(), "starting");
        assert_eq!(machine.pending_request_id(), Some("request_1"));
        assert!(machine.is_locked());

        machine.apply(&ConversationEvent::LocalTurnStartAccepted {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            pending_request_id: "request_1".to_owned(),
            mode: ThreadMode::Agent,
        });

        assert_eq!(machine.status_label(), "running");
        assert_eq!(machine.in_flight_turn_id(), Some("turn_1"));
    }

    #[test]
    fn conversation_state_machine_restores_running_when_cancel_is_rejected() {
        let mut machine = running_machine();

        machine.apply(&ConversationEvent::LocalTurnCancelRequested {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
        });
        assert_eq!(machine.status_label(), "cancelling");

        machine.apply(&ConversationEvent::LocalTurnCancelRejected {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            error: "not cancellable".to_owned(),
        });

        assert_eq!(machine.status_label(), "running");
    }

    #[test]
    fn conversation_state_machine_maps_interrupted_failed_turn_to_cancelled() {
        let mut machine = running_machine();

        machine.apply(&ConversationEvent::TurnFailed {
            thread_id: "thread_1".to_owned(),
            turn: turn(TurnStatus::Interrupted, Some("cancelled".to_owned())),
        });

        assert_eq!(
            machine.state(),
            &TurnFlowState::Cancelled {
                turn_id: "turn_1".to_owned(),
                error: Some("cancelled".to_owned()),
            }
        );
        assert!(machine.can_start_new_turn());
    }

    #[test]
    fn conversation_state_machine_blocks_on_execution_window_blocked() {
        let mut machine = running_machine();

        machine.apply(&ConversationEvent::TurnExecutionWindowBlocked {
            notification: TurnExecutionWindowBlockedNotification {
                workspace_id: "ws_1".to_owned(),
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                window_id: "window_1".to_owned(),
                window_index: 1,
                status: ExecutionWindowStatus::Blocked,
                exhaustion_reason: None,
                checkpoint_id: None,
                total_windows: 2,
                total_tool_calls: 10,
                reason: "needs input".to_owned(),
                blocked_at_unix_ms: 10,
            },
        });

        assert_eq!(
            machine.state(),
            &TurnFlowState::Blocked {
                turn_id: "turn_1".to_owned(),
            }
        );
        assert!(machine.is_blocked_turn("turn_1"));
        assert!(machine.can_start_new_turn());
    }

    fn running_machine() -> TurnStateMachine {
        let mut machine = TurnStateMachine::default();
        machine.apply(&ConversationEvent::TurnStarted {
            thread_id: "thread_1".to_owned(),
            turn: turn(TurnStatus::InProgress, None),
        });
        machine
    }

    fn turn(status: TurnStatus, error: Option<String>) -> Turn {
        Turn {
            id: "turn_1".to_owned(),
            status,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        }
    }
}
