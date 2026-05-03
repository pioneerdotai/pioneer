use super::events::ConversationEvent;
use pioneer_protocol::TurnStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TurnFlowState {
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
pub(super) struct TurnStateMachine {
    state: TurnFlowState,
}

impl TurnStateMachine {
    pub(super) fn reset(&mut self) {
        self.state = TurnFlowState::Idle;
    }

    pub(super) fn can_start_new_turn(&self) -> bool {
        matches!(
            self.state,
            TurnFlowState::Idle
                | TurnFlowState::Completed { .. }
                | TurnFlowState::Failed { .. }
                | TurnFlowState::Cancelled { .. }
        )
    }

    pub(super) fn is_locked(&self) -> bool {
        !self.can_start_new_turn()
    }

    pub(super) fn in_flight_turn_id(&self) -> Option<&str> {
        match &self.state {
            TurnFlowState::Starting { turn_id, .. }
            | TurnFlowState::Running { turn_id }
            | TurnFlowState::Cancelling { turn_id }
            | TurnFlowState::Completing { turn_id } => Some(turn_id.as_str()),
            TurnFlowState::Idle
            | TurnFlowState::Completed { .. }
            | TurnFlowState::Failed { .. }
            | TurnFlowState::Cancelled { .. } => None,
        }
    }

    pub(super) fn pending_request_id(&self) -> Option<&str> {
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
            | TurnFlowState::Cancelled { .. } => None,
        }
    }

    pub(super) fn status_label(&self) -> &'static str {
        match self.state {
            TurnFlowState::Idle => "idle",
            TurnFlowState::Starting { .. } => "starting",
            TurnFlowState::Running { .. } => "running",
            TurnFlowState::Cancelling { .. } => "cancelling",
            TurnFlowState::Completing { .. } => "completing",
            TurnFlowState::Completed { .. } => "completed",
            TurnFlowState::Failed { .. } => "failed",
            TurnFlowState::Cancelled { .. } => "cancelled",
        }
    }

    pub(super) fn apply(&mut self, event: &ConversationEvent) {
        match event {
            ConversationEvent::LocalTurnStartRequested {
                turn_id,
                pending_request_id,
                ..
            } => {
                self.state = TurnFlowState::Starting {
                    turn_id: turn_id.clone(),
                    pending_request_id: pending_request_id.clone(),
                };
            }
            ConversationEvent::LocalTurnStartAccepted {
                turn_id,
                pending_request_id,
                ..
            } => {
                if self.matches_starting(turn_id.as_str(), pending_request_id.as_str()) {
                    self.state = TurnFlowState::Running {
                        turn_id: turn_id.clone(),
                    };
                }
            }
            ConversationEvent::LocalTurnStartRejected {
                turn_id,
                pending_request_id,
                error,
                ..
            } => {
                if self.matches_starting(turn_id.as_str(), pending_request_id.as_str())
                    || self.can_start_new_turn()
                {
                    self.state = TurnFlowState::Failed {
                        turn_id: turn_id.clone(),
                        error: error.clone(),
                    };
                }
            }
            ConversationEvent::TurnStarted { turn, .. } => {
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
            ConversationEvent::TurnCompleted { turn, .. } => {
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
            ConversationEvent::TurnFailed { turn, .. } => {
                if self.matches_turn(turn.id.as_str()) || self.in_flight_turn_id().is_none() {
                    if turn.status == TurnStatus::Interrupted {
                        self.state = TurnFlowState::Cancelled {
                            turn_id: turn.id.clone(),
                            error: turn.error.clone(),
                        };
                    } else {
                        self.state = TurnFlowState::Failed {
                            turn_id: turn.id.clone(),
                            error: turn.error.clone().unwrap_or_else(|| {
                                t!("conversation.error.turn_failed").to_string()
                            }),
                        };
                    }
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
        }
    }

    pub(super) fn finalize_completing_turn(&mut self, turn_id: &str) -> bool {
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
            | TurnFlowState::Cancelled {
                turn_id: current_turn_id,
                ..
            } => current_turn_id == turn_id,
            TurnFlowState::Idle => false,
        }
    }
}
