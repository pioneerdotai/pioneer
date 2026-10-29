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
    detached_composer: bool,
    admission_requests: std::collections::HashMap<String, String>,
    confirmed_admissions: std::collections::HashMap<String, TurnStatus>,
}

impl TurnStateMachine {
    pub(crate) fn set_composer_execution_mode(
        &mut self,
        mode: pioneer_protocol::ThreadComposerExecutionMode,
    ) {
        self.detached_composer =
            mode == pioneer_protocol::ThreadComposerExecutionMode::DetachedTask;
    }

    pub(crate) fn is_detached_admission(&self, turn: &pioneer_protocol::Turn) -> bool {
        self.detached_composer
            && turn.mode != ThreadMode::Message
            && pioneer_protocol::ThreadComposerExecutionMode::DetachedTask
                .is_detached_admission(turn)
    }

    pub fn reset(&mut self) {
        self.state = TurnFlowState::Idle;
        self.admission_requests.clear();
        self.confirmed_admissions.clear();
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
        if self.ignores_admission_response(event) {
            return;
        }
        if let ConversationEvent::LocalTurnStartRequested {
            turn_id,
            pending_request_id,
            mode,
            ..
        } = event
            && self.detached_composer
            && *mode != ThreadMode::Message
        {
            self.admission_requests
                .insert(turn_id.clone(), pending_request_id.clone());
        }
        // Waiting for admission is still Starting. Acceptance releases only
        // that submission; detached Task execution never locks the Composer.
        match event {
            ConversationEvent::LocalTurnStartAccepted { turn_id, mode, .. }
                if self.detached_composer && *mode != ThreadMode::Message =>
            {
                self.confirm_admission_outcome(turn_id, TurnStatus::Completed, None);
                return;
            }
            ConversationEvent::LocalTurnStartRejected {
                turn_id,
                error,
                mode,
                ..
            } if self.detached_composer && *mode != ThreadMode::Message => {
                self.state = TurnFlowState::Failed {
                    turn_id: turn_id.clone(),
                    error: error.clone(),
                };
                return;
            }
            ConversationEvent::TurnStarted { turn, .. }
            | ConversationEvent::TurnCompleted { turn, .. }
            | ConversationEvent::TurnFailed { turn, .. }
            | ConversationEvent::TurnBlocked { turn, .. }
                if self.is_detached_admission(turn) =>
            {
                self.apply_detached_admission(turn);
                return;
            }
            _ => {}
        }
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
                // Admission items confirm message delivery, not execution ownership.
                // Keep the exact pending request until its admission is acknowledged.
                if self.matches_turn(turn_id.as_str())
                    && !self.is_local_admission(turn_id)
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

    pub(crate) fn is_current_turn(&self, turn_id: &str) -> bool {
        self.matches_turn(turn_id)
    }

    pub(crate) fn is_local_admission(&self, turn_id: &str) -> bool {
        self.detached_composer && self.admission_requests.contains_key(turn_id)
    }

    pub(crate) fn is_known_admission(&self, turn_id: &str) -> bool {
        self.detached_composer
            && (self.admission_requests.contains_key(turn_id)
                || self.confirmed_admissions.contains_key(turn_id))
    }

    pub(crate) fn ignores_admission_response(&self, event: &ConversationEvent) -> bool {
        let (turn_id, request_id, mode, rejected) = match event {
            ConversationEvent::LocalTurnStartRejected {
                turn_id,
                pending_request_id,
                mode,
                ..
            } => (turn_id, pending_request_id, mode, true),
            ConversationEvent::LocalTurnStartAccepted {
                turn_id,
                pending_request_id,
                mode,
                ..
            } => (turn_id, pending_request_id, mode, false),
            _ => return false,
        };
        if !self.detached_composer || *mode == ThreadMode::Message {
            return false;
        }
        self.admission_requests.get(turn_id) != Some(request_id)
            || !self.admission_can_control_flow(turn_id)
            || (rejected && self.confirmed_admissions.contains_key(turn_id))
            || (!rejected
                && self
                    .confirmed_admissions
                    .get(turn_id)
                    .is_some_and(|status| {
                        matches!(
                            status,
                            TurnStatus::Failed | TurnStatus::Blocked | TurnStatus::Interrupted
                        )
                    }))
    }

    pub(crate) fn confirm_admission_outcome(
        &mut self,
        turn_id: &str,
        status: TurnStatus,
        error: Option<&str>,
    ) -> bool {
        if self.confirmed_admissions.get(turn_id).is_some_and(|known| {
            matches!(
                known,
                TurnStatus::Failed | TurnStatus::Blocked | TurnStatus::Interrupted
            )
        }) && matches!(status, TurnStatus::InProgress | TurnStatus::Completed)
        {
            return false;
        }
        self.confirmed_admissions.insert(turn_id.to_owned(), status);
        // Proof and metadata belong to this Turn; composer ownership also survives
        // a local submission's terminal transition (including an unconfirmed timeout).
        if !self.admission_can_control_flow(turn_id) {
            return true;
        }
        self.state = match status {
            TurnStatus::Failed => TurnFlowState::Failed {
                turn_id: turn_id.to_owned(),
                error: error.unwrap_or(DEFAULT_TURN_FAILED_ERROR).to_owned(),
            },
            TurnStatus::Blocked => TurnFlowState::Blocked {
                turn_id: turn_id.to_owned(),
            },
            TurnStatus::Interrupted => TurnFlowState::Cancelled {
                turn_id: turn_id.to_owned(),
                error: error.map(str::to_owned),
            },
            TurnStatus::InProgress | TurnStatus::Completed => TurnFlowState::Completed {
                turn_id: turn_id.to_owned(),
            },
        };
        true
    }

    fn admission_can_control_flow(&self, turn_id: &str) -> bool {
        if self.matches_turn(turn_id) {
            return true;
        }
        match self.current_turn_id() {
            None => true,
            Some(current) => {
                // A fresh client can restore successive historical admissions. A
                // locally requested Turn or an agent Conversation retains ownership,
                // even after it releases the composer and has no in-flight Turn.
                self.in_flight_turn_id().is_none()
                    && self.confirmed_admissions.contains_key(current)
                    && !self.admission_requests.contains_key(current)
            }
        }
    }

    fn apply_detached_admission(&mut self, turn: &pioneer_protocol::Turn) {
        self.confirm_admission_outcome(&turn.id, turn.status, turn.error.as_deref());
    }

    pub fn sync_snapshot_turn(&mut self, turn: &pioneer_protocol::Turn) {
        if self.is_detached_admission(turn) {
            self.apply_detached_admission(turn);
            return;
        }
        if turn.turn_kind != TurnKind::Conversation || turn.mode == ThreadMode::Message {
            return;
        }
        if turn.status == TurnStatus::InProgress
            && matches!(&self.state, TurnFlowState::Cancelling { turn_id } if turn_id == &turn.id)
        {
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
        self.current_turn_id() == Some(turn_id)
    }

    pub(crate) fn current_turn_id(&self) -> Option<&str> {
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
            } => Some(current_turn_id),
            TurnFlowState::Idle => None,
        }
    }
}
