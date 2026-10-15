//! Turn resume coordination.

use crate::{conversation::ConversationEvent, threads::coordinator::ThreadCoordinator};
use anyhow::{Context as _, ensure};
use pioneer_protocol::{
    Turn, TurnGetParams, TurnGetResponse, TurnItemEventPayload, TurnItemsParams, TurnItemsResponse,
    TurnStatus,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

pub const TURN_RESUME_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(800);
pub const TURN_RESUME_RETRY_MAX_DELAY: Duration = Duration::from_millis(5_000);
pub const TURN_RESUME_IN_PROGRESS_POLL_DELAY: Duration = Duration::from_millis(800);
pub const TURN_RESUME_MISMATCH_RETRY_DELAY: Duration = Duration::from_secs(5);
pub const TURN_RESUME_ITEMS_PAGE_LIMIT: u32 = 200;

#[derive(Clone, Default, PartialEq, Eq)]
pub struct ThreadResumeCoordinator {
    pub in_progress: bool,
    pub retry_attempt: u32,
    pub next_attempt_at: Option<Instant>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnResumeQueueConnectionPlan {
    NotReady,
    Drive { connection_id: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnResumeQueueItemPlan {
    Skip,
    ResetMissingTurn,
    Resume,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScheduledTurnResumePlan {
    Skip,
    ResetMissingTurn,
    Resume,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnResumeSchedulePlan {
    pub retry_attempt: u32,
    pub next_attempt_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnResumeRetryPlan {
    pub attempt: u32,
    pub delay: Duration,
    pub next_attempt_at: Option<Instant>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnResumeStatusPlan {
    PollAfter(Duration),
    Complete,
    Fail,
    Block,
    Reset,
}

#[derive(Clone, Debug)]
pub enum TurnResumeSnapshotReduction {
    ScopeMismatch {
        expected_thread_id: String,
        actual_thread_id: String,
        expected_turn_id: String,
        actual_turn_id: String,
        retry_after: Duration,
    },
    Apply(TurnResumeSnapshotApplyReduction),
}

#[derive(Clone, Debug)]
pub struct TurnResumeItemsPageReduction {
    pub thread_id: String,
    pub workspace_id: String,
    pub replay_events: Vec<ConversationEvent>,
    pub next_cursor: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct TurnResumeSnapshotApplyReduction {
    pub thread_id: String,
    pub workspace_id: String,
    pub replay_events: Vec<ConversationEvent>,
    pub terminal_event: Option<ConversationEvent>,
    pub schedule_after: Option<Duration>,
    pub reset_thread_resume: bool,
    pub tick_conversation_after_terminal_event: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnResumeSnapshotFailurePlan {
    Retry { thread_id: String },
}

pub fn reset_thread_resume_coordinator(resume: &mut ThreadResumeCoordinator) {
    resume.in_progress = false;
    resume.retry_attempt = 0;
    resume.next_attempt_at = None;
}

pub fn begin_turn_resume_attempt(resume: &mut ThreadResumeCoordinator) {
    resume.in_progress = true;
    resume.next_attempt_at = None;
}

pub fn finish_turn_resume_attempt(resume: &mut ThreadResumeCoordinator) {
    resume.in_progress = false;
}

pub fn enqueue_turn_resume_thread(
    ready_threads: &mut VecDeque<String>,
    ready_thread_set: &mut HashSet<String>,
    thread_id: String,
) -> bool {
    if !ready_thread_set.insert(thread_id.clone()) {
        return false;
    }
    ready_threads.push_back(thread_id);
    true
}

pub fn dequeue_turn_resume_thread(
    ready_threads: &mut VecDeque<String>,
    ready_thread_set: &mut HashSet<String>,
) -> Option<String> {
    let thread_id = ready_threads.pop_front()?;
    ready_thread_set.remove(thread_id.as_str());
    Some(thread_id)
}

pub fn clear_turn_resume_queue(
    ready_threads: &mut VecDeque<String>,
    ready_thread_set: &mut HashSet<String>,
) {
    ready_threads.clear();
    ready_thread_set.clear();
}

pub fn plan_turn_resume_queue_connection(
    connection_id: Option<u64>,
    gateway_connected: bool,
) -> TurnResumeQueueConnectionPlan {
    if !gateway_connected {
        return TurnResumeQueueConnectionPlan::NotReady;
    }

    match connection_id {
        Some(connection_id) => TurnResumeQueueConnectionPlan::Drive { connection_id },
        None => TurnResumeQueueConnectionPlan::NotReady,
    }
}

pub fn plan_turn_resume_queue_item(
    resume_in_progress: bool,
    has_in_flight_turn: bool,
) -> TurnResumeQueueItemPlan {
    if resume_in_progress {
        return TurnResumeQueueItemPlan::Skip;
    }

    if !has_in_flight_turn {
        return TurnResumeQueueItemPlan::ResetMissingTurn;
    }

    TurnResumeQueueItemPlan::Resume
}

pub fn thread_ids_with_in_flight_turns(
    coordinators: &HashMap<String, ThreadCoordinator>,
) -> Vec<String> {
    coordinators
        .iter()
        .filter_map(|(thread_id, coordinator)| {
            coordinator
                .conversation
                .in_flight_turn_id()
                .is_some()
                .then_some(thread_id.to_owned())
        })
        .collect()
}

pub fn schedule_turn_resume_after_state(
    resume: &mut ThreadResumeCoordinator,
    delay: Duration,
    now: Instant,
) -> TurnResumeSchedulePlan {
    let next_attempt_at = now + delay;
    resume.next_attempt_at = Some(next_attempt_at);

    TurnResumeSchedulePlan {
        retry_attempt: resume.retry_attempt,
        next_attempt_at,
    }
}

pub fn should_fire_scheduled_turn_resume(
    resume: &ThreadResumeCoordinator,
    expected_retry_attempt: u32,
) -> bool {
    resume.retry_attempt == expected_retry_attempt && !resume.in_progress
}

pub fn plan_scheduled_turn_resume(
    resume: &ThreadResumeCoordinator,
    expected_retry_attempt: u32,
    has_in_flight_turn: bool,
) -> ScheduledTurnResumePlan {
    if !should_fire_scheduled_turn_resume(resume, expected_retry_attempt) {
        return ScheduledTurnResumePlan::Skip;
    }
    if !has_in_flight_turn {
        return ScheduledTurnResumePlan::ResetMissingTurn;
    }
    ScheduledTurnResumePlan::Resume
}

pub fn apply_turn_resume_retry(
    resume: Option<&mut ThreadResumeCoordinator>,
    now: Instant,
) -> TurnResumeRetryPlan {
    let Some(resume) = resume else {
        let delay = turn_resume_retry_delay(0);
        return TurnResumeRetryPlan {
            attempt: 1,
            delay,
            next_attempt_at: None,
        };
    };

    let delay = turn_resume_retry_delay(resume.retry_attempt);
    let attempt = resume.retry_attempt.saturating_add(1);
    let next_attempt_at = now + delay;

    resume.retry_attempt = attempt;
    resume.next_attempt_at = Some(next_attempt_at);

    TurnResumeRetryPlan {
        attempt,
        delay,
        next_attempt_at: Some(next_attempt_at),
    }
}

pub fn turn_resume_retry_delay(attempt: u32) -> Duration {
    let multiplier = 1u64 << attempt.min(8);
    let delay_ms = (TURN_RESUME_RETRY_INITIAL_DELAY.as_millis() as u64).saturating_mul(multiplier);
    Duration::from_millis(delay_ms.min(TURN_RESUME_RETRY_MAX_DELAY.as_millis() as u64))
}

pub fn turn_snapshot_matches_scope(
    expected_thread_id: &str,
    expected_turn_id: &str,
    snapshot: &TurnGetResponse,
) -> bool {
    expected_thread_id == snapshot.thread_id && expected_turn_id == snapshot.turn.id
}

pub fn turn_resume_turn_params(thread_id: String, turn_id: String) -> TurnGetParams {
    TurnGetParams { thread_id, turn_id }
}

pub fn turn_resume_items_page_params(
    thread_id: String,
    turn_id: String,
    after_sequence: Option<i64>,
) -> TurnItemsParams {
    TurnItemsParams {
        thread_id,
        turn_id,
        after_sequence,
        limit: Some(TURN_RESUME_ITEMS_PAGE_LIMIT),
    }
}

pub fn reduce_turn_resume_turn_snapshot(
    expected_thread_id: &str,
    expected_turn_id: &str,
    turn_snapshot: TurnGetResponse,
) -> TurnResumeSnapshotReduction {
    if !turn_snapshot_matches_scope(expected_thread_id, expected_turn_id, &turn_snapshot) {
        return TurnResumeSnapshotReduction::ScopeMismatch {
            expected_thread_id: expected_thread_id.to_owned(),
            actual_thread_id: turn_snapshot.thread_id,
            expected_turn_id: expected_turn_id.to_owned(),
            actual_turn_id: turn_snapshot.turn.id,
            retry_after: TURN_RESUME_MISMATCH_RETRY_DELAY,
        };
    }

    let thread_id = turn_snapshot.thread_id.clone();
    let workspace_id = turn_snapshot.workspace_id.clone();

    let status_plan = plan_turn_resume_after_status(turn_snapshot.turn.status.clone());
    let (
        terminal_event,
        schedule_after,
        reset_thread_resume,
        tick_conversation_after_terminal_event,
    ) = match status_plan {
        TurnResumeStatusPlan::PollAfter(delay) => (None, Some(delay), false, false),
        TurnResumeStatusPlan::Complete => (
            turn_resume_terminal_event(thread_id.clone(), turn_snapshot.turn),
            None,
            true,
            true,
        ),
        TurnResumeStatusPlan::Fail | TurnResumeStatusPlan::Block => (
            turn_resume_terminal_event(thread_id.clone(), turn_snapshot.turn),
            None,
            true,
            false,
        ),
        TurnResumeStatusPlan::Reset => (None, None, true, false),
    };

    TurnResumeSnapshotReduction::Apply(TurnResumeSnapshotApplyReduction {
        thread_id,
        workspace_id,
        replay_events: Vec::new(),
        terminal_event,
        schedule_after,
        reset_thread_resume,
        tick_conversation_after_terminal_event,
    })
}

/// Validates and reduces one bounded replay page. Scope and cursor checks live
/// in the shared client so every shell fails closed on malformed Gateway data.
pub fn reduce_turn_resume_items_page(
    turn_snapshot: &TurnGetResponse,
    requested_after_sequence: Option<i64>,
    item_snapshot: TurnItemsResponse,
) -> anyhow::Result<TurnResumeItemsPageReduction> {
    ensure!(
        item_snapshot.thread_id == turn_snapshot.thread_id,
        "turn/items/page returned another thread"
    );
    ensure!(
        item_snapshot.workspace_id == turn_snapshot.workspace_id,
        "turn/items/page returned another workspace"
    );
    ensure!(
        item_snapshot.turn_id == turn_snapshot.turn.id,
        "turn/items/page returned another turn"
    );
    ensure!(
        item_snapshot.events.len() <= TURN_RESUME_ITEMS_PAGE_LIMIT as usize,
        "turn/items/page exceeded the client item budget"
    );

    let requested_cursor = requested_after_sequence.unwrap_or(0);
    ensure!(
        item_snapshot.last_sequence >= requested_cursor,
        "turn/items/page cursor moved backwards"
    );
    match (item_snapshot.has_more, item_snapshot.next_cursor) {
        (true, Some(next_cursor)) => {
            ensure!(
                next_cursor == item_snapshot.last_sequence && next_cursor > requested_cursor,
                "turn/items/page returned a non-advancing cursor"
            );
        }
        (false, None) => {}
        _ => ensure!(
            false,
            "turn/items/page returned an inconsistent continuation"
        ),
    }

    let mut previous_sequence = requested_cursor;
    let mut replay_events = Vec::with_capacity(item_snapshot.events.len());
    for event in item_snapshot.events {
        ensure!(
            event.sequence >= previous_sequence && event.sequence <= item_snapshot.last_sequence,
            "turn/items/page event sequence is outside the page cursor"
        );
        previous_sequence = event.sequence;
        replay_events.push(
            turn_item_payload_to_conversation_event(
                turn_snapshot.workspace_id.as_str(),
                turn_snapshot.thread_id.as_str(),
                turn_snapshot.turn.id.as_str(),
                event.payload,
            )
            .context("turn/items/page contains an event outside the requested turn")?,
        );
    }

    Ok(TurnResumeItemsPageReduction {
        thread_id: item_snapshot.thread_id,
        workspace_id: item_snapshot.workspace_id,
        replay_events,
        next_cursor: item_snapshot.next_cursor,
    })
}

pub fn plan_turn_resume_snapshot_failure(thread_id: &str) -> TurnResumeSnapshotFailurePlan {
    TurnResumeSnapshotFailurePlan::Retry {
        thread_id: thread_id.to_owned(),
    }
}

pub fn turn_item_event_matches_snapshot(
    event_workspace_id: &str,
    event_thread_id: &str,
    snapshot_workspace_id: &str,
    snapshot_thread_id: &str,
) -> bool {
    event_thread_id == snapshot_thread_id && event_workspace_id == snapshot_workspace_id
}

pub fn turn_item_payload_to_conversation_event(
    snapshot_workspace_id: &str,
    snapshot_thread_id: &str,
    snapshot_turn_id: &str,
    payload: TurnItemEventPayload,
) -> Option<ConversationEvent> {
    let (event_workspace_id, event_thread_id, event_turn_id) = turn_item_payload_scope(&payload);
    if !turn_item_event_matches_snapshot(
        event_workspace_id,
        event_thread_id,
        snapshot_workspace_id,
        snapshot_thread_id,
    ) || event_turn_id != snapshot_turn_id
    {
        return None;
    }

    match payload {
        TurnItemEventPayload::ItemStarted {
            workspace_id,
            thread_id,
            turn_id,
            item,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemStarted {
            thread_id,
            turn_id,
            item,
        }),
        TurnItemEventPayload::ItemDelta {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            delta,
            stream,
            payload,
            markdown,
            markdown_version,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemDelta {
            thread_id,
            turn_id,
            item_id,
            delta,
            stream,
            payload,
            markdown,
            markdown_version,
        }),
        TurnItemEventPayload::ItemCompleted {
            workspace_id,
            thread_id,
            turn_id,
            item,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemCompleted {
            thread_id,
            turn_id,
            item,
        }),
        TurnItemEventPayload::ItemUpdated {
            workspace_id,
            thread_id,
            turn_id,
            item,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemUpdated {
            thread_id,
            turn_id,
            item,
        }),
        TurnItemEventPayload::ItemTimeoutDetected {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
            attempt_number,
            reason,
            recovery_job_id,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemTimeoutDetected {
            thread_id,
            turn_id,
            item_id,
            item_type,
            attempt_number,
            reason,
            recovery_job_id,
        }),
        TurnItemEventPayload::ItemRecoveryOpened {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
            ..
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemRecoveryOpened {
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
        }),
        TurnItemEventPayload::ItemRecoveryAttached {
            workspace_id,
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
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemRecoveryAttached {
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            recovery_item_id,
            recovery_item_type,
            existing_status,
            next_attempt_number,
        }),
        TurnItemEventPayload::ItemRetryScheduled {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
            next_run_at_unix,
            reason,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemRetryScheduled {
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
            next_run_at_unix,
            reason,
        }),
        TurnItemEventPayload::ItemRetryAttemptStarted {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemRetryAttemptStarted {
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
        }),
        TurnItemEventPayload::ItemRecoverySucceeded {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemRecoverySucceeded {
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
        }),
        TurnItemEventPayload::ItemRecoveryExhausted {
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
            status,
            error_message,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemRecoveryExhausted {
            thread_id,
            turn_id,
            item_id,
            item_type,
            recovery_job_id,
            attempt_number,
            status,
            error_message,
        }),
        TurnItemEventPayload::ItemToolRetryScheduled {
            workspace_id,
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
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemToolRetryScheduled {
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
        }),
        TurnItemEventPayload::ItemToolRetryResolved {
            workspace_id,
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
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemToolRetryResolved {
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
        }),
        TurnItemEventPayload::ItemToolRetryExhausted {
            workspace_id,
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
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::ItemToolRetryExhausted {
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
        }),
        TurnItemEventPayload::TurnToolLoopBudgetExceeded {
            workspace_id,
            thread_id,
            turn_id,
            limit_kind,
            limit,
            observed,
            action,
            reason,
        } => turn_item_event_matches_snapshot(
            workspace_id.as_str(),
            thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::TurnToolLoopBudgetExceeded {
            thread_id,
            turn_id,
            limit_kind,
            limit,
            observed,
            action,
            reason,
        }),
        TurnItemEventPayload::TurnExecutionWindowStarted(notification) => {
            turn_item_event_matches_snapshot(
                notification.workspace_id.as_str(),
                notification.thread_id.as_str(),
                snapshot_workspace_id,
                snapshot_thread_id,
            )
            .then_some(ConversationEvent::TurnExecutionWindowStarted { notification })
        }
        TurnItemEventPayload::TurnExecutionWindowExhausted(notification) => {
            turn_item_event_matches_snapshot(
                notification.workspace_id.as_str(),
                notification.thread_id.as_str(),
                snapshot_workspace_id,
                snapshot_thread_id,
            )
            .then_some(ConversationEvent::TurnExecutionWindowExhausted { notification })
        }
        TurnItemEventPayload::TurnExecutionWindowCheckpointed(notification) => {
            turn_item_event_matches_snapshot(
                notification.workspace_id.as_str(),
                notification.thread_id.as_str(),
                snapshot_workspace_id,
                snapshot_thread_id,
            )
            .then_some(ConversationEvent::TurnExecutionWindowCheckpointed { notification })
        }
        TurnItemEventPayload::TurnExecutionWindowContinued(notification) => {
            turn_item_event_matches_snapshot(
                notification.workspace_id.as_str(),
                notification.thread_id.as_str(),
                snapshot_workspace_id,
                snapshot_thread_id,
            )
            .then_some(ConversationEvent::TurnExecutionWindowContinued { notification })
        }
        TurnItemEventPayload::TurnExecutionWindowBlocked(notification) => {
            turn_item_event_matches_snapshot(
                notification.workspace_id.as_str(),
                notification.thread_id.as_str(),
                snapshot_workspace_id,
                snapshot_thread_id,
            )
            .then_some(ConversationEvent::TurnExecutionWindowBlocked { notification })
        }
        TurnItemEventPayload::TurnPermissionAudit(event) => turn_item_event_matches_snapshot(
            event.workspace_id.as_str(),
            event.thread_id.as_str(),
            snapshot_workspace_id,
            snapshot_thread_id,
        )
        .then_some(ConversationEvent::TurnPermissionAudit { event }),
    }
}

fn turn_item_payload_scope(payload: &TurnItemEventPayload) -> (&str, &str, &str) {
    match payload {
        TurnItemEventPayload::ItemStarted {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemDelta {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemCompleted {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemUpdated {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemTimeoutDetected {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemRecoveryOpened {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemRecoveryAttached {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemRetryScheduled {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemRetryAttemptStarted {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemRecoverySucceeded {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemRecoveryExhausted {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemToolRetryScheduled {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemToolRetryResolved {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::ItemToolRetryExhausted {
            workspace_id,
            thread_id,
            turn_id,
            ..
        }
        | TurnItemEventPayload::TurnToolLoopBudgetExceeded {
            workspace_id,
            thread_id,
            turn_id,
            ..
        } => (workspace_id.as_str(), thread_id.as_str(), turn_id.as_str()),
        TurnItemEventPayload::TurnExecutionWindowStarted(notification) => (
            notification.workspace_id.as_str(),
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
        ),
        TurnItemEventPayload::TurnExecutionWindowExhausted(notification) => (
            notification.workspace_id.as_str(),
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
        ),
        TurnItemEventPayload::TurnExecutionWindowCheckpointed(notification) => (
            notification.workspace_id.as_str(),
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
        ),
        TurnItemEventPayload::TurnExecutionWindowContinued(notification) => (
            notification.workspace_id.as_str(),
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
        ),
        TurnItemEventPayload::TurnExecutionWindowBlocked(notification) => (
            notification.workspace_id.as_str(),
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
        ),
        TurnItemEventPayload::TurnPermissionAudit(event) => (
            event.workspace_id.as_str(),
            event.thread_id.as_str(),
            event.turn_id.as_str(),
        ),
    }
}

pub fn plan_turn_resume_after_status(status: TurnStatus) -> TurnResumeStatusPlan {
    match status {
        TurnStatus::InProgress => {
            TurnResumeStatusPlan::PollAfter(TURN_RESUME_IN_PROGRESS_POLL_DELAY)
        }
        TurnStatus::Completed => TurnResumeStatusPlan::Complete,
        TurnStatus::Failed | TurnStatus::Interrupted => TurnResumeStatusPlan::Fail,
        TurnStatus::Blocked => TurnResumeStatusPlan::Block,
    }
}

pub fn turn_resume_terminal_event(thread_id: String, turn: Turn) -> Option<ConversationEvent> {
    match turn.status {
        TurnStatus::Completed => Some(ConversationEvent::TurnCompleted { thread_id, turn }),
        TurnStatus::Failed | TurnStatus::Interrupted => {
            Some(ConversationEvent::TurnFailed { thread_id, turn })
        }
        TurnStatus::Blocked => Some(ConversationEvent::TurnBlocked {
            thread_id,
            turn,
            resume: None,
        }),
        TurnStatus::InProgress => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::ConversationEvent;
    use crate::threads::coordinator::ThreadCoordinator;
    use pioneer_protocol::{
        SystemEventLevel, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, Turn, TurnGetResponse, TurnItem, TurnItemEvent, TurnItemEventPayload,
        TurnItemsResponse, TurnKind, TurnOrigin, TurnStatus,
    };

    fn thread(thread_id: &str, workspace_id: &str) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 2,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        }
    }

    #[test]
    fn queue_connection_and_item_plans_gate_resume_work() {
        assert_eq!(
            plan_turn_resume_queue_connection(Some(7), true),
            TurnResumeQueueConnectionPlan::Drive { connection_id: 7 }
        );
        assert_eq!(
            plan_turn_resume_queue_connection(None, true),
            TurnResumeQueueConnectionPlan::NotReady
        );
        assert_eq!(
            plan_turn_resume_queue_item(true, true),
            TurnResumeQueueItemPlan::Skip
        );
        assert_eq!(
            plan_turn_resume_queue_item(false, false),
            TurnResumeQueueItemPlan::ResetMissingTurn
        );
        assert_eq!(
            plan_turn_resume_queue_item(false, true),
            TurnResumeQueueItemPlan::Resume
        );
    }

    #[test]
    fn resume_queue_helpers_dedupe_and_clear_threads() {
        let mut ready_threads = VecDeque::new();
        let mut ready_thread_set = HashSet::new();

        assert!(enqueue_turn_resume_thread(
            &mut ready_threads,
            &mut ready_thread_set,
            "thread_a".to_owned()
        ));
        assert!(!enqueue_turn_resume_thread(
            &mut ready_threads,
            &mut ready_thread_set,
            "thread_a".to_owned()
        ));
        assert!(enqueue_turn_resume_thread(
            &mut ready_threads,
            &mut ready_thread_set,
            "thread_b".to_owned()
        ));

        assert_eq!(
            dequeue_turn_resume_thread(&mut ready_threads, &mut ready_thread_set).as_deref(),
            Some("thread_a")
        );
        assert!(!ready_thread_set.contains("thread_a"));
        assert!(ready_thread_set.contains("thread_b"));

        clear_turn_resume_queue(&mut ready_threads, &mut ready_thread_set);
        assert!(ready_threads.is_empty());
        assert!(ready_thread_set.is_empty());
    }

    #[test]
    fn resume_state_begin_finish_reset_and_schedule_are_deterministic() {
        let now = Instant::now();
        let mut resume = ThreadResumeCoordinator {
            retry_attempt: 2,
            ..ThreadResumeCoordinator::default()
        };

        begin_turn_resume_attempt(&mut resume);
        assert!(resume.in_progress);
        finish_turn_resume_attempt(&mut resume);
        assert!(!resume.in_progress);

        let schedule = schedule_turn_resume_after_state(&mut resume, Duration::from_secs(3), now);
        assert_eq!(schedule.retry_attempt, 2);
        assert_eq!(schedule.next_attempt_at, now + Duration::from_secs(3));
        assert!(should_fire_scheduled_turn_resume(&resume, 2));

        reset_thread_resume_coordinator(&mut resume);
        assert_eq!(resume.retry_attempt, 0);
        assert_eq!(resume.next_attempt_at, None);
    }

    #[test]
    fn scheduled_resume_plan_checks_attempt_and_in_flight_turn() {
        let resume = ThreadResumeCoordinator {
            retry_attempt: 2,
            in_progress: false,
            next_attempt_at: None,
        };

        assert_eq!(
            plan_scheduled_turn_resume(&resume, 1, true),
            ScheduledTurnResumePlan::Skip
        );
        assert_eq!(
            plan_scheduled_turn_resume(&resume, 2, false),
            ScheduledTurnResumePlan::ResetMissingTurn
        );
        assert_eq!(
            plan_scheduled_turn_resume(&resume, 2, true),
            ScheduledTurnResumePlan::Resume
        );
    }

    #[test]
    fn retry_plan_updates_existing_state_and_handles_missing_state() {
        let now = Instant::now();
        let mut resume = ThreadResumeCoordinator {
            retry_attempt: 2,
            ..ThreadResumeCoordinator::default()
        };

        let retry = apply_turn_resume_retry(Some(&mut resume), now);

        assert_eq!(retry.attempt, 3);
        assert_eq!(retry.delay, Duration::from_millis(3_200));
        assert_eq!(
            retry.next_attempt_at,
            Some(now + Duration::from_millis(3_200))
        );
        assert_eq!(resume.retry_attempt, 3);
        assert_eq!(turn_resume_retry_delay(20), TURN_RESUME_RETRY_MAX_DELAY);

        let missing = apply_turn_resume_retry(None, now);
        assert_eq!(missing.attempt, 1);
        assert_eq!(missing.delay, TURN_RESUME_RETRY_INITIAL_DELAY);
        assert_eq!(missing.next_attempt_at, None);
    }

    #[test]
    fn status_plan_maps_turn_status_to_resume_follow_up() {
        assert_eq!(
            plan_turn_resume_after_status(TurnStatus::InProgress),
            TurnResumeStatusPlan::PollAfter(TURN_RESUME_IN_PROGRESS_POLL_DELAY)
        );
        assert_eq!(
            plan_turn_resume_after_status(TurnStatus::Completed),
            TurnResumeStatusPlan::Complete
        );
        assert_eq!(
            plan_turn_resume_after_status(TurnStatus::Failed),
            TurnResumeStatusPlan::Fail
        );
        assert_eq!(
            plan_turn_resume_after_status(TurnStatus::Interrupted),
            TurnResumeStatusPlan::Fail
        );
        assert_eq!(
            plan_turn_resume_after_status(TurnStatus::Blocked),
            TurnResumeStatusPlan::Block
        );
    }

    #[test]
    fn snapshot_and_event_matching_are_scope_strict() {
        let turn_params = turn_resume_turn_params("thread_a".to_owned(), "turn_a".to_owned());
        assert_eq!(turn_params.thread_id, "thread_a");
        assert_eq!(turn_params.turn_id, "turn_a");
        let page_params =
            turn_resume_items_page_params("thread_a".to_owned(), "turn_a".to_owned(), Some(40));
        assert_eq!(page_params.thread_id, "thread_a");
        assert_eq!(page_params.turn_id, "turn_a");
        assert_eq!(page_params.after_sequence, Some(40));
        assert_eq!(page_params.limit, Some(TURN_RESUME_ITEMS_PAGE_LIMIT));

        assert!(turn_item_event_matches_snapshot(
            "ws_a", "thread_a", "ws_a", "thread_a"
        ));
        assert!(!turn_item_event_matches_snapshot(
            "ws_b", "thread_a", "ws_a", "thread_a"
        ));
    }

    #[test]
    fn terminal_event_maps_terminal_turns() {
        let completed = Turn {
            id: "turn_a".to_owned(),
            status: TurnStatus::Completed,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        match turn_resume_terminal_event("thread_a".to_owned(), completed) {
            Some(ConversationEvent::TurnCompleted { thread_id, turn }) => {
                assert_eq!(thread_id, "thread_a");
                assert_eq!(turn.status, TurnStatus::Completed);
            }
            event => panic!("unexpected event: {event:?}"),
        }

        let failed = Turn {
            id: "turn_b".to_owned(),
            status: TurnStatus::Interrupted,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        match turn_resume_terminal_event("thread_b".to_owned(), failed) {
            Some(ConversationEvent::TurnFailed { thread_id, turn }) => {
                assert_eq!(thread_id, "thread_b");
                assert_eq!(turn.status, TurnStatus::Interrupted);
            }
            event => panic!("unexpected event: {event:?}"),
        }

        let blocked = Turn {
            id: "turn_blocked".to_owned(),
            status: TurnStatus::Blocked,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: Some("needs review".to_owned()),
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        match turn_resume_terminal_event("thread_blocked".to_owned(), blocked) {
            Some(ConversationEvent::TurnBlocked {
                thread_id, turn, ..
            }) => {
                assert_eq!(thread_id, "thread_blocked");
                assert_eq!(turn.status, TurnStatus::Blocked);
                assert_eq!(turn.error.as_deref(), Some("needs review"));
            }
            event => panic!("unexpected event: {event:?}"),
        }

        let in_progress = Turn {
            id: "turn_c".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        assert!(turn_resume_terminal_event("thread_c".to_owned(), in_progress).is_none());
    }

    #[test]
    fn snapshot_reduction_retries_mismatched_thread() {
        let turn_snapshot = TurnGetResponse {
            thread_id: "thread_b".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn: Turn {
                id: "turn_a".to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: TurnKind::Conversation,
                origin: TurnOrigin::User,
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            },
        };
        match reduce_turn_resume_turn_snapshot("thread_a", "turn_a", turn_snapshot) {
            TurnResumeSnapshotReduction::ScopeMismatch {
                expected_thread_id,
                actual_thread_id,
                expected_turn_id,
                actual_turn_id,
                retry_after,
            } => {
                assert_eq!(expected_thread_id, "thread_a");
                assert_eq!(actual_thread_id, "thread_b");
                assert_eq!(expected_turn_id, "turn_a");
                assert_eq!(actual_turn_id, "turn_a");
                assert_eq!(retry_after, TURN_RESUME_MISMATCH_RETRY_DELAY);
            }
            reduction => panic!("unexpected reduction: {reduction:?}"),
        }
    }

    #[test]
    fn snapshot_reduction_replays_items_and_polls_in_progress() {
        let turn_snapshot = TurnGetResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn: Turn {
                id: "turn_a".to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: TurnKind::Conversation,
                origin: TurnOrigin::User,
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            },
        };
        let item_snapshot = TurnItemsResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            last_sequence: 1,
            events: vec![TurnItemEvent {
                sequence: 1,
                created_at: 1,
                payload: TurnItemEventPayload::ItemStarted {
                    workspace_id: "ws_a".to_owned(),
                    thread_id: "thread_a".to_owned(),
                    turn_id: "turn_a".to_owned(),
                    item: TurnItem::SystemEvent {
                        id: "item_a".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "started".to_owned(),
                        code: None,
                        details: None,
                    },
                },
            }],
            has_more: false,
            next_cursor: None,
        };

        let page = reduce_turn_resume_items_page(&turn_snapshot, None, item_snapshot)
            .expect("valid first page");
        assert_eq!(page.thread_id, "thread_a");
        assert_eq!(page.workspace_id, "ws_a");
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.replay_events.len(), 1);
        assert!(matches!(
            &page.replay_events[0],
            ConversationEvent::ItemStarted {
                thread_id,
                turn_id,
                item: TurnItem::SystemEvent { id, .. },
            } if thread_id == "thread_a" && turn_id == "turn_a" && id == "item_a"
        ));

        let TurnResumeSnapshotReduction::Apply(reduction) =
            reduce_turn_resume_turn_snapshot("thread_a", "turn_a", turn_snapshot)
        else {
            panic!("expected status reduction");
        };

        assert_eq!(reduction.thread_id, "thread_a");
        assert_eq!(reduction.workspace_id, "ws_a");
        assert_eq!(
            reduction.schedule_after,
            Some(TURN_RESUME_IN_PROGRESS_POLL_DELAY)
        );
        assert!(!reduction.reset_thread_resume);
        assert!(!reduction.tick_conversation_after_terminal_event);
        assert!(reduction.terminal_event.is_none());
        assert!(reduction.replay_events.is_empty());
    }

    #[test]
    fn snapshot_reduction_completed_turn_terminalizes_ticks_and_resets() {
        let turn_snapshot = TurnGetResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn: Turn {
                id: "turn_a".to_owned(),
                status: TurnStatus::Completed,
                turn_kind: TurnKind::Conversation,
                origin: TurnOrigin::User,
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            },
        };
        let TurnResumeSnapshotReduction::Apply(reduction) =
            reduce_turn_resume_turn_snapshot("thread_a", "turn_a", turn_snapshot)
        else {
            panic!("expected apply reduction");
        };

        assert_eq!(reduction.thread_id, "thread_a");
        assert_eq!(reduction.workspace_id, "ws_a");
        assert_eq!(reduction.schedule_after, None);
        assert!(reduction.reset_thread_resume);
        assert!(reduction.tick_conversation_after_terminal_event);
        assert!(matches!(
            reduction.terminal_event,
            Some(ConversationEvent::TurnCompleted { thread_id, turn })
                if thread_id == "thread_a" && turn.status == TurnStatus::Completed
        ));
    }

    #[test]
    fn snapshot_failure_plan_retries_requested_thread() {
        assert_eq!(
            plan_turn_resume_snapshot_failure("thread_a"),
            TurnResumeSnapshotFailurePlan::Retry {
                thread_id: "thread_a".to_owned()
            }
        );
    }

    #[test]
    fn incremental_turn_item_pages_preserve_a_long_event_log_without_loss() {
        let turn_snapshot = TurnGetResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn: Turn {
                id: "turn_a".to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: TurnKind::Conversation,
                origin: TurnOrigin::User,
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            },
        };
        let make_page = |start: i64, end: i64, has_more: bool| TurnItemsResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            events: (start..=end)
                .map(|sequence| TurnItemEvent {
                    sequence,
                    created_at: sequence,
                    payload: TurnItemEventPayload::ItemCompleted {
                        workspace_id: "ws_a".to_owned(),
                        thread_id: "thread_a".to_owned(),
                        turn_id: "turn_a".to_owned(),
                        item: TurnItem::SystemEvent {
                            id: format!("item_{sequence}"),
                            level: SystemEventLevel::Info,
                            message: "completed".to_owned(),
                            code: None,
                            details: None,
                        },
                    },
                })
                .collect(),
            last_sequence: end,
            has_more,
            next_cursor: has_more.then_some(end),
        };

        let mut cursor = None;
        let mut replayed = Vec::new();
        for page in [
            make_page(1, 200, true),
            make_page(201, 400, true),
            make_page(401, 401, false),
        ] {
            let reduced = reduce_turn_resume_items_page(&turn_snapshot, cursor, page)
                .expect("each bounded page should reduce");
            cursor = reduced.next_cursor;
            replayed.extend(reduced.replay_events);
        }

        assert_eq!(cursor, None);
        assert_eq!(replayed.len(), 401);
        assert!(matches!(
            replayed.first(),
            Some(ConversationEvent::ItemCompleted {
                item: TurnItem::SystemEvent { id, .. },
                ..
            }) if id == "item_1"
        ));
        assert!(matches!(
            replayed.last(),
            Some(ConversationEvent::ItemCompleted {
                item: TurnItem::SystemEvent { id, .. },
                ..
            }) if id == "item_401"
        ));
    }

    #[test]
    fn turn_items_page_rejects_foreign_events_instead_of_skipping_them() {
        let turn_snapshot = TurnGetResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn: Turn {
                id: "turn_a".to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: TurnKind::Conversation,
                origin: TurnOrigin::User,
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            },
        };
        let item_snapshot = TurnItemsResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            last_sequence: 2,
            events: vec![
                TurnItemEvent {
                    sequence: 1,
                    created_at: 1,
                    payload: TurnItemEventPayload::ItemStarted {
                        workspace_id: "ws_a".to_owned(),
                        thread_id: "thread_a".to_owned(),
                        turn_id: "turn_a".to_owned(),
                        item: TurnItem::SystemEvent {
                            id: "item_a".to_owned(),
                            level: SystemEventLevel::Info,
                            message: "started".to_owned(),
                            code: None,
                            details: None,
                        },
                    },
                },
                TurnItemEvent {
                    sequence: 2,
                    created_at: 2,
                    payload: TurnItemEventPayload::ItemCompleted {
                        workspace_id: "ws_b".to_owned(),
                        thread_id: "thread_a".to_owned(),
                        turn_id: "turn_a".to_owned(),
                        item: TurnItem::SystemEvent {
                            id: "foreign".to_owned(),
                            level: SystemEventLevel::Info,
                            message: "foreign".to_owned(),
                            code: None,
                            details: None,
                        },
                    },
                },
            ],
            has_more: false,
            next_cursor: None,
        };

        let error = reduce_turn_resume_items_page(&turn_snapshot, None, item_snapshot)
            .expect_err("foreign event must fail the whole page closed");
        assert!(error.to_string().contains("outside the requested turn"));
    }

    #[test]
    fn in_flight_thread_ids_are_collected_from_coordinators() {
        let mut coordinator = ThreadCoordinator::new(thread("thread_a", "ws_a"));
        coordinator
            .conversation
            .apply(ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                pending_request_id: "req_a".to_owned(),
                mode: pioneer_protocol::ThreadMode::Agent,
                user_text: "hello".to_owned(),
                attachments: Vec::new(),
            });
        let coordinators = HashMap::from([
            ("thread_a".to_owned(), coordinator),
            (
                "thread_b".to_owned(),
                ThreadCoordinator::new(thread("thread_b", "ws_a")),
            ),
        ]);

        assert_eq!(
            thread_ids_with_in_flight_turns(&coordinators),
            vec!["thread_a".to_owned()]
        );
    }
}
