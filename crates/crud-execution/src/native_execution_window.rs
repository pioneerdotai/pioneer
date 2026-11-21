//! Prepared transitions for the durable execution window and checkpoint stream.
use anyhow::{Context, Result};
use pioneer_crud_conversations::events::TurnEventPayload;
use sea_orm::{ConnectionTrait, DatabaseTransaction, entity::prelude::DateTimeWithTimeZone};
use std::{fmt, future::Future};

use crate::turn_execution_window::{
    self, NewTurnExecutionCheckpointRecord, NewTurnExecutionWindowRecord,
    PreparedTurnExecutionCheckpointInsert, PreparedTurnExecutionWindowInsert,
    PreparedTurnExecutionWindowStatsTransition, PreparedTurnExecutionWindowStatusTransition,
    TurnExecutionCheckpointKind, TurnExecutionWindowStatsRecord, execution_window_status_to_db,
};

/// A continued window is already durable, but an earlier event must be
/// projected before the agent can acknowledge its start.
#[derive(Debug)]
pub struct ExecutionWindowStartProjectionPending;

impl fmt::Display for ExecutionWindowStartProjectionPending {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("continued execution window start is waiting for an earlier turn event")
    }
}

impl std::error::Error for ExecutionWindowStartProjectionPending {}

#[derive(Debug, Clone)]
pub enum NativeExecutionWindowTransition {
    Started {
        notification: pioneer_protocol::TurnExecutionWindowStartedNotification,
        window: NewTurnExecutionWindowRecord,
        created_at: DateTimeWithTimeZone,
        updated_at: DateTimeWithTimeZone,
    },
    Exhausted {
        notification: pioneer_protocol::TurnExecutionWindowExhaustedNotification,
        stats: TurnExecutionWindowStatsRecord,
    },
    Checkpointed {
        notification: pioneer_protocol::TurnExecutionWindowCheckpointedNotification,
        checkpoint: NewTurnExecutionCheckpointRecord,
    },
    Continued {
        notification: pioneer_protocol::TurnExecutionWindowContinuedNotification,
        updated_at: DateTimeWithTimeZone,
    },
    Blocked {
        notification: pioneer_protocol::TurnExecutionWindowBlockedNotification,
        stats: TurnExecutionWindowStatsRecord,
    },
}

impl NativeExecutionWindowTransition {
    pub fn event_payload(&self) -> TurnEventPayload {
        match self {
            Self::Started { notification, .. } => {
                TurnEventPayload::TurnExecutionWindowStarted(notification.clone())
            }
            Self::Exhausted { notification, .. } => {
                TurnEventPayload::TurnExecutionWindowExhausted(notification.clone())
            }
            Self::Checkpointed { notification, .. } => {
                TurnEventPayload::TurnExecutionWindowCheckpointed(notification.clone())
            }
            Self::Continued { notification, .. } => {
                TurnEventPayload::TurnExecutionWindowContinued(notification.clone())
            }
            Self::Blocked { notification, .. } => {
                TurnEventPayload::TurnExecutionWindowBlocked(notification.clone())
            }
        }
    }
}

fn db_timestamp_from_unix_millis(value: i64) -> sea_orm::entity::prelude::DateTimeWithTimeZone {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(value)
        .map(|timestamp| timestamp.fixed_offset())
        .unwrap_or_else(|| chrono::Utc::now().fixed_offset())
}

async fn require_native_execution_window<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    window_index: u32,
    runtime_window_id: &str,
) -> Result<pioneer_entity::turn_execution_window::Model> {
    let window = turn_execution_window::latest_turn_execution_window_model(db, turn_id)
        .await?
        .with_context(|| {
            format!(
                "execution-window event for turn `{turn_id}` index {window_index} has no predecessor"
            )
        })?;
    if window.window_index != i64::from(window_index) {
        anyhow::bail!(
            "execution-window event for turn `{turn_id}` expected latest index {window_index}, found {}",
            window.window_index
        );
    }
    if !turn_execution_window::window_metadata_string_matches(
        db,
        window.id.as_str(),
        "runtimeWindowId",
        runtime_window_id,
    )
    .await?
    {
        anyhow::bail!(
            "execution-window event runtime id `{runtime_window_id}` does not match the stored id"
        );
    }
    Ok(window)
}

#[derive(Clone)]
pub struct PreparedWindowTransition {
    transition: NativeExecutionWindowTransition,
    prepared_started_window: Option<PreparedTurnExecutionWindowInsert>,
    prepared_checkpoint: Option<PreparedTurnExecutionCheckpointInsert>,
    checkpoint_payload_bytes: Option<usize>,
    prepared_continued_window: Option<PreparedTurnExecutionWindowInsert>,
    prepared_exhausted_stats: Option<PreparedTurnExecutionWindowStatsTransition>,
    prepared_blocked_stats: Option<[PreparedTurnExecutionWindowStatsTransition; 3]>,
    prepared_checkpoint_status: Option<PreparedTurnExecutionWindowStatusTransition>,
    prepared_continued_status: Option<PreparedTurnExecutionWindowStatusTransition>,
}

impl PreparedWindowTransition {
    pub fn new(transition: NativeExecutionWindowTransition) -> Result<Self> {
        let prepared_started_window = match &transition {
            NativeExecutionWindowTransition::Started {
                window,
                created_at,
                updated_at,
                ..
            } => Some(turn_execution_window::prepare_turn_execution_window_insert(
                window.clone(),
                *created_at,
                *updated_at,
            )?),
            _ => None,
        };
        let prepared_checkpoint = match &transition {
            NativeExecutionWindowTransition::Checkpointed { checkpoint, .. } => Some(
                turn_execution_window::prepare_turn_execution_checkpoint_insert(
                    checkpoint.clone(),
                )?,
            ),
            _ => None,
        };
        let checkpoint_payload_bytes = prepared_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.payload_bytes());
        let prepared_continued_window = match &transition {
            NativeExecutionWindowTransition::Continued { notification, .. } => {
                let started_at = db_timestamp_from_unix_millis(notification.continued_at_unix_ms);
                Some(turn_execution_window::prepare_turn_execution_window_insert(
                    NewTurnExecutionWindowRecord {
                        workspace_id: notification.workspace_id.clone(),
                        thread_id: notification.thread_id.clone(),
                        turn_id: notification.turn_id.clone(),
                        window_index: notification.window_index,
                        status: pioneer_protocol::ExecutionWindowStatus::Running,
                        exhaustion_reason: None,
                        agent_round_count: 0,
                        tool_call_count: 0,
                        provider_token_count: 0,
                        metadata_json: serde_json::json!({
                            "runtimeWindowId": notification.window_id,
                            "createdByContinuationCheckpointId": notification.checkpoint_id,
                        }),
                        started_at,
                    },
                    started_at,
                    match &transition {
                        NativeExecutionWindowTransition::Continued { updated_at, .. } => {
                            *updated_at
                        }
                        _ => unreachable!("matched continued transition"),
                    },
                )?)
            }
            _ => None,
        };
        let prepared_exhausted_stats = match &transition {
            NativeExecutionWindowTransition::Exhausted {
                notification,
                stats,
            } => Some(
                turn_execution_window::prepare_turn_execution_window_stats_transition(
                    pioneer_protocol::ExecutionWindowStatus::Running,
                    pioneer_protocol::ExecutionWindowStatus::Exhausted,
                    Some(notification.exhaustion_reason),
                    stats.clone(),
                )?,
            ),
            _ => None,
        };
        let prepared_blocked_stats = match &transition {
            NativeExecutionWindowTransition::Blocked {
                notification,
                stats,
            } => Some([
                turn_execution_window::prepare_turn_execution_window_stats_transition(
                    pioneer_protocol::ExecutionWindowStatus::Running,
                    pioneer_protocol::ExecutionWindowStatus::Blocked,
                    notification.exhaustion_reason,
                    stats.clone(),
                )?,
                turn_execution_window::prepare_turn_execution_window_stats_transition(
                    pioneer_protocol::ExecutionWindowStatus::Exhausted,
                    pioneer_protocol::ExecutionWindowStatus::Blocked,
                    notification.exhaustion_reason,
                    stats.clone(),
                )?,
                turn_execution_window::prepare_turn_execution_window_stats_transition(
                    pioneer_protocol::ExecutionWindowStatus::Checkpointed,
                    pioneer_protocol::ExecutionWindowStatus::Blocked,
                    notification.exhaustion_reason,
                    stats.clone(),
                )?,
            ]),
            _ => None,
        };
        let prepared_checkpoint_status = match &transition {
            NativeExecutionWindowTransition::Checkpointed { notification, .. } => Some(
                turn_execution_window::prepare_turn_execution_window_status_transition(
                    pioneer_protocol::ExecutionWindowStatus::Exhausted,
                    pioneer_protocol::ExecutionWindowStatus::Checkpointed,
                    db_timestamp_from_unix_millis(notification.created_at_unix_ms),
                ),
            ),
            _ => None,
        };
        let prepared_continued_status = match &transition {
            NativeExecutionWindowTransition::Continued { updated_at, .. } => Some(
                turn_execution_window::prepare_turn_execution_window_status_transition(
                    pioneer_protocol::ExecutionWindowStatus::Checkpointed,
                    pioneer_protocol::ExecutionWindowStatus::Continued,
                    *updated_at,
                ),
            ),
            _ => None,
        };
        Ok(Self {
            transition,
            prepared_started_window,
            prepared_checkpoint,
            checkpoint_payload_bytes,
            prepared_continued_window,
            prepared_exhausted_stats,
            prepared_blocked_stats,
            prepared_checkpoint_status,
            prepared_continued_status,
        })
    }

    /// The coordinator supplies the canonical event written with this transition.
    /// It holds the stream lock and checks exact replay before applying.
    pub async fn apply<'a, F, Fut>(
        self,
        transaction: &'a DatabaseTransaction,
        append_event: F,
    ) -> Result<()>
    where
        F: FnOnce(&'a DatabaseTransaction) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let Self {
            transition,
            prepared_started_window,
            prepared_checkpoint,
            checkpoint_payload_bytes,
            prepared_continued_window,
            prepared_exhausted_stats,
            prepared_blocked_stats,
            prepared_checkpoint_status,
            prepared_continued_status,
        } = self;
        match transition {
            NativeExecutionWindowTransition::Started {
                notification,
                window,
                ..
            } => {
                if notification.status != pioneer_protocol::ExecutionWindowStatus::Running {
                    anyhow::bail!(
                        "execution-window started event has non-running status {:?}",
                        notification.status
                    );
                }
                let latest = turn_execution_window::latest_turn_execution_window_model(
                    transaction,
                    notification.turn_id.as_str(),
                )
                .await?;
                if window.workspace_id != notification.workspace_id
                    || window.thread_id != notification.thread_id
                    || window.turn_id != notification.turn_id
                    || window.window_index != notification.window_index
                {
                    anyhow::bail!(
                        "execution-window started record does not match its notification"
                    );
                }
                if window.status != pioneer_protocol::ExecutionWindowStatus::Running
                    || window.exhaustion_reason.is_some()
                    || window
                        .metadata_json
                        .get("runtimeWindowId")
                        .and_then(serde_json::Value::as_str)
                        != Some(notification.window_id.as_str())
                {
                    anyhow::bail!(
                        "execution-window started record is not an exact running runtime window"
                    );
                }
                match latest {
                    None if notification.window_index == 1 => {}
                    Some(existing)
                        if existing.window_index == i64::from(notification.window_index)
                            && existing.status
                                == execution_window_status_to_db(
                                    pioneer_protocol::ExecutionWindowStatus::Running,
                                )
                            && existing.workspace_id == notification.workspace_id
                            && existing.thread_id == notification.thread_id
                            && existing.turn_id == notification.turn_id =>
                    {
                        let runtime_id_matches =
                            turn_execution_window::window_metadata_string_matches(
                                transaction,
                                existing.id.as_str(),
                                "runtimeWindowId",
                                notification.window_id.as_str(),
                            )
                            .await?;
                        let has_continuation_checkpoint =
                            turn_execution_window::window_metadata_string_is_present(
                                transaction,
                                existing.id.as_str(),
                                "createdByContinuationCheckpointId",
                            )
                            .await?;
                        if !runtime_id_matches || !has_continuation_checkpoint {
                            anyhow::bail!(
                                "execution-window started replay does not match its committed continuation"
                            );
                        }
                        if pioneer_crud_conversations::event_projection::has_unprojected_event(
                            transaction,
                            notification.turn_id.as_str(),
                        )
                        .await?
                        {
                            return Err(ExecutionWindowStartProjectionPending.into());
                        }
                        append_event(transaction).await?;
                        return Ok(());
                    }
                    _ => anyhow::bail!(
                        "execution-window started event is out of order or lacks a committed continuation for turn `{}` at index {}",
                        notification.turn_id,
                        notification.window_index
                    ),
                }
                append_event(transaction).await?;
                turn_execution_window::insert_prepared_turn_execution_window(
                    transaction,
                    prepared_started_window
                        .context("started transition is missing its prepared window insert")?,
                )
                .await?;
            }
            NativeExecutionWindowTransition::Exhausted {
                notification,
                stats,
            } => {
                if notification.status != pioneer_protocol::ExecutionWindowStatus::Exhausted {
                    anyhow::bail!(
                        "execution-window exhausted event has invalid status {:?}",
                        notification.status
                    );
                }
                let window = require_native_execution_window(
                    transaction,
                    notification.turn_id.as_str(),
                    notification.window_index,
                    notification.window_id.as_str(),
                )
                .await?;
                if stats.agent_round_count != notification.agent_round_count
                    || stats.tool_call_count != notification.tool_call_count
                    || stats.provider_token_count
                        != notification.provider_token_count.unwrap_or_default()
                {
                    anyhow::bail!(
                        "execution-window exhausted statistics do not match notification"
                    );
                }
                append_event(transaction).await?;
                turn_execution_window::transition_window_with_prepared_stats(
                    transaction,
                    window.id.as_str(),
                    prepared_exhausted_stats
                        .context("exhausted transition is missing its prepared statistics")?,
                )
                .await?;
            }
            NativeExecutionWindowTransition::Checkpointed {
                notification,
                mut checkpoint,
            } => {
                if notification.status != pioneer_protocol::ExecutionWindowStatus::Checkpointed {
                    anyhow::bail!(
                        "execution-window checkpoint event has invalid status {:?}",
                        notification.status
                    );
                }
                let window = require_native_execution_window(
                    transaction,
                    notification.turn_id.as_str(),
                    notification.window_index,
                    notification.window_id.as_str(),
                )
                .await?;
                checkpoint.window_id = window.id.clone();
                let prepared_checkpoint = prepared_checkpoint
                    .context("checkpoint transition is missing its prepared record")?
                    .with_window_id(window.id.clone());
                if checkpoint.id.as_deref() != Some(notification.checkpoint_id.as_str())
                    || checkpoint.workspace_id != notification.workspace_id
                    || checkpoint.thread_id != notification.thread_id
                    || checkpoint.turn_id != notification.turn_id
                {
                    anyhow::bail!(
                        "execution-window checkpoint record does not match its notification"
                    );
                }
                let expected_kind = match checkpoint.checkpoint_kind {
                    TurnExecutionCheckpointKind::WindowExhausted => "window_exhausted",
                    TurnExecutionCheckpointKind::TurnBlocked => "turn_blocked",
                    TurnExecutionCheckpointKind::StartupRecovery => "startup_recovery",
                };
                let payload_bytes = checkpoint_payload_bytes
                    .context("checkpoint transition is missing its prepared payload size")?;
                if notification.checkpoint_kind != expected_kind
                    || u64::try_from(payload_bytes).ok() != Some(notification.payload_bytes)
                {
                    anyhow::bail!(
                        "execution-window checkpoint wire metadata does not match its durable payload"
                    );
                }
                if window.status
                    != execution_window_status_to_db(
                        pioneer_protocol::ExecutionWindowStatus::Exhausted,
                    )
                {
                    anyhow::bail!(
                        "execution window `{}` cannot be checkpointed from {:?}",
                        window.id,
                        window.status
                    );
                }
                append_event(transaction).await?;
                let existing = turn_execution_window::get_turn_execution_checkpoint_model(
                    transaction,
                    notification.checkpoint_id.as_str(),
                )
                .await?;
                if let Some(existing) = existing {
                    if !prepared_checkpoint.matches_model(&existing) {
                        anyhow::bail!(
                            "checkpoint id `{}` conflicts with an existing durable checkpoint",
                            notification.checkpoint_id
                        );
                    }
                } else {
                    turn_execution_window::insert_prepared_turn_execution_checkpoint(
                        transaction,
                        prepared_checkpoint,
                    )
                    .await?;
                }
                turn_execution_window::transition_window_with_prepared_status(
                    transaction,
                    window.id.as_str(),
                    prepared_checkpoint_status
                        .context("checkpoint transition is missing its prepared status")?,
                )
                .await?;
            }
            NativeExecutionWindowTransition::Continued { notification, .. } => {
                if notification.status != pioneer_protocol::ExecutionWindowStatus::Continued {
                    anyhow::bail!(
                        "execution-window continued event has invalid status {:?}",
                        notification.status
                    );
                }
                let previous = require_native_execution_window(
                    transaction,
                    notification.turn_id.as_str(),
                    notification.previous_window_index,
                    notification.previous_window_id.as_str(),
                )
                .await?;
                if notification.window_index != notification.previous_window_index.saturating_add(1)
                {
                    anyhow::bail!(
                        "execution-window continuation skips from {} to {}",
                        notification.previous_window_index,
                        notification.window_index
                    );
                }
                let checkpoint = turn_execution_window::get_turn_execution_checkpoint_model(
                    transaction,
                    notification.checkpoint_id.as_str(),
                )
                .await?;
                if checkpoint
                    .as_ref()
                    .is_none_or(|checkpoint| checkpoint.window_id != previous.id)
                {
                    anyhow::bail!(
                        "execution window `{}` cannot continue without exact checkpoint `{}`",
                        previous.id,
                        notification.checkpoint_id
                    );
                }
                append_event(transaction).await?;
                turn_execution_window::transition_window_with_prepared_status(
                    transaction,
                    previous.id.as_str(),
                    prepared_continued_status
                        .context("continued transition is missing its prepared status")?,
                )
                .await?;
                turn_execution_window::insert_prepared_turn_execution_window(
                    transaction,
                    prepared_continued_window
                        .context("continued transition is missing its prepared successor window")?,
                )
                .await?;
            }
            NativeExecutionWindowTransition::Blocked { notification, .. } => {
                if notification.status != pioneer_protocol::ExecutionWindowStatus::Blocked {
                    anyhow::bail!(
                        "execution-window blocked event has invalid status {:?}",
                        notification.status
                    );
                }
                let window = require_native_execution_window(
                    transaction,
                    notification.turn_id.as_str(),
                    notification.window_index,
                    notification.window_id.as_str(),
                )
                .await?;
                let expected = if window.status
                    == execution_window_status_to_db(
                        pioneer_protocol::ExecutionWindowStatus::Running,
                    ) {
                    pioneer_protocol::ExecutionWindowStatus::Running
                } else if window.status
                    == execution_window_status_to_db(
                        pioneer_protocol::ExecutionWindowStatus::Exhausted,
                    )
                {
                    pioneer_protocol::ExecutionWindowStatus::Exhausted
                } else if window.status
                    == execution_window_status_to_db(
                        pioneer_protocol::ExecutionWindowStatus::Checkpointed,
                    )
                {
                    pioneer_protocol::ExecutionWindowStatus::Checkpointed
                } else if window.status
                    == execution_window_status_to_db(
                        pioneer_protocol::ExecutionWindowStatus::Blocked,
                    )
                {
                    pioneer_protocol::ExecutionWindowStatus::Running
                } else {
                    anyhow::bail!(
                        "execution window `{}` cannot be blocked from {:?}",
                        window.id,
                        window.status
                    )
                };
                append_event(transaction).await?;
                let prepared_stats = match expected {
                    pioneer_protocol::ExecutionWindowStatus::Running => prepared_blocked_stats
                        .as_ref()
                        .map(|prepared| prepared[0].clone()),
                    pioneer_protocol::ExecutionWindowStatus::Exhausted => prepared_blocked_stats
                        .as_ref()
                        .map(|prepared| prepared[1].clone()),
                    pioneer_protocol::ExecutionWindowStatus::Checkpointed => prepared_blocked_stats
                        .as_ref()
                        .map(|prepared| prepared[2].clone()),
                    _ => None,
                }
                .context("blocked transition is missing its prepared statistics")?;
                turn_execution_window::transition_window_with_prepared_stats(
                    transaction,
                    window.id.as_str(),
                    prepared_stats,
                )
                .await?;
            }
        }
        Ok(())
    }
}
