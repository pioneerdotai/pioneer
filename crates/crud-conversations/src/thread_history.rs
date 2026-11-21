//! Project append-only events and current collaboration state into client history.

use anyhow::{Context, Result};
use pioneer_entity::thread;
use pioneer_protocol::{
    ThreadHistoryEvent, ThreadHistoryEventPayload, ThreadMode,
    constants::events::{TURN_BLOCKED, TURN_COMPLETED, TURN_FAILED, TURN_STARTED},
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{EntityTrait, QuerySelect};
use std::collections::{HashMap, HashSet};

use crate::events::TurnEventPayload;
use crate::protocol_projection::{decode_turn_input_model, thread_snapshot_turn_from_db_model};
use crate::turn_event_reads::thread_event_query;
use crate::turn_order::THREAD_TURN_SNAPSHOT_BATCH_SIZE;
use crate::turn_payload::{capture_events, decode_events};
use crate::turn_reads::{find_turn_inputs_for_turns, find_turns_by_thread_and_ids};

#[derive(Debug, Clone)]
pub struct ThreadHistorySnapshot {
    pub workspace_id: String,
    pub events: Vec<ThreadHistoryEvent>,
}

pub async fn get_thread_history(
    db: &SqliteDatabase,
    thread_id: &str,
    limit_events: Option<u64>,
) -> Result<Option<ThreadHistorySnapshot>> {
    let reader = db.begin_read().await?;
    let Some(workspace_id) = thread::Entity::find_by_id(thread_id)
        .select_only()
        .column(thread::Column::WorkspaceId)
        .into_tuple::<String>()
        .one(&reader)
        .await?
    else {
        return Ok(None);
    };

    let mut rows = thread_event_query(thread_id, limit_events)
        .all(&reader)
        .await?;
    rows.reverse();
    // Read current Message metadata and input in the same snapshot as the
    // event page. Otherwise an edit can interleave those two lookups.
    let projection_ids = rows
        .iter()
        .filter_map(|row| {
            matches!(
                row.event_type.as_str(),
                TURN_STARTED | TURN_COMPLETED | TURN_FAILED | TURN_BLOCKED
            )
            .then_some(row.turn_id.as_str())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut current_rows = Vec::new();
    let mut input_rows = Vec::new();
    for ids in projection_ids.chunks(THREAD_TURN_SNAPSHOT_BATCH_SIZE) {
        let current = find_turns_by_thread_and_ids(&reader, thread_id, ids).await?;
        let messages = current
            .iter()
            .filter(|row| {
                row.send_mode.as_deref() == Some("message") && row.message_deleted_at.is_none()
            })
            .map(|row| row.id.clone())
            .collect::<Vec<_>>();
        input_rows.extend(find_turn_inputs_for_turns(&reader, &messages).await?);
        current_rows.extend(current);
    }
    let captured = capture_events(&reader, rows).await?;
    reader.commit().await?;

    let current_turns = current_rows
        .into_iter()
        .filter_map(|row| {
            thread_snapshot_turn_from_db_model(row)
                .transpose()
                .map(|result| result.map(|turn| (turn.id.clone(), turn)))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    let mut current_message_inputs = HashMap::<String, Vec<_>>::new();
    for row in input_rows {
        current_message_inputs
            .entry(row.turn_id.clone())
            .or_default()
            .push(decode_turn_input_model(row)?);
    }
    let decoded_rows = decode_events(captured)?;
    let mut events = Vec::with_capacity(decoded_rows.len());
    for mut row in decoded_rows {
        let mut payload = serde_json::from_value::<TurnEventPayload>(
            row.payload.take().context("missing turn event payload")?,
        )
        .with_context(|| format!("failed to decode turn_event payload `{}`", row.id))?;
        if let Some(current) = current_turns.get(row.turn_id.as_str()) {
            payload.project_work_owner(current.work_owner);
        }
        let mapped_payload = match payload {
            TurnEventPayload::TurnStarted(payload) => {
                let (turn, input) = if payload.turn.mode == ThreadMode::Message {
                    let current = current_turns
                        .get(row.turn_id.as_str())
                        .context("current Message history projection is unavailable")?;
                    if current.mode != ThreadMode::Message {
                        anyhow::bail!("current Message history projection changed mode");
                    }
                    let mut projected = current.clone();
                    projected.status = payload.turn.status;
                    let input = if current.message_deleted {
                        Vec::new()
                    } else {
                        current_message_inputs
                            .get(row.turn_id.as_str())
                            .cloned()
                            .unwrap_or_default()
                    };
                    (projected, input)
                } else {
                    (payload.turn, payload.input)
                };
                ThreadHistoryEventPayload::TurnStarted {
                    workspace_id: payload.thread.workspace_id.clone(),
                    thread_id: payload.thread.id.clone(),
                    turn,
                    input,
                }
            }
            TurnEventPayload::ItemStarted(notification) => ThreadHistoryEventPayload::ItemStarted {
                workspace_id: notification.workspace_id,
                thread_id: notification.thread_id,
                turn_id: notification.turn_id,
                item: notification.item,
            },
            TurnEventPayload::ItemCompleted(notification) => {
                ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item: notification.item,
                }
            }
            TurnEventPayload::ItemUpdated(notification) => ThreadHistoryEventPayload::ItemUpdated {
                workspace_id: notification.workspace_id,
                thread_id: notification.thread_id,
                turn_id: notification.turn_id,
                item: notification.item,
            },
            TurnEventPayload::ItemTimeoutDetected(notification) => {
                ThreadHistoryEventPayload::ItemTimeoutDetected {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    attempt_number: notification.attempt_number,
                    reason: notification.reason,
                    recovery_job_id: notification.recovery_job_id,
                }
            }
            TurnEventPayload::ItemRecoveryOpened(notification) => {
                ThreadHistoryEventPayload::ItemRecoveryOpened {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    trigger: notification.trigger,
                    action: notification.action,
                    attempt_number: notification.attempt_number,
                }
            }
            TurnEventPayload::ItemRecoveryAttached(notification) => {
                ThreadHistoryEventPayload::ItemRecoveryAttached {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    recovery_item_id: notification.recovery_item_id,
                    recovery_item_type: notification.recovery_item_type,
                    trigger: notification.trigger,
                    action: notification.action,
                    existing_status: notification.existing_status,
                    next_attempt_number: notification.next_attempt_number,
                }
            }
            TurnEventPayload::ItemRetryScheduled(notification) => {
                ThreadHistoryEventPayload::ItemRetryScheduled {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                    next_run_at_unix: notification.next_run_at_unix,
                    reason: notification.reason,
                }
            }
            TurnEventPayload::ItemRetryAttemptStarted(notification) => {
                ThreadHistoryEventPayload::ItemRetryAttemptStarted {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                }
            }
            TurnEventPayload::ItemRecoverySucceeded(notification) => {
                ThreadHistoryEventPayload::ItemRecoverySucceeded {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                }
            }
            TurnEventPayload::ItemRecoveryExhausted(notification) => {
                ThreadHistoryEventPayload::ItemRecoveryExhausted {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                    status: notification.status,
                    error_message: notification.error_message,
                }
            }
            TurnEventPayload::ItemToolRetryScheduled(notification) => {
                ThreadHistoryEventPayload::ItemToolRetryScheduled {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    tool_retry_episode_id: notification.tool_retry_episode_id,
                    tool_name: notification.tool_name,
                    attempt_number: notification.attempt_number,
                    error_class: notification.error_class,
                    retry_hint: notification.retry_hint,
                    budgets: notification.budgets,
                    failure_signature_fingerprint: notification.failure_signature_fingerprint,
                    reason: notification.reason,
                }
            }
            TurnEventPayload::ItemToolRetryResolved(notification) => {
                ThreadHistoryEventPayload::ItemToolRetryResolved {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    tool_retry_episode_id: notification.tool_retry_episode_id,
                    tool_name: notification.tool_name,
                    attempt_number: notification.attempt_number,
                    resolution: notification.resolution,
                    budgets: notification.budgets,
                    reason: notification.reason,
                }
            }
            TurnEventPayload::ItemToolRetryExhausted(notification) => {
                ThreadHistoryEventPayload::ItemToolRetryExhausted {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    tool_retry_episode_id: notification.tool_retry_episode_id,
                    tool_name: notification.tool_name,
                    attempt_number: notification.attempt_number,
                    error_class: notification.error_class,
                    exhaustion_kind: notification.exhaustion_kind,
                    budgets: notification.budgets,
                    failure_signature_fingerprint: notification.failure_signature_fingerprint,
                    reason: notification.reason,
                }
            }
            TurnEventPayload::TurnToolLoopBudgetExceeded(notification) => {
                ThreadHistoryEventPayload::TurnToolLoopBudgetExceeded {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    limit_kind: notification.limit_kind,
                    limit: notification.limit,
                    observed: notification.observed,
                    action: notification.action,
                    reason: notification.reason,
                }
            }
            TurnEventPayload::TurnExecutionWindowStarted(notification) => {
                ThreadHistoryEventPayload::TurnExecutionWindowStarted(notification)
            }
            TurnEventPayload::TurnExecutionWindowExhausted(notification) => {
                ThreadHistoryEventPayload::TurnExecutionWindowExhausted(notification)
            }
            TurnEventPayload::TurnExecutionWindowCheckpointed(notification) => {
                ThreadHistoryEventPayload::TurnExecutionWindowCheckpointed(notification)
            }
            TurnEventPayload::TurnExecutionWindowContinued(notification) => {
                ThreadHistoryEventPayload::TurnExecutionWindowContinued(notification)
            }
            TurnEventPayload::TurnExecutionWindowBlocked(notification) => {
                ThreadHistoryEventPayload::TurnExecutionWindowBlocked(notification)
            }
            TurnEventPayload::TurnPermissionAudit(event) => {
                ThreadHistoryEventPayload::TurnPermissionAudit(event)
            }
            TurnEventPayload::TurnMessageEdited(_) | TurnEventPayload::TurnMessageDeleted(_) => {
                continue;
            }
            TurnEventPayload::TurnCompleted(mut notification) => {
                if notification.turn.mode == ThreadMode::Message {
                    notification.turn = current_turns
                        .get(row.turn_id.as_str())
                        .filter(|turn| turn.mode == ThreadMode::Message)
                        .cloned()
                        .context("current completed Message history projection is unavailable")?;
                }
                ThreadHistoryEventPayload::TurnCompleted {
                    workspace_id: notification.workspace_id,
                    thread_id: notification.thread_id,
                    turn: notification.turn,
                }
            }
            TurnEventPayload::TurnFailed(notification) => ThreadHistoryEventPayload::TurnFailed {
                workspace_id: notification.workspace_id,
                thread_id: notification.thread_id,
                turn: notification.turn,
            },
            TurnEventPayload::TurnBlocked(notification) => ThreadHistoryEventPayload::TurnBlocked {
                workspace_id: notification.workspace_id,
                thread_id: notification.thread_id,
                turn: notification.turn,
                resume: notification.resume,
            },
        };

        events.push(ThreadHistoryEvent {
            turn_id: row.turn_id,
            sequence: row.sequence,
            created_at: row.created_at.timestamp_millis(),
            payload: mapped_payload,
        });
    }

    Ok(Some(ThreadHistorySnapshot {
        workspace_id,
        events,
    }))
}
