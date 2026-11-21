//! Client item event pages and the latest snapshot-only diff observations.
use anyhow::{Context, Result};
use pioneer_entity::{thread, turn, turn_item};
use pioneer_protocol::{TurnItem, TurnItemEvent, TurnItemEventPayload, TurnItemsResponse};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ColumnTrait, EntityTrait, JoinType, QueryFilter, QueryOrder, QuerySelect};
use std::collections::HashMap;

use crate::{events::TurnEventPayload, turn_event_reads, turn_payload};

pub async fn get_turn_item_events_page(
    db: &SqliteDatabase,
    thread_id: &str,
    turn_id: &str,
    after_sequence: Option<i64>,
    limit: usize,
) -> Result<Option<TurnItemsResponse>> {
    let limit = limit.max(1);
    let reader = db.begin_read().await?;
    // The page exists whenever its thread exists, including before any turn
    // with this ID has materialized.
    let Some(workspace_id) = thread::Entity::find_by_id(thread_id)
        .select_only()
        .column(thread::Column::WorkspaceId)
        .into_tuple::<String>()
        .one(&reader)
        .await?
    else {
        reader.commit().await?;
        return Ok(None);
    };
    let mut events_rows = turn_event_reads::turn_event_page_query(
        thread_id,
        turn_id,
        after_sequence,
        Some(limit.saturating_add(1) as u64),
    )
    .all(&reader)
    .await?;
    let has_more = events_rows.len() > limit;
    events_rows.truncate(limit);
    let body = turn_payload::capture_events(&reader, events_rows).await?;
    let snapshots = if has_more {
        Vec::new()
    } else {
        let rows = turn_item::Entity::find()
            .join(
                JoinType::InnerJoin,
                turn_item::Entity::belongs_to(turn::Entity)
                    .from(turn_item::Column::TurnId)
                    .to(turn::Column::Id)
                    .into(),
            )
            .filter(turn::Column::ThreadId.eq(thread_id))
            .filter(turn_item::Column::TurnId.eq(turn_id))
            .filter(turn_item::Column::ItemType.eq("system_event"))
            .order_by_asc(turn_item::Column::CreatedAt)
            .order_by_asc(turn_item::Column::ItemId)
            .all(&reader)
            .await?;
        turn_payload::capture_items(&reader, rows).await?
    };
    reader.commit().await?;
    let events_rows = turn_payload::decode_events(body)?;
    let snapshots = turn_payload::decode_items(snapshots)?;

    let mut events = Vec::new();
    let mut last_sequence = after_sequence.unwrap_or(0);
    let mut latest_agent_diff_payload_by_item_id = HashMap::<String, String>::new();

    for row in events_rows {
        last_sequence = row.sequence;

        let payload = serde_json::from_value::<TurnEventPayload>(
            row.payload.context("missing turn event payload")?,
        )
        .with_context(|| format!("failed to decode turn_event payload `{}`", row.id))?;

        let mapped_payload = match payload {
            TurnEventPayload::ItemStarted(notification) => TurnItemEventPayload::ItemStarted {
                workspace_id: workspace_id.clone(),
                thread_id: notification.thread_id,
                turn_id: notification.turn_id,
                item: notification.item,
            },
            TurnEventPayload::ItemCompleted(notification) => {
                remember_agent_diff_snapshot_payload(
                    &mut latest_agent_diff_payload_by_item_id,
                    &notification.item,
                )?;
                TurnItemEventPayload::ItemCompleted {
                    workspace_id: workspace_id.clone(),
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item: notification.item,
                }
            }
            TurnEventPayload::ItemUpdated(notification) => {
                remember_agent_diff_snapshot_payload(
                    &mut latest_agent_diff_payload_by_item_id,
                    &notification.item,
                )?;
                TurnItemEventPayload::ItemUpdated {
                    workspace_id: workspace_id.clone(),
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item: notification.item,
                }
            }
            TurnEventPayload::ItemTimeoutDetected(notification) => {
                TurnItemEventPayload::ItemTimeoutDetected {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::ItemRecoveryOpened {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::ItemRecoveryAttached {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::ItemRetryScheduled {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::ItemRetryAttemptStarted {
                    workspace_id: workspace_id.clone(),
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                }
            }
            TurnEventPayload::ItemRecoverySucceeded(notification) => {
                TurnItemEventPayload::ItemRecoverySucceeded {
                    workspace_id: workspace_id.clone(),
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                }
            }
            TurnEventPayload::ItemRecoveryExhausted(notification) => {
                TurnItemEventPayload::ItemRecoveryExhausted {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::ItemToolRetryScheduled {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::ItemToolRetryResolved {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::ItemToolRetryExhausted {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::TurnToolLoopBudgetExceeded {
                    workspace_id: workspace_id.clone(),
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
                TurnItemEventPayload::TurnExecutionWindowStarted(notification)
            }
            TurnEventPayload::TurnExecutionWindowExhausted(notification) => {
                TurnItemEventPayload::TurnExecutionWindowExhausted(notification)
            }
            TurnEventPayload::TurnExecutionWindowCheckpointed(notification) => {
                TurnItemEventPayload::TurnExecutionWindowCheckpointed(notification)
            }
            TurnEventPayload::TurnExecutionWindowContinued(notification) => {
                TurnItemEventPayload::TurnExecutionWindowContinued(notification)
            }
            TurnEventPayload::TurnExecutionWindowBlocked(notification) => {
                TurnItemEventPayload::TurnExecutionWindowBlocked(notification)
            }
            TurnEventPayload::TurnPermissionAudit(event) => {
                TurnItemEventPayload::TurnPermissionAudit(event)
            }
            TurnEventPayload::TurnStarted(_)
            | TurnEventPayload::TurnMessageEdited(_)
            | TurnEventPayload::TurnMessageDeleted(_)
            | TurnEventPayload::TurnCompleted(_)
            | TurnEventPayload::TurnFailed(_)
            | TurnEventPayload::TurnBlocked(_) => continue,
        };

        events.push(TurnItemEvent {
            sequence: row.sequence,
            created_at: row.created_at.timestamp_millis(),
            payload: mapped_payload,
        });
    }

    // Snapshot-only diff rows are appended exactly once, after the durable
    // event cursor reaches the final page. Appending them to every page
    // would duplicate the latest diff during incremental replay and could
    // make a sequence cursor cycle forever.
    if !has_more {
        append_agent_diff_snapshot_turn_item_events(
            snapshots,
            &mut events,
            &latest_agent_diff_payload_by_item_id,
            workspace_id.as_str(),
            thread_id,
            turn_id,
            last_sequence,
        )?;
    }

    Ok(Some(TurnItemsResponse {
        thread_id: thread_id.to_owned(),
        workspace_id,
        turn_id: turn_id.to_owned(),
        events,
        last_sequence,
        has_more,
        next_cursor: has_more.then_some(last_sequence),
    }))
}

fn remember_agent_diff_snapshot_payload(
    latest_agent_diff_payload_by_item_id: &mut HashMap<String, String>,
    item: &TurnItem,
) -> Result<()> {
    if !is_agent_diff_updated_item(item) {
        return Ok(());
    }

    let payload_json =
        serde_json::to_string(item).context("failed to serialize agent diff turn item payload")?;
    latest_agent_diff_payload_by_item_id.insert(item.item_id().to_owned(), payload_json);
    Ok(())
}

pub async fn latest_agent_diff_raw_payload_for_item(
    db: &SqliteDatabase,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<String>> {
    let rows = turn_event_reads::list_events_for_turn(db, thread_id, turn_id, None, None).await?;
    let mut latest_payload = None;

    for row in rows {
        let payload = serde_json::from_value::<TurnEventPayload>(
            row.payload.context("missing turn event payload")?,
        )
        .with_context(|| format!("failed to decode turn_event payload `{}`", row.id))?;
        let item = match payload {
            TurnEventPayload::ItemCompleted(notification) => notification.item,
            TurnEventPayload::ItemUpdated(notification) => notification.item,
            _ => continue,
        };
        if item.item_id() != item_id || !is_agent_diff_updated_item(&item) {
            continue;
        }
        latest_payload = Some(
            serde_json::to_string(&item)
                .context("failed to serialize raw agent diff turn item payload")?,
        );
    }

    Ok(latest_payload)
}

fn append_agent_diff_snapshot_turn_item_events(
    rows: Vec<turn_item::Model>,
    events: &mut Vec<TurnItemEvent>,
    latest_agent_diff_payload_by_item_id: &HashMap<String, String>,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    last_sequence: i64,
) -> Result<()> {
    for row in rows {
        let item = turn_payload::decode_item(&row).with_context(|| {
            format!(
                "failed to decode snapshot turn_item payload for turn `{turn_id}` item `{}`",
                row.item_id
            )
        })?;
        if !is_agent_diff_updated_item(&item) {
            continue;
        }

        let payload_json = serde_json::to_string(&item)
            .context("failed to serialize snapshot agent diff turn item payload")?;
        if latest_agent_diff_payload_by_item_id
            .get(item.item_id())
            .is_some_and(|latest_payload| latest_payload == &payload_json)
        {
            continue;
        }

        events.push(TurnItemEvent {
            sequence: last_sequence,
            created_at: row.updated_at.timestamp_millis(),
            payload: TurnItemEventPayload::ItemUpdated {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item,
            },
        });
    }

    Ok(())
}

pub fn is_agent_diff_updated_item(item: &TurnItem) -> bool {
    matches!(
        item,
        TurnItem::SystemEvent {
            code: Some(code),
            ..
        } if code == "agent_diff_updated"
    )
}
