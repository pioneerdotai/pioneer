//! Consistent task and execution diagnostic snapshot.

use anyhow::{Context, Result};
use pioneer_crud_execution::attempt_values::{
    ATTEMPT_STATUS_RUNNING, TURN_ITEM_STATUS_CANCELLED, TURN_ITEM_STATUS_COMPLETED,
    TURN_ITEM_STATUS_FAILED, TURN_ITEM_STATUS_TIMED_OUT,
};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use std::collections::HashMap;

use crate::task_actor_contract::TerminalTaskOccurrenceMismatch;

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantSnapshot {
    pub task_events: Vec<TaskRuntimeInvariantEventRecord>,
    pub delivered_task_results: Vec<TaskRuntimeInvariantDeliveryRecord>,
    pub terminal_occurrence_mismatches: Vec<TerminalTaskOccurrenceMismatch>,
    pub in_progress_turns: Vec<TaskRuntimeInvariantTurnRecord>,
    pub stale_turn_item_attempts: Vec<TaskRuntimeInvariantStaleAttemptRecord>,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantEventRecord {
    pub id: String,
    pub task_id: String,
    pub run_id: Option<String>,
    pub sequence: i64,
    pub event_type: String,
    pub payload_json: String,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantDeliveryRecord {
    pub delivery_id: String,
    pub task_id: String,
    pub run_id: String,
    pub run_status: Option<String>,
    pub result_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantTurnRecord {
    pub turn_id: String,
    pub thread_id: Option<String>,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct TaskRuntimeInvariantStaleAttemptRecord {
    pub turn_id: String,
    pub item_id: String,
    pub item_status: String,
    pub attempt_id: String,
    pub attempt_status: String,
    pub attempt_number: i64,
}

pub async fn load_task_runtime_invariant_snapshot(
    db: &PostgresDatabase,
) -> Result<TaskRuntimeInvariantSnapshot> {
    let reader = db.begin_read().await?;
    let event_rows = pioneer_entity::task_event::Entity::find()
        .order_by_asc(pioneer_entity::task_event::Column::TaskId)
        .order_by_asc(pioneer_entity::task_event::Column::Sequence)
        .all(&reader)
        .await
        .context("failed to load task events for runtime invariant scan")?;

    let task_runs_by_id = pioneer_entity::task_run::Entity::find()
        .select_only()
        .columns([
            pioneer_entity::task_run::Column::Id,
            pioneer_entity::task_run::Column::Status,
            pioneer_entity::task_run::Column::ResultJson,
        ])
        .into_tuple::<(String, String, Option<String>)>()
        .all(&reader)
        .await
        .context("failed to load task runs for runtime invariant scan")?
        .into_iter()
        .map(|(id, status, result)| (id, (status, result)))
        .collect::<HashMap<_, _>>();

    let delivered_task_results = pioneer_entity::task_delivery::Entity::find()
        .filter(pioneer_entity::task_delivery::Column::Status.eq("delivered"))
        .order_by_asc(pioneer_entity::task_delivery::Column::CreatedAt)
        .all(&reader)
        .await
        .context("failed to load delivered task deliveries for runtime invariant scan")?
        .into_iter()
        .map(|delivery| {
            let run = task_runs_by_id.get(delivery.run_id.as_str());
            TaskRuntimeInvariantDeliveryRecord {
                delivery_id: delivery.id,
                task_id: delivery.task_id,
                run_id: delivery.run_id,
                run_status: run.map(|(status, _)| status.clone()),
                result_json: run.and_then(|(_, result)| result.clone()),
            }
        })
        .collect::<Vec<_>>();

    let terminal_occurrence_mismatches =
        crate::task_actor_contract::list_terminal_task_occurrence_mismatches(&reader, u64::MAX)
            .await?;

    let in_progress_turns = pioneer_entity::turn::Entity::find()
        .filter(pioneer_entity::turn::Column::Status.eq("in_progress"))
        .order_by_asc(pioneer_entity::turn::Column::UpdatedAt)
        .all(&reader)
        .await
        .context("failed to load in-progress turns for runtime invariant scan")?
        .into_iter()
        .map(|row| TaskRuntimeInvariantTurnRecord {
            turn_id: row.id,
            thread_id: Some(row.thread_id),
            updated_at_unix: row.updated_at.timestamp(),
        })
        .collect::<Vec<_>>();

    let terminal_items = pioneer_entity::turn_item::Entity::find()
        .select_only()
        .columns([
            pioneer_entity::turn_item::Column::TurnId,
            pioneer_entity::turn_item::Column::ItemId,
            pioneer_entity::turn_item::Column::Status,
        ])
        .filter(pioneer_entity::turn_item::Column::Status.is_in([
            TURN_ITEM_STATUS_COMPLETED,
            TURN_ITEM_STATUS_FAILED,
            TURN_ITEM_STATUS_TIMED_OUT,
            TURN_ITEM_STATUS_CANCELLED,
        ]))
        .into_tuple::<(String, String, String)>()
        .all(&reader)
        .await
        .context("failed to load terminal turn items for runtime invariant scan")?
        .into_iter()
        .map(|(turn, item, status)| ((turn, item), status))
        .collect::<HashMap<_, _>>();

    let stale_turn_item_attempts = pioneer_entity::turn_item_attempt::Entity::find()
        .filter(
            pioneer_entity::turn_item_attempt::Column::Status
                .is_in([ATTEMPT_STATUS_RUNNING, TURN_ITEM_STATUS_TIMED_OUT]),
        )
        .order_by_asc(pioneer_entity::turn_item_attempt::Column::TurnId)
        .order_by_asc(pioneer_entity::turn_item_attempt::Column::ItemId)
        .order_by_asc(pioneer_entity::turn_item_attempt::Column::AttemptNumber)
        .all(&reader)
        .await
        .context("failed to load turn item attempts for runtime invariant scan")?
        .into_iter()
        .filter_map(|attempt| {
            let key = (attempt.turn_id.clone(), attempt.item_id.clone());
            let item_status = terminal_items.get(&key)?;
            Some(TaskRuntimeInvariantStaleAttemptRecord {
                turn_id: attempt.turn_id,
                item_id: attempt.item_id,
                item_status: item_status.clone(),
                attempt_id: attempt.id,
                attempt_status: attempt.status,
                attempt_number: attempt.attempt_number,
            })
        })
        .collect::<Vec<_>>();

    reader.commit().await?;

    let task_events = event_rows
        .into_iter()
        .map(|mut row| {
            let payload_json = crate::event_log::EventPayload::read(&mut row)?;
            Ok(TaskRuntimeInvariantEventRecord {
                id: row.id,
                task_id: row.task_id,
                run_id: row.run_id,
                sequence: row.sequence,
                event_type: row.event_type,
                payload_json,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TaskRuntimeInvariantSnapshot {
        task_events,
        delivered_task_results,
        terminal_occurrence_mismatches,
        in_progress_turns,
        stale_turn_item_attempts,
    })
}
