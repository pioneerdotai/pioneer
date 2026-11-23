//! Task review state consumed by the runtime invariant checker.

use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantSnapshot {
    pub thread_lineage: Vec<TaskReviewInvariantThreadLineageRecord>,
    pub primary_bindings: Vec<TaskReviewInvariantBindingRecord>,
    pub agent_specs: Vec<TaskReviewInvariantAgentSpecRecord>,
    pub task_run_turns: Vec<TaskReviewInvariantTurnRecord>,
    pub task_result_candidates: Vec<TaskReviewInvariantCandidateRecord>,
    pub task_result_review_events: Vec<TaskReviewInvariantReviewEventRecord>,
    pub task_runs: Vec<TaskReviewInvariantRunRecord>,
    pub write_locks: Vec<TaskReviewInvariantWriteLockRecord>,
    pub turn_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantThreadLineageRecord {
    pub child_thread_id: String,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantBindingRecord {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub execution_id: Option<String>,
    pub thread_id: String,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantAgentSpecRecord {
    pub task_id: String,
    pub run_id: Option<String>,
    pub tool_policy_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantTurnRecord {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub kind: String,
    pub round: i64,
    pub sequence: i64,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantCandidateRecord {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub task_run_turn_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub round: i64,
    pub status: String,
    pub result_json: Option<String>,
    pub final_review_event_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantReviewEventRecord {
    pub id: String,
    pub candidate_id: String,
    pub task_id: String,
    pub run_id: String,
    pub task_run_turn_id: String,
    pub decision: String,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantRunRecord {
    pub id: String,
    pub task_id: String,
    pub status: String,
    pub result_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskReviewInvariantWriteLockRecord {
    pub task_id: String,
    pub run_id: String,
    pub status: String,
    pub expires_at_unix: Option<i64>,
}

pub async fn load_task_review_invariant_snapshot(
    db: &impl ConnectionTrait,
) -> Result<TaskReviewInvariantSnapshot> {
    let thread_lineage = pioneer_entity::thread_lineage::Entity::find()
        .all(db)
        .await
        .context("failed to load task review thread lineage")?
        .into_iter()
        .map(|row| TaskReviewInvariantThreadLineageRecord {
            child_thread_id: row.child_thread_id,
        })
        .collect();
    let primary_bindings = pioneer_entity::task_run_thread_binding::Entity::find()
        .filter(pioneer_entity::task_run_thread_binding::Column::BindingKind.eq("primary_executor"))
        .all(db)
        .await
        .context("failed to load task review thread bindings")?
        .into_iter()
        .map(|row| TaskReviewInvariantBindingRecord {
            id: row.id,
            task_id: row.task_id,
            run_id: row.run_id,
            execution_id: row.execution_id,
            thread_id: row.thread_id,
        })
        .collect();
    let agent_specs = pioneer_entity::task_agent_spec::Entity::find()
        .all(db)
        .await
        .context("failed to load task review agent specs")?
        .into_iter()
        .map(|row| TaskReviewInvariantAgentSpecRecord {
            task_id: row.task_id,
            run_id: row.run_id,
            tool_policy_json: row.tool_policy_json.map(|value| value.to_string()),
        })
        .collect();
    let task_run_turns = pioneer_entity::task_run_turn::Entity::find()
        .all(db)
        .await
        .context("failed to load task review turns")?
        .into_iter()
        .map(|row| TaskReviewInvariantTurnRecord {
            id: row.id,
            task_id: row.task_id,
            run_id: row.run_id,
            thread_id: row.thread_id,
            turn_id: row.turn_id,
            kind: row.kind,
            round: i64::from(row.round),
            sequence: i64::from(row.sequence),
        })
        .collect();
    let task_result_candidates = pioneer_entity::task_result_candidate::Entity::find()
        .all(db)
        .await
        .context("failed to load task result candidates")?
        .into_iter()
        .map(|row| TaskReviewInvariantCandidateRecord {
            id: row.id,
            task_id: row.task_id,
            run_id: row.run_id,
            task_run_turn_id: row.task_run_turn_id,
            thread_id: row.thread_id,
            turn_id: row.turn_id,
            round: i64::from(row.round),
            status: row.status,
            result_json: row.result_json,
            final_review_event_id: row.final_review_event_id,
        })
        .collect();
    let task_result_review_events = pioneer_entity::task_result_review_event::Entity::find()
        .all(db)
        .await
        .context("failed to load task result review events")?
        .into_iter()
        .map(|row| TaskReviewInvariantReviewEventRecord {
            id: row.id,
            candidate_id: row.candidate_id,
            task_id: row.task_id,
            run_id: row.run_id,
            task_run_turn_id: row.task_run_turn_id,
            decision: row.decision,
        })
        .collect();
    let task_runs = pioneer_entity::task_run::Entity::find()
        .all(db)
        .await
        .context("failed to load task runs")?
        .into_iter()
        .map(|row| TaskReviewInvariantRunRecord {
            id: row.id,
            task_id: row.task_id,
            status: row.status,
            result_json: row.result_json,
        })
        .collect();
    let write_locks = pioneer_entity::task_write_lock::Entity::find()
        .all(db)
        .await
        .context("failed to load task review write locks")?
        .into_iter()
        .map(|row| TaskReviewInvariantWriteLockRecord {
            task_id: row.task_id,
            run_id: row.run_id,
            status: row.status,
            expires_at_unix: row.expires_at.map(|value| value.timestamp()),
        })
        .collect();
    let turn_ids = pioneer_entity::turn::Entity::find()
        .all(db)
        .await
        .context("failed to load turn ids")?
        .into_iter()
        .map(|row| row.id)
        .collect();

    Ok(TaskReviewInvariantSnapshot {
        thread_lineage,
        primary_bindings,
        agent_specs,
        task_run_turns,
        task_result_candidates,
        task_result_review_events,
        task_runs,
        write_locks,
        turn_ids,
    })
}
