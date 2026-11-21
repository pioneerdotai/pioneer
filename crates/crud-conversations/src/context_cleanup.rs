//! Context lifetime and shell receipt queries in the caller's transaction.
use anyhow::{Context, Result};
use pioneer_entity::{recovery_job, task_run_turn, turn, turn_llm_context};
use pioneer_protocol::TurnStatus;
use sea_orm::sea_query::{Expr, ExprTrait, Query, SelectStatement};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
};

use crate::turn_context::{TurnLlmContextEntry, entry_from_model};
use crate::values::turn_status_to_db;

/// Context rows retained by cleanup and admitted to newly built model history.
/// Exact source lookup for existing frozen histories remains independent of this rule.
pub const MODEL_HISTORY_CONTEXT_SOURCES: [&str; 5] = [
    "assistant_round",
    "tool_result_v2",
    "turn_input_projection_v1",
    "runtime_context_v1",
    "system_snapshot_v1",
];

pub fn model_history_context_source(source: &str) -> bool {
    MODEL_HISTORY_CONTEXT_SOURCES.contains(&source)
}

pub async fn find_existing_tool_result<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: Option<&str>,
) -> Result<Option<TurnLlmContextEntry>> {
    Ok(turn_llm_context::Entity::find()
        .filter(turn_llm_context::Column::TurnId.eq(turn_id))
        .filter(turn_llm_context::Column::ItemId.eq(item_id))
        .filter(turn_llm_context::Column::Source.eq("tool_result_v2"))
        .order_by_desc(turn_llm_context::Column::Sequence)
        .one(db)
        .await?
        .map(entry_from_model))
}

fn task_child_turn(turn_id: Expr) -> Expr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from(task_run_turn::Entity)
            .and_where(
                Expr::col((task_run_turn::Entity, task_run_turn::Column::TurnId)).eq(turn_id),
            )
            .to_owned(),
    )
}

/// Bounded point lookup for control-only receipts. A receipt is never model
/// history and uses the existing temporary context cleanup lifecycle.
pub async fn shell_control_receipt<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    item: &str,
) -> Result<Option<String>> {
    use pioneer_entity::thread;
    use sea_orm::JoinType;
    turn_llm_context::Entity::find()
        .select_only()
        .column(turn_llm_context::Column::Payload)
        .join(
            JoinType::InnerJoin,
            turn_llm_context::Entity::belongs_to(turn::Entity)
                .from(turn_llm_context::Column::TurnId)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .filter(thread::Column::Id.eq(thread_id))
        .filter(turn_llm_context::Column::TurnId.eq(turn_id))
        .filter(turn_llm_context::Column::ItemId.eq(item))
        .filter(turn_llm_context::Column::Source.eq("shell_control_v1"))
        .order_by_desc(turn_llm_context::Column::Sequence)
        .limit(1)
        .into_tuple::<String>()
        .one(db)
        .await
        .map_err(Into::into)
}

// Retain control receipts while recovery owns their facts. Canonical append or
// definitive lifecycle settlement releases them; they are never model history.
fn shell_control_releasable(settled_child: bool) -> sea_orm::Condition {
    sea_orm::Condition::any()
        .add(turn_llm_context::Column::Source.ne("shell_control_v1"))
        .add(Expr::exists(
            Query::select()
                .expr(Expr::val(1_i64))
                .from_as(turn_llm_context::Entity, "shell_canonical")
                .and_where(
                    Expr::col(("shell_canonical", turn_llm_context::Column::TurnId)).eq(Expr::col(
                        (turn_llm_context::Entity, turn_llm_context::Column::TurnId),
                    )),
                )
                .and_where(
                    Expr::col(("shell_canonical", turn_llm_context::Column::ItemId)).eq(Expr::col(
                        (turn_llm_context::Entity, turn_llm_context::Column::ItemId),
                    )),
                )
                .and_where(
                    Expr::col(("shell_canonical", turn_llm_context::Column::Source))
                        .eq("tool_result_v2"),
                )
                .to_owned(),
        ))
        .add(closed_shell_recovery_obligation(settled_child))
}
// Positive terminal execution evidence plus absence of durable recovery owners
// closes the undelivered-result obligation. A settled blocked job is audit only
// once these Turn/execution proofs hold; a resumable Blocked Turn never qualifies.
fn closed_shell_recovery_obligation(settled_child: bool) -> sea_orm::Condition {
    use pioneer_entity::{recovery_terminalization_outbox as outbox, turn_execution};
    // Use Entity predicates for all ownership/state checks in the deleting statement.
    let mut closed =
        sea_orm::Condition::all()
            .add(Expr::exists(
                Query::select()
                    .expr(Expr::val(1_i64))
                    .from(turn::Entity)
                    .and_where(Expr::col((turn::Entity, turn::Column::Id)).eq(Expr::col((
                        turn_llm_context::Entity,
                        turn_llm_context::Column::TurnId,
                    ))))
                    .and_where(Expr::col((turn::Entity, turn::Column::Status)).is_in([
                        "completed",
                        "failed",
                        "interrupted",
                    ]))
                    .to_owned(),
            ))
            .add(Expr::exists(
                Query::select()
                    .expr(Expr::val(1_i64))
                    .from(turn_execution::Entity)
                    .and_where(
                        Expr::col((turn_execution::Entity, turn_execution::Column::TurnId)).eq(
                            Expr::col((turn_llm_context::Entity, turn_llm_context::Column::TurnId)),
                        ),
                    )
                    .and_where(
                        Expr::col((turn_execution::Entity, turn_execution::Column::Status))
                            .is_in(["completed", "failed", "interrupted", "blocked"]),
                    )
                    .and_where(
                        Expr::col((turn_execution::Entity, turn_execution::Column::CompletedAt))
                            .is_not_null(),
                    )
                    .to_owned(),
            ))
            .add(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from(recovery_job::Entity)
                        .and_where(
                            Expr::col((recovery_job::Entity, recovery_job::Column::TurnId)).eq(
                                Expr::col((
                                    turn_llm_context::Entity,
                                    turn_llm_context::Column::TurnId,
                                )),
                            ),
                        )
                        .and_where(
                            Expr::col((recovery_job::Entity, recovery_job::Column::Status))
                                .is_in(["pending", "active"])
                                .or(Expr::col((
                                    recovery_job::Entity,
                                    recovery_job::Column::ResolutionPending,
                                ))
                                .eq(true)),
                        )
                        .to_owned(),
                )
                .not(),
            )
            .add(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from(outbox::Entity)
                        .and_where(Expr::col((outbox::Entity, outbox::Column::TurnId)).eq(
                            Expr::col((turn_llm_context::Entity, turn_llm_context::Column::TurnId)),
                        ))
                        .and_where(
                            Expr::col((outbox::Entity, outbox::Column::Status))
                                .is_not_in(["delivered", "cancelled"]),
                        )
                        .to_owned(),
                )
                .not(),
            );
    if !settled_child {
        closed = closed.add(
            task_child_turn(Expr::col((
                turn_llm_context::Entity,
                turn_llm_context::Column::TurnId,
            )))
            .not(),
        );
    }
    closed
}

pub async fn delete_settled_shell_controls<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    Ok(turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::TurnId.eq(turn_id))
        .filter(turn_llm_context::Column::Source.eq("shell_control_v1"))
        .filter(shell_control_releasable(false))
        .exec(db)
        .await?
        .rows_affected)
}

fn recovery_turns(job_ids: &[String]) -> SelectStatement {
    recovery_job::Entity::find()
        .select_only()
        .column(recovery_job::Column::TurnId)
        .filter(recovery_job::Column::Id.is_in(job_ids.to_vec()))
        .into_query()
}

pub async fn shell_child_turns_for_recovery_jobs<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
) -> Result<Vec<String>> {
    // Recovery can release a parked child dependency without another Turn/link
    // update. Republish only affected shell capsules to their existing owner.
    Ok(turn_llm_context::Entity::find()
        .select_only()
        .column(turn_llm_context::Column::TurnId)
        .distinct()
        .filter(turn_llm_context::Column::Source.eq("shell_control_v1"))
        .filter(turn_llm_context::Column::TurnId.in_subquery(recovery_turns(job_ids)))
        .filter(
            turn_llm_context::Column::TurnId.in_subquery(
                task_run_turn::Entity::find()
                    .select_only()
                    .column(task_run_turn::Column::TurnId)
                    .into_query(),
            ),
        )
        .limit(job_ids.len() as u64)
        .into_tuple::<String>()
        .all(db)
        .await?)
}

pub async fn delete_shell_controls_for_recovery_jobs<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
) -> Result<u64> {
    Ok(turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::Source.eq("shell_control_v1"))
        .filter(shell_control_releasable(false))
        .filter(turn_llm_context::Column::TurnId.in_subquery(recovery_turns(job_ids)))
        .exec(db)
        .await?
        .rows_affected)
}

pub async fn delete_turn_llm_context_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    delete_turn_llm_context_for_turn_inner(db, turn_id, false).await
}

/// Only the existing authoritative child-cleanup transaction may use this after
/// its READY proof. Active recovery/outbox obligations still prevent release.
pub async fn delete_turn_llm_context_for_settled_child<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    delete_turn_llm_context_for_turn_inner(db, turn_id, true).await
}

async fn delete_turn_llm_context_for_turn_inner<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    settled_child: bool,
) -> Result<u64> {
    let deleted = turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::Source.is_not_in(MODEL_HISTORY_CONTEXT_SOURCES))
        .filter(shell_control_releasable(settled_child))
        .filter(turn_llm_context::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete turn_llm_context rows for turn")?;
    Ok(deleted.rows_affected)
}

pub async fn delete_expired_turn_llm_context<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let deleted = turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::Source.is_not_in(MODEL_HISTORY_CONTEXT_SOURCES))
        .filter(shell_control_releasable(false))
        // The task-child owner retains pending projection capsules across
        // restart; TTL cleanup must not consume its durable obligation.
        .filter(
            task_child_turn(Expr::col((
                turn_llm_context::Entity,
                turn_llm_context::Column::TurnId,
            )))
            .not(),
        )
        .filter(turn_llm_context::Column::ExpiresAt.is_not_null())
        .filter(turn_llm_context::Column::ExpiresAt.lte(chrono::Utc::now().fixed_offset()))
        .exec(db)
        .await
        .context("failed to delete expired turn_llm_context rows")?;
    Ok(deleted.rows_affected)
}

pub async fn delete_turn_llm_context_for_terminal_turns<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let terminal_statuses = [
        turn_status_to_db(TurnStatus::Completed).to_owned(),
        turn_status_to_db(TurnStatus::Failed).to_owned(),
        turn_status_to_db(TurnStatus::Interrupted).to_owned(),
        turn_status_to_db(TurnStatus::Blocked).to_owned(),
    ];

    let terminal_turn_ids = turn::Entity::find()
        .select_only()
        .column(turn::Column::Id)
        .filter(turn::Column::Status.is_in(terminal_statuses))
        .filter(task_child_turn(Expr::col((turn::Entity, turn::Column::Id))).not())
        .into_query();

    let deleted = turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::Source.is_not_in(MODEL_HISTORY_CONTEXT_SOURCES))
        .filter(shell_control_releasable(false))
        .filter(turn_llm_context::Column::TurnId.in_subquery(terminal_turn_ids))
        .exec(db)
        .await
        .context("failed to delete terminal turn_llm_context rows")?;
    Ok(deleted.rows_affected)
}
