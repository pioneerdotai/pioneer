use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_llm_context};
use pioneer_protocol::{TurnStatus, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
    Set,
};

use crate::convention::turn_status_to_db;
use crate::repositories::compaction::MODEL_HISTORY_CONTEXT_SOURCES;

const DB_ID_LEN: usize = 21;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnLlmContextEntry {
    pub id: String,
    pub turn_id: String,
    pub item_id: Option<String>,
    pub attempt_id: Option<String>,
    pub sequence: i64,
    pub source: String,
    pub tool_name: Option<String>,
    pub payload: String,
    pub output_policy_snapshot: String,
    pub created_at: DateTimeWithTimeZone,
    pub expires_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnLlmContextAppendOutcome {
    pub entry: TurnLlmContextEntry,
    pub already_committed: bool,
}

#[derive(Debug, Clone)]
pub struct NewTurnLlmContextEntry {
    pub turn_id: String,
    pub item_id: Option<String>,
    pub attempt_id: Option<String>,
    pub sequence: i64,
    pub source: String,
    pub tool_name: Option<String>,
    pub payload: String,
    pub output_policy_snapshot: String,
    pub created_at: DateTimeWithTimeZone,
    pub expires_at: Option<DateTimeWithTimeZone>,
}

pub async fn insert_turn_llm_context<C: ConnectionTrait>(
    db: &C,
    entry: NewTurnLlmContextEntry,
) -> Result<turn_llm_context::Model> {
    let child_turn = entry.turn_id.clone();
    let creates_capsule =
        super::task_child_producers::context_will_create(db, &entry.turn_id, &entry.source).await?;
    let id = generate_id(DB_ID_LEN);
    turn_llm_context::Entity::insert(turn_llm_context::ActiveModel {
        id: Set(id.clone()),
        turn_id: Set(entry.turn_id),
        item_id: Set(entry.item_id),
        attempt_id: Set(entry.attempt_id),
        sequence: Set(entry.sequence),
        source: Set(entry.source),
        tool_name: Set(entry.tool_name),
        payload: Set(entry.payload),
        output_policy_snapshot: Set(entry.output_policy_snapshot),
        delivery_key: Set(None),
        created_at: Set(entry.created_at),
        expires_at: Set(entry.expires_at),
    })
    .exec(db)
    .await
    .context("failed to insert turn_llm_context row")?;

    if creates_capsule {
        super::task_child_producers::capsule_birth(db, &child_turn).await?;
    }
    turn_llm_context::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload inserted turn_llm_context row")?
        .context("inserted turn_llm_context row is missing")
}

pub async fn append_turn_llm_context<C: ConnectionTrait>(
    db: &C,
    mut entry: NewTurnLlmContextEntry,
    delivery_key: &str,
) -> Result<TurnLlmContextAppendOutcome> {
    let delivery_key = delivery_key.trim();
    if delivery_key.is_empty() {
        anyhow::bail!("turn_llm_context delivery key must not be empty");
    }
    if let Some(existing) = turn_llm_context::Entity::find()
        .filter(turn_llm_context::Column::TurnId.eq(entry.turn_id.clone()))
        .filter(turn_llm_context::Column::DeliveryKey.eq(delivery_key.to_owned()))
        .one(db)
        .await
        .context("failed to query idempotent turn_llm_context delivery")?
    {
        if existing.item_id != entry.item_id
            || existing.attempt_id != entry.attempt_id
            || existing.source != entry.source
            || existing.tool_name != entry.tool_name
            || existing.payload != entry.payload
            || existing.output_policy_snapshot != entry.output_policy_snapshot
            || existing.expires_at != entry.expires_at
        {
            anyhow::bail!(
                "turn_llm_context delivery key collision for turn `{}`",
                entry.turn_id
            );
        }
        return Ok(TurnLlmContextAppendOutcome {
            entry: entry_from_model(existing),
            already_committed: true,
        });
    }

    entry.sequence = next_sequence_for_turn(db, entry.turn_id.as_str()).await?;
    let child_turn = entry.turn_id.clone();
    let creates_capsule =
        super::task_child_producers::context_will_create(db, &entry.turn_id, &entry.source).await?;
    let id = generate_id(DB_ID_LEN);
    turn_llm_context::Entity::insert(turn_llm_context::ActiveModel {
        id: Set(id.clone()),
        turn_id: Set(entry.turn_id),
        item_id: Set(entry.item_id),
        attempt_id: Set(entry.attempt_id),
        sequence: Set(entry.sequence),
        source: Set(entry.source),
        tool_name: Set(entry.tool_name),
        payload: Set(entry.payload),
        output_policy_snapshot: Set(entry.output_policy_snapshot),
        delivery_key: Set(Some(delivery_key.to_owned())),
        created_at: Set(entry.created_at),
        expires_at: Set(entry.expires_at),
    })
    .exec(db)
    .await
    .context("failed to append turn_llm_context row")?;

    if creates_capsule {
        super::task_child_producers::capsule_birth(db, &child_turn).await?;
    }
    let model = turn_llm_context::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload appended turn_llm_context row")?
        .context("appended turn_llm_context row is missing")?;
    Ok(TurnLlmContextAppendOutcome {
        entry: entry_from_model(model),
        already_committed: false,
    })
}

/// Bounded point lookup for control-only receipts. A receipt is never model
/// history and uses the existing temporary context cleanup lifecycle.
pub(crate) async fn shell_control_receipt<C: ConnectionTrait>(
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

/// Return an already committed canonical result instead of replacing its bytes.
/// The caller validates the terminal source revision in this same transaction.
pub(crate) async fn append_recovered_shell_result<C: ConnectionTrait>(
    db: &C,
    entry: NewTurnLlmContextEntry,
    delivery_key: &str,
) -> Result<TurnLlmContextAppendOutcome> {
    anyhow::ensure!(
        entry.source == "tool_result_v2" && entry.item_id.is_some(),
        "invalid recovered shell result"
    );
    if let Some(existing) = turn_llm_context::Entity::find()
        .filter(turn_llm_context::Column::TurnId.eq(&entry.turn_id))
        .filter(turn_llm_context::Column::ItemId.eq(entry.item_id.clone()))
        .filter(turn_llm_context::Column::Source.eq("tool_result_v2"))
        .order_by_desc(turn_llm_context::Column::Sequence)
        .one(db)
        .await?
    {
        return Ok(TurnLlmContextAppendOutcome {
            entry: entry_from_model(existing),
            already_committed: true,
        });
    }
    append_turn_llm_context(db, entry, delivery_key).await
}

async fn next_sequence_for_turn<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<i64> {
    let max_sequence = db
        .query_one(
            &Query::select()
                .expr_as(
                    Expr::cust("COALESCE(MAX(sequence), 0)"),
                    Alias::new("max_sequence"),
                )
                .from(Alias::new("turn_llm_context"))
                .and_where(Expr::col(Alias::new("turn_id")).eq(turn_id.to_owned()))
                .to_owned(),
        )
        .await
        .context("failed to query max turn_llm_context sequence")?
        .and_then(|row| {
            row.try_get::<Option<i64>>("", "max_sequence")
                .ok()
                .flatten()
        })
        .unwrap_or(0);
    Ok(max_sequence.saturating_add(1))
}

pub async fn list_turn_llm_context<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<TurnLlmContextEntry>> {
    let rows = turn_llm_context::Entity::find()
        .filter(turn_llm_context::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(turn_llm_context::Column::Sequence)
        .all(db)
        .await
        .context("failed to list turn_llm_context rows")?;

    Ok(rows.into_iter().map(entry_from_model).collect())
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
    use pioneer_entity::{
        recovery_job, recovery_terminalization_outbox as outbox, task_run_turn, turn_execution,
    };
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
            Expr::exists(
                Query::select()
                    .expr(Expr::val(1_i64))
                    .from(task_run_turn::Entity)
                    .and_where(
                        Expr::col((task_run_turn::Entity, task_run_turn::Column::TurnId)).eq(
                            Expr::col((turn_llm_context::Entity, turn_llm_context::Column::TurnId)),
                        ),
                    )
                    .to_owned(),
            )
            .not(),
        );
    }
    closed
}

pub(crate) async fn delete_settled_shell_controls<C: ConnectionTrait>(
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

/// Outbox settlement already owns a bounded batch of jobs in its transaction.
/// Revalidate closure in the deleting statement; task-child capsules still
/// require their separate authoritative READY proof.
pub(crate) async fn settle_shell_controls_for_recovery_jobs<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
) -> Result<u64> {
    use pioneer_entity::{recovery_job, task_run_turn};
    let turns = recovery_job::Entity::find()
        .select_only()
        .column(recovery_job::Column::TurnId)
        .filter(recovery_job::Column::Id.is_in(job_ids.to_vec()))
        .into_query();
    // Recovery can release a parked child dependency without another Turn/link
    // update. Republish only affected shell capsules to their existing owner.
    let children = turn_llm_context::Entity::find()
        .select_only()
        .column(turn_llm_context::Column::TurnId)
        .distinct()
        .filter(turn_llm_context::Column::Source.eq("shell_control_v1"))
        .filter(turn_llm_context::Column::TurnId.in_subquery(turns.clone()))
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
        .await?;
    for turn_id in children {
        super::task_child_producers::capsule_birth(db, &turn_id).await?;
    }
    Ok(turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::Source.eq("shell_control_v1"))
        .filter(shell_control_releasable(false))
        .filter(turn_llm_context::Column::TurnId.in_subquery(turns))
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
pub(crate) async fn delete_turn_llm_context_for_settled_child<C: ConnectionTrait>(
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
        .filter(Expr::cust("NOT EXISTS (SELECT 1 FROM task_run_turn trt WHERE trt.turn_id = turn_llm_context.turn_id)"))
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
        .filter(Expr::cust(
            "NOT EXISTS (SELECT 1 FROM task_run_turn trt WHERE trt.turn_id = turn.id)",
        ))
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

fn entry_from_model(model: turn_llm_context::Model) -> TurnLlmContextEntry {
    TurnLlmContextEntry {
        id: model.id,
        turn_id: model.turn_id,
        item_id: model.item_id,
        attempt_id: model.attempt_id,
        sequence: model.sequence,
        source: model.source,
        tool_name: model.tool_name,
        payload: model.payload,
        output_policy_snapshot: model.output_policy_snapshot,
        created_at: model.created_at,
        expires_at: model.expires_at,
    }
}
