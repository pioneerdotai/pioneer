//! Runner admission, execution fences, state reconciliation, and manifest paging.
use anyhow::{Result, ensure};
use pioneer_compaction::runner::{FailureKind, RunnerPhase, RunnerState};
use pioneer_compaction::{ModelBudget, SourceRef};
use pioneer_entity::{
    compaction_context, compaction_execution_stop, compaction_manifest, compaction_operation,
    compaction_runner_plan, compaction_runner_state, turn,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
};

use crate::runner_state::{exact_json, observation, update_runner_state};
use crate::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS};

pub fn null_safe_eq(left: Expr, right: Expr) -> Expr {
    // SeaQuery does not expose the PostgreSQL null-safe equality operator.
    Expr::cust_with_exprs("$1 IS NOT DISTINCT FROM $2", [left, right])
}

#[derive(Clone, Debug)]
pub struct ManifestEntry {
    pub ordinal: u64,
    pub unit: u64,
    pub reference_only: bool,
    pub thread_id: String,
    pub source: SourceRef,
}
#[derive(Clone, Debug)]
pub struct RunnerPlanRecord {
    pub source_count: u64,
    pub reference_count: u64,
    pub budget: ModelBudget,
    pub ready: bool,
}

/// Manifest admission contains metadata only and resumes in bounded batches.
/// No provider may run until activate_runner has checked the whole manifest.
/// The execution turn is an immutable admission boundary. Its terminal
/// interruption fences checkpoint publication even before service cleanup.
pub async fn compaction_execution_cancelled<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    Ok(compaction_operation::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_operation::Entity::belongs_to(compaction_context::Entity)
                .from(compaction_operation::Column::Owner)
                .to(compaction_context::Column::Owner)
                .into(),
        )
        .expr(Expr::col((
            compaction_operation::Entity,
            compaction_operation::Column::Id,
        )))
        .filter(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Id,
            ))
            .eq(Expr::Value(operation.into()))
            .and(
                Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::ExecutionTurn,
                ))
                .is_not_null(),
            )
            .and(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_execution_stop::Entity, "stop")
                        .and_where(
                            Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                .eq(Expr::col((
                                    compaction_context::Entity,
                                    compaction_context::Column::Owner,
                                )))
                                .and(
                                    Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                        .eq(Expr::col((
                                            compaction_operation::Entity,
                                            compaction_operation::Column::ExecutionTurn,
                                        ))),
                                ),
                        )
                        .to_owned(),
                )
                .or(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(turn::Entity, "t")
                        .and_where(
                            Expr::col(("t", turn::Column::Id))
                                .eq(Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::ExecutionTurn,
                                )))
                                .and(
                                    Expr::col(("t", turn::Column::Status))
                                        .is_in(["interrupted", "cancelled"])
                                        .not(),
                                ),
                        )
                        .to_owned(),
                )
                .not()),
            ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}

pub async fn compaction_bind_execution_turn<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    turn: &str,
) -> Result<bool> {
    let changed = compaction_operation::Entity::update_many()
        .col_expr(
            compaction_operation::Column::ExecutionTurn,
            Expr::Value(turn.into()),
        )
        .filter(
            Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(operation.into()))
                .and(Expr::col(compaction_operation::Column::ExecutionTurn).is_null())
                .and(Expr::col(compaction_operation::Column::Status).eq(Expr::val("running")))
                .and(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_context::Entity, "c")
                        .join_as(
                            JoinType::InnerJoin,
                            turn::Entity,
                            "t",
                            Expr::col(("t", turn::Column::ThreadId))
                                .eq(Expr::col(("c", compaction_context::Column::ThreadId))),
                        )
                        .and_where(
                            Expr::col(("c", compaction_context::Column::Owner))
                                .eq(Expr::col(("compaction_operation", "owner")))
                                .and(
                                    Expr::col(("t", turn::Column::Id)).eq(Expr::Value(turn.into())),
                                )
                                .and(
                                    Expr::exists(
                                        Query::select()
                                            .expr(Expr::val(1_i64))
                                            .from_as(compaction_execution_stop::Entity, "stop")
                                            .and_where(
                                                Expr::col((
                                                    "stop",
                                                    compaction_execution_stop::Column::Owner,
                                                ))
                                                .eq(Expr::col((
                                                    "c",
                                                    compaction_context::Column::Owner,
                                                )))
                                                .and(
                                                    Expr::col((
                                                        "stop",
                                                        compaction_execution_stop::Column::TurnId,
                                                    ))
                                                    .eq(Expr::col(("t", turn::Column::Id))),
                                                ),
                                            )
                                            .to_owned(),
                                    )
                                    .not(),
                                ),
                        )
                        .to_owned(),
                )),
        )
        .exec(db)
        .await?;
    ensure!(
        compaction_operation::Entity::find()
            .select_only()
            .expr(Expr::val(1_i64))
            .filter(
                Expr::col(compaction_operation::Column::Id)
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col(compaction_operation::Column::ExecutionTurn)
                            .eq(Expr::Value(turn.into()))
                    )
            )
            .into_tuple::<i64>()
            .one(db)
            .await?
            .is_some(),
        "execution turn binding conflicts with operation scope"
    );
    Ok(changed.rows_affected > 0)
}

pub async fn compaction_prepare_runner<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    budget: &ModelBudget,
    source_count: u64,
    reference_count: u64,
) -> Result<bool> {
    ensure!(
        source_count > 0,
        "compaction needs a selected source or previous checkpoint"
    );
    let descriptor = serde_json::to_string(budget)?;
    ensure!(
        descriptor.len() <= SOURCE_PAGE_BYTES,
        "runner descriptor exceeds quantum"
    );
    let source_count = i64::try_from(source_count)?;
    let reference_count = i64::try_from(reference_count)?;
    let changed = db
        .execute(
            &Query::insert()
                .into_table(compaction_runner_plan::Entity)
                .columns([
                    compaction_runner_plan::Column::OperationId,
                    compaction_runner_plan::Column::SourceCount,
                    compaction_runner_plan::Column::ReferenceCount,
                    compaction_runner_plan::Column::Descriptor,
                ])
                .select_from(
                    compaction_operation::Entity::find()
                        .select_only()
                        .expr(Expr::col(compaction_operation::Column::Id))
                        .expr(Expr::Value(source_count.into()))
                        .expr(Expr::Value(reference_count.into()))
                        .expr(exact_json(descriptor.clone()))
                        .filter(
                            Expr::col(compaction_operation::Column::Id)
                                .eq(Expr::Value(operation.into()))
                                .and(
                                    Expr::col(compaction_operation::Column::Status)
                                        .eq(Expr::val("running")),
                                ),
                        )
                        .into_query(),
                )?
                .on_conflict(
                    OnConflict::columns(["operation_id"])
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    let exact = compaction_runner_plan::Entity::find()
        .select_only()
        .expr(Expr::col(compaction_runner_plan::Column::OperationId))
        .filter(
            Expr::col(compaction_runner_plan::Column::OperationId)
                .eq(Expr::Value(operation.into()))
                .and(
                    Expr::col(compaction_runner_plan::Column::SourceCount)
                        .eq(Expr::Value(source_count.into())),
                )
                .and(
                    Expr::col(compaction_runner_plan::Column::ReferenceCount)
                        .eq(Expr::Value(reference_count.into())),
                )
                .and(
                    Expr::col(compaction_runner_plan::Column::Descriptor)
                        .cast_as("text")
                        .eq(Expr::Value(descriptor.into())),
                ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(exact.is_some(), "runner plan idempotency conflict");
    Ok(changed.rows_affected() > 0)
}
pub async fn compaction_runner_plan<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<Option<RunnerPlanRecord>> {
    use pioneer_entity::compaction_runner_plan as plan;
    use sea_orm::EntityTrait;
    let Some(row) = plan::Entity::find_by_id(operation).one(db).await? else {
        return Ok(None);
    };
    Ok(Some(RunnerPlanRecord {
        source_count: u64::try_from(row.source_count)?,
        reference_count: u64::try_from(row.reference_count)?,
        budget: serde_json::from_value(row.descriptor)?,
        ready: row.ready != 0,
    }))
}

pub async fn compaction_runner_state<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<Option<RunnerState>> {
    observation(db, operation)
        .await?
        .map(|row| row.decode())
        .transpose()
}

/// A late cleanup from the previous request must not terminate a resumed request.
/// One bounded conditional write inherits the caller's write scope.
pub async fn compaction_finish_execution<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    execution_turn: Option<&str>,
    deadline_ms: u64,
    status: &str,
    outcome: &str,
) -> Result<bool> {
    ensure!(
        matches!(
            (status, outcome),
            ("failed", "deadline") | ("cancelled", "cancelled")
        ),
        "invalid reconciliation reason"
    );
    let mut update = compaction_operation::Entity::update_many()
        .col_expr(compaction_operation::Column::Status, Expr::val(status))
        .col_expr(compaction_operation::Column::Outcome, Expr::val(outcome))
        .filter(compaction_operation::Column::Id.eq(operation))
        .filter(compaction_operation::Column::Status.eq("running"))
        .filter(compaction_operation::Column::DeadlineMs.eq(i64::try_from(deadline_ms)?));
    if let Some(turn) = execution_turn {
        update = update.filter(compaction_operation::Column::ExecutionTurn.eq(turn));
    }
    Ok(update.exec(db).await?.rows_affected > 0)
}

/// Shared preparation for foreground reconciliation and pending repair. Decode
/// and encode happen outside the writer; the caller fences every source value.
pub fn reconciled_terminal_runner(
    state: &RunnerState,
    status: &str,
    outcome: Option<&str>,
) -> Result<RunnerState> {
    if status == "running"
        || status == "completed"
        || matches!(
            state.phase,
            RunnerPhase::Failed { .. } | RunnerPhase::Applied { .. }
        )
    {
        return Ok(state.clone());
    }
    let kind = if status == "cancelled" {
        FailureKind::Cancelled
    } else if outcome == Some("deadline") {
        FailureKind::Deadline
    } else {
        FailureKind::Permanent
    };
    state.terminate(kind)
}

/// Complete the state record after a control-plane terminal fence. No
/// attempt can advance past that fence. Preparation reads one bounded row;
/// the write revalidates its generation and durable terminal classification.
/// The internal bool reports the CAS outcome, independently of the state DTO.
pub async fn compaction_reconcile_runner_state<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<(Option<RunnerState>, bool)> {
    let row = compaction_runner_state::Entity::find_by_id(operation)
        .inner_join(compaction_operation::Entity)
        .select_only()
        .column_as(
            Expr::col((
                compaction_runner_state::Entity,
                compaction_runner_state::Column::State,
            ))
            .cast_as("text"),
            "state",
        )
        .column_as(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Status,
            )),
            "status",
        )
        .column_as(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Outcome,
            )),
            "outcome",
        )
        .into_tuple::<(String, String, Option<String>)>()
        .one(db)
        .await?;
    let Some((state, status, outcome)) = row else {
        return Ok((None, false));
    };
    let state: RunnerState = serde_json::from_str(&state)?;
    let terminal = reconciled_terminal_runner(&state, &status, outcome.as_deref())?;
    if terminal == state {
        return Ok((Some(state), false));
    }
    let encoded = serde_json::to_string(&terminal)?;
    ensure!(
        encoded.len() <= SOURCE_PAGE_BYTES,
        "runner state exceeds quantum"
    );
    let changed =
        update_runner_state(i64::try_from(terminal.generation)?, encoded)
            .filter(
                Expr::col(compaction_runner_state::Column::OperationId)
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col(compaction_runner_state::Column::Generation)
                            .eq(Expr::Value(i64::try_from(state.generation)?.into())),
                    )
                    .and(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(compaction_operation::Entity, "o")
                            .and_where(
                                Expr::col(("o", compaction_operation::Column::Id))
                                    .eq(Expr::Value(operation.into()))
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Status))
                                            .eq(Expr::Value(status.into())),
                                    )
                                    .and(null_safe_eq(
                                        Expr::col(("o", compaction_operation::Column::Outcome)),
                                        Expr::Value(outcome.into()),
                                    ))
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Status))
                                            .is_in(["cancelled", "failed", "stale"]),
                                    ),
                            )
                            .to_owned(),
                    )),
            )
            .exec(db)
            .await?;
    if changed.rows_affected == 1 {
        return Ok((Some(terminal), true));
    }
    // A concurrent reconciler may have won the same idempotent transition.
    Ok((compaction_runner_state(db, operation).await?, false))
}

pub async fn compaction_manifest_page<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    reference_only: bool,
    unit: u64,
    source_offset: u32,
) -> Result<Vec<ManifestEntry>> {
    #[derive(sea_orm::FromQueryResult)]
    struct ManifestSourceRow {
        ordinal: i64,
        unit_ordinal: i64,
        reference_only: i64,
        source_thread: String,
        source_scope: String,
        source_id: String,
        source_version: String,
    }
    let rows = compaction_manifest::Entity::find()
        .select_only()
        .columns([
            compaction_manifest::Column::Ordinal,
            compaction_manifest::Column::UnitOrdinal,
            compaction_manifest::Column::ReferenceOnly,
            compaction_manifest::Column::SourceThread,
            compaction_manifest::Column::SourceScope,
            compaction_manifest::Column::SourceId,
            compaction_manifest::Column::SourceVersion,
        ])
        .filter(compaction_manifest::Column::OperationId.eq(operation))
        .filter(compaction_manifest::Column::ReferenceOnly.eq(reference_only as i64))
        .filter(compaction_manifest::Column::UnitOrdinal.gte(i64::try_from(unit)?))
        .order_by_asc(compaction_manifest::Column::UnitOrdinal)
        .order_by_asc(compaction_manifest::Column::Ordinal)
        .limit(SOURCE_PAGE_ROWS)
        .offset(u64::from(source_offset))
        .into_model::<ManifestSourceRow>()
        .all(db)
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(ManifestEntry {
                ordinal: u64::try_from(row.ordinal)?,
                unit: u64::try_from(row.unit_ordinal)?,
                reference_only: row.reference_only != 0,
                thread_id: row.source_thread,
                source: SourceRef {
                    scope: row.source_scope,
                    id: row.source_id,
                    version: row.source_version,
                },
            })
        })
        .collect()
}
