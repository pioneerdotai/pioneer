//! Frozen source references for one completed Task result-producing turn.
//! Capture precedes candidate publication: a later review/delivery may select
//! this work, but must never discover additional live child history.
use super::compaction::*;
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::SourceRef;
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_entity::{
    compaction_delivery_output, compaction_frozen_history, compaction_task_output, task,
    task_delivery, task_result_candidate, task_run, task_run_turn, thread, turn,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QuerySelect};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskOutputSnapshot {
    pub task_run_turn_id: String,
    pub task_id: String,
    pub run_id: String,
    pub source_thread: String,
    pub source_turn: String,
    pub history: FrozenHistoryRef,
}

/// First accepted manifest wins across retries. This is one metadata write;
/// source materialization/hashing occurred before acquiring the writer.
/// All prepared identities are revalidated in the INSERT predicate. This
/// record alone grants no delivery or access to the source history.
pub(crate) async fn compaction_record_task_output(
    store: &CrudStore,
    workspace: &str,
    task_run_turn: &str,
    history: &FrozenHistoryRef,
) -> Result<TaskOutputSnapshot> {
    ensure!(
        history.format == 1,
        "unsupported Task output snapshot format"
    );
    if let Some(existing) = store
        .compaction_task_output(workspace, task_run_turn)
        .await?
    {
        return Ok(existing);
    }
    store
        .connection
        .execute(
            &Query::insert()
                .into_table(compaction_task_output::Entity)
                .columns([
                    compaction_task_output::Column::TaskRunTurnId,
                    compaction_task_output::Column::TaskId,
                    compaction_task_output::Column::RunId,
                    compaction_task_output::Column::WorkspaceId,
                    compaction_task_output::Column::SourceThread,
                    compaction_task_output::Column::SourceTurn,
                    compaction_task_output::Column::ManifestId,
                ])
                .select_from(
                    task_run_turn::Entity::find()
                        .select_only()
                        .join(
                            JoinType::InnerJoin,
                            task_run_turn::Entity::belongs_to(task_run::Entity)
                                .from(task_run_turn::Column::RunId)
                                .to(task_run::Column::Id)
                                .into(),
                        )
                        .join(
                            JoinType::InnerJoin,
                            task_run_turn::Entity::belongs_to(task::Entity)
                                .from(task_run_turn::Column::TaskId)
                                .to(task::Column::Id)
                                .into(),
                        )
                        .join(
                            JoinType::InnerJoin,
                            task_run_turn::Entity::belongs_to(turn::Entity)
                                .from(task_run_turn::Column::TurnId)
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
                        .join(
                            JoinType::InnerJoin,
                            turn::Entity::belongs_to(compaction_frozen_history::Entity)
                                .from(turn::Column::ThreadId)
                                .to(compaction_frozen_history::Column::OwnerThread)
                                .into(),
                        )
                        .filter(Expr::col((task_run::Entity, task_run::Column::TaskId)).eq(
                            Expr::col((task_run_turn::Entity, task_run_turn::Column::TaskId)),
                        ))
                        .filter(
                            Expr::col((turn::Entity, turn::Column::ThreadId)).eq(Expr::col((
                                task_run_turn::Entity,
                                task_run_turn::Column::ThreadId,
                            ))),
                        )
                        .filter(
                            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                                .eq(Expr::col((task::Entity, task::Column::WorkspaceId))),
                        )
                        .filter(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::WorkspaceId,
                            ))
                            .eq(Expr::col((task::Entity, task::Column::WorkspaceId))),
                        )
                        .expr(Expr::col((
                            task_run_turn::Entity,
                            task_run_turn::Column::Id,
                        )))
                        .expr(Expr::col((
                            task_run_turn::Entity,
                            task_run_turn::Column::TaskId,
                        )))
                        .expr(Expr::col((
                            task_run_turn::Entity,
                            task_run_turn::Column::RunId,
                        )))
                        .expr(Expr::col((task::Entity, task::Column::WorkspaceId)))
                        .expr(Expr::col((
                            task_run_turn::Entity,
                            task_run_turn::Column::ThreadId,
                        )))
                        .expr(Expr::col((
                            task_run_turn::Entity,
                            task_run_turn::Column::TurnId,
                        )))
                        .expr(Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::Id,
                        )))
                        .filter(
                            Expr::col((task_run_turn::Entity, task_run_turn::Column::Id))
                                .eq(Expr::Value(task_run_turn.into()))
                                .and(
                                    Expr::col((task::Entity, task::Column::WorkspaceId))
                                        .eq(Expr::Value(workspace.into())),
                                )
                                .and(
                                    Expr::col((turn::Entity, turn::Column::Status))
                                        .eq(Expr::val("completed")),
                                )
                                .and(
                                    Expr::col((task_run_turn::Entity, task_run_turn::Column::Kind))
                                        .is_in(["initial", "revision", "recovery"]),
                                )
                                .and(
                                    Expr::col((task_run::Entity, task_run::Column::Status))
                                        .ne(Expr::val("cancelled")),
                                )
                                .and(
                                    Expr::col((
                                        compaction_frozen_history::Entity,
                                        compaction_frozen_history::Column::Id,
                                    ))
                                    .eq(Expr::Value(history.manifest_id.clone().into())),
                                )
                                .and(
                                    Expr::col((
                                        compaction_frozen_history::Entity,
                                        compaction_frozen_history::Column::Ready,
                                    ))
                                    .eq(Expr::val(1_i64)),
                                )
                                .and(
                                    Expr::col((
                                        compaction_frozen_history::Entity,
                                        compaction_frozen_history::Column::IdentitySha256,
                                    ))
                                    .eq(Expr::Value(history.identity_sha256.clone().into())),
                                )
                                .and(
                                    Expr::col((
                                        compaction_frozen_history::Entity,
                                        compaction_frozen_history::Column::MessageCount,
                                    ))
                                    .eq(Expr::Value(i64::try_from(history.messages)?.into())),
                                ),
                        )
                        .into_query(),
                )?
                .on_conflict(
                    OnConflict::columns(["task_run_turn_id"])
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    store
        .compaction_task_output(workspace, task_run_turn)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("Task output snapshot has no completed canonical source binding")
        })
}

/// Point metadata read with the complete retained Task/turn relationship.
/// No result payload or mutable current child transcript is loaded.
pub(crate) async fn compaction_task_output<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    task_run_turn: &str,
) -> Result<Option<TaskOutputSnapshot>> {
    let row = compaction_task_output::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_task_output::Entity::belongs_to(task_run_turn::Entity)
                .from(compaction_task_output::Column::TaskRunTurnId)
                .to(task_run_turn::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((task_run_turn::Entity, task_run_turn::Column::TaskId)).eq(
                                Expr::col((
                                    compaction_task_output::Entity,
                                    compaction_task_output::Column::TaskId,
                                )),
                            ),
                        )
                        .add(
                            Expr::col((task_run_turn::Entity, task_run_turn::Column::RunId)).eq(
                                Expr::col((
                                    compaction_task_output::Entity,
                                    compaction_task_output::Column::RunId,
                                )),
                            ),
                        )
                        .add(
                            Expr::col((task_run_turn::Entity, task_run_turn::Column::ThreadId)).eq(
                                Expr::col((
                                    compaction_task_output::Entity,
                                    compaction_task_output::Column::SourceThread,
                                )),
                            ),
                        )
                        .add(
                            Expr::col((task_run_turn::Entity, task_run_turn::Column::TurnId)).eq(
                                Expr::col((
                                    compaction_task_output::Entity,
                                    compaction_task_output::Column::SourceTurn,
                                )),
                            ),
                        )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_task_output::Entity::belongs_to(task_run::Entity)
                .from(compaction_task_output::Column::RunId)
                .to(task_run::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((task_run::Entity, task_run::Column::TaskId)).eq(Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::TaskId,
                        ))),
                    )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_task_output::Entity::belongs_to(task::Entity)
                .from(compaction_task_output::Column::TaskId)
                .to(task::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((task::Entity, task::Column::WorkspaceId)).eq(Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::WorkspaceId,
                        ))),
                    )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_task_output::Entity::belongs_to(turn::Entity)
                .from(compaction_task_output::Column::SourceTurn)
                .to(turn::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((turn::Entity, turn::Column::ThreadId)).eq(Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::SourceThread,
                        ))),
                    )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_task_output::Entity::belongs_to(thread::Entity)
                .from(compaction_task_output::Column::SourceThread)
                .to(thread::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::WorkspaceId,
                        ))),
                    )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_task_output::Entity::belongs_to(compaction_frozen_history::Entity)
                .from(compaction_task_output::Column::ManifestId)
                .to(compaction_frozen_history::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::WorkspaceId,
                            ))
                            .eq(Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::WorkspaceId,
                            ))),
                        )
                        .add(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::OwnerThread,
                            ))
                            .eq(Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::SourceThread,
                            ))),
                        )
                        .add(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::Ready,
                            ))
                            .eq(Expr::val(1_i64)),
                        )
                })
                .into(),
        )
        .expr(Expr::col((
            compaction_task_output::Entity,
            compaction_task_output::Column::TaskId,
        )))
        .expr(Expr::col((
            compaction_task_output::Entity,
            compaction_task_output::Column::RunId,
        )))
        .expr(Expr::col((
            compaction_task_output::Entity,
            compaction_task_output::Column::SourceThread,
        )))
        .expr(Expr::col((
            compaction_task_output::Entity,
            compaction_task_output::Column::SourceTurn,
        )))
        .expr(Expr::col((
            compaction_frozen_history::Entity,
            compaction_frozen_history::Column::Id,
        )))
        .expr(Expr::col((
            compaction_frozen_history::Entity,
            compaction_frozen_history::Column::IdentitySha256,
        )))
        .expr(Expr::col((
            compaction_frozen_history::Entity,
            compaction_frozen_history::Column::MessageCount,
        )))
        .filter(
            Expr::col((
                compaction_task_output::Entity,
                compaction_task_output::Column::TaskRunTurnId,
            ))
            .eq(Expr::Value(task_run_turn.into()))
            .and(
                Expr::col((
                    compaction_task_output::Entity,
                    compaction_task_output::Column::WorkspaceId,
                ))
                .eq(Expr::Value(workspace.into())),
            ),
        )
        .into_model::<TaskOutputRow>()
        .one(db)
        .await?;
    row.map(|row| {
        Ok(TaskOutputSnapshot {
            task_run_turn_id: task_run_turn.into(),
            task_id: row.task_id,
            run_id: row.run_id,
            source_thread: row.source_thread,
            source_turn: row.source_turn,
            history: FrozenHistoryRef {
                format: 1,
                manifest_id: row.id,
                identity_sha256: row.identity_sha256,
                messages: u64::try_from(row.message_count)?,
            },
        })
    })
    .transpose()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskDeliveryOutputSnapshot {
    pub delivery_id: String,
    pub candidate_id: String,
    pub output: TaskOutputSnapshot,
}

/// Called only for a newly inserted DeliveryQueued row, inside the existing
/// Task event transaction after RunCompleted. That event must see the accepted
/// candidate in the same batch. No historical queue replay may infer a newer
/// candidate. These links grant neither delivery acknowledgement nor read ACL.
pub(crate) async fn bind_queued_task_output<C: ConnectionTrait>(
    db: &C,
    delivery: &pioneer_protocol::TaskDelivery,
) -> Result<()> {
    if delivery.result_snapshot.is_none() || delivery.error_snapshot.is_some() {
        return Ok(());
    }
    db.execute(
        &Query::insert()
            .into_table(compaction_delivery_output::Entity)
            .columns([
                compaction_delivery_output::Column::DeliveryId,
                compaction_delivery_output::Column::CandidateId,
                compaction_delivery_output::Column::TaskRunTurnId,
            ])
            .select_from(
                task_delivery::Entity::find()
                    .select_only()
                    .join(
                        JoinType::InnerJoin,
                        task_delivery::Entity::belongs_to(task_run::Entity)
                            .from(task_delivery::Column::RunId)
                            .to(task_run::Column::Id)
                            .into(),
                    )
                    .join(
                        JoinType::InnerJoin,
                        task_delivery::Entity::belongs_to(task_result_candidate::Entity)
                            .from(task_delivery::Column::RunId)
                            .to(task_result_candidate::Column::RunId)
                            .into(),
                    )
                    .join(
                        JoinType::InnerJoin,
                        task_result_candidate::Entity::belongs_to(compaction_task_output::Entity)
                            .from(task_result_candidate::Column::TaskRunTurnId)
                            .to(compaction_task_output::Column::TaskRunTurnId)
                            .into(),
                    )
                    .filter(
                        Expr::col((task_run::Entity, task_run::Column::TaskId)).eq(Expr::col((
                            task_delivery::Entity,
                            task_delivery::Column::TaskId,
                        ))),
                    )
                    .filter(
                        Expr::col((task_run::Entity, task_run::Column::Status))
                            .eq(Expr::val("succeeded")),
                    )
                    .filter(
                        Expr::col((
                            task_result_candidate::Entity,
                            task_result_candidate::Column::TaskId,
                        ))
                        .eq(Expr::col((
                            task_delivery::Entity,
                            task_delivery::Column::TaskId,
                        ))),
                    )
                    .filter(
                        Expr::col((
                            task_result_candidate::Entity,
                            task_result_candidate::Column::Status,
                        ))
                        .eq(Expr::val("accepted")),
                    )
                    .filter(
                        Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::TaskId,
                        ))
                        .eq(Expr::col((
                            task_result_candidate::Entity,
                            task_result_candidate::Column::TaskId,
                        ))),
                    )
                    .filter(
                        Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::RunId,
                        ))
                        .eq(Expr::col((
                            task_result_candidate::Entity,
                            task_result_candidate::Column::RunId,
                        ))),
                    )
                    .filter(
                        Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::SourceThread,
                        ))
                        .eq(Expr::col((
                            task_result_candidate::Entity,
                            task_result_candidate::Column::ThreadId,
                        ))),
                    )
                    .filter(
                        Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::SourceTurn,
                        ))
                        .eq(Expr::col((
                            task_result_candidate::Entity,
                            task_result_candidate::Column::TurnId,
                        ))),
                    )
                    .filter(
                        Expr::col((
                            compaction_task_output::Entity,
                            compaction_task_output::Column::WorkspaceId,
                        ))
                        .eq(Expr::col((
                            task_delivery::Entity,
                            task_delivery::Column::WorkspaceId,
                        ))),
                    )
                    .expr(Expr::col((
                        task_delivery::Entity,
                        task_delivery::Column::Id,
                    )))
                    .expr(Expr::col((
                        task_result_candidate::Entity,
                        task_result_candidate::Column::Id,
                    )))
                    .expr(Expr::col((
                        compaction_task_output::Entity,
                        compaction_task_output::Column::TaskRunTurnId,
                    )))
                    .filter(
                        Expr::col((task_delivery::Entity, task_delivery::Column::Id))
                            .eq(Expr::Value(delivery.id.clone().into()))
                            .and(
                                Expr::col((
                                    task_delivery::Entity,
                                    task_delivery::Column::WorkspaceId,
                                ))
                                .eq(Expr::Value(delivery.workspace_id.clone().into())),
                            )
                            .and(
                                Expr::col((task_delivery::Entity, task_delivery::Column::TaskId))
                                    .eq(Expr::Value(delivery.task_id.clone().into())),
                            )
                            .and(
                                Expr::col((task_delivery::Entity, task_delivery::Column::RunId))
                                    .eq(Expr::Value(delivery.run_id.clone().into())),
                            )
                            .and(
                                Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as(task_result_candidate::Entity, "other")
                                        .and_where(
                                            Expr::col((
                                                "other",
                                                task_result_candidate::Column::RunId,
                                            ))
                                            .eq(Expr::col((
                                                task_result_candidate::Entity,
                                                task_result_candidate::Column::RunId,
                                            )))
                                            .and(
                                                Expr::col((
                                                    "other",
                                                    task_result_candidate::Column::Status,
                                                ))
                                                .eq(Expr::val("accepted")),
                                            )
                                            .and(
                                                Expr::col((
                                                    "other",
                                                    task_result_candidate::Column::Id,
                                                ))
                                                .ne(Expr::col((
                                                    task_result_candidate::Entity,
                                                    task_result_candidate::Column::Id,
                                                ))),
                                            ),
                                        )
                                        .to_owned(),
                                )
                                .not(),
                            ),
                    )
                    .into_query(),
            )?
            .on_conflict(OnConflict::columns(["delivery_id"]).do_nothing().to_owned())
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(crate) async fn compaction_delivery_output(
    store: &CrudStore,
    workspace: &str,
    delivery: &str,
) -> Result<Option<TaskDeliveryOutputSnapshot>> {
    let row = compaction_delivery_output::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_delivery_output::Entity::belongs_to(task_delivery::Entity)
                .from(compaction_delivery_output::Column::DeliveryId)
                .to(task_delivery::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_delivery_output::Entity::belongs_to(compaction_task_output::Entity)
                .from(compaction_delivery_output::Column::TaskRunTurnId)
                .to(compaction_task_output::Column::TaskRunTurnId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::TaskId,
                            ))
                            .eq(Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::TaskId,
                            ))),
                        )
                        .add(
                            Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::RunId,
                            ))
                            .eq(Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::RunId,
                            ))),
                        )
                        .add(
                            Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::WorkspaceId,
                            ))
                            .eq(Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::WorkspaceId,
                            ))),
                        )
                })
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_delivery_output::Entity::belongs_to(task_result_candidate::Entity)
                .from(compaction_delivery_output::Column::CandidateId)
                .to(task_result_candidate::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((
                                task_result_candidate::Entity,
                                task_result_candidate::Column::TaskRunTurnId,
                            ))
                            .eq(Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::TaskRunTurnId,
                            ))),
                        )
                        .add(
                            Expr::col((
                                task_result_candidate::Entity,
                                task_result_candidate::Column::TaskId,
                            ))
                            .eq(Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::TaskId,
                            ))),
                        )
                        .add(
                            Expr::col((
                                task_result_candidate::Entity,
                                task_result_candidate::Column::RunId,
                            ))
                            .eq(Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::RunId,
                            ))),
                        )
                        .add(
                            Expr::col((
                                task_result_candidate::Entity,
                                task_result_candidate::Column::ThreadId,
                            ))
                            .eq(Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::SourceThread,
                            ))),
                        )
                        .add(
                            Expr::col((
                                task_result_candidate::Entity,
                                task_result_candidate::Column::TurnId,
                            ))
                            .eq(Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::SourceTurn,
                            ))),
                        )
                })
                .into(),
        )
        .expr(Expr::col((
            compaction_delivery_output::Entity,
            compaction_delivery_output::Column::CandidateId,
        )))
        .expr(Expr::col((
            compaction_delivery_output::Entity,
            compaction_delivery_output::Column::TaskRunTurnId,
        )))
        .filter(
            Expr::col((task_delivery::Entity, task_delivery::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((task_delivery::Entity, task_delivery::Column::Id))
                        .eq(Expr::Value(delivery.into())),
                ),
        )
        .into_tuple::<(String, String)>()
        .one(&store.connection)
        .await?;
    let Some((candidate_id, task_run_turn)) = row else {
        return Ok(None);
    };
    let Some(output) = store
        .compaction_task_output(workspace, &task_run_turn)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(TaskDeliveryOutputSnapshot {
        delivery_id: delivery.into(),
        candidate_id,
        output,
    }))
}

/// Discovery metadata only. The caller must authorize original-history access
/// before restoring the output manifest; a delivered summary is not a grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveredTaskOutputRef {
    pub delivery_id: String,
    pub candidate_id: String,
    pub task_run_turn_id: String,
    pub source_thread: String,
    pub source_turn: String,
    pub acknowledgement: SourceRef,
    pub capture_order: i64,
}

/// Request-local keyset cursor. The common Turn fence bounds the set of
/// locators; an active Turn retains its indexed sequence upper bound once.
/// This bound limits work, while capture_order remains the admission fence.
/// A fence-admitted event was already present when its Turn endpoint was read.
/// Appends cannot create additional fence-admitted keys. Edits/deletions are
/// rejected by source revision checks and the caller's retained source epoch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeliveredTaskOutputCursor {
    pub after_turn: String,
    pub active_turn: Option<String>,
    pub after_sequence: Option<i64>,
    pub event_high_water: Option<i64>,
}

/// Exact metadata selected by one read statement, before payload decoding.
/// Retained only for this request's bounded refresh/revalidation operation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct DeliveredTaskOutputEvent {
    pub source: SourceRef,
    pub sequence: i64,
    pub capture_order: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveredTaskOutputPage {
    pub entries: Vec<DeliveredTaskOutputRef>,
    pub unprojected_events: Vec<SourceRef>,
    /// Refresh exactly these sources, never rerun selection after a CAS.
    pub selected_events: Vec<DeliveredTaskOutputEvent>,
    pub next_cursor: DeliveredTaskOutputCursor,
    /// Original selection budget: one locator and at most 127 fenced event
    /// metadata rows. Exact refresh retains that page's selection counters.
    pub selected_turn_rows: usize,
    pub selected_event_rows: usize,
    pub done: bool,
}

const DELIVERED_OUTPUT_EVENT_ROWS: usize = 127;

/// The scoped delivery index is the starting point. Both existing Turn fence
/// components exclude newly created Turns, including IDs inserted below the
/// lexical high water. NULL creation metadata is the existing legacy case.
/// The scalar sequence seek reads one index endpoint, not the event history.
const DELIVERY_TURN_SQL: &str = r#"SELECT d.delivered_turn_id AS turn_id,
    (SELECT e.sequence FROM turn_event e WHERE e.turn_id=t.id
     ORDER BY e.sequence DESC LIMIT 1) AS event_high_water
FROM task_delivery d INDEXED BY compaction_delivery_turn
CROSS JOIN turn t ON t.id=d.delivered_turn_id AND t.thread_id=d.target_thread_id
CROSS JOIN thread h ON h.id=t.thread_id AND h.workspace_id=d.workspace_id
LEFT JOIN compaction_turn_creation ct ON ct.turn_id=t.id
WHERE d.workspace_id=? AND d.target_thread_id=? AND d.delivered_turn_id>?
  AND d.delivered_turn_id<=? AND (ct.sequence IS NULL OR ct.sequence<=?)
  AND d.status='delivered'
ORDER BY d.delivered_turn_id LIMIT 1"#;

/// Capture eligibility is checked BEFORE LIMIT: late events cannot fill a
/// page or alter its continuation. The fixed per-Turn upper seek bound also
/// prevents a continuing append from extending the SQL's inspected tail.
/// CROSS JOIN keeps revisions as point lookups from scoped canonical events.
const DELIVERY_EVENT_QUANTUM_SQL: &str = r#"
WITH quantum AS MATERIALIZED (
    SELECT e.id,e.turn_id,e.thread_id,e.sequence,e.event_type,
           r.revision AS selected_revision,r.capture_order AS selected_order
    FROM turn t CROSS JOIN thread h ON h.id=t.thread_id
    CROSS JOIN turn_event e ON e.turn_id=t.id AND e.thread_id=t.thread_id
    CROSS JOIN compaction_event_revision r ON r.source_id=e.id
    WHERE t.id=? AND t.thread_id=? AND h.workspace_id=? AND e.sequence>?
      AND e.sequence<=? AND r.turn_id=e.turn_id AND r.present=1
      AND r.capture_order<=?
    ORDER BY e.sequence LIMIT 127
)
"#;

/// A refreshed page is resolved by exact source keys and expected revisions.
/// No Turn/event pagination predicate is reused here. Any edit, deletion,
/// sequence/scope change, or replacement removes a row and fails validation.
const DELIVERY_RECHECK_QUANTUM_SQL: &str = r#"
WITH quantum AS MATERIALIZED (
    SELECT e.id,e.turn_id,e.thread_id,e.sequence,e.event_type,
           r.revision AS selected_revision,r.capture_order AS selected_order
    FROM json_each(?) w
    CROSS JOIN turn_event e ON e.id=json_extract(w.value,'$.source.id')
    CROSS JOIN turn t ON t.id=e.turn_id AND t.thread_id=e.thread_id
    CROSS JOIN thread h ON h.id=t.thread_id
    CROSS JOIN compaction_event_revision r ON r.source_id=e.id
    WHERE h.workspace_id=? AND t.thread_id=? AND r.turn_id=e.turn_id
      AND 'event:'||e.turn_id=json_extract(w.value,'$.source.scope')
      AND 'event-revision:'||r.revision=json_extract(w.value,'$.source.version')
      AND e.sequence=json_extract(w.value,'$.sequence')
      AND r.capture_order=json_extract(w.value,'$.capture_order')
      AND r.present=1 AND r.capture_order<=?
)
"#;

const DELIVERY_BINDINGS_SQL: &str = r#"
SELECT q.id AS event_id,q.turn_id AS event_turn,q.sequence,q.event_type,
       r.revision,r.capture_order,r.projection_revision,
       d.id AS delivery_id,b.candidate_id,b.task_run_turn_id,
       s.source_thread,s.source_turn,c.id AS bound_candidate
FROM quantum q
CROSS JOIN compaction_event_revision r ON r.source_id=q.id
    AND r.turn_id=q.turn_id AND r.revision=q.selected_revision
    AND r.capture_order=q.selected_order AND r.present=1
LEFT JOIN task_delivery d
    ON d.id=substr(r.item_id,length(?)+1) AND r.item_id=(? || d.id)
   AND q.event_type=? AND r.projection_revision=r.revision
   AND d.workspace_id=? AND d.target_thread_id=q.thread_id
   AND d.delivered_turn_id=q.turn_id AND d.status='delivered'
LEFT JOIN compaction_delivery_output b ON b.delivery_id=d.id
LEFT JOIN compaction_task_output s
    ON s.task_run_turn_id=b.task_run_turn_id AND s.task_id=d.task_id
   AND s.run_id=d.run_id AND s.workspace_id=d.workspace_id
LEFT JOIN task_result_candidate c
    ON c.id=b.candidate_id AND c.task_run_turn_id=s.task_run_turn_id
   AND c.task_id=s.task_id AND c.run_id=s.run_id
   AND c.thread_id=s.source_thread AND c.turn_id=s.source_turn
ORDER BY q.sequence
"#;

fn delivery_binding_values(workspace: &str) -> [sea_orm::Value; 4] {
    [
        pioneer_protocol::task_delivery_result_item_id("").into(),
        pioneer_protocol::task_delivery_result_item_id("").into(),
        pioneer_protocol::constants::events::ITEM_COMPLETED.into(),
        workspace.into(),
    ]
}

pub(crate) async fn compaction_delivered_output_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    cursor: &DeliveredTaskOutputCursor,
    fence: &HistoryReadFence,
) -> Result<DeliveredTaskOutputPage> {
    ensure!(
        cursor
            .active_turn
            .as_ref()
            .is_none_or(|turn| turn > &cursor.after_turn)
            && (cursor.active_turn.is_some() == cursor.event_high_water.is_some())
            && (cursor.active_turn.is_some() || cursor.after_sequence.is_none()),
        "invalid delivery metadata cursor"
    );
    let mut page = DeliveredTaskOutputPage {
        entries: Vec::new(),
        unprojected_events: Vec::new(),
        selected_events: Vec::new(),
        next_cursor: cursor.clone(),
        selected_turn_rows: 0,
        selected_event_rows: 0,
        done: false,
    };
    let (selected_turn, high_water) = if let Some(turn) = &cursor.active_turn {
        (turn.clone(), cursor.event_high_water)
    } else {
        let row = DeliveryTurnRow::find_by_statement(sqlite_specific_sql(
            DELIVERY_TURN_SQL,
            [
                workspace.into(),
                thread.into(),
                cursor.after_turn.clone().into(),
                fence.turn_id.clone().into(),
                fence.turn_order.into(),
            ],
        ))
        .one(db)
        .await?;
        let Some(row) = row else {
            page.done = true;
            return Ok(page);
        };
        page.selected_turn_rows = 1;
        (row.turn_id, row.event_high_water)
    };
    let Some(high_water) = high_water else {
        page.next_cursor = DeliveredTaskOutputCursor {
            after_turn: selected_turn,
            ..Default::default()
        };
        return Ok(page);
    };
    let quantum = if cursor.after_sequence.is_some() {
        DELIVERY_EVENT_QUANTUM_SQL.to_owned()
    } else {
        DELIVERY_EVENT_QUANTUM_SQL.replace(" AND e.sequence>?", "")
    };
    let sql = quantum + DELIVERY_BINDINGS_SQL;
    let mut values = vec![
        selected_turn.clone().into(),
        thread.into(),
        workspace.into(),
    ];
    if let Some(sequence) = cursor.after_sequence {
        values.push(sequence.into());
    }
    values.extend([high_water.into(), fence.event_order.into()]);
    values.extend(delivery_binding_values(workspace));
    let rows = DeliveredOutputRow::find_by_statement(sqlite_specific_sql(&sql, values))
        .all(db)
        .await?;
    // all() releases DB capacity before transforming or retaining metadata.
    page.next_cursor = if rows.len() == DELIVERED_OUTPUT_EVENT_ROWS {
        DeliveredTaskOutputCursor {
            after_turn: cursor.after_turn.clone(),
            active_turn: Some(selected_turn),
            after_sequence: rows.last().map(|row| row.sequence),
            event_high_water: Some(high_water),
        }
    } else {
        DeliveredTaskOutputCursor {
            after_turn: selected_turn,
            ..Default::default()
        }
    };
    populate_delivery_page(&mut page, rows);
    Ok(page)
}

pub(crate) async fn compaction_recheck_delivered_output_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    page: &DeliveredTaskOutputPage,
    fence: &HistoryReadFence,
) -> Result<DeliveredTaskOutputPage> {
    ensure!(
        page.selected_events.len() <= DELIVERED_OUTPUT_EVENT_ROWS,
        "delivery refresh exceeds metadata row budget"
    );
    // Serialization occurs before acquiring reader admission.
    let selected = serde_json::to_string(&page.selected_events)?;
    ensure!(
        selected.len() <= SOURCE_PAGE_BYTES,
        "delivery refresh exceeds metadata byte budget"
    );
    let sql = DELIVERY_RECHECK_QUANTUM_SQL.to_owned() + DELIVERY_BINDINGS_SQL;
    let mut values = vec![
        selected.into(),
        workspace.into(),
        thread.into(),
        fence.event_order.into(),
    ];
    values.extend(delivery_binding_values(workspace));
    let rows = DeliveredOutputRow::find_by_statement(sqlite_specific_sql(&sql, values))
        .all(db)
        .await?;
    let mut refreshed = DeliveredTaskOutputPage {
        entries: Vec::new(),
        unprojected_events: Vec::new(),
        selected_events: Vec::new(),
        next_cursor: page.next_cursor.clone(),
        selected_turn_rows: page.selected_turn_rows,
        selected_event_rows: 0,
        done: page.done,
    };
    populate_delivery_page(&mut refreshed, rows);
    // The unique (turn_id, sequence) keys preserve the original order. Full
    // equality validates every selected source, not just acknowledged rows;
    // append cannot add or remove a token in this exact-key query. Keep the
    // original continuation so refresh neither repeats nor skips a page.
    ensure!(
        refreshed.selected_events == page.selected_events,
        "selected delivery sources changed while refreshing metadata"
    );
    ensure!(
        refreshed.unprojected_events.is_empty(),
        "delivery metadata changed during refresh"
    );
    ensure!(
        page.entries
            .iter()
            .all(|entry| refreshed.entries.contains(entry)),
        "selected delivery binding changed while refreshing metadata"
    );
    Ok(refreshed)
}

fn populate_delivery_page(page: &mut DeliveredTaskOutputPage, rows: Vec<DeliveredOutputRow>) {
    page.selected_event_rows = rows.len();
    for row in rows {
        let acknowledgement = SourceRef {
            scope: format!("event:{}", row.event_turn),
            id: row.event_id,
            version: format!("event-revision:{}", row.revision),
        };
        page.selected_events.push(DeliveredTaskOutputEvent {
            source: acknowledgement.clone(),
            sequence: row.sequence,
            capture_order: row.capture_order,
        });
        if row.event_type != pioneer_protocol::constants::events::ITEM_COMPLETED {
            continue;
        }
        if row.projection_revision != Some(row.revision) {
            page.unprojected_events.push(acknowledgement);
            continue;
        }
        if let (
            Some(delivery_id),
            Some(candidate_id),
            Some(task_run_turn_id),
            Some(source_thread),
            Some(source_turn),
            Some(_),
        ) = (
            row.delivery_id,
            row.candidate_id,
            row.task_run_turn_id,
            row.source_thread,
            row.source_turn,
            row.bound_candidate,
        ) {
            page.entries.push(DeliveredTaskOutputRef {
                delivery_id,
                candidate_id,
                task_run_turn_id,
                source_thread,
                source_turn,
                acknowledgement,
                capture_order: row.capture_order,
            });
        }
    }
}

#[derive(sea_orm::FromQueryResult)]
struct TaskOutputRow {
    task_id: String,
    run_id: String,
    source_thread: String,
    source_turn: String,
    id: String,
    identity_sha256: String,
    message_count: i64,
}

use sea_orm::QueryTrait;

#[derive(sea_orm::FromQueryResult)]
struct DeliveryTurnRow {
    turn_id: String,
    event_high_water: Option<i64>,
}

#[derive(sea_orm::FromQueryResult)]
struct DeliveredOutputRow {
    event_id: String,
    event_turn: String,
    sequence: i64,
    event_type: String,
    revision: i64,
    capture_order: i64,
    projection_revision: Option<i64>,
    delivery_id: Option<String>,
    candidate_id: Option<String>,
    task_run_turn_id: Option<String>,
    source_thread: Option<String>,
    source_turn: Option<String>,
    bound_candidate: Option<String>,
}
