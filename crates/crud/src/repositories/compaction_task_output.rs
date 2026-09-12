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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveredTaskOutputPage {
    pub entries: Vec<DeliveredTaskOutputRef>,
    /// Destination events whose typed item metadata must be refreshed before
    /// deciding whether this quantum contains a delivered Task output.
    pub unprojected_events: Vec<SourceRef>,
    /// Advance even when this quantum contains no authorized-scope delivery.
    pub scanned_through: i64,
    pub done: bool,
}

/// Inspect at most 128 event revisions below the common capture fence.
/// The caller retains the first acknowledgement for each delivery ID across
/// pages; replayed notifications may occur in later quanta. No payload is read.
pub(crate) async fn compaction_delivered_output_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    after: i64,
    fence: &HistoryReadFence,
) -> Result<DeliveredTaskOutputPage> {
    ensure!(after >= 0, "invalid delivery capture cursor");
    let scanned_through = std::cmp::min(after.saturating_add(128), fence.event_order);
    let rows = DeliveredOutputRow::find_by_statement(sqlite_specific_sql(
            "WITH quantum AS MATERIALIZED (SELECT * FROM compaction_event_revision WHERE capture_order>? AND capture_order<=? ORDER BY capture_order LIMIT 128) SELECT d.id AS delivery_id,b.candidate_id,b.task_run_turn_id,s.source_thread,s.source_turn,e.id AS event_id,e.turn_id AS event_turn,r.revision,r.capture_order FROM quantum r JOIN task_delivery d ON d.id=substr(r.item_id,length(?)+1) JOIN thread th ON th.id=d.target_thread_id AND th.workspace_id=d.workspace_id JOIN compaction_delivery_output b ON b.delivery_id=d.id JOIN compaction_task_output s ON s.task_run_turn_id=b.task_run_turn_id AND s.task_id=d.task_id AND s.run_id=d.run_id AND s.workspace_id=d.workspace_id JOIN task_result_candidate c ON c.id=b.candidate_id AND c.task_run_turn_id=s.task_run_turn_id AND c.task_id=s.task_id AND c.run_id=s.run_id AND c.thread_id=s.source_thread AND c.turn_id=s.source_turn JOIN turn_event e ON e.turn_id=d.delivered_turn_id AND e.thread_id=d.target_thread_id WHERE r.source_id=e.id AND r.turn_id=e.turn_id AND r.present=1 AND r.projection_revision=r.revision AND r.item_id=(? || d.id) AND d.workspace_id=? AND d.target_thread_id=? AND d.status='delivered' AND e.event_type=? ORDER BY r.capture_order LIMIT 128",
            [after.into(),scanned_through.into(),pioneer_protocol::task_delivery_result_item_id("").into(),pioneer_protocol::task_delivery_result_item_id("").into(),workspace.into(),thread.into(),pioneer_protocol::constants::events::ITEM_COMPLETED.into()],
        )).all(db).await?;
    let unprojected = UnprojectedEventRow::find_by_statement(sqlite_specific_sql(
            "WITH quantum AS MATERIALIZED (SELECT * FROM compaction_event_revision WHERE capture_order>? AND capture_order<=? ORDER BY capture_order LIMIT 128) SELECT e.id,e.turn_id,r.revision FROM quantum r JOIN turn_event e ON e.id=r.source_id AND e.turn_id=r.turn_id JOIN thread th ON th.id=e.thread_id WHERE r.present=1 AND (r.projection_revision IS NULL OR r.projection_revision<>r.revision) AND e.thread_id=? AND th.workspace_id=? AND e.event_type=? ORDER BY r.capture_order LIMIT 128",
            [after.into(),scanned_through.into(),thread.into(),workspace.into(),pioneer_protocol::constants::events::ITEM_COMPLETED.into()],
        )).all(db).await?;
    let unprojected_events = unprojected
        .into_iter()
        .map(|row| {
            Ok(SourceRef {
                scope: format!("event:{}", row.turn_id),
                id: row.id,
                version: format!("event-revision:{}", row.revision),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let entries = rows
        .into_iter()
        .map(|row| {
            Ok(DeliveredTaskOutputRef {
                delivery_id: row.delivery_id,
                candidate_id: row.candidate_id,
                task_run_turn_id: row.task_run_turn_id,
                source_thread: row.source_thread,
                source_turn: row.source_turn,
                acknowledgement: SourceRef {
                    scope: format!("event:{}", row.event_turn),
                    id: row.event_id,
                    version: format!("event-revision:{}", row.revision),
                },
                capture_order: row.capture_order,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DeliveredTaskOutputPage {
        entries,
        unprojected_events,
        scanned_through,
        done: scanned_through >= fence.event_order,
    })
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

// SQLite MATERIALIZED CTE projections are typed even though their physical
// scan boundary requires SQL rather than a SeaORM entity select.
#[derive(sea_orm::FromQueryResult)]
struct DeliveredOutputRow {
    delivery_id: String,
    candidate_id: String,
    task_run_turn_id: String,
    source_thread: String,
    source_turn: String,
    event_id: String,
    event_turn: String,
    revision: i64,
    capture_order: i64,
}
#[derive(sea_orm::FromQueryResult)]
struct UnprojectedEventRow {
    id: String,
    turn_id: String,
    revision: i64,
}
