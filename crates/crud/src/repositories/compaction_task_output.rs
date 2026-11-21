//! Frozen source references for one completed Task result-producing turn.
//! Capture precedes candidate publication: a later review/delivery may select
//! this work, but must never discover additional live child history.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_entity::{
    compaction_frozen_history, compaction_task_output, task, task_run, task_run_turn, thread, turn,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

pub use pioneer_crud_compaction::task_output::{
    DeliveredTaskOutputCursor, DeliveredTaskOutputEvent, DeliveredTaskOutputPage,
    DeliveredTaskOutputRef, TaskDeliveryOutputSnapshot, TaskOutputSnapshot,
};
pub(crate) use pioneer_crud_compaction::task_output::{
    bind_queued_task_output, compaction_delivered_output_page, compaction_delivery_output,
    compaction_recheck_delivered_output_page, compaction_task_output,
};

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

use sea_orm::QueryTrait;
