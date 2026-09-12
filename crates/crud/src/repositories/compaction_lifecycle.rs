//! A service operation can publish its own lifecycle after a completed Turn.
//! It cannot publish provider/tool events or inherit a retired execution lease.
use crate::{CanonicalTurnEventPayload, CrudStore, TurnEventProjectionContext};
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_context, compaction_execution_stop, compaction_history_check, compaction_operation,
    compaction_runner_state, thread, turn,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};
use sea_orm::{QueryTrait, TransactionTrait};

/// Constant-size control-plane fence. The owning runtime awaits this write
/// before cancelling service work. It survives worker loss without changing
/// the completed user Turn or granting a new compaction attempt.
pub(crate) async fn compaction_stop_execution(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    turn: &str,
) -> Result<()> {
    let store = store.with_maintenance_reads_and_critical_writes();
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            // Validate and insert under the same existing writer transaction;
            // a concurrent scope change cannot enter between these operations.
            let scoped_turn = turn::Entity::find_by_id(turn)
                .select_only()
                .column(turn::Column::Id)
                .join(
                    JoinType::InnerJoin,
                    turn::Entity::belongs_to(thread::Entity)
                        .from(turn::Column::ThreadId)
                        .to(thread::Column::Id)
                        .into(),
                )
                .filter(thread::Column::Id.eq(thread))
                .filter(thread::Column::WorkspaceId.eq(workspace))
                .into_tuple::<String>()
                .one(&tx)
                .await?;
            ensure!(scoped_turn.is_some(), "compaction Stop scope mismatch");
            compaction_context::Entity::insert(compaction_context::ActiveModel {
                workspace_id: sea_orm::Set(workspace.to_owned()),
                thread_id: sea_orm::Set(thread.to_owned()),
                owner: sea_orm::Set(owner.to_owned()),
                ..Default::default()
            })
            .on_conflict(
                OnConflict::columns([compaction_context::Column::Owner])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&tx)
            .await?;
            let scoped_owner = compaction_context::Entity::find_by_id(owner)
                .select_only()
                .column(compaction_context::Column::Owner)
                .filter(compaction_context::Column::WorkspaceId.eq(workspace))
                .filter(compaction_context::Column::ThreadId.eq(thread))
                .into_tuple::<String>()
                .one(&tx)
                .await?;
            ensure!(scoped_owner.is_some(), "compaction Stop scope mismatch");
            compaction_execution_stop::Entity::insert(compaction_execution_stop::ActiveModel {
                owner: sea_orm::Set(owner.to_owned()),
                turn_id: sea_orm::Set(turn.to_owned()),
            })
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .exec_without_returning(&tx)
            .await?;
            compaction_history_check::Entity::update_many()
                .col_expr(
                    compaction_history_check::Column::State,
                    Expr::val("finished"),
                )
                .col_expr(
                    compaction_history_check::Column::Outcome,
                    Expr::val("cancelled"),
                )
                .filter(
                    Expr::col(compaction_history_check::Column::TurnId)
                        .eq(Expr::Value(turn.into()))
                        .and(Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_execution_stop::Entity, "stop")
                                .and_where(
                                    Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                        .eq(Expr::Value(owner.into()))
                                        .and(
                                            Expr::col((
                                                "stop",
                                                compaction_execution_stop::Column::TurnId,
                                            ))
                                            .eq(Expr::col(("compaction_history_check", "turn_id"))),
                                        ),
                                )
                                .to_owned(),
                        )),
                )
                .exec(&tx)
                .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
}

pub(crate) async fn compaction_materialize_lifecycle(
    store: &CrudStore,
    operation: &str,
    generation: u64,
    event: CanonicalTurnEventPayload,
    timestamp_secs: i64,
) -> Result<()> {
    let (item, terminal) = match &event {
        CanonicalTurnEventPayload::ItemStarted(n) => (&n.item, false),
        CanonicalTurnEventPayload::ItemCompleted(n) => (&n.item, true),
        _ => anyhow::bail!("compaction may only publish its lifecycle item"),
    };
    ensure!(
        matches!(item, pioneer_protocol::TurnItem::SystemEvent { id, code: Some(code), .. }
            if id == &format!("compaction:{operation}") && code == "agent_context_compaction"),
        "compaction lifecycle item identity mismatch"
    );
    ensure!(
        serde_json::to_vec(&event)?.len() <= super::compaction::SOURCE_PAGE_BYTES,
        "compaction lifecycle exceeds its metadata quantum"
    );
    // Prepare only bounded metadata outside capacity. The generation,
    // operation scope and terminal state are revalidated in the same
    // transaction as the canonical append and optional-delivery outbox.
    let guard = compaction_operation::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_operation::Entity::belongs_to(compaction_context::Entity)
                .from(compaction_operation::Column::Owner)
                .to(compaction_context::Column::Owner)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_operation::Entity::belongs_to(compaction_runner_state::Entity)
                .from(compaction_operation::Column::Id)
                .to(compaction_runner_state::Column::OperationId)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_operation::Entity::belongs_to(turn::Entity)
                .from(compaction_operation::Column::ExecutionTurn)
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
        .filter(
            Expr::col((turn::Entity, turn::Column::ThreadId)).eq(Expr::col((
                compaction_context::Entity,
                compaction_context::Column::ThreadId,
            ))),
        )
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(Expr::col((
                compaction_context::Entity,
                compaction_context::Column::WorkspaceId,
            ))),
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
                    compaction_runner_state::Entity,
                    compaction_runner_state::Column::Generation,
                ))
                .eq(Expr::Value(i64::try_from(generation)?.into())),
            )
            .and(
                Expr::col((
                    compaction_context::Entity,
                    compaction_context::Column::WorkspaceId,
                ))
                .eq(Expr::Value(event.workspace_id().into())),
            )
            .and(
                Expr::col((
                    compaction_context::Entity,
                    compaction_context::Column::ThreadId,
                ))
                .eq(Expr::Value(event.thread_id().into())),
            )
            .and(
                Expr::col((turn::Entity, turn::Column::Id)).eq(Expr::Value(event.turn_id().into())),
            )
            .and(
                Expr::Value(i64::from(terminal).into())
                    .eq(Expr::val(0_i64))
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::Status,
                        ))
                        .eq(Expr::val("running")),
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
                                            Expr::col((
                                                "stop",
                                                compaction_execution_stop::Column::TurnId,
                                            ))
                                            .eq(Expr::col((turn::Entity, turn::Column::Id))),
                                        ),
                                )
                                .to_owned(),
                        )
                        .not(),
                    )
                    .and(
                        Expr::col((turn::Entity, turn::Column::Status))
                            .is_in(["interrupted", "cancelled"])
                            .not(),
                    )
                    .or(Expr::Value(i64::from(terminal).into())
                        .eq(Expr::val(1_i64))
                        .and(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Status,
                            ))
                            .ne(Expr::val("running")),
                        )),
            ),
        )
        .build(sea_orm::DbBackend::Sqlite);
    store
        .with_maintenance_access()
        .materialize_turn_event_with_projection_context_and_guard(
            event,
            timestamp_secs,
            TurnEventProjectionContext {
                item_started_deadlines: None,
                enqueue_optional_deliveries: true,
            },
            None,
            Some(guard),
        )
        .await
}
