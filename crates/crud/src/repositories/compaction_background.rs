//! Durable locators for optional completed CLI history checks. No history copy.
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_context, compaction_execution_stop, compaction_history_check, compaction_operation,
    compaction_runner_state, compaction_turn_creation, thread, turn, turn_cli_runtime_binding,
    turn_item,
};
use sea_orm::sea_query::{
    Alias, BinOper, Expr, ExprTrait, Func, JoinType, OnConflict, Order, Query,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect,
};
#[derive(Debug, Clone, FromQueryResult)]
pub struct CompletedHistoryCheck {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub descriptor: Option<String>,
}
#[derive(Debug, Clone, FromQueryResult)]
pub struct CompactionLifecycleRecovery {
    pub id: String,
    pub owner: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub status: String,
    pub cancelled: bool,
}

pub(crate) async fn compaction_enqueue_native_history_check<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    descriptor: &str,
) -> Result<()> {
    ensure!(
        descriptor.len() <= 16384,
        "history descriptor exceeds bound"
    );
    db.execute(
        &Query::insert()
            .into_table(compaction_history_check::Entity)
            .columns([
                compaction_history_check::Column::TurnId,
                compaction_history_check::Column::Descriptor,
            ])
            .select_from(
                turn::Entity::find()
                    .select_only()
                    .join(
                        JoinType::InnerJoin,
                        turn::Entity::belongs_to(thread::Entity)
                            .from(turn::Column::ThreadId)
                            .to(thread::Column::Id)
                            .into(),
                    )
                    .expr(Expr::col((turn::Entity, turn::Column::Id)))
                    .expr(Expr::Value(descriptor.into()))
                    .filter(
                        Expr::col((turn::Entity, turn::Column::Id))
                            .eq(Expr::Value(turn.into()))
                            .and(
                                Expr::col((turn::Entity, turn::Column::ThreadId))
                                    .eq(Expr::Value(thread.into())),
                            )
                            .and(
                                Expr::col((thread::Entity, thread::Column::WorkspaceId))
                                    .eq(Expr::Value(workspace.into())),
                            )
                            .and(
                                Expr::col((turn::Entity, turn::Column::Status))
                                    .eq(Expr::val("completed")),
                            ),
                    )
                    .into_query(),
            )?
            .on_conflict(OnConflict::columns(["turn_id"]).do_nothing().to_owned())
            .to_owned(),
    )
    .await?;
    Ok(())
}
/// Reconcile lost terminal publications and abandoned deadline/Stop states
/// using bounded metadata. This scanner never admits a service generation.
pub(crate) async fn compaction_lifecycle_recovery<C: ConnectionTrait>(
    db: &C,
    now_ms: u64,
    after: &str,
) -> Result<Vec<CompactionLifecycleRecovery>> {
    compaction_operation::Entity::find()
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
            compaction_operation::Entity::belongs_to(turn::Entity)
                .from(compaction_operation::Column::ExecutionTurn)
                .to(turn::Column::Id)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((turn::Entity, turn::Column::ThreadId)).eq(Expr::col((
                            compaction_context::Entity,
                            compaction_context::Column::ThreadId,
                        ))),
                    )
                })
                .into(),
        )
        .expr(Expr::col((
            compaction_operation::Entity,
            compaction_operation::Column::Id,
        )))
        .expr(Expr::col((
            compaction_operation::Entity,
            compaction_operation::Column::Owner,
        )))
        .expr(Expr::col((
            compaction_context::Entity,
            compaction_context::Column::WorkspaceId,
        )))
        .expr(Expr::col((
            compaction_context::Entity,
            compaction_context::Column::ThreadId,
        )))
        .expr_as(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::ExecutionTurn,
            )),
            "turn_id",
        )
        .expr(Expr::col((
            compaction_operation::Entity,
            compaction_operation::Column::Status,
        )))
        .expr_as(
            Expr::col((turn::Entity, turn::Column::Status))
                .is_in(["interrupted", "cancelled"])
                .or(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_execution_stop::Entity, "stop")
                        .and_where(
                            Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                .eq(Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Owner,
                                )))
                                .and(
                                    Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                        .eq(Expr::col((turn::Entity, turn::Column::Id))),
                                ),
                        )
                        .to_owned(),
                )),
            "cancelled",
        )
        .filter(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Id,
            ))
            .gt(Expr::Value(after.into()))
            .and(
                Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::Status,
                ))
                .eq(Expr::val("running"))
                .and(
                    Expr::col((
                        compaction_operation::Entity,
                        compaction_operation::Column::DeadlineMs,
                    ))
                    .lte(Expr::Value(i64::try_from(now_ms)?.into()))
                    .or(Expr::col((turn::Entity, turn::Column::Status))
                        .is_in(["interrupted", "cancelled"]))
                    .or(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(compaction_execution_stop::Entity, "stop")
                            .and_where(
                                Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                    .eq(Expr::col((
                                        compaction_operation::Entity,
                                        compaction_operation::Column::Owner,
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
                    )),
                )
                .or(Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::Status,
                ))
                .ne(Expr::val("running"))
                .and(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_runner_state::Entity, "r")
                        .and_where(
                            Expr::col(("r", compaction_runner_state::Column::OperationId)).eq(
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Id,
                                )),
                            ),
                        )
                        .to_owned(),
                ))
                .and(
                    Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(turn_item::Entity, "item")
                            .and_where(
                                Expr::col(("item", turn_item::Column::TurnId))
                                    .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                    .and(Expr::col(("item", turn_item::Column::ItemId)).eq(
                                        Expr::val("compaction:").binary(
                                            BinOper::Custom("||"),
                                            Expr::col((
                                                compaction_operation::Entity,
                                                compaction_operation::Column::Id,
                                            )),
                                        ),
                                    ))
                                    .and(
                                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                            Expr::col(("item", turn_item::Column::Payload)),
                                            Expr::val("$.details.status"),
                                        ]))
                                        .is_in([
                                            "completed",
                                            "failed",
                                            "cancelled",
                                        ]),
                                    ),
                            )
                            .to_owned(),
                    )
                    .not(),
                )),
            ),
        )
        .order_by_asc(compaction_operation::Column::Id)
        .limit(16)
        .into_model::<CompactionLifecycleRecovery>()
        .all(db)
        .await
        .map_err(Into::into)
}

/// A newer execution invalidates an optional older check. Inspect one
/// locator at a time rather than bulk-updating a thread's retained history.
pub(crate) async fn compaction_history_check_is_current<C: ConnectionTrait>(
    db: &C,
    turn: &str,
) -> Result<bool> {
    Ok(compaction_history_check::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_history_check::Entity::belongs_to(turn::Entity)
                .from(compaction_history_check::Column::TurnId)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::LeftJoin,
            turn::Entity::belongs_to(compaction_turn_creation::Entity)
                .from(turn::Column::Id)
                .to(compaction_turn_creation::Column::TurnId)
                .into(),
        )
        .expr(Expr::col((
            compaction_history_check::Entity,
            compaction_history_check::Column::TurnId,
        )))
        .filter(
            Expr::col((
                compaction_history_check::Entity,
                compaction_history_check::Column::TurnId,
            ))
            .eq(Expr::Value(turn.into()))
            .and(
                Expr::col((
                    compaction_history_check::Entity,
                    compaction_history_check::Column::State,
                ))
                .eq(Expr::val("pending")),
            )
            .and(Expr::col((turn::Entity, turn::Column::Status)).eq(Expr::val("completed")))
            .and(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_execution_stop::Entity, "stop")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_context::Entity,
                            "c",
                            Expr::col(("c", compaction_context::Column::Owner)).eq(Expr::col((
                                "stop",
                                compaction_execution_stop::Column::Owner,
                            ))),
                        )
                        .and_where(
                            Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                .and(
                                    Expr::col(("c", compaction_context::Column::ThreadId))
                                        .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                                ),
                        )
                        .to_owned(),
                )
                .not(),
            )
            .and(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(turn::Entity, "later")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_turn_creation::Entity,
                            "seq",
                            Expr::col(("seq", compaction_turn_creation::Column::TurnId))
                                .eq(Expr::col(("later", turn::Column::Id))),
                        )
                        .and_where(
                            Expr::col(("later", turn::Column::ThreadId))
                                .eq(Expr::col((turn::Entity, turn::Column::ThreadId)))
                                .and(
                                    Expr::col(("seq", compaction_turn_creation::Column::Sequence))
                                        .gt(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                                            Expr::col((
                                                compaction_turn_creation::Entity,
                                                compaction_turn_creation::Column::Sequence,
                                            )),
                                            Expr::val(0_i64),
                                        ]))),
                                ),
                        )
                        .to_owned(),
                )
                .not(),
            ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}
/// One bounded metadata page; caller releases reader capacity before decoding.
pub(crate) async fn compaction_pending_history_checks<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<CompletedHistoryCheck>> {
    compaction_history_check::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_history_check::Entity::belongs_to(turn::Entity)
                .from(compaction_history_check::Column::TurnId)
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
            JoinType::LeftJoin,
            turn::Entity::belongs_to(turn_cli_runtime_binding::Entity)
                .from(turn::Column::Id)
                .to(turn_cli_runtime_binding::Column::TurnId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all()
                        .add(
                            Expr::col((
                                turn_cli_runtime_binding::Entity,
                                turn_cli_runtime_binding::Column::ThreadId,
                            ))
                            .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
                        )
                        .add(
                            Expr::col((
                                turn_cli_runtime_binding::Entity,
                                turn_cli_runtime_binding::Column::WorkspaceId,
                            ))
                            .eq(Expr::col((thread::Entity, thread::Column::WorkspaceId))),
                        )
                })
                .into(),
        )
        .expr(Expr::col((
            compaction_history_check::Entity,
            compaction_history_check::Column::TurnId,
        )))
        .expr(Expr::col((turn::Entity, turn::Column::ThreadId)))
        .expr(Expr::col((thread::Entity, thread::Column::WorkspaceId)))
        .expr_as(
            Expr::expr(Func::cust(Alias::new("coalesce")).args([
                Expr::col((
                    turn_cli_runtime_binding::Entity,
                    turn_cli_runtime_binding::Column::RuntimeId,
                )),
                Expr::val(""),
            ])),
            "runtime_id",
        )
        .expr_as(
            Expr::expr(Func::cust(Alias::new("coalesce")).args([
                Expr::col((
                    turn_cli_runtime_binding::Entity,
                    turn_cli_runtime_binding::Column::RuntimeKind,
                )),
                Expr::val(""),
            ])),
            "runtime_kind",
        )
        .expr(Expr::col((
            turn_cli_runtime_binding::Entity,
            turn_cli_runtime_binding::Column::Model,
        )))
        .expr(Expr::col((turn::Entity, turn::Column::ReasoningEffort)))
        .expr(Expr::col((
            compaction_history_check::Entity,
            compaction_history_check::Column::Descriptor,
        )))
        .filter(
            Expr::col((
                compaction_history_check::Entity,
                compaction_history_check::Column::State,
            ))
            .eq(Expr::val("pending")),
        )
        .order_by(
            Expr::col((
                compaction_history_check::Entity,
                compaction_history_check::Column::TurnId,
            )),
            Order::Asc,
        )
        .limit(16)
        .into_model::<CompletedHistoryCheck>()
        .all(db)
        .await
        .map_err(Into::into)
}
/// Persist the captured settings/deadline before service admission; restart
/// reuses this exact descriptor. It contains model metadata, never messages.
pub(crate) async fn compaction_capture_history_check<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    descriptor: &str,
) -> Result<Option<String>> {
    ensure!(
        descriptor.len() <= 16384,
        "CLI history descriptor exceeds bound"
    );
    let rows = compaction_history_check::Entity::update_many()
        .col_expr(
            compaction_history_check::Column::Descriptor,
            Func::coalesce([
                Expr::col(compaction_history_check::Column::Descriptor),
                Expr::val(descriptor),
            ])
            .into(),
        )
        .filter(compaction_history_check::Column::TurnId.eq(turn))
        .filter(compaction_history_check::Column::State.eq("pending"))
        .exec_with_returning(db)
        .await?;
    Ok(rows.into_iter().next().and_then(|row| row.descriptor))
}
pub(crate) async fn compaction_finish_history_check<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    outcome: &str,
) -> Result<()> {
    use pioneer_entity::compaction_history_check as check;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
    ensure!(
        matches!(outcome, "completed" | "cancelled" | "failed"),
        "invalid CLI history outcome"
    );
    check::Entity::update_many()
        .set(check::ActiveModel {
            state: Set("finished".to_owned()),
            outcome: Set(Some(outcome.to_owned())),
            ..Default::default()
        })
        .filter(check::Column::TurnId.eq(turn))
        .filter(check::Column::State.eq("pending"))
        .exec(db)
        .await?;
    Ok(())
}

use sea_orm::QueryTrait;
