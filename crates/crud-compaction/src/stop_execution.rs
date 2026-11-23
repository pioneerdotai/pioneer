use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_context, compaction_execution_stop, compaction_history_check, compaction_operation,
    thread, turn,
};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, TransactionTrait,
};

/// Constant-size control-plane fence. The owning runtime awaits this write
/// before cancelling service work. It survives worker loss without changing
/// the completed user Turn or granting a new compaction attempt.
pub(crate) async fn compaction_stop_execution(
    store: &crate::store::CompactionStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    turn: &str,
) -> Result<bool> {
    let store = crate::store::CompactionStore::new(
        store.connection.maintenance().with_critical_writes(),
        store.frozen_readers.clone(),
    );
    store
        .retry_write_on_conflict(|| async {
            let tx = store.connection.begin().await?;
            let fenced = insert_execution_stop(&tx, workspace, thread, owner, turn).await?;
            let finished = compaction_history_check::Entity::update_many()
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
                .filter(
                    Expr::col(compaction_history_check::Column::State)
                        .ne("finished")
                        .or(Expr::col(compaction_history_check::Column::Outcome).is_null())
                        .or(Expr::col(compaction_history_check::Column::Outcome).ne("cancelled")),
                )
                .exec(&tx)
                .await?;
            tx.commit().await?;
            Ok(fenced || finished.rows_affected > 0)
        })
        .await
}

pub(crate) const NO_PROGRESS_OUTCOME: &str = "no_progress_stop";

/// The operation identity remains terminal across explicit Turn Resume.
pub async fn stop_native_operations_for_no_progress<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<()> {
    compaction_operation::Entity::update_many()
        .filter(
            compaction_operation::Column::Owner
                .eq(crate::operations::native_owner(workspace, thread)),
        )
        .filter(compaction_operation::Column::ExecutionTurn.eq(turn))
        .filter(compaction_operation::Column::Status.eq("running"))
        .col_expr(compaction_operation::Column::Status, Expr::val("cancelled"))
        .col_expr(
            compaction_operation::Column::Outcome,
            Expr::val(NO_PROGRESS_OUTCOME),
        )
        .exec(db)
        .await?;
    Ok(())
}

async fn insert_execution_stop<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    owner: &str,
    turn: &str,
) -> Result<bool> {
    // Scope validation and the Stop marker share the caller's writer transaction.
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
        .one(db)
        .await?;
    ensure!(scoped_turn.is_some(), "compaction Stop scope mismatch");

    let context_inserted = compaction_context::Entity::insert(compaction_context::ActiveModel {
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
    .exec_without_returning(db)
    .await?;
    let scoped_owner = compaction_context::Entity::find_by_id(owner)
        .select_only()
        .column(compaction_context::Column::Owner)
        .filter(compaction_context::Column::WorkspaceId.eq(workspace))
        .filter(compaction_context::Column::ThreadId.eq(thread))
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(scoped_owner.is_some(), "compaction Stop scope mismatch");
    let stop_inserted =
        compaction_execution_stop::Entity::insert(compaction_execution_stop::ActiveModel {
            owner: sea_orm::Set(owner.to_owned()),
            turn_id: sea_orm::Set(turn.to_owned()),
        })
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .exec_without_returning(db)
        .await?;
    Ok(context_inserted > 0 || stop_inserted > 0)
}

/// Called within the terminal Turn transaction before closing item attempts.
pub async fn fence_native_preparation_on_terminal_stop<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> Result<()> {
    let owner = crate::operations::native_owner(workspace, thread);
    let running_operation = compaction_operation::Entity::find()
        .select_only()
        .column(compaction_operation::Column::Id)
        .filter(compaction_operation::Column::Owner.eq(owner.as_str()))
        .filter(compaction_operation::Column::ExecutionTurn.eq(turn))
        .filter(compaction_operation::Column::Status.eq("running"))
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some();
    if running_operation
        || pioneer_crud_execution::turn_item_attempt::has_running_context_preparation(
            db, turn, None, None,
        )
        .await?
    {
        insert_execution_stop(db, workspace, thread, &owner, turn).await?;
    }
    Ok(())
}
