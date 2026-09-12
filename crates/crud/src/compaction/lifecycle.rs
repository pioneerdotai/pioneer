//! A service operation can publish its own lifecycle after a completed Turn.
//! It cannot publish provider/tool events or inherit a retired execution lease.
use super::statement;
use crate::{CanonicalTurnEventPayload, CrudStore, TurnEventProjectionContext};
use anyhow::{Result, ensure};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{ConnectionTrait, TransactionTrait};

impl CrudStore {
    /// Constant-size control-plane fence. The owning runtime awaits this write
    /// before cancelling service work. It survives worker loss without changing
    /// the completed user Turn or granting a new compaction attempt.
    pub async fn compaction_stop_execution(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
        turn: &str,
    ) -> Result<()> {
        let store = self.with_maintenance_reads_and_critical_writes();
        store
            .run_serialized_write(|| async {
                let tx = store.connection.begin().await?;
                tx.execute_raw(statement(
                    &Query::insert()
                        .into_table("compaction_context")
                        .columns(["workspace_id", "thread_id", "owner"])
                        .select_from(
                            Query::select()
                                .expr(Expr::col(("th", "workspace_id")))
                                .expr(Expr::col(("th", "id")))
                                .expr(Expr::Value(owner.into()))
                                .from_as("thread", "th")
                                .join_as(
                                    JoinType::InnerJoin,
                                    "turn",
                                    "t",
                                    Expr::col(("t", "thread_id")).eq(Expr::col(("th", "id"))),
                                )
                                .and_where(
                                    Expr::col(("th", "workspace_id"))
                                        .eq(Expr::Value(workspace.into()))
                                        .and(Expr::col(("th", "id")).eq(Expr::Value(thread.into())))
                                        .and(Expr::col(("t", "id")).eq(Expr::Value(turn.into()))),
                                )
                                .to_owned(),
                        )?
                        .on_conflict(OnConflict::columns(["owner"]).do_nothing().to_owned())
                        .to_owned(),
                ))
                .await?;
                tx.execute_raw(statement(
                    &Query::insert()
                        .into_table("compaction_execution_stop")
                        .columns(["owner", "turn_id"])
                        .select_from(
                            Query::select()
                                .expr(Expr::col(("c", "owner")))
                                .expr(Expr::col(("t", "id")))
                                .from_as("compaction_context", "c")
                                .join_as(
                                    JoinType::InnerJoin,
                                    "turn",
                                    "t",
                                    Expr::col(("t", "thread_id")).eq(Expr::col(("c", "thread_id"))),
                                )
                                .and_where(
                                    Expr::col(("c", "owner"))
                                        .eq(Expr::Value(owner.into()))
                                        .and(
                                            Expr::col(("c", "workspace_id"))
                                                .eq(Expr::Value(workspace.into())),
                                        )
                                        .and(
                                            Expr::col(("c", "thread_id"))
                                                .eq(Expr::Value(thread.into())),
                                        )
                                        .and(Expr::col(("t", "id")).eq(Expr::Value(turn.into()))),
                                )
                                .to_owned(),
                        )?
                        .on_conflict(OnConflict::new().do_nothing().to_owned())
                        .to_owned(),
                ))
                .await?;
                ensure!(
                    tx.query_one_raw(statement(
                        &Query::select()
                            .expr(Expr::col(("c", "owner")))
                            .from_as("compaction_context", "c")
                            .join_as(
                                JoinType::InnerJoin,
                                "compaction_execution_stop",
                                "stop",
                                Expr::col(("stop", "owner")).eq(Expr::col(("c", "owner")))
                            )
                            .and_where(
                                Expr::col(("c", "owner"))
                                    .eq(Expr::Value(owner.into()))
                                    .and(
                                        Expr::col(("c", "workspace_id"))
                                            .eq(Expr::Value(workspace.into()))
                                    )
                                    .and(
                                        Expr::col(("c", "thread_id"))
                                            .eq(Expr::Value(thread.into()))
                                    )
                                    .and(
                                        Expr::col(("stop", "turn_id")).eq(Expr::Value(turn.into()))
                                    )
                            )
                            .to_owned()
                    ))
                    .await?
                    .is_some(),
                    "compaction Stop scope mismatch"
                );
                tx.execute_raw(statement(
                    &Query::update()
                        .table("compaction_history_check")
                        .value("state", Expr::val("finished"))
                        .value("outcome", Expr::val("cancelled"))
                        .and_where(
                            Expr::col("turn_id")
                                .eq(Expr::Value(turn.into()))
                                .and(Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as("compaction_execution_stop", "stop")
                                        .and_where(
                                            Expr::col(("stop", "owner"))
                                                .eq(Expr::Value(owner.into()))
                                                .and(Expr::col(("stop", "turn_id")).eq(Expr::col(
                                                    ("compaction_history_check", "turn_id"),
                                                ))),
                                        )
                                        .to_owned(),
                                )),
                        )
                        .to_owned(),
                ))
                .await?;
                tx.commit().await?;
                Ok(())
            })
            .await
    }

    pub async fn compaction_materialize_lifecycle(
        &self,
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
            serde_json::to_vec(&event)?.len() <= super::SOURCE_PAGE_BYTES,
            "compaction lifecycle exceeds its metadata quantum"
        );
        // Prepare only bounded metadata outside capacity. The generation,
        // operation scope and terminal state are revalidated in the same
        // transaction as the canonical append and optional-delivery outbox.
        let guard = statement(
            &Query::select()
                .expr(Expr::col(("o", "id")))
                .from_as("compaction_operation", "o")
                .join_as(
                    JoinType::InnerJoin,
                    "compaction_context",
                    "c",
                    Expr::col(("c", "owner")).eq(Expr::col(("o", "owner"))),
                )
                .join_as(
                    JoinType::InnerJoin,
                    "compaction_runner_state",
                    "s",
                    Expr::col(("s", "operation_id")).eq(Expr::col(("o", "id"))),
                )
                .join_as(
                    JoinType::InnerJoin,
                    "turn",
                    "t",
                    Expr::col(("t", "id"))
                        .eq(Expr::col(("o", "execution_turn")))
                        .and(Expr::col(("t", "thread_id")).eq(Expr::col(("c", "thread_id")))),
                )
                .join_as(
                    JoinType::InnerJoin,
                    "thread",
                    "th",
                    Expr::col(("th", "id"))
                        .eq(Expr::col(("t", "thread_id")))
                        .and(
                            Expr::col(("th", "workspace_id")).eq(Expr::col(("c", "workspace_id"))),
                        ),
                )
                .and_where(
                    Expr::col(("o", "id"))
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::col(("s", "generation"))
                                .eq(Expr::Value(i64::try_from(generation)?.into())),
                        )
                        .and(
                            Expr::col(("c", "workspace_id"))
                                .eq(Expr::Value(event.workspace_id().into())),
                        )
                        .and(
                            Expr::col(("c", "thread_id")).eq(Expr::Value(event.thread_id().into())),
                        )
                        .and(Expr::col(("t", "id")).eq(Expr::Value(event.turn_id().into())))
                        .and(
                            Expr::Value(i64::from(terminal).into())
                                .eq(Expr::val(0_i64))
                                .and(Expr::col(("o", "status")).eq(Expr::val("running")))
                                .and(
                                    Expr::exists(
                                        Query::select()
                                            .expr(Expr::val(1_i64))
                                            .from_as("compaction_execution_stop", "stop")
                                            .and_where(
                                                Expr::col(("stop", "owner"))
                                                    .eq(Expr::col(("c", "owner")))
                                                    .and(
                                                        Expr::col(("stop", "turn_id"))
                                                            .eq(Expr::col(("t", "id"))),
                                                    ),
                                            )
                                            .to_owned(),
                                    )
                                    .not(),
                                )
                                .and(
                                    Expr::col(("t", "status"))
                                        .is_in(["interrupted", "cancelled"])
                                        .not(),
                                )
                                .or(Expr::Value(i64::from(terminal).into())
                                    .eq(Expr::val(1_i64))
                                    .and(Expr::col(("o", "status")).ne(Expr::val("running")))),
                        ),
                )
                .to_owned(),
        );
        self.with_maintenance_access()
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
}
