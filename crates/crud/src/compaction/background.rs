//! Durable locators for optional completed CLI history checks. No history copy.
use super::*;
use sea_orm::sea_query::{
    Alias, BinOper, Expr, ExprTrait, Func, JoinType, OnConflict, Order, Query,
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
impl CrudStore {
    pub async fn compaction_enqueue_native_history_check(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        descriptor: &str,
    ) -> Result<()> {
        ensure!(
            descriptor.len() <= 16384,
            "history descriptor exceeds bound"
        );
        self.connection
            .execute_raw(statement(
                &Query::insert()
                    .into_table("compaction_history_check")
                    .columns(["turn_id", "descriptor"])
                    .select_from(
                        Query::select()
                            .expr(Expr::col(("t", "id")))
                            .expr(Expr::Value(descriptor.into()))
                            .from_as("turn", "t")
                            .join_as(
                                JoinType::InnerJoin,
                                "thread",
                                "th",
                                Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                            )
                            .and_where(
                                Expr::col(("t", "id"))
                                    .eq(Expr::Value(turn.into()))
                                    .and(
                                        Expr::col(("t", "thread_id"))
                                            .eq(Expr::Value(thread.into())),
                                    )
                                    .and(
                                        Expr::col(("th", "workspace_id"))
                                            .eq(Expr::Value(workspace.into())),
                                    )
                                    .and(Expr::col(("t", "status")).eq(Expr::val("completed"))),
                            )
                            .to_owned(),
                    )?
                    .on_conflict(OnConflict::columns(["turn_id"]).do_nothing().to_owned())
                    .to_owned(),
            ))
            .await?;
        Ok(())
    }
    /// Reconcile lost terminal publications and abandoned deadline/Stop states
    /// using bounded metadata. This scanner never admits a service generation.
    pub async fn compaction_lifecycle_recovery(
        &self,
        now_ms: u64,
        after: &str,
    ) -> Result<Vec<CompactionLifecycleRecovery>> {
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col(("o", "id")))
                    .expr(Expr::col(("o", "owner")))
                    .expr(Expr::col(("c", "workspace_id")))
                    .expr(Expr::col(("c", "thread_id")))
                    .expr_as(Expr::col(("o", "execution_turn")), "turn_id")
                    .expr(Expr::col(("o", "status")))
                    .expr_as(
                        Expr::col(("t", "status"))
                            .is_in(["interrupted", "cancelled"])
                            .or(Expr::exists(
                                Query::select()
                                    .expr(Expr::val(1_i64))
                                    .from_as("compaction_execution_stop", "stop")
                                    .and_where(
                                        Expr::col(("stop", "owner"))
                                            .eq(Expr::col(("o", "owner")))
                                            .and(
                                                Expr::col(("stop", "turn_id"))
                                                    .eq(Expr::col(("t", "id"))),
                                            ),
                                    )
                                    .to_owned(),
                            )),
                        "cancelled",
                    )
                    .from_as("compaction_operation", "o")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_context",
                        "c",
                        Expr::col(("c", "owner")).eq(Expr::col(("o", "owner"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id"))
                            .eq(Expr::col(("o", "execution_turn")))
                            .and(Expr::col(("t", "thread_id")).eq(Expr::col(("c", "thread_id")))),
                    )
                    .and_where(
                        Expr::col(("o", "id")).gt(Expr::Value(after.into())).and(
                            Expr::col(("o", "status"))
                                .eq(Expr::val("running"))
                                .and(
                                    Expr::col(("o", "deadline_ms"))
                                        .lte(Expr::Value(i64::try_from(now_ms)?.into()))
                                        .or(Expr::col(("t", "status"))
                                            .is_in(["interrupted", "cancelled"]))
                                        .or(Expr::exists(
                                            Query::select()
                                                .expr(Expr::val(1_i64))
                                                .from_as("compaction_execution_stop", "stop")
                                                .and_where(
                                                    Expr::col(("stop", "owner"))
                                                        .eq(Expr::col(("o", "owner")))
                                                        .and(
                                                            Expr::col(("stop", "turn_id"))
                                                                .eq(Expr::col(("t", "id"))),
                                                        ),
                                                )
                                                .to_owned(),
                                        )),
                                )
                                .or(Expr::col(("o", "status"))
                                    .ne(Expr::val("running"))
                                    .and(Expr::exists(
                                        Query::select()
                                            .expr(Expr::val(1_i64))
                                            .from_as("compaction_runner_state", "r")
                                            .and_where(
                                                Expr::col(("r", "operation_id"))
                                                    .eq(Expr::col(("o", "id"))),
                                            )
                                            .to_owned(),
                                    ))
                                    .and(
                                        Expr::exists(
                                            Query::select()
                                                .expr(Expr::val(1_i64))
                                                .from_as("turn_item", "item")
                                                .and_where(
                                                    Expr::col(("item", "turn_id"))
                                                        .eq(Expr::col(("t", "id")))
                                                        .and(Expr::col(("item", "item_id")).eq(
                                                            Expr::val("compaction:").binary(
                                                                BinOper::Custom("||"),
                                                                Expr::col(("o", "id")),
                                                            ),
                                                        ))
                                                        .and(
                                                            Expr::expr(
                                                                Func::cust(Alias::new(
                                                                    "json_extract",
                                                                ))
                                                                .args([
                                                                    Expr::col(("item", "payload")),
                                                                    Expr::val("$.details.status"),
                                                                ]),
                                                            )
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
                    .order_by_expr(Expr::col(("o", "id")), Order::Asc)
                    .limit(16)
                    .to_owned(),
            ))
            .await?;
        rows.iter()
            .map(|row| CompactionLifecycleRecovery::from_query_result(row, "").map_err(Into::into))
            .collect()
    }

    /// A newer execution invalidates an optional older check. Inspect one
    /// locator at a time rather than bulk-updating a thread's retained history.
    pub async fn compaction_history_check_is_current(&self, turn: &str) -> Result<bool> {
        Ok(self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("q", "turn_id")))
                    .from_as("compaction_history_check", "q")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("q", "turn_id"))),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        "compaction_turn_creation",
                        "own",
                        Expr::col(("own", "turn_id")).eq(Expr::col(("t", "id"))),
                    )
                    .and_where(
                        Expr::col(("q", "turn_id"))
                            .eq(Expr::Value(turn.into()))
                            .and(Expr::col(("q", "state")).eq(Expr::val("pending")))
                            .and(Expr::col(("t", "status")).eq(Expr::val("completed")))
                            .and(
                                Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as("compaction_execution_stop", "stop")
                                        .join_as(
                                            JoinType::InnerJoin,
                                            "compaction_context",
                                            "c",
                                            Expr::col(("c", "owner"))
                                                .eq(Expr::col(("stop", "owner"))),
                                        )
                                        .and_where(
                                            Expr::col(("stop", "turn_id"))
                                                .eq(Expr::col(("t", "id")))
                                                .and(
                                                    Expr::col(("c", "thread_id"))
                                                        .eq(Expr::col(("t", "thread_id"))),
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
                                        .from_as("turn", "later")
                                        .join_as(
                                            JoinType::InnerJoin,
                                            "compaction_turn_creation",
                                            "seq",
                                            Expr::col(("seq", "turn_id"))
                                                .eq(Expr::col(("later", "id"))),
                                        )
                                        .and_where(
                                            Expr::col(("later", "thread_id"))
                                                .eq(Expr::col(("t", "thread_id")))
                                                .and(Expr::col(("seq", "sequence")).gt(
                                                    Expr::expr(
                                                        Func::cust(Alias::new("coalesce")).args([
                                                            Expr::col(("own", "sequence")),
                                                            Expr::val(0_i64),
                                                        ]),
                                                    ),
                                                )),
                                        )
                                        .to_owned(),
                                )
                                .not(),
                            ),
                    )
                    .to_owned(),
            ))
            .await?
            .is_some())
    }
    /// One bounded metadata page; caller releases reader capacity before decoding.
    pub async fn compaction_pending_history_checks(&self) -> Result<Vec<CompletedHistoryCheck>> {
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col(("q", "turn_id")))
                    .expr(Expr::col(("t", "thread_id")))
                    .expr(Expr::col(("th", "workspace_id")))
                    .expr_as(
                        Expr::expr(
                            Func::cust(Alias::new("coalesce"))
                                .args([Expr::col(("b", "runtime_id")), Expr::val("")]),
                        ),
                        "runtime_id",
                    )
                    .expr_as(
                        Expr::expr(
                            Func::cust(Alias::new("coalesce"))
                                .args([Expr::col(("b", "runtime_kind")), Expr::val("")]),
                        ),
                        "runtime_kind",
                    )
                    .expr(Expr::col(("b", "model")))
                    .expr(Expr::col(("t", "reasoning_effort")))
                    .expr(Expr::col(("q", "descriptor")))
                    .from_as("compaction_history_check", "q")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("q", "turn_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        "turn_cli_runtime_binding",
                        "b",
                        Expr::col(("b", "turn_id"))
                            .eq(Expr::col(("t", "id")))
                            .and(Expr::col(("b", "thread_id")).eq(Expr::col(("t", "thread_id"))))
                            .and(
                                Expr::col(("b", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id"))),
                            ),
                    )
                    .and_where(Expr::col(("q", "state")).eq(Expr::val("pending")))
                    .order_by_expr(Expr::col(("q", "turn_id")), Order::Asc)
                    .limit(16)
                    .to_owned(),
            ))
            .await?;
        rows.iter()
            .map(|row| CompletedHistoryCheck::from_query_result(row, "").map_err(Into::into))
            .collect()
    }
    /// Persist the captured settings/deadline before service admission; restart
    /// reuses this exact descriptor. It contains model metadata, never messages.
    pub async fn compaction_capture_history_check(
        &self,
        turn: &str,
        descriptor: &str,
    ) -> Result<Option<String>> {
        ensure!(
            descriptor.len() <= 16384,
            "CLI history descriptor exceeds bound"
        );
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::update()
                    .table("compaction_history_check")
                    .value(
                        "descriptor",
                        Expr::expr(
                            Func::cust(Alias::new("coalesce"))
                                .args([Expr::col("descriptor"), Expr::Value(descriptor.into())]),
                        ),
                    )
                    .and_where(
                        Expr::col("turn_id")
                            .eq(Expr::Value(turn.into()))
                            .and(Expr::col("state").eq(Expr::val("pending"))),
                    )
                    .returning(Query::returning().expr(Expr::col("descriptor")).to_owned())
                    .to_owned(),
            ))
            .await?;
        row.map(|row| row.try_get("", "descriptor").map_err(Into::into))
            .transpose()
    }
    pub async fn compaction_finish_history_check(&self, turn: &str, outcome: &str) -> Result<()> {
        ensure!(
            matches!(outcome, "completed" | "cancelled" | "failed" | "disabled"),
            "invalid CLI history outcome"
        );
        self.connection
            .execute_raw(statement(
                &Query::update()
                    .table("compaction_history_check")
                    .value("state", Expr::val("finished"))
                    .value("outcome", Expr::Value(outcome.into()))
                    .and_where(
                        Expr::col("turn_id")
                            .eq(Expr::Value(turn.into()))
                            .and(Expr::col("state").eq(Expr::val("pending"))),
                    )
                    .to_owned(),
            ))
            .await?;
        Ok(())
    }
}
