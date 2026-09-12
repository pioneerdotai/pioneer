//! Operation ownership is admitted from a ready reference manifest, never from
//! a caller's claim that an arbitrary foreign source is its own work.
use super::statement;
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::FrozenHistoryRef;
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};
use sea_orm::{ConnectionTrait, TransactionTrait};

impl CrudStore {
    pub async fn compaction_bound_source_projection(
        &self,
        operation: &str,
    ) -> Result<Option<FrozenHistoryRef>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("h", "id")))
                    .expr(Expr::col(("h", "identity_sha256")))
                    .expr(Expr::col(("h", "message_count")))
                    .from_as("compaction_operation_projection", "p")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_frozen_history",
                        "h",
                        Expr::col(("h", "id")).eq(Expr::col(("p", "manifest_id"))),
                    )
                    .and_where(
                        Expr::col(("p", "operation_id"))
                            .eq(Expr::Value(operation.into()))
                            .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                            .and(
                                Expr::col(("h", "identity_sha256"))
                                    .eq(Expr::col(("p", "identity_sha256"))),
                            )
                            .and(
                                Expr::col(("h", "imports_sha256"))
                                    .eq(Expr::col(("p", "imports_sha256"))),
                            )
                            .and(
                                Expr::col(("h", "import_count"))
                                    .eq(Expr::col(("p", "import_count"))),
                            )
                            .and(
                                Expr::col(("h", "next_import"))
                                    .eq(Expr::col(("p", "import_count"))),
                            ),
                    )
                    .to_owned(),
            ))
            .await?;
        row.map(|row| {
            Ok(FrozenHistoryRef {
                format: 1,
                manifest_id: row.try_get("", "id")?,
                identity_sha256: row.try_get("", "identity_sha256")?,
                messages: u64::try_from(row.try_get::<i64>("", "message_count")?)?,
            })
        })
        .transpose()
    }

    /// Serialization is outside writer capacity. The writer revalidates the
    /// ready manifest and exact execution/TaskRun snapshot before storing its
    /// identity. This immutable binding also survives operation recovery.
    pub async fn compaction_bind_source_projection(
        &self,
        operation: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<()> {
        ensure!(descriptor.format == 1, "unsupported source projection");
        let json = serde_json::to_string(descriptor)?;
        ensure!(
            json.len() <= super::SOURCE_PAGE_BYTES,
            "oversized source projection"
        );
        let count = i64::try_from(descriptor.messages)?;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            // A parent manifest is eligible only through the exact accepted
            // TaskRun basis for this execution. Output candidate selection
            // remains a separate boundary; it excludes reviewer outputs.
            tx.execute_raw(statement(
                &Query::insert()
                    .into_table("compaction_operation_projection")
                    .columns([
                        "operation_id",
                        "manifest_id",
                        "identity_sha256",
                        "imports_sha256",
                        "import_count",
                    ])
                    .select_from(
                        Query::select()
                            .expr(Expr::col(("o", "id")))
                            .expr(Expr::col(("h", "id")))
                            .expr(Expr::col(("h", "identity_sha256")))
                            .expr(Expr::col(("h", "imports_sha256")))
                            .expr(Expr::col(("h", "import_count")))
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
                                "execution",
                                Expr::col(("execution", "id"))
                                    .eq(Expr::col(("o", "execution_turn")))
                                    .and(
                                        Expr::col(("execution", "thread_id"))
                                            .eq(Expr::col(("c", "thread_id"))),
                                    ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "compaction_frozen_history",
                                "h",
                                Expr::col(("h", "workspace_id"))
                                    .eq(Expr::col(("c", "workspace_id"))),
                            )
                            .and_where(
                                Expr::col(("o", "id"))
                                    .eq(Expr::Value(operation.into()))
                                    .and(Expr::col(("o", "status")).eq(Expr::val("running")))
                                    .and(
                                        Expr::col(("execution", "status"))
                                            .is_in(["interrupted", "cancelled"])
                                            .not(),
                                    )
                                    .and(
                                        Expr::col(("h", "id"))
                                            .eq(Expr::Value(descriptor.manifest_id.clone().into())),
                                    )
                                    .and(
                                        Expr::col(("h", "identity_sha256")).eq(Expr::Value(
                                            descriptor.identity_sha256.clone().into(),
                                        )),
                                    )
                                    .and(
                                        Expr::col(("h", "message_count"))
                                            .eq(Expr::Value(count.into())),
                                    )
                                    .and(
                                        Expr::col(("h", "next_ordinal"))
                                            .eq(Expr::col(("h", "message_count"))),
                                    )
                                    .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                                    .and(
                                        Expr::col(("h", "import_count"))
                                            .eq(Expr::col(("h", "next_import"))),
                                    )
                                    .and(
                                        Expr::col(("h", "owner_thread"))
                                            .eq(Expr::col(("c", "thread_id")))
                                            .or(Expr::exists(
                                                Query::select()
                                                    .expr(Expr::val(1_i64))
                                                    .from_as("task_run_turn", "rt")
                                                    .join_as(
                                                        JoinType::InnerJoin,
                                                        "task_run",
                                                        "r",
                                                        Expr::col(("r", "id"))
                                                            .eq(Expr::col(("rt", "run_id")))
                                                            .and(
                                                                Expr::col(("r", "task_id")).eq(
                                                                    Expr::col(("rt", "task_id")),
                                                                ),
                                                            ),
                                                    )
                                                    .join_as(
                                                        JoinType::InnerJoin,
                                                        "task",
                                                        "t",
                                                        Expr::col(("t", "id"))
                                                            .eq(Expr::col(("r", "task_id")))
                                                            .and(
                                                                Expr::col(("t", "workspace_id"))
                                                                    .eq(Expr::col((
                                                                        "c",
                                                                        "workspace_id",
                                                                    ))),
                                                            ),
                                                    )
                                                    .join_as(
                                                        JoinType::InnerJoin,
                                                        "thread_lineage",
                                                        "lineage",
                                                        Expr::col(("lineage", "child_thread_id"))
                                                            .eq(Expr::col(("rt", "thread_id")))
                                                            .and(
                                                                Expr::col((
                                                                    "lineage",
                                                                    "parent_thread_id",
                                                                ))
                                                                .eq(Expr::col((
                                                                    "h",
                                                                    "owner_thread",
                                                                ))),
                                                            ),
                                                    )
                                                    .join_as(
                                                        JoinType::InnerJoin,
                                                        "task_run_conversation_snapshot",
                                                        "basis",
                                                        Expr::col(("basis", "run_id"))
                                                            .eq(Expr::col(("r", "id")))
                                                            .and(
                                                                Expr::col(("basis", "task_id"))
                                                                    .eq(Expr::col(("t", "id"))),
                                                            )
                                                            .and(
                                                                Expr::col((
                                                                    "basis",
                                                                    "workspace_id",
                                                                ))
                                                                .eq(Expr::col((
                                                                    "c",
                                                                    "workspace_id",
                                                                ))),
                                                            ),
                                                    )
                                                    .and_where(
                                                        Expr::col(("rt", "turn_id"))
                                                            .eq(Expr::col(("execution", "id")))
                                                            .and(
                                                                Expr::col(("rt", "thread_id")).eq(
                                                                    Expr::col(("c", "thread_id")),
                                                                ),
                                                            )
                                                            .and(
                                                                Expr::col((
                                                                    "basis",
                                                                    "conversation_thread_id",
                                                                ))
                                                                .eq(Expr::col((
                                                                    "h",
                                                                    "owner_thread",
                                                                ))),
                                                            )
                                                            .and(
                                                                Expr::col((
                                                                    "basis",
                                                                    "history_json",
                                                                ))
                                                                .eq(Expr::Value(
                                                                    json.clone().into(),
                                                                )),
                                                            ),
                                                    )
                                                    .to_owned(),
                                            )),
                                    )
                                    .and(
                                        Expr::exists(
                                            Query::select()
                                                .expr(Expr::val(1_i64))
                                                .from_as("compaction_runner_plan", "p")
                                                .and_where(
                                                    Expr::col(("p", "operation_id"))
                                                        .eq(Expr::col(("o", "id")))
                                                        .and(
                                                            Expr::col(("p", "ready"))
                                                                .eq(Expr::val(1_i64)),
                                                        ),
                                                )
                                                .to_owned(),
                                        )
                                        .not(),
                                    ),
                            )
                            .to_owned(),
                    )?
                    .on_conflict(
                        OnConflict::columns(["operation_id"])
                            .do_nothing()
                            .to_owned(),
                    )
                    .to_owned(),
            ))
            .await?;
            ensure!(
                tx.query_one_raw(statement(
                    &Query::select()
                        .expr(Expr::col(("p", "operation_id")))
                        .from_as("compaction_operation_projection", "p")
                        .join_as(
                            JoinType::InnerJoin,
                            "compaction_frozen_history",
                            "h",
                            Expr::col(("h", "id")).eq(Expr::col(("p", "manifest_id")))
                        )
                        .and_where(
                            Expr::col(("p", "operation_id"))
                                .eq(Expr::Value(operation.into()))
                                .and(
                                    Expr::col(("p", "manifest_id"))
                                        .eq(Expr::Value(descriptor.manifest_id.clone().into()))
                                )
                                .and(
                                    Expr::col(("p", "identity_sha256"))
                                        .eq(Expr::Value(descriptor.identity_sha256.clone().into()))
                                )
                                .and(
                                    Expr::col(("h", "message_count")).eq(Expr::Value(count.into()))
                                )
                                .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                                .and(
                                    Expr::col(("h", "identity_sha256"))
                                        .eq(Expr::col(("p", "identity_sha256")))
                                )
                                .and(
                                    Expr::col(("h", "imports_sha256"))
                                        .eq(Expr::col(("p", "imports_sha256")))
                                )
                                .and(
                                    Expr::col(("h", "import_count"))
                                        .eq(Expr::col(("p", "import_count")))
                                )
                                .and(
                                    Expr::col(("h", "next_import"))
                                        .eq(Expr::col(("p", "import_count")))
                                )
                        )
                        .to_owned()
                ))
                .await?
                .is_some(),
                "source projection is not accepted by this execution or binding changed"
            );
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}
