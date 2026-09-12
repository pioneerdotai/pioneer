//! Metadata-only capture fences for history projection. The fence limits later
//! discovery; payload reads still require workspace/thread scope and revisions.
use super::*;
use sea_orm::sea_query::{Alias, BinOper, Expr, ExprTrait, Func, JoinType, Order, Query};

#[derive(Clone, Debug)]
pub struct HistoryReadFence {
    pub turn_order: i64,
    pub turn_id: String,
    pub input_order: i64,
    pub event_order: i64,
    pub context_order: i64,
    pub item_order: i64,
}
#[derive(Clone, Debug, FromQueryResult)]
pub struct HistoryTurnBoundary {
    pub creation_order: i64,
    /// Tie-breaker for turns predating the creation-order trigger. This is
    /// ordering metadata only, never proof of source coverage or a read fence.
    pub legacy_creation_order: i64,
    pub id: String,
    pub created_at: String,
    pub status: String,
    pub turn_kind: String,
    pub send_mode: Option<String>,
    pub input_high_water: i64,
    pub event_high_water: i64,
    pub context_high_water: i64,
    pub item_high_water: i64,
}
#[derive(Clone, Debug, FromQueryResult)]
pub struct HistoryCausalBoundary {
    pub delegated_command: bool,
    pub task_transport: bool,
    pub delivered_outcome: bool,
}
/// The immutable basis accepted for one exact child execution. This is a
/// storage locator plus its existing descriptor, not a new history copy.
#[derive(Clone, Debug)]
pub struct AcceptedTaskBasis {
    pub parent_thread: String,
    pub run_id: String,
    pub history_json: String,
}

impl CrudStore {
    /// Scope checks must not materialize thread.summary or any source payload.
    pub async fn compaction_thread_exists(&self, workspace: &str, thread: &str) -> Result<bool> {
        Ok(self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::val(1_i64))
                    .from("thread")
                    .and_where(
                        Expr::col("workspace_id")
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col("id").eq(Expr::Value(thread.into()))),
                    )
                    .to_owned(),
            ))
            .await?
            .is_some())
    }

    pub async fn compaction_turn_is_completed(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<bool> {
        Ok(self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::val(1_i64))
                    .from_as("turn", "t")
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .and_where(
                        Expr::col(("th", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("t", "thread_id")).eq(Expr::Value(thread.into())))
                            .and(Expr::col(("t", "id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("t", "status")).eq(Expr::val("completed"))),
                    )
                    .to_owned(),
            ))
            .await?
            .is_some())
    }

    /// Metadata locator for an already accepted legacy array. Payload remains
    /// in its original immutable TaskRun snapshot, read through source fragments.
    pub async fn compaction_legacy_task_basis_source(
        &self,
        workspace: &str,
        parent: &str,
        run: &str,
    ) -> Result<Option<SourceRef>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("source_scope"))
                    .expr(Expr::col("source_id"))
                    .expr(Expr::col("source_version"))
                    .from("compaction_live_sources")
                    .and_where(
                        Expr::col("workspace_id")
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col("thread_id").eq(Expr::Value(parent.into())))
                            .and(
                                Expr::col("source_scope").eq(Expr::val("task-basis:")
                                    .binary(BinOper::Custom("||"), Expr::Value(run.into()))),
                            )
                            .and(Expr::col("source_id").eq(Expr::Value(run.into()))),
                    )
                    .to_owned(),
            ))
            .await?;
        row.map(|row| {
            Ok(SourceRef {
                scope: row.try_get("", "source_scope")?,
                id: row.try_get("", "source_id")?,
                version: row.try_get("", "source_version")?,
            })
        })
        .transpose()
    }

    /// The parent basis admitted for this exact child execution. Attachment is
    /// deliberately irrelevant: it controls lifecycle/hooks, not history scope.
    /// Read only identity metadata, never the snapshot transcript or Task body.
    pub async fn compaction_task_basis_thread(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<String>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "conversation_thread_id")))
                    .from_as("task_run_turn", "rt")
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run_conversation_snapshot",
                        "s",
                        Expr::col(("s", "run_id"))
                            .eq(Expr::col(("rt", "run_id")))
                            .and(Expr::col(("s", "task_id")).eq(Expr::col(("rt", "task_id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread_lineage",
                        "l",
                        Expr::col(("l", "child_thread_id"))
                            .eq(Expr::col(("rt", "thread_id")))
                            .and(
                                Expr::col(("l", "parent_thread_id"))
                                    .eq(Expr::col(("s", "conversation_thread_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "child",
                        Expr::col(("child", "id")).eq(Expr::col(("rt", "thread_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "parent",
                        Expr::col(("parent", "id")).eq(Expr::col(("s", "conversation_thread_id"))),
                    )
                    .and_where(
                        Expr::col(("rt", "thread_id"))
                            .eq(Expr::Value(thread.into()))
                            .and(Expr::col(("rt", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("s", "workspace_id")).eq(Expr::Value(workspace.into())))
                            .and(
                                Expr::col(("child", "workspace_id"))
                                    .eq(Expr::col(("s", "workspace_id"))),
                            )
                            .and(
                                Expr::col(("parent", "workspace_id"))
                                    .eq(Expr::col(("s", "workspace_id"))),
                            ),
                    )
                    .to_owned(),
            ))
            .await?;
        row.map(|row| {
            row.try_get("", "conversation_thread_id")
                .map_err(Into::into)
        })
        .transpose()
    }

    /// For a destination without a creator execution, select the most recent
    /// admitted basis whose actual input existed at the shared capture fence.
    /// This reads relationship metadata, never a newer ancestor transcript.
    pub async fn compaction_latest_task_basis_turn(
        &self,
        workspace: &str,
        thread: &str,
        fence: &HistoryReadFence,
    ) -> Result<Option<String>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("t", "id")))
                    .from_as("turn", "t")
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run_turn",
                        "rt",
                        Expr::col(("rt", "thread_id"))
                            .eq(Expr::col(("t", "thread_id")))
                            .and(Expr::col(("rt", "turn_id")).eq(Expr::col(("t", "id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run_conversation_snapshot",
                        "s",
                        Expr::col(("s", "run_id"))
                            .eq(Expr::col(("rt", "run_id")))
                            .and(Expr::col(("s", "task_id")).eq(Expr::col(("rt", "task_id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread_lineage",
                        "l",
                        Expr::col(("l", "child_thread_id"))
                            .eq(Expr::col(("t", "thread_id")))
                            .and(
                                Expr::col(("l", "parent_thread_id"))
                                    .eq(Expr::col(("s", "conversation_thread_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "parent",
                        Expr::col(("parent", "id")).eq(Expr::col(("s", "conversation_thread_id"))),
                    )
                    .and_where(
                        Expr::col(("th", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("t", "thread_id")).eq(Expr::Value(thread.into())))
                            .and(
                                Expr::col(("s", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id"))),
                            )
                            .and(
                                Expr::col(("parent", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id"))),
                            )
                            .and(Expr::exists(
                                Query::select()
                                    .expr(Expr::val(1_i64))
                                    .from_as("turn_input", "i")
                                    .join_as(
                                        JoinType::InnerJoin,
                                        "compaction_input_revision",
                                        "r",
                                        Expr::col(("r", "source_id"))
                                            .eq(Expr::col(("i", "id")))
                                            .and(
                                                Expr::col(("r", "turn_id"))
                                                    .eq(Expr::col(("i", "turn_id"))),
                                            )
                                            .and(Expr::col(("r", "present")).eq(Expr::val(1_i64))),
                                    )
                                    .and_where(
                                        Expr::col(("i", "turn_id")).eq(Expr::col(("t", "id"))).and(
                                            Expr::col(("r", "capture_order"))
                                                .lte(Expr::Value(fence.input_order.into())),
                                        ),
                                    )
                                    .to_owned(),
                            )),
                    )
                    .order_by_expr(Expr::col(("t", "created_at")), Order::Desc)
                    .order_by_expr(Expr::col(("t", "id")), Order::Desc)
                    .limit(1)
                    .to_owned(),
            ))
            .await?;
        row.map(|row| row.try_get("", "id").map_err(Into::into))
            .transpose()
    }

    /// Read the already accepted TaskRun basis in byte-bounded fragments.
    /// Each fragment repeats the exact execution/lineage/workspace predicate;
    /// deletion or reparenting cannot yield a partially authorized transcript.
    /// Snapshot rows are immutable (insert-if-absent); decoding is outside DB
    /// capacity. Legacy arrays retain their original bytes until migration.
    pub async fn compaction_task_basis_snapshot(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<AcceptedTaskBasis>> {
        let Some(row) = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "run_id")))
                    .expr(Expr::col(("s", "conversation_thread_id")))
                    .expr(Expr::col(("s", "created_at")))
                    .expr_as(
                        Expr::expr(
                            Func::cust(Alias::new("length")).args([Expr::col((
                                "s",
                                "history_json",
                            ))
                            .cast_as(Alias::new("BLOB"))]),
                        ),
                        "history_bytes",
                    )
                    .from_as("task_run_turn", "rt")
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run_conversation_snapshot",
                        "s",
                        Expr::col(("s", "run_id"))
                            .eq(Expr::col(("rt", "run_id")))
                            .and(Expr::col(("s", "task_id")).eq(Expr::col(("rt", "task_id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread_lineage",
                        "l",
                        Expr::col(("l", "child_thread_id"))
                            .eq(Expr::col(("rt", "thread_id")))
                            .and(
                                Expr::col(("l", "parent_thread_id"))
                                    .eq(Expr::col(("s", "conversation_thread_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "child",
                        Expr::col(("child", "id")).eq(Expr::col(("rt", "thread_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "parent",
                        Expr::col(("parent", "id")).eq(Expr::col(("s", "conversation_thread_id"))),
                    )
                    .and_where(
                        Expr::col(("rt", "thread_id"))
                            .eq(Expr::Value(thread.into()))
                            .and(Expr::col(("rt", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("s", "workspace_id")).eq(Expr::Value(workspace.into())))
                            .and(
                                Expr::col(("child", "workspace_id"))
                                    .eq(Expr::col(("s", "workspace_id"))),
                            )
                            .and(
                                Expr::col(("parent", "workspace_id"))
                                    .eq(Expr::col(("s", "workspace_id"))),
                            ),
                    )
                    .to_owned(),
            ))
            .await?
        else {
            return Ok(None);
        };
        let run_id: String = row.try_get("", "run_id")?;
        let parent_thread: String = row.try_get("", "conversation_thread_id")?;
        let created_at: String = row.try_get("", "created_at")?;
        let history_bytes = usize::try_from(row.try_get::<i64>("", "history_bytes")?)?;
        let mut bytes = Vec::new();
        bytes.try_reserve(history_bytes)?;
        while bytes.len() < history_bytes {
            let count = SOURCE_PAGE_BYTES.min(history_bytes - bytes.len());
            let row = self
                .connection
                .query_one_raw(statement(
                    &Query::select()
                        .expr_as(
                            Expr::expr(Func::cust(Alias::new("substr")).args([
                                Expr::col(("s", "history_json")).cast_as(Alias::new("BLOB")),
                                Expr::Value(i64::try_from(bytes.len() + 1)?.into()),
                                Expr::Value(i64::try_from(count)?.into()),
                            ])),
                            "fragment",
                        )
                        .from_as("task_run_turn", "rt")
                        .join_as(
                            JoinType::InnerJoin,
                            "task_run_conversation_snapshot",
                            "s",
                            Expr::col(("s", "run_id"))
                                .eq(Expr::col(("rt", "run_id")))
                                .and(Expr::col(("s", "task_id")).eq(Expr::col(("rt", "task_id")))),
                        )
                        .join_as(
                            JoinType::InnerJoin,
                            "thread_lineage",
                            "l",
                            Expr::col(("l", "child_thread_id"))
                                .eq(Expr::col(("rt", "thread_id")))
                                .and(
                                    Expr::col(("l", "parent_thread_id"))
                                        .eq(Expr::col(("s", "conversation_thread_id"))),
                                ),
                        )
                        .join_as(
                            JoinType::InnerJoin,
                            "thread",
                            "child",
                            Expr::col(("child", "id")).eq(Expr::col(("rt", "thread_id"))),
                        )
                        .join_as(
                            JoinType::InnerJoin,
                            "thread",
                            "parent",
                            Expr::col(("parent", "id"))
                                .eq(Expr::col(("s", "conversation_thread_id"))),
                        )
                        .and_where(
                            Expr::col(("rt", "thread_id"))
                                .eq(Expr::Value(thread.into()))
                                .and(Expr::col(("rt", "turn_id")).eq(Expr::Value(turn.into())))
                                .and(
                                    Expr::col(("s", "workspace_id"))
                                        .eq(Expr::Value(workspace.into())),
                                )
                                .and(
                                    Expr::col(("child", "workspace_id"))
                                        .eq(Expr::col(("s", "workspace_id"))),
                                )
                                .and(
                                    Expr::col(("parent", "workspace_id"))
                                        .eq(Expr::col(("s", "workspace_id"))),
                                )
                                .and(
                                    Expr::col(("s", "run_id"))
                                        .eq(Expr::Value(run_id.clone().into())),
                                )
                                .and(
                                    Expr::col(("s", "conversation_thread_id"))
                                        .eq(Expr::Value(parent_thread.clone().into())),
                                )
                                .and(
                                    Expr::col(("s", "created_at"))
                                        .eq(Expr::Value(created_at.clone().into())),
                                )
                                .and(
                                    Expr::expr(
                                        Func::cust(Alias::new("length")).args([Expr::col((
                                            "s",
                                            "history_json",
                                        ))
                                        .cast_as(Alias::new("BLOB"))]),
                                    )
                                    .eq(Expr::Value(i64::try_from(history_bytes)?.into())),
                                ),
                        )
                        .to_owned(),
                ))
                .await?
                .ok_or_else(|| anyhow::anyhow!("accepted Task basis changed during read"))?;
            let fragment: Vec<u8> = row.try_get("", "fragment")?;
            ensure!(
                fragment.len() == count,
                "accepted Task basis fragment is incomplete"
            );
            bytes.extend(fragment);
        }
        Ok(Some(AcceptedTaskBasis {
            parent_thread,
            run_id,
            history_json: String::from_utf8(bytes)?,
        }))
    }

    /// Refresh a stale metadata cache after decoding a scoped canonical source
    /// outside database capacity. The revision CAS prevents a later event edit
    /// from being labelled using the earlier typed payload.
    pub async fn compaction_record_event_projection(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
        event: &crate::CanonicalTurnEventPayload,
    ) -> Result<bool> {
        let turn = source
            .scope
            .strip_prefix("event:")
            .ok_or_else(|| anyhow::anyhow!("projection source is not an event"))?;
        ensure!(
            event.workspace_id() == workspace
                && event.thread_id() == thread
                && event.turn_id() == turn,
            "canonical projection scope mismatch"
        );
        let (item, kind) = event_projection_metadata(event);
        Ok(self
            .connection
            .execute_raw(statement(
                &Query::update()
                    .table("compaction_event_revision")
                    .value("projection_revision", Expr::col("revision"))
                    .value("item_id", Expr::Value(item.into()))
                    .value("projection_kind", Expr::Value(kind.into()))
                    .and_where(
                        Expr::col("source_id")
                            .eq(Expr::Value(source.id.clone().into()))
                            .and(Expr::col("turn_id").eq(Expr::Value(turn.into())))
                            .and(Expr::col("present").eq(Expr::val(1_i64)))
                            .and(
                                Expr::val("event-revision:")
                                    .binary(BinOper::Custom("||"), Expr::col("revision"))
                                    .eq(Expr::Value(source.version.clone().into())),
                            )
                            .and(Expr::exists(
                                Query::select()
                                    .expr(Expr::val(1_i64))
                                    .from_as("turn_event", "e")
                                    .join_as(
                                        JoinType::InnerJoin,
                                        "turn",
                                        "t",
                                        Expr::col(("t", "id")).eq(Expr::col(("e", "turn_id"))).and(
                                            Expr::col(("t", "thread_id"))
                                                .eq(Expr::col(("e", "thread_id"))),
                                        ),
                                    )
                                    .join_as(
                                        JoinType::InnerJoin,
                                        "thread",
                                        "th",
                                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                                    )
                                    .and_where(
                                        Expr::col(("e", "id"))
                                            .eq(Expr::col((
                                                "compaction_event_revision",
                                                "source_id",
                                            )))
                                            .and(Expr::col(("e", "turn_id")).eq(Expr::col((
                                                "compaction_event_revision",
                                                "turn_id",
                                            ))))
                                            .and(
                                                Expr::col(("e", "thread_id"))
                                                    .eq(Expr::Value(thread.into())),
                                            )
                                            .and(
                                                Expr::col(("th", "workspace_id"))
                                                    .eq(Expr::Value(workspace.into())),
                                            ),
                                    )
                                    .to_owned(),
                            )),
                    )
                    .to_owned(),
            ))
            .await?
            .rows_affected()
            == 1)
    }

    /// Exact command/outcome relationship for a canonical delivered result.
    /// This is a point metadata query: neither matching text nor a delivered
    /// status alone establishes an alias. Frozen manifests preserve this link.
    pub async fn compaction_task_delivery_command(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        let Some(turn) = source.scope.strip_prefix("event:") else {
            return Ok(None);
        };
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("task", "created_by_turn_id")))
                    .from_as("turn_event", "e")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id"))
                            .eq(Expr::col(("e", "turn_id")))
                            .and(Expr::col(("t", "thread_id")).eq(Expr::col(("e", "thread_id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_event_revision",
                        "r",
                        Expr::col(("r", "source_id"))
                            .eq(Expr::col(("e", "id")))
                            .and(Expr::col(("r", "turn_id")).eq(Expr::col(("e", "turn_id"))))
                            .and(Expr::col(("r", "present")).eq(Expr::val(1_i64)))
                            .and(
                                Expr::col(("r", "projection_revision"))
                                    .eq(Expr::col(("r", "revision"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task_delivery",
                        "d",
                        Expr::col(("d", "delivered_turn_id"))
                            .eq(Expr::col(("e", "turn_id")))
                            .and(
                                Expr::col(("d", "target_thread_id"))
                                    .eq(Expr::col(("e", "thread_id"))),
                            )
                            .and(
                                Expr::col(("d", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id"))),
                            )
                            .and(
                                Expr::col(("r", "item_id")).eq(Expr::Value(
                                    pioneer_protocol::task_delivery_result_item_id("").into(),
                                )
                                .binary(BinOper::Custom("||"), Expr::col(("d", "id")))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task",
                        "task",
                        Expr::col(("task", "id"))
                            .eq(Expr::col(("d", "task_id")))
                            .and(
                                Expr::col(("task", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id"))),
                            )
                            .and(
                                Expr::col(("task", "created_by_thread_id"))
                                    .eq(Expr::col(("e", "thread_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run",
                        "run",
                        Expr::col(("run", "id"))
                            .eq(Expr::col(("d", "run_id")))
                            .and(Expr::col(("run", "task_id")).eq(Expr::col(("task", "id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "command",
                        Expr::col(("command", "id"))
                            .eq(Expr::col(("task", "created_by_turn_id")))
                            .and(
                                Expr::col(("command", "thread_id"))
                                    .eq(Expr::col(("task", "created_by_thread_id"))),
                            ),
                    )
                    .and_where(
                        Expr::col(("th", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("e", "thread_id")).eq(Expr::Value(thread.into())))
                            .and(Expr::col(("e", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("e", "id")).eq(Expr::Value(source.id.clone().into())))
                            .and(
                                Expr::val("event-revision:")
                                    .binary(BinOper::Custom("||"), Expr::col(("r", "revision")))
                                    .eq(Expr::Value(source.version.clone().into())),
                            )
                            .and(Expr::col(("e", "event_type")).eq(Expr::Value(
                                pioneer_protocol::constants::events::ITEM_COMPLETED.into(),
                            )))
                            .and(Expr::col(("d", "status")).eq(Expr::val("delivered")))
                            .and(
                                Expr::col(("task", "created_by_turn_id"))
                                    .binary(BinOper::Is, Expr::val(Option::<String>::None))
                                    .not(),
                            ),
                    )
                    .to_owned(),
            ))
            .await?;
        if let Some(row) = row {
            return Ok(Some(row.try_get("", "created_by_turn_id")?));
        }
        // Failed, blocked and interrupted occurrence turns have no result item.
        // Their exact TaskRun identity and acknowledged event establish closure,
        // including cancellation which intentionally suppresses TaskDelivery.
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("task", "created_by_turn_id")))
                    .from_as("turn_event", "e")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id"))
                            .eq(Expr::col(("e", "turn_id")))
                            .and(Expr::col(("t", "thread_id")).eq(Expr::col(("e", "thread_id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_event_revision",
                        "r",
                        Expr::col(("r", "source_id"))
                            .eq(Expr::col(("e", "id")))
                            .and(Expr::col(("r", "turn_id")).eq(Expr::col(("e", "turn_id"))))
                            .and(Expr::col(("r", "present")).eq(Expr::val(1_i64))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run",
                        "run",
                        Expr::col(("run", "id")).eq(Expr::col(("t", "id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task",
                        "task",
                        Expr::col(("task", "id"))
                            .eq(Expr::col(("run", "task_id")))
                            .and(
                                Expr::col(("task", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id"))),
                            )
                            .and(
                                Expr::col(("task", "created_by_thread_id"))
                                    .eq(Expr::col(("t", "thread_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "command",
                        Expr::col(("command", "id"))
                            .eq(Expr::col(("task", "created_by_turn_id")))
                            .and(
                                Expr::col(("command", "thread_id"))
                                    .eq(Expr::col(("t", "thread_id"))),
                            ),
                    )
                    .and_where(
                        Expr::col(("th", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("e", "thread_id")).eq(Expr::Value(thread.into())))
                            .and(Expr::col(("e", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("e", "id")).eq(Expr::Value(source.id.clone().into())))
                            .and(
                                Expr::val("event-revision:")
                                    .binary(BinOper::Custom("||"), Expr::col(("r", "revision")))
                                    .eq(Expr::Value(source.version.clone().into())),
                            )
                            .and(Expr::col(("t", "turn_kind")).eq(Expr::val("task_run")))
                            .and(Expr::col(("e", "event_type")).is_in([
                                Expr::Value(
                                    pioneer_protocol::constants::events::TURN_FAILED.into(),
                                ),
                                Expr::Value(
                                    pioneer_protocol::constants::events::TURN_BLOCKED.into(),
                                ),
                            ])),
                    )
                    .to_owned(),
            ))
            .await?;
        if let Some(row) = row {
            return Ok(Some(row.try_get("", "created_by_turn_id")?));
        }
        self.compaction_failed_delivery_command(workspace, thread, None, Some(source), i64::MAX)
            .await
    }

    /// Generic failed deliveries use a deterministic Turn ID, checked after
    /// releasing DB capacity. Page only delivery identities whose exact failure
    /// event is visible; never infer an outcome from mutable TaskRun status.
    async fn compaction_failed_delivery_command(
        &self,
        workspace: &str,
        thread: &str,
        command: Option<&str>,
        source: Option<&SourceRef>,
        event_fence: i64,
    ) -> Result<Option<String>> {
        let mut after = String::new();
        loop {
            let rows = self.connection.query_all_raw(statement(&Query::select().expr(Expr::col(("d", "id"))).expr(Expr::col(("d", "delivered_turn_id"))).expr(Expr::col(("task", "created_by_turn_id"))).from_as("task_delivery", "d").join_as(JoinType::InnerJoin, "task", "task", Expr::col(("task", "id")).eq(Expr::col(("d", "task_id"))).and(Expr::col(("task", "workspace_id")).eq(Expr::col(("d", "workspace_id")))).and(Expr::col(("task", "created_by_thread_id")).eq(Expr::col(("d", "target_thread_id"))))).join_as(JoinType::InnerJoin, "task_run", "run", Expr::col(("run", "id")).eq(Expr::col(("d", "run_id"))).and(Expr::col(("run", "task_id")).eq(Expr::col(("task", "id"))))).join_as(JoinType::InnerJoin, "turn", "command", Expr::col(("command", "id")).eq(Expr::col(("task", "created_by_turn_id"))).and(Expr::col(("command", "thread_id")).eq(Expr::col(("d", "target_thread_id"))))).join_as(JoinType::InnerJoin, "turn", "outcome", Expr::col(("outcome", "id")).eq(Expr::col(("d", "delivered_turn_id"))).and(Expr::col(("outcome", "thread_id")).eq(Expr::col(("d", "target_thread_id"))))).join_as(JoinType::InnerJoin, "thread", "th", Expr::col(("th", "id")).eq(Expr::col(("outcome", "thread_id"))).and(Expr::col(("th", "workspace_id")).eq(Expr::col(("d", "workspace_id"))))).and_where(Expr::col(("d", "workspace_id")).eq(Expr::Value(workspace.into())).and(Expr::col(("d", "target_thread_id")).eq(Expr::Value(thread.into()))).and(Expr::col(("d", "status")).eq(Expr::val("delivered"))).and(Expr::col(("d", "id")).gt(Expr::Value(after.clone().into()))).and(Expr::Value(command.map(str::to_owned).into()).binary(BinOper::Is, Expr::val(Option::<String>::None)).or(Expr::col(("task", "created_by_turn_id")).eq(Expr::Value(command.map(str::to_owned).into())))).and(Expr::exists(Query::select().expr(Expr::val(1_i64)).from_as("turn_event", "e").join_as(JoinType::InnerJoin, "compaction_event_revision", "r", Expr::col(("r", "source_id")).eq(Expr::col(("e", "id"))).and(Expr::col(("r", "turn_id")).eq(Expr::col(("e", "turn_id")))).and(Expr::col(("r", "present")).eq(Expr::val(1_i64)))).and_where(Expr::col(("e", "turn_id")).eq(Expr::col(("outcome", "id"))).and(Expr::col(("e", "thread_id")).eq(Expr::col(("outcome", "thread_id")))).and(Expr::col(("e", "event_type")).eq(Expr::Value(pioneer_protocol::constants::events::TURN_FAILED.into()))).and(Expr::col(("r", "capture_order")).lte(Expr::Value(event_fence.into()))).and(Expr::Value(source.map(|s|s.id.clone()).into()).binary(BinOper::Is, Expr::val(Option::<String>::None)).or(Expr::col(("e", "id")).eq(Expr::Value(source.map(|s|s.id.clone()).into())).and(Expr::col(("e", "turn_id")).eq(Expr::Value(source.and_then(|s|s.scope.strip_prefix("event:")).map(str::to_owned).into()))).and(Expr::val("event-revision:").binary(BinOper::Custom("||"), Expr::col(("r", "revision"))).eq(Expr::Value(source.map(|s|s.version.clone()).into())))))).to_owned()))).order_by_expr(Expr::col(("d", "id")), Order::Asc).limit(128).to_owned())).await?;
            if rows.is_empty() {
                return Ok(None);
            }
            for row in rows {
                let delivery: String = row.try_get("", "id")?;
                let outcome: String = row.try_get("", "delivered_turn_id")?;
                if outcome
                    == crate::canonical_agent_id('T', &format!("task-delivery-turn\0{delivery}"))
                {
                    return Ok(Some(row.try_get("", "created_by_turn_id")?));
                }
                after = delivery;
            }
        }
    }

    /// Bounded relationship metadata for a single selected turn. A Task status
    /// alone does not close its command: the identified outcome event must
    /// already exist below the same event fence. This grants no child-history
    /// access and reads no Task result or event payload.
    pub async fn compaction_history_causal_boundary(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        fence: &HistoryReadFence,
    ) -> Result<HistoryCausalBoundary> {
        let mut boundary = HistoryCausalBoundary::find_by_statement(statement(
            &Query::select()
                .expr_as(
                    Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as("task", "task")
                            .and_where(
                                Expr::col(("task", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id")))
                                    .and(
                                        Expr::col(("task", "created_by_thread_id"))
                                            .eq(Expr::col(("t", "thread_id"))),
                                    )
                                    .and(
                                        Expr::col(("task", "created_by_turn_id"))
                                            .eq(Expr::col(("t", "id"))),
                                    ),
                            )
                            .to_owned(),
                    ),
                    "delegated_command",
                )
                .expr_as(
                    Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as("task_run", "r")
                            .join_as(
                                JoinType::InnerJoin,
                                "task",
                                "task",
                                Expr::col(("task", "id")).eq(Expr::col(("r", "task_id"))),
                            )
                            .and_where(
                                Expr::col(("r", "id")).eq(Expr::col(("t", "id"))).and(
                                    Expr::col(("task", "workspace_id"))
                                        .eq(Expr::col(("th", "workspace_id"))),
                                ),
                            )
                            .to_owned(),
                    )
                    .or(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as("task_delivery", "d")
                            .and_where(
                                Expr::col(("d", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id")))
                                    .and(
                                        Expr::col(("d", "target_thread_id"))
                                            .eq(Expr::col(("t", "thread_id"))),
                                    )
                                    .and(
                                        Expr::col(("d", "delivered_turn_id"))
                                            .eq(Expr::col(("t", "id"))),
                                    ),
                            )
                            .to_owned(),
                    )),
                    "task_transport",
                )
                .expr_as(
                    Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as("task", "task")
                            .join_as(
                                JoinType::InnerJoin,
                                "task_delivery",
                                "d",
                                Expr::col(("d", "task_id")).eq(Expr::col(("task", "id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "task_run",
                                "run",
                                Expr::col(("run", "id")).eq(Expr::col(("d", "run_id"))).and(
                                    Expr::col(("run", "task_id")).eq(Expr::col(("task", "id"))),
                                ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "turn_event",
                                "e",
                                Expr::col(("e", "turn_id"))
                                    .eq(Expr::col(("d", "delivered_turn_id")))
                                    .and(
                                        Expr::col(("e", "thread_id"))
                                            .eq(Expr::col(("t", "thread_id"))),
                                    ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "compaction_event_revision",
                                "v",
                                Expr::col(("v", "source_id"))
                                    .eq(Expr::col(("e", "id")))
                                    .and(
                                        Expr::col(("v", "turn_id")).eq(Expr::col(("e", "turn_id"))),
                                    )
                                    .and(Expr::col(("v", "present")).eq(Expr::val(1_i64)))
                                    .and(
                                        Expr::col(("v", "projection_revision"))
                                            .eq(Expr::col(("v", "revision"))),
                                    ),
                            )
                            .and_where(
                                Expr::col(("task", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id")))
                                    .and(
                                        Expr::col(("task", "created_by_thread_id"))
                                            .eq(Expr::col(("t", "thread_id"))),
                                    )
                                    .and(
                                        Expr::col(("task", "created_by_turn_id"))
                                            .eq(Expr::col(("t", "id"))),
                                    )
                                    .and(
                                        Expr::col(("d", "workspace_id"))
                                            .eq(Expr::col(("th", "workspace_id"))),
                                    )
                                    .and(
                                        Expr::col(("d", "target_thread_id"))
                                            .eq(Expr::col(("t", "thread_id"))),
                                    )
                                    .and(Expr::col(("d", "status")).eq(Expr::val("delivered")))
                                    .and(
                                        Expr::col(("v", "capture_order"))
                                            .lte(Expr::Value(fence.event_order.into())),
                                    )
                                    .and(
                                        Expr::col(("v", "item_id")).eq(Expr::Value(
                                            pioneer_protocol::task_delivery_result_item_id("")
                                                .into(),
                                        )
                                        .binary(BinOper::Custom("||"), Expr::col(("d", "id")))),
                                    )
                                    .and(Expr::col(("e", "event_type")).eq(Expr::Value(
                                        pioneer_protocol::constants::events::ITEM_COMPLETED.into(),
                                    ))),
                            )
                            .to_owned(),
                    )
                    .or(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as("task", "task")
                            .join_as(
                                JoinType::InnerJoin,
                                "task_run",
                                "run",
                                Expr::col(("run", "task_id")).eq(Expr::col(("task", "id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "turn",
                                "occurrence",
                                Expr::col(("occurrence", "id"))
                                    .eq(Expr::col(("run", "id")))
                                    .and(
                                        Expr::col(("occurrence", "thread_id"))
                                            .eq(Expr::col(("t", "thread_id"))),
                                    )
                                    .and(
                                        Expr::col(("occurrence", "turn_kind"))
                                            .eq(Expr::val("task_run")),
                                    ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "turn_event",
                                "e",
                                Expr::col(("e", "turn_id"))
                                    .eq(Expr::col(("occurrence", "id")))
                                    .and(
                                        Expr::col(("e", "thread_id"))
                                            .eq(Expr::col(("occurrence", "thread_id"))),
                                    ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "compaction_event_revision",
                                "v",
                                Expr::col(("v", "source_id"))
                                    .eq(Expr::col(("e", "id")))
                                    .and(
                                        Expr::col(("v", "turn_id")).eq(Expr::col(("e", "turn_id"))),
                                    )
                                    .and(Expr::col(("v", "present")).eq(Expr::val(1_i64))),
                            )
                            .and_where(
                                Expr::col(("task", "workspace_id"))
                                    .eq(Expr::col(("th", "workspace_id")))
                                    .and(
                                        Expr::col(("task", "created_by_thread_id"))
                                            .eq(Expr::col(("t", "thread_id"))),
                                    )
                                    .and(
                                        Expr::col(("task", "created_by_turn_id"))
                                            .eq(Expr::col(("t", "id"))),
                                    )
                                    .and(
                                        Expr::col(("v", "capture_order"))
                                            .lte(Expr::Value(fence.event_order.into())),
                                    )
                                    .and(
                                        Expr::col(("e", "event_type")).is_in([
                                            Expr::Value(
                                                pioneer_protocol::constants::events::TURN_FAILED
                                                    .into(),
                                            ),
                                            Expr::Value(
                                                pioneer_protocol::constants::events::TURN_BLOCKED
                                                    .into(),
                                            ),
                                        ]),
                                    ),
                            )
                            .to_owned(),
                    )),
                    "delivered_outcome",
                )
                .from_as("turn", "t")
                .join_as(
                    JoinType::InnerJoin,
                    "thread",
                    "th",
                    Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                )
                .and_where(
                    Expr::col(("th", "workspace_id"))
                        .eq(Expr::Value(workspace.into()))
                        .and(Expr::col(("t", "thread_id")).eq(Expr::Value(thread.into())))
                        .and(Expr::col(("t", "id")).eq(Expr::Value(turn.into()))),
                )
                .to_owned(),
        ))
        .one(&self.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("causal history scope unavailable"))?;
        if boundary.delegated_command && !boundary.delivered_outcome {
            boundary.delivered_outcome = self
                .compaction_failed_delivery_command(
                    workspace,
                    thread,
                    Some(turn),
                    None,
                    fence.event_order,
                )
                .await?
                .is_some();
        }
        Ok(boundary)
    }

    /// One read fixes the append boundary before enumerating any turns. Source
    /// bounds use retained metadata with explicit insertion order; deleting or
    /// vacuuming canonical rows cannot change that order. MAX uses indexes and
    /// reads no payload. Every subsequent discovery checks its workspace scope.
    pub async fn compaction_history_read_fence(&self) -> Result<HistoryReadFence> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr_as(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(
                                        Func::cust(Alias::new("coalesce")).args([
                                            Expr::expr(
                                                Func::cust(Alias::new("max"))
                                                    .args([Expr::col("sequence")]),
                                            ),
                                            Expr::val(0_i64),
                                        ]),
                                    ))
                                    .from("compaction_turn_creation")
                                    .to_owned()
                                    .into(),
                            ),
                        ),
                        "turn_order",
                    )
                    .expr_as(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                                        Expr::expr(
                                            Func::cust(Alias::new("max")).args([Expr::col("id")]),
                                        ),
                                        Expr::val(""),
                                    ])))
                                    .from("turn")
                                    .to_owned()
                                    .into(),
                            ),
                        ),
                        "turn_id",
                    )
                    .expr_as(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(
                                        Func::cust(Alias::new("coalesce")).args([
                                            Expr::expr(
                                                Func::cust(Alias::new("max"))
                                                    .args([Expr::col("capture_order")]),
                                            ),
                                            Expr::val(0_i64),
                                        ]),
                                    ))
                                    .from("compaction_input_revision")
                                    .to_owned()
                                    .into(),
                            ),
                        ),
                        "input_order",
                    )
                    .expr_as(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(
                                        Func::cust(Alias::new("coalesce")).args([
                                            Expr::expr(
                                                Func::cust(Alias::new("max"))
                                                    .args([Expr::col("capture_order")]),
                                            ),
                                            Expr::val(0_i64),
                                        ]),
                                    ))
                                    .from("compaction_event_revision")
                                    .to_owned()
                                    .into(),
                            ),
                        ),
                        "event_order",
                    )
                    .expr_as(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(
                                        Func::cust(Alias::new("coalesce")).args([
                                            Expr::expr(
                                                Func::cust(Alias::new("max"))
                                                    .args([Expr::col("capture_order")]),
                                            ),
                                            Expr::val(0_i64),
                                        ]),
                                    ))
                                    .from("compaction_source_revision")
                                    .to_owned()
                                    .into(),
                            ),
                        ),
                        "context_order",
                    )
                    .expr_as(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(
                                        Func::cust(Alias::new("coalesce")).args([
                                            Expr::expr(
                                                Func::cust(Alias::new("max"))
                                                    .args([Expr::col("capture_order")]),
                                            ),
                                            Expr::val(0_i64),
                                        ]),
                                    ))
                                    .from("compaction_item_revision")
                                    .to_owned()
                                    .into(),
                            ),
                        ),
                        "item_order",
                    )
                    .to_owned(),
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("history read fence missing"))?;
        Ok(HistoryReadFence {
            turn_order: row.try_get("", "turn_order")?,
            turn_id: row.try_get("", "turn_id")?,
            input_order: row.try_get("", "input_order")?,
            event_order: row.try_get("", "event_order")?,
            context_order: row.try_get("", "context_order")?,
            item_order: row.try_get("", "item_order")?,
        })
    }

    /// Discover at most 128 metadata rows, including active turns. Eligibility
    /// is determined from complete canonical rounds/events below the captured
    /// fence, never by a later mutable terminal status alone.
    pub async fn compaction_history_turn_page(
        &self,
        workspace: &str,
        thread: &str,
        after: &str,
        fence: &HistoryReadFence,
    ) -> Result<Vec<HistoryTurnBoundary>> {
        Ok(HistoryTurnBoundary::find_by_statement(statement(
            &Query::select()
                .expr(Expr::col(("t", "id")))
                .expr_as(
                    Expr::expr(
                        Func::cust(Alias::new("coalesce")).args([
                            Expr::SubQuery(
                                None,
                                Box::new(
                                    Query::select()
                                        .expr(Expr::col("sequence"))
                                        .from("compaction_turn_creation")
                                        .and_where(Expr::col("turn_id").eq(Expr::col(("t", "id"))))
                                        .to_owned()
                                        .into(),
                                ),
                            ),
                            Expr::val(0_i64),
                        ]),
                    ),
                    "creation_order",
                )
                .expr_as(Expr::col(("t", "rowid")), "legacy_creation_order")
                .expr(Expr::col(("t", "created_at")))
                .expr(Expr::col(("t", "status")))
                .expr(Expr::col(("t", "turn_kind")))
                .expr(Expr::col(("t", "send_mode")))
                .expr_as(
                    Expr::SubQuery(
                        None,
                        Box::new(
                            Query::select()
                                .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                                    Expr::expr(Func::cust(Alias::new("max")).args([
                                        Expr::col(("s", "input_index")).add(Expr::val(1_i64)),
                                    ])),
                                    Expr::val(0_i64),
                                ])))
                                .from_as("turn_input", "s")
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_input_revision",
                                    "r",
                                    Expr::col(("r", "source_id"))
                                        .eq(Expr::col(("s", "id")))
                                        .and(
                                            Expr::col(("r", "turn_id"))
                                                .eq(Expr::col(("s", "turn_id"))),
                                        )
                                        .and(Expr::col(("r", "present")).eq(Expr::val(1_i64))),
                                )
                                .and_where(
                                    Expr::col(("s", "turn_id")).eq(Expr::col(("t", "id"))).and(
                                        Expr::col(("r", "capture_order"))
                                            .lte(Expr::Value(fence.input_order.into())),
                                    ),
                                )
                                .to_owned()
                                .into(),
                        ),
                    ),
                    "input_high_water",
                )
                .expr_as(
                    Expr::SubQuery(
                        None,
                        Box::new(
                            Query::select()
                                .expr(Expr::expr(
                                    Func::cust(Alias::new("coalesce")).args([
                                        Expr::expr(
                                            Func::cust(Alias::new("max"))
                                                .args([Expr::col(("s", "sequence"))]),
                                        ),
                                        Expr::val(0_i64),
                                    ]),
                                ))
                                .from_as("turn_event", "s")
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_event_revision",
                                    "r",
                                    Expr::col(("r", "source_id"))
                                        .eq(Expr::col(("s", "id")))
                                        .and(
                                            Expr::col(("r", "turn_id"))
                                                .eq(Expr::col(("s", "turn_id"))),
                                        )
                                        .and(Expr::col(("r", "present")).eq(Expr::val(1_i64))),
                                )
                                .and_where(
                                    Expr::col(("s", "turn_id")).eq(Expr::col(("t", "id"))).and(
                                        Expr::col(("r", "capture_order"))
                                            .lte(Expr::Value(fence.event_order.into())),
                                    ),
                                )
                                .to_owned()
                                .into(),
                        ),
                    ),
                    "event_high_water",
                )
                .expr_as(
                    Expr::SubQuery(
                        None,
                        Box::new(
                            Query::select()
                                .expr(Expr::expr(
                                    Func::cust(Alias::new("coalesce")).args([
                                        Expr::expr(
                                            Func::cust(Alias::new("max"))
                                                .args([Expr::col(("s", "sequence"))]),
                                        ),
                                        Expr::val(0_i64),
                                    ]),
                                ))
                                .from_as("turn_llm_context", "s")
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_source_revision",
                                    "r",
                                    Expr::col(("r", "source_id"))
                                        .eq(Expr::col(("s", "id")))
                                        .and(
                                            Expr::col(("r", "turn_id"))
                                                .eq(Expr::col(("s", "turn_id"))),
                                        )
                                        .and(Expr::col(("r", "present")).eq(Expr::val(1_i64))),
                                )
                                .and_where(
                                    Expr::col(("s", "turn_id")).eq(Expr::col(("t", "id"))).and(
                                        Expr::col(("r", "capture_order"))
                                            .lte(Expr::Value(fence.context_order.into())),
                                    ),
                                )
                                .to_owned()
                                .into(),
                        ),
                    ),
                    "context_high_water",
                )
                .expr_as(
                    Expr::SubQuery(
                        None,
                        Box::new(
                            Query::select()
                                .expr(Expr::expr(
                                    Func::cust(Alias::new("coalesce")).args([
                                        Expr::expr(
                                            Func::cust(Alias::new("max"))
                                                .args([Expr::col(("s", "rowid"))]),
                                        ),
                                        Expr::val(0_i64),
                                    ]),
                                ))
                                .from_as("turn_item", "s")
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_item_revision",
                                    "r",
                                    Expr::col(("r", "source_id"))
                                        .eq(Expr::col(("s", "id")))
                                        .and(
                                            Expr::col(("r", "turn_id"))
                                                .eq(Expr::col(("s", "turn_id"))),
                                        )
                                        .and(Expr::col(("r", "present")).eq(Expr::val(1_i64))),
                                )
                                .and_where(
                                    Expr::col(("s", "turn_id")).eq(Expr::col(("t", "id"))).and(
                                        Expr::col(("r", "capture_order"))
                                            .lte(Expr::Value(fence.item_order.into())),
                                    ),
                                )
                                .to_owned()
                                .into(),
                        ),
                    ),
                    "item_high_water",
                )
                .from_as("turn", "t")
                .join_as(
                    JoinType::InnerJoin,
                    "thread",
                    "th",
                    Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                )
                .and_where(
                    Expr::col(("th", "workspace_id"))
                        .eq(Expr::Value(workspace.into()))
                        .and(Expr::col(("th", "id")).eq(Expr::Value(thread.into())))
                        .and(Expr::col(("t", "id")).gt(Expr::Value(after.into())))
                        .and(Expr::col(("t", "id")).lte(Expr::Value(fence.turn_id.clone().into()))),
                )
                .order_by_expr(Expr::col(("t", "id")), Order::Asc)
                .limit(128)
                .to_owned(),
        ))
        .all(&self.connection)
        .await?)
    }
}

/// Computed from the typed event before a writer is acquired. These identities
/// let projection skip technical copies without parsing their retained bodies.
pub fn event_projection_metadata(
    event: &crate::CanonicalTurnEventPayload,
) -> (Option<String>, &'static str) {
    use crate::CanonicalTurnEventPayload as Event;
    use pioneer_protocol::TurnItem;
    let (item, update) = match event {
        Event::ItemStarted(value) => {
            let kind = if matches!(&value.item, TurnItem::SystemEvent {code:Some(code), ..} if code=="agent_context_compaction")
            {
                "technical"
            } else {
                "start"
            };
            return (Some(value.item.item_id().into()), kind);
        }
        Event::ItemCompleted(value) => (&value.item, false),
        Event::ItemUpdated(value) => (&value.item, true),
        Event::TurnStarted(_) => return (None, "input"),
        Event::TurnMessageEdited(_) => return (None, "input_revision"),
        Event::TurnMessageDeleted(_) => return (None, "input_deleted"),
        _ => return (None, "status"),
    };
    let kind = match item {
        TurnItem::SystemEvent { code, .. }
            if code.as_deref() == Some("agent_context_compaction") =>
        {
            "technical"
        }
        TurnItem::UserMessage { .. } => "input_copy",
        _ if update => "update",
        TurnItem::Reasoning { .. } => "reasoning",
        TurnItem::AgentMessage { .. } => "assistant",
        TurnItem::SystemEvent { .. } => "observation",
        _ => "tool_observation",
    };
    (Some(item.item_id().into()), kind)
}
