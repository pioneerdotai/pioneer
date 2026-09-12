//! Frozen source references for one completed Task result-producing turn.
//! Capture precedes candidate publication: a later review/delivery may select
//! this work, but must never discover additional live child history.
use super::*;
use pioneer_compaction::frozen::FrozenHistoryRef;
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Query};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskOutputSnapshot {
    pub task_run_turn_id: String,
    pub task_id: String,
    pub run_id: String,
    pub source_thread: String,
    pub source_turn: String,
    pub history: FrozenHistoryRef,
}

impl CrudStore {
    /// First accepted manifest wins across retries. This is one metadata write;
    /// source materialization/hashing occurred before acquiring the writer.
    /// All prepared identities are revalidated in the INSERT predicate. This
    /// record alone grants no delivery or access to the source history.
    pub async fn compaction_record_task_output(
        &self,
        workspace: &str,
        task_run_turn: &str,
        history: &FrozenHistoryRef,
    ) -> Result<TaskOutputSnapshot> {
        ensure!(
            history.format == 1,
            "unsupported Task output snapshot format"
        );
        if let Some(existing) = self
            .compaction_task_output(workspace, task_run_turn)
            .await?
        {
            return Ok(existing);
        }
        self.connection
            .execute_raw(statement(
                &Query::insert()
                    .into_table("compaction_task_output")
                    .columns([
                        "task_run_turn_id",
                        "task_id",
                        "run_id",
                        "workspace_id",
                        "source_thread",
                        "source_turn",
                        "manifest_id",
                    ])
                    .select_from(
                        Query::select()
                            .expr(Expr::col(("rt", "id")))
                            .expr(Expr::col(("rt", "task_id")))
                            .expr(Expr::col(("rt", "run_id")))
                            .expr(Expr::col(("task", "workspace_id")))
                            .expr(Expr::col(("rt", "thread_id")))
                            .expr(Expr::col(("rt", "turn_id")))
                            .expr(Expr::col(("h", "id")))
                            .from_as("task_run_turn", "rt")
                            .join_as(
                                JoinType::InnerJoin,
                                "task_run",
                                "run",
                                Expr::col(("run", "id"))
                                    .eq(Expr::col(("rt", "run_id")))
                                    .and(
                                        Expr::col(("run", "task_id"))
                                            .eq(Expr::col(("rt", "task_id"))),
                                    ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "task",
                                "task",
                                Expr::col(("task", "id")).eq(Expr::col(("rt", "task_id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "turn",
                                "t",
                                Expr::col(("t", "id")).eq(Expr::col(("rt", "turn_id"))).and(
                                    Expr::col(("t", "thread_id"))
                                        .eq(Expr::col(("rt", "thread_id"))),
                                ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "thread",
                                "th",
                                Expr::col(("th", "id"))
                                    .eq(Expr::col(("t", "thread_id")))
                                    .and(
                                        Expr::col(("th", "workspace_id"))
                                            .eq(Expr::col(("task", "workspace_id"))),
                                    ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "compaction_frozen_history",
                                "h",
                                Expr::col(("h", "owner_thread"))
                                    .eq(Expr::col(("t", "thread_id")))
                                    .and(
                                        Expr::col(("h", "workspace_id"))
                                            .eq(Expr::col(("task", "workspace_id"))),
                                    ),
                            )
                            .and_where(
                                Expr::col(("rt", "id"))
                                    .eq(Expr::Value(task_run_turn.into()))
                                    .and(
                                        Expr::col(("task", "workspace_id"))
                                            .eq(Expr::Value(workspace.into())),
                                    )
                                    .and(Expr::col(("t", "status")).eq(Expr::val("completed")))
                                    .and(
                                        Expr::col(("rt", "kind"))
                                            .is_in(["initial", "revision", "recovery"]),
                                    )
                                    .and(Expr::col(("run", "status")).ne(Expr::val("cancelled")))
                                    .and(
                                        Expr::col(("h", "id"))
                                            .eq(Expr::Value(history.manifest_id.clone().into())),
                                    )
                                    .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                                    .and(
                                        Expr::col(("h", "identity_sha256")).eq(Expr::Value(
                                            history.identity_sha256.clone().into(),
                                        )),
                                    )
                                    .and(
                                        Expr::col(("h", "message_count")).eq(Expr::Value(
                                            i64::try_from(history.messages)?.into(),
                                        )),
                                    ),
                            )
                            .to_owned(),
                    )?
                    .on_conflict(
                        OnConflict::columns(["task_run_turn_id"])
                            .do_nothing()
                            .to_owned(),
                    )
                    .to_owned(),
            ))
            .await?;
        self.compaction_task_output(workspace, task_run_turn)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("Task output snapshot has no completed canonical source binding")
            })
    }

    /// Point metadata read with the complete retained Task/turn relationship.
    /// No result payload or mutable current child transcript is loaded.
    pub async fn compaction_task_output(
        &self,
        workspace: &str,
        task_run_turn: &str,
    ) -> Result<Option<TaskOutputSnapshot>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "task_id")))
                    .expr(Expr::col(("s", "run_id")))
                    .expr(Expr::col(("s", "source_thread")))
                    .expr(Expr::col(("s", "source_turn")))
                    .expr(Expr::col(("h", "id")))
                    .expr(Expr::col(("h", "identity_sha256")))
                    .expr(Expr::col(("h", "message_count")))
                    .from_as("compaction_task_output", "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run_turn",
                        "rt",
                        Expr::col(("rt", "id"))
                            .eq(Expr::col(("s", "task_run_turn_id")))
                            .and(Expr::col(("rt", "task_id")).eq(Expr::col(("s", "task_id"))))
                            .and(Expr::col(("rt", "run_id")).eq(Expr::col(("s", "run_id"))))
                            .and(
                                Expr::col(("rt", "thread_id"))
                                    .eq(Expr::col(("s", "source_thread"))),
                            )
                            .and(Expr::col(("rt", "turn_id")).eq(Expr::col(("s", "source_turn")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run",
                        "run",
                        Expr::col(("run", "id"))
                            .eq(Expr::col(("s", "run_id")))
                            .and(Expr::col(("run", "task_id")).eq(Expr::col(("s", "task_id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task",
                        "task",
                        Expr::col(("task", "id"))
                            .eq(Expr::col(("s", "task_id")))
                            .and(
                                Expr::col(("task", "workspace_id"))
                                    .eq(Expr::col(("s", "workspace_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id"))
                            .eq(Expr::col(("s", "source_turn")))
                            .and(
                                Expr::col(("t", "thread_id")).eq(Expr::col(("s", "source_thread"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id"))
                            .eq(Expr::col(("s", "source_thread")))
                            .and(
                                Expr::col(("th", "workspace_id"))
                                    .eq(Expr::col(("s", "workspace_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_frozen_history",
                        "h",
                        Expr::col(("h", "id"))
                            .eq(Expr::col(("s", "manifest_id")))
                            .and(
                                Expr::col(("h", "workspace_id"))
                                    .eq(Expr::col(("s", "workspace_id"))),
                            )
                            .and(
                                Expr::col(("h", "owner_thread"))
                                    .eq(Expr::col(("s", "source_thread"))),
                            )
                            .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64))),
                    )
                    .and_where(
                        Expr::col(("s", "task_run_turn_id"))
                            .eq(Expr::Value(task_run_turn.into()))
                            .and(
                                Expr::col(("s", "workspace_id")).eq(Expr::Value(workspace.into())),
                            ),
                    )
                    .to_owned(),
            ))
            .await?;
        row.map(|row| {
            Ok(TaskOutputSnapshot {
                task_run_turn_id: task_run_turn.into(),
                task_id: row.try_get("", "task_id")?,
                run_id: row.try_get("", "run_id")?,
                source_thread: row.try_get("", "source_thread")?,
                source_turn: row.try_get("", "source_turn")?,
                history: FrozenHistoryRef {
                    format: 1,
                    manifest_id: row.try_get("", "id")?,
                    identity_sha256: row.try_get("", "identity_sha256")?,
                    messages: u64::try_from(row.try_get::<i64>("", "message_count")?)?,
                },
            })
        })
        .transpose()
    }
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
    db.execute_raw(statement(
        &Query::insert()
            .into_table("compaction_delivery_output")
            .columns(["delivery_id", "candidate_id", "task_run_turn_id"])
            .select_from(
                Query::select()
                    .expr(Expr::col(("d", "id")))
                    .expr(Expr::col(("c", "id")))
                    .expr(Expr::col(("s", "task_run_turn_id")))
                    .from_as("task_delivery", "d")
                    .join_as(
                        JoinType::InnerJoin,
                        "task_run",
                        "run",
                        Expr::col(("run", "id"))
                            .eq(Expr::col(("d", "run_id")))
                            .and(Expr::col(("run", "task_id")).eq(Expr::col(("d", "task_id"))))
                            .and(Expr::col(("run", "status")).eq(Expr::val("succeeded"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task_result_candidate",
                        "c",
                        Expr::col(("c", "run_id"))
                            .eq(Expr::col(("d", "run_id")))
                            .and(Expr::col(("c", "task_id")).eq(Expr::col(("d", "task_id"))))
                            .and(Expr::col(("c", "status")).eq(Expr::val("accepted"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_task_output",
                        "s",
                        Expr::col(("s", "task_run_turn_id"))
                            .eq(Expr::col(("c", "task_run_turn_id")))
                            .and(Expr::col(("s", "task_id")).eq(Expr::col(("c", "task_id"))))
                            .and(Expr::col(("s", "run_id")).eq(Expr::col(("c", "run_id"))))
                            .and(
                                Expr::col(("s", "source_thread")).eq(Expr::col(("c", "thread_id"))),
                            )
                            .and(Expr::col(("s", "source_turn")).eq(Expr::col(("c", "turn_id"))))
                            .and(
                                Expr::col(("s", "workspace_id"))
                                    .eq(Expr::col(("d", "workspace_id"))),
                            ),
                    )
                    .and_where(
                        Expr::col(("d", "id"))
                            .eq(Expr::Value(delivery.id.clone().into()))
                            .and(
                                Expr::col(("d", "workspace_id"))
                                    .eq(Expr::Value(delivery.workspace_id.clone().into())),
                            )
                            .and(
                                Expr::col(("d", "task_id"))
                                    .eq(Expr::Value(delivery.task_id.clone().into())),
                            )
                            .and(
                                Expr::col(("d", "run_id"))
                                    .eq(Expr::Value(delivery.run_id.clone().into())),
                            )
                            .and(
                                Expr::exists(
                                    Query::select()
                                        .expr(Expr::val(1_i64))
                                        .from_as("task_result_candidate", "other")
                                        .and_where(
                                            Expr::col(("other", "run_id"))
                                                .eq(Expr::col(("c", "run_id")))
                                                .and(
                                                    Expr::col(("other", "status"))
                                                        .eq(Expr::val("accepted")),
                                                )
                                                .and(
                                                    Expr::col(("other", "id"))
                                                        .ne(Expr::col(("c", "id"))),
                                                ),
                                        )
                                        .to_owned(),
                                )
                                .not(),
                            ),
                    )
                    .to_owned(),
            )?
            .on_conflict(OnConflict::columns(["delivery_id"]).do_nothing().to_owned())
            .to_owned(),
    ))
    .await?;
    Ok(())
}

impl CrudStore {
    pub async fn compaction_delivery_output(
        &self,
        workspace: &str,
        delivery: &str,
    ) -> Result<Option<TaskDeliveryOutputSnapshot>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("b", "candidate_id")))
                    .expr(Expr::col(("b", "task_run_turn_id")))
                    .from_as("compaction_delivery_output", "b")
                    .join_as(
                        JoinType::InnerJoin,
                        "task_delivery",
                        "d",
                        Expr::col(("d", "id")).eq(Expr::col(("b", "delivery_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_task_output",
                        "s",
                        Expr::col(("s", "task_run_turn_id"))
                            .eq(Expr::col(("b", "task_run_turn_id")))
                            .and(Expr::col(("s", "task_id")).eq(Expr::col(("d", "task_id"))))
                            .and(Expr::col(("s", "run_id")).eq(Expr::col(("d", "run_id"))))
                            .and(
                                Expr::col(("s", "workspace_id"))
                                    .eq(Expr::col(("d", "workspace_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "task_result_candidate",
                        "c",
                        Expr::col(("c", "id"))
                            .eq(Expr::col(("b", "candidate_id")))
                            .and(
                                Expr::col(("c", "task_run_turn_id"))
                                    .eq(Expr::col(("s", "task_run_turn_id"))),
                            )
                            .and(Expr::col(("c", "task_id")).eq(Expr::col(("s", "task_id"))))
                            .and(Expr::col(("c", "run_id")).eq(Expr::col(("s", "run_id"))))
                            .and(
                                Expr::col(("c", "thread_id")).eq(Expr::col(("s", "source_thread"))),
                            )
                            .and(Expr::col(("c", "turn_id")).eq(Expr::col(("s", "source_turn")))),
                    )
                    .and_where(
                        Expr::col(("d", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("d", "id")).eq(Expr::Value(delivery.into()))),
                    )
                    .to_owned(),
            ))
            .await?;
        let Some(row) = row else { return Ok(None) };
        let candidate_id: String = row.try_get("", "candidate_id")?;
        let task_run_turn: String = row.try_get("", "task_run_turn_id")?;
        let Some(output) = self
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

impl CrudStore {
    /// Inspect at most 128 event revisions below the common capture fence.
    /// The caller retains the first acknowledgement for each delivery ID across
    /// pages; replayed notifications may occur in later quanta. No payload is read.
    pub async fn compaction_delivered_output_page(
        &self,
        workspace: &str,
        thread: &str,
        after: i64,
        fence: &HistoryReadFence,
    ) -> Result<DeliveredTaskOutputPage> {
        ensure!(after >= 0, "invalid delivery capture cursor");
        let scanned_through = std::cmp::min(after.saturating_add(128), fence.event_order);
        let rows = self.connection.query_all_raw(sqlite_specific_sql(
            "WITH quantum AS MATERIALIZED (SELECT * FROM compaction_event_revision WHERE capture_order>? AND capture_order<=? ORDER BY capture_order LIMIT 128) SELECT d.id AS delivery_id,b.candidate_id,b.task_run_turn_id,s.source_thread,s.source_turn,e.id AS event_id,e.turn_id AS event_turn,r.revision,r.capture_order FROM quantum r JOIN task_delivery d ON d.id=substr(r.item_id,length(?)+1) JOIN thread th ON th.id=d.target_thread_id AND th.workspace_id=d.workspace_id JOIN compaction_delivery_output b ON b.delivery_id=d.id JOIN compaction_task_output s ON s.task_run_turn_id=b.task_run_turn_id AND s.task_id=d.task_id AND s.run_id=d.run_id AND s.workspace_id=d.workspace_id JOIN task_result_candidate c ON c.id=b.candidate_id AND c.task_run_turn_id=s.task_run_turn_id AND c.task_id=s.task_id AND c.run_id=s.run_id AND c.thread_id=s.source_thread AND c.turn_id=s.source_turn JOIN turn_event e ON e.turn_id=d.delivered_turn_id AND e.thread_id=d.target_thread_id WHERE r.source_id=e.id AND r.turn_id=e.turn_id AND r.present=1 AND r.projection_revision=r.revision AND r.item_id=(? || d.id) AND d.workspace_id=? AND d.target_thread_id=? AND d.status='delivered' AND e.event_type=? ORDER BY r.capture_order LIMIT 128",
            [after.into(),scanned_through.into(),pioneer_protocol::task_delivery_result_item_id("").into(),pioneer_protocol::task_delivery_result_item_id("").into(),workspace.into(),thread.into(),pioneer_protocol::constants::events::ITEM_COMPLETED.into()],
        )).await?;
        let unprojected = self.connection.query_all_raw(sqlite_specific_sql(
            "WITH quantum AS MATERIALIZED (SELECT * FROM compaction_event_revision WHERE capture_order>? AND capture_order<=? ORDER BY capture_order LIMIT 128) SELECT e.id,e.turn_id,r.revision FROM quantum r JOIN turn_event e ON e.id=r.source_id AND e.turn_id=r.turn_id JOIN thread th ON th.id=e.thread_id WHERE r.present=1 AND (r.projection_revision IS NULL OR r.projection_revision<>r.revision) AND e.thread_id=? AND th.workspace_id=? AND e.event_type=? ORDER BY r.capture_order LIMIT 128",
            [after.into(),scanned_through.into(),thread.into(),workspace.into(),pioneer_protocol::constants::events::ITEM_COMPLETED.into()],
        )).await?;
        let unprojected_events = unprojected
            .into_iter()
            .map(|row| {
                Ok(SourceRef {
                    scope: format!("event:{}", row.try_get::<String>("", "turn_id")?),
                    id: row.try_get("", "id")?,
                    version: format!("event-revision:{}", row.try_get::<i64>("", "revision")?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let entries = rows
            .into_iter()
            .map(|row| {
                Ok(DeliveredTaskOutputRef {
                    delivery_id: row.try_get("", "delivery_id")?,
                    candidate_id: row.try_get("", "candidate_id")?,
                    task_run_turn_id: row.try_get("", "task_run_turn_id")?,
                    source_thread: row.try_get("", "source_thread")?,
                    source_turn: row.try_get("", "source_turn")?,
                    acknowledgement: SourceRef {
                        scope: format!("event:{}", row.try_get::<String>("", "event_turn")?),
                        id: row.try_get("", "event_id")?,
                        version: format!("event-revision:{}", row.try_get::<i64>("", "revision")?),
                    },
                    capture_order: row.try_get("", "capture_order")?,
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
}
