//! Durable compaction metadata. Payloads remain in canonical history tables.
mod background;
pub use background::{CompactionLifecycleRecovery, CompletedHistoryCheck};
use sea_orm::sea_query::{
    Alias, Asterisk, BinOper, Expr, ExprTrait, Func, JoinType, OnConflict, Order, Query,
};
mod frozen;
mod frozen_import;
pub(crate) mod history;
mod lifecycle;
mod runner;
mod source_projection;
mod task_output;
use crate::CrudStore;
use anyhow::{Result, ensure};
pub use frozen_import::{
    EMPTY_FROZEN_IMPORT_SHA256, FROZEN_IMPORT_PAGE_BYTES, FrozenImportRecord, PreparedFrozenImport,
    frozen_import_identity,
};
pub use history::{
    AcceptedTaskBasis, HistoryCausalBoundary, HistoryReadFence, HistoryTurnBoundary,
    event_projection_metadata,
};
use pioneer_compaction::{Checkpoint, FORMAT_VERSION, OperationSnapshot, SourceRef};
pub use runner::{ManifestEntry, RunnerPlanRecord};
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement, TransactionTrait, Value};
use sha2::{Digest, Sha256};
pub(crate) use task_output::bind_queued_task_output;
pub use task_output::{
    DeliveredTaskOutputPage, DeliveredTaskOutputRef, TaskDeliveryOutputSnapshot, TaskOutputSnapshot,
};

#[derive(Clone, Debug)]
pub struct CheckpointEdges {
    pub owner: String,
    pub previous: Option<String>,
    pub format_version: u32,
    pub coverage: Vec<SourceRef>,
}

pub const SOURCE_PAGE_ROWS: u64 = 128;
pub const SOURCE_PAGE_BYTES: usize = 256 * 1024;
pub const CHECKPOINT_SOURCE_LIMIT: usize = 256;

// Render query builders through SeaORM while retaining the scoped connection's
// existing routing and transaction ownership (including UPDATE RETURNING).
fn statement(query: &impl sea_orm::StatementBuilder) -> Statement {
    sea_orm::StatementBuilder::build(query, &DbBackend::Sqlite)
}

// Kept only for correlated SQLite json_each snapshot validation and the two
// MATERIALIZED event quanta. These queries preserve one atomic validation or a
// physical scan boundary; ordinary reads/writes above use SeaQuery builders.
fn sqlite_specific_sql(statement: &str, values: impl IntoIterator<Item = Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, statement, values)
}

#[derive(Clone, Copy, Debug)]
pub enum CanonicalSource {
    Input,
    Event,
    ProviderContext,
    ToolItem,
}
impl CanonicalSource {
    fn table(self) -> &'static str {
        match self {
            Self::Input => "turn_input",
            Self::Event => "turn_event",
            Self::ProviderContext => "turn_llm_context",
            Self::ToolItem => "turn_item",
        }
    }
    fn revisions(self) -> &'static str {
        match self {
            Self::Input => "compaction_input_revision",
            Self::Event => "compaction_event_revision",
            Self::ProviderContext => "compaction_source_revision",
            Self::ToolItem => "compaction_item_revision",
        }
    }
    fn version_prefix(self) -> &'static str {
        match self {
            Self::Input => "input-revision",
            Self::Event => "event-revision",
            Self::ProviderContext => "revision",
            Self::ToolItem => "item-revision",
        }
    }
    fn prefix(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Event => "event",
            Self::ProviderContext => "context",
            Self::ToolItem => "item",
        }
    }
}

#[derive(Clone, Debug)]
pub struct SourceRecord {
    pub source_type: String,
    pub tool_name: Option<String>,
    pub projection_kind: Option<String>,
    pub item_id: Option<String>,
    pub reference: SourceRef,
    pub sequence: i64,
    pub payload: Option<String>,
    pub incomplete: bool,
}

#[derive(Clone, Debug)]
pub struct SourcePage {
    pub entries: Vec<SourceRecord>,
    pub next_sequence: i64,
}

#[derive(Clone, Debug)]
pub struct SourceAssertion {
    pub revision: Option<i64>,
    pub kind: CanonicalSource,
    pub turn_id: String,
    pub id: String,
    pub payload: String,
}
impl SourceAssertion {
    pub fn reference(&self) -> SourceRef {
        SourceRef {
            scope: format!("{}:{}", self.kind.prefix(), self.turn_id),
            id: self.id.clone(),
            version: self
                .revision
                .map(|v| match self.kind {
                    CanonicalSource::Input => format!("input-revision:{v}"),
                    CanonicalSource::ToolItem => format!("item-revision:{v}"),
                    CanonicalSource::Event => format!("event-revision:{v}"),
                    _ => format!("revision:{v}"),
                })
                .unwrap_or_else(|| hex::encode(Sha256::digest(self.payload.as_bytes()))),
        }
    }
}

#[derive(Clone, Debug, FromQueryResult)]
pub struct OperationRecord {
    pub id: String,
    pub owner: String,
    pub status: String,
    pub snapshot: String,
    pub deadline_ms: i64,
    pub attempts: i64,
    pub transient_retries: i64,
    pub correction: i64,
    pub next_portion: i64,
    pub outcome: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Applied,
    AlreadyApplied,
    Stale,
    Cancelled,
}

fn checkpoint_identity(checkpoint: &Checkpoint) -> Result<String> {
    let mut canonical = checkpoint.clone();
    canonical.coverage.sort();
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&canonical)?)))
}

impl CrudStore {
    /// Pin append-only discovery to a bounded high water mark. Edits remain
    /// governed by each source revision, not by this sequence boundary.
    pub async fn compaction_source_high_water(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: CanonicalSource,
    ) -> Result<i64> {
        let sequence = match kind {
            CanonicalSource::Input => Expr::col(("s", "input_index")).add(1),
            CanonicalSource::ToolItem => Expr::col(("s", "rowid")),
            _ => Expr::col(("s", "sequence")),
        };
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr_as(
                        Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(Func::cust(Alias::new("max")).args([sequence.clone()])),
                            Expr::val(0_i64),
                        ])),
                        "sequence",
                    )
                    .from_as(Alias::new(kind.table()), "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                    )
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
                            .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into()))),
                    )
                    .to_owned(),
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("source boundary missing"))?;
        Ok(row.try_get("", "sequence")?)
    }
    /// Scope is inherited from this handle. Background callers must use maintenance.
    /// Metadata discovery is bounded; payload parsing/hashing follows released reads.
    pub async fn compaction_source_page(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: CanonicalSource,
        after: i64,
    ) -> Result<SourcePage> {
        self.compaction_source_page_inner(workspace, thread, turn, kind, after, true, i64::MAX)
            .await
    }

    /// Discover versioned identities without materializing already covered
    /// payloads. The projection fetches only its uncovered sources afterward.
    pub async fn compaction_source_metadata_page(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: CanonicalSource,
        after: i64,
    ) -> Result<SourcePage> {
        self.compaction_source_page_inner(workspace, thread, turn, kind, after, false, i64::MAX)
            .await
    }

    pub async fn compaction_source_metadata_page_at_fence(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: CanonicalSource,
        after: i64,
        capture_order: i64,
    ) -> Result<SourcePage> {
        self.compaction_source_page_inner(
            workspace,
            thread,
            turn,
            kind,
            after,
            false,
            capture_order,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn compaction_source_page_inner(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: CanonicalSource,
        after: i64,
        include_payload: bool,
        capture_order: i64,
    ) -> Result<SourcePage> {
        let null = Expr::val(Option::<String>::None);
        let current_projection = |column| {
            Expr::case(
                Expr::col(("r", "projection_revision")).eq(Expr::col(("r", "revision"))),
                Expr::col(("r", column)),
            )
            .finally(null.clone())
            .into()
        };
        let (source_type, item_id, tool_name, projection_kind): (Expr, Expr, Expr, Expr) =
            match kind {
                CanonicalSource::Input => (
                    Expr::col(("s", "input_type")),
                    null.clone(),
                    null.clone(),
                    null.clone(),
                ),
                CanonicalSource::Event => (
                    Expr::col(("s", "event_type")),
                    current_projection("item_id"),
                    null.clone(),
                    current_projection("projection_kind"),
                ),
                CanonicalSource::ProviderContext => (
                    Expr::col(("s", "source")),
                    Expr::col(("s", "item_id")),
                    Expr::col(("s", "tool_name")),
                    null.clone(),
                ),
                CanonicalSource::ToolItem => (
                    Expr::col(("s", "item_type")),
                    Expr::col(("s", "item_id")),
                    null.clone(),
                    null.clone(),
                ),
            };
        let sequence_column = match kind {
            CanonicalSource::Input => Expr::col(("s", "input_index")).add(1),
            CanonicalSource::ToolItem => Expr::col(("s", "rowid")),
            _ => Expr::col(("s", "sequence")),
        };
        let payload_size = if include_payload {
            Expr::expr(
                Func::cust(Alias::new("length"))
                    .arg(Expr::col(("s", "payload")).cast_as(Alias::new("BLOB"))),
            )
        } else {
            Expr::val(0_i64)
        };
        let rows =
            self.connection
                .query_all_raw(statement(
                    &Query::select()
                        .expr(Expr::col(("s", "id")))
                        .expr_as(sequence_column.clone(), "sequence")
                        .expr_as(source_type.clone(), "source_type")
                        .expr_as(item_id.clone(), "item_id")
                        .expr_as(tool_name.clone(), "tool_name")
                        .expr_as(projection_kind.clone(), "projection_kind")
                        .expr_as(
                            Expr::expr(
                                Func::cust(Alias::new("coalesce"))
                                    .args([Expr::col(("r", "revision")), Expr::val(1_i64)]),
                            ),
                            "revision",
                        )
                        .expr_as(payload_size.clone(), "bytes")
                        .from_as(Alias::new(kind.table()), "s")
                        .join_as(
                            JoinType::LeftJoin,
                            Alias::new(kind.revisions()),
                            "r",
                            Expr::col(("r", "source_id"))
                                .eq(Expr::col(("s", "id")))
                                .and(Expr::col(("r", "turn_id")).eq(Expr::col(("s", "turn_id")))),
                        )
                        .join_as(
                            JoinType::InnerJoin,
                            "turn",
                            "t",
                            Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                        )
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
                                .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                                .and(sequence_column.clone().gt(Expr::Value(after.into())))
                                .and(
                                    Expr::expr(Func::cust(Alias::new("coalesce")).args([
                                        Expr::col(("r", "capture_order")),
                                        Expr::val(0_i64),
                                    ]))
                                    .lte(Expr::Value(capture_order.into())),
                                ),
                        )
                        .order_by_expr(sequence_column.clone(), Order::Asc)
                        .limit(u64::try_from(SOURCE_PAGE_ROWS)?)
                        .to_owned(),
                ))
                .await?;
        let mut page = SourcePage {
            entries: Vec::new(),
            next_sequence: after,
        };
        let mut bytes = 0usize;
        for row in rows {
            let size: i64 = row.try_get("", "bytes")?;
            let id: String = row.try_get("", "id")?;
            let sequence: i64 = row.try_get("", "sequence")?;
            let revision: i64 = row.try_get("", "revision")?;
            if include_payload
                && size >= 0
                && size as usize <= SOURCE_PAGE_BYTES
                && bytes + size as usize > SOURCE_PAGE_BYTES
            {
                break;
            }
            let payload = if !include_payload || size < 0 || size as usize > SOURCE_PAGE_BYTES {
                None
            } else {
                self.connection
                    .query_one_raw(statement(
                        &Query::select()
                            .expr(Expr::col(("s", "payload")))
                            .from_as(Alias::new(kind.table()), "s")
                            .join_as(
                                JoinType::LeftJoin,
                                Alias::new(kind.revisions()),
                                "r",
                                Expr::col(("r", "source_id"))
                                    .eq(Expr::col(("s", "id")))
                                    .and(
                                        Expr::col(("r", "turn_id")).eq(Expr::col(("s", "turn_id"))),
                                    ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "turn",
                                "t",
                                Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "thread",
                                "th",
                                Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                            )
                            .and_where(
                                Expr::col(("s", "id"))
                                    .eq(Expr::Value(id.clone().into()))
                                    .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                                    .and(
                                        Expr::col(("t", "thread_id"))
                                            .eq(Expr::Value(thread.into())),
                                    )
                                    .and(
                                        Expr::col(("th", "workspace_id"))
                                            .eq(Expr::Value(workspace.into())),
                                    )
                                    .and(
                                        Expr::expr(Func::cust(Alias::new("coalesce")).args([
                                            Expr::col(("r", "revision")),
                                            Expr::val(1_i64),
                                        ]))
                                        .eq(Expr::Value(revision.into())),
                                    )
                                    .and(
                                        Expr::expr(Func::cust(Alias::new("length")).args([
                                            Expr::col(("s", "payload")).cast_as(Alias::new("BLOB")),
                                        ]))
                                        .lte(
                                            Expr::Value(
                                                ((SOURCE_PAGE_BYTES - bytes) as u64).into(),
                                            ),
                                        ),
                                    ),
                            )
                            .to_owned(),
                    ))
                    .await?
                    .map(|row| row.try_get::<String>("", "payload"))
                    .transpose()?
            };
            // No connection/stream/permit is held here.
            let version = format!("{}:{}", kind.version_prefix(), revision);
            bytes += payload.as_ref().map_or(0, String::len);
            page.next_sequence = sequence;
            page.entries.push(SourceRecord {
                source_type: row.try_get("", "source_type")?,
                tool_name: row.try_get("", "tool_name")?,
                projection_kind: row.try_get("", "projection_kind")?,
                item_id: row.try_get("", "item_id")?,
                reference: SourceRef {
                    scope: format!("{}:{turn}", kind.prefix()),
                    id,
                    version,
                },
                sequence,
                incomplete: payload.is_none(),
                payload,
            });
        }
        Ok(page)
    }

    pub async fn compaction_admit(
        &self,
        workspace: &str,
        thread: &str,
        snapshot: &OperationSnapshot,
    ) -> Result<OperationRecord> {
        self.compaction_admit_for_turn(workspace, thread, snapshot, None)
            .await
    }

    pub async fn compaction_admit_for_turn(
        &self,
        workspace: &str,
        thread: &str,
        snapshot: &OperationSnapshot,
        execution_turn: Option<&str>,
    ) -> Result<OperationRecord> {
        let snapshot_json = serde_json::to_string(snapshot)?;
        ensure!(
            snapshot_json.len() <= SOURCE_PAGE_BYTES,
            "operation metadata exceeds bounded admission"
        );
        let deadline = i64::try_from(snapshot.admission.deadline_ms)?;
        // Preparation reads no mutable DB state. Owner and format are revalidated inside the write boundary.
        self.run_serialized_write(|| async {
            let txn = self.connection.begin().await?;
            txn.execute_raw(statement(&Query::insert().into_table("compaction_context").columns(["workspace_id", "thread_id", "owner"]).values_panic([Expr::Value(workspace.into()), Expr::Value(thread.into()), Expr::Value(snapshot.owner.clone().into())]).on_conflict(OnConflict::columns(["owner"]).do_nothing().to_owned()).to_owned())).await?;
            let valid = txn.query_one_raw(statement(&Query::select().expr(Expr::col("owner")).from("compaction_context").and_where(Expr::col("owner").eq(Expr::Value(snapshot.owner.clone().into())).and(Expr::col("workspace_id").eq(Expr::Value(workspace.into()))).and(Expr::col("thread_id").eq(Expr::Value(thread.into()))).and(Expr::col("format_version").eq(Expr::val(1_i64)))).to_owned())).await?;
            ensure!(valid.is_some(), "compaction owner scope or format mismatch");
            if let Some(turn) = execution_turn {
                ensure!(txn.query_one_raw(statement(&Query::select().expr(Expr::col(("c", "owner"))).from_as("compaction_context", "c").join_as(JoinType::InnerJoin, "turn", "t", Expr::col(("t", "thread_id")).eq(Expr::col(("c", "thread_id")))).and_where(Expr::col(("c", "owner")).eq(Expr::Value(snapshot.owner.clone().into())).and(Expr::col(("t", "id")).eq(Expr::Value(turn.into()))).and(Expr::exists(Query::select().expr(Expr::val(1_i64)).from_as("compaction_execution_stop", "stop").and_where(Expr::col(("stop", "owner")).eq(Expr::col(("c", "owner"))).and(Expr::col(("stop", "turn_id")).eq(Expr::col(("t", "id"))))).to_owned()).not()).and(Expr::col(("t", "status")).is_in(["interrupted", "cancelled"]).not())).to_owned())).await?.is_some(), "compaction execution was stopped or changed scope");
            }
            // Epochs were read before the source projection was resolved.
            // Revalidate their workspace and revision inside admission; parsing
            // the bounded metadata here is part of this atomic DB transition.
            let changed = txn.query_one_raw(sqlite_specific_sql("SELECT wanted.key FROM json_each(?,'$.source_epochs') wanted WHERE NOT EXISTS (SELECT 1 FROM thread t LEFT JOIN compaction_projection_epoch e ON e.thread_id=t.id WHERE t.id=wanted.key AND t.workspace_id=? AND COALESCE(e.version,0)=wanted.value) LIMIT 1", [snapshot_json.clone().into(), workspace.into()])).await?;
            ensure!(changed.is_none(), "source scope or epoch changed before admission");
            txn.execute_raw(statement(&Query::insert().into_table("compaction_operation").columns(["id", "owner", "fingerprint", "status", "snapshot", "deadline_ms", "expected_head", "execution_turn"]).values_panic([Expr::Value(snapshot.id.clone().into()), Expr::Value(snapshot.owner.clone().into()), Expr::Value(snapshot.plan.fingerprint.clone().into()), Expr::val("running"), Expr::Value(snapshot_json.clone().into()), Expr::Value(deadline.into()), Expr::Value(snapshot.expected_checkpoint.clone().into()), Expr::Value(execution_turn.map(str::to_owned).into())]).on_conflict(OnConflict::columns(["owner", "fingerprint"]).do_nothing().to_owned()).to_owned())).await?;
            txn.commit().await?;
            Ok(())
        }).await?;
        OperationRecord::find_by_statement(statement(
            &Query::select()
                .expr(Expr::col(Asterisk))
                .from("compaction_operation")
                .and_where(
                    Expr::col("owner")
                        .eq(Expr::Value(snapshot.owner.clone().into()))
                        .and(
                            Expr::col("fingerprint")
                                .eq(Expr::Value(snapshot.plan.fingerprint.clone().into())),
                        ),
                )
                .to_owned(),
        ))
        .one(&self.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("admitted operation missing"))
    }

    pub async fn compaction_operation_for_plan(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
        fingerprint: &str,
    ) -> Result<Option<OperationRecord>> {
        Ok(OperationRecord::find_by_statement(statement(
            &Query::select()
                .expr(Expr::col(("o", Asterisk)))
                .from_as("compaction_operation", "o")
                .join_as(
                    JoinType::InnerJoin,
                    "compaction_context",
                    "c",
                    Expr::col(("c", "owner")).eq(Expr::col(("o", "owner"))),
                )
                .and_where(
                    Expr::col(("c", "workspace_id"))
                        .eq(Expr::Value(workspace.into()))
                        .and(Expr::col(("c", "thread_id")).eq(Expr::Value(thread.into())))
                        .and(Expr::col(("o", "owner")).eq(Expr::Value(owner.into())))
                        .and(Expr::col(("o", "fingerprint")).eq(Expr::Value(fingerprint.into()))),
                )
                .to_owned(),
        ))
        .one(&self.connection)
        .await?)
    }

    pub async fn compaction_operation(&self, id: &str) -> Result<Option<OperationRecord>> {
        Ok(OperationRecord::find_by_statement(statement(
            &Query::select()
                .expr(Expr::col(Asterisk))
                .from("compaction_operation")
                .and_where(Expr::col("id").eq(Expr::Value(id.into())))
                .to_owned(),
        ))
        .one(&self.connection)
        .await?)
    }

    /// Validate a bounded batch of exact identities after preparing its JSON
    /// outside reader capacity. An edited/deleted source or stale summary may
    /// never pass an otherwise-fitting native preflight as current history.
    pub async fn compaction_sources_current(
        &self,
        workspace: &str,
        thread: &str,
        sources: &[SourceRef],
    ) -> Result<bool> {
        ensure!(
            sources.len() <= SOURCE_PAGE_ROWS as usize,
            "source validation batch exceeds row bound"
        );
        let payload = serde_json::to_string(sources)?;
        ensure!(
            payload.len() <= SOURCE_PAGE_BYTES,
            "source validation batch exceeds byte bound"
        );
        let row = self.connection.query_one_raw(sqlite_specific_sql("SELECT COUNT(*) AS matched FROM json_each(?) wanted WHERE EXISTS (SELECT 1 FROM compaction_live_sources s WHERE s.workspace_id=? AND s.thread_id=? AND s.source_scope=json_extract(wanted.value,'$.scope') AND s.source_id=json_extract(wanted.value,'$.id') AND s.source_version=json_extract(wanted.value,'$.version'))", [payload.into(),workspace.into(),thread.into()])).await?.ok_or_else(|| anyhow::anyhow!("source validation missing"))?;
        Ok(row.try_get::<i64>("", "matched")? == sources.len() as i64)
    }

    /// Resolve only the storage scope of one exact current revision. Used to
    /// validate transitive checkpoint coverage before any source body is read.
    /// The caller still checks the returned thread against accepted scopes.
    pub async fn compaction_reference_thread(
        &self,
        workspace: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        ensure!(
            source
                .scope
                .len()
                .saturating_add(source.id.len())
                .saturating_add(source.version.len())
                <= SOURCE_PAGE_BYTES,
            "source identity exceeds metadata quantum"
        );
        self.connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("thread_id"))
                    .from("compaction_live_sources")
                    .and_where(
                        Expr::col("workspace_id")
                            .eq(Expr::Value(workspace.into()))
                            .and(
                                Expr::col("source_scope")
                                    .eq(Expr::Value(source.scope.clone().into())),
                            )
                            .and(Expr::col("source_id").eq(Expr::Value(source.id.clone().into())))
                            .and(
                                Expr::col("source_version")
                                    .eq(Expr::Value(source.version.clone().into())),
                            ),
                    )
                    .limit(1)
                    .to_owned(),
            ))
            .await?
            .map(|row| row.try_get("", "thread_id").map_err(Into::into))
            .transpose()
    }

    pub async fn compaction_head(&self, owner: &str) -> Result<Option<String>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("head"))
                    .expr(Expr::col("format_version"))
                    .from("compaction_context")
                    .and_where(Expr::col("owner").eq(Expr::Value(owner.into())))
                    .to_owned(),
            ))
            .await?;
        match row {
            None => Ok(None),
            Some(row) => {
                ensure!(
                    row.try_get::<i64>("", "format_version")? == i64::from(FORMAT_VERSION),
                    "unsupported working-context version"
                );
                Ok(row.try_get("", "head")?)
            }
        }
    }

    /// This is the only admission of a provider attempt. Counters survive restart.
    pub async fn compaction_claim_attempt(
        &self,
        id: &str,
        expected_attempts: i64,
        now_ms: i64,
        transient_retry: bool,
        correction: bool,
    ) -> Result<bool> {
        let result = self
            .connection
            .execute_raw(statement(
                &Query::update()
                    .table("compaction_operation")
                    .value("attempts", Expr::col("attempts").add(Expr::val(1_i64)))
                    .value(
                        "transient_retries",
                        Expr::col("transient_retries")
                            .add(Expr::Value((transient_retry as i64).into())),
                    )
                    .value(
                        "correction",
                        Expr::col("correction").add(Expr::Value((correction as i64).into())),
                    )
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(id.into()))
                            .and(Expr::col("status").eq(Expr::val("running")))
                            .and(Expr::col("attempts").eq(Expr::Value(expected_attempts.into())))
                            .and(Expr::col("deadline_ms").gt(Expr::Value(now_ms.into())))
                            .and(
                                Expr::col("transient_retries")
                                    .add(Expr::Value((transient_retry as i64).into()))
                                    .lte(Expr::val(2_i64)),
                            )
                            .and(
                                Expr::col("correction")
                                    .add(Expr::Value((correction as i64).into()))
                                    .lte(Expr::val(1_i64)),
                            ),
                    )
                    .to_owned(),
            ))
            .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn compaction_finish(&self, id: &str, status: &str, outcome: &str) -> Result<()> {
        ensure!(
            ["failed", "cancelled", "stale"].contains(&status),
            "invalid terminal transition"
        );
        ensure!(
            outcome.len() <= 128,
            "outcome must be a bounded classification"
        );
        self.connection
            .execute_raw(statement(
                &Query::update()
                    .table("compaction_operation")
                    .value("status", Expr::Value(status.into()))
                    .value("outcome", Expr::Value(outcome.into()))
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(id.into()))
                            .and(Expr::col("status").eq(Expr::val("running"))),
                    )
                    .to_owned(),
            ))
            .await?;
        Ok(())
    }

    /// Saves complete intermediate results without publishing a working pointer.
    pub async fn compaction_save_candidate(
        &self,
        checkpoint: &Checkpoint,
        portion: i64,
    ) -> Result<()> {
        ensure!(
            checkpoint.format_version == FORMAT_VERSION && !checkpoint.summary.trim().is_empty(),
            "invalid candidate"
        );
        ensure!(
            checkpoint.summary.len() <= SOURCE_PAGE_BYTES
                && checkpoint.coverage.len() <= CHECKPOINT_SOURCE_LIMIT,
            "candidate exceeds storage quantum"
        );
        let selection = serde_json::to_string(&checkpoint.selection)?;
        let identity = checkpoint_identity(checkpoint)?;
        let projection = i64::try_from(checkpoint.projection_version)?;
        let coverage: Vec<_> = checkpoint
            .coverage
            .iter()
            .map(|s| {
                statement(
                    &Query::insert()
                        .into_table("compaction_coverage")
                        .columns([
                            "checkpoint_id",
                            "source_scope",
                            "source_id",
                            "source_version",
                        ])
                        .values_panic([
                            Expr::Value(checkpoint.id.clone().into()),
                            Expr::Value(s.scope.clone().into()),
                            Expr::Value(s.id.clone().into()),
                            Expr::Value(s.version.clone().into()),
                        ])
                        .on_conflict(OnConflict::new().do_nothing().to_owned())
                        .to_owned(),
                )
            })
            .collect();
        // The payload and coverage are prepared outside capacity. The running operation is revalidated below.
        self.run_serialized_write(|| async {
            let txn = self.connection.begin().await?;
            let running = txn
                .query_one_raw(statement(
                    &Query::select()
                        .expr(Expr::col("id"))
                        .from("compaction_operation")
                        .and_where(
                            Expr::col("id")
                                .eq(Expr::Value(checkpoint.operation_id.clone().into()))
                                .and(
                                    Expr::col("owner")
                                        .eq(Expr::Value(checkpoint.owner.clone().into())),
                                )
                                .and(Expr::col("status").eq(Expr::val("running"))),
                        )
                        .to_owned(),
                ))
                .await?;
            ensure!(running.is_some(), "operation is not running");
            txn.execute_raw(statement(
                &Query::insert()
                    .into_table("compaction_checkpoint")
                    .columns([
                        "id",
                        "operation_id",
                        "owner",
                        "previous",
                        "portion",
                        "summary",
                        "selection",
                        "projection_version",
                        "format_version",
                        "identity_sha256",
                        "status",
                    ])
                    .values_panic([
                        Expr::Value(checkpoint.id.clone().into()),
                        Expr::Value(checkpoint.operation_id.clone().into()),
                        Expr::Value(checkpoint.owner.clone().into()),
                        Expr::Value(checkpoint.previous.clone().into()),
                        Expr::Value(portion.into()),
                        Expr::Value(checkpoint.summary.clone().into()),
                        Expr::Value(selection.clone().into()),
                        Expr::Value(projection.into()),
                        Expr::Value(i64::from(FORMAT_VERSION).into()),
                        Expr::Value(identity.clone().into()),
                        Expr::val("candidate"),
                    ])
                    .on_conflict(
                        OnConflict::columns(["operation_id", "portion"])
                            .do_nothing()
                            .to_owned(),
                    )
                    .to_owned(),
            ))
            .await?;
            // Idempotency is exact, never silently accept a different result for the same portion.
            let exact = txn
                .query_one_raw(statement(
                    &Query::select()
                        .expr(Expr::col("id"))
                        .from("compaction_checkpoint")
                        .and_where(
                            Expr::col("id")
                                .eq(Expr::Value(checkpoint.id.clone().into()))
                                .and(
                                    Expr::col("operation_id")
                                        .eq(Expr::Value(checkpoint.operation_id.clone().into())),
                                )
                                .and(Expr::col("portion").eq(Expr::Value(portion.into())))
                                .and(
                                    Expr::col("identity_sha256")
                                        .eq(Expr::Value(identity.clone().into())),
                                ),
                        )
                        .to_owned(),
                ))
                .await?;
            ensure!(exact.is_some(), "candidate idempotency conflict");
            for statement in &coverage {
                txn.execute_raw(statement.clone()).await?;
            }
            txn.execute_raw(statement(
                &Query::update()
                    .table("compaction_operation")
                    .value(
                        "next_portion",
                        Expr::expr(
                            Func::cust(Alias::new("max")).args([
                                Expr::col("next_portion"),
                                Expr::Value((portion + 1).into()),
                            ]),
                        ),
                    )
                    .and_where(
                        Expr::col("id").eq(Expr::Value(checkpoint.operation_id.clone().into())),
                    )
                    .to_owned(),
            ))
            .await?;
            txn.commit().await?;
            Ok(())
        })
        .await
    }

    /// Coverage discovery must not read summary text before source authorization.
    pub async fn compaction_checkpoint_edges(&self, id: &str) -> Result<Option<CheckpointEdges>> {
        let Some(row) = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("owner"))
                    .expr(Expr::col("previous"))
                    .expr(Expr::col("format_version"))
                    .from("compaction_checkpoint")
                    .and_where(Expr::col("id").eq(Expr::Value(id.into())))
                    .to_owned(),
            ))
            .await?
        else {
            return Ok(None);
        };
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col("source_scope"))
                    .expr(Expr::col("source_id"))
                    .expr(Expr::col("source_version"))
                    .from("compaction_coverage")
                    .and_where(Expr::col("checkpoint_id").eq(Expr::Value(id.into())))
                    .order_by_expr(Expr::col("source_scope"), Order::Asc)
                    .order_by_expr(Expr::col("source_id"), Order::Asc)
                    .limit(CHECKPOINT_SOURCE_LIMIT as u64 + 1)
                    .to_owned(),
            ))
            .await?;
        ensure!(
            rows.len() <= CHECKPOINT_SOURCE_LIMIT,
            "checkpoint coverage exceeds supported quantum"
        );
        let coverage = rows
            .into_iter()
            .map(|row| {
                Ok(SourceRef {
                    scope: row.try_get("", "source_scope")?,
                    id: row.try_get("", "source_id")?,
                    version: row.try_get("", "source_version")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(CheckpointEdges {
            owner: row.try_get("", "owner")?,
            previous: row.try_get("", "previous")?,
            format_version: u32::try_from(row.try_get::<i64>("", "format_version")?)?,
            coverage,
        }))
    }

    pub async fn compaction_checkpoint(&self, id: &str) -> Result<Option<Checkpoint>> {
        let Some(row) = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(Asterisk))
                    .from("compaction_checkpoint")
                    .and_where(Expr::col("id").eq(Expr::Value(id.into())))
                    .to_owned(),
            ))
            .await?
        else {
            return Ok(None);
        };
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col("source_scope"))
                    .expr(Expr::col("source_id"))
                    .expr(Expr::col("source_version"))
                    .from("compaction_coverage")
                    .and_where(Expr::col("checkpoint_id").eq(Expr::Value(id.into())))
                    .order_by_expr(Expr::col("source_scope"), Order::Asc)
                    .order_by_expr(Expr::col("source_id"), Order::Asc)
                    .limit(CHECKPOINT_SOURCE_LIMIT as u64 + 1)
                    .to_owned(),
            ))
            .await?;
        ensure!(
            rows.len() <= CHECKPOINT_SOURCE_LIMIT,
            "checkpoint coverage exceeds supported quantum"
        );
        let coverage = rows
            .into_iter()
            .map(|r| {
                Ok(SourceRef {
                    scope: r.try_get("", "source_scope")?,
                    id: r.try_get("", "source_id")?,
                    version: r.try_get("", "source_version")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Deserialization follows release of all read resources.
        Ok(Some(Checkpoint {
            id: row.try_get("", "id")?,
            operation_id: row.try_get("", "operation_id")?,
            owner: row.try_get("", "owner")?,
            previous: row.try_get("", "previous")?,
            summary: row.try_get("", "summary")?,
            selection: serde_json::from_str(&row.try_get::<String>("", "selection")?)?,
            coverage,
            projection_version: row.try_get::<i64>("", "projection_version")? as u64,
            format_version: row.try_get::<i64>("", "format_version")? as u32,
        }))
    }

    /// Atomic CAS. Appends do not invalidate selected sources; edits, Stop and another head do.
    pub async fn compaction_apply(
        &self,
        checkpoint: &Checkpoint,
        expected_head: Option<&str>,
        assertions: &[SourceAssertion],
    ) -> Result<CommitOutcome> {
        ensure!(
            assertions.len() <= CHECKPOINT_SOURCE_LIMIT
                && assertions.iter().map(|s| s.payload.len()).sum::<usize>() <= SOURCE_PAGE_BYTES,
            "source revalidation exceeds quantum"
        );
        let mut references: Vec<_> = assertions.iter().map(SourceAssertion::reference).collect();
        let mut coverage = checkpoint.coverage.clone();
        references.sort();
        coverage.sort();
        ensure!(
            references == coverage,
            "candidate coverage differs from validated sources"
        );
        let assertions: Vec<_> = assertions
            .iter()
            .map(|s| {
                if let Some(revision) = s.revision {
                    let (table, revisions) = match s.kind {
                        CanonicalSource::Input => ("turn_input", "compaction_input_revision"),
                        CanonicalSource::ProviderContext => {
                            ("turn_llm_context", "compaction_source_revision")
                        }
                        CanonicalSource::ToolItem => ("turn_item", "compaction_item_revision"),
                        CanonicalSource::Event => ("turn_event", "compaction_event_revision"),
                    };
                    Ok(statement(
                        &Query::select()
                            .expr(Expr::col(("s", "id")))
                            .from_as(Alias::new(table), "s")
                            .join_as(
                                JoinType::InnerJoin,
                                Alias::new(revisions),
                                "r",
                                Expr::col(("r", "source_id"))
                                    .eq(Expr::col(("s", "id")))
                                    .and(
                                        Expr::col(("r", "turn_id")).eq(Expr::col(("s", "turn_id"))),
                                    ),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "turn",
                                "t",
                                Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "thread",
                                "th",
                                Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "compaction_context",
                                "c",
                                Expr::col(("c", "thread_id"))
                                    .eq(Expr::col(("t", "thread_id")))
                                    .and(
                                        Expr::col(("c", "workspace_id"))
                                            .eq(Expr::col(("th", "workspace_id"))),
                                    ),
                            )
                            .and_where(
                                Expr::col(("c", "owner"))
                                    .eq(Expr::Value(checkpoint.owner.clone().into()))
                                    .and(
                                        Expr::col(("s", "turn_id"))
                                            .eq(Expr::Value(s.turn_id.clone().into())),
                                    )
                                    .and(
                                        Expr::col(("s", "id")).eq(Expr::Value(s.id.clone().into())),
                                    )
                                    .and(
                                        Expr::col(("r", "revision"))
                                            .eq(Expr::Value(revision.into())),
                                    )
                                    .and(Expr::col(("r", "present")).eq(Expr::val(1_i64))),
                            )
                            .to_owned(),
                    ))
                } else {
                    Ok(statement(
                        &Query::select()
                            .expr(Expr::col(("s", "id")))
                            .from_as(Alias::new(s.kind.table()), "s")
                            .join_as(
                                JoinType::InnerJoin,
                                "turn",
                                "t",
                                Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "thread",
                                "th",
                                Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "compaction_context",
                                "c",
                                Expr::col(("c", "thread_id"))
                                    .eq(Expr::col(("t", "thread_id")))
                                    .and(
                                        Expr::col(("c", "workspace_id"))
                                            .eq(Expr::col(("th", "workspace_id"))),
                                    ),
                            )
                            .and_where(
                                Expr::col(("c", "owner"))
                                    .eq(Expr::Value(checkpoint.owner.clone().into()))
                                    .and(
                                        Expr::col(("s", "turn_id"))
                                            .eq(Expr::Value(s.turn_id.clone().into())),
                                    )
                                    .and(
                                        Expr::col(("s", "id")).eq(Expr::Value(s.id.clone().into())),
                                    )
                                    .and(
                                        Expr::col(("s", "payload"))
                                            .eq(Expr::Value(s.payload.clone().into())),
                                    ),
                            )
                            .to_owned(),
                    ))
                }
            })
            .collect::<Result<_>>()?;
        let identity = checkpoint_identity(checkpoint)?;
        self.run_serialized_write(|| async {
            let txn = self.connection.begin().await?;
            let exact = txn.query_one_raw(statement(&Query::select().expr(Expr::col("id")).from("compaction_checkpoint").and_where(Expr::col("id").eq(Expr::Value(checkpoint.id.clone().into())).and(Expr::col("identity_sha256").eq(Expr::Value(identity.clone().into())))).to_owned())).await?;
            ensure!(exact.is_some(), "candidate identity mismatch");
            let op = txn.query_one_raw(statement(&Query::select().expr(Expr::col("status")).from("compaction_operation").and_where(Expr::col("id").eq(Expr::Value(checkpoint.operation_id.clone().into())).and(Expr::col("owner").eq(Expr::Value(checkpoint.owner.clone().into()))).and(Expr::col("expected_head").binary(BinOper::Is, Expr::Value(expected_head.map(str::to_owned).into())))).to_owned())).await?;
            let status: String = op.ok_or_else(|| anyhow::anyhow!("operation missing"))?.try_get("", "status")?;
            if status == "completed" { txn.rollback().await?; return Ok(CommitOutcome::AlreadyApplied); }
            if status != "running" { txn.rollback().await?; return Ok(CommitOutcome::Cancelled); }
            let stopped = txn.query_one_raw(statement(&Query::select().expr(Expr::col(("o", "id"))).from_as("compaction_operation", "o").join_as(JoinType::InnerJoin, "compaction_context", "c", Expr::col(("c", "owner")).eq(Expr::col(("o", "owner")))).and_where(Expr::col(("o", "id")).eq(Expr::Value(checkpoint.operation_id.clone().into())).and(Expr::col(("o", "execution_turn")).binary(BinOper::Is, Expr::val(Option::<String>::None)).not()).and(Expr::exists(Query::select().expr(Expr::val(1_i64)).from_as("compaction_execution_stop", "stop").and_where(Expr::col(("stop", "owner")).eq(Expr::col(("c", "owner"))).and(Expr::col(("stop", "turn_id")).eq(Expr::col(("o", "execution_turn"))))).to_owned()))).to_owned())).await?;
            if stopped.is_some() { txn.rollback().await?; return Ok(CommitOutcome::Cancelled); }
            let dependency_changed = txn.query_one_raw(sqlite_specific_sql("SELECT o.id FROM compaction_operation o WHERE o.id=? AND EXISTS (SELECT 1 FROM json_each(o.snapshot,'$.source_epochs') wanted WHERE COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id=wanted.key),0)<>wanted.value OR NOT EXISTS (SELECT 1 FROM thread t JOIN compaction_context c ON c.workspace_id=t.workspace_id WHERE t.id=wanted.key AND c.owner=o.owner))", [checkpoint.operation_id.clone().into()])).await?;
            if dependency_changed.is_some() { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
            for statement in &assertions {
                if txn.query_one_raw(statement.clone()).await?.is_none() { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
            }
            let changed = txn.execute_raw(statement(&Query::update().table("compaction_context").value("head", Expr::Value(checkpoint.id.clone().into())).and_where(Expr::col("owner").eq(Expr::Value(checkpoint.owner.clone().into())).and(Expr::col("head").binary(BinOper::Is, Expr::Value(expected_head.map(str::to_owned).into()))).and(Expr::col("format_version").eq(Expr::val(1_i64))).and(Expr::exists(Query::select().expr(Expr::val(1_i64)).from_as("compaction_checkpoint", "p").and_where(Expr::col(("p", "id")).eq(Expr::Value(checkpoint.id.clone().into())).and(Expr::col(("p", "operation_id")).eq(Expr::Value(checkpoint.operation_id.clone().into()))).and(Expr::col(("p", "owner")).eq(Expr::col(("compaction_context", "owner")))).and(Expr::col(("p", "status")).eq(Expr::val("candidate")))).to_owned()))).to_owned())).await?;
            if changed.rows_affected() != 1 { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
            txn.execute_raw(statement(&Query::update().table("compaction_checkpoint").value("status", Expr::val("applied")).and_where(Expr::col("id").eq(Expr::Value(checkpoint.id.clone().into()))).to_owned())).await?;
            txn.execute_raw(statement(&Query::update().table("compaction_operation").value("status", Expr::val("completed")).value("outcome", Expr::val("applied")).and_where(Expr::col("id").eq(Expr::Value(checkpoint.operation_id.clone().into()))).to_owned())).await?;
            txn.commit().await?;
            Ok(CommitOutcome::Applied)
        }).await
    }
}

#[derive(Clone, Debug)]
pub struct CanonicalFragment {
    pub reference: SourceRef,
    pub text: String,
    pub next_character: Option<u64>,
}

impl CrudStore {
    /// Bounded Unicode-safe reads of very large canonical payloads. Revision is
    /// maintained atomically by source-table triggers, including deletes/reinserts.
    /// The same revision must be supplied for every subsequent fragment.
    pub async fn compaction_source_fragment(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
        expected_revision: Option<&str>,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        self.compaction_payload_fragment(
            workspace,
            thread,
            turn,
            id,
            expected_revision,
            character_offset,
            CanonicalSource::ProviderContext,
        )
        .await
    }

    async fn compaction_payload_fragment(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
        expected_revision: Option<&str>,
        character_offset: u64,
        kind: CanonicalSource,
    ) -> Result<Option<CanonicalFragment>> {
        let (table, revisions, scope, version_prefix) = (
            kind.table(),
            kind.revisions(),
            kind.prefix(),
            kind.version_prefix(),
        );
        let offset = i64::try_from(character_offset)?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("source offset overflow"))?;
        // Lazy initialization only touches a single pre-migration row; no full-table backfill.
        self.connection
            .execute_raw(statement(
                &Query::insert()
                    .into_table(Alias::new(revisions))
                    .columns(["source_id", "turn_id", "revision", "present"])
                    .select_from(
                        Query::select()
                            .expr(Expr::col(("s", "id")))
                            .expr(Expr::col(("s", "turn_id")))
                            .expr(Expr::val(1_i64))
                            .expr(Expr::val(1_i64))
                            .from_as(Alias::new(table), "s")
                            .join_as(
                                JoinType::InnerJoin,
                                "turn",
                                "t",
                                Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                            )
                            .join_as(
                                JoinType::InnerJoin,
                                "thread",
                                "th",
                                Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                            )
                            .and_where(
                                Expr::col(("s", "id"))
                                    .eq(Expr::Value(id.into()))
                                    .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                                    .and(Expr::col(("th", "id")).eq(Expr::Value(thread.into())))
                                    .and(
                                        Expr::col(("th", "workspace_id"))
                                            .eq(Expr::Value(workspace.into())),
                                    ),
                            )
                            .to_owned(),
                    )?
                    .on_conflict(OnConflict::columns(["source_id"]).do_nothing().to_owned())
                    .to_owned(),
            ))
            .await?;
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("r", "revision")))
                    .expr_as(
                        Expr::expr(Func::cust(Alias::new("substr")).args([
                            Expr::col(("s", "payload")),
                            Expr::Value(offset.into()),
                            Expr::val(16384_i64),
                        ])),
                        "fragment",
                    )
                    .expr_as(
                        Expr::expr(
                            Func::cust(Alias::new("length")).args([Expr::col(("s", "payload"))]),
                        ),
                        "characters",
                    )
                    .from_as(Alias::new(table), "s")
                    .join_as(
                        JoinType::InnerJoin,
                        Alias::new(revisions),
                        "r",
                        Expr::col(("r", "source_id"))
                            .eq(Expr::col(("s", "id")))
                            .and(Expr::col(("r", "present")).eq(Expr::val(1_i64)))
                            .and(Expr::col(("r", "turn_id")).eq(Expr::col(("s", "turn_id")))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .and_where(
                        Expr::col(("s", "id"))
                            .eq(Expr::Value(id.into()))
                            .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("th", "id")).eq(Expr::Value(thread.into())))
                            .and(
                                Expr::col(("th", "workspace_id")).eq(Expr::Value(workspace.into())),
                            ),
                    )
                    .to_owned(),
            ))
            .await?;
        let Some(row) = row else { return Ok(None) };
        let version = format!("{version_prefix}:{}", row.try_get::<i64>("", "revision")?);
        ensure!(
            expected_revision.is_none_or(|expected| expected == version),
            "stale source revision"
        );
        let text: String = row.try_get("", "fragment")?;
        let characters: i64 = row.try_get("", "characters")?;
        let end = character_offset.saturating_add(text.chars().count() as u64);
        ensure!(
            character_offset <= characters as u64,
            "source offset is past end"
        );
        Ok(Some(CanonicalFragment {
            reference: SourceRef {
                scope: format!("{scope}:{turn}"),
                id: id.into(),
                version,
            },
            text,
            next_character: (end < characters as u64).then_some(end),
        }))
    }
}

impl CrudStore {
    /// Resolve a tool item to its existing full canonical record without exposing
    /// another workspace/thread. Authorization remains the caller's mandatory gate.
    pub async fn compaction_tool_result_id(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<String>> {
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "id")))
                    .from_as("turn_llm_context", "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                    )
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
                            .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("s", "item_id")).eq(Expr::Value(item.into())))
                            .and(Expr::col(("s", "source")).eq(Expr::val("tool_result_v2"))),
                    )
                    .order_by_expr(Expr::col(("s", "sequence")), Order::Asc)
                    .limit(2)
                    .to_owned(),
            ))
            .await?;
        ensure!(rows.len() <= 1, "tool result source is ambiguous");
        Ok(rows.first().map(|r| r.try_get("", "id")).transpose()?)
    }
}

impl CrudStore {
    /// Prefer an already retained full shell source. Other tools use their canonical result.
    /// Both lookups and fragments retain the same workspace/thread/turn scope.
    pub async fn compaction_tool_result_fragment(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        expected_revision: Option<&str>,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "id")))
                    .from_as("turn_item", "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                    )
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
                            .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("s", "item_id")).eq(Expr::Value(item.into())))
                            .and(Expr::expr(
                                Func::cust(Alias::new("json_valid"))
                                    .args([Expr::col(("s", "payload"))]),
                            ))
                            .and(
                                Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                    Expr::col(("s", "payload")),
                                    Expr::val("$.storage.kind"),
                                ]))
                                .eq(Expr::val("shell")),
                            ),
                    )
                    .limit(1)
                    .to_owned(),
            ))
            .await?;
        if let Some(row) = row {
            let id: String = row.try_get("", "id")?;
            return self
                .compaction_payload_fragment(
                    workspace,
                    thread,
                    turn,
                    &id,
                    expected_revision,
                    character_offset,
                    CanonicalSource::ToolItem,
                )
                .await;
        }
        let Some(id) = self
            .compaction_tool_result_id(workspace, thread, turn, item)
            .await?
        else {
            return Ok(None);
        };
        self.compaction_source_fragment(
            workspace,
            thread,
            turn,
            &id,
            expected_revision,
            character_offset,
        )
        .await
    }
}

impl CrudStore {
    pub async fn compaction_reference_fragment(
        &self,
        workspace: &str,
        thread: &str,
        reference: &SourceRef,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        if reference.scope.starts_with("task-basis:") {
            let offset = i64::try_from(character_offset)?
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("source offset overflow"))?;
            let row = self
                .connection
                .query_one_raw(statement(
                    &Query::select()
                        .expr_as(
                            Expr::expr(Func::cust(Alias::new("substr")).args([
                                Expr::col(("t", "history_json")),
                                Expr::Value(offset.into()),
                                Expr::val(16384_i64),
                            ])),
                            "fragment",
                        )
                        .expr_as(
                            Expr::expr(
                                Func::cust(Alias::new("length"))
                                    .args([Expr::col(("t", "history_json"))]),
                            ),
                            "characters",
                        )
                        .from_as("task_run_conversation_snapshot", "t")
                        .join_as(
                            JoinType::InnerJoin,
                            "compaction_live_sources",
                            "s",
                            Expr::col(("s", "source_id"))
                                .eq(Expr::col(("t", "run_id")))
                                .and(
                                    Expr::col(("s", "source_scope")).eq(Expr::val("task-basis:")
                                        .binary(BinOper::Custom("||"), Expr::col(("t", "run_id")))),
                                ),
                        )
                        .and_where(
                            Expr::col(("s", "workspace_id"))
                                .eq(Expr::Value(workspace.into()))
                                .and(Expr::col(("s", "thread_id")).eq(Expr::Value(thread.into())))
                                .and(
                                    Expr::col(("s", "source_scope"))
                                        .eq(Expr::Value(reference.scope.clone().into())),
                                )
                                .and(
                                    Expr::col(("s", "source_id"))
                                        .eq(Expr::Value(reference.id.clone().into())),
                                )
                                .and(
                                    Expr::col(("s", "source_version"))
                                        .eq(Expr::Value(reference.version.clone().into())),
                                ),
                        )
                        .to_owned(),
                ))
                .await?;
            let Some(row) = row else {
                return Ok(None);
            };
            let text: String = row.try_get("", "fragment")?;
            let characters = u64::try_from(row.try_get::<i64>("", "characters")?)?;
            ensure!(character_offset <= characters, "source offset is past end");
            let end = character_offset.saturating_add(text.chars().count() as u64);
            return Ok(Some(CanonicalFragment {
                reference: reference.clone(),
                text,
                next_character: (end < characters).then_some(end),
            }));
        }
        if reference.scope.starts_with("checkpoint:") {
            let offset = i64::try_from(character_offset)?
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("source offset overflow"))?;
            let row = self
                .connection
                .query_one_raw(statement(
                    &Query::select()
                        .expr_as(
                            Expr::expr(Func::cust(Alias::new("substr")).args([
                                Expr::col(("p", "summary")),
                                Expr::Value(offset.into()),
                                Expr::val(16384_i64),
                            ])),
                            "fragment",
                        )
                        .expr_as(
                            Expr::expr(
                                Func::cust(Alias::new("length"))
                                    .args([Expr::col(("p", "summary"))]),
                            ),
                            "characters",
                        )
                        .from_as("compaction_checkpoint", "p")
                        .join_as(
                            JoinType::InnerJoin,
                            "compaction_live_sources",
                            "s",
                            Expr::col(("s", "source_id"))
                                .eq(Expr::col(("p", "id")))
                                .and(
                                    Expr::col(("s", "source_scope")).eq(Expr::val("checkpoint:")
                                        .binary(BinOper::Custom("||"), Expr::col(("p", "owner")))),
                                ),
                        )
                        .and_where(
                            Expr::col(("s", "workspace_id"))
                                .eq(Expr::Value(workspace.into()))
                                .and(Expr::col(("s", "thread_id")).eq(Expr::Value(thread.into())))
                                .and(
                                    Expr::col(("s", "source_scope"))
                                        .eq(Expr::Value(reference.scope.clone().into())),
                                )
                                .and(
                                    Expr::col(("s", "source_id"))
                                        .eq(Expr::Value(reference.id.clone().into())),
                                )
                                .and(
                                    Expr::col(("s", "source_version"))
                                        .eq(Expr::Value(reference.version.clone().into())),
                                ),
                        )
                        .to_owned(),
                ))
                .await?;
            let Some(row) = row else { return Ok(None) };
            let text: String = row.try_get("", "fragment")?;
            let characters = u64::try_from(row.try_get::<i64>("", "characters")?)?;
            ensure!(character_offset <= characters, "source offset is past end");
            let end = character_offset.saturating_add(text.chars().count() as u64);
            return Ok(Some(CanonicalFragment {
                reference: reference.clone(),
                text,
                next_character: (end < characters).then_some(end),
            }));
        }
        let (prefix, turn) = reference
            .scope
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid source scope"))?;
        let kind = match prefix {
            "input" => CanonicalSource::Input,
            "context" => CanonicalSource::ProviderContext,
            "event" => CanonicalSource::Event,
            "item" => CanonicalSource::ToolItem,
            _ => anyhow::bail!("unsupported canonical source"),
        };
        ensure!(
            reference
                .version
                .starts_with(&format!("{}:", kind.version_prefix())),
            "source revision is unknown"
        );
        self.compaction_payload_fragment(
            workspace,
            thread,
            turn,
            &reference.id,
            Some(&reference.version),
            character_offset,
            kind,
        )
        .await
    }
}

impl CrudStore {
    pub async fn compaction_projection_version(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<u64> {
        let row =
            self.connection
                .query_one_raw(statement(
                    &Query::select()
                        .expr_as(
                            Expr::expr(
                                Func::cust(Alias::new("coalesce"))
                                    .args([Expr::col(("e", "version")), Expr::val(0_i64)]),
                            ),
                            "version",
                        )
                        .from_as("thread", "t")
                        .join_as(
                            JoinType::LeftJoin,
                            "compaction_projection_epoch",
                            "e",
                            Expr::col(("e", "thread_id")).eq(Expr::col(("t", "id"))),
                        )
                        .and_where(Expr::col(("t", "id")).eq(Expr::Value(thread.into())).and(
                            Expr::col(("t", "workspace_id")).eq(Expr::Value(workspace.into())),
                        ))
                        .to_owned(),
                ))
                .await?
                .ok_or_else(|| anyhow::anyhow!("context scope unavailable"))?;
        Ok(u64::try_from(row.try_get::<i64>("", "version")?)?)
    }
    pub async fn compaction_checkpoint_source(
        &self,
        workspace: &str,
        thread: &str,
        checkpoint: &str,
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
                        Expr::col("source_id")
                            .eq(Expr::Value(checkpoint.into()))
                            .and(Expr::col("source_scope").like("checkpoint:%"))
                            .and(Expr::col("workspace_id").eq(Expr::Value(workspace.into())))
                            .and(Expr::col("thread_id").eq(Expr::Value(thread.into()))),
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
}

impl CrudStore {
    /// Resolve a runtime locator only after its durable append was acknowledged.
    /// Reads metadata, never a full result, and cannot cross the execution scope.
    pub async fn compaction_context_reference_for_item(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        source: &str,
    ) -> Result<Option<SourceRef>> {
        ensure!(
            ["assistant_round", "tool_result_v2"].contains(&source),
            "unsupported canonical runtime locator"
        );
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "id")))
                    .expr_as(
                        Expr::expr(
                            Func::cust(Alias::new("coalesce"))
                                .args([Expr::col(("r", "revision")), Expr::val(1_i64)]),
                        ),
                        "revision",
                    )
                    .from_as("turn_llm_context", "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        "compaction_source_revision",
                        "r",
                        Expr::col(("r", "source_id")).eq(Expr::col(("s", "id"))),
                    )
                    .and_where(
                        Expr::col(("th", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("th", "id")).eq(Expr::Value(thread.into())))
                            .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("s", "item_id")).eq(Expr::Value(item.into())))
                            .and(Expr::col(("s", "source")).eq(Expr::Value(source.into()))),
                    )
                    .order_by_expr(Expr::col(("s", "sequence")), Order::Desc)
                    .limit(1)
                    .to_owned(),
            ))
            .await?;
        row.map(|row| {
            Ok(SourceRef {
                scope: format!("context:{turn}"),
                id: row.try_get("", "id")?,
                version: format!("revision:{}", row.try_get::<i64>("", "revision")?),
            })
        })
        .transpose()
    }
    pub async fn compaction_tool_item_reference(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<SourceRef>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "id")))
                    .expr_as(
                        Expr::expr(
                            Func::cust(Alias::new("coalesce"))
                                .args([Expr::col(("r", "revision")), Expr::val(1_i64)]),
                        ),
                        "revision",
                    )
                    .from_as("turn_item", "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        "compaction_item_revision",
                        "r",
                        Expr::col(("r", "source_id")).eq(Expr::col(("s", "id"))),
                    )
                    .and_where(
                        Expr::col(("th", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("th", "id")).eq(Expr::Value(thread.into())))
                            .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("s", "item_id")).eq(Expr::Value(item.into())))
                            .and(
                                Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                    Expr::col(("s", "payload")),
                                    Expr::val("$.storage.kind"),
                                ]))
                                .eq(Expr::val("shell")),
                            ),
                    )
                    .limit(1)
                    .to_owned(),
            ))
            .await?;
        row.map(|row| {
            Ok(SourceRef {
                scope: format!("item:{turn}"),
                id: row.try_get("", "id")?,
                version: format!("item-revision:{}", row.try_get::<i64>("", "revision")?),
            })
        })
        .transpose()
    }
}

impl CrudStore {
    /// Resolve a saved shell result or frozen provider representation to its tool
    /// item. Read identity metadata only and require the exact live revision.
    pub async fn compaction_replay_item_id(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        let (table, prefix, kind) = if source.scope.starts_with("item:") {
            (
                "turn_item",
                "item:",
                Expr::expr(
                    Func::cust(Alias::new("json_extract"))
                        .args([Expr::col(("s", "payload")), Expr::val("$.storage.kind")]),
                )
                .eq(Expr::val("shell")),
            )
        } else if source.scope.starts_with("context:") {
            (
                "turn_llm_context",
                "context:",
                Expr::col(("s", "source")).eq(Expr::val("tool_result_v2")),
            )
        } else {
            return Ok(None);
        };
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "item_id")))
                    .from_as(table, "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_live_sources",
                        "l",
                        Expr::col(("l", "source_scope"))
                            .eq(Expr::val(prefix)
                                .binary(BinOper::Custom("||"), Expr::col(("s", "turn_id"))))
                            .and(Expr::col(("l", "source_id")).eq(Expr::col(("s", "id")))),
                    )
                    .and_where(
                        Expr::col(("l", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("l", "thread_id")).eq(Expr::Value(thread.into())))
                            .and(
                                Expr::col(("l", "source_scope"))
                                    .eq(Expr::Value(source.scope.clone().into())),
                            )
                            .and(
                                Expr::col(("l", "source_id"))
                                    .eq(Expr::Value(source.id.clone().into())),
                            )
                            .and(
                                Expr::col(("l", "source_version"))
                                    .eq(Expr::Value(source.version.clone().into())),
                            )
                            .and(kind),
                    )
                    .to_owned(),
            ))
            .await?;
        row.map(|row| Ok(row.try_get("", "item_id")?)).transpose()
    }
}

impl CrudStore {
    pub async fn compaction_item_reference(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<SourceRef>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("s", "id")))
                    .expr_as(
                        Expr::expr(
                            Func::cust(Alias::new("coalesce"))
                                .args([Expr::col(("r", "revision")), Expr::val(1_i64)]),
                        ),
                        "revision",
                    )
                    .from_as("turn_item", "s")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn",
                        "t",
                        Expr::col(("t", "id")).eq(Expr::col(("s", "turn_id"))),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "thread",
                        "th",
                        Expr::col(("th", "id")).eq(Expr::col(("t", "thread_id"))),
                    )
                    .join_as(
                        JoinType::LeftJoin,
                        "compaction_item_revision",
                        "r",
                        Expr::col(("r", "source_id"))
                            .eq(Expr::col(("s", "id")))
                            .and(Expr::col(("r", "turn_id")).eq(Expr::col(("s", "turn_id")))),
                    )
                    .and_where(
                        Expr::col(("th", "workspace_id"))
                            .eq(Expr::Value(workspace.into()))
                            .and(Expr::col(("th", "id")).eq(Expr::Value(thread.into())))
                            .and(Expr::col(("s", "turn_id")).eq(Expr::Value(turn.into())))
                            .and(Expr::col(("s", "item_id")).eq(Expr::Value(item.into()))),
                    )
                    .to_owned(),
            ))
            .await?;
        row.map(|row| {
            Ok(SourceRef {
                scope: format!("item:{turn}"),
                id: row.try_get("", "id")?,
                version: format!("item-revision:{}", row.try_get::<i64>("", "revision")?),
            })
        })
        .transpose()
    }
}
