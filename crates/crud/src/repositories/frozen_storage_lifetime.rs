//! Private progress for the existing frozen-storage worker. No durable queue,
//! global registry, connection, or capacity permit is retained between quanta.
use super::{checkpoint_proofs, compaction};
use crate::{CrudStore, frozen_lifetime::PreparedFrozenRoot};
use anyhow::{Result, ensure};
use pioneer_compaction::runner::{RunnerPhase, RunnerState};
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_context as context,
    compaction_coverage as coverage, compaction_frozen_history as history,
    compaction_frozen_layout as layout, compaction_frozen_span as span,
    compaction_operation as operation, compaction_operation_projection as projection,
    compaction_runner_state as runner, task, task_run_conversation_snapshot as task_input,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, JoinType, Order, Query, UnionType};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, Statement, TransactionTrait,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const FRAGMENT_BYTES: i64 = 64 * 1024;
const META_ROWS: u64 = 64;
// Legacy JSON has no producer byte cap. An oversized record remains unknown
// (and blocks only its retaining scope), rather than allowing one worker job
// to grow without bound. A normal authorized root replacement can verify it.
const LEGACY_LOCATOR_BYTES: i64 = FRAGMENT_BYTES * 128;

/// Owned by one existing maintenance worker, and dropped on cancellation.
/// Each discovery family reaches EOF once per pass; completed families yield
/// while the other bounded preparations finish.
#[derive(Default)]
pub struct FrozenStorageLifetimeProgress {
    phase: usize,
    pass_done: [bool; 6],
    // Reconciliation/sealing can release holds discovered earlier this pass.
    pass_changed: bool,
    locator_done: [bool; 4],
    physical_done: [bool; 2],
    scope: ScopeDiscovery,
    locators: [String; 4],
    locator_kind: usize,
    locator_pending: [VecDeque<(String, String, i64)>; 4],
    locator: Option<LocatorRead>,
    proof_after: String,
    proof_pending: VecDeque<String>,
    proof: Option<checkpoint_proofs::GraphPreparation>,
    expiry_after: String,
    expiry: Option<ExpiryPreparation>,
    physical_after: [Option<String>; 2],
    physical_until: [Option<String>; 2],
    physical_started: [bool; 2],
    physical_kind: usize,
    sweep: Option<SweepPreparation>,
    sweep_pending: VecDeque<SweepPreparation>,
    sweep_discover: bool,
    accounting: SweepAccounting,
    discard_after: String,
    discard: Option<DiscardPreparation>,
    pub(super) conversion: Option<super::compaction_frozen_storage::StagedCopyCheck>,
}
const ROOTS: [(&str, &str, &str); 4] = [
    ("task_run_conversation_snapshot", "run_id", "history_json"),
    ("turn_runtime_snapshot", "turn_id", "history_json"),
    (
        "thread_cli_runtime_binding",
        "thread_id",
        "resume_cursor_json",
    ),
    ("turn_cli_runtime_binding", "turn_id", "input_mapping_json"),
];
struct LocatorRead {
    kind: usize,
    id: String,
    workspace: String,
    size: i64,
    bytes: Vec<u8>,
}
impl LocatorRead {
    async fn step(&mut self, store: &CrudStore) -> Result<bool> {
        let (table, key, field) = ROOTS[self.kind];
        // SQLite BLOB fragment, fixed actual-byte budget; sentinel/identity CAS
        // below stays typed and never compares the accumulated JSON in writer.
        if i64::try_from(self.bytes.len())? < self.size {
            let row = store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                format!("SELECT substr(CAST({field} AS BLOB),?3,?4) AS fragment FROM {table} WHERE {key}=?1 AND workspace_id=?2 AND frozen_manifest_id='' AND length(CAST({field} AS BLOB))=?5 LIMIT 1"),
                [self.id.clone().into(),self.workspace.clone().into(),(self.bytes.len() as i64+1).into(),FRAGMENT_BYTES.into(),self.size.into()])).await?;
            let Some(row) = row else {
                return Ok(true);
            };
            let bytes: Vec<u8> = row.try_get("", "fragment")?;
            ensure!(
                !bytes.is_empty() && bytes.len() <= FRAGMENT_BYTES as usize,
                "legacy locator fragment missing"
            );
            self.bytes.extend(bytes);
            return Ok(false);
        }
        ensure!(
            self.bytes.len() as i64 == self.size,
            "legacy locator exact size mismatch"
        );
        let json = std::str::from_utf8(&self.bytes)?;
        let prepared = match self.kind {
            0 | 1 => PreparedFrozenRoot::history(json)?,
            2 => PreparedFrozenRoot::cli(json, true)?,
            3 => PreparedFrozenRoot::cli(json, false)?,
            _ => unreachable!(),
        };
        // All JSON/root-identity mutations have already cut over to ID/NULL.
        // Metadata-only changes keep ''. Delete/recreate cannot regain ''.
        // No updated_at comparison or large JSON comparison under writer.
        store
            .run_serialized_write(|| async {
                let tx = store.connection.begin().await?;
                let locator_guard = Condition::all()
                    .add(Expr::col(Alias::new(key)).eq(&self.id))
                    .add(Expr::col(Alias::new("workspace_id")).eq(&self.workspace))
                    .add(Expr::col(Alias::new("frozen_manifest_id")).eq(""));
                let current = tx
                    .query_one(
                        &Query::select()
                            .expr(Expr::val(1))
                            .from(Alias::new(table))
                            .cond_where(locator_guard.clone())
                            .limit(1)
                            .to_owned(),
                    )
                    .await?;
                if current.is_some() {
                    prepared.verify(&tx, &self.workspace).await?;
                    if self.kind == 0 {
                        let parent = Query::select()
                            .expr(Expr::val(1))
                            .from(task_input::Entity)
                            .join(
                                JoinType::InnerJoin,
                                task::Entity,
                                Expr::col((task::Entity, task::Column::Id))
                                    .eq(Expr::col((task_input::Entity, task_input::Column::TaskId)))
                                    .and(Expr::col((task::Entity, task::Column::WorkspaceId)).eq(
                                        Expr::col((
                                            task_input::Entity,
                                            task_input::Column::WorkspaceId,
                                        )),
                                    )),
                            )
                            .and_where(
                                Expr::col((task_input::Entity, task_input::Column::RunId))
                                    .eq(&self.id),
                            )
                            .limit(1)
                            .to_owned();
                        ensure!(
                            tx.query_one(&parent).await?.is_some(),
                            "legacy Task scope is missing"
                        );
                    }
                    tx.execute(
                        &Query::update()
                            .table(Alias::new(table))
                            .value(Alias::new("frozen_manifest_id"), prepared.locator())
                            .cond_where(locator_guard)
                            .to_owned(),
                    )
                    .await?;
                }
                tx.commit().await?;
                Ok(())
            })
            .await?;
        Ok(true)
    }
}

/// Reverse traversal is paged separately from payload extraction. New
/// publication always seals its ancestry; an unsealed historical public root
/// cannot appear after this traversal via either production publication API.
struct Reachability {
    workspace: String,
    pending: VecDeque<(String, bool)>,
    seen: BTreeSet<String>,
    active: BTreeSet<String>,
    current: Option<(String, usize, String, BTreeSet<String>)>,
    reachable: bool,
}
impl Reachability {
    fn new(id: &str, workspace: &str) -> Self {
        Self {
            workspace: workspace.into(),
            pending: VecDeque::from([(id.into(), false)]),
            seen: BTreeSet::new(),
            active: BTreeSet::new(),
            current: None,
            reachable: false,
        }
    }
    fn done(&self) -> bool {
        self.reachable || (self.current.is_none() && self.pending.is_empty())
    }
    async fn step(&mut self, store: &CrudStore) -> Result<()> {
        if self.done() {
            return Ok(());
        }
        if self.current.is_none() {
            let (id, exit) = self.pending.pop_back().expect("reverse pending");
            if exit {
                self.active.remove(&id);
                self.seen.insert(id);
                return Ok(());
            }
            if self.seen.contains(&id) {
                return Ok(());
            }
            ensure!(
                self.active.insert(id.clone()),
                "cyclic reverse checkpoint graph"
            );
            ensure!(
                self.seen.len() + self.active.len() <= 65_536,
                "reverse graph exceeds supported bound"
            );
            let topology = compaction::checkpoint_row(&store.connection, &id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("reverse checkpoint missing"))?;
            ensure!(
                topology.workspace_id == self.workspace && topology.format_version == 1,
                "reverse graph scope corrupt"
            );
            let published = Query::select()
                .expr(Expr::val(1))
                .from(checkpoint::Entity)
                .join(
                    JoinType::InnerJoin,
                    operation::Entity,
                    Expr::col((operation::Entity, operation::Column::Id))
                        .eq(Expr::col((
                            checkpoint::Entity,
                            checkpoint::Column::OperationId,
                        )))
                        .and(
                            Expr::col((operation::Entity, operation::Column::Owner))
                                .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                        ),
                )
                .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(&id))
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                        .eq("applied")
                        .or(Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                            .eq("retained")
                            .and(
                                Expr::col((operation::Entity, operation::Column::Status))
                                    .eq("completed"),
                            )),
                )
                .limit(1)
                .to_owned();
            self.reachable = store.connection.query_one(&published).await?.is_some();
            self.current = Some((id, 0, String::new(), BTreeSet::new()));
            return Ok(());
        }
        let (id, branch, after, children) = self.current.as_mut().expect("reverse current");
        let topology = compaction::checkpoint_row(&store.connection, id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("reverse checkpoint removed"))?;
        let row = &topology;
        let mut query = Query::select();
        if *branch == 0 {
            query
                .from(checkpoint::Entity)
                .columns(
                    [checkpoint::Column::Id, checkpoint::Column::Owner]
                        .map(|col| (checkpoint::Entity, col)),
                )
                .columns(
                    [context::Column::WorkspaceId, context::Column::ThreadId]
                        .map(|col| (context::Entity, col)),
                )
                .join(
                    JoinType::LeftJoin,
                    context::Entity,
                    Expr::col((context::Entity, context::Column::Owner))
                        .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                )
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Previous)).eq(id.clone()),
                )
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Id)).gt(after.clone()),
                )
                .order_by((checkpoint::Entity, checkpoint::Column::Id), Order::Asc);
        } else {
            query
                .from(coverage::Entity)
                .expr_as(
                    Expr::col((coverage::Entity, coverage::Column::CheckpointId)),
                    Alias::new("id"),
                )
                .columns(
                    [
                        coverage::Column::SourceScope,
                        coverage::Column::SourceVersion,
                    ]
                    .map(|col| (coverage::Entity, col)),
                )
                .column((context::Entity, context::Column::WorkspaceId))
                .join(
                    JoinType::LeftJoin,
                    checkpoint::Entity,
                    Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(Expr::col((
                        coverage::Entity,
                        coverage::Column::CheckpointId,
                    ))),
                )
                .join(
                    JoinType::LeftJoin,
                    context::Entity,
                    Expr::col((context::Entity, context::Column::Owner))
                        .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                )
                .and_where(Expr::col((coverage::Entity, coverage::Column::SourceId)).eq(id.clone()))
                .and_where(
                    Expr::col((coverage::Entity, coverage::Column::SourceScope))
                        .like("checkpoint:%"),
                )
                .and_where(
                    Expr::col((coverage::Entity, coverage::Column::CheckpointId)).gt(after.clone()),
                )
                .order_by(
                    (coverage::Entity, coverage::Column::CheckpointId),
                    Order::Asc,
                );
        }
        query.limit(1);
        let page = store.connection.query_all(&query).await?;
        for item in &page {
            let incoming: String = item.try_get("", "id")?;
            ensure!(
                item.try_get::<String>("", "workspace_id")? == self.workspace,
                "incoming checkpoint scope corrupt"
            );
            if *branch == 0 {
                ensure!(
                    item.try_get::<String>("", "owner")? == row.owner
                        && item.try_get::<String>("", "thread_id")? == row.thread_id,
                    "incoming previous owner corrupt"
                );
            } else {
                ensure!(
                    item.try_get::<String>("", "source_scope")?
                        == format!("checkpoint:{}", row.owner)
                        && item.try_get::<String>("", "source_version")? == row.identity_sha256,
                    "incoming foreign checkpoint identity corrupt"
                );
                // Verify selected historical ownership, not current raw lookup.
                // Metadata is bounded by the checkpoint's existing source cap.
                let incoming_topology = compaction::checkpoint_row(&store.connection, &incoming)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("incoming checkpoint missing"))?;
                let source = pioneer_compaction::SourceRef {
                    scope: format!("checkpoint:{}", row.owner),
                    id: id.clone(),
                    version: row.identity_sha256.clone(),
                };
                ensure!(
                    compaction::checkpoint_source_owner(
                        &store.connection,
                        &incoming_topology,
                        &source
                    )
                    .await?
                        == row.thread_id,
                    "incoming foreign checkpoint ownership corrupt"
                );
            }
            *after = incoming.clone();
            children.insert(incoming);
            ensure!(
                children.len() + self.pending.len() + self.active.len() + self.seen.len() <= 65_536,
                "reverse pending graph exceeds bound"
            );
        }
        if page.is_empty() {
            if *branch == 0 {
                *branch = 1;
                after.clear();
            } else {
                let id = id.clone();
                let children = std::mem::take(children);
                self.current = None;
                self.pending.push_back((id, true));
                self.pending
                    .extend(children.into_iter().map(|id| (id, false)));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct TerminalBinding {
    id: String,
    owner: String,
    status: String,
    generation: Option<i64>,
}
fn terminal(status: &str, generation: Option<i64>, state: Option<&str>) -> Result<bool> {
    if !matches!(status, "completed" | "failed" | "cancelled" | "stale") {
        return Ok(false);
    }
    let (Some(generation), Some(json)) = (generation, state) else {
        return Ok(false);
    };
    let state: RunnerState = serde_json::from_str(json)?;
    ensure!(
        i64::try_from(state.generation)? == generation,
        "terminal runner generation corrupt"
    );
    Ok(match status {
        "completed" => matches!(state.phase, RunnerPhase::Applied { .. }),
        "failed" | "cancelled" | "stale" => matches!(state.phase, RunnerPhase::Failed { .. }),
        _ => false,
    })
}
struct ExpiryPreparation {
    header: history::Model,
    operations_after: String,
    operations_done: bool,
    checkpoint_after: String,
    reverse: Option<Reachability>,
    checkpoints_done: bool,
    layout_kind: i64,
    layout: Option<LayoutCheck>,
}
impl ExpiryPreparation {
    fn new(header: history::Model) -> Self {
        Self {
            header,
            operations_after: String::new(),
            operations_done: false,
            checkpoint_after: String::new(),
            reverse: None,
            checkpoints_done: false,
            layout_kind: 0,
            layout: None,
        }
    }
    async fn step(&mut self, store: &CrudStore, accounting: &mut SweepAccounting) -> Result<bool> {
        if self.layout_kind < 2 {
            if let Some(layout) = self.layout.as_mut() {
                layout.step(store).await?;
                if layout.done {
                    self.layout = None;
                    self.layout_kind += 1;
                }
            } else {
                self.layout =
                    Some(LayoutCheck::new(store, &self.header.id, self.layout_kind).await?);
            }
            return Ok(false);
        }
        if !self.operations_done {
            // One bounded saved state per quantum, decoded after reader release.
            let row = store.connection.query_one(&Query::select().from(projection::Entity)
                .column((projection::Entity,projection::Column::OperationId))
                .columns([operation::Column::Owner,operation::Column::Status].map(|c| (operation::Entity,c)))
                .column((runner::Entity,runner::Column::Generation)).column((context::Entity,context::Column::WorkspaceId))
                .expr_as(Expr::cust("CASE WHEN length(CAST(compaction_runner_state.state AS BLOB))<=262144 THEN compaction_runner_state.state END"),Alias::new("state"))
                .join(JoinType::LeftJoin,operation::Entity,
                    Expr::col((operation::Entity,operation::Column::Id)).eq(Expr::col((projection::Entity,projection::Column::OperationId))))
                .join(JoinType::LeftJoin,context::Entity,
                    Expr::col((context::Entity,context::Column::Owner)).eq(Expr::col((operation::Entity,operation::Column::Owner))))
                .join(JoinType::LeftJoin,runner::Entity,
                    Expr::col((runner::Entity,runner::Column::OperationId)).eq(Expr::col((operation::Entity,operation::Column::Id))))
                .and_where(Expr::col((projection::Entity,projection::Column::ManifestId)).eq(&self.header.id))
                .and_where(Expr::col((projection::Entity,projection::Column::OperationId)).gt(&self.operations_after))
                .order_by((projection::Entity,projection::Column::OperationId),Order::Asc).limit(1).to_owned()).await?;
            if let Some(row) = row {
                ensure!(
                    row.try_get::<String>("", "workspace_id")? == self.header.workspace_id,
                    "origin operation scope corrupt"
                );
                let id: String = row.try_get("", "operation_id")?;
                let owner: String = row.try_get("", "owner")?;
                let status: String = row.try_get("", "status")?;
                let generation: Option<i64> = row.try_get("", "generation")?;
                let state: Option<String> = row.try_get("", "state")?;
                if !terminal(&status, generation, state.as_deref())? {
                    accounting.held += 1;
                    return Ok(true);
                }
                self.operations_after = id.clone();
                let binding = TerminalBinding {
                    id,
                    owner,
                    status,
                    generation,
                };
                store
                    .run_serialized_write(|| async {
                        let tx = store.connection.begin().await?;
                        let current = Query::select()
                            .expr(Expr::val(1))
                            .from(operation::Entity)
                            .join(
                                JoinType::InnerJoin,
                                runner::Entity,
                                Expr::col((runner::Entity, runner::Column::OperationId))
                                    .eq(Expr::col((operation::Entity, operation::Column::Id))),
                            )
                            .join(
                                JoinType::InnerJoin,
                                context::Entity,
                                Expr::col((context::Entity, context::Column::Owner))
                                    .eq(Expr::col((operation::Entity, operation::Column::Owner))),
                            )
                            .and_where(
                                Expr::col((operation::Entity, operation::Column::Id))
                                    .eq(&binding.id),
                            )
                            .and_where(
                                Expr::col((operation::Entity, operation::Column::Owner))
                                    .eq(&binding.owner),
                            )
                            .and_where(
                                Expr::col((operation::Entity, operation::Column::Status))
                                    .eq(&binding.status),
                            )
                            .and_where(
                                Expr::col((runner::Entity, runner::Column::Generation))
                                    .eq(binding.generation),
                            )
                            .and_where(
                                Expr::col((context::Entity, context::Column::WorkspaceId))
                                    .eq(&self.header.workspace_id),
                            )
                            .to_owned();
                        projection::Entity::update_many()
                            .col_expr(
                                projection::Column::TerminalGeneration,
                                Expr::val(binding.generation),
                            )
                            .filter(projection::Column::OperationId.eq(&binding.id))
                            .filter(projection::Column::ManifestId.eq(&self.header.id))
                            .filter(Expr::exists(current))
                            .exec(&tx)
                            .await?;
                        tx.commit().await?;
                        Ok(())
                    })
                    .await?;
                return Ok(false);
            }
            self.operations_done = true;
            return Ok(false);
        }
        if let Some(reverse) = self.reverse.as_mut() {
            reverse.step(store).await?;
            if reverse.done() {
                if reverse.reachable {
                    // A published legacy dependency is a hold, not poison.
                    // End this expiry attempt; the cyclic pass revisits it
                    // after the independent bounded proof preparation seals it.
                    return Ok(true);
                }
                self.reverse = None;
            }
            return Ok(false);
        }
        if !self.checkpoints_done {
            let query = Query::select()
                .column((checkpoint::Entity, checkpoint::Column::Id))
                .from(checkpoint::Entity)
                .join(
                    JoinType::InnerJoin,
                    projection::Entity,
                    Expr::col((projection::Entity, projection::Column::OperationId)).eq(Expr::col(
                        (checkpoint::Entity, checkpoint::Column::OperationId),
                    )),
                )
                .and_where(
                    Expr::col((projection::Entity, projection::Column::ManifestId))
                        .eq(&self.header.id),
                )
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Id))
                        .gt(&self.checkpoint_after),
                )
                .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::ProofVersion)).ne(1))
                .order_by((checkpoint::Entity, checkpoint::Column::Id), Order::Asc)
                .limit(1)
                .to_owned();
            let row = store.connection.query_one(&query).await?;
            if let Some(row) = row {
                let id: String = row.try_get("", "id")?;
                self.checkpoint_after = id.clone();
                self.reverse = Some(Reachability::new(&id, &self.header.workspace_id));
                return Ok(false);
            }
            self.checkpoints_done = true;
            return Ok(false);
        }
        if expire(store, &self.header).await? {
            accounting.expired += 1;
        } else {
            accounting.held += 1;
        }
        Ok(true)
    }
}

// Indexed roots only: JSON is authoritative at cutover writers and sentinel CAS,
// never parsed in expiry/sweep. A missing Task is unknown. Only a completed,
// failed or cancelled Task releases its inputs; blocked remains retaining.
// Releasing inputs do not retain even an inconsistent old snapshot scope;
// unresolved checkpoint ownership is a separate prerequisite. Retaining
// unknown Task inputs block both known workspace interpretations.
fn unknown_predicate(alias: &str) -> String {
    format!(
        r#"
EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN task t ON t.id=s.task_id WHERE s.frozen_manifest_id='' AND (s.workspace_id={alias}.workspace_id OR t.workspace_id={alias}.workspace_id OR s.workspace_id='' OR t.workspace_id='') AND (t.id IS NULL OR t.status NOT IN ('completed','failed','cancelled')))
OR EXISTS(SELECT 1 FROM turn_runtime_snapshot s WHERE s.frozen_manifest_id='' AND (s.workspace_id={alias}.workspace_id OR s.workspace_id=''))
OR EXISTS(SELECT 1 FROM thread_cli_runtime_binding s WHERE s.frozen_manifest_id='' AND (s.workspace_id={alias}.workspace_id OR s.workspace_id=''))
OR EXISTS(SELECT 1 FROM turn_cli_runtime_binding s WHERE s.frozen_manifest_id='' AND (s.workspace_id={alias}.workspace_id OR s.workspace_id=''))
OR EXISTS(SELECT 1 FROM compaction_context x JOIN compaction_operation o ON o.owner=x.owner JOIN compaction_runner_plan p ON p.operation_id=o.id LEFT JOIN compaction_operation_projection b ON b.operation_id=o.id WHERE x.workspace_id={alias}.workspace_id AND b.operation_id IS NULL AND NOT EXISTS(SELECT 1 FROM compaction_checkpoint c WHERE c.operation_id=o.id AND c.proof_version=1))
OR EXISTS(SELECT 1 FROM compaction_context x JOIN compaction_checkpoint c ON c.owner=x.owner LEFT JOIN compaction_operation_projection b ON b.operation_id=c.operation_id WHERE x.workspace_id={alias}.workspace_id AND c.proof_version=0 AND b.operation_id IS NULL)
"#
    )
}
fn root_predicate(alias: &str) -> String {
    format!(
        r#"{}
OR EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN task t ON t.id=s.task_id WHERE s.frozen_manifest_id={alias}.id AND (t.id IS NULL OR t.status NOT IN ('completed','failed','cancelled')))
OR EXISTS(SELECT 1 FROM turn_runtime_snapshot s WHERE s.frozen_manifest_id={alias}.id)
OR EXISTS(SELECT 1 FROM thread_cli_runtime_binding s WHERE s.frozen_manifest_id={alias}.id)
OR EXISTS(SELECT 1 FROM turn_cli_runtime_binding s WHERE s.frozen_manifest_id={alias}.id)
OR EXISTS(SELECT 1 FROM compaction_task_output s WHERE s.manifest_id={alias}.id)
"#,
        unknown_predicate(alias)
    )
}

fn healthy(alias: &str) -> String {
    format!(
        "{alias}.ready IN (0,1) AND {alias}.expired IN (0,1) AND {alias}.message_count>=0 AND {alias}.import_count>=0 AND {alias}.next_ordinal BETWEEN 0 AND {alias}.message_count AND {alias}.next_import BETWEEN 0 AND {alias}.import_count AND ({alias}.ready=0 OR ({alias}.next_ordinal={alias}.message_count AND {alias}.next_import={alias}.import_count))"
    )
}
async fn expire(store: &CrudStore, header: &history::Model) -> Result<bool> {
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            let h = history::Entity::find_by_id(&header.id)
                .one(&tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("expiry header missing"))?;
            crate::frozen_lifetime::logical_bounds(&h)?;
            ensure!(h.ready == 1 && h == *header, "expiry header changed");
            if store
                .frozen_readers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&h.id)
            {
                tx.commit().await?;
                return Ok(false);
            }
            let guard = tx
                .query_one(
                    &Query::select()
                        .expr(Expr::val(1))
                        .from_as(history::Entity, Alias::new("h"))
                        .and_where(Expr::col((Alias::new("h"), history::Column::Id)).eq(&h.id))
                        .and_where(Expr::cust(root_predicate("h")))
                        .limit(1)
                        .to_owned(),
                )
                .await?;
            if guard.is_some() {
                tx.commit().await?;
                return Ok(false);
            }
            // The per-binding terminal generation was verified in bounded steps.
            // All state/status writes invalidate it before becoming visible.
            let active = tx
                .query_one(
                    &Query::select()
                        .expr(Expr::val(1))
                        .from(projection::Entity)
                        .join(
                            JoinType::LeftJoin,
                            operation::Entity,
                            Expr::col((operation::Entity, operation::Column::Id)).eq(Expr::col((
                                projection::Entity,
                                projection::Column::OperationId,
                            ))),
                        )
                        .join(
                            JoinType::LeftJoin,
                            runner::Entity,
                            Expr::col((runner::Entity, runner::Column::OperationId))
                                .eq(Expr::col((operation::Entity, operation::Column::Id))),
                        )
                        .join(
                            JoinType::LeftJoin,
                            context::Entity,
                            Expr::col((context::Entity, context::Column::Owner))
                                .eq(Expr::col((operation::Entity, operation::Column::Owner))),
                        )
                        .and_where(
                            Expr::col((projection::Entity, projection::Column::ManifestId))
                                .eq(&h.id),
                        )
                        .cond_where(
                            Condition::any()
                                .add(
                                    Expr::col((operation::Entity, operation::Column::Id)).is_null(),
                                )
                                .add(
                                    Expr::col((operation::Entity, operation::Column::Status))
                                        .is_not_in(["completed", "failed", "cancelled", "stale"]),
                                )
                                .add(
                                    Expr::col((runner::Entity, runner::Column::OperationId))
                                        .is_null(),
                                )
                                .add(
                                    Expr::col((
                                        projection::Entity,
                                        projection::Column::TerminalGeneration,
                                    ))
                                    .is_null(),
                                )
                                .add(
                                    Expr::col((
                                        projection::Entity,
                                        projection::Column::TerminalGeneration,
                                    ))
                                    .ne(Expr::col((runner::Entity, runner::Column::Generation))),
                                )
                                .add(
                                    Expr::col((context::Entity, context::Column::WorkspaceId))
                                        .is_null(),
                                )
                                .add(
                                    Expr::col((context::Entity, context::Column::WorkspaceId))
                                        .ne(&h.workspace_id),
                                ),
                        )
                        .limit(1)
                        .to_owned(),
                )
                .await?;
            if active.is_some() {
                tx.commit().await?;
                return Ok(false);
            }
            let unsealed_public = tx
                .query_one(
                    &Query::select()
                        .expr(Expr::val(1))
                        .from(projection::Entity)
                        .join(
                            JoinType::InnerJoin,
                            checkpoint::Entity,
                            Expr::col((checkpoint::Entity, checkpoint::Column::OperationId)).eq(
                                Expr::col((projection::Entity, projection::Column::OperationId)),
                            ),
                        )
                        .join(
                            JoinType::InnerJoin,
                            operation::Entity,
                            Expr::col((operation::Entity, operation::Column::Id)).eq(Expr::col((
                                projection::Entity,
                                projection::Column::OperationId,
                            ))),
                        )
                        .and_where(
                            Expr::col((projection::Entity, projection::Column::ManifestId))
                                .eq(&h.id),
                        )
                        .and_where(
                            Expr::col((checkpoint::Entity, checkpoint::Column::ProofVersion)).ne(1),
                        )
                        .and_where(
                            Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                                .eq("applied")
                                .or(Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                                    .eq("retained")
                                    .and(
                                        Expr::col((operation::Entity, operation::Column::Status))
                                            .eq("completed"),
                                    )),
                        )
                        .limit(1)
                        .to_owned(),
                )
                .await?;
            if unsealed_public.is_some() {
                tx.commit().await?;
                return Ok(false);
            }
            let transitions = layout::Entity::find()
                .select_only()
                .column(layout::Column::ManifestId)
                .filter(
                    Condition::any()
                        .add(layout::Column::ManifestId.eq(&h.id))
                        .add(layout::Column::Candidate.eq(&h.id)),
                )
                .filter(
                    Condition::any()
                        .add(layout::Column::Active.ne(1))
                        .add(layout::Column::Pending.ne(0))
                        .add(layout::Column::Failed.ne(0))
                        .add(Expr::col(layout::Column::CopyNext).lt(Func::coalesce([
                            Expr::col(layout::Column::CopyTo),
                            Expr::val(0),
                        ]))),
                )
                .into_tuple::<String>()
                .one(&tx)
                .await?;
            if transitions.is_some() {
                tx.commit().await?;
                return Ok(false);
            }
            let expired = history::Entity::update_many()
                .col_expr(history::Column::Expired, Expr::val(1))
                .filter(history::Column::Id.eq(&h.id))
                .filter(history::Column::Expired.eq(0))
                .filter(history::Column::Ready.eq(1))
                .filter(
                    Expr::col(history::Column::NextOrdinal)
                        .eq(Expr::col(history::Column::MessageCount)),
                )
                .filter(
                    Expr::col(history::Column::NextImport)
                        .eq(Expr::col(history::Column::ImportCount)),
                )
                .exec(&tx)
                .await?;
            tx.commit().await?;
            Ok(expired.rows_affected > 0)
        })
        .await
}

/// Metadata-only geometry walk; no frozen payload is read. Complete headers
/// and published spans remain immutable through expiry. Incomplete mutations
/// fail closed against the saved bounds; final DELETE rechecks current ranges.
/// Artificial sweep pins would repeatedly prevent logical expiry.
struct LayoutCheck {
    scan: crate::frozen_lifetime::LayoutScan,
    done: bool,
}
impl LayoutCheck {
    async fn new(store: &CrudStore, id: &str, kind: i64) -> Result<Self> {
        let header = history::Entity::find_by_id(id)
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("physical backing scope missing"))?;
        crate::frozen_lifetime::header_bounds(&header)?;
        let scan = crate::frozen_lifetime::LayoutScan::new(header, kind, kind + 1)?;
        Ok(Self { scan, done: false })
    }
    async fn step(&mut self, store: &CrudStore) -> Result<()> {
        self.scan.step(store).await?;
        self.done = self.scan.done();
        Ok(())
    }
}
fn counter_health(h: &history::Model) -> bool {
    crate::frozen_lifetime::header_bounds(h).is_ok()
}
const SWEEP_ROWS: u64 = 256;
const SWEEP_BYTES: i64 = 1024 * 1024;
const ACTIVE_CONTAINERS: usize = 4;

#[derive(Default)]
struct SweepAccounting {
    rows: [u64; 2],
    bytes: [u64; 2],
    refused: u64,
    held: u64,
    zero: u64,
    poison: u64,
    expired: u64,
    proofs: u64,
}

#[derive(Default)]
pub(super) struct PhysicalDeleteResult {
    rows: u64,
    bytes: u64,
    refused: bool,
}
impl PhysicalDeleteResult {
    pub(super) fn refused() -> Self {
        Self {
            refused: true,
            ..Self::default()
        }
    }
    pub(super) fn from_rows(rows: &[sea_orm::QueryResult]) -> Result<Self> {
        let mut bytes = 0;
        for row in rows {
            bytes += row.try_get::<i64>("", "bytes")? as u64;
        }
        Ok(Self {
            rows: rows.len() as u64,
            bytes,
            refused: false,
        })
    }
}
impl SweepAccounting {
    async fn acknowledge(&mut self, kind: i64, result: PhysicalDeleteResult) {
        if result.refused {
            self.refused += 1;
            return;
        }
        self.rows[kind as usize] += result.rows;
        self.bytes[kind as usize] += result.bytes;
        self.zero += u64::from(result.rows == 0);
        if result.rows > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}

// A checked geometry token, local to this visit, not a global/durable revision.
// Active ready spans are immutable. Incomplete appends atomically change the
// header counters; activation changes the layout. Compare both before DELETE.
struct CheckedGeometry {
    h: history::Model,
    l: Option<layout::Model>,
}
struct SweepPreparation {
    manifest: String,
    kind: i64,
    until: i64,
    next: i64,
    checks: VecDeque<String>,
    checked: BTreeMap<String, CheckedGeometry>,
    current: Option<(LayoutCheck, CheckedGeometry)>,
    after: (i64, String),
    reverse_until: Option<(i64, String)>,
    span_until: Option<i64>,
    reverse_started: bool,
    discovered: bool,
    reverse: Vec<(i64, i64, String)>,
    free: VecDeque<(i64, i64)>,
    prepared: Option<String>,
}
impl SweepPreparation {
    fn new(manifest: String, kind: i64, until: i64) -> Self {
        Self {
            checks: VecDeque::from([manifest.clone()]),
            manifest,
            kind,
            until,
            next: 0,
            checked: BTreeMap::new(),
            current: None,
            after: (i64::MIN, String::new()),
            reverse_until: None,
            span_until: None,
            reverse_started: false,
            discovered: false,
            reverse: Vec::new(),
            free: VecDeque::new(),
            prepared: None,
        }
    }
    async fn prepare(&mut self, store: &CrudStore) -> Result<bool> {
        if self.span_until.is_none() {
            // Span writers insert/extend metadata; they never delete/reinsert
            // it. Freeze the rowid insertion frontier as well as the logical
            // keyset so consumers arriving behind the cursor cannot extend a
            // visit indefinitely. This is an indexed single-row seek.
            let row = store
                .connection
                .query_one_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    "SELECT COALESCE(MAX(rowid),0) AS span_until FROM compaction_frozen_span",
                ))
                .await?
                .ok_or_else(|| anyhow::anyhow!("span insertion frontier missing"))?;
            self.span_until = Some(row.try_get("", "span_until")?);
            return Ok(false);
        }
        if let Some((check, _)) = self.current.as_mut() {
            check.step(store).await?;
            if check.done {
                let (_, geometry) = self.current.take().expect("checked geometry");
                self.checked.insert(geometry.h.id.clone(), geometry);
            }
            return Ok(false);
        }
        if let Some(id) = self.checks.front().cloned() {
            if !self.checked.contains_key(&id) {
                let h = history::Entity::find_by_id(&id)
                    .one(&store.connection)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("physical backing scope missing"))?;
                let l = layout::Entity::find_by_id((id, self.kind))
                    .one(&store.connection)
                    .await?;
                let scan =
                    crate::frozen_lifetime::LayoutScan::new(h.clone(), self.kind, self.kind + 1)?;
                self.current = Some((LayoutCheck { scan, done: false }, CheckedGeometry { h, l }));
            }
            self.checks.pop_front();
            return Ok(false);
        }
        if !self.reverse_started {
            self.reverse_until = span::Entity::find()
                .select_only()
                .columns([span::Column::Start, span::Column::ManifestId])
                .filter(span::Column::SourceManifest.eq(&self.manifest))
                .filter(span::Column::Kind.eq(self.kind))
                .filter(Expr::cust_with_values(
                    "rowid<=?",
                    [self.span_until.expect("span frontier")],
                ))
                .order_by_desc(span::Column::Start)
                .order_by_desc(span::Column::ManifestId)
                .into_tuple::<(i64, String)>()
                .one(&store.connection)
                .await?;
            self.reverse_started = true;
            self.discovered = self.reverse_until.is_none();
            return Ok(false);
        }
        if !self.discovered {
            let upper = self.reverse_until.as_ref().expect("reverse visit bound");
            let page = span::Entity::find()
                .filter(span::Column::SourceManifest.eq(&self.manifest))
                .filter(span::Column::Kind.eq(self.kind))
                .filter(Expr::cust_with_values(
                    "rowid<=?",
                    [self.span_until.expect("span frontier")],
                ))
                .filter(
                    Expr::tuple([
                        Expr::col(span::Column::Start),
                        Expr::col(span::Column::ManifestId),
                    ])
                    .gt(Expr::tuple([
                        Expr::val(self.after.0),
                        Expr::val(&self.after.1),
                    ])),
                )
                .filter(
                    Expr::tuple([
                        Expr::col(span::Column::Start),
                        Expr::col(span::Column::ManifestId),
                    ])
                    .lte(Expr::tuple([Expr::val(upper.0), Expr::val(&upper.1)])),
                )
                .order_by_asc(span::Column::Start)
                .order_by_asc(span::Column::ManifestId)
                .limit(META_ROWS)
                .all(&store.connection)
                .await?;
            for s in &page {
                ensure!(s.start >= 0 && s.end > s.start, "invalid reverse span");
                self.after = (s.start, s.manifest_id.clone());
                self.checks.push_back(s.manifest_id.clone());
                self.reverse.push((s.start, s.end, s.manifest_id.clone()));
            }
            self.discovered = page.len() < META_ROWS as usize || &self.after == upper;
            return Ok(false);
        }
        // Build the complement once, after every metadata resource is released.
        let mut held = Vec::new();
        let own = &self.checked[&self.manifest];
        if own.h.expired == 0 && own.l.as_ref().is_none_or(|l| l.active != 1) {
            held.push((0, kind_bound(&own.h, self.kind)));
        }
        for (start, end, id) in &self.reverse {
            let g = &self.checked[id];
            if g.h.expired == 0 && g.l.as_ref().is_some_and(|l| l.active == 1) {
                held.push((*start, std::cmp::min(*end, kind_bound(&g.h, self.kind))));
            }
        }
        held.sort_unstable();
        let mut next = 0;
        for (start, end) in held {
            if end <= next {
                continue;
            }
            if start > next {
                self.free
                    .push_back((next, std::cmp::min(start, self.until.saturating_add(1))));
            }
            next = std::cmp::max(next, end);
            if next > self.until {
                break;
            }
        }
        if next <= self.until {
            self.free.push_back((next, self.until.saturating_add(1)));
        }
        self.prepared = Some(geometry_values(self.checked.values()));
        self.checked.clear();
        self.reverse = Vec::new();
        Ok(true)
    }
    async fn step(&mut self, store: &CrudStore, accounting: &mut SweepAccounting) -> Result<bool> {
        if self.prepared.is_none() {
            if self.prepare(store).await? && self.free.is_empty() {
                accounting.held += 1;
                return Ok(true);
            }
            return Ok(false);
        }
        let Some(&(start, end)) = self.free.front() else {
            return Ok(true);
        };
        let (data, payload) = physical_table(self.kind);
        let import_key = if self.kind == 1 {
            "message_ordinal"
        } else {
            "0"
        };
        let page = store.connection.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            format!("SELECT ordinal, bytes, length(CAST({payload} AS BLOB)) AS actual, {import_key} AS target FROM {data} WHERE manifest_id=?1 AND ordinal>=?2 AND ordinal<?3 ORDER BY ordinal LIMIT ?4"),
            [self.manifest.clone().into(), std::cmp::max(self.next, start).into(), end.into(), SWEEP_ROWS.into()])).await?;
        let mut keys = Vec::with_capacity(page.len());
        let mut total = 0;
        let mut next = std::cmp::max(self.next, start);
        for row in &page {
            let ordinal: i64 = row.try_get("", "ordinal")?;
            let bytes: i64 = row.try_get("", "bytes")?;
            let actual: i64 = row.try_get("", "actual")?;
            if ordinal < 0
                || !(0..=SWEEP_BYTES).contains(&bytes)
                || bytes != actual
                || row.try_get::<i64>("", "target")? < 0
            {
                accounting.poison += 1;
                next = ordinal.saturating_add(1);
                continue;
            }
            if total + actual > SWEEP_BYTES {
                break;
            }
            total += actual;
            keys.push((ordinal, bytes));
            next = ordinal.saturating_add(1);
        }
        if page.is_empty() {
            self.free.pop_front();
            self.next = end;
            return Ok(self.free.is_empty());
        }
        if !keys.is_empty() {
            // Build SQL/parameters before opening the transaction.
            let guard = sweep_guard(
                &self.manifest,
                self.kind,
                self.prepared.as_deref().expect("prepared"),
                keys[0].0,
                keys.last().expect("keys").0,
            );
            let delete = physical_delete(&self.manifest, self.kind, &keys)?;
            let committed = store
                .run_serialized_write(|| async {
                    let guard = guard.clone();
                    let delete = delete.clone();
                    let tx = store.connection.begin().await?;
                    if tx.query_one_raw(guard).await?.is_none() {
                        tx.commit().await?;
                        return Ok(None);
                    }
                    let deleted = tx.query_all_raw(delete).await?;
                    let result = PhysicalDeleteResult::from_rows(&deleted)?;
                    tx.commit().await?;
                    Ok(Some(result))
                })
                .await?;
            let Some(deleted) = committed else {
                accounting
                    .acknowledge(self.kind, PhysicalDeleteResult::refused())
                    .await;
                // Another container gets its turn; the next epoch re-prepares.
                return Ok(true);
            };
            accounting.acknowledge(self.kind, deleted).await;
        }
        self.next = next;
        if self.next >= end {
            self.free.pop_front();
        }
        Ok(self.free.is_empty())
    }
}
fn kind_bound(h: &history::Model, kind: i64) -> i64 {
    match (kind, h.ready) {
        (0, 1) => h.message_count,
        (0, _) => h.next_ordinal,
        (_, 1) => h.import_count,
        _ => h.next_import,
    }
}
fn physical_table(kind: i64) -> (&'static str, &'static str) {
    if kind == 0 {
        ("compaction_frozen_message_data", "reference_json")
    } else {
        ("compaction_frozen_import_data", "proof_json")
    }
}

const GEOMETRY_HEADER_FIELDS: [&str; 12] = [
    "id",
    "workspace_id",
    "owner_thread",
    "identity_sha256",
    "imports_sha256",
    "message_count",
    "import_count",
    "next_ordinal",
    "next_import",
    "ready",
    "expired",
    "storage_registered",
];
const GEOMETRY_LAYOUT_FIELDS: [&str; 10] = [
    "manifest_id",
    "kind",
    "active",
    "pending",
    "candidate",
    "compared",
    "copy_to",
    "copy_next",
    "cleanup_to",
    "failed",
];

// Prepare a VALUES relation once, using the SQL builder, outside DB capacity.
// Literal metadata avoids both SQLite's variable limit for large consumer sets
// and reparsing/serializing a full JSON inventory under the writer. Text is a
// hex literal so even unusual database identifiers remain data, including NUL.
fn geometry_values<'a>(geometries: impl IntoIterator<Item = &'a CheckedGeometry>) -> String {
    let columns = GEOMETRY_HEADER_FIELDS
        .iter()
        .map(|f| format!("h_{f}"))
        .chain(GEOMETRY_LAYOUT_FIELDS.iter().map(|f| format!("l_{f}")))
        .collect::<Vec<_>>();
    let mut query = Query::insert();
    query
        .into_table(Alias::new("checked"))
        .columns(columns.iter().map(Alias::new));
    for g in geometries {
        let h = &g.h;
        let mut values: Vec<sea_orm::Value> = vec![
            h.id.clone().into(),
            h.workspace_id.clone().into(),
            h.owner_thread.clone().into(),
            h.identity_sha256.clone().into(),
            h.imports_sha256.clone().into(),
            h.message_count.into(),
            h.import_count.into(),
            h.next_ordinal.into(),
            h.next_import.into(),
            h.ready.into(),
            h.expired.into(),
            h.storage_registered.into(),
        ];
        if let Some(l) = &g.l {
            values.extend([
                l.manifest_id.clone().into(),
                l.kind.into(),
                l.active.into(),
                l.pending.into(),
                l.candidate.clone().into(),
                l.compared.into(),
                l.copy_to.into(),
                l.copy_next.into(),
                l.cleanup_to.into(),
                l.failed.into(),
            ]);
        } else {
            values.extend(std::iter::repeat_n(
                sea_orm::Value::Int(None),
                GEOMETRY_LAYOUT_FIELDS.len(),
            ));
        }
        query.values_panic(values.into_iter().map(|value| match value {
            sea_orm::Value::String(Some(text)) => {
                Expr::cust(format!("CAST(X'{}' AS TEXT)", hex::encode(text.as_bytes())))
            }
            value => Expr::val(value),
        }));
    }
    let sql = query.to_string(sea_orm::sea_query::SqliteQueryBuilder);
    let (_, values) = sql.split_once("VALUES ").expect("geometry VALUES");
    format!(
        "WITH checked({}) AS MATERIALIZED (VALUES {values})",
        columns.join(",")
    )
}

// One common guard for the whole selected free interval, never a predicate
// repeated for each physical row. Missing FK parents explicitly fail closed.
fn sweep_guard(manifest: &str, kind: i64, prepared: &str, first: i64, last: i64) -> Statement {
    let h_fields = GEOMETRY_HEADER_FIELDS;
    let l_fields = GEOMETRY_LAYOUT_FIELDS;
    let same_h = h_fields
        .iter()
        .map(|f| format!("h.{f} IS e.h_{f}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Cleanup cursor movement cannot change published geometry. Validate its
    // current bounds without invalidating a continuing visit on every advance.
    let same_l = l_fields
        .iter()
        .map(|f| format!("l.{f} IS e.l_{f}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let health = healthy("h");
    let unknown = unknown_predicate("p");
    let hk = if kind == 0 {
        "message_count"
    } else {
        "import_count"
    };
    let nk = if kind == 0 {
        "next_ordinal"
    } else {
        "next_import"
    };
    Statement::from_sql_and_values(
        DbBackend::Sqlite,
        format!(
            r#"
{prepared}
SELECT 1 WHERE
NOT EXISTS(SELECT 1 FROM checked e LEFT JOIN compaction_frozen_history h ON h.id=e.h_id LEFT JOIN compaction_frozen_layout l ON l.manifest_id=e.h_id AND l.kind=?2 WHERE h.id IS NULL OR NOT ({same_h}) OR (l.manifest_id IS NOT NULL AND (l.cleanup_next<0 OR l.cleanup_next>l.cleanup_to)) OR NOT ((e.l_manifest_id IS NULL AND l.manifest_id IS NULL) OR (e.l_manifest_id IS NOT NULL AND l.manifest_id IS NOT NULL AND {same_l})))
AND EXISTS(SELECT 1 FROM compaction_frozen_history p WHERE p.id=?1)
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history p WHERE p.id=?1 AND ({unknown}))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=?1 AND (NOT ({health}) OR (h.expired=0 AND ?3<CASE h.ready WHEN 1 THEN h.{hk} ELSE h.{nk} END AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=?2 AND l.active=1))))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s LEFT JOIN checked e ON e.h_id=s.manifest_id LEFT JOIN compaction_frozen_history h ON h.id=s.manifest_id LEFT JOIN compaction_frozen_layout l ON l.manifest_id=s.manifest_id AND l.kind=s.kind WHERE s.source_manifest=?1 AND s.kind=?2 AND (e.h_id IS NULL OR h.id IS NULL OR NOT ({health}) OR l.manifest_id IS NULL OR l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0) OR (h.storage_registered<>0 AND l.manifest_id IS NULL) OR (h.expired=0 AND s.start<=?4 AND ?3<s.end AND ?3<CASE h.ready WHEN 1 THEN h.{hk} ELSE h.{nk} END)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.kind=?2 AND (l.manifest_id=?1 OR l.candidate=?1) AND (l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=?1 AND h.storage_registered<>0 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=?2))
"#
        ),
        [manifest.into(), kind.into(), first.into(), last.into()],
    )
}

// Repository-only SQLite bulk physical deletion. Its caller supplies the
// container guard in the same serialized write transaction.
fn physical_delete(manifest: &str, kind: i64, keys: &[(i64, i64)]) -> Result<Statement> {
    ensure!(
        matches!(kind, 0 | 1) && !keys.is_empty() && keys.len() <= SWEEP_ROWS as usize,
        "invalid physical batch"
    );
    let mut bytes = 0;
    let mut values: Vec<sea_orm::Value> = vec![manifest.into()];
    let mut tuples = Vec::with_capacity(keys.len());
    for &(ordinal, size) in keys {
        ensure!(
            ordinal >= 0 && (0..=SWEEP_BYTES).contains(&size),
            "invalid physical key"
        );
        bytes += size;
        tuples.push(format!("(?{},?{})", values.len() + 1, values.len() + 2));
        values.extend([ordinal.into(), size.into()]);
    }
    ensure!(bytes <= SWEEP_BYTES, "physical batch byte limit exceeded");
    let (data, payload) = physical_table(kind);
    let import_key = if kind == 1 {
        "AND d.message_ordinal>=0"
    } else {
        ""
    };
    Ok(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        format!(
            "WITH selected(ordinal,bytes) AS (VALUES {}) DELETE FROM {data} AS d WHERE d.manifest_id=?1 AND d.ordinal IN (SELECT ordinal FROM selected) AND EXISTS(SELECT 1 FROM selected k WHERE k.ordinal=d.ordinal AND k.bytes=d.bytes) AND length(CAST(d.{payload} AS BLOB))=d.bytes {import_key} RETURNING bytes",
            tuples.join(",")
        ),
        values,
    ))
}

pub(super) fn duplicate_cleanup_statements(
    h: &history::Model,
    l: &layout::Model,
    keys: &[(i64, i64)],
) -> Result<Option<(Statement, Statement)>> {
    if keys.is_empty() {
        return Ok(None);
    }
    let prepared = geometry_values(&[CheckedGeometry {
        h: h.clone(),
        l: Some(l.clone()),
    }]);
    Ok(Some((
        sweep_guard(
            &h.id,
            l.kind,
            &prepared,
            keys[0].0,
            keys.last().expect("keys").0,
        ),
        physical_delete(&h.id, l.kind, keys)?,
    )))
}

#[cfg(test)]
struct PhysicalKey {
    manifest: String,
    kind: i64,
    ordinal: i64,
    bytes: i64,
}
#[cfg(test)]
async fn delete_key(store: &CrudStore, key: &PhysicalKey) -> Result<()> {
    let tx = store.connection.begin().await?;
    delete_free_key(&tx, &key.manifest, key.kind, key.ordinal, key.bytes).await?;
    tx.commit().await?;
    Ok(())
}
#[cfg(test)]
async fn delete_free_key<C: ConnectionTrait>(
    db: &C,
    manifest: &str,
    kind: i64,
    ordinal: i64,
    bytes: i64,
) -> Result<u64> {
    ensure!(
        matches!(kind, 0 | 1) && ordinal >= 0 && (0..=1024 * 1024).contains(&bytes),
        "invalid physical key cannot be swept"
    );
    let data = if kind == 0 {
        "compaction_frozen_message_data"
    } else {
        "compaction_frozen_import_data"
    };
    let payload = if kind == 0 {
        "reference_json"
    } else {
        "proof_json"
    };
    let import_key = if kind == 1 {
        "AND d.message_ordinal>=0"
    } else {
        ""
    };
    let hk = if kind == 0 {
        "message_count"
    } else {
        "import_count"
    };
    let nk = if kind == 0 {
        "next_ordinal"
    } else {
        "next_import"
    };
    let health = healthy("h");
    let unknown = unknown_predicate("p");
    // Original exact-key retention oracle for existing focused tests.
    // Production uses the common interval guard plus physical_delete.
    let sql = format!(
        r#"DELETE FROM {data} AS d WHERE d.manifest_id=?1 AND d.ordinal=?2 AND d.bytes=?3
AND length(CAST(d.{payload} AS BLOB))=d.bytes {import_key}
AND EXISTS(SELECT 1 FROM compaction_frozen_history p WHERE p.id=d.manifest_id)
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history p WHERE p.id=d.manifest_id AND ({unknown}))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=d.manifest_id AND (NOT ({health}) OR (h.expired=0 AND d.ordinal<CASE h.ready WHEN 1 THEN h.{hk} ELSE h.{nk} END AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=?4 AND l.active=1))))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s LEFT JOIN compaction_frozen_history h ON h.id=s.manifest_id LEFT JOIN compaction_frozen_layout l ON l.manifest_id=h.id AND l.kind=s.kind WHERE s.source_manifest=d.manifest_id AND s.kind=?4 AND (h.id IS NULL OR NOT ({health}) OR l.manifest_id IS NULL OR l.failed<>0 OR (h.expired=0 AND l.active=1 AND s.start<=d.ordinal AND d.ordinal<s.end AND d.ordinal<CASE h.ready WHEN 1 THEN h.{hk} ELSE h.{nk} END)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=d.manifest_id AND l.kind=?4 AND (l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.candidate=d.manifest_id AND l.kind=?4 AND (l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s JOIN compaction_frozen_layout l ON l.manifest_id=s.manifest_id AND l.kind=s.kind WHERE s.source_manifest=d.manifest_id AND s.kind=?4 AND (l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=d.manifest_id AND h.storage_registered<>0 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=?4))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s JOIN compaction_frozen_history h ON h.id=s.manifest_id WHERE s.source_manifest=d.manifest_id AND s.kind=?4 AND h.storage_registered<>0 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=?4))
"#
    );
    Ok(db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            [manifest.into(), ordinal.into(), bytes.into(), kind.into()],
        ))
        .await?
        .rows_affected())
}

impl FrozenStorageLifetimeProgress {
    /// One eligible bounded phase step; skip at most one rotation of finished
    /// or scope-blocked phases without making the worker sleep for each skip.
    /// False means every discovery family has
    /// completed this pass, not that one phase happened to be empty. Rotating
    /// keysets revisit previously held rows on the next pass.
    pub async fn quantum(&mut self, store: &CrudStore) -> Result<bool> {
        let store = store.with_maintenance_access();
        let mut selected = None;
        for _ in 0..self.pass_done.len() {
            let phase = self.phase;
            self.phase = (phase + 1) % self.pass_done.len();
            if !self.pass_done[phase] && (!matches!(phase, 2 | 3 | 4) || self.scope.done) {
                selected = Some(phase);
                break;
            }
        }
        // Scope itself stays eligible until its checked EOF, so a blocked
        // cleanup phase can never turn into a busy loop or a false global EOF.
        let phase =
            selected.ok_or_else(|| anyhow::anyhow!("lifetime pass has no eligible phase"))?;
        let result = match phase {
            0 => self.locator_quantum(&store).await,
            1 => self.proof_quantum(&store).await,
            2 => self.expiry_quantum(&store).await,
            3 => self.sweep_quantum(&store).await,
            4 => self.discard_quantum(&store).await,
            _ => self.scope.step(&store).await,
        };
        let more = match result {
            Ok(more) => more,
            Err(error) => {
                if error.downcast_ref::<sea_orm::DbErr>().is_some() {
                    // Legacy graph/reachability steps can consume RAM work
                    // before a fallible DB call. Restart only the failed job,
                    // preserving its original identity and dropping old holds.
                    match phase {
                        1 => {
                            if let Some(proof) = self.proof.as_mut() {
                                proof.restart();
                            }
                        }
                        2 => {
                            if let Some(expiry) = self.expiry.as_mut() {
                                *expiry = ExpiryPreparation::new(expiry.header.clone());
                            }
                        }
                        4 => {
                            if let Some(discard) = self.discard.as_mut() {
                                discard.reverse =
                                    Reachability::new(&discard.id, &discard.reverse.workspace);
                            }
                        }
                        _ => {}
                    }
                    return Err(error);
                }
                if phase == 5 {
                    return Err(error);
                }
                self.accounting.refused += 1;
                // Data/authority refusal ends this object's visit. The indexed
                // cursor already advanced; other families continue immediately.
                match phase {
                    0 => self.locator = None,
                    1 => {
                        if self.proof.is_none() {
                            self.proof_pending.pop_front();
                        }
                        self.proof = None;
                    }
                    2 => self.expiry = None,
                    // sweep_quantum has already rotated its active queue.
                    4 => self.discard = None,
                    _ => {}
                }
                true
            }
        };
        if !more {
            self.pass_done[phase] = true;
        }
        if self.pass_done.iter().all(|done| *done) {
            self.pass_done = [false; 6];
            self.locator_done = [false; 4];
            self.physical_done = [false; 2];
            self.physical_started = [false; 2];
            self.physical_after = [None, None];
            self.physical_until = [None, None];
            tracing::debug!(
                message_rows = self.accounting.rows[0],
                message_bytes = self.accounting.bytes[0],
                import_rows = self.accounting.rows[1],
                import_bytes = self.accounting.bytes[1],
                refused = self.accounting.refused,
                held = self.accounting.held,
                zero = self.accounting.zero,
                poison = self.accounting.poison,
                expired_markers = self.accounting.expired,
                proof_preparations = self.accounting.proofs,
                "frozen physical cleanup committed totals"
            );
            // Confirm EOF again after newly verified roots/proofs, rather than
            // sleeping over expiry work that became eligible late in the pass.
            return Ok(std::mem::take(&mut self.pass_changed));
        }
        Ok(true)
    }
    /// Start another discovery epoch for completed cleanup families between
    /// worker batches. An unfinished proof graph must not delay newly free data.
    /// Active container continuations and paged preparations are preserved.
    pub fn revisit_completed_cleanup(&mut self) {
        // Once proof discovery reaches EOF, let every family finish together
        // so a quiet database still reaches the existing idle/backoff state.
        if self.pass_done[1] {
            return;
        }
        for phase in [2, 3, 4] {
            if self.pass_done[phase] {
                self.pass_done[phase] = false;
                if phase == 3 {
                    self.physical_done = [false; 2];
                    self.physical_started = [false; 2];
                    self.physical_after = [None, None];
                    self.physical_until = [None, None];
                }
            }
        }
    }
    pub(super) async fn acknowledge_physical(
        &mut self,
        kind: i64,
        result: Option<PhysicalDeleteResult>,
    ) {
        if let Some(result) = result {
            self.accounting.acknowledge(kind, result).await;
        }
    }
    pub(super) async fn conversion_scope(&mut self, store: &CrudStore) -> Result<bool> {
        if !self.scope.done {
            self.scope.step(store).await?;
            return Ok(false);
        }
        Ok(true)
    }
    async fn locator_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(read) = self.locator.as_mut() {
            if read.step(store).await? {
                self.locator = None;
                self.pass_changed = true;
            }
            return Ok(true);
        }
        let mut selected = None;
        for _ in 0..self.locator_done.len() {
            let kind = self.locator_kind;
            self.locator_kind = (kind + 1) % self.locator_done.len();
            if !self.locator_done[kind] {
                selected = Some(kind);
                break;
            }
        }
        let Some(kind) = selected else {
            return Ok(false);
        };
        let (table, key, field) = ROOTS[kind];
        if self.locator_pending[kind].is_empty() {
            // Page actual PK metadata, not a filtered LIMIT that could walk an
            // unbounded number of already-verified records in one read.
            let query = Query::select().from(Alias::new(table))
                .expr_as(Expr::col(Alias::new(key)), Alias::new("id"))
                .columns([Alias::new("workspace_id"), Alias::new("frozen_manifest_id")])
                .expr_as(Expr::cust(format!("CASE WHEN frozen_manifest_id='' THEN length(CAST({field} AS BLOB)) ELSE 0 END")), Alias::new("size"))
                .and_where(Expr::col(Alias::new(key)).gt(&self.locators[kind]))
                .order_by(Alias::new(key), Order::Asc).limit(META_ROWS).to_owned();
            let page = store.connection.query_all(&query).await?;
            if page.is_empty() {
                self.locators[kind].clear();
                self.locator_done[kind] = true;
                return Ok(!self.locator_done.iter().all(|done| *done));
            }
            for row in page {
                let id: String = row.try_get("", "id")?;
                self.locators[kind] = id.clone();
                if row
                    .try_get::<Option<String>>("", "frozen_manifest_id")?
                    .as_deref()
                    == Some("")
                {
                    self.locator_pending[kind].push_back((
                        id,
                        row.try_get("", "workspace_id")?,
                        row.try_get("", "size")?,
                    ));
                }
            }
            return Ok(true);
        }
        let (id, workspace, size) = self.locator_pending[kind]
            .pop_front()
            .expect("locator page");
        ensure!(
            (0..=LEGACY_LOCATOR_BYTES).contains(&size) && !workspace.is_empty(),
            "legacy locator scope/size unsupported"
        );
        self.locator = Some(LocatorRead {
            kind,
            id,
            workspace,
            size,
            bytes: Vec::new(),
        });
        Ok(true)
    }

    async fn proof_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(proof) = self.proof.as_mut() {
            proof.step(store).await?;
            if proof.finished() {
                self.proof = None;
                self.accounting.proofs += 1;
                self.pass_changed = true;
            }
            return Ok(true);
        }
        // Page headers before choosing public roots; no full filtered scan
        // when all old checkpoints have already been sealed.
        if self.proof_pending.is_empty() {
            let query = Query::select()
                .from(checkpoint::Entity)
                .columns(
                    [
                        checkpoint::Column::Id,
                        checkpoint::Column::ProofVersion,
                        checkpoint::Column::Status,
                    ]
                    .map(|column| (checkpoint::Entity, column)),
                )
                .expr_as(
                    Expr::col((operation::Entity, operation::Column::Status)),
                    Alias::new("operation_status"),
                )
                .expr_as(
                    Expr::col((projection::Entity, projection::Column::OperationId)),
                    Alias::new("bound"),
                )
                .join(
                    JoinType::LeftJoin,
                    operation::Entity,
                    Expr::col((operation::Entity, operation::Column::Id))
                        .eq(Expr::col((
                            checkpoint::Entity,
                            checkpoint::Column::OperationId,
                        )))
                        .and(
                            Expr::col((operation::Entity, operation::Column::Owner))
                                .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                        ),
                )
                .join(
                    JoinType::LeftJoin,
                    projection::Entity,
                    Expr::col((projection::Entity, projection::Column::OperationId)).eq(Expr::col(
                        (checkpoint::Entity, checkpoint::Column::OperationId),
                    )),
                )
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Id)).gt(&self.proof_after),
                )
                .order_by((checkpoint::Entity, checkpoint::Column::Id), Order::Asc)
                .limit(META_ROWS)
                .to_owned();
            let page = store.connection.query_all(&query).await?;
            if page.is_empty() {
                self.proof_after.clear();
                return Ok(false);
            }
            for row in page {
                let id: String = row.try_get("", "id")?;
                self.proof_after = id.clone();
                let version: i64 = row.try_get("", "proof_version")?;
                let status: String = row.try_get("", "status")?;
                let op: Option<String> = row.try_get("", "operation_status")?;
                let no_projection = row.try_get::<Option<String>>("", "bound")?.is_none();
                if version == 0
                    && (status == "applied"
                        || (status == "retained" && op.as_deref() == Some("completed"))
                        || no_projection)
                {
                    self.proof_pending.push_back(id);
                }
            }
            return Ok(true);
        }
        let id = self.proof_pending.front().expect("proof page").clone();
        // The page can be stale: an earlier root traversed this ancestor, or
        // foreground publication sealed it while it waited. Only maintenance
        // dequeue uses this shortcut; readers/publication still validate the
        // entire historical graph. Read scalar metadata, never summary JSON.
        let query = Query::select()
            .from(checkpoint::Entity)
            .column((checkpoint::Entity, checkpoint::Column::ProofVersion))
            .column((checkpoint::Entity, checkpoint::Column::FormatVersion))
            .column((checkpoint::Entity, checkpoint::Column::Status))
            .expr_as(
                Expr::col((operation::Entity, operation::Column::Status)),
                Alias::new("operation_status"),
            )
            .join(
                JoinType::InnerJoin,
                operation::Entity,
                Expr::col((operation::Entity, operation::Column::Id))
                    .eq(Expr::col((
                        checkpoint::Entity,
                        checkpoint::Column::OperationId,
                    )))
                    .and(
                        Expr::col((operation::Entity, operation::Column::Owner))
                            .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                    ),
            )
            .join(
                JoinType::InnerJoin,
                context::Entity,
                Expr::col((context::Entity, context::Column::Owner))
                    .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
            )
            .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(&id))
            .and_where(Expr::col((context::Entity, context::Column::FormatVersion)).eq(1))
            .and_where(Expr::col((context::Entity, context::Column::WorkspaceId)).ne(""))
            .and_where(Expr::col((context::Entity, context::Column::ThreadId)).ne(""))
            .limit(1)
            .to_owned();
        let row = store
            .connection
            .query_one(&query)
            .await?
            .ok_or_else(|| anyhow::anyhow!("queued checkpoint missing or scope corrupt"))?;
        let version: i64 = row.try_get("", "proof_version")?;
        let format: i64 = row.try_get("", "format_version")?;
        ensure!(
            format == 1 && matches!(version, 0 | 1),
            "queued checkpoint format/proofs unsupported"
        );
        let status: String = row.try_get("", "status")?;
        let operation_status: String = row.try_get("", "operation_status")?;
        let public =
            operation_status == "completed" && matches!(status.as_str(), "applied" | "retained");
        if version == 1 && public {
            self.proof_pending.pop_front();
            self.pass_changed = true;
            return Ok(true);
        }
        // The no-projection discovery branch can also select private raw
        // candidates. Preserve their full preparation/availability checks,
        // even if another caller sealed them while the page was queued.
        self.proof = Some(checkpoint_proofs::GraphPreparation::new(&id));
        self.proof_pending.pop_front();
        Ok(true)
    }

    async fn expiry_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(expiry) = self.expiry.as_mut() {
            if expiry.step(store, &mut self.accounting).await? {
                self.expiry = None;
            }
            return Ok(true);
        }
        let row = history::Entity::find()
            .select_only()
            .column(history::Column::Id)
            .filter(history::Column::Expired.eq(0))
            .filter(history::Column::Ready.eq(1))
            .filter(history::Column::Id.gt(&self.expiry_after))
            .order_by_asc(history::Column::Id)
            .into_tuple::<String>()
            .one(&store.connection)
            .await?;
        if let Some(id) = row {
            let header = history::Entity::find_by_id(&id)
                .one(&store.connection)
                .await?;
            self.expiry_after = id;
            let header = header.ok_or_else(|| anyhow::anyhow!("expiry discovery changed"))?;
            ensure!(counter_health(&header), "corrupt expiry counters");
            self.expiry = Some(ExpiryPreparation::new(header));
            Ok(true)
        } else {
            self.expiry_after.clear();
            Ok(false)
        }
    }
    async fn sweep_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        let active = self.sweep_pending.len() + usize::from(self.sweep.is_some());
        if active > 0
            && (!self.sweep_discover
                || active >= ACTIVE_CONTAINERS
                || self.physical_done.iter().all(|done| *done))
        {
            let mut sweep = self
                .sweep
                .take()
                .or_else(|| self.sweep_pending.pop_front())
                .expect("active sweep");
            let result = sweep.step(store, &mut self.accounting).await;
            if matches!(result, Ok(false))
                || result
                    .as_ref()
                    .is_err_and(|error| error.downcast_ref::<sea_orm::DbErr>().is_some())
            {
                self.sweep_pending.push_back(sweep);
            }
            self.sweep = self.sweep_pending.pop_front();
            self.sweep_discover = true;
            result?;
            return Ok(true);
        }
        self.sweep_discover = false;
        let mut selected = None;
        for _ in 0..2 {
            let kind = self.physical_kind;
            self.physical_kind = (kind + 1) % 2;
            if !self.physical_done[kind] {
                selected = Some(kind);
                break;
            }
        }
        let Some(kind) = selected else {
            return Ok(active > 0);
        };
        let (data, _) = physical_table(kind as i64);
        if !self.physical_started[kind] {
            let row = store
                .connection
                .query_one(
                    &Query::select()
                        .from(Alias::new(data))
                        .column(Alias::new("manifest_id"))
                        .order_by(Alias::new("manifest_id"), Order::Desc)
                        .limit(1)
                        .to_owned(),
                )
                .await?;
            self.physical_until[kind] = row
                .map(|r| r.try_get::<String>("", "manifest_id"))
                .transpose()?;
            self.physical_started[kind] = true;
        }
        let Some(upper) = &self.physical_until[kind] else {
            self.physical_done[kind] = true;
            return Ok(active > 0 || !self.physical_done.iter().all(|done| *done));
        };
        let mut query = Query::select();
        query
            .from(Alias::new(data))
            .column(Alias::new("manifest_id"))
            .and_where(Expr::col(Alias::new("manifest_id")).lte(upper))
            .order_by(Alias::new("manifest_id"), Order::Asc)
            .limit(1);
        if let Some(after) = &self.physical_after[kind] {
            query.and_where(Expr::col(Alias::new("manifest_id")).gt(after));
        }
        let row = store.connection.query_one(&query).await?;
        if let Some(row) = row {
            let manifest: String = row.try_get("", "manifest_id")?;
            let last = store
                .connection
                .query_one(
                    &Query::select()
                        .from(Alias::new(data))
                        .column(Alias::new("ordinal"))
                        .and_where(Expr::col(Alias::new("manifest_id")).eq(&manifest))
                        .order_by(Alias::new("ordinal"), Order::Desc)
                        .limit(1)
                        .to_owned(),
                )
                .await?;
            if let Some(last) = last {
                let until: i64 = last.try_get("", "ordinal")?;
                // An unrepresentable half-open end must not block the normal
                // keys preceding that poison row in the same container.
                if until >= 0 {
                    if until == i64::MAX {
                        self.accounting.poison += 1;
                    }
                    let sweep = SweepPreparation::new(
                        manifest.clone(),
                        kind as i64,
                        std::cmp::min(until, i64::MAX - 1),
                    );
                    if self.sweep.is_none() {
                        self.sweep = Some(sweep);
                    } else {
                        self.sweep_pending.push_back(sweep);
                    }
                } else {
                    self.accounting.poison += 1;
                }
            }
            // A DB failure before the bound/visit is ready acknowledges no
            // discovery item. Successful disappearance/poison may advance it.
            self.physical_after[kind] = Some(manifest);
            Ok(true)
        } else {
            self.physical_done[kind] = true;
            Ok(active > 0 || !self.physical_done.iter().all(|done| *done))
        }
    }
    async fn discard_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(discard) = self.discard.as_mut() {
            if !discard.reverse.done() {
                discard.reverse.step(store).await?;
                return Ok(true);
            }
            if discard.reverse.reachable {
                self.discard = None;
                return Ok(true);
            }
            discard.cleanup(store).await?;
            self.discard = None;
            return Ok(true);
        }
        let row = store
            .connection
            .query_one(
                &Query::select()
                    .from(checkpoint::Entity)
                    .column((checkpoint::Entity, checkpoint::Column::Id))
                    .column((operation::Entity, operation::Column::Status))
                    .columns(
                        [runner::Column::Generation, runner::Column::State]
                            .map(|c| (runner::Entity, c)),
                    )
                    .column((projection::Entity, projection::Column::ManifestId))
                    .join(
                        JoinType::InnerJoin,
                        operation::Entity,
                        Expr::col((operation::Entity, operation::Column::Id))
                            .eq(Expr::col((
                                checkpoint::Entity,
                                checkpoint::Column::OperationId,
                            )))
                            .and(
                                Expr::col((operation::Entity, operation::Column::Owner))
                                    .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                            ),
                    )
                    .join(
                        JoinType::InnerJoin,
                        projection::Entity,
                        Expr::col((projection::Entity, projection::Column::OperationId))
                            .eq(Expr::col((operation::Entity, operation::Column::Id))),
                    )
                    .join(
                        JoinType::InnerJoin,
                        history::Entity,
                        Expr::col((history::Entity, history::Column::Id)).eq(Expr::col((
                            projection::Entity,
                            projection::Column::ManifestId,
                        ))),
                    )
                    .join(
                        JoinType::InnerJoin,
                        runner::Entity,
                        Expr::col((runner::Entity, runner::Column::OperationId))
                            .eq(Expr::col((operation::Entity, operation::Column::Id))),
                    )
                    .and_where(
                        Expr::col((checkpoint::Entity, checkpoint::Column::Id))
                            .gt(&self.discard_after),
                    )
                    .and_where(Expr::col((history::Entity, history::Column::Expired)).eq(1))
                    .and_where(
                        Expr::col((operation::Entity, operation::Column::Status)).is_in([
                            "failed",
                            "cancelled",
                            "stale",
                        ]),
                    )
                    .and_where(
                        Expr::col((checkpoint::Entity, checkpoint::Column::Status)).ne("applied"),
                    )
                    .and_where(
                        Expr::cust("length(CAST(compaction_runner_state.state AS BLOB))")
                            .lte(256_i64 * 1024),
                    )
                    .order_by((checkpoint::Entity, checkpoint::Column::Id), Order::Asc)
                    .limit(1)
                    .to_owned(),
            )
            .await?;
        let Some(row) = row else {
            self.discard_after.clear();
            return Ok(false);
        };
        let id: String = row.try_get("", "id")?;
        let manifest: String = row.try_get("", "manifest_id")?;
        let status: String = row.try_get("", "status")?;
        let generation: Option<i64> = row.try_get("", "generation")?;
        let state: String = row.try_get("", "state")?;
        let scope = compaction::checkpoint_row(&store.connection, &id).await?;
        self.discard_after = id.clone();
        ensure!(
            terminal(&status, generation, Some(&state))?,
            "staging discard state unproved"
        );
        let scope = scope.ok_or_else(|| anyhow::anyhow!("discard checkpoint scope missing"))?;
        self.discard = Some(DiscardPreparation {
            reverse: Reachability::new(&id, &scope.workspace_id),
            id,
            manifest,
            status,
            generation,
        });
        Ok(true)
    }
}

struct DiscardPreparation {
    id: String,
    manifest: String,
    status: String,
    generation: Option<i64>,
    reverse: Reachability,
}
impl DiscardPreparation {
    async fn cleanup(&self, store: &CrudStore) -> Result<()> {
        // Sizes first, no payload decoding or large comparisons in the writer.
        // At most three exact keys and 1 MiB of logical row bytes per quantum.
        let mut keys = Vec::new();
        let mut total = 0_i64;
        for (table, size) in [
            (
                "compaction_checkpoint_import",
                "length(CAST(proof_json AS BLOB))+COALESCE(length(CAST(target_checkpoint_json AS BLOB)),0)+length(CAST(target_source_thread AS BLOB))+length(CAST(context_thread AS BLOB))",
            ),
            (
                "compaction_checkpoint_replay_alias",
                "length(CAST(covered_thread AS BLOB))+length(CAST(covered_scope AS BLOB))+length(CAST(covered_id AS BLOB))+length(CAST(covered_version AS BLOB))+length(CAST(replay_thread AS BLOB))+length(CAST(replay_scope AS BLOB))+length(CAST(replay_id AS BLOB))+length(CAST(replay_version AS BLOB))+COALESCE(length(CAST(tool_item_id AS BLOB)),0)",
            ),
            (
                "compaction_checkpoint_event_input",
                "length(CAST(source_thread AS BLOB))+length(CAST(source_scope AS BLOB))+length(CAST(source_id AS BLOB))+length(CAST(source_version AS BLOB))+length(CAST(role AS BLOB))",
            ),
        ] {
            let row = store
                .connection
                .query_one(
                    &Query::select()
                        .from(Alias::new(table))
                        .expr_as(Expr::col(Alias::new("rowid")), Alias::new("key"))
                        .expr_as(Expr::cust(size), Alias::new("bytes"))
                        .and_where(Expr::col(Alias::new("checkpoint_id")).eq(&self.id))
                        .order_by(Alias::new("rowid"), Order::Asc)
                        .limit(1)
                        .to_owned(),
                )
                .await?;
            if let Some(row) = row {
                let bytes: i64 = row.try_get("", "bytes")?;
                ensure!(
                    (0..=1024 * 1024).contains(&bytes),
                    "staging row size unsupported"
                );
                if total + bytes <= 1024 * 1024 {
                    total += bytes;
                    keys.push((table, row.try_get::<i64>("", "key")?, size, bytes));
                }
            }
        }
        store
            .run_serialized_write(|| async {
                let tx = store.connection.begin().await?;
                if store
                    .frozen_readers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains_key(&self.manifest)
                {
                    tx.commit().await?;
                    return Ok(());
                }
                let incoming_previous = checkpoint::Entity::find()
                    .select_only()
                    .column(checkpoint::Column::Id)
                    .filter(checkpoint::Column::Previous.eq(&self.id))
                    .into_query();
                let incoming_foreign = coverage::Entity::find()
                    .select_only()
                    .column(coverage::Column::CheckpointId)
                    .filter(coverage::Column::SourceId.eq(&self.id))
                    .filter(coverage::Column::SourceScope.like("checkpoint:%"))
                    .into_query();
                let mut eligible = Query::select();
                eligible
                    .expr(Expr::val(1))
                    .from(checkpoint::Entity)
                    .join(
                        JoinType::InnerJoin,
                        operation::Entity,
                        Expr::col((operation::Entity, operation::Column::Id)).eq(Expr::col((
                            checkpoint::Entity,
                            checkpoint::Column::OperationId,
                        ))),
                    )
                    .join(
                        JoinType::InnerJoin,
                        runner::Entity,
                        Expr::col((runner::Entity, runner::Column::OperationId))
                            .eq(Expr::col((operation::Entity, operation::Column::Id))),
                    )
                    .join(
                        JoinType::InnerJoin,
                        projection::Entity,
                        Expr::col((projection::Entity, projection::Column::OperationId))
                            .eq(Expr::col((operation::Entity, operation::Column::Id))),
                    )
                    .join(
                        JoinType::InnerJoin,
                        history::Entity,
                        Expr::col((history::Entity, history::Column::Id)).eq(Expr::col((
                            projection::Entity,
                            projection::Column::ManifestId,
                        ))),
                    )
                    .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(&self.id))
                    .and_where(
                        Expr::col((checkpoint::Entity, checkpoint::Column::Status)).ne("applied"),
                    )
                    .and_where(
                        Expr::col((operation::Entity, operation::Column::Status)).eq(&self.status),
                    )
                    .and_where(Expr::col((history::Entity, history::Column::Id)).eq(&self.manifest))
                    .and_where(Expr::col((history::Entity, history::Column::Expired)).eq(1))
                    .and_where(Expr::exists(incoming_previous).not())
                    .and_where(Expr::exists(incoming_foreign).not());
                match self.generation {
                    Some(generation) => {
                        eligible.and_where(
                            Expr::col((runner::Entity, runner::Column::Generation)).eq(generation),
                        );
                    }
                    None => {
                        eligible.and_where(
                            Expr::col((runner::Entity, runner::Column::Generation)).is_null(),
                        );
                    }
                }
                eligible.limit(1);
                let eligible = tx.query_one(&eligible).await?;
                if eligible.is_some() {
                    // Demote first, commit the marker and bounded rows atomically.
                    // A crash can leave marker 0, never ready over partial proofs.
                    checkpoint::Entity::update_many()
                        .col_expr(checkpoint::Column::ProofVersion, Expr::val(0))
                        .filter(checkpoint::Column::Id.eq(&self.id))
                        .filter(checkpoint::Column::ProofVersion.eq(1))
                        .exec(&tx)
                        .await?;
                    for (table, key, size, bytes) in &keys {
                        tx.execute(
                            &Query::delete()
                                .from_table(Alias::new(*table))
                                .and_where(Expr::col(Alias::new("checkpoint_id")).eq(&self.id))
                                .and_where(Expr::col(Alias::new("rowid")).eq(*key))
                                .and_where(Expr::cust(*size).eq(*bytes))
                                .to_owned(),
                        )
                        .await?;
                    }
                }
                tx.commit().await?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

/// One startup keyset pass establishes that legacy container/layout scopes
/// are inferable before ANY new lifetime cleanup. Subsequent supported writers
/// only create checked dependencies; mixed binaries/raw external writes are
/// outside the rollout contract. No global inventory is cached.
#[derive(Default)]
struct ScopeDiscovery {
    started: bool,
    family: usize,
    after: String,
    kind: i64,
    done: bool,
}
impl ScopeDiscovery {
    async fn step(&mut self, store: &CrudStore) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        let (table, key) = match self.family {
            0 => ("compaction_frozen_history", "id"),
            1 => ("compaction_frozen_layout", "manifest_id"),
            2 => ("compaction_frozen_message_data", "manifest_id"),
            3 => ("compaction_frozen_import_data", "manifest_id"),
            4 => ("compaction_operation", "id"),
            _ => ("compaction_checkpoint", "id"),
        };
        let mut query = Query::select();
        query
            .from(Alias::new(table))
            .expr_as(Expr::col(Alias::new(key)), Alias::new("manifest_id"));
        if self.family == 1 {
            query.column(Alias::new("kind"));
        } else {
            query.expr_as(Expr::val(i64::from(self.family == 3)), Alias::new("kind"));
        }
        if self.started {
            let mut after = Expr::col(Alias::new(key)).gt(&self.after);
            if self.family == 1 {
                after = after.or(Expr::col(Alias::new(key))
                    .eq(&self.after)
                    .and(Expr::col(Alias::new("kind")).gt(self.kind)));
            }
            query.and_where(after);
        }
        query.order_by(Alias::new(key), Order::Asc);
        if self.family == 1 {
            query.order_by(Alias::new("kind"), Order::Asc);
        }
        if matches!(self.family, 2 | 3) {
            query.order_by(Alias::new("ordinal"), Order::Asc);
        }
        query.limit(1);
        let row = store.connection.query_one(&query).await?;
        let Some(row) = row else {
            self.family += 1;
            self.after.clear();
            self.kind = -1;
            self.started = false;
            if self.family == 6 {
                self.done = true;
            }
            return Ok(true);
        };
        let id: String = row.try_get("", "manifest_id")?;
        let kind: i64 = row.try_get("", "kind")?;
        if self.family >= 4 {
            let node = Alias::new("node");
            let mut scoped = Query::select();
            let mut bound = Query::select();
            if self.family == 4 {
                scoped.from_as(operation::Entity, node.clone());
                bound
                    .from(projection::Entity)
                    .and_where(projection::Column::OperationId.eq(&id));
            } else {
                scoped.from_as(checkpoint::Entity, node.clone());
                bound
                    .from(checkpoint::Entity)
                    .join(
                        JoinType::InnerJoin,
                        projection::Entity,
                        Expr::col((projection::Entity, projection::Column::OperationId)).eq(
                            Expr::col((checkpoint::Entity, checkpoint::Column::OperationId)),
                        ),
                    )
                    .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(&id));
            }
            scoped
                .column((context::Entity, context::Column::WorkspaceId))
                .join(
                    JoinType::InnerJoin,
                    context::Entity,
                    Expr::col((context::Entity, context::Column::Owner))
                        .eq(Expr::col((node.clone(), Alias::new("owner")))),
                )
                .and_where(Expr::col((node, Alias::new("id"))).eq(&id))
                .and_where(Expr::col((context::Entity, context::Column::WorkspaceId)).ne(""));
            bound
                .column((history::Entity, history::Column::WorkspaceId))
                .join(
                    JoinType::InnerJoin,
                    history::Entity,
                    Expr::col((history::Entity, history::Column::Id)).eq(Expr::col((
                        projection::Entity,
                        projection::Column::ManifestId,
                    ))),
                )
                .and_where(Expr::col((history::Entity, history::Column::WorkspaceId)).ne(""));
            scoped.union(UnionType::All, bound).limit(1);
            ensure!(
                store.connection.query_one(&scoped).await?.is_some(),
                "legacy operation/checkpoint scope unknown; cleanup waits for repair"
            );
            self.after = id;
            self.kind = kind;
            self.started = true;
            return Ok(true);
        }
        let mut known = Query::select();
        known
            .column((history::Entity, history::Column::WorkspaceId))
            .from(history::Entity)
            .and_where(history::Column::Id.eq(&id))
            .and_where(history::Column::WorkspaceId.ne(""));
        for reverse in [false, true] {
            let (to, predicate) = if reverse {
                (layout::Column::Candidate, layout::Column::ManifestId)
            } else {
                (layout::Column::ManifestId, layout::Column::Candidate)
            };
            let branch = Query::select()
                .column((history::Entity, history::Column::WorkspaceId))
                .from(layout::Entity)
                .join(
                    JoinType::InnerJoin,
                    history::Entity,
                    Expr::col((history::Entity, history::Column::Id))
                        .eq(Expr::col((layout::Entity, to))),
                )
                .and_where(Expr::col((layout::Entity, predicate)).eq(&id))
                .and_where(Expr::col((history::Entity, history::Column::WorkspaceId)).ne(""))
                .to_owned();
            known.union(UnionType::All, branch);
        }
        for reverse in [false, true] {
            let (header, predicate) = if reverse {
                (span::Column::SourceManifest, span::Column::ManifestId)
            } else {
                (span::Column::ManifestId, span::Column::SourceManifest)
            };
            let branch = Query::select()
                .column((history::Entity, history::Column::WorkspaceId))
                .from(span::Entity)
                .join(
                    JoinType::InnerJoin,
                    history::Entity,
                    Expr::col((history::Entity, history::Column::Id))
                        .eq(Expr::col((span::Entity, header))),
                )
                .and_where(Expr::col((span::Entity, predicate)).eq(&id))
                .and_where(Expr::col((history::Entity, history::Column::WorkspaceId)).ne(""))
                .to_owned();
            known.union(UnionType::All, branch);
        }
        known.limit(1);
        let known = store.connection.query_one(&known).await?;
        ensure!(
            known.is_some(),
            "legacy frozen dependency scope unknown; cleanup waits for repair"
        );
        self.after = id;
        self.kind = kind;
        self.started = true;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_compaction::{
        Checkpoint, ModelSelection, SourceRef, Transport,
        frozen::{FrozenEventInputRole, FrozenHistoryRef, FrozenMessageRef},
        runner::FailureKind,
    };
    use sea_orm::Database;
    use sha2::{Digest, Sha256};
    // Real physical tables with FK OFF, using the existing fixture/schema.
    async fn batch_fixture(kind: i64, count: i64) -> CrudStore {
        let (store, _) = fixture(false).await;
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,next_ordinal,import_count,next_import,ready) VALUES('batch-physical','ws','thread','batch','batch',?1,0,?1,0,0)",
            [count.into()])).await.unwrap();
        let payload = format!("{{}}{}", " ".repeat(4998));
        for ordinal in 0..count {
            let sql = if kind == 0 {
                "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('batch-physical',?1,?2,5000)"
            } else {
                "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('batch-physical',?1,0,'event','event-'||?1,'v','thread',?2,5000)"
            };
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    sql,
                    [ordinal.into(), payload.clone().into()],
                ))
                .await
                .unwrap();
        }
        store.with_maintenance_access()
    }
    async fn batch_consumer(store: &CrudStore, id: &str, kind: i64, spans: i64) {
        let messages = if kind == 0 { spans } else { 0 };
        let imports = if kind == 1 { spans } else { 0 };
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,next_ordinal,import_count,next_import,ready) VALUES(?1,'ws','thread','consumer','consumer',?2,?2,?3,?3,1)",
            [id.into(), messages.into(), imports.into()])).await.unwrap();
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES(?1,?2,0,0)", [id.into(), kind.into()])).await.unwrap();
        for ordinal in 0..spans {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES(?1,?2,?3,?4,'batch-physical')",
                [id.into(), kind.into(), ordinal.into(), (ordinal+1).into()])).await.unwrap();
        }
        layout::Entity::update_many()
            .col_expr(layout::Column::Active, Expr::val(1))
            .filter(layout::Column::ManifestId.eq(id))
            .filter(layout::Column::Kind.eq(kind))
            .exec(&store.connection)
            .await
            .unwrap();
    }
    async fn prepare_batch(store: &CrudStore, kind: i64, until: i64) -> SweepPreparation {
        let mut sweep = SweepPreparation::new("batch-physical".into(), kind, until);
        for _ in 0..2000 {
            if sweep.prepare(store).await.unwrap() {
                return sweep;
            }
        }
        panic!("all geometry pages must finish without a consumer/span cap");
    }
    async fn batch_ordinals(store: &CrudStore, kind: i64) -> Vec<i64> {
        let (data, _) = physical_table(kind);
        store
            .connection
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                format!(
                    "SELECT ordinal FROM {data} WHERE manifest_id='batch-physical' ORDER BY ordinal"
                ),
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.try_get("", "ordinal").unwrap())
            .collect()
    }

    #[tokio::test]
    async fn container_batches_keep_shared_prefix_and_continue_past_poison_and_visit_end() {
        for kind in [0, 1] {
            let store = batch_fixture(kind, 601).await;
            // More consumers and spans than one metadata page; none is omitted.
            for consumer in 0..40 {
                batch_consumer(
                    &store,
                    &format!("consumer-{consumer:02}"),
                    kind,
                    if consumer == 0 { 65 } else { 1 },
                )
                .await;
            }
            let (data, _) = physical_table(kind);
            // Seed legacy corruption on one pinned fixture connection, then
            // restore CHECK enforcement before exercising production cleanup.
            let tx = store.connection.begin().await.unwrap();
            tx.execute_unprepared("PRAGMA ignore_check_constraints=ON")
                .await
                .unwrap();
            tx.execute_unprepared(&format!(
                "UPDATE {data} SET bytes=5001 WHERE manifest_id='batch-physical' AND ordinal=70"
            ))
            .await
            .unwrap();
            tx.execute_unprepared("PRAGMA ignore_check_constraints=OFF")
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let mut sweep = prepare_batch(&store, kind, 600).await;
            let prepared = sweep.prepared.clone();
            // A new physical tail must not extend this visit indefinitely.
            let sql = if kind == 0 {
                "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) SELECT manifest_id,601,reference_json,bytes FROM compaction_frozen_message_data WHERE manifest_id='batch-physical' AND ordinal=600"
            } else {
                "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) SELECT manifest_id,601,message_ordinal,source_scope,source_id||'-tail',source_version,source_thread,proof_json,bytes FROM compaction_frozen_import_data WHERE manifest_id='batch-physical' AND ordinal=600"
            };
            store.connection.execute_unprepared(sql).await.unwrap();
            let mut accounting = SweepAccounting::default();
            let mut useful = 0;
            let mut done = false;
            for _ in 0..20 {
                let before = (
                    accounting.rows[kind as usize],
                    accounting.bytes[kind as usize],
                );
                done = sweep.step(&store, &mut accounting).await.unwrap();
                let rows = accounting.rows[kind as usize] - before.0;
                let bytes = accounting.bytes[kind as usize] - before.1;
                assert!(rows <= 256 && bytes <= 1024 * 1024);
                useful += usize::from(rows > 0);
                assert_eq!(
                    sweep.prepared, prepared,
                    "next portion reuses checked geometry"
                );
                if done {
                    break;
                }
            }
            assert!(done && useful >= 3);
            assert_eq!(accounting.rows[kind as usize], 535);
            assert_eq!(accounting.bytes[kind as usize], 535 * 5000);
            assert_eq!(
                batch_ordinals(&store, kind).await,
                (0..65).chain([70, 601]).collect::<Vec<_>>()
            );
            // Fresh discovery sees the committed remainder, including the new
            // tail; poison neither loops nor prevents a later free row.
            let mut restart = FrozenStorageLifetimeProgress::default();
            // Discovery succeeds, then bound decoding fails. No visit/cursor
            // may be acknowledged until that second read succeeds.
            restart.physical_kind = kind as usize;
            let bad_bound = sql
                .replace(",601,", ",'unreadable-bound',")
                .replace("ordinal=600", "ordinal=601");
            store
                .connection
                .execute_unprepared(&bad_bound)
                .await
                .unwrap();
            let error = restart.sweep_quantum(&store).await.unwrap_err();
            assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
            assert!(restart.physical_after[kind as usize].is_none());
            assert!(restart.sweep.is_none() && restart.sweep_pending.is_empty());
            store.connection.execute_unprepared(&format!("DELETE FROM {data} WHERE manifest_id='batch-physical' AND ordinal='unreadable-bound'")).await.unwrap();
            restart.physical_kind = kind as usize;
            assert!(restart.sweep_quantum(&store).await.unwrap());
            assert_eq!(
                restart.physical_after[kind as usize].as_deref(),
                Some("batch-physical")
            );
            assert_eq!(restart.sweep.as_ref().unwrap().manifest, "batch-physical");
            let mut eof = false;
            for _ in 0..2000 {
                if !restart.sweep_quantum(&store).await.unwrap() {
                    eof = true;
                    break;
                }
            }
            assert!(eof);
            assert_eq!(restart.accounting.rows[kind as usize], 1);
            assert_eq!(
                batch_ordinals(&store, kind).await,
                (0..65).chain([70]).collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn batch_revalidates_new_consumers_and_capture_bounds_and_reports_only_commit() {
        for kind in [0, 1] {
            let store = batch_fixture(kind, 4).await;
            let mut sweep = prepare_batch(&store, kind, 3).await;
            batch_consumer(&store, "new-consumer", kind, 1).await;
            let mut accounting = SweepAccounting::default();
            assert!(sweep.step(&store, &mut accounting).await.unwrap());
            assert_eq!(accounting.rows, [0, 0]);
            assert_eq!(accounting.refused, 1);
            assert_eq!(batch_ordinals(&store, kind).await, vec![0, 1, 2, 3]);

            let mut sweep = prepare_batch(&store, kind, 3).await;
            // Separate committed bounds: messages retain two rows, imports one.
            store.connection.execute_unprepared("UPDATE compaction_frozen_history SET ready=0,message_count=4,import_count=4,next_ordinal=2,next_import=1 WHERE id='batch-physical'").await.unwrap();
            assert!(sweep.step(&store, &mut accounting).await.unwrap());
            assert_eq!(accounting.rows, [0, 0]);
            assert_eq!(accounting.refused, 2);
            assert_eq!(batch_ordinals(&store, kind).await, vec![0, 1, 2, 3]);

            let mut sweep = prepare_batch(&store, kind, 3).await;
            let (data, _) = physical_table(kind);
            store.connection.execute_unprepared(&format!("CREATE TRIGGER refuse_batch BEFORE DELETE ON {data} WHEN OLD.manifest_id='batch-physical' AND OLD.ordinal=3 BEGIN SELECT RAISE(ABORT,'fixture rollback'); END")).await.unwrap();
            assert!(sweep.step(&store, &mut accounting).await.is_err());
            assert_eq!(accounting.rows, [0, 0]);
            assert_eq!(accounting.bytes, [0, 0]);
            assert_eq!(batch_ordinals(&store, kind).await, vec![0, 1, 2, 3]);
            store
                .connection
                .execute_unprepared("DROP TRIGGER refuse_batch")
                .await
                .unwrap();
            assert!(sweep.step(&store, &mut accounting).await.unwrap());
            let retained = if kind == 0 { 2 } else { 1 };
            assert_eq!(
                batch_ordinals(&store, kind).await,
                (0..retained).collect::<Vec<_>>()
            );
            assert_eq!(accounting.rows[kind as usize], (4 - retained) as u64);
            assert_eq!(
                accounting.bytes[kind as usize],
                (4 - retained) as u64 * 5000
            );

            // A published layout reads a separate healthy backing. Unsuitable
            // redundant physical rows must not quarantine that whole layout.
            let duplicate = batch_fixture(kind, 6).await;
            let (data, payload) = physical_table(kind);
            let messages = if kind == 0 { 6_i64 } else { 0 };
            let imports = if kind == 1 { 6_i64 } else { 0 };
            duplicate.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,next_ordinal,import_count,next_import,ready) VALUES('duplicate-backing','ws','thread','backing','backing',?1,?1,?2,?2,1)",
                [messages.into(), imports.into()])).await.unwrap();
            let fields = if kind == 0 {
                "ordinal,reference_json,bytes"
            } else {
                "ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes"
            };
            duplicate.connection.execute_unprepared(&format!("INSERT INTO {data}(manifest_id,{fields}) SELECT 'duplicate-backing',{fields} FROM {data} WHERE manifest_id='batch-physical'")).await.unwrap();
            duplicate.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_frozen_history SET ready=1,message_count=?1,next_ordinal=?1,import_count=?2,next_import=?2 WHERE id='batch-physical'",
                [messages.into(), imports.into()])).await.unwrap();
            duplicate.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,cleanup_to) VALUES('batch-physical',?1,0,1,6)", [kind.into()])).await.unwrap();
            duplicate.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('batch-physical',?1,0,6,'duplicate-backing')", [kind.into()])).await.unwrap();
            layout::Entity::update_many()
                .col_expr(layout::Column::Active, Expr::val(1))
                .filter(layout::Column::ManifestId.eq("batch-physical"))
                .filter(layout::Column::Kind.eq(kind))
                .exec(&duplicate.connection)
                .await
                .unwrap();
            let resized_rows =
                [(1_i64, 256 * 1024 + 1), (2, 256 * 1024 - 5000)].map(|(ordinal, size)| {
                    (
                        ordinal,
                        size,
                        format!("{{}}{}", " ".repeat(size as usize - 2)),
                    )
                });
            // These deliberately invalid/legacy-sized physical rows cannot
            // be written through the normal schema constraints.
            let tx = duplicate.connection.begin().await.unwrap();
            tx.execute_unprepared("PRAGMA ignore_check_constraints=ON")
                .await
                .unwrap();
            tx.execute_unprepared(&format!(
                "UPDATE {data} SET bytes=5001 WHERE manifest_id='batch-physical' AND ordinal=0"
            ))
            .await
            .unwrap();
            for (ordinal, size, text) in resized_rows {
                tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    format!("UPDATE {data} SET {payload}=?1,bytes=?2 WHERE manifest_id='batch-physical' AND ordinal=?3"),
                    [text.into(), size.into(), ordinal.into()])).await.unwrap();
            }
            let bad_field = if kind == 0 {
                "bytes"
            } else {
                "message_ordinal"
            };
            tx.execute_unprepared(&format!(
                "UPDATE {data} SET {bad_field}=-1 WHERE manifest_id='batch-physical' AND ordinal=5"
            ))
            .await
            .unwrap();
            tx.execute_unprepared("PRAGMA ignore_check_constraints=OFF")
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let mut progress = FrozenStorageLifetimeProgress::default();
            while !progress.scope.done {
                progress.scope.step(&duplicate).await.unwrap();
            }
            duplicate.connection.execute_unprepared("CREATE TRIGGER refuse_cleanup_cursor BEFORE UPDATE OF cleanup_next ON compaction_frozen_layout BEGIN SELECT RAISE(ABORT,'fixture cleanup rollback'); END").await.unwrap();
            let error = duplicate
                .compact_frozen_storage_quantum(&mut progress)
                .await
                .unwrap_err();
            assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
            assert_eq!(
                batch_ordinals(&duplicate, kind).await,
                (0..6).collect::<Vec<_>>()
            );
            assert_eq!(progress.accounting.rows, [0, 0]);
            assert_eq!(progress.accounting.bytes, [0, 0]);
            let rolled_back = layout::Entity::find_by_id(("batch-physical".to_owned(), kind))
                .one(&duplicate.connection)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (
                    rolled_back.failed,
                    rolled_back.pending,
                    rolled_back.cleanup_next
                ),
                (0, 1, 0)
            );
            duplicate
                .connection
                .execute_unprepared("DROP TRIGGER refuse_cleanup_cursor")
                .await
                .unwrap();
            for (next, rows, bytes, remaining) in [
                (4, 2, 256 * 1024, vec![0, 1, 4, 5]),
                (6, 3, 256 * 1024 + 5000, vec![0, 1, 5]),
            ] {
                assert!(
                    duplicate
                        .compact_frozen_storage_quantum(&mut progress)
                        .await
                        .unwrap()
                );
                let l = layout::Entity::find_by_id(("batch-physical".to_owned(), kind))
                    .one(&duplicate.connection)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    (l.failed, l.cleanup_next, l.pending),
                    (0, next, i64::from(next < 6))
                );
                assert_eq!(batch_ordinals(&duplicate, kind).await, remaining);
                assert_eq!(progress.accounting.rows[kind as usize], rows);
                assert_eq!(progress.accounting.bytes[kind as usize], bytes);
            }
            // The >256 KiB healthy row belongs to lifetime sweep. It can still
            // delete it after duplicate cleanup, preserving both poison rows.
            let mut sweep = prepare_batch(&duplicate, kind, 5).await;
            assert!(
                sweep
                    .step(&duplicate, &mut progress.accounting)
                    .await
                    .unwrap()
            );
            assert_eq!(batch_ordinals(&duplicate, kind).await, vec![0, 5]);
            assert_eq!(progress.accounting.rows[kind as usize], 4);
            assert_eq!(
                progress.accounting.bytes[kind as usize],
                2 * 256 * 1024 + 5001
            );
        }
    }

    async fn fixture(published: bool) -> (CrudStore, FrozenHistoryRef) {
        fixture_with_checkpoint_ids(published, ["cp-0", "cp-1"]).await
    }
    async fn fixture_with_checkpoint_ids(
        published: bool,
        ids: [&str; 2],
    ) -> (CrudStore, FrozenHistoryRef) {
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options.max_connections(1).min_connections(1);
        let db = sea_orm::Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        let pragma: i64 = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA foreign_keys",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "foreign_keys")
            .unwrap();
        assert_eq!(pragma, 0, "production single writer uses FK OFF");
        Migrator::up(&db, None).await.unwrap();
        for sql in [
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)",
        ] {
            db.execute_unprepared(sql).await.unwrap();
        }
        let store = CrudStore::new(db);
        let db = store.database_connection();
        let sources: Vec<_> = (0..2)
            .map(|i| SourceRef {
                scope: "event:turn".into(),
                id: format!("event-{i}"),
                version: "event-revision:1".into(),
            })
            .collect();
        let references: Vec<_> = sources
            .iter()
            .enumerate()
            .map(|(i, s)| FrozenMessageRef {
                logical_turn_id: Some("turn".into()),
                context_thread: None,
                source_thread: "thread".into(),
                unit_id: format!("unit-{i}"),
                sources: vec![s.clone()],
                event_input_role: Some(FrozenEventInputRole::Authoritative),
                source_aliases: vec![],
                ambiguous_input_aliases: vec![],
                publication_aliases: None,
                inherited: false,
                complete: true,
                protected_input: false,
                wire_sha256: "a".repeat(64),
                replay_source: None,
                tool_item_id: None,
                tool_call_id: None,
                tool_name: None,
            })
            .collect();
        let mut digest = Sha256::new();
        for r in &references {
            let b = serde_json::to_vec(r).unwrap();
            digest.update((b.len() as u64).to_be_bytes());
            digest.update(b);
        }
        let h = FrozenHistoryRef {
            format: 1,
            manifest_id: "origin".into(),
            messages: 2,
            identity_sha256: hex::encode(digest.finalize()),
        };
        store
            .compaction_begin_frozen_history("ws", "thread", &h)
            .await
            .unwrap();
        store
            .compaction_append_frozen_history("ws", "thread", "origin", 0, &references)
            .await
            .unwrap();
        let hold = store
            .compaction_finish_frozen_history_held("ws", "thread", &h)
            .await
            .unwrap();
        let status = if published { "completed" } else { "failed" };
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,attempts,transient_retries,correction,next_portion) VALUES('op','owner','op',?1,'{}',1000,0,0,0,0)",[status.into()])).await.unwrap();
        let state = RunnerState {
            generation: 0,
            deadline_ms: 1000,
            attempts: 0,
            retries: 0,
            corrections: 0,
            target_tokens: 10,
            source_text_projection_version: 0,
            cursor: Default::default(),
            previous_checkpoint: None,
            phase: if published {
                RunnerPhase::Applied {
                    checkpoint: ids[1].into(),
                }
            } else {
                RunnerPhase::Failed {
                    kind: FailureKind::Deadline,
                }
            },
            resume_phase: None,
            observation: None,
            diagnostic: None,
        };
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('op',0,?1)",
            [serde_json::to_string(&state).unwrap().into()],
        ))
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO compaction_runner_plan(operation_id,source_count,reference_count,descriptor,ready) VALUES('op',2,0,'{}',1)").await.unwrap();
        db.execute_unprepared("INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT 'op',id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id='origin'").await.unwrap();
        for (i, s) in sources.iter().enumerate() {
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES('op',?1,?1,0,'thread',?2,?3,?4)",[(i as i64).into(),s.scope.clone().into(),s.id.clone().into(),s.version.clone().into()])).await.unwrap();
            let cp = Checkpoint {
                id: ids[i].into(),
                operation_id: "op".into(),
                owner: "owner".into(),
                previous: (i > 0).then(|| ids[0].into()),
                summary: format!("summary {i}"),
                selection: ModelSelection {
                    transport: Transport::Api,
                    instance: "p".into(),
                    model: "m".into(),
                    effort: None,
                },
                coverage: vec![s.clone()],
                projection_version: 1,
                format_version: 1,
            };
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES(?1,'op','owner',?2,?3,?4,?5,?6,1,1,?7)",[cp.id.clone().into(),cp.previous.clone().into(),(i as i64).into(),cp.summary.clone().into(),compaction::checkpoint_identity(&cp).unwrap().into(),serde_json::to_string(&cp.selection).unwrap().into(),if published&&i==1{"applied"}else{"candidate"}.into()])).await.unwrap();
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES(?1,?2,?3,?4)",[cp.id.into(),s.scope.clone().into(),s.id.clone().into(),s.version.clone().into()])).await.unwrap();
        }
        drop(hold);
        (store, h)
    }
    async fn value(store: &CrudStore, table: &str, id: &str) -> i64 {
        store
            .connection
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                format!("SELECT count(*) AS n FROM {table} WHERE checkpoint_id=?1"),
                [id.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap()
    }
    async fn prepared_expiry(store: &CrudStore) -> ExpiryPreparation {
        let header = history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let mut p = ExpiryPreparation::new(header);
        for _ in 0..500 {
            if p.layout_kind == 2 && p.operations_done && p.checkpoints_done && p.reverse.is_none()
            {
                return p;
            }
            assert!(
                !p.step(store, &mut SweepAccounting::default())
                    .await
                    .unwrap()
            );
        }
        panic!("bounded expiry preparation must finish");
    }

    async fn legacy_locator_fixture() -> (CrudStore, FrozenHistoryRef) {
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        let before = Migrator::migrations()
            .iter()
            .position(|m| m.name() == "m20261010_000001_frozen_lifetime")
            .unwrap();
        Migrator::up(&db, Some(before as u32)).await.unwrap();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
        )
        .await
        .unwrap();
        let h = FrozenHistoryRef {
            format: 1,
            manifest_id: "legacy-origin".into(),
            messages: 0,
            identity_sha256: hex::encode(Sha256::digest([])),
        };
        for index in 0..128 {
            let id = format!("root-{index:04}");
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES(?1,'ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [id.clone().into()])).await.unwrap();
            let json = if index == 127 {
                "malformed retaining legacy receipt".into()
            } else if index == 0 || index == 2 {
                serde_json::json!({"provider":"claude","padding":"x".repeat(if index == 2 { 150_000 } else { 0 }),"pioneerContext":{"version":4,"nativeThreadId":"native","acceptedTurnId":"turn","acceptedTurnRevision":1,"acceptedTurnDeleted":false,"contextOwnerThreadId":"root-0000","contextManifestOwnerThreadId":"root-0000","contextHistoryJson":serde_json::to_string(&h).unwrap()}}).to_string()
            } else {
                "{}".into()
            };
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO thread_cli_runtime_binding(thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,resume_cursor_json,status) VALUES(?1,'ws','runtime','claude','native',?2,'active')", [id.into(), json.into()])).await.unwrap();
        }
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES(?1,'ws','root-0000',?2,0,0,0,0,?2,1)", [h.manifest_id.clone().into(), h.identity_sha256.clone().into()])).await.unwrap();
        // Unknown rows are a genuine pre-cutover fixture. Guards stay installed
        // for every reconciliation, fresh write and corrupt-state action below.
        Migrator::up(&db, None).await.unwrap();
        (CrudStore::new(db).with_maintenance_access(), h)
    }

    async fn locator_marker(store: &CrudStore, id: &str) -> Option<String> {
        pioneer_entity::thread_cli_runtime_binding::Entity::find_by_id(id)
            .select_only()
            .column(pioneer_entity::thread_cli_runtime_binding::Column::FrozenManifestId)
            .into_tuple::<Option<String>>()
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn locator_portion_and_restart_keep_committed_ids_nulls_and_partial_byte_bounds() {
        let (store, h) = legacy_locator_fixture().await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        for _ in 0..64 {
            progress.quantum(&store).await.unwrap();
            if progress
                .locator
                .as_ref()
                .is_some_and(|read| read.id == "root-0002" && !read.bytes.is_empty())
            {
                break;
            }
        }
        assert!(
            progress.locators[2].as_str() < "root-0127",
            "only bounded metadata page was loaded"
        );
        assert!(progress.locator_pending[2].len() <= META_ROWS as usize);
        assert_eq!(
            locator_marker(&store, "root-0000").await.as_deref(),
            Some("legacy-origin")
        );
        assert_eq!(locator_marker(&store, "root-0001").await, None);
        assert_eq!(
            locator_marker(&store, "root-0126").await.as_deref(),
            Some("")
        );

        // Restart after an unfinished fragment: no promise to keep RAM bytes.
        let read = progress.locator.as_ref().unwrap();
        assert_eq!(read.id, "root-0002");
        assert_eq!(read.bytes.len(), FRAGMENT_BYTES as usize);
        drop(progress);
        assert!(store.frozen_readers.lock().unwrap().is_empty());
        let mut restart = FrozenStorageLifetimeProgress::default();
        for _ in 0..20 {
            restart.locator_quantum(&store).await.unwrap();
            if restart.locator.is_some() {
                break;
            }
        }
        assert_eq!(restart.locator.as_ref().unwrap().id, "root-0002");
        assert!(restart.locator.as_ref().unwrap().bytes.is_empty());
        assert!(
            restart
                .locator_pending
                .iter()
                .flatten()
                .all(|(id, _, _)| id != "root-0000" && id != "root-0001")
        );
        for _ in 0..1000 {
            if restart.locator_quantum(&store).await.is_err() {
                break;
            }
        }
        assert_eq!(
            locator_marker(&store, "root-0002").await.as_deref(),
            Some("legacy-origin")
        );
        assert_eq!(
            locator_marker(&store, "root-0127").await.as_deref(),
            Some("")
        );
        // Unsafe retaining unknown receipt still protects the workspace, even
        // when all useful reconciliation ahead of it has durably completed.
        let free = FrozenHistoryRef {
            format: 1,
            manifest_id: "free-in-ws".into(),
            messages: 0,
            identity_sha256: hex::encode(Sha256::digest([])),
        };
        store
            .compaction_begin_frozen_history("ws", "root-0000", &free)
            .await
            .unwrap();
        drop(
            store
                .compaction_finish_frozen_history_held("ws", "root-0000", &free)
                .await
                .unwrap(),
        );
        let header = history::Entity::find_by_id(&free.manifest_id)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        expire(&store, &header).await.unwrap();
        assert_eq!(
            history::Entity::find_by_id(&free.manifest_id)
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .expired,
            0
        );
        store.connection.execute_unprepared("UPDATE thread_cli_runtime_binding SET resume_cursor_json='{}',frozen_manifest_id=NULL WHERE thread_id='root-0127'").await.unwrap();
        expire(&store, &header).await.unwrap();
        assert_eq!(
            history::Entity::find_by_id(&free.manifest_id)
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .expired,
            1
        );
        assert_eq!(
            history::Entity::find_by_id(&h.manifest_id)
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .expired,
            0
        );
    }

    #[tokio::test]
    async fn supported_fresh_roots_do_not_reintroduce_unknown_or_invalidate_scope_cursor() {
        let (store, h) = legacy_locator_fixture().await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        assert!(!progress.conversion_scope(&store).await.unwrap());
        assert_eq!(progress.scope.after, h.manifest_id);
        for id in ["a-fresh", "z-fresh"] {
            let descriptor = FrozenHistoryRef {
                format: 1,
                manifest_id: id.into(),
                messages: 0,
                identity_sha256: hex::encode(Sha256::digest([])),
            };
            store
                .compaction_begin_frozen_history("ws", "root-0000", &descriptor)
                .await
                .unwrap();
        }
        let now = chrono::Utc::now().fixed_offset();
        for (id, json, expected) in [
            ("root-0000", "{}".to_owned(), None),
            ("root-0001", serde_json::json!({"provider":"claude","pioneerContext":{"version":4,"nativeThreadId":"native","acceptedTurnId":"turn","acceptedTurnRevision":1,"acceptedTurnDeleted":false,"contextManifestOwnerThreadId":"root-0000","contextOwnerThreadId":"root-0000","contextHistoryJson":serde_json::to_string(&h).unwrap()}}).to_string(), Some(h.manifest_id.clone())),
        ] {
            store.upsert_cli_runtime_thread_binding(crate::NewCliRuntimeThreadBinding {
                thread_id: id.into(), workspace_id: "ws".into(), runtime_id: "runtime".into(), runtime_kind: "claude".into(), native_thread_id: "native".into(),
                native_session_id: None, native_root_thread_id: None, native_cwd: None, native_model: None,
                resume_cursor_json: json, status: "active".into(), created_at: now, updated_at: now,
            }).await.unwrap();
            assert_eq!(locator_marker(&store, id).await, expected);
            assert!(store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE thread_cli_runtime_binding SET frozen_manifest_id='' WHERE thread_id=?1", [id.into()])).await.is_err());
        }
        for _ in 0..30 {
            if progress.scope.done {
                break;
            }
            progress.conversion_scope(&store).await.unwrap();
        }
        assert!(progress.scope.done);
        assert_eq!(
            locator_marker(&store, "root-0127").await.as_deref(),
            Some("")
        );
        assert!(store.frozen_readers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bounded_startup_steps_keep_cursor_barrier_and_distinct_backing_groups() {
        let (store, _) = fixture(true).await;
        for index in 0..256 {
            let descriptor = FrozenHistoryRef {
                format: 1,
                manifest_id: format!("scope-{index:04}"),
                messages: 0,
                identity_sha256: hex::encode(Sha256::digest([])),
            };
            store
                .compaction_begin_frozen_history("ws", "thread", &descriptor)
                .await
                .unwrap();
        }
        let mut progress = FrozenStorageLifetimeProgress::default();
        // A worker portion is a finite number of these existing useful steps.
        // Proof/locator/scopes remain fair while cleanup is scope-blocked.
        for _ in 0..64 {
            assert!(progress.quantum(&store).await.unwrap());
        }
        assert!(!progress.scope.done);
        assert_eq!(progress.scope.family, 0);
        assert!(progress.scope.after.starts_with("scope-"));
        assert!(progress.scope.after.as_str() < "scope-0255");
        assert!(
            progress.expiry.is_none() && progress.sweep.is_none() && progress.discard.is_none()
        );
        let after = progress.scope.after.clone();
        assert!(progress.quantum(&store).await.unwrap());
        assert!(progress.scope.after >= after);
        for _ in 0..1000 {
            if progress.scope.done {
                break;
            }
            progress.conversion_scope(&store).await.unwrap();
        }
        assert!(progress.scope.done);

        // The fixture has two physical rows in ONE backing manifest. Preserve
        // group discovery and the independent (manifest,kind) layout cursor.
        let mut groups = ScopeDiscovery {
            family: 2,
            ..Default::default()
        };
        groups.step(&store).await.unwrap();
        assert_eq!(groups.after, "origin");
        groups.step(&store).await.unwrap();
        assert_eq!(groups.family, 3);
        store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .unwrap();
        let mut layouts = ScopeDiscovery {
            family: 1,
            ..Default::default()
        };
        layouts.step(&store).await.unwrap();
        assert_eq!((&*layouts.after, layouts.kind), ("origin", 0));
        layouts.step(&store).await.unwrap();
        assert_eq!((&*layouts.after, layouts.kind), ("origin", 1));

        // Unknown legacy scope blocks even after successful earlier objects.
        // Model FK OFF corruption without disabling any installed trigger.
        store.connection.execute_unprepared("INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES('zz-orphan','missing','orphan','failed','{}',1)").await.unwrap();
        let mut restart = ScopeDiscovery::default();
        let mut refused = false;
        for _ in 0..1000 {
            if restart.step(&store).await.is_err() {
                refused = true;
                break;
            }
        }
        assert!(refused && !restart.done);
        assert!(
            !restart.step(&store).await.is_ok(),
            "poison never becomes EOF"
        );
        assert_eq!(
            history::Entity::find_by_id("origin")
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .expired,
            0
        );
    }

    #[tokio::test]
    async fn skipped_families_yield_to_unfinished_paths_and_confirm_global_eof() {
        let (store, _) = fixture(true).await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        progress.locator_done = [true, true, true, false];
        progress.locator_kind = 0;
        assert!(!progress.locator_quantum(&store).await.unwrap());
        assert!(progress.locator_done.iter().all(|done| *done));
        progress.physical_done = [true, false];
        progress.physical_kind = 0;
        assert!(!progress.sweep_quantum(&store).await.unwrap());
        assert!(progress.physical_done.iter().all(|done| *done));

        progress.pass_done = [true, true, false, false, false, false];
        progress.phase = 0;
        assert!(progress.quantum(&store).await.unwrap());
        assert_eq!(
            progress.scope.after, "origin",
            "skip reaches scope in this call"
        );
        assert!(progress.expiry.is_none(), "scope barrier still active");
        while !progress.scope.done {
            progress.scope.step(&store).await.unwrap();
        }
        progress.phase = 0;
        assert!(progress.quantum(&store).await.unwrap());
        assert!(
            progress.expiry.is_some(),
            "unfinished cleanup receives next eligible step"
        );
    }

    #[tokio::test]
    async fn queued_proof_rechecks_seal_from_traversal_or_concurrent_preparation() {
        for ancestor_first in [true, false] {
            let (store, _) = fixture_with_checkpoint_ids(true, ["b-parent", "a-root"]).await;
            store
                .connection
                .execute_unprepared(
                    "UPDATE compaction_checkpoint SET status='retained' WHERE id='b-parent'",
                )
                .await
                .unwrap();
            // Real discovery selects A then B at version zero. A's previous
            // traversal seals B before its dequeue; no artificial ready marker.
            let mut progress = FrozenStorageLifetimeProgress::default();
            progress.proof_quantum(&store).await.unwrap();
            assert_eq!(
                progress.proof_pending,
                VecDeque::from(["a-root".to_owned(), "b-parent".to_owned()])
            );
            if ancestor_first {
                progress.proof_quantum(&store).await.unwrap();
                let mut sweep = SweepPreparation::new("origin".into(), 0, 1);
                while !sweep.prepare(&store).await.unwrap() {}
                let geometry = sweep.prepared.clone();
                progress.sweep = Some(sweep);
                store.connection.execute_unprepared("CREATE TRIGGER refuse_freeze BEFORE UPDATE OF next_portion ON compaction_operation BEGIN SELECT RAISE(ABORT,'fixture freeze failure'); END").await.unwrap();
                progress.phase = 1;
                let error = progress.quantum(&store).await.unwrap_err();
                assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
                assert!(
                    !progress.proof.as_ref().unwrap().finished(),
                    "a consumed root frame is not proof EOF"
                );
                assert_eq!(progress.accounting.proofs, 0);
                assert_eq!(progress.sweep.as_ref().unwrap().prepared, geometry);
                store
                    .connection
                    .execute_unprepared("DROP TRIGGER refuse_freeze")
                    .await
                    .unwrap();
                store.connection.execute_unprepared("CREATE TRIGGER refuse_proof_stage BEFORE INSERT ON compaction_checkpoint_event_input BEGIN SELECT RAISE(ABORT,'fixture staging failure'); END").await.unwrap();
                let mut failed_with_hold = false;
                let mut observed_hold = false;
                for _ in 0..500 {
                    observed_hold |= !store.frozen_readers.lock().unwrap().is_empty();
                    progress.phase = 1;
                    if let Err(error) = progress.quantum(&store).await {
                        assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
                        failed_with_hold = true;
                        break;
                    }
                }
                assert!(failed_with_hold && observed_hold);
                assert!(
                    store.frozen_readers.lock().unwrap().is_empty(),
                    "restart drops the old origin hold"
                );
                assert!(!progress.proof.as_ref().unwrap().finished());
                assert_eq!(progress.accounting.proofs, 0);
                assert_eq!(progress.sweep.as_ref().unwrap().prepared, geometry);
                store
                    .connection
                    .execute_unprepared("DROP TRIGGER refuse_proof_stage")
                    .await
                    .unwrap();
                for _ in 0..500 {
                    if progress.proof.is_none() {
                        break;
                    }
                    progress.proof_quantum(&store).await.unwrap();
                }
                assert!(progress.proof.is_none());
            } else {
                checkpoint_proofs::prepare_graph(&store, "a-root")
                    .await
                    .unwrap();
                progress.proof_quantum(&store).await.unwrap();
                assert!(progress.proof.is_none(), "concurrently sealed A is skipped");
            }
            let evidence = store
                .compaction_checkpoint_edges("b-parent")
                .await
                .unwrap()
                .unwrap();
            progress.proof_quantum(&store).await.unwrap();
            assert!(
                progress.proof.is_none(),
                "sealed B must never create GraphPreparation"
            );
            assert!(progress.proof_pending.is_empty());
            let after = store
                .compaction_checkpoint_edges("b-parent")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(after.event_input_evidence, evidence.event_input_evidence);
            assert_eq!(after.replay_aliases.len(), evidence.replay_aliases.len());
            for (after, before) in after.replay_aliases.iter().zip(&evidence.replay_aliases) {
                assert_eq!(after.covered, before.covered);
                assert_eq!(after.replay, before.replay);
                assert_eq!(after.tool_item_id, before.tool_item_id);
            }
            assert!(store.frozen_readers.lock().unwrap().is_empty());
            // Restart discovery ignores both durable seals.
            let mut restart = FrozenStorageLifetimeProgress::default();
            restart.proof_quantum(&store).await.unwrap();
            assert!(restart.proof_pending.is_empty() && restart.proof.is_none());
        }
    }

    #[tokio::test]
    async fn queued_unsealed_missing_and_corrupt_proofs_do_not_gain_readiness() {
        for corruption in [
            "none",
            "private_sealed",
            "missing",
            "format",
            "scope",
            "version",
            "ownership",
        ] {
            let (store, _) = fixture(true).await;
            let mut progress = FrozenStorageLifetimeProgress::default();
            progress.proof_pending.push_back("cp-0".into());
            if corruption == "private_sealed" {
                checkpoint_proofs::prepare_graph(&store, "cp-0")
                    .await
                    .unwrap();
            }
            if corruption == "version" {
                // CHECK (proof_version IN (0,1)) rejects this before dequeue.
                // The unchanged marker must still take the unsealed path.
                assert!(
                    store
                        .connection
                        .execute_unprepared(
                            "UPDATE compaction_checkpoint SET proof_version=2 WHERE id='cp-0'"
                        )
                        .await
                        .is_err()
                );
                assert_eq!(
                    checkpoint::Entity::find_by_id("cp-0")
                        .select_only()
                        .column(checkpoint::Column::ProofVersion)
                        .into_tuple::<i64>()
                        .one(&store.connection)
                        .await
                        .unwrap(),
                    Some(0)
                );
            }
            let sql = match corruption {
                "missing" => Some("DELETE FROM compaction_checkpoint WHERE id='cp-0'"),
                "format" => {
                    Some("UPDATE compaction_checkpoint SET format_version=2 WHERE id='cp-0'")
                }
                "scope" => Some("DELETE FROM compaction_context WHERE owner='owner'"),
                "ownership" => Some("DELETE FROM compaction_manifest WHERE source_id='event-0'"),
                _ => None,
            };
            if let Some(sql) = sql {
                store.connection.execute_unprepared(sql).await.unwrap();
            }
            let result = progress.proof_quantum(&store).await;
            if matches!(
                corruption,
                "none" | "private_sealed" | "version" | "ownership"
            ) {
                result.unwrap();
                assert!(
                    progress.proof.is_some(),
                    "unsealed roots and private candidates retain full validation"
                );
                let mut refused = false;
                for _ in 0..500 {
                    if progress.proof_quantum(&store).await.is_err() {
                        refused = true;
                        break;
                    }
                    if progress.proof.is_none() {
                        break;
                    }
                }
                assert_eq!(refused, corruption == "ownership");
            } else {
                assert!(result.is_err() && progress.proof.is_none());
            }
            drop(progress);
            assert!(store.frozen_readers.lock().unwrap().is_empty());
            assert_eq!(
                history::Entity::find_by_id("origin")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap()
                    .expired,
                0
            );
        }
    }
    #[tokio::test]
    async fn empty_phase_and_completed_scope_are_not_idle_while_lifetime_work_is_pending() {
        let (store, _) = fixture(true).await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        while !progress.scope.done {
            progress.scope.step(&store).await.unwrap();
        }
        progress.phase = 5;
        assert!(
            progress.quantum(&store).await.unwrap(),
            "completed scope is only one family's EOF"
        );
        let mut idle = false;
        for _ in 0..10_000 {
            if !progress.quantum(&store).await.unwrap() {
                idle = true;
                assert!(
                    progress.proof.is_none()
                        && progress.expiry.is_none()
                        && progress.sweep.is_none()
                        && progress.discard.is_none()
                );
                assert!(
                    progress
                        .locator_pending
                        .iter()
                        .all(|pending| pending.is_empty())
                );
                break;
            }
        }
        assert!(
            idle,
            "held metadata must eventually reach a full bounded discovery EOF"
        );
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-1").await,
            1
        );
        let h = history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(h.expired, 1);
        assert!(
            progress.quantum(&store).await.unwrap(),
            "idle bits reset for cyclic held-key revisit"
        );
    }

    #[tokio::test]
    async fn fk_off_orphan_checkpoint_header_scope_and_malformed_edges_fail_closed() {
        for corruption in ["operation", "header", "scope", "dependency"] {
            let (store, h) = fixture(true).await;
            let sql = match corruption {
                "operation" => "DELETE FROM compaction_operation WHERE id='op'",
                "header" => "DELETE FROM compaction_frozen_history WHERE id='origin'",
                "scope" => "DELETE FROM compaction_context WHERE owner='owner'",
                _ => "UPDATE compaction_checkpoint SET previous='missing' WHERE id='cp-1'",
            };
            store.connection.execute_unprepared(sql).await.unwrap();
            let mut graph = checkpoint_proofs::GraphPreparation::new("cp-1");
            let mut refused = false;
            for _ in 0..500 {
                if graph.step(&store).await.is_err() {
                    refused = true;
                    break;
                }
                if graph.finished() {
                    break;
                }
            }
            assert!(
                refused,
                "{corruption}: explicit graph/identity checks must reject without FK enforcement"
            );
            drop(graph);
            if corruption == "header" {
                assert!(store.acquire_frozen_header("ws", &h).await.is_err());
            }
            let bytes:i64 = store.connection.query_one_raw(Statement::from_string(DbBackend::Sqlite,
                "SELECT bytes FROM compaction_frozen_message_data WHERE manifest_id='origin' AND ordinal=0"))
                .await.unwrap().unwrap().try_get("","bytes").unwrap();
            delete_key(
                &store,
                &PhysicalKey {
                    manifest: h.manifest_id.clone(),
                    kind: 0,
                    ordinal: 0,
                    bytes,
                },
            )
            .await
            .unwrap();
            let n:i64 = store.connection.query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='origin'"))
                .await.unwrap().unwrap().try_get("","n").unwrap();
            assert_eq!(n, 2, "refusal preserves actual frozen payloads");
            assert_eq!(
                value(&store, "compaction_checkpoint_event_input", "cp-1").await,
                0
            );
        }
    }

    #[tokio::test]
    async fn cancelled_queued_acquire_never_registers_a_late_reader() {
        let (store, h) = fixture(false).await;
        let held = store.connection.begin().await.unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                store.compaction_acquire_frozen_history("ws", &h)
            )
            .await
            .is_err()
        );
        held.rollback().await.unwrap();
        assert!(store.frozen_readers.lock().unwrap().is_empty());
        let p = prepared_expiry(&store).await;
        expire(&store, &p.header).await.unwrap();
        assert!(
            store
                .compaction_acquire_frozen_history("ws", &h)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn fragment_scan_cas_checks_real_replacement_recreate_and_metadata_orders() {
        for mutation in ["replace", "recreate", "metadata"] {
            let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
            options.max_connections(1).min_connections(1);
            let db = Database::connect(options).await.unwrap();
            db.execute_unprepared("PRAGMA foreign_keys=OFF")
                .await
                .unwrap();
            let pragma: i64 = db
                .query_one_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    "PRAGMA foreign_keys",
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "foreign_keys")
                .unwrap();
            assert_eq!(pragma, 0, "production single writer uses FK OFF");
            // Seed the legacy binding before frozen lifetime installs its
            // root verification triggers, even when later migrations exist.
            let before_frozen_lifetime = Migrator::migrations()
                .iter()
                .position(|migration| migration.name() == "m20261010_000001_frozen_lifetime")
                .expect("frozen lifetime migration must remain registered");
            Migrator::up(&db, Some(before_frozen_lifetime as u32))
                .await
                .unwrap();
            db.execute_unprepared(
                "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
            )
            .await
            .unwrap();
            db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
            let descriptor = FrozenHistoryRef {
                format: 1,
                manifest_id: "legacy-origin".into(),
                messages: 0,
                identity_sha256: "a".repeat(64),
            };
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES('legacy-origin','ws','thread',?1,0,0,0,0,?1,1)",[descriptor.identity_sha256.clone().into()])).await.unwrap();
            let json = serde_json::json!({"provider":"claude","padding":"x".repeat(150_000),"pioneerContext":{"version":4,"nativeThreadId":"native","acceptedTurnId":"turn","acceptedTurnRevision":1,"acceptedTurnDeleted":false,"contextOwnerThreadId":"thread","contextManifestOwnerThreadId":"thread","contextHistoryJson":serde_json::to_string(&descriptor).unwrap()}}).to_string();
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO thread_cli_runtime_binding(thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,resume_cursor_json,status) VALUES('thread','ws','runtime','claude','native',?1,'active')",[json.clone().into()])).await.unwrap();
            Migrator::up(&db, None).await.unwrap();
            let store = CrudStore::new(db);
            let mut read = LocatorRead {
                kind: 2,
                id: "thread".into(),
                workspace: "ws".into(),
                size: json.len() as i64,
                bytes: vec![],
            };
            assert!(!read.step(&store).await.unwrap());
            assert_eq!(read.bytes.len(), FRAGMENT_BYTES as usize);
            if mutation == "metadata" {
                store.connection.execute_unprepared("UPDATE thread_cli_runtime_binding SET status='active',updated_at=created_at WHERE thread_id='thread'").await.unwrap();
                while read.bytes.len() < json.len() {
                    assert!(!read.step(&store).await.unwrap());
                }
                assert!(read.step(&store).await.unwrap());
            } else {
                // Finish the old fragments before the authoritative write, so
                // final CAS, rather than a size mismatch, must lose the race.
                while read.bytes.len() < json.len() {
                    assert!(!read.step(&store).await.unwrap());
                }
                if mutation == "recreate" {
                    store
                        .connection
                        .execute_unprepared(
                            "DELETE FROM thread_cli_runtime_binding WHERE thread_id='thread'",
                        )
                        .await
                        .unwrap();
                    store.connection.execute_unprepared("INSERT INTO thread_cli_runtime_binding(thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,resume_cursor_json,status,frozen_manifest_id) VALUES('thread','ws','runtime','claude','native','{}','active',NULL)").await.unwrap();
                } else {
                    // Raw equivalent of the cut-over ID/NULL writer; same
                    // updated_at does not participate in sentinel CAS.
                    store.connection.execute_unprepared("UPDATE thread_cli_runtime_binding SET resume_cursor_json='{}',frozen_manifest_id=NULL,updated_at=created_at WHERE thread_id='thread'").await.unwrap();
                }
                assert!(read.step(&store).await.unwrap());
            }
            let current=store.connection.query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT resume_cursor_json,frozen_manifest_id FROM thread_cli_runtime_binding WHERE thread_id='thread'")).await.unwrap().unwrap();
            assert_eq!(
                current
                    .try_get::<Option<String>>("", "frozen_manifest_id")
                    .unwrap(),
                if mutation == "metadata" {
                    Some("legacy-origin".into())
                } else {
                    None
                }
            );
            assert_eq!(
                current.try_get::<String>("", "resume_cursor_json").unwrap(),
                if mutation == "metadata" {
                    json
                } else {
                    "{}".into()
                }
            );
        }
    }

    #[tokio::test]
    async fn published_legacy_root_backfills_candidate_ancestry_then_sweeps_origin_exactly() {
        let (store, h) = fixture(true).await;
        // Schema defaults model the legacy representation: no precredited seal.
        let mut p = FrozenStorageLifetimeProgress::default();
        for _ in 0..4000 {
            p.quantum(&store).await.unwrap();
        }
        for id in ["cp-0", "cp-1"] {
            let row = compaction::checkpoint_topology(&store.connection, id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.row.proof_version, 1);
            assert_eq!(
                value(&store, "compaction_checkpoint_event_input", id).await,
                1,
                "only local selected roles copied, not accumulated coverage"
            );
            assert_eq!(value(&store, "compaction_checkpoint_import", id).await, 0);
            assert_eq!(
                store
                    .compaction_checkpoint_edges(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .event_input_evidence
                    .len(),
                1
            );
        }
        assert!(
            store
                .compaction_acquire_frozen_history("ws", &h)
                .await
                .is_err()
        );
        let rows=store.connection.query_one_raw(Statement::from_string(DbBackend::Sqlite,"SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='origin'")).await.unwrap().unwrap();
        assert_eq!(rows.try_get::<i64>("", "n").unwrap(), 0);
    }
    #[tokio::test]
    async fn historical_edges_adopt_actual_seal_and_expiry_before_legacy_origin_acquire() {
        use super::super::compaction_runner::{PublicationTestPause, arm_publication_test_hook};
        for (graph_reader, control_change) in [(false, false), (true, false), (true, true)] {
            let (store, h) = fixture(true).await;
            let expected = store
                .compaction_checkpoint_edges("cp-1")
                .await
                .unwrap()
                .unwrap();
            assert!(!expected.event_input_evidence.is_empty());
            let mut pause = arm_publication_test_hook(
                &store,
                "op",
                PublicationTestPause::CheckpointOriginAcquire,
            );
            let reader_store = store.clone();
            let reader = tokio::spawn(async move {
                if graph_reader {
                    checkpoint_proofs::prepare_graph(&reader_store, "cp-1").await?;
                }
                reader_store.compaction_checkpoint_edges("cp-1").await
            });
            pause.reached().await; // topology and fresh marker 0, no origin hold
            assert!(store.frozen_readers.lock().unwrap().is_empty());
            if control_change {
                let saved = store
                    .connection
                    .query_one_raw(Statement::from_string(
                        DbBackend::Sqlite,
                        "SELECT state FROM compaction_runner_state WHERE operation_id='op'",
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                let mut state: RunnerState =
                    serde_json::from_str(&saved.try_get::<String>("", "state").unwrap()).unwrap();
                state.generation = 1;
                store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "UPDATE compaction_runner_state SET generation=1,state=?1 WHERE operation_id='op'",
                    [serde_json::to_string(&state).unwrap().into()])).await.unwrap();
            }
            checkpoint_proofs::prepare_graph(&store, "cp-1")
                .await
                .unwrap();
            let mut expiry = prepared_expiry(&store).await;
            assert!(
                expiry
                    .step(&store, &mut SweepAccounting::default())
                    .await
                    .unwrap()
            );
            assert_eq!(
                history::Entity::find_by_id("origin")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap()
                    .expired,
                1
            );
            let mut progress = FrozenStorageLifetimeProgress::default();
            for _ in 0..4000 {
                progress.quantum(&store).await.unwrap();
            }
            let physical = store.connection.query_one_raw(Statement::from_string(DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='origin'")).await.unwrap().unwrap();
            assert_eq!(physical.try_get::<i64>("", "n").unwrap(), 0);
            pause.release();
            let result = reader.await.unwrap();
            if control_change {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("proof operation changed during preparation")
                );
                assert!(store.frozen_readers.lock().unwrap().is_empty());
                assert!(
                    store
                        .compaction_acquire_frozen_history("ws", &h)
                        .await
                        .unwrap_err()
                        .to_string()
                        .starts_with("frozen_input_expired:")
                );
                continue;
            }
            let actual = result.unwrap().unwrap();
            let evidence = |edges: &compaction::CheckpointEdges| {
                let source = |s: &compaction::HistoricalSourceRef| {
                    (s.source_thread.clone(), s.source.clone())
                };
                (
                    edges.coverage.iter().map(source).collect::<Vec<_>>(),
                    edges
                        .replay_aliases
                        .iter()
                        .map(|a| {
                            (
                                source(&a.covered),
                                source(&a.replay),
                                a.tool_item_id.clone(),
                            )
                        })
                        .collect::<Vec<_>>(),
                    edges
                        .event_input_evidence
                        .iter()
                        .map(|e| (source(&e.source), e.role.clone()))
                        .collect::<Vec<_>>(),
                )
            };
            assert_eq!(evidence(&actual), evidence(&expected));
            assert!(
                store
                    .compaction_acquire_frozen_history("ws", &h)
                    .await
                    .unwrap_err()
                    .to_string()
                    .starts_with("frozen_input_expired:")
            );
            assert!(store.frozen_readers.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn concurrent_cold_preparations_share_historical_node_and_exact_sealed_staging() {
        let (store, _) = fixture(true).await;
        let mut first = checkpoint_proofs::GraphPreparation::new("cp-1");
        // Park the first preparation after a real selected staging insert in
        // its common historical cp-0. The other preparation seals that node.
        let mut staged = false;
        for _ in 0..200 {
            first.step(&store).await.unwrap();
            if value(&store, "compaction_checkpoint_event_input", "cp-0").await == 1 {
                staged = true;
                break;
            }
        }
        assert!(staged);
        assert_eq!(
            compaction::checkpoint_row(&store.connection, "cp-0")
                .await
                .unwrap()
                .unwrap()
                .proof_version,
            0
        );
        checkpoint_proofs::prepare_graph(&store, "cp-0")
            .await
            .unwrap();
        assert_eq!(
            compaction::checkpoint_row(&store.connection, "cp-0")
                .await
                .unwrap()
                .unwrap()
                .proof_version,
            1
        );
        for _ in 0..400 {
            if first.finished() {
                break;
            }
            first.step(&store).await.unwrap();
        }
        assert!(
            first.finished(),
            "common sealed node finishes without a retry loop"
        );
        for id in ["cp-0", "cp-1"] {
            assert_eq!(
                value(&store, "compaction_checkpoint_event_input", id).await,
                1
            );
            assert_eq!(
                store
                    .compaction_checkpoint_edges(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .event_input_evidence
                    .len(),
                1
            );
        }
        assert!(store.frozen_readers.lock().unwrap().is_empty());
        // Idempotent restart reuses ready rows and retains the whole previous graph.
        checkpoint_proofs::prepare_graph(&store, "cp-1")
            .await
            .unwrap();
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-0").await,
            1
        );
    }

    #[tokio::test]
    async fn cancelled_legacy_acquire_and_shared_node_control_change_do_not_leak_or_bypass_seal() {
        use super::super::compaction_runner::{PublicationTestPause, arm_publication_test_hook};
        let (store, _) = fixture(true).await;
        let mut pause =
            arm_publication_test_hook(&store, "op", PublicationTestPause::CheckpointOriginAcquire);
        let reader_store = store.clone();
        let reader =
            tokio::spawn(async move { reader_store.compaction_checkpoint_edges("cp-1").await });
        pause.reached().await;
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        drop(pause);
        assert!(store.frozen_readers.lock().unwrap().is_empty());
        // Two preparations have the same closed identity. Control change after
        // one seals the common node must still fence the other's final seal.
        let mut first = checkpoint_proofs::GraphPreparation::new("cp-0");
        let mut staged = false;
        for _ in 0..200 {
            first.step(&store).await.unwrap();
            if value(&store, "compaction_checkpoint_event_input", "cp-0").await == 1 {
                staged = true;
                break;
            }
        }
        assert!(staged);
        checkpoint_proofs::prepare_graph(&store, "cp-0")
            .await
            .unwrap();
        store
            .connection
            .execute_unprepared(
                "UPDATE compaction_runner_state SET generation=1 WHERE operation_id='op'",
            )
            .await
            .unwrap();
        let mut failed = false;
        for _ in 0..200 {
            if first.finished() {
                break;
            }
            if first.step(&store).await.is_err() {
                failed = true;
                break;
            }
        }
        assert!(
            failed,
            "already sealed evidence does not waive the saved control guard"
        );
        drop(first);
        assert!(store.frozen_readers.lock().unwrap().is_empty());
        assert_eq!(
            compaction::checkpoint_row(&store.connection, "cp-0")
                .await
                .unwrap()
                .unwrap()
                .proof_version,
            1
        );
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-0").await,
            1
        );
    }

    #[tokio::test]
    async fn exact_prepared_expiry_revalidates_task_roots_readers_and_expiry_first() {
        let (store, h) = fixture(false).await;
        checkpoint_proofs::prepare_graph(&store, "cp-1")
            .await
            .unwrap();
        store.connection.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal) VALUES('task','ws','thread','thread','agent','failed','fixture','fixture')").await.unwrap();
        store.connection.execute_unprepared("INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run','task','run',1,1,'failed','agent')").await.unwrap();
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) VALUES('run','task','ws','thread',?1,CURRENT_TIMESTAMP,?2)",
            [serde_json::to_string(&h).unwrap().into(), h.manifest_id.clone().into()])).await.unwrap();
        let mut prepared = prepared_expiry(&store).await;
        // Discovery saw a releasing Task. The serialized expiry writer must
        // still observe a newly retaining Task before marking/deleting payload.
        store
            .connection
            .execute_unprepared("UPDATE task SET status='blocked' WHERE id='task'")
            .await
            .unwrap();
        assert!(
            prepared
                .step(&store, &mut SweepAccounting::default())
                .await
                .unwrap()
        );
        assert_eq!(
            history::Entity::find_by_id("origin")
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .expired,
            0
        );
        store
            .connection
            .execute_unprepared("UPDATE task SET status='cancelled' WHERE id='task'")
            .await
            .unwrap();
        let mut prepared = prepared_expiry(&store).await;
        let hold = store
            .compaction_acquire_frozen_history("ws", &h)
            .await
            .unwrap();
        assert!(
            prepared
                .step(&store, &mut SweepAccounting::default())
                .await
                .unwrap()
        );
        assert_eq!(
            history::Entity::find_by_id("origin")
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .expired,
            0
        );
        drop(hold);
        let mut prepared = prepared_expiry(&store).await;
        assert!(
            prepared
                .step(&store, &mut SweepAccounting::default())
                .await
                .unwrap()
        );
        assert!(
            store
                .compaction_acquire_frozen_history("ws", &h)
                .await
                .is_err()
        );
        assert!(store.frozen_readers.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn unused_staging_reset_delete_rollback_and_restart_never_publish_partial_ready() {
        let (store, _) = fixture(false).await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        progress.scope.done = true;
        let header = history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        progress.expiry = Some(ExpiryPreparation::new(header));
        for _ in 0..100 {
            if progress.expiry.as_ref().unwrap().reverse.is_some() {
                break;
            }
            progress.expiry_quantum(&store).await.unwrap();
        }
        assert!(progress.expiry.as_ref().unwrap().reverse.is_some());
        store
            .connection
            .execute_unprepared(
                "ALTER TABLE compaction_checkpoint RENAME TO unavailable_checkpoint",
            )
            .await
            .unwrap();
        progress.phase = 2;
        let error = progress.quantum(&store).await.unwrap_err();
        assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
        let retry = progress.expiry.as_ref().unwrap();
        assert_eq!(retry.header.id, "origin");
        assert_eq!(retry.layout_kind, 0);
        assert!(!retry.operations_done && !retry.checkpoints_done && retry.reverse.is_none());
        assert_eq!(progress.accounting.expired, 0);
        assert!(store.frozen_readers.lock().unwrap().is_empty());
        store
            .connection
            .execute_unprepared(
                "ALTER TABLE unavailable_checkpoint RENAME TO compaction_checkpoint",
            )
            .await
            .unwrap();
        progress.expiry = None;
        checkpoint_proofs::prepare_graph(&store, "cp-1")
            .await
            .unwrap();
        let mut expiry = prepared_expiry(&store).await;
        assert!(
            expiry
                .step(&store, &mut SweepAccounting::default())
                .await
                .unwrap()
        );
        let discard = DiscardPreparation {
            id: "cp-1".into(),
            manifest: "origin".into(),
            status: "failed".into(),
            generation: Some(0),
            reverse: Reachability::new("cp-1", "ws"),
        };
        progress.discard = Some(discard);
        // The final pending reverse node is consumed before its first DB read.
        store
            .connection
            .execute_unprepared(
                "ALTER TABLE compaction_checkpoint RENAME TO unavailable_checkpoint",
            )
            .await
            .unwrap();
        progress.phase = 4;
        let error = progress.quantum(&store).await.unwrap_err();
        assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
        assert!(
            !progress.discard.as_ref().unwrap().reverse.done(),
            "failed reverse read cannot prove negative reachability"
        );
        store
            .connection
            .execute_unprepared(
                "ALTER TABLE unavailable_checkpoint RENAME TO compaction_checkpoint",
            )
            .await
            .unwrap();
        let mut discard = progress.discard.take().unwrap();
        assert_eq!(discard.id, "cp-1");
        while !discard.reverse.done() {
            discard.reverse.step(&store).await.unwrap();
        }
        assert!(!discard.reverse.reachable);
        store.connection.execute_unprepared("CREATE TRIGGER fixture_proof_delete_fault BEFORE DELETE ON compaction_checkpoint_event_input BEGIN SELECT RAISE(ABORT,'fixture delete fault'); END").await.unwrap();
        assert!(discard.cleanup(&store).await.is_err());
        assert_eq!(
            compaction::checkpoint_topology(&store.connection, "cp-1")
                .await
                .unwrap()
                .unwrap()
                .row
                .proof_version,
            1
        );
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-1").await,
            1
        );
        store
            .connection
            .execute_unprepared("DROP TRIGGER fixture_proof_delete_fault")
            .await
            .unwrap();
        discard.cleanup(&store).await.unwrap();
        assert_eq!(
            compaction::checkpoint_topology(&store.connection, "cp-1")
                .await
                .unwrap()
                .unwrap()
                .row
                .proof_version,
            0
        );
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-1").await,
            0
        );
        assert!(
            store.compaction_checkpoint_edges("cp-1").await.is_err(),
            "expired unused origin cannot reconstruct discarded input"
        );
        assert_eq!(value(&store, "compaction_coverage", "cp-1").await, 1);
        assert_eq!(
            compaction::checkpoint_topology(&store.connection, "cp-0")
                .await
                .unwrap()
                .unwrap()
                .row
                .proof_version,
            1,
            "existing incoming dependency preserves ancestor evidence"
        );
    }
    #[tokio::test]
    async fn published_dependency_added_after_discard_discovery_preserves_proofs() {
        let (store, _) = fixture(false).await;
        checkpoint_proofs::prepare_graph(&store, "cp-1")
            .await
            .unwrap();
        let mut expiry = prepared_expiry(&store).await;
        expiry
            .step(&store, &mut SweepAccounting::default())
            .await
            .unwrap();
        let mut discard = DiscardPreparation {
            id: "cp-1".into(),
            manifest: "origin".into(),
            status: "failed".into(),
            generation: Some(0),
            reverse: Reachability::new("cp-1", "ws"),
        };
        while !discard.reverse.done() {
            discard.reverse.step(&store).await.unwrap();
        }
        // A newly saved incoming edge is enough; no publication/state guess.
        store.connection.execute_unprepared("INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) SELECT 'incoming',operation_id,owner,id,2,summary,identity_sha256,selection,projection_version,format_version,'candidate' FROM compaction_checkpoint WHERE id='cp-1'").await.unwrap();
        discard.cleanup(&store).await.unwrap();
        assert_eq!(
            compaction::checkpoint_topology(&store.connection, "cp-1")
                .await
                .unwrap()
                .unwrap()
                .row
                .proof_version,
            1
        );
        assert_eq!(
            value(&store, "compaction_checkpoint_event_input", "cp-1").await,
            1
        );
    }
}
