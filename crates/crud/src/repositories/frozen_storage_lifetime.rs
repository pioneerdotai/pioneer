//! Private progress for the existing frozen-storage worker. No durable queue,
//! global registry, connection, or capacity permit is retained between quanta.
use super::{checkpoint_proofs, compaction};
use crate::{CrudStore, frozen_lifetime::PreparedFrozenRoot};
use anyhow::{Result, ensure};
use pioneer_compaction::runner::{RunnerPhase, RunnerState};
use pioneer_entity::compaction_frozen_history as history;
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, Statement, TransactionTrait};
use std::collections::{BTreeSet, VecDeque};

const FRAGMENT_BYTES: i64 = 64 * 1024;
const META_ROWS: u64 = 64;
// Legacy JSON has no producer byte cap. An oversized record remains unknown
// (and blocks only its retaining scope), rather than allowing one worker job
// to grow without bound. A normal authorized root replacement can verify it.
const LEGACY_LOCATOR_BYTES: i64 = FRAGMENT_BYTES * 128;

/// Owned by one existing maintenance worker, and dropped on cancellation.
/// Creating this value does not enable lifetime maintenance in the Gateway.
#[derive(Default)]
pub struct FrozenStorageLifetimeProgress {
    phase: usize,
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
    physical_after: [Option<(String, i64)>; 2],
    physical_kind: usize,
    sweep: Option<SweepPreparation>,
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
        store.run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            let current = tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                format!("SELECT 1 FROM {table} WHERE {key}=?1 AND workspace_id=?2 AND frozen_manifest_id='' LIMIT 1"),
                [self.id.clone().into(),self.workspace.clone().into()])).await?;
            if current.is_some() {
                prepared.verify(&tx,&self.workspace).await?;
                if self.kind==0 {
                    ensure!(tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                        "SELECT 1 FROM task_run_conversation_snapshot s JOIN task t ON t.id=s.task_id AND t.workspace_id=s.workspace_id WHERE s.run_id=?1 LIMIT 1",[self.id.clone().into()])).await?.is_some(),"legacy Task scope is missing");
                }
                tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    format!("UPDATE {table} SET frozen_manifest_id=?3 WHERE {key}=?1 AND workspace_id=?2 AND frozen_manifest_id=''"),
                    [self.id.clone().into(),self.workspace.clone().into(),prepared.locator().into()])).await?;
            }
            tx.commit().await?; Ok(())
        }).await?;
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
            self.reachable=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT 1 FROM compaction_checkpoint c JOIN compaction_operation o ON o.id=c.operation_id AND o.owner=c.owner WHERE c.id=?1 AND (c.status='applied' OR (c.status='retained' AND o.status='completed')) LIMIT 1",[id.clone().into()])).await?.is_some();
            self.current = Some((id, 0, String::new(), BTreeSet::new()));
            return Ok(());
        }
        let (id, branch, after, children) = self.current.as_mut().expect("reverse current");
        let topology = compaction::checkpoint_row(&store.connection, id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("reverse checkpoint removed"))?;
        let row = &topology;
        let sql = if *branch == 0 {
            "SELECT p.id,p.owner,c.workspace_id,c.thread_id FROM compaction_checkpoint p LEFT JOIN compaction_context c ON c.owner=p.owner WHERE p.previous=?1 AND p.id>?2 ORDER BY p.id LIMIT ?3"
        } else {
            "SELECT v.checkpoint_id AS id,v.source_scope,v.source_version,c.workspace_id FROM compaction_coverage v LEFT JOIN compaction_checkpoint p ON p.id=v.checkpoint_id LEFT JOIN compaction_context c ON c.owner=p.owner WHERE v.source_id=?1 AND v.source_scope LIKE 'checkpoint:%' AND v.checkpoint_id>?2 ORDER BY v.checkpoint_id LIMIT ?3"
        };
        let page = store
            .connection
            .query_all_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                [id.clone().into(), after.clone().into(), 1_i64.into()],
            ))
            .await?;
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
    async fn step(&mut self, store: &CrudStore) -> Result<bool> {
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
            let row=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT b.operation_id,o.owner,o.status,r.generation,CASE WHEN length(CAST(r.state AS BLOB))<=?3 THEN r.state END AS state,x.workspace_id FROM compaction_operation_projection b LEFT JOIN compaction_operation o ON o.id=b.operation_id LEFT JOIN compaction_context x ON x.owner=o.owner LEFT JOIN compaction_runner_state r ON r.operation_id=o.id WHERE b.manifest_id=?1 AND b.operation_id>?2 ORDER BY b.operation_id LIMIT 1",[self.header.id.clone().into(),self.operations_after.clone().into(),(256_i64*1024).into()])).await?;
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
                ensure!(
                    terminal(&status, generation, state.as_deref())?,
                    "origin operation is active or terminal state is unproved"
                );
                self.operations_after = id.clone();
                let binding = TerminalBinding {
                    id,
                    owner,
                    status,
                    generation,
                };
                store.run_serialized_write(||async{
                    let tx=store.connection.begin().await?;
                    tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,r#"UPDATE compaction_operation_projection SET terminal_generation=?3 WHERE operation_id=?1 AND manifest_id=?2 AND EXISTS(SELECT 1 FROM compaction_operation o JOIN compaction_runner_state r ON r.operation_id=o.id JOIN compaction_context x ON x.owner=o.owner WHERE o.id=?1 AND o.owner=?4 AND o.status=?5 AND r.generation=?3 AND x.workspace_id=?6)"#,[binding.id.clone().into(),self.header.id.clone().into(),binding.generation.into(),binding.owner.clone().into(),binding.status.clone().into(),self.header.workspace_id.clone().into()])).await?;
                    tx.commit().await?;Ok(())
                }).await?;
                return Ok(false);
            }
            self.operations_done = true;
            return Ok(false);
        }
        if let Some(reverse) = self.reverse.as_mut() {
            reverse.step(store).await?;
            if reverse.done() {
                ensure!(
                    !reverse.reachable,
                    "unsealed published ancestry retains origin"
                );
                self.reverse = None;
            }
            return Ok(false);
        }
        if !self.checkpoints_done {
            let row=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT c.id FROM compaction_checkpoint c JOIN compaction_operation_projection b ON b.operation_id=c.operation_id WHERE b.manifest_id=?1 AND c.id>?2 AND c.proof_version<>1 ORDER BY c.id LIMIT 1",[self.header.id.clone().into(),self.checkpoint_after.clone().into()])).await?;
            if let Some(row) = row {
                let id: String = row.try_get("", "id")?;
                self.checkpoint_after = id.clone();
                self.reverse = Some(Reachability::new(&id, &self.header.workspace_id));
                return Ok(false);
            }
            self.checkpoints_done = true;
            return Ok(false);
        }
        expire(store, &self.header).await?;
        Ok(true)
    }
}

// Indexed roots only: JSON is authoritative at cutover writers and sentinel CAS,
// never parsed in expiry/sweep. A missing Task is unknown. A completed Task
// does not retain an input even when its old snapshot scope is inconsistent;
// unresolved checkpoint ownership is a separate prerequisite. Retaining
// unknown Task inputs block both known workspace interpretations.
fn unknown_predicate(alias: &str) -> String {
    format!(
        r#"
EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN task t ON t.id=s.task_id WHERE s.frozen_manifest_id='' AND (s.workspace_id={alias}.workspace_id OR t.workspace_id={alias}.workspace_id OR s.workspace_id='' OR t.workspace_id='') AND (t.id IS NULL OR t.status<>'completed'))
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
OR EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN task t ON t.id=s.task_id WHERE s.frozen_manifest_id={alias}.id AND (t.id IS NULL OR t.status<>'completed'))
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
async fn expire(store: &CrudStore, header: &history::Model) -> Result<()> {
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;
        let h=history::Entity::find_by_id(&header.id).one(&tx).await?.ok_or_else(||anyhow::anyhow!("expiry header missing"))?;
        crate::frozen_lifetime::logical_bounds(&h)?;
        ensure!(h.ready==1 && h==*header,"expiry header changed");
        if store.frozen_readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner).contains_key(&h.id) {tx.commit().await?;return Ok(());}
        let guard=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,format!("SELECT 1 FROM compaction_frozen_history h WHERE h.id=?1 AND ({}) LIMIT 1",root_predicate("h")),[h.id.clone().into()])).await?;
        if guard.is_some() {tx.commit().await?;return Ok(());}
        // The per-binding terminal generation was verified in bounded steps.
        // All state/status writes invalidate it before becoming visible.
        let active=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,r#"
SELECT 1 FROM compaction_operation_projection b LEFT JOIN compaction_operation o ON o.id=b.operation_id
LEFT JOIN compaction_runner_state r ON r.operation_id=o.id LEFT JOIN compaction_context x ON x.owner=o.owner
WHERE b.manifest_id=?1 AND (o.id IS NULL OR o.status NOT IN ('completed','failed','cancelled','stale')
 OR r.operation_id IS NULL OR b.terminal_generation IS NULL OR b.terminal_generation<>r.generation
 OR x.workspace_id IS NULL OR x.workspace_id<>?2) LIMIT 1"#,[h.id.clone().into(),h.workspace_id.clone().into()])).await?;
        if active.is_some(){tx.commit().await?;return Ok(());}
        let unsealed_public=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT 1 FROM compaction_operation_projection b JOIN compaction_checkpoint c ON c.operation_id=b.operation_id JOIN compaction_operation o ON o.id=b.operation_id WHERE b.manifest_id=?1 AND c.proof_version<>1 AND (c.status='applied' OR (c.status='retained' AND o.status='completed')) LIMIT 1",[h.id.clone().into()])).await?;
        if unsealed_public.is_some(){tx.commit().await?;return Ok(());}
        let transitions=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_frozen_layout l WHERE (l.manifest_id=?1 OR l.candidate=?1) AND (l.active<>1 OR l.pending<>0 OR l.failed<>0 OR l.copy_next<COALESCE(l.copy_to,0)) LIMIT 1",[h.id.clone().into()])).await?;
        if transitions.is_some() {tx.commit().await?;return Ok(());}
        tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_frozen_history SET expired=1 WHERE id=?1 AND expired=0 AND ready=1 AND next_ordinal=message_count AND next_import=import_count",[h.id.clone().into()])).await?;
        tx.commit().await?;Ok(())
    }).await
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
struct PhysicalKey {
    manifest: String,
    kind: i64,
    ordinal: i64,
    bytes: i64,
}
struct SweepPreparation {
    key: PhysicalKey,
    checks: VecDeque<String>,
    seen: BTreeSet<String>,
    current: Option<LayoutCheck>,
    after: (i64, String),
    discovered: bool,
}
impl SweepPreparation {
    fn new(key: PhysicalKey) -> Self {
        Self {
            checks: VecDeque::from([key.manifest.clone()]),
            key,
            seen: BTreeSet::new(),
            current: None,
            after: (-1, String::new()),
            discovered: false,
        }
    }
    async fn step(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(check) = self.current.as_mut() {
            check.step(store).await?;
            if check.done {
                self.current = None;
            }
            return Ok(false);
        }
        if let Some(id) = self.checks.pop_front() {
            if self.seen.insert(id.clone()) {
                self.current = Some(LayoutCheck::new(store, &id, self.key.kind).await?);
            }
            return Ok(false);
        }
        if !self.discovered {
            // Discover the entire known backing set, not only overlapping
            // ordinals: a corrupt logical layout protects its whole container.
            let page=store.connection.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT start,manifest_id FROM compaction_frozen_span WHERE source_manifest=?1 AND kind=?2 AND (start>?3 OR (start=?3 AND manifest_id>?4)) ORDER BY start,manifest_id LIMIT ?5",[self.key.manifest.clone().into(),self.key.kind.into(),self.after.0.into(),self.after.1.clone().into(),(META_ROWS as i64).into()])).await?;
            for row in &page {
                let start: i64 = row.try_get("", "start")?;
                let id: String = row.try_get("", "manifest_id")?;
                self.after = (start, id.clone());
                self.checks.push_back(id);
            }
            self.discovered = page.len() < META_ROWS as usize;
            return Ok(false);
        }
        delete_key(store, &self.key).await?;
        Ok(true)
    }
}

/// The same exact-key guard is used by the existing duplicate-prefix cleanup.
/// An available physical header retains only its logical direct prefix.
async fn delete_key(store: &CrudStore, key: &PhysicalKey) -> Result<()> {
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            delete_free_key(&tx, &key.manifest, key.kind, key.ordinal, key.bytes).await?;
            tx.commit().await?;
            Ok(())
        })
        .await
}
pub(super) async fn delete_free_key<C: ConnectionTrait>(
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
    /// Exactly one phase step. Payload reads are one fragment/page per step;
    /// rotating keysets revisit previously held rows after roots disappear.
    pub async fn quantum(&mut self, store: &CrudStore) -> Result<bool> {
        let store = store.with_maintenance_access();
        let phase = self.phase;
        self.phase = (self.phase + 1) % 6;
        if matches!(phase, 2 | 3 | 4) && !self.scope.done {
            return Ok(false);
        }
        let result = match phase {
            0 => self.locator_quantum(&store).await,
            1 => self.proof_quantum(&store).await,
            2 => self.expiry_quantum(&store).await,
            3 => self.sweep_quantum(&store).await,
            4 => self.discard_quantum(&store).await,
            _ => self.scope.step(&store).await,
        };
        if result.is_err() {
            // Poison advances discovery instead of spinning on the same node.
            // Durable unknown/readiness markers continue to fail closed.
            match phase {
                0 => {
                    self.locator = None;
                }
                1 => {
                    self.proof = None;
                }
                2 => {
                    self.expiry = None;
                }
                3 => {
                    self.sweep = None;
                }
                4 => {
                    self.discard = None;
                }
                _ => {}
            }
        }
        result
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
            }
            return Ok(true);
        }
        let kind = self.locator_kind;
        self.locator_kind = (kind + 1) % 4;
        let (table, key, field) = ROOTS[kind];
        if self.locator_pending[kind].is_empty() {
            // Page actual PK metadata, not a filtered LIMIT that could walk an
            // unbounded number of already-verified records in one read.
            let page=store.connection.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                format!("SELECT {key} AS id,workspace_id,frozen_manifest_id,CASE WHEN frozen_manifest_id='' THEN length(CAST({field} AS BLOB)) ELSE 0 END AS size FROM {table} WHERE {key}>?1 ORDER BY {key} LIMIT 64"),[self.locators[kind].clone().into()])).await?;
            if page.is_empty() {
                self.locators[kind].clear();
                return Ok(false);
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
            }
            return Ok(true);
        }
        // Page headers before choosing public roots; no full filtered scan
        // when all old checkpoints have already been sealed.
        if self.proof_pending.is_empty() {
            let page=store.connection.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT c.id,c.proof_version,c.status,o.status AS operation_status,b.operation_id AS bound FROM compaction_checkpoint c LEFT JOIN compaction_operation o ON o.id=c.operation_id AND o.owner=c.owner LEFT JOIN compaction_operation_projection b ON b.operation_id=c.operation_id WHERE c.id>?1 ORDER BY c.id LIMIT 64",[self.proof_after.clone().into()])).await?;
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
        let id = self.proof_pending.pop_front().expect("proof page");
        self.proof = Some(checkpoint_proofs::GraphPreparation::new(&id));
        Ok(true)
    }

    async fn expiry_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(expiry) = self.expiry.as_mut() {
            if expiry.step(store).await? {
                self.expiry = None;
            }
            return Ok(true);
        }
        let row=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT id FROM compaction_frozen_history WHERE expired=0 AND ready=1 AND id>?1 ORDER BY id LIMIT 1",[self.expiry_after.clone().into()])).await?;
        if let Some(row) = row {
            let id: String = row.try_get("", "id")?;
            self.expiry_after = id.clone();
            let header = history::Entity::find_by_id(&id)
                .one(&store.connection)
                .await?
                .ok_or_else(|| anyhow::anyhow!("expiry discovery changed"))?;
            ensure!(counter_health(&header), "corrupt expiry counters");
            self.expiry = Some(ExpiryPreparation::new(header));
            Ok(true)
        } else {
            self.expiry_after.clear();
            Ok(false)
        }
    }
    async fn sweep_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(sweep) = self.sweep.as_mut() {
            if sweep.step(store).await? {
                self.sweep = None;
            }
            return Ok(true);
        }
        let kind = self.physical_kind;
        self.physical_kind = (kind + 1) % 2;
        let table = if kind == 0 {
            "compaction_frozen_message_data"
        } else {
            "compaction_frozen_import_data"
        };
        let (manifest, ordinal) = self.physical_after[kind].clone().unwrap_or_default();
        let extra = if kind == 1 { "message_ordinal" } else { "0" };
        let field = if kind == 1 {
            "proof_json"
        } else {
            "reference_json"
        };
        let (predicate, values) = if self.physical_after[kind].is_some() {
            (
                "(manifest_id,ordinal)>(?1,?2)",
                vec![manifest.into(), ordinal.into()],
            )
        } else {
            ("1", vec![])
        };
        let row=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            format!("SELECT manifest_id,ordinal,bytes,{extra} AS message_ordinal,length(CAST({field} AS BLOB)) AS actual_bytes FROM {table} WHERE {predicate} ORDER BY manifest_id,ordinal LIMIT 1"),values)).await?;
        if let Some(row) = row {
            let manifest: String = row.try_get("", "manifest_id")?;
            let ordinal: i64 = row.try_get("", "ordinal")?;
            let bytes: i64 = row.try_get("", "bytes")?;
            self.physical_after[kind] = Some((manifest.clone(), ordinal));
            ensure!(
                ordinal >= 0
                    && row.try_get::<i64>("", "message_ordinal")? >= 0
                    && bytes == row.try_get::<i64>("", "actual_bytes")?
                    && (0..=1024 * 1024).contains(&bytes),
                "physical discovery corrupt key/bytes"
            );
            self.sweep = Some(SweepPreparation::new(PhysicalKey {
                manifest,
                kind: kind as i64,
                ordinal,
                bytes,
            }));
            Ok(true)
        } else {
            self.physical_after[kind] = None;
            Ok(false)
        }
    }
    async fn discard_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(discard) = self.discard.as_mut() {
            if !discard.reverse.done() {
                discard.reverse.step(store).await?;
                return Ok(true);
            }
            ensure!(
                !discard.reverse.reachable,
                "published ancestry proofs retained"
            );
            discard.cleanup(store).await?;
            self.discard = None;
            return Ok(true);
        }
        let row=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,r#"
SELECT c.id,c.owner,c.operation_id,c.identity_sha256,o.status,r.generation,r.state,b.manifest_id
FROM compaction_checkpoint c JOIN compaction_operation o ON o.id=c.operation_id AND o.owner=c.owner
JOIN compaction_operation_projection b ON b.operation_id=o.id JOIN compaction_frozen_history h ON h.id=b.manifest_id
JOIN compaction_runner_state r ON r.operation_id=o.id
WHERE c.id>?1 AND h.expired=1 AND o.status IN ('failed','cancelled','stale') AND c.status<>'applied'
 AND length(CAST(r.state AS BLOB))<=?2
ORDER BY c.id LIMIT 1"#,[self.discard_after.clone().into(),(256_i64*1024).into()])).await?;
        let Some(row) = row else {
            self.discard_after.clear();
            return Ok(false);
        };
        let id: String = row.try_get("", "id")?;
        self.discard_after = id.clone();
        let manifest: String = row.try_get("", "manifest_id")?;
        let status: String = row.try_get("", "status")?;
        let generation: Option<i64> = row.try_get("", "generation")?;
        let state: String = row.try_get("", "state")?;
        ensure!(
            terminal(&status, generation, Some(&state))?,
            "staging discard state unproved"
        );
        let scope = compaction::checkpoint_row(&store.connection, &id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("discard checkpoint scope missing"))?;
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
            if let Some(row)=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,format!("SELECT rowid AS key,{size} AS bytes FROM {table} WHERE checkpoint_id=?1 ORDER BY rowid LIMIT 1"),[self.id.clone().into()])).await? {
                let bytes:i64=row.try_get("","bytes")?;
                ensure!((0..=1024*1024).contains(&bytes),"staging row size unsupported");
                if total+bytes<=1024*1024 { total+=bytes; keys.push((table,row.try_get::<i64>("","key")?,size,bytes)); }
            }
        }
        store.run_serialized_write(||async{
            let tx=store.connection.begin().await?;
            if store.frozen_readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner).contains_key(&self.manifest){tx.commit().await?;return Ok(());}
            let eligible=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,r#"
SELECT 1 FROM compaction_checkpoint c JOIN compaction_operation o ON o.id=c.operation_id
JOIN compaction_runner_state r ON r.operation_id=o.id JOIN compaction_operation_projection b ON b.operation_id=o.id
JOIN compaction_frozen_history h ON h.id=b.manifest_id
WHERE c.id=?1 AND c.status<>'applied' AND o.status=?2 AND r.generation IS ?3 AND h.id=?4 AND h.expired=1
AND NOT EXISTS(SELECT 1 FROM compaction_checkpoint p WHERE p.previous=c.id)
AND NOT EXISTS(SELECT 1 FROM compaction_coverage v WHERE v.source_id=c.id AND v.source_scope LIKE 'checkpoint:%')
LIMIT 1"#,[self.id.clone().into(),self.status.clone().into(),self.generation.into(),self.manifest.clone().into()])).await?;
            if eligible.is_some(){
                // Demote first, commit the marker and bounded rows atomically.
                // A crash can leave marker 0, never ready over partial proofs.
                tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_checkpoint SET proof_version=0 WHERE id=?1 AND proof_version=1",[self.id.clone().into()])).await?;
                for (table,key,size,bytes) in &keys {
                    tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,format!("DELETE FROM {table} WHERE checkpoint_id=?1 AND rowid=?2 AND ({size})=?3"),[self.id.clone().into(),(*key).into(),(*bytes).into()])).await?;
                }
            }
            tx.commit().await?;Ok(())
        }).await?;
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
        let sql = match self.family {
            0 => {
                "SELECT id AS manifest_id,0 AS kind FROM compaction_frozen_history WHERE id>?1 ORDER BY id LIMIT 1"
            }
            1 => {
                "SELECT manifest_id,kind FROM compaction_frozen_layout WHERE manifest_id>?1 OR (manifest_id=?1 AND kind>?2) ORDER BY manifest_id,kind LIMIT 1"
            }
            2 => {
                "SELECT manifest_id,0 AS kind FROM compaction_frozen_message_data WHERE manifest_id>?1 ORDER BY manifest_id,ordinal LIMIT 1"
            }
            3 => {
                "SELECT manifest_id,1 AS kind FROM compaction_frozen_import_data WHERE manifest_id>?1 ORDER BY manifest_id,ordinal LIMIT 1"
            }
            4 => {
                "SELECT id AS manifest_id,0 AS kind FROM compaction_operation WHERE id>?1 ORDER BY id LIMIT 1"
            }
            _ => {
                "SELECT id AS manifest_id,0 AS kind FROM compaction_checkpoint WHERE id>?1 ORDER BY id LIMIT 1"
            }
        };
        let sql = if !self.started {
            sql.replace("WHERE id>?1", "WHERE ?1 IS NOT NULL")
                .replace(
                    "WHERE manifest_id>?1 OR (manifest_id=?1 AND kind>?2)",
                    "WHERE ?1 IS NOT NULL AND ?2 IS NOT NULL",
                )
                .replace("WHERE manifest_id>?1", "WHERE ?1 IS NOT NULL")
        } else {
            sql.to_owned()
        };
        let mut values = vec![self.after.clone().into()];
        if self.family == 1 {
            values.push(self.kind.into());
        }
        let row = store
            .connection
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                values,
            ))
            .await?;
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
            let sql = if self.family == 4 {
                "SELECT x.workspace_id FROM compaction_operation o JOIN compaction_context x ON x.owner=o.owner WHERE o.id=?1 AND x.workspace_id<>'' UNION ALL SELECT h.workspace_id FROM compaction_operation_projection b JOIN compaction_frozen_history h ON h.id=b.manifest_id WHERE b.operation_id=?1 AND h.workspace_id<>'' LIMIT 1"
            } else {
                "SELECT x.workspace_id FROM compaction_checkpoint c JOIN compaction_context x ON x.owner=c.owner WHERE c.id=?1 AND x.workspace_id<>'' UNION ALL SELECT h.workspace_id FROM compaction_checkpoint c JOIN compaction_operation_projection b ON b.operation_id=c.operation_id JOIN compaction_frozen_history h ON h.id=b.manifest_id WHERE c.id=?1 AND h.workspace_id<>'' LIMIT 1"
            };
            ensure!(
                store
                    .connection
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        sql,
                        [id.clone().into()]
                    ))
                    .await?
                    .is_some(),
                "legacy operation/checkpoint scope unknown; cleanup waits for repair"
            );
            self.after = id;
            self.kind = kind;
            self.started = true;
            return Ok(true);
        }
        let known=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,r#"
SELECT h.workspace_id FROM compaction_frozen_history h WHERE h.id=?1 AND h.workspace_id<>''
UNION ALL SELECT h.workspace_id FROM compaction_frozen_layout l JOIN compaction_frozen_history h ON h.id=l.manifest_id WHERE l.candidate=?1 AND h.workspace_id<>''
UNION ALL SELECT h.workspace_id FROM compaction_frozen_layout l JOIN compaction_frozen_history h ON h.id=l.candidate WHERE l.manifest_id=?1 AND h.workspace_id<>''
UNION ALL SELECT h.workspace_id FROM compaction_frozen_span s JOIN compaction_frozen_history h ON h.id=s.manifest_id WHERE s.source_manifest=?1 AND h.workspace_id<>''
UNION ALL SELECT h.workspace_id FROM compaction_frozen_span s JOIN compaction_frozen_history h ON h.id=s.source_manifest WHERE s.manifest_id=?1 AND h.workspace_id<>''
LIMIT 1"#,[id.clone().into()])).await?;
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
    async fn fixture(published: bool) -> (CrudStore, FrozenHistoryRef) {
        let db = Database::connect("sqlite::memory:").await.unwrap();
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
                    checkpoint: "cp-1".into(),
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
                id: format!("cp-{i}"),
                operation_id: "op".into(),
                owner: "owner".into(),
                previous: (i > 0).then(|| "cp-0".into()),
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
            assert!(!p.step(store).await.unwrap());
        }
        panic!("bounded expiry preparation must finish");
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
            let db = Database::connect("sqlite::memory:").await.unwrap();
            Migrator::up(&db, Some((Migrator::migrations().len() - 1) as u32))
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
            assert!(expiry.step(&store).await.unwrap());
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
    async fn exact_prepared_expiry_revalidates_reader_first_and_expiry_first() {
        let (store, h) = fixture(false).await;
        checkpoint_proofs::prepare_graph(&store, "cp-1")
            .await
            .unwrap();
        let mut prepared = prepared_expiry(&store).await;
        let hold = store
            .compaction_acquire_frozen_history("ws", &h)
            .await
            .unwrap();
        assert!(prepared.step(&store).await.unwrap());
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
        assert!(prepared.step(&store).await.unwrap());
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
        checkpoint_proofs::prepare_graph(&store, "cp-1")
            .await
            .unwrap();
        let mut expiry = prepared_expiry(&store).await;
        assert!(expiry.step(&store).await.unwrap());
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
        expiry.step(&store).await.unwrap();
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
