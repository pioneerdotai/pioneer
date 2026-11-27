//! Bounded progress for the existing frozen-storage worker. Physical eligibility
//! is durable; no connection or capacity permit is retained between quanta.
use crate::checkpoint_proofs;
use crate::frozen_expiry::{ExpiryPreparation, ExpiryStep, LayoutCheck, terminal_runner};
use crate::frozen_geometry::{
    CheckedGeometry, PhysicalDeleteResult, SWEEP_BYTES, SWEEP_ROWS, geometry_values,
    physical_delete, physical_table, sweep_guard, sweep_validation,
};
#[cfg(test)]
use crate::frozen_lifetime::PreparedFrozenRoot;
use crate::frozen_reachability::Reachability;
#[cfg(test)]
use crate::frozen_storage_reads::DISCARD_PAGE_KEY_BYTES;
use crate::store::CompactionStore;
use crate::sweep_work as frozen_sweep_work;
use anyhow::{Result, ensure};
#[cfg(test)]
use pioneer_compaction::runner::{RunnerPhase, RunnerState};
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_context as context,
    compaction_coverage as coverage, compaction_frozen_history as history,
    compaction_frozen_layout as layout, compaction_frozen_span as span,
    compaction_operation as operation, compaction_operation_projection as projection,
    compaction_runner_state as runner, task, task_run_conversation_snapshot as task_input,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, JoinType, Order, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    QueryTrait, Statement, TransactionTrait,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::{Duration, Instant},
};

use crate::frozen_storage_reads as read_queries;
use read_queries::discard_preflight;

const META_ROWS: u64 = 64;

/// Owned by one existing maintenance worker, and dropped on cancellation.
/// Each discovery family reaches EOF once per pass; completed families yield
/// while the other bounded preparations finish.
#[derive(Default)]
pub struct FrozenStorageLifetimeProgress {
    phase: usize,
    // None is the recovery inventory; Some is a bounded, acknowledged set of
    // changed identities. It never substitutes for final writer authority.
    target_manifests: Option<Vec<String>>,
    target_checkpoints: Option<Vec<String>>,
    target_backings: Option<Vec<String>>,
    retries_only: bool,
    release_only: bool,
    pass_done: [bool; 4],
    // Reconciliation/sealing can release holds discovered earlier this pass.
    pass_changed: bool,
    // Periodic admission for already completed cleanup, independent of active
    // continuations and of the worker's separate conversion discovery EOF.
    cleanup_revisit_at: Option<Instant>,
    proof_after: String,
    proof_upper: Option<String>,
    proof_pending: VecDeque<String>,
    proof: Option<checkpoint_proofs::GraphPreparation>,
    expiry_after: String,
    expiry_upper: Option<String>,
    expiry_pending: VecDeque<read_queries::ExpiryHeader>,
    expiry: Option<ExpiryPreparation>,
    sweep: Option<SweepPreparation>,
    sweep_pending: VecDeque<SweepPreparation>,
    sweep_discover: bool,
    sweep_kind: i64,
    sweep_fanout: bool,
    sweep_round: [frozen_sweep_work::ServiceRound; 2],
    accounting: SweepAccounting,
    discard_after: String,
    discard_upper: Option<String>,
    discard_pending: VecDeque<(Option<String>, bool)>,
    discard: Option<DiscardPreparation>,
    conversion: crate::frozen_storage::FrozenConversionProgress,
}
async fn expiry_step(
    preparation: &mut ExpiryPreparation,
    store: &CompactionStore,
    accounting: &mut SweepAccounting,
) -> Result<bool> {
    let step = preparation
        .step(&store.connection, &store.frozen_readers)
        .await?;
    match step {
        ExpiryStep::More => return Ok(false),
        ExpiryStep::Held => accounting.held += 1,
        ExpiryStep::Expired => accounting.expired += 1,
        ExpiryStep::ProtectedByCheckpoint => {}
    }
    Ok(true)
}

#[cfg(test)]
async fn expire(store: &CompactionStore, header: &history::Model) -> Result<bool> {
    crate::frozen_expiry::publish_expiry(&store.connection, &store.frozen_readers, header).await
}

fn counter_health(h: &history::Model) -> bool {
    crate::frozen::header_bounds(h).is_ok()
}
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

#[derive(Debug, PartialEq, Eq)]
enum SweepStep {
    Blocked,
    Continue,
    Complete,
    Retry,
}

struct SweepPreparation {
    // A real fanout turn overlapped this visit. Its old preparation may have
    // observed a fence released by ACK without another Work revision change.
    // Consumed on successful finish; newly admitted visits start unmarked.
    invalidated: bool,
    work: Option<frozen_sweep_work::Work>,
    poison: bool,
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
            work: None,
            invalidated: false,
            poison: false,
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
    async fn prepare(&mut self, store: &CompactionStore) -> Result<bool> {
        if self.span_until.is_none() {
            // The immutable insertion ordinal freezes the indexed frontier
            // for this visit. New consumers are checked again before deletion.
            self.span_until = Some(
                span::Entity::find()
                    .select_only()
                    .column(span::Column::InsertionOrdinal)
                    .order_by_desc(span::Column::InsertionOrdinal)
                    .into_tuple::<i64>()
                    .one(&store.connection)
                    .await?
                    .unwrap_or(0),
            );
            return Ok(false);
        }
        if let Some((check, _)) = self.current.as_mut() {
            check.step(&store.connection).await?;
            if check.done() {
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
                let l = crate::frozen::find_layout(id, self.kind)
                    .one(&store.connection)
                    .await?;
                let scan = crate::frozen::LayoutScan::new(h.clone(), self.kind, self.kind + 1)?;
                self.current = Some((LayoutCheck::from_scan(scan), CheckedGeometry { h, l }));
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
                .filter(span::Column::InsertionOrdinal.lte(self.span_until.expect("span frontier")))
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
                .filter(span::Column::InsertionOrdinal.lte(self.span_until.expect("span frontier")))
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
        self.prepared = Some(geometry_values(self.checked.values())?);
        self.checked.clear();
        self.reverse = Vec::new();
        Ok(true)
    }
    async fn step(
        &mut self,
        store: &CompactionStore,
        accounting: &mut SweepAccounting,
    ) -> Result<SweepStep> {
        if self.prepared.is_none() {
            if self.prepare(store).await? && self.free.is_empty() {
                accounting.held += 1;
                return Ok(self.completed_step());
            }
            return Ok(SweepStep::Continue);
        }
        let Some(&(start, end)) = self.free.front() else {
            return Ok(self.completed_step());
        };
        let (data, payload) = physical_table(self.kind);
        let import_key = if self.kind == 1 {
            "message_ordinal"
        } else {
            "0::bigint"
        };
        let page = store.connection.query_all_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            format!("SELECT ordinal, bytes, octet_length({payload})::bigint AS actual, {import_key} AS target FROM {data} WHERE manifest_id=$1 AND ordinal>=$2 AND ordinal<$3 ORDER BY ordinal LIMIT $4"),
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
                self.poison = true;
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
            return Ok(self.completed_step());
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
            let fence = self.work.as_ref().map(frozen_sweep_work::Work::fence);
            let committed = store
                .retry_write_on_conflict(|| async {
                    let guard = guard.clone();
                    let delete = delete.clone();
                    let tx = store.connection.begin().await?;
                    if let Some(fence) = &fence {
                        if tx.query_one_raw(fence.clone()).await?.is_none() {
                            tx.commit().await?;
                            return Ok(None);
                        }
                    }
                    // A new retaining FK or layout publication must follow
                    // the backing header lock. Check current geometry only
                    // after PostgreSQL has granted that lock.
                    let backing = history::Entity::find_by_id(&self.manifest)
                        .select_only()
                        .column(history::Column::Id)
                        .lock_exclusive()
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?;
                    if backing.is_none() || tx.query_one_raw(guard).await?.is_none() {
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
                // Refusal is never successful completion of a generation.
                return Ok(SweepStep::Blocked);
            };
            accounting.acknowledge(self.kind, deleted).await;
        }
        self.next = next;
        if self.next >= end {
            self.free.pop_front();
        }
        Ok(self.completed_step())
    }
    fn completed_step(&self) -> SweepStep {
        if !self.free.is_empty() {
            SweepStep::Continue
        } else if self.poison {
            SweepStep::Retry
        } else {
            SweepStep::Complete
        }
    }

    fn completion_guard(&self) -> Statement {
        sweep_validation(
            &self.manifest,
            self.kind,
            self.prepared.as_deref().expect("prepared"),
            None,
        )
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
#[cfg(test)]
struct PhysicalKey {
    manifest: String,
    kind: i64,
    ordinal: i64,
    bytes: i64,
}
#[cfg(test)]
async fn delete_key(store: &CompactionStore, key: &PhysicalKey) -> Result<()> {
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
    let health = crate::frozen_geometry::healthy("h");
    let unknown = crate::frozen_expiry::unknown_predicate("p");
    // Original exact-key retention oracle for existing focused tests.
    // Production uses the common interval guard plus physical_delete.
    let sql = format!(
        r#"DELETE FROM {data} AS d WHERE d.manifest_id=$1 AND d.ordinal=$2 AND d.bytes=$3
AND octet_length(d.{payload}::text)=d.bytes {import_key}
AND EXISTS(SELECT 1 FROM compaction_frozen_history p WHERE p.id=d.manifest_id)
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history p WHERE p.id=d.manifest_id AND ({unknown}))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=d.manifest_id AND (NOT ({health}) OR (h.expired=0 AND d.ordinal<CASE h.ready WHEN 1 THEN h.{hk} ELSE h.{nk} END AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=$4 AND l.active=1))))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s LEFT JOIN compaction_frozen_history h ON h.id=s.manifest_id LEFT JOIN compaction_frozen_layout l ON l.manifest_id=h.id AND l.kind=s.kind WHERE s.source_manifest=d.manifest_id AND s.kind=$4 AND (h.id IS NULL OR NOT ({health}) OR l.manifest_id IS NULL OR l.failed<>0 OR (h.expired=0 AND l.active=1 AND s.start<=d.ordinal AND d.ordinal<s."end" AND d.ordinal<CASE h.ready WHEN 1 THEN h.{hk} ELSE h.{nk} END)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=d.manifest_id AND l.kind=$4 AND (l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.candidate=d.manifest_id AND l.kind=$4 AND (l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s JOIN compaction_frozen_layout l ON l.manifest_id=s.manifest_id AND l.kind=s.kind WHERE s.source_manifest=d.manifest_id AND s.kind=$4 AND (l.failed<>0 OR l.active<>1 OR l.copy_next<COALESCE(l.copy_to,0)))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=d.manifest_id AND h.storage_registered<>0 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=$4))
AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s JOIN compaction_frozen_history h ON h.id=s.manifest_id WHERE s.source_manifest=d.manifest_id AND s.kind=$4 AND h.storage_registered<>0 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=h.id AND l.kind=$4))
"#
    );
    Ok(db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [manifest.into(), ordinal.into(), bytes.into(), kind.into()],
        ))
        .await?
        .rows_affected())
}

impl FrozenStorageLifetimeProgress {
    /// Reopen finite discovery after a source invalidation or retry.
    pub fn restart_discovery(&mut self) {
        *self = Self::default();
    }

    /// Expand only addressed operation/checkpoint metadata. A missing scope,
    /// oversized set or legacy key deliberately requests the original resync.
    pub async fn restart_for_changes(
        &mut self,
        store: &CompactionStore,
        keys: Option<Vec<String>>,
    ) -> Result<bool> {
        let Some(keys) = keys else {
            self.restart_discovery();
            return Ok(true);
        };
        let retries_only = keys.is_empty();
        let mut manifests = BTreeSet::new();
        let mut checkpoints = BTreeSet::new();
        let mut operations = BTreeSet::new();
        let mut tasks = BTreeSet::new();
        let mut task_sources = Vec::new();
        for key in keys {
            match key.split_once(':') {
                Some(("m", id)) => {
                    manifests.insert(id.to_owned());
                }
                Some(("c", id)) => {
                    checkpoints.insert(id.to_owned());
                }
                Some(("o", id)) => {
                    operations.insert(id.to_owned());
                }
                Some(("t", id)) => {
                    tasks.insert(id.to_owned());
                }
                Some((kind @ ("r" | "e" | "v" | "g" | "y"), id)) => {
                    task_sources.push((kind.to_owned(), id.to_owned()));
                }
                _ => {
                    self.restart_discovery();
                    return Ok(true);
                }
            }
        }
        for (kind, id) in task_sources {
            let task_id = match kind.as_str() {
                "r" => {
                    pioneer_entity::task_run::Entity::find_by_id(id)
                        .select_only()
                        .column(pioneer_entity::task_run::Column::TaskId)
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                }
                "e" => {
                    pioneer_entity::task_run_execution::Entity::find_by_id(id)
                        .select_only()
                        .column(pioneer_entity::task_run_execution::Column::TaskId)
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                }
                "v" => {
                    pioneer_entity::task_run_turn::Entity::find_by_id(id)
                        .select_only()
                        .column(pioneer_entity::task_run_turn::Column::TaskId)
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                }
                "g" => {
                    pioneer_entity::task_trigger::Entity::find_by_id(id)
                        .select_only()
                        .column(pioneer_entity::task_trigger::Column::TaskId)
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                }
                _ => {
                    pioneer_entity::task_run_turn::Entity::find()
                        .select_only()
                        .column(pioneer_entity::task_run_turn::Column::TaskId)
                        .filter(pioneer_entity::task_run_turn::Column::TurnId.eq(id))
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                }
            };
            let Some(task_id) = task_id else {
                self.restart_discovery();
                return Ok(true);
            };
            tasks.insert(task_id);
        }
        for task_id in tasks {
            // Run/lease/receipt changes inside a live Task cannot release its
            // immutable input roots. Capsule/root deletion has its own m: delta.
            let terminal_task = task::Entity::find_by_id(&task_id)
                .select_only()
                .column(task::Column::Id)
                .filter(task::Column::Status.is_in(["completed", "failed", "cancelled"]))
                .into_tuple::<String>()
                .one(&store.connection)
                .await?;
            if terminal_task.is_none() {
                continue;
            }
            let roots = task_input::Entity::find()
                .select_only()
                .column(task_input::Column::FrozenManifestId)
                .filter(task_input::Column::TaskId.eq(&task_id))
                .limit(257)
                .into_tuple::<Option<String>>()
                .all(&store.connection)
                .await?;
            if roots.len() > 256 || roots.iter().any(|id| id.as_deref() == Some("")) {
                self.restart_discovery();
                return Ok(true);
            }
            manifests.extend(roots.into_iter().flatten());
            let outputs = pioneer_entity::compaction_task_output::Entity::find()
                .select_only()
                .column(pioneer_entity::compaction_task_output::Column::ManifestId)
                .filter(pioneer_entity::compaction_task_output::Column::TaskId.eq(&task_id))
                .limit(257)
                .into_tuple::<String>()
                .all(&store.connection)
                .await?;
            manifests.extend(outputs);
            if manifests.len() > 256 {
                self.restart_discovery();
                return Ok(true);
            }
        }
        if !manifests.is_empty() {
            let related = projection::Entity::find()
                .select_only()
                .column(projection::Column::OperationId)
                .filter(projection::Column::ManifestId.is_in(manifests.iter().cloned()))
                .limit(257)
                .into_tuple::<String>()
                .all(&store.connection)
                .await?;
            if related.len() > 256 {
                self.restart_discovery();
                return Ok(true);
            }
            operations.extend(related);
        }
        for id in checkpoints.clone() {
            if let Some(op) = checkpoint::Entity::find_by_id(id)
                .select_only()
                .column(checkpoint::Column::OperationId)
                .into_tuple::<String>()
                .one(&store.connection)
                .await?
            {
                operations.insert(op);
            }
        }
        for op in operations {
            let bound = projection::Entity::find_by_id(&op)
                .select_only()
                .column(projection::Column::ManifestId)
                .into_tuple::<String>()
                .one(&store.connection)
                .await?;
            let Some(id) = bound.filter(|id| !id.is_empty()) else {
                // Unbound legacy operations conservatively hold an entire
                // workspace. Exact IDs cannot represent their released scope.
                self.restart_discovery();
                return Ok(true);
            };
            manifests.insert(id);
            let ids = checkpoint::Entity::find()
                .select_only()
                .column(checkpoint::Column::Id)
                .filter(checkpoint::Column::OperationId.eq(&op))
                .limit(257)
                .into_tuple::<String>()
                .all(&store.connection)
                .await?;
            checkpoints.extend(ids);
            if checkpoints.len() + manifests.len() > 256 {
                self.restart_discovery();
                return Ok(true);
            }
        }
        let mut backings = manifests.clone();
        if !manifests.is_empty() {
            let sources = span::Entity::find()
                .select_only()
                .column(span::Column::SourceManifest)
                .filter(span::Column::ManifestId.is_in(manifests.iter().cloned()))
                .distinct()
                .limit(257)
                .into_tuple::<String>()
                .all(&store.connection)
                .await?;
            backings.extend(sources);
            let candidates = layout::Entity::find()
                .select_only()
                .column(layout::Column::Candidate)
                .filter(layout::Column::ManifestId.is_in(manifests.iter().cloned()))
                .distinct()
                .limit(257)
                .into_tuple::<Option<String>>()
                .all(&store.connection)
                .await?;
            backings.extend(candidates.into_iter().flatten());
            if backings.len() > 256 {
                self.restart_discovery();
                return Ok(true);
            }
        }
        *self = Self {
            target_manifests: Some(manifests.into_iter().collect()),
            target_checkpoints: Some(checkpoints.into_iter().collect()),
            target_backings: if retries_only {
                None
            } else {
                Some(backings.into_iter().collect())
            },
            retries_only,
            ..Self::default()
        };
        self.pass_done = self.discovery_phases();
        Ok(!self.target_manifests.as_ref().unwrap().is_empty())
    }

    fn discovery_phases(&self) -> [bool; 4] {
        match (&self.target_manifests, &self.target_checkpoints) {
            (Some(manifests), Some(checkpoints)) => [
                checkpoints.is_empty(),
                manifests.is_empty(),
                false,
                checkpoints.is_empty(),
            ],
            _ if self.release_only => [true, false, false, false],
            _ => [false; 4],
        }
    }

    pub async fn convert_storage(
        &mut self,
        db: &pioneer_postgres_pools::PostgresDatabase,
    ) -> Result<bool> {
        let (more, deleted) = crate::frozen_storage::maintain(
            db,
            &mut self.conversion,
            self.target_manifests.as_deref(),
        )
        .await?;
        if let Some((kind, deleted)) = deleted {
            self.acknowledge_physical(kind, Some(deleted)).await;
        }
        Ok(more)
    }

    /// A hold release after worker EOF reopens cleanup while preserving
    /// proof and conversion progress.
    pub fn recheck_released_holds(&mut self) {
        self.release_only = true;
        self.pass_done = [true, false, false, false];
        self.sweep_round = Default::default();
    }
    pub async fn next_retry_deadline(&self, store: &CompactionStore) -> Result<Option<i64>> {
        frozen_sweep_work::next_retry_deadline(&store.connection).await
    }
    /// One eligible bounded phase step; skip at most one rotation of finished
    /// phases without making the worker sleep for each skip.
    /// False means every discovery family has
    /// completed this pass, not that one phase happened to be empty. Rotating
    /// keysets revisit previously held rows on the next pass.
    pub async fn quantum(&mut self, store: &CompactionStore) -> Result<bool> {
        let store = store.with_maintenance_access();
        let mut selected = None;
        for _ in 0..self.pass_done.len() {
            let phase = self.phase;
            self.phase = (phase + 1) % self.pass_done.len();
            if !self.pass_done[phase] {
                selected = Some(phase);
                break;
            }
        }
        if let Some(phase) = selected {
            let result = match phase {
                0 => self.proof_quantum(&store).await,
                1 => self.expiry_quantum(&store).await,
                2 => self.sweep_quantum(&store).await,
                3 => self.discard_quantum(&store).await,
                _ => unreachable!("invalid discovery phase"),
            };
            let more = match result {
                Ok(more) => more,
                Err(error) => {
                    if error.downcast_ref::<sea_orm::DbErr>().is_some() {
                        // Graph/reachability steps can consume RAM work
                        // before a fallible DB call. Restart only the failed job,
                        // preserving its original identity and dropping old holds.
                        match phase {
                            0 => {
                                if let Some(proof) = self.proof.as_mut() {
                                    proof.restart();
                                }
                            }
                            1 => {
                                if let Some(expiry) = self.expiry.as_mut() {
                                    expiry.restart();
                                }
                            }
                            3 => {
                                if let Some(discard) = self.discard.as_mut() {
                                    discard.reverse =
                                        Reachability::new(&discard.id, discard.reverse.workspace());
                                }
                            }
                            _ => {}
                        }
                        return Err(error);
                    }
                    self.accounting.refused += 1;
                    // Data/authority refusal ends this object's visit. The indexed
                    // cursor already advanced; other families continue immediately.
                    match phase {
                        0 => {
                            if self.proof.is_none() {
                                self.proof_pending.pop_front();
                            }
                            self.proof = None;
                        }
                        1 => self.expiry = None,
                        // sweep_quantum has already rotated its active queue.
                        3 => self.discard = None,
                        _ => {}
                    }
                    true
                }
            };
            if !more {
                self.pass_done[phase] = true;
            }
        }
        if self.pass_done.iter().all(|done| *done) {
            self.pass_done = self.discovery_phases();
            self.sweep_round = Default::default();
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
            // Confirm EOF again after newly verified proofs, rather than
            // sleeping over expiry work that became eligible late in the pass.
            return Ok(std::mem::take(&mut self.pass_changed));
        }
        Ok(true)
    }
    /// Start another discovery epoch for completed cleanup families between
    /// worker batches, admitted by the worker's periodic idle interval. An
    /// unfinished proof graph alone is not a reason to reopen discovery.
    /// Active container continuations and paged preparations are preserved.
    /// The worker supplies its existing idle interval and monotonic batch time.
    pub fn revisit_completed_cleanup(&mut self, now: Instant, interval: Duration) {
        let due = self.cleanup_revisit_at.is_some_and(|at| now >= at);
        if self.cleanup_revisit_at.is_none() || due {
            self.cleanup_revisit_at = Some(now + interval);
        }
        if !due {
            return;
        }
        // Once proof discovery reaches EOF, let every family finish together
        // so a quiet database still reaches the existing idle/backoff state.
        if self.pass_done[0] {
            return;
        }
        for phase in [1, 2, 3] {
            if self.pass_done[phase] {
                self.pass_done[phase] = false;
            }
        }
    }
    async fn acknowledge_physical(&mut self, kind: i64, result: Option<PhysicalDeleteResult>) {
        if let Some(result) = result {
            self.accounting.acknowledge(kind, result).await;
        }
    }
    async fn proof_quantum(&mut self, store: &CompactionStore) -> Result<bool> {
        if let Some(proof) = self.proof.as_mut() {
            proof.step(store).await?;
            if proof.finished() {
                self.proof = None;
                self.accounting.proofs += 1;
                self.pass_changed = true;
            }
            return Ok(true);
        }
        // Seek unsealed headers through checkpoint_proof_discovery before
        // choosing roots. The page is advisory; dequeue rechecks current state.
        if self.proof_pending.is_empty() {
            if self.proof_upper.is_none() {
                self.proof_upper = Some(
                    crate::proof_discovery::proof_candidate_upper_bound(&store.connection)
                        .await?
                        .unwrap_or_default(),
                );
            }
            let page = crate::proof_discovery::proof_candidate_page(
                &store.connection,
                self.target_checkpoints.as_deref(),
                &self.proof_after,
                self.proof_upper.as_deref().unwrap_or_default(),
                META_ROWS,
            )
            .await?;
            let Some(page) = page else {
                self.proof_after.clear();
                self.proof_upper = None;
                return Ok(false);
            };
            self.proof_after = page.last_id;
            self.proof_pending.extend(page.eligible);
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

    async fn expiry_quantum(&mut self, store: &CompactionStore) -> Result<bool> {
        if let Some(expiry) = self.expiry.as_mut() {
            let expired_before = self.accounting.expired;
            if expiry_step(expiry, store, &mut self.accounting).await? {
                self.expiry = None;
            }
            if self.accounting.expired > expired_before {
                self.pass_changed = true;
                self.pass_done[2] = false;
                self.pass_done[3] = self.target_checkpoints.as_ref().is_some_and(Vec::is_empty);
            }
            return Ok(true);
        }
        if let Some(ids) = &self.target_manifests {
            let Some(id) = ids.iter().find(|id| *id > &self.expiry_after).cloned() else {
                self.expiry_after.clear();
                return Ok(false);
            };
            let header = history::Entity::find_by_id(&id)
                .filter(history::Column::Ready.eq(1_i64))
                .filter(history::Column::Expired.eq(0_i64))
                .one(&store.connection)
                .await?;
            self.expiry_after = id;
            if let Some(header) = header {
                ensure!(counter_health(&header), "corrupt expiry counters");
                self.expiry = Some(ExpiryPreparation::new(header));
            }
            return Ok(true);
        }
        // One bounded metadata page includes direct-root admission. No separate
        // query/continuation is needed for each retained history. A page's
        // negative admission is advisory; expire still checks current roots,
        // reader holds and the exact header in its write transaction.
        if self.expiry_pending.is_empty() {
            if self.expiry_upper.is_none() {
                self.expiry_upper = Some(
                    history::Entity::find()
                        .select_only()
                        .column(history::Column::Id)
                        .order_by_desc(history::Column::Id)
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                        .unwrap_or_default(),
                );
            }
            self.expiry_pending = read_queries::expiry_page(
                &store.connection,
                &self.expiry_after,
                self.expiry_upper.as_deref().unwrap_or_default(),
            )
            .await?;
            if self.expiry_pending.is_empty() {
                // Only an empty physical page is EOF, never a page of holds.
                self.expiry_after.clear();
                self.expiry_upper = None;
                return Ok(false);
            }
        }
        while let Some(read_queries::ExpiryHeader { header, held }) =
            self.expiry_pending.pop_front()
        {
            self.expiry_after = header.id.clone();
            ensure!(counter_health(&header), "corrupt expiry counters");
            if held {
                self.accounting.held += 1;
                continue;
            }
            self.expiry = Some(ExpiryPreparation::after_root_preflight(header));
            return Ok(true);
        }
        // At most the current 16-row page is skipped in one quantum. Root
        // changes during a pass remain pending in the existing invalidation
        // protocol and are rediscovered behind the cursor on the next pass.
        Ok(true)
    }
    fn invalidate_sweep(&mut self) {
        for round in &mut self.sweep_round {
            round.invalidate();
        }
        for sweep in self.sweep.iter_mut().chain(self.sweep_pending.iter_mut()) {
            sweep.invalidated = true;
        }
    }

    async fn sweep_quantum(&mut self, store: &CompactionStore) -> Result<bool> {
        let active = self.sweep_pending.len() + usize::from(self.sweep.is_some());
        if active > 0 && (!self.sweep_discover || active >= ACTIVE_CONTAINERS) {
            let mut sweep = self
                .sweep
                .take()
                .or_else(|| self.sweep_pending.pop_front())
                .expect("active sweep");
            let result = sweep.step(store, &mut self.accounting).await;
            // Keep ownership until the durable finish succeeds. DB errors and
            // cancellation cannot turn a dropped continuation into a clean key.
            let finish = match &result {
                Ok(SweepStep::Complete) => Some(frozen_sweep_work::Completion::Complete(
                    sweep.completion_guard(),
                )),
                Ok(SweepStep::Blocked) => Some(frozen_sweep_work::Completion::Blocked),
                Ok(SweepStep::Retry) => Some(frozen_sweep_work::Completion::Retry),
                Err(_) => Some(frozen_sweep_work::Completion::Retry),
                _ => None,
            };
            if let Some(validation) = finish {
                if let Some(work) = &sweep.work {
                    if let Err(error) = work.finish(&store.connection, validation).await {
                        self.sweep_pending.push_back(sweep);
                        self.sweep = self.sweep_pending.pop_front();
                        return Err(error);
                    }
                }
                if sweep.invalidated {
                    // Do not consume the overlap obligation on writer error.
                    // Cursor progress is retained; revisit only this family at
                    // its next frontier boundary, after the old visit settled.
                    self.sweep_round[sweep.kind as usize].invalidate();
                }
            } else {
                self.sweep_pending.push_back(sweep);
            }
            self.sweep = self.sweep_pending.pop_front();
            self.sweep_discover = true;
            result?;
            return Ok(true);
        }
        self.sweep_discover = false;
        // One fanout turn alternates with one physical admission turn. Active
        // preparations keep their existing rotation; fanout need not drain.
        let fanout_checked = self.sweep_fanout;
        if fanout_checked {
            self.sweep_fanout = false;
            if !self.retries_only
                && frozen_sweep_work::fanout_step(
                    &store.connection,
                    self.target_manifests.as_deref(),
                )
                .await?
            {
                self.invalidate_sweep();
                return Ok(true);
            }
        }
        self.sweep_fanout = true;
        let active_keys = self
            .sweep
            .iter()
            .chain(self.sweep_pending.iter())
            .map(|s| (s.manifest.as_str(), s.kind))
            .collect::<Vec<_>>();
        let kind = self.sweep_kind;
        self.sweep_kind = 1 - kind;
        let selected = frozen_sweep_work::discover(
            &store.connection,
            &active_keys,
            kind,
            &mut self.sweep_round[kind as usize],
            self.target_backings.as_deref(),
            self.retries_only,
        )
        .await?;
        let selected = match selected {
            frozen_sweep_work::Admission::Idle => {
                frozen_sweep_work::discover(
                    &store.connection,
                    &active_keys,
                    1 - kind,
                    &mut self.sweep_round[(1 - kind) as usize],
                    self.target_backings.as_deref(),
                    self.retries_only,
                )
                .await?
            }
            other => other,
        };
        let work = match selected {
            frozen_sweep_work::Admission::Selected(work) => work,
            frozen_sweep_work::Admission::Continue => return Ok(true),
            frozen_sweep_work::Admission::Idle => {
                // Empty physical admission is not EOF while propagation continues.
                let propagated = !fanout_checked
                    && (!self.retries_only
                        && frozen_sweep_work::fanout_step(
                            &store.connection,
                            self.target_manifests.as_deref(),
                        )
                        .await?);
                if propagated {
                    self.invalidate_sweep();
                }
                let more = propagated || active > 0;
                if !more {
                    self.sweep_round = Default::default();
                }
                return Ok(more);
            }
        };
        let (data, _) = physical_table(work.kind);
        let last = store
            .connection
            .query_one(
                &Query::select()
                    .column(Alias::new("ordinal"))
                    .from(Alias::new(data))
                    .and_where(Expr::col(Alias::new("manifest_id")).eq(&work.manifest))
                    .order_by(Alias::new("ordinal"), Order::Desc)
                    .limit(1)
                    .to_owned(),
            )
            .await?;
        if let Some(last) = last {
            let until: i64 = last.try_get("", "ordinal")?;
            if until < 0 {
                self.accounting.poison += 1;
                work.finish(&store.connection, frozen_sweep_work::Completion::Retry)
                    .await?;
                return Ok(true);
            }
            let mut sweep = SweepPreparation::new(
                work.manifest.clone(),
                work.kind,
                std::cmp::min(until, i64::MAX - 1),
            );
            sweep.poison = until == i64::MAX;
            sweep.work = Some(work);
            if self.sweep.is_none() {
                self.sweep = Some(sweep);
            } else {
                self.sweep_pending.push_back(sweep);
            }
        } else {
            // No rows is a completed generation. The same CAS/fanout barrier
            // protects a concurrent append after the inventory read.
            work.finish(
                &store.connection,
                frozen_sweep_work::Completion::Complete(
                    DbBackend::Postgres.build(&Query::select().expr(Expr::val(1)).to_owned()),
                ),
            )
            .await?;
        }
        Ok(true)
    }
    async fn discard_quantum(&mut self, store: &CompactionStore) -> Result<bool> {
        if let Some(discard) = self.discard.as_mut() {
            if !discard.reverse.done() {
                discard.reverse.step(&store.connection).await?;
                return Ok(true);
            }
            if discard.reverse.reachable() {
                self.discard = None;
                return Ok(true);
            }
            self.pass_changed |= discard.cleanup(store).await?;
            self.discard = None;
            return Ok(true);
        }
        if self.discard_pending.is_empty() {
            if let Some(ids) = &self.target_checkpoints {
                let page = ids
                    .iter()
                    .filter(|id| *id > &self.discard_after)
                    .take(META_ROWS as usize)
                    .cloned()
                    .map(|id| (Some(id), true))
                    .collect::<VecDeque<_>>();
                if page.is_empty() {
                    self.discard_after.clear();
                    return Ok(false);
                }
                self.discard_pending = page;
            }
        }
        if self.discard_pending.is_empty() {
            if self.discard_upper.is_none() {
                self.discard_upper = Some(
                    checkpoint::Entity::find()
                        .select_only()
                        .column(checkpoint::Column::Id)
                        .order_by_desc(checkpoint::Column::Id)
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                        .unwrap_or_default(),
                );
            }
            self.discard_pending = read_queries::discard_page(
                &store.connection,
                &self.discard_after,
                self.discard_upper.as_deref().unwrap_or_default(),
            )
            .await?;
        }
        let (id, candidate) = match self.discard_pending.front() {
            Some((Some(id), candidate)) => (Some(id.clone()), *candidate),
            Some((None, _)) => {
                // Preserve the legacy TEXT ID domain without retaining a page
                // of oversized keys. A fresh single seek also handles deletion
                // of the sentinel row. Never acknowledge its old cached tail.
                self.discard_pending.clear();
                let id = checkpoint::Entity::find()
                    .select_only()
                    .column(checkpoint::Column::Id)
                    .filter(checkpoint::Column::Id.gt(&self.discard_after))
                    .filter(
                        checkpoint::Column::Id
                            .lte(self.discard_upper.as_deref().unwrap_or_default()),
                    )
                    .order_by_asc(checkpoint::Column::Id)
                    .into_tuple::<String>()
                    .one(&store.connection)
                    .await?;
                (id, true)
            }
            None => (None, false),
        };
        let Some(id) = id else {
            // Only an empty physical seek is EOF. Rejected eligibility on a
            // nonempty page always returns more; the existing pass/revisit
            // policy later rediscovers eligibility changes behind the cursor.
            self.discard_after.clear();
            self.discard_upper = None;
            return Ok(false);
        };
        // Most keys have no discard work and need no individual SQL/AST at all.
        // A cached positive is not authority: recheck current incoming edges
        // and binding before touching runner payload. A negative can become
        // eligible later; the existing finite-pass invalidation/revisit keeps
        // that change pending, including keys behind the cursor.
        if !candidate || !discard_preflight(&store.connection, &id).await? {
            self.discard_after = id;
            self.discard_pending.pop_front();
            return Ok(true);
        }
        // Discovery/preflight may already be stale. Fetch all current binding
        // facts together, only now touching runner payload/size. Any DB error
        // leaves this visit unacknowledged; a successful refusal is not EOF.
        let discard = prepare_discard_candidate(store, &id).await?;
        self.discard_after = id;
        self.discard_pending.pop_front();
        self.discard = discard;
        Ok(true)
    }
}

// Both reads are advisory: cleanup still owns final authorization in its
// write transaction. No reader permit survives query_one_raw/checkpoint_row.
async fn prepare_discard_candidate(
    store: &CompactionStore,
    id: &str,
) -> Result<Option<DiscardPreparation>> {
    let row = store.connection.query_one_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT c.operation_id,c.owner,c.status AS checkpoint_status,p.manifest_id,o.status,r.generation,r.state::text AS state,context.workspace_id,context.thread_id \
         FROM compaction_checkpoint c \
         JOIN compaction_operation o ON o.id=c.operation_id AND o.owner=c.owner \
         JOIN compaction_context context ON context.owner=c.owner \
         JOIN compaction_operation_projection p ON p.operation_id=o.id \
         JOIN compaction_frozen_history h ON h.id=p.manifest_id \
         JOIN compaction_runner_state r ON r.operation_id=o.id \
         WHERE c.id=$1 AND c.status<>'applied' AND c.format_version=1 AND h.expired=1 \
         AND o.status IN ('failed','cancelled','stale') \
         AND octet_length(r.state::text)<=262144 LIMIT 1",
        [id.into()],
    )).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let operation: String = row.try_get("", "operation_id")?;
    let owner: String = row.try_get("", "owner")?;
    let checkpoint_status: String = row.try_get("", "checkpoint_status")?;
    let manifest: String = row.try_get("", "manifest_id")?;
    let status: String = row.try_get("", "status")?;
    let generation: Option<i64> = row.try_get("", "generation")?;
    let state: String = row.try_get("", "state")?;
    // Parse only after reader release. Invalid JSON/generation/phase is a
    // conservative refusal, never a fabricated terminal proof or a stuck key.
    if !terminal_runner(&status, generation, Some(&state)).unwrap_or(false) {
        return Ok(None);
    }
    // The binding and runner share one reader snapshot. The final writer
    // still rechecks revisions, holds and incoming edges after traversal.
    let workspace: String = row.try_get("", "workspace_id")?;
    let thread = row.try_get("", "thread_id")?;
    Ok(Some(DiscardPreparation {
        reverse: Reachability::new(id, &workspace),
        id: id.into(),
        manifest,
        status,
        generation,
        operation,
        owner,
        checkpoint_status,
        thread,
    }))
}

struct DiscardPreparation {
    id: String,
    manifest: String,
    status: String,
    generation: Option<i64>,
    reverse: Reachability,
    operation: String,
    owner: String,
    checkpoint_status: String,
    thread: String,
}
impl DiscardPreparation {
    async fn cleanup(&self, store: &CompactionStore) -> Result<bool> {
        // Sizes first, no payload decoding or large comparisons in the writer.
        // At most three exact keys and 1 MiB of logical row bytes per quantum.
        let mut keys = Vec::new();
        let mut total = 0_i64;
        for (table, ordinal, size) in [
            (
                "compaction_checkpoint_import",
                "import_ordinal",
                "octet_length(proof_json::text)::bigint+COALESCE(octet_length(target_checkpoint_json::text)::bigint,0)+octet_length(target_source_thread::text)::bigint+octet_length(context_thread::text)::bigint",
            ),
            (
                "compaction_checkpoint_replay_alias",
                "insertion_ordinal",
                "octet_length(covered_thread::text)::bigint+octet_length(covered_scope::text)::bigint+octet_length(covered_id::text)::bigint+octet_length(covered_version::text)::bigint+octet_length(replay_thread::text)::bigint+octet_length(replay_scope::text)::bigint+octet_length(replay_id::text)::bigint+octet_length(replay_version::text)::bigint+COALESCE(octet_length(tool_item_id::text)::bigint,0)",
            ),
            (
                "compaction_checkpoint_event_input",
                "insertion_ordinal",
                "octet_length(source_thread::text)::bigint+octet_length(source_scope::text)::bigint+octet_length(source_id::text)::bigint+octet_length(source_version::text)::bigint+octet_length(role::text)::bigint",
            ),
        ] {
            let row = store
                .connection
                .query_one(
                    &Query::select()
                        .from(Alias::new(table))
                        .expr_as(Expr::col(Alias::new(ordinal)), Alias::new("key"))
                        .expr_as(Expr::cust(size), Alias::new("bytes"))
                        .and_where(Expr::col(Alias::new("checkpoint_id")).eq(&self.id))
                        .order_by(Alias::new(ordinal), Order::Asc)
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
                    keys.push((table, ordinal, row.try_get::<i64>("", "key")?, size, bytes));
                }
            }
        }
        let change = store
            .connection
            .work_signal("maintenance.frozen")
            .changing();
        let changed = store
            .retry_write_on_conflict(|| async {
                let tx = store.connection.begin().await?;
                if crate::frozen_lifetime::cleanup_blocked(
                    &store.frozen_readers,
                    &self.manifest,
                    &store.connection.work_signal("maintenance.frozen.release"),
                ) {
                    tx.commit().await?;
                    return Ok(false);
                }
                if checkpoint::Entity::find_by_id(&self.id)
                    .select_only()
                    .column(checkpoint::Column::Id)
                    .lock_exclusive()
                    .into_tuple::<String>()
                    .one(&tx)
                    .await?
                    .is_none()
                {
                    tx.commit().await?;
                    return Ok(false);
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
                        context::Entity,
                        Expr::col((context::Entity, context::Column::Owner))
                            .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
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
                    // The reader's terminal proof/reverse traversal belongs to
                    // this exact operation and scope, including unsealed keys
                    // whose identity is not protected by sealed-row triggers.
                    .and_where(
                        Expr::col((checkpoint::Entity, checkpoint::Column::OperationId))
                            .eq(&self.operation),
                    )
                    .and_where(
                        Expr::col((checkpoint::Entity, checkpoint::Column::Owner)).eq(&self.owner),
                    )
                    .and_where(
                        Expr::col((operation::Entity, operation::Column::Owner)).eq(&self.owner),
                    )
                    .and_where(
                        Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                            .eq(&self.checkpoint_status),
                    )
                    .and_where(
                        Expr::col((checkpoint::Entity, checkpoint::Column::FormatVersion)).eq(1),
                    )
                    .and_where(
                        Expr::col((context::Entity, context::Column::WorkspaceId))
                            .eq(self.reverse.workspace()),
                    )
                    .and_where(
                        Expr::col((context::Entity, context::Column::ThreadId)).eq(&self.thread),
                    )
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
                    for (table, ordinal, key, size, bytes) in &keys {
                        tx.execute(
                            &Query::delete()
                                .from_table(Alias::new(*table))
                                .and_where(Expr::col(Alias::new("checkpoint_id")).eq(&self.id))
                                .and_where(Expr::col(Alias::new(*ordinal)).eq(*key))
                                .and_where(Expr::cust(*size).eq(*bytes))
                                .to_owned(),
                        )
                        .await?;
                    }
                }
                tx.commit().await?;
                Ok(eligible.is_some() && !keys.is_empty())
            })
            .await?;
        // Deleting legacy import/proof rows can release opaque outgoing roots.
        // Recover their complete dependency set rather than guessing an ID.
        change.completed(changed);
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    include!("frozen_sweep_tests.rs");
    mod expiry_preflight {
        include!("frozen_storage_lifetime_expiry_tests.rs");
    }
    mod proof_discovery {
        include!("frozen_storage_lifetime_proof_discovery_tests.rs");
    }
    mod discard_admission {
        include!("frozen_storage_lifetime_discard_tests.rs");
    }
    use pioneer_compaction::{
        Checkpoint, ModelSelection, SourceRef, Transport,
        frozen::{FrozenEventInputRole, FrozenHistoryRef, FrozenMessageRef},
        runner::FailureKind,
    };
    use sha2::{Digest, Sha256};
    // Real physical tables with the production constraints and triggers.
    async fn batch_fixture(kind: i64, count: i64) -> CompactionStore {
        let (store, _) = fixture(false).await;
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,next_ordinal,import_count,next_import,ready) VALUES('batch-physical','ws','thread','batch','batch',$1,0,$1,0,0)",
            [count.into()])).await.unwrap();
        let payload = format!("{{}}{}", " ".repeat(4998));
        for ordinal in 0..count {
            let sql = if kind == 0 {
                "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('batch-physical',$1,$2::text::json,5000)"
            } else {
                "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES('batch-physical',$1,0,'event','event-'||$1::bigint,'v','thread',$2::text::json,5000)"
            };
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    sql,
                    [ordinal.into(), payload.clone().into()],
                ))
                .await
                .unwrap();
        }
        store.with_maintenance_access()
    }
    async fn batch_consumer(store: &CompactionStore, id: &str, kind: i64, spans: i64) {
        let messages = if kind == 0 { spans } else { 0 };
        let imports = if kind == 1 { spans } else { 0 };
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,next_ordinal,import_count,next_import,ready) VALUES($1,'ws','thread','consumer','consumer',$2,$2,$3,$3,1)",
            [id.into(), messages.into(), imports.into()])).await.unwrap();
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES($1,$2,0,0)", [id.into(), kind.into()])).await.unwrap();
        for ordinal in 0..spans {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES($1,$2,$3,$4,'batch-physical')",
                [id.into(), kind.into(), ordinal.into(), (ordinal+1).into()])).await.unwrap();
        }
        store.connection.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE compaction_frozen_layout AS layout SET active=1,verified_span_revision=(SELECT c.revision FROM compaction_frozen_sweep_consumer c WHERE c.manifest_id=layout.manifest_id AND c.kind=layout.kind)
             WHERE layout.manifest_id=$1 AND layout.kind=$2",
            [id.into(), kind.into()],
        )).await.unwrap();
    }
    async fn prepare_batch(store: &CompactionStore, kind: i64, until: i64) -> SweepPreparation {
        let mut sweep = SweepPreparation::new("batch-physical".into(), kind, until);
        for _ in 0..2000 {
            if sweep.prepare(store).await.unwrap() {
                return sweep;
            }
        }
        panic!("all geometry pages must finish without a consumer/span cap");
    }
    async fn batch_ordinals(store: &CompactionStore, kind: i64) -> Vec<i64> {
        let (data, _) = physical_table(kind);
        store
            .connection
            .query_all_raw(Statement::from_string(
                DbBackend::Postgres,
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

    async fn pause_fixture_bytes_check(db: &impl ConnectionTrait, data: &str) -> String {
        let constraint = format!("{data}_check");
        let definition = db.query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=$1::text::regclass AND conname=$2",
            [data.into(), constraint.as_str().into()],
        )).await.unwrap().unwrap().try_get_by_index::<String>(0).unwrap();
        db.execute_unprepared(&format!("ALTER TABLE {data} DROP CONSTRAINT {constraint}"))
            .await
            .unwrap();
        format!("ALTER TABLE {data} ADD CONSTRAINT {constraint} {definition} NOT VALID")
    }

    #[pioneer_postgres_test::postgres_test]
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
            let restore_check = pause_fixture_bytes_check(&tx, data).await;
            tx.execute_unprepared(&format!(
                "UPDATE {data} SET bytes=5001 WHERE manifest_id='batch-physical' AND ordinal=70"
            ))
            .await
            .unwrap();
            tx.execute_unprepared(&restore_check).await.unwrap();
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
                done = sweep.step(&store, &mut accounting).await.unwrap() != SweepStep::Continue;
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
            // Fresh durable discovery sees the committed remainder and the
            // appended tail. A read failure cannot clean or drop durable work.
            let mut restart = FrozenStorageLifetimeProgress::default();
            store
                .connection
                .execute_unprepared(
                    "ALTER TABLE compaction_frozen_sweep_work RENAME TO unavailable_sweep_work",
                )
                .await
                .unwrap();
            assert!(restart.sweep_quantum(&store).await.is_err());
            assert!(restart.sweep.is_none() && restart.sweep_pending.is_empty());
            store
                .connection
                .execute_unprepared(
                    "ALTER TABLE unavailable_sweep_work RENAME TO compaction_frozen_sweep_work",
                )
                .await
                .unwrap();
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

    #[pioneer_postgres_test::postgres_test]
    async fn batch_revalidates_new_consumers_and_capture_bounds_and_reports_only_commit() {
        for kind in [0, 1] {
            let store = batch_fixture(kind, 4).await;
            let mut sweep = prepare_batch(&store, kind, 3).await;
            batch_consumer(&store, "new-consumer", kind, 1).await;
            let mut accounting = SweepAccounting::default();
            assert_eq!(
                sweep.step(&store, &mut accounting).await.unwrap(),
                SweepStep::Blocked
            );
            assert_eq!(accounting.rows, [0, 0]);
            assert_eq!(accounting.refused, 1);
            assert_eq!(batch_ordinals(&store, kind).await, vec![0, 1, 2, 3]);

            let mut sweep = prepare_batch(&store, kind, 3).await;
            // Separate committed bounds: messages retain two rows, imports one.
            store.connection.execute_unprepared("UPDATE compaction_frozen_history SET ready=0,message_count=4,import_count=4,next_ordinal=2,next_import=1 WHERE id='batch-physical'").await.unwrap();
            assert_eq!(
                sweep.step(&store, &mut accounting).await.unwrap(),
                SweepStep::Blocked
            );
            assert_eq!(accounting.rows, [0, 0]);
            assert_eq!(accounting.refused, 2);
            assert_eq!(batch_ordinals(&store, kind).await, vec![0, 1, 2, 3]);

            let mut sweep = prepare_batch(&store, kind, 3).await;
            let (data, _) = physical_table(kind);
            store.connection.execute_unprepared(&format!("CREATE FUNCTION refuse_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rollback'; END; $$; CREATE TRIGGER refuse_batch BEFORE DELETE ON {data} FOR EACH ROW WHEN (OLD.manifest_id='batch-physical' AND OLD.ordinal=3) EXECUTE FUNCTION refuse_batch()")).await.unwrap();
            assert!(sweep.step(&store, &mut accounting).await.is_err());
            assert_eq!(accounting.rows, [0, 0]);
            assert_eq!(accounting.bytes, [0, 0]);
            assert_eq!(batch_ordinals(&store, kind).await, vec![0, 1, 2, 3]);
            store
                .connection
                .execute_unprepared(&format!(
                    "DROP TRIGGER refuse_batch ON {data}; DROP FUNCTION refuse_batch()"
                ))
                .await
                .unwrap();
            assert_eq!(
                sweep.step(&store, &mut accounting).await.unwrap(),
                SweepStep::Complete
            );
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
            duplicate.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,next_ordinal,import_count,next_import,ready) VALUES('duplicate-backing','ws','thread','backing','backing',$1,$1,$2,$2,1)",
                [messages.into(), imports.into()])).await.unwrap();
            let fields = if kind == 0 {
                "ordinal,reference_json,bytes"
            } else {
                "ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes"
            };
            duplicate.connection.execute_unprepared(&format!("INSERT INTO {data}(manifest_id,{fields}) SELECT 'duplicate-backing',{fields} FROM {data} WHERE manifest_id='batch-physical'")).await.unwrap();
            duplicate.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "UPDATE compaction_frozen_history SET ready=1,message_count=$1,next_ordinal=$1,import_count=$2,next_import=$2 WHERE id='batch-physical'",
                [messages.into(), imports.into()])).await.unwrap();
            duplicate.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,cleanup_to) VALUES('batch-physical',$1,0,1,6)", [kind.into()])).await.unwrap();
            duplicate.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('batch-physical',$1,0,6,'duplicate-backing')", [kind.into()])).await.unwrap();
            duplicate.connection.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE compaction_frozen_layout AS layout SET active=1,verified_span_revision=(SELECT c.revision FROM compaction_frozen_sweep_consumer c WHERE c.manifest_id=layout.manifest_id AND c.kind=layout.kind)
                 WHERE layout.manifest_id='batch-physical' AND layout.kind=$1",
                [kind.into()],
            )).await.unwrap();
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
            let restore_check = pause_fixture_bytes_check(&tx, data).await;
            tx.execute_unprepared(&format!(
                "UPDATE {data} SET bytes=5001 WHERE manifest_id='batch-physical' AND ordinal=0"
            ))
            .await
            .unwrap();
            for (ordinal, size, text) in resized_rows {
                tx.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                    format!("UPDATE {data} SET {payload}=$1,bytes=$2 WHERE manifest_id='batch-physical' AND ordinal=$3"),
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
            tx.execute_unprepared(&restore_check).await.unwrap();
            tx.commit().await.unwrap();
            let mut progress = FrozenStorageLifetimeProgress::default();
            duplicate.connection.execute_unprepared("CREATE FUNCTION refuse_cleanup_cursor() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture cleanup rollback'; END; $$; CREATE TRIGGER refuse_cleanup_cursor BEFORE UPDATE OF cleanup_next ON compaction_frozen_layout FOR EACH ROW EXECUTE FUNCTION refuse_cleanup_cursor()").await.unwrap();
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
            let rolled_back = crate::frozen::find_layout("batch-physical".to_owned(), kind)
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
                .execute_unprepared("DROP TRIGGER refuse_cleanup_cursor ON compaction_frozen_layout; DROP FUNCTION refuse_cleanup_cursor()")
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
                let l = crate::frozen::find_layout("batch-physical".to_owned(), kind)
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
            assert_eq!(
                sweep
                    .step(&duplicate, &mut progress.accounting)
                    .await
                    .unwrap(),
                SweepStep::Retry
            );
            assert_eq!(batch_ordinals(&duplicate, kind).await, vec![0, 5]);
            assert_eq!(progress.accounting.rows[kind as usize], 4);
            assert_eq!(
                progress.accounting.bytes[kind as usize],
                2 * 256 * 1024 + 5001
            );
        }
    }

    async fn postgres_store(fixture: &pioneer_postgres_test::TestDatabase) -> CompactionStore {
        CompactionStore::for_test(pioneer_postgres_pools::PostgresDatabase::new(
            fixture.connect_read_only().await.unwrap(),
            fixture.connect_with_limit(1).await.unwrap(),
        ))
    }

    async fn postgres_fixture(
        fixture: &pioneer_postgres_test::TestDatabase,
        published: bool,
    ) -> (CompactionStore, FrozenHistoryRef) {
        postgres_fixture_with_checkpoint_ids(fixture, published, ["cp-0", "cp-1"]).await
    }

    async fn postgres_fixture_with_checkpoint_ids(
        fixture: &pioneer_postgres_test::TestDatabase,
        published: bool,
        ids: [&str; 2],
    ) -> (CompactionStore, FrozenHistoryRef) {
        fixture_in_store(postgres_store(fixture).await, published, ids).await
    }

    async fn fixture(published: bool) -> (CompactionStore, FrozenHistoryRef) {
        fixture_with_checkpoint_ids(published, ["cp-0", "cp-1"]).await
    }
    async fn fixture_with_checkpoint_ids(
        published: bool,
        ids: [&str; 2],
    ) -> (CompactionStore, FrozenHistoryRef) {
        let db = pioneer_postgres_test::scoped_database().await.unwrap();
        postgres_fixture_with_checkpoint_ids(&db, published, ids).await
    }
    async fn fixture_in_store(
        store: CompactionStore,
        published: bool,
        ids: [&str; 2],
    ) -> (CompactionStore, FrozenHistoryRef) {
        let db = store.database_connection();
        for sql in [
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)",
        ] {
            db.execute_unprepared(sql).await.unwrap();
        }
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
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,attempts,transient_retries,correction,next_portion) VALUES('op','owner','op',$1,'{}',1000,0,0,0,0)",[status.into()])).await.unwrap();
        let state = RunnerState {
            generation: 0,
            deadline_ms: 1000,
            attempts: 0,
            retries: 0,
            portion_attempts: 0,
            overflow_rejections: 0,
            overflow_input_ceiling: None,
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
            retry_feedback: None,
            observation: None,
            diagnostic: None,
        };
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('op',0,$1::jsonb)",
            [serde_json::to_string(&state).unwrap().into()],
        ))
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO compaction_runner_plan(operation_id,source_count,reference_count,descriptor,ready) VALUES('op',2,0,'{}',1)").await.unwrap();
        db.execute_unprepared("INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT 'op',id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id='origin'").await.unwrap();
        for (i, s) in sources.iter().enumerate() {
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES('op',$1,$1,0,'thread',$2,$3,$4)",[(i as i64).into(),s.scope.clone().into(),s.id.clone().into(),s.version.clone().into()])).await.unwrap();
            let cp = Checkpoint {
                id: ids[i].into(),
                operation_id: "op".into(),
                owner: "owner".into(),
                previous: (i > 0).then(|| ids[0].into()),
                summary: format!("summary {i}"),
                selection: ModelSelection {
                    service_mode: None,
                    context_authority: None,
                    context_window: None,
                    transport: Transport::Api,
                    instance: "p".into(),
                    model: "m".into(),
                    effort: None,
                },
                coverage: vec![s.clone()],
                projection_version: 1,
                format_version: 1,
            };
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES($1,'op','owner',$2,$3,$4,$5,$6::json,1,1,$7)",[cp.id.clone().into(),cp.previous.clone().into(),(i as i64).into(),cp.summary.clone().into(),crate::runner_writes::checkpoint_identity(&cp).unwrap().into(),serde_json::to_string(&cp.selection).unwrap().into(),if published&&i==1{"applied"}else{"candidate"}.into()])).await.unwrap();
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES($1,$2,$3,$4)",[cp.id.into(),s.scope.clone().into(),s.id.clone().into(),s.version.clone().into()])).await.unwrap();
        }
        drop(hold);
        (store, h)
    }
    async fn value(store: &CompactionStore, table: &str, id: &str) -> i64 {
        store
            .connection
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                format!("SELECT count(*) AS n FROM {table} WHERE checkpoint_id=$1"),
                [id.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap()
    }
    async fn prepared_expiry(store: &CompactionStore) -> ExpiryPreparation {
        let header = history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let mut p = ExpiryPreparation::new(header);
        for _ in 0..500 {
            if p.completed_layouts() == 2
                && p.operations_checked()
                && p.checkpoints_checked()
                && !p.is_preparing_reverse_dependencies()
            {
                return p;
            }
            assert!(
                !expiry_step(&mut p, store, &mut SweepAccounting::default())
                    .await
                    .unwrap()
            );
        }
        panic!("bounded expiry preparation must finish");
    }

    async fn locator_marker(store: &CompactionStore, id: &str) -> Option<String> {
        pioneer_entity::thread_cli_runtime_binding::Entity::find_by_id(id)
            .select_only()
            .column(pioneer_entity::thread_cli_runtime_binding::Column::FrozenManifestId)
            .into_tuple::<Option<String>>()
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
    }

    #[pioneer_postgres_test::postgres_test]
    async fn current_frozen_roots_publish_checked_locators() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, h) = postgres_fixture(fixture, true).await;
            store.connection.execute_unprepared(
                "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class) VALUES('other-thread','ws','','agent','m','p','active','user','workspace')"
            ).await.unwrap();
            for id in ["a-fresh", "z-fresh"] {
                let descriptor = FrozenHistoryRef {
                    format: 1,
                    manifest_id: id.into(),
                    messages: 0,
                    identity_sha256: hex::encode(Sha256::digest([])),
                };
                store
                    .compaction_begin_frozen_history("ws", "thread", &descriptor)
                    .await
                    .unwrap();
            }
            let now = chrono::Utc::now().fixed_offset();
            for (id, json, expected) in [
                ("thread", "{}".to_owned(), None),
                ("other-thread", serde_json::json!({"provider":"claude","pioneerContext":{"version":4,"nativeThreadId":"native","acceptedTurnId":"turn","acceptedTurnRevision":1,"acceptedTurnDeleted":false,"contextManifestOwnerThreadId":"thread","contextOwnerThreadId":"thread","contextHistoryJson":serde_json::to_string(&h).unwrap()}}).to_string(), Some(h.manifest_id.clone())),
            ] {
                store.upsert_cli_runtime_thread_binding(pioneer_crud_execution::cli_runtime_binding::NewCliRuntimeThreadBinding {
                    thread_id: id.into(), workspace_id: "ws".into(), runtime_id: "runtime".into(), runtime_kind: "claude".into(), native_thread_id: "native".into(),
                    native_session_id: None, native_root_thread_id: None, native_cwd: None, native_model: None,
                    resume_cursor_json: json, status: "active".into(), created_at: now, updated_at: now,
                }).await.unwrap();
                assert_eq!(locator_marker(&store, id).await, expected);
                assert!(store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                    "UPDATE thread_cli_runtime_binding SET frozen_manifest_id='' WHERE thread_id=$1", [id.into()])).await.is_err());
            }
            assert!(store.frozen_readers.lock().unwrap().is_empty());
            Ok(())
        }).await.unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn pending_layouts_are_registered_without_a_startup_inventory() {
        let (store, _) = fixture(true).await;
        store
            .connection
            .execute_unprepared(
                "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version)
             VALUES('missing','','thread',1);
             INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms)
             VALUES('zz-orphan','missing','orphan','failed','{}',1)",
            )
            .await
            .unwrap();
        assert!(
            pioneer_entity::compaction_frozen_sweep_work::Entity::find()
                .filter(
                    pioneer_entity::compaction_frozen_sweep_work::Column::ManifestId.eq("origin")
                )
                .one(&store.connection)
                .await
                .unwrap()
                .is_some()
        );
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut idle = false;
        for _ in 0..24 {
            if !store
                .compact_frozen_storage_quantum(&mut progress)
                .await
                .unwrap()
            {
                idle = true;
                break;
            }
        }
        assert!(
            idle,
            "the unrelated historical operation must not delay layout maintenance"
        );
        let origin = history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((origin.storage_registered, origin.expired), (1, 0));
        let kinds = layout::Entity::find()
            .filter(layout::Column::ManifestId.eq("origin"))
            .order_by_asc(layout::Column::Kind)
            .all(&store.connection)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.kind)
            .collect::<Vec<_>>();
        assert_eq!(kinds, [0, 1]);
    }

    #[pioneer_postgres_test::postgres_test]
    async fn skipped_families_yield_to_unfinished_paths_and_confirm_global_eof() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, _) = postgres_fixture(fixture, true).await;
            let mut progress = FrozenStorageLifetimeProgress::default();
            // Bootstrap and invalidation continuations are real work even while
            // physical discovery itself temporarily finds no eligible key.
            assert!(progress.sweep_quantum(&store).await.unwrap());

            progress.pass_done = [true, false, false, false];
            progress.phase = 0;
            assert!(progress.quantum(&store).await.unwrap());
            assert!(
                progress.expiry.is_some(),
                "unfinished cleanup receives next eligible step"
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn queued_proof_rechecks_seal_from_traversal_or_concurrent_preparation() {
        proof_discovery::queued_raw_candidate_rechecks_concurrent_seal().await;
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
                store.connection.execute_unprepared("CREATE FUNCTION refuse_freeze() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture freeze failure'; END; $$; CREATE TRIGGER refuse_freeze BEFORE UPDATE OF next_portion ON compaction_operation FOR EACH ROW EXECUTE FUNCTION refuse_freeze()").await.unwrap();
                progress.phase = 0;
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
                    .execute_unprepared("DROP TRIGGER refuse_freeze ON compaction_operation; DROP FUNCTION refuse_freeze()")
                    .await
                    .unwrap();
                store.connection.execute_unprepared("CREATE FUNCTION refuse_proof_stage() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture staging failure'; END; $$; CREATE TRIGGER refuse_proof_stage BEFORE INSERT ON compaction_checkpoint_event_input FOR EACH ROW EXECUTE FUNCTION refuse_proof_stage()").await.unwrap();
                let mut failed_with_hold = false;
                let mut observed_hold = false;
                for _ in 0..500 {
                    observed_hold |= !store.frozen_readers.lock().unwrap().is_empty();
                    progress.phase = 0;
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
                    .execute_unprepared("DROP TRIGGER refuse_proof_stage ON compaction_checkpoint_event_input; DROP FUNCTION refuse_proof_stage()")
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

        // Discovery is advisory even when public status changes before dequeue.
        // Use real preparation for the seal, then withdraw public status and
        // availability. A private sealed candidate must take full validation.
        let (store, _) = fixture(true).await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        assert!(progress.proof_quantum(&store).await.unwrap());
        assert_eq!(progress.proof_pending, VecDeque::from(["cp-1".to_owned()]));
        checkpoint_proofs::prepare_graph(&store, "cp-1")
            .await
            .unwrap();
        for sql in [
            "UPDATE compaction_checkpoint SET status='candidate' WHERE id='cp-1'",
            "UPDATE compaction_frozen_history SET expired=1 WHERE id='origin'",
        ] {
            store.connection.execute_unprepared(sql).await.unwrap();
        }
        assert!(progress.proof_quantum(&store).await.unwrap());
        assert!(
            progress.proof.is_some(),
            "private seal is not a dequeue shortcut"
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
        assert!(
            refused,
            "private candidate must still require its bound input"
        );
        drop(progress);
        assert!(store.frozen_readers.lock().unwrap().is_empty());
    }

    #[pioneer_postgres_test::postgres_test]
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
            pioneer_postgres_test::with_database(async |fixture| {
                let (store, _) = postgres_fixture(fixture, true).await;
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
                    "ownership" => {
                        Some("DELETE FROM compaction_manifest WHERE source_id='event-0'")
                    }
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
                Ok(())
            })
            .await
            .unwrap();
        }
    }
    #[pioneer_postgres_test::postgres_test]
    async fn empty_phase_is_not_idle_while_lifetime_work_is_pending() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, _) = postgres_fixture(fixture, true).await;
            let mut progress = FrozenStorageLifetimeProgress::default();
            progress.phase = 0;
            assert!(
                progress.quantum(&store).await.unwrap(),
                "an empty or finished phase does not end the complete pass"
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
            Ok(())
        })
        .await
        .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn fk_off_orphan_checkpoint_header_scope_and_malformed_edges_fail_closed() {
        for corruption in ["operation", "header", "scope", "dependency"] {
            pioneer_postgres_test::with_database(async |fixture| {
                let (store, h) = postgres_fixture(fixture, true).await;
                // Bypass just the FKs that would prevent this injected
                // orphan or cascade away its evidence. Every app trigger
                // and all other constraints stay enabled on this fixture.
                let sql = match corruption {
                    "operation" => "ALTER TABLE compaction_checkpoint DROP CONSTRAINT compaction_checkpoint_operation_id_fkey;
                                    DELETE FROM compaction_operation WHERE id='op'",
                    "header" => "ALTER TABLE compaction_operation_projection DROP CONSTRAINT compaction_operation_projection_manifest_id_fkey;
                                 ALTER TABLE compaction_frozen_message_data DROP CONSTRAINT compaction_frozen_message_data_manifest_id_fkey;
                                 ALTER TABLE compaction_frozen_import_data DROP CONSTRAINT compaction_frozen_import_data_manifest_id_fkey;
                                 DELETE FROM compaction_frozen_history WHERE id='origin'",
                    "scope" => "ALTER TABLE compaction_operation DROP CONSTRAINT compaction_operation_owner_fkey;
                                DELETE FROM compaction_context WHERE owner='owner'",
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
                let bytes:i64 = store.connection.query_one_raw(Statement::from_string(DbBackend::Postgres,
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
                let n:i64 = store.connection.query_one_raw(Statement::from_string(DbBackend::Postgres,"SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='origin'"))
                    .await.unwrap().unwrap().try_get("","n").unwrap();
                assert_eq!(n, 2, "refusal preserves actual frozen payloads");
                assert_eq!(
                    value(&store, "compaction_checkpoint_event_input", "cp-1").await,
                    0
                );
                 Ok(())
             })
             .await
             .unwrap();
        }
    }

    #[pioneer_postgres_test::postgres_test]
    async fn cancelled_queued_acquire_never_registers_a_late_reader() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, h) = postgres_fixture(fixture, false).await;
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
            expire(&store, p.header()).await.unwrap();
            assert!(
                store
                    .compaction_acquire_frozen_history("ws", &h)
                    .await
                    .is_err()
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn published_legacy_root_backfills_candidate_ancestry_then_sweeps_origin_exactly() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, h) = postgres_fixture(fixture, true).await;
            // Schema defaults model the legacy representation: no precredited seal.
            let mut p = FrozenStorageLifetimeProgress::default();
            for _ in 0..4000 {
                p.quantum(&store).await.unwrap();
            }
            for id in ["cp-0", "cp-1"] {
                let row = crate::checkpoint_topology::checkpoint_topology(&store.connection, id)
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
            let rows=store.connection.query_one_raw(Statement::from_string(DbBackend::Postgres,"SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='origin'")).await.unwrap().unwrap();
            assert_eq!(rows.try_get::<i64>("", "n").unwrap(), 0);
             Ok(())
         })
         .await
         .unwrap();
    }
    #[pioneer_postgres_test::postgres_test]
    async fn historical_edges_adopt_actual_seal_and_expiry_before_legacy_origin_acquire() {
        use crate::publication_test_hook::{PublicationTestPause, arm_publication_test_hook};
        for (graph_reader, control_change) in [(false, false), (true, false), (true, true)] {
            let (store, h) = fixture(true).await;
            let expected = store
                .compaction_checkpoint_edges("cp-1")
                .await
                .unwrap()
                .unwrap();
            assert!(!expected.event_input_evidence.is_empty());
            let mut pause = arm_publication_test_hook(
                &store.connection,
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
                        DbBackend::Postgres,
                        "SELECT state::text AS state FROM compaction_runner_state WHERE operation_id='op'",
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                let mut state: RunnerState =
                    serde_json::from_str(&saved.try_get::<String>("", "state").unwrap()).unwrap();
                state.generation = 1;
                store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                    "UPDATE compaction_runner_state SET generation=1,state=$1::text::json WHERE operation_id='op'",
                    [serde_json::to_string(&state).unwrap().into()])).await.unwrap();
            }
            checkpoint_proofs::prepare_graph(&store, "cp-1")
                .await
                .unwrap();
            let mut expiry = prepared_expiry(&store).await;
            assert!(
                expiry_step(&mut expiry, &store, &mut SweepAccounting::default())
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
            let physical = store.connection.query_one_raw(Statement::from_string(DbBackend::Postgres,
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
            let evidence = |edges: &checkpoint_proofs::CheckpointEdges| {
                let source = |s: &checkpoint_proofs::HistoricalSourceRef| {
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

    #[pioneer_postgres_test::postgres_test]
    async fn concurrent_cold_preparations_share_historical_node_and_exact_sealed_staging() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, _) = postgres_fixture(fixture, true).await;
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
                crate::proof_publication::checkpoint_row(&store.connection, "cp-0")
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
                crate::proof_publication::checkpoint_row(&store.connection, "cp-0")
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
            Ok(())
        })
        .await
        .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn cancelled_legacy_acquire_and_shared_node_control_change_do_not_leak_or_bypass_seal() {
        pioneer_postgres_test::with_database(async |fixture| {
            use crate::publication_test_hook::{PublicationTestPause, arm_publication_test_hook};
            let (store, _) = postgres_fixture(fixture, true).await;
            let mut pause = arm_publication_test_hook(
                &store.connection,
                "op",
                PublicationTestPause::CheckpointOriginAcquire,
            );
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
                crate::proof_publication::checkpoint_row(&store.connection, "cp-0")
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
            Ok(())
        })
        .await
        .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn exact_prepared_expiry_revalidates_task_roots_readers_and_expiry_first() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, h) = postgres_fixture(fixture, false).await;
            checkpoint_proofs::prepare_graph(&store, "cp-1")
                .await
                .unwrap();
            store.connection.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal) VALUES('task','ws','thread','thread','agent','failed','fixture','fixture')").await.unwrap();
            store.connection.execute_unprepared("INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run','task','run',1,1,'failed','agent')").await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) VALUES('run','task','ws','thread',$1,CURRENT_TIMESTAMP,$2)",
                [serde_json::to_string(&h).unwrap().into(), h.manifest_id.clone().into()])).await.unwrap();
            let mut prepared = prepared_expiry(&store).await;
            // Discovery saw a releasing Task. The expiry transaction must
            // still observe a newly retaining Task before marking/deleting payload.
            store
                .connection
                .execute_unprepared("UPDATE task SET status='blocked' WHERE id='task'")
                .await
                .unwrap();
            assert!(
                expiry_step(&mut prepared, &store, &mut SweepAccounting::default())
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
                expiry_step(&mut prepared, &store, &mut SweepAccounting::default())
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
                expiry_step(&mut prepared, &store, &mut SweepAccounting::default())
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
             Ok(())
         })
         .await
         .unwrap();
    }
    #[pioneer_postgres_test::postgres_test]
    async fn closed_unpublished_portions_without_runner_do_not_retain_frozen_input() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, _) = postgres_fixture(fixture, false).await;
            store
                .connection
                .execute_unprepared("DELETE FROM compaction_runner_state WHERE operation_id='op'")
                .await
                .unwrap();
            store
                .connection
                .execute_unprepared("UPDATE compaction_runner_plan SET ready=0 WHERE operation_id='op'")
                .await
                .unwrap();
            store
                .connection
                .execute_unprepared("UPDATE compaction_operation SET status='running' WHERE id='op'")
                .await
                .unwrap();
            let candidate = store.compaction_checkpoint("cp-1").await.unwrap().unwrap();
            // The repository closes portions without creating a runner or attempts.
            store
                .compaction_save_candidate(&candidate, 1)
                .await
                .unwrap();
            store
                .compaction_finish("op", "failed", "deadline")
                .await
                .unwrap();
            let op = operation::Entity::find_by_id("op")
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(op.next_portion, 2);
            assert_eq!(op.attempts, 0);
            assert!(store.compaction_runner_state("op").await.unwrap().is_none());
            assert!(
                !store
                    .compaction_resume_deadline("op", "next-turn", 2000)
                    .await
                    .unwrap()
            );
            let mut progress = FrozenStorageLifetimeProgress::default();
            for _ in 0..2500 {
                progress.quantum(&store).await.unwrap();
            }
            assert_eq!(
                history::Entity::find_by_id("origin")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap()
                    .expired,
                1
            );
            let physical = store.connection.query_one_raw(Statement::from_string(DbBackend::Postgres,
                "SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='origin'"))
                .await.unwrap().unwrap();
            assert_eq!(physical.try_get::<i64>("", "n").unwrap(), 0);
            assert_eq!(
                operation::Entity::find_by_id("op")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap()
                    .next_portion,
                2
            );
            for id in ["cp-0", "cp-1"] {
                let cp = crate::proof_publication::checkpoint_row(&store.connection, id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(cp.proof_version, 0);
                assert_eq!(cp.status, "candidate");
            }
            assert!(
                projection::Entity::find_by_id("op")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap()
                    .terminal_generation
                    .is_none()
            );
            assert!(store.compaction_runner_state("op").await.unwrap().is_none());
             Ok(())
         })
         .await
         .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn pre_runner_expiry_writer_rechecks_failure_and_absent_state() {
        for change in [
            "UPDATE compaction_operation SET status='running',outcome=NULL WHERE id='op'",
            "UPDATE compaction_operation SET outcome=NULL WHERE id='op'",
            "UPDATE compaction_operation SET attempts=1 WHERE id='op'",
            "runner appeared",
            "UPDATE compaction_operation_projection SET identity_sha256='changed' WHERE operation_id='op'",
            "UPDATE compaction_runner_plan SET ready=1 WHERE operation_id='op'",
        ] {
            pioneer_postgres_test::with_database(async |fixture| {
                let (store, _) = postgres_fixture(fixture, false).await;
                let saved = runner::Entity::find_by_id("op")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap();
                store
                    .connection
                    .execute_unprepared("DELETE FROM compaction_runner_state WHERE operation_id='op'")
                    .await
                    .unwrap();
                store
                    .connection
                    .execute_unprepared(
                        "UPDATE compaction_operation SET outcome='deadline' WHERE id='op'",
                    )
                    .await
                    .unwrap();
                store
                    .connection
                    .execute_unprepared(
                        "UPDATE compaction_runner_plan SET ready=0 WHERE operation_id='op'",
                    )
                    .await
                    .unwrap();
                // Candidate checkpoints with no published incoming path still go
                // through the ordinary reverse traversal; attempts=0 is no proof.
                let mut prepared = prepared_expiry(&store).await;
                if change == "runner appeared" {
                    store.connection.execute_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('op',$1,$2)",
                        [saved.generation.into(),saved.state.into()],
                    )).await.unwrap();
                    // Activation also makes the plan ready. The insert trigger
                    // invalidates terminal_generation; current writer facts refuse.
                    store
                        .connection
                        .execute_unprepared(
                            "UPDATE compaction_runner_plan SET ready=1 WHERE operation_id='op'",
                        )
                        .await
                        .unwrap();
                } else {
                    store.connection.execute_unprepared(change).await.unwrap();
                }
                let mut accounting = SweepAccounting::default();
                assert!(expiry_step(&mut prepared, &store, &mut accounting).await.unwrap());
                assert_eq!(accounting.expired, 0, "{change}");
                assert_eq!(
                    history::Entity::find_by_id("origin")
                        .one(&store.connection)
                        .await
                        .unwrap()
                        .unwrap()
                        .expired,
                    0,
                    "{change}",
                );
                 Ok(())
             })
             .await
             .unwrap();
        }
    }

    #[pioneer_postgres_test::postgres_test]
    async fn unused_staging_reset_delete_rollback_and_restart_never_publish_partial_ready() {
        let (store, _) = fixture(false).await;
        let mut progress = FrozenStorageLifetimeProgress::default();

        let header = history::Entity::find_by_id("origin")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        progress.expiry = Some(ExpiryPreparation::new(header));
        for _ in 0..100 {
            if progress
                .expiry
                .as_ref()
                .unwrap()
                .is_preparing_reverse_dependencies()
            {
                break;
            }
            progress.expiry_quantum(&store).await.unwrap();
        }
        assert!(
            progress
                .expiry
                .as_ref()
                .unwrap()
                .is_preparing_reverse_dependencies()
        );
        store
            .connection
            .execute_unprepared(
                "ALTER TABLE compaction_checkpoint RENAME TO unavailable_checkpoint",
            )
            .await
            .unwrap();
        progress.phase = 1;
        let error = progress.quantum(&store).await.unwrap_err();
        assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
        let retry = progress.expiry.as_ref().unwrap();
        assert_eq!(retry.header().id, "origin");
        assert!(!retry.roots_checked());
        assert_eq!(retry.completed_layouts(), 0);
        assert!(
            !retry.operations_checked()
                && !retry.checkpoints_checked()
                && !retry.is_preparing_reverse_dependencies()
        );
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
            expiry_step(&mut expiry, &store, &mut SweepAccounting::default())
                .await
                .unwrap()
        );
        let discard = DiscardPreparation {
            id: "cp-1".into(),
            manifest: "origin".into(),
            status: "failed".into(),
            generation: Some(0),
            reverse: Reachability::new("cp-1", "ws"),
            operation: "op".into(),
            owner: "owner".into(),
            checkpoint_status: "candidate".into(),
            thread: "thread".into(),
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
        progress.phase = 3;
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
            discard.reverse.step(&store.connection).await.unwrap();
        }
        assert!(!discard.reverse.reachable());
        store.connection.execute_unprepared("CREATE FUNCTION fixture_proof_delete_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture delete fault'; END; $$; CREATE TRIGGER fixture_proof_delete_fault BEFORE DELETE ON compaction_checkpoint_event_input FOR EACH ROW EXECUTE FUNCTION fixture_proof_delete_fault()").await.unwrap();
        assert!(discard.cleanup(&store).await.is_err());
        assert_eq!(
            crate::checkpoint_topology::checkpoint_topology(&store.connection, "cp-1")
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
            .execute_unprepared("DROP TRIGGER fixture_proof_delete_fault ON compaction_checkpoint_event_input; DROP FUNCTION fixture_proof_delete_fault()")
            .await
            .unwrap();
        discard.cleanup(&store).await.unwrap();
        assert_eq!(
            crate::checkpoint_topology::checkpoint_topology(&store.connection, "cp-1")
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
            crate::checkpoint_topology::checkpoint_topology(&store.connection, "cp-0")
                .await
                .unwrap()
                .unwrap()
                .row
                .proof_version,
            1,
            "existing incoming dependency preserves ancestor evidence"
        );
    }
    #[pioneer_postgres_test::postgres_test]
    async fn discard_preflight_skips_incoming_and_empty_keys_and_retries_db_errors() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, _) = postgres_fixture(fixture, false).await;
            let store = store.with_maintenance_access();
            checkpoint_proofs::prepare_graph(&store, "cp-1")
                .await
                .unwrap();
            expiry_step(&mut prepared_expiry(&store).await, &store, &mut SweepAccounting::default())
                .await
                .unwrap();
            store.connection.execute_unprepared("INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) SELECT 'cp-05-empty',operation_id,owner,NULL,2,summary,identity_sha256,selection,projection_version,format_version,'candidate' FROM compaction_checkpoint WHERE id='cp-0'").await.unwrap();
            let mut progress = FrozenStorageLifetimeProgress::default();

            progress.phase = 3;
            store
                .connection
                .execute_unprepared(
                    "ALTER TABLE compaction_checkpoint_import RENAME TO unavailable_import",
                )
                .await
                .unwrap();
            let error = progress.quantum(&store).await.unwrap_err();
            assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
            assert!(progress.discard_after.is_empty());
            assert!(progress.discard.is_none() && !progress.pass_done[3]);
            store
                .connection
                .execute_unprepared(
                    "ALTER TABLE unavailable_import RENAME TO compaction_checkpoint_import",
                )
                .await
                .unwrap();

            // A missing column makes *any* payload or length access fail in PostgreSQL,
            // even if JSON decoding were still deferred. No SQL-text assertion or
            // production instrumentation is needed for this negative control.
            store
                .connection
                .execute_unprepared(
                    "ALTER TABLE compaction_runner_state RENAME COLUMN state TO unavailable_state",
                )
                .await
                .unwrap();
            store.connection.execute_unprepared("ALTER TABLE compaction_runner_state RENAME COLUMN generation TO unavailable_generation").await.unwrap();
            for id in ["cp-0", "cp-05-empty"] {
                assert!(progress.discard_quantum(&store).await.unwrap());
                assert_eq!(progress.discard_after, id);
                assert!(
                    progress.discard.is_none(),
                    "refusal must not start reverse traversal"
                );
            }
            assert!(discard_preflight(&store.connection, "cp-1").await.unwrap());
            // Even an unknown foreign owner/version uses the final guard's broad
            // retention predicate; preflight must not reinterpret it as safe.
            store.connection.execute_unprepared("INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES('cp-05-empty','checkpoint:unknown','cp-1','unknown')").await.unwrap();
            assert!(!discard_preflight(&store.connection, "cp-1").await.unwrap());
            assert!(progress.discard_quantum(&store).await.unwrap());
            assert_eq!(progress.discard_after, "cp-1");
            assert!(
                progress.discard.is_none(),
                "foreign refusal must not read runner"
            );
            assert!(!progress.discard_quantum(&store).await.unwrap());
            store
                .connection
                .execute_unprepared("DELETE FROM compaction_coverage WHERE checkpoint_id='cp-05-empty'")
                .await
                .unwrap();
            // Next pass retries previously held keys. Positive preflight now reaches
            // the point read and fails without acknowledging cp-1.
            for id in ["cp-0", "cp-05-empty"] {
                assert!(progress.discard_quantum(&store).await.unwrap());
                assert_eq!(progress.discard_after, id);
            }
            let error = progress.discard_quantum(&store).await.unwrap_err();
            assert!(error.downcast_ref::<sea_orm::DbErr>().is_some());
            assert_eq!(progress.discard_after, "cp-05-empty");
            assert!(progress.discard.is_none());
            store
                .connection
                .execute_unprepared(
                    "ALTER TABLE compaction_runner_state RENAME COLUMN unavailable_state TO state",
                )
                .await
                .unwrap();
            store.connection.execute_unprepared("ALTER TABLE compaction_runner_state RENAME COLUMN unavailable_generation TO generation").await.unwrap();
            assert!(progress.discard_quantum(&store).await.unwrap());
            assert_eq!(progress.discard.as_ref().unwrap().id, "cp-1");
            for _ in 0..64 {
                if progress.discard.is_none() {
                    break;
                }
                progress.discard_quantum(&store).await.unwrap();
            }
            assert!(progress.discard.is_none());
            assert!(
                !discard_preflight(&store.connection, "cp-1").await.unwrap(),
                "demoted empty key has no work"
            );
            assert_eq!(
                value(&store, "compaction_checkpoint_event_input", "cp-1").await,
                0
            );

            let mut restart = FrozenStorageLifetimeProgress::default();
            for id in ["cp-0", "cp-05-empty", "cp-1"] {
                assert!(restart.discard_quantum(&store).await.unwrap());
                assert_eq!(restart.discard_after, id);
                assert!(restart.discard.is_none());
            }
            assert!(!restart.discard_quantum(&store).await.unwrap());
            assert!(restart.discard_after.is_empty());
             Ok(())
         })
         .await
         .unwrap();
    }

    // The graph-refusal tests protect retained data; ordinary proof reads must
    // also leave finished cleanup parked when nobody waited on their holds.
    #[pioneer_postgres_test::postgres_test]
    async fn refused_proof_reads_do_not_wake_cleanup_or_revalidate_origin() {
        pioneer_postgres_test::with_database(async |fixture| {
            use futures_util::FutureExt;
            let (store, _) = postgres_fixture(fixture, true).await;
            // Model pre-existing damaged legacy input; the production identity
            // guard correctly prevents creating it after publication today.
            store
                .connection
                .execute_unprepared("DROP TRIGGER frozen_complete_identity ON compaction_frozen_history")
                .await
                .unwrap();
            store.connection.execute_unprepared(
                "UPDATE compaction_frozen_history SET identity_sha256=repeat('0',64) WHERE id='origin'"
            ).await.unwrap();
            store.connection.execute_unprepared(
                "UPDATE compaction_operation_projection SET identity_sha256=repeat('0',64) WHERE manifest_id='origin'"
            ).await.unwrap();
            let database = store.database_connection();
            let mut source = database.work_signal("maintenance.frozen").subscribe();
            let mut html = database.work_signal("maintenance.html").subscribe();
            let mut released = database
                .work_signal("maintenance.frozen.release")
                .subscribe();
            source.begin_pass(&database).await.unwrap();
            html.begin_pass(&database).await.unwrap();
            let mut progress = FrozenStorageLifetimeProgress::default();
            let mut idle = false;
            for _ in 0..4000 {
                if !progress.quantum(&store).await.unwrap() {
                    idle = true;
                    break;
                }
            }
            assert!(idle && progress.accounting.refused > 0);
            assert!(store.frozen_readers.lock().unwrap().is_empty());
            assert!(source.wait(None).now_or_never().is_none());
            assert!(released.wait(None).now_or_never().is_none());
            // Expiry may have notified HTML for unrelated rows. Acknowledge before
            // the release-only pass whose proof reads must stay parked.
            html.begin_pass(&database).await.unwrap();
            let refusals = progress.accounting.refused;
            progress.recheck_released_holds();
            idle = false;
            for _ in 0..4000 {
                if !progress.quantum(&store).await.unwrap() {
                    idle = true;
                    break;
                }
            }
            assert!(idle);
            assert_eq!(progress.accounting.refused, refusals);
            assert!(released.wait(None).now_or_never().is_none());
            assert!(source.wait(None).now_or_never().is_none());
            assert!(html.wait(None).now_or_never().is_none());
            // Even a mixed internal/external read family stays quiet if neither
            // cleanup owner actually refused work because of these readers.
            let header = history::Entity::find_by_id("origin")
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap();
            let descriptor = FrozenHistoryRef {
                format: 1,
                manifest_id: header.id,
                messages: header.message_count as u64,
                identity_sha256: header.identity_sha256,
            };
            let external = store
                .compaction_acquire_frozen_history("ws", &descriptor)
                .await
                .unwrap();
            let (_, internal) = store
                .acquire_frozen_header("ws", &descriptor)
                .await
                .unwrap();
            drop(external);
            assert!(released.wait(None).now_or_never().is_none());
            drop(internal);
            assert!(released.wait(None).now_or_never().is_none());
             Ok(())
         })
         .await
         .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn periodic_cleanup_revisit_sees_released_reader_before_proof_eof() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, descriptor) = postgres_fixture(fixture, false).await;
            checkpoint_proofs::prepare_graph(&store, "cp-1")
                .await
                .unwrap();
            let hold = store
                .compaction_acquire_frozen_history("ws", &descriptor)
                .await
                .unwrap();
            let mut progress = FrozenStorageLifetimeProgress::default();

            progress.pass_done = [false, false, true, true];
            progress.proof = Some(checkpoint_proofs::GraphPreparation::new("cp-1"));
            progress.expiry = Some(prepared_expiry(&store).await);
            progress.expiry_after = "origin".into();
            let now = Instant::now();
            let interval = Duration::from_secs(60);
            progress.revisit_completed_cleanup(now, interval);
            progress.phase = 1;
            assert!(progress.quantum(&store).await.unwrap());
            assert!(progress.expiry.is_none());
            progress.phase = 1;
            assert!(progress.quantum(&store).await.unwrap());
            assert!(progress.pass_done[1], "held expiry visit reached real EOF");
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
            progress.revisit_completed_cleanup(now + interval - Duration::from_secs(1), interval);
            assert!(progress.pass_done[1]);
            progress.revisit_completed_cleanup(now + interval, interval);
            assert!(!progress.pass_done[1] && !progress.pass_done[0]);
            progress.phase = 1;
            assert!(progress.quantum(&store).await.unwrap());
            assert!(
                progress.expiry.is_some(),
                "periodic revisit rediscovers released origin"
            );
            for _ in 0..500 {
                if progress.expiry.is_none() {
                    break;
                }
                progress.expiry_quantum(&store).await.unwrap();
            }
            assert!(progress.expiry.is_none());
            assert_eq!(
                history::Entity::find_by_id("origin")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap()
                    .expired,
                1
            );
            assert!(progress.proof.is_some() && !progress.pass_done[0]);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn cleanup_revisit_keeps_active_preparations_frontiers_and_pass_confirmation() {
        pioneer_postgres_test::with_database(async |fixture| {
            let (store, _) = postgres_fixture(fixture, false).await;
            let mut progress = FrozenStorageLifetimeProgress::default();

            progress.pass_done = [false, true, false, true];
            progress.proof = Some(checkpoint_proofs::GraphPreparation::new("cp-1"));
            progress.proof_pending.push_back("queued-proof".into());
            progress.proof_after = "proof-frontier".into();
            progress.sweep = Some(SweepPreparation::new("active".into(), 0, 42));
            progress
                .sweep_pending
                .push_back(SweepPreparation::new("queued".into(), 1, 24));
            let now = Instant::now();
            let interval = Duration::from_secs(60);
            // Repeated unchanged worker batches do not reopen completed families,
            // even while a real proof preparation and queue remain unfinished.
            for seconds in 0..60 {
                progress.revisit_completed_cleanup(now + Duration::from_secs(seconds), interval);
                assert_eq!(progress.pass_done, [false, true, false, true]);
            }
            progress.revisit_completed_cleanup(now + interval, interval);
            assert_eq!(progress.pass_done, [false, false, false, false]);
            assert!(progress.proof.is_some());
            assert_eq!(progress.proof_after, "proof-frontier");
            assert_eq!(progress.proof_pending.front().unwrap(), "queued-proof");
            assert_eq!(progress.sweep.as_ref().unwrap().until, 42);
            assert_eq!(progress.sweep_pending.front().unwrap().manifest, "queued");

            // Periodic admission reopens durable pending discovery only. No RAM
            // inventory frontier exists to reset, and active visits survive above.
            progress.sweep = None;
            progress.sweep_pending.clear();
            progress.pass_done[2] = true;
            progress.revisit_completed_cleanup(now + interval * 2, interval);
            assert!(!progress.pass_done[2]);

            progress.proof = None;
            progress.proof_pending.clear();
            progress.pass_done = [true, true, true, true];
            progress.pass_changed = true;
            progress.revisit_completed_cleanup(now + interval * 3, interval);
            assert_eq!(progress.pass_done, [true, true, true, true]);
            assert!(
                progress.quantum(&store).await.unwrap(),
                "real change requests confirmation pass"
            );
            progress.pass_done = [true, true, true, true];
            assert!(
                !progress.quantum(&store).await.unwrap(),
                "unchanged complete pass reaches idle"
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn discard_preflight_does_not_authorize_later_incoming_hold_generation_or_status_change()
    {
        for change in ["previous", "foreign", "reader", "generation", "status"] {
            pioneer_postgres_test::with_database(async |fixture| {
                let (store, _) = postgres_fixture(fixture, false).await;
                checkpoint_proofs::prepare_graph(&store, "cp-1")
                    .await
                    .unwrap();
                expiry_step(&mut prepared_expiry(&store).await, &store, &mut SweepAccounting::default())
                    .await
                    .unwrap();
                let mut progress = FrozenStorageLifetimeProgress::default();
                // cp-0's incoming-edge refusal is covered in the keyset test.
                progress.discard_after = "cp-0".into();
                progress.discard_quantum(&store).await.unwrap();
                let mut discard = progress.discard.take().expect("preflight admitted cp-1");
                assert_eq!(discard.id, "cp-1");
                while !discard.reverse.done() {
                    discard.reverse.step(&store.connection).await.unwrap();
                }
                let mut hold = None;
                match change {
                    "previous" => {
                        store.connection.execute_unprepared("INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) SELECT 'incoming',operation_id,owner,id,2,summary,identity_sha256,selection,projection_version,format_version,'candidate' FROM compaction_checkpoint WHERE id='cp-1'").await.unwrap();
                    }
                    "foreign" => {
                        store.connection.execute_unprepared("INSERT INTO compaction_checkpoint(id,operation_id,owner,previous,portion,summary,identity_sha256,selection,projection_version,format_version,status) SELECT 'incoming',operation_id,owner,NULL,2,summary,identity_sha256,selection,projection_version,format_version,'candidate' FROM compaction_checkpoint WHERE id='cp-1'").await.unwrap();
                        store.connection.execute_unprepared("INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES('incoming','checkpoint:unknown','cp-1','unknown')").await.unwrap();
                    }
                    "reader" => {
                        // Exercise the last writer hold fence directly: normal
                        // public acquire already refuses an expired manifest.
                        hold = Some(crate::frozen_lifetime::FrozenReadHold::register(
                            &store.frozen_readers,
                            "origin",
                        ));
                    }
                    "generation" => {
                        let row = runner::Entity::find_by_id("op")
                            .one(&store.connection)
                            .await
                            .unwrap()
                            .unwrap();
                        let mut state: RunnerState = serde_json::from_value(row.state).unwrap();
                        state.generation += 1;
                        store.connection.execute_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "UPDATE compaction_runner_state SET generation=generation+1,state=$1::text::json WHERE operation_id='op'",
                            [serde_json::to_string(&state).unwrap().into()],
                        )).await.unwrap();
                    }
                    "status" => {
                        store
                            .connection
                            .execute_unprepared(
                                "UPDATE compaction_operation SET status='cancelled' WHERE id='op'",
                            )
                            .await
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
                discard.cleanup(&store).await.unwrap();
                assert_eq!(
                    crate::checkpoint_topology::checkpoint_topology(&store.connection, "cp-1")
                        .await
                        .unwrap()
                        .unwrap()
                        .row
                        .proof_version,
                    1,
                    "{change}"
                );
                assert_eq!(
                    value(&store, "compaction_checkpoint_event_input", "cp-1").await,
                    1,
                    "{change}"
                );
                drop(hold);
                 Ok(())
             })
             .await
             .unwrap();
        }
    }
}

#[cfg(test)]
#[path = "frozen_conversion_cancellation_tests.rs"]
mod conversion_cancellation_tests;
