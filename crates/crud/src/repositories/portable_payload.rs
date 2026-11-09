//! SQLite lifecycle/storage for the byte-only portable codec.
//! Owner discovery, fragments and publication use their existing Entities.
//! Custom expressions are confined to SQLite scalar metadata/ownership facts.
use crate::{CrudStore, portable_payload_codec as codec};
use anyhow::{Context, Result, ensure};
use pioneer_entity::{
    portable_payload as value, portable_payload_block as block,
    portable_payload_dictionary as dictionary,
};
use pioneer_observability::portable_payload as observe;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, OnConflict};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, Condition, ConnectionTrait, DbBackend, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, QueryTrait, TransactionTrait,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    ops::Deref,
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

const BLOCK: usize = 64 * 1024;
// A retired header is kept as the bounded cleanup cursor. Before removing any
// blocks, invalidate its digest durably: a capture that lost the source-to-hold
// race must retry its source, never decode a partially reclaimed value. Holds
// acquired before reclamation prevent this transition altogether.
const RECLAIMING_SHA256: &[u8] = &[];
const PAYLOAD_ID_LEN: usize = 12;
const RESERVATION_ATTEMPTS: u8 = 8;

// Register only cleanup actually blocked by a read hold. Ordinary maintenance
// reads must not invalidate their own completed pass when their hold drops.
fn blocked_cleanup() -> &'static Mutex<BTreeMap<String, Vec<pioneer_sqlite::WorkSignal>>> {
    static BLOCKED: OnceLock<Mutex<BTreeMap<String, Vec<pioneer_sqlite::WorkSignal>>>> =
        OnceLock::new();
    BLOCKED.get_or_init(Mutex::default)
}

type PayloadGates = Mutex<BTreeMap<String, Weak<RwLock<()>>>>;
fn payload_gates() -> &'static PayloadGates {
    static GATES: OnceLock<PayloadGates> = OnceLock::new();
    GATES.get_or_init(Mutex::default)
}
fn payload_gate(id: &str) -> Arc<RwLock<()>> {
    let mut gates = payload_gates()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let gate = gates.get(id).and_then(Weak::upgrade).unwrap_or_else(|| {
        let gate = Arc::new(RwLock::new(()));
        gates.insert(id.to_owned(), Arc::downgrade(&gate));
        gate
    });
    gate
}
fn release_payload_gate(id: &str) {
    let mut gates = payload_gates()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if gates.get(id).is_some_and(|gate| gate.strong_count() == 0) {
        gates.remove(id);
    }
}

/// Memory-only protection of an immutable payload reference. It owns no database
/// handle, transaction, stream, scheduling permit or background work.
#[derive(Clone)]
pub struct PayloadReadHold(Arc<PayloadReadLease>);
#[derive(Debug)]
struct PayloadReadLease {
    id: String,
    guard: Option<OwnedRwLockReadGuard<()>>,
}
impl std::fmt::Debug for PayloadReadHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PayloadReadHold")
    }
}
impl PayloadReadHold {
    /// Capture synchronously while mapping a row, before its writer commits.
    /// A reader losing the gap to cleanup must retry the entire source fetch.
    pub(crate) fn capture(id: &str, family: Family) -> Result<Self> {
        let guard = payload_gate(id).try_read_owned().map_err(|_| {
            let error = anyhow::Error::new(PreflightRace);
            match observe::hold_rejected(family.observed()) {
                Some(receipt) => error.context(receipt),
                None => error,
            }
        })?;
        Ok(Self(Arc::new(PayloadReadLease {
            id: id.to_owned(),
            guard: Some(guard),
        })))
    }
}
impl Drop for PayloadReadLease {
    fn drop(&mut self) {
        release_payload_hold(&self.id, &mut self.guard);
    }
}
struct PayloadCleanupHold {
    id: String,
    guard: Option<OwnedRwLockWriteGuard<()>>,
}
impl PayloadCleanupHold {
    fn try_acquire(id: &str) -> Option<Self> {
        let guard = payload_gate(id).try_write_owned().ok()?;
        Some(Self {
            id: id.to_owned(),
            guard: Some(guard),
        })
    }
}
impl Drop for PayloadCleanupHold {
    fn drop(&mut self) {
        release_payload_hold(&self.id, &mut self.guard);
    }
}

fn release_payload_hold<T>(id: &str, guard: &mut Option<T>) {
    // Both read holds and a competing cleanup writer can block discovery.
    // Serialize release with registration; notify every blocked DB runtime
    // sharing this existing payload gate, after releasing all native locks.
    let mut blocked = blocked_cleanup()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    drop(guard.take());
    release_payload_gate(id);
    let signals = blocked.remove(id).unwrap_or_default();
    drop(blocked);
    for signal in signals {
        signal.notify();
    }
}

pub(crate) struct PayloadHeader {
    value: value::Model,
    _hold: PayloadReadHold,
}
impl Deref for PayloadHeader {
    type Target = value::Model;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Family {
    ToolText,
    ToolMetadata,
    CliMapping,
    TaskHistory,
    TaskEvent,
}
impl Family {
    fn spec(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::ToolText => ("tool_output_chunk", "id", "text"),
            Self::ToolMetadata => ("tool_output_chunk", "id", "metadata"),
            Self::CliMapping => ("turn_cli_runtime_binding", "turn_id", "input_mapping_json"),
            Self::TaskHistory => ("task_run_conversation_snapshot", "run_id", "history_json"),
            Self::TaskEvent => ("task_event", "id", "payload_json"),
        }
    }
    fn observed(self) -> observe::Family {
        match self {
            Self::ToolText => observe::Family::ToolText,
            Self::ToolMetadata => observe::Family::ToolMetadata,
            Self::CliMapping => observe::Family::CliMapping,
            Self::TaskHistory => observe::Family::TaskHistory,
            Self::TaskEvent => observe::Family::TaskEvent,
        }
    }
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::ToolText => "tool.text",
            Self::ToolMetadata => "tool.metadata",
            Self::CliMapping => "cli.mapping",
            Self::TaskHistory => "task.history",
            Self::TaskEvent => "task.event",
        }
    }
}
fn observed_family(name: &str) -> observe::Family {
    match name {
        "tool.text" => observe::Family::ToolText,
        "tool.metadata" => observe::Family::ToolMetadata,
        "cli.mapping" => observe::Family::CliMapping,
        "task.history" => observe::Family::TaskHistory,
        "task.event" => observe::Family::TaskEvent,
        _ => observe::Family::Mixed,
    }
}

// The codec remains DB-neutral. This fixed-size adapter drains its private
// cache/native timings even on an error or unwind, without retaining input.
struct CodecObservation(codec::CodecDiagnostics);
impl Drop for CodecObservation {
    fn drop(&mut self) {
        let d = &self.0;
        for (stage, elapsed) in [
            (observe::Stage::DictionaryLookup, d.lookup_elapsed),
            (observe::Stage::DictionaryPrepare, d.prepare_elapsed),
            (observe::Stage::DictionaryTrain, d.train_elapsed),
            (observe::Stage::Compress, d.compress_elapsed),
            (observe::Stage::Decompress, d.decompress_elapsed),
            (observe::Stage::VerifyAssemble, d.verify_elapsed),
        ] {
            if !elapsed.is_zero() {
                observe::phase(stage, elapsed);
            }
        }
        for (event, count) in [
            (observe::DictionaryEvent::Hit, d.cache_hits),
            (observe::DictionaryEvent::Miss, d.cache_misses),
            (observe::DictionaryEvent::Train, d.trainings),
            (observe::DictionaryEvent::Prepare, d.preparations),
            (
                observe::DictionaryEvent::TrainFallback,
                d.training_fallbacks,
            ),
        ] {
            for _ in 0..count {
                observe::dictionary_event(event);
            }
        }
    }
}
fn codec_observed<T>(
    operation: impl FnOnce(&mut codec::CodecDiagnostics) -> Result<T>,
) -> Result<T> {
    let mut observation = CodecObservation(codec::CodecDiagnostics {
        on_stage: Some(|stage| {
            observe::live_stage(match stage {
                codec::CodecStage::Train => observe::Stage::DictionaryTrain,
                codec::CodecStage::Prepare => observe::Stage::DictionaryPrepare,
                codec::CodecStage::Compress => observe::Stage::Compress,
                codec::CodecStage::Decompress => observe::Stage::Decompress,
            })
        }),
        ..Default::default()
    });
    operation(&mut observation.0)
}

// The only varying schema facts are these five existing Entity columns. Keep
// ordinary owner access typed rather than dispatching SQL table/column strings.
macro_rules! target {
    ($family:expr, |$entity:ident, $col:ident, $key:ident, $field:ident, $reference:ident| $body:block) => {{
        macro_rules! apply {
            ($module:ident, $key_variant:ident, $field_variant:ident, $ref_variant:ident) => {{
                use pioneer_entity::$module::{Column as $col, Entity as $entity};
                let $key = $col::$key_variant;
                let $field = $col::$field_variant;
                let $reference = $col::$ref_variant;
                $body
            }};
        }
        match $family {
            Family::ToolText => apply!(tool_output_chunk, Id, Text, TextPayloadId),
            Family::ToolMetadata => apply!(tool_output_chunk, Id, Metadata, MetadataPayloadId),
            Family::CliMapping => apply!(
                turn_cli_runtime_binding,
                TurnId,
                InputMappingJson,
                InputMappingJsonPayloadId
            ),
            Family::TaskHistory => apply!(
                task_run_conversation_snapshot,
                RunId,
                HistoryJson,
                HistoryJsonPayloadId
            ),
            Family::TaskEvent => apply!(task_event, Id, PayloadJson, PayloadJsonPayloadId),
        }
    }};
}

#[derive(sea_orm::FromQueryResult)]
struct PendingSource {
    owner: String,
    portable_revision: i64,
    portable_identity: String,
    owner_stamp: String,
    bytes: i64,
    eligible: bool,
    invalid_root: bool,
}

#[derive(sea_orm::FromQueryResult)]
struct PendingKey {
    owner: String,
}

// NULL is a verified inline history; the empty-string legacy sentinel must
// still be resolved by frozen-locator recovery. A known Task frozen descriptor
// already points to its stored history and has no work for this codec.
// Use the same admission for the finite ceiling and every pending page.
fn pending_locator_condition(family: Family) -> Condition {
    match family {
        Family::TaskHistory => Condition::all().add(
            pioneer_entity::task_run_conversation_snapshot::Column::FrozenManifestId.is_null(),
        ),
        Family::CliMapping => {
            use pioneer_entity::turn_cli_runtime_binding::Column;
            Condition::any()
                .add(Column::FrozenManifestId.is_null())
                .add(Column::FrozenManifestId.ne(""))
        }
        _ => Condition::all(),
    }
}

async fn pending_upper(store: &CrudStore, family: Family) -> Result<Option<PendingKey>> {
    let _phase = observe::stage(observe::Stage::SourceRead);
    store
        .run_background_database_quantum(|| async {
            target!(family, |Target, Col, key, field, reference| {
                let mut query = Target::find()
                    .select_only()
                    .column_as(key, "owner")
                    .filter(reference.is_null())
                    .filter(field.is_not_null())
                    .filter(pending_locator_condition(family))
                    .filter(Expr::cust(terminal_admission_sql(family)));
                query = query.order_by_desc(key);
                Ok(query
                    .limit(1)
                    .into_model::<PendingKey>()
                    .one(&store.connection)
                    .await?)
            })
        })
        .await
}

async fn pending_sources(
    store: &CrudStore,
    family: Family,
    after: Option<&str>,
    upper: Option<&str>,
    limit: u64,
) -> Result<Vec<PendingSource>> {
    let _phase = observe::stage(observe::Stage::SourceRead);
    store
        .run_background_database_quantum(|| async {
            target!(family, |Target, Col, key, field, reference| {
                let mut query = Target::find()
                    .select_only()
                    .column_as(key, "owner")
                    .columns([Col::PortableRevision, Col::PortableIdentity])
                    .expr_as(Expr::cust(owner_stamp_sql(family)), "owner_stamp")
                    .expr_as(Expr::cust(eligibility_sql(family)), "eligible")
                    .expr_as(Expr::cust(invalid_root_sql(family)), "invalid_root")
                    .expr_as(
                        Func::cust(Alias::new("octet_length")).arg(Expr::col(field)),
                        "bytes",
                    )
                    .filter(reference.is_null())
                    .filter(field.is_not_null())
                    .filter(pending_locator_condition(family))
                    .filter(Expr::cust(terminal_admission_sql(family)));
                if let Some(after) = after {
                    query = query.filter(key.gt(after));
                }
                if let Some(upper) = upper {
                    query = query.filter(key.lte(upper));
                }
                query = query.order_by_asc(key);
                Ok(query
                    .limit(limit)
                    .into_model::<PendingSource>()
                    .all(&store.connection)
                    .await?)
            })
        })
        .await
}

/// Only this worker owns its pending preparation. Staged headers are retired
/// until publication; cancellation/restart leaves them eligible for cleanup.
#[derive(Default)]
pub struct PortablePayloadProgress {
    family: usize,
    finished: [bool; 5],
    pending: Option<Preparation>,
    after: [Option<String>; 5],
    // Finite per-pass fence only: reset/re-discover after EOF. A hot stream
    // of higher keys cannot prevent revisiting logical updates below cursor.
    upper: [Option<String>; 5],
    cleanup_after: Option<String>,
    cleanup_upper: Option<String>,
    cleanup_finished: bool,
}
struct Preparation {
    family: Family,
    owner: String,
    revision: i64,
    identity: String,
    owner_stamp: String,
    size: usize,
    offset: usize,
    bytes: Vec<u8>,
    digest: Sha256,
    chars: i64,
    opaque: bool,
    json_projection: Option<super::portable_payload_json::Projection>,
    prepared: Prepared,
    dictionary: Option<codec::Dictionary>,
    shape: Option<u8>,
}
struct Prepared {
    _read_hold: Option<PayloadReadHold>,
    header: value::Model,
    blocks: Vec<block::Model>,
    next: i64,
    written: i64,
    encoded_bytes: usize,
    finalized: bool,
    installed: bool,
    reservation_attempts: u8,
    projection: Option<String>,
}

impl PortablePayloadProgress {
    pub async fn quantum(&mut self, store: &CrudStore) -> Result<bool> {
        let family = self
            .pending
            .as_ref()
            .map_or(observe::Family::Mixed, |p| p.family.observed());
        observe::scope_result(family, observe::Mode::Background, self.quantum_inner(store)).await
    }
    async fn quantum_inner(&mut self, store: &CrudStore) -> Result<bool> {
        if let Some(mut pending) = self.pending.take() {
            let result = pending.step(store).await;
            match result {
                Ok(false) => self.pending = Some(pending),
                Ok(true) => {
                    // Publication can lose its source CAS and leave retired
                    // staging behind the already finished cleanup frontier.
                    self.cleanup_finished = false;
                    self.cleanup_upper = None;
                    self.cleanup_after = None;
                }
                Err(error) => {
                    self.cleanup_finished = false;
                    self.cleanup_upper = None;
                    self.cleanup_after = None;
                    // Database phases already retry transient lock failures.
                    // Advance this pass; a failed source is diagnosed and
                    // revisited after EOF. It cannot starve later sources.
                    // Only the fixed field family is diagnostic; source IDs,
                    // JSON, dictionary bytes and driver errors stay out of logs.
                    tracing::warn!(
                        family = pending.family.name(),
                        "portable payload preparation failed; source remains pending"
                    );
                    return Err(error);
                }
            }
            return Ok(true);
        }
        if !self.cleanup_finished {
            if self.cleanup_upper.is_none() {
                self.cleanup_upper = value::Entity::find()
                    .select_only()
                    .column(value::Column::Id)
                    .filter(value::Column::Retired.eq(1))
                    .order_by_desc(value::Column::Id)
                    .into_tuple::<String>()
                    .one(&store.connection)
                    .await?;
            }
            if let Some(upper) = self.cleanup_upper.as_deref() {
                let (cleaned, cleanup_after) = store
                    .run_background_database_quantum(|| {
                        cleanup(store, self.cleanup_after.as_deref(), Some(upper))
                    })
                    .await?;
                self.cleanup_after = cleanup_after;
                if cleaned {
                    return Ok(true);
                }
            }
            self.cleanup_finished = true;
        }
        // Only families installed by the corresponding target migration are
        // registered here. Completed EOF is deliberately revisited next cycle.
        const FAMILIES: &[Family] = &[
            Family::ToolText,
            Family::ToolMetadata,
            Family::CliMapping,
            Family::TaskHistory,
            Family::TaskEvent,
        ];
        if self.finished.iter().all(|done| *done) {
            self.finished = [false; 5];
            self.cleanup_finished = false;
            self.cleanup_upper = None;
            return Ok(false);
        }
        while self.finished[self.family] {
            self.family = (self.family + 1) % FAMILIES.len();
        }
        let family = FAMILIES[self.family];
        observe::set_family(family.observed());
        self.family = (self.family + 1) % FAMILIES.len();
        // NULL metadata is final SQL NULL, not an empty/pending value.
        let index = family as usize;
        if self.after[index].is_none() {
            self.upper[index] = pending_upper(store, family).await?.map(|row| row.owner);
        }
        let Some(upper) = self.upper[index].as_deref() else {
            self.finished[index] = true;
            observe::outcome(observe::Outcome::NoWork);
            return Ok(true);
        };
        // Start with one row for dense queues. Only refusals grow the next
        // page, and growth resets each quantum: an eligible tail is never
        // repeatedly fetched in large pages. No candidates survive an await
        // into another quantum; source preparation/publication revalidate CAS.
        let mut remaining = 64_u64;
        let mut limit = 1_u64;
        while remaining > 0 {
            let rows = pending_sources(
                store,
                family,
                self.after[index].as_deref(),
                Some(upper),
                limit,
            )
            .await?;
            let exhausted = rows.len() < limit as usize;
            remaining -= rows.len() as u64;
            for row in rows {
                self.after[index] = Some(row.owner.clone());
                ensure!(!row.invalid_root, "invalid frozen history counters");
                if !row.eligible {
                    // Outcome describes the entire quantum, not individual skips.
                    // A later read/counter failure must retain Error classification.
                    continue;
                }
                let payload_id = pioneer_protocol::generate_id(PAYLOAD_ID_LEN);
                self.pending = Some(Preparation {
                    family,
                    owner: row.owner,
                    revision: row.portable_revision,
                    identity: row.portable_identity,
                    owner_stamp: row.owner_stamp,
                    size: usize::try_from(row.bytes)?,
                    offset: 0,
                    bytes: Vec::new(),
                    digest: Sha256::new(),
                    chars: 0,
                    opaque: false,
                    json_projection: (family == Family::CliMapping)
                        .then(super::portable_payload_json::Projection::default),
                    prepared: Prepared {
                        _read_hold: Some(PayloadReadHold::capture(&payload_id, family)?),
                        header: value::Model {
                            id: payload_id,
                            family: family.name().into(),
                            original_bytes: row.bytes,
                            original_chars: 0,
                            sha256: vec![0; 32],
                            raw: 1,
                            retired: 1,
                            dictionary_id: None,
                        },
                        blocks: Vec::new(),
                        next: 0,
                        written: 0,
                        encoded_bytes: 0,
                        finalized: false,
                        installed: false,
                        reservation_attempts: 0,
                        projection: None,
                    },
                    dictionary: None,
                    shape: None,
                });
                return Ok(true);
            }
            if exhausted {
                self.after[index] = None;
                self.upper[index] = None;
                self.finished[index] = true;
                break;
            }
            limit = std::cmp::min(limit * 2, remaining);
        }
        // A bounded source page is not EOF for the other families.
        observe::outcome(observe::Outcome::NoWork);
        Ok(true)
    }
}
impl Preparation {
    async fn step(&mut self, store: &CrudStore) -> Result<bool> {
        let owner = &self.owner;
        let revision = self.revision;
        let identity = &self.identity;
        let stamp = &self.owner_stamp;
        let prepared = &mut self.prepared;
        // Source reads carry this guard themselves. Staging/finalization steps
        // also revalidate before work: a resumed Turn/Run must not keep paying
        // for a preparation admitted while it was terminal. No DB lease spans
        // codec work; publication independently repeats the full predicate.
        if !prepared.installed || !prepared.blocks.is_empty() || self.offset >= self.size {
            let family = self.family;
            let current = store
                .run_background_database_quantum(|| async {
                    target!(family, |Target, Col, key, _field, reference| {
                        Ok(Target::find()
                            .select_only()
                            .column(key)
                            .filter(key.eq(owner))
                            .filter(Col::PortableRevision.eq(revision))
                            .filter(Col::PortableIdentity.eq(identity))
                            .filter(reference.is_null())
                            .filter(Expr::cust(owner_stamp_sql(family)).eq(stamp))
                            .filter(Expr::cust(eligibility_sql(family)))
                            .into_tuple::<String>()
                            .one(&store.connection)
                            .await?
                            .is_some())
                    })
                })
                .await?;
            if !current {
                observe::outcome(observe::Outcome::Stale);
                return Ok(true);
            }
        }

        if !prepared.installed {
            ensure!(
                prepared._read_hold.is_some(),
                "portable payload reservation hold missing"
            );
            ensure!(
                prepared.reservation_attempts < RESERVATION_ATTEMPTS,
                "portable payload reservation attempts exhausted"
            );
            prepared.reservation_attempts += 1;
            let affected = {
                let _phase = observe::stage(observe::Stage::Publish);
                store
                    .run_background_database_quantum(|| async {
                        Ok(
                            value::Entity::insert(prepared.header.clone().into_active_model())
                                .on_conflict(
                                    OnConflict::column(value::Column::Id)
                                        .do_nothing()
                                        .to_owned(),
                                )
                                .exec_without_returning(&store.connection)
                                .await?,
                        )
                    })
                    .await?
            };
            match affected {
                1 => prepared.installed = true,
                0 => {
                    // A targeted conflict never establishes ownership, even if
                    // every header field matches. Drop the foreign-ID hold now.
                    prepared._read_hold.take();
                    ensure!(
                        prepared.reservation_attempts < RESERVATION_ATTEMPTS,
                        "portable payload reservation attempts exhausted"
                    );
                    let id = pioneer_protocol::generate_id(PAYLOAD_ID_LEN);
                    let hold = PayloadReadHold::capture(&id, self.family)?;
                    prepared.header.id = id;
                    prepared._read_hold = Some(hold);
                }
                _ => anyhow::bail!("portable payload reservation affected invalid row count"),
            }
            return Ok(false);
        }
        // At most sixteen encoded blocks (about 1 MiB), never a full value.
        if prepared.blocks.len() >= 16
            || (self.offset == self.size && self.bytes.is_empty() && !prepared.blocks.is_empty())
        {
            let _phase = observe::stage(observe::Stage::Publish);
            store
                .run_background_database_quantum(|| async {
                    block::Entity::insert_many(
                        prepared
                            .blocks
                            .iter()
                            .cloned()
                            .map(IntoActiveModel::into_active_model),
                    )
                    .on_conflict(
                        OnConflict::columns([block::Column::PayloadId, block::Column::Ordinal])
                            .do_nothing()
                            .to_owned(),
                    )
                    .exec_without_returning(&store.connection)
                    .await?;
                    Ok(())
                })
                .await?;
            prepared.blocks.clear();
            return Ok(false);
        }
        if self.offset < self.size {
            ensure!(
                self.bytes.len() < 4,
                "portable UTF-8 carry exceeds one scalar"
            );
            let count = (BLOCK - self.bytes.len()).min(self.size - self.offset);
            let family = self.family;
            let source_offset = i64::try_from(self.offset + 1)?;
            let source_size = i64::try_from(self.size)?;
            let source_owner = &self.owner;
            let source_revision = self.revision;
            let source_identity = &self.identity;
            let source_stamp = &self.owner_stamp;
            let (table, _, column) = family.spec();
            // Select rowid only; the driver holds this exact guard's snapshot
            // while reading a native TEXT/BLOB range on the same reader.
            let guard = target!(family, |Target, Col, key, field, reference| {
                Target::find()
                    .select_only()
                    .expr(Expr::col((Alias::new(table), Alias::new("rowid"))))
                    .filter(key.eq(source_owner))
                    .filter(Col::PortableRevision.eq(source_revision))
                    .filter(reference.is_null())
                    .filter(
                        Expr::expr(Func::cust(Alias::new("octet_length")).arg(Expr::col(field)))
                            .eq(source_size),
                    )
                    .filter(Col::PortableIdentity.eq(source_identity))
                    .filter(Expr::cust(owner_stamp_sql(family)).eq(source_stamp))
                    .filter(Expr::cust(eligibility_sql(family)))
                    .limit(1)
                    .build(DbBackend::Sqlite)
            });
            let read_phase = observe::stage(observe::Stage::SourceRead);
            let row = store
                .run_background_database_quantum(|| async {
                    Ok(store
                        .connection
                        .read_blob_range(
                            pioneer_sqlite::SqliteBlobSource { table, column },
                            guard.clone(),
                            u64::try_from(source_offset - 1)?,
                            count,
                            u64::try_from(source_size)?,
                        )
                        .await?)
                })
                .await?;
            drop(read_phase);
            let Some(fragment) = row else {
                observe::outcome(observe::Outcome::Stale);
                return Ok(true);
            };
            observe::bytes(observe::ByteKind::ReadOriginal, fragment.len() as u64);
            let verify_source = observe::stage(observe::Stage::VerifyAssemble);
            ensure!(
                fragment.len() == count,
                "portable source fragment incomplete"
            );
            ensure!(
                self.family != Family::TaskHistory || !fragment.contains(&0),
                "portable Task history contains unsupported legacy NUL"
            );
            drop(verify_source);
            if self.size > 126 && self.dictionary.is_none() {
                self.dictionary =
                    Some(load_or_prepare_dictionary(store, self.family, &fragment).await?);
            }
            let _verify = observe::stage(observe::Stage::VerifyAssemble);
            if self.shape.is_none() {
                self.shape = fragment.iter().copied().find(|b| *b != b' ');
            }
            if let Some(projection) = &mut self.json_projection {
                projection.feed(&fragment);
            }
            self.digest.update(&fragment);
            self.offset += fragment.len();
            self.bytes.extend(fragment);
        }
        if !self.bytes.is_empty() {
            let verify = observe::stage(observe::Stage::VerifyAssemble);
            let mut end = self.bytes.len().min(BLOCK);
            // SQL byte fragments may end inside a scalar. Retain at most the
            // unfinished scalar for the next short read, never re-read bytes.
            loop {
                match std::str::from_utf8(&self.bytes[..end]) {
                    Ok(_) => break,
                    Err(error) if error.error_len().is_none() => end = error.valid_up_to(),
                    Err(_) => anyhow::bail!("portable source contains invalid UTF-8"),
                }
            }
            ensure!(
                end > 0 || self.offset < self.size,
                "portable source has truncated UTF-8"
            );
            if end > 0 {
                let text = std::str::from_utf8(&self.bytes[..end])?;
                let chars = text.chars().count() as i64;
                drop(verify);
                if self.size > 126 {
                    let data = codec_observed(|diagnostics| {
                        codec::encode_observed(
                            text.as_bytes(),
                            self.dictionary
                                .as_ref()
                                .context("portable dictionary missing")?,
                            diagnostics,
                        )
                    })?;
                    observe::bytes(observe::ByteKind::EncodedOriginal, end as u64);
                    observe::bytes(observe::ByteKind::Encoded, data.len() as u64);
                    observe::blocks(observe::BlockKind::Encoded, 1);
                    let _assembly = observe::stage(observe::Stage::VerifyAssemble);
                    prepared.encoded_bytes = prepared
                        .encoded_bytes
                        .checked_add(data.len() + 48)
                        .context("portable encoded byte overflow")?;
                    prepared.blocks.push(block::Model {
                        payload_id: prepared.header.id.clone(),
                        ordinal: prepared.next,
                        byte_offset: prepared.written,
                        char_offset: self.chars,
                        original_bytes: end as i64,
                        original_chars: chars,
                        data,
                    });
                    prepared.next += 1;
                }
                prepared.written += end as i64;
                self.chars += chars;
                self.bytes.drain(..end);
            }
            return Ok(false);
        }
        if self.offset < self.size {
            return Ok(false);
        }
        if !prepared.finalized {
            let verify = observe::stage(observe::Stage::VerifyAssemble);
            ensure!(
                prepared.written == self.size as i64,
                "portable staged byte total mismatch"
            );
            if let Some(projection) = &mut self.json_projection {
                self.opaque = projection.finish();
            }
            let unsupported = self
                .json_projection
                .as_ref()
                .is_some_and(|p| p.unsupported());
            if unsupported {
                tracing::warn!("portable CLI projection retained unsupported legacy JSON as raw");
            }
            // PPAY v1 has a 78-byte envelope and the benefit model adds
            // 48 bytes per block row. Even a zero-byte body cannot win here.
            let raw = self.size <= 126 || prepared.encoded_bytes >= self.size || unsupported;
            prepared.header.raw = i32::from(raw);
            prepared.header.original_chars = self.chars;
            prepared.header.sha256 = self.digest.clone().finalize().to_vec();
            prepared.header.dictionary_id = if raw {
                None
            } else {
                Some(dictionary_key(
                    self.family.name(),
                    self.dictionary
                        .as_ref()
                        .context("portable dictionary missing")?
                        .digest(),
                ))
            };
            prepared.projection = if raw {
                None
            } else {
                Some(match self.family {
                    // Exactly [] is too small to reach compressed state. All
                    // other legacy arrays retain their '[' source-kind predicate.
                    Family::TaskHistory => {
                        if self.shape == Some(b'[') {
                            "[".into()
                        } else {
                            "{}".into()
                        }
                    }
                    Family::CliMapping => {
                        if self.opaque {
                            r#"{"pioneerContextBasis":{"historyJson":"opaque"}}"#.into()
                        } else {
                            "{}".into()
                        }
                    }
                    _ => String::new(),
                })
            };
            drop(verify);
            let _publication = observe::stage(observe::Stage::Publish);
            store
                .run_background_database_quantum(|| async {
                    value::Entity::update_many()
                        .col_expr(value::Column::Raw, Expr::value(prepared.header.raw))
                        .col_expr(
                            value::Column::OriginalChars,
                            Expr::value(prepared.header.original_chars),
                        )
                        .col_expr(
                            value::Column::Sha256,
                            Expr::value(prepared.header.sha256.clone()),
                        )
                        .col_expr(
                            value::Column::DictionaryId,
                            Expr::value(prepared.header.dictionary_id.clone()),
                        )
                        .filter(value::Column::Id.eq(&prepared.header.id))
                        .filter(value::Column::Retired.eq(1))
                        .exec(&store.connection)
                        .await?;
                    Ok(())
                })
                .await?;
            prepared.finalized = true;
            return Ok(false);
        }
        // An unprofitable value is terminal raw. Discard staged blocks in
        // bounded portions before publishing its completed reference.
        if prepared.header.raw == 1 && prepared.next > 0 && {
            let _phase = observe::stage(observe::Stage::Cleanup);
            store
                .run_background_database_quantum(|| clear_blocks(store, &prepared.header.id))
                .await?
        } {
            return Ok(false);
        }
        store
            .run_background_database_quantum(|| {
                publish(
                    store,
                    self.family,
                    &self.owner,
                    self.revision,
                    &self.identity,
                    &self.owner_stamp,
                    prepared,
                )
            })
            .await?;
        Ok(true)
    }
}
use sea_orm::IntoActiveModel;
async fn load_or_prepare_dictionary(
    store: &CrudStore,
    family: Family,
    sample: &[u8],
) -> Result<codec::Dictionary> {
    if let Some(row) = dictionary_row(store, family).await? {
        let result = codec_observed(|d| codec::Dictionary::from_bytes_observed(row.data, d))?;
        ensure!(
            dictionary_key(family.name(), result.digest()) == row.id,
            "portable dictionary digest mismatch"
        );
        return Ok(result);
    }
    let samples = sample[..sample.len().min(1024 * 1024)]
        .chunks(1024)
        .collect::<Vec<_>>();
    let dict = codec_observed(|d| codec::train_observed(&samples, d))?;
    let publish_dictionary = observe::stage(observe::Stage::Publish);
    store
        .run_background_database_quantum(|| async {
            dictionary::Entity::insert(dictionary::ActiveModel {
                id: Set(dictionary_key(family.name(), dict.digest())),
                family: Set(family.name().into()),
                version: Set(1),
                data: Set(dict.bytes().to_vec()),
            })
            .on_conflict(
                OnConflict::columns([dictionary::Column::Family, dictionary::Column::Version])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&store.connection)
            .await?;
            Ok(())
        })
        .await?;
    drop(publish_dictionary);
    let winner = dictionary_row(store, family)
        .await?
        .context("portable dictionary publication missing")?;
    let dict = codec_observed(|d| codec::Dictionary::from_bytes_observed(winner.data, d))?;
    ensure!(
        dictionary_key(family.name(), dict.digest()) == winner.id,
        "portable dictionary digest mismatch"
    );
    Ok(dict)
}
async fn cleanup(
    store: &CrudStore,
    after: Option<&str>,
    through: Option<&str>,
) -> Result<(bool, Option<String>)> {
    let _phase = observe::stage(observe::Stage::Cleanup);
    // A held/staging value must not starve other retired values. Fetch only
    // IDs (never payload bytes); skip held IDs with a bounded seek page.
    let mut query = value::Entity::find()
        .select_only()
        .column(value::Column::Id)
        .filter(value::Column::Retired.eq(1))
        .order_by_asc(value::Column::Id);
    if let Some(after) = after {
        query = query.filter(value::Column::Id.gt(after));
    }
    if let Some(through) = through {
        query = query.filter(value::Column::Id.lte(through));
    }
    let candidates = query
        .limit(64)
        .into_tuple::<String>()
        .all(&store.connection)
        .await?;
    let full_page = candidates.len() == 64;
    let mut last = None;
    for id in candidates {
        last = Some(id.clone());
        // Never await a logical gate while occupying database capacity.
        let exclusive = {
            let mut blocked = blocked_cleanup()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let exclusive = PayloadCleanupHold::try_acquire(&id);
            if exclusive.is_none() {
                let signal = store.connection.work_signal("maintenance.portable");
                let signals = blocked.entry(id.clone()).or_default();
                if !signals
                    .iter()
                    .any(|existing| existing.same_channel(&signal))
                {
                    signals.push(signal);
                }
            }
            exclusive
        };
        let Some(_exclusive) = exclusive else {
            continue;
        };
        // Revalidate after discovery. Live/staging publication has its own
        // read hold, so it cannot overlap this exclusive deletion quantum.
        let marked = value::Entity::update_many()
            .col_expr(
                value::Column::Sha256,
                Expr::value(RECLAIMING_SHA256.to_vec()),
            )
            .filter(value::Column::Id.eq(&id))
            .filter(value::Column::Retired.eq(1))
            .exec(&store.connection)
            .await?;
        if marked.rows_affected == 0 {
            continue;
        }
        observe::outcome(observe::Outcome::Cleanup);
        if !clear_blocks(store, &id).await? {
            value::Entity::delete_many()
                .filter(value::Column::Id.eq(id))
                .filter(value::Column::Retired.eq(1))
                .exec(&store.connection)
                .await?;
        }
        // Stay on this header until all bounded deletion batches finish.
        return Ok((true, after.map(str::to_owned)));
    }
    Ok(if full_page {
        (true, last)
    } else {
        (false, None)
    })
}

pub(crate) async fn header(
    db: &pioneer_sqlite::SqliteDatabase,
    id: &str,
    family: Family,
) -> Result<PayloadHeader> {
    let hold = PayloadReadHold::capture(id, family)?;
    read_header(db, id, family, hold, false).await
}

/// A record captured before commit may outlive logical replacement/deletion.
/// Its hold protects the original backing even after its header is retired.
pub(crate) async fn header_with_hold(
    db: &pioneer_sqlite::SqliteDatabase,
    id: &str,
    family: Family,
    hold: &PayloadReadHold,
) -> Result<PayloadHeader> {
    ensure!(hold.0.id == id, "portable payload hold identity mismatch");
    read_header(db, id, family, hold.clone(), true).await
}

async fn read_header(
    db: &pioneer_sqlite::SqliteDatabase,
    id: &str,
    family: Family,
    hold: PayloadReadHold,
    captured: bool,
) -> Result<PayloadHeader> {
    let row = {
        let _read = observe::stage(observe::Stage::BlockRead);
        value::Entity::find_by_id(id).one(db).await?
    }
    .ok_or_else(|| {
        observe::outcome(observe::Outcome::Stale);
        PreflightRace
    })?;
    ensure!(
        row.family == family.name()
            && matches!(row.retired, 0 | 1)
            && row.original_bytes >= 0
            && row.original_chars >= 0
            && row.original_chars <= row.original_bytes
            && row.original_bytes <= row.original_chars.saturating_mul(4)
            && matches!(row.raw, 0 | 1)
            && (row.sha256.len() == 32 || (row.retired == 1 && row.sha256 == RECLAIMING_SHA256))
            && ((row.raw == 1 && row.dictionary_id.is_none())
                || (row.raw == 0 && row.original_bytes > 0 && row.dictionary_id.is_some())),
        "portable payload header invalid"
    );
    if row.retired == 1 && (!captured || row.sha256 == RECLAIMING_SHA256) {
        observe::outcome(observe::Outcome::Stale);
        return Err(PreflightRace.into());
    }
    Ok(PayloadHeader {
        value: row,
        _hold: hold,
    })
}
/// Call only after releasing any transaction. A full logical read remains O(n).
pub(crate) async fn hydrate(
    db: &pioneer_sqlite::SqliteDatabase,
    family: Family,
    id: Option<&str>,
    plain: String,
) -> Result<String> {
    hydrate_with_hold(db, family, id, plain, None).await
}

pub(crate) async fn hydrate_with_hold(
    db: &pioneer_sqlite::SqliteDatabase,
    family: Family,
    id: Option<&str>,
    plain: String,
    hold: Option<&PayloadReadHold>,
) -> Result<String> {
    observe::scope_result(
        family.observed(),
        observe::Mode::Full,
        hydrate_with_hold_inner(db, family, id, plain, hold),
    )
    .await
}

async fn hydrate_with_hold_inner(
    db: &pioneer_sqlite::SqliteDatabase,
    family: Family,
    id: Option<&str>,
    plain: String,
    hold: Option<&PayloadReadHold>,
) -> Result<String> {
    let Some(id) = id else {
        ensure!(hold.is_none(), "portable plain value has a payload hold");
        observe::bytes(observe::ByteKind::ReadOriginal, plain.len() as u64);
        return Ok(plain);
    };
    let meta = match hold {
        Some(hold) => header_with_hold(db, id, family, hold).await?,
        None => header(db, id, family).await?,
    };
    hydrate_header(db, &meta, plain).await
}

pub(crate) async fn hydrate_header(
    db: &pioneer_sqlite::SqliteDatabase,
    meta: &PayloadHeader,
    plain: String,
) -> Result<String> {
    observe::scope_result(
        observed_family(&meta.family),
        observe::Mode::Full,
        hydrate_header_inner(db, meta, plain),
    )
    .await
}

async fn hydrate_header_inner(
    db: &pioneer_sqlite::SqliteDatabase,
    meta: &PayloadHeader,
    plain: String,
) -> Result<String> {
    if meta.raw == 1 {
        let _verify = observe::stage(observe::Stage::VerifyAssemble);
        observe::bytes(observe::ByteKind::ReadOriginal, plain.len() as u64);
        ensure!(
            plain.len() as i64 == meta.original_bytes
                && plain.chars().count() as i64 == meta.original_chars
                && Sha256::digest(plain.as_bytes()).as_slice() == meta.sha256.as_slice(),
            "portable raw payload mismatch"
        );
        observe::outcome(observe::Outcome::Raw);
        return Ok(plain);
    }
    let size = usize::try_from(meta.original_bytes)?;
    let dict = load_read_dictionary(db, meta).await?;
    let mut bytes = Vec::new();
    let mut next = 0_i64;
    let mut previous: Option<BlockPosition> = None;
    loop {
        // Composite primary-key seek, including a final EOF query to reject
        // extra rows. Every reader is released before validation/decode.
        let blocks = {
            let _read = observe::stage(observe::Stage::BlockRead);
            let mut query = block::Entity::find().filter(block::Column::PayloadId.eq(&meta.id));
            if next > 0 {
                query = query.filter(block::Column::Ordinal.gte(next));
            }
            query
                .order_by_asc(block::Column::Ordinal)
                .limit(16)
                .all(db)
                .await?
        };
        if blocks.is_empty() {
            break;
        }
        for b in blocks {
            ensure!(
                b.payload_id == meta.id && b.ordinal == next,
                "portable payload block missing or identity mismatch"
            );
            let current = BlockPosition::from(&b);
            current.validate(meta)?;
            if let Some(previous) = &previous {
                current.follows(previous)?;
            }
            observe::bytes(observe::ByteKind::ReadEncoded, b.data.len() as u64);
            observe::blocks(observe::BlockKind::Read, 1);
            let decoded = codec_observed(|d| codec::decode_observed(&b.data, Some(&dict), d))?;
            observe::bytes(observe::ByteKind::ReadOriginal, decoded.len() as u64);
            let _verify = observe::stage(observe::Stage::VerifyAssemble);
            ensure!(
                decoded.len() as i64 == b.original_bytes,
                "portable block length mismatch"
            );
            ensure!(
                std::str::from_utf8(&decoded)?.chars().count() as i64 == b.original_chars,
                "portable block character count mismatch"
            );
            bytes.extend(decoded);
            next = next
                .checked_add(1)
                .context("portable block ordinal overflow")?;
            previous = Some(current);
        }
    }
    let _verify = observe::stage(observe::Stage::VerifyAssemble);
    ensure!(
        bytes.len() == size && Sha256::digest(&bytes).as_slice() == meta.sha256.as_slice(),
        "portable payload original digest mismatch"
    );
    let text = String::from_utf8(bytes)?;
    ensure!(
        text.chars().count() as i64 == meta.original_chars,
        "portable payload character count mismatch"
    );
    Ok(text)
}
/// Only metadata of the origin and the immediate predecessor is inspected.
/// At most three overlapping blocks are loaded; their bytes are decoded after
/// the queries release database capacity, with the header's logical hold alive.
#[derive(Clone, sea_orm::FromQueryResult)]
struct BlockPosition {
    ordinal: i64,
    byte_offset: i64,
    char_offset: i64,
    original_bytes: i64,
    original_chars: i64,
}
impl From<&block::Model> for BlockPosition {
    fn from(b: &block::Model) -> Self {
        Self {
            ordinal: b.ordinal,
            byte_offset: b.byte_offset,
            char_offset: b.char_offset,
            original_bytes: b.original_bytes,
            original_chars: b.original_chars,
        }
    }
}
impl BlockPosition {
    fn validate(&self, meta: &PayloadHeader) -> Result<()> {
        let b = self;
        // checked arithmetic rejects even adversarial i64 metadata before any
        // slice/index calculation. Full nonfinal UTF-8 blocks lose at most 3B.
        let byte_end = b
            .byte_offset
            .checked_add(b.original_bytes)
            .context("portable block byte position overflow")?;
        let char_end = b
            .char_offset
            .checked_add(b.original_chars)
            .context("portable block character position overflow")?;
        let min_byte_offset = b
            .ordinal
            .checked_mul((BLOCK - 3) as i64)
            .context("portable block ordinal overflow")?;
        let max_byte_offset = b
            .ordinal
            .checked_mul(BLOCK as i64)
            .context("portable block ordinal overflow")?;
        ensure!(
            b.ordinal >= 0
                && b.byte_offset >= 0
                && b.char_offset >= 0
                && (1..=BLOCK as i64).contains(&b.original_bytes)
                && (1..=b.original_bytes).contains(&b.original_chars)
                && b.original_chars >= (b.original_bytes + 3) / 4
                && b.char_offset <= b.byte_offset
                && b.char_offset >= b.byte_offset / 4 + i64::from(b.byte_offset % 4 != 0)
                && (min_byte_offset..=max_byte_offset).contains(&b.byte_offset)
                && byte_end <= meta.original_bytes
                && char_end <= meta.original_chars
                && (byte_end == meta.original_bytes) == (char_end == meta.original_chars)
                && (byte_end == meta.original_bytes || b.original_bytes >= (BLOCK - 3) as i64),
            "portable block metadata invalid"
        );
        if b.ordinal == 0 {
            ensure!(
                b.byte_offset == 0 && b.char_offset == 0,
                "portable block origin invalid"
            );
        }
        Ok(())
    }
    fn follows(&self, previous: &Self) -> Result<()> {
        ensure!(
            previous.ordinal.checked_add(1) == Some(self.ordinal)
                && previous.byte_offset.checked_add(previous.original_bytes)
                    == Some(self.byte_offset)
                && previous.char_offset.checked_add(previous.original_chars)
                    == Some(self.char_offset),
            "portable block continuity invalid"
        );
        Ok(())
    }
}
async fn block_position<C: ConnectionTrait>(
    db: &C,
    id: &str,
    ordinal: i64,
) -> Result<BlockPosition> {
    block::Entity::find_by_id((id.to_owned(), ordinal))
        .select_only()
        .columns([
            block::Column::Ordinal,
            block::Column::ByteOffset,
            block::Column::CharOffset,
            block::Column::OriginalBytes,
            block::Column::OriginalChars,
        ])
        .into_model::<BlockPosition>()
        .one(db)
        .await?
        .context("portable block position missing")
}
pub(crate) async fn read_range(
    db: &pioneer_sqlite::SqliteDatabase,
    meta: &PayloadHeader,
    start: i64,
    count: usize,
    characters: bool,
) -> Result<Vec<u8>> {
    observe::scope_result(
        observed_family(&meta.family),
        observe::Mode::Range,
        read_range_inner(db, meta, start, count, characters),
    )
    .await
}

async fn read_range_inner(
    db: &pioneer_sqlite::SqliteDatabase,
    meta: &PayloadHeader,
    start: i64,
    count: usize,
    characters: bool,
) -> Result<Vec<u8>> {
    ensure!(
        meta.raw == 0 && start >= 0 && count <= if characters { 16384 } else { BLOCK },
        "portable range budget invalid"
    );
    let total = if characters {
        meta.original_chars
    } else {
        meta.original_bytes
    };
    ensure!(start <= total, "portable range starts past end");
    let count = std::cmp::min(count as i64, total - start);
    if count == 0 {
        return Ok(Vec::new());
    }
    let end = start + count; // count <= total - start, hence no overflow.
    let column = if characters {
        block::Column::CharOffset
    } else {
        block::Column::ByteOffset
    };
    let read_phase = observe::stage(observe::Stage::BlockRead);
    let anchor = block::Entity::find()
        .select_only()
        .columns([
            block::Column::Ordinal,
            block::Column::ByteOffset,
            block::Column::CharOffset,
            block::Column::OriginalBytes,
            block::Column::OriginalChars,
        ])
        .filter(block::Column::PayloadId.eq(&meta.id))
        .filter(column.lte(start))
        .order_by_desc(column)
        .limit(1)
        .into_model::<BlockPosition>()
        .one(db)
        .await?
        .context("portable range block missing")?;
    anchor.validate(meta)?;
    let origin = if anchor.ordinal == 0 {
        anchor.clone()
    } else {
        block_position(db, &meta.id, 0).await?
    };
    origin.validate(meta)?;
    if anchor.ordinal > 0 {
        let previous = if anchor.ordinal == 1 {
            origin.clone()
        } else {
            block_position(db, &meta.id, anchor.ordinal - 1).await?
        };
        previous.validate(meta)?;
        if previous.ordinal == 1 {
            previous.follows(&origin)?;
        }
        anchor.follows(&previous)?;
    }
    let anchor_offset = if characters {
        anchor.char_offset
    } else {
        anchor.byte_offset
    };
    let anchor_length = if characters {
        anchor.original_chars
    } else {
        anchor.original_bytes
    };
    ensure!(
        anchor_offset <= start && start < anchor_offset + anchor_length,
        "portable range anchor does not contain start"
    );
    // Use composite-PK point reads. A LIMIT on an offset-filtered ordinal
    // range can scan the entire tail when fewer than three blocks overlap.
    let mut blocks = Vec::new();
    let mut ordinal = anchor.ordinal;
    for _ in 0..3 {
        let b = block::Entity::find_by_id((meta.id.clone(), ordinal))
            .one(db)
            .await?
            .context("portable range block missing")?;
        observe::bytes(observe::ByteKind::ReadEncoded, b.data.len() as u64);
        observe::blocks(observe::BlockKind::Read, 1);
        let offset = if characters {
            b.char_offset
        } else {
            b.byte_offset
        };
        let length = if characters {
            b.original_chars
        } else {
            b.original_bytes
        };
        let block_end = offset
            .checked_add(length)
            .context("portable range block overflow")?;
        blocks.push(b);
        if block_end >= end {
            break;
        }
        ordinal = ordinal
            .checked_add(1)
            .context("portable range ordinal overflow")?;
    }
    drop(read_phase);
    let dict = load_read_dictionary(db, meta).await?;
    let mut output = Vec::new();
    let mut position = start;
    let mut previous = None;
    for b in blocks {
        let current = BlockPosition::from(&b);
        current.validate(meta)?;
        if let Some(previous) = &previous {
            current.follows(previous)?;
        } else {
            ensure!(
                current.ordinal == anchor.ordinal
                    && current.byte_offset == anchor.byte_offset
                    && current.char_offset == anchor.char_offset
                    && current.original_bytes == anchor.original_bytes
                    && current.original_chars == anchor.original_chars,
                "portable range anchor changed"
            );
        }
        let offset = if characters {
            b.char_offset
        } else {
            b.byte_offset
        };
        let length = if characters {
            b.original_chars
        } else {
            b.original_bytes
        };
        ensure!(
            offset <= position && position < offset + length,
            "portable range block does not contain position"
        );
        let decoded = codec_observed(|d| codec::decode_observed(&b.data, Some(&dict), d))?;
        observe::bytes(observe::ByteKind::ReadOriginal, decoded.len() as u64);
        let _verify = observe::stage(observe::Stage::VerifyAssemble);
        ensure!(
            decoded.len() as i64 == b.original_bytes,
            "portable block length mismatch"
        );
        let text = std::str::from_utf8(&decoded)?;
        ensure!(
            text.chars().count() as i64 == b.original_chars,
            "portable block character count mismatch"
        );
        let local = usize::try_from(position - offset)?;
        let take = usize::try_from(std::cmp::min(end - position, length - local as i64))?;
        if characters {
            output.extend(
                text.chars()
                    .skip(local)
                    .take(take)
                    .collect::<String>()
                    .into_bytes(),
            );
        } else {
            output.extend_from_slice(&decoded[local..local + take]);
        }
        position += take as i64;
        if position == end {
            break;
        }
        previous = Some(current);
    }
    ensure!(position == end, "portable range incomplete");
    Ok(output)
}

async fn load_read_dictionary(
    db: &pioneer_sqlite::SqliteDatabase,
    meta: &PayloadHeader,
) -> Result<codec::Dictionary> {
    let dictionary_id = meta
        .dictionary_id
        .as_deref()
        .context("portable payload dictionary reference missing")?;
    let dictionary_phase = observe::stage(observe::Stage::DictionaryLoad);
    let dict = dictionary::Entity::find_by_id(dictionary_id)
        .one(db)
        .await?
        .context("portable payload dictionary missing")?;
    drop(dictionary_phase);
    ensure!(
        dict.family == meta.family,
        "portable dictionary family mismatch"
    );
    let dict = codec_observed(|d| codec::Dictionary::from_bytes_observed(dict.data, d))?;
    ensure!(
        dictionary_key(&meta.family, dict.digest()) == dictionary_id,
        "portable dictionary digest mismatch"
    );
    Ok(dict)
}

fn dictionary_key(family: &str, digest: [u8; 32]) -> String {
    format!("{family}:1:{}", hex::encode(digest))
}

#[derive(Debug)]
pub(crate) struct PreflightRace;
impl std::fmt::Display for PreflightRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("portable payload changed after preflight")
    }
}
impl std::error::Error for PreflightRace {}

/// A caller-local claim survives an awaited rollback/cancellation. Declare
/// before resource-owning locals/futures so its Drop follows their release.
#[derive(Default)]
pub(crate) struct DeferredHoldRejection(Option<observe::HoldRejectionRecord>);
impl DeferredHoldRejection {
    fn claim(error: &anyhow::Error) -> Self {
        Self(
            error
                .downcast_ref::<observe::HoldRejection>()
                .and_then(observe::HoldRejection::take),
        )
    }
    pub(crate) fn capture<T>(&mut self, result: &Result<T>) {
        if self.0.is_none() {
            if let Err(error) = result {
                *self = Self::claim(error);
            }
        }
    }
}
impl Drop for DeferredHoldRejection {
    fn drop(&mut self) {
        if let Some(record) = self.0.take() {
            record.record();
        }
    }
}
/// Existing error arms intentionally ignore rollback failure and return the
/// original error. Keep that policy and the refusal through a cancelled await:
/// rollback owns the transaction until it finishes/drops, then this guard records.
pub(crate) async fn rollback_with_hold_refusal(
    transaction: pioneer_sqlite::SqliteTransaction,
    error: anyhow::Error,
) -> anyhow::Error {
    let refusal = DeferredHoldRejection::claim(&error);
    let _ = transaction.rollback().await;
    drop(refusal);
    error
}

/// Existing CRUD retry attempts own their DB futures. Destroy the complete
/// operation before recording; do this inside the attempt's WorkCarrier so
/// its original work token is still active. Includes the final failed attempt.
pub(crate) async fn after_database_owners<T>(
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let mut future = Box::pin(future);
    let result = future.as_mut().await;
    drop(future);
    let mut refusal = DeferredHoldRejection::default();
    refusal.capture(&result);
    drop(refusal);
    result
}

// Availability refusals are expected; malformed persisted header counters are
// not. Match frozen_lifetime::header_bounds, including expired backing headers.
fn invalid_root_sql(family: Family) -> String {
    if !matches!(family, Family::CliMapping | Family::TaskHistory) {
        return "0".into();
    }
    let (table, _, _) = family.spec();
    format!(
        "EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id={table}.frozen_manifest_id AND NOT (h.expired IN (0,1) AND h.ready IN (0,1) AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal BETWEEN 0 AND h.message_count AND h.next_import BETWEEN 0 AND h.import_count AND (h.ready=0 OR (h.next_ordinal=h.message_count AND h.next_import=h.import_count))))"
    )
}

// Optional archival work is tied to the concrete execution, never to whether
// a reusable CLI process or recurring Task happens to remain alive. These
// indexed relational guards are shared by discovery, source reads and publish.
fn terminal_admission_sql(family: Family) -> &'static str {
    match family {
        Family::ToolText | Family::ToolMetadata => {
            "EXISTS(SELECT 1 FROM turn t WHERE t.id=tool_output_chunk.turn_id AND t.status IN ('completed','failed','interrupted','blocked'))"
        }
        Family::CliMapping => {
            "EXISTS(SELECT 1 FROM turn t WHERE t.id=turn_cli_runtime_binding.turn_id AND t.status IN ('completed','failed','interrupted','blocked'))"
        }
        Family::TaskHistory => {
            "EXISTS(SELECT 1 FROM task_run r WHERE r.id=task_run_conversation_snapshot.run_id AND r.task_id=task_run_conversation_snapshot.task_id AND r.status IN ('succeeded','failed','blocked','cancelled','timed_out')) AND (source_turn_id IS NULL OR EXISTS(SELECT 1 FROM turn t WHERE t.id=task_run_conversation_snapshot.source_turn_id AND t.status IN ('completed','failed','interrupted','blocked')))"
        }
        Family::TaskEvent => {
            "(turn_id IS NULL OR EXISTS(SELECT 1 FROM turn et WHERE et.id=task_event.turn_id AND et.status IN ('completed','failed','interrupted','blocked'))) AND ((run_id IS NOT NULL AND EXISTS(SELECT 1 FROM task_run r WHERE r.id=task_event.run_id AND r.task_id=task_event.task_id AND r.status IN ('succeeded','failed','blocked','cancelled','timed_out'))) OR (run_id IS NULL AND NOT EXISTS(SELECT 1 FROM task_run r WHERE r.task_id=task_event.task_id AND r.status NOT IN ('succeeded','failed','blocked','cancelled','timed_out')) AND NOT EXISTS(SELECT 1 FROM task owner JOIN turn creator ON creator.id=owner.created_by_turn_id WHERE owner.id=task_event.task_id AND creator.status NOT IN ('completed','failed','interrupted','blocked'))))"
        }
    }
}

// Correlated ownership/root predicates are shared by bounded discovery and
// atomic publication. SeaORM has no typed relation for all these heterogeneous
// owners; keep the existing correlated SQL expressions on the Entity path.
fn eligibility_sql(family: Family) -> String {
    let (table, _, _) = family.spec();
    let owner_guard = match family {
        Family::TaskHistory => {
            " AND EXISTS(SELECT 1 FROM task t JOIN task_run r ON r.task_id=t.id WHERE r.id=task_run_conversation_snapshot.run_id AND t.id=task_run_conversation_snapshot.task_id AND t.workspace_id=task_run_conversation_snapshot.workspace_id)"
        }
        Family::ToolText | Family::ToolMetadata => {
            " AND EXISTS(SELECT 1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=tool_output_chunk.turn_id)"
        }
        Family::CliMapping => {
            " AND EXISTS(SELECT 1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=turn_cli_runtime_binding.turn_id AND th.workspace_id=turn_cli_runtime_binding.workspace_id AND t.thread_id=turn_cli_runtime_binding.thread_id)"
        }
        Family::TaskEvent => " AND EXISTS(SELECT 1 FROM task t WHERE t.id=task_event.task_id)",
    };
    let root_guard = if family == Family::TaskHistory {
        // Publication can race a logical replacement of inline history with a
        // frozen descriptor. Revalidate admission as well as the existing CAS.
        " AND frozen_manifest_id IS NULL".into()
    } else if family == Family::CliMapping {
        format!(
            " AND (frozen_manifest_id IS NULL OR (frozen_manifest_id<>'' AND EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id={table}.frozen_manifest_id AND h.workspace_id={table}.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)))"
        )
    } else {
        String::new()
    };
    format!(
        "({}){} AND ({})",
        owner_guard.trim_start_matches(" AND "),
        root_guard,
        terminal_admission_sql(family)
    )
}

async fn publish(
    store: &CrudStore,
    family: Family,
    owner: &str,
    revision: i64,
    identity: &str,
    owner_stamp: &str,
    prepared: &Prepared,
) -> Result<()> {
    let _phase = observe::stage(observe::Stage::Publish);
    let tx = store.connection.begin().await?;
    // Source revision triggers cover logical payload and ownership updates.
    // Unknown/expired roots cannot pass the existing verified root trigger.
    // Revalidate Task/run/workspace at publication, not from preparation.
    let affected = target!(family, |Target, Col, key, field, reference| {
        let mut update = Target::update_many()
            .col_expr(reference, Expr::value(prepared.header.id.clone()))
            .filter(key.eq(owner))
            .filter(Col::PortableRevision.eq(revision))
            .filter(Col::PortableIdentity.eq(identity))
            .filter(Expr::cust(owner_stamp_sql(family)).eq(owner_stamp))
            .filter(reference.is_null())
            .filter(Expr::cust(eligibility_sql(family)));
        if let Some(projection) = &prepared.projection {
            update = update.col_expr(field, Expr::value(projection.clone()));
        }
        update.exec(&tx).await?.rows_affected
    });
    if affected == 1 {
        let published = value::Entity::update_many()
            .col_expr(value::Column::Retired, Expr::value(0))
            .filter(value::Column::Id.eq(&prepared.header.id))
            .filter(value::Column::Retired.eq(1))
            .exec(&tx)
            .await?;
        ensure!(
            published.rows_affected == 1,
            "portable staged header disappeared before publication"
        );
    }
    ensure!(
        affected <= 1,
        "portable publication affected invalid row count"
    );
    if affected == 0 && matches!(family, Family::CliMapping | Family::TaskHistory) {
        let invalid = target!(family, |Target, Col, key, _field, _reference| {
            Target::find()
                .select_only()
                .expr(Expr::cust(invalid_root_sql(family)))
                .filter(key.eq(owner))
                .into_tuple::<bool>()
                .one(&tx)
                .await?
        });
        ensure!(invalid != Some(true), "invalid frozen history counters");
    }
    tx.commit().await?;
    // A CAS miss is expected when source/owner/root changes during preparation.
    // Leave the staged header retired; dropping Preparation releases its hold,
    // and ordinary bounded cleanup reclaims it. No source completion is stored.
    if affected == 0 {
        observe::outcome(observe::Outcome::Stale);
        return Ok(());
    }
    let raw = prepared.header.raw == 1;
    let original = prepared.header.original_bytes as u64;
    let encoded = if raw {
        original
    } else {
        // Existing benefit selection includes a 48-byte row estimate per block.
        // Exclude that estimate: this counter measures the actual PPAY bytes.
        prepared
            .encoded_bytes
            .saturating_sub(prepared.next as usize * 48) as u64
    };
    observe::bytes(observe::ByteKind::PublishedOriginal, original);
    observe::bytes(observe::ByteKind::PublishedEncoded, encoded);
    observe::blocks(
        observe::BlockKind::Published,
        if raw { 0 } else { prepared.next as u64 },
    );
    observe::outcome(if raw {
        observe::Outcome::Raw
    } else {
        observe::Outcome::Published
    });
    Ok(())
}
async fn dictionary_row(store: &CrudStore, family: Family) -> Result<Option<dictionary::Model>> {
    let _phase = observe::stage(observe::Stage::DictionaryLoad);
    store
        .run_background_database_quantum(|| async {
            Ok(dictionary::Entity::find()
                .filter(dictionary::Column::Family.eq(family.name()))
                .filter(dictionary::Column::Version.eq(1))
                .one(&store.connection)
                .await?)
        })
        .await
}

async fn clear_blocks(store: &CrudStore, id: &str) -> Result<bool> {
    let ordinals = block::Entity::find()
        .select_only()
        .column(block::Column::Ordinal)
        .filter(block::Column::PayloadId.eq(id))
        .order_by_asc(block::Column::Ordinal)
        .limit(16)
        .into_tuple::<i64>()
        .all(&store.connection)
        .await?;
    if ordinals.is_empty() {
        return Ok(false);
    }
    let deleted = block::Entity::delete_many()
        .filter(block::Column::PayloadId.eq(id))
        .filter(block::Column::Ordinal.is_in(ordinals))
        .exec(&store.connection)
        .await?;
    observe::blocks(observe::BlockKind::Cleaned, deleted.rows_affected);
    Ok(true)
}

// Fixed, small relational ownership facts. A Task completion/reparent, parent
// workspace/root owner or identity change, expiry or delete cannot publish
// preparation captured before it. Only fixed-size relational facts are read.
fn owner_stamp_sql(family: Family) -> &'static str {
    match family {
        Family::ToolText | Family::ToolMetadata => {
            "coalesce((SELECT json_array(th.workspace_id,t.thread_id,t.status) FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=tool_output_chunk.turn_id),'')"
        }
        Family::CliMapping => {
            "coalesce((SELECT json_array(th.workspace_id,t.thread_id,t.status,(SELECT json_array(h.workspace_id,h.owner_thread,h.identity_sha256,h.message_count,h.import_count,h.imports_sha256,h.expired,h.ready,h.next_ordinal,h.next_import) FROM compaction_frozen_history h WHERE h.id=turn_cli_runtime_binding.frozen_manifest_id)) FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=turn_cli_runtime_binding.turn_id),'')"
        }
        Family::TaskHistory => {
            "coalesce((SELECT json_array(t.workspace_id,t.owner_kind,t.owner_id,t.status,t.updated_at,(SELECT json_array(h.workspace_id,h.owner_thread,h.identity_sha256,h.message_count,h.import_count,h.imports_sha256,h.expired,h.ready,h.next_ordinal,h.next_import) FROM compaction_frozen_history h WHERE h.id=task_run_conversation_snapshot.frozen_manifest_id)) FROM task t WHERE t.id=task_run_conversation_snapshot.task_id),'')"
        }
        Family::TaskEvent => {
            "coalesce((SELECT json_array(t.workspace_id,t.owner_kind,t.owner_id,t.status,t.updated_at) FROM task t WHERE t.id=task_event.task_id),'')"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_observability::{live, work};

    static TELEMETRY_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[derive(Default)]
    struct ReleaseObserver {
        token: std::sync::atomic::AtomicU64,
        released: std::sync::atomic::AtomicUsize,
        eager: std::sync::atomic::AtomicBool,
    }
    impl pioneer_sqlite::SqliteWriteObserver for ReleaseObserver {
        fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
            use std::sync::atomic::Ordering::SeqCst;
            if matches!(event, pioneer_sqlite::SqliteWriteEvent::Released { .. }) {
                let token = self.token.load(SeqCst);
                if let Some(snapshot) = live::snapshot() {
                    self.eager.fetch_or(
                        snapshot.active.iter().any(|w| {
                            w.context.token == token && w.portable_hold_rejection.is_some()
                        }),
                        SeqCst,
                    );
                }
                self.released.fetch_add(1, SeqCst);
            }
        }
    }
    fn caller() -> work::WorkCarrier {
        work::WorkCarrier::independent(
            work::Boundary::Rpc,
            work::WorkKind::RpcRead,
            work::Trigger::Rpc,
            work::ExecutorRole::Gateway,
        )
    }
    fn refusal_receipts(token: u64) -> usize {
        live::snapshot().unwrap().journal.iter().filter(|r| matches!(
            &r.payload, live::JournalPayload::Work(w)
                if w.context.token == token && !matches!(w.state, live::WorkState::Terminal(_))
                    && w.portable_hold_rejection == Some(observe::Family::TaskEvent)
                    && w.portable_payload.is_none()
        )).count()
    }

    #[tokio::test]
    async fn initial_capture_rejection_records_only_after_transaction_release() {
        let _telemetry_lock = TELEMETRY_TEST_LOCK.lock().await;
        use std::sync::atomic::Ordering::SeqCst;
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                pioneer_observability::set_telemetry_enabled(self.0);
            }
        }
        let _reset = Reset(pioneer_observability::telemetry_enabled());
        pioneer_observability::set_telemetry_enabled(true);
        let observer = Arc::new(ReleaseObserver::default());
        let writer = pioneer_sqlite::SqliteWriteExecutor::with_observer(
            sea_orm::Database::connect("sqlite::memory:").await.unwrap(),
            observer.clone(),
        );
        let reader = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        reader
            .execute_unprepared("PRAGMA query_only=ON")
            .await
            .unwrap();
        let db = pioneer_sqlite::SqliteDatabase::from_executor(reader, writer).maintenance();
        let id = uuid::Uuid::new_v4().to_string();
        let cleanup = PayloadCleanupHold::try_acquire(&id).unwrap();
        // Exercise both implicit transaction Drop and the actual explicit rollback
        // boundary. The latter flush runs under a different caller context.
        for explicit_rollback in [false, true] {
            let owner = caller();
            let token = owner.context().unwrap().token;
            observer.token.store(token, SeqCst);
            let result: Result<()> = owner
                .scope_result(after_database_owners(async {
                    let transaction = db.begin().await?;
                    let error = PayloadReadHold::capture(&id, Family::TaskEvent).unwrap_err();
                    assert!(error.downcast_ref::<PreflightRace>().is_some());
                    assert_eq!(refusal_receipts(token), 0);
                    if explicit_rollback {
                        let other = caller();
                        let other_token = other.context().unwrap().token;
                        let error = other
                            .scope(async { rollback_with_hold_refusal(transaction, error).await })
                            .await;
                        assert_eq!(refusal_receipts(other_token), 0);
                        Err(error)
                    } else {
                        // No receipt Drop may run before the transaction's Drop.
                        Err(error)
                    }
                }))
                .await;
            let error = result.unwrap_err();
            assert!(error.downcast_ref::<PreflightRace>().is_some());
            assert!(
                error
                    .downcast_ref::<observe::HoldRejection>()
                    .unwrap()
                    .take()
                    .is_none()
            );
            assert_eq!(refusal_receipts(token), 1); // outer retry boundary did not double count
        }
        assert_eq!(observer.released.load(SeqCst), 2);
        assert!(!observer.eager.load(SeqCst));

        // Cancellation destroys DB owners before a previously claimed receipt;
        // the owner is still live when its cancellation detail is recorded.
        let owner = caller();
        let token = owner.context().unwrap().token;
        observer.token.store(token, SeqCst);
        let (ready, reached) = tokio::sync::oneshot::channel();
        let mut operation = Box::pin(owner.scope_result(after_database_owners(async {
            let mut deferred = DeferredHoldRejection::default();
            let _transaction = db.begin().await?;
            let result = PayloadReadHold::capture(&id, Family::TaskEvent);
            deferred.capture(&result);
            assert_eq!(refusal_receipts(token), 0);
            ready.send(()).unwrap();
            std::future::pending::<Result<()>>().await
        })));
        tokio::select! {
            _ = &mut operation => panic!("cancellation fixture completed"),
            _ = reached => {}
        }
        drop(operation);
        assert_eq!(refusal_receipts(token), 1);
        assert!(!observer.eager.load(SeqCst));
        assert_eq!(observer.released.load(SeqCst), 3);

        // Capture epoch is retained across a consent cycle, not reconstructed at flush.
        let owner = caller();
        let token = owner.context().unwrap().token;
        observer.token.store(token, SeqCst);
        let _: Result<()> = owner
            .scope_result(after_database_owners(async {
                let _transaction = db.begin().await?;
                let error = PayloadReadHold::capture(&id, Family::TaskEvent).unwrap_err();
                pioneer_observability::set_telemetry_enabled(false);
                pioneer_observability::set_telemetry_enabled(true);
                Err(error)
            }))
            .await;
        assert_eq!(refusal_receipts(token), 0);
        let fresh = caller();
        let fresh_token = fresh.context().unwrap().token;
        observer.token.store(fresh_token, SeqCst);
        let _: Result<()> = fresh
            .scope_result(after_database_owners(async {
                let _transaction = db.begin().await?;
                Err(PayloadReadHold::capture(&id, Family::TaskEvent).unwrap_err())
            }))
            .await;
        assert_eq!(refusal_receipts(fresh_token), 1);
        drop(cleanup);
        let _hold = PayloadReadHold::capture(&id, Family::TaskEvent).unwrap();
        assert_eq!(refusal_receipts(fresh_token), 1);
        assert!(db.reader_query_only_enabled().await.unwrap());
    }

    #[derive(Default)]
    struct ReadCount(std::sync::atomic::AtomicUsize);
    impl pioneer_sqlite::SqliteReadObserver for ReadCount {
        fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
            if matches!(
                event,
                pioneer_sqlite::SqliteReadEvent::OperationFinished { .. }
            ) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
    async fn reservation_fixture() -> (tempfile::TempDir, CrudStore) {
        reservation_fixture_observed(Arc::new(ReadCount::default())).await
    }
    async fn reservation_fixture_observed(
        observer: Arc<ReadCount>,
    ) -> (tempfile::TempDir, CrudStore) {
        use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
        use sea_orm::{ConnectOptions, Database};
        pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("portable.sqlite");
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
        options.max_connections(1).min_connections(1);
        let connection = Database::connect(options).await.unwrap();
        let writer = SqliteWriteExecutor::new(connection.clone());
        let maintenance = SqliteDatabase::from_executor(connection, writer.clone()).maintenance();
        maintenance
            .execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        maintenance
            .execute_unprepared("PRAGMA journal_mode=WAL")
            .await
            .unwrap();
        writer
            .run_migrations::<migration::Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
        options.max_connections(1).min_connections(1);
        let reader = Database::connect(options).await.unwrap();
        reader
            .execute_unprepared("PRAGMA query_only=ON")
            .await
            .unwrap();
        let store = CrudStore::new(SqliteDatabase::from_executor_with_read_observer(
            reader, writer, observer,
        ))
        .with_maintenance_access();
        for sql in [
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        ] {
            store.connection.execute_unprepared(sql).await.unwrap();
        }
        (dir, store)
    }

    #[tokio::test]
    async fn shared_payload_gate_release_wakes_each_blocked_database_cleanup() {
        use futures_util::FutureExt;
        use sea_orm::ActiveModelTrait;
        let (_first_dir, first) = reservation_fixture().await;
        let (_second_dir, second) = reservation_fixture().await;
        for writer_hold in [false, true] {
            let id = uuid::Uuid::new_v4().to_string();
            for store in [&first, &second] {
                value::ActiveModel {
                    id: Set(id.clone()),
                    family: Set(Family::TaskHistory.name().into()),
                    original_bytes: Set(0),
                    original_chars: Set(0),
                    sha256: Set(vec![0; 32]),
                    raw: Set(1),
                    retired: Set(1),
                    dictionary_id: Set(None),
                }
                .insert(&store.connection)
                .await
                .unwrap();
            }
            let first_db = first.database_connection();
            let second_db = second.database_connection();
            let mut first_wake = first_db.work_signal("maintenance.portable").subscribe();
            let mut second_wake = second_db.work_signal("maintenance.portable").subscribe();
            first_wake.begin_pass(&first_db).await.unwrap();
            second_wake.begin_pass(&second_db).await.unwrap();
            let read_hold =
                (!writer_hold).then(|| PayloadReadHold::capture(&id, Family::TaskHistory).unwrap());
            let cleanup_hold = writer_hold.then(|| PayloadCleanupHold::try_acquire(&id).unwrap());
            assert!(!cleanup(&first, None, None).await.unwrap().0);
            assert!(!cleanup(&second, None, None).await.unwrap().0);
            drop(read_hold);
            drop(cleanup_hold);
            assert!(first_wake.wait(None).now_or_never().is_some());
            assert!(second_wake.wait(None).now_or_never().is_some());
            for store in [&first, &second] {
                for _ in 0..4 {
                    cleanup(store, None, None).await.unwrap();
                }
                assert!(
                    value::Entity::find_by_id(&id)
                        .one(&store.connection)
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        }
    }

    async fn pending_preparation(progress: &mut PortablePayloadProgress, store: &CrudStore) {
        for _ in 0..30 {
            progress.quantum(store).await.unwrap();
            if progress.pending.is_some() {
                return;
            }
        }
        panic!("legacy source was not discovered");
    }
    #[tokio::test]
    async fn publication_refusals_preserve_source_and_cleanup_but_sql_errors_propagate() {
        for mutation in [
            "replace_with_frozen",
            "DELETE FROM task_run WHERE id='run'",
            "UPDATE task_run SET task_id='another-task' WHERE id='run'",
            "UPDATE task SET workspace_id='another-workspace',status='completed' WHERE id='task'",
            "DELETE FROM task WHERE id='task'",
            "UPDATE task SET owner_id='another-thread' WHERE id='task'",
            "UPDATE task_run_conversation_snapshot SET history_json='[]',frozen_manifest_id=NULL WHERE run_id='run'",
            "CREATE TRIGGER fixture_publish_fault BEFORE UPDATE OF history_json_payload_id ON task_run_conversation_snapshot BEGIN SELECT RAISE(ABORT,'fixture publication fault'); END",
        ] {
            let (_dir, store) = reservation_fixture().await;
            for sql in [
                "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('task','ws','thread','thread','thread','turn','agent','running','fixture','fixture')",
                "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run','task','run',1,1,'succeeded','agent')",
                "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES('root','ws','thread','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',0,0,0,0,'',1)",
            ] {
                store.connection.execute_unprepared(sql).await.unwrap();
            }
            let descriptor = serde_json::to_string(&pioneer_compaction::frozen::FrozenHistoryRef {
                format: 1,
                manifest_id: "root".into(),
                messages: 0,
                identity_sha256: "a".repeat(64),
            })
            .unwrap();
            let mut history =
                serde_json::json!([{ "role": "user", "content": "inline publication fixture" }])
                    .to_string();
            // Preserve exact JSON bytes while ensuring real staged blocks for
            // refusal/hold/cleanup coverage, including a final-publication race.
            history.push_str(&" ".repeat(200_000));
            let snapshot = crate::NewTaskRunConversationSnapshot {
                run_id: "run".into(),
                task_id: "task".into(),
                workspace_id: "ws".into(),
                conversation_thread_id: "thread".into(),
                source_turn_id: None,
                history_json: history.clone(),
                created_at: chrono::Utc::now().fixed_offset(),
            };
            let database = store.database_connection();
            let mut wake = database.work_signal("maintenance.portable").subscribe();
            wake.begin_pass(&database).await.unwrap();
            store
                .insert_task_run_conversation_snapshot_if_absent(snapshot.clone())
                .await
                .unwrap();
            assert!(futures_util::FutureExt::now_or_never(wake.wait(None)).is_some());
            wake.begin_pass(&database).await.unwrap();
            store
                .insert_task_run_conversation_snapshot_if_absent(snapshot)
                .await
                .unwrap();
            assert!(
                futures_util::FutureExt::now_or_never(wake.wait(None)).is_none(),
                "snapshot replay must stay quiet"
            );
            let mut progress = PortablePayloadProgress::default();
            pending_preparation(&mut progress, &store).await;
            // Stop after finalization and raw block cleanup, immediately before
            // atomic publication. No sleeps or race-dependent timing.
            let mut prepared = progress.pending.take().unwrap();
            for _ in 0..30 {
                let no_blocks = block::Entity::find()
                    .filter(block::Column::PayloadId.eq(&prepared.prepared.header.id))
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .is_none();
                if prepared.prepared.finalized && (prepared.prepared.header.raw != 1 || no_blocks) {
                    break;
                }
                assert!(!prepared.step(&store).await.unwrap());
            }
            assert!(prepared.prepared.finalized);
            assert_eq!(prepared.prepared.header.raw, 0);
            assert!(
                block::Entity::find()
                    .filter(block::Column::PayloadId.eq(&prepared.prepared.header.id))
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .is_some()
            );
            let id = prepared.prepared.header.id.clone();
            if mutation == "replace_with_frozen" {
                store.connection.execute_raw(sea_orm::Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "UPDATE task_run_conversation_snapshot SET history_json=?,frozen_manifest_id='root' WHERE run_id='run'",
                    [descriptor.clone().into()],
                )).await.unwrap();
            } else {
                store.connection.execute_unprepared(mutation).await.unwrap();
            }
            let expected_error = mutation.starts_with("CREATE TRIGGER");
            progress.pending = Some(prepared);
            let result = progress.quantum(&store).await;
            if expected_error {
                assert!(
                    result.is_err(),
                    "SQLite failure must reach worker error backoff"
                );
            } else {
                assert!(
                    result.unwrap(),
                    "stale preparation completes without error backoff"
                );
            }
            assert!(progress.pending.is_none());
            let source = pioneer_entity::task_run_conversation_snapshot::Entity::find_by_id("run")
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap();
            assert!(source.history_json_payload_id.is_none());
            assert_eq!(
                source.history_json,
                if mutation == "replace_with_frozen" {
                    &descriptor
                } else if mutation.contains("history_json='[]'") {
                    "[]"
                } else {
                    &history
                }
            );
            assert_eq!(
                value::Entity::find_by_id(&id)
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap()
                    .retired,
                1
            );
            // Rejected staging remains protected while a reader holds it, then
            // ordinary bounded cleanup can reclaim both header and blocks.
            let database = store.database_connection();
            let mut wake = database.work_signal("maintenance.portable").subscribe();
            wake.begin_pass(&database).await.unwrap();
            let unblocked = PayloadReadHold::capture(&id, Family::TaskHistory).unwrap();
            drop(unblocked);
            assert!(
                futures_util::FutureExt::now_or_never(wake.wait(None)).is_none(),
                "ordinary reader release must not reopen completed cleanup"
            );
            let hold = PayloadReadHold::capture(&id, Family::TaskHistory).unwrap();
            assert!(!cleanup(&store, None, None).await.unwrap().0);
            assert!(
                value::Entity::find_by_id(&id)
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .is_some()
            );
            drop(hold);
            // Also covers legacy/generic repository captures: only a blocked
            // cleanup installs a release obligation, avoiding read self-wakes.
            tokio::time::timeout(std::time::Duration::from_secs(1), wake.wait(None))
                .await
                .unwrap();
            if mutation.contains("history_json='[]'") {
                let mut idle = false;
                for _ in 0..100 {
                    if !progress.quantum(&store).await.unwrap() {
                        idle = true;
                        break;
                    }
                }
                assert!(idle, "source refusal must clean staging before worker EOF");
            } else {
                for _ in 0..10 {
                    cleanup(&store, None, None).await.unwrap();
                }
            }
            assert!(
                value::Entity::find_by_id(&id)
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                block::Entity::find()
                    .filter(block::Column::PayloadId.eq(&id))
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn frozen_task_descriptors_stay_out_of_codec_while_inline_and_old_payloads_work() {
        use pioneer_entity::task_run_conversation_snapshot as snapshot;
        use sea_orm::{PaginatorTrait, Statement};
        let reads = Arc::new(ReadCount::default());
        let (_dir, store) = reservation_fixture_observed(reads.clone()).await;
        for sql in [
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('task','ws','thread','thread','thread','turn','agent','running','fixture','fixture')",
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES('root','ws','thread','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',0,0,0,0,'',1)",
            "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<2047) INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) SELECT printf('z-%04d',i),'task',printf('z-%04d',i),1,i+1,'succeeded','agent' FROM n",
        ] {
            store.connection.execute_unprepared(sql).await.unwrap();
        }
        let descriptor = serde_json::to_string(&pioneer_compaction::frozen::FrozenHistoryRef {
            format: 1,
            manifest_id: "root".into(),
            messages: 0,
            identity_sha256: "a".repeat(64),
        })
        .unwrap();
        store.connection.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,frozen_manifest_id,created_at) SELECT id,'task','ws','thread',?,'root',CURRENT_TIMESTAMP FROM task_run",
            [descriptor.clone().into()],
        )).await.unwrap();

        // Persist legacy physical formats directly: the current worker must
        // continue reading them even though it no longer creates these values.
        let original = format!("{descriptor}{}", " ".repeat(4096));
        let dict = codec::Dictionary::from_bytes(original.as_bytes().to_vec()).unwrap();
        let dict_id = dictionary_key(Family::TaskHistory.name(), dict.digest());
        dictionary::Entity::insert(dictionary::ActiveModel {
            id: Set(dict_id.clone()),
            family: Set(Family::TaskHistory.name().into()),
            version: Set(1),
            data: Set(dict.bytes().to_vec()),
        })
        .exec(&store.connection)
        .await
        .unwrap();
        let encoded = codec::encode(original.as_bytes(), &dict).unwrap();
        assert!(codec::is_compressed(&encoded).unwrap());
        for (run, id, raw) in [
            ("z-0000", "legacyraw001", true),
            ("z-0001", "legacyzip001", false),
        ] {
            value::Entity::insert(value::ActiveModel {
                id: Set(id.into()),
                family: Set(Family::TaskHistory.name().into()),
                original_bytes: Set(original.len() as i64),
                original_chars: Set(original.chars().count() as i64),
                sha256: Set(Sha256::digest(original.as_bytes()).to_vec()),
                raw: Set(i32::from(raw)),
                retired: Set(0),
                dictionary_id: Set((!raw).then(|| dict_id.clone())),
            })
            .exec(&store.connection)
            .await
            .unwrap();
            if !raw {
                block::Entity::insert(block::ActiveModel {
                    payload_id: Set(id.into()),
                    ordinal: Set(0),
                    byte_offset: Set(0),
                    char_offset: Set(0),
                    original_bytes: Set(original.len() as i64),
                    original_chars: Set(original.chars().count() as i64),
                    data: Set(encoded.clone()),
                })
                .exec(&store.connection)
                .await
                .unwrap();
            }
            snapshot::Entity::update_many()
                .col_expr(
                    snapshot::Column::HistoryJson,
                    Expr::value(if raw { original.clone() } else { "{}".into() }),
                )
                .col_expr(snapshot::Column::HistoryJsonPayloadId, Expr::value(id))
                .filter(snapshot::Column::RunId.eq(run))
                .exec(&store.connection)
                .await
                .unwrap();
        }
        store
            .connection
            .execute_unprepared("UPDATE compaction_frozen_history SET expired=1")
            .await
            .unwrap();
        let before = reads.0.load(std::sync::atomic::Ordering::SeqCst);
        let mut progress = PortablePayloadProgress::default();
        let mut eof = false;
        for _ in 0..12 {
            if !progress.quantum(&store).await.unwrap() {
                eof = true;
                break;
            }
        }
        assert!(
            eof,
            "known descriptors must not form a pending compression tail"
        );
        assert!(
            reads.0.load(std::sync::atomic::Ordering::SeqCst) - before <= 6,
            "an excluded descriptor catalog must reach EOF without walking its rows"
        );
        assert_eq!(
            value::Entity::find()
                .count(&store.connection)
                .await
                .unwrap(),
            2
        );

        // An inline key beyond the frozen catalog forces pending pages to skip
        // the known descriptor prefix as well as applying the ceiling filter.
        store.connection.execute_unprepared("INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('zz-inline','task','zz-inline',1,2049,'succeeded','agent')").await.unwrap();
        let inline =
            serde_json::json!([{"role":"user", "content":"actual inline history ".repeat(1000)}])
                .to_string();
        store
            .insert_task_run_conversation_snapshot_if_absent(
                crate::NewTaskRunConversationSnapshot {
                    run_id: "zz-inline".into(),
                    task_id: "task".into(),
                    workspace_id: "ws".into(),
                    conversation_thread_id: "thread".into(),
                    source_turn_id: None,
                    history_json: inline.clone(),
                    created_at: chrono::Utc::now().fixed_offset(),
                },
            )
            .await
            .unwrap();
        let before_inline = reads.0.load(std::sync::atomic::Ordering::SeqCst);
        eof = false;
        for _ in 0..100 {
            if !progress.quantum(&store).await.unwrap() {
                eof = true;
                break;
            }
        }
        assert!(eof);
        assert!(
            reads.0.load(std::sync::atomic::Ordering::SeqCst) - before_inline < 90,
            "the inline page must not walk the frozen descriptor prefix"
        );
        let source = snapshot::Entity::find_by_id("zz-inline")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let payload = value::Entity::find_by_id(source.history_json_payload_id.unwrap())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            payload.raw, 0,
            "actual inline history must still be compressed"
        );
        assert_eq!(
            value::Entity::find()
                .count(&store.connection)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            store
                .get_task_run_conversation_snapshot("zz-inline")
                .await
                .unwrap()
                .unwrap()
                .history_json,
            inline
        );
        assert_eq!(
            store
                .get_task_run_conversation_snapshot("z-0000")
                .await
                .unwrap()
                .unwrap()
                .history_json,
            original
        );
        assert_eq!(
            store
                .get_task_run_conversation_snapshot("z-0001")
                .await
                .unwrap()
                .unwrap()
                .history_json,
            original
        );
        assert_eq!(
            store
                .get_task_run_conversation_snapshot("z-2047")
                .await
                .unwrap()
                .unwrap()
                .history_json,
            descriptor
        );
        assert!(
            snapshot::Entity::find_by_id("z-2047")
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .history_json_payload_id
                .is_none()
        );
    }

    #[tokio::test]
    async fn cli_discovery_skip_before_corruption_records_quantum_error() {
        use sea_orm::Statement;
        let _telemetry_lock = TELEMETRY_TEST_LOCK.lock().await;
        let (_dir, store) = reservation_fixture().await;
        store.connection.execute_unprepared(
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('task','ws','thread','thread','thread','turn','agent','running','fixture','fixture')"
        ).await.unwrap();
        for (number, id) in [(1_i64, "a-skip"), (2, "b-damaged")] {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES(?,'task',?,1,?,'succeeded','agent')", [id.into(), id.into(), number.into()]
            )).await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES(?,'ws','thread',?,0,0,0,0,'',1)", [id.into(), "a".repeat(64).into()]
            )).await.unwrap();
            let history = serde_json::to_string(&pioneer_compaction::frozen::FrozenHistoryRef {
                format: 1,
                manifest_id: id.into(),
                messages: 0,
                identity_sha256: "a".repeat(64),
            })
            .unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES(?,'thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                [id.into()],
            )).await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO turn_cli_runtime_binding(turn_id,thread_id,continuation_thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,status,input_mapping_json,frozen_manifest_id,created_at,updated_at) VALUES(?,'thread','thread','ws','codex','codex',?,'completed',?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                [id.into(), id.into(), serde_json::json!({"pioneerContextBasis":{"historyJson":history}}).to_string().into(), id.into()],
            )).await.unwrap();
        }
        store
            .connection
            .execute_unprepared("UPDATE compaction_frozen_history SET expired=1")
            .await
            .unwrap();
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                pioneer_observability::set_telemetry_enabled(self.0);
            }
        }
        let _reset = Reset(pioneer_observability::telemetry_enabled());
        pioneer_observability::set_telemetry_enabled(true);
        for corrupted in [false, true] {
            if corrupted {
                // A damaged, unsealed legacy backing header. Retain the admitted
                // source and the valid expired predecessor without weakening guards.
                store
                    .connection
                    .execute_unprepared(
                        "DELETE FROM compaction_frozen_history WHERE id='b-damaged'",
                    )
                    .await
                    .unwrap();
                store.connection.execute_unprepared(
                    "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,next_import,imports_sha256,ready) VALUES('b-damaged','ws','thread','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',0,-1,0,0,'',0)"
                ).await.unwrap();
            }
            let mut progress = PortablePayloadProgress::default();
            let mut observed = false;
            // Find the CLI mapping quantum by its public observation, without
            // coupling the assertion to the order of families in the round.
            for _ in 0..10 {
                let owner = caller();
                let token = owner.context().unwrap().token;
                let result = owner.scope(progress.quantum(&store)).await;
                let terminals: Vec<_> = live::snapshot()
                    .unwrap()
                    .journal
                    .into_iter()
                    .filter_map(|r| match r.payload {
                        live::JournalPayload::Work(w)
                            if w.context.parent == Some(token)
                                && matches!(w.state, live::WorkState::Terminal(_))
                                && w.portable_payload.is_some() =>
                        {
                            Some(w)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(terminals.len(), 1);
                let operation = terminals[0].portable_payload.unwrap();
                if operation.family != observe::Family::CliMapping {
                    assert!(result.unwrap());
                    continue;
                }
                if corrupted {
                    assert_eq!(
                        result.unwrap_err().to_string(),
                        "invalid frozen history counters"
                    );
                } else {
                    assert!(result.unwrap());
                }
                assert_eq!(operation.family, observe::Family::CliMapping);
                assert_eq!(operation.mode, observe::Mode::Background);
                assert_eq!(
                    operation.outcome,
                    Some(if corrupted {
                        observe::Outcome::Error
                    } else {
                        observe::Outcome::NoWork
                    })
                );
                assert_eq!(
                    terminals[0].state,
                    live::WorkState::Terminal(if corrupted {
                        work::Outcome::Failed
                    } else {
                        work::Outcome::Completed
                    })
                );
                observed = true;
                break;
            }
            assert!(observed, "CLI mapping discovery must receive service");
        }
    }

    #[tokio::test]
    async fn refusal_pages_continue_without_events_and_revisit_newly_eligible_old_keys() {
        use sea_orm::Statement;
        use std::sync::atomic::Ordering::SeqCst;
        let reads = Arc::new(ReadCount::default());
        let (_dir, store) = reservation_fixture_observed(reads.clone()).await;
        // Legacy rows with missing owner/run are ordinary availability refusals.
        for i in 0..130 {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) VALUES(?,'missing','ws','thread','[]',CURRENT_TIMESTAMP,NULL)",
                [format!("run-{i:03}").into()])).await.unwrap();
        }
        let mut progress = PortablePayloadProgress::default();
        let before = reads.0.load(SeqCst);
        let mut eof = false;
        for _ in 0..20 {
            if !progress.quantum(&store).await.unwrap() {
                eof = true;
                break;
            }
        }
        assert!(eof, "full refusal pages must continue to global EOF");
        // Three pages of at most 64 candidates: 7 + 7 + 2 queries, plus
        // one frontier for each family and cleanup. No per-row seeks.
        assert!(reads.0.load(SeqCst) - before <= 22);
        for sql in [
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('missing','ws','thread','thread','thread','turn','agent','running','fixture','fixture')",
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run-000','missing','run-000',1,1,'succeeded','agent')",
        ] {
            store.connection.execute_unprepared(sql).await.unwrap();
        }
        // Existing finite-pass reset revisits a key below the former cursor.
        let mut completed = false;
        for _ in 0..40 {
            if !progress.quantum(&store).await.unwrap() {
                completed = true;
                break;
            }
        }
        assert!(completed);
        let row = pioneer_entity::task_run_conversation_snapshot::Entity::find_by_id("run-000")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert!(row.history_json_payload_id.is_some());
    }

    #[tokio::test]
    async fn tiny_raw_threshold_preserves_values_without_codec_staging_and_cas_loss() {
        use pioneer_entity::tool_output_chunk as chunk;
        for text in [
            String::new(),
            "x".repeat(126),
            "я".repeat(63),
            "x".repeat(127),
        ] {
            for stale in [false, true] {
                let (_dir, store) = reservation_fixture().await;
                let output = pioneer_protocol::ItemDeltaNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item_id: "tiny".into(),
                    delta: "seed".into(),
                    stream: None,
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                };
                store.record_tool_output("tiny", &output).await.unwrap();
                chunk::Entity::update_many()
                    .col_expr(chunk::Column::Text, Expr::value(text.clone()))
                    .filter(chunk::Column::Id.eq("tiny"))
                    .exec(&store.connection)
                    .await
                    .unwrap();
                if text.len() <= 126 {
                    // These fail the real write path if codec work is staged.
                    store.connection.execute_unprepared("CREATE TRIGGER reject_dictionary BEFORE INSERT ON portable_payload_dictionary BEGIN SELECT RAISE(ABORT,'tiny dictionary'); END").await.unwrap();
                    store.connection.execute_unprepared("CREATE TRIGGER reject_block BEFORE INSERT ON portable_payload_block BEGIN SELECT RAISE(ABORT,'tiny block'); END").await.unwrap();
                }
                let mut progress = PortablePayloadProgress::default();
                pending_preparation(&mut progress, &store).await;
                let mut preparation = progress.pending.take().unwrap();
                for _ in 0..12 {
                    if preparation.prepared.finalized {
                        break;
                    }
                    assert!(!preparation.step(&store).await.unwrap());
                }
                assert!(preparation.prepared.finalized);
                if text.len() <= 126 {
                    assert_eq!(preparation.prepared.header.raw, 1);
                    assert!(preparation.dictionary.is_none());
                } else {
                    assert!(preparation.dictionary.is_some());
                }
                if stale {
                    chunk::Entity::update_many()
                        .col_expr(chunk::Column::Text, Expr::value("replacement"))
                        .filter(chunk::Column::Id.eq("tiny"))
                        .exec(&store.connection)
                        .await
                        .unwrap();
                }
                for _ in 0..4 {
                    if preparation.step(&store).await.unwrap() {
                        break;
                    }
                }
                let row = chunk::Entity::find_by_id("tiny")
                    .one(&store.connection)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(row.metadata.is_none() && row.metadata_payload_id.is_none());
                if stale {
                    assert!(row.text_payload_id.is_none());
                    assert_eq!(row.text, "replacement");
                } else {
                    let id = row
                        .text_payload_id
                        .as_deref()
                        .expect("terminal publication");
                    assert_eq!(
                        hydrate(
                            &store.connection,
                            Family::ToolText,
                            Some(id),
                            row.text.clone()
                        )
                        .await
                        .unwrap(),
                        text
                    );
                    let mut restarted = PortablePayloadProgress::default();
                    for _ in 0..30 {
                        if !restarted.quantum(&store).await.unwrap() {
                            break;
                        }
                    }
                    assert_eq!(
                        chunk::Entity::find_by_id("tiny")
                            .one(&store.connection)
                            .await
                            .unwrap()
                            .unwrap()
                            .text_payload_id,
                        row.text_payload_id
                    );
                }
            }
        }
    }

    fn collide_with(progress: &mut PortablePayloadProgress, id: &str) {
        let pending = progress.pending.as_mut().unwrap();
        assert!(!pending.prepared.installed);
        pending.prepared._read_hold.take();
        pending.prepared.header.id = id.to_owned();
        pending.prepared._read_hold = Some(PayloadReadHold::capture(id, pending.family).unwrap());
    }

    #[tokio::test]
    async fn reservation_collision_retries_own_blocks_and_abandons_unconfirmed_candidates() {
        let reads = Arc::new(ReadCount::default());
        let (_dir, store) = reservation_fixture_observed(reads.clone()).await;
        let output = pioneer_protocol::ItemDeltaNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item_id: "reservation".into(),
            delta: "collision bytes 🦀\n".repeat(4000),
            stream: None,
            payload: None,
            markdown: None,
            markdown_version: None,
        };
        store.record_tool_output("original", &output).await.unwrap();
        // Existing legacy TEXT can exceed today's delta limit. Keep the real
        // source/progress path and force >16 blocks for bounded reclamation.
        let legacy = output.delta.repeat(20);
        pioneer_entity::tool_output_chunk::Entity::update_many()
            .col_expr(
                pioneer_entity::tool_output_chunk::Column::Text,
                Expr::value(legacy.clone()),
            )
            .filter(pioneer_entity::tool_output_chunk::Column::Id.eq("original"))
            .exec(&store.connection)
            .await
            .unwrap();
        let mut progress = PortablePayloadProgress::default();
        pending_preparation(&mut progress, &store).await;
        let foreign_id = "AAAAAAAAAAAA";
        collide_with(&mut progress, foreign_id);
        // Matching family/size/placeholder digest must never imply ownership.
        let foreign = progress.pending.as_ref().unwrap().prepared.header.clone();
        let foreign_hold = PayloadReadHold::capture(foreign_id, Family::ToolText).unwrap();
        value::Entity::insert(foreign.clone().into_active_model())
            .exec(&store.connection)
            .await
            .unwrap();
        let foreign_block = block::Model {
            payload_id: foreign_id.into(),
            ordinal: 0,
            byte_offset: 0,
            char_offset: 0,
            original_bytes: 1,
            original_chars: 1,
            data: vec![42; 78],
        };
        block::Entity::insert(foreign_block.clone().into_active_model())
            .exec(&store.connection)
            .await
            .unwrap();
        progress.quantum(&store).await.unwrap(); // actual conflicting INSERT
        let pending = progress.pending.as_ref().unwrap();
        assert_eq!(pending.offset, 0);
        assert!(!pending.prepared.installed);
        assert!(pending.prepared.blocks.is_empty());
        let reserved = pending.prepared.header.id.clone();
        assert_ne!(reserved, foreign_id);
        assert_eq!(reserved.len(), 12);
        assert!(reserved.bytes().all(|b| b.is_ascii_alphanumeric()));
        progress.quantum(&store).await.unwrap();
        assert!(progress.pending.as_ref().unwrap().prepared.installed);
        // Retry the same successfully written block batch, as after an ambiguous
        // batch result. Confirmed reservation must retain the same payload ID.
        for _ in 0..40 {
            let p = progress.pending.as_ref().unwrap();
            if p.prepared.blocks.len() == 16 {
                break;
            }
            progress.quantum(&store).await.unwrap();
        }
        let batch = progress.pending.as_ref().unwrap().prepared.blocks.clone();
        assert!(!batch.is_empty());
        progress.quantum(&store).await.unwrap();
        progress.pending.as_mut().unwrap().prepared.blocks = batch.clone();
        progress.quantum(&store).await.unwrap();
        assert_eq!(
            progress.pending.as_ref().unwrap().prepared.header.id,
            reserved
        );
        let blocks = block::Entity::find()
            .filter(block::Column::PayloadId.eq(&reserved))
            .order_by_asc(block::Column::Ordinal)
            .all(&store.connection)
            .await
            .unwrap();
        assert_eq!(blocks, batch);
        for _ in 0..100 {
            progress.quantum(&store).await.unwrap();
        }
        let rows = store
            .tool_output_page("ws", "thread", "turn", "reservation", 0)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.text, legacy);
        assert_eq!(
            value::Entity::find_by_id(foreign_id)
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap(),
            foreign
        );
        assert_eq!(
            block::Entity::find_by_id((foreign_id.to_owned(), 0))
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap(),
            foreign_block
        );

        let captured = header(&store.connection, &reserved, Family::ToolText)
            .await
            .unwrap();
        assert_eq!(captured.sha256.len(), 32);
        value::Entity::update_many()
            .col_expr(value::Column::Sha256, Expr::value(vec![0_u8; 32]))
            .filter(value::Column::Id.eq(&reserved))
            .exec(&store.connection)
            .await
            .unwrap();
        let corrupt = header(&store.connection, &reserved, Family::ToolText)
            .await
            .unwrap();
        assert!(
            hydrate_header(&store.connection, &corrupt, String::new())
                .await
                .is_err()
        );
        drop(corrupt);
        value::Entity::update_many()
            .col_expr(value::Column::Sha256, Expr::value(captured.sha256.clone()))
            .filter(value::Column::Id.eq(&reserved))
            .exec(&store.connection)
            .await
            .unwrap();
        // The real large UTF-8 fixture crosses several full-reader batches.
        // Random ranges retain their independent byte/character offset path.
        for start in [BLOCK - 2, 16 * BLOCK - 2] {
            assert_eq!(
                read_range(&store.connection, &captured, start as i64, 31, false)
                    .await
                    .unwrap(),
                legacy.as_bytes()[start..start + 31]
            );
        }
        assert_eq!(
            read_range(&store.connection, &captured, 16000, 127, true)
                .await
                .unwrap(),
            legacy
                .chars()
                .skip(16000)
                .take(127)
                .collect::<String>()
                .into_bytes()
        );
        let boundary = block::Entity::find_by_id((reserved.clone(), 16))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        for fault in [0, 1, 2] {
            match fault {
                0 => {
                    block::Entity::delete_by_id((reserved.clone(), 16))
                        .exec(&store.connection)
                        .await
                        .unwrap();
                }
                1 => {
                    block::Entity::update_many()
                        .col_expr(
                            block::Column::CharOffset,
                            Expr::value(boundary.char_offset + 1),
                        )
                        .filter(block::Column::PayloadId.eq(&reserved))
                        .filter(block::Column::Ordinal.eq(16))
                        .exec(&store.connection)
                        .await
                        .unwrap();
                }
                _ => {
                    block::Entity::update_many()
                        .col_expr(block::Column::Data, Expr::value(vec![0_u8; 78]))
                        .filter(block::Column::PayloadId.eq(&reserved))
                        .filter(block::Column::Ordinal.eq(16))
                        .exec(&store.connection)
                        .await
                        .unwrap();
                }
            }
            assert!(
                hydrate_header(&store.connection, &captured, String::new())
                    .await
                    .is_err()
            );
            block::Entity::delete_by_id((reserved.clone(), 16))
                .exec(&store.connection)
                .await
                .unwrap();
            block::Entity::insert(boundary.clone().into_active_model())
                .exec(&store.connection)
                .await
                .unwrap();
        }
        pioneer_entity::tool_output_chunk::Entity::delete_many()
            .filter(pioneer_entity::tool_output_chunk::Column::Id.eq("original"))
            .exec(&store.connection)
            .await
            .unwrap();
        assert!(
            !store
                .run_background_database_quantum(|| cleanup(&store, None, None))
                .await
                .unwrap()
                .0
        );
        // Measure only this successful logical read, after fault restoration
        // and deletion/blocked cleanup. The UTF-8 value spans >16 blocks.
        // Two sequential batches + dictionary + EOF fit comfortably in eight
        // reads; the old per-range hydration needs dozens of reader operations.
        let before_full_read = reads.0.load(std::sync::atomic::Ordering::SeqCst);
        let hydrated = hydrate_header(&store.connection, &captured, String::new())
            .await
            .unwrap();
        let full_read_operations =
            reads.0.load(std::sync::atomic::Ordering::SeqCst) - before_full_read;
        assert_eq!(hydrated, legacy);
        assert!(
            full_read_operations <= 8,
            "full hydration exceeded its sequential-read budget: {full_read_operations}"
        );
        drop(captured);
        assert!(
            store
                .run_background_database_quantum(|| cleanup(&store, None, None))
                .await
                .unwrap()
                .0
        );
        let reclaiming = value::Entity::find_by_id(&reserved)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reclaiming.retired, 1);
        assert!(reclaiming.sha256.is_empty());
        assert!(
            !block::Entity::find()
                .filter(block::Column::PayloadId.eq(&reserved))
                .all(&store.connection)
                .await
                .unwrap()
                .is_empty()
        );
        let late_hold = PayloadReadHold::capture(&reserved, Family::ToolText).unwrap();
        let error = header_with_hold(&store.connection, &reserved, Family::ToolText, &late_hold)
            .await
            .err()
            .unwrap();
        assert!(error.downcast_ref::<PreflightRace>().is_some());
        drop(late_hold);
        for _ in 0..4 {
            store
                .run_background_database_quantum(|| cleanup(&store, None, None))
                .await
                .unwrap();
        }
        assert!(
            value::Entity::find_by_id(&reserved)
                .one(&store.connection)
                .await
                .unwrap()
                .is_none()
        );

        // Old UUID-shaped references remain readable; only newly allocated IDs
        // change. SHA-256("abc") is an independent published fixture.
        let old_id = "00000000-0000-4000-8000-000000000001";
        value::Entity::insert(
            value::Model {
                id: old_id.into(),
                family: Family::ToolText.name().into(),
                original_bytes: 3,
                original_chars: 3,
                sha256: hex::decode(
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                )
                .unwrap(),
                raw: 1,
                retired: 0,
                dictionary_id: None,
            }
            .into_active_model(),
        )
        .exec(&store.connection)
        .await
        .unwrap();
        assert_eq!(
            hydrate(
                &store.connection,
                Family::ToolText,
                Some(old_id),
                "abc".into()
            )
            .await
            .unwrap(),
            "abc"
        );
        value::Entity::delete_by_id(old_id)
            .exec(&store.connection)
            .await
            .unwrap();

        store
            .record_tool_output("exhausted", &output)
            .await
            .unwrap();
        pending_preparation(&mut progress, &store).await;
        collide_with(&mut progress, foreign_id);
        progress
            .pending
            .as_mut()
            .unwrap()
            .prepared
            .reservation_attempts = RESERVATION_ATTEMPTS - 1;
        assert!(progress.quantum(&store).await.is_err());
        assert!(progress.pending.is_none());
        let source = pioneer_entity::tool_output_chunk::Entity::find()
            .filter(pioneer_entity::tool_output_chunk::Column::Id.eq("exhausted"))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert!(source.text_payload_id.is_none());
        assert_eq!(source.text, output.delta);
        assert_eq!(
            value::Entity::find_by_id(foreign_id)
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap(),
            foreign
        );

        // Cancellation while reservation waits for the existing writer leaves
        // the source untouched and must not claim somebody else's header.
        let mut cancelled = PortablePayloadProgress::default();
        pending_preparation(&mut cancelled, &store).await;
        let candidate = cancelled
            .pending
            .as_ref()
            .unwrap()
            .prepared
            .header
            .id
            .clone();
        let transaction = store.connection.begin().await.unwrap();
        let mut future = Box::pin(cancelled.quantum(&store));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(future.as_mut(), &mut cx).is_pending());
        drop(future);
        transaction.rollback().await.unwrap();
        drop(cancelled);
        assert!(
            value::Entity::find_by_id(candidate)
                .one(&store.connection)
                .await
                .unwrap()
                .is_none()
        );
        let source = pioneer_entity::tool_output_chunk::Entity::find()
            .filter(pioneer_entity::tool_output_chunk::Column::Id.eq("exhausted"))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert!(source.text_payload_id.is_none());
        assert_eq!(source.text, output.delta);
        assert_eq!(
            block::Entity::find_by_id((foreign_id.to_owned(), 0))
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap(),
            foreign_block
        );
        drop(foreign_hold);
    }
    async fn finish_archive_pass(progress: &mut PortablePayloadProgress, store: &CrudStore) {
        for _ in 0..500 {
            if !progress.quantum(store).await.unwrap() {
                return;
            }
        }
        panic!("bounded archival pass did not reach EOF");
    }

    async fn archive_family_refs(store: &CrudStore) -> Vec<Option<String>> {
        use pioneer_entity::{
            task_event as event, task_run_conversation_snapshot as snapshot,
            tool_output_chunk as chunk, turn_cli_runtime_binding as mapping,
        };
        let tool = chunk::Entity::find()
            .filter(chunk::Column::Id.eq("active-output"))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let cli = mapping::Entity::find_by_id("turn")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let history = snapshot::Entity::find_by_id("active-run")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let events = event::Entity::find()
            .filter(event::Column::TaskId.eq("active-task"))
            .order_by_asc(event::Column::Sequence)
            .all(&store.connection)
            .await
            .unwrap();
        let mut refs = vec![
            tool.text_payload_id,
            tool.metadata_payload_id,
            cli.input_mapping_json_payload_id,
            history.history_json_payload_id,
        ];
        refs.extend(
            events
                .into_iter()
                .map(|event| event.payload_json_payload_id),
        );
        refs
    }

    #[tokio::test]
    async fn every_archive_family_waits_for_its_turn_and_run_then_revisits_old_keys() {
        use sea_orm::{PaginatorTrait, Statement};
        for (turn_status, run_status) in [
            ("completed", "succeeded"),
            ("failed", "failed"),
            ("interrupted", "cancelled"),
            ("blocked", "blocked"),
            ("failed", "timed_out"),
        ] {
            let (_dir, store) = reservation_fixture().await;
            store
                .connection
                .execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='turn'")
                .await
                .unwrap();
            store
                .record_tool_output(
                    "active-output",
                    &pioneer_protocol::ItemDeltaNotification {
                        workspace_id: "ws".into(),
                        thread_id: "thread".into(),
                        turn_id: "turn".into(),
                        item_id: "item".into(),
                        delta: "live output ".repeat(400),
                        stream: None,
                        payload: Some(serde_json::json!({"live":true})),
                        markdown: None,
                        markdown_version: None,
                    },
                )
                .await
                .unwrap();
            for sql in [
                "INSERT INTO turn_cli_runtime_binding(turn_id,thread_id,continuation_thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,status,input_mapping_json,frozen_manifest_id,created_at,updated_at) VALUES('turn','thread','thread','ws','codex','codex','native','running','{}',NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('active-task','ws','thread','thread','thread','turn','agent','running','fixture','fixture')",
                "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('active-run','active-task','active-run',1,1,'running','agent')",
                "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,frozen_manifest_id,created_at) VALUES('active-run','active-task','ws','thread','turn','[]',NULL,CURRENT_TIMESTAMP)",
                "INSERT INTO task_event(id,task_id,run_id,thread_id,turn_id,sequence,event_type,payload_json,created_at) VALUES('run-event','active-task','active-run','thread','turn',1,'fixture','{}',CURRENT_TIMESTAMP)",
                "INSERT INTO task_event(id,task_id,sequence,event_type,payload_json,created_at) VALUES('task-event','active-task',2,'fixture','{}',CURRENT_TIMESTAMP)",
            ] {
                store.connection.execute_unprepared(sql).await.unwrap();
            }
            let mut progress = PortablePayloadProgress::default();
            finish_archive_pass(&mut progress, &store).await;
            assert_eq!(archive_family_refs(&store).await, vec![None; 6]);
            assert_eq!(
                value::Entity::find()
                    .count(&store.connection)
                    .await
                    .unwrap(),
                0,
                "active work must not even stage an archival header"
            );
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "UPDATE turn SET status=? WHERE id='turn'",
                    [turn_status.into()],
                ))
                .await
                .unwrap();
            // Tool text/metadata and CLI mapping are now independently eligible;
            // the Run and task-level events still have an active execution.
            finish_archive_pass(&mut progress, &store).await;
            let refs = archive_family_refs(&store).await;
            assert!(refs[..3].iter().all(Option::is_some));
            assert!(refs[3..].iter().all(Option::is_none));
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "UPDATE task_run SET status=? WHERE id='active-run'",
                    [run_status.into()],
                ))
                .await
                .unwrap();
            // The reusable Task remains running, as for a recurring Task.
            finish_archive_pass(&mut progress, &store).await;
            assert!(
                archive_family_refs(&store)
                    .await
                    .iter()
                    .all(Option::is_some)
            );
        }
    }

    #[tokio::test]
    async fn resumed_turn_discards_preparation_before_reading_and_before_publish() {
        use pioneer_entity::tool_output_chunk as chunk;
        for finalized in [false, true] {
            let (_dir, store) = reservation_fixture().await;
            let text = "archive only after completion ".repeat(4000);
            store
                .record_tool_output(
                    "resume-output",
                    &pioneer_protocol::ItemDeltaNotification {
                        workspace_id: "ws".into(),
                        thread_id: "thread".into(),
                        turn_id: "turn".into(),
                        item_id: "item".into(),
                        delta: text.clone(),
                        stream: None,
                        payload: None,
                        markdown: None,
                        markdown_version: None,
                    },
                )
                .await
                .unwrap();
            let mut progress = PortablePayloadProgress::default();
            pending_preparation(&mut progress, &store).await;
            let mut pending = progress.pending.take().unwrap();
            if finalized {
                for _ in 0..50 {
                    if pending.prepared.finalized {
                        break;
                    }
                    assert!(!pending.step(&store).await.unwrap());
                }
                assert!(pending.prepared.finalized);
            }
            store
                .connection
                .execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='turn'")
                .await
                .unwrap();
            assert!(pending.step(&store).await.unwrap());
            drop(pending);
            let row = chunk::Entity::find()
                .filter(chunk::Column::Id.eq("resume-output"))
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap();
            assert!(row.text_payload_id.is_none());
            assert_eq!(row.text, text);
            store
                .connection
                .execute_unprepared("UPDATE turn SET status='interrupted' WHERE id='turn'")
                .await
                .unwrap();
            // The abandoned source is behind this pass's captured cursor.
            // Finish that finite pass before rediscovering the old key.
            finish_archive_pass(&mut progress, &store).await;
            finish_archive_pass(&mut progress, &store).await;
            let row = chunk::Entity::find()
                .filter(chunk::Column::Id.eq("resume-output"))
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap();
            assert!(row.text_payload_id.is_some());
        }
    }

    #[tokio::test]
    async fn active_output_is_quiet_and_terminal_commit_wakes_archive_without_new_payload() {
        use futures_util::FutureExt;
        let (_dir, store) = reservation_fixture().await;
        store
            .connection
            .execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='turn'")
            .await
            .unwrap();
        let database = store.database_connection();
        let mut wake = database.work_signal("maintenance.portable").subscribe();
        wake.begin_pass(&database).await.unwrap();
        store
            .record_tool_output(
                "wake-output",
                &pioneer_protocol::ItemDeltaNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item_id: "item".into(),
                    delta: "source remains readable".into(),
                    stream: None,
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            )
            .await
            .unwrap();
        assert!(
            wake.wait(None).now_or_never().is_none(),
            "active output must not wake the archive worker"
        );
        let (_, mut turn) = store.get_turn("thread", "turn").await.unwrap().unwrap();
        turn.status = pioneer_protocol::TurnStatus::Completed;
        store
            .materialize_turn_completed(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn,
                },
                1_700_000_100,
            )
            .await
            .unwrap();
        assert!(
            wake.wait(None).now_or_never().is_some(),
            "canonical terminal commit must release deferred archival work"
        );
        let mut progress = PortablePayloadProgress::default();
        finish_archive_pass(&mut progress, &store).await;
        let row = pioneer_entity::tool_output_chunk::Entity::find()
            .filter(pioneer_entity::tool_output_chunk::Column::Id.eq("wake-output"))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert!(row.text_payload_id.is_some());
    }
}
