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
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter,
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
const RECLAIMING_SHA256: &str = "----------------------------------------------------------------";

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

/// Memory-only protection of an immutable UUID payload. It owns no database
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
    pub(crate) fn capture(id: &str) -> Result<Self> {
        let guard = payload_gate(id).try_read_owned().map_err(|_| {
            observe::outcome(observe::Outcome::Stale);
            PreflightRace
        })?;
        Ok(Self(Arc::new(PayloadReadLease {
            id: id.to_owned(),
            guard: Some(guard),
        })))
    }
}
impl Drop for PayloadReadLease {
    fn drop(&mut self) {
        drop(self.guard.take());
        release_payload_gate(&self.id);
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
        drop(self.guard.take());
        release_payload_gate(&self.id);
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
    let mut observation = CodecObservation(Default::default());
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
}

async fn pending_source(
    store: &CrudStore,
    family: Family,
    after: Option<&str>,
    upper: Option<&str>,
    descending: bool,
) -> Result<Option<PendingSource>> {
    let _phase = observe::stage(observe::Stage::SourceRead);
    store
        .run_background_database_quantum(|| async {
            target!(family, |Target, Col, key, field, reference| {
                let mut query = Target::find()
                    .select_only()
                    .column_as(key, "owner")
                    .columns([Col::PortableRevision, Col::PortableIdentity])
                    .expr_as(Expr::cust(owner_stamp_sql(family)), "owner_stamp")
                    .expr_as(
                        Func::cust(Alias::new("octet_length")).arg(Expr::col(field)),
                        "bytes",
                    )
                    .filter(reference.is_null())
                    .filter(field.is_not_null());
                if matches!(family, Family::CliMapping | Family::TaskHistory) {
                    query = query.filter(
                        Expr::col(Alias::new("frozen_manifest_id"))
                            .is_null()
                            .or(Expr::col(Alias::new("frozen_manifest_id")).ne("")),
                    );
                }
                if let Some(after) = after {
                    query = query.filter(key.gt(after));
                }
                if let Some(upper) = upper {
                    query = query.filter(key.lte(upper));
                }
                query = if descending {
                    query.order_by_desc(key)
                } else {
                    query.order_by_asc(key)
                };
                Ok(query
                    .limit(1)
                    .into_model::<PendingSource>()
                    .one(&store.connection)
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
    pending: Option<Preparation>,
    after: [Option<String>; 5],
    // Finite per-pass fence only: reset/re-discover after EOF. A hot stream
    // of higher keys cannot prevent revisiting logical updates below cursor.
    upper: [Option<String>; 5],
    cleanup_after: Option<String>,
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
    _read_hold: PayloadReadHold,
    header: value::Model,
    blocks: Vec<block::Model>,
    next: i64,
    written: i64,
    encoded_bytes: usize,
    finalized: bool,
    installed: bool,
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
                Ok(true) => {}
                Err(error) => {
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
        let (cleaned, cleanup_after) = store
            .run_background_database_quantum(|| cleanup(store, self.cleanup_after.as_deref()))
            .await?;
        self.cleanup_after = cleanup_after;
        if cleaned {
            return Ok(true);
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
        let family = FAMILIES[self.family];
        observe::set_family(family.observed());
        self.family = (self.family + 1) % FAMILIES.len();
        // NULL metadata is final SQL NULL, not an empty/pending value.
        let index = family as usize;
        if self.after[index].is_none() {
            self.upper[index] = pending_source(store, family, None, None, true)
                .await?
                .map(|row| row.owner);
        }
        let Some(upper) = self.upper[index].as_deref() else {
            observe::outcome(observe::Outcome::NoWork);
            return Ok(false);
        };
        let row = pending_source(
            store,
            family,
            self.after[index].as_deref(),
            Some(upper),
            false,
        )
        .await?;
        if let Some(row) = row {
            self.after[family as usize] = Some(row.owner.clone());
            let payload_id = uuid::Uuid::new_v4().to_string();
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
                    _read_hold: PayloadReadHold::capture(&payload_id)?,
                    header: value::Model {
                        id: payload_id,
                        family: family.name().into(),
                        original_bytes: row.bytes,
                        original_chars: 0,
                        sha256: "0".repeat(64),
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
                    projection: None,
                },
                dictionary: None,
                shape: None,
            });
            return Ok(true);
        }
        self.after[family as usize] = None;
        self.upper[family as usize] = None;
        observe::outcome(observe::Outcome::NoWork);
        Ok(false)
    }
}
impl Preparation {
    async fn step(&mut self, store: &CrudStore) -> Result<bool> {
        let prepared = &mut self.prepared;
        if !prepared.installed {
            let _phase = observe::stage(observe::Stage::Publish);
            store
                .run_background_database_quantum(|| async {
                    value::Entity::insert(prepared.header.clone().into_active_model())
                        .on_conflict(
                            OnConflict::column(value::Column::Id)
                                .do_nothing()
                                .to_owned(),
                        )
                        .exec_without_returning(&store.connection)
                        .await?;
                    Ok(())
                })
                .await?;
            prepared.installed = true;
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
            if self.dictionary.is_none() {
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
                observe::live_stage(observe::Stage::Compress);
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
            let raw = prepared.encoded_bytes >= self.size || self.size == 0 || unsupported;
            prepared.header.raw = i32::from(raw);
            prepared.header.original_chars = self.chars;
            prepared.header.sha256 = hex::encode(self.digest.clone().finalize());
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
        if prepared.header.raw == 1 && {
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
    observe::live_stage(observe::Stage::DictionaryTrain);
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
async fn cleanup(store: &CrudStore, after: Option<&str>) -> Result<(bool, Option<String>)> {
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
        let Some(_exclusive) = PayloadCleanupHold::try_acquire(&id) else {
            continue;
        };
        // Revalidate after discovery. Live/staging publication has its own
        // read hold, so it cannot overlap this exclusive deletion quantum.
        let marked = value::Entity::update_many()
            .col_expr(value::Column::Sha256, Expr::value(RECLAIMING_SHA256))
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
    let hold = PayloadReadHold::capture(id)?;
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
            && row.sha256.len() == 64
            && (row.sha256.bytes().all(|b| b.is_ascii_hexdigit())
                || (row.retired == 1 && row.sha256 == RECLAIMING_SHA256))
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
                && hex::encode(Sha256::digest(plain.as_bytes())) == meta.sha256,
            "portable raw payload mismatch"
        );
        observe::outcome(observe::Outcome::Raw);
        return Ok(plain);
    }
    let size = usize::try_from(meta.original_bytes)?;
    let mut bytes = Vec::new();
    let mut offset = 0;
    while offset < size {
        let part = read_range(db, meta, offset as i64, (size - offset).min(BLOCK), false).await?;
        ensure!(!part.is_empty(), "portable payload block missing");
        offset += part.len();
        let _assembly = observe::stage(observe::Stage::VerifyAssemble);
        bytes.extend(part);
    }
    let _verify = observe::stage(observe::Stage::VerifyAssemble);
    ensure!(
        bytes.len() == size && hex::encode(Sha256::digest(&bytes)) == meta.sha256,
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
    let count = (count as i64).min(total - start);
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
        observe::live_stage(observe::Stage::Decompress);
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
        let take = usize::try_from((end - position).min(length - local as i64))?;
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

async fn publish(
    store: &CrudStore,
    family: Family,
    owner: &str,
    revision: i64,
    identity: &str,
    owner_stamp: &str,
    prepared: &Prepared,
) -> Result<()> {
    let (table, _, _) = family.spec();
    let _phase = observe::stage(observe::Stage::Publish);
    let tx = store.connection.begin().await?;
    // Source revision triggers cover logical payload and ownership updates.
    // Unknown/expired roots cannot pass the existing verified root trigger.
    // Revalidate Task/run/workspace at publication, not from preparation.
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
    let root_guard = if matches!(family, Family::CliMapping | Family::TaskHistory) {
        format!(
            " AND (frozen_manifest_id IS NULL OR (frozen_manifest_id<>'' AND EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id={table}.frozen_manifest_id AND h.workspace_id={table}.workspace_id AND h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count)))"
        )
    } else {
        String::new()
    };
    let affected = target!(family, |Target, Col, key, field, reference| {
        let mut update = Target::update_many()
            .col_expr(reference, Expr::value(prepared.header.id.clone()))
            .filter(key.eq(owner))
            .filter(Col::PortableRevision.eq(revision))
            .filter(Col::PortableIdentity.eq(identity))
            .filter(Expr::cust(owner_stamp_sql(family)).eq(owner_stamp))
            .filter(reference.is_null())
            .filter(Expr::cust(owner_guard.trim_start_matches(" AND ")));
        if !root_guard.is_empty() {
            update = update.filter(Expr::cust(root_guard.trim_start_matches(" AND ")));
        }
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
    tx.commit().await?;
    // A stale/unavailable owner is not committed completion. Diagnose it and
    // use the existing worker's bounded backoff rather than repeatedly encoding
    // an expired root or orphan source without an observable refusal.
    if affected != 1 {
        observe::outcome(observe::Outcome::Stale);
    }
    ensure!(affected == 1, "portable source changed before publication");
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
            "coalesce((SELECT json_array(th.workspace_id,t.thread_id) FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=tool_output_chunk.turn_id),'')"
        }
        Family::CliMapping => {
            "coalesce((SELECT json_array(th.workspace_id,t.thread_id,(SELECT json_array(h.workspace_id,h.owner_thread,h.identity_sha256,h.message_count,h.import_count,h.imports_sha256,h.expired,h.ready,h.next_ordinal,h.next_import) FROM compaction_frozen_history h WHERE h.id=turn_cli_runtime_binding.frozen_manifest_id)) FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=turn_cli_runtime_binding.turn_id),'')"
        }
        Family::TaskHistory => {
            "coalesce((SELECT json_array(t.workspace_id,t.owner_kind,t.owner_id,t.status,t.updated_at,(SELECT json_array(h.workspace_id,h.owner_thread,h.identity_sha256,h.message_count,h.import_count,h.imports_sha256,h.expired,h.ready,h.next_ordinal,h.next_import) FROM compaction_frozen_history h WHERE h.id=task_run_conversation_snapshot.frozen_manifest_id)) FROM task t WHERE t.id=task_run_conversation_snapshot.task_id),'')"
        }
        Family::TaskEvent => {
            "coalesce((SELECT json_array(t.workspace_id,t.owner_kind,t.owner_id,t.status,t.updated_at) FROM task t WHERE t.id=task_event.task_id),'')"
        }
    }
}
