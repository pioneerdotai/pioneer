//! Permanent selected checkpoint evidence, prepared with the existing writer.
//! Historical evidence is not a new consumer grant.
use super::compaction::{
    self, CHECKPOINT_SOURCE_LIMIT, CheckpointEdgesRow, HistoricalEventInputEvidenceRow as Event,
    HistoricalReplayAliasRow as Alias, SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS,
};
use super::compaction_frozen_import::{FROZEN_IMPORT_PAGE_BYTES, FrozenImportRecord};
use crate::{CrudStore, FrozenReadHold};
use anyhow::{Result, ensure};
use pioneer_compaction::{
    OperationSnapshot, SourceRef,
    frozen::{FrozenEventInputRole, FrozenMessageRef},
};
use pioneer_entity::{
    compaction_checkpoint_import as import, compaction_frozen_history as history,
};
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement, TransactionTrait};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

type Ownership = BTreeMap<SourceRef, BTreeSet<String>>;
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Metadata {
    pub aliases: BTreeSet<Alias>,
    pub events: BTreeSet<Event>,
}

/// The single selection rule, shared by legacy reads and new preparation.
fn select_reference(
    reference: &FrozenMessageRef,
    ownership: &Ownership,
    metadata: &mut Metadata,
) -> Result<()> {
    for source in reference.sources.iter().filter(|source| {
        ownership
            .get(*source)
            .is_some_and(|threads| threads.len() == 1 && threads.contains(&reference.source_thread))
    }) {
        for edge in reference.publication_edges_for(source) {
            metadata.aliases.insert(Alias {
                covered_thread: edge.covered_thread,
                replay_thread: edge.replay_thread,
                covered_scope: edge.covered.scope,
                covered_id: edge.covered.id,
                covered_version: edge.covered.version,
                replay_scope: edge.replay.scope,
                replay_id: edge.replay.id,
                replay_version: edge.replay.version,
                tool_item_id: edge.tool_item_id,
            });
        }
    }
    if let [source] = reference.sources.as_slice()
        && let Some(role) = reference.event_input_role
        && ownership
            .get(source)
            .is_some_and(|threads| threads.len() == 1 && threads.contains(&reference.source_thread))
    {
        let event = Event {
            source_thread: reference.source_thread.clone(),
            source_scope: source.scope.clone(),
            source_id: source.id.clone(),
            source_version: source.version.clone(),
            role: match role {
                FrozenEventInputRole::Authoritative => "authoritative",
                FrozenEventInputRole::Deleted => "deleted",
                FrozenEventInputRole::InputCopy => "input_copy",
            }
            .into(),
        };
        ensure!(
            !metadata
                .events
                .iter()
                .any(|old| old.source_thread == event.source_thread
                    && old.source_scope == event.source_scope
                    && old.source_id == event.source_id
                    && old.source_version == event.source_version
                    && old.role != event.role),
            "conflicting historical event role"
        );
        metadata.events.insert(event);
    }
    ensure!(
        metadata.aliases.len() <= CHECKPOINT_SOURCE_LIMIT
            && metadata.events.len() <= CHECKPOINT_SOURCE_LIMIT,
        "checkpoint selected metadata exceeds supported quantum"
    );
    Ok(())
}

/// A missing frozen binding is legitimate only for the original raw assertion
/// admission, whose complete plan was durably saved (frozen admission saves an
/// empty compact/coverage plan). Historical ownership must still be exact.
fn raw_contract(
    snapshot: &OperationSnapshot,
    owner: &str,
    row: &CheckpointEdgesRow,
    ownership: &Ownership,
) -> Result<()> {
    let admitted = snapshot.plan.coverage.iter().collect::<BTreeSet<_>>();
    ensure!(
        owner == row.owner
            && snapshot.owner == row.owner
            && snapshot.id == row.operation_id
            && !snapshot.plan.compact.is_empty()
            && !snapshot.plan.coverage.is_empty()
            && ownership.keys().all(|source| admitted.contains(source)),
        "checkpoint lost expected frozen binding or original raw contract"
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum Phase {
    Raw,
    Messages,
    Imports,
    Done,
}

/// Only the current scan is pinned; no connection/permit is retained. Each
/// step reads one bounded message page or one original import/target pair.
pub(super) struct OriginScan {
    pub row: CheckpointEdgesRow,
    ownership: Ownership,
    pub header: Option<history::Model>,
    pub hold: Option<FrozenReadHold>,
    pub metadata: Metadata,
    layout: Option<crate::frozen_lifetime::LayoutScan>,
    raw: Option<RawAdmissionRead>,
    pending_import: Option<PendingImport>,
    phase: Phase,
    next_message: i64,
    next_import: u64,
    messages_digest: Sha256,
    imports_digest: Sha256,
    imports_page: VecDeque<FrozenImportRecord>,
    pub selected_import_count: u64,
    selected_import_digest: Sha256,
}

/// Historical reads may adopt a concurrently sealed node. This decision is
/// local to checkpoint evidence; exact frozen replay still uses acquire directly.
pub(super) enum OriginRead {
    Legacy(OriginScan),
    Permanent(compaction::CheckpointTopology),
}

impl OriginScan {
    pub async fn new(
        store: &CrudStore,
        row: CheckpointEdgesRow,
        ownership: Ownership,
    ) -> Result<OriginRead> {
        let row = compaction::refresh_checkpoint_row(&store.connection, &row).await?;
        if row.proof_version == 1 {
            return Ok(OriginRead::Permanent(compaction::CheckpointTopology {
                row,
                ownership,
            }));
        }
        let (header, hold, phase) = if let Some(manifest) = &row.manifest_id {
            let (descriptor, header) = super::compaction_source_projection::projection_identity(
                &store.connection,
                &row.operation_id,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint lost its bound origin"))?;
            ensure!(
                &descriptor.manifest_id == manifest && header.workspace_id == row.workspace_id,
                "checkpoint origin scope mismatch"
            );
            #[cfg(any(test, feature = "test-support"))]
            super::compaction_runner::pause_publication_test_hook(
                store.connection.runtime_identity(),
                &row.operation_id,
                super::compaction_runner::PublicationTestPause::CheckpointOriginAcquire,
            )
            .await;
            let (_, hold) = match store
                .acquire_frozen_header(&row.workspace_id, &descriptor)
                .await
            {
                Ok(acquired) => acquired,
                Err(error) => {
                    // Only the existing expiry classification may cross this
                    // boundary. Other missing/malformed/binding errors remain
                    // errors, even if another preparation has sealed the node.
                    if error.to_string().starts_with("frozen_input_expired:") {
                        let current =
                            compaction::refresh_checkpoint_row(&store.connection, &row).await?;
                        if current.proof_version == 1 {
                            return Ok(OriginRead::Permanent(compaction::CheckpointTopology {
                                row: current,
                                ownership,
                            }));
                        }
                    }
                    return Err(error);
                }
            };
            (Some(header), Some(hold), Phase::Messages)
        } else {
            (None, None, Phase::Raw)
        };
        let raw = if matches!(phase, Phase::Raw) {
            Some(RawAdmissionRead::new(store, &row.operation_id).await?)
        } else {
            None
        };
        let layout = header
            .clone()
            .map(|h| crate::frozen_lifetime::LayoutScan::new(h, 0, 2))
            .transpose()?;
        Ok(OriginRead::Legacy(Self {
            row,
            ownership,
            layout,
            pending_import: None,
            raw,
            header,
            hold,
            metadata: Metadata::default(),
            phase,
            next_message: 0,
            next_import: 0,
            messages_digest: Sha256::new(),
            imports_digest: Sha256::new(),
            imports_page: VecDeque::new(),
            selected_import_count: 0,
            selected_import_digest: Sha256::new(),
        }))
    }
    pub fn done(&self) -> bool {
        matches!(self.phase, Phase::Done)
    }
    pub fn selected_digest(&self) -> String {
        hex::encode(self.selected_import_digest.clone().finalize())
    }
    pub async fn step(&mut self, store: &CrudStore) -> Result<Option<import::Model>> {
        if let Some(pending) = self.pending_import.as_mut() {
            if let Some(member) = pending.member.as_mut() {
                if !member.done() {
                    member.step(store).await?;
                    return Ok(None);
                }
                ensure!(member.found, "selected checkpoint exceeds original grant");
            }
            original_binding(
                store,
                &self.row.workspace_id,
                &pending.context,
                &pending.record,
                true,
            )
            .await?;
            let pending = self.pending_import.take().expect("selected import");
            digest(
                &mut self.selected_import_digest,
                &import_identity(&pending.proof)?,
            );
            self.selected_import_count += 1;
            return Ok(Some(pending.proof));
        }
        if let Some(layout) = self.layout.as_mut() {
            layout.step(store).await?;
            if layout.done() {
                self.layout = None;
            }
            return Ok(None);
        }
        match self.phase {
            Phase::Raw => {
                let raw = self.raw.as_mut().expect("raw origin");
                raw.step(store).await?;
                if raw.done {
                    let raw = self.raw.take().expect("complete raw origin");
                    let owner = raw.owner.clone();
                    raw_contract(&raw.finish()?, &owner, &self.row, &self.ownership)?;
                    self.phase = Phase::Done;
                }
                Ok(None)
            }
            Phase::Messages => {
                self.messages_step(store).await?;
                Ok(None)
            }
            Phase::Imports => self.import_step(store).await,
            Phase::Done => Ok(None),
        }
    }
    async fn messages_step(&mut self, store: &CrudStore) -> Result<()> {
        let header = self.header.as_ref().expect("origin phase");
        let manifest = &header.id;
        let sizes = store
            .connection
            .query_all_raw(compaction::checkpoint_projection_page_sizes_statement(
                manifest,
                self.next_message,
            ))
            .await?;
        if sizes.is_empty() {
            ensure!(
                self.next_message == header.message_count,
                "checkpoint origin metadata is incomplete"
            );
            ensure!(
                hex::encode(self.messages_digest.clone().finalize()) == header.identity_sha256,
                "checkpoint origin message digest mismatch"
            );
            self.phase = Phase::Imports;
            return Ok(());
        }
        let mut end = self.next_message;
        let mut bytes = 0usize;
        for size in sizes {
            let ordinal: i64 = size.try_get("", "ordinal")?;
            let size: i64 = size.try_get("", "bytes")?;
            ensure!(
                (0..=SOURCE_PAGE_BYTES as i64).contains(&size),
                "invalid frozen reference size"
            );
            if bytes + size as usize > SOURCE_PAGE_BYTES {
                break;
            }
            ensure!(
                ordinal == end && end < header.message_count,
                "checkpoint origin ordinal mismatch"
            );
            bytes += size as usize;
            end += 1;
        }
        ensure!(
            end > self.next_message,
            "checkpoint origin page made no progress"
        );
        let rows = store
            .connection
            .query_all_raw(compaction::checkpoint_projection_page_statement(
                manifest,
                self.next_message,
                end,
            ))
            .await?;
        #[cfg(test)]
        compaction::checkpoint_projection_page_test_pause(&store.connection, manifest).await;
        ensure!(
            rows.len() == usize::try_from(end - self.next_message)?
                && rows.len() <= SOURCE_PAGE_ROWS as usize,
            "checkpoint origin readback incomplete"
        );
        let mut expected = self.next_message;
        let mut loaded = 0usize;
        for row in rows {
            let ordinal: i64 = row.try_get("", "ordinal")?;
            let json: String = row.try_get("", "reference_json")?;
            let size: i64 = row.try_get("", "bytes")?;
            ensure!(
                ordinal == expected && size >= 0 && json.len() == usize::try_from(size)?,
                "checkpoint origin exact size/ordinal mismatch"
            );
            loaded += json.len();
            ensure!(
                loaded <= SOURCE_PAGE_BYTES,
                "checkpoint origin byte quantum exceeded"
            );
            let reference: FrozenMessageRef = serde_json::from_str(&json)?;
            reference.validate()?;
            let canonical = serde_json::to_vec(&reference)?;
            self.messages_digest
                .update((canonical.len() as u64).to_be_bytes());
            self.messages_digest.update(canonical);
            select_reference(&reference, &self.ownership, &mut self.metadata)?;
            expected += 1;
        }
        #[cfg(test)]
        compaction::record_checkpoint_projection_page_test_read(
            &store.connection,
            manifest,
            self.next_message,
            end,
            usize::try_from(end - self.next_message)?,
            loaded,
        );
        self.next_message = end;
        Ok(())
    }
    async fn import_step(&mut self, store: &CrudStore) -> Result<Option<import::Model>> {
        let header = self.header.as_ref().expect("origin phase");
        if self.imports_page.is_empty() {
            self.imports_page = compaction::frozen_import::compaction_frozen_import_page(
                &store.connection,
                &header.workspace_id,
                &header.owner_thread,
                &header.id,
                self.next_import,
            )
            .await?
            .into();
            if self.imports_page.is_empty() {
                ensure!(
                    self.next_import == u64::try_from(header.import_count)?,
                    "checkpoint origin imports incomplete"
                );
                ensure!(
                    hex::encode(self.imports_digest.clone().finalize()) == header.imports_sha256,
                    "checkpoint origin import digest mismatch"
                );
                self.phase = Phase::Done;
                return Ok(None);
            }
        }
        ensure!(
            self.next_import < u64::try_from(header.import_count)?,
            "checkpoint origin has extra imports"
        );
        let record = self.imports_page.pop_front().expect("loaded import");
        let json = serde_json::to_string(&record)?;
        digest(&mut self.imports_digest, json.as_bytes());
        let ordinal = self.next_import;
        self.next_import += 1;
        let target = target_reference(store, &header.id, record.message_ordinal).await?;
        let selected = target.sources.iter().any(|source| {
            self.ownership.get(source).is_some_and(|threads| {
                threads.len() == 1 && threads.contains(&target.source_thread)
            })
        });
        if !selected {
            return Ok(None);
        }
        ensure!(
            !target.inherited && target.complete && !target.protected_input,
            "selected import target is not accepted own work"
        );
        let context = target
            .context_thread
            .as_deref()
            .unwrap_or(&target.source_thread)
            .to_owned();
        ensure!(
            context == header.owner_thread || context == self.row.thread_id,
            "selected import target context mismatch"
        );
        let checkpoint_target = match target.sources.as_slice() {
            [source] if source.scope.starts_with("checkpoint:") => Some(source.clone()),
            _ => None,
        };
        ensure!(
            checkpoint_target.is_some()
                || (target.source_thread == record.source_thread
                    && target.sources.contains(&record.source)),
            "selected import target/source mismatch"
        );
        let member = if let Some(checkpoint) = &checkpoint_target {
            Some(
                HistoricalMember::new(
                    store,
                    &self.row.workspace_id,
                    &target.source_thread,
                    checkpoint,
                    &record.source_thread,
                    &record.source,
                )
                .await?,
            )
        } else {
            None
        };
        let target_checkpoint_json = checkpoint_target
            .map(|s| serde_json::to_string(&s))
            .transpose()?;
        let size = json.len()
            + target.source_thread.len()
            + context.len()
            + target_checkpoint_json.as_ref().map_or(0, String::len);
        ensure!(
            size <= FROZEN_IMPORT_PAGE_BYTES,
            "selected import proof exceeds quantum"
        );
        let proof = import::Model {
            checkpoint_id: self.row.id.clone(),
            import_ordinal: i64::try_from(ordinal)?,
            message_ordinal: i64::try_from(record.message_ordinal)?,
            target_source_thread: target.source_thread,
            context_thread: context.clone(),
            target_checkpoint_json,
            proof_json: json,
            bytes: i64::try_from(size)?,
        };
        self.pending_import = Some(PendingImport {
            proof,
            record,
            context,
            member,
        });
        Ok(None)
    }
}

fn digest(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}
fn import_identity(proof: &import::Model) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        proof.import_ordinal,
        proof.message_ordinal,
        &proof.target_source_thread,
        &proof.context_thread,
        &proof.target_checkpoint_json,
        &proof.proof_json,
        proof.bytes,
    ))?)
}
fn target_reference_statement(manifest: &str, ordinal: u64) -> Result<Statement> {
    Ok(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT reference_json,bytes FROM compaction_frozen_message WHERE manifest_id=?1 AND ordinal=?2 AND bytes BETWEEN 0 AND ?3 AND length(CAST(reference_json AS BLOB))=bytes LIMIT 1",
        [
            manifest.into(),
            i64::try_from(ordinal)?.into(),
            i64::try_from(SOURCE_PAGE_BYTES)?.into(),
        ],
    ))
}
async fn target_reference(
    store: &CrudStore,
    manifest: &str,
    ordinal: u64,
) -> Result<FrozenMessageRef> {
    // The logical view applies H's bounds to both direct and shared branches.
    // Understated retained-output bytes must not return a payload at all.
    let row = store
        .connection
        .query_one_raw(target_reference_statement(manifest, ordinal)?)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("original import target missing, inconsistent or oversized")
        })?;
    let json: String = row.try_get("", "reference_json")?;
    let bytes: i64 = row.try_get("", "bytes")?;
    ensure!(
        json.len() == usize::try_from(bytes)?,
        "original import target size mismatch"
    );
    let target: FrozenMessageRef = serde_json::from_str(&json)?;
    target.validate()?;
    Ok(target)
}

/// Exact historical membership, without recursively extracting origins. Full
/// alias/event graph validation remains a separate preparation/consumer guard.
struct PendingImport {
    proof: import::Model,
    record: FrozenImportRecord,
    context: String,
    member: Option<HistoricalMember>,
}
struct HistoricalMember {
    workspace: String,
    wanted_thread: String,
    wanted: SourceRef,
    pending: Vec<(SourceRef, String, bool)>,
    active: BTreeSet<(SourceRef, String)>,
    done: BTreeSet<(SourceRef, String)>,
    found: bool,
    topology: Option<(SourceRef, String, compaction::CheckpointTopologyRead)>,
}
impl HistoricalMember {
    async fn new(
        store: &CrudStore,
        workspace: &str,
        root_thread: &str,
        root: &SourceRef,
        wanted_thread: &str,
        wanted: &SourceRef,
    ) -> Result<Self> {
        ensure!(
            compaction::compaction_checkpoint_source(
                &store.connection,
                workspace,
                root_thread,
                &root.id
            )
            .await?
            .as_ref()
                == Some(root),
            "import checkpoint is not an exact published source"
        );
        Ok(Self {
            workspace: workspace.into(),
            wanted_thread: wanted_thread.into(),
            wanted: wanted.clone(),
            pending: vec![(root.clone(), root_thread.into(), false)],
            active: BTreeSet::new(),
            done: BTreeSet::new(),
            found: false,
            topology: None,
        })
    }
    fn done(&self) -> bool {
        self.pending.is_empty() && self.topology.is_none()
    }
    async fn step(&mut self, store: &CrudStore) -> Result<()> {
        if self.topology.is_none() {
            let Some((source, thread, exiting)) = self.pending.pop() else {
                return Ok(());
            };
            let key = (source.clone(), thread.clone());
            if exiting {
                self.active.remove(&key);
                self.done.insert(key);
                return Ok(());
            }
            if self.done.contains(&key) {
                return Ok(());
            }
            if source == self.wanted && thread == self.wanted_thread {
                self.found = true;
            }
            if !source.scope.starts_with("checkpoint:") {
                self.done.insert(key);
                return Ok(());
            }
            ensure!(self.active.insert(key), "cyclic checkpoint import coverage");
            ensure!(
                self.done.len() + self.active.len() <= 65_536,
                "checkpoint import graph exceeds supported bound"
            );
            let read = compaction::CheckpointTopologyRead::new(&store.connection, &source.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("checkpoint import node missing"))?;
            self.topology = Some((source, thread, read));
            return Ok(());
        }
        let (_, _, read) = self.topology.as_mut().expect("import topology");
        read.step(&store.connection).await?;
        if !read.done {
            return Ok(());
        }
        let (source, thread, read) = self.topology.take().expect("completed import topology");
        let topology = read.finish()?;
        let row = topology.row;
        ensure!(
            row.workspace_id == self.workspace
                && row.thread_id == thread
                && row.format_version == 1
                && source.scope == format!("checkpoint:{}", row.owner)
                && source.version == row.identity_sha256,
            "checkpoint import historical identity mismatch"
        );
        self.pending.push((source, thread, true));
        if let Some(previous) = row.previous {
            let previous = compaction::checkpoint_row(&store.connection, &previous)
                .await?
                .ok_or_else(|| anyhow::anyhow!("checkpoint import previous missing"))?;
            ensure!(
                previous.owner == row.owner
                    && previous.thread_id == row.thread_id
                    && previous.workspace_id == self.workspace
                    && previous.format_version == 1,
                "checkpoint import previous ownership mismatch"
            );
            self.pending.push((
                SourceRef {
                    scope: format!("checkpoint:{}", previous.owner),
                    id: previous.id,
                    version: previous.identity_sha256,
                },
                previous.thread_id,
                false,
            ));
        }
        for (source, threads) in topology.ownership {
            for thread in threads {
                self.pending.push((source.clone(), thread, false));
            }
        }
        Ok(())
    }
}

/// Original acceptance metadata survives canonical source edits. Only a fresh
/// delivery grant uses current-source predicates in the existing import helper.
async fn original_binding(
    store: &CrudStore,
    workspace: &str,
    context: &str,
    proof: &FrozenImportRecord,
    read_output: bool,
) -> Result<()> {
    ensure!(
        !proof.source_thread.is_empty()
            && !proof.source.scope.is_empty()
            && !proof.source.id.is_empty()
            && !proof.source.version.is_empty()
            && !proof.delivery_id.is_empty()
            && !proof.candidate_id.is_empty()
            && proof
                .acknowledgement
                .version
                .strip_prefix("event-revision:")
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|n| n > 0),
        "malformed original accepted import identity"
    );
    let row = store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite, r#"
SELECT h.id,h.message_count,h.identity_sha256,h.ready,h.expired,h.next_ordinal,h.import_count,h.next_import,
       o.workspace_id,o.source_thread,h.owner_thread,ack.revision AS ack_revision
FROM task_delivery d
JOIN compaction_delivery_output b ON b.delivery_id=d.id
JOIN compaction_task_output o ON o.task_run_turn_id=b.task_run_turn_id
JOIN compaction_frozen_history h ON h.id=o.manifest_id
JOIN task_result_candidate c ON c.id=b.candidate_id AND c.task_id=d.task_id AND c.run_id=d.run_id
JOIN compaction_event_revision ack ON ack.source_id=?6 AND ack.turn_id=d.delivered_turn_id
WHERE d.id=?1 AND b.candidate_id=?2 AND o.manifest_id=?3 AND d.workspace_id=?4
  AND o.workspace_id=d.workspace_id AND h.workspace_id=o.workspace_id AND o.task_id=d.task_id AND o.run_id=d.run_id
  AND d.target_thread_id=?5 AND d.status='delivered'
  AND 'event:'||ack.turn_id=?7 AND ack.item_id=?8
LIMIT 1"#, [proof.delivery_id.clone().into(),proof.candidate_id.clone().into(),proof.output_manifest.clone().into(),workspace.into(),context.into(),proof.acknowledgement.id.clone().into(),proof.acknowledgement.scope.clone().into(),pioneer_protocol::task_delivery_result_item_id(&proof.delivery_id).into()])).await?
        .ok_or_else(|| anyhow::anyhow!("original accepted import binding mismatch"))?;
    let count: i64 = row.try_get("", "message_count")?;
    ensure!(
        i64::try_from(proof.output_ordinal)? < count
            && count >= 0
            && row.try_get::<i64>("", "ready")? == 1
            && row.try_get::<i64>("", "expired")? == 0
            && row.try_get::<i64>("", "next_ordinal")? == count
            && row.try_get::<i64>("", "import_count")? >= 0
            && row.try_get::<i64>("", "next_import")? == row.try_get::<i64>("", "import_count")?
            && row.try_get::<String>("", "owner_thread")?
                == row.try_get::<String>("", "source_thread")?
            && proof
                .acknowledgement
                .version
                .strip_prefix("event-revision:")
                .and_then(|n| n.parse::<i64>().ok())
                .is_some_and(
                    |revision| revision <= row.try_get::<i64>("", "ack_revision").unwrap_or(0)
                ),
        "original output ordinal out of bounds"
    );
    if read_output {
        let original =
            target_reference(store, &proof.output_manifest, proof.output_ordinal).await?;
        let owner: String = row.try_get("", "source_thread")?;
        ensure!(
            !original.inherited
                && original.complete
                && !original.protected_input
                && original
                    .context_thread
                    .as_deref()
                    .unwrap_or(&original.source_thread)
                    == owner
                && original.source_thread == proof.source_thread
                && original.sources.contains(&proof.source),
            "original accepted output membership mismatch"
        );
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
struct Control {
    owner: String,
    status: String,
    generation: Option<i64>,
}
async fn control<C: ConnectionTrait>(db: &C, operation: &str) -> Result<Control> {
    Control::find_by_statement(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT o.owner,o.status,s.generation FROM compaction_operation o LEFT JOIN compaction_runner_state s ON s.operation_id=o.id WHERE o.id=?1 LIMIT 1",[operation.into()]))
        .one(db).await?.ok_or_else(|| anyhow::anyhow!("proof operation missing"))
}

async fn stage_import(store: &CrudStore, proof: &import::Model) -> Result<()> {
    let values = vec![
        proof.checkpoint_id.clone().into(),
        proof.import_ordinal.into(),
        proof.message_ordinal.into(),
        proof.target_source_thread.clone().into(),
        proof.context_thread.clone().into(),
        proof.target_checkpoint_json.clone().into(),
        proof.proof_json.clone().into(),
        proof.bytes.into(),
    ];
    store.run_serialized_write(|| async {
        let tx=store.connection.begin().await?;
        tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_checkpoint_import(checkpoint_id,import_ordinal,message_ordinal,target_source_thread,context_thread,target_checkpoint_json,proof_json,bytes) SELECT ?1,?2,?3,?4,?5,?6,?7,?8 WHERE EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id=?1 AND proof_version=0) ON CONFLICT DO NOTHING",values.clone())).await?;
        let exact=import::Model::find_by_statement(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT * FROM compaction_checkpoint_import WHERE checkpoint_id=?1 AND import_ordinal=?2 AND bytes BETWEEN 0 AND ?3 AND length(CAST(proof_json AS BLOB))+length(CAST(target_source_thread AS BLOB))+length(CAST(context_thread AS BLOB))+COALESCE(length(CAST(target_checkpoint_json AS BLOB)),0)<=?3 LIMIT 1",
            [proof.checkpoint_id.clone().into(),proof.import_ordinal.into(),i64::try_from(FROZEN_IMPORT_PAGE_BYTES)?.into()])).one(&tx).await?;
        ensure!(exact.as_ref()==Some(proof),"selected import staging conflict");
        tx.commit().await?; Ok(())
    }).await
}
async fn stage_alias(store: &CrudStore, id: &str, alias: &Alias) -> Result<()> {
    let values = vec![
        id.into(),
        alias.covered_thread.clone().into(),
        alias.covered_scope.clone().into(),
        alias.covered_id.clone().into(),
        alias.covered_version.clone().into(),
        alias.replay_thread.clone().into(),
        alias.replay_scope.clone().into(),
        alias.replay_id.clone().into(),
        alias.replay_version.clone().into(),
        alias.tool_item_id.clone().into(),
    ];
    ensure!(
        serde_json::to_vec(alias)?.len() <= SOURCE_PAGE_BYTES,
        "oversized selected alias"
    );
    store.run_serialized_write(||async{
        let tx=store.connection.begin().await?;
        tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_checkpoint_replay_alias(checkpoint_id,covered_thread,covered_scope,covered_id,covered_version,replay_thread,replay_scope,replay_id,replay_version,tool_item_id) SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10 WHERE EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id=?1 AND proof_version=0) ON CONFLICT DO NOTHING",values.clone())).await?;
        let found=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_checkpoint_replay_alias WHERE checkpoint_id=?1 AND covered_thread=?2 AND covered_scope=?3 AND covered_id=?4 AND covered_version=?5 AND replay_thread=?6 AND replay_scope=?7 AND replay_id=?8 AND replay_version=?9 AND tool_item_id IS ?10 LIMIT 1",values.clone())).await?;
        ensure!(found.is_some(),"alias staging conflict"); tx.commit().await?;Ok(())
    }).await
}
async fn stage_event(store: &CrudStore, id: &str, event: &Event) -> Result<()> {
    let values = vec![
        id.into(),
        event.source_thread.clone().into(),
        event.source_scope.clone().into(),
        event.source_id.clone().into(),
        event.source_version.clone().into(),
        event.role.clone().into(),
    ];
    ensure!(
        serde_json::to_vec(event)?.len() <= SOURCE_PAGE_BYTES,
        "oversized selected event"
    );
    store.run_serialized_write(||async{
        let tx=store.connection.begin().await?;
        tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_checkpoint_event_input(checkpoint_id,source_thread,source_scope,source_id,source_version,role) SELECT ?1,?2,?3,?4,?5,?6 WHERE EXISTS(SELECT 1 FROM compaction_checkpoint WHERE id=?1 AND proof_version=0) ON CONFLICT DO NOTHING",values.clone())).await?;
        let found=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_checkpoint_event_input WHERE checkpoint_id=?1 AND source_thread=?2 AND source_scope=?3 AND source_id=?4 AND source_version=?5 AND role=?6 LIMIT 1",values.clone())).await?;
        ensure!(found.is_some(),"event role staging conflict");tx.commit().await?;Ok(())
    }).await
}

/// Sizes are obtained before payload. A selected ordinal need not be contiguous:
/// it is the original import ordinal, not a renumbered accumulated closure.
async fn read_import(store: &CrudStore, id: &str, after: i64) -> Result<Option<import::Model>> {
    let Some(size)=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT import_ordinal,bytes FROM compaction_checkpoint_import WHERE checkpoint_id=?1 AND import_ordinal>?2 ORDER BY import_ordinal LIMIT 1",[id.into(),after.into()])).await? else{return Ok(None)};
    let ordinal: i64 = size.try_get("", "import_ordinal")?;
    let bytes: i64 = size.try_get("", "bytes")?;
    ensure!(
        (0..=FROZEN_IMPORT_PAGE_BYTES as i64).contains(&bytes),
        "invalid permanent import size"
    );
    let row=import::Model::find_by_statement(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT * FROM compaction_checkpoint_import WHERE checkpoint_id=?1 AND import_ordinal=?2 AND bytes=?3 AND length(CAST(proof_json AS BLOB))+length(CAST(target_source_thread AS BLOB))+length(CAST(context_thread AS BLOB))+COALESCE(length(CAST(target_checkpoint_json AS BLOB)),0)=?3 LIMIT 1",[id.into(),ordinal.into(),bytes.into()])).one(&store.connection).await?;
    ensure!(
        row.is_some(),
        "permanent import readback missing or inconsistent"
    );
    Ok(row)
}

#[derive(Default)]
struct MetadataReadback {
    alias_after: i64,
    event_after: i64,
    events: bool,
    done: bool,
    metadata: Metadata,
}
impl MetadataReadback {
    /// One physical row per step, with a sizes-first lookup.
    async fn step(&mut self, store: &CrudStore, id: &str) -> Result<()> {
        let (table, after, columns) = if self.events {
            (
                "compaction_checkpoint_event_input",
                self.event_after,
                "source_thread,source_scope,source_id,source_version,role",
            )
        } else {
            (
                "compaction_checkpoint_replay_alias",
                self.alias_after,
                "covered_thread,covered_scope,covered_id,covered_version,replay_thread,replay_scope,replay_id,replay_version,tool_item_id",
            )
        };
        let sum = columns
            .split(',')
            .map(|c| format!("COALESCE(length(CAST({c} AS BLOB)),0)"))
            .collect::<Vec<_>>()
            .join("+");
        let size=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            format!("SELECT rowid,{sum} AS bytes FROM {table} WHERE checkpoint_id=?1 AND rowid>?2 ORDER BY rowid LIMIT 1"),[id.into(),after.into()])).await?;
        let Some(size) = size else {
            if self.events {
                self.done = true;
            } else {
                self.events = true;
            }
            return Ok(());
        };
        let rowid: i64 = size.try_get("", "rowid")?;
        let bytes: i64 = size.try_get("", "bytes")?;
        ensure!(
            (0..=SOURCE_PAGE_BYTES as i64).contains(&bytes),
            "oversized permanent metadata"
        );
        let row=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            format!("SELECT checkpoint_id,{columns} FROM {table} WHERE checkpoint_id=?1 AND rowid=?2 AND {sum}=?3 LIMIT 1"),[id.into(),rowid.into(),bytes.into()])).await?.ok_or_else(||anyhow::anyhow!("permanent metadata readback disappeared"))?;
        if self.events {
            let saved =
                pioneer_entity::compaction_checkpoint_event_input::Model::from_query_result(
                    &row, "",
                )?;
            self.event_after = rowid;
            self.metadata.events.insert(Event {
                source_thread: saved.source_thread,
                source_scope: saved.source_scope,
                source_id: saved.source_id,
                source_version: saved.source_version,
                role: saved.role,
            });
        } else {
            let saved =
                pioneer_entity::compaction_checkpoint_replay_alias::Model::from_query_result(
                    &row, "",
                )?;
            self.alias_after = rowid;
            self.metadata.aliases.insert(Alias {
                covered_thread: saved.covered_thread,
                covered_scope: saved.covered_scope,
                covered_id: saved.covered_id,
                covered_version: saved.covered_version,
                replay_thread: saved.replay_thread,
                replay_scope: saved.replay_scope,
                replay_id: saved.replay_id,
                replay_version: saved.replay_version,
                tool_item_id: saved.tool_item_id,
            });
        }
        ensure!(
            self.metadata.aliases.len() <= CHECKPOINT_SOURCE_LIMIT
                && self.metadata.events.len() <= CHECKPOINT_SOURCE_LIMIT,
            "permanent metadata exceeds bound"
        );
        Ok(())
    }
}

/// Incremental private preparation is reused by foreground publication and the
/// existing maintenance quantum. Only this current origin has a temporary hold.
pub(super) struct NodePreparation {
    scan: OriginScan,
    control: Control,
    aliases: VecDeque<Alias>,
    events: VecDeque<Event>,
    scanning: bool,
    metadata_readback: MetadataReadback,
    import_after: i64,
    import_count: u64,
    import_digest: Sha256,
    imports_done: bool,
    pub done: bool,
    graph_validated: bool,
    previous_ready: bool,
    dependency_after: Option<SourceRef>,
    dependencies_ready: bool,
}
impl NodePreparation {
    async fn new(
        store: &CrudStore,
        topology: compaction::CheckpointTopology,
        reachable: bool,
    ) -> Result<NodeWork> {
        ensure!(
            topology.row.proof_version == 0,
            "node is already sealed or has unsupported version"
        );
        let ctl = control(&store.connection, &topology.row.operation_id).await?;
        ensure!(
            ctl.owner == topology.row.owner,
            "checkpoint operation owner mismatch"
        );
        validate_operation_state(store, &topology.row, &ctl).await?;
        let scan = match OriginScan::new(store, topology.row, topology.ownership).await? {
            OriginRead::Legacy(scan) => scan,
            OriginRead::Permanent(topology) => {
                // Adopting already verified evidence cannot erase a Stop/resume
                // observed during this preparation's control boundary.
                ensure!(
                    control(&store.connection, &topology.row.operation_id).await? == ctl,
                    "proof operation changed during preparation"
                );
                return Ok(NodeWork::Read(
                    PermanentRead::new_in_graph(store, topology, reachable).await?,
                ));
            }
        };
        Ok(NodeWork::Prepare(Self {
            scan,
            control: ctl,
            aliases: VecDeque::new(),
            events: VecDeque::new(),
            scanning: true,
            metadata_readback: MetadataReadback::default(),
            import_after: -1,
            import_count: 0,
            import_digest: Sha256::new(),
            imports_done: false,
            done: false,
            graph_validated: false,
            previous_ready: false,
            dependency_after: None,
            dependencies_ready: false,
        }))
    }
    pub async fn step(&mut self, store: &CrudStore) -> Result<()> {
        if self.done {
            return Ok(());
        }
        if self.scanning {
            if !self.scan.done() {
                if let Some(proof) = self.scan.step(store).await? {
                    stage_import(store, &proof).await?;
                }
                return Ok(());
            }
            self.aliases = self.scan.metadata.aliases.iter().cloned().collect();
            self.events = self.scan.metadata.events.iter().cloned().collect();
            self.scanning = false;
        }
        if let Some(alias) = self.aliases.front() {
            stage_alias(store, &self.scan.row.id, alias).await?;
            self.aliases.pop_front();
            return Ok(());
        }
        if let Some(event) = self.events.front() {
            stage_event(store, &self.scan.row.id, event).await?;
            self.events.pop_front();
            return Ok(());
        }
        if !self.metadata_readback.done {
            self.metadata_readback
                .step(store, &self.scan.row.id)
                .await?;
            return Ok(());
        }
        ensure!(
            self.metadata_readback.metadata == self.scan.metadata,
            "selected metadata exact readback conflict"
        );
        if !self.imports_done {
            if let Some(proof) = read_import(store, &self.scan.row.id, self.import_after).await? {
                self.import_after = proof.import_ordinal;
                self.import_count += 1;
                ensure!(
                    self.import_count <= self.scan.selected_import_count,
                    "extra selected import rows"
                );
                digest(&mut self.import_digest, &import_identity(&proof)?);
                return Ok(());
            }
            ensure!(
                self.import_count == self.scan.selected_import_count
                    && hex::encode(self.import_digest.clone().finalize())
                        == self.scan.selected_digest(),
                "selected imports exact readback conflict"
            );
            self.imports_done = true;
            return Ok(());
        }
        ensure!(
            self.graph_validated,
            "checkpoint full graph was not validated before seal"
        );
        if !self.dependencies_ready {
            self.dependency_step(store).await?;
            return Ok(());
        }
        self.seal(store).await?;
        self.done = true;
        // Release the pin immediately, including an idle completed worker job.
        self.scan.hold.take();
        Ok(())
    }
    async fn dependency_step(&mut self, store: &CrudStore) -> Result<()> {
        let row = &self.scan.row;
        #[cfg(test)]
        super::compaction_runner::record_proof_preparation_check(&row.operation_id, false);
        if !self.previous_ready {
            if let Some(previous) = &row.previous {
                ensure!(store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "SELECT 1 FROM compaction_checkpoint WHERE id=?1 AND owner=?2 AND proof_version=1 LIMIT 1",
                    [previous.clone().into(),row.owner.clone().into()])).await?.is_some(),"previous proof is not ready");
            }
            self.previous_ready = true;
            return Ok(());
        }
        use std::ops::Bound::{Excluded, Unbounded};
        let next = if let Some(after) = &self.dependency_after {
            self.scan
                .ownership
                .range((Excluded(after), Unbounded))
                .next()
        } else {
            self.scan.ownership.iter().next()
        };
        if let Some((source, _)) = next {
            if source.scope.starts_with("checkpoint:") {
                ensure!(store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "SELECT 1 FROM compaction_checkpoint WHERE id=?1 AND 'checkpoint:'||owner=?2 AND identity_sha256=?3 AND proof_version=1 LIMIT 1",
                    [source.id.clone().into(),source.scope.clone().into(),source.version.clone().into()])).await?.is_some(),"foreign proof is not ready");
            }
            self.dependency_after = Some(source.clone());
        } else {
            self.dependencies_ready = true;
        }
        Ok(())
    }
    async fn seal(&self, store: &CrudStore) -> Result<()> {
        let row = &self.scan.row;
        ensure!(
            self.dependencies_ready
                && self.graph_validated
                && self.imports_done
                && self.metadata_readback.done,
            "proof seal preparation incomplete"
        );
        #[cfg(any(test, feature = "test-support"))]
        super::compaction_runner::pause_publication_test_hook(
            store.connection.runtime_identity(),
            &row.operation_id,
            super::compaction_runner::PublicationTestPause::ProofSeal,
        )
        .await;
        store.run_serialized_write(||async {
            #[cfg(test)]
            let _writer=super::compaction_runner::PublicationWriterTestGuard::enter_seal(&row.operation_id);
            let tx=store.connection.begin().await?;
            ensure!(control(&tx,&row.operation_id).await?==self.control,"proof operation changed during preparation");
            let mut current=compaction::checkpoint_row(&tx,&row.id).await?.ok_or_else(||anyhow::anyhow!("proof candidate disappeared"))?;
            ensure!(matches!(current.proof_version,0|1),"unsupported proof version at seal");
            current.proof_version=row.proof_version;
            ensure!(current==*row,"proof identity/status/binding changed");
            ensure!(current.coverage_closed==1,"proof portion lost its immutable boundary");
            if let Some(header)=&self.scan.header {
                let exact=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_frozen_history h WHERE h.id=?1 AND workspace_id=?2 AND owner_thread=?3 AND h.identity_sha256=?4 AND message_count=?5 AND h.imports_sha256=?6 AND h.import_count=?7 AND ready=1 AND expired=0 AND next_ordinal=message_count AND next_import=import_count AND EXISTS(SELECT 1 FROM compaction_operation_projection b WHERE b.operation_id=?8 AND b.manifest_id=h.id AND b.identity_sha256=h.identity_sha256 AND b.imports_sha256=h.imports_sha256 AND b.import_count=h.import_count) LIMIT 1",[header.id.clone().into(),header.workspace_id.clone().into(),header.owner_thread.clone().into(),header.identity_sha256.clone().into(),header.message_count.into(),header.imports_sha256.clone().into(),header.import_count.into(),row.operation_id.clone().into()])).await?;
                ensure!(exact.is_some(),"proof origin changed or became unavailable");
                ensure!(tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_runner_plan WHERE operation_id=?1 AND ready=1 LIMIT 1",[row.operation_id.clone().into()])).await?.is_some(),"proof origin plan lost readiness");
            }
            // Coverage/ownership/binding/identity are immutable behind the
            // closed portion. Staging writers add only exact selected values;
            // cleanup requires expired origin + no reader/active operation.
            // Every checked dependency has a permanent incoming edge from this
            // frozen node, which forbids demotion even while our marker is 0.
            // Consequently no set scan or per-dependency query belongs here.
            tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE compaction_checkpoint SET proof_version=1 WHERE id=?1 AND proof_version=0",[row.id.clone().into()])).await?;
            tx.commit().await?;Ok(())
        }).await
    }
}

fn validate_metadata(metadata: &Metadata, ownership: &Ownership) -> Result<()> {
    for alias in &metadata.aliases {
        ensure!(
            !alias.covered_thread.is_empty()
                && !alias.covered_scope.is_empty()
                && !alias.covered_id.is_empty()
                && !alias.covered_version.is_empty()
                && !alias.replay_thread.is_empty()
                && !alias.replay_scope.is_empty()
                && !alias.replay_id.is_empty()
                && !alias.replay_version.is_empty(),
            "malformed permanent replay identity"
        );
    }
    for event in &metadata.events {
        let source = SourceRef {
            scope: event.source_scope.clone(),
            id: event.source_id.clone(),
            version: event.source_version.clone(),
        };
        ensure!(
            source.scope.starts_with("event:")
                && ownership.get(&source).is_some_and(
                    |threads| threads.len() == 1 && threads.contains(&event.source_thread)
                )
                && matches!(
                    event.role.as_str(),
                    "authoritative" | "deleted" | "input_copy"
                ),
            "permanent event evidence is outside exact coverage"
        );
    }
    Ok(())
}

async fn validate_permanent_import(
    store: &CrudStore,
    row: &CheckpointEdgesRow,
    ownership: &Ownership,
    proof: &import::Model,
) -> Result<()> {
    ensure!(
        proof.checkpoint_id == row.id
            && proof.import_ordinal >= 0
            && proof.message_ordinal >= 0
            && !proof.context_thread.is_empty()
            && !proof.target_source_thread.is_empty(),
        "invalid permanent import placement"
    );
    let origin = row
        .manifest_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("raw checkpoint has unexpected import proofs"))?;
    // Tombstone identity/counts are retained. No expired origin row is read.
    let header=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT owner_thread,message_count,import_count FROM compaction_frozen_history WHERE id=?1 AND workspace_id=?2 LIMIT 1",[origin.clone().into(),row.workspace_id.clone().into()])).await?.ok_or_else(||anyhow::anyhow!("permanent proof origin identity lost"))?;
    let owner: String = header.try_get("", "owner_thread")?;
    ensure!(
        proof.message_ordinal < header.try_get::<i64>("", "message_count")?
            && proof.import_ordinal < header.try_get::<i64>("", "import_count")?
            && (proof.context_thread == owner || proof.context_thread == row.thread_id),
        "permanent import origin bounds/context mismatch"
    );
    let accepted: FrozenImportRecord = serde_json::from_str(&proof.proof_json)?;
    ensure!(
        accepted.message_ordinal == u64::try_from(proof.message_ordinal)?
            && !accepted.source_thread.is_empty()
            && !accepted.source.scope.is_empty()
            && !accepted.source.id.is_empty()
            && !accepted.source.version.is_empty()
            && !accepted.delivery_id.is_empty()
            && !accepted.candidate_id.is_empty()
            && !accepted.output_manifest.is_empty()
            && accepted.acknowledgement.scope.starts_with("event:")
            && !accepted.acknowledgement.id.is_empty()
            && accepted
                .acknowledgement
                .version
                .strip_prefix("event-revision:")
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|v| v > 0),
        "malformed permanent original acceptance"
    );
    let target = if let Some(json) = &proof.target_checkpoint_json {
        let target: SourceRef = serde_json::from_str(json)?;
        ensure!(
            target.scope.starts_with("checkpoint:")
                && !target.id.is_empty()
                && !target.version.is_empty(),
            "malformed permanent checkpoint target"
        );
        target
    } else {
        ensure!(
            proof.target_source_thread == accepted.source_thread,
            "permanent direct import thread mismatch"
        );
        accepted.source.clone()
    };
    ensure!(
        ownership.get(&target).is_some_and(
            |threads| threads.len() == 1 && threads.contains(&proof.target_source_thread)
        ),
        "permanent import target is not exact selected coverage"
    );
    Ok(())
}

struct PermanentRead {
    layout: Option<crate::frozen_lifetime::LayoutScan>,
    raw: Option<RawAdmissionRead>,
    row: CheckpointEdgesRow,
    ownership: Ownership,
    hold: Option<FrozenReadHold>,
    metadata: MetadataReadback,
    after: i64,
    done: bool,
}
impl PermanentRead {
    async fn new(store: &CrudStore, topology: compaction::CheckpointTopology) -> Result<Self> {
        let reachable = published_reachable(store, &topology.row).await?;
        Self::new_in_graph(store, topology, reachable).await
    }
    async fn new_in_graph(
        store: &CrudStore,
        topology: compaction::CheckpointTopology,
        reachable: bool,
    ) -> Result<Self> {
        let row = topology.row;
        ensure!(row.proof_version == 1, "checkpoint proofs are not ready");
        let public = compaction::compaction_checkpoint_source(
            &store.connection,
            &row.workspace_id,
            &row.thread_id,
            &row.id,
        )
        .await?
        .is_some();
        let mut raw = None;
        let hold = if !public && !reachable {
            if let Some(manifest) = &row.manifest_id {
                let (descriptor, _) = super::compaction_source_projection::projection_identity(
                    &store.connection,
                    &row.operation_id,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("candidate bound input missing"))?;
                ensure!(
                    &descriptor.manifest_id == manifest,
                    "candidate bound input changed"
                );
                Some(
                    store
                        .acquire_frozen_header(&row.workspace_id, &descriptor)
                        .await?
                        .1,
                )
            } else {
                raw = Some(RawAdmissionRead::new(store, &row.operation_id).await?);
                None
            }
        } else {
            None
        };
        let layout = if hold.is_some() {
            let (_, h) = super::compaction_source_projection::projection_identity(
                &store.connection,
                &row.operation_id,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("candidate origin missing"))?;
            Some(crate::frozen_lifetime::LayoutScan::new(h, 0, 2)?)
        } else {
            None
        };
        Ok(Self {
            layout,
            raw,
            row,
            ownership: topology.ownership,
            hold,
            metadata: MetadataReadback::default(),
            after: -1,
            done: false,
        })
    }
    async fn step(&mut self, store: &CrudStore) -> Result<()> {
        if let Some(raw) = self.raw.as_mut() {
            raw.step(store).await?;
            if raw.done {
                let raw = self.raw.take().expect("complete raw admission");
                let owner = raw.owner.clone();
                raw_contract(&raw.finish()?, &owner, &self.row, &self.ownership)?;
            }
            return Ok(());
        }
        if let Some(layout) = self.layout.as_mut() {
            layout.step(store).await?;
            if layout.done() {
                self.layout = None;
            }
            return Ok(());
        }

        if self.done {
            return Ok(());
        }
        if !self.metadata.done {
            self.metadata.step(store, &self.row.id).await?;
            return Ok(());
        }
        validate_metadata(&self.metadata.metadata, &self.ownership)?;
        if let Some(proof) = read_import(store, &self.row.id, self.after).await? {
            validate_permanent_import(store, &self.row, &self.ownership, &proof).await?;
            self.after = proof.import_ordinal;
            return Ok(());
        }
        // A reset is only permitted for an unused terminal candidate after
        // expiry. Such a reader pins the origin, so reset cannot cross it.
        let ready=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_checkpoint WHERE id=?1 AND proof_version=1 AND identity_sha256=?2 LIMIT 1",[self.row.id.clone().into(),self.row.identity_sha256.clone().into()])).await?;
        ensure!(
            ready.is_some(),
            "checkpoint proof readiness changed during read"
        );
        self.done = true;
        self.hold.take();
        Ok(())
    }
}
pub(super) async fn permanent_metadata(
    store: &CrudStore,
    row: CheckpointEdgesRow,
    ownership: Ownership,
) -> Result<Metadata> {
    let mut read =
        PermanentRead::new(store, compaction::CheckpointTopology { row, ownership }).await?;
    while !read.done {
        read.step(store).await?;
    }
    Ok(read.metadata.metadata)
}

/// Postorder preparation retains full previous/foreign traversal. Frames carry
/// exact expected ownership and identity, so sharing a node cannot weaken scope.
pub(super) struct GraphPreparation {
    pending: Vec<Frame>,
    active: BTreeSet<String>,
    done: BTreeSet<String>,
    current: Option<NodeWork>,
    workspace: Option<String>,
    root: String,
    evidence: BTreeMap<String, GraphEvidence>,
    validation: Option<CachedGraphValidation>,
    reachable: BTreeSet<String>,
    topology: Option<(Frame, compaction::CheckpointTopologyRead)>,
}
struct Frame {
    id: String,
    expected: Option<(String, String, Option<SourceRef>)>,
    exit: bool,
}
enum NodeWork {
    Identity(CheckpointIdentityRead),
    Prepare(NodePreparation),
    Read(PermanentRead),
}
impl GraphPreparation {
    pub fn new(root: &str) -> Self {
        Self {
            pending: vec![Frame {
                id: root.into(),
                expected: None,
                exit: false,
            }],
            active: BTreeSet::new(),
            done: BTreeSet::new(),
            current: None,
            workspace: None,
            root: root.into(),
            evidence: BTreeMap::new(),
            validation: None,
            reachable: BTreeSet::new(),
            topology: None,
        }
    }
    pub fn finished(&self) -> bool {
        self.current.is_none() && self.topology.is_none() && self.pending.is_empty()
    }
    pub async fn step(&mut self, store: &CrudStore) -> Result<()> {
        if let Some(current) = &mut self.current {
            if let NodeWork::Identity(identity) = current {
                identity.step(store).await?;
                if identity.done {
                    let topology = identity.topology.clone();
                    let reachable = self.reachable.contains(&topology.row.id);
                    *current = NodePreparation::new(store, topology, reachable).await?;
                }
                return Ok(());
            }

            if let NodeWork::Prepare(node) = current
                && node.imports_done
                && !node.graph_validated
            {
                if self.validation.is_none() {
                    self.validation = Some(CachedGraphValidation::new(GraphEvidence::from_scan(
                        &node.scan,
                    )));
                }
                let validation = self.validation.as_mut().expect("graph validation");
                validation.step(&self.evidence)?;
                if !validation.finished {
                    return Ok(());
                }
                ensure!(
                    node.scan.row.id != self.root || !validation.leaves.is_empty(),
                    "checkpoint has no historical coverage"
                );
                self.validation = None;
                node.graph_validated = true;
                return Ok(());
            }
            let (id, done) = match current {
                NodeWork::Identity(_) => unreachable!(),
                NodeWork::Prepare(node) => {
                    node.step(store).await?;
                    (node.scan.row.id.clone(), node.done)
                }
                NodeWork::Read(read) => {
                    read.step(store).await?;
                    (read.row.id.clone(), read.done)
                }
            };
            if done {
                let evidence = match current {
                    NodeWork::Identity(_) => unreachable!(),
                    NodeWork::Prepare(node) => GraphEvidence::from_scan(&node.scan),
                    NodeWork::Read(read) => GraphEvidence {
                        row: read.row.clone(),
                        ownership: read.ownership.clone(),
                        metadata: read.metadata.metadata.clone(),
                    },
                };
                if matches!(current, NodeWork::Read(_)) {
                    if self.validation.is_none() {
                        self.validation = Some(CachedGraphValidation::new(evidence.clone()));
                    }
                    let validation = self.validation.as_mut().expect("graph validation");
                    validation.step(&self.evidence)?;
                    if !validation.finished {
                        return Ok(());
                    }
                    ensure!(
                        id != self.root || !validation.leaves.is_empty(),
                        "checkpoint has no historical coverage"
                    );
                    self.validation = None;
                }
                self.evidence.insert(id.clone(), evidence);
                self.active.remove(&id);
                self.done.insert(id);
                self.current = None;
            }
            return Ok(());
        }
        if self.topology.is_none() {
            let Some(frame) = self.pending.pop() else {
                return Ok(());
            };
            freeze_checkpoint(store, &frame.id).await?;
            let read = compaction::CheckpointTopologyRead::new(&store.connection, &frame.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("checkpoint preparation graph node missing"))?;
            self.topology = Some((frame, read));
            return Ok(());
        }
        let (_, read) = self.topology.as_mut().expect("topology preparation");
        read.step(&store.connection).await?;
        if !read.done {
            return Ok(());
        }
        let (frame, read) = self.topology.take().expect("completed topology");
        let topology = read.finish()?;
        let row = &topology.row;
        ensure!(
            row.format_version == 1 && matches!(row.proof_version, 0 | 1),
            "unsupported checkpoint format/proofs"
        );
        if let Some(workspace) = &self.workspace {
            ensure!(
                &row.workspace_id == workspace,
                "checkpoint preparation workspace mismatch"
            )
        } else {
            self.workspace = Some(row.workspace_id.clone());
        }
        if let Some((owner, thread, source)) = &frame.expected {
            ensure!(
                &row.owner == owner && &row.thread_id == thread,
                "checkpoint preparation ownership mismatch"
            );
            if let Some(source) = source {
                ensure!(
                    source.version == row.identity_sha256
                        && source.scope == format!("checkpoint:{}", row.owner)
                        && compaction::compaction_checkpoint_source(
                            &store.connection,
                            &row.workspace_id,
                            thread,
                            &row.id
                        )
                        .await?
                        .as_ref()
                            == Some(source),
                    "checkpoint dependency is not exact published source"
                );
            }
        }
        if compaction::compaction_checkpoint_source(
            &store.connection,
            &row.workspace_id,
            &row.thread_id,
            &row.id,
        )
        .await?
        .is_some()
        {
            self.reachable.insert(row.id.clone());
        }
        if self.reachable.contains(&row.id) {
            if let Some(previous) = &row.previous {
                self.reachable.insert(previous.clone());
            }
            self.reachable.extend(
                topology
                    .ownership
                    .keys()
                    .filter(|s| s.scope.starts_with("checkpoint:"))
                    .map(|s| s.id.clone()),
            );
        }
        if frame.exit {
            self.current = Some(match row.proof_version {
                0 => NodeWork::Identity(CheckpointIdentityRead::new(store, topology).await?),
                1 => {
                    let reachable = self.reachable.contains(&row.id);
                    NodeWork::Read(PermanentRead::new_in_graph(store, topology, reachable).await?)
                }
                _ => unreachable!(),
            });
            return Ok(());
        }
        if self.done.contains(&frame.id) {
            return Ok(());
        }
        ensure!(
            self.active.insert(frame.id.clone()),
            "cyclic checkpoint preparation graph"
        );
        ensure!(
            self.active.len() + self.done.len() <= 65_536,
            "checkpoint preparation graph exceeds bound"
        );
        self.pending.push(Frame {
            id: frame.id,
            expected: frame.expected,
            exit: true,
        });
        if let Some(previous) = &row.previous {
            self.pending.push(Frame {
                id: previous.clone(),
                expected: Some((row.owner.clone(), row.thread_id.clone(), None)),
                exit: false,
            });
        }
        for (source, threads) in topology.ownership {
            if let Some(owner) = source.scope.strip_prefix("checkpoint:") {
                self.pending.push(Frame {
                    id: source.id.clone(),
                    expected: Some((
                        owner.into(),
                        threads.into_iter().next().expect("exact owner"),
                        Some(source),
                    )),
                    exit: false,
                });
            }
        }
        Ok(())
    }
}
pub(super) async fn prepare_graph(store: &CrudStore, root: &str) -> Result<()> {
    // Postorder preparation validates the same complete previous/foreign graph
    // and alias policy; it does not rescan each available origin in a preflight.
    let mut graph = GraphPreparation::new(root);
    while !graph.finished() {
        graph.step(store).await?;
    }
    ensure!(
        graph.done.contains(&graph.root),
        "checkpoint preparation did not reach root"
    );
    Ok(())
}

/// Only new, explicitly admitted raw candidates may establish ownership from
/// current exact assertions. Legacy reconciliation never reconstructs it.
pub(super) async fn prepare_raw_ownership(
    store: &CrudStore,
    checkpoint: &pioneer_compaction::Checkpoint,
) -> Result<Option<Vec<(i64, SourceRef, String)>>> {
    if super::compaction_source_projection::projection_identity(
        &store.connection,
        &checkpoint.operation_id,
    )
    .await?
    .is_some()
    {
        return Ok(None);
    }
    let mut admission = RawAdmissionRead::new(store, &checkpoint.operation_id).await?;
    while !admission.done {
        admission.step(store).await?;
    }
    let owner = admission.owner.clone();
    let original = admission.finish()?;
    ensure!(
        owner == checkpoint.owner
            && original.id == checkpoint.operation_id
            && original.owner == checkpoint.owner
            && !original.plan.compact.is_empty()
            && !original.plan.coverage.is_empty(),
        "raw candidate has no original assertion admission"
    );
    let context = store
        .connection
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT workspace_id FROM compaction_context WHERE owner=?1 LIMIT 1",
            [checkpoint.owner.clone().into()],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("raw context missing"))?;
    let workspace: String = context.try_get("", "workspace_id")?;
    let mut original_ordinals = BTreeMap::new();
    for (ordinal, source) in original.plan.coverage.iter().enumerate() {
        original_ordinals.entry(source).or_insert(ordinal);
    }
    let mut rows = Vec::new();
    for source in &checkpoint.coverage {
        let ordinal = *original_ordinals
            .get(source)
            .ok_or_else(|| anyhow::anyhow!("raw candidate exceeds original admission"))?;
        let thread = compaction::compaction_reference_thread(&store.connection, &workspace, source)
            .await?
            .ok_or_else(|| anyhow::anyhow!("raw candidate assertion ownership unavailable"))?;
        rows.push((i64::try_from(ordinal)?, source.clone(), thread));
    }
    Ok(Some(rows))
}
pub(super) async fn write_raw_ownership<C: ConnectionTrait>(
    db: &C,
    checkpoint: &pioneer_compaction::Checkpoint,
    rows: &[(i64, SourceRef, String)],
) -> Result<()> {
    ensure!(
        db.query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT 1 FROM compaction_operation_projection WHERE operation_id=?1 LIMIT 1",
            [checkpoint.operation_id.clone().into()]
        ))
        .await?
        .is_none(),
        "raw admission unexpectedly acquired a projection"
    );
    let context = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT workspace_id FROM compaction_context WHERE owner=?1 LIMIT 1",
            [checkpoint.owner.clone().into()],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("raw context lost"))?;
    let workspace: String = context.try_get("", "workspace_id")?;
    for (ordinal, source, thread) in rows {
        ensure!(
            compaction::compaction_reference_thread(db, &workspace, source)
                .await?
                .as_ref()
                == Some(thread),
            "raw assertion ownership changed"
        );
        let values = vec![
            checkpoint.operation_id.clone().into(),
            (*ordinal).into(),
            thread.clone().into(),
            source.scope.clone().into(),
            source.id.clone().into(),
            source.version.clone().into(),
        ];
        if db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_manifest WHERE operation_id=?1 AND ordinal=?2 AND reference_only=0 AND source_thread=?3 AND source_scope=?4 AND source_id=?5 AND source_version=?6 LIMIT 1",values.clone())).await?.is_some() { continue; }
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES (?1,?2,?2,0,?3,?4,?5,?6) ON CONFLICT DO NOTHING",values.clone())).await?;
        ensure!(db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_manifest WHERE operation_id=?1 AND ordinal=?2 AND reference_only=0 AND source_thread=?3 AND source_scope=?4 AND source_id=?5 AND source_version=?6 LIMIT 1",values)).await?.is_some(),"raw historical ownership conflict");
    }
    Ok(())
}

/// Coverage cannot be inferred from a summary. Recreate the exact immutable
/// identity with bounded reads, releasing reader capacity before decode/hash.
struct CheckpointIdentityRead {
    topology: compaction::CheckpointTopology,
    selection: String,
    projection_version: i64,
    size: i64,
    bytes: Vec<u8>,
    done: bool,
}
impl CheckpointIdentityRead {
    async fn new(store: &CrudStore, topology: compaction::CheckpointTopology) -> Result<Self> {
        let meta=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT projection_version,selection,length(CAST(summary AS BLOB)) AS size FROM compaction_checkpoint WHERE id=?1 AND length(CAST(selection AS BLOB))<=?2 LIMIT 1",[topology.row.id.clone().into(),i64::try_from(SOURCE_PAGE_BYTES)?.into()])).await?.ok_or_else(||anyhow::anyhow!("checkpoint identity metadata missing or oversized"))?;
        let size: i64 = meta.try_get("", "size")?;
        ensure!(
            (1..=13_107 * 128).contains(&size),
            "checkpoint summary size invalid"
        );
        Ok(Self {
            topology,
            selection: meta.try_get("", "selection")?,
            projection_version: meta.try_get("", "projection_version")?,
            size,
            bytes: Vec::new(),
            done: false,
        })
    }
    async fn step(&mut self, store: &CrudStore) -> Result<()> {
        if self.done {
            return Ok(());
        }
        let row = &self.topology.row;
        if (self.bytes.len() as i64) < self.size {
            let page=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT substr(CAST(summary AS BLOB),?2,?3) AS fragment FROM compaction_checkpoint WHERE id=?1 AND identity_sha256=?4 AND length(CAST(summary AS BLOB))=?5 LIMIT 1",[row.id.clone().into(),(self.bytes.len() as i64+1).into(),i64::try_from(SOURCE_PAGE_BYTES)?.into(),row.identity_sha256.clone().into(),self.size.into()])).await?.ok_or_else(||anyhow::anyhow!("checkpoint summary identity changed"))?;
            let fragment: Vec<u8> = page.try_get("", "fragment")?;
            ensure!(
                !fragment.is_empty() && fragment.len() <= SOURCE_PAGE_BYTES,
                "checkpoint summary fragment missing"
            );
            self.bytes.extend(fragment);
            return Ok(());
        }
        ensure!(
            self.bytes.len() as i64 == self.size,
            "checkpoint summary size mismatch"
        );
        let checkpoint = pioneer_compaction::Checkpoint {
            id: row.id.clone(),
            operation_id: row.operation_id.clone(),
            owner: row.owner.clone(),
            previous: row.previous.clone(),
            summary: String::from_utf8(std::mem::take(&mut self.bytes))?,
            selection: serde_json::from_str(&self.selection)?,
            coverage: self.topology.ownership.keys().cloned().collect(),
            projection_version: u64::try_from(self.projection_version)?,
            format_version: u32::try_from(row.format_version)?,
        };
        ensure!(
            compaction::checkpoint_identity(&checkpoint)? == row.identity_sha256,
            "checkpoint exact coverage/identity mismatch"
        );
        self.done = true;
        Ok(())
    }
}

#[derive(Clone)]
struct GraphEvidence {
    row: CheckpointEdgesRow,
    ownership: Ownership,
    metadata: Metadata,
}
impl GraphEvidence {
    fn from_scan(scan: &OriginScan) -> Self {
        Self {
            row: scan.row.clone(),
            ownership: scan.ownership.clone(),
            metadata: scan.metadata.clone(),
        }
    }
}
struct CachedGraphValidation {
    current: GraphEvidence,
    pending: Vec<(String, bool)>,
    active: BTreeSet<String>,
    done: BTreeSet<String>,
    leaves: BTreeSet<pioneer_compaction::frozen::ScopedReplaySource>,
    aliases: pioneer_compaction::frozen::ReplayAliasGraph,
    roles: BTreeMap<(String, String, String, String), String>,
    target_after: Option<pioneer_compaction::frozen::ScopedReplaySource>,
    finished: bool,
}
impl CachedGraphValidation {
    fn new(current: GraphEvidence) -> Self {
        Self {
            pending: vec![(current.row.id.clone(), false)],
            current,
            active: BTreeSet::new(),
            done: BTreeSet::new(),
            leaves: BTreeSet::new(),
            aliases: Default::default(),
            roles: BTreeMap::new(),
            target_after: None,
            finished: false,
        }
    }
    fn step(&mut self, cache: &BTreeMap<String, GraphEvidence>) -> Result<()> {
        use pioneer_compaction::frozen::ScopedReplaySource;
        if self.finished {
            return Ok(());
        }
        if let Some((id, exit)) = self.pending.pop() {
            if exit {
                self.active.remove(&id);
                self.done.insert(id);
                return Ok(());
            }
            if self.done.contains(&id) {
                return Ok(());
            }
            ensure!(
                self.active.insert(id.clone()),
                "cyclic prepared checkpoint graph"
            );
            ensure!(
                self.active.len() + self.done.len() <= 65_536,
                "prepared checkpoint graph exceeds bound"
            );
            let node = if id == self.current.row.id {
                &self.current
            } else {
                cache
                    .get(&id)
                    .ok_or_else(|| anyhow::anyhow!("prepared dependency graph missing"))?
            };
            ensure!(
                node.row.workspace_id == self.current.row.workspace_id
                    && node.row.format_version == 1,
                "prepared graph scope/format mismatch"
            );
            validate_metadata(&node.metadata, &node.ownership)?;
            for alias in &node.metadata.aliases {
                self.aliases.insert(
                    ScopedReplaySource {
                        thread: alias.replay_thread.clone(),
                        source: SourceRef {
                            scope: alias.replay_scope.clone(),
                            id: alias.replay_id.clone(),
                            version: alias.replay_version.clone(),
                        },
                    },
                    ScopedReplaySource {
                        thread: alias.covered_thread.clone(),
                        source: SourceRef {
                            scope: alias.covered_scope.clone(),
                            id: alias.covered_id.clone(),
                            version: alias.covered_version.clone(),
                        },
                    },
                    alias.tool_item_id.as_deref(),
                )?;
            }
            for event in &node.metadata.events {
                let key = (
                    event.source_thread.clone(),
                    event.source_scope.clone(),
                    event.source_id.clone(),
                    event.source_version.clone(),
                );
                ensure!(
                    self.roles
                        .insert(key, event.role.clone())
                        .is_none_or(|old| old == event.role),
                    "conflicting prepared historical event roles"
                );
            }
            self.pending.push((id, true));
            if let Some(previous) = &node.row.previous {
                let previous_node = cache
                    .get(previous)
                    .ok_or_else(|| anyhow::anyhow!("prepared previous graph missing"))?;
                ensure!(
                    previous_node.row.owner == node.row.owner
                        && previous_node.row.thread_id == node.row.thread_id,
                    "prepared previous ownership mismatch"
                );
                self.pending.push((previous.clone(), false));
            }
            for (source, threads) in &node.ownership {
                let thread = threads
                    .iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("prepared ownership missing"))?;
                if source.scope.starts_with("checkpoint:") {
                    let foreign = cache
                        .get(&source.id)
                        .ok_or_else(|| anyhow::anyhow!("prepared foreign dependency missing"))?;
                    ensure!(
                        foreign.row.thread_id == *thread
                            && source.scope == format!("checkpoint:{}", foreign.row.owner)
                            && source.version == foreign.row.identity_sha256,
                        "prepared foreign identity mismatch"
                    );
                    self.pending.push((source.id.clone(), false));
                } else {
                    self.leaves.insert(ScopedReplaySource {
                        thread: thread.clone(),
                        source: source.clone(),
                    });
                }
            }
            return Ok(());
        }
        let (after, finished) = self
            .aliases
            .validate_targets_page(&self.leaves, self.target_after.as_ref())?;
        self.target_after = after;
        self.finished = finished;
        Ok(())
    }
}
#[cfg(test)]
fn validate_cached_graph(
    root: &str,
    current: &GraphEvidence,
    cache: &BTreeMap<String, GraphEvidence>,
) -> Result<bool> {
    ensure!(root == current.row.id, "wrong graph root");
    let mut validation = CachedGraphValidation::new(current.clone());
    while !validation.finished {
        validation.step(cache)?;
    }
    Ok(!validation.leaves.is_empty())
}

/// Local status is not reachability: a legacy intermediate node may retain
/// any status while an applied root still reaches it through immutable edges.
/// Reverse discovery uses the existing previous and exact coverage indexes.
async fn published_reachable(store: &CrudStore, initial: &CheckpointEdgesRow) -> Result<bool> {
    let mut pending = vec![initial.id.clone()];
    let mut visited = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id.clone()) {
            continue;
        }
        ensure!(
            visited.len() <= 65_536,
            "reverse checkpoint graph exceeds bound"
        );
        let node = compaction::checkpoint_row(&store.connection, &id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("reverse checkpoint node missing"))?;
        ensure!(
            node.workspace_id == initial.workspace_id,
            "reverse checkpoint workspace mismatch"
        );
        if compaction::compaction_checkpoint_source(
            &store.connection,
            &initial.workspace_id,
            &node.thread_id,
            &id,
        )
        .await?
        .is_some()
        {
            return Ok(true);
        }
        for foreign in [false, true] {
            let mut after = String::new();
            loop {
                let sql = if foreign {
                    "SELECT v.checkpoint_id AS id FROM compaction_coverage v JOIN compaction_checkpoint p ON p.id=v.checkpoint_id WHERE v.source_scope=?1 AND v.source_id=?2 AND v.source_version=?3 AND v.checkpoint_id>?4 AND p.proof_version=1 ORDER BY v.checkpoint_id LIMIT ?5"
                } else {
                    "SELECT id FROM compaction_checkpoint WHERE owner=?1 AND previous=?2 AND ?3 IS NOT NULL AND id>?4 AND proof_version=1 ORDER BY id LIMIT ?5"
                };
                let rows = store
                    .connection
                    .query_all_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        sql,
                        [
                            if foreign {
                                format!("checkpoint:{}", node.owner).into()
                            } else {
                                node.owner.clone().into()
                            },
                            id.clone().into(),
                            node.identity_sha256.clone().into(),
                            after.clone().into(),
                            SOURCE_PAGE_ROWS.into(),
                        ],
                    ))
                    .await?;
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    after = row.try_get("", "id")?;
                    ensure!(
                        pending.len() + visited.len() < 65_536,
                        "reverse checkpoint pending bound exceeded"
                    );
                    let incoming = compaction::checkpoint_row(&store.connection, &after)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("incoming checkpoint disappeared"))?;
                    ensure!(
                        incoming.workspace_id == initial.workspace_id
                            && incoming.format_version == 1
                            && incoming.proof_version == 1,
                        "incoming checkpoint scope/readiness mismatch"
                    );
                    if foreign {
                        let source = SourceRef {
                            scope: format!("checkpoint:{}", node.owner),
                            id: id.clone(),
                            version: node.identity_sha256.clone(),
                        };
                        ensure!(
                            compaction::checkpoint_source_owner(
                                &store.connection,
                                &incoming,
                                &source
                            )
                            .await?
                                == node.thread_id,
                            "incoming checkpoint exact ownership mismatch"
                        );
                    } else {
                        ensure!(
                            incoming.owner == node.owner
                                && incoming.thread_id == node.thread_id
                                && incoming.previous.as_deref() == Some(id.as_str()),
                            "incoming previous ownership mismatch"
                        );
                    }
                    pending.push(after.clone());
                }
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn correction_fixture() -> CrudStore {
        use migration::{Migrator, MigratorTrait};
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        for sql in [
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)",
        ] {
            db.execute_unprepared(sql).await.unwrap();
        }
        CrudStore::new(db)
    }

    #[tokio::test]
    async fn raw_admission_fragments_reject_control_change_and_restart_without_whole_snapshot_cap()
    {
        use pioneer_compaction::{
            CompactionMode, CompactionPlan, CompactionSettings, CoverageDomain, ModelSelection,
            Transport,
        };
        let store = correction_fixture().await;
        let snapshot = OperationSnapshot {
            id: "raw".into(),
            owner: "owner".into(),
            expected_checkpoint: None,
            projection_version: 0,
            source_epochs: Default::default(),
            admission: CompactionSettings::default()
                .admit(
                    &ModelSelection {
                        transport: Transport::Api,
                        instance: "fixture".into(),
                        model: "model".into(),
                        effort: None,
                    },
                    None,
                    0,
                )
                .unwrap(),
            plan: CompactionPlan {
                mode: CompactionMode::Normal,
                coverage_domain: CoverageDomain::WorkingContext,
                compact: (0..256).collect(),
                retain: vec![],
                fingerprint: "raw".into(),
                coverage: (0..256)
                    .map(|i| SourceRef {
                        scope: "event:turn".into(),
                        id: format!("{i:04}-{}", "x".repeat(2048)),
                        version: "event-revision:1".into(),
                    })
                    .collect(),
            },
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.len() > 2 * SOURCE_PAGE_BYTES);
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,next_portion) VALUES('raw','owner','raw','failed',?1,1,1)", [json.clone().into()])).await.unwrap();
        store.connection.execute_unprepared("INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('raw',7,'{}')").await.unwrap();
        // Open legacy admission: fragment identity rejects a changed length or
        // fingerprint even when scalar status/generation did not change.
        for (id, mutation) in [
            ("raw-length", "snapshot=snapshot||' '"),
            ("raw-fingerprint", "fingerprint='other'"),
        ] {
            let mut open = snapshot.clone();
            open.id = id.into();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES(?1,'owner',?1,'running',?2,1)",
                [id.into(),serde_json::to_string(&open).unwrap().into()])).await.unwrap();
            let mut pages = RawAdmissionRead::new(&store, id).await.unwrap();
            pages.step(&store).await.unwrap();
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    format!("UPDATE compaction_operation SET {mutation} WHERE id=?1"),
                    [id.into()],
                ))
                .await
                .unwrap();
            let error = pages.step(&store).await.unwrap_err();
            assert!(format!("{error:#}").contains("identity changed between fragments"));
        }
        let mut interrupted = RawAdmissionRead::new(&store, "raw").await.unwrap();
        interrupted.step(&store).await.unwrap();
        assert_eq!(interrupted.bytes.len(), SOURCE_PAGE_BYTES);
        assert!(!interrupted.done);
        // Even same-length JSON replacement is rejected for closed portions.
        assert!(
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "UPDATE compaction_operation SET snapshot=?1 WHERE id='raw'",
                    [json.replacen("0000-", "zzzz-", 1).into()]
                ))
                .await
                .is_err()
        );
        store
            .connection
            .execute_unprepared(
                "UPDATE compaction_runner_state SET generation=8 WHERE operation_id='raw'",
            )
            .await
            .unwrap();
        assert!(interrupted.step(&store).await.is_err());
        drop(interrupted); // cancellation releases only private partial bytes
        let mut restart = RawAdmissionRead::new(&store, "raw").await.unwrap();
        let mut steps = 0;
        while !restart.done {
            let before = restart.bytes.len();
            restart.step(&store).await.unwrap();
            assert!(restart.bytes.len() - before <= SOURCE_PAGE_BYTES);
            steps += 1;
        }
        assert!(steps >= 4);
        assert_eq!(
            serde_json::to_string(&restart.finish().unwrap()).unwrap(),
            json
        );
    }

    #[tokio::test]
    async fn retained_target_payload_statement_rejects_understated_direct_and_shared_bytes() {
        let store = correction_fixture().await;
        store.connection.execute_unprepared("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,ready) VALUES('output','ws','thread','output',1,1,1),('backing','ws','thread','backing',1,1,1)").await.unwrap();
        let oversized = "x".repeat(SOURCE_PAGE_BYTES + 1);
        for id in ["output", "backing"] {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES(?1,0,?2,1)", [id.into(), oversized.clone().into()])).await.unwrap();
        }
        for shared in [false, true] {
            if shared {
                for sql in [
                    "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('output',0,0,1)",
                    "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('output',0,0,1,'backing')",
                    "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id='output' AND kind=0",
                ] {
                    store.connection.execute_unprepared(sql).await.unwrap();
                }
            }
            assert!(
                store
                    .connection
                    .query_one_raw(target_reference_statement("output", 0).unwrap())
                    .await
                    .unwrap()
                    .is_none(),
                "payload SQL must return no oversized JSON"
            );
            assert!(target_reference(&store, "output", 0).await.is_err());
        }
        let n: i64 = store
            .connection
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT count(*) AS n FROM compaction_frozen_message_data".into(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(n, 2, "bounded refusal preserves both backing rows");
    }

    #[test]
    fn selected_evidence_matches_original_extractor_exact_scoped_sets() {
        use pioneer_compaction::frozen::{
            FrozenInputAliasConflict, FrozenPublicationAliases, FrozenSourceAlias,
        };
        let source = |kind: &str, id: &str| SourceRef {
            scope: format!("{kind}:turn"),
            id: id.into(),
            version: format!("revision-{id}"),
        };
        let copied = source("input", "copy");
        let make = |id: &str, role: Option<FrozenEventInputRole>| FrozenMessageRef {
            logical_turn_id: None,
            source_thread: "thread".into(),
            context_thread: None,
            unit_id: id.into(),
            sources: vec![source("event", id)],
            event_input_role: role,
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
        };
        let mut authoritative = make("event", Some(FrozenEventInputRole::Authoritative));
        authoritative.replay_source = Some(source("item", "tool-replay"));
        authoritative.tool_item_id = Some("tool".into());
        authoritative.source_aliases.push(FrozenSourceAlias {
            represented_thread: "thread".into(),
            represented_source: source("input", "covered"),
            source_thread: "thread".into(),
            source: copied.clone(),
        });
        authoritative
            .ambiguous_input_aliases
            .push(FrozenInputAliasConflict {
                source_thread: "thread".into(),
                source: copied,
            });
        let mut input_carrier = make("input-carrier", None);
        input_carrier.sources = vec![source("input", "covered")];
        input_carrier.source_aliases = std::mem::take(&mut authoritative.source_aliases);
        let deleted = make("deleted", Some(FrozenEventInputRole::Deleted));
        let input_copy = make("input-copy", Some(FrozenEventInputRole::InputCopy));
        let reference_only = make("reference-only", Some(FrozenEventInputRole::Authoritative));
        let mut inherited = make("inherited", None);
        inherited.sources = vec![source("checkpoint", "inherited")];
        // An exact empty incremental set must suppress inherited aliases.
        inherited.source_aliases = input_carrier.source_aliases.clone();
        inherited.publication_aliases = Some(FrozenPublicationAliases {
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
        });
        let mut wrong_owner = make("event", Some(FrozenEventInputRole::Deleted));
        wrong_owner.source_thread = "other-thread".into();
        let ambiguous_owner = make("ambiguous-owner", Some(FrozenEventInputRole::Deleted));
        let refs = vec![
            authoritative,
            input_carrier,
            deleted,
            input_copy,
            reference_only,
            inherited,
            wrong_owner,
            ambiguous_owner,
        ];
        let mut ownership = Ownership::new();
        for id in ["event", "deleted", "input-copy"] {
            ownership.insert(source("event", id), BTreeSet::from(["thread".into()]));
        }
        ownership.insert(
            source("input", "covered"),
            BTreeSet::from(["thread".into()]),
        );
        ownership.insert(
            source("checkpoint", "inherited"),
            BTreeSet::from(["thread".into()]),
        );
        for reference in &refs {
            reference.validate().unwrap();
        }
        ownership.insert(
            source("event", "ambiguous-owner"),
            BTreeSet::from(["thread".into(), "other-thread".into()]),
        );
        // Oracle: the original selected extraction at the fixed base, before
        // this change; compare full identities, never just cardinalities.
        let mut original = Metadata::default();
        for reference in &refs {
            let selected = reference
                .sources
                .iter()
                .filter(|source| {
                    ownership.get(*source).is_some_and(|threads| {
                        threads.len() == 1 && threads.contains(&reference.source_thread)
                    })
                })
                .collect::<Vec<_>>();
            for source in selected {
                for edge in reference.publication_edges_for(source) {
                    original.aliases.insert(Alias {
                        covered_thread: edge.covered_thread,
                        replay_thread: edge.replay_thread,
                        covered_scope: edge.covered.scope,
                        covered_id: edge.covered.id,
                        covered_version: edge.covered.version,
                        replay_scope: edge.replay.scope,
                        replay_id: edge.replay.id,
                        replay_version: edge.replay.version,
                        tool_item_id: edge.tool_item_id,
                    });
                }
            }
            if reference.sources.len() == 1
                && let Some(role) = reference.event_input_role
            {
                let source = &reference.sources[0];
                if ownership.get(source).is_some_and(|threads| {
                    threads.len() == 1 && threads.contains(&reference.source_thread)
                }) {
                    original.events.insert(Event {
                        source_thread: reference.source_thread.clone(),
                        source_scope: source.scope.clone(),
                        source_id: source.id.clone(),
                        source_version: source.version.clone(),
                        role: match role {
                            FrozenEventInputRole::Authoritative => "authoritative",
                            FrozenEventInputRole::Deleted => "deleted",
                            FrozenEventInputRole::InputCopy => "input_copy",
                        }
                        .into(),
                    });
                }
            }
        }
        let mut selected = Metadata::default();
        for reference in &refs {
            select_reference(reference, &ownership, &mut selected).unwrap();
        }
        assert_eq!(selected, original);
        assert_eq!(
            selected
                .events
                .iter()
                .map(|e| (e.source_id.as_str(), e.role.as_str()))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                ("event", "authoritative"),
                ("deleted", "deleted"),
                ("input-copy", "input_copy")
            ])
        );
        assert!(
            selected
                .aliases
                .iter()
                .any(|a| a.tool_item_id.as_deref() == Some("tool"))
        );
        assert!(
            selected
                .aliases
                .iter()
                .any(|a| a.covered_id == "copy" && a.replay_id == "copy")
        );
        let contradictory = make("event", Some(FrozenEventInputRole::Deleted));
        assert!(select_reference(&contradictory, &ownership, &mut selected).is_err());
    }

    #[test]
    fn frozen_stream_hashes_keep_their_distinct_existing_length_protocols() {
        let value = b"selected reference";
        let mut messages = Sha256::new();
        messages.update((value.len() as u64).to_be_bytes());
        messages.update(value);
        let mut imports = Sha256::new();
        digest(&mut imports, value);
        assert_ne!(
            hex::encode(messages.finalize()),
            hex::encode(imports.finalize())
        );
        let mut expected = Sha256::new();
        expected.update((value.len() as u64).to_le_bytes());
        expected.update(value);
        let mut actual = Sha256::new();
        digest(&mut actual, value);
        assert_eq!(actual.finalize(), expected.finalize());
    }
    #[test]
    fn cached_full_graph_rejects_lost_ancestry_and_foreign_owner_even_for_empty_metadata() {
        let row = CheckpointEdgesRow {
            coverage_closed: 1,
            id: "root".into(),
            proof_version: 0,
            status: "candidate".into(),
            owner: "owner".into(),
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            identity_sha256: "identity".into(),
            previous: Some("missing".into()),
            format_version: 1,
            operation_id: "operation".into(),
            manifest_id: None,
        };
        let mut graph = GraphEvidence {
            row,
            ownership: Ownership::new(),
            metadata: Metadata::default(),
        };
        assert!(validate_cached_graph("root", &graph, &BTreeMap::new()).is_err());
        graph.row.previous = None;
        let source = SourceRef {
            scope: "checkpoint:foreign-owner".into(),
            id: "foreign".into(),
            version: "foreign-identity".into(),
        };
        graph
            .ownership
            .insert(source, BTreeSet::from(["foreign-thread".into()]));
        let mut foreign = graph.clone();
        foreign.row.id = "foreign".into();
        foreign.row.owner = "wrong-owner".into();
        foreign.row.thread_id = "foreign-thread".into();
        foreign.row.identity_sha256 = "foreign-identity".into();
        foreign.ownership.clear();
        assert!(
            validate_cached_graph(
                "root",
                &graph,
                &BTreeMap::from([("foreign".into(), foreign)])
            )
            .is_err()
        );
    }
    #[test]
    fn cached_graph_traverses_deep_previous_and_rejects_cycle_after_valid_tip() {
        let mut cache = BTreeMap::new();
        for index in 0..2048 {
            let row = CheckpointEdgesRow {
                coverage_closed: 1,
                id: format!("node-{index}"),
                proof_version: 1,
                status: "retained".into(),
                owner: "owner".into(),
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                identity_sha256: format!("identity-{index}"),
                previous: (index > 0).then(|| format!("node-{}", index - 1)),
                format_version: 1,
                operation_id: "operation".into(),
                manifest_id: None,
            };
            let ownership = if index == 0 {
                BTreeMap::from([(
                    SourceRef {
                        scope: "event:turn".into(),
                        id: "leaf".into(),
                        version: "event-revision:1".into(),
                    },
                    BTreeSet::from(["thread".into()]),
                )])
            } else {
                Ownership::new()
            };
            cache.insert(
                row.id.clone(),
                GraphEvidence {
                    row,
                    ownership,
                    metadata: Metadata::default(),
                },
            );
        }
        let root = cache.remove("node-2047").unwrap();
        assert!(validate_cached_graph(&root.row.id, &root, &cache).unwrap());
        cache.get_mut("node-0").unwrap().row.previous = Some(root.row.id.clone());
        assert!(validate_cached_graph(&root.row.id, &root, &cache).is_err());
        cache.get_mut("node-0").unwrap().row.previous = None;
        cache.remove("node-4");
        assert!(validate_cached_graph(&root.row.id, &root, &cache).is_err());
    }
}

async fn validate_operation_state(
    store: &CrudStore,
    row: &CheckpointEdgesRow,
    control: &Control,
) -> Result<()> {
    use pioneer_compaction::runner::RunnerPhase;
    if let Some(generation) = control.generation {
        let saved=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT state FROM compaction_runner_state WHERE operation_id=?1 AND generation=?2 AND length(CAST(state AS BLOB))<=?3 LIMIT 1",[row.operation_id.clone().into(),generation.into(),i64::try_from(SOURCE_PAGE_BYTES)?.into()])).await?.ok_or_else(||anyhow::anyhow!("proof runner state missing or oversized"))?;
        let state: pioneer_compaction::runner::RunnerState =
            serde_json::from_str(&saved.try_get::<String>("", "state")?)?;
        ensure!(
            i64::try_from(state.generation)? == generation,
            "proof runner generation mismatch"
        );
        let consistent = match control.status.as_str() {
            "running" => !matches!(
                state.phase,
                RunnerPhase::Applied { .. } | RunnerPhase::Failed { .. }
            ),
            "completed" => matches!(state.phase, RunnerPhase::Applied { .. }),
            "failed" | "cancelled" | "stale" => matches!(state.phase, RunnerPhase::Failed { .. }),
            _ => false,
        };
        ensure!(consistent, "proof operation/runner state contradiction");
    } else {
        ensure!(
            row.manifest_id.is_none(),
            "bound checkpoint runner state missing"
        );
    }
    if row.manifest_id.is_some() {
        ensure!(store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT 1 FROM compaction_runner_plan WHERE operation_id=?1 AND ready=1 LIMIT 1",[row.operation_id.clone().into()])).await?.is_some(),"checkpoint bound plan is not ready");
    }
    Ok(())
}

/// Close the existing portion boundary before preparing a legacy candidate.
/// New candidate writers already close it in their atomic coverage write set.
/// This is an immutable portion boundary, not a revision counter: it never
/// advances on edits, and introduces no new schema/state/publication fence.
async fn freeze_checkpoint(store: &CrudStore, id: &str) -> Result<()> {
    let row = compaction::checkpoint_row(&store.connection, id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint to freeze missing"))?;
    if row.coverage_closed == 1 {
        return Ok(());
    }
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;
        let row=tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT p.portion,p.operation_id FROM compaction_checkpoint p JOIN compaction_operation o ON o.id=p.operation_id AND o.owner=p.owner WHERE p.id=?1 LIMIT 1",[id.into()])).await?
            .ok_or_else(||anyhow::anyhow!("checkpoint to freeze missing"))?;
        let portion:i64=row.try_get("","portion")?;
        ensure!(portion>=0 && portion<i64::MAX,"invalid checkpoint portion boundary");
        let operation:String=row.try_get("","operation_id")?;
        tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "UPDATE compaction_operation SET next_portion=max(next_portion,?2) WHERE id=?1 AND next_portion<?2",
            [operation.into(),(portion+1).into()])).await?;
        tx.commit().await?;Ok(())
    }).await
}

/// Admission payloads have a domain bound, not a whole-JSON page limit. Only
/// one fragment is returned per statement/worker step; decode happens after
/// reader capacity is released. Admission writers insert/reuse saved
/// snapshots. Deadline resume preserves the plan and advances control generation
/// atomically; fragment/seal fences reject that transition. Closed portions guard
/// every other snapshot change during legacy proof preparation.
struct RawAdmissionRead {
    id: String,
    owner: String,
    fingerprint: String,
    control: Control,
    size: i64,
    bytes: Vec<u8>,
    done: bool,
}
impl RawAdmissionRead {
    async fn new(store: &CrudStore, id: &str) -> Result<Self> {
        let row=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT owner,fingerprint,length(CAST(snapshot AS BLOB)) AS size FROM compaction_operation WHERE id=?1 LIMIT 1",[id.into()])).await?
            .ok_or_else(||anyhow::anyhow!("original raw admission missing"))?;
        let size: i64 = row.try_get("", "size")?;
        ensure!(size > 0, "empty original raw admission");
        Ok(Self {
            id: id.into(),
            owner: row.try_get("", "owner")?,
            fingerprint: row.try_get("", "fingerprint")?,
            control: control(&store.connection, id).await?,
            size,
            bytes: Vec::new(),
            done: false,
        })
    }
    async fn step(&mut self, store: &CrudStore) -> Result<()> {
        if self.done {
            return Ok(());
        }
        ensure!(
            control(&store.connection, &self.id).await? == self.control,
            "raw admission control changed between fragments"
        );
        let row=store.connection.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT substr(CAST(o.snapshot AS BLOB),?4,?5) AS fragment FROM compaction_operation o LEFT JOIN compaction_runner_state r ON r.operation_id=o.id WHERE o.id=?1 AND o.owner=?2 AND o.fingerprint=?3 AND length(CAST(o.snapshot AS BLOB))=?6 AND o.status=?7 AND r.generation IS ?8 LIMIT 1",
            [self.id.clone().into(),self.owner.clone().into(),self.fingerprint.clone().into(),(i64::try_from(self.bytes.len())?+1).into(),i64::try_from(SOURCE_PAGE_BYTES)?.into(),self.size.into(),self.control.status.clone().into(),self.control.generation.into()])).await?
            .ok_or_else(||anyhow::anyhow!("raw admission identity changed between fragments"))?;
        let fragment: Vec<u8> = row.try_get("", "fragment")?;
        if i64::try_from(self.bytes.len())? == self.size {
            ensure!(fragment.is_empty(), "raw admission extra fragment");
            self.done = true;
        } else {
            ensure!(
                !fragment.is_empty() && fragment.len() <= SOURCE_PAGE_BYTES,
                "raw admission fragment missing or oversized"
            );
            self.bytes.extend(fragment);
            ensure!(
                i64::try_from(self.bytes.len())? <= self.size,
                "raw admission exceeded exact size"
            );
        }
        Ok(())
    }
    fn finish(self) -> Result<OperationSnapshot> {
        ensure!(
            self.done && i64::try_from(self.bytes.len())? == self.size,
            "raw admission incomplete"
        );
        let snapshot: OperationSnapshot = serde_json::from_slice(&self.bytes)?;
        ensure!(
            snapshot.id == self.id && snapshot.owner == self.owner,
            "raw admission identity mismatch"
        );
        Ok(snapshot)
    }
}
