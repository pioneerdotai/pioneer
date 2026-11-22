//! Permanent selected checkpoint evidence, prepared with the existing writer.
//! Historical evidence is not a new consumer grant.
use crate::checkpoint_topology::{CheckpointTopology, CheckpointTopologyRead, checkpoint_topology};
use crate::frozen_import::{self, FrozenImportRecord};
use crate::frozen_lifetime::FrozenReadHold;
use crate::frozen_reachability::checkpoint_source_owner;
use crate::proof_graph::{
    CheckpointEdgesRow, HistoricalEventInputEvidenceRow, HistoricalEventInputEvidenceRow as Event,
    HistoricalReplayAliasRow, HistoricalReplayAliasRow as Alias,
};
use crate::proof_publication::{checkpoint_row, refresh_checkpoint_row};
use crate::proof_staging::FROZEN_IMPORT_PAGE_BYTES;
#[cfg(any(test, feature = "test-support"))]
use crate::proof_test_metrics::{PublicationWriterTestGuard, record_proof_preparation_check};
#[cfg(any(test, feature = "test-support"))]
use crate::publication_test_hook::{PublicationTestPause, pause_publication_test_hook};
use crate::source_revision::compaction_checkpoint_source;
use crate::store::CompactionStore;
use crate::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS};
use anyhow::{Result, ensure};
use pioneer_compaction::{SourceRef, frozen::FrozenMessageRef};
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_checkpoint_event_input as event_input,
    compaction_checkpoint_import as import, compaction_checkpoint_replay_alias as replay_alias,
    compaction_context as context, compaction_coverage as coverage,
    compaction_delivery_output as delivery_output, compaction_event_revision as ack,
    compaction_frozen_history as history, compaction_manifest as manifest,
    compaction_operation_projection as projection, compaction_runner_plan as plan,
    compaction_runner_state as runner, compaction_task_output as task_output,
    task_delivery as delivery, task_result_candidate as candidate, thread, turn,
};
use sea_orm::sea_query::{Alias as SqlAlias, Expr, ExprTrait, JoinType, OnConflict, Order, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, QueryTrait, RelationTrait, Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::checkpoint_topology::CheckpointIdentityRead;
use crate::proof_admission::{RawAdmissionRead, freeze_checkpoint, validate_operation_state};
pub use crate::proof_graph::target_reference;
use crate::proof_graph::{
    CachedGraphValidation, GraphEvidence, Metadata, Ownership, add_import_to_digest,
    import_identity, raw_contract, select_reference, validate_metadata,
};

use crate::proof_publication::{ProofControl, read_proof_control, seal_checkpoint};

use crate::proof_staging::{MetadataReadback, read_import, stage_alias, stage_event, stage_import};

fn graph_evidence_from_scan(scan: &OriginScan) -> GraphEvidence {
    GraphEvidence {
        row: scan.row.clone(),
        ownership: scan.ownership.clone(),
        metadata: scan.metadata.clone(),
    }
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
pub struct OriginScan {
    pub row: CheckpointEdgesRow,
    ownership: Ownership,
    pub header: Option<history::Model>,
    pub hold: Option<FrozenReadHold>,
    pub metadata: Metadata,
    layout: Option<crate::frozen::LayoutScan>,
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
pub enum OriginRead {
    Legacy(OriginScan),
    Permanent(CheckpointTopology),
}

impl OriginScan {
    pub async fn new(
        store: &CompactionStore,
        row: CheckpointEdgesRow,
        ownership: Ownership,
    ) -> Result<OriginRead> {
        let row = refresh_checkpoint_row(&store.connection, &row).await?;
        if row.proof_version == 1 {
            return Ok(OriginRead::Permanent(CheckpointTopology { row, ownership }));
        }
        let (header, hold, phase) = if let Some(manifest) = &row.manifest_id {
            let (descriptor, header) =
                crate::source_projection::projection_identity(&store.connection, &row.operation_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("checkpoint lost its bound origin"))?;
            ensure!(
                &descriptor.manifest_id == manifest && header.workspace_id == row.workspace_id,
                "checkpoint origin scope mismatch"
            );
            #[cfg(any(test, feature = "test-support"))]
            pause_publication_test_hook(
                store.connection.runtime_identity(),
                &row.operation_id,
                PublicationTestPause::CheckpointOriginAcquire,
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
                        let current = refresh_checkpoint_row(&store.connection, &row).await?;
                        if current.proof_version == 1 {
                            return Ok(OriginRead::Permanent(CheckpointTopology {
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
            Some(RawAdmissionRead::new(&store.connection, &row.operation_id).await?)
        } else {
            None
        };
        let layout = header
            .clone()
            .map(|h| crate::frozen::LayoutScan::new(h, 0, 2))
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
    pub async fn step(&mut self, store: &CompactionStore) -> Result<Option<import::Model>> {
        if let Some(pending) = self.pending_import.as_mut() {
            if let Some(member) = pending.member.as_mut() {
                if !member.done() {
                    member.step(store).await?;
                    return Ok(None);
                }
                ensure!(member.found, "selected checkpoint exceeds original grant");
            }
            original_binding(store, &self.row.workspace_id, &pending.record, true).await?;
            let pending = self.pending_import.take().expect("selected import");
            add_import_to_digest(
                &mut self.selected_import_digest,
                &import_identity(&pending.proof)?,
            );
            self.selected_import_count += 1;
            return Ok(Some(pending.proof));
        }
        if let Some(layout) = self.layout.as_mut() {
            layout.step(&store.connection).await?;
            if layout.done() {
                self.layout = None;
            }
            return Ok(None);
        }
        match self.phase {
            Phase::Raw => {
                let raw = self.raw.as_mut().expect("raw origin");
                raw.step(&store.connection).await?;
                if raw.is_complete() {
                    let raw = self.raw.take().expect("complete raw origin");
                    let owner = raw.owner().to_owned();
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
    async fn messages_step(&mut self, store: &CompactionStore) -> Result<()> {
        let header = self.header.as_ref().expect("origin phase");
        let manifest = &header.id;
        let sizes = store
            .connection
            .query_all_raw(checkpoint_projection_page_sizes_statement(
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
            .query_all_raw(checkpoint_projection_page_statement(
                manifest,
                self.next_message,
                end,
            ))
            .await?;
        #[cfg(any(test, feature = "test-support"))]
        checkpoint_projection_page_test_pause(&store.connection, manifest).await;
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
        #[cfg(any(test, feature = "test-support"))]
        record_checkpoint_projection_page_test_read(
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
    async fn import_step(&mut self, store: &CompactionStore) -> Result<Option<import::Model>> {
        let header = self.header.as_ref().expect("origin phase");
        if self.imports_page.is_empty() {
            self.imports_page = frozen_import::compaction_frozen_import_page(
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
        add_import_to_digest(&mut self.imports_digest, json.as_bytes());
        let ordinal = self.next_import;
        self.next_import += 1;
        let target =
            target_reference(&store.connection, &header.id, record.message_ordinal).await?;
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
            checkpoint_target.is_some() || record.matches_render_target(&target),
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
                    record.render_source(),
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
            member,
        });
        Ok(None)
    }
}

/// Exact historical membership, without recursively extracting origins. Full
/// alias/event graph validation remains a separate preparation/consumer guard.
struct PendingImport {
    proof: import::Model,
    record: FrozenImportRecord,
    member: Option<HistoricalMember>,
}
pub struct HistoricalMember {
    workspace: String,
    wanted_thread: String,
    wanted: SourceRef,
    pending: Vec<(SourceRef, String, bool)>,
    active: BTreeSet<(SourceRef, String)>,
    done: BTreeSet<(SourceRef, String)>,
    found: bool,
    topology: Option<(SourceRef, String, CheckpointTopologyRead)>,
}
impl HistoricalMember {
    pub async fn new(
        store: &CompactionStore,
        workspace: &str,
        root_thread: &str,
        root: &SourceRef,
        wanted_thread: &str,
        wanted: &SourceRef,
    ) -> Result<Self> {
        ensure!(
            compaction_checkpoint_source(&store.connection, workspace, root_thread, &root.id)
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
    pub fn done(&self) -> bool {
        self.pending.is_empty() && self.topology.is_none()
    }
    pub fn found(&self) -> bool {
        self.found
    }
    pub async fn step(&mut self, store: &CompactionStore) -> Result<()> {
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
            let read = CheckpointTopologyRead::new(&store.connection, &source.id)
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
            let previous = checkpoint_row(&store.connection, &previous)
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
pub async fn original_binding(
    store: &CompactionStore,
    workspace: &str,
    proof: &FrozenImportRecord,
    read_output: bool,
) -> Result<()> {
    proof
        .validate_preview_archive(&store.connection, workspace)
        .await?;
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
    let acknowledgement_turn = proof
        .acknowledgement
        .scope
        .strip_prefix("event:")
        .ok_or_else(|| {
            anyhow::anyhow!("original accepted import acknowledgement scope mismatch")
        })?;
    // This is the original delivery's acceptance, not a new grant to the
    // current checkpoint context. Accepted Task bases can carry this exact
    // record through multiple children; append revalidates each forwarding
    // binding. The acknowledgement turn must still belong to the original
    // delivery target in the same workspace.
    let query = Query::select()
        .from(delivery::Entity)
        .columns(
            [
                history::Column::Id,
                history::Column::MessageCount,
                history::Column::IdentitySha256,
                history::Column::Ready,
                history::Column::Expired,
                history::Column::NextOrdinal,
                history::Column::ImportCount,
                history::Column::NextImport,
                history::Column::OwnerThread,
            ]
            .map(|c| (history::Entity, c)),
        )
        .columns(
            [
                task_output::Column::WorkspaceId,
                task_output::Column::SourceThread,
            ]
            .map(|c| (task_output::Entity, c)),
        )
        .expr_as(
            Expr::col((ack::Entity, ack::Column::Revision)),
            SqlAlias::new("ack_revision"),
        )
        .join(
            JoinType::InnerJoin,
            delivery_output::Entity,
            Expr::col((delivery_output::Entity, delivery_output::Column::DeliveryId))
                .eq(Expr::col((delivery::Entity, delivery::Column::Id))),
        )
        .join(
            JoinType::InnerJoin,
            task_output::Entity,
            Expr::col((task_output::Entity, task_output::Column::TaskRunTurnId)).eq(Expr::col((
                delivery_output::Entity,
                delivery_output::Column::TaskRunTurnId,
            ))),
        )
        .join(
            JoinType::InnerJoin,
            history::Entity,
            Expr::col((history::Entity, history::Column::Id)).eq(Expr::col((
                task_output::Entity,
                task_output::Column::ManifestId,
            ))),
        )
        .join(
            JoinType::InnerJoin,
            candidate::Entity,
            Expr::col((candidate::Entity, candidate::Column::Id))
                .eq(Expr::col((
                    delivery_output::Entity,
                    delivery_output::Column::CandidateId,
                )))
                .and(
                    Expr::col((candidate::Entity, candidate::Column::TaskId))
                        .eq(Expr::col((delivery::Entity, delivery::Column::TaskId)))
                        .and(
                            Expr::col((candidate::Entity, candidate::Column::RunId))
                                .eq(Expr::col((delivery::Entity, delivery::Column::RunId))),
                        ),
                ),
        )
        .join(
            JoinType::InnerJoin,
            ack::Entity,
            Expr::col((ack::Entity, ack::Column::TurnId)).eq(Expr::col((
                delivery::Entity,
                delivery::Column::DeliveredTurnId,
            ))),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity,
            Expr::col((turn::Entity, turn::Column::Id))
                .eq(Expr::col((ack::Entity, ack::Column::TurnId))),
        )
        .join(
            JoinType::InnerJoin,
            thread::Entity,
            Expr::col((thread::Entity, thread::Column::Id))
                .eq(Expr::col((turn::Entity, turn::Column::ThreadId))),
        )
        .and_where(Expr::col((delivery::Entity, delivery::Column::Id)).eq(&proof.delivery_id))
        .and_where(
            Expr::col((
                delivery_output::Entity,
                delivery_output::Column::CandidateId,
            ))
            .eq(&proof.candidate_id),
        )
        .and_where(
            Expr::col((task_output::Entity, task_output::Column::ManifestId))
                .eq(&proof.output_manifest),
        )
        .and_where(Expr::col((delivery::Entity, delivery::Column::WorkspaceId)).eq(workspace))
        .and_where(
            Expr::col((turn::Entity, turn::Column::ThreadId)).eq(Expr::col((
                delivery::Entity,
                delivery::Column::TargetThreadId,
            ))),
        )
        .and_where(Expr::col((delivery::Entity, delivery::Column::Status)).eq("delivered"))
        .and_where(Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(workspace))
        .and_where(Expr::col((ack::Entity, ack::Column::SourceId)).eq(&proof.acknowledgement.id))
        .and_where(Expr::col((ack::Entity, ack::Column::TurnId)).eq(acknowledgement_turn))
        .and_where(Expr::col((ack::Entity, ack::Column::ItemId)).eq(
            pioneer_protocol::task_delivery_result_item_id(&proof.delivery_id),
        ))
        .and_where(
            Expr::col((task_output::Entity, task_output::Column::WorkspaceId))
                .eq(Expr::col((delivery::Entity, delivery::Column::WorkspaceId))),
        )
        .and_where(
            Expr::col((history::Entity, history::Column::WorkspaceId)).eq(Expr::col((
                task_output::Entity,
                task_output::Column::WorkspaceId,
            ))),
        )
        .and_where(
            Expr::col((task_output::Entity, task_output::Column::TaskId))
                .eq(Expr::col((delivery::Entity, delivery::Column::TaskId))),
        )
        .and_where(
            Expr::col((task_output::Entity, task_output::Column::RunId))
                .eq(Expr::col((delivery::Entity, delivery::Column::RunId))),
        )
        .limit(1)
        .to_owned();
    let row = store
        .connection
        .query_one(&query)
        .await?
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
        let original = target_reference(
            &store.connection,
            &proof.output_manifest,
            proof.output_ordinal,
        )
        .await?;
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

/// Incremental private preparation is reused by foreground publication and the
/// existing maintenance quantum. Only this current origin has a temporary hold.
pub struct NodePreparation {
    scan: OriginScan,
    control: ProofControl,
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
        store: &CompactionStore,
        topology: CheckpointTopology,
        reachable: bool,
    ) -> Result<NodeWork> {
        ensure!(
            topology.row.proof_version == 0,
            "node is already sealed or has unsupported version"
        );
        let ctl = read_proof_control(&store.connection, &topology.row.operation_id).await?;
        ensure!(
            ctl.owner == topology.row.owner,
            "checkpoint operation owner mismatch"
        );
        validate_operation_state(&store.connection, &topology.row, &ctl).await?;
        let scan = match OriginScan::new(store, topology.row, topology.ownership).await? {
            OriginRead::Legacy(scan) => scan,
            OriginRead::Permanent(topology) => {
                // Adopting already verified evidence cannot erase a Stop/resume
                // observed during this preparation's control boundary.
                ensure!(
                    read_proof_control(&store.connection, &topology.row.operation_id).await? == ctl,
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
    pub async fn step(&mut self, store: &CompactionStore) -> Result<()> {
        if self.done {
            return Ok(());
        }
        if self.scanning {
            if !self.scan.done() {
                if let Some(proof) = self.scan.step(store).await? {
                    store
                        .retry_write_on_conflict(|| stage_import(&store.connection, &proof))
                        .await?;
                }
                return Ok(());
            }
            self.aliases = self.scan.metadata.aliases.iter().cloned().collect();
            self.events = self.scan.metadata.events.iter().cloned().collect();
            self.scanning = false;
        }
        if let Some(alias) = self.aliases.front() {
            store
                .retry_write_on_conflict(|| {
                    stage_alias(&store.connection, &self.scan.row.id, alias)
                })
                .await?;
            self.aliases.pop_front();
            return Ok(());
        }
        if let Some(event) = self.events.front() {
            store
                .retry_write_on_conflict(|| {
                    stage_event(&store.connection, &self.scan.row.id, event)
                })
                .await?;
            self.events.pop_front();
            return Ok(());
        }
        if !self.metadata_readback.done {
            self.metadata_readback
                .step(&store.connection, &self.scan.row.id)
                .await?;
            return Ok(());
        }
        ensure!(
            self.metadata_readback.metadata == self.scan.metadata,
            "selected metadata exact readback conflict"
        );
        if !self.imports_done {
            if let Some(proof) =
                read_import(&store.connection, &self.scan.row.id, self.import_after).await?
            {
                self.import_after = proof.import_ordinal;
                self.import_count += 1;
                ensure!(
                    self.import_count <= self.scan.selected_import_count,
                    "extra selected import rows"
                );
                add_import_to_digest(&mut self.import_digest, &import_identity(&proof)?);
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
    async fn dependency_step(&mut self, store: &CompactionStore) -> Result<()> {
        let row = &self.scan.row;
        #[cfg(any(test, feature = "test-support"))]
        record_proof_preparation_check(&row.operation_id, false);
        if !self.previous_ready {
            if let Some(previous) = &row.previous {
                ensure!(
                    checkpoint::Entity::find_by_id(previous)
                        .select_only()
                        .column(checkpoint::Column::Id)
                        .filter(checkpoint::Column::Owner.eq(&row.owner))
                        .filter(checkpoint::Column::ProofVersion.eq(1))
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                        .is_some(),
                    "previous proof is not ready"
                );
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
                ensure!(
                    checkpoint::Entity::find_by_id(&source.id)
                        .select_only()
                        .column(checkpoint::Column::Id)
                        .filter(
                            checkpoint::Column::Owner
                                .eq(source.scope.strip_prefix("checkpoint:").unwrap())
                        )
                        .filter(checkpoint::Column::IdentitySha256.eq(&source.version))
                        .filter(checkpoint::Column::ProofVersion.eq(1))
                        .into_tuple::<String>()
                        .one(&store.connection)
                        .await?
                        .is_some(),
                    "foreign proof is not ready"
                );
            }
            self.dependency_after = Some(source.clone());
        } else {
            self.dependencies_ready = true;
        }
        Ok(())
    }
    async fn seal(&self, store: &CompactionStore) -> Result<()> {
        let row = &self.scan.row;
        ensure!(
            self.dependencies_ready
                && self.graph_validated
                && self.imports_done
                && self.metadata_readback.done,
            "proof seal preparation incomplete"
        );
        #[cfg(any(test, feature = "test-support"))]
        pause_publication_test_hook(
            store.connection.runtime_identity(),
            &row.operation_id,
            PublicationTestPause::ProofSeal,
        )
        .await;
        let foreign = self
            .scan
            .ownership
            .keys()
            .filter_map(|source| {
                let owner = source.scope.strip_prefix("checkpoint:")?;
                Some((source.id.as_str(), owner, source.version.as_str()))
            })
            .collect::<BTreeSet<_>>();
        store
            .retry_write_on_conflict(|| async {
                #[cfg(any(test, feature = "test-support"))]
                let _writer = PublicationWriterTestGuard::enter_seal(&row.operation_id);
                let tx = store.connection.begin().await?;
                seal_checkpoint(&tx, row, &self.control, self.scan.header.as_ref(), &foreign)
                    .await?;
                tx.commit().await?;
                Ok(())
            })
            .await
    }
}

async fn validate_permanent_import(
    store: &CompactionStore,
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
    let (owner, messages, imports) = history::Entity::find_by_id(origin)
        .select_only()
        .columns([
            history::Column::OwnerThread,
            history::Column::MessageCount,
            history::Column::ImportCount,
        ])
        .filter(history::Column::WorkspaceId.eq(&row.workspace_id))
        .into_tuple::<(String, i64, i64)>()
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("permanent proof origin identity lost"))?;
    ensure!(
        proof.message_ordinal < messages
            && proof.import_ordinal < imports
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
        accepted.render_source().clone()
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
    layout: Option<crate::frozen::LayoutScan>,
    raw: Option<RawAdmissionRead>,
    row: CheckpointEdgesRow,
    ownership: Ownership,
    hold: Option<FrozenReadHold>,
    metadata: MetadataReadback,
    after: i64,
    done: bool,
}
impl PermanentRead {
    async fn new(store: &CompactionStore, topology: CheckpointTopology) -> Result<Self> {
        let reachable = published_reachable(store, &topology.row).await?;
        Self::new_in_graph(store, topology, reachable).await
    }
    async fn new_in_graph(
        store: &CompactionStore,
        topology: CheckpointTopology,
        reachable: bool,
    ) -> Result<Self> {
        let row = topology.row;
        ensure!(row.proof_version == 1, "checkpoint proofs are not ready");
        let public = compaction_checkpoint_source(
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
                let (descriptor, _) = crate::source_projection::projection_identity(
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
                raw = Some(RawAdmissionRead::new(&store.connection, &row.operation_id).await?);
                None
            }
        } else {
            None
        };
        let layout = if hold.is_some() {
            let (_, h) =
                crate::source_projection::projection_identity(&store.connection, &row.operation_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("candidate origin missing"))?;
            Some(crate::frozen::LayoutScan::new(h, 0, 2)?)
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
    async fn step(&mut self, store: &CompactionStore) -> Result<()> {
        if let Some(raw) = self.raw.as_mut() {
            raw.step(&store.connection).await?;
            if raw.is_complete() {
                let raw = self.raw.take().expect("complete raw admission");
                let owner = raw.owner().to_owned();
                raw_contract(&raw.finish()?, &owner, &self.row, &self.ownership)?;
            }
            return Ok(());
        }
        if let Some(layout) = self.layout.as_mut() {
            layout.step(&store.connection).await?;
            if layout.done() {
                self.layout = None;
            }
            return Ok(());
        }

        if self.done {
            return Ok(());
        }
        if !self.metadata.done {
            self.metadata.step(&store.connection, &self.row.id).await?;
            return Ok(());
        }
        validate_metadata(&self.metadata.metadata, &self.ownership)?;
        if let Some(proof) = read_import(&store.connection, &self.row.id, self.after).await? {
            validate_permanent_import(store, &self.row, &self.ownership, &proof).await?;
            self.after = proof.import_ordinal;
            return Ok(());
        }
        // A reset is only permitted for an unused terminal candidate after
        // expiry. Such a reader pins the origin, so reset cannot cross it.
        let ready = checkpoint::Entity::find_by_id(&self.row.id)
            .select_only()
            .column(checkpoint::Column::Id)
            .filter(checkpoint::Column::ProofVersion.eq(1))
            .filter(checkpoint::Column::IdentitySha256.eq(&self.row.identity_sha256))
            .into_tuple::<String>()
            .one(&store.connection)
            .await?;
        ensure!(
            ready.is_some(),
            "checkpoint proof readiness changed during read"
        );
        self.done = true;
        self.hold.take();
        Ok(())
    }
}
pub async fn permanent_metadata(
    store: &CompactionStore,
    row: CheckpointEdgesRow,
    ownership: Ownership,
) -> Result<Metadata> {
    let mut read = PermanentRead::new(store, CheckpointTopology { row, ownership }).await?;
    while !read.done {
        read.step(store).await?;
    }
    Ok(read.metadata.metadata)
}

/// Postorder preparation retains full previous/foreign traversal. Frames carry
/// exact expected ownership and identity, so sharing a node cannot weaken scope.
pub struct GraphPreparation {
    pending: Vec<Frame>,
    active: BTreeSet<String>,
    done: BTreeSet<String>,
    current: Option<NodeWork>,
    workspace: Option<String>,
    root: String,
    evidence: BTreeMap<String, GraphEvidence>,
    validation: Option<CachedGraphValidation>,
    reachable: BTreeSet<String>,
    topology: Option<(Frame, CheckpointTopologyRead)>,
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
    /// A failed step may have consumed a frame/import before its DB operation.
    /// Restart this job from its root; drop all old reader holds and RAM scans.
    /// Already committed staging is idempotent and will be verified again.
    pub fn restart(&mut self) {
        *self = Self::new(&self.root);
    }
    pub fn finished(&self) -> bool {
        self.current.is_none() && self.topology.is_none() && self.pending.is_empty()
    }
    pub async fn step(&mut self, store: &CompactionStore) -> Result<()> {
        if let Some(current) = &mut self.current {
            if let NodeWork::Identity(identity) = current {
                identity.step(&store.connection).await?;
                if let Some(topology) = identity.verified_topology().cloned() {
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
                    self.validation = Some(CachedGraphValidation::new(graph_evidence_from_scan(
                        &node.scan,
                    )));
                }
                let validation = self.validation.as_mut().expect("graph validation");
                validation.step(&self.evidence)?;
                if !validation.finished() {
                    return Ok(());
                }
                ensure!(
                    node.scan.row.id != self.root || validation.has_leaves(),
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
                    NodeWork::Prepare(node) => graph_evidence_from_scan(&node.scan),
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
                    if !validation.finished() {
                        return Ok(());
                    }
                    ensure!(
                        id != self.root || validation.has_leaves(),
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
            freeze_checkpoint(&store.connection, &frame.id).await?;
            let read = CheckpointTopologyRead::new(&store.connection, &frame.id)
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
                        && compaction_checkpoint_source(
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
        if compaction_checkpoint_source(
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
                0 => NodeWork::Identity(
                    CheckpointIdentityRead::new(&store.connection, topology).await?,
                ),
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
pub async fn prepare_graph(store: &CompactionStore, root: &str) -> Result<()> {
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
async fn published_reachable(
    store: &CompactionStore,
    initial: &CheckpointEdgesRow,
) -> Result<bool> {
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
        let node = checkpoint_row(&store.connection, &id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("reverse checkpoint node missing"))?;
        ensure!(
            node.workspace_id == initial.workspace_id,
            "reverse checkpoint workspace mismatch"
        );
        if compaction_checkpoint_source(
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
                let mut query = Query::select();
                if foreign {
                    query
                        .from(coverage::Entity)
                        .expr_as(
                            Expr::col((coverage::Entity, coverage::Column::CheckpointId)),
                            SqlAlias::new("id"),
                        )
                        .join(
                            JoinType::InnerJoin,
                            checkpoint::Entity,
                            Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(Expr::col(
                                (coverage::Entity, coverage::Column::CheckpointId),
                            )),
                        )
                        .and_where(
                            Expr::col((coverage::Entity, coverage::Column::SourceScope))
                                .eq(format!("checkpoint:{}", node.owner)),
                        )
                        .and_where(
                            Expr::col((coverage::Entity, coverage::Column::SourceId)).eq(&id),
                        )
                        .and_where(
                            Expr::col((coverage::Entity, coverage::Column::SourceVersion))
                                .eq(&node.identity_sha256),
                        )
                        .and_where(
                            Expr::col((coverage::Entity, coverage::Column::CheckpointId))
                                .gt(&after),
                        )
                        .and_where(
                            Expr::col((checkpoint::Entity, checkpoint::Column::ProofVersion)).eq(1),
                        )
                        .order_by(
                            (coverage::Entity, coverage::Column::CheckpointId),
                            Order::Asc,
                        );
                } else {
                    query
                        .from(checkpoint::Entity)
                        .column(checkpoint::Column::Id)
                        .and_where(checkpoint::Column::Owner.eq(&node.owner))
                        .and_where(checkpoint::Column::Previous.eq(&id))
                        .and_where(checkpoint::Column::Id.gt(&after))
                        .and_where(checkpoint::Column::ProofVersion.eq(1))
                        .order_by(checkpoint::Column::Id, Order::Asc);
                }
                query.limit(SOURCE_PAGE_ROWS);
                let rows = store.connection.query_all(&query).await?;
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    after = row.try_get("", "id")?;
                    ensure!(
                        pending.len() + visited.len() < 65_536,
                        "reverse checkpoint pending bound exceeded"
                    );
                    let incoming = checkpoint_row(&store.connection, &after)
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
                            checkpoint_source_owner(&store.connection, &incoming, &source).await?
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
    async fn correction_fixture() -> CompactionStore {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection().clone();
        for sql in [
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)",
        ] {
            db.execute_unprepared(sql).await.unwrap();
        }
        CompactionStore::new(
            pioneer_postgres_pools::PostgresDatabase::new(
                fixture.connect_read_only().await.unwrap(),
                db,
            ),
            Default::default(),
        )
    }

    #[pioneer_postgres_test::postgres_test]
    async fn malformed_original_ack_scope_is_consistency_error_without_panic() {
        let store = correction_fixture().await;
        let proof = FrozenImportRecord {
            preview_archive: None,
            message_ordinal: 0,
            source_thread: "thread".into(),
            source: SourceRef {
                scope: "event:source-turn".into(),
                id: "source".into(),
                version: "event-revision:1".into(),
            },
            delivery_id: "delivery".into(),
            candidate_id: "candidate".into(),
            output_manifest: "output".into(),
            output_ordinal: 0,
            acknowledgement: SourceRef {
                scope: "item:ack-turn".into(),
                id: "ack".into(),
                version: "event-revision:1".into(),
            },
        };
        let error = original_binding(&store, "ws", &proof, true)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("acknowledgement scope mismatch"));
        assert!(store.frozen_readers.lock().unwrap().is_empty());
    }
}

#[derive(Clone, Debug)]
pub struct CheckpointEdges {
    pub owner: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub identity_sha256: String,
    pub previous: Option<String>,
    pub format_version: u32,
    pub coverage: Vec<HistoricalSourceRef>,
    /// Alternate canonical rows represented by a covered source in the
    /// immutable input manifest. These include provider replay rows and exact
    /// Task input copies. They suppress duplicates only; they are not grants.
    pub replay_aliases: Vec<HistoricalReplayAlias>,
    /// Event-input relationships captured in the immutable source projection
    /// of the operation that published this checkpoint.
    pub event_input_evidence: Vec<HistoricalEventInputEvidence>,
}

/// A source as it belonged to the operation that published a checkpoint.
/// `source_version` is historical evidence, not a request to resolve today's
/// canonical row at that version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoricalSourceRef {
    pub source_thread: String,
    pub source: SourceRef,
}

#[derive(Clone, Debug)]
pub struct HistoricalReplayAlias {
    pub covered: HistoricalSourceRef,
    pub replay: HistoricalSourceRef,
    pub tool_item_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoricalEventInputEvidence {
    pub source: HistoricalSourceRef,
    pub role: String,
}

pub async fn compaction_checkpoint_edges(
    store: &CompactionStore,
    id: &str,
) -> Result<Option<CheckpointEdges>> {
    let Some(CheckpointTopology { row, ownership }) =
        checkpoint_topology(&store.connection, id).await?
    else {
        return Ok(None);
    };
    let (replay_aliases, event_input_evidence) =
        checkpoint_projection_metadata(store, &row, &ownership).await?;
    Ok(Some(CheckpointEdges {
        owner: row.owner,
        workspace_id: row.workspace_id,
        thread_id: row.thread_id,
        identity_sha256: row.identity_sha256,
        previous: row.previous,
        format_version: u32::try_from(row.format_version)?,
        coverage: ownership
            .into_iter()
            .map(|(source, mut threads)| HistoricalSourceRef {
                source_thread: threads.pop_first().expect("one historical owner"),
                source,
            })
            .collect(),
        replay_aliases: replay_aliases
            .into_iter()
            .map(|alias| HistoricalReplayAlias {
                covered: HistoricalSourceRef {
                    source_thread: alias.covered_thread,
                    source: SourceRef {
                        scope: alias.covered_scope,
                        id: alias.covered_id,
                        version: alias.covered_version,
                    },
                },
                replay: HistoricalSourceRef {
                    source_thread: alias.replay_thread,
                    source: SourceRef {
                        scope: alias.replay_scope,
                        id: alias.replay_id,
                        version: alias.replay_version,
                    },
                },
                tool_item_id: alias.tool_item_id,
            })
            .collect(),
        event_input_evidence: event_input_evidence
            .into_iter()
            .map(|evidence| HistoricalEventInputEvidence {
                source: HistoricalSourceRef {
                    source_thread: evidence.source_thread,
                    source: SourceRef {
                        scope: evidence.source_scope,
                        id: evidence.source_id,
                        version: evidence.source_version,
                    },
                },
                role: evidence.role,
            })
            .collect(),
    }))
}

// Published operation projections and their logical frozen messages are immutable.
// Storage sharing may change between pages, but preserves each logical ordinal. The page
// statements reproduce the logical view's active-layout choice while binding the requested
// manifest and ordinal range directly on both physical data branches. No transaction or
// query stream spans pages: reader capacity is released before references are decoded and
// the next page is requested.
async fn checkpoint_projection_metadata(
    store: &CompactionStore,
    checkpoint_row: &CheckpointEdgesRow,
    ownership: &std::collections::BTreeMap<SourceRef, std::collections::BTreeSet<String>>,
) -> Result<(
    Vec<HistoricalReplayAliasRow>,
    Vec<HistoricalEventInputEvidenceRow>,
)> {
    let mut scan = match OriginScan::new(store, checkpoint_row.clone(), ownership.clone()).await? {
        OriginRead::Legacy(scan) => scan,
        OriginRead::Permanent(topology) => {
            let current = refresh_metadata_topology(store, checkpoint_row, ownership).await?;
            ensure!(
                current == topology.row,
                "checkpoint changed before permanent metadata"
            );
            let metadata = permanent_metadata(store, topology.row, topology.ownership).await?;
            return Ok((
                metadata.aliases.into_iter().collect(),
                metadata.events.into_iter().collect(),
            ));
        }
    };
    while !scan.done() {
        scan.step(store).await?;
    }
    refresh_metadata_topology(store, checkpoint_row, ownership).await?;
    Ok((
        scan.metadata.aliases.into_iter().collect(),
        scan.metadata.events.into_iter().collect(),
    ))
}

// Foreground only: a close after topology EOF/acquire must not promote an open
// observation into authoritative sealed ownership. Recheck the exact frozen set
// in released-capacity pages. GraphPreparation already closes before its scan.
async fn refresh_metadata_topology(
    store: &CompactionStore,
    observed: &CheckpointEdgesRow,
    ownership: &std::collections::BTreeMap<SourceRef, std::collections::BTreeSet<String>>,
) -> Result<CheckpointEdgesRow> {
    let current = refresh_checkpoint_row(&store.connection, observed).await?;
    if !observed.coverage_closed && current.coverage_closed {
        let frozen = checkpoint_topology(&store.connection, &current.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("closed checkpoint disappeared"))?;
        let latest = refresh_checkpoint_row(&store.connection, &current).await?;
        ensure!(
            frozen.ownership == *ownership,
            "checkpoint historical topology changed after close"
        );
        return Ok(latest);
    }
    Ok(current)
}

#[cfg(any(test, feature = "test-support"))]
struct CheckpointProjectionPageTestPause {
    token: std::sync::Arc<()>,
    reached: Option<tokio::sync::oneshot::Sender<()>>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointProjectionPageTestRead {
    pub start: i64,
    pub end: i64,
    pub rows: usize,
    pub bytes: usize,
}

#[cfg(any(test, feature = "test-support"))]
pub struct CheckpointProjectionPageTestObserver {
    key: (usize, String),
    token: std::sync::Arc<()>,
    _database: pioneer_postgres_pools::PostgresDatabase,
    reads: std::sync::Arc<std::sync::Mutex<Vec<CheckpointProjectionPageTestRead>>>,
}

#[cfg(any(test, feature = "test-support"))]
impl CheckpointProjectionPageTestObserver {
    pub fn reads(&self) -> Vec<CheckpointProjectionPageTestRead> {
        self.reads
            .lock()
            .expect("projection read lock poisoned")
            .clone()
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for CheckpointProjectionPageTestObserver {
    fn drop(&mut self) {
        let mut observers = checkpoint_projection_page_test_observers()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if observers
            .get(&self.key)
            .is_some_and(|(token, _)| std::sync::Arc::ptr_eq(token, &self.token))
        {
            observers.remove(&self.key);
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn observe_checkpoint_projection_page_test_reads(
    database: &pioneer_postgres_pools::PostgresDatabase,
    manifest: &str,
) -> CheckpointProjectionPageTestObserver {
    let reads = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let key = (database.runtime_identity(), manifest.to_owned());
    let token = std::sync::Arc::new(());
    let mut observers = checkpoint_projection_page_test_observers()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if observers.contains_key(&key) {
        drop(observers);
        panic!("projection read observer already armed");
    }
    observers.insert(
        key.clone(),
        (token.clone(), std::sync::Arc::downgrade(&reads)),
    );
    drop(observers);
    CheckpointProjectionPageTestObserver {
        key,
        token,
        _database: database.clone(),
        reads,
    }
}

#[cfg(any(test, feature = "test-support"))]
type CheckpointProjectionPageTestObservers = std::sync::Mutex<
    std::collections::HashMap<
        (usize, String),
        (
            std::sync::Arc<()>,
            std::sync::Weak<std::sync::Mutex<Vec<CheckpointProjectionPageTestRead>>>,
        ),
    >,
>;

#[cfg(any(test, feature = "test-support"))]
fn checkpoint_projection_page_test_observers() -> &'static CheckpointProjectionPageTestObservers {
    use std::sync::OnceLock;
    static OBSERVERS: OnceLock<CheckpointProjectionPageTestObservers> = OnceLock::new();
    OBSERVERS.get_or_init(Default::default)
}

#[cfg(any(test, feature = "test-support"))]
pub fn record_checkpoint_projection_page_test_read(
    database: &pioneer_postgres_pools::PostgresDatabase,
    manifest: &str,
    start: i64,
    end: i64,
    rows: usize,
    bytes: usize,
) {
    let key = (database.runtime_identity(), manifest.to_owned());
    let observer = checkpoint_projection_page_test_observers()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
        .and_then(|(_, observer)| std::sync::Weak::upgrade(observer));
    if let Some(observer) = observer {
        observer
            .lock()
            .expect("projection read lock poisoned")
            .push(CheckpointProjectionPageTestRead {
                start,
                end,
                rows,
                bytes,
            });
    }
}

#[cfg(any(test, feature = "test-support"))]
pub struct CheckpointProjectionPageTestHook {
    key: (usize, String),
    token: std::sync::Arc<()>,
    _database: pioneer_postgres_pools::PostgresDatabase,
    reached: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
}

#[cfg(any(test, feature = "test-support"))]
impl CheckpointProjectionPageTestHook {
    pub async fn reached(&mut self) {
        let reached = self
            .reached
            .take()
            .expect("projection hook already awaited");
        tokio::time::timeout(std::time::Duration::from_secs(10), reached)
            .await
            .expect("projection page hook was not reached before the diagnostic deadline")
            .expect("projection lookup ended before reaching the page hook");
    }

    pub fn release(mut self) {
        self.release
            .take()
            .expect("projection page hook already released")
            .send(())
            .expect("projection lookup ended before page-hook release");
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for CheckpointProjectionPageTestHook {
    fn drop(&mut self) {
        let mut hooks = checkpoint_projection_page_test_hooks()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if hooks
            .get(&self.key)
            .is_some_and(|hook| std::sync::Arc::ptr_eq(&hook.token, &self.token))
        {
            hooks.remove(&self.key);
        }
        // Dropping `release` also frees a participant that already took this
        // registration and is waiting at the barrier.
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn arm_checkpoint_projection_page_test_hook(
    database: &pioneer_postgres_pools::PostgresDatabase,
    manifest: &str,
) -> CheckpointProjectionPageTestHook {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let key = (database.runtime_identity(), manifest.to_owned());
    let token = std::sync::Arc::new(());
    let mut hooks = checkpoint_projection_page_test_hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if hooks.contains_key(&key) {
        drop(hooks);
        panic!("projection page hook already armed");
    }
    hooks.insert(
        key.clone(),
        CheckpointProjectionPageTestPause {
            token: token.clone(),
            reached: Some(reached_tx),
            release: release_rx,
        },
    );
    drop(hooks);
    CheckpointProjectionPageTestHook {
        key,
        token,
        _database: database.clone(),
        reached: Some(reached_rx),
        release: Some(release_tx),
    }
}

#[cfg(any(test, feature = "test-support"))]
fn checkpoint_projection_page_test_hooks() -> &'static std::sync::Mutex<
    std::collections::HashMap<(usize, String), CheckpointProjectionPageTestPause>,
> {
    use std::sync::{Mutex, OnceLock};
    static HOOKS: OnceLock<
        Mutex<std::collections::HashMap<(usize, String), CheckpointProjectionPageTestPause>>,
    > = OnceLock::new();
    HOOKS.get_or_init(Default::default)
}

#[cfg(any(test, feature = "test-support"))]
pub async fn checkpoint_projection_page_test_pause(
    database: &pioneer_postgres_pools::PostgresDatabase,
    manifest: &str,
) {
    let key = (database.runtime_identity(), manifest.to_owned());
    let pause = checkpoint_projection_page_test_hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&key);
    if let Some(mut pause) = pause {
        if let Some(reached) = pause.reached.take() {
            let _ = reached.send(());
        }
        let _ = pause.release.await;
    }
}

pub fn checkpoint_projection_page_sizes_statement(manifest: &str, start: i64) -> Statement {
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT d.ordinal,d.bytes FROM compaction_frozen_message_data d \
         JOIN compaction_frozen_history h ON h.id=d.manifest_id \
         WHERE h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count AND d.ordinal>=0 AND d.ordinal<h.message_count AND d.manifest_id=$1 AND d.ordinal>=$2 \
           AND NOT EXISTS (SELECT 1 FROM compaction_frozen_layout l \
                           WHERE l.manifest_id=d.manifest_id AND l.kind=0 AND l.active=1) \
         UNION ALL \
         SELECT d.ordinal,d.bytes FROM compaction_frozen_span s \
         JOIN compaction_frozen_message_data d \
           ON d.manifest_id=s.source_manifest AND d.ordinal>=s.start AND d.ordinal<s.end \
         JOIN compaction_frozen_layout l \
           ON l.manifest_id=s.manifest_id AND l.kind=s.kind AND l.active=1 \
         JOIN compaction_frozen_history h ON h.id=s.manifest_id \
         WHERE h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count AND d.ordinal>=0 AND d.ordinal<h.message_count AND s.manifest_id=$1 AND s.kind=0 AND d.ordinal>=$2 \
         ORDER BY ordinal LIMIT $3",
        [
            manifest.into(),
            start.into(),
            (SOURCE_PAGE_ROWS as i64).into(),
        ],
    )
}

pub fn checkpoint_projection_page_statement(manifest: &str, start: i64, end: i64) -> Statement {
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT d.ordinal,d.reference_json,d.bytes FROM compaction_frozen_message_data d \
         JOIN compaction_frozen_history h ON h.id=d.manifest_id \
         WHERE h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count AND d.ordinal>=0 AND d.ordinal<h.message_count AND d.manifest_id=$1 AND d.ordinal>=$2 AND d.ordinal<$3 AND d.bytes BETWEEN 0 AND 262144 AND octet_length(d.reference_json::text)=d.bytes \
           AND NOT EXISTS (SELECT 1 FROM compaction_frozen_layout l \
                           WHERE l.manifest_id=d.manifest_id AND l.kind=0 AND l.active=1) \
         UNION ALL \
         SELECT d.ordinal,d.reference_json,d.bytes FROM compaction_frozen_span s \
         JOIN compaction_frozen_message_data d \
           ON d.manifest_id=s.source_manifest AND d.ordinal>=s.start AND d.ordinal<s.end \
         JOIN compaction_frozen_layout l \
           ON l.manifest_id=s.manifest_id AND l.kind=s.kind AND l.active=1 \
         JOIN compaction_frozen_history h ON h.id=s.manifest_id \
         WHERE h.expired=0 AND h.ready=1 AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count AND h.next_import=h.import_count AND d.ordinal>=0 AND d.ordinal<h.message_count AND s.manifest_id=$1 AND s.kind=0 AND d.ordinal>=$2 AND d.ordinal<$3 AND d.bytes BETWEEN 0 AND 262144 AND octet_length(d.reference_json::text)=d.bytes \
         ORDER BY ordinal",
        [manifest.into(), start.into(), end.into()],
    )
}
