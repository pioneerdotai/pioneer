//! Immutable own-import evidence attached to the newly admitted frozen context.
//! Preparation decodes reference metadata outside database capacity. Publication
//! is fenced by the manifest's declared import count and independent digest.
use crate::checkpoint_proofs::compaction_checkpoint_edges;
use crate::source_revision::compaction_checkpoint_source;
use crate::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS};

use crate::frozen_views::{
    import as compaction_frozen_import, message as compaction_frozen_message,
};
use crate::store::CompactionStore;
use anyhow::{Result, ensure};
use pioneer_compaction::SourceRef;
use pioneer_compaction::frozen::FrozenMessageRef;
use pioneer_entity::{
    compaction_delivery_output, compaction_event_revision, compaction_frozen_history,
    compaction_frozen_import_data, compaction_task_output, task_delivery, turn_event,
};
use sea_orm::sea_query::{Alias, BinOper, Expr, ExprTrait, JoinType, OnConflict};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use sea_orm::{ConnectionTrait, TransactionTrait};
use sha2::{Digest, Sha256};

#[cfg(any(test, feature = "test-support"))]
static CHECKPOINT_IMPORT_GRAPH_READS: std::sync::OnceLock<
    std::sync::Mutex<
        std::collections::BTreeMap<
            (usize, String),
            std::sync::Weak<std::sync::atomic::AtomicUsize>,
        >,
    >,
> = std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-support"))]
pub struct CheckpointImportGraphReadObserver {
    key: (usize, String),
    reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(any(test, feature = "test-support"))]
impl CheckpointImportGraphReadObserver {
    pub fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for CheckpointImportGraphReadObserver {
    fn drop(&mut self) {
        CHECKPOINT_IMPORT_GRAPH_READS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn observe_checkpoint_import_graph_reads(
    db: &pioneer_postgres_pools::PostgresDatabase,
    workspace: &str,
) -> CheckpointImportGraphReadObserver {
    let key = (db.runtime_identity(), workspace.to_owned());
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let previous = CHECKPOINT_IMPORT_GRAPH_READS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key.clone(), std::sync::Arc::downgrade(&reads));
    assert!(previous.is_none(), "graph read observer already installed");
    CheckpointImportGraphReadObserver { key, reads }
}

#[cfg(any(test, feature = "test-support"))]
fn observe_checkpoint_import_graph_read(store: &CompactionStore, workspace: &str) {
    let key = (
        store.database_connection().runtime_identity(),
        workspace.to_owned(),
    );
    if let Some(reads) = CHECKPOINT_IMPORT_GRAPH_READS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
        .and_then(std::sync::Weak::upgrade)
    {
        reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

pub use crate::frozen_import::EMPTY_FROZEN_IMPORT_SHA256;
pub use crate::proof_staging::FROZEN_IMPORT_PAGE_BYTES;

use crate::frozen_import::prepare_preview_archive_guards;
pub use crate::frozen_import::{FrozenImportRecord, PreviewArchiveProjection};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryCheckpointImportSource {
    pub output_ordinal: u64,
    pub source_thread: String,
    pub source: SourceRef,
}

/// Fields are private: callers cannot mint an own claim from a delivery ID.
#[derive(Clone, Debug)]
pub struct PreparedFrozenImport {
    _frozen_hold: std::sync::Arc<crate::frozen_lifetime::FrozenReadHold>,
    workspace: String,
    destination: String,
    output_digest: String,
    original_json: String,
    archive_guards: Option<(sea_orm::Statement, sea_orm::Statement)>,
    record: FrozenImportRecord,
    accepted_basis: Option<AcceptedImportBasis>,
    /// A forwarded immutable grant may be attached to the published checkpoint
    /// that atomically replaces its accepted input. The stored record remains
    /// the original grant; this private binding only authorizes its placement
    /// in the new manifest.
    target_checkpoint: Option<(String, SourceRef)>,
}

#[derive(Clone, Debug)]
struct AcceptedImportBasis {
    turn: String,
    history_json: String,
    history_sha256: Vec<u8>,
    history_bytes: i64,
    manifest: String,
    digest: String,
    imports_digest: String,
    import_count: u64,
    ordinal: i64,
    proof_json: String,
}
impl PreparedFrozenImport {
    pub fn source(&self) -> &SourceRef {
        self.record.render_source()
    }
    pub fn source_thread(&self) -> &str {
        &self.record.source_thread
    }
    pub fn estimated_write_bytes(&self, target: &FrozenMessageRef) -> Result<usize> {
        Ok(self.original_json.len()
            + self
                .accepted_basis
                .as_ref()
                .map_or(0, |b| b.history_json.len() + b.proof_json.len())
            + serde_json::to_vec(&self.record)?.len()
            + serde_json::to_vec(target)?.len()
            + 64)
    }
    pub(crate) fn record_at(&self, message: u64) -> FrozenImportRecord {
        let mut record = self.record.clone();
        record.message_ordinal = message;
        record
    }
}

pub fn frozen_import_identity(imports: &[(u64, PreparedFrozenImport)]) -> Result<String> {
    let mut digest = Sha256::new();
    for (message, prepared) in imports {
        let bytes = serde_json::to_vec(&prepared.record_at(*message))?;
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    Ok(hex::encode(digest.finalize()))
}

#[allow(clippy::too_many_arguments)]
pub async fn compaction_prepare_frozen_import(
    store: &CompactionStore,
    workspace: &str,
    destination: &str,
    delivery: &str,
    acknowledgement: &SourceRef,
    output_ordinal: u64,
    source_thread: &str,
    source: &SourceRef,
) -> Result<PreparedFrozenImport> {
    let snapshot = store
        .compaction_delivery_output(workspace, delivery)
        .await?
        .ok_or_else(|| anyhow::anyhow!("accepted output binding is unavailable"))?;
    let frozen_hold = std::sync::Arc::new(
        store
            .compaction_acquire_frozen_history(workspace, &snapshot.output.history)
            .await?,
    );
    let acknowledged = task_delivery::Entity::find_by_id(delivery)
        .select_only()
        .column(task_delivery::Column::Id)
        .join(
            JoinType::InnerJoin,
            task_delivery::Entity::belongs_to(turn_event::Entity)
                .from(task_delivery::Column::DeliveredTurnId)
                .to(turn_event::Column::TurnId)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn_event::Entity::belongs_to(compaction_event_revision::Entity)
                .from(turn_event::Column::Id)
                .to(compaction_event_revision::Column::SourceId)
                .into(),
        )
        .filter(task_delivery::Column::WorkspaceId.eq(workspace))
        .filter(task_delivery::Column::TargetThreadId.eq(destination))
        .filter(task_delivery::Column::Status.eq("delivered"))
        .filter(
            Expr::col((turn_event::Entity, turn_event::Column::ThreadId)).eq(Expr::col((
                task_delivery::Entity,
                task_delivery::Column::TargetThreadId,
            ))),
        )
        .filter(
            Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::TurnId,
            ))
            .eq(Expr::col((turn_event::Entity, turn_event::Column::TurnId))),
        )
        .filter(compaction_event_revision::Column::Present.eq(1_i64))
        .filter(
            Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::ProjectionRevision,
            ))
            .eq(Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::Revision,
            ))),
        )
        .filter(turn_event::Column::Id.eq(acknowledgement.id.clone()))
        .filter(
            Expr::val("event:")
                .binary(
                    BinOper::Custom("||"),
                    Expr::col((turn_event::Entity, turn_event::Column::TurnId)),
                )
                .eq(acknowledgement.scope.clone()),
        )
        .filter(
            Expr::val("event-revision:")
                .binary(
                    BinOper::Custom("||"),
                    Expr::col((
                        compaction_event_revision::Entity,
                        compaction_event_revision::Column::Revision,
                    )),
                )
                .eq(acknowledgement.version.clone()),
        )
        .filter(
            turn_event::Column::EventType.eq(pioneer_protocol::constants::events::ITEM_COMPLETED),
        )
        .filter(
            compaction_event_revision::Column::ItemId
                .eq(pioneer_protocol::task_delivery_result_item_id(delivery)),
        )
        .into_tuple::<String>()
        .one(&store.connection)
        .await?
        .is_some();
    ensure!(
        acknowledged,
        "output has no exact acknowledged destination binding"
    );
    let original_json = compaction_frozen_message::Entity::find()
        .inner_join(compaction_frozen_history::Entity)
        .select_only()
        .column(compaction_frozen_message::Column::ReferenceJson)
        .filter(
            compaction_frozen_history::Column::Id.eq(snapshot.output.history.manifest_id.clone()),
        )
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(
            compaction_frozen_history::Column::OwnerThread
                .eq(snapshot.output.source_thread.clone()),
        )
        .filter(compaction_frozen_history::Column::Ready.eq(1_i64))
        .filter(
            compaction_frozen_history::Column::IdentitySha256.eq(snapshot
                .output
                .history
                .identity_sha256
                .clone()),
        )
        .filter(compaction_frozen_message::Column::Ordinal.eq(i64::try_from(output_ordinal)?))
        .filter(compaction_frozen_message::Column::Bytes.lte(SOURCE_PAGE_BYTES as i64))
            .filter(Expr::cust("octet_length(compaction_frozen_message.reference_json)=compaction_frozen_message.bytes AND compaction_frozen_message.bytes>=0"))
        .into_tuple::<String>()
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("output origin reference is unavailable"))?;
    let original: FrozenMessageRef = serde_json::from_str(&original_json)?;
    original.validate()?;
    ensure!(
        !original.inherited
            && original.complete
            && !original.protected_input
            && original
                .context_thread
                .as_deref()
                .unwrap_or(&original.source_thread)
                == snapshot.output.source_thread,
        "inherited or unfinished work cannot become accepted own work"
    );
    // An independently published summary is the imported source, not a grant
    // to extract arbitrary raw leaves from its historical coverage. Direct raw
    // references remain exact-current.
    let archive = store
        .html_preview_archived_frozen_result(workspace, &original)
        .await?;
    let projection = archive
        .as_ref()
        .filter(|(_, _, current)| current == source && source_thread == original.source_thread)
        .map(|(_, hash, current)| PreviewArchiveProjection {
            source: current.clone(),
            wire_sha256: hash.clone(),
        });
    let mut found = None;
    for reference in &original.sources {
        if projection.is_some()
            && original.sources.len() == 1
            && reference.scope.starts_with("context:")
        {
            found = Some(reference.clone());
            break;
        }
        let thread = store
            .compaction_reference_thread(workspace, reference)
            .await?
            .ok_or_else(|| anyhow::anyhow!("output source changed"))?;
        ensure!(
            thread == original.source_thread,
            "output source changed ownership"
        );
        if reference == source && thread == source_thread {
            found = Some(reference.clone());
        }
    }
    let grant = found
        .ok_or_else(|| anyhow::anyhow!("source is outside the accepted own output message"))?;
    let record = FrozenImportRecord {
        message_ordinal: 0,
        source_thread: source_thread.into(),
        source: grant,
        delivery_id: delivery.into(),
        candidate_id: snapshot.candidate_id,
        output_manifest: snapshot.output.history.manifest_id,
        output_ordinal,
        acknowledgement: acknowledgement.clone(),
        preview_archive: projection,
    };
    let archive_guards =
        prepare_preview_archive_guards(&store.connection, workspace, &record).await?;
    Ok(PreparedFrozenImport {
        _frozen_hold: frozen_hold,
        workspace: workspace.into(),
        destination: destination.into(),
        accepted_basis: None,
        target_checkpoint: None,
        output_digest: snapshot.output.history.identity_sha256,
        original_json,
        archive_guards,
        record,
    })
}

/// Attach exact immutable grants from one delivered Task output to the
/// published checkpoint that replaces precisely those output sources. This is
/// deliberately separate from accepted TaskRun-basis forwarding: authority
/// comes from the delivery acknowledgement and immutable output manifest.
#[allow(clippy::too_many_arguments)]
pub async fn compaction_prepare_delivery_checkpoint_imports(
    store: &CompactionStore,
    workspace: &str,
    destination: &str,
    delivery: &str,
    acknowledgement: &SourceRef,
    sources: &[DeliveryCheckpointImportSource],
    checkpoint_thread: &str,
    checkpoint: &SourceRef,
) -> Result<Vec<PreparedFrozenImport>> {
    ensure!(
        !sources.is_empty(),
        "delivery checkpoint import set is empty"
    );
    ensure!(
        sources.len() <= 65_536,
        "delivery checkpoint import set exceeds supported quantum"
    );
    let unique = sources
        .iter()
        .map(|source| {
            (
                source.output_ordinal,
                source.source_thread.clone(),
                source.source.clone(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    ensure!(
        unique.len() == sources.len(),
        "delivery checkpoint imports are duplicated"
    );
    let mut prepared = Vec::with_capacity(sources.len());
    for source in sources {
        prepared.push(
            compaction_prepare_frozen_import(
                store,
                workspace,
                destination,
                delivery,
                acknowledgement,
                source.output_ordinal,
                &source.source_thread,
                &source.source,
            )
            .await?,
        );
    }
    let wanted = prepared
        .iter()
        .map(|import| {
            (
                import.record.source_thread.clone(),
                import.record.render_source().clone(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    ensure!(
        checkpoint_historically_contains(
            store,
            workspace,
            checkpoint_thread,
            checkpoint,
            &wanted,
            true,
        )
        .await?,
        "delivery checkpoint exceeds its accepted output grants"
    );
    for import in &mut prepared {
        import.target_checkpoint = Some((checkpoint_thread.into(), checkpoint.clone()));
    }
    Ok(prepared)
}

/// Forward only evidence from the exact TaskRun basis accepted by this child.
/// Preparation reads immutable reference/import metadata outside the writer.
/// Publication revalidates the TaskRun binding, ready digest, exact proof and
/// target reference. Direct raw forwarding also remains exact-current.
pub async fn compaction_prepare_accepted_import(
    store: &CompactionStore,
    workspace: &str,
    destination: &str,
    turn: &str,
    ordinal: u64,
) -> Result<PreparedFrozenImport> {
    let prepared = prepare_accepted_import(store, workspace, destination, turn, ordinal).await?;
    ensure!(
        accepted_import_current(&store.connection, &prepared).await?,
        "accepted import binding changed"
    );
    Ok(prepared)
}

/// Carry an immutable accepted raw grant onto the checkpoint that replaces it.
/// This validates only the frozen TaskRun binding and saved checkpoint
/// coverage; the covered raw row is deliberately not required to remain live.
pub async fn compaction_prepare_accepted_checkpoint_import(
    store: &CompactionStore,
    workspace: &str,
    destination: &str,
    turn: &str,
    ordinal: u64,
    checkpoint_thread: &str,
    checkpoint: &SourceRef,
) -> Result<PreparedFrozenImport> {
    let mut prepared = compaction_prepare_accepted_checkpoint_imports(
        store,
        workspace,
        destination,
        turn,
        std::slice::from_ref(&ordinal),
        checkpoint_thread,
        checkpoint,
    )
    .await?;
    Ok(prepared.pop().expect("one requested import"))
}

/// Prepare every immutable grant for one checkpoint projection. Historical
/// metadata is traversed once for the target; individual ordinals are matched
/// against the resulting exact, scoped member set.
pub async fn compaction_prepare_accepted_checkpoint_imports(
    store: &CompactionStore,
    workspace: &str,
    destination: &str,
    turn: &str,
    ordinals: &[u64],
    checkpoint_thread: &str,
    checkpoint: &SourceRef,
) -> Result<Vec<PreparedFrozenImport>> {
    ensure!(!ordinals.is_empty(), "checkpoint import set is empty");
    ensure!(
        ordinals.len() <= 65_536,
        "checkpoint import set exceeds supported quantum"
    );
    let unique = ordinals
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    ensure!(
        unique.len() == ordinals.len(),
        "checkpoint import ordinals are duplicated"
    );
    let mut prepared = Vec::with_capacity(ordinals.len());
    for ordinal in ordinals {
        let import = prepare_accepted_import(store, workspace, destination, turn, *ordinal).await?;
        ensure!(
            accepted_import_binding_current(&store.connection, &import).await?,
            "accepted checkpoint replacement binding changed"
        );
        prepared.push(import);
    }
    let wanted = prepared
        .iter()
        .map(|import| {
            (
                import.record.source_thread.clone(),
                import.record.render_source().clone(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    ensure!(
        checkpoint_historically_contains(
            store,
            workspace,
            checkpoint_thread,
            checkpoint,
            &wanted,
            false,
        )
        .await?,
        "accepted checkpoint replacement binding changed"
    );
    for import in &mut prepared {
        import.target_checkpoint = Some((checkpoint_thread.into(), checkpoint.clone()));
    }
    Ok(prepared)
}

async fn prepare_accepted_import(
    store: &CompactionStore,
    workspace: &str,
    destination: &str,
    turn: &str,
    ordinal: u64,
) -> Result<PreparedFrozenImport> {
    let basis = store
        .compaction_task_basis_snapshot(workspace, destination, turn)
        .await?
        .ok_or_else(|| anyhow::anyhow!("accepted Task basis is unavailable"))?;
    let descriptor: pioneer_compaction::frozen::FrozenHistoryRef =
        serde_json::from_str(&basis.history_json)?;
    let frozen_hold = std::sync::Arc::new(
        store
            .compaction_acquire_frozen_history(workspace, &descriptor)
            .await?,
    );
    let (import_count, imports_digest) = store
        .compaction_frozen_import_state(workspace, &basis.parent_thread, &descriptor.manifest_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("accepted import manifest is not ready"))?;
    ensure!(
        ordinal < import_count,
        "accepted import ordinal is outside the manifest"
    );
    let ordinal = i64::try_from(ordinal)?;
    let proof_json =
        compaction_frozen_import::Entity::find_by_id((descriptor.manifest_id.clone(), ordinal))
            .select_only()
            .column(compaction_frozen_import::Column::ProofJson)
            .filter(compaction_frozen_import::Column::Bytes.lte(SOURCE_PAGE_BYTES as i64))
            .filter(Expr::cust("octet_length(proof_json)=bytes AND bytes>=0"))
            .into_tuple::<String>()
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted import is unavailable"))?;
    let record: FrozenImportRecord = serde_json::from_str(&proof_json)?;
    let archive_guards =
        prepare_preview_archive_guards(&store.connection, workspace, &record).await?;
    Ok(PreparedFrozenImport {
        _frozen_hold: frozen_hold,
        workspace: workspace.into(),
        destination: destination.into(),
        output_digest: String::new(),
        original_json: String::new(),
        archive_guards,
        record,
        accepted_basis: Some(AcceptedImportBasis {
            turn: turn.into(),
            // Hash original bytes before entering any writer transaction.
            history_sha256: Sha256::digest(basis.history_json.as_bytes()).to_vec(),
            history_bytes: i64::try_from(basis.history_json.len())?,
            history_json: basis.history_json,
            manifest: descriptor.manifest_id,
            digest: descriptor.identity_sha256,
            imports_digest,
            import_count,
            ordinal,
            proof_json,
        }),
        target_checkpoint: None,
    })
}

async fn accepted_import_current<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedFrozenImport,
) -> Result<bool> {
    if let Some((current, _)) = &prepared.archive_guards {
        return Ok(accepted_import_binding_current(db, prepared).await?
            && db.query_one_raw(current.clone()).await?.is_some());
    }
    Ok(db
        .query_one_raw(accepted_import_current_statement(
            ACCEPTED_IMPORT_CURRENT_SQL,
            prepared,
        )?)
        .await?
        .is_some())
}

async fn accepted_import_binding_current<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedFrozenImport,
) -> Result<bool> {
    Ok(db
        .query_one_raw(accepted_import_current_statement(
            ACCEPTED_IMPORT_BINDING_CURRENT_SQL,
            prepared,
        )?)
        .await?
        .is_some())
}

/// Check saved coverage only. The target root and any checkpoint used as an
/// atomic grant must still be published objects; expanded predecessors and raw
/// leaves are historical metadata, not live source dependencies.
async fn checkpoint_historically_contains(
    store: &CompactionStore,
    workspace: &str,
    checkpoint_thread: &str,
    checkpoint: &SourceRef,
    wanted: &std::collections::BTreeSet<(String, SourceRef)>,
    exact: bool,
) -> Result<bool> {
    if compaction_checkpoint_source(
        &store.connection,
        workspace,
        checkpoint_thread,
        &checkpoint.id,
    )
    .await?
    .as_ref()
        != Some(checkpoint)
    {
        anyhow::bail!("checkpoint replacement root is unavailable");
    }
    let mut pending = vec![(checkpoint.clone(), checkpoint_thread.to_owned(), false)];
    let mut done = std::collections::BTreeSet::new();
    let mut visiting = std::collections::BTreeSet::new();
    let mut visited = 0usize;
    let mut found = std::collections::BTreeSet::new();
    while let Some((source, thread, exiting)) = pending.pop() {
        let key = (source.clone(), thread.clone());
        if exiting {
            visiting.remove(&key);
            done.insert(key);
            continue;
        }
        if done.contains(&key) {
            continue;
        }
        visited = visited
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("checkpoint graph size overflow"))?;
        ensure!(
            visited <= 65_536,
            "checkpoint historical coverage exceeds supported quantum"
        );
        if wanted.contains(&(thread.clone(), source.clone())) {
            if source.scope.starts_with("checkpoint:")
                && compaction_checkpoint_source(&store.connection, workspace, &thread, &source.id)
                    .await?
                    .as_ref()
                    != Some(&source)
            {
                return Ok(false);
            }
            found.insert((thread.clone(), source.clone()));
            done.insert(key);
            continue;
        }
        if !source.scope.starts_with("checkpoint:") {
            if exact {
                return Ok(false);
            }
            done.insert(key);
            continue;
        }
        ensure!(visiting.insert(key), "cyclic checkpoint coverage");
        #[cfg(any(test, feature = "test-support"))]
        observe_checkpoint_import_graph_read(store, workspace);
        let edges = compaction_checkpoint_edges(store, &source.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint coverage node disappeared"))?;
        ensure!(
            edges.workspace_id == workspace
                && edges.thread_id == thread
                && source.scope == format!("checkpoint:{}", edges.owner)
                && source.version == edges.identity_sha256
                && edges.format_version == pioneer_compaction::FORMAT_VERSION,
            "checkpoint historical ownership, identity or format changed"
        );
        pending.push((source, thread, true));
        if let Some(previous) = edges.previous {
            #[cfg(any(test, feature = "test-support"))]
            observe_checkpoint_import_graph_read(store, workspace);
            let previous_edges = compaction_checkpoint_edges(store, &previous)
                .await?
                .ok_or_else(|| anyhow::anyhow!("previous checkpoint disappeared"))?;
            ensure!(
                previous_edges.owner == edges.owner
                    && previous_edges.workspace_id == edges.workspace_id
                    && previous_edges.thread_id == edges.thread_id
                    && previous_edges.format_version == pioneer_compaction::FORMAT_VERSION,
                "previous checkpoint changed historical ownership or format"
            );
            pending.push((
                SourceRef {
                    scope: format!("checkpoint:{}", previous_edges.owner),
                    id: previous,
                    version: previous_edges.identity_sha256,
                },
                previous_edges.thread_id,
                false,
            ));
        }
        pending.extend(
            edges
                .coverage
                .into_iter()
                .map(|covered| (covered.source, covered.source_thread, false)),
        );
    }
    Ok(found == *wanted)
}

const ACCEPTED_IMPORT_BINDING_CURRENT_SQL: &str = r#"
SELECT 1
FROM task_run_turn execution
JOIN task_run_conversation_snapshot snapshot
  ON snapshot.run_id=execution.run_id AND snapshot.task_id=execution.task_id
JOIN thread_lineage lineage
  ON lineage.child_thread_id=execution.thread_id
 AND lineage.parent_thread_id=snapshot.conversation_thread_id
JOIN thread child
  ON child.id=execution.thread_id AND child.workspace_id=snapshot.workspace_id
JOIN compaction_frozen_history h
  ON h.id=$1
 AND h.owner_thread=snapshot.conversation_thread_id
 AND h.workspace_id=snapshot.workspace_id
JOIN compaction_frozen_import i
  ON i.manifest_id=h.id AND i.ordinal=$2
WHERE execution.thread_id=$3
  AND execution.turn_id=$4
  AND snapshot.workspace_id=$5
  AND ((snapshot.history_json_payload_id IS NULL AND snapshot.history_json=$6)
    OR (snapshot.history_json_payload_id IS NOT NULL AND EXISTS (
      SELECT 1 FROM portable_payload_value original
      WHERE original.id=snapshot.history_json_payload_id
        AND original.sha256=$7 AND original.original_bytes=$8
    )))
  AND h.ready=1 AND h.expired=0
  AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count
  AND i.ordinal>=0 AND i.ordinal<h.import_count
  AND i.message_ordinal>=0 AND i.message_ordinal<h.message_count
  AND h.identity_sha256=$9
  AND h.next_import=h.import_count
  AND i.proof_json=$10
  AND h.imports_sha256=$11
  AND h.import_count=$12
LIMIT 1
"#;

// This is the existence-only expansion of the source branches in
// compaction_live_sources. Keep each branch's predicates in sync with the view.
// The correlated LIMIT 1 keeps each PostgreSQL primary-key probe inside
// its source branch. A flattened join can scan unrelated revisions.
const ACCEPTED_IMPORT_CURRENT_SQL: &str = r#"
SELECT 1
FROM task_run_turn execution
JOIN task_run_conversation_snapshot snapshot
  ON snapshot.run_id=execution.run_id AND snapshot.task_id=execution.task_id
JOIN thread_lineage lineage
  ON lineage.child_thread_id=execution.thread_id
 AND lineage.parent_thread_id=snapshot.conversation_thread_id
JOIN thread child
  ON child.id=execution.thread_id AND child.workspace_id=snapshot.workspace_id
JOIN compaction_frozen_history h
  ON h.id=$1
 AND h.owner_thread=snapshot.conversation_thread_id
 AND h.workspace_id=snapshot.workspace_id
JOIN compaction_frozen_import i
  ON i.manifest_id=h.id AND i.ordinal=$2
WHERE execution.thread_id=$3
  AND execution.turn_id=$4
  AND snapshot.workspace_id=$5
  AND ((snapshot.history_json_payload_id IS NULL AND snapshot.history_json=$6)
    OR (snapshot.history_json_payload_id IS NOT NULL AND EXISTS (
      SELECT 1 FROM portable_payload_value original
      WHERE original.id=snapshot.history_json_payload_id
        AND original.sha256=$7 AND original.original_bytes=$8
    )))
  AND h.ready=1 AND h.expired=0
  AND h.message_count>=0 AND h.import_count>=0 AND h.next_ordinal=h.message_count
  AND i.ordinal>=0 AND i.ordinal<h.import_count
  AND i.message_ordinal>=0 AND i.message_ordinal<h.message_count
  AND h.identity_sha256=$9
  AND h.next_import=h.import_count
  AND i.proof_json=$10
  AND h.imports_sha256=$11
  AND h.import_count=$12
  AND (
    EXISTS (
      SELECT 1
      FROM LATERAL (
        SELECT source_id, turn_id, revision, present
        FROM compaction_source_revision context_revision
        WHERE context_revision.source_id=i.source_id
        LIMIT 1
      ) current_context_revision
      JOIN LATERAL (
        SELECT id, turn_id FROM turn_llm_context context_source
        WHERE context_source.id=current_context_revision.source_id
        LIMIT 1
      ) current_context_source ON current_context_source.turn_id=current_context_revision.turn_id
      JOIN turn context_turn ON context_turn.id=current_context_revision.turn_id
      JOIN thread context_thread ON context_thread.id=context_turn.thread_id
      WHERE current_context_revision.present=1
        AND context_thread.workspace_id=h.workspace_id
        AND context_turn.thread_id=i.source_thread
        AND 'context:'||current_context_revision.turn_id=i.source_scope
        AND 'revision:'||current_context_revision.revision=i.source_version
    )
    OR EXISTS (
      SELECT 1
      FROM LATERAL (
        SELECT source_id, turn_id, revision, present
        FROM compaction_item_revision item_revision
        WHERE item_revision.source_id=i.source_id
        LIMIT 1
      ) current_item_revision
      JOIN LATERAL (
        SELECT id, turn_id FROM turn_item item_source
        WHERE item_source.id=current_item_revision.source_id
        LIMIT 1
      ) current_item_source ON current_item_source.turn_id=current_item_revision.turn_id
      JOIN turn item_turn ON item_turn.id=current_item_revision.turn_id
      JOIN thread item_thread ON item_thread.id=item_turn.thread_id
      WHERE current_item_revision.present=1
        AND item_thread.workspace_id=h.workspace_id
        AND item_turn.thread_id=i.source_thread
        AND 'item:'||current_item_revision.turn_id=i.source_scope
        AND 'item-revision:'||current_item_revision.revision=i.source_version
    )
    OR EXISTS (
      SELECT 1
      FROM LATERAL (
        SELECT source_id, turn_id, revision, present
        FROM compaction_event_revision event_revision
        WHERE event_revision.source_id=i.source_id
        LIMIT 1
      ) current_event_revision
      JOIN LATERAL (
        SELECT id, turn_id FROM turn_event event_source
        WHERE event_source.id=current_event_revision.source_id
        LIMIT 1
      ) current_event_source ON current_event_source.turn_id=current_event_revision.turn_id
      JOIN turn event_turn ON event_turn.id=current_event_revision.turn_id
      JOIN thread event_thread ON event_thread.id=event_turn.thread_id
      WHERE current_event_revision.present=1
        AND event_thread.workspace_id=h.workspace_id
        AND event_turn.thread_id=i.source_thread
        AND 'event:'||current_event_revision.turn_id=i.source_scope
        AND 'event-revision:'||current_event_revision.revision=i.source_version
    )
    OR EXISTS (
      SELECT 1
      FROM LATERAL (
        SELECT source_id, turn_id, revision, present
        FROM compaction_input_revision input_revision
        WHERE input_revision.source_id=i.source_id
        LIMIT 1
      ) current_input_revision
      JOIN LATERAL (
        SELECT id, turn_id FROM turn_input input_source
        WHERE input_source.id=current_input_revision.source_id
        LIMIT 1
      ) current_input_source ON current_input_source.turn_id=current_input_revision.turn_id
      JOIN turn input_turn ON input_turn.id=current_input_revision.turn_id
      JOIN thread input_thread ON input_thread.id=input_turn.thread_id
      WHERE current_input_revision.present=1
        AND input_thread.workspace_id=h.workspace_id
        AND input_turn.thread_id=i.source_thread
        AND 'input:'||current_input_revision.turn_id=i.source_scope
        AND 'input-revision:'||current_input_revision.revision=i.source_version
    )
    OR EXISTS (
      SELECT 1
      FROM LATERAL (
        SELECT id, owner, operation_id, status, format_version, identity_sha256
        FROM compaction_checkpoint checkpoint
        WHERE checkpoint.id=i.source_id
        LIMIT 1
      ) current_checkpoint
      JOIN compaction_context context ON context.owner=current_checkpoint.owner
      WHERE (
          current_checkpoint.status='applied'
          OR (
            current_checkpoint.status='retained'
            AND EXISTS (
              SELECT 1
              FROM compaction_operation committed
              WHERE committed.id=current_checkpoint.operation_id
                AND committed.status='completed'
            )
          )
        )
        AND context.workspace_id=h.workspace_id
        AND context.thread_id=i.source_thread
        AND 'checkpoint:'||current_checkpoint.owner=i.source_scope
        AND current_checkpoint.identity_sha256=i.source_version
        AND current_checkpoint.format_version=1
    )
    OR EXISTS (
      SELECT 1
      FROM LATERAL (
        SELECT run_id, workspace_id, conversation_thread_id, history_json_has_array_prefix
        FROM task_run_conversation_snapshot basis
        WHERE basis.run_id=i.source_id
        LIMIT 1
      ) current_basis
      JOIN thread basis_thread
        ON basis_thread.id=current_basis.conversation_thread_id
       AND basis_thread.workspace_id=current_basis.workspace_id
      LEFT JOIN compaction_task_basis_revision revision ON revision.run_id=current_basis.run_id
      WHERE current_basis.history_json_has_array_prefix
        AND current_basis.workspace_id=h.workspace_id
        AND current_basis.conversation_thread_id=i.source_thread
        AND 'task-basis:'||current_basis.run_id=i.source_scope
        AND 'task-basis-revision:'||COALESCE(revision.revision,1)=i.source_version
    )
  )
LIMIT 1
"#;

fn accepted_import_current_statement(
    sql: &str,
    prepared: &PreparedFrozenImport,
) -> Result<sea_orm::Statement> {
    let basis = prepared
        .accepted_basis
        .as_ref()
        .expect("accepted import proof");
    // The set predicate mirrors the checked frozen-source view and compares
    // the original digest rather than decoding a body inside publication.
    Ok(sea_orm::Statement::from_sql_and_values(
        sea_orm::DbBackend::Postgres,
        sql,
        [
            basis.manifest.clone().into(),
            basis.ordinal.into(),
            prepared.destination.clone().into(),
            basis.turn.clone().into(),
            prepared.workspace.clone().into(),
            basis.history_json.clone().into(),
            basis.history_sha256.clone().into(),
            basis.history_bytes.into(),
            basis.digest.clone().into(),
            basis.proof_json.clone().into(),
            basis.imports_digest.clone().into(),
            i64::try_from(basis.import_count)?.into(),
        ],
    ))
}

pub async fn compaction_append_frozen_imports(
    store: &CompactionStore,
    workspace: &str,
    owner: &str,
    manifest: &str,
    start: u64,
    imports: &[(u64, PreparedFrozenImport)],
) -> Result<()> {
    ensure!(
        imports.len() as u64 <= SOURCE_PAGE_ROWS,
        "import batch row limit"
    );
    let mut batch = Vec::new();
    let mut bytes = 0;
    for (index, (message, prepared)) in imports.iter().enumerate() {
        ensure!(
            prepared.workspace == workspace && prepared.destination == owner,
            "import preparation scope mismatch"
        );
        let target_json = compaction_frozen_message::Entity::find()
            .inner_join(compaction_frozen_history::Entity)
            .select_only()
            .column(compaction_frozen_message::Column::ReferenceJson)
            .filter(compaction_frozen_history::Column::Id.eq(manifest))
            .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
            .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
            .filter(compaction_frozen_message::Column::Ordinal.eq(i64::try_from(*message)?))
            .filter(compaction_frozen_message::Column::Bytes.lte(SOURCE_PAGE_BYTES as i64))
            .filter(Expr::cust("octet_length(compaction_frozen_message.reference_json)=compaction_frozen_message.bytes AND compaction_frozen_message.bytes>=0"))
            .into_tuple::<String>()
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("target reference is unavailable"))?;
        let target: FrozenMessageRef = serde_json::from_str(&target_json)?;
        target.validate()?;
        let target_matches =
            if let Some((checkpoint_thread, checkpoint)) = &prepared.target_checkpoint {
                target.source_thread == *checkpoint_thread
                    && target.sources.as_slice() == std::slice::from_ref(checkpoint)
            } else {
                prepared.record.matches_render_target(&target)
            };
        ensure!(
            !target.inherited
                && target.complete
                && !target.protected_input
                && target
                    .context_thread
                    .as_deref()
                    .unwrap_or(&target.source_thread)
                    == owner
                && target_matches,
            "target message is not this accepted own projection"
        );
        let record = prepared.record_at(*message);
        let json = serde_json::to_string(&record)?;
        bytes += prepared.estimated_write_bytes(&target)?;
        ensure!(
            bytes <= FROZEN_IMPORT_PAGE_BYTES && json.len() <= SOURCE_PAGE_BYTES,
            "import batch byte limit"
        );
        batch.push((
            i64::try_from(
                start
                    .checked_add(index as u64)
                    .ok_or_else(|| anyhow::anyhow!("import ordinal overflow"))?,
            )?,
            record,
            prepared,
            target_json,
            json,
        ));
    }
    let tx = store.connection.begin().await?;
    let row = compaction_frozen_history::Entity::find_by_id(manifest)
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("import target manifest is unavailable"))?;
    crate::frozen::logical_bounds(&row)?;
    let ready = row.ready == 1;
    let count = row.import_count;
    let next = row.next_import;
    let start = i64::try_from(start)?;
    let end = start
        .checked_add(i64::try_from(batch.len())?)
        .ok_or_else(|| anyhow::anyhow!("import ordinal overflow"))?;
    ensure!(
        start <= next && end <= count && (start == next || end <= next),
        "imports are not a sequential batch or exact retry"
    );
    for (ordinal, record, prepared, target_json, json) in &batch {
        if !ready && *ordinal >= next {
            if let Some((current, historical)) = &prepared.archive_guards {
                let query =
                    if prepared.accepted_basis.is_some() && prepared.target_checkpoint.is_some() {
                        historical
                    } else {
                        current
                    };
                ensure!(
                    tx.query_one_raw(query.clone()).await?.is_some(),
                    "archival preview import changed before publication"
                );
            }
            // The existing transaction holds the validated dependency snapshot
            // through insertion; exact retries below retain their prior behavior.
            let forwarded = if prepared.accepted_basis.is_some() {
                let binding_current =
                    if let Some((checkpoint_thread, checkpoint)) = &prepared.target_checkpoint {
                        accepted_import_binding_current(&tx, prepared).await?
                            && compaction_checkpoint_source(
                                &tx,
                                workspace,
                                checkpoint_thread,
                                &checkpoint.id,
                            )
                            .await?
                            .as_ref()
                                == Some(checkpoint)
                            // A checkpoint used as the accepted atomic grant is
                            // a direct dependency of this write. Recheck that
                            // object only; its historical raw coverage remains
                            // outside the bounded writer transaction.
                            && (!record.source.scope.starts_with("checkpoint:")
                                || compaction_checkpoint_source(
                                    &tx,
                                    workspace,
                                    &record.source_thread,
                                    &record.source.id,
                                )
                                .await?
                                .as_ref()
                                    == Some(&record.source))
                    } else {
                        accepted_import_current(&tx, prepared).await?
                    };
                binding_current
                    && compaction_frozen_message::Entity::find_by_id((
                        manifest.to_owned(),
                        i64::try_from(record.message_ordinal)?,
                    ))
                    .filter(
                        compaction_frozen_message::Column::ReferenceJson.eq(target_json.clone()),
                    )
                    .one(&tx)
                    .await?
                    .is_some()
            } else {
                false
            };
            let delivery_checkpoint_current = if prepared.accepted_basis.is_none() {
                if let Some((checkpoint_thread, checkpoint)) = &prepared.target_checkpoint {
                    compaction_checkpoint_source(&tx, workspace, checkpoint_thread, &checkpoint.id)
                        .await?
                        .as_ref()
                        == Some(checkpoint)
                } else {
                    true
                }
            } else {
                false
            };
            if forwarded
                || (delivery_checkpoint_current
                    && prepared.accepted_basis.is_none()
                    && compaction_frozen_history::Entity::find()
                        .select_only()
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_frozen_history::Entity::belongs_to(
                                compaction_frozen_message::Entity,
                            )
                            .from(compaction_frozen_history::Column::Id)
                            .to(compaction_frozen_message::Column::ManifestId)
                            .into(),
                            Alias::new("target"),
                        )
                        .join(
                            JoinType::InnerJoin,
                            compaction_frozen_history::Entity::belongs_to(task_delivery::Entity)
                                .from(compaction_frozen_history::Column::WorkspaceId)
                                .to(task_delivery::Column::WorkspaceId)
                                .into(),
                        )
                        .join(
                            JoinType::InnerJoin,
                            task_delivery::Entity::belongs_to(compaction_delivery_output::Entity)
                                .from(task_delivery::Column::Id)
                                .to(compaction_delivery_output::Column::DeliveryId)
                                .into(),
                        )
                        .join(
                            JoinType::InnerJoin,
                            compaction_delivery_output::Entity::belongs_to(
                                compaction_task_output::Entity,
                            )
                            .from(compaction_delivery_output::Column::TaskRunTurnId)
                            .to(compaction_task_output::Column::TaskRunTurnId)
                            .into(),
                        )
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_task_output::Entity::belongs_to(
                                compaction_frozen_history::Entity,
                            )
                            .from(compaction_task_output::Column::ManifestId)
                            .to(compaction_frozen_history::Column::Id)
                            .into(),
                            Alias::new("original"),
                        )
                        .join_as(
                            JoinType::InnerJoin,
                            sea_orm::RelationDef::from(
                                compaction_frozen_history::Entity::belongs_to(
                                    compaction_frozen_message::Entity,
                                )
                                .from(compaction_frozen_history::Column::Id)
                                .to(compaction_frozen_message::Column::ManifestId),
                            )
                            .from_alias(Alias::new("original")),
                            Alias::new("origin"),
                        )
                        .join(
                            JoinType::InnerJoin,
                            task_delivery::Entity::belongs_to(turn_event::Entity)
                                .from(task_delivery::Column::TargetThreadId)
                                .to(turn_event::Column::ThreadId)
                                .into(),
                        )
                        .join(
                            JoinType::InnerJoin,
                            turn_event::Entity::belongs_to(compaction_event_revision::Entity)
                                .from(turn_event::Column::Id)
                                .to(compaction_event_revision::Column::SourceId)
                                .into(),
                        )
                        .join(
                            JoinType::InnerJoin,
                            compaction_live_sources::join(
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::WorkspaceId,
                                compaction_live_sources::Column::WorkspaceId,
                            ),
                        )
                        .filter(
                            Expr::col(("target", compaction_frozen_message::Column::Ordinal))
                                .eq(Expr::Value(i64::try_from(record.message_ordinal)?.into())),
                        )
                        .filter(
                            Expr::col(("target", compaction_frozen_message::Column::ReferenceJson))
                                .eq(Expr::Value(target_json.clone().into())),
                        )
                        .filter(
                            Expr::col((task_delivery::Entity, task_delivery::Column::Id))
                                .eq(Expr::Value(record.delivery_id.clone().into())),
                        )
                        .filter(
                            Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::TargetThreadId,
                            ))
                            .eq(Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::OwnerThread,
                            ))),
                        )
                        .filter(
                            Expr::col((task_delivery::Entity, task_delivery::Column::Status))
                                .eq(Expr::val("delivered")),
                        )
                        .filter(
                            Expr::col((
                                compaction_delivery_output::Entity,
                                compaction_delivery_output::Column::CandidateId,
                            ))
                            .eq(Expr::Value(record.candidate_id.clone().into())),
                        )
                        .filter(
                            Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::TaskId,
                            ))
                            .eq(Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::TaskId,
                            ))),
                        )
                        .filter(
                            Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::RunId,
                            ))
                            .eq(Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::RunId,
                            ))),
                        )
                        .filter(
                            Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::WorkspaceId,
                            ))
                            .eq(Expr::col((
                                task_delivery::Entity,
                                task_delivery::Column::WorkspaceId,
                            ))),
                        )
                        .filter(
                            Expr::col((
                                compaction_task_output::Entity,
                                compaction_task_output::Column::ManifestId,
                            ))
                            .eq(Expr::Value(record.output_manifest.clone().into())),
                        )
                        .filter(
                            Expr::col(("original", compaction_frozen_history::Column::Ready))
                                .eq(Expr::val(1_i64)),
                        )
                        .filter(
                            Expr::col((
                                "original",
                                compaction_frozen_history::Column::IdentitySha256,
                            ))
                            .eq(Expr::Value(prepared.output_digest.clone().into())),
                        )
                        .filter(
                            Expr::col(("origin", compaction_frozen_message::Column::Ordinal))
                                .eq(Expr::Value(i64::try_from(record.output_ordinal)?.into())),
                        )
                        .filter(
                            Expr::col(("origin", compaction_frozen_message::Column::ReferenceJson))
                                .eq(Expr::Value(prepared.original_json.clone().into())),
                        )
                        .filter(
                            Expr::col((turn_event::Entity, turn_event::Column::Id))
                                .eq(Expr::Value(record.acknowledgement.id.clone().into())),
                        )
                        .filter(
                            Expr::col((turn_event::Entity, turn_event::Column::TurnId)).eq(
                                Expr::col((
                                    task_delivery::Entity,
                                    task_delivery::Column::DeliveredTurnId,
                                )),
                            ),
                        )
                        .filter(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::TurnId,
                            ))
                            .eq(Expr::col((turn_event::Entity, turn_event::Column::TurnId))),
                        )
                        .filter(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::Present,
                            ))
                            .eq(Expr::val(1_i64)),
                        )
                        .filter(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::ProjectionRevision,
                            ))
                            .eq(Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::Revision,
                            ))),
                        )
                        .filter(
                            Expr::col((
                                compaction_live_sources::Column::Table,
                                compaction_live_sources::Column::SourceScope,
                            ))
                            .eq(Expr::Value(prepared.source().scope.clone().into())),
                        )
                        .filter(
                            Expr::col((
                                compaction_live_sources::Column::Table,
                                compaction_live_sources::Column::SourceId,
                            ))
                            .eq(Expr::Value(prepared.source().id.clone().into())),
                        )
                        .filter(
                            Expr::col((
                                compaction_live_sources::Column::Table,
                                compaction_live_sources::Column::SourceVersion,
                            ))
                            .eq(Expr::Value(prepared.source().version.clone().into())),
                        )
                        .filter(
                            Expr::col((
                                compaction_live_sources::Column::Table,
                                compaction_live_sources::Column::ThreadId,
                            ))
                            .eq(Expr::Value(record.source_thread.clone().into())),
                        )
                        .expr(Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::Id,
                        )))
                        .filter(
                            Expr::col((
                                compaction_frozen_history::Entity,
                                compaction_frozen_history::Column::Id,
                            ))
                            .eq(Expr::Value(manifest.into()))
                            .and(
                                Expr::col((
                                    compaction_frozen_history::Entity,
                                    compaction_frozen_history::Column::WorkspaceId,
                                ))
                                .eq(Expr::Value(workspace.into())),
                            )
                            .and(
                                Expr::col((
                                    compaction_frozen_history::Entity,
                                    compaction_frozen_history::Column::OwnerThread,
                                ))
                                .eq(Expr::Value(owner.into())),
                            )
                            .and(
                                Expr::col((
                                    compaction_frozen_history::Entity,
                                    compaction_frozen_history::Column::Ready,
                                ))
                                .eq(Expr::val(0_i64)),
                            )
                            .and(
                                Expr::val("event:")
                                    .binary(
                                        BinOper::Custom("||"),
                                        Expr::col((turn_event::Entity, turn_event::Column::TurnId)),
                                    )
                                    .eq(Expr::Value(record.acknowledgement.scope.clone().into())),
                            )
                            .and(
                                Expr::val("event-revision:")
                                    .binary(
                                        BinOper::Custom("||"),
                                        Expr::col((
                                            compaction_event_revision::Entity,
                                            compaction_event_revision::Column::Revision,
                                        )),
                                    )
                                    .eq(Expr::Value(record.acknowledgement.version.clone().into())),
                            )
                            .and(
                                Expr::col((turn_event::Entity, turn_event::Column::EventType)).eq(
                                    Expr::Value(
                                        pioneer_protocol::constants::events::ITEM_COMPLETED.into(),
                                    ),
                                ),
                            )
                            .and(
                                Expr::col((
                                    compaction_event_revision::Entity,
                                    compaction_event_revision::Column::ItemId,
                                ))
                                .eq(Expr::Value(
                                    pioneer_protocol::task_delivery_result_item_id(
                                        &record.delivery_id,
                                    )
                                    .into(),
                                )),
                            ),
                        )
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                        .is_some())
            {
                let source =
                    crate::frozen_storage::append_source(&tx, manifest, 1, *ordinal).await?;
                compaction_frozen_import_data::Entity::insert(
                    compaction_frozen_import_data::ActiveModel {
                        manifest_id: sea_orm::Set(source),
                        ordinal: sea_orm::Set(*ordinal),
                        message_ordinal: sea_orm::Set(i64::try_from(record.message_ordinal)?),
                        source_scope: sea_orm::Set(record.source.scope.clone()),
                        source_id: sea_orm::Set(record.source.id.clone()),
                        source_version: sea_orm::Set(record.source.version.clone()),
                        source_thread: sea_orm::Set(record.source_thread.clone()),
                        proof_json: sea_orm::Set(json.clone()),
                        bytes: sea_orm::Set(i64::try_from(json.len())?),
                    },
                )
                .on_conflict(
                    OnConflict::columns([
                        compaction_frozen_import_data::Column::ManifestId,
                        compaction_frozen_import_data::Column::Ordinal,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .exec_without_returning(&tx)
                .await?;
            }
        }
    }
    if start == next && !ready {
        compaction_frozen_history::Entity::update_many()
            .col_expr(
                compaction_frozen_history::Column::NextImport,
                Expr::Value(end.into()),
            )
            .filter(
                Expr::col(compaction_frozen_history::Column::Id)
                    .eq(Expr::Value(manifest.into()))
                    .and(
                        Expr::col(compaction_frozen_history::Column::NextImport)
                            .eq(Expr::Value(next.into())),
                    )
                    .and(Expr::col(compaction_frozen_history::Column::Ready).eq(Expr::val(0_i64))),
            )
            .exec(&tx)
            .await?;
    }
    for (ordinal, _, _, _, json) in &batch {
        ensure!(
            compaction_frozen_import::Entity::find()
                .select_only()
                .expr(Expr::val(1_i64))
                .filter(
                    Expr::col(compaction_frozen_import::Column::ManifestId)
                        .eq(Expr::Value(manifest.into()))
                        .and(
                            Expr::col(compaction_frozen_import::Column::Ordinal)
                                .eq(Expr::Value((*ordinal).into()))
                        )
                        .and(
                            Expr::col(compaction_frozen_import::Column::ProofJson)
                                .eq(Expr::Value(json.clone().into()))
                        )
                        .and(
                            Expr::col(compaction_frozen_import::Column::Bytes)
                                .eq(Expr::val(i64::try_from(json.len())?))
                        )
                )
                .into_tuple::<i64>()
                .one(&tx)
                .await?
                .is_some(),
            "import binding changed or retry changed immutable metadata"
        );
    }
    tx.commit().await?;
    Ok(())
}

pub async fn compaction_frozen_import_state<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    owner: &str,
    manifest: &str,
) -> Result<Option<(u64, String)>> {
    if let Some(h) = compaction_frozen_history::Entity::find_by_id(manifest)
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
        .one(db)
        .await?
    {
        crate::frozen::logical_bounds(&h)?;
    }
    use sea_orm::ColumnTrait;
    let row = compaction_frozen_history::Entity::find_by_id(manifest)
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
        .filter(compaction_frozen_history::Column::Ready.eq(1_i64))
        .filter(
            Expr::col(compaction_frozen_history::Column::ImportCount)
                .eq(Expr::col(compaction_frozen_history::Column::NextImport)),
        )
        .one(db)
        .await?;
    row.map(|row| Ok((u64::try_from(row.import_count)?, row.imports_sha256)))
        .transpose()
}

pub use crate::frozen_import::compaction_frozen_import_page;

use crate::live_sources as compaction_live_sources;

#[cfg(test)]
#[path = "compaction_frozen_import_tests.rs"]
mod tests;
