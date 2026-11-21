//! Durable compaction metadata. Payloads remain in canonical history tables.
pub(crate) use super::compaction_background as background;
pub(crate) use super::compaction_frozen as frozen;
pub(crate) use super::compaction_frozen_import as frozen_import;
pub(crate) use super::compaction_history as history;
pub(crate) use super::compaction_lifecycle as lifecycle;
pub(crate) use super::compaction_runner as runner;
pub(crate) use super::compaction_source_projection as source_projection;
pub(crate) use super::compaction_task_output as task_output;

pub use pioneer_crud_conversations::context_cleanup::{
    MODEL_HISTORY_CONTEXT_SOURCES, model_history_context_source,
};

pub use super::compaction_lifecycle_pending::{
    CompactionLifecycleCandidate, CompactionLifecycleClaim, CompactionLifecycleStorageError,
};
use crate::CrudStore;
use anyhow::{Context, Result, ensure};
pub use background::CompletedHistoryCheck;
#[cfg(any(test, feature = "test-support"))]
pub use frozen_import::{CheckpointImportGraphReadObserver, observe_checkpoint_import_graph_reads};
pub use frozen_import::{
    DeliveryCheckpointImportSource, EMPTY_FROZEN_IMPORT_SHA256, FROZEN_IMPORT_PAGE_BYTES,
    FrozenImportRecord, PreparedFrozenImport, PreviewArchiveProjection, frozen_import_identity,
};
pub use history::{
    AcceptedTaskBasis, ChildHistoryParent, HistoryCausalBoundary, HistoryReadFence,
    HistoryTurnBoundary, TaskInputCopyAlias, event_projection_metadata,
};
pub use lifecycle::PreparedCompactionLifecycle;
use pioneer_compaction::{Checkpoint, FORMAT_VERSION, SourceRef};
use pioneer_entity::{
    compaction_checkpoint, compaction_context, compaction_coverage, compaction_event_revision,
    compaction_execution_stop, compaction_input_revision, compaction_item_revision,
    compaction_operation, compaction_operation_projection, compaction_source_revision,
    task_run_conversation_snapshot, thread, turn, turn_event, turn_input, turn_item,
    turn_llm_context,
};
pub use runner::{ManifestEntry, RunnerPlanRecord};
#[cfg(any(test, feature = "test-support"))]
pub use runner::{
    PublicationTestHookHandle, PublicationTestPause, arm_publication_test_hook,
    trigger_publication_test_hook,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, JoinType, OnConflict, Query};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use sha2::{Digest, Sha256};
pub(crate) use task_output::bind_queued_task_output;
pub use task_output::{
    DeliveredTaskOutputCursor, DeliveredTaskOutputEvent, DeliveredTaskOutputPage,
    DeliveredTaskOutputRef, TaskDeliveryOutputSnapshot, TaskOutputSnapshot,
};

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

pub use pioneer_crud_compaction::checkpoint_projection::{CheckpointBody, CheckpointMetadata};
pub(crate) use pioneer_crud_compaction::checkpoint_projection::{
    compaction_checkpoint_body, compaction_checkpoint_metadata,
};

pub use pioneer_crud_compaction::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS};
pub const CHECKPOINT_SOURCE_LIMIT: usize = pioneer_compaction::REPLAY_ALIAS_LIMIT;

pub use pioneer_crud_compaction::canonical_history::{
    CanonicalFragment, PagedSource, SourcePage, SourceRecord,
};
pub use pioneer_crud_compaction::runner_writes::CanonicalSource;

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

pub use pioneer_crud_compaction::operations::OperationRecord;

#[derive(Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Applied,
    AlreadyApplied,
    Stale,
    Cancelled,
}

pub(super) use pioneer_crud_compaction::runner_writes::checkpoint_identity;

pub(crate) use pioneer_crud_compaction::canonical_history::{
    compaction_context_reference_for_item, compaction_item_created_at, compaction_item_reference,
    compaction_projection_version, compaction_reference_fragment, compaction_reference_payload,
    compaction_reference_payload_batch, compaction_replay_item_id, compaction_source_fragment,
    compaction_source_high_water, compaction_source_metadata_page,
    compaction_source_metadata_page_at_fence, compaction_source_page,
    compaction_source_page_inner_bounded, compaction_tool_result_fragment,
    compaction_tool_result_id,
};
pub(crate) use pioneer_crud_compaction::tool_sources::compaction_tool_item_reference;

pub(crate) use pioneer_crud_compaction::operations::{
    compaction_admit, compaction_admit_for_turn, compaction_invalid_completion_progress,
    compaction_operation, compaction_operation_for_plan,
};

pub use pioneer_crud_compaction::source_revision::HistoricalEventProjection;
pub(crate) use pioneer_crud_compaction::source_revision::{
    compaction_checkpoint_source, compaction_historical_event_projections,
    compaction_model_context_sources_current, compaction_reference_thread,
    compaction_sources_current,
};

pub(crate) use pioneer_crud_compaction::operations::{
    compaction_claim_attempt, compaction_finish, compaction_head,
};

/// Saves complete intermediate results without publishing a working pointer.
pub(crate) async fn compaction_save_candidate(
    store: &CrudStore,
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
    let raw_ownership = super::checkpoint_proofs::prepare_raw_ownership(store, checkpoint).await?;
    let coverage: Vec<_> = checkpoint
        .coverage
        .iter()
        .map(|source| compaction_coverage::ActiveModel {
            checkpoint_id: sea_orm::Set(checkpoint.id.clone()),
            source_scope: sea_orm::Set(source.scope.clone()),
            source_id: sea_orm::Set(source.id.clone()),
            source_version: sea_orm::Set(source.version.clone()),
        })
        .collect();
    // The payload and coverage are prepared outside capacity. The running operation is revalidated below.
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            let running = compaction_operation::Entity::find()
                .select_only()
                .expr(Expr::col(compaction_operation::Column::Id))
                .filter(
                    Expr::col(compaction_operation::Column::Id)
                        .eq(Expr::Value(checkpoint.operation_id.clone().into()))
                        .and(
                            Expr::col(compaction_operation::Column::Owner)
                                .eq(Expr::Value(checkpoint.owner.clone().into())),
                        )
                        .and(
                            Expr::col(compaction_operation::Column::Status)
                                .eq(Expr::val("running")),
                        ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            ensure!(running.is_some(), "operation is not running");
            if let Some(rows) = &raw_ownership {
                super::checkpoint_proofs::write_raw_ownership(&txn, checkpoint, rows).await?;
            }
            compaction_checkpoint::Entity::insert(compaction_checkpoint::ActiveModel {
                proof_version: sea_orm::Set(0),
                id: sea_orm::Set((checkpoint.id.clone()).to_owned()),
                operation_id: sea_orm::Set((checkpoint.operation_id.clone()).to_owned()),
                owner: sea_orm::Set((checkpoint.owner.clone()).to_owned()),
                previous: sea_orm::Set(checkpoint.previous.clone()),
                portion: sea_orm::Set(portion),
                summary: sea_orm::Set((checkpoint.summary.clone()).to_owned()),
                selection: sea_orm::Set(serde_json::from_str(&selection)?),
                projection_version: sea_orm::Set(projection),
                format_version: sea_orm::Set(i64::from(FORMAT_VERSION)),
                identity_sha256: sea_orm::Set((identity.clone()).to_owned()),
                status: sea_orm::Set(("candidate").to_owned()),
            })
            .on_conflict(
                OnConflict::columns([
                    compaction_checkpoint::Column::OperationId,
                    compaction_checkpoint::Column::Portion,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec_without_returning(&txn)
            .await?;
            // Idempotency is exact, never silently accept a different result for the same portion.
            let exact = compaction_checkpoint::Entity::find()
                .select_only()
                .expr(Expr::col(compaction_checkpoint::Column::Id))
                .filter(
                    Expr::col(compaction_checkpoint::Column::Id)
                        .eq(Expr::Value(checkpoint.id.clone().into()))
                        .and(
                            Expr::col(compaction_checkpoint::Column::OperationId)
                                .eq(Expr::Value(checkpoint.operation_id.clone().into())),
                        )
                        .and(
                            Expr::col(compaction_checkpoint::Column::Portion)
                                .eq(Expr::Value(portion.into())),
                        )
                        .and(
                            Expr::col(compaction_checkpoint::Column::IdentitySha256)
                                .eq(Expr::Value(identity.clone().into())),
                        ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            ensure!(exact.is_some(), "candidate idempotency conflict");
            for model in &coverage {
                compaction_coverage::Entity::insert(model.clone())
                    .on_conflict(OnConflict::new().do_nothing().to_owned())
                    .exec_without_returning(&txn)
                    .await?;
            }
            compaction_operation::Entity::update_many()
                .col_expr(
                    compaction_operation::Column::NextPortion,
                    Expr::expr(Func::cust(Alias::new("greatest")).args([
                        Expr::col(compaction_operation::Column::NextPortion),
                        Expr::Value((portion + 1).into()),
                    ])),
                )
                .filter(
                    Expr::col(compaction_operation::Column::Id)
                        .eq(Expr::Value(checkpoint.operation_id.clone().into())),
                )
                .exec(&txn)
                .await?;
            txn.commit().await?;
            Ok(())
        })
        .await
}

// These existing reader-predicate fixtures intentionally model corrupt legacy
// headers that predate the complete-identity guard. Seed that historical state
// atomically and restore the exact production DDL before any repository read;
// no production transition or race fixture runs with a weakened guard.
#[cfg(test)]
pub(super) async fn seed_legacy_frozen_header(db: &pioneer_sqlite::SqliteDatabase, sql: &str) {
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "ALTER TABLE compaction_frozen_history DISABLE TRIGGER frozen_complete_identity",
    )
    .await
    .unwrap();
    tx.execute_unprepared(sql).await.unwrap();
    tx.execute_unprepared(
        "ALTER TABLE compaction_frozen_history ENABLE TRIGGER frozen_complete_identity",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[cfg(test)]
pub(super) use pioneer_crud_compaction::plan_test_support::{
    replace_task_basis, seed_canonical_plan_noise, seed_compressed_sources, seed_task_basis,
};

pub(super) use pioneer_crud_compaction::proof_publication::{
    checkpoint_row, refresh_checkpoint_row,
};

pub(super) use pioneer_crud_compaction::frozen_reachability::checkpoint_source_owner;

use pioneer_crud_compaction::checkpoint_topology::checkpoint_coverage;
pub(super) use pioneer_crud_compaction::checkpoint_topology::{
    CheckpointTopology, CheckpointTopologyRead, checkpoint_topology,
};

pub(crate) async fn compaction_checkpoint_edges(
    store: &CrudStore,
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

pub(super) use pioneer_crud_compaction::proof_graph::{
    CheckpointEdgesRow, HistoricalEventInputEvidenceRow, HistoricalReplayAliasRow,
};

// Published operation projections and their logical frozen messages are immutable.
// Storage sharing may change between pages, but preserves each logical ordinal. The page
// statements reproduce the logical view's active-layout choice while binding the requested
// manifest and ordinal range directly on both physical data branches. No transaction or
// query stream spans pages: reader capacity is released before references are decoded and
// the next page is requested.
async fn checkpoint_projection_metadata(
    store: &CrudStore,
    checkpoint_row: &CheckpointEdgesRow,
    ownership: &std::collections::BTreeMap<SourceRef, std::collections::BTreeSet<String>>,
) -> Result<(
    Vec<HistoricalReplayAliasRow>,
    Vec<HistoricalEventInputEvidenceRow>,
)> {
    let mut scan = match super::checkpoint_proofs::OriginScan::new(
        store,
        checkpoint_row.clone(),
        ownership.clone(),
    )
    .await?
    {
        super::checkpoint_proofs::OriginRead::Legacy(scan) => scan,
        super::checkpoint_proofs::OriginRead::Permanent(topology) => {
            let current = refresh_metadata_topology(store, checkpoint_row, ownership).await?;
            ensure!(
                current == topology.row,
                "checkpoint changed before permanent metadata"
            );
            let metadata = super::checkpoint_proofs::permanent_metadata(
                store,
                topology.row,
                topology.ownership,
            )
            .await?;
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
    store: &CrudStore,
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

#[cfg(test)]
struct CheckpointProjectionPageTestPause {
    token: std::sync::Arc<()>,
    reached: Option<tokio::sync::oneshot::Sender<()>>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CheckpointProjectionPageTestRead {
    pub(super) start: i64,
    pub(super) end: i64,
    pub(super) rows: usize,
    pub(super) bytes: usize,
}

#[cfg(test)]
pub(super) struct CheckpointProjectionPageTestObserver {
    key: (usize, String),
    token: std::sync::Arc<()>,
    _database: pioneer_sqlite::SqliteDatabase,
    reads: std::sync::Arc<std::sync::Mutex<Vec<CheckpointProjectionPageTestRead>>>,
}

#[cfg(test)]
impl CheckpointProjectionPageTestObserver {
    pub(super) fn reads(&self) -> Vec<CheckpointProjectionPageTestRead> {
        self.reads
            .lock()
            .expect("projection read lock poisoned")
            .clone()
    }
}

#[cfg(test)]
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

#[cfg(test)]
pub(super) fn observe_checkpoint_projection_page_test_reads(
    database: &pioneer_sqlite::SqliteDatabase,
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

#[cfg(test)]
type CheckpointProjectionPageTestObservers = std::sync::Mutex<
    std::collections::HashMap<
        (usize, String),
        (
            std::sync::Arc<()>,
            std::sync::Weak<std::sync::Mutex<Vec<CheckpointProjectionPageTestRead>>>,
        ),
    >,
>;

#[cfg(test)]
fn checkpoint_projection_page_test_observers() -> &'static CheckpointProjectionPageTestObservers {
    use std::sync::OnceLock;
    static OBSERVERS: OnceLock<CheckpointProjectionPageTestObservers> = OnceLock::new();
    OBSERVERS.get_or_init(Default::default)
}

#[cfg(test)]
pub(super) fn record_checkpoint_projection_page_test_read(
    database: &pioneer_sqlite::SqliteDatabase,
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

#[cfg(test)]
pub(super) struct CheckpointProjectionPageTestHook {
    key: (usize, String),
    token: std::sync::Arc<()>,
    _database: pioneer_sqlite::SqliteDatabase,
    reached: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
}

#[cfg(test)]
impl CheckpointProjectionPageTestHook {
    pub(super) async fn reached(&mut self) {
        let reached = self
            .reached
            .take()
            .expect("projection hook already awaited");
        tokio::time::timeout(std::time::Duration::from_secs(10), reached)
            .await
            .expect("projection page hook was not reached before the diagnostic deadline")
            .expect("projection lookup ended before reaching the page hook");
    }

    pub(super) fn release(mut self) {
        self.release
            .take()
            .expect("projection page hook already released")
            .send(())
            .expect("projection lookup ended before page-hook release");
    }
}

#[cfg(test)]
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

#[cfg(test)]
pub(super) fn arm_checkpoint_projection_page_test_hook(
    database: &pioneer_sqlite::SqliteDatabase,
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

#[cfg(test)]
fn checkpoint_projection_page_test_hooks() -> &'static std::sync::Mutex<
    std::collections::HashMap<(usize, String), CheckpointProjectionPageTestPause>,
> {
    use std::sync::{Mutex, OnceLock};
    static HOOKS: OnceLock<
        Mutex<std::collections::HashMap<(usize, String), CheckpointProjectionPageTestPause>>,
    > = OnceLock::new();
    HOOKS.get_or_init(Default::default)
}

#[cfg(test)]
pub(super) async fn checkpoint_projection_page_test_pause(
    database: &pioneer_sqlite::SqliteDatabase,
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

pub(super) fn checkpoint_projection_page_sizes_statement(manifest: &str, start: i64) -> Statement {
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

pub(super) fn checkpoint_projection_page_statement(
    manifest: &str,
    start: i64,
    end: i64,
) -> Statement {
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

pub(crate) async fn compaction_checkpoint<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<Checkpoint>> {
    let Some(row) = compaction_checkpoint::Entity::find_by_id(id)
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let coverage = checkpoint_coverage(db, id).await?;
    // Decode only after the bounded database reads have released their capacity.
    Ok(Some(Checkpoint {
        id: row.id,
        operation_id: row.operation_id,
        owner: row.owner,
        previous: row.previous,
        summary: row.summary,
        selection: serde_json::from_value(row.selection)?,
        coverage,
        projection_version: row.projection_version as u64,
        format_version: row.format_version as u32,
    }))
}

/// Atomic CAS. Appends do not invalidate selected sources; edits, Stop and another head do.
pub(crate) async fn compaction_apply(
    store: &CrudStore,
    checkpoint: &Checkpoint,
    expected_head: Option<&str>,
    assertions: &[SourceAssertion],
) -> Result<CommitOutcome> {
    // The legacy assertion writer has no frozen projection or manifest. Its
    // exact source assertions are the publication proof. If an operation did
    // bind a frozen projection, require its graph to be readable as the runner
    // publication path does; otherwise the legacy CAS remains unchanged.
    let has_projection =
        compaction_operation_projection::Entity::find_by_id(&checkpoint.operation_id)
            .select_only()
            .column(compaction_operation_projection::Column::OperationId)
            .into_tuple::<String>()
            .one(&store.connection)
            .await?
            .is_some();
    super::checkpoint_proofs::prepare_graph(store, &checkpoint.id).await?;
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
    let identity = checkpoint_identity(checkpoint)?;
    store.run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            let exact = compaction_checkpoint::Entity::find().select_only().column(compaction_checkpoint::Column::Id).filter(Expr::col(compaction_checkpoint::Column::Id).eq(Expr::Value(checkpoint.id.clone().into())).and(Expr::col(compaction_checkpoint::Column::IdentitySha256).eq(Expr::Value(identity.clone().into())))).into_tuple::<String>().one(&txn).await?;
            ensure!(exact.is_some(), "candidate identity mismatch");
            ensure!(compaction_checkpoint::Entity::find_by_id(&checkpoint.id).select_only().column(compaction_checkpoint::Column::Id)
                .filter(compaction_checkpoint::Column::ProofVersion.eq(1)).into_tuple::<String>().one(&txn).await?.is_some(), "candidate proofs not ready");
            let op = compaction_operation::Entity::find()
            .select_only()
            .column(compaction_operation::Column::Status)
            .filter(Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(checkpoint.operation_id.clone()
                        .into()))
                .and(Expr::col(compaction_operation::Column::Owner)
                    .eq(Expr::Value(checkpoint.owner.clone()
                            .into())))
                .and(Expr::cust_with_exprs("$1 IS NOT DISTINCT FROM $2", [Expr::col(compaction_operation::Column::ExpectedHead), Expr::val(expected_head)])))
            .into_tuple::<String>()
            .one(&txn)
            .await?;
            let status: String = op.ok_or_else(|| anyhow::anyhow!("operation missing"))?;
            if status == "completed" { txn.rollback().await?; return Ok(CommitOutcome::AlreadyApplied); }
            if status != "running" { txn.rollback().await?; return Ok(CommitOutcome::Cancelled); }
            let projection_still_matches =
                compaction_operation_projection::Entity::find_by_id(&checkpoint.operation_id)
                    .select_only()
                    .column(compaction_operation_projection::Column::OperationId)
                    .into_tuple::<String>()
                    .one(&txn).await?.is_some() == has_projection;
            if !projection_still_matches { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
            let stopped = compaction_operation::Entity::find()
            .select_only()
            .join(JoinType::InnerJoin, compaction_operation::Entity::belongs_to(compaction_context::Entity)
                .from(compaction_operation::Column::Owner)
                .to(compaction_context::Column::Owner)
                .into())
            .expr(Expr::col((compaction_operation::Entity, compaction_operation::Column::Id)))
            .filter(Expr::col((compaction_operation::Entity, compaction_operation::Column::Id))
                .eq(Expr::Value(checkpoint.operation_id.clone()
                        .into()))
                .and(Expr::col((compaction_operation::Entity, compaction_operation::Column::ExecutionTurn))
                    .is_not_null())
                .and(Expr::exists(Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_execution_stop::Entity, "stop")
                        .and_where(Expr::col(("stop", compaction_execution_stop::Column::Owner))
                            .eq(Expr::col((compaction_context::Entity, compaction_context::Column::Owner)))
                            .and(Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                .eq(Expr::col((compaction_operation::Entity, compaction_operation::Column::ExecutionTurn)))))
                        .to_owned())))
            .into_tuple::<String>()
            .one(&txn)
            .await?;
            if stopped.is_some() { txn.rollback().await?; return Ok(CommitOutcome::Cancelled); }
            if let Some(previous) = &checkpoint.previous {
                let previous_available = txn.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT p.id FROM compaction_checkpoint p WHERE p.id=$1 AND p.owner=$2 AND p.format_version=1 AND (p.status='applied' OR (p.status='retained' AND EXISTS (SELECT 1 FROM compaction_operation committed WHERE committed.id=p.operation_id AND committed.status='completed'))) LIMIT 1",
                    [previous.clone().into(), checkpoint.owner.clone().into()],
                )).await?;
                if previous_available.is_none() { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
            }
            // The legacy writer has no per-reference manifest. For an initial
            // raw-only checkpoint its captured epochs still fence unasserted
            // accepted context. Once a previous published checkpoint is the
            // basis, that checkpoint is atomic: old covered mutations may
            // advance a coarse thread epoch, while every newly covered raw
            // source below is still checked by its exact assertion.
            if checkpoint.previous.is_none() {
                let dependency_changed = txn.query_one_raw(Statement::from_sql_and_values(DbBackend::Postgres,"SELECT o.id FROM compaction_operation o WHERE o.id=$1 AND EXISTS (SELECT 1 FROM json_each(o.snapshot::json->'source_epochs') wanted WHERE COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id=wanted.key),0)<>wanted.value::text::bigint OR NOT EXISTS (SELECT 1 FROM thread t JOIN compaction_context c ON c.workspace_id=t.workspace_id WHERE t.id=wanted.key AND c.owner=o.owner))", [checkpoint.operation_id.clone()
                            .into()]))
                .await?;
                if dependency_changed.is_some() { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
            }
            for assertion in assertions {
                let valid = match assertion.kind {
CanonicalSource::Input => source_assertion_matches::<_,turn_input::Entity,compaction_input_revision::Entity>(
                    &txn,&checkpoint.owner,assertion,(turn_input::Column::Id,turn_input::Column::TurnId,turn_input::Column::Payload),
                    (compaction_input_revision::Column::SourceId,compaction_input_revision::Column::TurnId,compaction_input_revision::Column::Revision,compaction_input_revision::Column::Present)).await?,
CanonicalSource::Event => source_assertion_matches::<_,turn_event::Entity,compaction_event_revision::Entity>(
                    &txn,&checkpoint.owner,assertion,(turn_event::Column::Id,turn_event::Column::TurnId,turn_event::Column::Payload),
                    (compaction_event_revision::Column::SourceId,compaction_event_revision::Column::TurnId,compaction_event_revision::Column::Revision,compaction_event_revision::Column::Present)).await?,
CanonicalSource::ProviderContext => source_assertion_matches::<_,turn_llm_context::Entity,compaction_source_revision::Entity>(
                    &txn,&checkpoint.owner,assertion,(turn_llm_context::Column::Id,turn_llm_context::Column::TurnId,turn_llm_context::Column::Payload),
                    (compaction_source_revision::Column::SourceId,compaction_source_revision::Column::TurnId,compaction_source_revision::Column::Revision,compaction_source_revision::Column::Present)).await?,
CanonicalSource::ToolItem => source_assertion_matches::<_,turn_item::Entity,compaction_item_revision::Entity>(
                    &txn,&checkpoint.owner,assertion,(turn_item::Column::Id,turn_item::Column::TurnId,turn_item::Column::Payload),
                    (compaction_item_revision::Column::SourceId,compaction_item_revision::Column::TurnId,compaction_item_revision::Column::Revision,compaction_item_revision::Column::Present)).await?,
                };
                if !valid { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
            }
            let changed = compaction_context::Entity::update_many()
            .col_expr(compaction_context::Column::Head, Expr::Value(checkpoint.id.clone()
                    .into()))
            .filter(Expr::col(compaction_context::Column::Owner)
                .eq(Expr::Value(checkpoint.owner.clone()
                        .into()))
                .and(Expr::cust_with_exprs("$1 IS NOT DISTINCT FROM $2", [Expr::col(compaction_context::Column::Head), Expr::val(expected_head)]))
                .and(Expr::col(compaction_context::Column::FormatVersion)
                    .eq(Expr::val(1_i64)))
                .and(Expr::exists(Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_checkpoint::Entity, "p")
                        .and_where(Expr::col(("p", compaction_checkpoint::Column::Id))
                            .eq(Expr::Value(checkpoint.id.clone()
                                    .into()))
                            .and(Expr::col(("p", compaction_checkpoint::Column::OperationId))
                                .eq(Expr::Value(checkpoint.operation_id.clone()
                                        .into())))
                            .and(Expr::col(("p", compaction_checkpoint::Column::Owner))
                                .eq(Expr::col(("compaction_context", "owner"))))
                            .and(Expr::col(("p", compaction_checkpoint::Column::Status))
                                .eq(Expr::val("candidate"))))
                        .to_owned())))
            .exec(&txn)
            .await?;
            if changed.rows_affected != 1 { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
            compaction_checkpoint::Entity::update_many().col_expr(compaction_checkpoint::Column::Status, Expr::val("applied")).filter(Expr::col(compaction_checkpoint::Column::Id).eq(Expr::Value(checkpoint.id.clone().into()))).exec(&txn).await?;
            compaction_operation::Entity::update_many().col_expr(compaction_operation::Column::Status, Expr::val("completed")).col_expr(compaction_operation::Column::Outcome, Expr::val("applied")).filter(Expr::col(compaction_operation::Column::Id).eq(Expr::Value(checkpoint.operation_id.clone().into()))).exec(&txn).await?;
            txn.commit().await?;
            Ok(CommitOutcome::Applied)
        }).await
}

// This bounded point check executes inside the existing commit transaction.
// All source bytes and checkpoint digests were prepared before writer capacity.
async fn source_assertion_matches<C: ConnectionTrait, E: EntityTrait, R: EntityTrait>(
    db: &C,
    owner: &str,
    assertion: &SourceAssertion,
    source_columns: (E::Column, E::Column, E::Column),
    revision_columns: (R::Column, R::Column, R::Column, R::Column),
) -> Result<bool> {
    let (id, turn_id, payload) = source_columns;
    let (revision_id, revision_turn, revision, present) = revision_columns;
    let mut query = E::find()
        .select_only()
        .column(id)
        .join(
            JoinType::InnerJoin,
            E::belongs_to(turn::Entity)
                .from(turn_id)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            thread::Entity::belongs_to(compaction_context::Entity)
                .from((thread::Column::Id, thread::Column::WorkspaceId))
                .to((
                    compaction_context::Column::ThreadId,
                    compaction_context::Column::WorkspaceId,
                ))
                .into(),
        )
        .filter(compaction_context::Column::Owner.eq(owner))
        .filter(turn_id.eq(assertion.turn_id.clone()))
        .filter(id.eq(assertion.id.clone()));
    if let Some(expected) = assertion.revision {
        query = query
            .join(
                JoinType::InnerJoin,
                E::belongs_to(R::default())
                    .from((id, turn_id))
                    .to((revision_id, revision_turn))
                    .into(),
            )
            .filter(revision.eq(expected))
            .filter(present.eq(1_i64));
    } else {
        query = query.filter(payload.eq(assertion.payload.clone()));
    }
    Ok(query.into_tuple::<String>().one(db).await?.is_some())
}

pub use pioneer_crud_compaction::history_check_result::{
    HistoryCheckDiagnostic, HistoryCheckOutcome,
};

#[cfg(test)]
#[path = "compaction_source_lookup_tests.rs"]
mod source_lookup_tests;
