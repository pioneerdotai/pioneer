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
use pioneer_compaction::{Checkpoint, FORMAT_VERSION, OperationSnapshot, SourceRef};
use pioneer_entity::{
    compaction_checkpoint, compaction_context, compaction_coverage, compaction_event_revision,
    compaction_execution_stop, compaction_input_revision, compaction_item_revision,
    compaction_operation, compaction_operation_projection, compaction_projection_epoch,
    compaction_source_revision, task_run_conversation_snapshot, thread, turn, turn_event,
    turn_input, turn_item, turn_llm_context,
};
use pioneer_observability::portable_payload as observe;
pub use runner::{ManifestEntry, RunnerPlanRecord};
#[cfg(any(test, feature = "test-support"))]
pub use runner::{
    PublicationTestHookHandle, PublicationTestPause, arm_publication_test_hook,
    trigger_publication_test_hook,
};
use sea_orm::sea_query::{
    Alias, BinOper, Expr, ExprTrait, Func, JoinType, OnConflict, Order, Query,
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement, TransactionTrait};
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

/// Sources with a stable sequence used for bounded discovery. Tool items are
/// resolved by exact references and deliberately have no paging API.
#[derive(Clone, Copy, Debug)]
pub enum PagedSource {
    Input,
    Event,
    ProviderContext,
}

impl From<PagedSource> for CanonicalSource {
    fn from(source: PagedSource) -> Self {
        match source {
            PagedSource::Input => Self::Input,
            PagedSource::Event => Self::Event,
            PagedSource::ProviderContext => Self::ProviderContext,
        }
    }
}

pub use pioneer_crud_compaction::runner_writes::CanonicalSource;

#[derive(Clone, Debug)]
pub struct SourceRecord {
    pub source_type: String,
    pub tool_name: Option<String>,
    pub projection_kind: Option<String>,
    pub item_id: Option<String>,
    pub reference: SourceRef,
    pub sequence: i64,
    pub created_at: sea_orm::prelude::DateTimeWithTimeZone,
    pub payload: Option<String>,
    pub incomplete: bool,
}

/// Projection metadata retained for the exact event revision that was last
/// decoded. This does not imply that the covered raw event is still present.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoricalEventProjection {
    pub reference: SourceRef,
    pub item_id: Option<String>,
    pub projection_kind: String,
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

pub use pioneer_crud_compaction::operations::OperationRecord;

#[derive(Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Applied,
    AlreadyApplied,
    Stale,
    Cancelled,
}

pub(super) use pioneer_crud_compaction::runner_writes::checkpoint_identity;

/// Pin append-only discovery to a bounded high water mark. Edits remain
/// governed by each source revision, not by this sequence boundary.
pub(crate) async fn compaction_source_high_water<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
) -> Result<i64> {
    use pioneer_entity::{turn_event, turn_input, turn_llm_context};
    match kind {
        PagedSource::Input => {
            source_high_water::<C, turn_input::Entity>(
                db,
                workspace,
                thread,
                turn,
                turn_input::Column::TurnId,
                Expr::col((turn_input::Entity, turn_input::Column::InputIndex)).add(1),
            )
            .await
        }
        PagedSource::ProviderContext => {
            source_high_water::<C, turn_llm_context::Entity>(
                db,
                workspace,
                thread,
                turn,
                turn_llm_context::Column::TurnId,
                Expr::col((turn_llm_context::Entity, turn_llm_context::Column::Sequence)),
            )
            .await
        }
        PagedSource::Event => {
            source_high_water::<C, turn_event::Entity>(
                db,
                workspace,
                thread,
                turn,
                turn_event::Column::TurnId,
                Expr::col((turn_event::Entity, turn_event::Column::Sequence)),
            )
            .await
        }
    }
}

async fn source_high_water<C: ConnectionTrait, E: EntityTrait>(
    db: &C,
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    turn_column: E::Column,
    sequence: Expr,
) -> Result<i64> {
    use sea_orm::{ColumnTrait, QuerySelect};
    E::find()
        .select_only()
        .expr(Func::coalesce([
            Func::max(sequence).into(),
            Expr::val(0_i64),
        ]))
        .join(
            JoinType::InnerJoin,
            E::belongs_to(turn::Entity)
                .from(turn_column)
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
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .filter(thread::Column::Id.eq(thread_id))
        .filter(turn::Column::Id.eq(turn_id))
        .into_tuple::<i64>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source boundary missing"))
}
/// Scope is inherited from this handle. Background callers must use maintenance.
/// Metadata discovery is bounded; payload parsing/hashing follows released reads.
pub(crate) async fn compaction_source_page(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
) -> Result<SourcePage> {
    store
        .compaction_source_page_inner(workspace, thread, turn, kind, after, true, i64::MAX)
        .await
}

/// Discover versioned identities without materializing already covered
/// payloads. The projection fetches only its uncovered sources afterward.
pub(crate) async fn compaction_source_metadata_page(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
) -> Result<SourcePage> {
    store
        .compaction_source_page_inner(workspace, thread, turn, kind, after, false, i64::MAX)
        .await
}

pub(crate) async fn compaction_source_metadata_page_at_fence(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
    capture_order: i64,
) -> Result<SourcePage> {
    store
        .compaction_source_page_inner(workspace, thread, turn, kind, after, false, capture_order)
        .await
}

pub(crate) async fn compaction_item_created_at(
    store: &CrudStore,
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    id: &str,
) -> Result<Option<sea_orm::prelude::DateTimeWithTimeZone>> {
    Ok(turn_item::Entity::find_by_id(id)
        .join(
            JoinType::InnerJoin,
            turn_item::Entity::belongs_to(turn::Entity)
                .from(turn_item::Column::TurnId)
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
        .filter(turn_item::Column::TurnId.eq(turn_id))
        .filter(turn::Column::ThreadId.eq(thread_id))
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .select_only()
        .column(turn_item::Column::CreatedAt)
        .into_tuple::<sea_orm::prelude::DateTimeWithTimeZone>()
        .one(&store.connection)
        .await?)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compaction_source_page_inner(
    db: &pioneer_sqlite::SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
    include_payload: bool,
    capture_order: i64,
) -> Result<SourcePage> {
    compaction_source_page_inner_bounded(
        db,
        workspace,
        thread,
        turn,
        kind,
        after,
        include_payload,
        capture_order,
        None,
    )
    .await
}

pub(crate) async fn compaction_source_page_inner_bounded(
    db: &pioneer_sqlite::SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
    include_payload: bool,
    capture_order: i64,
    before: Option<sea_orm::prelude::DateTimeWithTimeZone>,
) -> Result<SourcePage> {
    match kind {
        PagedSource::Input => {
            read_source_page(
                db,
                turn_input_projection(workspace, thread, turn),
                kind,
                turn,
                after,
                include_payload,
                capture_order,
                before,
            )
            .await
        }
        PagedSource::Event => {
            read_source_page(
                db,
                turn_event_projection(workspace, thread, turn),
                kind,
                turn,
                after,
                include_payload,
                capture_order,
                before,
            )
            .await
        }
        PagedSource::ProviderContext => {
            read_source_page(
                db,
                turn_llm_context_projection(workspace, thread, turn),
                kind,
                turn,
                after,
                include_payload,
                capture_order,
                before,
            )
            .await
        }
    }
}

// Exact reads share scoped identity/revision metadata. Only sequenced sources
// carry discovery fields; tool items cannot acquire a physical rowid dependency.
struct CanonicalProjection<E: EntityTrait, P = SourcePaging> {
    query: sea_orm::Select<E>,
    id: E::Column,
    payload: E::Column,
    portable: Option<E::Column>,
    revision: Expr,
    present: Expr,
    paging: P,
}

struct SourcePaging {
    sequence: Expr,
    created_at: Expr,
    source_type: Expr,
    item_id: Expr,
    tool_name: Expr,
    projection_kind: Expr,
    capture_order: Expr,
}

#[derive(FromQueryResult)]
struct CanonicalSourceMetadata {
    id: String,
    sequence: i64,
    created_at: sea_orm::prelude::DateTimeWithTimeZone,
    source_type: String,
    item_id: Option<String>,
    tool_name: Option<String>,
    projection_kind: Option<String>,
    revision: i64,
    bytes: i64,
}

#[allow(clippy::too_many_arguments)]
async fn read_source_page<E: EntityTrait>(
    db: &pioneer_sqlite::SqliteDatabase,
    projection: CanonicalProjection<E>,
    kind: PagedSource,
    turn: &str,
    after: i64,
    include_payload: bool,
    capture_order: i64,
    before: Option<sea_orm::prelude::DateTimeWithTimeZone>,
) -> Result<SourcePage> {
    let kind = CanonicalSource::from(kind);
    let payload_bytes = logical_payload_bytes::<E>(projection.payload, projection.portable);
    let rows = projection
        .query
        .clone()
        .select_only()
        .column(projection.id)
        .expr_as(projection.paging.sequence.clone(), "sequence")
        .expr_as(projection.paging.created_at.clone(), "created_at")
        .expr_as(projection.paging.source_type, "source_type")
        .expr_as(projection.paging.item_id, "item_id")
        .expr_as(projection.paging.tool_name, "tool_name")
        .expr_as(projection.paging.projection_kind, "projection_kind")
        .expr_as(projection.revision.clone(), "revision")
        .expr_as(
            if include_payload {
                payload_bytes.clone()
            } else {
                Expr::val(0_i64)
            },
            "bytes",
        )
        .filter(projection.paging.sequence.clone().gt(after))
        .filter(projection.paging.capture_order.lte(capture_order))
        .filter(match before {
            Some(before) if matches!(kind, CanonicalSource::Input) => {
                history::history_input_before(projection.paging.created_at, before)
            }
            Some(before) if matches!(kind, CanonicalSource::Event) => {
                history::history_event_before(
                    projection.paging.created_at,
                    Expr::col((turn_event::Entity, turn_event::Column::EventType)),
                    before,
                )
            }
            Some(before) => history::history_before(projection.paging.created_at, before),
            None => Expr::val(true).into(),
        })
        .order_by(projection.paging.sequence, Order::Asc)
        .limit(SOURCE_PAGE_ROWS)
        .into_model::<CanonicalSourceMetadata>()
        .all(db)
        .await?;
    let mut page = SourcePage {
        entries: Vec::new(),
        next_sequence: after,
    };
    let mut bytes = 0usize;
    for row in rows {
        let size = row.bytes;
        ensure!(
            !include_payload || size >= 0,
            "canonical portable payload size missing"
        );
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
            read_source_payload(
                db,
                CanonicalProjection {
                    query: projection.query.clone(),
                    id: projection.id,
                    payload: projection.payload,
                    portable: projection.portable,
                    revision: projection.revision.clone(),
                    // Discovery also supports old rows whose revision is
                    // not registered yet; preserve that page contract.
                    present: Expr::val(1_i64),
                    paging: (),
                },
                &row.id,
                row.revision,
            )
            .await?
        };
        // Reader capacity is released before parsing or assembling the page.
        bytes += payload.as_ref().map_or(0, String::len);
        page.next_sequence = row.sequence;
        page.entries.push(SourceRecord {
            source_type: row.source_type,
            item_id: row.item_id,
            tool_name: row.tool_name,
            projection_kind: row.projection_kind,
            reference: SourceRef {
                scope: format!("{}:{turn}", kind.prefix()),
                id: row.id,
                version: format!("{}:{}", kind.version_prefix(), row.revision),
            },
            sequence: row.sequence,
            created_at: row.created_at,
            incomplete: payload.is_none(),
            payload,
        });
    }
    Ok(page)
}
fn turn_input_projection(
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
) -> CanonicalProjection<turn_input::Entity> {
    let null = Expr::val(Option::<String>::None);

    CanonicalProjection {
        query: turn_input::Entity::find()
            .join(
                JoinType::LeftJoin,
                turn_input::Entity::belongs_to(compaction_input_revision::Entity)
                    .from(turn_input::Column::Id)
                    .to(compaction_input_revision::Column::SourceId)
                    .on_condition(|_, _| {
                        sea_orm::Condition::all().add(
                            Expr::col((
                                compaction_input_revision::Entity,
                                compaction_input_revision::Column::TurnId,
                            ))
                            .eq(Expr::col((turn_input::Entity, turn_input::Column::TurnId))),
                        )
                    })
                    .into(),
            )
            .join(
                JoinType::InnerJoin,
                turn_input::Entity::belongs_to(turn::Entity)
                    .from(turn_input::Column::TurnId)
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
            .filter(thread::Column::WorkspaceId.eq(workspace))
            .filter(turn::Column::ThreadId.eq(thread_id))
            .filter(turn_input::Column::TurnId.eq(turn_id)),
        id: turn_input::Column::Id,
        payload: turn_input::Column::Payload,
        portable: None,
        revision: Func::coalesce([
            Expr::col((
                compaction_input_revision::Entity,
                compaction_input_revision::Column::Revision,
            )),
            Expr::val(1_i64),
        ])
        .into(),
        present: Expr::col((
            compaction_input_revision::Entity,
            compaction_input_revision::Column::Present,
        )),
        paging: SourcePaging {
            sequence: Expr::col((turn_input::Entity, turn_input::Column::InputIndex)).add(1),
            created_at: Expr::col((turn_input::Entity, turn_input::Column::CreatedAt)),
            source_type: Expr::col((turn_input::Entity, turn_input::Column::InputType)),
            item_id: null.clone(),
            tool_name: null.clone(),
            projection_kind: null.clone(),
            capture_order: Func::coalesce([
                Expr::col((
                    compaction_input_revision::Entity,
                    compaction_input_revision::Column::CaptureOrder,
                )),
                Expr::val(0_i64),
            ])
            .into(),
        },
    }
}
fn turn_event_projection(
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
) -> CanonicalProjection<turn_event::Entity> {
    let null = Expr::val(Option::<String>::None);
    let current_projection = |column| {
        Expr::case(
            Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::ProjectionRevision,
            ))
            .eq(Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::Revision,
            ))),
            Expr::col((compaction_event_revision::Entity, column)),
        )
        .finally(null.clone())
        .into()
    };
    CanonicalProjection {
        query: turn_event::Entity::find()
            .join(
                JoinType::LeftJoin,
                turn_event::Entity::belongs_to(compaction_event_revision::Entity)
                    .from(turn_event::Column::Id)
                    .to(compaction_event_revision::Column::SourceId)
                    .on_condition(|_, _| {
                        sea_orm::Condition::all().add(
                            Expr::col((
                                compaction_event_revision::Entity,
                                compaction_event_revision::Column::TurnId,
                            ))
                            .eq(Expr::col((turn_event::Entity, turn_event::Column::TurnId))),
                        )
                    })
                    .into(),
            )
            .join(
                JoinType::InnerJoin,
                turn_event::Entity::belongs_to(turn::Entity)
                    .from(turn_event::Column::TurnId)
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
            .filter(thread::Column::WorkspaceId.eq(workspace))
            .filter(turn::Column::ThreadId.eq(thread_id))
            .filter(turn_event::Column::TurnId.eq(turn_id)),
        id: turn_event::Column::Id,
        payload: turn_event::Column::Payload,
        portable: Some(turn_event::Column::PayloadPayloadId),
        revision: Func::coalesce([
            Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::Revision,
            )),
            Expr::val(1_i64),
        ])
        .into(),
        present: Expr::col((
            compaction_event_revision::Entity,
            compaction_event_revision::Column::Present,
        )),
        paging: SourcePaging {
            sequence: Expr::col((turn_event::Entity, turn_event::Column::Sequence)),
            created_at: Expr::col((turn_event::Entity, turn_event::Column::CreatedAt)),
            source_type: Expr::col((turn_event::Entity, turn_event::Column::EventType)),
            item_id: current_projection(compaction_event_revision::Column::ItemId),
            tool_name: null.clone(),
            projection_kind: current_projection(compaction_event_revision::Column::ProjectionKind),
            capture_order: Func::coalesce([
                Expr::col((
                    compaction_event_revision::Entity,
                    compaction_event_revision::Column::CaptureOrder,
                )),
                Expr::val(0_i64),
            ])
            .into(),
        },
    }
}
fn turn_llm_context_projection(
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
) -> CanonicalProjection<turn_llm_context::Entity> {
    let null = Expr::val(Option::<String>::None);

    CanonicalProjection {
        query: turn_llm_context::Entity::find()
            .join(
                JoinType::LeftJoin,
                turn_llm_context::Entity::belongs_to(compaction_source_revision::Entity)
                    .from(turn_llm_context::Column::Id)
                    .to(compaction_source_revision::Column::SourceId)
                    .on_condition(|_, _| {
                        sea_orm::Condition::all().add(
                            Expr::col((
                                compaction_source_revision::Entity,
                                compaction_source_revision::Column::TurnId,
                            ))
                            .eq(Expr::col((
                                turn_llm_context::Entity,
                                turn_llm_context::Column::TurnId,
                            ))),
                        )
                    })
                    .into(),
            )
            .join(
                JoinType::InnerJoin,
                turn_llm_context::Entity::belongs_to(turn::Entity)
                    .from(turn_llm_context::Column::TurnId)
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
            .filter(thread::Column::WorkspaceId.eq(workspace))
            .filter(turn::Column::ThreadId.eq(thread_id))
            .filter(turn_llm_context::Column::TurnId.eq(turn_id)),
        id: turn_llm_context::Column::Id,
        payload: turn_llm_context::Column::Payload,
        portable: None,
        revision: Func::coalesce([
            Expr::col((
                compaction_source_revision::Entity,
                compaction_source_revision::Column::Revision,
            )),
            Expr::val(1_i64),
        ])
        .into(),
        present: Expr::col((
            compaction_source_revision::Entity,
            compaction_source_revision::Column::Present,
        )),
        paging: SourcePaging {
            sequence: Expr::col((turn_llm_context::Entity, turn_llm_context::Column::Sequence)),
            created_at: Expr::col((
                turn_llm_context::Entity,
                turn_llm_context::Column::CreatedAt,
            )),
            source_type: Expr::col((turn_llm_context::Entity, turn_llm_context::Column::Source)),
            item_id: Expr::col((turn_llm_context::Entity, turn_llm_context::Column::ItemId)),
            tool_name: Expr::col((turn_llm_context::Entity, turn_llm_context::Column::ToolName)),
            projection_kind: null.clone(),
            capture_order: Func::coalesce([
                Expr::col((
                    compaction_source_revision::Entity,
                    compaction_source_revision::Column::CaptureOrder,
                )),
                Expr::val(0_i64),
            ])
            .into(),
        },
    }
}
fn turn_item_projection(
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
) -> CanonicalProjection<turn_item::Entity, ()> {
    CanonicalProjection {
        query: turn_item::Entity::find()
            .join(
                JoinType::LeftJoin,
                turn_item::Entity::belongs_to(compaction_item_revision::Entity)
                    .from(turn_item::Column::Id)
                    .to(compaction_item_revision::Column::SourceId)
                    .on_condition(|_, _| {
                        sea_orm::Condition::all().add(
                            Expr::col((
                                compaction_item_revision::Entity,
                                compaction_item_revision::Column::TurnId,
                            ))
                            .eq(Expr::col((turn_item::Entity, turn_item::Column::TurnId))),
                        )
                    })
                    .into(),
            )
            .join(
                JoinType::InnerJoin,
                turn_item::Entity::belongs_to(turn::Entity)
                    .from(turn_item::Column::TurnId)
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
            .filter(thread::Column::WorkspaceId.eq(workspace))
            .filter(turn::Column::ThreadId.eq(thread_id))
            .filter(turn_item::Column::TurnId.eq(turn_id)),
        id: turn_item::Column::Id,
        payload: turn_item::Column::Payload,
        portable: Some(turn_item::Column::PayloadPayloadId),
        revision: Func::coalesce([
            Expr::col((
                compaction_item_revision::Entity,
                compaction_item_revision::Column::Revision,
            )),
            Expr::val(1_i64),
        ])
        .into(),
        present: Expr::col((
            compaction_item_revision::Entity,
            compaction_item_revision::Column::Present,
        )),
        paging: (),
    }
}

pub(crate) use pioneer_crud_compaction::operations::{
    compaction_admit, compaction_admit_for_turn, compaction_invalid_completion_progress,
    compaction_operation, compaction_operation_for_plan,
};

/// Validate a bounded batch of directly consumed source identities. Canonical
/// rows and task bases are exact-current; a checkpoint is current when the
/// checkpoint object itself is published in the requested scope.
// The correlated LIMIT 1 keeps each PostgreSQL primary-key probe inside
// its source branch. A flattened join can scan unrelated revisions.
pub(super) use pioneer_crud_compaction::source_revision::compaction_sources_current_statement;

pub(crate) async fn compaction_sources_current<C: ConnectionTrait>(
    db: &C,
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
    let row = MatchedSourceCount::find_by_statement(compaction_sources_current_statement(
        payload, workspace, thread,
    ))
    .one(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("source validation missing"))?;
    Ok(row.matched == sources.len() as i64)
}

/// Admit only cleanup-stable context identities to a newly published history.
/// The same scoped revision lookup used by source validation is required here;
/// a source name alone cannot authenticate a caller-supplied reference.
pub(crate) async fn compaction_model_context_sources_current<C: ConnectionTrait>(
    db: &C,
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
    let row = MatchedSourceCount::find_by_statement(Statement::from_sql_and_values(DbBackend::Postgres,
        r#"SELECT COUNT(*) AS matched FROM json_array_elements($1::text::json) AS wanted(value) WHERE EXISTS (
            SELECT 1 FROM compaction_source_revision revision
            JOIN turn_llm_context context_source
              ON context_source.id=revision.source_id AND context_source.turn_id=revision.turn_id
            JOIN turn context_turn ON context_turn.id=revision.turn_id
            JOIN thread context_thread ON context_thread.id=context_turn.thread_id
            WHERE revision.present=1 AND context_thread.workspace_id=$2
              AND context_turn.thread_id=$3
              AND context_source.source IN ($4,$5,$6,$7,$8)
              AND 'context:'||revision.turn_id=(wanted.value->>'scope')
              AND revision.source_id=(wanted.value->>'id')
              AND 'revision:'||revision.revision=(wanted.value->>'version')
        )"#,
        [
            payload.into(),
            workspace.into(),
            thread.into(),
            MODEL_HISTORY_CONTEXT_SOURCES[0].into(),
            MODEL_HISTORY_CONTEXT_SOURCES[1].into(),
            MODEL_HISTORY_CONTEXT_SOURCES[2].into(),
            MODEL_HISTORY_CONTEXT_SOURCES[3].into(),
            MODEL_HISTORY_CONTEXT_SOURCES[4].into(),
        ],
    ))
    .one(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("context source validation missing"))?;
    Ok(row.matched == sources.len() as i64)
}

#[derive(FromQueryResult)]
struct HistoricalEventProjectionRow {
    source_id: String,
    turn_id: String,
    source_version: String,
    item_id: Option<String>,
    projection_kind: String,
}

/// Read only the durable relationship metadata for exact captured revisions.
/// `present` and the raw `turn_event` row are intentionally not consulted: a
/// published checkpoint remains authoritative after its covered payload is
/// edited or removed.
pub(crate) async fn compaction_historical_event_projections<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    sources: &[SourceRef],
) -> Result<Vec<HistoricalEventProjection>> {
    ensure!(
        sources.len() <= SOURCE_PAGE_ROWS as usize,
        "event projection metadata batch exceeds row bound"
    );
    ensure!(
        sources
            .iter()
            .all(|source| source.scope.starts_with("event:")),
        "event projection metadata received a non-event source"
    );
    let payload = serde_json::to_string(sources)?;
    ensure!(
        payload.len() <= SOURCE_PAGE_BYTES,
        "event projection metadata batch exceeds byte bound"
    );
    let rows = HistoricalEventProjectionRow::find_by_statement(Statement::from_sql_and_values(DbBackend::Postgres,
        "SELECT r.source_id,r.turn_id,'event-revision:'||r.projection_revision AS source_version,r.item_id,r.projection_kind \
         FROM json_array_elements($1::text::json) WITH ORDINALITY AS w(value, ordinal) \
         JOIN compaction_event_revision r \
           ON 'event:'||r.turn_id=(w.value->>'scope') \
          AND r.source_id=(w.value->>'id') \
          AND 'event-revision:'||r.projection_revision=(w.value->>'version') \
          AND r.projection_kind IS NOT NULL \
         JOIN turn t ON t.id=r.turn_id AND t.thread_id=$2 \
         JOIN thread h ON h.id=t.thread_id AND h.workspace_id=$3 \
         ORDER BY w.ordinal",
        [payload.into(), thread.into(), workspace.into()],
    ))
    .all(db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| HistoricalEventProjection {
            reference: SourceRef {
                scope: format!("event:{}", row.turn_id),
                id: row.source_id,
                version: row.source_version,
            },
            item_id: row.item_id,
            projection_kind: row.projection_kind,
        })
        .collect())
}

/// Resolve a directly consumed source's owning thread. Checkpoint ownership is
/// recorded on the published object; raw ownership remains an exact live-row
/// lookup. Historical checkpoint coverage uses its saved manifest ownership.
pub(crate) async fn compaction_reference_thread<C: ConnectionTrait>(
    db: &C,
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
    if source.scope.starts_with("checkpoint:") {
        return Ok(compaction_live_sources::ThreadRow::find_by_statement(
            Statement::from_sql_and_values(DbBackend::Postgres,
                "SELECT c.thread_id FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner WHERE c.workspace_id=$1 AND 'checkpoint:'||p.owner=$2 AND p.id=$3 AND p.identity_sha256=$4 AND p.format_version=1 AND (p.status='applied' OR (p.status='retained' AND EXISTS(SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed'))) LIMIT 1",
                [
                    workspace.into(),
                    source.scope.clone().into(),
                    source.id.clone().into(),
                    source.version.clone().into(),
                ],
            ),
        )
        .one(db)
        .await?
        .map(|row| row.thread_id));
    }
    Ok(compaction_live_sources::ThreadRow::find_by_statement(
        db.get_database_backend().build(
            &(sea_orm::sea_query::Query::select()
                .from(compaction_live_sources::Column::Table)
                .expr(Expr::col(compaction_live_sources::Column::ThreadId))
                .and_where(
                    Expr::col(compaction_live_sources::Column::WorkspaceId)
                        .eq(Expr::Value(workspace.into()))
                        .and(
                            Expr::col(compaction_live_sources::Column::SourceScope)
                                .eq(Expr::Value(source.scope.clone().into())),
                        )
                        .and(
                            Expr::col(compaction_live_sources::Column::SourceId)
                                .eq(Expr::Value(source.id.clone().into())),
                        )
                        .and(
                            Expr::col(compaction_live_sources::Column::SourceVersion)
                                .eq(Expr::Value(source.version.clone().into())),
                        ),
                )
                .limit(1)
                .to_owned()),
        ),
    )
    .one(db)
    .await?
    .map(|row| row.thread_id))
}

pub(crate) async fn compaction_head<C: ConnectionTrait>(
    db: &C,
    owner: &str,
) -> Result<Option<String>> {
    let row = compaction_context::Entity::find_by_id(owner)
        .one(db)
        .await?;
    match row {
        None => Ok(None),
        Some(row) => {
            ensure!(
                row.format_version == i64::from(FORMAT_VERSION),
                "unsupported working-context version"
            );
            Ok(row.head)
        }
    }
}

/// This is the only admission of a provider attempt. Counters survive restart.
pub(crate) async fn compaction_claim_attempt<C: ConnectionTrait>(
    db: &C,
    id: &str,
    expected_attempts: i64,
    now_ms: i64,
    transient_retry: bool,
    correction: bool,
) -> Result<bool> {
    let result = compaction_operation::Entity::update_many()
        .col_expr(
            compaction_operation::Column::Attempts,
            Expr::col(compaction_operation::Column::Attempts).add(Expr::val(1_i64)),
        )
        .col_expr(
            compaction_operation::Column::TransientRetries,
            Expr::col(compaction_operation::Column::TransientRetries)
                .add(Expr::Value((transient_retry as i64).into())),
        )
        .col_expr(
            compaction_operation::Column::Correction,
            Expr::col(compaction_operation::Column::Correction)
                .add(Expr::Value((correction as i64).into())),
        )
        .filter(
            Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(id.into()))
                .and(Expr::col(compaction_operation::Column::Status).eq(Expr::val("running")))
                .and(
                    Expr::col(compaction_operation::Column::Attempts)
                        .eq(Expr::Value(expected_attempts.into())),
                )
                .and(
                    Expr::col(compaction_operation::Column::DeadlineMs)
                        .gt(Expr::Value(now_ms.into())),
                )
                .and(
                    Expr::col(compaction_operation::Column::TransientRetries)
                        .add(Expr::Value((transient_retry as i64).into()))
                        .lte(Expr::val(2_i64)),
                )
                .and(
                    Expr::col(compaction_operation::Column::Correction)
                        .add(Expr::Value((correction as i64).into()))
                        .lte(Expr::val(1_i64)),
                ),
        )
        .exec(db)
        .await?;
    Ok(result.rows_affected == 1)
}

pub(crate) async fn compaction_finish<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: &str,
    outcome: &str,
) -> Result<bool> {
    ensure!(
        ["failed", "cancelled", "stale"].contains(&status),
        "invalid terminal transition"
    );
    ensure!(
        outcome.len() <= 128,
        "outcome must be a bounded classification"
    );
    let changed = compaction_operation::Entity::update_many()
        .col_expr(
            compaction_operation::Column::Status,
            Expr::Value(status.into()),
        )
        .col_expr(
            compaction_operation::Column::Outcome,
            Expr::Value(outcome.into()),
        )
        .filter(
            Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(id.into()))
                .and(Expr::col(compaction_operation::Column::Status).eq(Expr::val("running"))),
        )
        .exec(db)
        .await?;
    Ok(changed.rows_affected > 0)
}

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
pub(super) async fn seed_compressed_sources(db: &pioneer_sqlite::SqliteDatabase) {
    use pioneer_crud_conversations::{turn_event, turn_item};
    use pioneer_crud_payloads::storage::PreparedPayload;

    let wire = format!(r#"{{"value":"{}"}}"#, "canonical 🌍".repeat(8192));
    let event = PreparedPayload::new(&wire).unwrap();
    let item = PreparedPayload::new(&wire).unwrap();
    let now = chrono::Utc::now().fixed_offset();
    let tx = db.begin().await.unwrap();
    turn_event::lock_stream(&tx, "source-turn").await.unwrap();
    turn_event::insert(
        &tx,
        turn_event::NewEvent {
            id: "event-source",
            thread_id: "source-thread",
            turn_id: "source-turn",
            sequence: 1,
            event_type: "fixture",
            idempotency_key: "event-source",
            created_at: now,
        },
        &event,
    )
    .await
    .unwrap();
    turn_item::upsert(
        &tx,
        turn_item::ItemWrite {
            id: "item-source".into(),
            turn_id: "source-turn".into(),
            item_id: "item-source".into(),
            item_type: "command_execution".into(),
            status: Some("completed".into()),
            created_at: now,
            updated_at: now,
        },
        &item,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[cfg(test)]
pub(super) async fn seed_canonical_plan_noise(db: &pioneer_sqlite::SqliteDatabase) {
    // PostgreSQL can prefer a sequential scan on tiny tables. Populate other
    // canonical scopes so this checks the plan relevant to retained histories.
    db.execute_unprepared(
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,created_at)
         SELECT 'plan-context-'||n,'other-turn',n,'noise','{}'::json,CURRENT_TIMESTAMP FROM generate_series(100,4095) n;
         INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at)
         SELECT 'plan-event-'||n,'other-thread','other-turn',n,'noise','{}'::json,CURRENT_TIMESTAMP FROM generate_series(100,4095) n;
         INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at)
         SELECT 'plan-item-'||n,'other-turn','plan-item-'||n,'command_execution','completed','{}'::json,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM generate_series(100,4095) n;
         INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at)
         SELECT 'plan-input-'||n,'other-turn',n,'text','noise','{}'::json,CURRENT_TIMESTAMP FROM generate_series(100,4095) n;
         ANALYZE turn_llm_context;
         ANALYZE turn_event;
         ANALYZE turn_item;
         ANALYZE turn_input;
         ANALYZE compaction_source_revision;
         ANALYZE compaction_event_revision;
         ANALYZE compaction_item_revision;
         ANALYZE compaction_input_revision;",
    ).await.unwrap();
}

#[cfg(test)]
pub(super) async fn seed_task_basis(db: &pioneer_sqlite::SqliteDatabase) {
    use pioneer_crud_tasks::conversation_snapshot::{
        NewTaskRunConversationSnapshot, insert_if_absent,
    };

    let snapshot = NewTaskRunConversationSnapshot {
        run_id: "basis-run".into(),
        task_id: "task".into(),
        workspace_id: "ws".into(),
        conversation_thread_id: "source-thread".into(),
        source_turn_id: None,
        history_json: "[]".into(),
        created_at: chrono::Utc::now().fixed_offset(),
    };
    let prepared =
        pioneer_crud_payloads::storage::PreparedPayload::new(&snapshot.history_json).unwrap();
    let tx = db.begin().await.unwrap();
    let (_, inserted) = insert_if_absent(&tx, &snapshot, None, Some(&prepared))
        .await
        .unwrap();
    assert!(inserted);
    tx.commit().await.unwrap();
}

#[cfg(test)]
pub(super) async fn replace_task_basis(db: &pioneer_sqlite::SqliteDatabase, text: &str) {
    let prepared = pioneer_crud_payloads::storage::PreparedPayload::new(text).unwrap();
    let tx = db.begin().await.unwrap();
    prepared.insert(&tx).await.unwrap();
    let result = tx
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_run_conversation_snapshot
         SET history_json_payload_id=$1, history_json_has_array_prefix=$2
         WHERE run_id='basis-run'",
            [
                prepared.id().into(),
                text.trim_start_matches(' ').starts_with('[').into(),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);
    tx.commit().await.unwrap();
}

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

#[derive(Clone, Debug)]
pub struct CanonicalFragment {
    pub reference: SourceRef,
    pub text: String,
    pub next_character: Option<u64>,
}

/// Bounded Unicode-safe reads of very large canonical payloads. Revision is
/// maintained atomically by source-table triggers, including deletes/reinserts.
/// The same revision must be supplied for every subsequent fragment.
pub(crate) async fn compaction_source_fragment(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    id: &str,
    expected_revision: Option<&str>,
    character_offset: u64,
) -> Result<Option<CanonicalFragment>> {
    store
        .compaction_payload_fragment(
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

pub(crate) async fn compaction_payload_fragment(
    db: &pioneer_sqlite::SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    id: &str,
    expected_revision: Option<&str>,
    character_offset: u64,
    kind: CanonicalSource,
) -> Result<Option<CanonicalFragment>> {
    let (scope, version_prefix) = (kind.prefix(), kind.version_prefix());
    let offset = i64::try_from(character_offset)?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("source offset overflow"))?;
    // Legacy registration is an explicit preparation phase. This path is a
    // physically read-only exact revision lookup and must never queue a writer.
    let row = match kind {
        CanonicalSource::Input => {
            read_source_fragment(
                db,
                turn_input_projection(workspace, thread, turn),
                id,
                offset,
            )
            .await?
        }
        CanonicalSource::Event => {
            read_source_fragment(
                db,
                turn_event_projection(workspace, thread, turn),
                id,
                offset,
            )
            .await?
        }
        CanonicalSource::ProviderContext => {
            read_source_fragment(
                db,
                turn_llm_context_projection(workspace, thread, turn),
                id,
                offset,
            )
            .await?
        }
        CanonicalSource::ToolItem => {
            read_source_fragment(
                db,
                turn_item_projection(workspace, thread, turn),
                id,
                offset,
            )
            .await?
        }
    };
    let Some(row) = row else { return Ok(None) };
    let version = format!("{version_prefix}:{}", row.revision);
    ensure!(
        expected_revision.is_none_or(|expected| expected == version),
        "stale source revision"
    );
    let text: String = row.fragment;
    let characters: i64 = row.characters;
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

/// Resolve a tool item to its existing full canonical record without exposing
/// another workspace/thread. Authorization remains the caller's mandatory gate.
pub(crate) async fn compaction_tool_result_id<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    item: &str,
) -> Result<Option<String>> {
    let rows = turn_llm_context::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            turn_llm_context::Entity::belongs_to(turn::Entity)
                .from(turn_llm_context::Column::TurnId)
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
        .expr(Expr::col((
            turn_llm_context::Entity,
            turn_llm_context::Column::Id,
        )))
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((turn::Entity, turn::Column::ThreadId))
                        .eq(Expr::Value(thread.into())),
                )
                .and(
                    Expr::col((turn_llm_context::Entity, turn_llm_context::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((turn_llm_context::Entity, turn_llm_context::Column::ItemId))
                        .eq(Expr::Value(item.into())),
                )
                .and(
                    Expr::col((turn_llm_context::Entity, turn_llm_context::Column::Source))
                        .eq(Expr::val("tool_result_v2")),
                ),
        )
        .order_by(
            Expr::col((turn_llm_context::Entity, turn_llm_context::Column::Sequence)),
            Order::Asc,
        )
        .limit(2)
        .into_tuple::<String>()
        .all(db)
        .await?;
    ensure!(rows.len() <= 1, "tool result source is ambiguous");
    Ok(rows.into_iter().next())
}

/// Prefer an already retained full shell source. Other tools use their canonical result.
/// Both lookups and fragments retain the same workspace/thread/turn scope.
pub(crate) async fn compaction_tool_result_fragment(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    item: &str,
    expected_revision: Option<&str>,
    character_offset: u64,
) -> Result<Option<CanonicalFragment>> {
    if let Some(source) =
        compaction_tool_item_reference(&store.connection, workspace, thread, turn, item).await?
    {
        return store
            .compaction_payload_fragment(
                workspace,
                thread,
                turn,
                &source.id,
                expected_revision.or(Some(source.version.as_str())),
                character_offset,
                CanonicalSource::ToolItem,
            )
            .await;
    }
    let Some(id) = store
        .compaction_tool_result_id(workspace, thread, turn, item)
        .await?
    else {
        return Ok(None);
    };
    store
        .compaction_source_fragment(
            workspace,
            thread,
            turn,
            &id,
            expected_revision,
            character_offset,
        )
        .await
}

pub(crate) async fn compaction_reference_fragment(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
    character_offset: u64,
) -> Result<Option<CanonicalFragment>> {
    if reference.scope.starts_with("task-basis:") {
        return observe::scope_result(observe::Family::TaskHistory, observe::Mode::Range, async {
            let reader = store.connection.begin_read().await?;
            let captured = pioneer_crud_compaction::task_basis::capture(
                &reader,
                workspace,
                thread,
                reference,
                Some(character_offset),
            )
            .await?;
            reader.commit().await?;
            captured
                .map(|captured| {
                    Ok(CanonicalFragment {
                        reference: reference.clone(),
                        text: captured.body.into_text()?,
                        next_character: captured.next_character,
                    })
                })
                .transpose()
        })
        .await;
    }
    if reference.scope.starts_with("checkpoint:") {
        let offset = i64::try_from(character_offset)?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("source offset overflow"))?;
        let row = SourceFragmentRow::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT 1::bigint AS revision,substr(p.summary,$1::bigint::int,16384) AS fragment,length(p.summary)::bigint AS characters
FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
WHERE p.id=$2 AND 'checkpoint:'||p.owner=$3 AND p.identity_sha256=$4
 AND p.format_version=1 AND c.workspace_id=$5 AND c.thread_id=$6
 AND (p.status='applied' OR (p.status='retained' AND EXISTS(
  SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
LIMIT 1"#,
            [
                offset.into(),
                reference.id.clone().into(),
                reference.scope.clone().into(),
                reference.version.clone().into(),
                workspace.into(),
                thread.into(),
            ],
        ))
        .one(&store.connection)
        .await?;
        let Some(SourceFragmentRow {
            fragment: text,
            characters,
            ..
        }) = row
        else {
            return Ok(None);
        };
        let characters = u64::try_from(characters)?;
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
    store
        .compaction_payload_fragment(
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

/// Parse and validate one canonical revision before fetching its payload.
fn exact_canonical_revision(reference: &SourceRef, kind: CanonicalSource) -> Result<i64> {
    let prefix = format!("{}:", kind.version_prefix());
    let revision = reference
        .version
        .strip_prefix(&prefix)
        .ok_or_else(|| anyhow::anyhow!("source revision is unknown"))?
        .parse::<i64>()?;
    ensure!(
        reference.version == format!("{prefix}{revision}"),
        "source revision is not canonical"
    );
    Ok(revision)
}

const COMPACTION_REFERENCE_CHECKPOINT_PAYLOAD_SQL: &str = r#"
SELECT 1::bigint AS revision,p.summary AS fragment,length(p.summary)::bigint AS characters
FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
WHERE p.id=$1 AND 'checkpoint:'||p.owner=$2 AND p.identity_sha256=$3
 AND p.format_version=1 AND c.workspace_id=$4 AND c.thread_id=$5
 AND (p.status='applied' OR (p.status='retained' AND EXISTS(
  SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
LIMIT 1
"#;

fn compaction_reference_checkpoint_payload_statement(
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Statement {
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        COMPACTION_REFERENCE_CHECKPOINT_PAYLOAD_SQL,
        [
            reference.id.clone().into(),
            reference.scope.clone().into(),
            reference.version.clone().into(),
            workspace.into(),
            thread.into(),
        ],
    )
}

pub(crate) async fn compaction_reference_payload(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Result<Option<String>> {
    if reference.scope.starts_with("task-basis:") {
        let reader = store.connection.begin_read().await?;
        let captured = pioneer_crud_compaction::task_basis::capture(
            &reader, workspace, thread, reference, None,
        )
        .await?;
        reader.commit().await?;
        return captured
            .map(|captured| captured.body.into_text())
            .transpose();
    }
    if reference.scope.starts_with("checkpoint:") {
        let row = SourceFragmentRow::find_by_statement(
            compaction_reference_checkpoint_payload_statement(workspace, thread, reference),
        )
        .one(&store.connection)
        .await?;
        return Ok(row.map(|row| row.fragment));
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
    let expected = exact_canonical_revision(reference, kind)?;
    let payload = match kind {
        CanonicalSource::Input => {
            read_source_payload(
                &store.connection,
                turn_input_projection(workspace, thread, turn),
                &reference.id,
                expected,
            )
            .await?
        }
        CanonicalSource::Event => {
            read_source_payload(
                &store.connection,
                turn_event_projection(workspace, thread, turn),
                &reference.id,
                expected,
            )
            .await?
        }
        CanonicalSource::ProviderContext => {
            read_source_payload(
                &store.connection,
                turn_llm_context_projection(workspace, thread, turn),
                &reference.id,
                expected,
            )
            .await?
        }
        CanonicalSource::ToolItem => {
            read_source_payload(
                &store.connection,
                turn_item_projection(workspace, thread, turn),
                &reference.id,
                expected,
            )
            .await?
        }
    };
    Ok(payload)
}

fn logical_payload_bytes<E: EntityTrait>(payload: E::Column, reference: Option<E::Column>) -> Expr {
    let plain = Func::cust(Alias::new("octet_length"))
        .arg(Expr::col((E::default(), payload)).cast_as("text"))
        .cast_as("bigint");
    match reference {
        None => plain,
        Some(reference) => {
            use pioneer_entity::portable_payload_value as p;
            Expr::case(Expr::col((E::default(), reference)).is_null(), plain)
                .finally(Expr::from(
                    Query::select()
                        .column(p::Column::OriginalBytes)
                        .from(p::Entity)
                        .and_where(
                            Expr::col((p::Entity, p::Column::Id))
                                .eq(Expr::col((E::default(), reference))),
                        )
                        .to_owned(),
                ))
                .into()
        }
    }
}

async fn read_source_payload<E: EntityTrait, P>(
    db: &pioneer_sqlite::SqliteDatabase,
    projection: CanonicalProjection<E, P>,
    id: &str,
    expected_revision: i64,
) -> Result<Option<String>> {
    if let Some(reference) = projection.portable {
        use pioneer_crud_payloads::storage::{self, Selection};
        let reader = db.begin_read().await?;
        let row = projection
            .query
            .select_only()
            .expr(Expr::col((E::default(), projection.payload)).cast_as("text"))
            .column(reference)
            .filter(projection.id.eq(id))
            .filter(projection.revision.eq(expected_revision))
            .filter(projection.present.eq(1_i64))
            .into_tuple::<(Option<String>, Option<String>)>()
            .one(&reader)
            .await?;
        let Some((inline, pointer)) = row else {
            return Ok(None);
        };
        let body = match pointer {
            Some(pointer) => Some(
                storage::capture(&reader, &pointer, Selection::Full)
                    .await?
                    .context("missing canonical event payload")?,
            ),
            None => None,
        };
        reader.commit().await?;
        return body
            .map(|body| body.into_text())
            .transpose()
            .map(|stored| stored.or(inline));
    }
    let row = projection
        .query
        .select_only()
        .expr(Expr::col((E::default(), projection.payload)).cast_as("text"))
        .filter(projection.id.eq(id))
        .filter(projection.revision.eq(expected_revision))
        .filter(projection.present.eq(1_i64))
        .into_tuple::<String>()
        .one(db)
        .await?;
    Ok(row)
}

/// Batch canonical history by row count and original UTF-8 bytes.
pub(crate) async fn compaction_reference_payload_batch(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    references: &[SourceRef],
) -> Result<(usize, Vec<String>)> {
    ensure!(!references.is_empty(), "empty history payload batch");
    let batchable = references
        .iter()
        .take(SOURCE_PAGE_ROWS as usize)
        .take_while(|source| {
            matches!(
                source.scope.split_once(':'),
                Some(("input" | "context" | "event" | "item", _))
            )
        })
        .count();
    if batchable == 0 {
        let payload = compaction_reference_payload(store, workspace, thread, &references[0])
            .await?
            .ok_or_else(|| anyhow::anyhow!("selected history source disappeared"))?;
        return Ok((1, vec![payload]));
    }
    let candidates = &references[..batchable];
    for reference in candidates {
        let kind = match reference.scope.split_once(':').map(|(kind, _)| kind) {
            Some("input") => CanonicalSource::Input,
            Some("context") => CanonicalSource::ProviderContext,
            Some("event") => CanonicalSource::Event,
            Some("item") => CanonicalSource::ToolItem,
            _ => unreachable!("source batch is restricted to canonical payload tables"),
        };
        exact_canonical_revision(reference, kind)?;
    }
    pioneer_crud_compaction::source_payloads::read_page(
        &store.connection,
        workspace,
        thread,
        candidates,
        SOURCE_PAGE_BYTES,
    )
    .await
}

pub(crate) async fn compaction_projection_version<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
) -> Result<u64> {
    let row = thread::Entity::find()
        .select_only()
        .join(
            JoinType::LeftJoin,
            thread::Entity::belongs_to(compaction_projection_epoch::Entity)
                .from(thread::Column::Id)
                .to(compaction_projection_epoch::Column::ThreadId)
                .into(),
        )
        .expr_as(
            Expr::expr(Func::cust(Alias::new("coalesce")).args([
                Expr::col((
                    compaction_projection_epoch::Entity,
                    compaction_projection_epoch::Column::Version,
                )),
                Expr::val(0_i64),
            ])),
            "version",
        )
        .filter(
            Expr::col((thread::Entity, thread::Column::Id))
                .eq(Expr::Value(thread.into()))
                .and(
                    Expr::col((thread::Entity, thread::Column::WorkspaceId))
                        .eq(Expr::Value(workspace.into())),
                ),
        )
        .into_tuple::<i64>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("context scope unavailable"))?;
    Ok(u64::try_from(row)?)
}
const COMPACTION_CHECKPOINT_SOURCE_SQL: &str = r#"
SELECT 'checkpoint:'||p.owner AS source_scope,p.id AS source_id,
 p.identity_sha256 AS source_version
FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
WHERE p.id=$1 AND c.workspace_id=$2 AND c.thread_id=$3
 AND p.format_version=1
 AND (p.status='applied' OR (p.status='retained' AND EXISTS(
  SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
LIMIT 1
"#;

fn compaction_checkpoint_source_statement(
    workspace: &str,
    thread: &str,
    checkpoint: &str,
) -> Statement {
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        COMPACTION_CHECKPOINT_SOURCE_SQL,
        [checkpoint.into(), workspace.into(), thread.into()],
    )
}

pub(crate) async fn compaction_checkpoint_source<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    checkpoint: &str,
) -> Result<Option<SourceRef>> {
    let row = compaction_live_sources::SourceRow::find_by_statement(
        compaction_checkpoint_source_statement(workspace, thread, checkpoint),
    )
    .one(db)
    .await?
    .map(|row| (row.source_scope, row.source_id, row.source_version));
    row.map(|(scope, id, version)| Ok(SourceRef { scope, id, version }))
        .transpose()
}

/// Resolve a runtime locator only after its durable append was acknowledged.
/// Reads metadata, never a full result, and cannot cross the execution scope.
pub(crate) async fn compaction_context_reference_for_item<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    item: &str,
    source: &str,
) -> Result<Option<SourceRef>> {
    ensure!(
        [
            "assistant_round",
            "tool_result_v2",
            "turn_input_projection_v1",
            "runtime_context_v1",
        ]
        .contains(&source),
        "unsupported canonical runtime locator"
    );
    let row = turn_llm_context::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            turn_llm_context::Entity::belongs_to(turn::Entity)
                .from(turn_llm_context::Column::TurnId)
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
            JoinType::LeftJoin,
            turn_llm_context::Entity::belongs_to(compaction_source_revision::Entity)
                .from(turn_llm_context::Column::Id)
                .to(compaction_source_revision::Column::SourceId)
                .into(),
        )
        .expr(Expr::col((
            turn_llm_context::Entity,
            turn_llm_context::Column::Id,
        )))
        .expr_as(
            Expr::expr(Func::cust(Alias::new("coalesce")).args([
                Expr::col((
                    compaction_source_revision::Entity,
                    compaction_source_revision::Column::Revision,
                )),
                Expr::val(1_i64),
            ])),
            "revision",
        )
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(Expr::col((thread::Entity, thread::Column::Id)).eq(Expr::Value(thread.into())))
                .and(
                    Expr::col((turn_llm_context::Entity, turn_llm_context::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((turn_llm_context::Entity, turn_llm_context::Column::ItemId))
                        .eq(Expr::Value(item.into())),
                )
                .and(
                    Expr::col((turn_llm_context::Entity, turn_llm_context::Column::Source))
                        .eq(Expr::Value(source.into())),
                ),
        )
        .order_by(
            Expr::col((turn_llm_context::Entity, turn_llm_context::Column::Sequence)),
            Order::Desc,
        )
        .limit(1)
        .into_tuple::<(String, i64)>()
        .one(db)
        .await?;
    row.map(|(id, revision)| {
        Ok(SourceRef {
            scope: format!("context:{turn}"),
            id,
            version: format!("revision:{}", revision),
        })
    })
    .transpose()
}
pub(crate) use pioneer_crud_compaction::tool_sources::compaction_tool_item_reference;
use pioneer_crud_compaction::tool_sources::turn_item_has_shell_storage;

/// Resolve a saved shell result or frozen provider representation to its tool
/// item. Read identity metadata only and require the exact live revision.
pub(crate) async fn compaction_replay_item_id<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    source: &SourceRef,
) -> Result<Option<String>> {
    if let Some(turn) = source.scope.strip_prefix("item:") {
        replay_item_id(
            db,
            turn_item_projection(workspace, thread, turn),
            turn_item::Column::ItemId,
            turn_item_has_shell_storage(),
            workspace,
            thread,
            source,
        )
        .await
    } else if let Some(turn) = source.scope.strip_prefix("context:") {
        replay_item_id(
            db,
            turn_llm_context_projection(workspace, thread, turn),
            turn_llm_context::Column::ItemId,
            turn_llm_context::Column::Source.eq("tool_result_v2"),
            workspace,
            thread,
            source,
        )
        .await
    } else {
        Ok(None)
    }
}

pub(crate) async fn compaction_item_reference<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    item: &str,
) -> Result<Option<SourceRef>> {
    let row = turn_item::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            turn_item::Entity::belongs_to(turn::Entity)
                .from(turn_item::Column::TurnId)
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
            JoinType::LeftJoin,
            turn_item::Entity::belongs_to(compaction_item_revision::Entity)
                .from(turn_item::Column::Id)
                .to(compaction_item_revision::Column::SourceId)
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((
                            compaction_item_revision::Entity,
                            compaction_item_revision::Column::TurnId,
                        ))
                        .eq(Expr::col((turn_item::Entity, turn_item::Column::TurnId))),
                    )
                })
                .into(),
        )
        .expr(Expr::col((turn_item::Entity, turn_item::Column::Id)))
        .expr_as(
            Expr::expr(Func::cust(Alias::new("coalesce")).args([
                Expr::col((
                    compaction_item_revision::Entity,
                    compaction_item_revision::Column::Revision,
                )),
                Expr::val(1_i64),
            ])),
            "revision",
        )
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(Expr::col((thread::Entity, thread::Column::Id)).eq(Expr::Value(thread.into())))
                .and(
                    Expr::col((turn_item::Entity, turn_item::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((turn_item::Entity, turn_item::Column::ItemId))
                        .eq(Expr::Value(item.into())),
                ),
        )
        .into_tuple::<(String, i64)>()
        .one(db)
        .await?;
    row.map(|(id, revision)| {
        Ok(SourceRef {
            scope: format!("item:{turn}"),
            id,
            version: format!("item-revision:{}", revision),
        })
    })
    .transpose()
}

use super::compaction_live_sources;

#[derive(FromQueryResult)]
struct SourceFragmentRow {
    revision: i64,
    fragment: String,
    characters: i64,
}

async fn read_source_fragment<E: EntityTrait, P>(
    db: &pioneer_sqlite::SqliteDatabase,
    projection: CanonicalProjection<E, P>,
    id: &str,
    offset: i64,
) -> Result<Option<SourceFragmentRow>> {
    const PAGE_CHARACTERS: usize = 16_384;
    if let Some(reference) = projection.portable {
        use pioneer_crud_payloads::storage::{self, Selection};
        let reader = db.begin_read().await?;
        let row = projection
            .query
            .select_only()
            .expr(projection.revision)
            .expr(Expr::col((E::default(), projection.payload)).cast_as("text"))
            .column(reference)
            .filter(projection.id.eq(id))
            .filter(projection.present.eq(1_i64))
            .into_tuple::<(i64, Option<String>, Option<String>)>()
            .one(&reader)
            .await?;
        let Some((revision, inline, pointer)) = row else {
            return Ok(None);
        };
        let start = usize::try_from(offset - 1)?;
        if let Some(pointer) = pointer {
            let meta = storage::metadata(&reader, &pointer)
                .await?
                .context("missing canonical event payload")?;
            ensure!(start <= meta.original_chars, "source offset is past end");
            let end = start
                .saturating_add(PAGE_CHARACTERS)
                .min(meta.original_chars);
            let captured = storage::capture(&reader, &pointer, Selection::Characters(start..end))
                .await?
                .context("missing canonical event payload")?;
            reader.commit().await?;
            return Ok(Some(SourceFragmentRow {
                revision,
                fragment: captured.into_text()?,
                characters: i64::try_from(meta.original_chars)?,
            }));
        }
        reader.commit().await?;
        let inline = inline.context("missing inline event payload")?;
        return Ok(Some(SourceFragmentRow {
            revision,
            fragment: inline.chars().skip(start).take(PAGE_CHARACTERS).collect(),
            characters: i64::try_from(inline.chars().count())?,
        }));
    }
    let offset = i32::try_from(offset).context("source offset is past end")?;
    Ok(projection
        .query
        .select_only()
        .expr_as(projection.revision, "revision")
        .expr_as(
            Func::cust(Alias::new("substr")).args([
                Expr::col((E::default(), projection.payload)),
                Expr::val(offset),
                Expr::val(PAGE_CHARACTERS as i32),
            ]),
            "fragment",
        )
        .expr_as(
            Expr::from(Func::char_length(Expr::col((
                E::default(),
                projection.payload,
            ))))
            .cast_as("bigint"),
            "characters",
        )
        .filter(projection.id.eq(id))
        .filter(projection.present.eq(1_i64))
        .into_model::<SourceFragmentRow>()
        .one(db)
        .await?)
}

#[allow(clippy::too_many_arguments)]
async fn replay_item_id<C: ConnectionTrait, E: EntityTrait, P>(
    db: &C,
    projection: CanonicalProjection<E, P>,
    item: E::Column,
    kind: Expr,
    workspace: &str,
    thread: &str,
    source: &SourceRef,
) -> Result<Option<String>> {
    Ok(projection
        .query
        .select_only()
        .column(item)
        .join(
            JoinType::InnerJoin,
            compaction_live_sources::join(
                E::default(),
                projection.id,
                compaction_live_sources::Column::SourceId,
            ),
        )
        .filter(
            Expr::col((
                compaction_live_sources::Column::Table,
                compaction_live_sources::Column::WorkspaceId,
            ))
            .eq(workspace),
        )
        .filter(
            Expr::col((
                compaction_live_sources::Column::Table,
                compaction_live_sources::Column::ThreadId,
            ))
            .eq(thread),
        )
        .filter(
            Expr::col((
                compaction_live_sources::Column::Table,
                compaction_live_sources::Column::SourceScope,
            ))
            .eq(source.scope.clone()),
        )
        .filter(
            Expr::col((
                compaction_live_sources::Column::Table,
                compaction_live_sources::Column::SourceId,
            ))
            .eq(source.id.clone()),
        )
        .filter(
            Expr::col((
                compaction_live_sources::Column::Table,
                compaction_live_sources::Column::SourceVersion,
            ))
            .eq(source.version.clone()),
        )
        .filter(kind)
        .into_tuple::<String>()
        .one(db)
        .await?)
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

#[derive(FromQueryResult)]
struct MatchedSourceCount {
    matched: i64,
}

pub use pioneer_crud_compaction::history_check_result::{
    HistoryCheckDiagnostic, HistoryCheckOutcome,
};

#[cfg(test)]
#[path = "compaction_source_lookup_tests.rs"]
mod source_lookup_tests;
