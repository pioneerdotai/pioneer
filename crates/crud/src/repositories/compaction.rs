//! Durable compaction metadata. Payloads remain in canonical history tables.
pub(crate) use super::compaction_background as background;
pub(crate) use super::compaction_frozen as frozen;
pub(crate) use super::compaction_frozen_import as frozen_import;
pub(crate) use super::compaction_history as history;
pub(crate) use super::compaction_lifecycle as lifecycle;
pub(crate) use super::compaction_runner as runner;
pub(crate) use super::compaction_source_projection as source_projection;
pub(crate) use super::compaction_task_output as task_output;
use crate::CrudStore;
use anyhow::{Result, ensure};
pub use background::{CompactionLifecycleRecovery, CompletedHistoryCheck};
pub use frozen_import::{
    EMPTY_FROZEN_IMPORT_SHA256, FROZEN_IMPORT_PAGE_BYTES, FrozenImportRecord, PreparedFrozenImport,
    frozen_import_identity,
};
pub use history::{
    AcceptedTaskBasis, HistoryCausalBoundary, HistoryReadFence, HistoryTurnBoundary,
    event_projection_metadata,
};
use pioneer_compaction::{Checkpoint, FORMAT_VERSION, OperationSnapshot, SourceRef};
use pioneer_entity::{
    compaction_checkpoint, compaction_context, compaction_coverage, compaction_event_revision,
    compaction_execution_stop, compaction_input_revision, compaction_item_revision,
    compaction_operation, compaction_projection_epoch, compaction_source_revision,
    task_run_conversation_snapshot, thread, turn, turn_event, turn_input, turn_item,
    turn_llm_context,
};
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
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement, TransactionTrait, Value};
use sha2::{Digest, Sha256};
pub(crate) use task_output::bind_queued_task_output;
pub use task_output::{
    DeliveredTaskOutputPage, DeliveredTaskOutputRef, TaskDeliveryOutputSnapshot, TaskOutputSnapshot,
};

#[derive(Clone, Debug)]
pub struct CheckpointEdges {
    pub owner: String,
    pub identity_sha256: String,
    pub previous: Option<String>,
    pub format_version: u32,
    pub coverage: Vec<SourceRef>,
}

#[derive(Clone, Debug)]
pub struct CheckpointBody {
    pub id: String,
    pub operation_id: String,
    pub owner: String,
    pub previous: Option<String>,
    pub summary: String,
    pub identity_sha256: String,
    pub selection: pioneer_compaction::ModelSelection,
    pub projection_version: u64,
    pub format_version: u32,
}

#[derive(Clone, Debug)]
pub struct CheckpointMetadata {
    pub id: String,
    pub operation_id: String,
    pub owner: String,
    pub previous: Option<String>,
    pub identity_sha256: String,
    pub selection: pioneer_compaction::ModelSelection,
    pub projection_version: u64,
    pub format_version: u32,
}

pub const SOURCE_PAGE_ROWS: u64 = 128;
pub const SOURCE_PAGE_BYTES: usize = 256 * 1024;
pub const CHECKPOINT_SOURCE_LIMIT: usize = 256;

// Kept only for correlated SQLite json_each snapshot validation and the two
// MATERIALIZED event quanta. These queries preserve one atomic validation or a
// physical scan boundary; ordinary reads/writes use Entity/ActiveModel.
pub(super) fn sqlite_specific_sql(
    statement: &str,
    values: impl IntoIterator<Item = Value>,
) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, statement, values)
}

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

#[derive(Clone, Copy, Debug)]
pub enum CanonicalSource {
    Input,
    Event,
    ProviderContext,
    ToolItem,
}
impl CanonicalSource {
    pub(super) fn version_prefix(self) -> &'static str {
        match self {
            Self::Input => "input-revision",
            Self::Event => "event-revision",
            Self::ProviderContext => "revision",
            Self::ToolItem => "item-revision",
        }
    }
    pub(super) fn prefix(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Event => "event",
            Self::ProviderContext => "context",
            Self::ToolItem => "item",
        }
    }
}

#[derive(Clone, Debug)]
pub struct SourceRecord {
    pub source_type: String,
    pub tool_name: Option<String>,
    pub projection_kind: Option<String>,
    pub item_id: Option<String>,
    pub reference: SourceRef,
    pub sequence: i64,
    pub payload: Option<String>,
    pub incomplete: bool,
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

#[derive(Clone, Debug, FromQueryResult)]
pub struct OperationRecord {
    pub id: String,
    pub owner: String,
    pub status: String,
    pub snapshot: String,
    pub deadline_ms: i64,
    pub attempts: i64,
    pub transient_retries: i64,
    pub correction: i64,
    pub next_portion: i64,
    pub outcome: Option<String>,
}

impl From<compaction_operation::Model> for OperationRecord {
    fn from(row: compaction_operation::Model) -> Self {
        Self {
            id: row.id,
            owner: row.owner,
            status: row.status,
            snapshot: row.snapshot,
            deadline_ms: row.deadline_ms,
            attempts: row.attempts,
            transient_retries: row.transient_retries,
            correction: row.correction,
            next_portion: row.next_portion,
            outcome: row.outcome,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Applied,
    AlreadyApplied,
    /// A reader proof raced a relevant mutation. The candidate and successful
    /// provider result remain durable; only publication validation is retried.
    RetryValidation,
    Stale,
    Cancelled,
}

pub(super) fn checkpoint_identity(checkpoint: &Checkpoint) -> Result<String> {
    let mut canonical = checkpoint.clone();
    canonical.coverage.sort();
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&canonical)?)))
}

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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compaction_source_page_inner<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
    include_payload: bool,
    capture_order: i64,
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
    revision: Expr,
    present: Expr,
    paging: P,
}

struct SourcePaging {
    sequence: Expr,
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
    source_type: String,
    item_id: Option<String>,
    tool_name: Option<String>,
    projection_kind: Option<String>,
    revision: i64,
    bytes: i64,
}

#[allow(clippy::too_many_arguments)]
async fn read_source_page<C: ConnectionTrait, E: EntityTrait>(
    db: &C,
    projection: CanonicalProjection<E>,
    kind: PagedSource,
    turn: &str,
    after: i64,
    include_payload: bool,
    capture_order: i64,
) -> Result<SourcePage> {
    let kind = CanonicalSource::from(kind);
    let payload_bytes: Expr = Func::char_length(
        Expr::col((E::default(), projection.payload)).cast_as(Alias::new("BLOB")),
    )
    .into();
    let rows = projection
        .query
        .clone()
        .select_only()
        .column(projection.id)
        .expr_as(projection.paging.sequence.clone(), "sequence")
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
            projection
                .query
                .clone()
                .select_only()
                .column(projection.payload)
                .filter(projection.id.eq(row.id.clone()))
                .filter(projection.revision.clone().eq(row.revision))
                .filter(
                    payload_bytes
                        .clone()
                        .lte((SOURCE_PAGE_BYTES - bytes) as u64),
                )
                .into_tuple::<String>()
                .one(db)
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

pub(crate) async fn compaction_admit(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    snapshot: &OperationSnapshot,
) -> Result<OperationRecord> {
    store
        .compaction_admit_for_turn(workspace, thread, snapshot, None)
        .await
}

pub(crate) async fn compaction_admit_for_turn(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    snapshot: &OperationSnapshot,
    execution_turn: Option<&str>,
) -> Result<OperationRecord> {
    let snapshot_json = serde_json::to_string(snapshot)?;
    ensure!(
        snapshot_json.len() <= SOURCE_PAGE_BYTES,
        "operation metadata exceeds bounded admission"
    );
    let deadline = i64::try_from(snapshot.admission.deadline_ms)?;
    // Preparation reads no mutable DB state. Owner and format are revalidated inside the write boundary.
    store.run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            compaction_context::Entity::insert(compaction_context::ActiveModel {workspace_id: sea_orm::Set((workspace).to_owned()), thread_id: sea_orm::Set((thread).to_owned()), owner: sea_orm::Set((snapshot.owner.clone()).to_owned()), ..Default::default() }).on_conflict(OnConflict::columns([compaction_context::Column::Owner]).do_nothing().to_owned()).exec_without_returning(&txn).await?;
            let valid = compaction_context::Entity::find()
            .select_only()
            .column(compaction_context::Column::Owner)
            .filter(Expr::col(compaction_context::Column::Owner)
                .eq(Expr::Value(snapshot.owner.clone()
                        .into()))
                .and(Expr::col(compaction_context::Column::WorkspaceId)
                    .eq(Expr::Value(workspace.into())))
                .and(Expr::col(compaction_context::Column::ThreadId)
                    .eq(Expr::Value(thread.into())))
                .and(Expr::col(compaction_context::Column::FormatVersion)
                    .eq(Expr::val(1_i64))))
            .into_tuple::<String>()
            .one(&txn)
            .await?;
            ensure!(valid.is_some(), "compaction owner scope or format mismatch");
            if let Some(turn) = execution_turn {
                ensure!(compaction_context::Entity::find()
                    .select_only()
                    .join(JoinType::InnerJoin, compaction_context::Entity::belongs_to(turn::Entity)
                        .from(compaction_context::Column::ThreadId)
                        .to(turn::Column::ThreadId)
                        .into())
                    .expr(Expr::col((compaction_context::Entity, compaction_context::Column::Owner)))
                    .filter(Expr::col((compaction_context::Entity, compaction_context::Column::Owner))
                        .eq(Expr::Value(snapshot.owner.clone()
                                .into()))
                        .and(Expr::col((turn::Entity, turn::Column::Id))
                            .eq(Expr::Value(turn.into())))
                        .and(Expr::exists(Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_execution_stop::Entity, "stop")
                                .and_where(Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                    .eq(Expr::col((compaction_context::Entity, compaction_context::Column::Owner)))
                                    .and(Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                        .eq(Expr::col((turn::Entity, turn::Column::Id)))))
                                .to_owned())
                            .not())
                        .and(Expr::col((turn::Entity, turn::Column::Status))
                            .is_in(["interrupted", "cancelled"])
                            .not()))
                    .into_tuple::<String>()
                    .one(&txn)
                    .await?.is_some(), "compaction execution was stopped or changed scope");
            }
            // Epochs were read before the source projection was resolved.
            // Revalidate their workspace and revision inside admission; parsing
            // the bounded metadata here is part of this atomic DB transition.
            let changed = txn.query_one_raw(sqlite_specific_sql("SELECT wanted.key FROM json_each(?,'$.source_epochs') wanted WHERE NOT EXISTS (SELECT 1 FROM thread t LEFT JOIN compaction_projection_epoch e ON e.thread_id=t.id WHERE t.id=wanted.key AND t.workspace_id=? AND COALESCE(e.version,0)=wanted.value) LIMIT 1", [snapshot_json.clone().into(), workspace.into()])).await?;
            ensure!(changed.is_none(), "source scope or epoch changed before admission");
            compaction_operation::Entity::insert(compaction_operation::ActiveModel {id: sea_orm::Set((snapshot.id.clone())
                        .to_owned()), owner: sea_orm::Set((snapshot.owner.clone())
                        .to_owned()), fingerprint: sea_orm::Set((snapshot.plan.fingerprint.clone())
                        .to_owned()), status: sea_orm::Set(("running")
                        .to_owned()), snapshot: sea_orm::Set((snapshot_json.clone())
                        .to_owned()), deadline_ms: sea_orm::Set(deadline), expected_head: sea_orm::Set(snapshot.expected_checkpoint.clone()), execution_turn: sea_orm::Set(execution_turn.map(str::to_owned)), ..Default::default() })
            .on_conflict(OnConflict::columns([compaction_operation::Column::Owner, compaction_operation::Column::Fingerprint])
                .do_nothing()
                .to_owned())
            .exec_without_returning(&txn)
            .await?;
            txn.commit().await?;
            Ok(())
        }).await?;
    compaction_operation::Entity::find()
        .filter(compaction_operation::Column::Owner.eq(snapshot.owner.clone()))
        .filter(compaction_operation::Column::Fingerprint.eq(snapshot.plan.fingerprint.clone()))
        .into_model::<OperationRecord>()
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("admitted operation missing"))
}

pub(crate) async fn compaction_operation_for_plan<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    owner: &str,
    fingerprint: &str,
) -> Result<Option<OperationRecord>> {
    use sea_orm::ColumnTrait;
    Ok(compaction_operation::Entity::find()
        .inner_join(compaction_context::Entity)
        .filter(compaction_context::Column::WorkspaceId.eq(workspace))
        .filter(compaction_context::Column::ThreadId.eq(thread))
        .filter(compaction_operation::Column::Owner.eq(owner))
        .filter(compaction_operation::Column::Fingerprint.eq(fingerprint))
        .one(db)
        .await?
        .map(OperationRecord::from))
}

pub(crate) async fn compaction_operation<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<OperationRecord>> {
    use pioneer_entity::compaction_operation as operation;
    use sea_orm::EntityTrait;
    Ok(operation::Entity::find_by_id(id)
        .one(db)
        .await?
        .map(OperationRecord::from))
}

/// Validate a bounded batch of exact identities after preparing its JSON
/// outside reader capacity. An edited/deleted source or stale summary may
/// never pass an otherwise-fitting native preflight as current history.
const COMPACTION_SOURCES_CURRENT_SQL: &str = r#"
SELECT COUNT(*) AS matched
FROM json_each(?1) wanted
WHERE
  EXISTS (
    SELECT 1
    FROM compaction_source_revision context_revision
    JOIN turn_llm_context context_source
      ON context_source.id=context_revision.source_id
     AND context_source.turn_id=context_revision.turn_id
    JOIN turn context_turn ON context_turn.id=context_revision.turn_id
    JOIN thread context_thread ON context_thread.id=context_turn.thread_id
    WHERE context_revision.present=1
      AND context_thread.workspace_id=?2
      AND context_turn.thread_id=?3
      AND 'context:'||context_revision.turn_id=json_extract(wanted.value,'$.scope')
      AND context_revision.source_id=json_extract(wanted.value,'$.id')
      AND 'revision:'||context_revision.revision=json_extract(wanted.value,'$.version')
  )
  OR EXISTS (
    SELECT 1
    FROM compaction_item_revision item_revision
    JOIN turn_item item_source
      ON item_source.id=item_revision.source_id
     AND item_source.turn_id=item_revision.turn_id
    JOIN turn item_turn ON item_turn.id=item_revision.turn_id
    JOIN thread item_thread ON item_thread.id=item_turn.thread_id
    WHERE item_revision.present=1
      AND item_thread.workspace_id=?2
      AND item_turn.thread_id=?3
      AND 'item:'||item_revision.turn_id=json_extract(wanted.value,'$.scope')
      AND item_revision.source_id=json_extract(wanted.value,'$.id')
      AND 'item-revision:'||item_revision.revision=json_extract(wanted.value,'$.version')
  )
  OR EXISTS (
    SELECT 1
    FROM compaction_event_revision event_revision
    JOIN turn_event event_source
      ON event_source.id=event_revision.source_id
     AND event_source.turn_id=event_revision.turn_id
    JOIN turn event_turn ON event_turn.id=event_revision.turn_id
    JOIN thread event_thread ON event_thread.id=event_turn.thread_id
    WHERE event_revision.present=1
      AND event_thread.workspace_id=?2
      AND event_turn.thread_id=?3
      AND 'event:'||event_revision.turn_id=json_extract(wanted.value,'$.scope')
      AND event_revision.source_id=json_extract(wanted.value,'$.id')
      AND 'event-revision:'||event_revision.revision=json_extract(wanted.value,'$.version')
  )
  OR EXISTS (
    SELECT 1
    FROM compaction_input_revision input_revision
    JOIN turn_input input_source
      ON input_source.id=input_revision.source_id
     AND input_source.turn_id=input_revision.turn_id
    JOIN turn input_turn ON input_turn.id=input_revision.turn_id
    JOIN thread input_thread ON input_thread.id=input_turn.thread_id
    WHERE input_revision.present=1
      AND input_thread.workspace_id=?2
      AND input_turn.thread_id=?3
      AND 'input:'||input_revision.turn_id=json_extract(wanted.value,'$.scope')
      AND input_revision.source_id=json_extract(wanted.value,'$.id')
      AND 'input-revision:'||input_revision.revision=json_extract(wanted.value,'$.version')
  )
  OR EXISTS (
    SELECT 1
    FROM compaction_checkpoint live_checkpoint
    JOIN compaction_context live_context ON live_context.owner=live_checkpoint.owner
    LEFT JOIN compaction_projection_epoch live_epoch ON live_epoch.thread_id=live_context.thread_id
    WHERE (
        live_checkpoint.status='applied'
        OR (
          live_checkpoint.status='retained'
          AND EXISTS (
            SELECT 1
            FROM compaction_operation live_operation
            WHERE live_operation.id=live_checkpoint.operation_id
              AND live_operation.status='completed'
          )
        )
      )
      AND live_checkpoint.projection_version=COALESCE(live_epoch.version,0)
      AND live_context.workspace_id=?2
      AND live_context.thread_id=?3
      AND 'checkpoint:'||live_checkpoint.owner=json_extract(wanted.value,'$.scope')
      AND live_checkpoint.id=json_extract(wanted.value,'$.id')
      AND live_checkpoint.identity_sha256=json_extract(wanted.value,'$.version')
  )
  OR EXISTS (
    SELECT 1
    FROM task_run_conversation_snapshot basis
    JOIN thread basis_thread
      ON basis_thread.id=basis.conversation_thread_id
     AND basis_thread.workspace_id=basis.workspace_id
    LEFT JOIN compaction_task_basis_revision basis_revision
      ON basis_revision.run_id=basis.run_id
    WHERE substr(ltrim(basis.history_json),1,1)='['
      AND basis.workspace_id=?2
      AND basis.conversation_thread_id=?3
      AND 'task-basis:'||basis.run_id=json_extract(wanted.value,'$.scope')
      AND basis.run_id=json_extract(wanted.value,'$.id')
      AND 'task-basis-revision:'||COALESCE(basis_revision.revision,1)=json_extract(wanted.value,'$.version')
  )
  OR EXISTS (
    SELECT 1
    FROM compaction_checkpoint immutable_checkpoint
    JOIN compaction_context immutable_context ON immutable_context.owner=immutable_checkpoint.owner
    WHERE immutable_context.workspace_id=?4
      AND immutable_context.thread_id=?5
      AND 'checkpoint:'||immutable_checkpoint.owner=json_extract(wanted.value,'$.scope')
      AND immutable_checkpoint.id=json_extract(wanted.value,'$.id')
      AND immutable_checkpoint.identity_sha256=json_extract(wanted.value,'$.version')
      AND immutable_checkpoint.format_version=1
      AND (
        immutable_checkpoint.status='applied'
        OR (
          immutable_checkpoint.status='retained'
          AND EXISTS (
            SELECT 1
            FROM compaction_operation immutable_operation
            WHERE immutable_operation.id=immutable_checkpoint.operation_id
              AND immutable_operation.status='completed'
          )
        )
      )
  )
"#;

fn compaction_sources_current_statement(
    payload: String,
    workspace: &str,
    thread: &str,
) -> Statement {
    sqlite_specific_sql(
        COMPACTION_SOURCES_CURRENT_SQL,
        [
            payload.into(),
            workspace.into(),
            thread.into(),
            workspace.into(),
            thread.into(),
        ],
    )
}

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
    // A checkpoint identity survives unrelated appends; callers expand and
    // validate every DAG leaf immediately after this bounded identity batch.
    // Its publication epoch is not a lifetime for the immutable checkpoint.
    let row = MatchedSourceCount::find_by_statement(compaction_sources_current_statement(
        payload, workspace, thread,
    ))
    .one(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("source validation missing"))?;
    Ok(row.matched == sources.len() as i64)
}

/// Resolve only the storage scope of one exact current revision. Used to
/// validate transitive checkpoint coverage before any source body is read.
/// The caller still checks the returned thread against accepted scopes.
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
            sqlite_specific_sql(
                "SELECT c.thread_id FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner WHERE c.workspace_id=? AND 'checkpoint:'||p.owner=? AND p.id=? AND p.identity_sha256=? AND p.format_version=1 AND (p.status='applied' OR (p.status='retained' AND EXISTS(SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed'))) LIMIT 1",
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
) -> Result<()> {
    ensure!(
        ["failed", "cancelled", "stale"].contains(&status),
        "invalid terminal transition"
    );
    ensure!(
        outcome.len() <= 128,
        "outcome must be a bounded classification"
    );
    compaction_operation::Entity::update_many()
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
    Ok(())
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
            compaction_checkpoint::Entity::insert(compaction_checkpoint::ActiveModel {
                id: sea_orm::Set((checkpoint.id.clone()).to_owned()),
                operation_id: sea_orm::Set((checkpoint.operation_id.clone()).to_owned()),
                owner: sea_orm::Set((checkpoint.owner.clone()).to_owned()),
                previous: sea_orm::Set(checkpoint.previous.clone()),
                portion: sea_orm::Set(portion),
                summary: sea_orm::Set((checkpoint.summary.clone()).to_owned()),
                selection: sea_orm::Set((selection.clone()).to_owned()),
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
                    Expr::expr(Func::cust(Alias::new("max")).args([
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

async fn checkpoint_coverage<C: ConnectionTrait>(db: &C, id: &str) -> Result<Vec<SourceRef>> {
    use sea_orm::{ColumnTrait, QueryOrder, QuerySelect};
    let rows = compaction_coverage::Entity::find()
        .select_only()
        .column(compaction_coverage::Column::SourceScope)
        .column(compaction_coverage::Column::SourceId)
        .column(compaction_coverage::Column::SourceVersion)
        .filter(compaction_coverage::Column::CheckpointId.eq(id))
        .order_by_asc(compaction_coverage::Column::SourceScope)
        .order_by_asc(compaction_coverage::Column::SourceId)
        .limit(CHECKPOINT_SOURCE_LIMIT as u64 + 1)
        .into_tuple::<(String, String, String)>()
        .all(db)
        .await?;
    ensure!(
        rows.len() <= CHECKPOINT_SOURCE_LIMIT,
        "checkpoint coverage exceeds supported quantum"
    );
    Ok(rows
        .into_iter()
        .map(|(scope, id, version)| SourceRef { scope, id, version })
        .collect())
}

/// Coverage discovery must not read summary text before source authorization.
pub(crate) async fn compaction_checkpoint_edges<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CheckpointEdges>> {
    use sea_orm::QuerySelect;
    let Some((owner, identity_sha256, previous, format_version)) =
        compaction_checkpoint::Entity::find_by_id(id)
            .select_only()
            .column(compaction_checkpoint::Column::Owner)
            .column(compaction_checkpoint::Column::IdentitySha256)
            .column(compaction_checkpoint::Column::Previous)
            .column(compaction_checkpoint::Column::FormatVersion)
            .into_tuple::<(String, String, Option<String>, i64)>()
            .one(db)
            .await?
    else {
        return Ok(None);
    };
    let coverage = checkpoint_coverage(db, id).await?;
    Ok(Some(CheckpointEdges {
        owner,
        identity_sha256,
        previous,
        format_version: u32::try_from(format_version)?,
        coverage,
    }))
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
        selection: serde_json::from_str(&row.selection)?,
        coverage,
        projection_version: row.projection_version as u64,
        format_version: row.format_version as u32,
    }))
}

/// Load projection data without re-reading coverage. Callers must first
/// authorize and validate the exact checkpoint SourceRef through metadata.
pub(crate) async fn compaction_checkpoint_body<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CheckpointBody>> {
    use sea_orm::QuerySelect;
    let Some((
        id,
        operation_id,
        owner,
        previous,
        summary,
        identity_sha256,
        selection,
        projection_version,
        format_version,
    )) = compaction_checkpoint::Entity::find_by_id(id)
        .select_only()
        .column(compaction_checkpoint::Column::Id)
        .column(compaction_checkpoint::Column::OperationId)
        .column(compaction_checkpoint::Column::Owner)
        .column(compaction_checkpoint::Column::Previous)
        .column(compaction_checkpoint::Column::Summary)
        .column(compaction_checkpoint::Column::IdentitySha256)
        .column(compaction_checkpoint::Column::Selection)
        .column(compaction_checkpoint::Column::ProjectionVersion)
        .column(compaction_checkpoint::Column::FormatVersion)
        .into_tuple::<(
            String,
            String,
            String,
            Option<String>,
            String,
            String,
            String,
            i64,
            i64,
        )>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    // Decode only after the bounded database read has released its capacity.
    Ok(Some(CheckpointBody {
        id,
        operation_id,
        owner,
        previous,
        summary,
        identity_sha256,
        selection: serde_json::from_str(&selection)?,
        projection_version: u64::try_from(projection_version)?,
        format_version: u32::try_from(format_version)?,
    }))
}

/// Projection metadata without summary payload. Selection is decoded here to
/// preserve the existing fail-closed validation without retaining body text.
/// Callers must first authorize the exact checkpoint SourceRef through edges.
pub(crate) async fn compaction_checkpoint_metadata<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CheckpointMetadata>> {
    use sea_orm::QuerySelect;
    let Some((
        id,
        operation_id,
        owner,
        previous,
        identity_sha256,
        selection,
        projection_version,
        format_version,
    )) = compaction_checkpoint::Entity::find_by_id(id)
        .select_only()
        .column(compaction_checkpoint::Column::Id)
        .column(compaction_checkpoint::Column::OperationId)
        .column(compaction_checkpoint::Column::Owner)
        .column(compaction_checkpoint::Column::Previous)
        .column(compaction_checkpoint::Column::IdentitySha256)
        .column(compaction_checkpoint::Column::Selection)
        .column(compaction_checkpoint::Column::ProjectionVersion)
        .column(compaction_checkpoint::Column::FormatVersion)
        .into_tuple::<(
            String,
            String,
            String,
            Option<String>,
            String,
            String,
            i64,
            i64,
        )>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(CheckpointMetadata {
        id,
        operation_id,
        owner,
        previous,
        identity_sha256,
        selection: serde_json::from_str(&selection)?,
        projection_version: u64::try_from(projection_version)?,
        format_version: u32::try_from(format_version)?,
    }))
}

/// Atomic CAS. Appends do not invalidate selected sources; edits, Stop and another head do.
pub(crate) async fn compaction_apply(
    store: &CrudStore,
    checkpoint: &Checkpoint,
    expected_head: Option<&str>,
    assertions: &[SourceAssertion],
) -> Result<CommitOutcome> {
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
            let op = compaction_operation::Entity::find()
            .select_only()
            .column(compaction_operation::Column::Status)
            .filter(Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(checkpoint.operation_id.clone()
                        .into()))
                .and(Expr::col(compaction_operation::Column::Owner)
                    .eq(Expr::Value(checkpoint.owner.clone()
                            .into())))
                .and(Expr::col(compaction_operation::Column::ExpectedHead)
                    .binary(BinOper::Is, Expr::Value(expected_head.map(str::to_owned)
                            .into()))))
            .into_tuple::<String>()
            .one(&txn)
            .await?;
            let status: String = op.ok_or_else(|| anyhow::anyhow!("operation missing"))?;
            if status == "completed" { txn.rollback().await?; return Ok(CommitOutcome::AlreadyApplied); }
            if status != "running" { txn.rollback().await?; return Ok(CommitOutcome::Cancelled); }
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
                    .binary(BinOper::Is, Expr::val(Option::<String>::None))
                    .not())
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
            let dependency_changed = txn.query_one_raw(sqlite_specific_sql("SELECT o.id FROM compaction_operation o WHERE o.id=? AND EXISTS (SELECT 1 FROM json_each(o.snapshot,'$.source_epochs') wanted WHERE COALESCE((SELECT version FROM compaction_projection_epoch WHERE thread_id=wanted.key),0)<>wanted.value OR NOT EXISTS (SELECT 1 FROM thread t JOIN compaction_context c ON c.workspace_id=t.workspace_id WHERE t.id=wanted.key AND c.owner=o.owner))", [checkpoint.operation_id.clone()
                        .into()]))
            .await?;
            if dependency_changed.is_some() { txn.rollback().await?; return Ok(CommitOutcome::Stale); }
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
                .and(Expr::col(compaction_context::Column::Head)
                    .binary(BinOper::Is, Expr::Value(expected_head.map(str::to_owned)
                            .into())))
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

pub(crate) async fn compaction_payload_fragment<C: ConnectionTrait>(
    db: &C,
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
        .expr(Expr::col((turn_item::Entity, turn_item::Column::Id)))
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((turn::Entity, turn::Column::ThreadId))
                        .eq(Expr::Value(thread.into())),
                )
                .and(
                    Expr::col((turn_item::Entity, turn_item::Column::TurnId))
                        .eq(Expr::Value(turn.into())),
                )
                .and(
                    Expr::col((turn_item::Entity, turn_item::Column::ItemId))
                        .eq(Expr::Value(item.into())),
                )
                .and(Expr::expr(Func::cust(Alias::new("json_valid")).args([
                    Expr::col((turn_item::Entity, turn_item::Column::Payload)),
                ])))
                .and(
                    Expr::expr(Func::cust(Alias::new("json_extract")).args([
                        Expr::col((turn_item::Entity, turn_item::Column::Payload)),
                        Expr::val("$.storage.kind"),
                    ]))
                    .eq(Expr::val("shell")),
                ),
        )
        .limit(1)
        .into_tuple::<String>()
        .one(&store.connection)
        .await?;
    if let Some(id) = row {
        return store
            .compaction_payload_fragment(
                workspace,
                thread,
                turn,
                &id,
                expected_revision,
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
        let offset = i64::try_from(character_offset)?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("source offset overflow"))?;
        let row = task_run_conversation_snapshot::Entity::find()
            .select_only()
            .join(
                JoinType::InnerJoin,
                compaction_live_sources::join(
                    task_run_conversation_snapshot::Entity,
                    task_run_conversation_snapshot::Column::RunId,
                    compaction_live_sources::Column::SourceId,
                )
                .on_condition(|_, _| {
                    sea_orm::Condition::all().add(
                        Expr::col((
                            compaction_live_sources::Column::Table,
                            compaction_live_sources::Column::SourceScope,
                        ))
                        .eq(Expr::val("task-basis:").binary(
                            BinOper::Custom("||"),
                            Expr::col((
                                task_run_conversation_snapshot::Entity,
                                task_run_conversation_snapshot::Column::RunId,
                            )),
                        )),
                    )
                }),
            )
            .expr_as(
                Expr::expr(Func::cust(Alias::new("substr")).args([
                    Expr::col((
                        task_run_conversation_snapshot::Entity,
                        task_run_conversation_snapshot::Column::HistoryJson,
                    )),
                    Expr::Value(offset.into()),
                    Expr::val(16384_i64),
                ])),
                "fragment",
            )
            .expr_as(
                Expr::expr(Func::cust(Alias::new("length")).args([Expr::col((
                    task_run_conversation_snapshot::Entity,
                    task_run_conversation_snapshot::Column::HistoryJson,
                ))])),
                "characters",
            )
            .filter(
                Expr::col((
                    compaction_live_sources::Column::Table,
                    compaction_live_sources::Column::WorkspaceId,
                ))
                .eq(Expr::Value(workspace.into()))
                .and(
                    Expr::col((
                        compaction_live_sources::Column::Table,
                        compaction_live_sources::Column::ThreadId,
                    ))
                    .eq(Expr::Value(thread.into())),
                )
                .and(
                    Expr::col((
                        compaction_live_sources::Column::Table,
                        compaction_live_sources::Column::SourceScope,
                    ))
                    .eq(Expr::Value(reference.scope.clone().into())),
                )
                .and(
                    Expr::col((
                        compaction_live_sources::Column::Table,
                        compaction_live_sources::Column::SourceId,
                    ))
                    .eq(Expr::Value(reference.id.clone().into())),
                )
                .and(
                    Expr::col((
                        compaction_live_sources::Column::Table,
                        compaction_live_sources::Column::SourceVersion,
                    ))
                    .eq(Expr::Value(reference.version.clone().into())),
                ),
            )
            .into_tuple::<(String, i64)>()
            .one(&store.connection)
            .await?;
        let Some((text, characters)) = row else {
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
    if reference.scope.starts_with("checkpoint:") {
        let offset = i64::try_from(character_offset)?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("source offset overflow"))?;
        let row = SourceFragmentRow::find_by_statement(sqlite_specific_sql(
            r#"WITH RECURSIVE graph(source_scope,source_id,source_version) AS (
 SELECT 'checkpoint:'||p.owner,p.id,p.identity_sha256
 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
 WHERE p.id=? AND 'checkpoint:'||p.owner=? AND p.identity_sha256=?
  AND p.format_version=1 AND c.workspace_id=? AND c.thread_id=?
  AND (p.status='applied' OR (p.status='retained' AND EXISTS(
   SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
 UNION
 SELECT v.source_scope,v.source_id,v.source_version
 FROM graph g JOIN compaction_coverage v ON v.checkpoint_id=g.source_id
 WHERE g.source_scope LIKE 'checkpoint:%'
 UNION
 SELECT 'checkpoint:'||COALESCE(previous.owner,''),node.previous,COALESCE(previous.identity_sha256,'')
 FROM graph g JOIN compaction_checkpoint node ON node.id=g.source_id
 LEFT JOIN compaction_checkpoint previous ON previous.id=node.previous
 WHERE g.source_scope LIKE 'checkpoint:%' AND node.previous IS NOT NULL
 LIMIT 65537
)
SELECT 1 AS revision,substr(root.summary,?,16384) AS fragment,length(root.summary) AS characters
FROM compaction_checkpoint root
WHERE root.id=? AND (SELECT COUNT(*) FROM graph)<65537
 AND EXISTS(SELECT 1 FROM graph leaf WHERE leaf.source_scope NOT LIKE 'checkpoint:%')
 AND NOT EXISTS(SELECT 1 FROM graph g WHERE NOT (
  (g.source_scope NOT LIKE 'checkpoint:%' AND EXISTS(
   SELECT 1 FROM compaction_live_sources s
   WHERE s.workspace_id=? AND s.source_scope=g.source_scope
    AND s.source_id=g.source_id AND s.source_version=g.source_version
  )) OR (g.source_scope LIKE 'checkpoint:%' AND EXISTS(
   SELECT 1 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
   WHERE p.id=g.source_id AND 'checkpoint:'||p.owner=g.source_scope
    AND p.identity_sha256=g.source_version AND p.format_version=1 AND c.workspace_id=?
    AND (p.status='applied' OR (p.status='retained' AND EXISTS(
     SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
  ))
 )) LIMIT 1"#,
            [
                reference.id.clone().into(),
                reference.scope.clone().into(),
                reference.version.clone().into(),
                workspace.into(),
                thread.into(),
                offset.into(),
                reference.id.clone().into(),
                workspace.into(),
                workspace.into(),
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

/// Load one exact source as a whole value. The reader is released when this
/// future returns; callers parse/hash only afterward. Compressed event/item
/// rows are deliberately loaded one object at a time so SQLite performs one
/// transparent decompression and memory is bounded by one admitted object.
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

macro_rules! non_checkpoint_graph_source_exists {
    ($workspace:literal) => {
        concat!(
            r#"(
    EXISTS (
      SELECT 1
      FROM compaction_source_revision context_revision
      JOIN turn_llm_context context_source
        ON context_source.id=context_revision.source_id
       AND context_source.turn_id=context_revision.turn_id
      JOIN turn context_turn ON context_turn.id=context_revision.turn_id
      JOIN thread context_thread ON context_thread.id=context_turn.thread_id
      WHERE context_revision.present=1
        AND context_thread.workspace_id="#,
            $workspace,
            r#"
        AND 'context:'||context_revision.turn_id=g.source_scope
        AND context_revision.source_id=g.source_id
        AND 'revision:'||context_revision.revision=g.source_version
    )
    OR EXISTS (
      SELECT 1
      FROM compaction_item_revision item_revision
      JOIN turn_item item_source
        ON item_source.id=item_revision.source_id
       AND item_source.turn_id=item_revision.turn_id
      JOIN turn item_turn ON item_turn.id=item_revision.turn_id
      JOIN thread item_thread ON item_thread.id=item_turn.thread_id
      WHERE item_revision.present=1
        AND item_thread.workspace_id="#,
            $workspace,
            r#"
        AND 'item:'||item_revision.turn_id=g.source_scope
        AND item_revision.source_id=g.source_id
        AND 'item-revision:'||item_revision.revision=g.source_version
    )
    OR EXISTS (
      SELECT 1
      FROM compaction_event_revision event_revision
      JOIN turn_event event_source
        ON event_source.id=event_revision.source_id
       AND event_source.turn_id=event_revision.turn_id
      JOIN turn event_turn ON event_turn.id=event_revision.turn_id
      JOIN thread event_thread ON event_thread.id=event_turn.thread_id
      WHERE event_revision.present=1
        AND event_thread.workspace_id="#,
            $workspace,
            r#"
        AND 'event:'||event_revision.turn_id=g.source_scope
        AND event_revision.source_id=g.source_id
        AND 'event-revision:'||event_revision.revision=g.source_version
    )
    OR EXISTS (
      SELECT 1
      FROM compaction_input_revision input_revision
      JOIN turn_input input_source
        ON input_source.id=input_revision.source_id
       AND input_source.turn_id=input_revision.turn_id
      JOIN turn input_turn ON input_turn.id=input_revision.turn_id
      JOIN thread input_thread ON input_thread.id=input_turn.thread_id
      WHERE input_revision.present=1
        AND input_thread.workspace_id="#,
            $workspace,
            r#"
        AND 'input:'||input_revision.turn_id=g.source_scope
        AND input_revision.source_id=g.source_id
        AND 'input-revision:'||input_revision.revision=g.source_version
    )
    OR EXISTS (
      SELECT 1
      FROM task_run_conversation_snapshot basis
      JOIN thread basis_thread
        ON basis_thread.id=basis.conversation_thread_id
       AND basis_thread.workspace_id=basis.workspace_id
      LEFT JOIN compaction_task_basis_revision basis_revision
        ON basis_revision.run_id=basis.run_id
      WHERE substr(ltrim(basis.history_json),1,1)='['
        AND basis.workspace_id="#,
            $workspace,
            r#"
        AND 'task-basis:'||basis.run_id=g.source_scope
        AND basis.run_id=g.source_id
        AND 'task-basis-revision:'||COALESCE(basis_revision.revision,1)=g.source_version
    )
  )"#,
        )
    };
}

const COMPACTION_REFERENCE_CHECKPOINT_PAYLOAD_SQL: &str = concat!(
    r#"WITH RECURSIVE graph(source_scope,source_id,source_version) AS (
 SELECT 'checkpoint:'||p.owner,p.id,p.identity_sha256
 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
 WHERE p.id=?1 AND 'checkpoint:'||p.owner=?2 AND p.identity_sha256=?3
  AND p.format_version=1 AND c.workspace_id=?4 AND c.thread_id=?5
  AND (p.status='applied' OR (p.status='retained' AND EXISTS(
   SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
 UNION
 SELECT v.source_scope,v.source_id,v.source_version
 FROM graph g JOIN compaction_coverage v ON v.checkpoint_id=g.source_id
 WHERE g.source_scope LIKE 'checkpoint:%'
 UNION
 SELECT 'checkpoint:'||COALESCE(previous.owner,''),node.previous,COALESCE(previous.identity_sha256,'')
 FROM graph g JOIN compaction_checkpoint node ON node.id=g.source_id
 LEFT JOIN compaction_checkpoint previous ON previous.id=node.previous
 WHERE g.source_scope LIKE 'checkpoint:%' AND node.previous IS NOT NULL
 LIMIT 65537
)
SELECT 1 AS revision,root.summary AS fragment,length(root.summary) AS characters
FROM compaction_checkpoint root
WHERE root.id=?6 AND (SELECT COUNT(*) FROM graph)<65537
 AND EXISTS(SELECT 1 FROM graph leaf WHERE leaf.source_scope NOT LIKE 'checkpoint:%')
 AND NOT EXISTS(SELECT 1 FROM graph g WHERE NOT (
  (g.source_scope NOT LIKE 'checkpoint:%' AND "#,
    non_checkpoint_graph_source_exists!("?7"),
    r#") OR (g.source_scope LIKE 'checkpoint:%' AND EXISTS(
   SELECT 1 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
   WHERE p.id=g.source_id AND 'checkpoint:'||p.owner=g.source_scope
    AND p.identity_sha256=g.source_version AND p.format_version=1 AND c.workspace_id=?8
    AND (p.status='applied' OR (p.status='retained' AND EXISTS(
     SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
  ))
 )) LIMIT 1"#,
);

fn compaction_reference_checkpoint_payload_statement(
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Statement {
    sqlite_specific_sql(
        COMPACTION_REFERENCE_CHECKPOINT_PAYLOAD_SQL,
        [
            reference.id.clone().into(),
            reference.scope.clone().into(),
            reference.version.clone().into(),
            workspace.into(),
            thread.into(),
            reference.id.clone().into(),
            workspace.into(),
            workspace.into(),
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
        return Ok(task_run_conversation_snapshot::Entity::find()
            .select_only()
            .join(
                JoinType::InnerJoin,
                compaction_live_sources::join(
                    task_run_conversation_snapshot::Entity,
                    task_run_conversation_snapshot::Column::RunId,
                    compaction_live_sources::Column::SourceId,
                ),
            )
            .column(task_run_conversation_snapshot::Column::HistoryJson)
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
                .eq(&reference.scope),
            )
            .filter(
                Expr::col((
                    compaction_live_sources::Column::Table,
                    compaction_live_sources::Column::SourceId,
                ))
                .eq(&reference.id),
            )
            .filter(
                Expr::col((
                    compaction_live_sources::Column::Table,
                    compaction_live_sources::Column::SourceVersion,
                ))
                .eq(&reference.version),
            )
            .into_tuple::<String>()
            .one(&store.connection)
            .await?);
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

async fn read_source_payload<C: ConnectionTrait, E: EntityTrait, P>(
    db: &C,
    projection: CanonicalProjection<E, P>,
    id: &str,
    expected_revision: i64,
) -> Result<Option<String>> {
    Ok(projection
        .query
        .select_only()
        .column(projection.payload)
        .filter(projection.id.eq(id))
        .filter(projection.revision.eq(expected_revision))
        .filter(projection.present.eq(1_i64))
        .into_tuple::<String>()
        .one(db)
        .await?)
}

#[derive(FromQueryResult)]
struct PayloadSizeRow {
    ordinal: i64,
    bytes: i64,
}

#[derive(FromQueryResult)]
struct PayloadValueRow {
    ordinal: i64,
    payload: String,
}

/// Batch the uncompressed canonical columns by both row count and actual UTF-8
/// bytes. Event and item payloads may be transparent-zstd views and therefore
/// intentionally take the single-object path above: a size probe would itself
/// decompress the complete value and repeat that work.
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
            source.scope.starts_with("input:") || source.scope.starts_with("context:")
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
        let kind = if reference.scope.starts_with("input:") {
            CanonicalSource::Input
        } else {
            CanonicalSource::ProviderContext
        };
        exact_canonical_revision(reference, kind)?;
    }
    let encoded = serde_json::to_string(candidates)?;
    let projection = |field: &str| {
        format!(
            "SELECT CAST(w.key AS INTEGER) AS ordinal,{field} FROM json_each(?) w \
         JOIN compaction_live_sources s ON s.workspace_id=? AND s.thread_id=? \
          AND s.source_scope=json_extract(w.value,'$.scope') \
          AND s.source_id=json_extract(w.value,'$.id') \
          AND s.source_version=json_extract(w.value,'$.version') \
         JOIN turn_input i ON substr(s.source_scope,1,6)='input:' \
          AND i.id=s.source_id AND i.turn_id=substr(s.source_scope,7) \
         UNION ALL \
         SELECT CAST(w.key AS INTEGER) AS ordinal,{context_field} FROM json_each(?) w \
         JOIN compaction_live_sources s ON s.workspace_id=? AND s.thread_id=? \
          AND s.source_scope=json_extract(w.value,'$.scope') \
          AND s.source_id=json_extract(w.value,'$.id') \
          AND s.source_version=json_extract(w.value,'$.version') \
         JOIN turn_llm_context c ON substr(s.source_scope,1,8)='context:' \
          AND c.id=s.source_id AND c.turn_id=substr(s.source_scope,9) ORDER BY ordinal",
            context_field = field.replace("i.payload", "c.payload")
        )
    };
    let values = || {
        [
            encoded.clone().into(),
            workspace.into(),
            thread.into(),
            encoded.clone().into(),
            workspace.into(),
            thread.into(),
        ]
    };
    let sizes = PayloadSizeRow::find_by_statement(sqlite_specific_sql(
        &projection("length(CAST(i.payload AS BLOB)) AS bytes"),
        values(),
    ))
    .all(&store.connection)
    .await?;
    ensure!(
        sizes.len() == candidates.len(),
        "selected history source disappeared"
    );
    let mut consumed = 0usize;
    let mut bytes = 0usize;
    for (ordinal, row) in sizes.iter().enumerate() {
        ensure!(
            row.ordinal == ordinal as i64 && row.bytes >= 0,
            "invalid history payload size"
        );
        let size = usize::try_from(row.bytes)?;
        if consumed > 0 && bytes.saturating_add(size) > SOURCE_PAGE_BYTES {
            break;
        }
        bytes = bytes.saturating_add(size);
        consumed += 1;
    }
    // A single large value is admitted rather than imposing a new history limit.
    let selected = serde_json::to_string(&candidates[..consumed])?;
    let selected_values = || {
        [
            selected.clone().into(),
            workspace.into(),
            thread.into(),
            selected.clone().into(),
            workspace.into(),
            thread.into(),
        ]
    };
    let rows = PayloadValueRow::find_by_statement(sqlite_specific_sql(
        &projection("i.payload AS payload"),
        selected_values(),
    ))
    .all(&store.connection)
    .await?;
    ensure!(
        rows.len() == consumed,
        "selected history source changed while loading"
    );
    let mut payloads = Vec::with_capacity(consumed);
    for (ordinal, row) in rows.into_iter().enumerate() {
        ensure!(
            row.ordinal == ordinal as i64,
            "history payload order changed"
        );
        payloads.push(row.payload);
    }
    Ok((consumed, payloads))
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
const COMPACTION_CHECKPOINT_SOURCE_SQL: &str = concat!(
    r#"WITH RECURSIVE graph(source_scope,source_id,source_version) AS (
 SELECT 'checkpoint:'||p.owner,p.id,p.identity_sha256
 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
 WHERE p.id=?1 AND c.workspace_id=?2 AND c.thread_id=?3
  AND p.format_version=1
  AND (p.status='applied' OR (p.status='retained' AND EXISTS(
   SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
 UNION
 SELECT v.source_scope,v.source_id,v.source_version
 FROM graph g JOIN compaction_coverage v ON v.checkpoint_id=g.source_id
 WHERE g.source_scope LIKE 'checkpoint:%'
 UNION
 SELECT 'checkpoint:'||COALESCE(previous.owner,''),node.previous,COALESCE(previous.identity_sha256,'')
 FROM graph g JOIN compaction_checkpoint node ON node.id=g.source_id
 LEFT JOIN compaction_checkpoint previous ON previous.id=node.previous
 WHERE g.source_scope LIKE 'checkpoint:%' AND node.previous IS NOT NULL
 LIMIT 65537
)
SELECT root.source_scope,root.source_id,root.source_version
FROM graph root
WHERE root.source_scope LIKE 'checkpoint:%' AND root.source_id=?4
 AND (SELECT COUNT(*) FROM graph)<65537
 AND EXISTS(SELECT 1 FROM graph leaf WHERE leaf.source_scope NOT LIKE 'checkpoint:%')
 AND NOT EXISTS(SELECT 1 FROM graph g WHERE NOT (
  (g.source_scope NOT LIKE 'checkpoint:%' AND "#,
    non_checkpoint_graph_source_exists!("?5"),
    r#") OR (g.source_scope LIKE 'checkpoint:%' AND EXISTS(
   SELECT 1 FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner
   WHERE p.id=g.source_id AND 'checkpoint:'||p.owner=g.source_scope
    AND p.identity_sha256=g.source_version AND p.format_version=1 AND c.workspace_id=?6
    AND (p.status='applied' OR (p.status='retained' AND EXISTS(
     SELECT 1 FROM compaction_operation o WHERE o.id=p.operation_id AND o.status='completed')))
  ))
 )) LIMIT 1"#,
);

fn compaction_checkpoint_source_statement(
    workspace: &str,
    thread: &str,
    checkpoint: &str,
) -> Statement {
    sqlite_specific_sql(
        COMPACTION_CHECKPOINT_SOURCE_SQL,
        [
            checkpoint.into(),
            workspace.into(),
            thread.into(),
            checkpoint.into(),
            workspace.into(),
            workspace.into(),
        ],
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
        ["assistant_round", "tool_result_v2"].contains(&source),
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
pub(crate) async fn compaction_tool_item_reference<C: ConnectionTrait>(
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
                )
                .and(
                    Expr::expr(Func::cust(Alias::new("json_extract")).args([
                        Expr::col((turn_item::Entity, turn_item::Column::Payload)),
                        Expr::val("$.storage.kind"),
                    ]))
                    .eq(Expr::val("shell")),
                ),
        )
        .limit(1)
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
            Expr::expr(Func::cust(Alias::new("json_extract")).args([
                Expr::col((turn_item::Entity, turn_item::Column::Payload)),
                Expr::val("$.storage.kind"),
            ]))
            .eq("shell"),
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

async fn read_source_fragment<C: ConnectionTrait, E: EntityTrait, P>(
    db: &C,
    projection: CanonicalProjection<E, P>,
    id: &str,
    offset: i64,
) -> Result<Option<SourceFragmentRow>> {
    Ok(projection
        .query
        .select_only()
        .expr_as(projection.revision, "revision")
        .expr_as(
            Func::cust(Alias::new("substr")).args([
                Expr::col((E::default(), projection.payload)),
                Expr::val(offset),
                Expr::val(16384_i64),
            ]),
            "fragment",
        )
        .expr_as(
            Func::char_length(Expr::col((E::default(), projection.payload))),
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

/// SeaORM has no INSERT ... SELECT operation. This single conditional write
/// seeds one old canonical row without adding a read/write race or a new
/// transaction. Both sides use Entity columns; canonical content is not read.
pub(super) async fn seed_canonical_revision<C: ConnectionTrait, E: EntityTrait, R: EntityTrait>(
    db: &C,
    workspace: &str,
    source_thread: &str,
    source_turn: &str,
    source_id: &str,
    source_columns: (E::Column, E::Column),
    revision_columns: (R::Column, R::Column, R::Column, R::Column),
) -> Result<()> {
    use sea_orm::QueryTrait;
    let (id, turn_id) = source_columns;
    let (revision_id, revision_turn, revision, present) = revision_columns;
    let source = E::find()
        .select_only()
        .column(id)
        .column(turn_id)
        .expr(Expr::val(1_i64))
        .expr(Expr::val(1_i64))
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
        .filter(id.eq(source_id))
        .filter(turn_id.eq(source_turn))
        .filter(thread::Column::Id.eq(source_thread))
        .filter(thread::Column::WorkspaceId.eq(workspace));
    db.execute(
        &Query::insert()
            .into_table(R::default())
            .columns([revision_id, revision_turn, revision, present])
            .select_from(source.into_query())?
            .on_conflict(OnConflict::columns([revision_id]).do_nothing().to_owned())
            .to_owned(),
    )
    .await?;
    Ok(())
}

#[derive(FromQueryResult)]
struct MatchedSourceCount {
    matched: i64,
}

pub use super::compaction_check_result::{HistoryCheckDiagnostic, HistoryCheckOutcome};

#[cfg(test)]
#[path = "compaction_source_lookup_tests.rs"]
mod source_lookup_tests;
