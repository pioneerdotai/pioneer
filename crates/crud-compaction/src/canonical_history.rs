//! Scoped canonical history discovery and bounded payload reads.

use anyhow::{Context, Result, ensure};
use pioneer_compaction::SourceRef;
use pioneer_entity::{
    compaction_event_revision, compaction_input_revision, compaction_item_revision,
    compaction_projection_epoch, compaction_source_revision, thread, turn, turn_event, turn_input,
    turn_item, turn_llm_context,
};
use pioneer_observability::portable_payload as observe;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, JoinType, Order, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect,
};

use crate::live_sources as compaction_live_sources;
use crate::runner_writes::CanonicalSource;
use crate::tool_sources::{compaction_tool_item_reference, turn_item_has_shell_storage};
use crate::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS, history};

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

#[derive(Clone, Debug)]
pub struct SourcePage {
    pub entries: Vec<SourceRecord>,
    pub next_sequence: i64,
}

/// Pin append-only discovery to a bounded high water mark. Edits remain
/// governed by each source revision, not by this sequence boundary.
pub async fn compaction_source_high_water<C: ConnectionTrait>(
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
pub async fn compaction_source_page(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
) -> Result<SourcePage> {
    compaction_source_page_inner(db, workspace, thread, turn, kind, after, true, i64::MAX).await
}

/// Discover versioned identities without materializing already covered
/// payloads. The projection fetches only its uncovered sources afterward.
pub async fn compaction_source_metadata_page(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
) -> Result<SourcePage> {
    compaction_source_page_inner(db, workspace, thread, turn, kind, after, false, i64::MAX).await
}

pub async fn compaction_source_metadata_page_at_fence(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    kind: PagedSource,
    after: i64,
    capture_order: i64,
) -> Result<SourcePage> {
    compaction_source_page_inner(
        db,
        workspace,
        thread,
        turn,
        kind,
        after,
        false,
        capture_order,
    )
    .await
}

pub async fn compaction_item_created_at(
    db: &SqliteDatabase,
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
        .one(db)
        .await?)
}

#[allow(clippy::too_many_arguments)]
async fn compaction_source_page_inner(
    db: &SqliteDatabase,
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

pub async fn compaction_source_page_inner_bounded(
    db: &SqliteDatabase,
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
    db: &SqliteDatabase,
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

#[derive(Clone, Debug)]
pub struct CanonicalFragment {
    pub reference: SourceRef,
    pub text: String,
    pub next_character: Option<u64>,
}

/// Bounded Unicode-safe reads of very large canonical payloads. Revision is
/// maintained atomically by source-table triggers, including deletes/reinserts.
/// The same revision must be supplied for every subsequent fragment.
pub async fn compaction_source_fragment(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    id: &str,
    expected_revision: Option<&str>,
    character_offset: u64,
) -> Result<Option<CanonicalFragment>> {
    compaction_payload_fragment(
        db,
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

async fn compaction_payload_fragment(
    db: &SqliteDatabase,
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
pub async fn compaction_tool_result_id<C: ConnectionTrait>(
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
pub async fn compaction_tool_result_fragment(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
    item: &str,
    expected_revision: Option<&str>,
    character_offset: u64,
) -> Result<Option<CanonicalFragment>> {
    if let Some(source) = compaction_tool_item_reference(db, workspace, thread, turn, item).await? {
        return compaction_payload_fragment(
            db,
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
    let Some(id) = compaction_tool_result_id(db, workspace, thread, turn, item).await? else {
        return Ok(None);
    };
    compaction_source_fragment(
        db,
        workspace,
        thread,
        turn,
        &id,
        expected_revision,
        character_offset,
    )
    .await
}

pub async fn compaction_reference_fragment(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
    character_offset: u64,
) -> Result<Option<CanonicalFragment>> {
    if reference.scope.starts_with("task-basis:") {
        return observe::scope_result(observe::Family::TaskHistory, observe::Mode::Range, async {
            let reader = db.begin_read().await?;
            let captured = crate::task_basis::capture(
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
        use pioneer_entity::{compaction_checkpoint as checkpoint, compaction_context as context};
        let offset = i64::try_from(character_offset)?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("source offset overflow"))?;
        let summary = Expr::col((checkpoint::Entity, checkpoint::Column::Summary));
        let row =
            crate::source_revision::published_checkpoint_in_workspace(workspace, &reference.id)
                .filter(context::Column::ThreadId.eq(thread))
                .filter(crate::source_revision::checkpoint_scope_filter(
                    &reference.scope,
                ))
                .filter(checkpoint::Column::IdentitySha256.eq(&reference.version))
                .expr(Func::cust(Alias::new("substr")).args([
                    summary.clone(),
                    Expr::val(offset).cast_as("int"),
                    Expr::val(16_384_i32),
                ]))
                .expr(Expr::from(Func::char_length(summary)).cast_as("bigint"))
                .into_tuple::<(String, i64)>()
                .one(db)
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
    compaction_payload_fragment(
        db,
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

pub async fn compaction_reference_payload(
    db: &SqliteDatabase,
    workspace: &str,
    thread: &str,
    reference: &SourceRef,
) -> Result<Option<String>> {
    if reference.scope.starts_with("task-basis:") {
        let reader = db.begin_read().await?;
        let captured =
            crate::task_basis::capture(&reader, workspace, thread, reference, None).await?;
        reader.commit().await?;
        return captured
            .map(|captured| captured.body.into_text())
            .transpose();
    }
    if reference.scope.starts_with("checkpoint:") {
        return crate::source_revision::compaction_reference_checkpoint_payload(
            db, workspace, thread, reference,
        )
        .await;
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
                db,
                turn_input_projection(workspace, thread, turn),
                &reference.id,
                expected,
            )
            .await?
        }
        CanonicalSource::Event => {
            read_source_payload(
                db,
                turn_event_projection(workspace, thread, turn),
                &reference.id,
                expected,
            )
            .await?
        }
        CanonicalSource::ProviderContext => {
            read_source_payload(
                db,
                turn_llm_context_projection(workspace, thread, turn),
                &reference.id,
                expected,
            )
            .await?
        }
        CanonicalSource::ToolItem => {
            read_source_payload(
                db,
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
    db: &SqliteDatabase,
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
pub async fn compaction_reference_payload_batch(
    db: &SqliteDatabase,
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
        let payload = compaction_reference_payload(db, workspace, thread, &references[0])
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
    crate::source_payloads::read_page(db, workspace, thread, candidates, SOURCE_PAGE_BYTES).await
}

pub async fn compaction_projection_version<C: ConnectionTrait>(
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
/// Resolve a runtime locator only after its durable append was acknowledged.
/// Reads metadata, never a full result, and cannot cross the execution scope.
pub async fn compaction_context_reference_for_item<C: ConnectionTrait>(
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

/// Resolve a saved shell result or frozen provider representation to its tool
/// item. Read identity metadata only and require the exact live revision.
pub async fn compaction_replay_item_id<C: ConnectionTrait>(
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

pub async fn compaction_item_reference<C: ConnectionTrait>(
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

#[derive(FromQueryResult)]
struct SourceFragmentRow {
    revision: i64,
    fragment: String,
    characters: i64,
}

async fn read_source_fragment<E: EntityTrait, P>(
    db: &SqliteDatabase,
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

#[cfg(test)]
#[path = "canonical_history_tests.rs"]
mod tests;
