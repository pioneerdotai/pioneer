//! Bounded projections of turn metadata at a captured history fence.
use anyhow::Result;
use pioneer_entity::{
    compaction_event_revision, compaction_input_revision, compaction_source_revision,
    compaction_turn_creation, thread, turn, turn_event, turn_input, turn_llm_context,
};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, JoinType, Order, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect,
};

use crate::SOURCE_PAGE_BYTES;
use crate::history_fence::HistoryReadFence;

#[derive(Clone, Debug, FromQueryResult)]
pub struct HistoryTurnBoundary {
    pub creation_order: i64,
    /// Tie-breaker for turns predating the creation-order trigger. This is
    /// ordering metadata only, never proof of source coverage or a read fence.
    pub legacy_creation_order: i64,
    pub id: String,
    pub created_at: sea_orm::prelude::DateTimeWithTimeZone,
    pub status: String,
    pub turn_kind: String,
    pub send_mode: Option<String>,
    pub input_high_water: i64,
    pub event_high_water: i64,
    pub context_high_water: i64,
}

pub fn history_before(created_at: Expr, before: sea_orm::prelude::DateTimeWithTimeZone) -> Expr {
    debug_assert_eq!(before.timestamp_subsec_nanos(), 0);
    created_at.lt(before)
}

/// Editing replaces a turn's current input rows. Their new insertion date is
/// not a new parent contribution; the original turn still fixes membership.
/// Ordinary event/provider appends continue to use their own recorded dates.
pub fn history_input_before(
    created_at: Expr,
    before: sea_orm::prelude::DateTimeWithTimeZone,
) -> Expr {
    history_before(created_at, before).or(Expr::col((turn::Entity, turn::Column::MessageRevision))
        .gt(0_i64)
        .and(history_before(
            Expr::col((turn::Entity, turn::Column::CreatedAt)),
            before,
        )))
}

/// Only structural edit/delete events update an already admitted message;
/// their later dates must not resurrect an earlier input or hide its revision.
pub fn history_event_before(
    created_at: Expr,
    event_type: Expr,
    before: sea_orm::prelude::DateTimeWithTimeZone,
) -> Expr {
    history_before(created_at, before).or(event_type
        .is_in([
            pioneer_protocol::constants::events::TURN_MESSAGE_EDITED,
            pioneer_protocol::constants::events::TURN_MESSAGE_DELETED,
        ])
        .and(history_before(
            Expr::col((turn::Entity, turn::Column::CreatedAt)),
            before,
        )))
}

/// Discover at most 128 metadata rows, including active turns. Eligibility
/// is determined from complete canonical rounds/events below the captured
/// fence, never by a later mutable terminal status alone.
pub async fn compaction_history_turn_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    after: &str,
    fence: &HistoryReadFence,
) -> Result<Vec<HistoryTurnBoundary>> {
    compaction_history_turn_page_inner(db, workspace, thread, after, fence, None).await
}

/// The accepted manifest already names exact turns. Restrict the metadata
/// query before its per-turn input/event/context boundary subqueries run.
pub async fn compaction_history_selected_turn_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    selected: &[String],
    fence: &HistoryReadFence,
) -> Result<Vec<HistoryTurnBoundary>> {
    anyhow::ensure!(
        selected.len() <= 64,
        "selected history turn page exceeds row bound"
    );
    anyhow::ensure!(
        selected.iter().map(String::len).sum::<usize>() <= SOURCE_PAGE_BYTES,
        "selected history turn page exceeds byte bound"
    );
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    compaction_history_turn_page_inner(db, workspace, thread, "", fence, Some(selected)).await
}

async fn compaction_history_turn_page_inner<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    after: &str,
    fence: &HistoryReadFence,
    selected: Option<&[String]>,
) -> Result<Vec<HistoryTurnBoundary>> {
    // Canonical writes and their revision journals commit together. A foreign
    // or unavailable thread has no readable history in this scope.
    if thread::Entity::find_by_id(thread)
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .select_only()
        .column(thread::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_none()
    {
        return Ok(Vec::new());
    }
    let mut query = turn::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .expr(Expr::col((turn::Entity, turn::Column::Id)))
        .expr_as(
            Expr::expr(
                Func::cust(Alias::new("coalesce")).args([
                    Expr::SubQuery(
                        None,
                        Box::new(
                            Query::select()
                                .expr(Expr::col(compaction_turn_creation::Column::Sequence))
                                .from(compaction_turn_creation::Entity)
                                .and_where(
                                    Expr::col(compaction_turn_creation::Column::TurnId)
                                        .eq(Expr::col((turn::Entity, turn::Column::Id))),
                                )
                                .to_owned()
                                .into(),
                        ),
                    ),
                    Expr::val(0_i64),
                ]),
            ),
            "creation_order",
        )
        .expr_as(
            Expr::col((turn::Entity, turn::Column::InsertionOrdinal)),
            "legacy_creation_order",
        )
        .expr(Expr::col((turn::Entity, turn::Column::CreatedAt)))
        .expr(Expr::col((turn::Entity, turn::Column::Status)))
        .expr(Expr::col((turn::Entity, turn::Column::TurnKind)))
        .expr(Expr::col((turn::Entity, turn::Column::SendMode)))
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(
                            Func::cust(Alias::new("coalesce")).args([
                                Expr::expr(
                                    Func::cust(Alias::new("max")).args([Expr::col((
                                        "s",
                                        turn_input::Column::InputIndex,
                                    ))
                                    .add(Expr::val(1_i64))]),
                                ),
                                Expr::val(0_i64),
                            ]),
                        ))
                        .from_as(turn_input::Entity, "s")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_input_revision::Entity,
                            "r",
                            Expr::col(("r", compaction_input_revision::Column::SourceId))
                                .eq(Expr::col(("s", turn_input::Column::Id)))
                                .and(
                                    Expr::col(("r", compaction_input_revision::Column::TurnId))
                                        .eq(Expr::col(("s", turn_input::Column::TurnId))),
                                )
                                .and(
                                    Expr::col(("r", compaction_input_revision::Column::Present))
                                        .eq(Expr::val(1_i64)),
                                ),
                        )
                        .and_where(
                            Expr::col(("s", turn_input::Column::TurnId))
                                .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                .and(
                                    Expr::col((
                                        "r",
                                        compaction_input_revision::Column::CaptureOrder,
                                    ))
                                    .lte(Expr::Value(fence.input_order.into())),
                                )
                                .and(match fence.before {
                                    Some(before) => history_input_before(
                                        Expr::col(("s", turn_input::Column::CreatedAt)),
                                        before,
                                    ),
                                    None => Expr::val(true).into(),
                                }),
                        )
                        .to_owned()
                        .into(),
                ),
            ),
            "input_high_water",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(
                            Func::cust(Alias::new("coalesce")).args([
                                Expr::expr(
                                    Func::cust(Alias::new("max"))
                                        .args([Expr::col(("s", turn_event::Column::Sequence))]),
                                ),
                                Expr::val(0_i64),
                            ]),
                        ))
                        .from_as(turn_event::Entity, "s")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_event_revision::Entity,
                            "r",
                            Expr::col(("r", compaction_event_revision::Column::SourceId))
                                .eq(Expr::col(("s", turn_event::Column::Id)))
                                .and(
                                    Expr::col(("r", compaction_event_revision::Column::TurnId))
                                        .eq(Expr::col(("s", turn_event::Column::TurnId))),
                                )
                                .and(
                                    Expr::col(("r", compaction_event_revision::Column::Present))
                                        .eq(Expr::val(1_i64)),
                                ),
                        )
                        .and_where(
                            Expr::col(("s", turn_event::Column::TurnId))
                                .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                .and(
                                    Expr::col((
                                        "r",
                                        compaction_event_revision::Column::CaptureOrder,
                                    ))
                                    .lte(Expr::Value(fence.event_order.into())),
                                )
                                .and(match fence.before {
                                    Some(before) => history_event_before(
                                        Expr::col(("s", turn_event::Column::CreatedAt)),
                                        Expr::col(("s", turn_event::Column::EventType)),
                                        before,
                                    ),
                                    None => Expr::val(true).into(),
                                }),
                        )
                        .to_owned()
                        .into(),
                ),
            ),
            "event_high_water",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args(
                            [
                                Expr::expr(
                                    Func::cust(Alias::new("max")).args([Expr::col((
                                        "s",
                                        turn_llm_context::Column::Sequence,
                                    ))]),
                                ),
                                Expr::val(0_i64),
                            ],
                        )))
                        .from_as(turn_llm_context::Entity, "s")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_source_revision::Entity,
                            "r",
                            Expr::col(("r", compaction_source_revision::Column::SourceId))
                                .eq(Expr::col(("s", turn_llm_context::Column::Id)))
                                .and(
                                    Expr::col(("r", compaction_source_revision::Column::TurnId))
                                        .eq(Expr::col(("s", turn_llm_context::Column::TurnId))),
                                )
                                .and(
                                    Expr::col(("r", compaction_source_revision::Column::Present))
                                        .eq(Expr::val(1_i64)),
                                ),
                        )
                        .and_where(
                            Expr::col(("s", turn_llm_context::Column::TurnId))
                                .eq(Expr::col((turn::Entity, turn::Column::Id)))
                                .and(
                                    Expr::col((
                                        "r",
                                        compaction_source_revision::Column::CaptureOrder,
                                    ))
                                    .lte(Expr::Value(fence.context_order.into())),
                                )
                                .and(match fence.before {
                                    Some(before) => history_before(
                                        Expr::col(("s", turn_llm_context::Column::CreatedAt)),
                                        before,
                                    ),
                                    None => Expr::val(true).into(),
                                }),
                        )
                        .to_owned()
                        .into(),
                ),
            ),
            "context_high_water",
        )
        .filter(
            Expr::col((thread::Entity, thread::Column::WorkspaceId))
                .eq(Expr::Value(workspace.into()))
                .and(Expr::col((thread::Entity, thread::Column::Id)).eq(Expr::Value(thread.into())))
                .and(Expr::col((turn::Entity, turn::Column::Id)).gt(Expr::Value(after.into())))
                .and(
                    Expr::col((turn::Entity, turn::Column::Id))
                        .lte(Expr::Value(fence.turn_id.clone().into())),
                ),
        );
    query = query.filter(
        Expr::exists(
            Query::select()
                .expr(Expr::val(1_i64))
                .from(compaction_turn_creation::Entity)
                .and_where(
                    Expr::col(compaction_turn_creation::Column::TurnId)
                        .eq(Expr::col((turn::Entity, turn::Column::Id))),
                )
                .and_where(
                    Expr::col(compaction_turn_creation::Column::Sequence).gt(fence.turn_order),
                )
                .to_owned(),
        )
        .not(),
    );
    if let Some(before) = fence.before {
        query = query.filter(history_before(
            Expr::col((turn::Entity, turn::Column::CreatedAt)),
            before,
        ));
    }
    if let Some(selected) = selected {
        query = query.filter(turn::Column::Id.is_in(selected.iter().cloned()));
    }
    Ok(query
        .order_by(Expr::col((turn::Entity, turn::Column::Id)), Order::Asc)
        .limit(128)
        .into_model::<HistoryTurnBoundary>()
        .all(db)
        .await?)
}
