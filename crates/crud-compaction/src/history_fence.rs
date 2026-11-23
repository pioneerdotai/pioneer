use anyhow::Result;
use pioneer_entity::{
    compaction_event_revision, compaction_input_revision, compaction_source_revision,
    compaction_turn_creation, turn,
};
use sea_orm::sea_query::{Alias, Expr, Func, Query};
use sea_orm::{ConnectionTrait, EntityTrait, FromQueryResult, QuerySelect};

#[derive(Clone, Debug, FromQueryResult)]
pub struct HistoryReadFence {
    pub turn_order: i64,
    pub turn_id: String,
    pub input_order: i64,
    pub event_order: i64,
    pub context_order: i64,
    /// Request-local historical cutoff, independent of today's append fence.
    #[sea_orm(skip)]
    pub before: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    #[sea_orm(skip)]
    pub source_turn: Option<String>,
}

/// One read fixes the append boundary before enumerating any turns. Source
/// bounds use retained metadata with explicit insertion order; deleting or
/// vacuuming canonical rows cannot change that order. MAX uses indexes and
/// reads no payload. Every subsequent discovery checks its workspace scope.
pub async fn compaction_history_read_fence<C: ConnectionTrait>(db: &C) -> Result<HistoryReadFence> {
    // The aggregate base yields one row even for an empty database; all six
    // boundaries are still captured by one PostgreSQL statement snapshot.
    compaction_turn_creation::Entity::find()
        .select_only()
        .expr_as(
            Func::coalesce([
                Func::max(Expr::col(compaction_turn_creation::Column::Sequence)).into(),
                Expr::val(0_i64),
            ]),
            "turn_order",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(
                                Func::cust(Alias::new("max")).args([Expr::col(turn::Column::Id)]),
                            ),
                            Expr::val(""),
                        ])))
                        .from(turn::Entity)
                        .to_owned()
                        .into(),
                ),
            ),
            "turn_id",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(Func::cust(Alias::new("max")).args([Expr::col(
                                compaction_input_revision::Column::CaptureOrder,
                            )])),
                            Expr::val(0_i64),
                        ])))
                        .from(compaction_input_revision::Entity)
                        .to_owned()
                        .into(),
                ),
            ),
            "input_order",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(Func::cust(Alias::new("max")).args([Expr::col(
                                compaction_event_revision::Column::CaptureOrder,
                            )])),
                            Expr::val(0_i64),
                        ])))
                        .from(compaction_event_revision::Entity)
                        .to_owned()
                        .into(),
                ),
            ),
            "event_order",
        )
        .expr_as(
            Expr::SubQuery(
                None,
                Box::new(
                    Query::select()
                        .expr(Expr::expr(Func::cust(Alias::new("coalesce")).args([
                            Expr::expr(Func::cust(Alias::new("max")).args([Expr::col(
                                compaction_source_revision::Column::CaptureOrder,
                            )])),
                            Expr::val(0_i64),
                        ])))
                        .from(compaction_source_revision::Entity)
                        .to_owned()
                        .into(),
                ),
            ),
            "context_order",
        )
        .into_model::<HistoryReadFence>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("history read fence missing"))
}
