use anyhow::{Context, Result};
use pioneer_entity::{self_improvement_run, self_improvement_source_turn, thread, turn};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, Func, Query};
use sea_orm::{
    ColumnTrait, Condition as Cond, ConnectionTrait, EntityTrait, JoinType, QueryFilter,
    QuerySelect, Select,
};

pub const SOURCE_ORIGINS: [&str; 3] = ["collaborative", "direct_message", "user"];
pub const SOURCE_ACCESS_CLASSES: [&str; 2] = ["private", "workspace"];

/// Chronological reference for backfill. Source IDs represent discovery order, not
/// message age. Use the original user turn date of successfully analyzed tasks.
/// A single aggregate row is materialized; no history payload is read here.
pub async fn processed_history_date<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Option<i64>> {
    let source_date = Func::max(Expr::col((turn::Entity, turn::Column::CreatedAt)));
    self_improvement_source_turn::Entity::find()
        .select_only()
        .column_as(Expr::from(source_date), "source_date")
        .join(
            JoinType::InnerJoin,
            self_improvement_source_turn::Entity::belongs_to(turn::Entity)
                .from(self_improvement_source_turn::Column::TurnId)
                .to(turn::Column::Id)
                .into(),
        )
        .filter(self_improvement_source_turn::Column::WorkspaceId.eq(workspace_id))
        .filter(processed_source_predicate())
        .into_tuple::<Option<DateTimeWithTimeZone>>()
        .one(db)
        .await?
        .context("missing processed history date aggregate")
        .map(|date| date.map(|date| date.timestamp()))
}

// Completed ranges are the durable consumption ledger, including previous activation epochs.
// Backfill can rewind over holes without turning already-consumed sources into new anchors.
pub fn processed_source_predicate() -> Expr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1))
            .from(self_improvement_run::Entity)
            .and_where(
                Expr::col((
                    self_improvement_run::Entity,
                    self_improvement_run::Column::WorkspaceId,
                ))
                .equals((
                    self_improvement_source_turn::Entity,
                    self_improvement_source_turn::Column::WorkspaceId,
                )),
            )
            .and_where(
                self_improvement_run::Column::Status
                    .eq(crate::self_improvement_run::STATUS_COMPLETED),
            )
            .and_where(
                Expr::col((
                    self_improvement_run::Entity,
                    self_improvement_run::Column::SourceLowerExclusive,
                ))
                .lt(Expr::col((
                    self_improvement_source_turn::Entity,
                    self_improvement_source_turn::Column::Id,
                ))),
            )
            .and_where(
                Expr::col((
                    self_improvement_run::Entity,
                    self_improvement_run::Column::SourceUpperInclusive,
                ))
                .gte(Expr::col((
                    self_improvement_source_turn::Entity,
                    self_improvement_source_turn::Column::Id,
                ))),
            )
            .to_owned(),
    )
}

#[derive(Clone)]
pub struct PreparedSourceTurnCheck {
    pub source_lower_exclusive: i64,
    pub source_upper_inclusive: i64,
    expected_count: i64,
    query: Select<self_improvement_source_turn::Entity>,
}

impl PreparedSourceTurnCheck {
    pub async fn matches<C: ConnectionTrait>(self, db: &C) -> Result<bool> {
        let (matched_count, new_anchor_count) = self
            .query
            .into_tuple::<(i64, i64)>()
            .one(db)
            .await
            .context("failed to verify cited Agent skill source provenance")?
            .context("Agent skill source provenance query returned no aggregate row")?;
        Ok(matched_count == self.expected_count && new_anchor_count > 0)
    }
}

pub fn prepare_source_turn_check(
    workspace_id: &str,
    source_turn_ids: &[String],
    frozen_bounds: Option<(i64, i64)>,
) -> Result<Option<PreparedSourceTurnCheck>> {
    let Some((source_lower_exclusive, source_upper_inclusive)) = frozen_bounds else {
        return Ok(None);
    };
    let expected_count = i64::try_from(source_turn_ids.len())
        .context("accepted Agent skill source turn count exceeds durable integer range")?;
    let new_anchor = Cond::all()
        .add(self_improvement_source_turn::Column::Id.gt(source_lower_exclusive))
        .add(self_improvement_source_turn::Column::Id.lte(source_upper_inclusive))
        .add(processed_source_predicate().not());
    let mut query = self_improvement_source_turn::Entity::find()
        .select_only()
        .column_as(
            Expr::from(Func::count(Expr::col((
                self_improvement_source_turn::Entity,
                self_improvement_source_turn::Column::Id,
            )))),
            "matched_count",
        )
        .column_as(
            Expr::from(Func::count(
                Expr::case(new_anchor, Expr::cust("1")).finally(Expr::cust("NULL")),
            )),
            "new_anchor_count",
        );
    QuerySelect::query(&mut query)
        .join(
            JoinType::InnerJoin,
            turn::Entity,
            Cond::all()
                .add(Expr::col((turn::Entity, turn::Column::Id)).equals((
                    self_improvement_source_turn::Entity,
                    self_improvement_source_turn::Column::TurnId,
                )))
                .add(Expr::col((turn::Entity, turn::Column::ThreadId)).equals((
                    self_improvement_source_turn::Entity,
                    self_improvement_source_turn::Column::ThreadId,
                ))),
        )
        .join(
            JoinType::InnerJoin,
            thread::Entity,
            Cond::all()
                .add(
                    Expr::col((thread::Entity, thread::Column::Id))
                        .equals((turn::Entity, turn::Column::ThreadId)),
                )
                .add(
                    Expr::col((thread::Entity, thread::Column::WorkspaceId)).equals((
                        self_improvement_source_turn::Entity,
                        self_improvement_source_turn::Column::WorkspaceId,
                    )),
                ),
        )
        .and_where(self_improvement_source_turn::Column::WorkspaceId.eq(workspace_id))
        .and_where(
            self_improvement_source_turn::Column::TurnId.is_in(source_turn_ids.iter().cloned()),
        )
        .and_where(turn::Column::Status.eq("completed"))
        .and_where(turn::Column::TurnKind.eq("conversation"))
        .and_where(turn::Column::Origin.eq("user"))
        .and_where(thread::Column::AccessClass.is_in(SOURCE_ACCESS_CLASSES))
        .and_where(thread::Column::SidebarVisibility.eq("visible"))
        .and_where(thread::Column::OriginKind.is_in(SOURCE_ORIGINS));
    Ok(Some(PreparedSourceTurnCheck {
        source_lower_exclusive,
        source_upper_inclusive,
        expected_count,
        query,
    }))
}
