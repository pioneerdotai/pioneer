use anyhow::{Context, Result};
use pioneer_entity::self_improvement_workspace_state;
use sea_orm::ExprTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};

use crate::SelfImprovementWorkspaceStateRecord;

pub async fn ensure<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<self_improvement_workspace_state::Model> {
    self_improvement_workspace_state::Entity::insert(
        self_improvement_workspace_state::ActiveModel {
            workspace_id: Set(workspace_id.to_owned()),
            activation_epoch: Set(0),
            cursor_source_id: Set(0),
            effective_enabled_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        },
    )
    .on_conflict(
        OnConflict::column(self_improvement_workspace_state::Column::WorkspaceId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .with_context(|| {
        format!("failed to initialize self-improvement state for workspace `{workspace_id}`")
    })?;

    find(db, workspace_id).await?.with_context(|| {
        format!(
            "self-improvement state is missing after initialization for workspace `{workspace_id}`"
        )
    })
}

pub async fn find<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Option<self_improvement_workspace_state::Model>> {
    self_improvement_workspace_state::Entity::find_by_id(workspace_id.to_owned())
        .one(db)
        .await
        .with_context(|| {
            format!("failed to load self-improvement state for workspace `{workspace_id}`")
        })
}

pub async fn activate_if_inactive<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    baseline_source_id: i64,
    effective_enabled_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = self_improvement_workspace_state::Entity::update_many()
        .col_expr(
            self_improvement_workspace_state::Column::ActivationEpoch,
            Expr::col(self_improvement_workspace_state::Column::ActivationEpoch).add(1),
        )
        .col_expr(
            self_improvement_workspace_state::Column::CursorSourceId,
            Expr::value(baseline_source_id),
        )
        .col_expr(
            self_improvement_workspace_state::Column::EffectiveEnabledAt,
            Expr::value(Some(effective_enabled_at)),
        )
        .col_expr(
            self_improvement_workspace_state::Column::UpdatedAt,
            Expr::value(effective_enabled_at),
        )
        .filter(self_improvement_workspace_state::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(self_improvement_workspace_state::Column::EffectiveEnabledAt.is_null())
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to activate self-improvement for workspace `{workspace_id}`")
        })?
        .rows_affected
        == 1;

    Ok(affected)
}

pub async fn deactivate_if_active<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = self_improvement_workspace_state::Entity::update_many()
        .col_expr(
            self_improvement_workspace_state::Column::EffectiveEnabledAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            self_improvement_workspace_state::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(self_improvement_workspace_state::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(self_improvement_workspace_state::Column::EffectiveEnabledAt.is_not_null())
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to deactivate self-improvement for workspace `{workspace_id}`")
        })?
        .rows_affected
        == 1;

    Ok(affected)
}

pub fn record_from_model(
    model: self_improvement_workspace_state::Model,
) -> SelfImprovementWorkspaceStateRecord {
    SelfImprovementWorkspaceStateRecord {
        workspace_id: model.workspace_id,
        activation_epoch: model.activation_epoch,
        cursor_source_id: model.cursor_source_id,
        effective_enabled_at_unix: model.effective_enabled_at.map(|value| value.timestamp()),
        created_at_unix: model.created_at.timestamp(),
        updated_at_unix: model.updated_at.timestamp(),
    }
}
