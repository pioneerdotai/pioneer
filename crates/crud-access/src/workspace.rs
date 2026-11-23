use anyhow::{Context, Result};
use pioneer_entity::workspace;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict, Query};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, Set,
};

pub const DEFAULT_WORKSPACE_ID: &str = "000000000000000000000";
pub const DEFAULT_WORKSPACE_NAME: &str = "Default Workspace";

pub async fn find<C: ConnectionTrait>(db: &C, id: &str) -> Result<Option<workspace::Model>> {
    workspace::Entity::find_by_id(id)
        .one(db)
        .await
        .context("read workspace")
}

pub async fn list<C: ConnectionTrait>(db: &C) -> Result<Vec<workspace::Model>> {
    workspace::Entity::find()
        .order_by_asc(workspace::Column::CreatedAt)
        .order_by_asc(workspace::Column::Id)
        .all(db)
        .await
        .context("list workspaces")
}

/// Serialize changes that choose the current workspace across connections.
/// SeaORM's row locks cannot protect the empty set or the gap after clearing
/// the previous selection. PostgreSQL releases this local lock at commit.
pub async fn lock_current_selection<C: ConnectionTrait>(transaction: &C) -> Result<()> {
    let statement = Query::select()
        .expr(Expr::cust_with_values(
            "pg_advisory_xact_lock($1)",
            [1987281851_i64],
        ))
        .to_owned();
    transaction.query_one(&statement).await?;
    Ok(())
}

pub async fn create<C: ConnectionTrait>(
    transaction: &C,
    id: &str,
    name: Option<String>,
) -> Result<(workspace::Model, bool)> {
    lock_current_selection(transaction).await?;
    if let Some(model) = find(transaction, id).await? {
        return Ok((model, false));
    }
    let name = match name {
        Some(name) => name,
        None => {
            let count = workspace::Entity::find().count(transaction).await?;
            format!("Workspace {}", count + 1)
        }
    };
    let current = workspace::Entity::find()
        .filter(workspace::Column::IsCurrent.eq(true))
        .filter(workspace::Column::IsActive.eq(true))
        .count(transaction)
        .await?
        > 0;
    let inserted = workspace::ActiveModel {
        id: Set(id.to_owned()),
        name: Set(name),
        is_active: Set(true),
        is_current: Set(!current),
        ..Default::default()
    }
    .insert(transaction)
    .await
    .context("create workspace")?;
    Ok((inserted, true))
}

pub async fn set_current<C: ConnectionTrait>(
    transaction: &C,
    model: workspace::Model,
    now: DateTimeWithTimeZone,
) -> Result<workspace::Model> {
    workspace::Entity::update_many()
        .filter(workspace::Column::IsActive.eq(true))
        .filter(workspace::Column::IsCurrent.eq(true))
        .col_expr(workspace::Column::IsCurrent, Expr::val(false))
        .col_expr(workspace::Column::UpdatedAt, Expr::val(now))
        .exec(transaction)
        .await
        .context("clear previous workspace selection")?;
    let mut active: workspace::ActiveModel = model.into();
    active.is_current = Set(true);
    active.updated_at = Set(now);
    active.update(transaction).await.context("select workspace")
}

pub async fn set_name<C: ConnectionTrait>(
    db: &C,
    model: workspace::Model,
    name: String,
    now: DateTimeWithTimeZone,
) -> Result<workspace::Model> {
    let mut active: workspace::ActiveModel = model.into();
    active.name = Set(name);
    active.updated_at = Set(now);
    active.update(db).await.context("rename workspace")
}

pub async fn insert_default<C: ConnectionTrait>(db: &C) -> Result<bool> {
    let count = workspace::Entity::insert(workspace::ActiveModel {
        id: Set(DEFAULT_WORKSPACE_ID.to_owned()),
        name: Set(DEFAULT_WORKSPACE_NAME.to_owned()),
        is_active: Set(true),
        is_current: Set(true),
        ..Default::default()
    })
    .on_conflict(OnConflict::new().do_nothing().to_owned())
    .exec_without_returning(db)
    .await
    .context("insert default workspace")?;
    Ok(count == 1)
}
