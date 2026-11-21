use anyhow::{Context, Result};
use pioneer_entity::{thread, thread_lineage};
use pioneer_protocol::TaskThreadLineage;
use sea_orm::sea_query::{Alias, BinOper, Expr, ExprTrait, LockType, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

pub async fn upsert_lineage<C: ConnectionTrait>(
    db: &C,
    lineage: &TaskThreadLineage,
) -> Result<bool> {
    // Reviewer admission can publish its lineage before the execution creates
    // a thread header. When the header exists, coordinate with its writers.
    thread::Entity::find_by_id(&lineage.child_thread_id)
        .select_only()
        .column(thread::Column::Id)
        .lock(LockType::NoKeyUpdate)
        .into_tuple::<String>()
        .one(db)
        .await?;
    let created_at = chrono::DateTime::from_timestamp(lineage.created_at, 0)
        .unwrap_or_else(chrono::Utc::now)
        .fixed_offset();
    let updated_columns = [
        thread_lineage::Column::ParentThreadId,
        thread_lineage::Column::RootThreadId,
        thread_lineage::Column::Depth,
        thread_lineage::Column::OriginKind,
        thread_lineage::Column::CreatedByThreadId,
        thread_lineage::Column::CreatedByTurnId,
    ];
    // The conflict itself serializes competing writes, including the first
    // admission. A replay never advances projection state or emits new work.
    let changed = updated_columns
        .iter()
        .copied()
        .fold(Condition::any(), |condition, column| {
            condition.add(Expr::col((thread_lineage::Entity, column)).binary(
                BinOper::Custom("IS DISTINCT FROM"),
                Expr::col((Alias::new("excluded"), column)),
            ))
        });
    let affected = thread_lineage::Entity::insert(thread_lineage::ActiveModel {
        child_thread_id: Set(lineage.child_thread_id.clone()),
        parent_thread_id: Set(lineage.parent_thread_id.clone()),
        root_thread_id: Set(lineage.root_thread_id.clone()),
        depth: Set(lineage.depth),
        origin_kind: Set(lineage.origin_kind.clone()),
        created_by_thread_id: Set(lineage.created_by_thread_id.clone()),
        created_by_turn_id: Set(lineage.created_by_turn_id.clone()),
        created_at: Set(created_at),
        ..Default::default()
    })
    .on_conflict(
        OnConflict::column(thread_lineage::Column::ChildThreadId)
            .update_columns(updated_columns)
            .action_cond_where(changed)
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to upsert thread lineage")?;
    Ok(affected != 0)
}

pub async fn find_lineage_by_child_thread<C: ConnectionTrait>(
    db: &C,
    child_thread_id: &str,
) -> Result<Option<thread_lineage::Model>> {
    thread_lineage::Entity::find_by_id(child_thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread lineage by child thread")
}

pub async fn list_children_for_parent_thread<C: ConnectionTrait>(
    db: &C,
    parent_thread_id: &str,
) -> Result<Vec<thread_lineage::Model>> {
    thread_lineage::Entity::find()
        .filter(thread_lineage::Column::ParentThreadId.eq(parent_thread_id.to_owned()))
        .order_by_asc(thread_lineage::Column::CreatedAt)
        .order_by_asc(thread_lineage::Column::ChildThreadId)
        .all(db)
        .await
        .context("failed to list child thread lineage rows")
}

pub async fn list_lineage_by_root_thread<C: ConnectionTrait>(
    db: &C,
    root_thread_id: &str,
) -> Result<Vec<thread_lineage::Model>> {
    thread_lineage::Entity::find()
        .filter(thread_lineage::Column::RootThreadId.eq(root_thread_id.to_owned()))
        .order_by_asc(thread_lineage::Column::Depth)
        .order_by_asc(thread_lineage::Column::CreatedAt)
        .order_by_asc(thread_lineage::Column::ChildThreadId)
        .all(db)
        .await
        .context("failed to list thread lineage subtree")
}

pub async fn list_lineage_by_root_thread_page<C: ConnectionTrait>(
    db: &C,
    root_thread_id: &str,
    after: Option<&str>,
) -> Result<Vec<thread_lineage::Model>> {
    let mut query = thread_lineage::Entity::find()
        .filter(thread_lineage::Column::RootThreadId.eq(root_thread_id.to_owned()));
    if let Some(after) = after {
        query = query.filter(thread_lineage::Column::ChildThreadId.gt(after));
    }
    query
        .order_by_asc(thread_lineage::Column::ChildThreadId)
        .limit(200)
        .all(db)
        .await
        .context("failed to list lineage page")
}

/// One bounded step for direct-child enumeration or cursor-driven DFS.
/// Unique child IDs give an exclusive, immutable keyset even when creation
/// timestamps coincide. No root/depth filter: the actual parent edge is the
/// authority, including for a subtree rooted at an intermediate node.
pub async fn list_children_page<C: ConnectionTrait>(
    db: &C,
    parent_thread_id: &str,
    after_child_thread_id: Option<&str>,
    limit: u64,
) -> Result<Vec<thread_lineage::Model>> {
    anyhow::ensure!(
        (1..=200).contains(&limit),
        "child candidate page limit must be 1..=200"
    );
    let mut query = thread_lineage::Entity::find()
        .filter(thread_lineage::Column::ParentThreadId.eq(parent_thread_id.to_owned()));
    if let Some(after) = after_child_thread_id {
        query = query.filter(thread_lineage::Column::ChildThreadId.gt(after.to_owned()));
    }
    query
        .order_by_asc(thread_lineage::Column::ChildThreadId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list child lineage page")
}
