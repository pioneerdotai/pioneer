use anyhow::{Context, Result, bail};
use pioneer_entity::thread_lineage;
use pioneer_protocol::TaskThreadLineage;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::util::unix_to_datetime;

pub async fn upsert_lineage<C: ConnectionTrait>(db: &C, lineage: &TaskThreadLineage) -> Result<()> {
    thread_lineage::Entity::insert(thread_lineage::ActiveModel {
        child_thread_id: Set(lineage.child_thread_id.clone()),
        parent_thread_id: Set(lineage.parent_thread_id.clone()),
        root_thread_id: Set(lineage.root_thread_id.clone()),
        depth: Set(lineage.depth),
        origin_kind: Set(lineage.origin_kind.clone()),
        created_by_thread_id: Set(lineage.created_by_thread_id.clone()),
        created_by_turn_id: Set(lineage.created_by_turn_id.clone()),
        created_at: Set(unix_to_datetime(lineage.created_at)),
    })
    .on_conflict(
        OnConflict::column(thread_lineage::Column::ChildThreadId)
            .update_columns([
                thread_lineage::Column::ParentThreadId,
                thread_lineage::Column::RootThreadId,
                thread_lineage::Column::Depth,
                thread_lineage::Column::OriginKind,
                thread_lineage::Column::CreatedByThreadId,
                thread_lineage::Column::CreatedByTurnId,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert thread lineage")?;
    Ok(())
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
        .all(db)
        .await
        .context("failed to list thread lineage subtree")
}

pub async fn list_lineage_by_root_thread_bounded<C: ConnectionTrait>(
    db: &C,
    root_thread_id: &str,
    limit: u64,
) -> Result<Vec<thread_lineage::Model>> {
    if limit == 0 || limit > 128 {
        bail!("thread lineage projection exceeds its bounded limit");
    }
    let rows = thread_lineage::Entity::find()
        .filter(thread_lineage::Column::RootThreadId.eq(root_thread_id.to_owned()))
        .order_by_asc(thread_lineage::Column::Depth)
        .order_by_asc(thread_lineage::Column::CreatedAt)
        .order_by_asc(thread_lineage::Column::ChildThreadId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list bounded thread lineage subtree")?;
    Ok(rows)
}
