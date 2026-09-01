use anyhow::{Context, Result};
use pioneer_entity::{thread_folder, thread_placement};
use sea_orm::entity::ActiveModelTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set, Statement};

pub async fn list_folders_by_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<thread_folder::Model>> {
    thread_folder::Entity::find()
        .filter(thread_folder::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_asc(thread_folder::Column::Name)
        .order_by_asc(thread_folder::Column::Id)
        .all(db)
        .await
        .context("failed to list thread folders by workspace")
}

pub async fn list_placements_by_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<thread_placement::Model>> {
    thread_placement::Entity::find()
        .filter(thread_placement::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_asc(thread_placement::Column::ThreadId)
        .all(db)
        .await
        .context("failed to list thread placements by workspace")
}

pub async fn find_placement_by_thread_id<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<thread_placement::Model>> {
    thread_placement::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread placement by thread id")
}

pub async fn find_folder_by_id<C: ConnectionTrait>(
    db: &C,
    folder_id: &str,
) -> Result<Option<thread_folder::Model>> {
    thread_folder::Entity::find_by_id(folder_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread folder by id")
}

/// Checks the prospective parent chain entirely inside SQLite. Using one
/// recursive statement avoids loading and walking an unbounded workspace
/// folder graph while a write transaction owns the physical writer.
pub async fn folder_parent_would_create_cycle<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    folder_id: &str,
    parent_folder_id: &str,
) -> Result<bool> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "WITH RECURSIVE ancestors(id, parent_folder_id) AS (\
               SELECT id, parent_folder_id FROM thread_folder \
               WHERE id = ? AND workspace_id = ? \
               UNION \
               SELECT parent.id, parent.parent_folder_id \
               FROM thread_folder parent \
               JOIN ancestors child ON parent.id = child.parent_folder_id \
               WHERE parent.workspace_id = ?\
             ) \
             SELECT EXISTS(SELECT 1 FROM ancestors WHERE id = ?) AS would_cycle"
                .to_owned(),
            [
                parent_folder_id.into(),
                workspace_id.into(),
                workspace_id.into(),
                folder_id.into(),
            ],
        ))
        .await
        .context("failed to validate thread folder ancestry")?
        .context("thread folder ancestry query returned no result")?;
    Ok(row.try_get::<i64>("", "would_cycle")? != 0)
}

pub async fn insert_folder<C: ConnectionTrait>(
    db: &C,
    folder_id: &str,
    workspace_id: &str,
    parent_folder_id: Option<&str>,
    name: &str,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    thread_folder::Entity::insert(thread_folder::ActiveModel {
        id: Set(folder_id.to_owned()),
        workspace_id: Set(workspace_id.to_owned()),
        parent_folder_id: Set(parent_folder_id.map(str::to_owned)),
        name: Set(name.to_owned()),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .exec(db)
    .await
    .context("failed to insert thread folder")?;

    Ok(())
}

pub async fn update_folder_parent<C: ConnectionTrait>(
    db: &C,
    folder_id: &str,
    parent_folder_id: Option<&str>,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let Some(model) = thread_folder::Entity::find_by_id(folder_id.to_owned())
        .one(db)
        .await
        .context("failed to query thread folder for parent update")?
    else {
        return Ok(());
    };

    let mut active_model: thread_folder::ActiveModel = model.into();
    active_model.parent_folder_id = Set(parent_folder_id.map(str::to_owned));
    active_model.updated_at = Set(updated_at);
    active_model
        .update(db)
        .await
        .context("failed to update thread folder parent")?;

    Ok(())
}

pub async fn delete_folder<C: ConnectionTrait>(db: &C, folder_id: &str) -> Result<u64> {
    let result = thread_folder::Entity::delete_by_id(folder_id.to_owned())
        .exec(db)
        .await
        .context("failed to delete thread folder")?;
    Ok(result.rows_affected)
}

pub async fn reparent_child_folders<C: ConnectionTrait>(
    db: &C,
    from_parent_folder_id: &str,
    to_parent_folder_id: Option<&str>,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    thread_folder::Entity::update_many()
        .col_expr(
            thread_folder::Column::ParentFolderId,
            Expr::value(to_parent_folder_id.map(str::to_owned)),
        )
        .col_expr(thread_folder::Column::UpdatedAt, Expr::value(updated_at))
        .filter(thread_folder::Column::ParentFolderId.eq(from_parent_folder_id.to_owned()))
        .exec(db)
        .await
        .context("failed to reparent child folders")?;

    Ok(())
}

pub async fn move_thread_placements_to_folder<C: ConnectionTrait>(
    db: &C,
    from_folder_id: &str,
    to_folder_id: Option<&str>,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    thread_placement::Entity::update_many()
        .col_expr(
            thread_placement::Column::FolderId,
            Expr::value(to_folder_id.map(str::to_owned)),
        )
        .col_expr(thread_placement::Column::UpdatedAt, Expr::value(updated_at))
        .filter(thread_placement::Column::FolderId.eq(from_folder_id.to_owned()))
        .exec(db)
        .await
        .context("failed to move thread placements between folders")?;

    Ok(())
}

pub async fn upsert_thread_placement<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    folder_id: Option<&str>,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    thread_placement::Entity::insert(thread_placement::ActiveModel {
        thread_id: Set(thread_id.to_owned()),
        workspace_id: Set(workspace_id.to_owned()),
        folder_id: Set(folder_id.map(str::to_owned)),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::column(thread_placement::Column::ThreadId)
            .update_columns([
                thread_placement::Column::WorkspaceId,
                thread_placement::Column::FolderId,
                thread_placement::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert thread placement")?;

    Ok(())
}
