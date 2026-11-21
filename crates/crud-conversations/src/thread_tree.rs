use anyhow::{Context, Result};
use chrono::Timelike;
use pioneer_entity::{thread, thread_folder, thread_placement, workspace};
use pioneer_protocol::{ThreadFolder, ThreadPlacement, generate_id};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::entity::ActiveModelTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, LockType, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, DbBackend, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, Statement, TransactionTrait,
};

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

/// Serialize topology changes within the workspace before inspecting parents.
pub async fn lock_topology(db: &DatabaseTransaction, workspace_id: &str) -> Result<()> {
    workspace::Entity::find_by_id(workspace_id)
        .select_only()
        .column(workspace::Column::Id)
        .lock(LockType::NoKeyUpdate)
        .into_tuple::<String>()
        .one(db)
        .await?
        .context("missing workspace for folder topology change")?;
    Ok(())
}

/// Read a bounded-width recursive chain in the transaction that owns the
/// topology lock. SeaORM has no recursive union API on Entity::find.
pub async fn folder_parent_would_create_cycle<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    folder_id: &str,
    parent_folder_id: &str,
) -> Result<bool> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "WITH RECURSIVE ancestors(id, parent_folder_id) AS (\
               SELECT id, parent_folder_id FROM thread_folder \
               WHERE id = $1 AND workspace_id = $2 \
               UNION \
               SELECT parent.id, parent.parent_folder_id \
               FROM thread_folder parent \
               JOIN ancestors child ON parent.id = child.parent_folder_id \
               WHERE parent.workspace_id = $2\
             ) \
             SELECT EXISTS(SELECT 1 FROM ancestors WHERE id = $3) AS would_cycle"
                .to_owned(),
            [
                parent_folder_id.into(),
                workspace_id.into(),
                folder_id.into(),
            ],
        ))
        .await
        .context("failed to validate thread folder ancestry")?
        .context("thread folder ancestry query returned no result")?;
    Ok(row.try_get("", "would_cycle")?)
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

pub async fn list_thread_folders(
    db: &SqliteDatabase,
    workspace_id: &str,
) -> Result<Vec<ThreadFolder>> {
    let models = list_folders_by_workspace(db, workspace_id).await?;
    Ok(models
        .into_iter()
        .map(thread_folder_from_db_model)
        .collect())
}

pub async fn list_thread_placements(
    db: &SqliteDatabase,
    workspace_id: &str,
) -> Result<Vec<ThreadPlacement>> {
    let models = list_placements_by_workspace(db, workspace_id).await?;
    Ok(models
        .into_iter()
        .map(thread_placement_from_db_model)
        .collect())
}

pub async fn get_thread_placement(
    db: &SqliteDatabase,
    thread_id: &str,
) -> Result<Option<ThreadPlacement>> {
    Ok(find_placement_by_thread_id(db, thread_id)
        .await?
        .map(thread_placement_from_db_model))
}

pub async fn create_thread_folder(
    db: &SqliteDatabase,
    workspace_id: &str,
    parent_folder_id: Option<&str>,
    name: &str,
) -> Result<ThreadFolder> {
    let transaction = db.begin().await?;
    lock_topology(&transaction, workspace_id).await?;
    if let Some(parent_folder_id) = parent_folder_id {
        let Some(parent) = find_folder_by_id(&transaction, parent_folder_id).await? else {
            anyhow::bail!("parent folder `{parent_folder_id}` was not found");
        };

        if parent.workspace_id != workspace_id {
            anyhow::bail!(
                "parent folder `{parent_folder_id}` belongs to workspace `{}`",
                parent.workspace_id
            );
        }
    }

    let created_at = chrono::Utc::now()
        .with_nanosecond(0)
        .expect("whole-second timestamp")
        .fixed_offset();
    let now = created_at.timestamp();
    let folder_id = generate_id(21);
    insert_folder(
        &transaction,
        folder_id.as_str(),
        workspace_id,
        parent_folder_id,
        name,
        created_at,
        created_at,
    )
    .await?;

    transaction.commit().await?;
    Ok(ThreadFolder {
        id: folder_id,
        workspace_id: workspace_id.to_owned(),
        parent_folder_id: parent_folder_id.map(str::to_owned),
        name: name.to_owned(),
        created_at: now,
        updated_at: now,
    })
}

pub async fn delete_thread_folder_promote(
    db: &SqliteDatabase,
    workspace_id: &str,
    folder_id: &str,
) -> Result<bool> {
    let transaction = db
        .begin()
        .await
        .context("failed to begin folder delete transaction")?;
    lock_topology(&transaction, workspace_id).await?;

    let folder = find_folder_by_id(&transaction, folder_id).await?;
    let Some(folder) = folder else {
        transaction
            .rollback()
            .await
            .context("failed to rollback folder delete transaction")?;
        return Ok(false);
    };

    if folder.workspace_id != workspace_id {
        transaction
            .rollback()
            .await
            .context("failed to rollback folder delete transaction")?;
        anyhow::bail!(
            "folder `{folder_id}` belongs to workspace `{}`",
            folder.workspace_id
        );
    }

    let now = chrono::Utc::now()
        .with_nanosecond(0)
        .expect("whole-second timestamp")
        .fixed_offset();
    reparent_child_folders(
        &transaction,
        folder_id,
        folder.parent_folder_id.as_deref(),
        now,
    )
    .await?;

    move_thread_placements_to_folder(
        &transaction,
        folder_id,
        folder.parent_folder_id.as_deref(),
        now,
    )
    .await?;

    delete_folder(&transaction, folder_id).await?;

    transaction
        .commit()
        .await
        .context("failed to commit folder delete transaction")?;

    Ok(true)
}

pub async fn move_thread_to_folder(
    db: &SqliteDatabase,
    workspace_id: &str,
    thread_id: &str,
    folder_id: Option<&str>,
) -> Result<()> {
    let transaction = db.begin().await?;
    lock_topology(&transaction, workspace_id).await?;
    thread::Entity::find_by_id(thread_id)
        .select_only()
        .column(thread::Column::Id)
        .filter(thread::Column::WorkspaceId.eq(workspace_id))
        .lock(LockType::Share)
        .into_tuple::<String>()
        .one(&transaction)
        .await?
        .context("thread is missing from the placement workspace")?;

    if let Some(folder_id) = folder_id {
        let Some(folder) = find_folder_by_id(&transaction, folder_id).await? else {
            anyhow::bail!("folder `{folder_id}` was not found");
        };

        if folder.workspace_id != workspace_id {
            anyhow::bail!(
                "folder `{folder_id}` belongs to workspace `{}`",
                folder.workspace_id
            );
        }
    }

    let now = chrono::Utc::now()
        .with_nanosecond(0)
        .expect("whole-second timestamp")
        .fixed_offset();
    upsert_thread_placement(&transaction, workspace_id, thread_id, folder_id, now, now).await?;
    transaction.commit().await?;
    Ok(())
}

pub async fn move_folder(
    db: &SqliteDatabase,
    workspace_id: &str,
    folder_id: &str,
    parent_folder_id: Option<&str>,
) -> Result<()> {
    if parent_folder_id == Some(folder_id) {
        anyhow::bail!("cannot move folder into itself");
    }

    let transaction = db
        .begin()
        .await
        .context("failed to begin folder move transaction")?;
    lock_topology(&transaction, workspace_id).await?;

    let Some(folder) = find_folder_by_id(&transaction, folder_id).await? else {
        transaction
            .rollback()
            .await
            .context("failed to rollback folder move transaction")?;
        anyhow::bail!("folder `{folder_id}` was not found");
    };
    if folder.workspace_id != workspace_id {
        transaction
            .rollback()
            .await
            .context("failed to rollback cross-workspace folder move transaction")?;
        anyhow::bail!(
            "folder `{folder_id}` belongs to workspace `{}`",
            folder.workspace_id
        );
    }

    if let Some(parent_folder_id) = parent_folder_id {
        let Some(parent) = find_folder_by_id(&transaction, parent_folder_id).await? else {
            transaction
                .rollback()
                .await
                .context("failed to rollback folder move transaction")?;
            anyhow::bail!("parent folder `{parent_folder_id}` was not found");
        };
        if parent.workspace_id != workspace_id {
            transaction
                .rollback()
                .await
                .context("failed to rollback cross-workspace parent move transaction")?;
            anyhow::bail!(
                "parent folder `{parent_folder_id}` belongs to workspace `{}`",
                parent.workspace_id
            );
        }
        if folder_parent_would_create_cycle(&transaction, workspace_id, folder_id, parent_folder_id)
            .await?
        {
            transaction
                .rollback()
                .await
                .context("failed to rollback cyclic folder move transaction")?;
            anyhow::bail!(
                "cannot move folder `{folder_id}` into its descendant `{parent_folder_id}`"
            );
        }
    }

    if folder.parent_folder_id.as_deref() == parent_folder_id {
        transaction
            .rollback()
            .await
            .context("failed to rollback folder move transaction")?;
        return Ok(());
    }

    let now = chrono::Utc::now()
        .with_nanosecond(0)
        .expect("whole-second timestamp")
        .fixed_offset();
    update_folder_parent(&transaction, folder_id, parent_folder_id, now).await?;

    transaction
        .commit()
        .await
        .context("failed to commit folder move transaction")?;

    Ok(())
}

fn thread_folder_from_db_model(model: pioneer_entity::thread_folder::Model) -> ThreadFolder {
    ThreadFolder {
        id: model.id,
        workspace_id: model.workspace_id,
        parent_folder_id: model.parent_folder_id,
        name: model.name,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    }
}

fn thread_placement_from_db_model(
    model: pioneer_entity::thread_placement::Model,
) -> ThreadPlacement {
    ThreadPlacement {
        thread_id: model.thread_id,
        workspace_id: model.workspace_id,
        folder_id: model.folder_id,
    }
}
