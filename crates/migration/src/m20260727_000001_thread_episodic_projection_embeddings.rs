use sea_orm_migration::{
    prelude::*,
    schema::*,
    sea_orm::{ConnectionTrait, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        add_column_if_missing(
            manager,
            "thread_episodic_items",
            "projection_group_id",
            "TEXT",
        )
        .await?;
        add_column_if_missing(
            manager,
            "thread_episodic_items",
            "embedding_artifact_id",
            "TEXT",
        )
        .await?;

        manager
            .get_connection()
            .execute_unprepared(
                "UPDATE thread_episodic_items
                 SET projection_group_id = id
                 WHERE projection_group_id IS NULL OR projection_group_id = ''",
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_episodic_embedding_artifacts")
                    .if_not_exists()
                    .col(string("id").string_len(64).primary_key())
                    .col(string("workspace_id").string_len(21))
                    .col(string("pipeline_identity_hash").string_len(64))
                    .col(string("input_hash").string_len(64))
                    .col(string("provider_id").string_len(64))
                    .col(text("model"))
                    .col(integer("dimension"))
                    .col(boolean("normalized"))
                    .col(binary("vector_bytes"))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(
                        timestamp_with_time_zone("last_used_at").default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        create_index(
            manager,
            "idx_thread_episodic_items_projection_group",
            "thread_episodic_items",
            &["workspace_id", "projection_group_id"],
            false,
        )
        .await?;
        create_index(
            manager,
            "idx_thread_episodic_items_embedding_artifact",
            "thread_episodic_items",
            &["embedding_artifact_id"],
            false,
        )
        .await?;
        create_index(
            manager,
            "idx_thread_episodic_embedding_artifacts_workspace",
            "thread_episodic_embedding_artifacts",
            &["workspace_id", "last_used_at"],
            false,
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for index in [
            "idx_thread_episodic_embedding_artifacts_workspace",
            "idx_thread_episodic_items_embedding_artifact",
            "idx_thread_episodic_items_projection_group",
        ] {
            manager
                .drop_index(Index::drop().if_exists().name(index).to_owned())
                .await?;
        }
        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("thread_episodic_embedding_artifacts")
                    .to_owned(),
            )
            .await?;
        drop_column_if_present(manager, "thread_episodic_items", "embedding_artifact_id").await?;
        drop_column_if_present(manager, "thread_episodic_items", "projection_group_id").await
    }
}

async fn add_column_if_missing(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
    sql_type: &str,
) -> Result<(), DbErr> {
    if !column_exists(manager, table, column).await? {
        manager
            .get_connection()
            .execute_unprepared(
                format!("ALTER TABLE {table} ADD COLUMN {column} {sql_type}").as_str(),
            )
            .await?;
    }
    Ok(())
}

async fn drop_column_if_present(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
) -> Result<(), DbErr> {
    if column_exists(manager, table, column).await? {
        manager
            .get_connection()
            .execute_unprepared(format!("ALTER TABLE {table} DROP COLUMN {column}").as_str())
            .await?;
    }
    Ok(())
}

async fn column_exists(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
) -> Result<bool, DbErr> {
    let statement = Statement::from_string(
        manager.get_database_backend(),
        format!("SELECT 1 FROM pragma_table_info('{table}') WHERE name = '{column}' LIMIT 1"),
    );
    Ok(manager
        .get_connection()
        .query_one_raw(statement)
        .await?
        .is_some())
}

async fn create_index(
    manager: &SchemaManager<'_>,
    name: &str,
    table: &str,
    columns: &[&str],
    unique: bool,
) -> Result<(), DbErr> {
    let mut index = Index::create();
    index.if_not_exists().name(name).table(Alias::new(table));
    for column in columns {
        index.col(Alias::new(*column));
    }
    if unique {
        index.unique();
    }
    manager.create_index(index.to_owned()).await
}
