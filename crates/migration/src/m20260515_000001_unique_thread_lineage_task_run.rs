use sea_orm_migration::{prelude::*, sea_orm::DbBackend};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        deduplicate_existing_thread_lineage_runs(manager).await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("uidx_thread_lineage_task_run")
                    .table("thread_lineage")
                    .col("task_run_id")
                    .unique()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("uidx_thread_lineage_task_run")
                    .table("thread_lineage")
                    .to_owned(),
            )
            .await
    }
}

async fn deduplicate_existing_thread_lineage_runs(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    let sql = match manager.get_database_backend() {
        DbBackend::MySql | DbBackend::Postgres | DbBackend::Sqlite => {
            "DELETE FROM thread_lineage \
             WHERE child_thread_id IN ( \
                 SELECT child_thread_id FROM ( \
                     SELECT child_thread_id, \
                            ROW_NUMBER() OVER ( \
                                PARTITION BY task_run_id \
                                ORDER BY created_at ASC, child_thread_id ASC \
                            ) AS duplicate_rank \
                     FROM thread_lineage \
                 ) duplicate_thread_lineage \
                 WHERE duplicate_rank > 1 \
             )"
        }
        _ => {
            return Err(DbErr::Custom(
                "unsupported database backend for thread_lineage deduplication".to_string(),
            ));
        }
    };

    manager.get_connection().execute_unprepared(sql).await?;

    Ok(())
}
