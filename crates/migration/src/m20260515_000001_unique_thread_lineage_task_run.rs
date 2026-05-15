use sea_orm_migration::{prelude::*, schema::*, sea_orm::DbBackend};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum TaskEvent {
    Table,
    IdempotencyKey,
}

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
            .await?;

        create_task_run_execution_table(manager).await?;
        add_task_event_idempotency(manager).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_task_event_idempotency(manager).await?;
        drop_task_run_execution_table(manager).await?;

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

async fn add_task_event_idempotency(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if !manager.has_column("task_event", "idempotency_key").await? {
        manager
            .alter_table(
                Table::alter()
                    .table(TaskEvent::Table)
                    .add_column(
                        ColumnDef::new(TaskEvent::IdempotencyKey)
                            .string_len(256)
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;
    }

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uidx_task_event_task_idempotency_key")
                .table(TaskEvent::Table)
                .col("task_id")
                .col(TaskEvent::IdempotencyKey)
                .unique()
                .to_owned(),
        )
        .await?;

    Ok(())
}

async fn drop_task_event_idempotency(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .drop_index(
            Index::drop()
                .if_exists()
                .name("uidx_task_event_task_idempotency_key")
                .table(TaskEvent::Table)
                .to_owned(),
        )
        .await?;

    if manager.has_column("task_event", "idempotency_key").await? {
        manager
            .alter_table(
                Table::alter()
                    .table(TaskEvent::Table)
                    .drop_column(TaskEvent::IdempotencyKey)
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}

async fn create_task_run_execution_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("task_run_execution")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("task_id").string_len(21))
                .col(string("task_run_id").string_len(21))
                .col(string("executor_kind").string_len(32))
                .col(string("status").string_len(32))
                .col(string_null("worker_id").string_len(128))
                .col(timestamp_with_time_zone_null("lease_until"))
                .col(timestamp_with_time_zone_null("heartbeat_at"))
                .col(string_null("child_thread_id").string_len(21))
                .col(string_null("child_turn_id").string_len(21))
                .col(timestamp_with_time_zone_null("started_at"))
                .col(timestamp_with_time_zone_null("completed_at"))
                .col(text_null("result_json"))
                .col(text_null("error_json"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uidx_task_run_execution_run")
                .table("task_run_execution")
                .col("task_run_id")
                .unique()
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uidx_task_run_execution_child_thread")
                .table("task_run_execution")
                .col("child_thread_id")
                .unique()
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uidx_task_run_execution_child_turn")
                .table("task_run_execution")
                .col("child_turn_id")
                .unique()
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_run_execution_task")
                .table("task_run_execution")
                .col("task_id")
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_run_execution_status_lease")
                .table("task_run_execution")
                .col("status")
                .col("lease_until")
                .to_owned(),
        )
        .await?;

    Ok(())
}

async fn drop_task_run_execution_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .drop_index(
            Index::drop()
                .if_exists()
                .name("idx_task_run_execution_status_lease")
                .table("task_run_execution")
                .to_owned(),
        )
        .await?;
    manager
        .drop_index(
            Index::drop()
                .if_exists()
                .name("idx_task_run_execution_task")
                .table("task_run_execution")
                .to_owned(),
        )
        .await?;
    manager
        .drop_index(
            Index::drop()
                .if_exists()
                .name("uidx_task_run_execution_child_turn")
                .table("task_run_execution")
                .to_owned(),
        )
        .await?;
    manager
        .drop_index(
            Index::drop()
                .if_exists()
                .name("uidx_task_run_execution_child_thread")
                .table("task_run_execution")
                .to_owned(),
        )
        .await?;
    manager
        .drop_index(
            Index::drop()
                .if_exists()
                .name("uidx_task_run_execution_run")
                .table("task_run_execution")
                .to_owned(),
        )
        .await?;
    manager
        .drop_table(Table::drop().table("task_run_execution").to_owned())
        .await?;

    Ok(())
}
