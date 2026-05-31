use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DbBackend, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        precheck_target_runtime_rows(manager).await?;
        drop_old_indexes(manager).await?;
        drop_old_columns(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        add_old_columns_as_nullable(manager).await?;
        recreate_old_indexes(manager).await
    }
}

async fn precheck_target_runtime_rows(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for table in [
        "thread_lineage",
        "task_run_thread_binding",
        "task_run_turn",
        "task_result_candidate",
        "task_result_review_event",
    ] {
        if !manager.has_table(table).await? {
            return Err(DbErr::Custom(format!(
                "cannot drop legacy task runtime fields: required table `{table}` is missing"
            )));
        }
    }

    if manager.has_column("thread_lineage", "task_run_id").await?
        && manager
            .has_column("thread_lineage", "child_turn_id")
            .await?
    {
        ensure_zero(
            manager,
            "legacy thread_lineage rows without primary executor binding",
            r#"
            select count(*) as count
            from thread_lineage tl
            left join task_run_thread_binding b
              on b.run_id = tl.task_run_id
             and b.binding_kind = 'primary_executor'
             and b.thread_id = tl.child_thread_id
            where b.id is null
            "#,
        )
        .await?;

        ensure_zero(
            manager,
            "legacy thread_lineage rows without matching task_run_turn",
            r#"
            select count(*) as count
            from thread_lineage tl
            left join task_run_turn trt
              on trt.run_id = tl.task_run_id
             and trt.thread_id = tl.child_thread_id
             and trt.turn_id = tl.child_turn_id
            where trt.id is null
            "#,
        )
        .await?;
    }

    if manager
        .has_column("task_run_execution", "child_thread_id")
        .await?
        && manager
            .has_column("task_run_execution", "child_turn_id")
            .await?
    {
        ensure_zero(
            manager,
            "legacy task_run_execution child threads without primary executor binding",
            r#"
            select count(*) as count
            from task_run_execution tre
            left join task_run_thread_binding b
              on b.run_id = tre.task_run_id
             and b.binding_kind = 'primary_executor'
             and b.thread_id = tre.child_thread_id
            where tre.executor_kind = 'agent'
              and tre.child_thread_id is not null
              and b.id is null
            "#,
        )
        .await?;

        ensure_zero(
            manager,
            "legacy task_run_execution child turns without matching task_run_turn",
            r#"
            select count(*) as count
            from task_run_execution tre
            left join task_run_turn trt
              on trt.run_id = tre.task_run_id
             and trt.thread_id = tre.child_thread_id
             and trt.turn_id = tre.child_turn_id
            where tre.executor_kind = 'agent'
              and tre.child_thread_id is not null
              and tre.child_turn_id is not null
              and trt.id is null
            "#,
        )
        .await?;
    }

    ensure_zero(
        manager,
        "primary executor bindings without thread_lineage graph rows",
        r#"
        select count(*) as count
        from task_run_thread_binding b
        left join thread_lineage tl on tl.child_thread_id = b.thread_id
        where b.binding_kind = 'primary_executor'
          and tl.child_thread_id is null
        "#,
    )
    .await?;

    ensure_zero(
        manager,
        "task_run_turn rows without thread_lineage graph rows",
        r#"
        select count(*) as count
        from task_run_turn trt
        left join thread_lineage tl on tl.child_thread_id = trt.thread_id
        where tl.child_thread_id is null
        "#,
    )
    .await?;

    ensure_zero(
        manager,
        "completed task runs without accepted result candidates",
        r#"
        select count(*) as count
        from task_run r
        join task_run_thread_binding b
          on b.run_id = r.id
         and b.binding_kind = 'primary_executor'
        where r.status = 'succeeded'
          and r.result_json is not null
          and not exists (
              select 1
              from task_result_candidate c
              where c.run_id = r.id
                and c.status = 'accepted'
                and c.result_json is not null
          )
        "#,
    )
    .await?;

    Ok(())
}

async fn drop_old_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for (name, table) in [
        ("uidx_thread_lineage_task_run", "thread_lineage"),
        ("idx_thread_lineage_task", "thread_lineage"),
        ("idx_thread_lineage_run", "thread_lineage"),
        ("uidx_task_run_execution_child_thread", "task_run_execution"),
        ("uidx_task_run_execution_child_turn", "task_run_execution"),
    ] {
        manager
            .drop_index(Index::drop().if_exists().name(name).table(table).to_owned())
            .await?;
    }
    Ok(())
}

async fn drop_old_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for column in ["child_turn_id", "parent_turn_id", "task_id", "task_run_id"] {
        drop_column_if_exists(manager, "thread_lineage", column).await?;
    }
    for column in ["child_thread_id", "child_turn_id"] {
        drop_column_if_exists(manager, "task_run_execution", column).await?;
    }
    Ok(())
}

async fn add_old_columns_as_nullable(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    add_string_column_if_missing(manager, "thread_lineage", "child_turn_id").await?;
    add_string_column_if_missing(manager, "thread_lineage", "parent_turn_id").await?;
    add_string_column_if_missing(manager, "thread_lineage", "task_id").await?;
    add_string_column_if_missing(manager, "thread_lineage", "task_run_id").await?;
    add_string_column_if_missing(manager, "task_run_execution", "child_thread_id").await?;
    add_string_column_if_missing(manager, "task_run_execution", "child_turn_id").await?;
    Ok(())
}

async fn recreate_old_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    create_index_if_columns_exist(
        manager,
        "uidx_thread_lineage_task_run",
        "thread_lineage",
        &["task_run_id"],
        true,
    )
    .await?;
    create_index_if_columns_exist(
        manager,
        "idx_thread_lineage_task",
        "thread_lineage",
        &["task_id"],
        false,
    )
    .await?;
    create_index_if_columns_exist(
        manager,
        "idx_thread_lineage_run",
        "thread_lineage",
        &["task_run_id"],
        false,
    )
    .await?;
    create_index_if_columns_exist(
        manager,
        "uidx_task_run_execution_child_thread",
        "task_run_execution",
        &["child_thread_id"],
        true,
    )
    .await?;
    create_index_if_columns_exist(
        manager,
        "uidx_task_run_execution_child_turn",
        "task_run_execution",
        &["child_turn_id"],
        true,
    )
    .await
}

async fn ensure_zero(manager: &SchemaManager<'_>, label: &str, sql: &str) -> Result<(), DbErr> {
    let count = query_count(manager, sql).await?;
    if count == 0 {
        return Ok(());
    }
    Err(DbErr::Custom(format!(
        "cannot drop legacy task runtime fields: {label}: {count}"
    )))
}

async fn query_count(manager: &SchemaManager<'_>, sql: &str) -> Result<i64, DbErr> {
    let row = manager
        .get_connection()
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql.to_owned()))
        .await?;
    let Some(row) = row else {
        return Ok(0);
    };
    row.try_get::<i64>("", "count")
        .map_err(|error| DbErr::Custom(error.to_string()))
}

async fn drop_column_if_exists(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
) -> Result<(), DbErr> {
    if !manager.has_column(table, column).await? {
        return Ok(());
    }
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(table))
                .drop_column(Alias::new(column))
                .to_owned(),
        )
        .await
}

async fn add_string_column_if_missing(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
) -> Result<(), DbErr> {
    if manager.has_column(table, column).await? {
        return Ok(());
    }
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(table))
                .add_column(ColumnDef::new(Alias::new(column)).string_len(21).null())
                .to_owned(),
        )
        .await
}

async fn create_index_if_columns_exist(
    manager: &SchemaManager<'_>,
    name: &str,
    table: &str,
    columns: &[&str],
    unique: bool,
) -> Result<(), DbErr> {
    for column in columns {
        if !manager.has_column(table, *column).await? {
            return Ok(());
        }
    }
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
