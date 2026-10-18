//! Inbox migrations: one source file, unchanged released identities and order.

pub(crate) mod receipts {
    //! Personal read receipts; completed runs remain the only inbox source.
    use sea_orm_migration::{prelude::*, schema::text};

    // Historical schema identifiers must not depend on generated runtime entities.
    #[derive(DeriveIden)]
    enum TaskRunRead {
        Table,
        PrincipalId,
        RunId,
        ReadAt,
    }

    #[derive(DeriveIden)]
    enum TaskRun {
        Table,
        Id,
        TaskId,
    }

    #[derive(DeriveIden)]
    enum Task {
        Table,
        Id,
        WorkspaceId,
    }

    #[derive(DeriveIden)]
    enum Workspace {
        Table,
        Id,
    }

    #[derive(DeriveIden)]
    enum UserNotificationOutbox {
        Table,
        RecipientPrincipalId,
        RunId,
        TaskId,
        WorkspaceId,
        AcknowledgedAt,
    }

    pub struct Migration;
    impl MigrationName for Migration {
        fn name(&self) -> &str {
            "m20261016_000001_task_completion_inbox"
        }
    }

    #[async_trait::async_trait]
    impl MigrationTrait for Migration {
        fn use_transaction(&self) -> Option<bool> {
            Some(true)
        }
        async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            // Preserve the original TEXT storage, composite key and run FK. In
            // particular, read_at is an exact saved timestamp string, not epoch time.
            // The FK is descriptive: Gateway writers run with foreign_keys=OFF.
            manager
                .create_table(
                    Table::create()
                        .table(TaskRunRead::Table)
                        .col(text(TaskRunRead::PrincipalId))
                        .col(text(TaskRunRead::RunId))
                        .col(text(TaskRunRead::ReadAt))
                        .primary_key(
                            Index::create()
                                .col(TaskRunRead::PrincipalId)
                                .col(TaskRunRead::RunId)
                                .primary(),
                        )
                        .foreign_key(
                            ForeignKey::create()
                                .from(TaskRunRead::Table, TaskRunRead::RunId)
                                .to(TaskRun::Table, TaskRun::Id)
                                .on_delete(ForeignKeyAction::Cascade),
                        )
                        .to_owned(),
                )
                .await?;
            // Validate legacy provenance explicitly; FK enforcement is disabled.
            // Copying a historical read grants no access: current TaskRead/root ACL
            // and terminal state are checked by every inbox query and read mutation.
            let source =
                Query::select()
                    .column((
                        UserNotificationOutbox::Table,
                        UserNotificationOutbox::RecipientPrincipalId,
                    ))
                    .column((UserNotificationOutbox::Table, UserNotificationOutbox::RunId))
                    .expr(
                        Expr::col((
                            UserNotificationOutbox::Table,
                            UserNotificationOutbox::AcknowledgedAt,
                        ))
                        .min(),
                    )
                    .from(UserNotificationOutbox::Table)
                    .inner_join(
                        TaskRun::Table,
                        Condition::all()
                            .add(Expr::col((TaskRun::Table, TaskRun::Id)).equals((
                                UserNotificationOutbox::Table,
                                UserNotificationOutbox::RunId,
                            )))
                            .add(Expr::col((TaskRun::Table, TaskRun::TaskId)).equals((
                                UserNotificationOutbox::Table,
                                UserNotificationOutbox::TaskId,
                            ))),
                    )
                    .inner_join(
                        Task::Table,
                        Condition::all()
                            .add(
                                Expr::col((Task::Table, Task::Id))
                                    .equals((TaskRun::Table, TaskRun::TaskId)),
                            )
                            .add(Expr::col((Task::Table, Task::WorkspaceId)).equals((
                                UserNotificationOutbox::Table,
                                UserNotificationOutbox::WorkspaceId,
                            ))),
                    )
                    .inner_join(
                        Workspace::Table,
                        Expr::col((Workspace::Table, Workspace::Id))
                            .equals((Task::Table, Task::WorkspaceId)),
                    )
                    .and_where(
                        Expr::col((
                            UserNotificationOutbox::Table,
                            UserNotificationOutbox::AcknowledgedAt,
                        ))
                        .is_not_null(),
                    )
                    .group_by_columns([
                        (
                            UserNotificationOutbox::Table,
                            UserNotificationOutbox::RecipientPrincipalId,
                        ),
                        (UserNotificationOutbox::Table, UserNotificationOutbox::RunId),
                    ])
                    .to_owned();
            let backfill = Query::insert()
                .into_table(TaskRunRead::Table)
                .columns([
                    TaskRunRead::PrincipalId,
                    TaskRunRead::RunId,
                    TaskRunRead::ReadAt,
                ])
                .select_from(source)
                .map_err(|error| {
                    DbErr::Custom(format!("invalid inbox backfill projection: {error}"))
                })?
                .to_owned();
            manager.get_connection().execute(&backfill).await?;
            manager
                .create_index(
                    Index::create()
                        .name("idx_user_notification_run_read")
                        .table(UserNotificationOutbox::Table)
                        .col(UserNotificationOutbox::RecipientPrincipalId)
                        .col(UserNotificationOutbox::RunId)
                        .col(UserNotificationOutbox::WorkspaceId)
                        .col(UserNotificationOutbox::TaskId)
                        .col(UserNotificationOutbox::AcknowledgedAt)
                        .and_where(Expr::col(UserNotificationOutbox::AcknowledgedAt).is_not_null())
                        .to_owned(),
                )
                .await?;
            Ok(())
        }
        async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .drop_index(
                    Index::drop()
                        .name("idx_user_notification_run_read")
                        .table(UserNotificationOutbox::Table)
                        .to_owned(),
                )
                .await?;
            manager
                .drop_table(Table::drop().table(TaskRunRead::Table).to_owned())
                .await?;
            Ok(())
        }
    }
}

pub(crate) mod activation {
    //! Preserve the released inbox activation fact before applying unread filtering.
    use sea_orm_migration::{prelude::*, schema::*};

    #[derive(DeriveIden)]
    enum TaskInboxActivation {
        Table,
        Id,
        StartedAt,
    }
    #[derive(DeriveIden)]
    enum SeaqlMigrations {
        Table,
        Version,
        AppliedAt,
    }
    #[derive(DeriveIden)]
    enum TaskDelivery {
        Table,
        WorkspaceId,
        TargetThreadId,
        DeliveredTurnId,
        TaskId,
        RunId,
    }
    pub struct Migration;
    impl MigrationName for Migration {
        fn name(&self) -> &str {
            "m20261017_000001_task_inbox_activation"
        }
    }
    #[async_trait::async_trait]
    impl MigrationTrait for Migration {
        fn use_transaction(&self) -> Option<bool> {
            Some(true)
        }
        async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .create_table(
                    Table::create()
                        .table(TaskInboxActivation::Table)
                        .col(integer(TaskInboxActivation::Id).primary_key())
                        .col(big_integer(TaskInboxActivation::StartedAt))
                        .to_owned(),
                )
                .await?;
            // SeaORM persists applied_at as Unix seconds after each successful migration.
            // Fail closed if that durable fact is missing; never substitute repair time.
            let source = Query::select()
                .expr(Expr::val(1))
                .column(SeaqlMigrations::AppliedAt)
                .from(SeaqlMigrations::Table)
                .and_where(
                    Expr::col(SeaqlMigrations::Version)
                        .eq("m20261016_000001_task_completion_inbox"),
                )
                .to_owned();
            let insert = Query::insert()
                .into_table(TaskInboxActivation::Table)
                .columns([TaskInboxActivation::Id, TaskInboxActivation::StartedAt])
                .select_from(source)
                .map_err(|e| DbErr::Custom(e.to_string()))?
                .to_owned();
            let result = manager.get_connection().execute(&insert).await?;
            if result.rows_affected() != 1 {
                return Err(DbErr::Custom(
                    "missing original inbox migration activation fact".into(),
                ));
            }
            manager
                .create_index(
                    Index::create()
                        .name("idx_task_delivery_inbox_result")
                        .table(TaskDelivery::Table)
                        .col(TaskDelivery::WorkspaceId)
                        .col(TaskDelivery::TargetThreadId)
                        .col(TaskDelivery::DeliveredTurnId)
                        .col(TaskDelivery::TaskId)
                        .col(TaskDelivery::RunId)
                        .to_owned(),
                )
                .await?;
            Ok(())
        }
        async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .drop_index(
                    Index::drop()
                        .name("idx_task_delivery_inbox_result")
                        .table(TaskDelivery::Table)
                        .to_owned(),
                )
                .await?;
            manager
                .drop_table(Table::drop().table(TaskInboxActivation::Table).to_owned())
                .await
        }
    }
}

pub(crate) mod indexes {
    //! Narrow covering indexes for the workspace-first unread inbox scan.
    use sea_orm_migration::{
        prelude::*,
        sea_query::{ExprTrait, IndexColumn, PostgresQueryBuilder},
    };

    #[derive(DeriveIden)]
    enum Task {
        Table,
        WorkspaceId,
        Id,
    }
    #[derive(DeriveIden)]
    enum TaskRun {
        Table,
        TaskId,
        CompletedAt,
        Id,
        Status,
    }

    pub struct Migration;
    impl MigrationName for Migration {
        fn name(&self) -> &str {
            "m20261018_000002_task_inbox_query_indexes"
        }
    }
    #[async_trait::async_trait]
    impl MigrationTrait for Migration {
        fn use_transaction(&self) -> Option<bool> {
            Some(true)
        }

        async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .create_index(
                    Index::create()
                        .name("idx_task_inbox_workspace_id")
                        .table(Task::Table)
                        .col(Task::WorkspaceId)
                        .col(Task::Id)
                        .to_owned(),
                )
                .await?;
            let epoch = Func::cust(Alias::new("strftime"))
                .args([Expr::val("%s"), Expr::col(TaskRun::CompletedAt)])
                .cast_as(Alias::new("INTEGER"));
            let run_index = Index::create()
                .name("idx_task_run_inbox_epoch")
                .table(TaskRun::Table)
                .col(TaskRun::TaskId)
                .col(IndexColumn::from(epoch))
                .col(TaskRun::Id)
                .col(TaskRun::Status)
                .col(TaskRun::CompletedAt)
                .to_owned();
            // SeaQuery 1.0.2 accepts IndexColumn::Expr but its SQLite IndexBuilder
            // inherits prepare_index_columns' panic for expressions. The Postgres
            // renderer supports them and emits SQLite-compatible CREATE INDEX for
            // this restricted AST (ordinary columns and CAST/strftime only). Keep
            // the complete statement typed; no hand-written DDL/string rewriting.
            manager
                .get_connection()
                .execute_unprepared(&run_index.to_string(PostgresQueryBuilder))
                .await?;
            Ok(())
        }

        async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .drop_index(
                    Index::drop()
                        .name("idx_task_run_inbox_epoch")
                        .table(TaskRun::Table)
                        .to_owned(),
                )
                .await?;
            manager
                .drop_index(
                    Index::drop()
                        .name("idx_task_inbox_workspace_id")
                        .table(Task::Table)
                        .to_owned(),
                )
                .await
        }
    }
}

/// New state is nullable on historical rows: never stamp repair time onto access.
pub(crate) mod access_start {
    use sea_orm_migration::prelude::*;

    #[derive(DeriveIden)]
    enum Field {
        CompletionSequence,
        InboxSequence,
        InboxAfterSequence,
        InboxWorkspaceAfterSequence,
        InboxWorkspaceStartedAt,
    }
    pub struct Migration;
    impl MigrationName for Migration {
        fn name(&self) -> &str {
            "m20261018_000003_task_inbox_access_start"
        }
    }
    #[async_trait::async_trait]
    impl MigrationTrait for Migration {
        fn use_transaction(&self) -> Option<bool> {
            Some(true)
        }
        async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("task_inbox_activation"))
                        .add_column(
                            ColumnDef::new(Field::CompletionSequence)
                                .big_integer()
                                .not_null()
                                .default(0),
                        )
                        .to_owned(),
                )
                .await?;
            for (table, field) in [
                ("task_run", Field::InboxSequence),
                ("workspace_membership", Field::InboxAfterSequence),
                ("thread_membership", Field::InboxAfterSequence),
                ("gateway_principal", Field::InboxAfterSequence),
                ("thread", Field::InboxWorkspaceAfterSequence),
            ] {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(table))
                            .add_column(ColumnDef::new(field).big_integer().null())
                            .to_owned(),
                    )
                    .await?;
            }
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("thread"))
                        .add_column(
                            ColumnDef::new(Field::InboxWorkspaceStartedAt)
                                .timestamp_with_time_zone()
                                .null(),
                        )
                        .to_owned(),
                )
                .await?;
            Ok(())
        }
        async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            for (table, field) in [
                ("thread", Field::InboxWorkspaceStartedAt),
                ("thread", Field::InboxWorkspaceAfterSequence),
                ("gateway_principal", Field::InboxAfterSequence),
                ("thread_membership", Field::InboxAfterSequence),
                ("workspace_membership", Field::InboxAfterSequence),
                ("task_run", Field::InboxSequence),
                ("task_inbox_activation", Field::CompletionSequence),
            ] {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(table))
                            .drop_column(field)
                            .to_owned(),
                    )
                    .await?;
            }
            Ok(())
        }
    }
}
