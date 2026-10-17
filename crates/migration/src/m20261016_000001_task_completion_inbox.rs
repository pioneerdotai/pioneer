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

#[derive(DeriveMigrationName)]
pub struct Migration;

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
        let source = Query::select()
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
                    .add(
                        Expr::col((TaskRun::Table, TaskRun::Id))
                            .equals((UserNotificationOutbox::Table, UserNotificationOutbox::RunId)),
                    )
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
            .map_err(|error| DbErr::Custom(format!("invalid inbox backfill projection: {error}")))?
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
