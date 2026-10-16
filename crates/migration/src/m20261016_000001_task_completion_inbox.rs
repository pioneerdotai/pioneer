//! Personal read receipts; completed runs remain the only inbox source.
use pioneer_entity::{task, task_run, task_run_read, user_notification_outbox};
use sea_orm_migration::{prelude::*, schema::text};

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
        manager
            .create_table(
                Table::create()
                    .table(task_run_read::Entity)
                    .col(text(task_run_read::Column::PrincipalId))
                    .col(text(task_run_read::Column::RunId))
                    .col(text(task_run_read::Column::ReadAt))
                    .primary_key(
                        Index::create()
                            .col(task_run_read::Column::PrincipalId)
                            .col(task_run_read::Column::RunId)
                            .primary(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(task_run_read::Entity, task_run_read::Column::RunId)
                            .to(task_run::Entity, task_run::Column::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        // Exact legacy provenance: only the original recipient inherits a read.
        let source = Query::select()
            .column((
                user_notification_outbox::Entity,
                user_notification_outbox::Column::RecipientPrincipalId,
            ))
            .column((
                user_notification_outbox::Entity,
                user_notification_outbox::Column::RunId,
            ))
            .expr(
                Expr::col((
                    user_notification_outbox::Entity,
                    user_notification_outbox::Column::AcknowledgedAt,
                ))
                .min(),
            )
            .from(user_notification_outbox::Entity)
            .inner_join(
                task_run::Entity,
                Condition::all()
                    .add(Expr::col((task_run::Entity, task_run::Column::Id)).equals((
                        user_notification_outbox::Entity,
                        user_notification_outbox::Column::RunId,
                    )))
                    .add(
                        Expr::col((task_run::Entity, task_run::Column::TaskId)).equals((
                            user_notification_outbox::Entity,
                            user_notification_outbox::Column::TaskId,
                        )),
                    ),
            )
            .inner_join(
                task::Entity,
                Condition::all()
                    .add(
                        Expr::col((task::Entity, task::Column::Id))
                            .equals((task_run::Entity, task_run::Column::TaskId)),
                    )
                    .add(
                        Expr::col((task::Entity, task::Column::WorkspaceId)).equals((
                            user_notification_outbox::Entity,
                            user_notification_outbox::Column::WorkspaceId,
                        )),
                    ),
            )
            .and_where(
                Expr::col((
                    user_notification_outbox::Entity,
                    user_notification_outbox::Column::AcknowledgedAt,
                ))
                .is_not_null(),
            )
            .group_by_columns([
                (
                    user_notification_outbox::Entity,
                    user_notification_outbox::Column::RecipientPrincipalId,
                ),
                (
                    user_notification_outbox::Entity,
                    user_notification_outbox::Column::RunId,
                ),
            ])
            .to_owned();
        let backfill = Query::insert()
            .into_table(task_run_read::Entity)
            .columns([
                task_run_read::Column::PrincipalId,
                task_run_read::Column::RunId,
                task_run_read::Column::ReadAt,
            ])
            .select_from(source)
            .map_err(|error| DbErr::Custom(format!("invalid inbox backfill projection: {error}")))?
            .to_owned();
        manager.get_connection().execute(&backfill).await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_user_notification_run_read")
                    .table(user_notification_outbox::Entity)
                    .col(user_notification_outbox::Column::RecipientPrincipalId)
                    .col(user_notification_outbox::Column::RunId)
                    .col(user_notification_outbox::Column::WorkspaceId)
                    .col(user_notification_outbox::Column::TaskId)
                    .col(user_notification_outbox::Column::AcknowledgedAt)
                    .and_where(
                        Expr::col(user_notification_outbox::Column::AcknowledgedAt).is_not_null(),
                    )
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
                    .table(user_notification_outbox::Entity)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(task_run_read::Entity).to_owned())
            .await?;
        Ok(())
    }
}
