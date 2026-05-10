use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(HookRun::Table)
                    .add_column(ColumnDef::new(HookRun::ResumeStateJson).text().null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_hook_run_status_queued_at")
                    .table(HookRun::Table)
                    .col(HookRun::Status)
                    .col(HookRun::QueuedAt)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_hook_run_status_queued_at")
                    .table(HookRun::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(HookRun::Table)
                    .drop_column(HookRun::ResumeStateJson)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum HookRun {
    Table,
    Status,
    QueuedAt,
    ResumeStateJson,
}
