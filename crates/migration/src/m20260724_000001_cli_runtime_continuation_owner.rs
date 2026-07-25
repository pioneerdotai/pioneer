use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("turn_cli_runtime_binding", "continuation_thread_id")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("turn_cli_runtime_binding"))
                        .add_column(
                            string("continuation_thread_id")
                                .string_len(21)
                                .not_null()
                                .default(""),
                        )
                        .to_owned(),
                )
                .await?;
        }

        manager
            .get_connection()
            .execute_unprepared(
                "UPDATE turn_cli_runtime_binding \
                 SET continuation_thread_id = thread_id \
                 WHERE continuation_thread_id = ''",
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_cli_runtime_binding_continuation")
                    .table(Alias::new("turn_cli_runtime_binding"))
                    .col(Alias::new("workspace_id"))
                    .col(Alias::new("runtime_id"))
                    .col(Alias::new("continuation_thread_id"))
                    .col(Alias::new("created_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(
                "UPDATE thread SET origin_kind = 'collaborative' \
                 WHERE origin_kind = 'user' AND sidebar_visibility = 'visible'",
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "UPDATE thread SET origin_kind = 'user' \
                 WHERE origin_kind = 'collaborative'",
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("idx_turn_cli_runtime_binding_continuation")
                    .table(Alias::new("turn_cli_runtime_binding"))
                    .to_owned(),
            )
            .await?;
        if manager
            .has_column("turn_cli_runtime_binding", "continuation_thread_id")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("turn_cli_runtime_binding"))
                        .drop_column(Alias::new("continuation_thread_id"))
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}
