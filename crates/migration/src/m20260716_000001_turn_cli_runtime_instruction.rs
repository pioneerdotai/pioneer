use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("turn_cli_runtime_instruction"))
                    .if_not_exists()
                    .col(string("turn_id").string_len(21).primary_key())
                    .col(string("runtime_kind").string_len(64).not_null())
                    .col(string("transport_kind").string_len(64).not_null())
                    .col(text("instruction_text").not_null())
                    .col(string("instruction_fingerprint").string_len(64).not_null())
                    .col(text("section_ids_json").not_null())
                    .col(string("compiler_version").string_len(64).not_null())
                    .col(
                        timestamp_with_time_zone("created_at")
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_turn_cli_runtime_instruction_turn")
                            .from(
                                Alias::new("turn_cli_runtime_instruction"),
                                Alias::new("turn_id"),
                            )
                            .to(Alias::new("turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_instruction_fingerprint")
                    .table(Alias::new("turn_cli_runtime_instruction"))
                    .col(Alias::new("instruction_fingerprint"))
                    .if_not_exists()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("turn_cli_runtime_instruction"))
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
