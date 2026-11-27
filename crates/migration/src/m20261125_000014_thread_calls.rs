use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let calls = Alias::new("thread_call");
        let thread = Alias::new("thread");
        manager
            .create_table(
                Table::create()
                    .table(calls.clone())
                    .col(
                        ColumnDef::new(Alias::new("id"))
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Alias::new("thread_id")).text().not_null())
                    // Terminal calls release their thread's slot. Retain them for history and retry.
                    .col(
                        ColumnDef::new(Alias::new("active_slot"))
                            .text()
                            .unique_key(),
                    )
                    .col(
                        ColumnDef::new(Alias::new("created_at"))
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Alias::new("state")).json_binary().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_thread_call_thread")
                            .from(calls.clone(), Alias::new("thread_id"))
                            .to(thread, Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_thread_call_history")
                    .table(calls)
                    .col(Alias::new("thread_id"))
                    .col(Alias::new("created_at"))
                    .col(Alias::new("id"))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::drop_tables(manager, &["thread_call"]).await
    }
}
