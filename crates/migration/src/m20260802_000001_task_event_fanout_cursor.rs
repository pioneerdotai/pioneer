use sea_orm_migration::{prelude::*, schema::*};

const TASK_EVENT_FANOUT_CURSOR: &str = "task_event_fanout_cursor";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new(TASK_EVENT_FANOUT_CURSOR))
                    .if_not_exists()
                    .col(string("task_id").string_len(21).primary_key())
                    .col(big_integer("last_sequence").default(0))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_task_event_fanout_cursor_task")
                            .from(Alias::new(TASK_EVENT_FANOUT_CURSOR), Alias::new("task_id"))
                            .to(Alias::new("task"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check((
                        "ck_task_event_fanout_cursor_sequence",
                        Expr::cust("last_sequence >= 0"),
                    ))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(TASK_EVENT_FANOUT_CURSOR))
                    .if_exists()
                    .to_owned(),
            )
            .await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}
