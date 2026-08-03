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

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm_migration::sea_orm::{ConnectionTrait, Database, Statement};

    #[tokio::test]
    async fn migration_creates_schema_without_backfilling_historical_events() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        let manager = SchemaManager::new(&connection);
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("task"))
                    .col(string("id").string_len(21).primary_key())
                    .to_owned(),
            )
            .await
            .expect("minimal task table should create");
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("task_event"))
                    .col(string("id").string_len(21).primary_key())
                    .col(string("task_id").string_len(21))
                    .col(big_integer("sequence"))
                    .to_owned(),
            )
            .await
            .expect("minimal task event table should create");
        connection
            .execute_unprepared("INSERT INTO task (id) VALUES ('task_existing')")
            .await
            .expect("historical task should insert");
        connection
            .execute_unprepared(
                "INSERT INTO task_event (id, task_id, sequence) \
                 VALUES ('event_existing', 'task_existing', 1)",
            )
            .await
            .expect("historical task event should insert");

        Migration
            .up(&manager)
            .await
            .expect("fanout cursor schema migration should succeed");

        let row = connection
            .query_one_raw(Statement::from_string(
                connection.get_database_backend(),
                "SELECT COUNT(*) AS cursor_count FROM task_event_fanout_cursor".to_owned(),
            ))
            .await
            .expect("cursor count should query")
            .expect("cursor count row should exist");
        assert_eq!(
            row.try_get::<i64>("", "cursor_count")
                .expect("cursor count should decode"),
            0,
            "historical data belongs to the non-blocking Gateway backfill"
        );
    }
}
