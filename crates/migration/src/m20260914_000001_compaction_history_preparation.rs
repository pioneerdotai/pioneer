use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Schema only: never scan existing history inside the migration. The
        // bounded maintenance worker registers it after startup, while an
        // operation that needs an unfinished history can safely help it along.
        let db = manager.get_connection();
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_history_preparation"))
                    .col(text("thread_id").primary_key())
                    .col(text("turn_id").null())
                    .col(big_integer("upper_turn_rowid").default(0))
                    .col(big_integer("turn_rowid").default(0))
                    .col(big_integer("source_kind").default(0))
                    .col(big_integer("after_sequence").default(-1))
                    .col(big_integer("step").default(0))
                    .col(big_integer("inserted").default(0))
                    .col(big_integer("input_order").default(0))
                    .col(big_integer("event_order").default(0))
                    .col(big_integer("context_order").default(0))
                    .col(big_integer("ready").default(0))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_history_preparation"),
                                Alias::new("thread_id"),
                            )
                            .to(Alias::new("thread"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        // SeaQuery has no SQLite CREATE TRIGGER builder; only trigger DDL is raw.
        // New threads have no legacy history; their sources are covered by the
        // existing revision triggers. This migration does not scan or mark old
        // threads ready; the maintenance worker discovers them incrementally.
        db.execute_unprepared(
            "CREATE TRIGGER compaction_new_thread_prepared AFTER INSERT ON thread
             BEGIN INSERT INTO compaction_history_preparation(thread_id,ready)
             VALUES(NEW.id,1); END",
        )
        .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP TRIGGER compaction_new_thread_prepared")
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("compaction_history_preparation"))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
