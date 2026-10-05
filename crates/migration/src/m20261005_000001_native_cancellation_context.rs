use sea_orm::{ConnectionTrait, DatabaseBackend};
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;
const CONTEXT: &str = "native_cancellation_context";
const STREAM: &str = "turn_event_projection_stream_state";
const MARKERS: [&str; 3] = [
    "accepted_terminal_event_id",
    "accepted_terminal_event_type",
    "accepted_terminal_sequence",
];

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new(CONTEXT))
                    .if_not_exists()
                    .col(text("turn_id").primary_key())
                    .col(text("thread_id"))
                    .col(text("workspace_id"))
                    .col(text("execution_owner_id"))
                    .col(text("context_json"))
                    .col(text("context_sha256"))
                    .col(text("accepted_event_id").null())
                    .col(timestamp_with_time_zone("created_at"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(Alias::new(CONTEXT), Alias::new("turn_id"))
                            .to(Alias::new("turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        for name in MARKERS {
            if !manager.has_column(STREAM, name).await? {
                let column = if name == "accepted_terminal_sequence" {
                    big_integer(name).null().take()
                } else {
                    text(name).null().take()
                };
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(STREAM))
                            .add_column(column)
                            .to_owned(),
                    )
                    .await?;
            }
        }
        // Distinct cancellation IDs must coexist with immutable activated
        // Blocked obligations. Keep the PK and existing nonunique turn/batch,
        // due and completed indexes; do not add an index or rewrite any rows.
        if manager
            .has_index(
                "native_terminal_effect_outbox",
                "uidx_native_terminal_effect_turn_kind",
            )
            .await?
        {
            manager
                .drop_index(
                    Index::drop()
                        .name("uidx_native_terminal_effect_turn_kind")
                        .table(Alias::new("native_terminal_effect_outbox"))
                        .to_owned(),
                )
                .await?;
        }
        // No event index, history scan/backfill, view FK or extra worker.
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        if manager.has_table(CONTEXT).await? {
            let query = Query::select()
                .expr(Expr::val(1))
                .from(Alias::new(CONTEXT))
                .limit(1)
                .to_owned();
            if db
                .query_one_raw(DatabaseBackend::Sqlite.build(&query))
                .await?
                .is_some()
            {
                return Err(DbErr::Custom(
                    "native cancellation context contains durable data".into(),
                ));
            }
        }
        // Check every installed field, including a partially installed/corrupt marker.
        for name in MARKERS {
            if manager.has_column(STREAM, name).await? {
                let query = Query::select()
                    .expr(Expr::val(1))
                    .from(Alias::new(STREAM))
                    .and_where(Expr::col(Alias::new(name)).is_not_null())
                    .limit(1)
                    .to_owned();
                if db
                    .query_one_raw(DatabaseBackend::Sqlite.build(&query))
                    .await?
                    .is_some()
                {
                    return Err(DbErr::Custom(
                        "projection stream contains accepted terminal markers".into(),
                    ));
                }
            }
        }
        // Restore the pre-migration constraint only after durable-data guards.
        // If legacy data cannot satisfy it, SQLite rejects this downgrade and
        // the migration transaction retains the context/marker schema.
        manager
            .create_index(
                Index::create()
                    .name("uidx_native_terminal_effect_turn_kind")
                    .table(Alias::new("native_terminal_effect_outbox"))
                    .if_not_exists()
                    .col(Alias::new("turn_id"))
                    .col(Alias::new("effect_kind"))
                    .unique()
                    .to_owned(),
            )
            .await?;
        for name in MARKERS {
            if manager.has_column(STREAM, name).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(STREAM))
                            .drop_column(Alias::new(name))
                            .to_owned(),
                    )
                    .await?;
            }
        }
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(CONTEXT))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
