use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::{prelude::*, schema::*};

const CHECKPOINT_TABLE: &str = "read_model_repair_checkpoint";
const DIRTY_TABLE: &str = "read_model_repair_dirty_turn_item";
const SEQUENCE_TABLE: &str = "read_model_repair_change_sequence";
const TURN_ITEM: &str = "turn_item";
const TURN_ITEM_ZSTD: &str = "_turn_item_zstd";

const INSERT_TRIGGER: &str = "read_model_repair_turn_item_insert";
const UPDATE_TRIGGER: &str = "read_model_repair_turn_item_update";
const DELETE_TRIGGER: &str = "read_model_repair_turn_item_delete";
const DIRTY_GENERATION_INDEX: &str = "idx_read_model_repair_dirty_generation";

#[derive(DeriveMigrationName)]
pub struct Migration;

fn quoted(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

async fn sqlite_object_type(
    manager: &SchemaManager<'_>,
    name: &str,
) -> Result<Option<String>, DbErr> {
    manager
        .get_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT type FROM sqlite_master WHERE name = ? LIMIT 1",
            [name.to_owned().into()],
        ))
        .await?
        .map(|row| row.try_get::<String>("", "type"))
        .transpose()
}

async fn turn_item_storage_table(manager: &SchemaManager<'_>) -> Result<&'static str, DbErr> {
    if sqlite_object_type(manager, TURN_ITEM_ZSTD)
        .await?
        .as_deref()
        == Some("table")
    {
        return Ok(TURN_ITEM_ZSTD);
    }
    if sqlite_object_type(manager, TURN_ITEM).await?.as_deref() == Some("table") {
        return Ok(TURN_ITEM);
    }
    Err(DbErr::Migration(
        "read-model repair migration could not find turn_item storage table".to_owned(),
    ))
}

async fn create_change_trigger(
    manager: &SchemaManager<'_>,
    trigger_name: &str,
    event: &str,
    storage_table: &str,
    row_prefix: &str,
    when_clause: Option<&str>,
) -> Result<(), DbErr> {
    let connection = manager.get_connection();
    let when_clause = when_clause
        .map(|condition| format!("WHEN {condition}"))
        .unwrap_or_default();
    connection
        .execute_unprepared(
            format!(
                r#"
                CREATE TRIGGER IF NOT EXISTS {trigger}
                AFTER {event} ON {storage}
                FOR EACH ROW
                {when_clause}
                BEGIN
                    UPDATE {sequence}
                    SET generation = generation + 1
                    WHERE singleton_key = 1;

                    INSERT INTO {dirty} (turn_item_id, generation, changed_at)
                    SELECT {row_prefix}.id, generation, CURRENT_TIMESTAMP
                    FROM {sequence}
                    WHERE singleton_key = 1
                    ON CONFLICT(turn_item_id) DO UPDATE SET
                        generation = excluded.generation,
                        changed_at = excluded.changed_at;
                END
                "#,
                trigger = quoted(trigger_name),
                storage = quoted(storage_table),
                sequence = quoted(SEQUENCE_TABLE),
                dirty = quoted(DIRTY_TABLE),
            )
            .as_str(),
        )
        .await?;
    Ok(())
}

async fn create_delete_trigger(
    manager: &SchemaManager<'_>,
    storage_table: &str,
) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            format!(
                r#"
                CREATE TRIGGER IF NOT EXISTS {trigger}
                AFTER DELETE ON {storage}
                FOR EACH ROW
                BEGIN
                    DELETE FROM {dirty} WHERE turn_item_id = OLD.id;
                END
                "#,
                trigger = quoted(DELETE_TRIGGER),
                storage = quoted(storage_table),
                dirty = quoted(DIRTY_TABLE),
            )
            .as_str(),
        )
        .await?;
    Ok(())
}

async fn create_repair_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(CHECKPOINT_TABLE))
                .if_not_exists()
                .col(text("repair_key").primary_key())
                .col(integer("algorithm_version"))
                .col(text("full_scan_status"))
                .col(text("full_scan_cursor_id").null())
                .col(text("full_scan_high_watermark_id").null())
                .col(text("incremental_status"))
                .col(big_integer("incremental_high_watermark_generation").null())
                .col(big_integer("incremental_cursor_generation").null())
                .col(big_integer("last_completed_generation").default(0))
                .col(timestamp_with_time_zone("full_scan_started_at").null())
                .col(timestamp_with_time_zone("full_scan_completed_at").null())
                .col(timestamp_with_time_zone("incremental_started_at").null())
                .col(timestamp_with_time_zone("incremental_completed_at").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .check((
                    "ck_read_model_repair_checkpoint_full_scan_status",
                    Expr::cust("full_scan_status IN ('running', 'completed')"),
                ))
                .check((
                    "ck_read_model_repair_checkpoint_incremental_status",
                    Expr::cust("incremental_status IN ('running', 'completed')"),
                ))
                .check((
                    "ck_read_model_repair_checkpoint_last_completed_generation",
                    Expr::cust("last_completed_generation >= 0"),
                ))
                .to_owned(),
        )
        .await?;

    manager
        .create_table(
            Table::create()
                .table(Alias::new(SEQUENCE_TABLE))
                .if_not_exists()
                .col(integer("singleton_key").primary_key())
                .col(big_integer("generation"))
                .check((
                    "ck_read_model_repair_change_sequence_singleton",
                    Expr::cust("singleton_key = 1"),
                ))
                .check((
                    "ck_read_model_repair_change_sequence_generation",
                    Expr::cust("generation >= 0"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .exec_stmt(
            Query::insert()
                .into_table(Alias::new(SEQUENCE_TABLE))
                .columns([Alias::new("singleton_key"), Alias::new("generation")])
                .values_panic([1.into(), 0.into()])
                .on_conflict(
                    OnConflict::column(Alias::new("singleton_key"))
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;

    manager
        .create_table(
            Table::create()
                .table(Alias::new(DIRTY_TABLE))
                .if_not_exists()
                .col(text("turn_item_id").primary_key())
                .col(big_integer("generation"))
                .col(timestamp_with_time_zone("changed_at").default(Expr::current_timestamp()))
                .check((
                    "ck_read_model_repair_dirty_generation",
                    Expr::cust("generation > 0"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .name(DIRTY_GENERATION_INDEX)
                .table(Alias::new(DIRTY_TABLE))
                .col(Alias::new("generation"))
                .col(Alias::new("turn_item_id"))
                .if_not_exists()
                .to_owned(),
        )
        .await
}

async fn drop_table(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    manager
        .drop_table(
            Table::drop()
                .table(Alias::new(table))
                .if_exists()
                .to_owned(),
        )
        .await
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() != DatabaseBackend::Sqlite {
            return Err(DbErr::Migration(
                "incremental read-model repair requires SQLite".to_owned(),
            ));
        }

        create_repair_schema(manager).await?;

        let storage_table = turn_item_storage_table(manager).await?;
        create_change_trigger(
            manager,
            INSERT_TRIGGER,
            "INSERT",
            storage_table,
            "NEW",
            Some("NEW.status IN ('completed', 'failed', 'timed_out', 'cancelled')"),
        )
        .await?;
        create_change_trigger(
            manager,
            UPDATE_TRIGGER,
            "UPDATE OF status, payload",
            storage_table,
            "NEW",
            Some(
                "(OLD.status IN ('completed', 'failed', 'timed_out', 'cancelled') OR \
                   NEW.status IN ('completed', 'failed', 'timed_out', 'cancelled')) AND \
                 (OLD.status IS NOT NEW.status OR \
                  (typeof(NEW.payload) = 'text' AND OLD.payload IS NOT NEW.payload))",
            ),
        )
        .await?;
        create_delete_trigger(manager, storage_table).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        for trigger in [INSERT_TRIGGER, UPDATE_TRIGGER, DELETE_TRIGGER] {
            connection
                .execute_unprepared(format!("DROP TRIGGER IF EXISTS {}", quoted(trigger)).as_str())
                .await?;
        }
        manager
            .drop_index(
                Index::drop()
                    .name(DIRTY_GENERATION_INDEX)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        drop_table(manager, DIRTY_TABLE).await?;
        drop_table(manager, SEQUENCE_TABLE).await?;
        drop_table(manager, CHECKPOINT_TABLE).await?;
        Ok(())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}
