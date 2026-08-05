use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::{prelude::*, schema::*};

const TURN_EVENT: &str = "turn_event";
const TURN_LLM_CONTEXT: &str = "turn_llm_context";
const TURN_EVENT_DELIVERY: &str = "turn_event_delivery";
const TURN_EVENT_ZSTD: &str = "_turn_event_zstd";

fn quoted(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

async fn sqlite_object_type(
    manager: &SchemaManager<'_>,
    name: &str,
) -> Result<Option<String>, DbErr> {
    if manager.get_database_backend() != DatabaseBackend::Sqlite {
        return Ok(None);
    }
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

async fn compressed_turn_event_is_active(manager: &SchemaManager<'_>) -> Result<bool, DbErr> {
    Ok(
        sqlite_object_type(manager, TURN_EVENT).await?.as_deref() == Some("view")
            && sqlite_object_type(manager, TURN_EVENT_ZSTD)
                .await?
                .as_deref()
                == Some("table"),
    )
}

async fn drop_compressed_turn_event_projection(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let connection = manager.get_connection();
    let trigger_rows = connection
        .query_all_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?",
            [TURN_EVENT.to_owned().into()],
        ))
        .await?;
    for row in trigger_rows {
        let name = row.try_get::<String>("", "name")?;
        connection
            .execute_unprepared(format!("DROP TRIGGER IF EXISTS {}", quoted(&name)).as_str())
            .await?;
    }
    connection
        .execute_unprepared(format!("DROP VIEW IF EXISTS {}", quoted(TURN_EVENT)).as_str())
        .await?;
    Ok(())
}

/// Recreate sqlite-zstd's public writable view after a schema change to its
/// backing table. sqlite-zstd intentionally snapshots the column list into the
/// view and INSTEAD OF triggers, so altering the backing table alone would make
/// the new column invisible and unwritable.
async fn create_compressed_turn_event_projection(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let connection = manager.get_connection();
    let rows = connection
        .query_all_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!("PRAGMA table_info({})", quoted(TURN_EVENT_ZSTD)),
        ))
        .await?;
    let mut public_columns = Vec::new();
    let mut primary_keys = Vec::new();
    for row in rows {
        let name = row.try_get::<String>("", "name")?;
        let primary_key = row.try_get::<i64>("", "pk")?;
        if name == "_payload_dict" {
            continue;
        }
        if primary_key > 0 {
            primary_keys.push((primary_key, name.clone()));
        }
        public_columns.push(name);
    }
    primary_keys.sort_by_key(|(position, _)| *position);
    if public_columns.is_empty() || primary_keys.is_empty() {
        return Err(DbErr::Migration(
            "compressed turn_event backing table has no public columns or primary key".to_owned(),
        ));
    }

    let select_columns = public_columns
        .iter()
        .map(|column| {
            if column == "payload" {
                "zstd_decompress_col(\"payload\", 1, \"_payload_dict\", 1) AS \"payload\""
                    .to_owned()
            } else {
                quoted(column)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute_unprepared(
            format!(
                "CREATE VIEW {} AS SELECT {} FROM {}",
                quoted(TURN_EVENT),
                select_columns,
                quoted(TURN_EVENT_ZSTD)
            )
            .as_str(),
        )
        .await?;

    let mut insert_columns = Vec::new();
    let mut insert_values = Vec::new();
    for column in &public_columns {
        insert_columns.push(quoted(column));
        insert_values.push(format!("NEW.{}", quoted(column)));
        if column == "payload" {
            insert_columns.push(quoted("_payload_dict"));
            insert_values.push("NULL".to_owned());
        }
    }
    connection
        .execute_unprepared(
            format!(
                "CREATE TRIGGER {} INSTEAD OF INSERT ON {} FOR EACH ROW BEGIN INSERT INTO {} ({}) VALUES ({}); END",
                quoted("turn_event_insert_trigger"),
                quoted(TURN_EVENT),
                quoted(TURN_EVENT_ZSTD),
                insert_columns.join(", "),
                insert_values.join(", ")
            )
            .as_str(),
        )
        .await?;

    let primary_key_condition = primary_keys
        .iter()
        .map(|(_, column)| format!("{} = OLD.{}", quoted(column), quoted(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    connection
        .execute_unprepared(
            format!(
                "CREATE TRIGGER {} INSTEAD OF DELETE ON {} FOR EACH ROW BEGIN DELETE FROM {} WHERE {}; END",
                quoted("turn_event_delete_trigger"),
                quoted(TURN_EVENT),
                quoted(TURN_EVENT_ZSTD),
                primary_key_condition
            )
            .as_str(),
        )
        .await?;

    for column in &public_columns {
        let update = if column == "payload" {
            "\"payload\" = NEW.\"payload\", \"_payload_dict\" = NULL".to_owned()
        } else {
            format!("{} = NEW.{}", quoted(column), quoted(column))
        };
        connection
            .execute_unprepared(
                format!(
                    "CREATE TRIGGER {} INSTEAD OF UPDATE OF {} ON {} FOR EACH ROW BEGIN UPDATE {} SET {} WHERE {}; END",
                    quoted(format!("turn_event_update_{column}_trigger").as_str()),
                    quoted(column),
                    quoted(TURN_EVENT),
                    quoted(TURN_EVENT_ZSTD),
                    update,
                    primary_key_condition
                )
                .as_str(),
            )
            .await?;
    }
    Ok(())
}

async fn alter_turn_event_for_delivery_key(
    manager: &SchemaManager<'_>,
    add: bool,
) -> Result<&'static str, DbErr> {
    if !compressed_turn_event_is_active(manager).await? {
        let mut alter = Table::alter().table(Alias::new(TURN_EVENT)).to_owned();
        if add {
            alter.add_column(string("idempotency_key").string_len(64).null());
        } else {
            alter.drop_column(Alias::new("idempotency_key"));
        }
        manager.alter_table(alter).await?;
        return Ok(TURN_EVENT);
    }

    drop_compressed_turn_event_projection(manager).await?;
    let operation = if add {
        "ADD COLUMN \"idempotency_key\" varchar(64) NULL"
    } else {
        "DROP COLUMN \"idempotency_key\""
    };
    manager
        .get_connection()
        .execute_unprepared(format!("ALTER TABLE {} {operation}", quoted(TURN_EVENT_ZSTD)).as_str())
        .await?;
    create_compressed_turn_event_projection(manager).await?;
    Ok(TURN_EVENT_ZSTD)
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let turn_event_storage = alter_turn_event_for_delivery_key(manager, true).await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_turn_event_turn_id_idempotency_key")
                    .table(Alias::new(turn_event_storage))
                    .col(Alias::new("turn_id"))
                    .col(Alias::new("idempotency_key"))
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TURN_LLM_CONTEXT))
                    .add_column(string("delivery_key").string_len(64).null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_turn_llm_context_turn_id_delivery_key")
                    .table(Alias::new(TURN_LLM_CONTEXT))
                    .col(Alias::new("turn_id"))
                    .col(Alias::new("delivery_key"))
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Alias::new(TURN_EVENT_DELIVERY))
                    .if_not_exists()
                    .col(string("id").string_len(21).primary_key())
                    .col(string("event_id").string_len(21))
                    .col(string("thread_id").string_len(21))
                    .col(string("turn_id").string_len(21))
                    .col(big_integer("sequence"))
                    .col(string("consumer").string_len(32))
                    .col(string("status").string_len(32).default("pending"))
                    .col(integer("attempt_count").default(0))
                    .col(text("last_error").null())
                    .col(timestamp_with_time_zone("next_run_at"))
                    .col(string("claim_token").string_len(21).null())
                    .col(timestamp_with_time_zone("claim_expires_at").null())
                    .col(timestamp_with_time_zone("delivered_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_turn_event_delivery_event")
                            .from(Alias::new(TURN_EVENT_DELIVERY), Alias::new("event_id"))
                            .to(Alias::new(turn_event_storage), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check((
                        "ck_turn_event_delivery_attempt_count",
                        Expr::cust("attempt_count >= 0"),
                    ))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_turn_event_delivery_event_consumer")
                    .table(Alias::new(TURN_EVENT_DELIVERY))
                    .col(Alias::new("event_id"))
                    .col(Alias::new("consumer"))
                    .unique()
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_delivery_due")
                    .table(Alias::new(TURN_EVENT_DELIVERY))
                    .col(Alias::new("consumer"))
                    .col(Alias::new("status"))
                    .col(Alias::new("next_run_at"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_delivery_turn_sequence")
                    .table(Alias::new(TURN_EVENT_DELIVERY))
                    .col(Alias::new("turn_id"))
                    .col(Alias::new("sequence"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(TURN_EVENT_DELIVERY))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("uq_turn_llm_context_turn_id_delivery_key")
                    .table(Alias::new(TURN_LLM_CONTEXT))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TURN_LLM_CONTEXT))
                    .drop_column(Alias::new("delivery_key"))
                    .to_owned(),
            )
            .await?;
        let turn_event_storage = if compressed_turn_event_is_active(manager).await? {
            TURN_EVENT_ZSTD
        } else {
            TURN_EVENT
        };
        manager
            .drop_index(
                Index::drop()
                    .name("uq_turn_event_turn_id_idempotency_key")
                    .table(Alias::new(turn_event_storage))
                    .to_owned(),
            )
            .await?;
        alter_turn_event_for_delivery_key(manager, false)
            .await
            .map(|_| ())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}
