use sea_orm_migration::{
    prelude::*,
    schema::{big_integer, string, text, timestamp_with_time_zone},
};

const TURN: &str = "turn";
const THREAD: &str = "thread";
const GATEWAY_PRINCIPAL: &str = "gateway_principal";
const TURN_MESSAGE_REVISION: &str = "turn_message_revision";
const THREAD_READ_CURSOR: &str = "thread_read_cursor";

const TURN_COLUMNS: &[&str] = &[
    "send_mode",
    "author_display_name_snapshot",
    "author_nickname_snapshot",
    "author_avatar_revision_snapshot",
    "reply_to_turn_id",
    "mentions_json",
    "message_revision",
    "message_deleted_at",
    "message_deleted_by_actor_id",
    "message_deleted_by_actor_kind",
];

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        add_turn_columns(manager).await?;
        create_turn_indexes(manager).await?;
        create_turn_message_revision(manager).await?;
        create_thread_read_cursor(manager).await?;
        create_thread_read_cursor_index(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        ensure_no_epic6_turns(manager).await?;
        ensure_table_empty(manager, THREAD_READ_CURSOR).await?;
        ensure_table_empty(manager, TURN_MESSAGE_REVISION).await?;
        drop_index(manager, "idx_thread_read_cursor_thread_sort").await?;
        drop_table(manager, THREAD_READ_CURSOR).await?;
        drop_table(manager, TURN_MESSAGE_REVISION).await?;
        drop_index(manager, "idx_turn_thread_message_created").await?;
        drop_index(manager, "idx_turn_thread_reply").await?;
        drop_index(manager, "idx_turn_thread_created_id").await?;
        drop_turn_columns(manager).await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn add_turn_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let columns = [
        string("send_mode")
            .string_len(16)
            .null()
            .check((
                "ck_turn_send_mode",
                Expr::cust("send_mode IS NULL OR send_mode IN ('message', 'chat', 'agent')"),
            ))
            .to_owned(),
        text("author_display_name_snapshot")
            .null()
            .check((
                "ck_turn_author_display_name_snapshot",
                Expr::cust(
                    "author_display_name_snapshot IS NULL OR \
                     length(author_display_name_snapshot) BETWEEN 1 AND 512",
                ),
            ))
            .to_owned(),
        string("author_nickname_snapshot")
            .string_len(32)
            .null()
            .check((
                "ck_turn_author_nickname_snapshot",
                Expr::cust(
                    "author_nickname_snapshot IS NULL OR \
                     length(author_nickname_snapshot) BETWEEN 2 AND 32",
                ),
            ))
            .to_owned(),
        string("author_avatar_revision_snapshot")
            .string_len(64)
            .null()
            .to_owned(),
        string("reply_to_turn_id")
            .string_len(21)
            .null()
            .extra("REFERENCES turn(id) ON UPDATE NO ACTION ON DELETE RESTRICT")
            .to_owned(),
        text("mentions_json")
            .not_null()
            .default("[]")
            .check((
                "ck_turn_mentions_json",
                Expr::cust("length(mentions_json) <= 65536"),
            ))
            .to_owned(),
        big_integer("message_revision")
            .not_null()
            .default(0)
            .check((
                "ck_turn_message_revision",
                Expr::cust("message_revision >= 0"),
            ))
            .to_owned(),
        timestamp_with_time_zone("message_deleted_at")
            .null()
            .to_owned(),
        string("message_deleted_by_actor_id")
            .string_len(21)
            .null()
            .to_owned(),
        string("message_deleted_by_actor_kind")
            .string_len(32)
            .null()
            .check((
                "ck_turn_message_deleted_actor",
                Expr::cust(
                    "(message_deleted_by_actor_kind IS NULL \
                        AND message_deleted_by_actor_id IS NULL) \
                     OR (message_deleted_by_actor_kind = 'system' \
                        AND message_deleted_by_actor_id IS NULL) \
                     OR (message_deleted_by_actor_kind = 'principal' \
                        AND length(message_deleted_by_actor_id) = 21 \
                        AND message_deleted_by_actor_id NOT GLOB '*[^A-Za-z0-9]*')",
                ),
            ))
            .to_owned(),
    ];

    for column in columns {
        let name = column.get_column_name();
        if !manager.has_column(TURN, name.as_str()).await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(TURN))
                        .add_column(column)
                        .to_owned(),
                )
                .await?;
        }
    }
    Ok(())
}

async fn create_turn_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .if_not_exists()
            .name("idx_turn_thread_created_id")
            .table(Alias::new(TURN))
            .col(Alias::new("thread_id"))
            .col(Alias::new("created_at"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_turn_thread_reply")
            .table(Alias::new(TURN))
            .col(Alias::new("thread_id"))
            .col(Alias::new("reply_to_turn_id"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    manager
        .get_connection()
        .execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_turn_thread_message_created \
             ON turn (thread_id, created_at, id) WHERE send_mode = 'message'",
        )
        .await?;
    Ok(())
}

async fn create_turn_message_revision(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .if_not_exists()
                .table(Alias::new(TURN_MESSAGE_REVISION))
                .col(string("turn_id").string_len(21))
                .col(big_integer("revision"))
                .col(text("input_json"))
                .col(text("mentions_json"))
                .col(string("changed_by_actor_kind").string_len(32))
                .col(string("changed_by_actor_id").string_len(21).null())
                .col(string("change_kind").string_len(16))
                .col(timestamp_with_time_zone("created_at"))
                .primary_key(
                    Index::create()
                        .name("pk_turn_message_revision")
                        .col(Alias::new("turn_id"))
                        .col(Alias::new("revision"))
                        .primary(),
                )
                .check((
                    "ck_turn_message_revision_number",
                    Expr::cust("revision >= 0"),
                ))
                .check((
                    "ck_turn_message_revision_payload",
                    Expr::cust("length(input_json) <= 1048576 AND length(mentions_json) <= 65536"),
                ))
                .check((
                    "ck_turn_message_revision_change_kind",
                    Expr::cust("change_kind IN ('edit', 'delete')"),
                ))
                .check((
                    "ck_turn_message_revision_actor",
                    Expr::cust(
                        "(changed_by_actor_kind = 'system' AND changed_by_actor_id IS NULL) \
                         OR (changed_by_actor_kind = 'principal' \
                            AND length(changed_by_actor_id) = 21 \
                            AND changed_by_actor_id NOT GLOB '*[^A-Za-z0-9]*')",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_turn_message_revision_turn")
                        .from(Alias::new(TURN_MESSAGE_REVISION), Alias::new("turn_id"))
                        .to(Alias::new(TURN), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_thread_read_cursor(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .if_not_exists()
                .table(Alias::new(THREAD_READ_CURSOR))
                .col(string("principal_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("last_read_sort_key").string_len(128))
                .col(string("last_read_turn_id").string_len(21))
                .col(timestamp_with_time_zone("updated_at"))
                .primary_key(
                    Index::create()
                        .name("pk_thread_read_cursor")
                        .col(Alias::new("principal_id"))
                        .col(Alias::new("thread_id"))
                        .primary(),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_thread_read_cursor_principal")
                        .from(Alias::new(THREAD_READ_CURSOR), Alias::new("principal_id"))
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_thread_read_cursor_thread")
                        .from(Alias::new(THREAD_READ_CURSOR), Alias::new("thread_id"))
                        .to(Alias::new(THREAD), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_thread_read_cursor_turn")
                        .from(
                            Alias::new(THREAD_READ_CURSOR),
                            Alias::new("last_read_turn_id"),
                        )
                        .to(Alias::new(TURN), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_thread_read_cursor_index(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_thread_read_cursor_thread_sort")
                .table(Alias::new(THREAD_READ_CURSOR))
                .col(Alias::new("thread_id"))
                .col(Alias::new("last_read_sort_key"))
                .to_owned(),
        )
        .await
}

async fn drop_turn_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for column in TURN_COLUMNS.iter().rev() {
        if manager.has_column(TURN, column).await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(TURN))
                        .drop_column(Alias::new(*column))
                        .to_owned(),
                )
                .await?;
        }
    }
    Ok(())
}

async fn ensure_table_empty(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(Alias::new(table))
        .to_owned();
    let count = manager
        .get_connection()
        .query_one(&query)
        .await?
        .map(|row| row.try_get::<i64>("", "count"))
        .transpose()?
        .unwrap_or(0);
    if count == 0 {
        Ok(())
    } else {
        Err(DbErr::Custom(format!(
            "cannot roll back Epic 6 while {table} contains collaboration data"
        )))
    }
}

async fn ensure_no_epic6_turns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(Alias::new(TURN))
        .and_where(Expr::col(Alias::new("send_mode")).is_not_null())
        .to_owned();
    let count = manager
        .get_connection()
        .query_one(&query)
        .await?
        .map(|row| row.try_get::<i64>("", "count"))
        .transpose()?
        .unwrap_or(0);
    if count == 0 {
        Ok(())
    } else {
        Err(DbErr::Custom(format!(
            "cannot roll back Epic 6 while turn contains {count} collaboration row(s)"
        )))
    }
}

async fn drop_index(manager: &SchemaManager<'_>, index: &str) -> Result<(), DbErr> {
    manager
        .drop_index(Index::drop().if_exists().name(index).to_owned())
        .await
}

async fn drop_table(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    manager
        .drop_table(
            Table::drop()
                .if_exists()
                .table(Alias::new(table))
                .to_owned(),
        )
        .await
}
