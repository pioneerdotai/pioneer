use sea_orm_migration::sea_orm::DbBackend;
use sea_orm_migration::{prelude::*, schema::*};
#[derive(DeriveMigrationName)]
pub struct Migration;
const PENDING: &str = "task_child_reconcile_pending";
const SCOPE: &str = "task_child_reconcile_scope";
const INDEXES: &[(&str, &str, &[&str])] = &[
    (
        "idx_task_child_pending_due",
        PENDING,
        &["next_attempt_at", "turn_id"],
    ),
    (
        "idx_task_child_scope_due",
        SCOPE,
        &["next_attempt_at", "scope_kind", "scope_key"],
    ),
    (
        "idx_task_child_link_thread",
        "task_run_turn",
        &["thread_id", "id"],
    ),
    (
        "idx_task_child_creator",
        "task",
        &["created_by_thread_id", "created_by_turn_id", "id"],
    ),
    (
        "idx_task_child_accepted_source",
        "task_run_conversation_snapshot",
        &["source_turn_id", "run_id"],
    ),
    (
        "idx_task_child_parent_turn",
        "turn",
        &["thread_id", "turn_kind", "id"],
    ),
];
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Opaque identities/locators must not coalesce under a MySQL/MariaDB
        // default case-insensitive collation. Use the ordinary table builder;
        // PostgreSQL/SQLite retain their case-sensitive identity semantics.
        let identity_table = |mut table: sea_orm_migration::sea_query::TableCreateStatement| {
            if manager.get_database_backend() == DbBackend::MySql {
                table.character_set("utf8mb4").collate("utf8mb4_bin");
            }
            table
        };
        // Deadlines are signed Unix seconds, including claim expiry. NULL is
        // durable parking, never an expired lease. Tokens are opaque identities.
        manager
            .create_table(identity_table(
                Table::create()
                    .table(PENDING)
                    .col(string_len("turn_id", 255).primary_key())
                    .col(string_len("thread_id", 255))
                    .col(string_len("revision", 255))
                    .col(big_integer("next_attempt_at").null())
                    .col(big_integer("attempt_count").default(0))
                    .col(string_len("claim_token", 255).null())
                    .to_owned(),
            ))
            .await?;
        manager
            .create_table(identity_table(
                Table::create()
                    .table(SCOPE)
                    .col(string_len("scope_kind", 32))
                    .col(string_len("scope_key", 512))
                    .primary_key(Index::create().col("scope_kind").col("scope_key"))
                    .col(string_len("incarnation_token", 255))
                    .col(string_len("requested_revision", 255))
                    .col(string_len("pass_revision", 255).null())
                    .col(string_len("cursor", 512).null())
                    .col(string_len("upper_watermark", 512).null())
                    .col(big_integer("next_attempt_at").null())
                    .col(big_integer("attempt_count").default(0))
                    .col(string_len("claim_token", 255).null())
                    .to_owned(),
            ))
            .await?;
        // These four source indexes require installation scans/storage. Queue
        // indexes start empty. Existing run(task_id,id), run/sequence and group
        // attempt uniqueness are reused, not duplicated.
        for (name, table, columns) in INDEXES {
            let mut index = Index::create();
            index.name(*name).table(*table);
            for column in *columns {
                index.col(*column);
            }
            manager.create_index(index.to_owned()).await?;
        }
        // One bounded bootstrap locator; EOF retains its durable done row.
        manager
            .exec_stmt(
                Query::insert()
                    .into_table(SCOPE)
                    .columns([
                        "scope_kind",
                        "scope_key",
                        "incarnation_token",
                        "requested_revision",
                        "next_attempt_at",
                        "attempt_count",
                    ])
                    .values_panic([
                        "historical-seed".into(),
                        "v1".into(),
                        "child-seed-v1".into(),
                        "child-seed-v1".into(),
                        0i64.into(),
                        0i64.into(),
                    ])
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, table, _) in INDEXES.iter().rev() {
            manager
                .drop_index(Index::drop().name(*name).table(*table).to_owned())
                .await?;
        }
        for table in [SCOPE, PENDING] {
            manager
                .drop_table(Table::drop().table(table).to_owned())
                .await?;
        }
        Ok(())
    }
}
