use sea_orm_migration::{prelude::*, schema::*};

const APPLIED_PATCH_RECORD: &str = "applied_patch_record";
const APPLIED_PATCH_CHANGE_INDEX: &str = "applied_patch_change_index";
const PATCH_SNAPSHOT: &str = "patch_snapshot";
const PATCH_COMMIT_INTENT: &str = "patch_commit_intent";
const PATCH_COMMIT_TERMINAL: &str = "patch_commit_terminal";
const PATCH_SNAPSHOT_RESERVATION: &str = "patch_snapshot_reservation";
const TURN_DIFF_STATE: &str = "turn_diff_state";
const CODEX_AGGREGATE_STATE: &str = "codex_aggregate_state";

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Durable file-history tables are deliberately append-oriented. The
/// aggregate, snapshot and intent tables are projections/coordination state;
/// the applied record table is the immutable source log.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_applied_patch_record(manager).await?;
        create_applied_patch_change_index(manager).await?;
        create_patch_snapshot(manager).await?;
        create_patch_commit_intent(manager).await?;
        create_patch_commit_terminal(manager).await?;
        create_patch_snapshot_reservation(manager).await?;
        create_turn_diff_state(manager).await?;
        create_codex_aggregate_state(manager).await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "patch history migration is irreversible".to_owned(),
        ))
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn create_applied_patch_record(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(APPLIED_PATCH_RECORD))
                .if_not_exists()
                .col(text("id").primary_key())
                .col(integer("schema_version"))
                .col(text("thread_id"))
                .col(text("turn_id"))
                .col(text("invocation_id"))
                .col(text("environment_id").default(""))
                .col(big_integer("commit_ordinal"))
                .col(text("authority").default("native_patch_engine"))
                .col(text("provenance").default("native_engine"))
                .col(text("exactness").default("uncertain"))
                .col(big_integer("committed_at_unix_ms").default(0))
                .col(binary("plan_fingerprint"))
                .col(text("outcome_json"))
                .col(text("changes_json"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;

    create_index(
        manager,
        "uq_applied_patch_record_invocation",
        APPLIED_PATCH_RECORD,
        &["thread_id", "turn_id", "invocation_id"],
        true,
    )
    .await?;
    create_index(
        manager,
        "uq_applied_patch_record_commit_ordinal",
        APPLIED_PATCH_RECORD,
        &["thread_id", "turn_id", "commit_ordinal"],
        true,
    )
    .await?;
    create_index(
        manager,
        "idx_applied_patch_record_thread_turn_ordinal",
        APPLIED_PATCH_RECORD,
        &["thread_id", "turn_id", "commit_ordinal"],
        false,
    )
    .await
}

async fn create_applied_patch_change_index(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(APPLIED_PATCH_CHANGE_INDEX))
                .if_not_exists()
                .col(text("record_id"))
                .col(text("thread_id"))
                .col(text("turn_id"))
                .col(text("invocation_id"))
                .col(text("environment_id"))
                .col(big_integer("commit_ordinal"))
                .col(integer("sequence"))
                .col(text("source_path"))
                .col(text("destination_path").null())
                .col(text("change_json"))
                .primary_key(
                    Index::create()
                        .name("pk_applied_patch_change_index")
                        .col(Alias::new("record_id"))
                        .col(Alias::new("sequence")),
                )
                .to_owned(),
        )
        .await?;

    create_index(
        manager,
        "uq_patch_change_index_commit_sequence",
        APPLIED_PATCH_CHANGE_INDEX,
        &["thread_id", "turn_id", "commit_ordinal", "sequence"],
        true,
    )
    .await?;
    create_index(
        manager,
        "idx_patch_change_index_thread_source_order",
        APPLIED_PATCH_CHANGE_INDEX,
        &[
            "thread_id",
            "source_path",
            "turn_id",
            "commit_ordinal",
            "sequence",
        ],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_patch_change_index_thread_destination_order",
        APPLIED_PATCH_CHANGE_INDEX,
        &[
            "thread_id",
            "destination_path",
            "turn_id",
            "commit_ordinal",
            "sequence",
        ],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_patch_change_index_environment_source_order",
        APPLIED_PATCH_CHANGE_INDEX,
        &[
            "thread_id",
            "environment_id",
            "source_path",
            "turn_id",
            "commit_ordinal",
            "sequence",
        ],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_patch_change_index_environment_destination_order",
        APPLIED_PATCH_CHANGE_INDEX,
        &[
            "thread_id",
            "environment_id",
            "destination_path",
            "turn_id",
            "commit_ordinal",
            "sequence",
        ],
        false,
    )
    .await
}

async fn create_patch_snapshot(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(PATCH_SNAPSHOT))
                .if_not_exists()
                .col(text("domain_id"))
                .col(binary("content_hash"))
                .col(big_integer("byte_len"))
                .col(text("encoding"))
                .col(text("line_endings_json"))
                .col(binary("compressed_bytes"))
                .col(big_integer("raw_byte_len"))
                .col(big_integer("ref_count").default(0))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .primary_key(
                    Index::create()
                        .name("pk_patch_snapshot")
                        .col(Alias::new("domain_id"))
                        .col(Alias::new("content_hash"))
                        .col(Alias::new("byte_len")),
                )
                .check((
                    "ck_patch_snapshot_raw_byte_len",
                    Expr::col(Alias::new("raw_byte_len")).gte(0),
                ))
                .check((
                    "ck_patch_snapshot_ref_count",
                    Expr::col(Alias::new("ref_count")).gte(0),
                ))
                .to_owned(),
        )
        .await
}

async fn create_patch_commit_intent(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(PATCH_COMMIT_INTENT))
                .if_not_exists()
                .col(text("thread_id"))
                .col(text("turn_id"))
                .col(text("invocation_id"))
                .col(big_integer("commit_ordinal"))
                .col(binary("plan_fingerprint"))
                .col(text("operations_json"))
                .col(text("recovery_json"))
                .col(text("progress_json"))
                .col(text("status"))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .primary_key(
                    Index::create()
                        .name("pk_patch_commit_intent")
                        .col(Alias::new("thread_id"))
                        .col(Alias::new("turn_id"))
                        .col(Alias::new("invocation_id")),
                )
                .to_owned(),
        )
        .await?;

    create_index(
        manager,
        "uq_patch_commit_intent_commit_ordinal",
        PATCH_COMMIT_INTENT,
        &["thread_id", "turn_id", "commit_ordinal"],
        true,
    )
    .await?;
    create_index(
        manager,
        "idx_patch_commit_intent_pending_order",
        PATCH_COMMIT_INTENT,
        &[
            "status",
            "updated_at",
            "thread_id",
            "turn_id",
            "invocation_id",
        ],
        false,
    )
    .await
}

async fn create_patch_commit_terminal(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(PATCH_COMMIT_TERMINAL))
                .if_not_exists()
                .col(text("thread_id"))
                .col(text("turn_id"))
                .col(text("invocation_id"))
                .col(big_integer("commit_ordinal"))
                .col(binary("plan_fingerprint"))
                .col(text("operations_json"))
                .col(text("authority"))
                .col(text("status"))
                .col(text("record_id").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .primary_key(
                    Index::create()
                        .name("pk_patch_commit_terminal")
                        .col(Alias::new("thread_id"))
                        .col(Alias::new("turn_id"))
                        .col(Alias::new("invocation_id")),
                )
                .to_owned(),
        )
        .await?;

    create_index(
        manager,
        "uq_patch_commit_terminal_commit_ordinal",
        PATCH_COMMIT_TERMINAL,
        &["thread_id", "turn_id", "commit_ordinal"],
        true,
    )
    .await
}

async fn create_patch_snapshot_reservation(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(PATCH_SNAPSHOT_RESERVATION))
                .if_not_exists()
                .col(text("thread_id"))
                .col(text("turn_id"))
                .col(text("invocation_id"))
                .col(big_integer("logical_bytes"))
                .col(big_integer("physical_bytes"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .primary_key(
                    Index::create()
                        .name("pk_patch_snapshot_reservation")
                        .col(Alias::new("thread_id"))
                        .col(Alias::new("turn_id"))
                        .col(Alias::new("invocation_id")),
                )
                .check((
                    "ck_patch_snapshot_reservation_logical_bytes",
                    Expr::col(Alias::new("logical_bytes")).gte(0),
                ))
                .check((
                    "ck_patch_snapshot_reservation_physical_bytes",
                    Expr::col(Alias::new("physical_bytes")).gte(0),
                ))
                .to_owned(),
        )
        .await
}

async fn create_turn_diff_state(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(TURN_DIFF_STATE))
                .if_not_exists()
                .col(text("thread_id"))
                .col(text("turn_id"))
                .col(big_integer("revision"))
                .col(text("authority"))
                .col(integer("exact"))
                .col(text("coverage_json"))
                .col(big_integer("applied_through_ordinal").null())
                .col(big_integer("record_count"))
                .col(integer("final_state").default(0))
                .col(text("state_json"))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .primary_key(
                    Index::create()
                        .name("pk_turn_diff_state")
                        .col(Alias::new("thread_id"))
                        .col(Alias::new("turn_id")),
                )
                .check((
                    "ck_turn_diff_state_exact",
                    Expr::col(Alias::new("exact")).is_in([0, 1]),
                ))
                .check((
                    "ck_turn_diff_state_final_state",
                    Expr::col(Alias::new("final_state")).is_in([0, 1]),
                ))
                .to_owned(),
        )
        .await
}

async fn create_codex_aggregate_state(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(CODEX_AGGREGATE_STATE))
                .if_not_exists()
                .col(text("thread_id"))
                .col(text("turn_id"))
                .col(big_integer("revision"))
                .col(integer("final_state").default(0))
                .col(text("state_json"))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .primary_key(
                    Index::create()
                        .name("pk_codex_aggregate_state")
                        .col(Alias::new("thread_id"))
                        .col(Alias::new("turn_id")),
                )
                .check((
                    "ck_codex_aggregate_state_final_state",
                    Expr::col(Alias::new("final_state")).is_in([0, 1]),
                ))
                .to_owned(),
        )
        .await
}

async fn create_index(
    manager: &SchemaManager<'_>,
    name: &str,
    table: &str,
    columns: &[&str],
    unique: bool,
) -> Result<(), DbErr> {
    let mut statement = Index::create();
    statement
        .if_not_exists()
        .name(name)
        .table(Alias::new(table));
    for column in columns {
        statement.col(Alias::new(*column));
    }
    if unique {
        statement.unique();
    }
    manager.create_index(statement.to_owned()).await
}
