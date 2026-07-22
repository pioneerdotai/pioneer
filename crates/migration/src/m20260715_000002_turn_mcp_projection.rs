use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

const LEGACY_UNRECOVERABLE: &str = "legacy_unrecoverable";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_turn_mcp_projection_table(manager).await?;
        extend_turn_mcp_binding(manager).await?;
        extend_thread_cli_runtime_binding(manager).await?;
        extend_turn_cli_runtime_binding(manager).await?;
        backfill_legacy_binding_metadata(manager).await?;
        backfill_legacy_projection_headers(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (table, columns) in [
            (
                "turn_cli_runtime_binding",
                &[
                    "mcp_projection_activation_generation",
                    "mcp_session_generation",
                    "mcp_isolation_contract_fingerprint",
                    "mcp_provider_contract_fingerprint",
                    "mcp_projection_fingerprint",
                    "mcp_manifest_hash",
                    "mcp_adapter_kind",
                ][..],
            ),
            (
                "thread_cli_runtime_binding",
                &[
                    "provider_session_last_verified_process_generation",
                    "provider_session_lifecycle_state",
                    "provider_session_id",
                    "mcp_session_generation",
                    "mcp_isolation_contract_fingerprint",
                    "mcp_provider_contract_fingerprint",
                    "mcp_projection_fingerprint",
                    "mcp_manifest_hash",
                    "mcp_adapter_kind",
                ][..],
            ),
            (
                "turn_mcp_binding",
                &[
                    "projection_activation_generation",
                    "runtime_generation",
                    "effective_timeout_ms",
                    "annotations_digest",
                    "annotations_json",
                    "provider_schema_fingerprint",
                    "canonical_schema_fingerprint",
                    "provider_callable_name",
                    "canonical_callable_name",
                ][..],
            ),
        ] {
            for column in columns {
                drop_column_if_present(manager, table, column).await?;
            }
        }

        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("turn_mcp_projection"))
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}

async fn create_turn_mcp_projection_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new("turn_mcp_projection"))
                .if_not_exists()
                .col(string("turn_id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21).not_null())
                .col(integer("projection_version").not_null())
                .col(string("manifest_hash").string_len(128).not_null())
                .col(string("resolution_status").string_len(64).not_null())
                .col(integer("tool_count").not_null())
                .col(
                    timestamp_with_time_zone("created_at")
                        .not_null()
                        .default(Expr::current_timestamp()),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_turn_mcp_projection_turn")
                        .from(Alias::new("turn_mcp_projection"), Alias::new("turn_id"))
                        .to(Alias::new("turn"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .to_owned(),
        )
        .await?;

    create_index_if_missing(
        manager,
        "idx_turn_mcp_projection_workspace_created",
        "turn_mcp_projection",
        &["workspace_id", "created_at"],
    )
    .await?;
    create_index_if_missing(
        manager,
        "idx_turn_mcp_projection_manifest",
        "turn_mcp_projection",
        &["manifest_hash"],
    )
    .await
}

async fn extend_turn_mcp_binding(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "canonical_callable_name",
        string("canonical_callable_name")
            .string_len(255)
            .not_null()
            .default(LEGACY_UNRECOVERABLE)
            .to_owned(),
    )
    .await?;
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "provider_callable_name",
        string("provider_callable_name")
            .string_len(255)
            .not_null()
            .default(LEGACY_UNRECOVERABLE)
            .to_owned(),
    )
    .await?;
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "canonical_schema_fingerprint",
        string("canonical_schema_fingerprint")
            .string_len(128)
            .not_null()
            .default(LEGACY_UNRECOVERABLE)
            .to_owned(),
    )
    .await?;
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "provider_schema_fingerprint",
        string("provider_schema_fingerprint")
            .string_len(128)
            .not_null()
            .default(LEGACY_UNRECOVERABLE)
            .to_owned(),
    )
    .await?;
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "annotations_json",
        text("annotations_json").not_null().default("{}").to_owned(),
    )
    .await?;
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "annotations_digest",
        string("annotations_digest")
            .string_len(128)
            .not_null()
            .default(LEGACY_UNRECOVERABLE)
            .to_owned(),
    )
    .await?;
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "effective_timeout_ms",
        big_integer("effective_timeout_ms")
            .not_null()
            .default(0)
            .to_owned(),
    )
    .await?;
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "runtime_generation",
        big_integer("runtime_generation")
            .not_null()
            .default(0)
            .to_owned(),
    )
    .await?;
    add_column_if_missing(
        manager,
        "turn_mcp_binding",
        "projection_activation_generation",
        big_integer("projection_activation_generation")
            .not_null()
            .default(0)
            .to_owned(),
    )
    .await
}

async fn extend_thread_cli_runtime_binding(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for column in [
        "mcp_adapter_kind",
        "mcp_manifest_hash",
        "mcp_projection_fingerprint",
        "mcp_provider_contract_fingerprint",
        "mcp_isolation_contract_fingerprint",
        "provider_session_id",
        "provider_session_lifecycle_state",
    ] {
        add_column_if_missing(
            manager,
            "thread_cli_runtime_binding",
            column,
            text(column).null().to_owned(),
        )
        .await?;
    }
    for column in [
        "mcp_session_generation",
        "provider_session_last_verified_process_generation",
    ] {
        add_column_if_missing(
            manager,
            "thread_cli_runtime_binding",
            column,
            big_integer(column).null().to_owned(),
        )
        .await?;
    }
    Ok(())
}

async fn extend_turn_cli_runtime_binding(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for column in [
        "mcp_adapter_kind",
        "mcp_manifest_hash",
        "mcp_projection_fingerprint",
        "mcp_provider_contract_fingerprint",
        "mcp_isolation_contract_fingerprint",
    ] {
        add_column_if_missing(
            manager,
            "turn_cli_runtime_binding",
            column,
            text(column).null().to_owned(),
        )
        .await?;
    }
    for column in [
        "mcp_session_generation",
        "mcp_projection_activation_generation",
    ] {
        add_column_if_missing(
            manager,
            "turn_cli_runtime_binding",
            column,
            big_integer(column).null().to_owned(),
        )
        .await?;
    }
    Ok(())
}

async fn backfill_legacy_binding_metadata(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "UPDATE turn_mcp_binding \
             SET canonical_callable_name = callable_name, \
                 provider_callable_name = callable_name \
             WHERE canonical_callable_name = 'legacy_unrecoverable' \
                OR provider_callable_name = 'legacy_unrecoverable'",
        )
        .await?;
    Ok(())
}

async fn backfill_legacy_projection_headers(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT OR IGNORE INTO turn_mcp_projection \
                (turn_id, workspace_id, projection_version, manifest_hash, resolution_status, tool_count, created_at) \
             SELECT binding.turn_id, pioneer_thread.workspace_id, 0, \
                    'legacy_unrecoverable', 'legacy_unrecoverable', COUNT(*), MIN(binding.created_at) \
             FROM turn_mcp_binding AS binding \
             INNER JOIN turn AS pioneer_turn ON pioneer_turn.id = binding.turn_id \
             INNER JOIN thread AS pioneer_thread ON pioneer_thread.id = pioneer_turn.thread_id \
             GROUP BY binding.turn_id, pioneer_thread.workspace_id",
        )
        .await?;
    Ok(())
}

async fn add_column_if_missing(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
    definition: ColumnDef,
) -> Result<(), DbErr> {
    if !manager.has_column(table, column).await? {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(table))
                    .add_column(definition)
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}

async fn drop_column_if_present(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
) -> Result<(), DbErr> {
    if manager.has_column(table, column).await? {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(table))
                    .drop_column(Alias::new(column))
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}

async fn create_index_if_missing(
    manager: &SchemaManager<'_>,
    name: &str,
    table: &str,
    columns: &[&str],
) -> Result<(), DbErr> {
    let mut statement = Index::create();
    statement
        .name(name)
        .table(Alias::new(table))
        .if_not_exists();
    for column in columns {
        statement.col(Alias::new(*column));
    }
    manager.create_index(statement.to_owned()).await
}
