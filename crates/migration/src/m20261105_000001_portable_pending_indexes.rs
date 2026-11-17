//! Rebuild only the four NOT NULL pending indexes. SQLite cannot always
//! eliminate redundant IS NOT NULL terms when matching a partial index.
//! Installation scans the owner tables and builds indexes; no payload backfill.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

async fn rebuild(m: &SchemaManager<'_>, historical: bool) -> Result<(), DbErr> {
    for (table, key, field, frozen) in [
        ("tool_output_chunk", "id", "text", false),
        (
            "turn_cli_runtime_binding",
            "turn_id",
            "input_mapping_json",
            true,
        ),
        (
            "task_run_conversation_snapshot",
            "run_id",
            "history_json",
            true,
        ),
        ("task_event", "id", "payload_json", false),
    ] {
        let name = format!("{table}_{field}_portable_pending");
        let reference = format!("{field}_payload_id");
        m.drop_index(
            Index::drop()
                .name(&name)
                .table(Alias::new(table))
                .to_owned(),
        )
        .await?;
        let mut pending = Expr::col(Alias::new(&reference)).is_null();
        if historical {
            pending = pending.and(Expr::col(Alias::new(field)).is_not_null());
        }
        if frozen {
            pending = pending.and(
                Expr::col(Alias::new("frozen_manifest_id"))
                    .is_null()
                    .or(Expr::col(Alias::new("frozen_manifest_id")).ne("")),
            );
        }
        m.create_index(
            Index::create()
                .name(&name)
                .table(Alias::new(table))
                .col(Alias::new(&reference))
                .col(Alias::new(key))
                .and_where(pending)
                .to_owned(),
        )
        .await?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        rebuild(m, false).await
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        rebuild(m, true).await
    }
}
