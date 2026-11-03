//! Installs only the empty work set and two durable bootstrap locators.
use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_query::{Expr, Query};
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        let mut table = Table::create();
        table.table("html_preview_io_work");
        for name in [
            "family",
            "object_id",
            "incarnation",
            "requested_revision",
            "pass_revision",
            "state",
        ] {
            table.col(ColumnDef::new(name).string_len(255).not_null());
        }
        table.col(ColumnDef::new("workspace_id").text().not_null());
        table.col(ColumnDef::new("ordinal").big_integer().not_null());
        table.col(ColumnDef::new("due_at").big_integer());
        table.col(ColumnDef::new("claim_token").string_len(255));
        table.col(ColumnDef::new("cursor_id").text());
        table.col(ColumnDef::new("cursor_ordinal").big_integer());
        table.col(ColumnDef::new("upper_id").text());
        table.col(ColumnDef::new("upper_ordinal").big_integer());
        table.col(
            ColumnDef::new("seed_version")
                .big_integer()
                .not_null()
                .default(0),
        );
        table.col(ColumnDef::new("error_class").string_len(255));
        table.check(Expr::col("family").is_in([
            "witness",
            "copies",
            "seed_witness",
            "seed_copies",
        ]));
        table.check(Expr::col("state").is_in([
            "ready",
            "timed",
            "running",
            "blocked",
            "done",
            "quarantine",
        ]));
        table.check(
            Expr::col("state")
                .is_in(["ready", "timed", "running"])
                .and(Expr::col("due_at").is_not_null())
                .or(Expr::col("state")
                    .is_in(["blocked", "done", "quarantine"])
                    .and(Expr::col("due_at").is_null())),
        );
        table.primary_key(
            Index::create()
                .col("family")
                .col("object_id")
                .col("ordinal"),
        );
        m.create_table(table.to_owned()).await?;
        m.create_index(
            Index::create()
                .name("idx_html_preview_io_due")
                .table("html_preview_io_work")
                .col("due_at")
                .col("family")
                .col("object_id")
                .col("ordinal")
                .to_owned(),
        )
        .await?;
        for family in ["seed_witness", "seed_copies"] {
            m.get_connection()
                .execute(
                    &Query::insert()
                        .into_table("html_preview_io_work")
                        .columns([
                            "family",
                            "object_id",
                            "ordinal",
                            "workspace_id",
                            "incarnation",
                            "requested_revision",
                            "pass_revision",
                            "state",
                            "due_at",
                        ])
                        .values_panic([
                            family.into(),
                            "".into(),
                            0_i64.into(),
                            "".into(),
                            family.into(),
                            "initial".into(),
                            "".into(),
                            "ready".into(),
                            0_i64.into(),
                        ])
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        m.drop_table(Table::drop().table("html_preview_io_work").to_owned())
            .await
    }
}
