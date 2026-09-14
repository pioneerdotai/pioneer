//! Schema-only migration: legacy rows remain readable. Maintenance converts
//! manifests in bounded quanta; no history scan or payload rewrite at startup.
use sea_orm_migration::sea_query::{Expr, ExprTrait, Query, UnionType};
use sea_orm_migration::{prelude::*, schema::*};
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        m.alter_table(
            Table::alter()
                .table(Alias::new("compaction_frozen_history"))
                .add_column(big_integer("storage_registered").default(0))
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("frozen_storage_discovery")
                .table(Alias::new("compaction_frozen_history"))
                .col("ready")
                .col("storage_registered")
                .col("id")
                .to_owned(),
        )
        .await?;
        for name in ["compaction_frozen_layout", "compaction_frozen_span"] {
            let mut table = Table::create();
            table
                .table(Alias::new(name))
                .col(text("manifest_id"))
                .col(big_integer("kind"));
            if name.ends_with("layout") {
                table
                    .col(big_integer("active").default(0))
                    .col(big_integer("pending").default(1))
                    .col(text("candidate").null())
                    .col(big_integer("compared").default(0))
                    .col(big_integer("copy_to").null())
                    .col(big_integer("copy_next").default(0))
                    .col(big_integer("cleanup_to").default(0))
                    .col(big_integer("cleanup_next").default(0))
                    .col(big_integer("failed").default(0))
                    .primary_key(Index::create().col("manifest_id").col("kind"));
            } else {
                table
                    .col(big_integer("start"))
                    .col(big_integer("end"))
                    .col(text("source_manifest"))
                    .primary_key(Index::create().col("manifest_id").col("kind").col("start"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(Alias::new(name), Alias::new("source_manifest"))
                            .to(Alias::new("compaction_frozen_history"), Alias::new("id")),
                    );
            }
            table.foreign_key(
                ForeignKey::create()
                    .from(Alias::new(name), Alias::new("manifest_id"))
                    .to(Alias::new("compaction_frozen_history"), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Cascade),
            );
            m.create_table(table.to_owned()).await?;
        }
        m.create_index(
            Index::create()
                .name("frozen_layout_pending")
                .table(Alias::new("compaction_frozen_layout"))
                .col("pending")
                .col("kind")
                .col("manifest_id")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("frozen_span_source")
                .table(Alias::new("compaction_frozen_span"))
                .col("source_manifest")
                .col("kind")
                .col("start")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("frozen_history_content")
                .table(Alias::new("compaction_frozen_history"))
                .col("workspace_id")
                .col("owner_thread")
                .col("ready")
                .col("identity_sha256")
                .col("imports_sha256")
                .to_owned(),
        )
        .await?;
        for (kind, name) in [
            (0, "compaction_frozen_message"),
            (1, "compaction_frozen_import"),
        ] {
            let data = format!("{name}_data");
            m.rename_table(
                Table::rename()
                    .table(Alias::new(name), Alias::new(&data))
                    .to_owned(),
            )
            .await?;
            let fields: &[&str] = if kind == 0 {
                &["ordinal", "reference_json", "bytes"]
            } else {
                &[
                    "ordinal",
                    "message_ordinal",
                    "source_scope",
                    "source_id",
                    "source_version",
                    "source_thread",
                    "proof_json",
                    "bytes",
                ]
            };
            let mut legacy = Query::select();
            legacy.column((Alias::new("d"), Alias::new("manifest_id")));
            for field in fields {
                legacy.column((Alias::new("d"), Alias::new(*field)));
            }
            legacy
                .from_as(Alias::new(&data), Alias::new("d"))
                .and_where(
                    Expr::exists(
                        Query::select()
                            .expr(Expr::val(1))
                            .from_as(Alias::new("compaction_frozen_layout"), Alias::new("l"))
                            .and_where(
                                Expr::col(("l", "manifest_id")).eq(Expr::col(("d", "manifest_id"))),
                            )
                            .and_where(Expr::col(("l", "kind")).eq(kind))
                            .and_where(Expr::col(("l", "active")).eq(1))
                            .to_owned(),
                    )
                    .not(),
                );
            let mut shared = Query::select();
            shared.column((Alias::new("s"), Alias::new("manifest_id")));
            for field in fields {
                shared.column((Alias::new("d"), Alias::new(*field)));
            }
            shared
                .from_as(Alias::new("compaction_frozen_span"), Alias::new("s"))
                .join_as(
                    JoinType::InnerJoin,
                    Alias::new(&data),
                    Alias::new("d"),
                    Expr::col(("s", "source_manifest")).eq(Expr::col(("d", "manifest_id"))),
                )
                .join_as(
                    JoinType::InnerJoin,
                    Alias::new("compaction_frozen_layout"),
                    Alias::new("l"),
                    Expr::col(("l", "manifest_id"))
                        .eq(Expr::col(("s", "manifest_id")))
                        .and(Expr::col(("l", "kind")).eq(Expr::col(("s", "kind")))),
                )
                .and_where(Expr::col(("s", "kind")).eq(kind))
                .and_where(Expr::col(("l", "active")).eq(1))
                .and_where(Expr::col(("d", "ordinal")).gte(Expr::col(("s", "start"))))
                .and_where(Expr::col(("d", "ordinal")).lt(Expr::col(("s", "end"))));
            legacy.union(UnionType::All, shared);
            // This SeaQuery/SchemaManager version has no CREATE VIEW builder.
            // Only the DDL wrapper is SQLite SQL; the SELECT is built above.
            m.get_connection()
                .execute_unprepared(&format!(
                    "CREATE VIEW \"{name}\" AS {}",
                    legacy.to_string(SqliteQueryBuilder)
                ))
                .await?;
        }
        Ok(())
    }
    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "shared frozen ranges require materialization before downgrade".into(),
        ))
    }
}
