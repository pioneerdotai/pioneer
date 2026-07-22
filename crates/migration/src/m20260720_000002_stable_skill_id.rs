use crate::stable_skill_id::LEGACY_RELATION_TABLES;
use sea_orm_migration::{
    prelude::*,
    schema::{boolean, string, text, timestamp_with_time_zone},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        tracing::info!(
            migration = "stable_skill_id",
            "stable SkillId schema migration started"
        );

        drop_conflicting_legacy_indexes(manager).await?;
        for (canonical, legacy) in LEGACY_RELATION_TABLES {
            manager
                .rename_table(
                    Table::rename()
                        .table(Alias::new(canonical), Alias::new(legacy))
                        .to_owned(),
                )
                .await?;
        }
        create_stable_tables(manager).await?;
        create_stable_indexes(manager).await?;

        tracing::info!(
            migration = "stable_skill_id",
            "stable SkillId schema migration completed; data backfill is deferred"
        );
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (canonical, _) in LEGACY_RELATION_TABLES.into_iter().rev() {
            manager
                .drop_table(
                    Table::drop()
                        .table(Alias::new(canonical))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }
        for (_, legacy) in LEGACY_RELATION_TABLES.into_iter().rev() {
            manager
                .drop_table(
                    Table::drop()
                        .table(Alias::new(legacy))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }

        create_legacy_tables(manager).await?;
        create_legacy_indexes(manager).await?;
        Ok(())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn drop_conflicting_legacy_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for name in [
        "idx_turn_skill_binding_turn_id",
        "idx_skill_workspace_policy_workspace",
    ] {
        manager
            .drop_index(Index::drop().name(name).if_exists().to_owned())
            .await?;
    }
    Ok(())
}

async fn create_stable_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("skill_installation")
                .col(string("id").string_len(21).primary_key())
                .col(string("owner").string_len(255).null())
                .col(string("slug").string_len(255))
                .col(string("version").string_len(64).null())
                .col(string("source_kind").string_len(32))
                .col(string("scope_key").string_len(255))
                .col(text("source_ref"))
                .col(text("install_path"))
                .col(string("trust_level").string_len(32))
                .col(string("fingerprint").string_len(128))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_workspace_policy")
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("skill_id").string_len(21))
                .col(boolean("enabled").null())
                .col(boolean("allow_implicit_invocation").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("turn_skill_binding")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21))
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("skill_version").string_len(64).null())
                .col(string("fingerprint").string_len(128))
                .col(string("source_kind").string_len(32))
                .col(string("resolved_reason").string_len(32))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_audit_event")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(string("action").string_len(64))
                .col(string("decision").string_len(32))
                .col(string("reason_code").string_len(128).null())
                .col(text("details_json").default("{}"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_dependency_snapshot")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(text("diagnostics_json").default("[]"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

async fn create_stable_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for statement in [
        Index::create()
            .name("idx_turn_skill_binding_turn_id")
            .table("turn_skill_binding")
            .col("turn_id")
            .to_owned(),
        Index::create()
            .name("uq_turn_skill_binding_turn_id_skill_id")
            .table("turn_skill_binding")
            .col("turn_id")
            .col("skill_id")
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_skill_audit_event_skill_id_created_at")
            .table("skill_audit_event")
            .col("skill_id")
            .col("created_at")
            .to_owned(),
        Index::create()
            .name("idx_skill_audit_event_turn_id")
            .table("skill_audit_event")
            .col("turn_id")
            .to_owned(),
        Index::create()
            .name("idx_skill_dependency_snapshot_skill_id_created_at")
            .table("skill_dependency_snapshot")
            .col("skill_id")
            .col("created_at")
            .to_owned(),
        Index::create()
            .name("idx_skill_dependency_snapshot_turn_id")
            .table("skill_dependency_snapshot")
            .col("turn_id")
            .to_owned(),
        Index::create()
            .name("idx_skill_installation_scope_source")
            .table("skill_installation")
            .col("scope_key")
            .col("source_kind")
            .to_owned(),
        Index::create()
            .name("idx_skill_installation_source_ref")
            .table("skill_installation")
            .col("source_ref")
            .to_owned(),
        Index::create()
            .name("idx_skill_workspace_policy_workspace")
            .table("skill_workspace_policy")
            .col("workspace_id")
            .to_owned(),
        Index::create()
            .name("uq_skill_workspace_policy_workspace_skill_id")
            .table("skill_workspace_policy")
            .col("workspace_id")
            .col("skill_id")
            .unique()
            .to_owned(),
    ] {
        manager.create_index(statement).await?;
    }
    Ok(())
}

async fn create_legacy_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("turn_skill_binding")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21))
                .col(string("skill_slug").string_len(255))
                .col(string("skill_version").string_len(64).null())
                .col(string("fingerprint").string_len(128))
                .col(string("source_kind").string_len(32))
                .col(string("resolved_reason").string_len(32))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_installation")
                .col(string("id").string_len(21).primary_key())
                .col(string("slug").string_len(255))
                .col(string("version").string_len(64).null())
                .col(string("source_kind").string_len(32))
                .col(string("scope_key").string_len(255))
                .col(text("source_ref"))
                .col(text("install_path"))
                .col(string("trust_level").string_len(32))
                .col(string("fingerprint").string_len(128))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_audit_event")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(string("action").string_len(64))
                .col(string("decision").string_len(32))
                .col(string("reason_code").string_len(128).null())
                .col(text("details_json").default("{}"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_dependency_snapshot")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(text("diagnostics_json").default("[]"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_workspace_policy")
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(boolean("enabled").null())
                .col(boolean("allow_implicit_invocation").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

async fn create_legacy_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for statement in [
        Index::create()
            .name("idx_turn_skill_binding_turn_id")
            .table("turn_skill_binding")
            .col("turn_id")
            .to_owned(),
        Index::create()
            .name("uq_turn_skill_binding_turn_id_skill_source")
            .table("turn_skill_binding")
            .col("turn_id")
            .col("skill_slug")
            .col("source_kind")
            .unique()
            .to_owned(),
        Index::create()
            .name("uq_skill_installation_slug_source_scope")
            .table("skill_installation")
            .col("slug")
            .col("source_kind")
            .col("scope_key")
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_skill_installation_scope")
            .table("skill_installation")
            .col("scope_key")
            .to_owned(),
        Index::create()
            .name("idx_skill_audit_event_slug_created_at")
            .table("skill_audit_event")
            .col("skill_slug")
            .col("created_at")
            .to_owned(),
        Index::create()
            .name("idx_skill_audit_event_turn_skill_source")
            .table("skill_audit_event")
            .col("turn_id")
            .col("skill_slug")
            .col("source_kind")
            .to_owned(),
        Index::create()
            .name("idx_skill_dependency_snapshot_turn_skill_source")
            .table("skill_dependency_snapshot")
            .col("turn_id")
            .col("skill_slug")
            .col("source_kind")
            .to_owned(),
        Index::create()
            .name("idx_skill_workspace_policy_workspace")
            .table("skill_workspace_policy")
            .col("workspace_id")
            .to_owned(),
        Index::create()
            .name("uq_skill_workspace_policy_workspace_skill_source")
            .table("skill_workspace_policy")
            .col("workspace_id")
            .col("skill_slug")
            .col("source_kind")
            .unique()
            .to_owned(),
    ] {
        manager.create_index(statement).await?;
    }
    Ok(())
}
