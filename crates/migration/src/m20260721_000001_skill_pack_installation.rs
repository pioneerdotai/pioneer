use sea_orm_migration::{
    prelude::*,
    schema::{string, text, timestamp_with_time_zone},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

const SKILL_INSTALLATION: &str = "skill_installation";
const SKILL_INSTALLATION_BEFORE_PACKS: &str = "skill_installation_before_packs";
const SKILL_INSTALLATION_WITH_PACKS: &str = "skill_installation_with_packs";
const SKILL_PACK_INSTALLATION: &str = "skill_pack_installation";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_skill_pack_installation(manager).await?;
        rebuild_skill_installation_with_pack_membership(manager).await?;
        create_skill_pack_indexes(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        rebuild_skill_installation_without_pack_membership(manager).await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(SKILL_PACK_INSTALLATION))
                    .to_owned(),
            )
            .await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn create_skill_pack_installation(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(SKILL_PACK_INSTALLATION))
                .col(string("id").string_len(21).primary_key())
                .col(text("name"))
                .col(text("scope_key"))
                .col(text("source_kind"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .to_owned(),
        )
        .await
}

async fn rebuild_skill_installation_with_pack_membership(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    rename_table(manager, SKILL_INSTALLATION, SKILL_INSTALLATION_BEFORE_PACKS).await?;
    create_skill_installation(manager, true).await?;

    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO skill_installation (\
                 id, owner, slug, version, source_kind, scope_key, source_ref, install_path, \
                 trust_level, fingerprint, created_at, updated_at, pack_id, pack_member_key\
             ) \
             SELECT \
                 id, owner, slug, version, source_kind, scope_key, source_ref, install_path, \
                 trust_level, fingerprint, created_at, updated_at, NULL, NULL \
             FROM skill_installation_before_packs",
        )
        .await?;

    drop_table(manager, SKILL_INSTALLATION_BEFORE_PACKS).await?;
    create_base_skill_installation_indexes(manager).await
}

async fn rebuild_skill_installation_without_pack_membership(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    rename_table(manager, SKILL_INSTALLATION, SKILL_INSTALLATION_WITH_PACKS).await?;
    create_skill_installation(manager, false).await?;

    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO skill_installation (\
                 id, owner, slug, version, source_kind, scope_key, source_ref, install_path, \
                 trust_level, fingerprint, created_at, updated_at\
             ) \
             SELECT \
                 id, owner, slug, version, source_kind, scope_key, source_ref, install_path, \
                 trust_level, fingerprint, created_at, updated_at \
             FROM skill_installation_with_packs",
        )
        .await?;

    drop_table(manager, SKILL_INSTALLATION_WITH_PACKS).await?;
    create_base_skill_installation_indexes(manager).await
}

async fn create_skill_installation(
    manager: &SchemaManager<'_>,
    with_pack_membership: bool,
) -> Result<(), DbErr> {
    let mut table = Table::create();
    table
        .table(Alias::new(SKILL_INSTALLATION))
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
        .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()));

    if with_pack_membership {
        table
            .col(string("pack_id").string_len(21).null())
            .col(text("pack_member_key").null())
            .check((
                "ck_skill_installation_pack_membership",
                Expr::cust(
                    "((pack_id IS NULL AND pack_member_key IS NULL) OR \
                     (pack_id IS NOT NULL AND pack_member_key IS NOT NULL))",
                ),
            ))
            .foreign_key(
                ForeignKey::create()
                    .name("fk_skill_installation_pack")
                    .from(Alias::new(SKILL_INSTALLATION), Alias::new("pack_id"))
                    .to(Alias::new(SKILL_PACK_INSTALLATION), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Restrict),
            );
    }

    manager.create_table(table.to_owned()).await
}

async fn create_base_skill_installation_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_skill_installation_scope_source")
            .table(Alias::new(SKILL_INSTALLATION))
            .col(Alias::new("scope_key"))
            .col(Alias::new("source_kind"))
            .to_owned(),
        Index::create()
            .name("idx_skill_installation_source_ref")
            .table(Alias::new(SKILL_INSTALLATION))
            .col(Alias::new("source_ref"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

async fn create_skill_pack_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_skill_pack_installation_scope_name_id")
            .table(Alias::new(SKILL_PACK_INSTALLATION))
            .col(Alias::new("scope_key"))
            .col(Alias::new("name"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .name("idx_skill_installation_pack_member_id")
            .table(Alias::new(SKILL_INSTALLATION))
            .col(Alias::new("pack_id"))
            .col(Alias::new("pack_member_key"))
            .col(Alias::new("id"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }

    manager
        .get_connection()
        .execute_unprepared(
            "CREATE UNIQUE INDEX uq_skill_installation_pack_member_key \
             ON skill_installation (pack_id, pack_member_key) \
             WHERE pack_id IS NOT NULL",
        )
        .await?;
    Ok(())
}

async fn rename_table(manager: &SchemaManager<'_>, from: &str, to: &str) -> Result<(), DbErr> {
    manager
        .rename_table(
            Table::rename()
                .table(Alias::new(from), Alias::new(to))
                .to_owned(),
        )
        .await
}

async fn drop_table(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    manager
        .drop_table(Table::drop().table(Alias::new(table)).to_owned())
        .await
}
