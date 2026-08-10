use sea_orm_migration::{
    prelude::*,
    schema::{binary, integer, string, timestamp_with_time_zone},
};

const GATEWAY_PRINCIPAL: &str = "gateway_principal";
const PRINCIPAL_AVATAR: &str = "principal_avatar";
const PRINCIPAL_AVATAR_CURRENT: &str = "principal_avatar_current";
const PRINCIPAL_AVATAR_LEGACY: &str = "principal_avatar_legacy";
const PRINCIPAL_AVATAR_REVISION: &str = "principal_avatar_revision";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_revision_table(manager).await?;
        create_current_table(manager, PRINCIPAL_AVATAR_CURRENT).await?;

        manager
            .get_connection()
            .execute_unprepared(
                "INSERT INTO principal_avatar_revision \
                 (id, principal_id, content_hash, media_type, content, width, height, created_at) \
                 SELECT principal_id || ':' || lower(hex(content_hash)), \
                        principal_id, content_hash, media_type, content, width, height, created_at \
                 FROM principal_avatar",
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared(
                "INSERT INTO principal_avatar_current (principal_id, revision_id, updated_at) \
                 SELECT principal_id, \
                        principal_id || ':' || lower(hex(content_hash)), \
                        updated_at \
                 FROM principal_avatar",
            )
            .await?;

        manager
            .drop_table(Table::drop().table(Alias::new(PRINCIPAL_AVATAR)).to_owned())
            .await?;
        manager
            .rename_table(
                Table::rename()
                    .table(
                        Alias::new(PRINCIPAL_AVATAR_CURRENT),
                        Alias::new(PRINCIPAL_AVATAR),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_legacy_table(manager).await?;
        manager
            .get_connection()
            .execute_unprepared(
                "INSERT INTO principal_avatar_legacy \
                 (principal_id, media_type, content, content_hash, width, height, created_at, updated_at) \
                 SELECT current.principal_id, revision.media_type, revision.content, \
                        revision.content_hash, revision.width, revision.height, \
                        revision.created_at, current.updated_at \
                 FROM principal_avatar AS current \
                 INNER JOIN principal_avatar_revision AS revision \
                   ON revision.id = current.revision_id",
            )
            .await?;
        manager
            .drop_table(Table::drop().table(Alias::new(PRINCIPAL_AVATAR)).to_owned())
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(PRINCIPAL_AVATAR_REVISION))
                    .to_owned(),
            )
            .await?;
        manager
            .rename_table(
                Table::rename()
                    .table(
                        Alias::new(PRINCIPAL_AVATAR_LEGACY),
                        Alias::new(PRINCIPAL_AVATAR),
                    )
                    .to_owned(),
            )
            .await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn create_revision_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(PRINCIPAL_AVATAR_REVISION))
                .col(string("id").string_len(86).primary_key())
                .col(string("principal_id").string_len(21))
                .col(binary("content_hash"))
                .col(string("media_type").string_len(32))
                .col(binary("content"))
                .col(integer("width"))
                .col(integer("height"))
                .col(timestamp_with_time_zone("created_at"))
                .check((
                    "ck_principal_avatar_revision_id",
                    Expr::cust(
                        "length(id) = 86 \
                         AND id = principal_id || ':' || lower(hex(content_hash))",
                    ),
                ))
                .check((
                    "ck_principal_avatar_revision_principal_id",
                    Expr::cust(
                        "length(principal_id) = 21 \
                         AND principal_id NOT GLOB '*[^A-Za-z0-9]*'",
                    ),
                ))
                .check((
                    "ck_principal_avatar_revision_media_type",
                    Expr::cust("media_type IN ('image/png', 'image/jpeg', 'image/webp')"),
                ))
                .check((
                    "ck_principal_avatar_revision_content",
                    Expr::cust(
                        "length(content) BETWEEN 1 AND 262144 AND length(content_hash) = 32",
                    ),
                ))
                .check((
                    "ck_principal_avatar_revision_dimensions",
                    Expr::cust("width BETWEEN 1 AND 1024 AND height BETWEEN 1 AND 1024"),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_principal_avatar_revision_principal")
                        .from(
                            Alias::new(PRINCIPAL_AVATAR_REVISION),
                            Alias::new("principal_id"),
                        )
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .name("uidx_principal_avatar_revision_principal_hash")
                .table(Alias::new(PRINCIPAL_AVATAR_REVISION))
                .col(Alias::new("principal_id"))
                .col(Alias::new("content_hash"))
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_current_table(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(table))
                .col(string("principal_id").string_len(21).primary_key())
                .col(string("revision_id").string_len(86).unique_key())
                .col(timestamp_with_time_zone("updated_at"))
                .check((
                    "ck_principal_avatar_current_revision_id",
                    Expr::cust(
                        "length(revision_id) = 86 \
                         AND substr(revision_id, 1, 22) = principal_id || ':'",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_principal_avatar_current_revision")
                        .from(Alias::new(table), Alias::new("revision_id"))
                        .to(Alias::new(PRINCIPAL_AVATAR_REVISION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_legacy_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(PRINCIPAL_AVATAR_LEGACY))
                .col(string("principal_id").string_len(21).primary_key())
                .col(string("media_type").string_len(32))
                .col(binary("content"))
                .col(binary("content_hash"))
                .col(integer("width"))
                .col(integer("height"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .check((
                    "ck_principal_avatar_legacy_content",
                    Expr::cust(
                        "length(content) BETWEEN 1 AND 262144 AND length(content_hash) = 32",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_principal_avatar_legacy_principal")
                        .from(
                            Alias::new(PRINCIPAL_AVATAR_LEGACY),
                            Alias::new("principal_id"),
                        )
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}
