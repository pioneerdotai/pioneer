use sea_orm_migration::{
    prelude::*,
    schema::{binary, integer, string, timestamp_with_time_zone},
};

const GATEWAY_IDENTITY: &str = "gateway_identity";
const DEVICE: &str = "device";
const AUTH_SESSION: &str = "auth_session";
const AUTH_REFRESH_CREDENTIAL: &str = "auth_refresh_credential";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // The v2 refresh envelope is intentionally incompatible with the
        // predecessor chain. Preserve identities and product data, but force
        // every device to establish a fresh session.
        reset_existing_device_sessions(manager).await?;
        // SQLite applies ON DELETE RESTRICT while dropping a table. Sever the
        // old self-references first so the historical table can be dropped.
        detach_legacy_refresh_chain(manager).await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
                    .to_owned(),
            )
            .await?;
        create_current_refresh_table(manager).await?;
        create_current_refresh_indexes(manager).await?;
        mark_auth_not_ready(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // A downgrade cannot translate v2 envelopes back into legacy bearer
        // values, so it uses the same explicit session reset.
        reset_existing_device_sessions(manager).await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
                    .to_owned(),
            )
            .await?;
        create_legacy_refresh_table(manager).await?;
        create_legacy_refresh_indexes(manager).await?;
        mark_auth_not_ready(manager).await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn detach_legacy_refresh_chain(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .execute(
            Query::update()
                .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
                .values([
                    (Alias::new("status"), "revoked".into()),
                    (Alias::new("consumed_at"), Option::<String>::None.into()),
                    (Alias::new("replaced_by_id"), Option::<String>::None.into()),
                ])
                .to_owned(),
        )
        .await
}

async fn reset_existing_device_sessions(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .execute(
            Query::update()
                .table(Alias::new(AUTH_SESSION))
                .values([
                    (Alias::new("status"), "revoked".into()),
                    (Alias::new("updated_at"), Expr::current_timestamp()),
                    (Alias::new("revoked_at"), Expr::current_timestamp()),
                    (Alias::new("revoke_reason"), "security_reset".into()),
                ])
                .and_where(Expr::col(Alias::new("status")).is_in(["pending", "active"]))
                .to_owned(),
        )
        .await?;
    manager
        .execute(
            Query::update()
                .table(Alias::new(DEVICE))
                .values([
                    (Alias::new("status"), "revoked".into()),
                    (Alias::new("updated_at"), Expr::current_timestamp()),
                    (Alias::new("revoked_at"), Expr::current_timestamp()),
                ])
                .and_where(Expr::col(Alias::new("status")).is_in(["pending", "active"]))
                .to_owned(),
        )
        .await
}

async fn mark_auth_not_ready(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .execute(
            Query::update()
                .table(Alias::new(GATEWAY_IDENTITY))
                .values([
                    (Alias::new("auth_schema_version"), 0.into()),
                    (Alias::new("auth_ready_at"), Option::<String>::None.into()),
                ])
                .to_owned(),
        )
        .await
}

async fn create_current_refresh_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
                .col(string("id").string_len(21).primary_key())
                .col(string("session_id").string_len(21))
                .col(string("token_family_id").string_len(21))
                .col(integer("generation"))
                .col(binary("token_hash"))
                .col(timestamp_with_time_zone("issued_at"))
                .col(timestamp_with_time_zone("expires_at"))
                .check((
                    "ck_auth_refresh_ids",
                    Expr::cust(
                        "length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(session_id) = 21 \
                         AND session_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(token_family_id) = 21 \
                         AND token_family_id NOT GLOB '*[^A-Za-z0-9]*'",
                    ),
                ))
                .check(("ck_auth_refresh_generation", Expr::cust("generation >= 0")))
                .check((
                    "ck_auth_refresh_hash",
                    Expr::cust("length(token_hash) = 32"),
                ))
                .check((
                    "ck_auth_refresh_expiry",
                    Expr::cust("expires_at >= issued_at"),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_auth_refresh_session")
                        .from(
                            Alias::new(AUTH_REFRESH_CREDENTIAL),
                            Alias::new("session_id"),
                        )
                        .to(Alias::new(AUTH_SESSION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .to_owned(),
        )
        .await
}

async fn create_current_refresh_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_auth_refresh_session")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("session_id"))
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_auth_refresh_family")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("token_family_id"))
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_auth_refresh_token_hash")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("token_hash"))
            .unique()
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

async fn create_legacy_refresh_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
                .col(string("id").string_len(21).primary_key())
                .col(string("session_id").string_len(21))
                .col(string("token_family_id").string_len(21))
                .col(integer("generation"))
                .col(binary("token_hash"))
                .col(string("status").string_len(32))
                .col(timestamp_with_time_zone("issued_at"))
                .col(timestamp_with_time_zone("expires_at"))
                .col(timestamp_with_time_zone("consumed_at").null())
                .col(string("replaced_by_id").string_len(21).null())
                .check((
                    "ck_auth_refresh_ids",
                    Expr::cust(
                        "length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(session_id) = 21 \
                         AND session_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(token_family_id) = 21 \
                         AND token_family_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND (replaced_by_id IS NULL OR (\
                            length(replaced_by_id) = 21 \
                            AND replaced_by_id NOT GLOB '*[^A-Za-z0-9]*'\
                         ))",
                    ),
                ))
                .check(("ck_auth_refresh_generation", Expr::cust("generation >= 0")))
                .check((
                    "ck_auth_refresh_hash",
                    Expr::cust("length(token_hash) = 32"),
                ))
                .check((
                    "ck_auth_refresh_expiry",
                    Expr::cust("expires_at >= issued_at"),
                ))
                .check((
                    "ck_auth_refresh_status",
                    Expr::cust("status IN ('current', 'rotated', 'revoked', 'expired')"),
                ))
                .check((
                    "ck_auth_refresh_terminal",
                    Expr::cust(
                        "(status = 'current' AND consumed_at IS NULL \
                            AND replaced_by_id IS NULL) \
                         OR (status = 'rotated' AND consumed_at IS NOT NULL \
                            AND replaced_by_id IS NOT NULL) \
                         OR (status IN ('revoked', 'expired') AND replaced_by_id IS NULL)",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_auth_refresh_session")
                        .from(
                            Alias::new(AUTH_REFRESH_CREDENTIAL),
                            Alias::new("session_id"),
                        )
                        .to(Alias::new(AUTH_SESSION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_auth_refresh_replaced_by")
                        .from(
                            Alias::new(AUTH_REFRESH_CREDENTIAL),
                            Alias::new("replaced_by_id"),
                        )
                        .to(Alias::new(AUTH_REFRESH_CREDENTIAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_legacy_refresh_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_auth_refresh_token_hash")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("token_hash"))
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_auth_refresh_generation")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("session_id"))
            .col(Alias::new("generation"))
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_auth_refresh_current")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("session_id"))
            .unique()
            .cond_where(Expr::col(Alias::new("status")).eq("current"))
            .to_owned(),
        Index::create()
            .name("idx_auth_refresh_family_status")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("token_family_id"))
            .col(Alias::new("status"))
            .to_owned(),
        Index::create()
            .name("idx_auth_refresh_expiry_status")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("expires_at"))
            .col(Alias::new("status"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}
