use sea_orm_migration::{
    prelude::*,
    schema::{binary, integer, string, text, timestamp_with_time_zone},
};

const GATEWAY_IDENTITY: &str = "gateway_identity";
const GATEWAY_PRINCIPAL: &str = "gateway_principal";
const DEVICE: &str = "device";
const AUTH_SESSION: &str = "auth_session";
const AUTH_REFRESH_CREDENTIAL: &str = "auth_refresh_credential";

const AUTH_SCHEMA_VERSION: &str = "auth_schema_version";
const AUTH_READY_AT: &str = "auth_ready_at";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        add_gateway_auth_columns(manager).await?;
        create_device(manager).await?;
        create_device_indexes(manager).await?;
        create_auth_session(manager).await?;
        create_auth_session_indexes(manager).await?;
        create_auth_refresh_credential(manager).await?;
        create_auth_refresh_credential_indexes(manager).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_table(manager, AUTH_REFRESH_CREDENTIAL).await?;
        drop_table(manager, AUTH_SESSION).await?;
        drop_table(manager, DEVICE).await?;
        drop_gateway_auth_columns(manager).await?;
        Ok(())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn add_gateway_auth_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if !manager
        .has_column(GATEWAY_IDENTITY, AUTH_SCHEMA_VERSION)
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(GATEWAY_IDENTITY))
                    .add_column(integer(AUTH_SCHEMA_VERSION).default(0).check((
                        "ck_gateway_identity_auth_schema_version",
                        Expr::cust("auth_schema_version >= 0"),
                    )))
                    .to_owned(),
            )
            .await?;
    }

    if !manager.has_column(GATEWAY_IDENTITY, AUTH_READY_AT).await? {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(GATEWAY_IDENTITY))
                    .add_column(timestamp_with_time_zone(AUTH_READY_AT).null())
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}

async fn create_device(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(DEVICE))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("gateway_id").string_len(21))
                .col(string("principal_id").string_len(21))
                .col(string("installation_id").string_len(255).null())
                .col(string("display_name").string_len(255).null())
                .col(string("client_kind").string_len(32).null())
                .col(text("platform").null())
                .col(text("client_version").null())
                .col(string("status").string_len(32))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .col(timestamp_with_time_zone("last_seen_at").null())
                .col(timestamp_with_time_zone("revoked_at").null())
                .check((
                    "ck_device_ids",
                    Expr::cust(
                        "length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(gateway_id) = 21 \
                         AND gateway_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(principal_id) = 21 \
                         AND principal_id NOT GLOB '*[^A-Za-z0-9]*'",
                    ),
                ))
                .check((
                    "ck_device_installation",
                    Expr::cust(
                        "installation_id IS NULL OR (\
                            length(trim(installation_id)) BETWEEN 1 AND 255 \
                            AND installation_id = trim(installation_id)\
                        )",
                    ),
                ))
                .check((
                    "ck_device_display_name",
                    Expr::cust(
                        "display_name IS NULL OR (\
                            length(trim(display_name)) BETWEEN 1 AND 255 \
                            AND display_name = trim(display_name)\
                        )",
                    ),
                ))
                .check((
                    "ck_device_client_kind",
                    Expr::cust(
                        "client_kind IS NULL OR client_kind IN ('desktop', 'mobile', 'other')",
                    ),
                ))
                .check((
                    "ck_device_metadata_group",
                    Expr::cust(
                        "(installation_id IS NULL AND display_name IS NULL \
                            AND client_kind IS NULL AND platform IS NULL \
                            AND client_version IS NULL) \
                         OR (installation_id IS NOT NULL AND display_name IS NOT NULL \
                            AND client_kind IS NOT NULL)",
                    ),
                ))
                .check((
                    "ck_device_status",
                    Expr::cust("status IN ('pending', 'active', 'revoked')"),
                ))
                .check((
                    "ck_device_state",
                    Expr::cust(
                        "(status = 'pending' AND installation_id IS NULL \
                            AND last_seen_at IS NULL AND revoked_at IS NULL) \
                         OR (status = 'active' AND installation_id IS NOT NULL \
                            AND last_seen_at IS NOT NULL AND revoked_at IS NULL) \
                         OR (status = 'revoked' AND revoked_at IS NOT NULL AND (\
                            (installation_id IS NULL AND last_seen_at IS NULL) \
                            OR (installation_id IS NOT NULL AND last_seen_at IS NOT NULL)\
                         ))",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_device_gateway")
                        .from(Alias::new(DEVICE), Alias::new("gateway_id"))
                        .to(Alias::new(GATEWAY_IDENTITY), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_device_principal")
                        .from(Alias::new(DEVICE), Alias::new("principal_id"))
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_device_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .if_not_exists()
            .name("idx_device_owner_status")
            .table(Alias::new(DEVICE))
            .col(Alias::new("gateway_id"))
            .col(Alias::new("principal_id"))
            .col(Alias::new("status"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_device_active_installation")
            .table(Alias::new(DEVICE))
            .col(Alias::new("gateway_id"))
            .col(Alias::new("principal_id"))
            .col(Alias::new("installation_id"))
            .unique()
            .cond_where(Expr::col(Alias::new("status")).eq("active"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_device_principal_last_seen")
            .table(Alias::new(DEVICE))
            .col(Alias::new("principal_id"))
            .col(Alias::new("last_seen_at"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

async fn create_auth_session(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AUTH_SESSION))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("gateway_id").string_len(21))
                .col(string("principal_id").string_len(21))
                .col(string("device_id").string_len(21))
                .col(string("token_family_id").string_len(21))
                .col(string("created_by_session_id").string_len(21).null())
                .col(binary("activation_token_hash"))
                .col(binary("activation_locator_hash"))
                .col(integer("activation_failed_attempts").default(0))
                .col(timestamp_with_time_zone("activation_expires_at"))
                .col(timestamp_with_time_zone("activated_at").null())
                .col(string("status").string_len(32))
                .col(integer("refresh_generation"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .col(timestamp_with_time_zone("last_seen_at").null())
                .col(timestamp_with_time_zone("last_refreshed_at").null())
                .col(timestamp_with_time_zone("refresh_expires_at").null())
                .col(timestamp_with_time_zone("revoked_at").null())
                .col(string("revoke_reason").string_len(64).null())
                .check((
                    "ck_auth_session_ids",
                    Expr::cust(
                        "length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(gateway_id) = 21 \
                         AND gateway_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(principal_id) = 21 \
                         AND principal_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(device_id) = 21 \
                         AND device_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(token_family_id) = 21 \
                         AND token_family_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND (created_by_session_id IS NULL OR (\
                            length(created_by_session_id) = 21 \
                            AND created_by_session_id NOT GLOB '*[^A-Za-z0-9]*'\
                         ))",
                    ),
                ))
                .check((
                    "ck_auth_session_activation_hash",
                    Expr::cust("length(activation_token_hash) = 32"),
                ))
                .check((
                    "ck_auth_session_activation_locator_hash",
                    Expr::cust("length(activation_locator_hash) = 32"),
                ))
                .check((
                    "ck_auth_session_activation_attempts",
                    Expr::cust(
                        "activation_failed_attempts >= 0 AND activation_failed_attempts <= 5",
                    ),
                ))
                .check((
                    "ck_auth_session_activation_expiry",
                    Expr::cust("activation_expires_at >= created_at"),
                ))
                .check((
                    "ck_auth_session_status",
                    Expr::cust("status IN ('pending', 'active', 'revoked', 'expired')"),
                ))
                .check((
                    "ck_auth_session_generation",
                    Expr::cust("refresh_generation >= 0"),
                ))
                .check((
                    "ck_auth_session_refresh_expiry",
                    Expr::cust("refresh_expires_at IS NULL OR refresh_expires_at >= created_at"),
                ))
                .check((
                    "ck_auth_session_runtime_group",
                    Expr::cust(
                        "(activated_at IS NULL AND last_seen_at IS NULL \
                            AND last_refreshed_at IS NULL AND refresh_expires_at IS NULL) \
                         OR (activated_at IS NOT NULL AND last_seen_at IS NOT NULL \
                            AND last_refreshed_at IS NOT NULL \
                            AND refresh_expires_at IS NOT NULL)",
                    ),
                ))
                .check((
                    "ck_auth_session_state",
                    Expr::cust(
                        "(status = 'pending' AND activation_failed_attempts < 5 \
                            AND activated_at IS NULL AND revoked_at IS NULL \
                            AND revoke_reason IS NULL) \
                         OR (status = 'active' AND activation_failed_attempts < 5 \
                            AND activated_at IS NOT NULL AND revoked_at IS NULL \
                            AND revoke_reason IS NULL) \
                         OR (status = 'revoked' AND revoked_at IS NOT NULL \
                            AND revoke_reason IS NOT NULL) \
                         OR (status = 'expired' AND revoked_at IS NOT NULL \
                            AND revoke_reason IS NULL)",
                    ),
                ))
                .check((
                    "ck_auth_session_revoke_reason",
                    Expr::cust(
                        "revoke_reason IS NULL OR revoke_reason IN (\
                            'logout', 'self_revoke', 'device_revoke', \
                            'activation_attempts_exceeded', 'refresh_reuse', \
                            'principal_suspended', 'principal_removed', \
                            'superseded', 'security_reset'\
                        )",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_auth_session_gateway")
                        .from(Alias::new(AUTH_SESSION), Alias::new("gateway_id"))
                        .to(Alias::new(GATEWAY_IDENTITY), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_auth_session_principal")
                        .from(Alias::new(AUTH_SESSION), Alias::new("principal_id"))
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_auth_session_device")
                        .from(Alias::new(AUTH_SESSION), Alias::new("device_id"))
                        .to(Alias::new(DEVICE), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_auth_session_created_by")
                        .from(
                            Alias::new(AUTH_SESSION),
                            Alias::new("created_by_session_id"),
                        )
                        .to(Alias::new(AUTH_SESSION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_auth_session_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_token_family")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("token_family_id"))
            .unique()
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_activation_hash")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("activation_token_hash"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_pending_activation_locator_hash")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("gateway_id"))
            .col(Alias::new("activation_locator_hash"))
            .unique()
            .cond_where(Expr::col(Alias::new("status")).eq("pending"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_pending_creator")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("created_by_session_id"))
            .unique()
            .cond_where(
                Cond::all()
                    .add(Expr::col(Alias::new("status")).eq("pending"))
                    .add(Expr::col(Alias::new("created_by_session_id")).is_not_null()),
            )
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_pending_local")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("gateway_id"))
            .col(Alias::new("principal_id"))
            .unique()
            .cond_where(
                Cond::all()
                    .add(Expr::col(Alias::new("status")).eq("pending"))
                    .add(Expr::col(Alias::new("created_by_session_id")).is_null()),
            )
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_active_device")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("device_id"))
            .unique()
            .cond_where(Expr::col(Alias::new("status")).eq("active"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_principal_status")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("principal_id"))
            .col(Alias::new("status"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_device_status")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("device_id"))
            .col(Alias::new("status"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_session_expiry_status")
            .table(Alias::new(AUTH_SESSION))
            .col(Alias::new("refresh_expires_at"))
            .col(Alias::new("status"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

async fn create_auth_refresh_credential(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
                .if_not_exists()
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

async fn create_auth_refresh_credential_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .if_not_exists()
            .name("idx_auth_refresh_token_hash")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("token_hash"))
            .unique()
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_refresh_generation")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("session_id"))
            .col(Alias::new("generation"))
            .unique()
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_refresh_current")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("session_id"))
            .unique()
            .cond_where(Expr::col(Alias::new("status")).eq("current"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_auth_refresh_family_status")
            .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
            .col(Alias::new("token_family_id"))
            .col(Alias::new("status"))
            .to_owned(),
        Index::create()
            .if_not_exists()
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

async fn drop_gateway_auth_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for column in [AUTH_READY_AT, AUTH_SCHEMA_VERSION] {
        if manager.has_column(GATEWAY_IDENTITY, column).await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(GATEWAY_IDENTITY))
                        .drop_column(Alias::new(column))
                        .to_owned(),
                )
                .await?;
        }
    }
    Ok(())
}

async fn drop_table(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    manager
        .drop_table(
            Table::drop()
                .table(Alias::new(table))
                .if_exists()
                .to_owned(),
        )
        .await
}
