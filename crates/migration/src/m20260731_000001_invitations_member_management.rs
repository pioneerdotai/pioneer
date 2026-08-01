use sea_orm_migration::{
    prelude::*,
    schema::{binary, integer, string, text, timestamp_with_time_zone},
};

const GATEWAY_IDENTITY: &str = "gateway_identity";
const GATEWAY_PRINCIPAL: &str = "gateway_principal";
const DEVICE: &str = "device";
const AUTH_SESSION: &str = "auth_session";
const INVITATION: &str = "invitation";
const INVITATION_WORKSPACE_GRANT: &str = "invitation_workspace_grant";
const PRINCIPAL_AVATAR: &str = "principal_avatar";
const AUDIT_EVENT: &str = "audit_event";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_invitation(manager).await?;
        create_invitation_indexes(manager).await?;
        create_invitation_workspace_grant(manager).await?;
        create_principal_avatar(manager).await?;
        create_audit_event(manager).await?;
        create_audit_event_indexes(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in [
            AUDIT_EVENT,
            PRINCIPAL_AVATAR,
            INVITATION_WORKSPACE_GRANT,
            INVITATION,
        ] {
            ensure_table_empty(manager, table).await?;
        }
        drop_table(manager, AUDIT_EVENT).await?;
        drop_table(manager, PRINCIPAL_AVATAR).await?;
        drop_table(manager, INVITATION_WORKSPACE_GRANT).await?;
        drop_table(manager, INVITATION).await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn create_invitation(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(INVITATION))
                .col(string("id").string_len(21).primary_key())
                .col(string("gateway_id").string_len(21))
                .col(string("created_by_principal_id").string_len(21))
                .col(string("created_by_session_id").string_len(21))
                .col(string("status").string_len(32))
                .col(binary("token_hash").null())
                .col(integer("token_format_version"))
                .col(timestamp_with_time_zone("expires_at"))
                .col(timestamp_with_time_zone("accepted_at").null())
                .col(timestamp_with_time_zone("revoked_at").null())
                .col(timestamp_with_time_zone("expired_at").null())
                .col(string("accepted_principal_id").string_len(21).null())
                .col(string("accepted_device_id").string_len(21).null())
                .col(string("accepted_session_id").string_len(21).null())
                .col(string("revoke_reason").string_len(64).null())
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .check((
                    "ck_invitation_ids",
                    Expr::cust(
                        "length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(gateway_id) = 21 \
                         AND gateway_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(created_by_principal_id) = 21 \
                         AND created_by_principal_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(created_by_session_id) = 21 \
                         AND created_by_session_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND (accepted_principal_id IS NULL OR (\
                            length(accepted_principal_id) = 21 \
                            AND accepted_principal_id NOT GLOB '*[^A-Za-z0-9]*'\
                         )) \
                         AND (accepted_device_id IS NULL OR (\
                            length(accepted_device_id) = 21 \
                            AND accepted_device_id NOT GLOB '*[^A-Za-z0-9]*'\
                         )) \
                         AND (accepted_session_id IS NULL OR (\
                            length(accepted_session_id) = 21 \
                            AND accepted_session_id NOT GLOB '*[^A-Za-z0-9]*'\
                         ))",
                    ),
                ))
                .check((
                    "ck_invitation_status",
                    Expr::cust("status IN ('pending', 'accepted', 'revoked', 'expired')"),
                ))
                .check((
                    "ck_invitation_token_format",
                    Expr::cust(
                        "token_format_version = 1 \
                         AND (token_hash IS NULL OR length(token_hash) = 32)",
                    ),
                ))
                .check((
                    "ck_invitation_revoke_reason",
                    Expr::cust(
                        "revoke_reason IS NULL OR revoke_reason IN (\
                            'inviter_revoked', 'inviter_unavailable', \
                            'grant_authority_lost', 'workspace_unavailable'\
                         )",
                    ),
                ))
                .check((
                    "ck_invitation_state",
                    Expr::cust(
                        "(status = 'pending' AND token_hash IS NOT NULL \
                            AND accepted_at IS NULL AND revoked_at IS NULL \
                            AND expired_at IS NULL AND accepted_principal_id IS NULL \
                            AND accepted_device_id IS NULL AND accepted_session_id IS NULL \
                            AND revoke_reason IS NULL) \
                         OR (status = 'accepted' AND token_hash IS NULL \
                            AND accepted_at IS NOT NULL AND revoked_at IS NULL \
                            AND expired_at IS NULL AND accepted_principal_id IS NOT NULL \
                            AND accepted_device_id IS NOT NULL AND accepted_session_id IS NOT NULL \
                            AND revoke_reason IS NULL) \
                         OR (status = 'revoked' AND token_hash IS NULL \
                            AND accepted_at IS NULL AND revoked_at IS NOT NULL \
                            AND expired_at IS NULL AND accepted_principal_id IS NULL \
                            AND accepted_device_id IS NULL AND accepted_session_id IS NULL \
                            AND revoke_reason IS NOT NULL) \
                         OR (status = 'expired' AND token_hash IS NULL \
                            AND accepted_at IS NULL AND revoked_at IS NULL \
                            AND expired_at IS NOT NULL AND accepted_principal_id IS NULL \
                            AND accepted_device_id IS NULL AND accepted_session_id IS NULL \
                            AND revoke_reason IS NULL)",
                    ),
                ))
                .check((
                    "ck_invitation_timestamps",
                    Expr::cust("expires_at > created_at AND updated_at >= created_at"),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_invitation_gateway")
                        .from(Alias::new(INVITATION), Alias::new("gateway_id"))
                        .to(Alias::new(GATEWAY_IDENTITY), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_invitation_creator_principal")
                        .from(
                            Alias::new(INVITATION),
                            Alias::new("created_by_principal_id"),
                        )
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_invitation_creator_session")
                        .from(Alias::new(INVITATION), Alias::new("created_by_session_id"))
                        .to(Alias::new(AUTH_SESSION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_invitation_accepted_principal")
                        .from(Alias::new(INVITATION), Alias::new("accepted_principal_id"))
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_invitation_accepted_device")
                        .from(Alias::new(INVITATION), Alias::new("accepted_device_id"))
                        .to(Alias::new(DEVICE), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_invitation_accepted_session")
                        .from(Alias::new(INVITATION), Alias::new("accepted_session_id"))
                        .to(Alias::new(AUTH_SESSION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_invitation_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_invitation_token_hash")
            .table(Alias::new(INVITATION))
            .col(Alias::new("token_hash"))
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_invitation_gateway_status_expiry")
            .table(Alias::new(INVITATION))
            .col(Alias::new("gateway_id"))
            .col(Alias::new("status"))
            .col(Alias::new("expires_at"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .name("idx_invitation_creator_status_created")
            .table(Alias::new(INVITATION))
            .col(Alias::new("created_by_principal_id"))
            .col(Alias::new("status"))
            .col(Alias::new("created_at"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .name("idx_invitation_accepted_principal")
            .table(Alias::new(INVITATION))
            .col(Alias::new("accepted_principal_id"))
            .to_owned(),
        Index::create()
            .name("idx_invitation_creator_session")
            .table(Alias::new(INVITATION))
            .col(Alias::new("created_by_session_id"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

async fn create_invitation_workspace_grant(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(INVITATION_WORKSPACE_GRANT))
                .col(string("invitation_id").string_len(21))
                .col(string("workspace_id").string_len(21))
                .col(timestamp_with_time_zone("created_at"))
                .primary_key(
                    Index::create()
                        .name("pk_invitation_workspace_grant")
                        .col(Alias::new("invitation_id"))
                        .col(Alias::new("workspace_id"))
                        .primary(),
                )
                .check((
                    "ck_invitation_workspace_grant_ids",
                    Expr::cust(
                        "length(invitation_id) = 21 \
                         AND invitation_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(workspace_id) = 21 \
                         AND workspace_id NOT GLOB '*[^A-Za-z0-9]*'",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_invitation_workspace_grant_invitation")
                        .from(
                            Alias::new(INVITATION_WORKSPACE_GRANT),
                            Alias::new("invitation_id"),
                        )
                        .to(Alias::new(INVITATION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .to_owned(),
        )
        .await
}

async fn create_principal_avatar(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(PRINCIPAL_AVATAR))
                .col(string("principal_id").string_len(21).primary_key())
                .col(string("media_type").string_len(32))
                .col(binary("content"))
                .col(binary("content_hash"))
                .col(integer("width"))
                .col(integer("height"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .check((
                    "ck_principal_avatar_principal_id",
                    Expr::cust(
                        "length(principal_id) = 21 \
                         AND principal_id NOT GLOB '*[^A-Za-z0-9]*'",
                    ),
                ))
                .check((
                    "ck_principal_avatar_media_type",
                    Expr::cust("media_type IN ('image/png', 'image/jpeg', 'image/webp')"),
                ))
                .check((
                    "ck_principal_avatar_content",
                    Expr::cust(
                        "length(content) BETWEEN 1 AND 262144 AND length(content_hash) = 32",
                    ),
                ))
                .check((
                    "ck_principal_avatar_dimensions",
                    Expr::cust("width BETWEEN 1 AND 1024 AND height BETWEEN 1 AND 1024"),
                ))
                .check((
                    "ck_principal_avatar_timestamps",
                    Expr::cust("updated_at >= created_at"),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_principal_avatar_principal")
                        .from(Alias::new(PRINCIPAL_AVATAR), Alias::new("principal_id"))
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_audit_event(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AUDIT_EVENT))
                .col(string("id").string_len(21).primary_key())
                .col(string("gateway_id").string_len(21))
                .col(string("actor_principal_id").string_len(21).null())
                .col(string("actor_session_id").string_len(21).null())
                .col(string("action").string_len(64))
                .col(string("domain").string_len(32))
                .col(string("target_kind").string_len(32))
                .col(string("target_id").string_len(21))
                .col(string("workspace_id").string_len(21).null())
                .col(integer("metadata_version"))
                .col(text("metadata_json"))
                .col(timestamp_with_time_zone("created_at"))
                .check((
                    "ck_audit_event_ids",
                    Expr::cust(
                        "length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(gateway_id) = 21 \
                         AND gateway_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(target_id) = 21 \
                         AND target_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND (actor_principal_id IS NULL OR (\
                            length(actor_principal_id) = 21 \
                            AND actor_principal_id NOT GLOB '*[^A-Za-z0-9]*'\
                         )) \
                         AND (actor_session_id IS NULL OR (\
                            length(actor_session_id) = 21 \
                            AND actor_session_id NOT GLOB '*[^A-Za-z0-9]*'\
                         )) \
                         AND (workspace_id IS NULL OR (\
                            length(workspace_id) = 21 \
                            AND workspace_id NOT GLOB '*[^A-Za-z0-9]*'\
                         ))",
                    ),
                ))
                .check((
                    "ck_audit_event_actor",
                    Expr::cust("actor_session_id IS NULL OR actor_principal_id IS NOT NULL"),
                ))
                .check((
                    "ck_audit_event_domain",
                    Expr::cust("domain = 'administration'"),
                ))
                .check((
                    "ck_audit_event_action",
                    Expr::cust(
                        "action IN (\
                            'invitation_created', 'invitation_revoked', \
                            'invitation_expired', 'invitation_accepted', \
                            'workspace_member_added', 'workspace_member_removed', \
                            'member_suspended', 'member_restored', 'member_removed', \
                            'member_recovery_device_created'\
                         )",
                    ),
                ))
                .check((
                    "ck_audit_event_target",
                    Expr::cust(
                        "target_kind IN (\
                            'invitation', 'principal', 'workspace_membership', 'device_session'\
                         ) \
                         AND (target_kind != 'workspace_membership' OR workspace_id IS NOT NULL)",
                    ),
                ))
                .check((
                    "ck_audit_event_metadata",
                    Expr::cust(
                        "metadata_version = 1 \
                         AND length(metadata_json) BETWEEN 1 AND 16384",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_audit_event_gateway")
                        .from(Alias::new(AUDIT_EVENT), Alias::new("gateway_id"))
                        .to(Alias::new(GATEWAY_IDENTITY), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_audit_event_actor_principal")
                        .from(Alias::new(AUDIT_EVENT), Alias::new("actor_principal_id"))
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_audit_event_actor_session")
                        .from(Alias::new(AUDIT_EVENT), Alias::new("actor_session_id"))
                        .to(Alias::new(AUTH_SESSION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_audit_event_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_audit_event_gateway_created")
            .table(Alias::new(AUDIT_EVENT))
            .col(Alias::new("gateway_id"))
            .col(Alias::new("created_at"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .name("idx_audit_event_actor_created")
            .table(Alias::new(AUDIT_EVENT))
            .col(Alias::new("actor_principal_id"))
            .col(Alias::new("created_at"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .name("idx_audit_event_target_created")
            .table(Alias::new(AUDIT_EVENT))
            .col(Alias::new("target_kind"))
            .col(Alias::new("target_id"))
            .col(Alias::new("created_at"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .name("idx_audit_event_workspace_created")
            .table(Alias::new(AUDIT_EVENT))
            .col(Alias::new("workspace_id"))
            .col(Alias::new("created_at"))
            .col(Alias::new("id"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

async fn ensure_table_empty(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(Alias::new(table))
        .to_owned();
    let row = manager.get_connection().query_one(&query).await?;
    let count = match row {
        Some(row) => row.try_get::<i64>("", "count")?,
        None => 0,
    };
    if count == 0 {
        Ok(())
    } else {
        Err(DbErr::Custom(format!(
            "cannot roll back Epic 5 while {table} contains durable business data: {count} row(s)"
        )))
    }
}

async fn drop_table(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    manager
        .drop_table(
            Table::drop()
                .if_exists()
                .table(Alias::new(table))
                .to_owned(),
        )
        .await
}
