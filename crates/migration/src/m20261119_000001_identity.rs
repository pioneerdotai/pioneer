use crate::drop_tables;
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::{
    big_integer, big_integer_null, binary, binary_null, boolean, json, text, text_null,
    timestamp_with_time_zone, timestamp_with_time_zone_null,
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("workspace")
                    .col(text("id").primary_key())
                    .col(text("name"))
                    .col(boolean("is_active").default(true))
                    .col(boolean("is_current").default(false))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_workspace_single_active_current")
                    .table("workspace")
                    .unique()
                    .col("is_current")
                    .and_where(Expr::cust(r#"is_active = TRUE AND is_current = TRUE"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_workspace_active_id")
                    .table("workspace")
                    .col("is_active")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        // The default workspace normally installed by Gateway bootstrap.

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("workspace")
                    .columns(["id", "name", "is_active", "is_current"])
                    .values_panic([
                        "000000000000000000000".into(),
                        "Default Workspace".into(),
                        true.into(),
                        true.into(),
                    ])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("gateway_identity")
                    .col(text("id").primary_key())
                    .col(big_integer("singleton_key"))
                    .col(big_integer("identity_bootstrap_version").default(0))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(big_integer("auth_schema_version").default(0))
                    .col(timestamp_with_time_zone_null("auth_ready_at"))
                    .col(big_integer("unread_started_at").default(0))
                    .index(Index::create().unique().col("singleton_key"))
                    .check(Expr::cust(r#"auth_schema_version >= 0"#))
                    .check(Expr::cust(r#"singleton_key = 1"#))
                    .check(Expr::cust(r#"identity_bootstrap_version >= 0"#))
                    .check(Expr::cust(
                        r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C""#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("gateway_principal")
                .col(text("id").primary_key())
                .col(text("gateway_id"))
                .col(text("kind"))
                .col(text_null("role_key"))
                .col(text("status"))
                .col(text("display_name"))
                .col(text("nickname"))
                .col(text("nickname_key"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone_null("removed_at"))
                .col(big_integer("authorization_guard").default(1))
                .col(big_integer_null("inbox_after_sequence"))
                .check(Expr::cust(r#"authorization_guard = 1 AND ( ( kind = 'superuser' AND role_key IS NULL AND status = 'active' AND removed_at IS NULL ) OR ( kind = 'user' AND role_key IS NOT NULL AND length ( role_key ) BETWEEN 1 AND 32 AND substr ( role_key , 1 , 1 ) ~ '^[a-z]$' COLLATE "C" AND role_key !~ '[^a-z0-9_-]' COLLATE "C" AND ( ( status IN ( 'active' , 'suspended' ) AND removed_at IS NULL ) OR ( status = 'removed' AND removed_at IS NOT NULL ) ) ) )"#))
                .check(Expr::cust(r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( gateway_id ) = 21 AND gateway_id !~ '[^A-Za-z0-9]' COLLATE "C""#))
                .check(Expr::cust(r#"kind IN ( 'superuser' , 'user' )"#))
                .check(Expr::cust(r#"status IN ( 'active' , 'suspended' , 'removed' )"#))
                .check(Expr::cust(r#"( status = 'removed' AND removed_at IS NOT NULL ) OR ( status != 'removed' AND removed_at IS NULL )"#))
                .check(Expr::cust(r#"kind != 'superuser' OR ( role_key IS NULL AND status = 'active' AND removed_at IS NULL )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_gateway_principal_one_superuser")
                    .table("gateway_principal")
                    .unique()
                    .col("gateway_id")
                    .and_where(Expr::cust(r#"kind = 'superuser'"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_gateway_principal_nickname")
                    .table("gateway_principal")
                    .unique()
                    .col("gateway_id")
                    .col("nickname_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_gateway_principal_status")
                    .table("gateway_principal")
                    .col("gateway_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_gateway_principal_authorization")
                    .table("gateway_principal")
                    .col("gateway_id")
                    .col("kind")
                    .col("status")
                    .col("role_key")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("device")
                .col(text("id").primary_key())
                .col(text("gateway_id"))
                .col(text("principal_id"))
                .col(text_null("installation_id"))
                .col(text_null("display_name"))
                .col(text_null("client_kind"))
                .col(text_null("platform"))
                .col(text_null("client_version"))
                .col(text("status"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .col(timestamp_with_time_zone_null("last_seen_at"))
                .col(timestamp_with_time_zone_null("revoked_at"))
                .check(Expr::cust(r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( gateway_id ) = 21 AND gateway_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( principal_id ) = 21 AND principal_id !~ '[^A-Za-z0-9]' COLLATE "C""#))
                .check(Expr::cust(r#"installation_id IS NULL OR ( length ( trim ( installation_id ) ) BETWEEN 1 AND 255 AND installation_id = trim ( installation_id ) )"#))
                .check(Expr::cust(r#"display_name IS NULL OR ( length ( trim ( display_name ) ) BETWEEN 1 AND 255 AND display_name = trim ( display_name ) )"#))
                .check(Expr::cust(r#"client_kind IS NULL OR client_kind IN ( 'desktop' , 'mobile' , 'other' )"#))
                .check(Expr::cust(r#"( installation_id IS NULL AND display_name IS NULL AND client_kind IS NULL AND platform IS NULL AND client_version IS NULL ) OR ( installation_id IS NOT NULL AND display_name IS NOT NULL AND client_kind IS NOT NULL )"#))
                .check(Expr::cust(r#"status IN ( 'pending' , 'active' , 'revoked' )"#))
                .check(Expr::cust(r#"( status = 'pending' AND installation_id IS NULL AND last_seen_at IS NULL AND revoked_at IS NULL ) OR ( status = 'active' AND installation_id IS NOT NULL AND last_seen_at IS NOT NULL AND revoked_at IS NULL ) OR ( status = 'revoked' AND revoked_at IS NOT NULL AND ( ( installation_id IS NULL AND last_seen_at IS NULL ) OR ( installation_id IS NOT NULL AND last_seen_at IS NOT NULL ) ) )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_device_owner_status")
                    .table("device")
                    .col("gateway_id")
                    .col("principal_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_device_active_installation")
                    .table("device")
                    .unique()
                    .col("gateway_id")
                    .col("principal_id")
                    .col("installation_id")
                    .and_where(Expr::cust(r#""status" = 'active'"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_device_principal_last_seen")
                    .table("device")
                    .col("principal_id")
                    .col("last_seen_at")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("auth_session")
                .col(text("id").primary_key())
                .col(text("gateway_id"))
                .col(text("principal_id"))
                .col(text("device_id"))
                .col(text("token_family_id"))
                .col(text_null("created_by_session_id"))
                .col(binary("activation_token_hash"))
                .col(binary("activation_locator_hash"))
                .col(big_integer("activation_failed_attempts").default(0))
                .col(timestamp_with_time_zone("activation_expires_at"))
                .col(timestamp_with_time_zone_null("activated_at"))
                .col(text("status"))
                .col(big_integer("refresh_generation"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .col(timestamp_with_time_zone_null("last_seen_at"))
                .col(timestamp_with_time_zone_null("last_refreshed_at"))
                .col(timestamp_with_time_zone_null("refresh_expires_at"))
                .col(timestamp_with_time_zone_null("revoked_at"))
                .col(text_null("revoke_reason"))
                .check(Expr::cust(r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( gateway_id ) = 21 AND gateway_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( principal_id ) = 21 AND principal_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( device_id ) = 21 AND device_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( token_family_id ) = 21 AND token_family_id !~ '[^A-Za-z0-9]' COLLATE "C" AND ( created_by_session_id IS NULL OR ( length ( created_by_session_id ) = 21 AND created_by_session_id !~ '[^A-Za-z0-9]' COLLATE "C" ) )"#))
                .check(Expr::cust(r#"length ( activation_token_hash ) = 32"#))
                .check(Expr::cust(r#"length ( activation_locator_hash ) = 32"#))
                .check(Expr::cust(r#"activation_failed_attempts >= 0 AND activation_failed_attempts <= 5"#))
                .check(Expr::cust(r#"activation_expires_at >= created_at"#))
                .check(Expr::cust(r#"status IN ( 'pending' , 'active' , 'revoked' , 'expired' )"#))
                .check(Expr::cust(r#"refresh_generation >= 0"#))
                .check(Expr::cust(r#"refresh_expires_at IS NULL OR refresh_expires_at >= created_at"#))
                .check(Expr::cust(r#"( activated_at IS NULL AND last_seen_at IS NULL AND last_refreshed_at IS NULL AND refresh_expires_at IS NULL ) OR ( activated_at IS NOT NULL AND last_seen_at IS NOT NULL AND last_refreshed_at IS NOT NULL AND refresh_expires_at IS NOT NULL )"#))
                .check(Expr::cust(r#"( status = 'pending' AND activation_failed_attempts < 5 AND activated_at IS NULL AND revoked_at IS NULL AND revoke_reason IS NULL ) OR ( status = 'active' AND activation_failed_attempts < 5 AND activated_at IS NOT NULL AND revoked_at IS NULL AND revoke_reason IS NULL ) OR ( status = 'revoked' AND revoked_at IS NOT NULL AND revoke_reason IS NOT NULL ) OR ( status = 'expired' AND revoked_at IS NOT NULL AND revoke_reason IS NULL )"#))
                .check(Expr::cust(r#"revoke_reason IS NULL OR revoke_reason IN ( 'logout' , 'self_revoke' , 'device_revoke' , 'activation_attempts_exceeded' , 'refresh_reuse' , 'principal_suspended' , 'principal_removed' , 'superseded' , 'security_reset' )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_token_family")
                    .table("auth_session")
                    .unique()
                    .col("token_family_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_activation_hash")
                    .table("auth_session")
                    .col("activation_token_hash")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_pending_activation_locator_hash")
                    .table("auth_session")
                    .unique()
                    .col("gateway_id")
                    .col("activation_locator_hash")
                    .and_where(Expr::cust(r#""status" = 'pending'"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_pending_creator")
                    .table("auth_session")
                    .unique()
                    .col("created_by_session_id")
                    .and_where(Expr::cust(
                        r#""status" = 'pending' AND "created_by_session_id" IS NOT NULL"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_pending_local")
                    .table("auth_session")
                    .unique()
                    .col("gateway_id")
                    .col("principal_id")
                    .and_where(Expr::cust(
                        r#""status" = 'pending' AND "created_by_session_id" IS NULL"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_active_device")
                    .table("auth_session")
                    .unique()
                    .col("device_id")
                    .and_where(Expr::cust(r#""status" = 'active'"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_principal_status")
                    .table("auth_session")
                    .col("principal_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_device_status")
                    .table("auth_session")
                    .col("device_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_session_expiry_status")
                    .table("auth_session")
                    .col("refresh_expires_at")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("auth_refresh_credential")
                .col(text("id").primary_key())
                .col(text("session_id"))
                .col(text("token_family_id"))
                .col(big_integer("generation"))
                .col(binary("token_hash"))
                .col(timestamp_with_time_zone("issued_at"))
                .col(timestamp_with_time_zone("expires_at"))
                .col(text_null("exchange_request_id"))
                .check(Expr::cust(r#"exchange_request_id IS NULL OR ( length ( exchange_request_id ) = 21 AND exchange_request_id !~ '[^A-Za-z0-9]' COLLATE "C" )"#))
                .check(Expr::cust(r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( session_id ) = 21 AND session_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( token_family_id ) = 21 AND token_family_id !~ '[^A-Za-z0-9]' COLLATE "C""#))
                .check(Expr::cust(r#"generation >= 0"#))
                .check(Expr::cust(r#"length ( token_hash ) = 32"#))
                .check(Expr::cust(r#"expires_at >= issued_at"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_refresh_session")
                    .table("auth_refresh_credential")
                    .unique()
                    .col("session_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_refresh_family")
                    .table("auth_refresh_credential")
                    .unique()
                    .col("token_family_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_auth_refresh_token_hash")
                    .table("auth_refresh_credential")
                    .unique()
                    .col("token_hash")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("workspace_membership")
                .col(text("principal_id"))
                .col(text("workspace_id"))
                .col(text("granted_by_actor_kind"))
                .col(text_null("granted_by_actor_id"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .col(big_integer_null("inbox_after_sequence"))
                .primary_key(Index::create().col("principal_id").col("workspace_id"))
                .check(Expr::cust(r#"length ( principal_id ) = 21 AND principal_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( workspace_id ) = 21 AND workspace_id !~ '[^A-Za-z0-9]' COLLATE "C""#))
                .check(Expr::cust(r#"( granted_by_actor_kind = 'system' AND granted_by_actor_id IS NULL ) OR ( granted_by_actor_kind = 'principal' AND length ( granted_by_actor_id ) = 21 AND granted_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C" )"#))
                .check(Expr::cust(r#"updated_at >= created_at"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_workspace_membership_principal")
                    .table("workspace_membership")
                    .col("principal_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_workspace_membership_workspace")
                    .table("workspace_membership")
                    .col("workspace_id")
                    .col("principal_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("invitation")
                .col(text("id").primary_key())
                .col(text("gateway_id"))
                .col(text("created_by_principal_id"))
                .col(text("created_by_session_id"))
                .col(text("status"))
                .col(binary_null("token_hash"))
                .col(big_integer("token_format_version"))
                .col(timestamp_with_time_zone("expires_at"))
                .col(timestamp_with_time_zone_null("accepted_at"))
                .col(timestamp_with_time_zone_null("revoked_at"))
                .col(timestamp_with_time_zone_null("expired_at"))
                .col(text_null("accepted_principal_id"))
                .col(text_null("accepted_device_id"))
                .col(text_null("accepted_session_id"))
                .col(text_null("revoke_reason"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .col(text("target_role_key").default("member"))
                .check(Expr::cust(r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( gateway_id ) = 21 AND gateway_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( created_by_principal_id ) = 21 AND created_by_principal_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( created_by_session_id ) = 21 AND created_by_session_id !~ '[^A-Za-z0-9]' COLLATE "C" AND ( accepted_principal_id IS NULL OR ( length ( accepted_principal_id ) = 21 AND accepted_principal_id !~ '[^A-Za-z0-9]' COLLATE "C" ) ) AND ( accepted_device_id IS NULL OR ( length ( accepted_device_id ) = 21 AND accepted_device_id !~ '[^A-Za-z0-9]' COLLATE "C" ) ) AND ( accepted_session_id IS NULL OR ( length ( accepted_session_id ) = 21 AND accepted_session_id !~ '[^A-Za-z0-9]' COLLATE "C" ) )"#))
                .check(Expr::cust(r#"status IN ( 'pending' , 'accepted' , 'revoked' , 'expired' )"#))
                .check(Expr::cust(r#"token_format_version = 1 AND ( token_hash IS NULL OR length ( token_hash ) = 32 )"#))
                .check(Expr::cust(r#"revoke_reason IS NULL OR revoke_reason IN ( 'inviter_revoked' , 'inviter_unavailable' , 'grant_authority_lost' , 'workspace_unavailable' )"#))
                .check(Expr::cust(r#"( status = 'pending' AND token_hash IS NOT NULL AND accepted_at IS NULL AND revoked_at IS NULL AND expired_at IS NULL AND accepted_principal_id IS NULL AND accepted_device_id IS NULL AND accepted_session_id IS NULL AND revoke_reason IS NULL ) OR ( status = 'accepted' AND token_hash IS NULL AND accepted_at IS NOT NULL AND revoked_at IS NULL AND expired_at IS NULL AND accepted_principal_id IS NOT NULL AND accepted_device_id IS NOT NULL AND accepted_session_id IS NOT NULL AND revoke_reason IS NULL ) OR ( status = 'revoked' AND token_hash IS NULL AND accepted_at IS NULL AND revoked_at IS NOT NULL AND expired_at IS NULL AND accepted_principal_id IS NULL AND accepted_device_id IS NULL AND accepted_session_id IS NULL AND revoke_reason IS NOT NULL ) OR ( status = 'expired' AND token_hash IS NULL AND accepted_at IS NULL AND revoked_at IS NULL AND expired_at IS NOT NULL AND accepted_principal_id IS NULL AND accepted_device_id IS NULL AND accepted_session_id IS NULL AND revoke_reason IS NULL )"#))
                .check(Expr::cust(r#"expires_at > created_at AND updated_at >= created_at"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_invitation_token_hash")
                    .table("invitation")
                    .unique()
                    .col("token_hash")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_invitation_gateway_status_expiry")
                    .table("invitation")
                    .col("gateway_id")
                    .col("status")
                    .col("expires_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_invitation_creator_status_created")
                    .table("invitation")
                    .col("created_by_principal_id")
                    .col("status")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_invitation_accepted_principal")
                    .table("invitation")
                    .col("accepted_principal_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_invitation_creator_session")
                    .table("invitation")
                    .col("created_by_session_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("invitation_workspace_grant")
                .col(text("invitation_id"))
                .col(text("workspace_id"))
                .col(timestamp_with_time_zone("created_at"))
                .primary_key(Index::create().col("invitation_id").col("workspace_id"))
                .check(Expr::cust(r#"length ( invitation_id ) = 21 AND invitation_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( workspace_id ) = 21 AND workspace_id !~ '[^A-Za-z0-9]' COLLATE "C""#))
                .to_owned(),
        ).await?;

        manager.create_table(
            Table::create().table("audit_event")
                .col(text("id").primary_key())
                .col(text("gateway_id"))
                .col(text_null("actor_principal_id"))
                .col(text_null("actor_session_id"))
                .col(text("action"))
                .col(text("domain"))
                .col(text("target_kind"))
                .col(text("target_id"))
                .col(text_null("workspace_id"))
                .col(big_integer("metadata_version"))
                .col(json("metadata_json"))
                .col(timestamp_with_time_zone("created_at"))
                .col(big_integer("policy_generation").default(1))
                .col(text_null("policy_role_key"))
                .col(text("policy_fingerprint").default("0000000000000000000000000000000000000000000000000000000000000000"))
                .check(Expr::cust(r#"policy_generation > 0 AND length ( policy_fingerprint ) = 64 AND policy_fingerprint !~ '[^0-9a-f]' COLLATE "C" AND ( policy_role_key IS NULL OR ( length ( policy_role_key ) BETWEEN 1 AND 32 AND substr ( policy_role_key , 1 , 1 ) ~ '^[a-z]$' COLLATE "C" AND policy_role_key !~ '[^a-z0-9_-]' COLLATE "C" ) )"#))
                .check(Expr::cust(r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( gateway_id ) = 21 AND gateway_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( target_id ) = 21 AND target_id !~ '[^A-Za-z0-9]' COLLATE "C" AND ( actor_principal_id IS NULL OR ( length ( actor_principal_id ) = 21 AND actor_principal_id !~ '[^A-Za-z0-9]' COLLATE "C" ) ) AND ( actor_session_id IS NULL OR ( length ( actor_session_id ) = 21 AND actor_session_id !~ '[^A-Za-z0-9]' COLLATE "C" ) ) AND ( workspace_id IS NULL OR ( length ( workspace_id ) = 21 AND workspace_id !~ '[^A-Za-z0-9]' COLLATE "C" ) )"#))
                .check(Expr::cust(r#"actor_session_id IS NULL OR actor_principal_id IS NOT NULL"#))
                .check(Expr::cust(r#"domain = 'administration'"#))
                .check(Expr::cust(r#"action IN ( 'invitation_created' , 'invitation_revoked' , 'invitation_expired' , 'invitation_accepted' , 'workspace_member_added' , 'workspace_member_removed' , 'member_suspended' , 'member_restored' , 'member_removed' , 'member_recovery_device_created' )"#))
                .check(Expr::cust(r#"target_kind IN ( 'invitation' , 'principal' , 'workspace_membership' , 'device_session' ) AND ( target_kind != 'workspace_membership' OR workspace_id IS NOT NULL )"#))
                .check(Expr::cust(r#"metadata_version = 1 AND length ( metadata_json::text ) BETWEEN 1 AND 16384"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_audit_event_gateway_created")
                    .table("audit_event")
                    .col("gateway_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_audit_event_actor_created")
                    .table("audit_event")
                    .col("actor_principal_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_audit_event_target_created")
                    .table("audit_event")
                    .col("target_kind")
                    .col("target_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_audit_event_workspace_created")
                    .table("audit_event")
                    .col("workspace_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("principal_avatar_revision")
                .col(text("id").primary_key())
                .col(text("principal_id"))
                .col(binary("content_hash"))
                .col(text("media_type"))
                .col(binary("content"))
                .col(big_integer("width"))
                .col(big_integer("height"))
                .col(timestamp_with_time_zone("created_at"))
                .check(Expr::cust(r#"length ( id ) = 86 AND id = principal_id || ':' || encode(content_hash, 'hex')"#))
                .check(Expr::cust(r#"length ( principal_id ) = 21 AND principal_id !~ '[^A-Za-z0-9]' COLLATE "C""#))
                .check(Expr::cust(r#"media_type IN ( 'image/png' , 'image/jpeg' , 'image/webp' )"#))
                .check(Expr::cust(r#"length ( content ) BETWEEN 1 AND 262144 AND length ( content_hash ) = 32"#))
                .check(Expr::cust(r#"width BETWEEN 1 AND 1024 AND height BETWEEN 1 AND 1024"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_principal_avatar_revision_principal_hash")
                    .table("principal_avatar_revision")
                    .unique()
                    .col("principal_id")
                    .col("content_hash")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("principal_avatar")
                .col(text("principal_id").primary_key())
                .col(text("revision_id"))
                .col(timestamp_with_time_zone("updated_at"))
                .index(Index::create().unique().col("revision_id"))
                .check(Expr::cust(r#"length ( revision_id ) = 86 AND substr ( revision_id , 1 , 22 ) = principal_id || ':'"#))
                .to_owned(),
        ).await?;

        manager
            .create_table(
                Table::create()
                    .table("authorization_policy_state")
                    .col(big_integer("singleton_id").primary_key())
                    .col(big_integer("generation"))
                    .col(text("code_policy_fingerprint").default(""))
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(r#"singleton_id = 1 AND generation > 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("authorization_policy_state")
                    .columns([
                        "singleton_id",
                        "generation",
                        "code_policy_fingerprint",
                        "updated_at",
                    ])
                    .values_panic([
                        1_i64.into(),
                        1_i64.into(),
                        "".into(),
                        Expr::cust(r#"statement_timestamp()"#).into(),
                    ])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("authorization_change_feed")
                    .col(big_integer("generation").primary_key())
                    .col(text("change_kind"))
                    .col(json("affected_scope_json"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("actor_nickname_index")
                .col(text("workspace_id"))
                .col(text("nickname_key"))
                .col(text("owner_kind"))
                .col(text("owner_id"))
                .col(text("status").default("active"))
                .col(timestamp_with_time_zone("claimed_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone_null("tombstoned_at"))
                .primary_key(Index::create().col("workspace_id").col("nickname_key"))
                .check(Expr::cust(r#"( status = 'active' AND tombstoned_at IS NULL ) OR ( status = 'tombstoned' AND tombstoned_at IS NOT NULL )"#))
                .check(Expr::cust(r#"owner_kind IN ( 'principal' , 'agent' , 'reserved' )"#))
                .check(Expr::cust(r#"length ( nickname_key ) BETWEEN 2 AND 32 AND nickname_key !~ '[^a-z0-9._-]' COLLATE "C""#))
                .to_owned(),
        ).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables(
            manager,
            &[
                "actor_nickname_index",
                "authorization_change_feed",
                "authorization_policy_state",
                "principal_avatar",
                "principal_avatar_revision",
                "audit_event",
                "invitation_workspace_grant",
                "invitation",
                "workspace_membership",
                "auth_refresh_credential",
                "auth_session",
                "device",
                "gateway_principal",
                "gateway_identity",
                "workspace",
            ],
        )
        .await
    }
}
