use crate::drop_tables;
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::{
    big_integer, big_integer_null, boolean, boolean_null, json, text, text_null,
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
                    .table("skill_upload_session")
                    .col(text("upload_id").primary_key())
                    .col(text("workspace_id"))
                    .col(big_integer("connection_id"))
                    .col(text("status"))
                    .col(text("file_name"))
                    .col(text("archive_format"))
                    .col(big_integer("compressed_size_bytes"))
                    .col(big_integer("received_bytes").default(0))
                    .col(text("sha256"))
                    .col(text("payload_path"))
                    .col(big_integer("created_at_unix"))
                    .col(big_integer("expires_at_unix"))
                    .col(big_integer_null("finalized_at_unix"))
                    .col(big_integer_null("consumed_at_unix"))
                    .col(big_integer_null("aborted_at_unix"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text("purpose").default("skill"))
                    .check(Expr::cust(r#""purpose" IN ( 'skill' , 'plugin' , 'app' )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_upload_session_workspace_status")
                    .table("skill_upload_session")
                    .col("workspace_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_upload_session_expires_at")
                    .table("skill_upload_session")
                    .col("expires_at_unix")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_upload_session_connection")
                    .table("skill_upload_session")
                    .col("connection_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("mcp_server_installation")
                    .col(text("id").primary_key())
                    .col(text("scope_kind"))
                    .col(text("scope_key"))
                    .col(text("name"))
                    .col(text_null("display_name"))
                    .col(text("source_kind"))
                    .col(json("source_ref"))
                    .col(text("transport_kind"))
                    .col(json("transport_json"))
                    .col(json("auth_json").default("{}"))
                    .col(json("secret_refs_json").default("[]"))
                    .col(boolean("enabled").default(true))
                    .col(boolean("allow_implicit_invocation").default(true))
                    .col(boolean("required").default(false))
                    .col(text("fingerprint"))
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
                    .name("uq_mcp_server_installation_scope_name")
                    .table("mcp_server_installation")
                    .unique()
                    .col("scope_kind")
                    .col("scope_key")
                    .col("name")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_mcp_server_installation_scope")
                    .table("mcp_server_installation")
                    .col("scope_kind")
                    .col("scope_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_mcp_server_installation_transport")
                    .table("mcp_server_installation")
                    .col("transport_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("mcp_server_catalog_snapshot")
                    .col(text("id").primary_key())
                    .col(text("server_installation_id"))
                    .col(text("catalog_version"))
                    .col(json("server_info_json").default("{}"))
                    .col(text_null("server_instructions_hash"))
                    .col(json("tools_json").default("[]"))
                    .col(json("resources_json").default("[]"))
                    .col(json("resource_templates_json").default("[]"))
                    .col(json("prompts_json").default("[]"))
                    .col(
                        timestamp_with_time_zone("generated_at")
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
                    .name("uq_mcp_server_catalog_snapshot_server")
                    .table("mcp_server_catalog_snapshot")
                    .unique()
                    .col("server_installation_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_mcp_server_catalog_snapshot_generated_at")
                    .table("mcp_server_catalog_snapshot")
                    .col("generated_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("mcp_audit_event")
                    .col(text("id").primary_key())
                    .col(text_null("turn_id"))
                    .col(text_null("server_installation_id"))
                    .col(text("server_name"))
                    .col(text_null("raw_tool_name"))
                    .col(text_null("callable_name"))
                    .col(text_null("catalog_version"))
                    .col(text("action"))
                    .col(text("decision"))
                    .col(text_null("reason_code"))
                    .col(json("details_json").default("{}"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_mcp_audit_event_server_created_at")
                    .table("mcp_audit_event")
                    .col("server_name")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_mcp_audit_event_turn_server")
                    .table("mcp_audit_event")
                    .col("turn_id")
                    .col("server_name")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_mcp_audit_event_action_created_at")
                    .table("mcp_audit_event")
                    .col("action")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_agents_doc")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text_null("folder_id"))
                    .col(text("status"))
                    .col(text("title").default("AGENTS.md"))
                    .col(text("content").default(""))
                    .col(text("content_sha256"))
                    .col(big_integer("version").default(1))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone_null("archived_at"))
                    .col(text_null("created_by"))
                    .col(text_null("updated_by"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_thread_agents_doc_root_active")
                    .table("thread_agents_doc")
                    .unique()
                    .col("workspace_id")
                    .and_where(Expr::cust(
                        r#""folder_id" IS NULL AND "status" IN ('draft', 'active')"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_thread_agents_doc_folder_active")
                    .table("thread_agents_doc")
                    .unique()
                    .col("workspace_id")
                    .col("folder_id")
                    .and_where(Expr::cust(
                        r#""folder_id" IS NOT NULL AND "status" IN ('draft', 'active')"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_agents_doc_workspace_folder_status")
                    .table("thread_agents_doc")
                    .col("workspace_id")
                    .col("folder_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_agents_doc_workspace_status_updated")
                    .table("thread_agents_doc")
                    .col("workspace_id")
                    .col("status")
                    .col("updated_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_agents_doc_revision")
                    .col(text("id").primary_key())
                    .col(text("doc_id"))
                    .col(big_integer("version"))
                    .col(text("content_sha256"))
                    .col(text("content"))
                    .col(
                        timestamp_with_time_zone("saved_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text_null("saved_by"))
                    .col(text("save_reason"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_thread_agents_doc_revision_doc_version")
                    .table("thread_agents_doc_revision")
                    .unique()
                    .col("doc_id")
                    .col("version")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_agents_doc_revision_doc_version")
                    .table("thread_agents_doc_revision")
                    .col("doc_id")
                    .col("version")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("skill_workspace_policy")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("skill_id"))
                    .col(boolean_null("enabled"))
                    .col(boolean_null("allow_implicit_invocation"))
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
                    .name("idx_skill_workspace_policy_workspace")
                    .table("skill_workspace_policy")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_skill_workspace_policy_workspace_skill_id")
                    .table("skill_workspace_policy")
                    .unique()
                    .col("workspace_id")
                    .col("skill_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("skill_audit_event")
                    .col(text("id").primary_key())
                    .col(text_null("turn_id"))
                    .col(text("skill_id"))
                    .col(text_null("skill_owner"))
                    .col(text("skill_slug"))
                    .col(text("source_kind"))
                    .col(text("action"))
                    .col(text("decision"))
                    .col(text_null("reason_code"))
                    .col(text("details_json").default("{}"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust("details_json IS JSON"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_audit_event_skill_id_created_at")
                    .table("skill_audit_event")
                    .col("skill_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_audit_event_turn_id")
                    .table("skill_audit_event")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("skill_dependency_snapshot")
                    .col(text("id").primary_key())
                    .col(text_null("turn_id"))
                    .col(text("skill_id"))
                    .col(text_null("skill_owner"))
                    .col(text("skill_slug"))
                    .col(text("source_kind"))
                    .col(text("diagnostics_json").default("[]"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust("diagnostics_json IS JSON"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_dependency_snapshot_skill_id_created_at")
                    .table("skill_dependency_snapshot")
                    .col("skill_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_dependency_snapshot_turn_id")
                    .table("skill_dependency_snapshot")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("skill_pack_installation")
                    .col(text("id").primary_key())
                    .col(text("name"))
                    .col(text("scope_key"))
                    .col(text("source_kind"))
                    .col(timestamp_with_time_zone("created_at"))
                    .col(timestamp_with_time_zone("updated_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_pack_installation_scope_name_id")
                    .table("skill_pack_installation")
                    .col("scope_key")
                    .col("name")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("skill_installation")
                .col(text("id").primary_key())
                .col(text_null("owner"))
                .col(text("slug"))
                .col(text_null("version"))
                .col(text("source_kind"))
                .col(text("scope_key"))
                .col(text("source_ref"))
                .col(text("install_path"))
                .col(text("trust_level"))
                .col(text("fingerprint"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(text_null("pack_id"))
                .col(text_null("pack_member_key"))
                .check(Expr::cust(r#"( ( pack_id IS NULL AND pack_member_key IS NULL ) OR ( pack_id IS NOT NULL AND pack_member_key IS NOT NULL ) )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_installation_scope_source")
                    .table("skill_installation")
                    .col("scope_key")
                    .col("source_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_installation_source_ref")
                    .table("skill_installation")
                    .col("source_ref")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_installation_pack_member_id")
                    .table("skill_installation")
                    .col("pack_id")
                    .col("pack_member_key")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_skill_installation_pack_member_key")
                    .table("skill_installation")
                    .unique()
                    .col("pack_id")
                    .col("pack_member_key")
                    .and_where(Expr::cust(r#"pack_id IS NOT NULL"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_installation_source_scope_id")
                    .table("skill_installation")
                    .col("source_kind")
                    .col("scope_key")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_skill_installation_import_provenance")
                    .table("skill_installation")
                    .col("source_kind")
                    .col("scope_key")
                    .col("source_ref")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("package_installation")
                .col(text("id").primary_key())
                .col(text("workspace_id"))
                .col(text("source_upload_id"))
                .col(text("package_path"))
                .col(text("data_path"))
                .col(text("package_fingerprint"))
                .col(boolean("enabled").default(true))
                .col(boolean("allow_implicit_invocation").default(false))
                .col(text("state"))
                .col(big_integer("revision"))
                .col(text_null("pending_json"))
                .col(text_null("last_error_json"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .col(text("kind").default("plugin"))
                .index(Index::create().unique().col("source_upload_id"))
                .check(Expr::cust(r#""state" IN ( 'installing' , 'installed' , 'updating' , 'removing' , 'interrupted' )"#))
                .check(Expr::cust(r#""revision" > 0"#))
                .check(Expr::cust("pending_json IS NULL OR (pending_json IS JSON AND octet_length(pending_json) <= 65536)"))
                .check(Expr::cust("last_error_json IS NULL OR last_error_json IS JSON"))
                .check(Expr::cust(r#""kind" IN ( 'plugin' , 'app' )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("ix_package_workspace_kind")
                    .table("package_installation")
                    .col("workspace_id")
                    .col("kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("plugin_keyword")
                    .col(text("plugin_id"))
                    .col(big_integer("position"))
                    .col(text("keyword"))
                    .primary_key(Index::create().col("plugin_id").col("position"))
                    .check(Expr::cust(r#""position" >= 0 AND "position" < 64"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("ix_plugin_keyword_value")
                    .table("plugin_keyword")
                    .col("keyword")
                    .col("plugin_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("plugin_default_prompt")
                    .col(text("plugin_id"))
                    .col(big_integer("position"))
                    .col(text("prompt"))
                    .primary_key(Index::create().col("plugin_id").col("position"))
                    .check(Expr::cust(r#""position" >= 0 AND "position" < 12"#))
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("package_component")
                .col(text("package_id"))
                .col(text("kind"))
                .col(text("member_key"))
                .col(text_null("member_path"))
                .col(text_null("skill_id"))
                .col(text_null("mcp_installation_id"))
                .col(text_null("package_fingerprint"))
                .col(text("status"))
                .col(text_null("diagnostic"))
                .col(text("override_fields_json").default("[]"))
                .check(Expr::cust("override_fields_json IS JSON ARRAY"))
                .primary_key(Index::create().col("package_id").col("kind").col("member_key"))
                .index(Index::create().unique().col("mcp_installation_id"))
                .index(Index::create().unique().col("skill_id"))
                .check(Expr::cust(r#""kind" IN ( 'skill' , 'mcp' )"#))
                .check(Expr::cust(r#""status" IN ( 'installed' , 'invalid' , 'failed' , 'removed_by_user' )"#))
                .check(Expr::cust(r#"( "kind" = 'skill' AND "mcp_installation_id" IS NULL ) OR ( "kind" = 'mcp' AND "skill_id" IS NULL )"#))
                .to_owned(),
        ).await?;

        manager
            .create_table(
                Table::create()
                    .table("plugin_installation")
                    .col(text("id").primary_key())
                    .col(text("name"))
                    .col(text_null("version"))
                    .col(text_null("display_name"))
                    .col(text_null("short_description"))
                    .col(text_null("description"))
                    .col(text_null("long_description"))
                    .col(text_null("developer_name"))
                    .col(text_null("author_name"))
                    .col(text_null("author_email"))
                    .col(text_null("author_url"))
                    .col(text_null("homepage"))
                    .col(text_null("repository"))
                    .col(text_null("documentation_url"))
                    .col(text_null("support_url"))
                    .col(text_null("privacy_policy_url"))
                    .col(text_null("terms_of_service_url"))
                    .col(text_null("license"))
                    .col(text_null("category"))
                    .col(text_null("brand_color"))
                    .col(text_null("logo_path"))
                    .col(text_null("logo_revision"))
                    .col(text_null("composer_icon_path"))
                    .col(text_null("composer_icon_revision"))
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("app_installation")
                .col(text("id").primary_key())
                .col(text("slug"))
                .col(text("manifest_json"))
                .col(text("localizations_json").default("{}"))
                .check(Expr::cust(r#"CASE WHEN (manifest_json IS JSON) THEN json_typeof(manifest_json::json) = 'object' AND json_typeof(manifest_json::json -> 'slug') = 'string' AND ((manifest_json)::json #>> '{slug}') = slug ELSE FALSE END"#))
                .check(Expr::cust(r#"CASE WHEN (localizations_json IS JSON) THEN json_typeof(localizations_json::json) = 'object' ELSE FALSE END"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("ix_app_slug")
                    .table("app_installation")
                    .col("slug")
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables(
            manager,
            &[
                "app_installation",
                "plugin_installation",
                "package_component",
                "plugin_default_prompt",
                "plugin_keyword",
                "package_installation",
                "skill_installation",
                "skill_pack_installation",
                "skill_dependency_snapshot",
                "skill_audit_event",
                "skill_workspace_policy",
                "thread_agents_doc_revision",
                "thread_agents_doc",
                "mcp_audit_event",
                "mcp_server_catalog_snapshot",
                "mcp_server_installation",
                "skill_upload_session",
            ],
        )
        .await
    }
}
