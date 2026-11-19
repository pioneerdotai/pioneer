use crate::drop_tables;
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::{
    big_integer, big_integer_null, binary, integer, json, json_binary, text, text_null,
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
                    .table("artifact_blob")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("sha256"))
                    .col(big_integer("size_bytes"))
                    .col(text_null("mime_type"))
                    .col(text("storage_backend"))
                    .col(text("storage_key"))
                    .col(text_null("encryption_key_id"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone_null("last_verified_at"))
                    .col(json("metadata_json").default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_artifact_blob_workspace_sha_size_backend")
                    .table("artifact_blob")
                    .unique()
                    .col("workspace_id")
                    .col("sha256")
                    .col("size_bytes")
                    .col("storage_backend")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_blob_workspace_sha256")
                    .table("artifact_blob")
                    .col("workspace_id")
                    .col("sha256")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("artifact")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text_null("primary_thread_id"))
                    .col(text_null("current_version_id"))
                    .col(text("display_name"))
                    .col(text("kind"))
                    .col(text_null("mime_type"))
                    .col(text("status"))
                    .col(text("created_by_kind"))
                    .col(text_null("created_by_actor_id"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone_null("deleted_at"))
                    .col(json("metadata_json").default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_workspace_created")
                    .table("artifact")
                    .col("workspace_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_primary_thread")
                    .table("artifact")
                    .col("workspace_id")
                    .col("primary_thread_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_status")
                    .table("artifact")
                    .col("workspace_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("artifact_html_preview_discovery")
                    .table("artifact")
                    .col("id")
                    .and_where(Expr::cust(
                        r#""created_by_kind" = 'tool' AND "created_by_actor_id" = 'html_preview'"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("artifact_version")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("artifact_id"))
                    .col(big_integer("version_number"))
                    .col(text("blob_id"))
                    .col(text_null("source_uri"))
                    .col(text_null("source_path_redacted"))
                    .col(text_null("created_by_turn_id"))
                    .col(text_null("created_by_message_id"))
                    .col(text_null("created_by_turn_item_id"))
                    .col(text_null("created_by_tool_call_id"))
                    .col(text_null("created_by_task_id"))
                    .col(text_null("created_by_task_run_id"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(json("metadata_json").default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_artifact_version_number")
                    .table("artifact_version")
                    .unique()
                    .col("workspace_id")
                    .col("artifact_id")
                    .col("version_number")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_version_blob")
                    .table("artifact_version")
                    .col("workspace_id")
                    .col("blob_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_version_turn")
                    .table("artifact_version")
                    .col("workspace_id")
                    .col("created_by_turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("artifact_binding")
                    .col(text("id").primary_key())
                    .col(text("artifact_id"))
                    .col(text_null("artifact_version_id"))
                    .col(text("workspace_id"))
                    .col(text_null("thread_id"))
                    .col(text_null("turn_id"))
                    .col(text_null("message_id"))
                    .col(text_null("turn_item_id"))
                    .col(text_null("tool_call_id"))
                    .col(text_null("task_id"))
                    .col(text_null("task_run_id"))
                    .col(text("binding_kind"))
                    .col(text("direction"))
                    .col(big_integer_null("item_index"))
                    .col(text_null("role"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(json("metadata_json").default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_binding_thread")
                    .table("artifact_binding")
                    .col("workspace_id")
                    .col("thread_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_binding_turn")
                    .table("artifact_binding")
                    .col("workspace_id")
                    .col("turn_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_binding_message")
                    .table("artifact_binding")
                    .col("workspace_id")
                    .col("message_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_binding_task")
                    .table("artifact_binding")
                    .col("workspace_id")
                    .col("task_id")
                    .col("task_run_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_binding_artifact")
                    .table("artifact_binding")
                    .col("artifact_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("artifact_external_ref")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("artifact_id"))
                    .col(text_null("artifact_version_id"))
                    .col(text("provider"))
                    .col(text_null("model_family"))
                    .col(text("transport_kind"))
                    .col(text("external_id"))
                    .col(text_null("external_uri"))
                    .col(timestamp_with_time_zone_null("expires_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(json("metadata_json").default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_artifact_external_ref")
                    .table("artifact_external_ref")
                    .unique()
                    .col("workspace_id")
                    .col("artifact_id")
                    .col("artifact_version_id")
                    .col("provider")
                    .col("model_family")
                    .col("transport_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_external_ref_artifact")
                    .table("artifact_external_ref")
                    .col("workspace_id")
                    .col("artifact_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_external_ref_expiry")
                    .table("artifact_external_ref")
                    .col("workspace_id")
                    .col("expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("artifact_projection")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("artifact_id"))
                    .col(text("artifact_version_id"))
                    .col(text("projection_kind"))
                    .col(text("status"))
                    .col(text_null("text_content"))
                    .col(text_null("blob_id"))
                    .col(json("metadata_json").default("{}"))
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
                    .name("idx_artifact_projection_artifact")
                    .table("artifact_projection")
                    .col("workspace_id")
                    .col("artifact_id")
                    .col("projection_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_projection_status")
                    .table("artifact_projection")
                    .col("workspace_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("artifact_upload_session")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text_null("thread_id"))
                    .col(text_null("turn_id"))
                    .col(text_null("message_id"))
                    .col(text("display_name"))
                    .col(big_integer_null("expected_size_bytes"))
                    .col(text_null("expected_sha256"))
                    .col(text_null("mime_type"))
                    .col(text("status"))
                    .col(text("temp_storage_key"))
                    .col(big_integer("received_bytes").default(0))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone("expires_at"))
                    .col(text("metadata_json").default("{}"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_upload_session_workspace_status")
                    .table("artifact_upload_session")
                    .col("workspace_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_artifact_upload_session_expiry")
                    .table("artifact_upload_session")
                    .col("workspace_id")
                    .col("expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("applied_patch_record")
                    .col(text("id").primary_key())
                    .col(big_integer("schema_version"))
                    .col(text("thread_id"))
                    .col(text("turn_id").extra(r#"COLLATE "C""#))
                    .col(text("invocation_id"))
                    .col(text("environment_id").default(""))
                    .col(big_integer("commit_ordinal"))
                    .col(text("authority").default("native_patch_engine"))
                    .col(text("provenance").default("native_engine"))
                    .col(text("exactness").default("uncertain"))
                    .col(big_integer("committed_at_unix_ms").default(0))
                    .col(binary("plan_fingerprint"))
                    .col(json("outcome_json"))
                    .col(json("changes_json"))
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
                    .name("uq_applied_patch_record_invocation")
                    .table("applied_patch_record")
                    .unique()
                    .col("thread_id")
                    .col("turn_id")
                    .col("invocation_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_applied_patch_record_commit_ordinal")
                    .table("applied_patch_record")
                    .unique()
                    .col("thread_id")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_applied_patch_record_thread_turn_ordinal")
                    .table("applied_patch_record")
                    .col("thread_id")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("applied_patch_change_index")
                    .col(text("record_id"))
                    .col(text("thread_id"))
                    .col(text("turn_id").extra(r#"COLLATE "C""#))
                    .col(text("invocation_id"))
                    .col(text("environment_id").extra(r#"COLLATE "C""#))
                    .col(big_integer("commit_ordinal"))
                    .col(big_integer("sequence"))
                    .col(text("source_path"))
                    .col(text_null("destination_path"))
                    .col(json("change_json"))
                    .primary_key(Index::create().col("record_id").col("sequence"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_patch_change_index_commit_sequence")
                    .table("applied_patch_change_index")
                    .unique()
                    .col("thread_id")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_patch_change_index_thread_source_order")
                    .table("applied_patch_change_index")
                    .col("thread_id")
                    .col("source_path")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_patch_change_index_thread_destination_order")
                    .table("applied_patch_change_index")
                    .col("thread_id")
                    .col("destination_path")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_patch_change_index_environment_source_order")
                    .table("applied_patch_change_index")
                    .col("thread_id")
                    .col("environment_id")
                    .col("source_path")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_patch_change_index_environment_destination_order")
                    .table("applied_patch_change_index")
                    .col("thread_id")
                    .col("environment_id")
                    .col("destination_path")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("patch_snapshot")
                    .col(text("domain_id"))
                    .col(binary("content_hash"))
                    .col(big_integer("byte_len"))
                    .col(text("encoding"))
                    .col(json_binary("line_endings_json"))
                    .col(binary("compressed_bytes"))
                    .col(big_integer("raw_byte_len"))
                    .col(big_integer("ref_count").default(0))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .primary_key(
                        Index::create()
                            .col("domain_id")
                            .col("content_hash")
                            .col("byte_len"),
                    )
                    .check(Expr::cust(r#""raw_byte_len" >= 0"#))
                    .check(Expr::cust(r#""ref_count" >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("patch_commit_intent")
                    .col(text("thread_id"))
                    .col(text("turn_id").extra(r#"COLLATE "C""#))
                    .col(text("invocation_id"))
                    .col(big_integer("commit_ordinal"))
                    .col(binary("plan_fingerprint"))
                    .col(json("operations_json"))
                    .col(json("recovery_json"))
                    .col(json("progress_json"))
                    .col(text("status"))
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .primary_key(
                        Index::create()
                            .col("thread_id")
                            .col("turn_id")
                            .col("invocation_id"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_patch_commit_intent_commit_ordinal")
                    .table("patch_commit_intent")
                    .unique()
                    .col("thread_id")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_patch_commit_intent_pending_order")
                    .table("patch_commit_intent")
                    .col("status")
                    .col("updated_at")
                    .col("thread_id")
                    .col("turn_id")
                    .col("invocation_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("patch_commit_terminal")
                    .col(text("thread_id"))
                    .col(text("turn_id").extra(r#"COLLATE "C""#))
                    .col(text("invocation_id"))
                    .col(big_integer("commit_ordinal"))
                    .col(binary("plan_fingerprint"))
                    .col(json("operations_json"))
                    .col(text("authority"))
                    .col(text("status"))
                    .col(text_null("record_id"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .primary_key(
                        Index::create()
                            .col("thread_id")
                            .col("turn_id")
                            .col("invocation_id"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_patch_commit_terminal_commit_ordinal")
                    .table("patch_commit_terminal")
                    .unique()
                    .col("thread_id")
                    .col("turn_id")
                    .col("commit_ordinal")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("patch_snapshot_reservation")
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(text("invocation_id"))
                    .col(big_integer("logical_bytes"))
                    .col(big_integer("physical_bytes"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .primary_key(
                        Index::create()
                            .col("thread_id")
                            .col("turn_id")
                            .col("invocation_id"),
                    )
                    .check(Expr::cust(r#""logical_bytes" >= 0"#))
                    .check(Expr::cust(r#""physical_bytes" >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_diff_state")
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(big_integer("revision"))
                    .col(text("authority"))
                    .col(big_integer("exact"))
                    .col(json("coverage_json"))
                    .col(big_integer_null("applied_through_ordinal"))
                    .col(big_integer("record_count"))
                    .col(big_integer("final_state").default(0))
                    .col(json("state_json"))
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .primary_key(Index::create().col("thread_id").col("turn_id"))
                    .check(Expr::cust(r#""exact" IN ( 0 , 1 )"#))
                    .check(Expr::cust(r#""final_state" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("tool_output_chunk")
                    .col(
                        big_integer("ordinal")
                            .primary_key()
                            .extra("GENERATED BY DEFAULT AS IDENTITY"),
                    )
                    .col(text("id"))
                    .col(text("turn_id"))
                    .col(text("item_id"))
                    .col(text("stream"))
                    .col(text("text"))
                    .col(text_null("metadata"))
                    .col(big_integer("portable_revision").default(0))
                    .col(text("portable_identity").default(""))
                    .col(text_null("text_payload_id"))
                    .col(text_null("metadata_payload_id"))
                    .index(Index::create().unique().col("id"))
                    .check(Expr::cust(r#""stream" IN ('agent_message', 'stdout', 'stderr', 'tool_progress', 'file_change', 'generic')"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("tool_output_chunk_turn_item")
                    .table("tool_output_chunk")
                    .col("turn_id")
                    .col("item_id")
                    .col("ordinal")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("tool_output_chunk_metadata_portable_pending")
                    .table("tool_output_chunk")
                    .col("metadata_payload_id")
                    .col("id")
                    .and_where(Expr::cust(
                        r#""metadata_payload_id" IS NULL AND "metadata" IS NOT NULL"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("tool_output_chunk_text_portable_pending")
                    .table("tool_output_chunk")
                    .col("text_payload_id")
                    .col("id")
                    .and_where(Expr::cust(r#""text_payload_id" IS NULL"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("portable_payload_dictionary")
                    .col(text("id").primary_key())
                    .col(text("family"))
                    .col(integer("version"))
                    .col(binary("data"))
                    .check(Expr::cust(r#"length ( data ) BETWEEN 1 AND 32768"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("portable_payload_dictionary_family_version")
                    .table("portable_payload_dictionary")
                    .unique()
                    .col("family")
                    .col("version")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("portable_payload")
                .col(text("id").primary_key())
                .col(text("family"))
                .col(big_integer("original_bytes"))
                .col(big_integer("original_chars"))
                .col(binary("sha256"))
                .col(integer("raw"))
                .col(integer("retired").default(1))
                .col(text_null("dictionary_id"))
                .check(Expr::cust(r#""original_bytes" >= 0"#))
                .check(Expr::cust(r#"original_chars >= 0 AND original_chars <= original_bytes"#))
                .check(Expr::cust(r#"( length ( sha256 ) = 32 OR ( retired = 1 AND length ( sha256 ) = 0 ) )"#))
                .check(Expr::cust(r#""raw" IN ( 0 , 1 )"#))
                .check(Expr::cust(r#""retired" IN ( 0 , 1 )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("portable_payload_retired")
                    .table("portable_payload")
                    .col("retired")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("portable_payload_block")
                    .col(text("payload_id"))
                    .col(big_integer("ordinal"))
                    .col(big_integer("byte_offset"))
                    .col(big_integer("char_offset"))
                    .col(big_integer("original_bytes"))
                    .col(big_integer("original_chars"))
                    .col(binary("data"))
                    .primary_key(Index::create().col("payload_id").col("ordinal"))
                    .check(Expr::cust(r#""ordinal" >= 0"#))
                    .check(Expr::cust(r#""byte_offset" >= 0"#))
                    .check(Expr::cust(r#""char_offset" >= 0"#))
                    .check(Expr::cust(r#""original_bytes" BETWEEN 1 AND 65536"#))
                    .check(Expr::cust(
                        r#"original_chars >= 1 AND original_chars <= original_bytes"#,
                    ))
                    .check(Expr::cust(r#"length ( data ) BETWEEN 78 AND 66638"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("portable_payload_block_bytes")
                    .table("portable_payload_block")
                    .col("payload_id")
                    .col("byte_offset")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("portable_payload_block_chars")
                    .table("portable_payload_block")
                    .col("payload_id")
                    .col("char_offset")
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables(
            manager,
            &[
                "portable_payload_block",
                "portable_payload",
                "portable_payload_dictionary",
                "tool_output_chunk",
                "turn_diff_state",
                "patch_snapshot_reservation",
                "patch_commit_terminal",
                "patch_commit_intent",
                "patch_snapshot",
                "applied_patch_change_index",
                "applied_patch_record",
                "artifact_upload_session",
                "artifact_projection",
                "artifact_external_ref",
                "artifact_binding",
                "artifact_version",
                "artifact",
                "artifact_blob",
            ],
        )
        .await
    }
}
