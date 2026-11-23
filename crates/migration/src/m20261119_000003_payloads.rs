use crate::drop_tables;
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::{
    big_integer, big_integer_null, binary, binary_null, boolean, json, small_integer,
    small_integer_null, text, text_null, timestamp_with_time_zone, timestamp_with_time_zone_null,
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
                    .col(text("metadata_json").default("{}"))
                    .check(Expr::cust("metadata_json IS JSON"))
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
                    .col(text("metadata_json").default("{}"))
                    .check(Expr::cust("metadata_json IS JSON"))
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
                    .col(text("metadata_json").default("{}"))
                    .check(Expr::cust("metadata_json IS JSON"))
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
                    .col(text("metadata_json").default("{}"))
                    .check(Expr::cust("metadata_json IS JSON"))
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
                    .col(text("metadata_json").default("{}"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust("metadata_json IS JSON"))
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
                    .col(text("outcome_json"))
                    .check(Expr::cust("outcome_json IS JSON"))
                    .col(text("changes_json"))
                    .check(Expr::cust("changes_json IS JSON"))
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
                    .col(text("change_json"))
                    .check(Expr::cust("change_json IS JSON"))
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
                    .col(text("line_endings_json"))
                    .check(Expr::cust("line_endings_json IS JSON"))
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

        // Startup reconciliation decodes immutable patch records between
        // bounded transactions. Its scratch data cannot live on a pooled
        // connection and is scoped to the durable scan identifier.
        manager
            .create_table(
                Table::create()
                    .table("patch_snapshot_scan")
                    .col(text("id").primary_key())
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust("statement_timestamp()")),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("patch_snapshot_scan_age")
                    .table("patch_snapshot_scan")
                    .col("updated_at")
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table("patch_snapshot_scan_reference")
                    .col(text("scan_id"))
                    .col(text("domain_id").extra(r#"COLLATE "C""#))
                    .col(binary("content_hash"))
                    .col(big_integer("byte_len"))
                    .col(big_integer("ref_count"))
                    .primary_key(
                        Index::create()
                            .col("scan_id")
                            .col("domain_id")
                            .col("content_hash")
                            .col("byte_len"),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from("patch_snapshot_scan_reference", "scan_id")
                            .to("patch_snapshot_scan", "id")
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check(Expr::cust("ref_count > 0 AND byte_len >= 0"))
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
                    .col(text("operations_json"))
                    .check(Expr::cust("operations_json IS JSON"))
                    .col(text("recovery_json"))
                    .check(Expr::cust("recovery_json IS JSON"))
                    .col(text("progress_json"))
                    .check(Expr::cust("progress_json IS JSON"))
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
                    .col(text("operations_json"))
                    .check(Expr::cust("operations_json IS JSON"))
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
                    .col(text("coverage_json"))
                    .check(Expr::cust("coverage_json IS JSON"))
                    .col(big_integer_null("applied_through_ordinal"))
                    .col(big_integer("record_count"))
                    .col(big_integer("final_state").default(0))
                    .col(text("state_json"))
                    .check(Expr::cust("state_json IS JSON"))
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
                    .col(text_null("stream"))
                    .col(binary("text"))
                    .col(ColumnDef::new("text_encoding").small_integer().not_null())
                    .col(big_integer("text_original_bytes"))
                    .col(big_integer("text_original_chars"))
                    .col(binary("text_sha256"))
                    .col(text_null("metadata"))
                    .check(Expr::cust("text_original_bytes BETWEEN 0 AND 131072"))
                    .check(Expr::cust("text_original_chars BETWEEN 0 AND text_original_bytes"))
                    .check(Expr::cust("octet_length(text_sha256) = 32"))
                    .check(Expr::cust("(text_encoding = 0 AND octet_length(text) = text_original_bytes) OR (text_encoding = 1 AND octet_length(text) + 2 < text_original_bytes)"))
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

        // SeaQuery does not expose PostgreSQL column storage strategy.
        manager
            .get_connection()
            .execute_unprepared(include_str!("sql/m20261119_000003_payloads/storage.sql"))
            .await?;

        create_portable_payload_storage(manager).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables(
            manager,
            &[
                "portable_payload_part",
                "portable_payload_value",
                "tool_output_chunk",
                "turn_diff_state",
                "patch_snapshot_reservation",
                "patch_commit_terminal",
                "patch_commit_intent",
                "patch_snapshot_scan_reference",
                "patch_snapshot_scan",
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

async fn create_portable_payload_storage(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("portable_payload_value")
                .col(text("id").primary_key())
                .col(big_integer("original_bytes"))
                .col(big_integer("original_chars"))
                .col(binary("sha256"))
                .col(small_integer_null("codec"))
                .col(binary_null("body"))
                .col(boolean("turn_item_shell").default(false))
                .check(Expr::cust("original_chars BETWEEN 0 AND original_bytes"))
                .check(Expr::cust("octet_length(sha256) = 32"))
                .check(Expr::cust(
                    "CASE WHEN original_bytes <= 65536 THEN
                         codec IS NOT NULL AND body IS NOT NULL
                         AND ((codec = 0 AND octet_length(body) = original_bytes)
                             OR (codec = 1 AND octet_length(body) + 2 < original_bytes))
                     ELSE codec IS NULL AND body IS NULL END",
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("portable_payload_part")
                .col(text("payload_id"))
                .col(big_integer("byte_offset"))
                .col(big_integer("char_offset"))
                .col(big_integer("original_bytes"))
                .col(big_integer("original_chars"))
                .col(binary("sha256"))
                .col(small_integer("codec"))
                .col(binary("body"))
                .primary_key(Index::create().col("payload_id").col("byte_offset"))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_portable_payload_part_value")
                        .from("portable_payload_part", "payload_id")
                        .to("portable_payload_value", "id")
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check(Expr::cust("byte_offset >= 0 AND char_offset BETWEEN 0 AND byte_offset"))
                .check(Expr::cust("original_bytes BETWEEN 1 AND 65536 AND original_chars BETWEEN 1 AND original_bytes"))
                .check(Expr::cust("octet_length(sha256) = 32"))
                .check(Expr::cust(
                    "(codec = 0 AND octet_length(body) = original_bytes)
                     OR (codec = 1 AND octet_length(body) + 2 < original_bytes)",
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .name("portable_payload_part_chars")
                .table("portable_payload_part")
                .col("payload_id")
                .col("char_offset")
                .unique()
                .to_owned(),
        )
        .await?;
    // SeaQuery does not expose PostgreSQL's per-column TOAST storage strategy.
    manager
        .get_connection()
        .execute_unprepared(include_str!(
            "sql/m20261119_000003_payloads/portable_payload_storage.sql"
        ))
        .await?;
    Ok(())
}
