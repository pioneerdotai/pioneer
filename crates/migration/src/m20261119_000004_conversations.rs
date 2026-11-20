use crate::drop_tables;
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::{
    big_integer, big_integer_null, binary, boolean, json, json_null, small_integer, text,
    text_null, timestamp_with_time_zone, timestamp_with_time_zone_null,
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.create_table(
            Table::create().table("thread")
                .col(text("id").primary_key().extra(r#"COLLATE "C""#))
                .col(text("workspace_id"))
                .col(text_null("name"))
                .col(text("preview"))
                .col(text("mode"))
                .col(text("model"))
                .col(text("model_provider"))
                .col(text("status"))
                .col(text("origin_kind").default("user"))
                .col(text("sidebar_visibility").default("visible"))
                .col(text_null("agent_nickname"))
                .col(text_null("agent_role"))
                .col(text_null("summary"))
                .col(big_integer_null("summary_turn_count").default(0))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(text_null("created_by_actor_id"))
                .col(text("access_class").default("private"))
                .col(text_null("preview_author_json"))
                .check(Expr::cust("preview_author_json IS NULL OR preview_author_json IS JSON"))
                .col(text_null("created_by_actor_kind"))
                .col(big_integer_null("inbox_workspace_after_sequence"))
                .col(timestamp_with_time_zone_null("inbox_workspace_started_at"))
                .col(big_integer("workspace_unread_started_at").default(0))
                .check(Expr::cust(r#"access_class IN ( 'private' , 'workspace' , 'internal' )"#))
                .check(Expr::cust(r#"( created_by_actor_kind IS NULL AND created_by_actor_id IS NULL ) OR ( created_by_actor_kind = 'system' AND created_by_actor_id IS NULL ) OR ( created_by_actor_kind = 'principal' AND created_by_actor_id IS NOT NULL AND length ( created_by_actor_id ) = 21 AND created_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C" ) OR ( created_by_actor_kind = 'agent_execution' AND created_by_actor_id IS NOT NULL AND length ( created_by_actor_id ) = 21 AND created_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C" )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_workspace_sidebar_visibility")
                    .table("thread")
                    .col("workspace_id")
                    .col("sidebar_visibility")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_origin_kind")
                    .table("thread")
                    .col("origin_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_workspace_id")
                    .table("thread")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_workspace_access")
                    .table("thread")
                    .col("workspace_id")
                    .col("access_class")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_workspace_created_id")
                    .table("thread")
                    .col("workspace_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_sandox_policy")
                    .col(text("thread_id").primary_key())
                    .col(text("mode"))
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
            .create_table(
                Table::create()
                    .table("thread_folder")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text_null("parent_folder_id"))
                    .col(text("name"))
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
                    .name("idx_thread_folder_workspace_id")
                    .table("thread_folder")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_folder_parent_folder_id")
                    .table("thread_folder")
                    .col("parent_folder_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_placement")
                    .col(text("thread_id").primary_key())
                    .col(text("workspace_id"))
                    .col(text_null("folder_id"))
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
                    .name("idx_thread_placement_workspace_id")
                    .table("thread_placement")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_placement_folder_id")
                    .table("thread_placement")
                    .col("folder_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_lineage")
                    .col(text("child_thread_id").primary_key())
                    .col(text("parent_thread_id"))
                    .col(text("root_thread_id"))
                    .col(big_integer("depth"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text_null("origin_kind"))
                    .col(text_null("created_by_thread_id"))
                    .col(text_null("created_by_turn_id"))
                    .col(history_publication_xid())
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_lineage_parent")
                    .table("thread_lineage")
                    .col("parent_thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_lineage_root_depth")
                    .table("thread_lineage")
                    .col("root_thread_id")
                    .col("depth")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_lineage_origin")
                    .table("thread_lineage")
                    .col("origin_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_lineage_created_by_thread")
                    .table("thread_lineage")
                    .col("created_by_thread_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("turn")
                .col(text("id").extra(r#"COLLATE "C""#).primary_key())
                .col(text("thread_id"))
                .col(text("status"))
                .col(text_null("error"))
                .col(text("prompt_manifest_json").default("{}"))
                .check(Expr::cust("prompt_manifest_json IS JSON"))
                .col(text_null("prompt_compiler_version"))
                .col(text_null("prompt_profile"))
                .col(text_null("prompt_fingerprint_stable"))
                .col(text_null("prompt_fingerprint_dynamic"))
                .col(text_null("prompt_fingerprint_full"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(text("turn_kind").default("conversation"))
                .col(text("origin").default("user"))
                .col(text_null("reasoning_effort"))
                .col(text_null("permission_profile_mode"))
                .col(text_null("permission_profile_source"))
                .col(text_null("permission_profile_snapshot_json"))
                .check(Expr::cust("permission_profile_snapshot_json IS NULL OR permission_profile_snapshot_json IS JSON"))
                .col(big_integer_null("execution_security_snapshot_version"))
                .col(text_null("execution_security_snapshot_json"))
                .check(Expr::cust("execution_security_snapshot_json IS NULL OR execution_security_snapshot_json IS JSON"))
                .col(text_null("initiated_by_actor_id"))
                .col(json_null("execution_authorization_context_json"))
                .col(text_null("send_mode"))
                .col(text_null("author_display_name_snapshot"))
                .col(text_null("author_nickname_snapshot"))
                .col(text_null("author_avatar_revision_snapshot"))
                .col(text_null("reply_to_turn_id"))
                .col(text("mentions_json").default("[]"))
                .check(Expr::cust("mentions_json IS JSON"))
                .col(big_integer("message_revision").default(0))
                .col(timestamp_with_time_zone_null("message_deleted_at"))
                .col(text_null("message_deleted_by_actor_id"))
                .col(text_null("message_deleted_by_actor_kind"))
                .col(text_null("author_agent_snapshot_json"))
                .check(Expr::cust("author_agent_snapshot_json IS NULL OR author_agent_snapshot_json IS JSON"))
                .col(text_null("initiated_by_actor_kind"))
                .col(text("work_owner").default("turn"))
                .col(text_null("package_selection_json"))
                .check(Expr::cust("package_selection_json IS NULL OR package_selection_json IS JSON"))
                .col(history_publication_xid())
                .col(ColumnDef::new("insertion_ordinal").big_integer().not_null().unique_key().extra("GENERATED ALWAYS AS IDENTITY"))
                .check(Expr::cust(r#"send_mode IS NULL OR send_mode IN ( 'message' , 'chat' , 'agent' )"#))
                .check(Expr::cust(r#"author_display_name_snapshot IS NULL OR length ( author_display_name_snapshot ) BETWEEN 1 AND 512"#))
                .check(Expr::cust(r#"author_nickname_snapshot IS NULL OR length ( author_nickname_snapshot ) BETWEEN 2 AND 32"#))
                .check(Expr::cust(r#"length ( mentions_json::text ) <= 65536"#))
                .check(Expr::cust(r#"message_revision >= 0"#))
                .check(Expr::cust(r#"( message_deleted_by_actor_kind IS NULL AND message_deleted_by_actor_id IS NULL ) OR ( message_deleted_by_actor_kind = 'system' AND message_deleted_by_actor_id IS NULL ) OR ( message_deleted_by_actor_kind = 'principal' AND length ( message_deleted_by_actor_id ) = 21 AND message_deleted_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C" )"#))
                .check(Expr::cust(r#"( initiated_by_actor_kind IS NULL AND initiated_by_actor_id IS NULL ) OR ( initiated_by_actor_kind = 'system' AND initiated_by_actor_id IS NULL ) OR ( initiated_by_actor_kind = 'principal' AND initiated_by_actor_id IS NOT NULL AND length ( initiated_by_actor_id ) = 21 AND initiated_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C" ) OR ( initiated_by_actor_kind = 'agent_execution' AND initiated_by_actor_id IS NOT NULL AND length ( initiated_by_actor_id ) = 21 AND initiated_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C" )"#))
                .check(Expr::cust(r#"work_owner IN ( 'turn' , 'detached_task' )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_thread_id")
                    .table("turn")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_prompt_fingerprint_full")
                    .table("turn")
                    .col("prompt_fingerprint_full")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_thread_created_id")
                    .table("turn")
                    .col("thread_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_thread_reply")
                    .table("turn")
                    .col("thread_id")
                    .col("reply_to_turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_thread_message_created")
                    .table("turn")
                    .col("thread_id")
                    .col("created_at")
                    .col("id")
                    .and_where(Expr::cust(r#"send_mode = 'message'"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_turn_history_order")
                    .table("turn")
                    .col("thread_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_task_child_parent_turn")
                    .table("turn")
                    .col("thread_id")
                    .col("turn_kind")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_input")
                    .col(text("id").primary_key())
                    .col(text("turn_id"))
                    .col(big_integer("input_index"))
                    .col(text("input_type"))
                    .col(text_null("text"))
                    .col(text("payload").default("{}"))
                    .check(Expr::cust("payload IS JSON"))
                    .col(history_publication_xid())
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_input_turn_id")
                    .table("turn_input")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_input_turn_id_input_index")
                    .table("turn_input")
                    .unique()
                    .col("turn_id")
                    .col("input_index")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_event")
                    .col(text("id").primary_key())
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(big_integer("sequence"))
                    .col(text("event_type"))
                    .col(json_null("payload"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text_null("idempotency_key"))
                    .col(big_integer("portable_revision").default(0))
                    .col(text("portable_identity").default(""))
                    .col(text_null("payload_payload_id").unique_key())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_turn_event_payload")
                            .from("turn_event", "payload_payload_id")
                            .to("portable_payload_value", "id"),
                    )
                    .check(Expr::cust(
                        "(payload IS NULL) <> (payload_payload_id IS NULL)",
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_events_turn_id_sequence")
                    .table("turn_event")
                    .unique()
                    .col("turn_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_events_thread_id")
                    .table("turn_event")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_event_turn_id_idempotency_key")
                    .table("turn_event")
                    .unique()
                    .col("turn_id")
                    .col("idempotency_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_root_completion_event")
                    .table("turn_event")
                    .col("turn_id")
                    .col("event_type")
                    .col(("created_at", IndexOrder::Desc))
                    .col(("id", IndexOrder::Desc))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_item")
                    .col(text("id").primary_key())
                    .col(text("turn_id"))
                    .col(text("item_id"))
                    .col(text("item_type"))
                    .col(text_null("status"))
                    .col(json_null("payload"))
                    .col(big_integer("active_attempt_number").default(0))
                    .col(text_null("active_attempt_status"))
                    .col(text_null("active_attempt_id"))
                    .col(timestamp_with_time_zone_null("last_heartbeat_at"))
                    .col(timestamp_with_time_zone_null("lease_expires_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(big_integer("portable_revision").default(0))
                    .col(text("portable_identity").default(""))
                    .col(text_null("payload_payload_id").unique_key())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_turn_item_payload")
                            .from("turn_item", "payload_payload_id")
                            .to("portable_payload_value", "id"),
                    )
                    .check(Expr::cust(
                        "(payload IS NULL) <> (payload_payload_id IS NULL)",
                    ))
                    .col(history_publication_xid())
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_item_turn_id")
                    .table("turn_item")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_item_turn_id_item_id")
                    .table("turn_item")
                    .unique()
                    .col("turn_id")
                    .col("item_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_root_ready_item")
                    .table("turn_item")
                    .col("turn_id")
                    .col("item_type")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_item_attempt")
                    .col(text("id").primary_key())
                    .col(text("turn_id"))
                    .col(text("item_id"))
                    .col(text("item_type"))
                    .col(big_integer("attempt_number"))
                    .col(text("status"))
                    .col(text_null("timeout_reason"))
                    .col(text_null("failure_reason"))
                    .col(text_null("recovery_action"))
                    .col(text_null("idempotency_key"))
                    .col(text_null("trace_id"))
                    .col(text("payload").default("{}"))
                    .col(
                        timestamp_with_time_zone("started_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone_null("last_heartbeat_at"))
                    .col(timestamp_with_time_zone_null("lease_expires_at"))
                    .col(timestamp_with_time_zone_null("idle_deadline_at"))
                    .col(timestamp_with_time_zone_null("hard_deadline_at"))
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(big_integer_null("started_event_sequence"))
                    .col(text_null("recovery_suppressed_reason"))
                    .col(timestamp_with_time_zone_null("recovery_suppressed_at"))
                    .col(json_null("recovery_suppression_context_json"))
                    .col(text_null("execution_class"))
                    .check(Expr::cust("payload IS JSON"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_item_attempt_turn_item")
                    .table("turn_item_attempt")
                    .col("turn_id")
                    .col("item_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_item_attempt_turn_item_attempt_number")
                    .table("turn_item_attempt")
                    .unique()
                    .col("turn_id")
                    .col("item_id")
                    .col("attempt_number")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_item_attempt_status_lease")
                    .table("turn_item_attempt")
                    .col("status")
                    .col("lease_expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_item_attempt_status_idle")
                    .table("turn_item_attempt")
                    .col("status")
                    .col("idle_deadline_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_item_attempt_status_hard")
                    .table("turn_item_attempt")
                    .col("status")
                    .col("hard_deadline_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_item_attempt_recovery_suppressed")
                    .table("turn_item_attempt")
                    .col("recovery_suppressed_reason")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_item_attempt_execution_class_id")
                    .table("turn_item_attempt")
                    .col("execution_class")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_llm_context")
                    .col(text("id").primary_key())
                    .col(text("turn_id"))
                    .col(text_null("item_id"))
                    .col(text_null("attempt_id"))
                    .col(big_integer("sequence"))
                    .col(text("source"))
                    .col(text_null("tool_name"))
                    .col(text("payload"))
                    .col(text("output_policy_snapshot").default("{}"))
                    .check(Expr::cust("output_policy_snapshot IS JSON"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone_null("expires_at"))
                    .col(text_null("delivery_key"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_llm_context_turn_id")
                    .table("turn_llm_context")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_llm_context_turn_id_sequence")
                    .table("turn_llm_context")
                    .unique()
                    .col("turn_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_llm_context_turn_item")
                    .table("turn_llm_context")
                    .col("turn_id")
                    .col("item_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_llm_context_expires_at")
                    .table("turn_llm_context")
                    .col("expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_llm_context_turn_id_delivery_key")
                    .table("turn_llm_context")
                    .unique()
                    .col("turn_id")
                    .col("delivery_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_context_item_locator")
                    .table("turn_llm_context")
                    .col("turn_id")
                    .col("source")
                    .col("item_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_status_history")
                    .col(text("id").primary_key())
                    .col(text("turn_id"))
                    .col(text("status"))
                    .col(text_null("error"))
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
                    .name("idx_turn_status_history_turn_id_created_at")
                    .table("turn_status_history")
                    .col("turn_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_mcp_binding")
                    .col(text("id").primary_key())
                    .col(text("turn_id"))
                    .col(text("server_installation_id"))
                    .col(text("server_name"))
                    .col(text("raw_tool_name"))
                    .col(text("callable_name"))
                    .col(text("catalog_version"))
                    .col(text("fingerprint"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text("selection_reason").default("implicit_policy"))
                    .col(text_null("capability_id"))
                    .col(text("canonical_callable_name").default("legacy_unrecoverable"))
                    .col(text("provider_callable_name").default("legacy_unrecoverable"))
                    .col(text("canonical_schema_fingerprint").default("legacy_unrecoverable"))
                    .col(text("provider_schema_fingerprint").default("legacy_unrecoverable"))
                    .col(text("annotations_json").default("{}"))
                    .check(Expr::cust("annotations_json IS JSON"))
                    .col(text("annotations_digest").default("legacy_unrecoverable"))
                    .col(big_integer("effective_timeout_ms").default(0))
                    .col(big_integer("runtime_generation").default(0))
                    .col(big_integer("projection_activation_generation").default(0))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_mcp_binding_turn_id")
                    .table("turn_mcp_binding")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_mcp_binding_turn_callable")
                    .table("turn_mcp_binding")
                    .unique()
                    .col("turn_id")
                    .col("callable_name")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_mcp_binding_server_tool")
                    .table("turn_mcp_binding")
                    .col("server_name")
                    .col("raw_tool_name")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_execution_window")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(big_integer("window_index"))
                    .col(text("status"))
                    .col(text_null("exhaustion_reason"))
                    .col(big_integer("agent_round_count").default(0))
                    .col(big_integer("tool_call_count").default(0))
                    .col(big_integer("provider_token_count").default(0))
                    .col(text("metadata_json").default("{}"))
                    .check(Expr::cust("metadata_json IS JSON"))
                    .col(timestamp_with_time_zone("started_at"))
                    .col(timestamp_with_time_zone_null("completed_at"))
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
                    .name("uidx_turn_execution_window_turn_index")
                    .table("turn_execution_window")
                    .unique()
                    .col("turn_id")
                    .col("window_index")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_window_turn_id")
                    .table("turn_execution_window")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_window_thread_turn")
                    .table("turn_execution_window")
                    .col("thread_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_window_status")
                    .table("turn_execution_window")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_execution_checkpoint")
                    .col(text("id").primary_key())
                    .col(text("window_id"))
                    .col(text("workspace_id"))
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(text("checkpoint_kind"))
                    .col(text("payload_json"))
                    .check(Expr::cust("payload_json IS JSON"))
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
                    .name("idx_turn_execution_checkpoint_window")
                    .table("turn_execution_checkpoint")
                    .col("window_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_checkpoint_turn")
                    .table("turn_execution_checkpoint")
                    .col("turn_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_checkpoint_thread_turn")
                    .table("turn_execution_checkpoint")
                    .col("thread_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_checkpoint_kind")
                    .table("turn_execution_checkpoint")
                    .col("checkpoint_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_event_projection_state")
                    .col(text("event_id").primary_key())
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(big_integer("sequence"))
                    .col(text("status"))
                    .col(big_integer("attempt_count").default(0))
                    .col(text_null("last_error"))
                    .col(timestamp_with_time_zone("next_run_at"))
                    .col(text_null("claim_token"))
                    .col(timestamp_with_time_zone_null("claim_expires_at"))
                    .col(text("projection_context_json").default("{}"))
                    .col(timestamp_with_time_zone_null("projected_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust("projection_context_json IS JSON"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_projection_due")
                    .table("turn_event_projection_state")
                    .col("status")
                    .col("next_run_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_projection_claim")
                    .table("turn_event_projection_state")
                    .col("claim_expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_projection_turn")
                    .table("turn_event_projection_state")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_projection_turn_sequence")
                    .table("turn_event_projection_state")
                    .col("turn_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_projection_thread_turn")
                    .table("turn_event_projection_state")
                    .col("thread_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_runtime_snapshot")
                    .col(text("turn_id").primary_key())
                    .col(text("thread_id"))
                    .col(text("workspace_id"))
                    .col(text("mode"))
                    .col(text("model"))
                    .col(text("provider_name"))
                    .col(json("hook_runtime_context_json"))
                    .col(json("workspace_skill_policies_json"))
                    .col(json("input_json"))
                    .col(json("capabilities_json"))
                    .col(json("resolved_artifacts_json"))
                    .col(json("runtime_environment_json"))
                    .col(json("history_json"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text_null("reasoning_effort"))
                    .col(json_null("agent_skill_versions_json"))
                    .col(text_null("frozen_manifest_id").default(""))
                    .check(Expr::cust(r#""mode" IN ('message', 'chat', 'agent')"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_runtime_snapshot_thread")
                    .table("turn_runtime_snapshot")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_runtime_snapshot_workspace")
                    .table("turn_runtime_snapshot")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("turn_runtime_snapshot_frozen_root")
                    .table("turn_runtime_snapshot")
                    .col("frozen_manifest_id")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("turn_runtime_snapshot_frozen_locator_discovery")
                    .table("turn_runtime_snapshot")
                    .col("frozen_manifest_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_cli_runtime_binding")
                    .col(text("thread_id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("runtime_id"))
                    .col(text("runtime_kind"))
                    .col(text("native_thread_id"))
                    .col(text_null("native_session_id"))
                    .col(text_null("native_root_thread_id"))
                    .col(text_null("native_cwd"))
                    .col(text_null("native_model"))
                    .col(json("resume_cursor_json").default("{}"))
                    .col(text("status"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text_null("mcp_adapter_kind"))
                    .col(text_null("mcp_manifest_hash"))
                    .col(text_null("mcp_projection_fingerprint"))
                    .col(text_null("mcp_provider_contract_fingerprint"))
                    .col(text_null("mcp_isolation_contract_fingerprint"))
                    .col(text_null("provider_session_id"))
                    .col(text_null("provider_session_lifecycle_state"))
                    .col(big_integer_null("mcp_session_generation"))
                    .col(big_integer_null(
                        "provider_session_last_verified_process_generation",
                    ))
                    .col(text_null("frozen_manifest_id").default(""))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_cli_runtime_binding_workspace_runtime")
                    .table("thread_cli_runtime_binding")
                    .col("workspace_id")
                    .col("runtime_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_cli_runtime_binding_runtime_native_thread")
                    .table("thread_cli_runtime_binding")
                    .col("runtime_id")
                    .col("native_thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_cli_runtime_binding_workspace_status")
                    .table("thread_cli_runtime_binding")
                    .col("workspace_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("thread_cli_runtime_binding_frozen_root")
                    .table("thread_cli_runtime_binding")
                    .col("frozen_manifest_id")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("thread_cli_runtime_binding_frozen_locator_discovery")
                    .table("thread_cli_runtime_binding")
                    .col("frozen_manifest_id")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_cli_runtime_binding")
                    .col(text("turn_id").primary_key())
                    .col(text("thread_id"))
                    .col(text("workspace_id"))
                    .col(text("runtime_id"))
                    .col(text("runtime_kind"))
                    .col(text("native_thread_id"))
                    .col(text_null("native_turn_id"))
                    .col(text_null("request_id"))
                    .col(text("status"))
                    .col(text_null("model"))
                    .col(text_null("cwd"))
                    .col(json_null("sandbox_json"))
                    .col(text_null("approval_policy"))
                    .col(binary("input_mapping_json").default(Expr::cust("decode('7b7d', 'hex')")))
                    .col(small_integer("input_mapping_codec").default(0))
                    .col(big_integer("input_mapping_bytes").default(2))
                    .col(big_integer("input_mapping_chars").default(2))
                    .col(binary("input_mapping_sha256").default(Expr::cust("sha256(decode('7b7d', 'hex'))")))
                    .col(boolean("input_mapping_has_inline_history").default(false))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text_null("mcp_adapter_kind"))
                    .col(text_null("mcp_manifest_hash"))
                    .col(text_null("mcp_projection_fingerprint"))
                    .col(text_null("mcp_provider_contract_fingerprint"))
                    .col(text_null("mcp_isolation_contract_fingerprint"))
                    .col(big_integer_null("mcp_session_generation"))
                    .col(big_integer_null("mcp_projection_activation_generation"))
                    .col(text_null("native_goal_status"))
                    .col(text_null("native_goal_turn_id"))
                    .col(timestamp_with_time_zone_null("native_goal_observed_at"))
                    .col(text("continuation_thread_id").default(""))
                    .col(text_null("frozen_manifest_id").default(""))
                    .col(big_integer("portable_revision").default(0))
                    .col(text("portable_identity").default(""))
                    .check(Expr::cust(
                        "input_mapping_codec IN (0, 1)
                         AND input_mapping_chars BETWEEN 0 AND input_mapping_bytes
                         AND octet_length(input_mapping_sha256) = 32
                         AND ((input_mapping_codec = 0 AND octet_length(input_mapping_json) = input_mapping_bytes)
                              OR (input_mapping_codec = 1 AND octet_length(input_mapping_json) + 2 < input_mapping_bytes))",
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_binding_thread")
                    .table("turn_cli_runtime_binding")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_binding_runtime_native_turn")
                    .table("turn_cli_runtime_binding")
                    .col("runtime_id")
                    .col("native_turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_binding_native_thread")
                    .table("turn_cli_runtime_binding")
                    .col("runtime_id")
                    .col("native_thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_binding_request")
                    .table("turn_cli_runtime_binding")
                    .col("request_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_binding_workspace_status")
                    .table("turn_cli_runtime_binding")
                    .col("workspace_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_binding_continuation")
                    .table("turn_cli_runtime_binding")
                    .col("workspace_id")
                    .col("runtime_id")
                    .col("continuation_thread_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_cli_turn_binding_status_created_turn")
                    .table("turn_cli_runtime_binding")
                    .col("status")
                    .col("created_at")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("turn_cli_runtime_binding_frozen_root")
                    .table("turn_cli_runtime_binding")
                    .col("frozen_manifest_id")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("turn_cli_runtime_binding_frozen_locator_discovery")
                    .table("turn_cli_runtime_binding")
                    .col("frozen_manifest_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(include_str!(
                "sql/m20261119_000004_conversations/input_mapping_storage.sql"
            ))
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_timeline_projection_meta")
                    .col(text("projection_key").primary_key())
                    .col(big_integer("projection_version"))
                    .col(text("status"))
                    .col(big_integer("source_thread_count").default(0))
                    .col(big_integer("source_turn_count").default(0))
                    .col(big_integer("source_turn_item_count").default(0))
                    .col(big_integer("source_turn_event_count").default(0))
                    .col(text_null("last_error"))
                    .col(timestamp_with_time_zone_null("backfill_started_at"))
                    .col(timestamp_with_time_zone_null("backfilled_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text_null("projection_config_hash"))
                    .col(text_null("projection_config_json"))
                    .check(Expr::cust(
                        "projection_config_json IS NULL OR projection_config_json IS JSON",
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_timeline_block")
                    .col(text("block_id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("thread_id"))
                    .col(text_null("turn_id"))
                    .col(text("block_kind"))
                    .col(text("sort_key").extra(r#"COLLATE "C""#))
                    .col(text_null("source_kind"))
                    .col(text_null("source_key"))
                    .col(timestamp_with_time_zone_null("started_at"))
                    .col(timestamp_with_time_zone_null("completed_at"))
                    .col(text("metadata_json").default("{}"))
                    .check(Expr::cust("metadata_json IS JSON"))
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
                    .name("idx_thread_timeline_block_thread_sort")
                    .table("thread_timeline_block")
                    .col("thread_id")
                    .col("sort_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_timeline_block_thread_kind")
                    .table("thread_timeline_block")
                    .col("thread_id")
                    .col("block_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_timeline_block_turn")
                    .table("thread_timeline_block")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_work_projection")
                    .col(text("turn_id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("thread_id"))
                    .col(text("presentation"))
                    .col(text("state"))
                    .col(big_integer("work_count").default(0))
                    .col(big_integer("visible_work_count").default(0))
                    .col(big_integer("hidden_work_count").default(0))
                    .col(text_null("first_work_item_id"))
                    .col(text_null("last_work_item_id"))
                    .col(timestamp_with_time_zone_null("started_at"))
                    .col(timestamp_with_time_zone_null("completed_at"))
                    .col(big_integer_null("elapsed_ms"))
                    .col(big_integer("source_high_watermark").default(0))
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
                    .name("idx_turn_work_projection_thread")
                    .table("turn_work_projection")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_work_item_projection")
                    .col(text("work_item_id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(text("item_id"))
                    .col(text_null("source_event_id"))
                    .col(big_integer("source_sequence").default(0))
                    .col(text("order_key").extra(r#"COLLATE "C""#))
                    .col(text("item_type"))
                    .col(text("visibility"))
                    .col(text("classification"))
                    .col(text("status"))
                    .col(timestamp_with_time_zone_null("started_at"))
                    .col(timestamp_with_time_zone_null("completed_at"))
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
                    .name("uq_turn_work_item_projection_turn_item")
                    .table("turn_work_item_projection")
                    .unique()
                    .col("turn_id")
                    .col("item_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_work_item_projection_turn_order")
                    .table("turn_work_item_projection")
                    .col("turn_id")
                    .col("order_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_work_item_projection_turn_visibility")
                    .table("turn_work_item_projection")
                    .col("turn_id")
                    .col("visibility")
                    .col("order_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_liveness")
                    .col(text("turn_id").primary_key())
                    .col(text("thread_id"))
                    .col(big_integer("last_activity_sequence"))
                    .col(text("last_activity_kind"))
                    .col(text_null("last_activity_item_id"))
                    .col(text_null("last_activity_item_type"))
                    .col(timestamp_with_time_zone("last_activity_at"))
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
                    .name("idx_turn_liveness_thread")
                    .table("turn_liveness")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_cli_runtime_attempt")
                    .col(text("id").primary_key())
                    .col(text("turn_id").extra(r#"COLLATE "C""#))
                    .col(big_integer("attempt_index"))
                    .col(text("runtime_id"))
                    .col(text("runtime_kind"))
                    .col(text("native_thread_id"))
                    .col(text_null("native_turn_id"))
                    .col(text_null("recovery_job_id"))
                    .col(text_null("recovery_attempt_id"))
                    .col(big_integer_null("execution_window_index"))
                    .col(text("status"))
                    .col(text_null("failure_reason"))
                    .col(timestamp_with_time_zone_null("started_at"))
                    .col(timestamp_with_time_zone_null("completed_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone_null("recovery_confirmed_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_turn_cli_runtime_attempt_turn_index")
                    .table("turn_cli_runtime_attempt")
                    .unique()
                    .col("turn_id")
                    .col("attempt_index")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_turn_cli_runtime_attempt_recovery_attempt")
                    .table("turn_cli_runtime_attempt")
                    .unique()
                    .col("recovery_attempt_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_turn_cli_runtime_attempt_runtime_native_turn")
                    .table("turn_cli_runtime_attempt")
                    .unique()
                    .col("runtime_id")
                    .col("native_turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_attempt_turn_status")
                    .table("turn_cli_runtime_attempt")
                    .col("turn_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_attempt_recovery_job")
                    .table("turn_cli_runtime_attempt")
                    .col("recovery_job_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_mcp_projection")
                    .col(text("turn_id").primary_key())
                    .col(text("workspace_id"))
                    .col(big_integer("projection_version"))
                    .col(text("manifest_hash"))
                    .col(text("resolution_status"))
                    .col(big_integer("tool_count"))
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
                    .name("idx_turn_mcp_projection_workspace_created")
                    .table("turn_mcp_projection")
                    .col("workspace_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_mcp_projection_manifest")
                    .table("turn_mcp_projection")
                    .col("manifest_hash")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_cli_runtime_instruction")
                    .col(text("turn_id").primary_key())
                    .col(text("runtime_kind"))
                    .col(text("transport_kind"))
                    .col(text("instruction_text"))
                    .col(text("instruction_fingerprint"))
                    .col(json("section_ids_json"))
                    .col(text("compiler_version"))
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
                    .name("idx_turn_cli_runtime_instruction_fingerprint")
                    .table("turn_cli_runtime_instruction")
                    .col("instruction_fingerprint")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_cli_runtime_execution_segment")
                    .col(text("id").primary_key())
                    .col(text("attempt_id"))
                    .col(text("turn_id"))
                    .col(big_integer("segment_index"))
                    .col(text("runtime_id"))
                    .col(text("native_thread_id"))
                    .col(text("native_turn_id"))
                    .col(text("status"))
                    .col(text_null("failure_reason"))
                    .col(timestamp_with_time_zone("started_at"))
                    .col(timestamp_with_time_zone_null("completed_at"))
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
                    .name("uidx_turn_cli_runtime_execution_segment_attempt_index")
                    .table("turn_cli_runtime_execution_segment")
                    .unique()
                    .col("attempt_id")
                    .col("segment_index")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_turn_cli_runtime_execution_segment_runtime_native_turn")
                    .table("turn_cli_runtime_execution_segment")
                    .unique()
                    .col("runtime_id")
                    .col("native_turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_execution_segment_turn_status")
                    .table("turn_cli_runtime_execution_segment")
                    .col("turn_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_cli_runtime_execution_segment_attempt_status")
                    .table("turn_cli_runtime_execution_segment")
                    .col("attempt_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_turn_cli_runtime_execution_segment_turn_running")
                    .table("turn_cli_runtime_execution_segment")
                    .unique()
                    .col("turn_id")
                    .and_where(Expr::cust(r#""status" = 'running'"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_skill_binding")
                    .col(text("id").primary_key())
                    .col(text("turn_id"))
                    .col(text("skill_id"))
                    .col(text_null("skill_owner"))
                    .col(text("skill_slug"))
                    .col(text_null("skill_version"))
                    .col(text("fingerprint"))
                    .col(text("source_kind"))
                    .col(text("resolved_reason"))
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
                    .name("idx_turn_skill_binding_turn_id")
                    .table("turn_skill_binding")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_skill_binding_turn_id_skill_id")
                    .table("turn_skill_binding")
                    .unique()
                    .col("turn_id")
                    .col("skill_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("thread_membership")
                .col(text("thread_id"))
                .col(text("principal_id"))
                .col(text("added_by_actor_kind"))
                .col(text_null("added_by_actor_id"))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .col(big_integer_null("inbox_after_sequence"))
                .primary_key(Index::create().col("thread_id").col("principal_id"))
                .check(Expr::cust(r#"length ( thread_id ) = 21 AND thread_id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( principal_id ) = 21 AND principal_id !~ '[^A-Za-z0-9]' COLLATE "C""#))
                .check(Expr::cust(r#"( added_by_actor_kind = 'system' AND added_by_actor_id IS NULL ) OR ( added_by_actor_kind = 'principal' AND length ( added_by_actor_id ) = 21 AND added_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C" )"#))
                .check(Expr::cust(r#"updated_at >= created_at"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_membership_principal")
                    .table("thread_membership")
                    .col("principal_id")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_membership_thread")
                    .table("thread_membership")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("turn_message_revision")
                .col(text("turn_id"))
                .col(big_integer("revision"))
                .col(text("input_json"))
                .check(Expr::cust("input_json IS JSON"))
                .col(text("mentions_json"))
                .check(Expr::cust("mentions_json IS JSON"))
                .col(text("changed_by_actor_kind"))
                .col(text_null("changed_by_actor_id"))
                .col(text("change_kind"))
                .col(timestamp_with_time_zone("created_at"))
                .primary_key(Index::create().col("turn_id").col("revision"))
                .check(Expr::cust(r#"revision >= 0"#))
                .check(Expr::cust(r#"length ( input_json::text ) <= 1048576 AND length ( mentions_json::text ) <= 65536"#))
                .check(Expr::cust(r#"change_kind IN ( 'edit' , 'delete' )"#))
                .check(Expr::cust(r#"( changed_by_actor_kind = 'system' AND changed_by_actor_id IS NULL ) OR ( changed_by_actor_kind = 'principal' AND length ( changed_by_actor_id ) = 21 AND changed_by_actor_id !~ '[^A-Za-z0-9]' COLLATE "C" )"#))
                .to_owned(),
        ).await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_read_cursor")
                    .col(text("principal_id"))
                    .col(text("thread_id"))
                    .col(text("last_read_sort_key").extra(r#"COLLATE "C""#))
                    .col(text("last_read_turn_id"))
                    .col(timestamp_with_time_zone("updated_at"))
                    .primary_key(Index::create().col("principal_id").col("thread_id"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_thread_read_cursor_thread_sort")
                    .table("thread_read_cursor")
                    .col("thread_id")
                    .col("last_read_sort_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_event_delivery")
                    .col(text("id").primary_key())
                    .col(text("event_id"))
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(big_integer("sequence"))
                    .col(text("consumer"))
                    .col(text("status").default("pending"))
                    .col(big_integer("attempt_count").default(0))
                    .col(text_null("last_error"))
                    .col(timestamp_with_time_zone("next_run_at"))
                    .col(text_null("claim_token"))
                    .col(timestamp_with_time_zone_null("claim_expires_at"))
                    .col(timestamp_with_time_zone_null("delivered_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(r#"attempt_count >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_turn_event_delivery_event_consumer")
                    .table("turn_event_delivery")
                    .unique()
                    .col("event_id")
                    .col("consumer")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_delivery_due")
                    .table("turn_event_delivery")
                    .col("consumer")
                    .col("status")
                    .col("next_run_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_delivery_turn_sequence")
                    .table("turn_event_delivery")
                    .col("turn_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_delivery_unfinished_predecessor")
                    .table("turn_event_delivery")
                    .col("consumer")
                    .col("turn_id")
                    .col("sequence")
                    .and_where(Expr::cust(r#"status NOT IN ('delivered', 'exhausted')"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_admission")
                    .col(text("turn_id").primary_key())
                    .col(text("thread_id"))
                    .col(text("workspace_id"))
                    .col(text("request_digest"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(big_integer_null("policy_generation"))
                    .col(text_null("role_key"))
                    .col(text_null("policy_fingerprint"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_admission_scope")
                    .table("turn_admission")
                    .unique()
                    .col("workspace_id")
                    .col("thread_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_finalization")
                    .col(text("turn_id").primary_key())
                    .col(text("thread_id"))
                    .col(text("workspace_id"))
                    .col(big_integer("generation"))
                    .col(text("item_id"))
                    .col(text("item_json"))
                    .col(text("item_digest"))
                    .col(text("status").default("prepared"))
                    .col(timestamp_with_time_zone("prepared_at"))
                    .col(timestamp_with_time_zone_null("committed_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .index(Index::create().unique().col("item_id"))
                    .check(Expr::cust(r#"generation > 0"#))
                    .check(Expr::cust(r#"status IN ( 'prepared' , 'committed' )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_finalization_status")
                    .table("turn_finalization")
                    .col("status")
                    .col("prepared_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_event_projection_stream_state")
                    .col(text("turn_id").primary_key())
                    .col(text("thread_id"))
                    .col(text("status"))
                    .col(text_null("blocking_event_id"))
                    .col(text_null("last_error"))
                    .col(timestamp_with_time_zone_null("quarantined_at"))
                    .col(timestamp_with_time_zone_null("restored_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(big_integer("projected_through_sequence").default(0))
                    .col(big_integer("receipts_compacted_through_sequence").default(0))
                    .col(text_null("accepted_terminal_event_id"))
                    .col(text_null("accepted_terminal_event_type"))
                    .col(big_integer_null("accepted_terminal_sequence"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_projection_stream_status")
                    .table("turn_event_projection_stream_state")
                    .col("status")
                    .col("updated_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_event_projection_stream_blocker")
                    .table("turn_event_projection_stream_state")
                    .col("blocking_event_id")
                    .to_owned(),
            )
            .await?;

        manager.create_index(
            Index::create().name("idx_turn_event_projection_stream_cleanup_work").table("turn_event_projection_stream_state")
                .col("turn_id")
                .and_where(Expr::cust(r#"status = 'healthy' AND receipts_compacted_through_sequence >= 0 AND projected_through_sequence > receipts_compacted_through_sequence"#))
                .to_owned(),
        ).await?;

        manager.create_table(
            Table::create().table("turn_execution")
                .col(text("turn_id").primary_key())
                .col(text("thread_id"))
                .col(text("workspace_id"))
                .col(text("executor_kind"))
                .col(text_null("executor_key"))
                .col(text("status"))
                .col(text("owner_id"))
                .col(big_integer("owner_generation").default(1))
                .col(timestamp_with_time_zone("lease_until"))
                .col(timestamp_with_time_zone("heartbeat_at"))
                .col(timestamp_with_time_zone_null("started_at"))
                .col(timestamp_with_time_zone_null("completed_at"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .check(Expr::cust(r#"executor_kind IN ( 'native_agent' , 'api_provider' , 'cli_runtime' , 'acp_runtime' )"#))
                .check(Expr::cust(r#"status IN ( 'queued' , 'starting' , 'running' , 'recovering' , 'completed' , 'failed' , 'interrupted' , 'blocked' )"#))
                .check(Expr::cust(r#"owner_generation > 0"#))
                .check(Expr::cust(r#"length ( trim ( owner_id ) ) > 0"#))
                .check(Expr::cust(r#"lease_until >= heartbeat_at"#))
                .check(Expr::cust(r#"status != 'running' OR started_at IS NOT NULL"#))
                .check(Expr::cust(r#"( status IN ( 'queued' , 'starting' , 'running' , 'recovering' ) AND completed_at IS NULL ) OR ( status IN ( 'completed' , 'failed' , 'interrupted' , 'blocked' ) AND completed_at IS NOT NULL )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_status_lease")
                    .table("turn_execution")
                    .col("status")
                    .col("lease_until")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_owner_status")
                    .table("turn_execution")
                    .col("owner_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_turn_execution_thread")
                    .table("turn_execution")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("unread_message_position")
                    .col(text("thread_id"))
                    .col(text("block_id"))
                    .col(text("turn_id"))
                    .col(text("sort_key"))
                    .col(big_integer("published_at"))
                    .primary_key(Index::create().col("thread_id").col("block_id"))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables(
            manager,
            &[
                "unread_message_position",
                "turn_execution",
                "turn_event_projection_stream_state",
                "turn_finalization",
                "turn_admission",
                "turn_event_delivery",
                "thread_read_cursor",
                "turn_message_revision",
                "thread_membership",
                "turn_skill_binding",
                "turn_cli_runtime_execution_segment",
                "turn_cli_runtime_instruction",
                "turn_mcp_projection",
                "turn_cli_runtime_attempt",
                "turn_liveness",
                "turn_work_item_projection",
                "turn_work_projection",
                "thread_timeline_block",
                "thread_timeline_projection_meta",
                "turn_cli_runtime_binding",
                "thread_cli_runtime_binding",
                "turn_runtime_snapshot",
                "turn_event_projection_state",
                "turn_execution_checkpoint",
                "turn_execution_window",
                "turn_mcp_binding",
                "turn_status_history",
                "turn_llm_context",
                "turn_item_attempt",
                "turn_item",
                "turn_event",
                "turn_input",
                "turn",
                "thread_lineage",
                "thread_placement",
                "thread_folder",
                "thread_sandox_policy",
                "thread",
            ],
        )
        .await
    }
}

fn history_publication_xid() -> ColumnDef {
    // Keep the full top-level transaction ID across row updates, unlike xmin.
    // SeaORM 2.0 cannot generate a model column for the PostgreSQL xid8 type.
    text("publication_xid")
        .default(Expr::cust("pg_current_xact_id()::text"))
        .take()
}
