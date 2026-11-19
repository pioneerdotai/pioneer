use crate::drop_tables;
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::{
    big_integer, big_integer_null, boolean, json, json_binary, json_null, text, text_null,
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
                    .table("recovery_job")
                    .col(text("id").primary_key())
                    .col(text("turn_id"))
                    .col(text("item_id"))
                    .col(text("item_type"))
                    .col(text_null("source_attempt_id"))
                    .col(text("status"))
                    .col(text("trigger"))
                    .col(text("action"))
                    .col(text_null("reason"))
                    .col(json("policy").default("{}"))
                    .col(text_null("error_class"))
                    .col(text_null("transport_stage"))
                    .col(big_integer_null("retry_after_ms"))
                    .col(big_integer("provider_attempt_number").default(0))
                    .col(json("policy_snapshot").default("{}"))
                    .col(big_integer("run_count").default(0))
                    .col(big_integer("max_attempts").default(0))
                    .col(text_null("last_error"))
                    .col(
                        timestamp_with_time_zone("scheduled_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("next_run_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(text_null("claim_token"))
                    .col(timestamp_with_time_zone_null("claimed_at"))
                    .col(timestamp_with_time_zone_null("claim_expires_at"))
                    .col(text_null("active_attempt_id"))
                    .col(timestamp_with_time_zone_null("active_attempt_started_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(boolean("resolution_pending").default(false))
                    .col(json_null("diagnostic"))
                    .col(text_null("last_failure_attempt_id"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_recovery_job_status_next_run")
                    .table("recovery_job")
                    .col("status")
                    .col("next_run_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_recovery_job_turn_item")
                    .table("recovery_job")
                    .col("turn_id")
                    .col("item_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_recovery_job_claim_lease")
                    .table("recovery_job")
                    .col("status")
                    .col("claim_expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_recovery_job_active_attempt")
                    .table("recovery_job")
                    .col("active_attempt_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_recovery_job_active_turn")
                    .table("recovery_job")
                    .unique()
                    .col("turn_id")
                    .and_where(Expr::cust(r#"status = 'active'"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_recovery_job_unresolved_turn")
                    .table("recovery_job")
                    .unique()
                    .col("turn_id")
                    .and_where(Expr::cust(r#"resolution_pending = TRUE"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("native_event_cleanup_recovery_blocker")
                    .table("recovery_job")
                    .col("turn_id")
                    .and_where(Expr::cust(
                        r#""status" IN ('pending', 'active') OR "resolution_pending" = TRUE"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("cli_runtime_pending_request")
                    .col(text("request_id").primary_key())
                    .col(text("runtime_id"))
                    .col(text("runtime_kind"))
                    .col(text("workspace_id"))
                    .col(text("thread_id"))
                    .col(text_null("turn_id"))
                    .col(text_null("native_thread_id"))
                    .col(text_null("native_turn_id"))
                    .col(text_null("native_item_id"))
                    .col(text("request_kind"))
                    .col(json("payload_json"))
                    .col(text("status"))
                    .col(json_null("response_json"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone_null("resolved_at"))
                    .col(text_null("initiating_principal_id"))
                    .col(text_null("initiating_session_id"))
                    .col(big_integer_null("initiating_session_generation"))
                    .col(text_null("authorization_context_fingerprint"))
                    .col(text_null("responding_principal_id"))
                    .col(text_null("responding_session_id"))
                    .col(big_integer_null("response_authorization_revision"))
                    .col(big_integer("delivery_attempts").default(0))
                    .col(text_null("delivery_error"))
                    .col(boolean("response_contains_secret").default(false))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_cli_runtime_pending_request_thread_turn")
                    .table("cli_runtime_pending_request")
                    .col("workspace_id")
                    .col("thread_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_cli_runtime_pending_request_runtime_status")
                    .table("cli_runtime_pending_request")
                    .col("runtime_id")
                    .col("status")
                    .col("updated_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_cli_runtime_pending_request_native_turn")
                    .table("cli_runtime_pending_request")
                    .col("runtime_id")
                    .col("native_thread_id")
                    .col("native_turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_cli_runtime_pending_request_status_created")
                    .table("cli_runtime_pending_request")
                    .col("status")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_cli_runtime_pending_request_kind")
                    .table("cli_runtime_pending_request")
                    .col("request_kind")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("cli_runtime_native_event")
                    .col(text("id").primary_key())
                    .col(text("runtime_id"))
                    .col(text("runtime_kind"))
                    .col(text_null("workspace_id"))
                    .col(text_null("thread_id"))
                    .col(text_null("turn_id"))
                    .col(text_null("native_thread_id"))
                    .col(text_null("native_turn_id"))
                    .col(text("native_method"))
                    .col(text("payload_redacted_json"))
                    .col(big_integer("sequence"))
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
                    .name("idx_cli_runtime_native_event_thread_sequence")
                    .table("cli_runtime_native_event")
                    .col("runtime_id")
                    .col("thread_id")
                    .col("turn_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_cli_runtime_native_event_native_thread")
                    .table("cli_runtime_native_event")
                    .col("runtime_id")
                    .col("native_thread_id")
                    .col("native_turn_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_cli_runtime_native_event_method")
                    .table("cli_runtime_native_event")
                    .col("runtime_id")
                    .col("native_method")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager.create_index(
            Index::create().name("native_event_cleanup_candidate").table("cli_runtime_native_event")
                .col("turn_id")
                .col("runtime_id")
                .col("id")
                .col(Expr::cust(r#"octet_length(payload_redacted_json)"#))
                .and_where(Expr::cust(r#"turn_id IS NOT NULL AND native_method IN ('item/started','item/completed','item/agentMessage/delta',
    'item/commandExecution/outputDelta','turn/diff/updated',
    'thread/tokenUsage/updated','account/rateLimits/updated')
                AND octet_length(payload_redacted_json)<=262144"#))
                .to_owned(),
        ).await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_skill")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("slug"))
                    .col(text_null("active_version_id"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(big_integer_null("evidence_latest_at_unix"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_skill_workspace_slug")
                    .table("agent_skill")
                    .unique()
                    .col("workspace_id")
                    .col("slug")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_skill_version")
                    .col(text("id").primary_key())
                    .col(text("skill_id"))
                    .col(big_integer("version_number"))
                    .col(text_null("source_run_id"))
                    .col(text_null("parent_version_id"))
                    .col(text("candidate_key"))
                    .col(text("display_name"))
                    .col(text("skill_markdown"))
                    .col(text("instruction_body"))
                    .col(text("when_to_use"))
                    .col(text("when_not_to_use"))
                    .col(text("fingerprint"))
                    .col(json("source_turn_ids_json"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(big_integer_null("evidence_latest_at_unix"))
                    .check(Expr::cust(r#"version_number >= 1"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_skill_version_number")
                    .table("agent_skill_version")
                    .unique()
                    .col("skill_id")
                    .col("version_number")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_skill_version_fingerprint")
                    .table("agent_skill_version")
                    .unique()
                    .col("skill_id")
                    .col("fingerprint")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_skill_version_run_candidate")
                    .table("agent_skill_version")
                    .unique()
                    .col("source_run_id")
                    .col("candidate_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_skill_version_parent")
                    .table("agent_skill_version")
                    .col("parent_version_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("recovery_terminalization_outbox")
                .col(text("recovery_job_id").primary_key())
                .col(text("turn_id"))
                .col(text("item_id"))
                .col(text("item_type"))
                .col(text("recovery_status"))
                .col(big_integer("attempt_number"))
                .col(text("error_message"))
                .col(text("status").default("pending"))
                .col(big_integer("attempt_count").default(0))
                .col(text_null("last_error"))
                .col(timestamp_with_time_zone("next_run_at"))
                .col(text_null("claim_token"))
                .col(timestamp_with_time_zone_null("claim_expires_at"))
                .col(timestamp_with_time_zone_null("delivered_at"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(text_null("quarantine_reason_code"))
                .col(timestamp_with_time_zone_null("quarantined_at"))
                .check(Expr::cust(r#"attempt_number > 0"#))
                .check(Expr::cust(r#"attempt_count >= 0"#))
                .check(Expr::cust(r#"status IN ( 'pending' , 'delivering' , 'failed' , 'delivered' , 'cancelled' )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_recovery_terminalization_due")
                    .table("recovery_terminalization_outbox")
                    .col("status")
                    .col("next_run_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_recovery_terminalization_turn")
                    .table("recovery_terminalization_outbox")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("execution_admission_lease")
                    .col(text("id").primary_key())
                    .col(text("subject_kind"))
                    .col(text("subject_id"))
                    .col(text("operation_class"))
                    .col(text("quota_bucket"))
                    .col(text("principal_id"))
                    .col(text("role_key"))
                    .col(text("workspace_id"))
                    .col(text("policy_fingerprint"))
                    .col(text("status"))
                    .col(timestamp_with_time_zone("created_at"))
                    .col(timestamp_with_time_zone_null("released_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_execution_admission_lease_subject")
                    .table("execution_admission_lease")
                    .unique()
                    .col("subject_kind")
                    .col("subject_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_admission_lease_quota")
                    .table("execution_admission_lease")
                    .col("status")
                    .col("quota_bucket")
                    .col("workspace_id")
                    .col("role_key")
                    .col("principal_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("native_agent_config")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text_null("system_key"))
                    .col(text("display_name"))
                    .col(text("nickname"))
                    .col(boolean("enabled").default(true))
                    .col(text_null("avatar_revision"))
                    .col(big_integer("config_revision").default(1))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(
                        r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C""#,
                    ))
                    .check(Expr::cust(r#"config_revision >= 1"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_native_agent_config_workspace_system_key")
                    .table("native_agent_config")
                    .unique()
                    .col("workspace_id")
                    .col("system_key")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("agent_identity")
                .col(text("id").primary_key())
                .col(text("workspace_id"))
                .col(text("source_kind"))
                .col(text("source_id"))
                .col(big_integer("source_revision").default(1))
                .col(text("source_fingerprint"))
                .col(text("status").default("active"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone_null("retired_at"))
                .check(Expr::cust(r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C""#))
                .check(Expr::cust(r#"source_kind IN ( 'native_agent' , 'cli_runtime_instance' , 'ephemeral' )"#))
                .check(Expr::cust(r#"source_revision >= 1"#))
                .check(Expr::cust(r#"status IN ( 'active' , 'retired' )"#))
                .check(Expr::cust(r#"( status = 'retired' AND retired_at IS NOT NULL ) OR ( status = 'active' AND retired_at IS NULL )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_identity_workspace_source")
                    .table("agent_identity")
                    .unique()
                    .col("workspace_id")
                    .col("source_kind")
                    .col("source_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_presentation_snapshot")
                    .col(text("id").primary_key())
                    .col(text("agent_identity_id"))
                    .col(big_integer("source_revision"))
                    .col(text("source_fingerprint"))
                    .col(text("display_name"))
                    .col(text("nickname"))
                    .col(text_null("avatar_revision"))
                    .col(text_null("role_label"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(
                        r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C""#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_presentation_snapshot_source")
                    .table("agent_presentation_snapshot")
                    .unique()
                    .col("agent_identity_id")
                    .col("source_revision")
                    .col("source_fingerprint")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("agent_execution")
                .col(text("id").primary_key())
                .col(text("workspace_id"))
                .col(text("agent_identity_id"))
                .col(big_integer("identity_source_revision"))
                .col(text("identity_source_fingerprint"))
                .col(text_null("parent_execution_id"))
                .col(text_null("parent_task_id"))
                .col(text_null("parent_thread_id"))
                .col(text("home_root_thread_id"))
                .col(text("work_graph_root_execution_id"))
                .col(json("requested_identity_selection_json"))
                .col(json("requested_profile_selection_json"))
                .col(text_null("resolved_profile_id"))
                .col(text_null("resolved_profile_fingerprint"))
                .col(text_null("presentation_snapshot_id"))
                .col(text("authorization_context_fingerprint"))
                .col(big_integer("execution_generation").default(1))
                .col(text("status").default("created"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone_null("finished_at"))
                .check(Expr::cust(r#"length ( id ) = 21 AND id !~ '[^A-Za-z0-9]' COLLATE "C" AND length ( work_graph_root_execution_id ) = 21"#))
                .check(Expr::cust(r#"execution_generation >= 1"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_execution_graph_status")
                    .table("agent_execution")
                    .col("work_graph_root_execution_id")
                    .col("status")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_execution_identity_status")
                    .table("agent_execution")
                    .col("agent_identity_id")
                    .col("status")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_execution_parent")
                    .table("agent_execution")
                    .col("parent_execution_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_action")
                    .col(text("id").primary_key())
                    .col(text("execution_id"))
                    .col(text("action_kind"))
                    .col(text("idempotency_key"))
                    .col(text("request_fingerprint"))
                    .col(text("status").default("prepared"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(timestamp_with_time_zone_null("committed_at"))
                    .col(json_null("response_json"))
                    .check(Expr::cust(
                        r#"status IN ( 'prepared' , 'committed' , 'failed' )"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_action_idempotency")
                    .table("agent_action")
                    .unique()
                    .col("execution_id")
                    .col("idempotency_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_action_receipt")
                    .col(text("id").primary_key())
                    .col(text("action_id"))
                    .col(text("actor_kind"))
                    .col(text_null("actor_id"))
                    .col(text("decision"))
                    .col(text("policy_fingerprint"))
                    .col(text_null("execution_grant_fingerprint"))
                    .col(big_integer_null("execution_grant_policy_generation"))
                    .col(text_null("source_scope_id"))
                    .col(text_null("destination_scope_id"))
                    .col(text_null("action_kind"))
                    .col(text_null("authorized_resource_action"))
                    .col(text_null("subject_role_key"))
                    .col(big_integer_null("execution_generation"))
                    .col(big_integer_null("source_policy_generation"))
                    .col(big_integer_null("destination_policy_generation"))
                    .col(big_integer_null("route_generation"))
                    .col(text_null("disclosure_class"))
                    .col(text_null("decision_fingerprint"))
                    .col(
                        timestamp_with_time_zone("committed_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(json_null("response_json"))
                    .col(json_null("route_receipt_json"))
                    .check(Expr::cust(
                        r#"decision IN ( 'allowed' , 'denied' , 'failed' )"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_action_receipt_action")
                    .table("agent_action_receipt")
                    .unique()
                    .col("action_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_delegation_route_event")
                    .col(text("id").primary_key())
                    .col(text("route_id"))
                    .col(text("event_kind"))
                    .col(big_integer("route_generation"))
                    .col(json("authority_actor_json"))
                    .col(text("authority_fingerprint"))
                    .col(timestamp_with_time_zone("occurred_at"))
                    .check(Expr::cust(
                        r#"event_kind IN ( 'created' , 'revoked' , 'expired' )"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_route_event_generation")
                    .table("agent_delegation_route_event")
                    .unique()
                    .col("route_id")
                    .col("route_generation")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_execution_grant")
                    .col(text("id").primary_key())
                    .col(text("execution_id"))
                    .col(text_null("parent_execution_id"))
                    .col(text("child_identity_id"))
                    .col(text("grant_fingerprint"))
                    .col(json("grant_json"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_turn_response_execution")
                    .col(text("turn_id").primary_key())
                    .col(text("execution_id"))
                    .col(text("presentation_snapshot_id"))
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
                    .name("ix_agent_turn_response_execution_id")
                    .table("agent_turn_response_execution")
                    .col("execution_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_action_outbox")
                    .col(text("id").primary_key())
                    .col(text("action_id"))
                    .col(text("owner_execution_id"))
                    .col(text("payload_json"))
                    .col(text("status").default("pending"))
                    .col(big_integer("attempts").default(0))
                    .col(timestamp_with_time_zone_null("next_attempt_at"))
                    .col(timestamp_with_time_zone_null("delivered_at"))
                    .col(text_null("last_error"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(
                        r#"status IN ( 'pending' , 'delivered' , 'failed' )"#,
                    ))
                    .check(Expr::cust(r#"attempts >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_outbox_action")
                    .table("agent_action_outbox")
                    .unique()
                    .col("action_id")
                    .to_owned(),
            )
            .await?;

        manager.create_index(
            Index::create().name("idx_agent_action_outbox_timed").table("agent_action_outbox")
                .col("next_attempt_at")
                .col("created_at")
                .col("id")
                .and_where(Expr::cust(r#"next_attempt_at IS NOT NULL AND (status='pending' OR (status='failed' AND attempts<8))"#))
                .to_owned(),
        ).await?;

        // SeaQuery has no CREATE FUNCTION builder.
        manager
            .get_connection()
            .execute_unprepared(include_str!(
                "sql/m20261119_000005_execution/outbox_reconciliation.sql"
            ))
            .await?;

        manager.create_index(
            Index::create().name("idx_agent_action_outbox_exhausted").table("agent_action_outbox")
                .col(Expr::cust(r#"COALESCE(next_attempt_at, '-infinity'::timestamptz)"#))
                .col("created_at")
                .col("id")
                .and_where(Expr::cust(r#"status IN ('pending','failed') AND attempts=8 AND agent_action_outbox_start_candidate(payload_json)"#))
                .to_owned(),
        ).await?;

        manager.create_index(
            Index::create().name("idx_agent_action_outbox_immediate").table("agent_action_outbox")
                .col("created_at")
                .col("id")
                .and_where(Expr::cust(r#""next_attempt_at" IS NULL AND "status" IN ('pending', 'failed') AND "attempts" < 8 AND ("status" = 'pending' OR "last_error" IS NULL OR "last_error" NOT IN ('outbox_start_integrity_rejected', 'outbox_start_authority_denied', 'outbox_start_runtime_unavailable', 'outbox_foreground_conflict', 'outbox_start_outcome_unknown'))"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_action_outbox_status_created_id")
                    .table("agent_action_outbox")
                    .col("status")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("agent_work_resource_scope")
                .col(text("root_execution_id").primary_key())
                .col(big_integer("scope_generation").default(1))
                .col(big_integer("max_concurrency").default(1))
                .col(big_integer("max_queue_depth").default(2048))
                .col(big_integer("max_depth").default(64))
                .col(big_integer("max_fan_out").default(128))
                .col(big_integer("max_total_nodes").default(4096))
                .col(text("aggregate_usage_json"))
                .col(big_integer("queue_generation").default(0))
                .col(big_integer("last_scheduled_sequence").default(0))
                .col(timestamp_with_time_zone_null("last_scheduled_at"))
                .col(text("status").default("active"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .check(Expr::cust(r#"scope_generation >= 1"#))
                .check(Expr::cust(r#"max_concurrency >= 1"#))
                .check(Expr::cust(r#"queue_generation >= 0"#))
                .check(Expr::cust(r#"max_queue_depth BETWEEN 1 AND 2048 AND max_depth BETWEEN 1 AND 64 AND max_fan_out BETWEEN 1 AND 128 AND max_total_nodes BETWEEN 1 AND 4096"#))
                .check(Expr::cust(r#"last_scheduled_sequence >= 0"#))
                .check(Expr::cust(r#"status IN ( 'active' , 'queued' , 'recovering' , 'closed' )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_work_scope_fair_cursor")
                    .table("agent_work_resource_scope")
                    .col("status")
                    .col("last_scheduled_sequence")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("agent_execution_resource_state")
                .col(text("id").primary_key())
                .col(text("execution_id"))
                .col(big_integer("attempt_generation").default(1))
                .col(big_integer("progress_sequence").default(0))
                .col(text("progress_frontier_json"))
                .col(timestamp_with_time_zone_null("last_progress_at"))
                .col(timestamp_with_time_zone_null("last_heartbeat_at"))
                .col(timestamp_with_time_zone_null("idle_deadline"))
                .col(timestamp_with_time_zone_null("hard_deadline"))
                .col(text("local_usage_json"))
                .col(text_null("permit_id"))
                .col(text("branch_key"))
                .col(big_integer("fair_order"))
                .col(text("status").default("queued"))
                .col(big_integer("fencing_generation").default(1))
                .col(timestamp_with_time_zone_null("fenced_at"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .check(Expr::cust(r#"attempt_generation >= 1"#))
                .check(Expr::cust(r#"progress_sequence >= 0"#))
                .check(Expr::cust(r#"fencing_generation >= 1"#))
                .check(Expr::cust(r#"status IN ( 'queued' , 'running' , 'paused' , 'completed' , 'failed' , 'cancelled' , 'fenced' )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_resource_state_attempt")
                    .table("agent_execution_resource_state")
                    .unique()
                    .col("execution_id")
                    .col("attempt_generation")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("agent_work_queue")
                .col(text("id").primary_key())
                .col(text("root_execution_id"))
                .col(text("execution_id"))
                .col(big_integer("attempt_generation").default(1))
                .col(text("branch_key"))
                .col(big_integer("enqueue_sequence"))
                .col(text("state").default("queued"))
                .col(timestamp_with_time_zone_null("eligible_at"))
                .col(text_null("claim_token"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .check(Expr::cust(r#"attempt_generation >= 1"#))
                .check(Expr::cust(r#"state IN ( 'queued' , 'claimed' , 'running' , 'recovering' , 'released' , 'cancelled' )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_work_queue_attempt")
                    .table("agent_work_queue")
                    .unique()
                    .col("root_execution_id")
                    .col("execution_id")
                    .col("attempt_generation")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_work_queue_root_state")
                    .table("agent_work_queue")
                    .col("root_execution_id")
                    .col("state")
                    .col("branch_key")
                    .col("enqueue_sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_work_queue_ready")
                    .table("agent_work_queue")
                    .col("state")
                    .col("eligible_at")
                    .col("enqueue_sequence")
                    .col("root_execution_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("agent_running_permit")
                .col(text("id").primary_key())
                .col(text("root_execution_id"))
                .col(text("execution_id"))
                .col(big_integer("attempt_generation").default(1))
                .col(big_integer("lease_generation").default(1))
                .col(text("status").default("held"))
                .col(timestamp_with_time_zone("acquired_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone_null("released_at"))
                .col(timestamp_with_time_zone_null("fenced_at"))
                .check(Expr::cust(r#"attempt_generation >= 1"#))
                .check(Expr::cust(r#"lease_generation >= 1"#))
                .check(Expr::cust(r#"status IN ( 'held' , 'released' , 'fenced' )"#))
                .check(Expr::cust(r#"( status = 'held' AND released_at IS NULL AND fenced_at IS NULL ) OR ( status != 'held' AND ( released_at IS NOT NULL OR fenced_at IS NOT NULL ) )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_running_permit_attempt")
                    .table("agent_running_permit")
                    .unique()
                    .col("root_execution_id")
                    .col("execution_id")
                    .col("attempt_generation")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_domain_commit")
                    .col(text("id").primary_key())
                    .col(text("mutation_kind"))
                    .col(text("idempotency_key"))
                    .col(text("request_fingerprint"))
                    .col(text("execution_id"))
                    .col(text("actor_identity_id"))
                    .col(text("receipt_id"))
                    .col(text("outbox_id"))
                    .col(text("status").default("committed"))
                    .col(
                        timestamp_with_time_zone("committed_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(
                        r#"status IN ( 'committed' , 'replayed' , 'failed' )"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_domain_commit_execution_idempotency")
                    .table("agent_domain_commit")
                    .unique()
                    .col("execution_id")
                    .col("idempotency_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_domain_commit_receipt")
                    .table("agent_domain_commit")
                    .unique()
                    .col("receipt_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_agent_domain_commit_outbox")
                    .table("agent_domain_commit")
                    .unique()
                    .col("outbox_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_work_branch_schedule")
                    .col(text("root_execution_id"))
                    .col(text("branch_key"))
                    .col(timestamp_with_time_zone_null("last_scheduled_at"))
                    .col(big_integer("last_scheduled_sequence").default(0))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .primary_key(Index::create().col("root_execution_id").col("branch_key"))
                    .check(Expr::cust(r#"last_scheduled_sequence >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_work_branch_fair_cursor")
                    .table("agent_work_branch_schedule")
                    .col("root_execution_id")
                    .col("last_scheduled_sequence")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("agent_action_timeline_target")
                .col(text("target_key").primary_key())
                .col(text("action_id"))
                .col(text("turn_id"))
                .col(text_null("turn_item_id"))
                .col(text("target_kind"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .index(Index::create().unique().col("turn_item_id"))
                .index(Index::create().unique().col("action_id"))
                .check(Expr::cust(r#"( target_kind = 'turn_input' AND turn_item_id IS NULL ) OR ( target_kind = 'turn_item' AND turn_item_id IS NOT NULL )"#))
                .to_owned(),
        ).await?;

        manager
            .create_table(
                Table::create()
                    .table("agent_work_scheduler_state")
                    .col(text("scheduler_key").primary_key())
                    .col(big_integer("schedule_generation").default(0))
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(r#"schedule_generation >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("agent_work_scheduler_state")
                    .columns(["scheduler_key", "schedule_generation", "updated_at"])
                    .values_panic([
                        "global".into(),
                        0_i64.into(),
                        Expr::cust(r#"statement_timestamp()"#).into(),
                    ])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("codex_aggregate_state")
                    .col(text("thread_id"))
                    .col(text("turn_id"))
                    .col(big_integer("revision"))
                    .col(big_integer("final_state").default(0))
                    .col(json("state_json"))
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .primary_key(Index::create().col("thread_id").col("turn_id"))
                    .check(Expr::cust(r#""final_state" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("read_model_repair_checkpoint")
                    .col(text("repair_key").primary_key())
                    .col(big_integer("algorithm_version"))
                    .col(text("full_scan_status"))
                    .col(text_null("full_scan_cursor_id"))
                    .col(text_null("full_scan_high_watermark_id"))
                    .col(text("incremental_status"))
                    .col(big_integer_null("incremental_high_watermark_generation"))
                    .col(big_integer_null("incremental_cursor_generation"))
                    .col(big_integer("last_completed_generation").default(0))
                    .col(timestamp_with_time_zone_null("full_scan_started_at"))
                    .col(timestamp_with_time_zone_null("full_scan_completed_at"))
                    .col(timestamp_with_time_zone_null("incremental_started_at"))
                    .col(timestamp_with_time_zone_null("incremental_completed_at"))
                    .col(
                        timestamp_with_time_zone("created_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(
                        r#"full_scan_status IN ( 'running' , 'completed' )"#,
                    ))
                    .check(Expr::cust(
                        r#"incremental_status IN ( 'running' , 'completed' )"#,
                    ))
                    .check(Expr::cust(r#"last_completed_generation >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("read_model_repair_change_sequence")
                    .col(big_integer("singleton_key").primary_key())
                    .col(big_integer("generation"))
                    .check(Expr::cust(r#"singleton_key = 1"#))
                    .check(Expr::cust(r#"generation >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("read_model_repair_change_sequence")
                    .columns(["singleton_key", "generation"])
                    .values_panic([1_i64.into(), 0_i64.into()])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("read_model_repair_dirty_turn_item")
                    .col(text("turn_item_id").primary_key())
                    .col(big_integer("generation"))
                    .col(
                        timestamp_with_time_zone("changed_at")
                            .default(Expr::cust(r#"statement_timestamp()"#)),
                    )
                    .check(Expr::cust(r#"generation > 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_read_model_repair_dirty_generation")
                    .table("read_model_repair_dirty_turn_item")
                    .col("generation")
                    .col("turn_item_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("native_terminal_effect_outbox")
                .col(text("effect_id").primary_key())
                .col(text("batch_id"))
                .col(text("workspace_id"))
                .col(text("thread_id"))
                .col(text("turn_id"))
                .col(big_integer("runtime_generation"))
                .col(text("effect_kind"))
                .col(text("gate_kind"))
                .col(text("payload_json"))
                .col(text("payload_sha256"))
                .col(text("payload_identity_sha256"))
                .col(text_null("handler_checkpoint_json"))
                .col(text_null("handler_checkpoint_sha256"))
                .col(text("status").default("prepared"))
                .col(text_null("accepted_candidate_id"))
                .col(big_integer("attempt_count").default(0))
                .col(big_integer("max_attempts"))
                .col(text_null("last_error_code"))
                .col(text_null("last_error_message"))
                .col(timestamp_with_time_zone_null("next_run_at"))
                .col(text_null("claim_token"))
                .col(timestamp_with_time_zone_null("claim_expires_at"))
                .col(timestamp_with_time_zone_null("terminal_committed_at"))
                .col(timestamp_with_time_zone_null("completed_at"))
                .col(timestamp_with_time_zone("prepared_at"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(big_integer("gate_probe_at").default(0))
                .col(big_integer("gate_probe_attempts").default(0))
                .col(text_null("gate_probe_token"))
                .check(Expr::cust(r#"runtime_generation > 0"#))
                .check(Expr::cust(r#"attempt_count >= 0 AND attempt_count <= max_attempts AND max_attempts BETWEEN 1 AND 20"#))
                .check(Expr::cust(r#"effect_kind IN ( 'post_turn_hook' , 'attached_task_cleanup' )"#))
                .check(Expr::cust(r#"gate_kind IN ( 'terminal_commit' , 'accepted_task_result' )"#))
                .check(Expr::cust(r#"status IN ( 'prepared' , 'waiting_acceptance' , 'ready' , 'running' , 'retry_wait' , 'succeeded' , 'unresolved' , 'discarded' , 'superseded' )"#))
                .check(Expr::cust(r#"length ( payload_sha256 ) = 64"#))
                .check(Expr::cust(r#"length ( payload_identity_sha256 ) = 64"#))
                .check(Expr::cust(r#"( handler_checkpoint_json IS NULL AND handler_checkpoint_sha256 IS NULL ) OR ( handler_checkpoint_json IS NOT NULL AND handler_checkpoint_sha256 IS NOT NULL AND length ( handler_checkpoint_sha256 ) = 64 )"#))
                .check(Expr::cust(r#"( status = 'prepared' AND terminal_committed_at IS NULL AND completed_at IS NULL AND next_run_at IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL ) OR ( status = 'waiting_acceptance' AND terminal_committed_at IS NOT NULL AND completed_at IS NULL AND next_run_at IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL ) OR ( status IN ( 'ready' , 'retry_wait' ) AND terminal_committed_at IS NOT NULL AND completed_at IS NULL AND next_run_at IS NOT NULL AND claim_token IS NULL AND claim_expires_at IS NULL ) OR ( status = 'running' AND terminal_committed_at IS NOT NULL AND completed_at IS NULL AND next_run_at IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL ) OR ( status IN ( 'succeeded' , 'unresolved' , 'discarded' ) AND terminal_committed_at IS NOT NULL AND completed_at IS NOT NULL AND next_run_at IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL ) OR ( status = 'superseded' AND terminal_committed_at IS NULL AND completed_at IS NOT NULL AND next_run_at IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_native_terminal_effect_turn")
                    .table("native_terminal_effect_outbox")
                    .col("turn_id")
                    .col("batch_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_native_terminal_effect_completed")
                    .table("native_terminal_effect_outbox")
                    .col("status")
                    .col("completed_at")
                    .col("effect_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_native_terminal_effect_gate_due")
                    .table("native_terminal_effect_outbox")
                    .col("gate_probe_at")
                    .col("prepared_at")
                    .col("effect_id")
                    .and_where(Expr::cust(r#"status = 'waiting_acceptance'"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uidx_native_terminal_effect_probe_token")
                    .table("native_terminal_effect_outbox")
                    .unique()
                    .col("gate_probe_token")
                    .and_where(Expr::cust(r#"gate_probe_token IS NOT NULL"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_native_terminal_effect_due")
                    .table("native_terminal_effect_outbox")
                    .col("status")
                    .col("next_run_at")
                    .col("prepared_at")
                    .col("effect_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_native_terminal_effect_expired")
                    .table("native_terminal_effect_outbox")
                    .col("status")
                    .col("claim_expires_at")
                    .col("prepared_at")
                    .col("effect_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("native_event_cleanup_job")
                    .col(text("turn_id").primary_key())
                    .col(text("state"))
                    .col(big_integer("available_at").default(0))
                    .col(big_integer("last_served_at").default(0))
                    .col(big_integer("revision").default(1))
                    .col(text_null("last_error"))
                    .check(Expr::cust(r#""state" IN ( 'queued' , 'waiting' )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("native_event_cleanup_new")
                    .table("native_event_cleanup_job")
                    .col("turn_id")
                    .and_where(Expr::cust(
                        r#""state" = 'queued' AND "available_at" = 0 AND "last_served_at" = 0"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("native_event_cleanup_served")
                    .table("native_event_cleanup_job")
                    .col("last_served_at")
                    .col("turn_id")
                    .and_where(Expr::cust(
                        r#""state" = 'queued' AND "available_at" = 0 AND "last_served_at" > 0"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("native_event_cleanup_due_retry")
                    .table("native_event_cleanup_job")
                    .col("available_at")
                    .col("turn_id")
                    .and_where(Expr::cust(r#""state" = 'queued' AND "available_at" > 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("native_event_cleanup_bootstrap")
                    .col(big_integer("singleton").primary_key())
                    .col(text_null("cursor_id"))
                    .col(boolean("complete").default(false))
                    .check(Expr::cust(r#""singleton" = 1"#))
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("native_event_cleanup_bootstrap")
                    .columns(["singleton", "complete"])
                    .values_panic([1_i64.into(), false.into()])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("native_event_cleanup_scheduler")
                    .col(big_integer("singleton").primary_key())
                    .col(big_integer("new_jobs_since_served").default(0))
                    .check(Expr::cust(r#""singleton" = 1"#))
                    .check(Expr::cust(r#""new_jobs_since_served" BETWEEN 0 AND 4"#))
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("native_event_cleanup_scheduler")
                    .columns(["singleton", "new_jobs_since_served"])
                    .values_panic([1_i64.into(), 0_i64.into()])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("provider_usage_observation")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("operation_kind"))
                    .col(text("owner_id"))
                    .col(text("status"))
                    .col(json("usage_json"))
                    .col(big_integer("started_at"))
                    .col(big_integer("updated_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_provider_usage_owner")
                    .table("provider_usage_observation")
                    .col("workspace_id")
                    .col("owner_id")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("native_cancellation_context")
                    .col(text("turn_id").primary_key())
                    .col(text("thread_id"))
                    .col(text("workspace_id"))
                    .col(text("execution_owner_id"))
                    .col(text("context_json"))
                    .col(text("context_sha256"))
                    .col(text_null("accepted_event_id"))
                    .col(timestamp_with_time_zone("created_at"))
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("agent_delegation_route")
                .col(text("id").primary_key())
                .col(text("source_execution_id"))
                .col(text("destination_thread_id"))
                .col(text_null("destination_agent_identity_id"))
                .col(text_null("home_capsule_id"))
                .col(text("route_kind"))
                .col(text("grant_fingerprint"))
                .col(text_null("source_capsule_id"))
                .col(text_null("destination_capsule_id"))
                .col(text_null("source_workspace_id"))
                .col(text_null("destination_workspace_id"))
                .col(text_null("source_gateway_id"))
                .col(text_null("destination_gateway_id"))
                .col(text_null("source_identity_id"))
                .col(text_null("destination_profile_id"))
                .col(json_binary("allowed_actions_json").default("[]"))
                .col(json_binary("disclosure_json").default("{}"))
                .col(big_integer("route_generation").default(1))
                .col(big_integer("source_policy_generation").default(1))
                .col(big_integer("destination_policy_generation").default(1))
                .col(big_integer("hop_count").default(0))
                .col(big_integer("max_hops").default(8))
                .col(text_null("return_route_id"))
                .col(json_null("authority_actor_json"))
                .col(text_null("authority_fingerprint"))
                .col(text("status").default("prepared"))
                .col(timestamp_with_time_zone("created_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone("updated_at").default(Expr::cust(r#"statement_timestamp()"#)))
                .col(timestamp_with_time_zone_null("expires_at"))
                .check(Expr::cust(r#"status IN ( 'prepared' , 'active' , 'expired' , 'revoked' )"#))
                .check(Expr::cust(r#"route_kind IN ( 'execution_bound' , 'identity_bound' , 'capsule_bound' )"#))
                .check(Expr::cust(r#"route_generation >= 1 AND source_policy_generation >= 1 AND destination_policy_generation >= 1"#))
                .check(Expr::cust(r#"hop_count >= 0 AND max_hops BETWEEN 1 AND 8 AND hop_count <= max_hops"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_route_expiry")
                    .table("agent_delegation_route")
                    .col("status")
                    .col("expires_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_route_graph_edges")
                    .table("agent_delegation_route")
                    .col("source_workspace_id")
                    .col("destination_workspace_id")
                    .col("source_gateway_id")
                    .col("destination_gateway_id")
                    .col("status")
                    .col("expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_route_identity_active")
                    .table("agent_delegation_route")
                    .col("route_kind")
                    .col("source_identity_id")
                    .col("status")
                    .col("expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_route_source_page")
                    .table("agent_delegation_route")
                    .col("source_execution_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_agent_route_capsule_active")
                    .table("agent_delegation_route")
                    .col("source_capsule_id")
                    .col("route_kind")
                    .col("status")
                    .col("expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("execution_tariff")
                    .col(text("workspace_id"))
                    .col(text("content_hash"))
                    .col(json("evidence_json"))
                    .col(big_integer("captured_at_ms"))
                    .primary_key(Index::create().col("workspace_id").col("content_hash"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("execution_observation")
                    .col(
                        big_integer("sequence")
                            .primary_key()
                            .extra("GENERATED BY DEFAULT AS IDENTITY"),
                    )
                    .col(text("workspace_id"))
                    .col(text("operation_id"))
                    .col(big_integer("revision"))
                    .col(big_integer("source_revision"))
                    .col(text("resolution_origin"))
                    .col(text_null("thread_id"))
                    .col(text_null("root_thread_id"))
                    .col(text_null("turn_id"))
                    .col(text_null("task_id"))
                    .col(text_null("task_run_id"))
                    .col(text_null("task_execution_id"))
                    .col(text_null("execution_id"))
                    .col(text_null("window_id"))
                    .col(text("owner_kind"))
                    .col(text("owner_id"))
                    .col(text_null("parent_operation_id"))
                    .col(text("kind"))
                    .col(text("purpose"))
                    .col(text_null("provider"))
                    .col(text_null("model"))
                    .col(text("status"))
                    .col(big_integer("recorded_at_ms"))
                    .col(big_integer_null("start_wall_ms"))
                    .col(big_integer_null("end_wall_ms"))
                    .col(big_integer_null("start_mono_ns"))
                    .col(big_integer_null("end_mono_ns"))
                    .col(big_integer_null("elapsed_ns"))
                    .col(text_null("process_run_id"))
                    .col(text_null("start_process_run_id"))
                    .col(text_null("tariff_hash"))
                    .col(json("facts_json"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_observation_parent")
                    .table("execution_observation")
                    .col("workspace_id")
                    .col("parent_operation_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_observation_workspace_frontier")
                    .table("execution_observation")
                    .col("workspace_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_observation_identity")
                    .table("execution_observation")
                    .unique()
                    .col("workspace_id")
                    .col("operation_id")
                    .col("revision")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_observation_thread")
                    .table("execution_observation")
                    .col("workspace_id")
                    .col("thread_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_observation_turn")
                    .table("execution_observation")
                    .col("turn_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_observation_task")
                    .table("execution_observation")
                    .col("workspace_id")
                    .col("task_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_observation_execution")
                    .table("execution_observation")
                    .col("workspace_id")
                    .col("execution_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_execution_observation_owner")
                    .table("execution_observation")
                    .col("workspace_id")
                    .col("owner_kind")
                    .col("owner_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables(
            manager,
            &[
                "execution_observation",
                "execution_tariff",
                "agent_delegation_route",
                "native_cancellation_context",
                "provider_usage_observation",
                "native_event_cleanup_scheduler",
                "native_event_cleanup_bootstrap",
                "native_event_cleanup_job",
                "native_terminal_effect_outbox",
                "read_model_repair_dirty_turn_item",
                "read_model_repair_change_sequence",
                "read_model_repair_checkpoint",
                "codex_aggregate_state",
                "agent_work_scheduler_state",
                "agent_action_timeline_target",
                "agent_work_branch_schedule",
                "agent_domain_commit",
                "agent_running_permit",
                "agent_work_queue",
                "agent_execution_resource_state",
                "agent_work_resource_scope",
                "agent_action_outbox",
                "agent_turn_response_execution",
                "agent_execution_grant",
                "agent_delegation_route_event",
                "agent_action_receipt",
                "agent_action",
                "agent_execution",
                "agent_presentation_snapshot",
                "agent_identity",
                "native_agent_config",
                "execution_admission_lease",
                "recovery_terminalization_outbox",
                "agent_skill_version",
                "agent_skill",
                "cli_runtime_native_event",
                "cli_runtime_pending_request",
                "recovery_job",
            ],
        )
        .await?;

        manager
            .get_connection()
            .execute_unprepared("DROP FUNCTION agent_action_outbox_start_candidate(text)")
            .await?;
        Ok(())
    }
}
