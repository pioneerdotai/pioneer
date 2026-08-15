pub use sea_orm_migration::prelude::*;

pub struct Migrator;

pub mod stable_skill_id;

mod m20260313_125253_create_workspace_table;
mod m20260510_000001_add_hook_run_resume_state;
mod m20260515_000001_unique_thread_lineage_task_run;
mod m20260517_000001_workspace_single_current;
mod m20260523_000001_turn_mcp_binding_metadata;
mod m20260531_000001_task_review;
mod m20260531_000002_remove_old_task_execution_fields;
mod m20260531_000003_task_agent_review_policy;
mod m20260603_000001_turn_execution_window;
mod m20260613_000001_turn_event_projection_state;
mod m20260614_000001_thread_episodic_memory;
mod m20260616_000001_cli_runtime_bindings;
mod m20260624_000001_turn_reasoning_effort;
mod m20260626_000001_semantic_timeline_projection;
mod m20260628_000001_turn_permission_profile;
mod m20260701_000001_turn_liveness;
mod m20260704_000001_backfill_task_agent_permission_cap;
mod m20260704_000002_thread_episodic_items_no_chunks;
mod m20260706_000001_turn_execution_security_snapshot;
mod m20260707_000001_projection_meta_config;
mod m20260714_000001_cli_runtime_turn_attempt;
mod m20260715_000001_cli_runtime_recovery_confirmation;
mod m20260715_000002_turn_mcp_projection;
mod m20260716_000001_turn_cli_runtime_instruction;
mod m20260720_000001_cli_runtime_execution_segment;
mod m20260720_000002_stable_skill_id;
mod m20260721_000001_skill_pack_installation;
mod m20260724_000001_cli_runtime_continuation_owner;
mod m20260726_000001_self_improvement_core;
mod m20260726_000002_gateway_identity_foundation;
mod m20260727_000001_thread_episodic_projection_embeddings;
mod m20260728_000001_device_sessions;
mod m20260729_000001_refresh_credentials_v2;
mod m20260730_000001_authorization_foundation;
mod m20260730_000002_execution_authorization_context;
mod m20260731_000001_invitations_member_management;
mod m20260802_000001_task_event_fanout_cursor;
mod m20260804_000001_shared_messages_authorship;
mod m20260804_000002_refresh_exchange_recovery;
mod m20260805_000001_native_durable_delivery;
mod m20260806_000001_atomic_turn_terminalization;
mod m20260810_000001_immutable_principal_avatar_revisions;
mod m20260813_000001_authorization_governance;
mod m20260815_000001_task_delivery_thread_targets;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260313_125253_create_workspace_table::Migration),
            Box::new(m20260510_000001_add_hook_run_resume_state::Migration),
            Box::new(m20260515_000001_unique_thread_lineage_task_run::Migration),
            Box::new(m20260517_000001_workspace_single_current::Migration),
            Box::new(m20260523_000001_turn_mcp_binding_metadata::Migration),
            Box::new(m20260531_000001_task_review::Migration),
            Box::new(m20260531_000002_remove_old_task_execution_fields::Migration),
            Box::new(m20260531_000003_task_agent_review_policy::Migration),
            Box::new(m20260603_000001_turn_execution_window::Migration),
            Box::new(m20260613_000001_turn_event_projection_state::Migration),
            Box::new(m20260614_000001_thread_episodic_memory::Migration),
            Box::new(m20260616_000001_cli_runtime_bindings::Migration),
            Box::new(m20260624_000001_turn_reasoning_effort::Migration),
            Box::new(m20260626_000001_semantic_timeline_projection::Migration),
            Box::new(m20260628_000001_turn_permission_profile::Migration),
            Box::new(m20260701_000001_turn_liveness::Migration),
            Box::new(m20260704_000001_backfill_task_agent_permission_cap::Migration),
            Box::new(m20260704_000002_thread_episodic_items_no_chunks::Migration),
            Box::new(m20260706_000001_turn_execution_security_snapshot::Migration),
            Box::new(m20260707_000001_projection_meta_config::Migration),
            Box::new(m20260714_000001_cli_runtime_turn_attempt::Migration),
            Box::new(m20260715_000002_turn_mcp_projection::Migration),
            Box::new(m20260715_000001_cli_runtime_recovery_confirmation::Migration),
            Box::new(m20260716_000001_turn_cli_runtime_instruction::Migration),
            Box::new(m20260720_000001_cli_runtime_execution_segment::Migration),
            Box::new(m20260720_000002_stable_skill_id::Migration),
            Box::new(m20260721_000001_skill_pack_installation::Migration),
            Box::new(m20260724_000001_cli_runtime_continuation_owner::Migration),
            Box::new(m20260726_000001_self_improvement_core::Migration),
            Box::new(m20260726_000002_gateway_identity_foundation::Migration),
            Box::new(m20260727_000001_thread_episodic_projection_embeddings::Migration),
            Box::new(m20260728_000001_device_sessions::Migration),
            Box::new(m20260729_000001_refresh_credentials_v2::Migration),
            Box::new(m20260730_000001_authorization_foundation::Migration),
            Box::new(m20260730_000002_execution_authorization_context::Migration),
            Box::new(m20260731_000001_invitations_member_management::Migration),
            Box::new(m20260802_000001_task_event_fanout_cursor::Migration),
            Box::new(m20260804_000001_shared_messages_authorship::Migration),
            Box::new(m20260804_000002_refresh_exchange_recovery::Migration),
            Box::new(m20260805_000001_native_durable_delivery::Migration),
            Box::new(m20260806_000001_atomic_turn_terminalization::Migration),
            Box::new(m20260810_000001_immutable_principal_avatar_revisions::Migration),
            Box::new(m20260813_000001_authorization_governance::Migration),
            Box::new(m20260815_000001_task_delivery_thread_targets::Migration),
        ]
    }
}
