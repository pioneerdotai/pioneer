use sea_orm_migration::{
    prelude::*,
    schema::{big_integer, integer, string, text, timestamp_with_time_zone},
};

const SOURCE_TURN: &str = "self_improvement_source_turn";
const WORKSPACE_STATE: &str = "self_improvement_workspace_state";
const RUN: &str = "self_improvement_run";
const AGENT_SKILL: &str = "agent_skill";
const AGENT_SKILL_VERSION: &str = "agent_skill_version";
const TURN_RUNTIME_SNAPSHOT: &str = "turn_runtime_snapshot";
const AGENT_SKILL_VERSIONS_JSON: &str = "agent_skill_versions_json";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_source_turn(manager).await?;
        create_workspace_state(manager).await?;
        create_run(manager).await?;
        create_agent_skill(manager).await?;
        create_agent_skill_version(manager).await?;
        create_indexes(manager).await?;
        if !manager
            .has_column(TURN_RUNTIME_SNAPSHOT, AGENT_SKILL_VERSIONS_JSON)
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(TURN_RUNTIME_SNAPSHOT))
                        .add_column(text(AGENT_SKILL_VERSIONS_JSON).null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column(TURN_RUNTIME_SNAPSHOT, AGENT_SKILL_VERSIONS_JSON)
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(TURN_RUNTIME_SNAPSHOT))
                        .drop_column(Alias::new(AGENT_SKILL_VERSIONS_JSON))
                        .to_owned(),
                )
                .await?;
        }
        drop_table(manager, AGENT_SKILL).await?;
        drop_table(manager, AGENT_SKILL_VERSION).await?;
        drop_table(manager, RUN).await?;
        drop_table(manager, WORKSPACE_STATE).await?;
        drop_table(manager, SOURCE_TURN).await?;
        Ok(())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn create_source_turn(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(SOURCE_TURN))
                .col(integer("id").auto_increment().primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("turn_id").string_len(21).unique_key())
                .col(
                    string("task_delivery_id")
                        .string_len(21)
                        .null()
                        .unique_key(),
                )
                .col(string("terminal_event_id").string_len(21).unique_key())
                .col(timestamp_with_time_zone("terminal_at"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .foreign_key(&mut workspace_foreign_key(
                    "fk_self_improvement_source_turn_workspace",
                    SOURCE_TURN,
                ))
                .to_owned(),
        )
        .await
}

async fn create_workspace_state(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(WORKSPACE_STATE))
                .col(string("workspace_id").string_len(21).primary_key())
                .col(big_integer("activation_epoch").default(0))
                .col(big_integer("cursor_source_id").default(0))
                .col(timestamp_with_time_zone("effective_enabled_at").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .check((
                    "ck_self_improvement_workspace_state_non_negative",
                    Expr::cust("activation_epoch >= 0 AND cursor_source_id >= 0"),
                ))
                .foreign_key(&mut workspace_foreign_key(
                    "fk_self_improvement_workspace_state_workspace",
                    WORKSPACE_STATE,
                ))
                .to_owned(),
        )
        .await
}

async fn create_run(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(RUN))
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(big_integer("activation_epoch"))
                .col(string("scheduled_date_utc").string_len(10))
                .col(big_integer("source_lower_exclusive"))
                .col(big_integer("source_upper_inclusive"))
                .col(string("status").string_len(32))
                .col(string("claim_token").string_len(64).null())
                .col(string("claimed_by").string_len(255).null())
                .col(timestamp_with_time_zone("lease_expires_at").null())
                .col(integer("attempt_count").default(0))
                .col(timestamp_with_time_zone("next_attempt_at").null())
                .col(string("learner_provider").string_len(255))
                .col(string("learner_model").string_len(255))
                .col(string("reviewer_provider").string_len(255))
                .col(string("reviewer_model").string_len(255))
                .col(string("pipeline_contract_version").string_len(64))
                .col(text("analysis_cursor_json").null())
                .col(text("analysis_digest_json").null())
                .col(string("outcome").string_len(32).null())
                .col(string("applied_action").string_len(32).null())
                .col(string("skill_id").string_len(21).null())
                .col(string("previous_version_id").string_len(21).null())
                .col(string("resulting_version_id").string_len(21).null())
                .col(text("result_summary").null())
                .col(text("last_error").null())
                .col(
                    timestamp_with_time_zone("created_at").default(Expr::current_timestamp()),
                )
                .col(
                    timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()),
                )
                .check((
                    "ck_self_improvement_run_status",
                    Expr::cust(
                        "status IN ('pending', 'running', 'completed', 'failed', 'cancelled')",
                    ),
                ))
                .check((
                    "ck_self_improvement_run_outcome",
                    Expr::cust("outcome IS NULL OR outcome IN ('applied', 'no_change')"),
                ))
                .check((
                    "ck_self_improvement_run_action",
                    Expr::cust(
                        "applied_action IS NULL OR applied_action IN ('create', 'update', 'rollback')",
                    ),
                ))
                .check((
                    "ck_self_improvement_run_non_negative",
                    Expr::cust(
                        "activation_epoch >= 0 AND source_lower_exclusive >= 0 \
                         AND source_upper_inclusive >= source_lower_exclusive \
                         AND attempt_count >= 0",
                    ),
                ))
                .foreign_key(&mut workspace_foreign_key(
                    "fk_self_improvement_run_workspace",
                    RUN,
                ))
                .to_owned(),
        )
        .await
}

async fn create_agent_skill(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_SKILL))
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("slug").string_len(255))
                .col(string("active_version_id").string_len(21).null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .foreign_key(&mut workspace_foreign_key(
                    "fk_agent_skill_workspace",
                    AGENT_SKILL,
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_skill_active_version")
                        .from(Alias::new(AGENT_SKILL), Alias::new("active_version_id"))
                        .to(Alias::new(AGENT_SKILL_VERSION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::SetNull),
                )
                .to_owned(),
        )
        .await
}

async fn create_agent_skill_version(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_SKILL_VERSION))
                .col(string("id").string_len(21).primary_key())
                .col(string("skill_id").string_len(21))
                .col(big_integer("version_number"))
                .col(string("source_run_id").string_len(21).null())
                .col(string("parent_version_id").string_len(21).null())
                .col(string("candidate_key").string_len(128))
                .col(string("display_name").string_len(255))
                .col(text("skill_markdown"))
                .col(text("instruction_body"))
                .col(text("when_to_use"))
                .col(text("when_not_to_use"))
                .col(string("fingerprint").string_len(128))
                .col(text("source_turn_ids_json"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .check((
                    "ck_agent_skill_version_number",
                    Expr::cust("version_number >= 1"),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_skill_version_skill")
                        .from(Alias::new(AGENT_SKILL_VERSION), Alias::new("skill_id"))
                        .to(Alias::new(AGENT_SKILL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_skill_version_source_run")
                        .from(Alias::new(AGENT_SKILL_VERSION), Alias::new("source_run_id"))
                        .to(Alias::new(RUN), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::SetNull),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_skill_version_parent")
                        .from(
                            Alias::new(AGENT_SKILL_VERSION),
                            Alias::new("parent_version_id"),
                        )
                        .to(Alias::new(AGENT_SKILL_VERSION), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::SetNull),
                )
                .to_owned(),
        )
        .await
}

async fn create_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_self_improvement_source_turn_workspace_id")
            .table(Alias::new(SOURCE_TURN))
            .col(Alias::new("workspace_id"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .name("idx_self_improvement_source_turn_thread_id")
            .table(Alias::new(SOURCE_TURN))
            .col(Alias::new("workspace_id"))
            .col(Alias::new("thread_id"))
            .col(Alias::new("id"))
            .to_owned(),
        Index::create()
            .name("uq_self_improvement_run_daily")
            .table(Alias::new(RUN))
            .col(Alias::new("workspace_id"))
            .col(Alias::new("activation_epoch"))
            .col(Alias::new("scheduled_date_utc"))
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_self_improvement_run_claimable")
            .table(Alias::new(RUN))
            .col(Alias::new("status"))
            .col(Alias::new("next_attempt_at"))
            .col(Alias::new("lease_expires_at"))
            .to_owned(),
        Index::create()
            .name("idx_self_improvement_run_workspace_status")
            .table(Alias::new(RUN))
            .col(Alias::new("workspace_id"))
            .col(Alias::new("status"))
            .col(Alias::new("scheduled_date_utc"))
            .to_owned(),
        Index::create()
            .name("uq_agent_skill_workspace_slug")
            .table(Alias::new(AGENT_SKILL))
            .col(Alias::new("workspace_id"))
            .col(Alias::new("slug"))
            .unique()
            .to_owned(),
        Index::create()
            .name("uq_agent_skill_version_number")
            .table(Alias::new(AGENT_SKILL_VERSION))
            .col(Alias::new("skill_id"))
            .col(Alias::new("version_number"))
            .unique()
            .to_owned(),
        Index::create()
            .name("uq_agent_skill_version_fingerprint")
            .table(Alias::new(AGENT_SKILL_VERSION))
            .col(Alias::new("skill_id"))
            .col(Alias::new("fingerprint"))
            .unique()
            .to_owned(),
        Index::create()
            .name("uq_agent_skill_version_run_candidate")
            .table(Alias::new(AGENT_SKILL_VERSION))
            .col(Alias::new("source_run_id"))
            .col(Alias::new("candidate_key"))
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_agent_skill_version_parent")
            .table(Alias::new(AGENT_SKILL_VERSION))
            .col(Alias::new("parent_version_id"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

fn workspace_foreign_key(name: &str, from_table: &str) -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name(name)
        .from(Alias::new(from_table), Alias::new("workspace_id"))
        .to(Alias::new("workspace"), Alias::new("id"))
        .on_update(ForeignKeyAction::NoAction)
        .on_delete(ForeignKeyAction::Cascade)
        .to_owned()
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
