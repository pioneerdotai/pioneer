use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        add_exact_task_reviewer(manager).await?;
        create_branch_schedule_schema(manager).await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "the Agent domain upgrade is irreversible".to_owned(),
        ))
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

const REVIEW_EVENT: &str = "task_result_review_event";
const REVIEWER_REF: &str = "reviewer_ref_json";

async fn add_exact_task_reviewer(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if manager.has_column(REVIEW_EVENT, REVIEWER_REF).await? {
        return Ok(());
    }

    if manager.get_database_backend() != DatabaseBackend::Sqlite {
        return Err(DbErr::Migration(
            "the local Agent domain upgrade requires SQLite".to_owned(),
        ));
    }

    let invalid_user_rows = manager
        .get_connection()
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS row_count FROM task_result_review_event \
             WHERE reviewer_kind = 'user' \
               AND (reviewer_user_id IS NULL OR length(trim(reviewer_user_id)) = 0)"
                .to_owned(),
        ))
        .await?
        .map(|row| row.try_get::<i64>("", "row_count"))
        .transpose()?
        .unwrap_or_default();
    if invalid_user_rows != 0 {
        return Err(DbErr::Migration(format!(
            "cannot migrate {invalid_user_rows} user review events without an exact principal"
        )));
    }

    for sql in [
        "ALTER TABLE task_result_review_event RENAME TO task_result_review_event_upgrade_source",
        "CREATE TABLE task_result_review_event (\
            id VARCHAR(96) NOT NULL PRIMARY KEY, \
            candidate_id VARCHAR(96) NOT NULL, \
            task_id VARCHAR(21) NOT NULL, \
            run_id VARCHAR(21) NOT NULL, \
            task_run_turn_id VARCHAR(96) NOT NULL, \
            reviewer_kind VARCHAR(32) NOT NULL, \
            reviewer_thread_id VARCHAR(21) NULL, \
            reviewer_turn_id VARCHAR(21) NULL, \
            reviewer_user_id VARCHAR(128) NULL, \
            reviewer_agent_spec_id VARCHAR(96) NULL, \
            event_kind VARCHAR(32) NOT NULL, \
            decision VARCHAR(32) NOT NULL, \
            feedback_text TEXT NULL, \
            feedback_json TEXT NULL, \
            confidence DOUBLE NULL, \
            supersedes_review_event_id VARCHAR(96) NULL, \
            next_task_run_turn_id VARCHAR(96) NULL, \
            created_at TIMESTAMP_WITH_TIMEZONE_TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, \
            reviewer_ref_json TEXT NOT NULL\
        )",
        "INSERT INTO task_result_review_event (\
            id, candidate_id, task_id, run_id, task_run_turn_id, reviewer_kind, \
            reviewer_thread_id, reviewer_turn_id, reviewer_user_id, reviewer_agent_spec_id, \
            event_kind, decision, feedback_text, feedback_json, confidence, \
            supersedes_review_event_id, next_task_run_turn_id, created_at, reviewer_ref_json\
         ) SELECT \
            id, candidate_id, task_id, run_id, task_run_turn_id, \
            CASE WHEN reviewer_kind IN ('parent_agent', 'review_agent') THEN 'system' ELSE reviewer_kind END, \
            CASE WHEN reviewer_kind IN ('parent_agent', 'review_agent', 'runtime_auto', 'system') THEN NULL ELSE reviewer_thread_id END, \
            CASE WHEN reviewer_kind IN ('parent_agent', 'review_agent', 'runtime_auto', 'system') THEN NULL ELSE reviewer_turn_id END, \
            CASE WHEN reviewer_kind = 'user' THEN reviewer_user_id ELSE NULL END, \
            CASE WHEN reviewer_kind IN ('parent_agent', 'review_agent', 'runtime_auto', 'system') THEN NULL ELSE reviewer_agent_spec_id END, \
            event_kind, decision, feedback_text, feedback_json, confidence, \
            supersedes_review_event_id, next_task_run_turn_id, created_at, \
            CASE WHEN reviewer_kind = 'user' \
                 THEN '{\"kind\":\"principal\",\"id\":' || json_quote(reviewer_user_id) || '}' \
                 ELSE '{\"kind\":\"runtime_policy\"}' END \
         FROM task_result_review_event_upgrade_source",
        "DROP TABLE task_result_review_event_upgrade_source",
        "CREATE INDEX idx_task_result_review_event_candidate ON task_result_review_event (candidate_id)",
        "CREATE INDEX idx_task_result_review_event_run ON task_result_review_event (run_id)",
        "CREATE INDEX idx_task_result_review_event_reviewer_thread ON task_result_review_event (reviewer_thread_id, reviewer_turn_id)",
        "CREATE INDEX idx_task_result_review_event_reviewer_user ON task_result_review_event (reviewer_user_id)",
        "CREATE INDEX idx_task_result_review_event_reviewer_agent ON task_result_review_event (reviewer_agent_spec_id)",
        "CREATE INDEX idx_task_result_review_event_next_turn ON task_result_review_event (next_task_run_turn_id)",
    ] {
        manager.get_connection().execute_unprepared(sql).await?;
    }
    Ok(())
}

// Branch scheduling is new Agent-domain state. It is created directly in its
// final shape; no data can legitimately exist in the preceding fresh Agent
// tables before the migrator completes this batch.

const RESOURCE_SCOPE_TABLE: &str = "agent_work_resource_scope";
const BRANCH_SCHEDULE_TABLE: &str = "agent_work_branch_schedule";

async fn create_branch_schedule_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(BRANCH_SCHEDULE_TABLE))
                .if_not_exists()
                .col(string("root_execution_id").string_len(21))
                .col(string("branch_key").string_len(255))
                .col(timestamp_with_time_zone("last_scheduled_at").null())
                .col(big_integer("last_scheduled_sequence").default(0))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .primary_key(
                    Index::create()
                        .col(Alias::new("root_execution_id"))
                        .col(Alias::new("branch_key")),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_work_branch_root")
                        .from(
                            Alias::new(BRANCH_SCHEDULE_TABLE),
                            Alias::new("root_execution_id"),
                        )
                        .to(
                            Alias::new(RESOURCE_SCOPE_TABLE),
                            Alias::new("root_execution_id"),
                        )
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_work_branch_schedule_sequence",
                    Expr::cust("last_scheduled_sequence >= 0"),
                ))
                .to_owned(),
        )
        .await
}
