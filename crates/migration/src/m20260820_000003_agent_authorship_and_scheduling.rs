use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        add_turn_agent_author_snapshot(manager).await?;
        create_action_timeline_target_schema(manager).await?;
        add_thread_preview_author(manager).await?;
        create_scheduler_fairness_schema(manager).await?;
        migrate_conversation_actor_constraints(manager).await?;
        migrate_task_turn_execution_ownership(manager).await?;
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

// --- turn agent author snapshot ---

const TURN_TABLE: &str = "turn";
const TURN_AUTHOR_SNAPSHOT_COLUMN: &str = "author_agent_snapshot_json";

async fn add_turn_agent_author_snapshot(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if !manager
        .has_column(TURN_TABLE, TURN_AUTHOR_SNAPSHOT_COLUMN)
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TURN_TABLE))
                    .add_column(text(TURN_AUTHOR_SNAPSHOT_COLUMN).null())
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}

// --- action timeline target ---

const ACTION_TIMELINE_TARGET_TABLE: &str = "agent_action_timeline_target";
const TURN_ITEM_BACKING_TABLE: &str = "_turn_item_zstd";

async fn create_action_timeline_target_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    // sqlite-zstd exposes `turn_item` as a writable view after moving the
    // durable rows into `_turn_item_zstd`. SQLite foreign keys cannot target a
    // view, so upgrades of an already-compressed database must reference the
    // backing table. On a fresh database the ordinary table is still present;
    // sqlite-zstd's later rename rewrites this reference to the backing name.
    let turn_item_table = if manager.has_table(TURN_ITEM_BACKING_TABLE).await? {
        TURN_ITEM_BACKING_TABLE
    } else {
        "turn_item"
    };
    manager
        .create_table(
            Table::create()
                .table(Alias::new(ACTION_TIMELINE_TARGET_TABLE))
                .if_not_exists()
                .col(string("target_key").string_len(64).primary_key())
                .col(string("action_id").string_len(21).unique_key())
                .col(string("turn_id").string_len(21))
                .col(string("turn_item_id").string_len(21).null().unique_key())
                .col(string("target_kind").string_len(16))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_action_timeline_target_action")
                        .from(
                            Alias::new(ACTION_TIMELINE_TARGET_TABLE),
                            Alias::new("action_id"),
                        )
                        .to(Alias::new("agent_action"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_action_timeline_target_turn")
                        .from(
                            Alias::new(ACTION_TIMELINE_TARGET_TABLE),
                            Alias::new("turn_id"),
                        )
                        .to(Alias::new("turn"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_action_timeline_target_item")
                        .from(
                            Alias::new(ACTION_TIMELINE_TARGET_TABLE),
                            Alias::new("turn_item_id"),
                        )
                        .to(Alias::new(turn_item_table), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .check((
                    "ck_agent_action_timeline_target_shape",
                    Expr::cust(
                        "(target_kind = 'turn_input' AND turn_item_id IS NULL) OR \
                         (target_kind = 'turn_item' AND turn_item_id IS NOT NULL)",
                    ),
                ))
                .to_owned(),
        )
        .await
}

// --- thread preview author ---

const THREAD_TABLE: &str = "thread";
const THREAD_PREVIEW_AUTHOR_COLUMN: &str = "preview_author_json";

async fn add_thread_preview_author(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if !manager
        .has_column(THREAD_TABLE, THREAD_PREVIEW_AUTHOR_COLUMN)
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(THREAD_TABLE))
                    .add_column(text(THREAD_PREVIEW_AUTHOR_COLUMN).null())
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}

// --- scheduler fairness cursor ---

const SCHEDULER_RESOURCE_SCOPE_TABLE: &str = "agent_work_resource_scope";
const SCHEDULER_BRANCH_TABLE: &str = "agent_work_branch_schedule";
const SCHEDULER_STATE_TABLE: &str = "agent_work_scheduler_state";
const SCHEDULER_SEQUENCE_COLUMN: &str = "last_scheduled_sequence";

async fn create_scheduler_fairness_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(SCHEDULER_STATE_TABLE))
                .if_not_exists()
                .col(string("scheduler_key").string_len(32).primary_key())
                .col(big_integer("schedule_generation").default(0))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .check((
                    "ck_agent_work_scheduler_generation",
                    Expr::cust("schedule_generation >= 0"),
                ))
                .to_owned(),
        )
        .await?;
    let insert = match manager.get_database_backend() {
        sea_orm::DatabaseBackend::MySql => {
            "INSERT IGNORE INTO agent_work_scheduler_state \
             (scheduler_key, schedule_generation, updated_at) \
             VALUES ('global', 0, CURRENT_TIMESTAMP)"
        }
        sea_orm::DatabaseBackend::Postgres | sea_orm::DatabaseBackend::Sqlite => {
            "INSERT INTO agent_work_scheduler_state \
             (scheduler_key, schedule_generation, updated_at) \
             VALUES ('global', 0, CURRENT_TIMESTAMP) \
             ON CONFLICT(scheduler_key) DO NOTHING"
        }
        _ => {
            return Err(DbErr::Custom(
                "unsupported database backend for agent domain scheduler cursor".to_owned(),
            ));
        }
    };
    manager.get_connection().execute_unprepared(insert).await?;
    manager
        .create_index(
            Index::create()
                .name("idx_agent_work_scope_fair_cursor")
                .table(Alias::new(SCHEDULER_RESOURCE_SCOPE_TABLE))
                .col(Alias::new("status"))
                .col(Alias::new(SCHEDULER_SEQUENCE_COLUMN))
                .col(Alias::new("created_at"))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .name("idx_agent_work_branch_fair_cursor")
                .table(Alias::new(SCHEDULER_BRANCH_TABLE))
                .col(Alias::new("root_execution_id"))
                .col(Alias::new(SCHEDULER_SEQUENCE_COLUMN))
                .col(Alias::new("created_at"))
                .to_owned(),
        )
        .await?;
    Ok(())
}

// --- conversation actor constraints ---

const THREAD_ACTOR_COLUMNS: ActorColumns = ActorColumns {
    table: "thread",
    kind: "created_by_actor_kind",
    actor_id: "created_by_actor_id",
};
const TURN_ACTOR_COLUMNS: ActorColumns = ActorColumns {
    table: "turn",
    kind: "initiated_by_actor_kind",
    actor_id: "initiated_by_actor_id",
};

#[derive(Clone, Copy)]
struct ActorColumns {
    table: &'static str,
    kind: &'static str,
    actor_id: &'static str,
}

/// Rebuilds the existing actor-kind columns in place while the application is
/// stopped for its normal schema upgrade. The migration is transactional, so
/// Gateway never observes a partially converted actor contract.
async fn migrate_conversation_actor_constraints(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    migrate_actor_kind_constraint(manager, THREAD_ACTOR_COLUMNS).await?;
    migrate_actor_kind_constraint(manager, TURN_ACTOR_COLUMNS).await?;
    Ok(())
}

/// Existing materialized Task turns predate the mandatory execution-owner
/// row. Convert every one while the application is stopped so runtime and
/// recovery have a single invariant: a durable Turn always owns a
/// `turn_execution` record. Historical failed Task attempts that never
/// materialized their preallocated Turn id remain ordinary terminal attempt
/// records and must not gain an orphan execution row.
async fn migrate_task_turn_execution_ownership(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if manager.get_database_backend() != DatabaseBackend::Sqlite {
        return Err(DbErr::Migration(
            "the local Agent domain upgrade requires SQLite".to_owned(),
        ));
    }
    execute(
        manager,
        "INSERT INTO turn_execution (\
            turn_id, thread_id, workspace_id, executor_kind, executor_key, status, \
            owner_id, owner_generation, lease_until, heartbeat_at, started_at, completed_at, \
            created_at, updated_at\
         ) SELECT \
            task_turn.turn_id, task_turn.thread_id, task.workspace_id, \
            CASE WHEN EXISTS (\
                SELECT 1 FROM turn_cli_runtime_instruction instruction \
                WHERE instruction.turn_id = task_turn.turn_id\
            ) THEN 'cli_runtime' ELSE 'api_provider' END, \
            NULL, \
            CASE task_turn.status \
                WHEN 'in_progress' THEN 'recovering' \
                WHEN 'candidate_created' THEN 'completed' \
                WHEN 'review_recorded' THEN 'completed' \
                WHEN 'failed' THEN 'failed' \
                WHEN 'blocked' THEN 'blocked' \
                WHEN 'interrupted' THEN 'interrupted' \
                WHEN 'cancelled' THEN 'interrupted' \
                ELSE 'blocked' \
            END, \
            'database_upgrade', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, \
            CASE WHEN task_turn.status = 'in_progress' \
                 THEN COALESCE(task_turn.started_at, task_turn.created_at) \
                 ELSE task_turn.started_at END, \
            CASE WHEN task_turn.status = 'in_progress' THEN NULL \
                 ELSE COALESCE(task_turn.completed_at, task_turn.created_at) END, \
            task_turn.created_at, CURRENT_TIMESTAMP \
         FROM task_run_turn task_turn \
         JOIN task ON task.id = task_turn.task_id \
         JOIN turn materialized_turn \
           ON materialized_turn.id = task_turn.turn_id \
          AND materialized_turn.thread_id = task_turn.thread_id \
         LEFT JOIN turn_execution execution ON execution.turn_id = task_turn.turn_id \
         WHERE execution.turn_id IS NULL"
            .to_owned(),
    )
    .await?;

    let missing = manager
        .get_connection()
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS row_count FROM task_run_turn task_turn \
             JOIN turn materialized_turn \
               ON materialized_turn.id = task_turn.turn_id \
              AND materialized_turn.thread_id = task_turn.thread_id \
             LEFT JOIN turn_execution execution ON execution.turn_id = task_turn.turn_id \
             WHERE execution.turn_id IS NULL"
                .to_owned(),
        ))
        .await?
        .map(|row| row.try_get::<i64>("", "row_count"))
        .transpose()?
        .unwrap_or_default();
    if missing != 0 {
        return Err(DbErr::Migration(format!(
            "failed to migrate {missing} Task turn execution-owner rows"
        )));
    }
    Ok(())
}

async fn migrate_actor_kind_constraint(
    manager: &SchemaManager<'_>,
    columns: ActorColumns,
) -> Result<(), DbErr> {
    let replacement = actor_kind_replacement(columns);
    if !manager
        .has_column(columns.table, replacement.as_str())
        .await?
    {
        let backend = manager.get_database_backend();
        let principal_clause = valid_identified_actor_clause(
            backend,
            replacement.as_str(),
            columns.actor_id,
            "principal",
        )?;
        let agent_clause = valid_identified_actor_clause(
            backend,
            replacement.as_str(),
            columns.actor_id,
            "agent_execution",
        )?;
        let check = format!(
            "({replacement} IS NULL AND {actor_id} IS NULL) \
              OR ({replacement} = 'system' AND {actor_id} IS NULL) \
              OR {principal_clause} OR {agent_clause}",
            actor_id = columns.actor_id,
        );
        let constraint_name = format!("ck_{}_{}_agent_execution", columns.table, columns.kind);
        execute(
            manager,
            format!(
                "ALTER TABLE {table} ADD COLUMN {replacement} VARCHAR(32) NULL \
                 CONSTRAINT {constraint_name} CHECK ({check})",
                table = columns.table,
            ),
        )
        .await?;
    }

    execute(
        manager,
        format!(
            "UPDATE {table} SET {replacement} = {kind}",
            table = columns.table,
            kind = columns.kind,
        ),
    )
    .await?;
    ensure_actor_copy_complete(manager, columns).await?;
    drop_sqlite_pair_guards(manager, columns).await?;
    execute(
        manager,
        format!(
            "ALTER TABLE {table} DROP COLUMN {kind}",
            table = columns.table,
            kind = columns.kind,
        ),
    )
    .await?;
    execute(
        manager,
        format!(
            "ALTER TABLE {table} RENAME COLUMN {replacement} TO {kind}",
            table = columns.table,
            kind = columns.kind,
        ),
    )
    .await?;
    create_sqlite_pair_guards(manager, columns).await
}

async fn ensure_actor_copy_complete(
    manager: &SchemaManager<'_>,
    columns: ActorColumns,
) -> Result<(), DbErr> {
    let replacement = actor_kind_replacement(columns);
    let row = manager
        .get_connection()
        .query_one_raw(Statement::from_string(
            manager.get_database_backend(),
            format!(
                "SELECT COUNT(*) AS mismatch_count FROM {table} \
                 WHERE COALESCE({replacement}, '') <> COALESCE({kind}, '')",
                table = columns.table,
                kind = columns.kind,
            ),
        ))
        .await?;
    let mismatch_count = row
        .map(|row| row.try_get::<i64>("", "mismatch_count"))
        .transpose()?
        .unwrap_or_default();
    if mismatch_count != 0 {
        return Err(DbErr::Migration(format!(
            "failed to migrate {} conversation actor rows in {}",
            mismatch_count, columns.table,
        )));
    }
    Ok(())
}

fn actor_kind_replacement(columns: ActorColumns) -> String {
    format!("{}_actor_constraint", columns.kind)
}

async fn drop_sqlite_pair_guards(
    manager: &SchemaManager<'_>,
    columns: ActorColumns,
) -> Result<(), DbErr> {
    if manager.get_database_backend() != DatabaseBackend::Sqlite {
        return Ok(());
    }
    for suffix in ["insert", "update"] {
        execute(
            manager,
            format!(
                "DROP TRIGGER IF EXISTS {}",
                pair_guard_name(columns, suffix)
            ),
        )
        .await?;
    }
    Ok(())
}

async fn create_sqlite_pair_guards(
    manager: &SchemaManager<'_>,
    columns: ActorColumns,
) -> Result<(), DbErr> {
    if manager.get_database_backend() != DatabaseBackend::Sqlite {
        return Ok(());
    }
    let predicate = strict_actor_pair_predicate(columns);
    for (suffix, timing) in [("insert", "INSERT"), ("update", "UPDATE")] {
        execute(
            manager,
            format!(
                "CREATE TRIGGER {trigger} BEFORE {timing} ON {table} \
                 WHEN NOT ({predicate}) \
                 BEGIN \
                     SELECT RAISE(ABORT, 'invalid persisted conversation actor pair'); \
                 END",
                trigger = pair_guard_name(columns, suffix),
                table = columns.table,
            ),
        )
        .await?;
    }
    Ok(())
}

fn strict_actor_pair_predicate(columns: ActorColumns) -> String {
    let kind = format!("NEW.{}", columns.kind);
    let actor_id = format!("NEW.{}", columns.actor_id);
    let principal = valid_sqlite_identified_actor_clause(&kind, &actor_id, "principal");
    let agent = valid_sqlite_identified_actor_clause(&kind, &actor_id, "agent_execution");
    format!(
        "(({kind} IS NULL AND {actor_id} IS NULL) \
          OR ({kind} IS NOT NULL AND (\
              ({kind} = 'system' AND {actor_id} IS NULL) \
              OR {principal} OR {agent}\
          )))"
    )
}

fn valid_sqlite_identified_actor_clause(kind: &str, actor_id: &str, expected_kind: &str) -> String {
    format!(
        "({kind} = '{expected_kind}' AND {actor_id} IS NOT NULL \
          AND length({actor_id}) = 21 \
          AND {actor_id} NOT GLOB '*[^A-Za-z0-9]*')"
    )
}

fn pair_guard_name(columns: ActorColumns, suffix: &str) -> String {
    format!(
        "conversation_actor_{}_{}_pair_{}",
        columns.table, columns.kind, suffix
    )
}

fn valid_identified_actor_clause(
    backend: DatabaseBackend,
    kind: &str,
    actor_id: &str,
    expected_kind: &str,
) -> Result<String, DbErr> {
    let valid_characters = match backend {
        DatabaseBackend::Sqlite => format!("{actor_id} NOT GLOB '*[^A-Za-z0-9]*'"),
        DatabaseBackend::Postgres => format!("{actor_id} !~ '[^A-Za-z0-9]'"),
        DatabaseBackend::MySql => format!("{actor_id} NOT REGEXP '[^A-Za-z0-9]'"),
        _ => {
            return Err(DbErr::Migration(
                "agent domain actor constraints do not support this database backend".to_owned(),
            ));
        }
    };
    Ok(format!(
        "({kind} = '{expected_kind}' AND {actor_id} IS NOT NULL \
          AND length({actor_id}) = 21 \
          AND {valid_characters})"
    ))
}

async fn execute(manager: &SchemaManager<'_>, sql: String) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_raw(Statement::from_string(manager.get_database_backend(), sql))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_identifier_predicate_uses_backend_specific_operators() {
        let sqlite = valid_identified_actor_clause(
            DatabaseBackend::Sqlite,
            "actor_kind",
            "actor_id",
            "agent_execution",
        )
        .expect("SQLite actor predicate should be supported");
        assert!(sqlite.contains("NOT GLOB"));
        assert!(!sqlite.contains("REGEXP"));

        let postgres = valid_identified_actor_clause(
            DatabaseBackend::Postgres,
            "actor_kind",
            "actor_id",
            "agent_execution",
        )
        .expect("Postgres actor predicate should be supported");
        assert!(postgres.contains("!~ '[^A-Za-z0-9]'"));
        assert!(!postgres.contains("GLOB"));

        let mysql = valid_identified_actor_clause(
            DatabaseBackend::MySql,
            "actor_kind",
            "actor_id",
            "agent_execution",
        )
        .expect("MySQL actor predicate should be supported");
        assert!(mysql.contains("NOT REGEXP"));
        assert!(!mysql.contains("GLOB"));
    }
}
