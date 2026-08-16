use migration::{MigrationTrait, Migrator, MigratorTrait};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement, TryGetable};
use tempfile::TempDir;

struct BeforeLatestMigrator;

#[async_trait::async_trait]
impl MigratorTrait for BeforeLatestMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        let mut migrations = Migrator::migrations();
        migrations.pop().expect("migration registry is not empty");
        migrations
    }
}

struct BeforeTaskDeliveryTargetsMigrator;

#[async_trait::async_trait]
impl MigratorTrait for BeforeTaskDeliveryTargetsMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        let mut migrations = Migrator::migrations();
        // The task-delivery migration is followed by a later stream-state
        // migration.  This fixture must stop before the former, rather than
        // merely before the current latest migration.
        migrations.pop().expect("migration registry is not empty");
        migrations
            .pop()
            .expect("task-delivery migration is registered");
        migrations
    }
}

fn sqlite_url(directory: &TempDir) -> String {
    format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("migration-gate.sqlite3").display()
    )
}

async fn connect(url: &str) -> DatabaseConnection {
    Database::connect(url)
        .await
        .expect("connect migration test database")
}

async fn applied_migration_count(database: &DatabaseConnection) -> i64 {
    let row = database
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM seaql_migrations",
        ))
        .await
        .expect("query migration journal")
        .expect("migration journal count row");
    i64::try_get(&row, "", "count").expect("decode migration count")
}

async fn table_exists(database: &DatabaseConnection, table: &str) -> bool {
    let row = database
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table.into()],
        ))
        .await
        .expect("query sqlite schema")
        .expect("sqlite schema count row");
    i64::try_get(&row, "", "count").expect("decode table count") == 1
}

async fn column_exists(database: &DatabaseConnection, table: &str, column: &str) -> bool {
    let statement = format!(
        "SELECT COUNT(*) AS count FROM pragma_table_info('{}') WHERE name = ?",
        table.replace('\'', "''")
    );
    let row = database
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            statement,
            [column.into()],
        ))
        .await
        .expect("query sqlite table columns")
        .expect("sqlite column count row");
    i64::try_get(&row, "", "count").expect("decode column count") == 1
}

#[tokio::test]
async fn migration_fresh_schema_is_idempotent_across_database_restart() {
    let directory = tempfile::tempdir().expect("create migration test directory");
    let url = sqlite_url(&directory);
    let expected = Migrator::migrations().len() as i64;

    let first = connect(&url).await;
    Migrator::up(&first, None)
        .await
        .expect("apply fresh schema");
    assert_eq!(applied_migration_count(&first).await, expected);
    first
        .close()
        .await
        .expect("close first database connection");

    let restarted = connect(&url).await;
    Migrator::up(&restarted, None)
        .await
        .expect("reapply migrations after restart");
    assert_eq!(applied_migration_count(&restarted).await, expected);
    assert!(table_exists(&restarted, "turn").await);
    assert!(table_exists(&restarted, "task_run").await);
    assert!(table_exists(&restarted, "turn_execution_checkpoint").await);
    assert!(table_exists(&restarted, "turn_finalization").await);
    assert!(table_exists(&restarted, "turn_admission").await);
    assert!(table_exists(&restarted, "recovery_terminalization_outbox").await);
    restarted
        .close()
        .await
        .expect("close restarted database connection");
}

#[tokio::test]
async fn migration_previous_release_upgrades_after_database_restart() {
    let directory = tempfile::tempdir().expect("create migration test directory");
    let url = sqlite_url(&directory);
    let all_migrations = Migrator::migrations();
    let expected_before = all_migrations.len() as i64 - 1;
    let expected_after = all_migrations.len() as i64;

    let previous = connect(&url).await;
    BeforeLatestMigrator::up(&previous, None)
        .await
        .expect("apply previous release schema");
    assert_eq!(applied_migration_count(&previous).await, expected_before);
    previous
        .close()
        .await
        .expect("close previous release connection");

    let upgraded = connect(&url).await;
    Migrator::up(&upgraded, None)
        .await
        .expect("upgrade previous release after restart");
    assert_eq!(applied_migration_count(&upgraded).await, expected_after);
    assert!(column_exists(&upgraded, "auth_refresh_credential", "exchange_request_id").await);
    assert!(table_exists(&upgraded, "turn_admission").await);
    assert!(table_exists(&upgraded, "turn_finalization").await);
    assert!(table_exists(&upgraded, "recovery_terminalization_outbox").await);
    upgraded
        .close()
        .await
        .expect("close upgraded database connection");
}

#[tokio::test]
async fn migration_upgrades_legacy_task_delivery_contract_without_runtime_fallbacks() {
    let directory = tempfile::tempdir().expect("create migration test directory");
    let url = sqlite_url(&directory);
    let previous = connect(&url).await;
    BeforeTaskDeliveryTargetsMigrator::up(&previous, None)
        .await
        .expect("apply previous release schema");
    previous
        .execute_unprepared(
            r#"
            INSERT INTO workspace (id, name, is_active, is_current)
            VALUES ('workspace_migration', 'Migration test', 1, 1);

            INSERT INTO task (
                id, workspace_id, owner_kind, owner_id, created_by_thread_id,
                executor_kind, status, title, goal, delivery_policy_json
            ) VALUES
                ('task_legacy_origin', (SELECT id FROM workspace LIMIT 1), 'thread',
                 'thread_old_owner', 'thread_old_current', 'agent', 'scheduled',
                 'Legacy origin delivery', 'Migrate it',
                 '{"mode":"owner_thread","includeResult":true,"format":"summary"}'),
                ('task_legacy_exact', (SELECT id FROM workspace LIMIT 1), 'thread',
                 'thread_old_owner', 'thread_old_current', 'system', 'scheduled',
                 'Legacy exact delivery', 'Migrate it',
                 '{"mode":"thread","threadId":"thread_exact","includeResult":true,"format":"summary"}');

            INSERT INTO task_execution_admission (
                task_id, workspace_id, root_thread_id, initiating_principal_id,
                authorization_context_json
            ) VALUES (
                'task_legacy_origin', (SELECT id FROM workspace LIMIT 1),
                'thread_origin', 'principal_test', '{}'
            );

            INSERT INTO task_delivery (
                id, workspace_id, task_id, run_id, delivery_key, mode,
                target_thread_id, status, attempt_count, max_attempts
            ) VALUES
                ('delivery_legacy_origin', (SELECT id FROM workspace LIMIT 1),
                 'task_legacy_origin', 'run_origin',
                 'task_legacy_origin:run_origin:owner_thread:thread_origin',
                 'owner_thread', 'thread_origin', 'pending', 0, 1),
                ('delivery_legacy_exact', (SELECT id FROM workspace LIMIT 1),
                 'task_legacy_exact', 'run_exact',
                 'task_legacy_exact:run_exact:thread:thread_exact',
                 'thread', 'thread_exact', 'pending', 0, 1);
            "#,
        )
        .await
        .expect("seed legacy Task delivery rows");
    previous.close().await.expect("close previous database");

    let upgraded = connect(&url).await;
    Migrator::up(&upgraded, None)
        .await
        .expect("migrate legacy Task delivery rows");

    assert!(column_exists(&upgraded, "task_delivery", "thread_target").await);
    let origin_policy = string_column(
        &upgraded,
        "SELECT delivery_policy_json AS value FROM task WHERE id = 'task_legacy_origin'",
    )
    .await;
    let origin_policy: serde_json::Value =
        serde_json::from_str(origin_policy.as_str()).expect("origin policy JSON");
    assert_eq!(origin_policy["mode"], "thread");
    assert_eq!(origin_policy["threadTarget"], "origin_thread");
    assert_eq!(origin_policy["threadId"], "thread_origin");

    let exact_policy = string_column(
        &upgraded,
        "SELECT delivery_policy_json AS value FROM task WHERE id = 'task_legacy_exact'",
    )
    .await;
    let exact_policy: serde_json::Value =
        serde_json::from_str(exact_policy.as_str()).expect("exact policy JSON");
    assert_eq!(exact_policy["mode"], "thread");
    assert_eq!(exact_policy["threadTarget"], "exact_thread");
    assert_eq!(exact_policy["threadId"], "thread_exact");

    assert_delivery_row(
        &upgraded,
        "delivery_legacy_origin",
        "origin_thread",
        "task_legacy_origin:run_origin:thread:origin_thread:thread_origin",
    )
    .await;
    assert_delivery_row(
        &upgraded,
        "delivery_legacy_exact",
        "exact_thread",
        "task_legacy_exact:run_exact:thread:exact_thread:thread_exact",
    )
    .await;
}

async fn string_column(database: &DatabaseConnection, sql: &str) -> String {
    let row = database
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql))
        .await
        .expect("query string column")
        .expect("string column row");
    String::try_get(&row, "", "value").expect("decode string column")
}

async fn assert_delivery_row(
    database: &DatabaseConnection,
    delivery_id: &str,
    expected_target: &str,
    expected_key: &str,
) {
    let row = database
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT mode, thread_target, delivery_key FROM task_delivery WHERE id = ?",
            [delivery_id.into()],
        ))
        .await
        .expect("query migrated delivery")
        .expect("migrated delivery row");
    assert_eq!(
        String::try_get(&row, "", "mode").expect("delivery mode"),
        "thread"
    );
    assert_eq!(
        String::try_get(&row, "", "thread_target").expect("delivery thread target"),
        expected_target
    );
    assert_eq!(
        String::try_get(&row, "", "delivery_key").expect("delivery key"),
        expected_key
    );
}
