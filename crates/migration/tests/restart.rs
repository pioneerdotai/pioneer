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
    upgraded
        .close()
        .await
        .expect("close upgraded database connection");
}
