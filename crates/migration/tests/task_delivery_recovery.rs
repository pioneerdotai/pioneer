//! Runtime regressions are intentionally deferred until implementation acceptance.
use migration::{Migrator, MigratorTrait};
use pioneer_entity::{task_delivery as delivery, task_delivery_recovery_retry as retry};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, Database, DatabaseBackend, DbErr, EntityTrait, Set,
    Statement, TransactionTrait,
};

const MIGRATION: &str = "m20261004_000006_task_delivery_recovery";

// Keep schema tests on the recovery release, so down(1) targets recovery
// rather than a later, potentially irreversible migration.
struct RecoveryFixtureMigrator;
impl MigratorTrait for RecoveryFixtureMigrator {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        let mut migrations = Migrator::migrations();
        migrations.truncate(migration_position() + 1);
        migrations
    }
}
const OBJECTS: [&str; 8] = [
    "idx_task_delivery_recovery_source",
    "task_delivery_recovery_retry",
    "idx_task_delivery_recovery_retry_due",
    "task_delivery_recovery_retry_task_delivery_update",
    "task_delivery_recovery_retry_task_delivery_delete",
    "task_delivery_recovery_retry_task_delivery_attempt_insert",
    "task_delivery_recovery_retry_task_delivery_attempt_update",
    "task_delivery_recovery_retry_task_delivery_attempt_delete",
];

fn migration_position() -> usize {
    Migrator::migrations()
        .iter()
        .position(|m| m.name() == MIGRATION)
        .expect("delivery recovery migration is registered")
}

async fn before_recovery() -> (tempfile::TempDir, SqliteDatabase) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("schema.sqlite3");
    let mut writer_options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    writer_options.max_connections(1).min_connections(1);
    let writer = Database::connect(writer_options).await.unwrap();
    let mut reader_options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    reader_options
        .map_sqlx_sqlite_opts(|options| options.read_only(true).pragma("query_only", "ON"));
    let reader = Database::connect(reader_options).await.unwrap();
    let database = SqliteDatabase::new(reader, writer).maintenance();
    up(&database, Some(migration_position() as u32))
        .await
        .unwrap();
    (directory, database)
}

async fn up(database: &SqliteDatabase, steps: Option<u32>) -> Result<(), DbErr> {
    // Production executor: schema DDL and its completion marker share a writer
    // transaction. Failure rolls back even DDL installed before the failure.
    let tx = database.begin().await?;
    match RecoveryFixtureMigrator::up(&*tx, steps).await {
        Ok(()) => tx.commit().await,
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn objects(database: &SqliteDatabase) -> Vec<String> {
    let mut names = Vec::new();
    for name in OBJECTS {
        if database
            .query_one_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE name=?",
                [name.into()],
            ))
            .await
            .unwrap()
            .is_some()
        {
            names.push(name.to_owned());
        }
    }
    names
}

async fn marker(database: &SqliteDatabase) -> bool {
    database
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT version FROM seaql_migrations WHERE version=?",
            [MIGRATION.into()],
        ))
        .await
        .unwrap()
        .is_some()
}

async fn assert_coverage(database: &SqliteDatabase) {
    assert_eq!(objects(database).await, OBJECTS);
    assert!(marker(database).await);
    for (index, columns) in [
        (OBJECTS[0], vec!["status", "updated_at", "id"]),
        (OBJECTS[2], vec!["next_probe_at", "delivery_id"]),
    ] {
        let rows = database
            .query_all_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                [index.into()],
            ))
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|r| r.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>(),
            columns
        );
    }
    let rows = database
        .query_all_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT name,pk FROM pragma_table_info(?) ORDER BY cid",
            [OBJECTS[1].into()],
        ))
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.try_get::<String>("", "name").unwrap())
            .collect::<Vec<_>>(),
        [
            "delivery_id",
            "expected_attempt_id",
            "expected_attempt_count",
            "expected_updated_at",
            "retry_token",
            "next_probe_at",
            "attempts"
        ]
    );
    assert_eq!(rows[0].try_get::<i64>("", "pk").unwrap(), 1);
    assert!(database.reader_query_only_enabled().await.unwrap());
}

async fn insert_retry(database: &SqliteDatabase) -> retry::Model {
    retry::ActiveModel {
        delivery_id: Set("delivery".to_owned()),
        expected_attempt_id: Set(None),
        expected_attempt_count: Set(1),
        expected_updated_at: Set("2026-10-04T00:00:00Z".parse().unwrap()),
        retry_token: Set("original-token".to_owned()),
        next_probe_at: Set(4_000_000_306),
        attempts: Set(6),
    }
    .insert(database)
    .await
    .unwrap()
}

#[tokio::test]
async fn recovery_migration_installs_objects_and_repeated_up_preserves_backoff() {
    let (_directory, database) = before_recovery().await;
    up(&database, None).await.unwrap();
    assert_coverage(&database).await;
    let before = insert_retry(&database).await;
    up(&database, None).await.unwrap();
    assert_coverage(&database).await;
    assert_eq!(
        retry::Entity::find_by_id("delivery")
            .one(&database)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    database.close().await.unwrap();
}

#[tokio::test]
async fn recovery_migration_ddl_failure_rolls_back_objects_and_marker_then_retries() {
    let (_directory, database) = before_recovery().await;
    // Fourth trigger collides: index/table/due index and three triggers have
    // already been installed when this DDL fails. This object predates up.
    database.execute_unprepared("CREATE TRIGGER task_delivery_recovery_retry_task_delivery_attempt_update AFTER UPDATE ON task_delivery_attempt WHEN 0 BEGIN SELECT 1; END").await.unwrap();
    assert!(up(&database, None).await.is_err());
    assert_eq!(objects(&database).await, [OBJECTS[6]]);
    assert!(!marker(&database).await);
    database
        .execute_unprepared(
            "DROP TRIGGER task_delivery_recovery_retry_task_delivery_attempt_update",
        )
        .await
        .unwrap();
    up(&database, None).await.unwrap();
    assert_coverage(&database).await;
    database.close().await.unwrap();
}

#[tokio::test]
async fn recovery_migration_down_then_up_restores_all_physical_cleanup_coverage() {
    let (_directory, database) = before_recovery().await;
    up(&database, None).await.unwrap();
    let tx = database.begin().await.unwrap();
    RecoveryFixtureMigrator::down(&*tx, Some(1)).await.unwrap();
    tx.commit().await.unwrap();
    assert!(objects(&database).await.is_empty());
    assert!(!marker(&database).await);
    up(&database, None).await.unwrap();
    assert_coverage(&database).await;
    delivery::ActiveModel {
        id: Set("delivery".to_owned()),
        workspace_id: Set("ws".to_owned()),
        task_id: Set("task".to_owned()),
        run_id: Set("run".to_owned()),
        delivery_key: Set("key".to_owned()),
        mode: Set("thread".to_owned()),
        thread_target: Set(Some("exact_thread".to_owned())),
        target_thread_id: Set(Some("thread".to_owned())),
        status: Set("delivering".to_owned()),
        attempt_count: Set(1),
        max_attempts: Set(3),
        created_at: Set("2026-10-04T00:00:00Z".parse().unwrap()),
        updated_at: Set("2026-10-04T00:00:00Z".parse().unwrap()),
        ..Default::default()
    }
    .insert(&database)
    .await
    .unwrap();
    insert_retry(&database).await;
    database
        .execute_unprepared("UPDATE task_delivery SET status=status WHERE id='delivery'")
        .await
        .unwrap();
    assert!(
        retry::Entity::find_by_id("delivery")
            .one(&database)
            .await
            .unwrap()
            .is_some()
    );
    database
        .execute_unprepared("UPDATE task_delivery SET last_error='changed' WHERE id='delivery'")
        .await
        .unwrap();
    assert!(
        retry::Entity::find_by_id("delivery")
            .one(&database)
            .await
            .unwrap()
            .is_none()
    );
    insert_retry(&database).await;
    database.execute_unprepared("INSERT INTO task_delivery_attempt(id,delivery_id,attempt_number,status,started_at) VALUES ('attempt','delivery',1,'delivering','2026-10-04T00:00:00Z')").await.unwrap();
    assert!(
        retry::Entity::find_by_id("delivery")
            .one(&database)
            .await
            .unwrap()
            .is_none()
    );
    insert_retry(&database).await;
    database
        .execute_unprepared("UPDATE task_delivery_attempt SET error='changed' WHERE id='attempt'")
        .await
        .unwrap();
    assert!(
        retry::Entity::find_by_id("delivery")
            .one(&database)
            .await
            .unwrap()
            .is_none()
    );
    insert_retry(&database).await;
    database
        .execute_unprepared("DELETE FROM task_delivery_attempt WHERE id='attempt'")
        .await
        .unwrap();
    assert!(
        retry::Entity::find_by_id("delivery")
            .one(&database)
            .await
            .unwrap()
            .is_none()
    );
    insert_retry(&database).await;
    database
        .execute_unprepared("DELETE FROM task_delivery WHERE id='delivery'")
        .await
        .unwrap();
    assert!(
        retry::Entity::find_by_id("delivery")
            .one(&database)
            .await
            .unwrap()
            .is_none()
    );
    database.close().await.unwrap();
}
