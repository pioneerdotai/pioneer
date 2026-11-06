//! Schema contract only: the legacy row is intentionally seeded at the SQL
//! boundary, before the new application route kind exists. Never run here.
use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

const NAME: &str = "m20261104_000001_capsule_bound_routes";

#[tokio::test]
async fn upgrade_preserves_route_rows_objects_and_refuses_lossy_down() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    let migrations = Migrator::migrations();
    let index = migrations.iter().position(|m| m.name() == NAME).unwrap();
    Migrator::up(&db, Some(index as u32)).await.unwrap();
    for sql in [
        "INSERT INTO agent_delegation_route(id,source_execution_id,destination_thread_id,route_kind,grant_fingerprint,status) VALUES('legacy','creator','destination','execution_bound','fingerprint','active')",
        "CREATE INDEX capsule_fixture_index ON agent_delegation_route(grant_fingerprint)",
        "CREATE TABLE capsule_fixture_audit(route_id TEXT)",
        "CREATE TRIGGER capsule_fixture_trigger AFTER UPDATE ON agent_delegation_route BEGIN INSERT INTO capsule_fixture_audit VALUES(new.id); END",
        "INSERT INTO agent_delegation_route_event(id,route_id,event_kind,route_generation,authority_actor_json,authority_fingerprint,occurred_at) VALUES('event','legacy','created',1,'{}','fingerprint',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    Migrator::up(&db, Some(1)).await.unwrap();
    for sql in [
        "SELECT 1 FROM agent_delegation_route WHERE id='legacy' AND route_kind='execution_bound'",
        "SELECT 1 FROM agent_delegation_route_event WHERE id='event' AND route_id='legacy'",
        "SELECT 1 FROM sqlite_master WHERE name='capsule_fixture_index' AND type='index'",
        "SELECT 1 FROM sqlite_master WHERE name='capsule_fixture_trigger' AND type='trigger'",
        "SELECT 1 FROM sqlite_master WHERE name='idx_agent_route_capsule_active' AND type='index'",
    ] {
        assert!(
            db.query_one_raw(Statement::from_string(DbBackend::Sqlite, sql.to_owned()))
                .await
                .unwrap()
                .is_some()
        );
    }
    db.execute_unprepared(
        "UPDATE agent_delegation_route SET route_kind='capsule_bound' WHERE id='legacy'",
    )
    .await
    .unwrap();
    assert!(
        db.query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT 1 FROM capsule_fixture_audit WHERE route_id='legacy'".to_owned()
        ))
        .await
        .unwrap()
        .is_some()
    );
    assert!(Migrator::down(&db, Some(1)).await.is_err());
    assert!(
        db.query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT 1 FROM agent_delegation_route WHERE id='legacy' AND route_kind='capsule_bound'"
                .to_owned()
        ))
        .await
        .unwrap()
        .is_some()
    );
    db.execute_unprepared(
        "UPDATE agent_delegation_route SET route_kind='execution_bound' WHERE id='legacy'",
    )
    .await
    .unwrap();
    Migrator::down(&db, Some(1)).await.unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE agent_delegation_route SET route_kind='capsule_bound' WHERE id='legacy'"
        )
        .await
        .is_err()
    );
    Migrator::up(&db, Some(1)).await.unwrap();
    db.execute_unprepared(
        "UPDATE agent_delegation_route SET route_kind='capsule_bound' WHERE id='legacy'",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn fresh_install_accepts_capsule_kind_and_rejects_unknown_kind() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO agent_delegation_route(id,source_execution_id,destination_thread_id,route_kind,grant_fingerprint) VALUES('new','creator','destination','capsule_bound','fingerprint')").await.unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE agent_delegation_route SET route_kind='unknown' WHERE id='new'"
        )
        .await
        .is_err()
    );
}

// Use a file and a single-connection pool so a discarded lease can be replaced
// without destroying the database. The TEMP marker identifies physical reuse.
async fn pooled_upgrade_fixture() -> (tempfile::TempDir, sea_orm::DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let mut options = sea_orm::ConnectOptions::new(format!(
        "sqlite:{}?mode=rwc",
        directory.path().join("routes.db").display()
    ));
    options.max_connections(1).min_connections(0);
    let db = Database::connect(options).await.unwrap();
    let index = Migrator::migrations()
        .iter()
        .position(|m| m.name() == NAME)
        .unwrap();
    Migrator::up(&db, Some(index as u32)).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO agent_delegation_route(id,source_execution_id,destination_thread_id,route_kind,grant_fingerprint) VALUES('legacy','creator','destination','execution_bound','fingerprint')").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=ON")
        .await
        .unwrap();
    db.execute_unprepared("CREATE TEMP TABLE physical_lease_marker(value INTEGER)")
        .await
        .unwrap();
    (directory, db)
}

async fn assert_clean_after_interruption(db: &sea_orm::DatabaseConnection, expected_marker: i64) {
    // Pin observation too: all assertions refer to the same replacement lease.
    use sea_orm::sqlx;
    let mut connection = db.get_sqlite_connection_pool().acquire().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&mut *connection)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_temp_master WHERE name='physical_lease_marker'"
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap(),
        expected_marker,
        "only a verified restored physical connection may be reused"
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT route_kind,grant_fingerprint FROM agent_delegation_route WHERE id='legacy'"
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap(),
        ("execution_bound".to_owned(), "fingerprint".to_owned())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_master WHERE name='agent_delegation_route_capsule_upgrade'"
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap(),
        0,
        "partial rebuild must roll back"
    );
}

#[tokio::test]
async fn fk_on_error_after_setting_change_restores_physical_connection() {
    let (_directory, db) = pooled_upgrade_fixture().await;
    // A global index-name collision fails after the table/rows have been
    // rebuilt. Unlike a commit hook, this leaves a normally rollbackable txn.
    db.execute_unprepared("CREATE TABLE conflicting_index_owner(value INTEGER)")
        .await
        .unwrap();
    db.execute_unprepared(
        "CREATE INDEX idx_agent_route_capsule_active ON conflicting_index_owner(value)",
    )
    .await
    .unwrap();
    let error = Migrator::migrations()
        .pop()
        .unwrap()
        .up(&migration::SchemaManager::new(&db))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already exists"));
    assert_clean_after_interruption(&db, 1).await;
}

// Inject through the existing SQLite hook on the exact next migration lease.
async fn reject_commits(connection: &mut sea_orm::sqlx::SqliteConnection) {
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(|| false);
}

#[tokio::test]
async fn commit_failure_with_unsettled_transaction_discards_connection() {
    use sea_orm::sqlx;
    for original_fk in [0_i64, 1] {
        let (_directory, db) = pooled_upgrade_fixture().await;
        let mut connection = db.get_sqlite_connection_pool().acquire().await.unwrap();
        if original_fk == 0 {
            sqlx::raw_sql("PRAGMA foreign_keys=OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(&mut *connection)
                .await
                .unwrap(),
            original_fk
        );
        // SQLite rolls back a rejected commit; SQLx retains tracked depth when
        // its ensuing ROLLBACK reports no active transaction, for ON and OFF.
        reject_commits(&mut connection).await;
        drop(connection);
        let migration = Migrator::migrations().pop().unwrap();
        let error = migration
            .up(&migration::SchemaManager::new(&db))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("migration connection state was not restored")
        );
        // Replacement connections use this pool's default FK=ON, even when
        // the discarded lease entered with FK=OFF. TEMP marker loss proves
        // physical replacement; the shared assertions also check row rollback.
        assert_clean_after_interruption(&db, 0).await;
        migration
            .up(&migration::SchemaManager::new(&db))
            .await
            .unwrap();
        assert_clean_after_interruption(&db, 0).await;
    }
}

#[tokio::test]
async fn preexisting_unsettled_transaction_is_rejected_and_discarded() {
    use sea_orm::sqlx::{self, Connection};
    let (_directory, db) = pooled_upgrade_fixture().await;
    let mut connection = db.get_sqlite_connection_pool().acquire().await.unwrap();
    reject_commits(&mut connection).await;
    let mut txn = connection.begin().await.unwrap();
    sqlx::raw_sql(
        "UPDATE agent_delegation_route SET grant_fingerprint='uncommitted' WHERE id='legacy'",
    )
    .execute(&mut *txn)
    .await
    .unwrap();
    assert!(txn.commit().await.is_err());
    // Flush the queued rollback before inspecting SQLx's tracked state.
    sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert!(connection.is_in_transaction());
    drop(connection);
    let migration = Migrator::migrations().pop().unwrap();
    let error = migration
        .up(&migration::SchemaManager::new(&db))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot enter an active transaction")
    );
    assert_clean_after_interruption(&db, 0).await;
    migration
        .up(&migration::SchemaManager::new(&db))
        .await
        .unwrap();
    assert_clean_after_interruption(&db, 0).await;
}

#[tokio::test]
async fn fk_on_cancellation_during_rebuild_discards_physical_connection() {
    let (_directory, db) = pooled_upgrade_fixture().await;
    let (reached, wait) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let mut reached = Some(reached);
    let mut connection = db.get_sqlite_connection_pool().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |event| {
            if event.table == "agent_delegation_route_capsule_upgrade" {
                if let Some(reached) = reached.take() {
                    let _ = reached.send(());
                    // Bounded safety net; cancellation is coordinated by the row
                    // copy callback, not a sleep or an assumed PRAGMA ordering.
                    let _ = resume.recv_timeout(std::time::Duration::from_secs(10));
                }
            }
        });
    drop(connection);
    let migration_db = db.clone();
    let work = tokio::spawn(async move {
        Migrator::migrations()
            .pop()
            .unwrap()
            .up(&migration::SchemaManager::new(&migration_db))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), wait)
        .await
        .unwrap()
        .unwrap();
    work.abort();
    // Await Drop of the migration future while the SQLite worker is still
    // parked. Its transaction and dirty physical lease must both be dropped.
    assert!(work.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    assert_clean_after_interruption(&db, 0).await;
}

#[tokio::test]
async fn fk_settings_survive_success_and_ambient_transaction_is_rejected_without_changes() {
    use sea_orm::TransactionTrait;
    let (_directory, db) = pooled_upgrade_fixture().await;
    let migrations = Migrator::migrations();
    let migration = migrations.last().unwrap();
    let txn = db.begin().await.unwrap();
    let error = migration
        .up(&migration::SchemaManager::new(&txn))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("standalone SQLite pool"));
    let row = txn
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA foreign_keys".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "foreign_keys").unwrap(), 1);
    txn.rollback().await.unwrap();
    migration
        .up(&migration::SchemaManager::new(&db))
        .await
        .unwrap();
    // Successful ON entry restores the SAME physical connection, not a pool
    // default observed on an arbitrary second connection.
    let mut connection = db.get_sqlite_connection_pool().acquire().await.unwrap();
    use sea_orm::sqlx;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&mut *connection)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_temp_master WHERE name='physical_lease_marker'"
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap(),
        1
    );
    drop(connection);
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    migration
        .down(&migration::SchemaManager::new(&db))
        .await
        .unwrap();
    let mut connection = db.get_sqlite_connection_pool().acquire().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&mut *connection)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_temp_master WHERE name='physical_lease_marker'"
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap(),
        1
    );
}
