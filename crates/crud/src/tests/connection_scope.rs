use super::*;

#[tokio::test]
async fn store_preserves_typed_database_scope_across_construction_and_write_reclassification() {
    pioneer_postgres_test::with_database(async |fixture| {
        let reader = fixture.connect_read_only().await.unwrap();
        let writer = fixture.connection().clone();
        let database = pioneer_postgres_pools::PostgresDatabase::new(reader, writer)
            .maintenance()
            .with_critical_writes();

        let store = CrudStore::new(database);
        let scoped = store.database_connection();

        assert_eq!(
            scoped.read_class(),
            pioneer_postgres_pools::PostgresReadClass::Maintenance
        );
        assert_eq!(
            scoped.write_class(),
            pioneer_postgres_pools::PostgresWriteClass::Critical
        );

        let background_control_plane = store.with_maintenance_reads_and_critical_writes();
        let scoped = background_control_plane.database_connection();
        assert_eq!(
            scoped.read_class(),
            pioneer_postgres_pools::PostgresReadClass::Maintenance
        );
        assert_eq!(
            scoped.write_class(),
            pioneer_postgres_pools::PostgresWriteClass::Critical
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn pure_maintenance_read_does_not_reserve_the_writer() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let database = pioneer_postgres_pools::PostgresDatabase::new(reader, writer).maintenance();
    let store = CrudStore::new(database.clone());
    let transaction = database
        .with_critical_writes()
        .begin()
        .await
        .expect("occupy the writer");
    let read_database = store.database_connection();

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        store.run_background_database_quantum(|| {
            let read_database = read_database.clone();
            async move {
                read_database
                    .query_one_raw(Statement::from_string(
                        DatabaseBackend::Postgres,
                        "SELECT 1 AS value".to_owned(),
                    ))
                    .await?;
                Ok(())
            }
        }),
    )
    .await
    .expect("pure maintenance read must not wait for the occupied writer")
    .expect("pure maintenance read must succeed");

    transaction.rollback().await.expect("release writer");
}

#[tokio::test]
async fn ambiguous_transport_outcome_is_not_replayed() {
    pioneer_postgres_test::with_database(async |fixture| {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let store = postgres_store_with_workspace(fixture, "ws_task").await;

        let attempts = AtomicUsize::new(0);
        let result: anyhow::Result<()> =
            store
                .retry_write_on_conflict(|| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err(sea_orm::DbErr::Exec(sea_orm::RuntimeErr::SqlxError(
                            std::sync::Arc::new(sea_orm::sqlx::Error::Io(std::io::Error::new(
                                std::io::ErrorKind::ConnectionReset,
                                "unknown commit outcome",
                            ))),
                        ))
                        .into())
                    }
                })
                .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn postgres_driver_distinguishes_retryable_sqlstates() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection();
    let store = CrudStore::new(db.clone());
    for (state, retry) in [
        ("40001", true),
        ("40P01", true),
        ("55P03", false),
        ("P0001", false),
    ] {
        let sql = format!(
            "DO $$ BEGIN RAISE EXCEPTION 'classifier fixture' USING ERRCODE = '{state}'; END $$"
        );
        let error = db.execute_unprepared(&sql).await.unwrap_err().into();
        assert_eq!(
            pioneer_postgres_pools::is_retryable_database_conflict(&error),
            retry
        );
        let mut attempts = 0;
        let result = store
            .retry_write_on_conflict(|| {
                attempts += 1;
                let first = attempts == 1;
                let sql = &sql;
                async move {
                    if first {
                        db.execute_unprepared(sql).await?;
                    }
                    Ok(())
                }
            })
            .await;
        assert_eq!(result.is_ok(), retry);
        assert_eq!(attempts, if retry { 2 } else { 1 });
    }
}
