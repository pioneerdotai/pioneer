use super::*;
use crate::{find_turn_initiator, resolve_turn_authorization_scope};
use pioneer_sqlite::SqliteDatabase;

// The storage API returns opaque nonblank envelopes. Validation belongs to the
// runtime loader; neither NULL nor blank storage may acquire fabricated JSON.
#[tokio::test]
async fn authorization_scalar_and_binding_reads_preserve_absence_and_reject_corrupt_facts() {
    let store = test_store_with_workspace("auth-reads").await;
    assert!(
        store
            .get_turn_execution_authorization_context("missing")
            .await
            .unwrap()
            .is_none()
    );
    let (store, _, _) = start_test_turn(store, "auth-reads", "thread", "turn").await;
    assert!(
        store
            .get_turn_execution_authorization_context("turn")
            .await
            .unwrap()
            .is_none()
    );
    for encoded in ["", " \t\n", " {\"opaque\":true} ", "malformed"] {
        // Seed persisted bytes directly: the public setter rejects blanks and
        // trims nonblank input, while the getter must handle legacy/raw rows.
        pioneer_entity::turn::Entity::update_many()
            .col_expr(
                pioneer_entity::turn::Column::ExecutionAuthorizationContextJson,
                Expr::value(encoded),
            )
            .filter(pioneer_entity::turn::Column::Id.eq("turn"))
            .exec(&store.connection)
            .await
            .unwrap();
        assert_eq!(
            store
                .get_turn_execution_authorization_context("turn")
                .await
                .unwrap(),
            if encoded.trim().is_empty() {
                None
            } else {
                Some(encoded.to_owned())
            }
        );
    }
    assert!(
        store
            .get_turn_execution_binding("wrong-thread", "turn")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_turn_execution_status("thread", "missing")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_turn_execution_status("wrong-thread", "turn")
            .await
            .unwrap()
            .is_none()
    );

    // These unrelated snapshots must not be decoded by the authorization path.
    store.connection.execute_unprepared(
        "UPDATE turn SET prompt_manifest_json='broken', permission_profile_snapshot_json='broken', \
         mentions_json='broken' WHERE id='turn'"
    ).await.unwrap();
    let binding = store
        .get_turn_execution_binding("thread", "turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.id, "turn");
    assert_eq!(binding.turn_kind, TurnKind::Conversation);
    assert_eq!(binding.origin, pioneer_protocol::TurnOrigin::User);
    assert_eq!(
        store
            .get_turn_execution_status("thread", "turn")
            .await
            .unwrap(),
        Some(TurnStatus::InProgress)
    );
    for (column, valid) in [
        ("status", "in_progress"),
        ("turn_kind", "conversation"),
        ("origin", "user"),
    ] {
        store
            .connection
            .execute_unprepared(&format!(
                "UPDATE turn SET {column}='unknown' WHERE id='turn'"
            ))
            .await
            .unwrap();
        assert!(
            store
                .get_turn_execution_binding("thread", "turn")
                .await
                .is_err(),
            "{column} must fail closed"
        );
        if column == "status" {
            assert!(
                store
                    .get_turn_execution_status("thread", "turn")
                    .await
                    .is_err()
            );
        }
        store
            .connection
            .execute_unprepared(&format!(
                "UPDATE turn SET {column}='{valid}' WHERE id='turn'"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn authorization_projections_use_only_required_columns_on_scoped_physical_readers() {
    use pioneer_sqlite::{
        SqliteReadClass, SqliteReadEvent, SqliteReadOutcome, SqliteWriteExecutor,
        sqlite_connection_url, sqlite_read_only_connection_url,
    };
    use sea_orm::ConnectOptions;
    let path = OptionalDeliveryDatabasePath(
        std::env::temp_dir().join(format!("pioneer-authorization-{}.sqlite", generate_id(12))),
    );
    let mut options = ConnectOptions::new(sqlite_connection_url(&path.0));
    options.max_connections(1);
    let mut writer = Database::connect(options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    writer
        .execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','Authorization',1,1)",
        )
        .await
        .unwrap();
    let seed = CrudStore::new(writer.clone());
    let (seed, _, _) = start_test_turn(seed, "ws", "thread", "turn").await;
    seed.set_turn_execution_authorization_context("turn", "opaque envelope")
        .await
        .unwrap();
    drop(seed);
    let reader_statements = Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
    let writer_statements = Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
    writer.set_metric_callback({
        let statements = writer_statements.clone();
        move |info| statements.lock().unwrap().push(info.statement.clone())
    });
    let mut options = ConnectOptions::new(sqlite_read_only_connection_url(&path.0));
    options.max_connections(2);
    options.map_sqlx_sqlite_opts(|options| {
        options
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let mut reader = Database::connect(options).await.unwrap();
    reader.set_metric_callback({
        let statements = reader_statements.clone();
        move |info| statements.lock().unwrap().push(info.statement.clone())
    });
    let reads = Arc::new(ThreadSnapshotReadObserver::default());
    let writes = Arc::new(OptionalDeliveryDatabaseObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, writes.clone()),
        reads.clone(),
    );
    database.validate_reader().await.unwrap();
    let interactive = CrudStore::new(database.clone());
    let maintenance = interactive.with_maintenance_access();
    let critical = interactive.with_maintenance_reads_and_critical_writes();
    let held_writer = database.begin().await.unwrap();
    for (store, class) in [
        (&interactive, SqliteReadClass::Interactive),
        (&maintenance, SqliteReadClass::Maintenance),
        (&critical, SqliteReadClass::Maintenance),
    ] {
        reader_statements.lock().unwrap().clear();
        writer_statements.lock().unwrap().clear();
        reads.events.lock().unwrap().clear();
        writes.events.lock().unwrap().clear();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            assert_eq!(
                store
                    .get_turn_execution_authorization_context("turn")
                    .await
                    .unwrap()
                    .as_deref(),
                Some("opaque envelope")
            );
            assert_eq!(
                store
                    .get_turn_execution_binding("thread", "turn")
                    .await
                    .unwrap()
                    .unwrap()
                    .turn_kind,
                TurnKind::Conversation
            );
            assert_eq!(
                store
                    .get_turn_execution_status("thread", "turn")
                    .await
                    .unwrap(),
                Some(TurnStatus::InProgress)
            );
            assert_eq!(
                find_turn_initiator(&store.database_connection(), "turn")
                    .await
                    .unwrap(),
                Some(PersistedActorRef::System)
            );
            let scope = resolve_turn_authorization_scope(
                &store.database_connection(),
                "turn",
                Some("ws"),
                Some("thread"),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(scope.turn_id, "turn");
            assert!(
                resolve_turn_authorization_scope(
                    &store.database_connection(),
                    "turn",
                    Some("other-ws"),
                    None
                )
                .await
                .unwrap()
                .is_none()
            );
            assert!(
                resolve_turn_authorization_scope(
                    &store.database_connection(),
                    "turn",
                    None,
                    Some("other-thread")
                )
                .await
                .unwrap()
                .is_none()
            );
        })
        .await
        .expect("authorization reads must finish while writer is occupied");
        assert!(writer_statements.lock().unwrap().is_empty());
        assert!(
            writes.events.lock().unwrap().is_empty(),
            "no writer reservation"
        );
        let statements = reader_statements.lock().unwrap();
        let turn_selects = statements
            .iter()
            .filter(|s| s.sql.contains("FROM \"turn\""))
            .collect::<Vec<_>>();
        // Contract: precisely the authority fields, never a full row or payload.
        // Count and column assertions detect a second full lookup as well.
        let columns = [
            "\"turn\".\"execution_authorization_context_json\"",
            "\"turn\".\"id\", \"turn\".\"status\", \"turn\".\"turn_kind\", \"turn\".\"origin\"",
            "\"turn\".\"status\"",
            "\"turn\".\"initiated_by_actor_kind\", \"turn\".\"initiated_by_actor_id\"",
            "\"turn\".\"id\", \"turn\".\"thread_id\"",
            "\"turn\".\"id\", \"turn\".\"thread_id\"",
            "\"turn\".\"id\", \"turn\".\"thread_id\"",
        ];
        assert_eq!(turn_selects.len(), columns.len());
        for (statement, columns) in turn_selects.iter().zip(columns) {
            assert_eq!(
                statement.sql.split(" FROM ").next().unwrap(),
                format!("SELECT {columns}")
            );
        }
        let events = reads.events.lock().unwrap();
        let finished = events
            .iter()
            .filter_map(|event| match event {
                SqliteReadEvent::OperationFinished { class, outcome, .. } => {
                    Some((*class, *outcome))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            finished,
            vec![(class, SqliteReadOutcome::Ok); statements.len()]
        );
    }
    held_writer.rollback().await.unwrap();
    database.close().await.unwrap();
}
