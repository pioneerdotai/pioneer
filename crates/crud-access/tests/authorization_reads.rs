use anyhow::{Context, Result};
use pioneer_crud_access::{
    authorization_scope::resolve_turn_authorization_scope, turn_execution::*,
};
use pioneer_protocol::{PersistedActorRef, TurnKind, TurnStatus, TurnWorkOwner};
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteReadOutcome,
    SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement, TransactionTrait};
use std::sync::{Arc, Mutex};

async fn seed(database: &impl ConnectionTrait) -> Result<()> {
    database
        .execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws', 'Authorization');
         INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', 'ws', '', 'agent', 'model', 'provider', 'active');
         INSERT INTO turn (id, thread_id, status, turn_kind, origin, initiated_by_actor_kind)
             VALUES ('turn', 'thread', 'in_progress', 'conversation', 'user', 'system')",
        )
        .await?;
    Ok(())
}

#[derive(Default)]
struct ReadEvents {
    events: Mutex<Vec<SqliteReadEvent>>,
}

impl SqliteReadObserver for ReadEvents {
    fn observe(&self, event: SqliteReadEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[derive(Default)]
struct WriteEvents {
    events: Mutex<Vec<SqliteWriteEvent>>,
}

impl SqliteWriteObserver for WriteEvents {
    fn observe(&self, event: SqliteWriteEvent) {
        self.events.lock().unwrap().push(event);
    }
}

// The storage API returns opaque nonblank envelopes. Validation belongs to the
// runtime loader; neither NULL nor blank storage may acquire fabricated JSON.
#[tokio::test]
async fn authorization_scalar_and_binding_reads_preserve_absence_and_reject_corrupt_facts()
-> Result<()> {
    pioneer_postgres_test::with_database(async |fixture| {
        let database = fixture.connection();
        assert!(
            find_turn_execution_authorization_context(database, "missing")
                .await
                .unwrap()
                .is_none()
        );
        seed(database).await?;
        assert!(
            find_turn_execution_authorization_context(database, "turn")
                .await
                .unwrap()
                .is_none()
        );
        for encoded in [r#" {"opaque":true} "#, r#""malformed""#, "null"] {
            database.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE turn SET execution_authorization_context_json = $1::text::json WHERE id = 'turn'",
                [encoded.into()],
            )).await?;
            assert_eq!(
                find_turn_execution_authorization_context(database, "turn").await?.as_deref(),
                Some(encoded)
            );
        }
        assert!(database.execute_unprepared(
            "UPDATE turn SET execution_authorization_context_json = 'malformed' WHERE id = 'turn'"
        ).await.is_err());
        assert_eq!(
            find_turn_execution_authorization_context(database, "turn").await?.as_deref(),
            Some("null")
        );
        assert!(
            find_turn_execution_binding(database, "wrong-thread", "turn")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            find_turn_execution_status(database, "thread", "missing")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            find_turn_execution_status(database, "wrong-thread", "turn")
                .await
                .unwrap()
                .is_none()
        );

        // These unrelated snapshots must not be decoded by the authorization path.
        database.execute_unprepared(
            "UPDATE turn SET prompt_manifest_json='[]', permission_profile_snapshot_json='true', \
             mentions_json='null' WHERE id='turn'"
        ).await.unwrap();
        let binding = find_turn_execution_binding(database, "thread", "turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(binding.id, "turn");
        assert_eq!(binding.turn_kind, TurnKind::Conversation);
        assert_eq!(binding.origin, pioneer_protocol::TurnOrigin::User);
        assert_eq!(binding.work_owner, TurnWorkOwner::Turn);
        database
            .execute_unprepared("UPDATE turn SET work_owner='detached_task' WHERE id='turn'")
            .await
            .unwrap();
        assert_eq!(
            find_turn_execution_binding(database, "thread", "turn")
                .await
                .unwrap()
                .unwrap()
                .work_owner,
            TurnWorkOwner::DetachedTask
        );
        assert_eq!(
            find_turn_execution_status(database, "thread", "turn")
                .await
                .unwrap(),
            Some(TurnStatus::InProgress)
        );
        for (column, valid) in [
            ("status", "in_progress"),
            ("turn_kind", "conversation"),
            ("origin", "user"),
            ("work_owner", "detached_task"),
        ] {
            // Seed a corrupt tag in this private clone, then restore enforcement
            // for future writes before exercising the published reader.
            if column == "work_owner" {
                let tx = database.begin().await?;
                tx.execute_unprepared("ALTER TABLE turn DROP CONSTRAINT turn_work_owner_check").await?;
                tx.execute_unprepared("UPDATE turn SET work_owner='unknown' WHERE id='turn'").await?;
                tx.execute_unprepared(
                    "ALTER TABLE turn ADD CONSTRAINT turn_work_owner_check
                     CHECK (work_owner IN ('turn', 'detached_task')) NOT VALID",
                ).await?;
                tx.commit().await?;
            } else {
                database.execute_unprepared(&format!(
                    "UPDATE turn SET {column}='unknown' WHERE id='turn'"
                )).await?;
            }
            assert!(
                find_turn_execution_binding(database, "thread", "turn")
                    .await
                    .is_err(),
                "{column} must fail closed"
            );
            if column == "status" {
                assert!(
                    find_turn_execution_status(database, "thread", "turn")
                        .await
                        .is_err()
                );
            }
            database
                .execute_unprepared(&format!(
                    "UPDATE turn SET {column}='{valid}' WHERE id='turn'"
                ))
                .await
                .unwrap();
        }
        Ok(())
    }).await
}

#[tokio::test]
async fn authorization_projections_use_only_required_columns_on_scoped_physical_readers()
-> Result<()> {
    pioneer_postgres_test::with_database(async |fixture| {
        let mut writer = fixture.connection().clone();
        seed(&writer).await?;
        writer.execute_unprepared(
            r#"UPDATE turn SET execution_authorization_context_json='"opaque envelope"' WHERE id='turn'"#,
        ).await?;
        let reader_statements = Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
        let writer_statements = Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
        writer.set_metric_callback({
            let statements = writer_statements.clone();
            move |info| statements.lock().unwrap().push(info.statement.clone())
        });
        let mut options = ConnectOptions::new(fixture.url().to_string());
        options.max_connections(2).sqlx_logging(false).map_sqlx_postgres_opts(|options| {
            options.options([("default_transaction_read_only", "on")])
        });
        let mut reader = Database::connect(options).await.unwrap();
        reader.set_metric_callback({
            let statements = reader_statements.clone();
            move |info| statements.lock().unwrap().push(info.statement.clone())
        });
        let reads = Arc::new(ReadEvents::default());
        let writes = Arc::new(WriteEvents::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, writes.clone()),
            reads.clone(),
        );
        database.validate_reader().await.unwrap();
        let interactive = database.clone();
        let maintenance = database.maintenance();
        let critical = database.maintenance().with_critical_writes();
        let held_writer = database.begin().await?;
        held_writer.execute_unprepared("SELECT id FROM turn WHERE id='turn' FOR UPDATE").await?;
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
                    find_turn_execution_authorization_context(store, "turn")
                        .await
                        .unwrap()
                        .as_deref(),
                    Some(r#""opaque envelope""#)
                );
                assert_eq!(
                    find_turn_execution_binding(store, "thread", "turn")
                        .await
                        .unwrap()
                        .unwrap()
                        .turn_kind,
                    TurnKind::Conversation
                );
                assert_eq!(
                    find_turn_execution_status(store, "thread", "turn")
                        .await
                        .unwrap(),
                    Some(TurnStatus::InProgress)
                );
                assert_eq!(
                    find_turn_initiator(store, "turn")
                        .await
                        .unwrap(),
                    Some(PersistedActorRef::System)
                );
                let scope = resolve_turn_authorization_scope(
                    store,
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
                        store,
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
                        store,
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
                r#"CAST("turn"."execution_authorization_context_json" AS text) AS "authorization_context_json""#,
                "\"turn\".\"id\", \"turn\".\"status\", \"turn\".\"turn_kind\", \"turn\".\"origin\", \"turn\".\"work_owner\"",
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
        Ok(())
    }).await
}
