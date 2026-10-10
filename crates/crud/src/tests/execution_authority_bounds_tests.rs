use super::*;
use crate::MAX_EXECUTION_AUTHORITY_PAGE_ROWS;
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
    SqliteWriteExecutor,
};

#[derive(Default)]
struct Reads(std::sync::Mutex<Vec<SqliteReadClass>>);
impl SqliteReadObserver for Reads {
    fn observe(&self, event: SqliteReadEvent) {
        if let SqliteReadEvent::OperationFinished { class, .. } = event {
            self.0.lock().unwrap().push(class);
        }
    }
}

async fn query_only_store(path: &std::path::Path, reads: Arc<Reads>) -> CrudStore {
    // Connections are initialized only here. Every fixture/migration/query
    // below uses the actual scoped SqliteDatabase and serialized writer.
    let mut options = sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(path));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|options| options.pragma("journal_mode", "WAL"));
    let writer = SqliteWriteExecutor::new(Database::connect(options).await.unwrap());
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let mut options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(path));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|options| {
            options
                .read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(options).await.unwrap();
    let database = SqliteDatabase::from_executor_with_read_observer(reader, writer, reads);
    assert!(database.reader_query_only_enabled().await.unwrap());
    let store = CrudStore::new(database);
    let now = unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_authority_bounds".into()),
        name: Set("Authority bounds".into()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(&store.connection)
    .await
    .unwrap();
    store
}

#[tokio::test]
async fn turn_context_write_and_physical_read_bound_unicode_and_preserve_atomic_envelope() {
    let directory = tempfile::tempdir().unwrap();
    let reads = Arc::new(Reads::default());
    let store = query_only_store(&directory.path().join("authority.sqlite"), reads.clone()).await;
    let thread = sample_thread(
        "ws_authority_bounds",
        "thread_authority_bounds",
        1_700_000_000,
    );
    let turn = sample_turn("turn_authority_bounds");
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    store
        .set_turn_execution_authorization_context(&turn.id, "{\"original\":true}")
        .await
        .unwrap();
    let oversized = "é".repeat(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES / 2 + 1);
    assert!(
        store
            .set_turn_execution_authorization_context(&turn.id, &oversized)
            .await
            .is_err()
    );
    let security = TurnExecutionSecuritySnapshot::unrestricted_full_access("/workspace", 1);
    assert!(
        store
            .set_turn_execution_envelope(&turn.id, &security, &oversized)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get_turn_execution_authorization_context(&turn.id)
            .await
            .unwrap()
            .as_deref(),
        Some("{\"original\":true}")
    );
    assert!(
        store
            .get_turn_execution_security_snapshot(&turn.id)
            .await
            .unwrap()
            .is_none()
    );
    // Legacy/corrupt storage deliberately bypasses the guarded setter through
    // the same serialized writer. Reader CASE must never materialize its body.
    store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE \"turn\" SET execution_authorization_context_json=? WHERE id=?",
            [oversized.into(), turn.id.clone().into()],
        ))
        .await
        .unwrap();
    reads.0.lock().unwrap().clear();
    assert!(
        store
            .with_maintenance_access()
            .get_turn_execution_authorization_context(&turn.id)
            .await
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );
    let records = store
        .with_maintenance_access()
        .list_active_execution_authorities(None, u64::MAX)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].turn_id, turn.id);
    assert!(records[0].authority_envelope_json.is_none());
    assert!(
        reads
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|class| *class == SqliteReadClass::Maintenance)
    );
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
}

#[tokio::test]
async fn task_authority_bounds_preserve_immutable_admission_and_bounded_cursor_progress() {
    let directory = tempfile::tempdir().unwrap();
    let reads = Arc::new(Reads::default());
    let store = query_only_store(
        &directory.path().join("task-authority.sqlite"),
        reads.clone(),
    )
    .await;
    let timestamp = 1_700_000_000;
    let thread = sample_thread("ws_authority_bounds", "thread_task_bounds", timestamp);
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &sample_turn("turn_task_bounds"),
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let mut first_admission = None;
    for index in 0..MAX_EXECUTION_AUTHORITY_PAGE_ROWS + 2 {
        let mut task = sample_task(timestamp);
        task.id = format!("task_bounds_{index:03}");
        task.workspace_id = thread.workspace_id.clone();
        task.owner_id = Some(thread.id.clone());
        task.created_by_thread_id = Some(thread.id.clone());
        task.created_by_turn_id = Some("turn_task_bounds".into());
        let admission = NewTaskExecutionAdmission {
            task_id: task.id.clone(),
            workspace_id: thread.workspace_id.clone(),
            root_thread_id: thread.id.clone(),
            initiating_principal_id: "P".repeat(21),
            authorization_context_json: "{\"original\":true}".into(),
            created_at: unix_to_datetime(timestamp),
            execution_lease: None,
        };
        store
            .append_task_events_with_execution_admission(
                vec![TaskEventPayload::TaskCreated { task }],
                timestamp,
                admission.clone(),
            )
            .await
            .unwrap();
        if index == 0 {
            first_admission = Some(admission);
        }
    }
    let original = first_admission.unwrap();
    let mut oversized = original.clone();
    oversized.authorization_context_json =
        "é".repeat(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES / 2 + 1);
    assert!(
        store
            .append_task_events_with_execution_readmission(Vec::new(), timestamp, oversized.clone())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get_task_execution_admission(&original.task_id)
            .await
            .unwrap()
            .unwrap()
            .authorization_context_json,
        original.authorization_context_json
    );
    store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE task_execution_admission SET authorization_context_json=? WHERE task_id=?",
            [
                oversized.authorization_context_json.into(),
                original.task_id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    reads.0.lock().unwrap().clear();
    let maintenance = store.with_maintenance_access();
    assert!(
        maintenance
            .get_task_execution_admission(&original.task_id)
            .await
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );
    let first = maintenance
        .list_active_task_execution_authorities(None, u64::MAX)
        .await
        .unwrap();
    assert_eq!(first.len(), MAX_EXECUTION_AUTHORITY_PAGE_ROWS as usize);
    assert_eq!(first[0].task_id, original.task_id);
    assert!(first[0].authority_envelope_json.is_none());
    let second = maintenance
        .list_active_task_execution_authorities(Some(&first.last().unwrap().task_id), u64::MAX)
        .await
        .unwrap();
    assert_eq!(second.len(), 2);
    assert!(
        second
            .iter()
            .all(|record| record.authority_envelope_json.as_deref() == Some("{\"original\":true}"))
    );
    let observed = reads.0.lock().unwrap().clone();
    assert!(observed.len() >= 3);
    assert!(
        observed
            .iter()
            .all(|class| *class == SqliteReadClass::Maintenance)
    );
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
}
