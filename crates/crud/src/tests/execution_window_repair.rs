use super::*;
use pioneer_entity::turn as turn_entity;
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteWriteClass, SqliteWriteEvent, SqliteWriteExecutor,
    sqlite_read_only_connection_url,
};
use sea_orm::ConnectOptions;

#[tokio::test]
async fn terminal_window_repair_handles_history_beyond_sqlite_parameter_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("gateway.db");
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(1);
    options.map_sqlx_sqlite_opts(|options| options.foreign_keys(false));
    let writer = Database::connect(options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(sqlite_read_only_connection_url(&path));
    options.max_connections(2);
    options.map_sqlx_sqlite_opts(|options| {
        options
            .foreign_keys(false)
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(OptionalDeliveryDatabaseObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    database.validate_reader().await.unwrap();
    let store = CrudStore::new(database.clone());
    let now = unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_repair".into()),
        name: Set("Window repair".into()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(&store.connection)
    .await
    .unwrap();
    let (store, _, _) = start_test_turn(store, "ws_repair", "thread_repair", "active").await;
    let terminal_turn = |id: String, status: &str| turn_entity::ActiveModel {
        id: Set(id),
        thread_id: Set("thread_repair".into()),
        status: Set(status.into()),
        error: Set(Some("terminal reason".into())),
        prompt_manifest_json: Set("{}".into()),
        turn_kind: Set("conversation".into()),
        origin: Set("user".into()),
        created_at: Set(now),
        updated_at: Set(now),
        ..Default::default()
    };
    // More terminal turns than SQLite's 32,766 bind parameters, even when
    // almost all history has no active execution window to repair.
    for batch in 0..256 {
        turn_entity::Entity::insert_many(
            (0..128).map(|index| {
                terminal_turn(format!("history_{}", batch * 128 + index), "completed")
            }),
        )
        .exec(&store.connection)
        .await
        .unwrap();
    }
    let mut expected_windows = Vec::new();
    for (turn_status, expected_status) in [
        ("completed", ExecutionWindowStatus::Completed),
        ("failed", ExecutionWindowStatus::Failed),
        ("interrupted", ExecutionWindowStatus::Interrupted),
        ("blocked", ExecutionWindowStatus::Blocked),
        ("active", ExecutionWindowStatus::Running),
    ] {
        if turn_status != "active" {
            turn_entity::Entity::insert(terminal_turn(turn_status.into(), turn_status))
                .exec(&store.connection)
                .await
                .unwrap();
        }
        for (index, initial_status) in [
            ExecutionWindowStatus::Running,
            ExecutionWindowStatus::Checkpointed,
            ExecutionWindowStatus::Completed,
        ]
        .into_iter()
        .enumerate()
        {
            let window = store
                .create_turn_execution_window(
                    NewTurnExecutionWindowRecord {
                        workspace_id: "ws_repair".into(),
                        thread_id: "thread_repair".into(),
                        turn_id: turn_status.into(),
                        window_index: index as u32 + 1,
                        status: initial_status,
                        exhaustion_reason: None,
                        agent_round_count: 2,
                        tool_call_count: 3,
                        provider_token_count: 5,
                        metadata_json: serde_json::json!({"source": "retained"}),
                        started_at: now,
                    },
                    now,
                    now,
                )
                .await
                .unwrap();
            let should_repair =
                turn_status != "active" && initial_status != ExecutionWindowStatus::Completed;
            let status = if should_repair {
                expected_status
            } else {
                initial_status
            };
            expected_windows.push((window, status, should_repair));
        }
    }
    observer.reads.lock().unwrap().clear();
    observer.events.lock().unwrap().clear();
    let maintenance = store.with_maintenance_access();
    let repaired_at = now + chrono::Duration::seconds(10);
    assert_eq!(
        maintenance
            .close_active_execution_windows_for_terminal_turns(repaired_at)
            .await
            .unwrap(),
        8
    );
    let reads = std::mem::take(&mut *observer.reads.lock().unwrap());
    assert!(!reads.is_empty());
    assert!(
        reads
            .iter()
            .all(|class| *class == SqliteReadClass::Maintenance)
    );
    let writes = std::mem::take(&mut *observer.events.lock().unwrap())
        .into_iter()
        .filter_map(|event| match event {
            SqliteWriteEvent::Acquired { class, .. } => Some(class),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(writes, vec![SqliteWriteClass::Maintenance; 8]);
    let reopened = CrudStore::new(database).with_maintenance_access();
    assert_eq!(
        reopened
            .close_active_execution_windows_for_terminal_turns(
                repaired_at + chrono::Duration::seconds(10)
            )
            .await
            .unwrap(),
        0
    );
    for (mut expected, status, repaired) in expected_windows {
        if repaired {
            expected.status = status;
            expected.completed_at = Some(repaired_at);
            expected.updated_at = repaired_at;
            expected.metadata_json = serde_json::json!({
                "source": "retained",
                "repairedBy": "bootstrap_terminal_turn_window_repair",
                "terminalStatus": match status {
                    ExecutionWindowStatus::Completed => "completed",
                    ExecutionWindowStatus::Failed => "failed",
                    ExecutionWindowStatus::Interrupted => "interrupted",
                    ExecutionWindowStatus::Blocked => "blocked",
                    _ => unreachable!(),
                },
                "terminalReason": "terminal reason",
            });
        }
        assert_eq!(
            reopened
                .get_turn_execution_window(&expected.id)
                .await
                .unwrap(),
            Some(expected)
        );
    }
}
