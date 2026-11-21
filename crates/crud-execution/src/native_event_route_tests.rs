use super::*;
use crate::time::unix_to_datetime;
use sea_orm::{EntityTrait, Set};

use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
    SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::TransactionTrait;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct CliBindingRoutes {
    reads: Mutex<Vec<SqliteReadClass>>,
    writes: Mutex<Vec<SqliteWriteEvent>>,
}

impl SqliteReadObserver for CliBindingRoutes {
    fn observe(&self, event: SqliteReadEvent) {
        if let SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}

impl SqliteWriteObserver for CliBindingRoutes {
    fn observe(&self, event: SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
    }
}

#[pioneer_postgres_test::postgres_test]
async fn native_boundary_uses_scoped_routes_and_cancelled_queue_keeps_no_capacity() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let routes = Arc::new(CliBindingRoutes::default());
    let db = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, routes.clone()),
        routes.clone(),
    );
    let maintenance_db = db.maintenance();
    let store = NativeEventRepository::new(&db);
    let maintenance = NativeEventRepository::new(&maintenance_db);
    let event = NewCliRuntimeNativeEvent {
        id: "boundary-route".to_owned(),
        runtime_id: "claude".to_owned(),
        runtime_kind: "claude".to_owned(),
        workspace_id: None,
        thread_id: None,
        turn_id: None,
        native_thread_id: None,
        native_turn_id: None,
        native_method: "claude/assistant_record_boundary".to_owned(),
        payload_redacted_json:
            serde_json::json!({"messageUuid":"01900000-0000-7000-8000-000000000001"}).to_string(),
        sequence: 1,
        created_at: chrono::Utc::now().fixed_offset(),
    };
    maintenance
        .append_cli_runtime_native_event_if_absent(event.clone())
        .await
        .unwrap();
    assert!(routes.writes.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteWriteEvent::Acquired {
            class: SqliteWriteClass::Maintenance,
            ..
        }
    )));
    maintenance
        .latest_cli_runtime_native_event(CliRuntimeNativeEventListFilter {
            native_method: Some(event.native_method.clone()),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        routes
            .reads
            .lock()
            .unwrap()
            .contains(&SqliteReadClass::Maintenance)
    );

    let blocker = db.begin().await.unwrap();
    let before = routes.writes.lock().unwrap().len();
    let queued_db = maintenance_db.clone();
    let queued = tokio::spawn(async move {
        NativeEventRepository::new(&queued_db)
            .append_cli_runtime_native_event_if_absent(NewCliRuntimeNativeEvent {
                id: "cancelled-boundary".to_owned(),
                ..event
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if routes.writes.lock().unwrap()[before..].iter().any(|event| {
                matches!(
                    event,
                    SqliteWriteEvent::Enqueued {
                        class: SqliteWriteClass::Maintenance,
                        ..
                    }
                )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    queued.abort();
    assert!(queued.await.unwrap_err().is_cancelled());
    assert!(
        routes
            .writes
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(event,
                SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Maintenance, queue, .. }
                    if queue.maintenance == 0
            ))
    );
    blocker.rollback().await.unwrap();
    maintenance
        .append_cli_runtime_native_event_if_absent(NewCliRuntimeNativeEvent {
            id: "after-cancel-boundary".to_owned(),
            runtime_id: "claude".to_owned(),
            runtime_kind: "claude".to_owned(),
            workspace_id: None,
            thread_id: None,
            turn_id: None,
            native_thread_id: None,
            native_turn_id: None,
            native_method: "claude/after_cancel_boundary".to_owned(),
            payload_redacted_json: "{}".to_owned(),
            sequence: 2,
            created_at: chrono::Utc::now().fixed_offset(),
        })
        .await
        .unwrap();
    assert!(
        maintenance
            .latest_cli_runtime_native_event(CliRuntimeNativeEventListFilter {
                native_method: Some("claude/after_cancel_boundary".to_owned()),
                limit: Some(1),
                ..Default::default()
            })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        maintenance
            .latest_cli_runtime_native_event(CliRuntimeNativeEventListFilter {
                native_method: Some("claude/assistant_record_boundary".to_owned()),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        maintenance
            .list_cli_runtime_native_events(CliRuntimeNativeEventListFilter {
                runtime_id: Some("claude".to_owned()),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .unwrap()
            .iter()
            .all(|record| record.id != "cancelled-boundary")
    );
    assert!(
        store
            .latest_cli_runtime_native_event(CliRuntimeNativeEventListFilter {
                native_method: Some("claude/assistant_record_boundary".to_owned()),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn native_event_cleanup_keeps_active_and_lifecycle_events() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_cli_native_compaction";
        let thread_id = "thread_cli_native_compaction";
        let terminal_turn_id = "turn_cli_native_terminal";
        let active_turn_id = "turn_cli_native_active";
        let active_binding_turn_id = "turn_cli_native_terminal_active_binding";
        let timestamp = 1_700_040_000;
        let created_at = unix_to_datetime(timestamp);
        let db = crate::test_fixture::fixture_event_scope(fixture, workspace_id, thread_id, terminal_turn_id).await;
        db.execute_unprepared(
            "INSERT INTO turn (id, thread_id, status, turn_kind, origin) VALUES
              ('turn_cli_native_active', 'thread_cli_native_compaction', 'in_progress', 'conversation', 'user'),
              ('turn_cli_native_terminal_active_binding', 'thread_cli_native_compaction', 'in_progress', 'conversation', 'user')",
        ).await?;
        let store = NativeEventRepository::new(&db);

        for turn_id in [terminal_turn_id, active_binding_turn_id] {
            pioneer_entity::turn::Entity::update(pioneer_entity::turn::ActiveModel {
                id: Set(turn_id.to_owned()),
                status: Set("completed".to_owned()),
                updated_at: Set(created_at),
                ..Default::default()
            })
            .exec(&db)
            .await
            .expect("turn should update to terminal status");
        }

        for (turn_id, status) in [
            (terminal_turn_id, "completed"),
            (active_turn_id, "running"),
            (active_binding_turn_id, "running"),
        ] {
            let binding = crate::cli_runtime_binding::NewCliRuntimeTurnBinding {
                    turn_id: turn_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    continuation_thread_id: thread_id.to_owned(),
                    workspace_id: workspace_id.to_owned(),
                    runtime_id: "codex".to_owned(),
                    runtime_kind: "codex".to_owned(),
                    native_thread_id: "native-thread-cli-compaction".to_owned(),
                    native_turn_id: Some(format!("native-{turn_id}")),
                    request_id: None,
                    status: status.to_owned(),
                    model: None,
                    cwd: None,
                    sandbox_json: None,
                    approval_policy: None,
                    input_mapping_json: "{}".to_owned(),
                    created_at,
                    updated_at: created_at,
            };
            let mapping = crate::input_mapping::InputMapping::prepare(&binding.input_mapping_json, false)?;
            crate::cli_runtime_binding::upsert_turn_binding(
                &db, binding, mapping, &crate::test_fixture::InlineInput,
            )
            .await
            .expect("turn binding should persist");
        }

        let mut sequence = 1_i64;
        for (id, turn_id, method) in [
            (
                "native-terminal-agent-delta",
                Some(terminal_turn_id),
                "item/agentMessage/delta",
            ),
            (
                "native-terminal-output-delta",
                Some(terminal_turn_id),
                "item/commandExecution/outputDelta",
            ),
            (
                "native-terminal-diff",
                Some(terminal_turn_id),
                "turn/diff/updated",
            ),
            (
                "native-terminal-usage",
                Some(terminal_turn_id),
                "thread/tokenUsage/updated",
            ),
            (
                "native-terminal-rate-limit",
                Some(terminal_turn_id),
                "account/rateLimits/updated",
            ),
            (
                "native-terminal-completed",
                Some(terminal_turn_id),
                "item/completed",
            ),
            (
                "native-active-agent-delta",
                Some(active_turn_id),
                "item/agentMessage/delta",
            ),
            (
                "native-active-binding-agent-delta",
                Some(active_binding_turn_id),
                "item/agentMessage/delta",
            ),
            (
                "native-unbound-rate-limit",
                None,
                "account/rateLimits/updated",
            ),
        ] {
            store
                .append_cli_runtime_native_event(NewCliRuntimeNativeEvent {
                    id: id.to_owned(),
                    runtime_id: "codex".to_owned(),
                    runtime_kind: "codex".to_owned(),
                    workspace_id: Some(workspace_id.to_owned()),
                    thread_id: Some(thread_id.to_owned()),
                    turn_id: turn_id.map(str::to_owned),
                    native_thread_id: Some("native-thread-cli-compaction".to_owned()),
                    native_turn_id: turn_id.map(|turn_id| format!("native-{turn_id}")),
                    native_method: method.to_owned(),
                    payload_redacted_json: serde_json::json!({"nativeItemId":"item"})
                        .to_string(),
                    sequence,
                    created_at,
                })
                .await
                .expect("native event should persist");
            sequence += 1;
        }

        let initial = store
            .list_cli_runtime_native_events(CliRuntimeNativeEventListFilter {
                runtime_id: Some("codex".to_owned()),
                thread_id: Some(thread_id.to_owned()),
                ..Default::default()
            })
            .await
            .expect("initial native events should list");
        assert_eq!(initial.len(), 9);
        let maintenance = db.maintenance();
        let mut deleted = 0;
        let mut finished = false;
        for _ in 0..20 {
            let metrics = crate::native_event_cleanup::run_quantum(&maintenance)
                .await
                .expect("cleanup should succeed");
            deleted += metrics.events_deleted;
            if metrics.jobs_examined == 0 {
                finished = true;
                break;
            }
        }
        assert!(finished, "cleanup should settle within the bounded fixture");
        assert_eq!(deleted, 6);

        let remaining = store
            .list_cli_runtime_native_events(CliRuntimeNativeEventListFilter {
                runtime_id: Some("codex".to_owned()),
                thread_id: Some(thread_id.to_owned()),
                ..Default::default()
            })
            .await
            .expect("remaining native events should list");
        let remaining_ids = remaining
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            remaining_ids,
            vec![
                "native-active-agent-delta",
                "native-active-binding-agent-delta",
                "native-unbound-rate-limit",
            ]
        );
        Ok(())
    })
    .await
    .unwrap();
}
