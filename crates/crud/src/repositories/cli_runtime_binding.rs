pub use pioneer_crud_execution::cli_runtime_binding::*;

#[cfg(test)]
mod tests {
    use super::*;

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
        let store = crate::CrudStore::new(db.clone());
        let maintenance = store.with_maintenance_access();
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
                serde_json::json!({"messageUuid":"01900000-0000-7000-8000-000000000001"})
                    .to_string(),
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
        let queued_store = maintenance.clone();
        let queued = tokio::spawn(async move {
            queued_store
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
}
