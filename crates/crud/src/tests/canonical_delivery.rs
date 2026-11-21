use super::*;

#[tokio::test]
async fn native_event_commit_atomically_enqueues_idempotent_optional_deliveries() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_native_outbox",
            "thr_native_outbox",
            "turn_native_outbox",
        )
        .await;
        let event = CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
            workspace_id: "ws_native_outbox".to_owned(),
            thread_id: "thr_native_outbox".to_owned(),
            turn_id: "turn_native_outbox".to_owned(),
            item: TurnItem::SystemEvent {
                id: "native_outbox_item".to_owned(),
                level: SystemEventLevel::Info,
                message: "committed".to_owned(),
                code: Some("native.outbox.test".to_owned()),
                details: None,
            },
        });

        store
            .materialize_native_agent_turn_event(event.clone(), 1_700_000_100, None)
            .await
            .expect("native event should commit");
        store
            .materialize_native_agent_turn_event(event, 1_700_000_100, None)
            .await
            .expect("exact native event replay should be idempotent");

        let rows = pioneer_entity::turn_event_delivery::Entity::find()
            .filter(
                pioneer_entity::turn_event_delivery::Column::TurnId
                    .eq("turn_native_outbox".to_owned()),
            )
            .all(&store.connection)
            .await
            .expect("outbox rows should list");
        assert_eq!(
            rows.len(),
            2,
            "live and episodic consumers enqueue once each"
        );
        assert!(rows.iter().all(|row| row.status == "pending"));

        let live = store
            .claim_due_turn_event_deliveries("live_notification", 1_700_000_100, 10)
            .await
            .expect("live delivery should claim");
        assert_eq!(live.len(), 1);
        assert!(
            store
                .complete_turn_event_delivery(
                    live[0].id.as_str(),
                    live[0].claim_token.as_str(),
                    1_700_000_101,
                )
                .await
                .expect("delivery completion should persist")
        );
        assert!(
            store
                .claim_due_turn_event_deliveries("live_notification", 1_700_000_102, 10)
                .await
                .expect("delivered rows should not reclaim")
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_event_append_is_fenced_by_durable_execution_owner() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_native_owner_fence",
            "thr_native_owner_fence",
            "turn_native_owner_fence",
        )
        .await;
        let created_at = unix_to_datetime(1_700_000_100);
        pioneer_crud_execution::turn_execution::insert_immutable(
            &store.connection,
            crate::NewTurnExecution {
                turn_id: "turn_native_owner_fence".to_owned(),
                thread_id: "thr_native_owner_fence".to_owned(),
                workspace_id: "ws_native_owner_fence".to_owned(),
                executor_kind: crate::TurnExecutorKind::NativeAgent,
                executor_key: Some("test-provider".to_owned()),
                status: crate::TurnExecutionStatus::Running,
                owner_id: "current-owner".to_owned(),
                lease_until: unix_to_datetime(1_700_000_145),
                created_at,
            },
        )
        .await
        .expect("execution ownership should persist");
        let event = CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
            workspace_id: "ws_native_owner_fence".to_owned(),
            thread_id: "thr_native_owner_fence".to_owned(),
            turn_id: "turn_native_owner_fence".to_owned(),
            item: TurnItem::SystemEvent {
                id: "native_owner_fence_item".to_owned(),
                level: SystemEventLevel::Info,
                message: "committed by the current owner".to_owned(),
                code: Some("native.owner.fence".to_owned()),
                details: None,
            },
        });

        let error = store
            .materialize_native_agent_turn_event_owned(
                event.clone(),
                1_700_000_101,
                None,
                "replaced-owner",
            )
            .await
            .expect_err("a stale execution owner must be fenced");
        assert!(format!("{error:#}").contains("stale execution owner"));
        assert_eq!(
            pioneer_entity::turn_event::Entity::find()
                .filter(
                    pioneer_entity::turn_event::Column::TurnId
                        .eq("turn_native_owner_fence".to_owned()),
                )
                .all(&store.connection)
                .await
                .expect("turn events should list")
                .len(),
            1,
            "the stale callback must not append a canonical event"
        );

        store
            .materialize_native_agent_turn_event_owned(event, 1_700_000_102, None, "current-owner")
            .await
            .expect("the current execution owner should append the event");
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn native_optional_delivery_preserves_scoped_routes_and_cancels_queued_claims() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteWriteClass, SqliteWriteEvent, SqliteWriteExecutor,
    };
    use sea_orm::TransactionTrait;

    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let observer = Arc::new(OptionalDeliveryDatabaseObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    database.validate_reader().await.unwrap();
    let interactive = CrudStore::new(database.clone());
    let timestamp = unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_delivery_scope".to_owned()),
        name: Set("Delivery scope fixture".to_owned()),
        is_active: Set(true),
        is_current: Set(false),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
    })
    .exec(&interactive.connection)
    .await
    .unwrap();
    let (interactive, thread, turn) = start_test_turn(
        interactive,
        "ws_delivery_scope",
        "thr_delivery_scope",
        "turn_delivery_scope",
    )
    .await;
    append_optional_delivery_test_items(&interactive, &thread, &turn.id, 2).await;
    observer.reads.lock().unwrap().clear();
    observer.events.lock().unwrap().clear();
    let maintenance = interactive.with_maintenance_access();
    for (store, consumer, read, write) in [
        (
            &interactive,
            "live_notification",
            SqliteReadClass::Interactive,
            SqliteWriteClass::Interactive,
        ),
        (
            &maintenance,
            "thread_episodic",
            SqliteReadClass::Maintenance,
            SqliteWriteClass::Maintenance,
        ),
    ] {
        let claims = store
            .claim_due_turn_event_deliveries(consumer, 1_700_000_110, 2)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        let reads = std::mem::take(&mut *observer.reads.lock().unwrap());
        assert!(!reads.is_empty() && reads.iter().all(|class| *class == read));
        let events = std::mem::take(&mut *observer.events.lock().unwrap());
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    SqliteWriteEvent::Acquired { class, .. } => Some(*class),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![write]
        );
        assert!(
            store
                .complete_turn_event_delivery(&claims[0].id, &claims[0].claim_token, 1_700_000_110)
                .await
                .unwrap()
        );
        observer.events.lock().unwrap().clear();
    }

    let source = |text: &str| TurnItem::UserMessage {
        id: "route_source".to_owned(),
        text: text.to_owned(),
        attachments: vec![],
    };
    interactive
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: turn.id.clone(),
                item: source("initial"),
            },
            1_700_000_120,
        )
        .await
        .unwrap();
    for (store, text, read, write) in [
        (
            &interactive,
            "interactive save",
            SqliteReadClass::Interactive,
            SqliteWriteClass::Interactive,
        ),
        (
            &maintenance,
            "maintenance save",
            SqliteReadClass::Maintenance,
            SqliteWriteClass::Maintenance,
        ),
    ] {
        observer.reads.lock().unwrap().clear();
        observer.events.lock().unwrap().clear();
        store
            .materialize_item_snapshot_updated(
                ItemUpdatedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: source(text),
                },
                1_700_000_121,
            )
            .await
            .unwrap();
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|class| *class == read)
        );
        let events = observer.events.lock().unwrap();
        assert!(events.iter().any(
            |event| matches!(event, SqliteWriteEvent::Acquired { class, .. } if *class == write)
        ));
        assert!(events.iter().all(
            |event| !matches!(event, SqliteWriteEvent::Acquired { class, .. } if *class != write)
        ));
    }
    let transaction = database.with_critical_writes().begin().await.unwrap();
    observer.events.lock().unwrap().clear();
    let cancelled = tokio::spawn({
        let store = maintenance.clone();
        async move {
            store
                .claim_due_turn_event_deliveries("live_notification", 1_700_000_110, 2)
                .await
        }
    });
    observer.wait_for_enqueued(1).await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert!(observer.events.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteWriteEvent::Cancelled {
            class: SqliteWriteClass::Maintenance,
            queue,
            ..
        } if queue.maintenance == 0
    )));
    let snapshot_cancelled = tokio::spawn({
        let store = maintenance.clone();
        let update = ItemUpdatedNotification {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            item: source("cancelled save"),
        };
        async move {
            store
                .materialize_item_snapshot_updated(update, 1_700_000_122)
                .await
        }
    });
    observer.wait_for_enqueued(2).await;
    snapshot_cancelled.abort();
    assert!(snapshot_cancelled.await.unwrap_err().is_cancelled());
    assert_eq!(
        interactive
            .get_turn_item(&turn.id, "route_source")
            .await
            .unwrap()
            .unwrap(),
        source("maintenance save")
    );
    let mut pending = Vec::new();
    for (store, consumer) in [
        (interactive, "live_notification"),
        (maintenance, "thread_episodic"),
    ] {
        pending.push(tokio::spawn(async move {
            store
                .claim_due_turn_event_deliveries(consumer, 1_700_000_110, 2)
                .await
        }));
    }
    observer.wait_for_enqueued(4).await;
    transaction.rollback().await.unwrap();
    for claim in pending {
        assert_eq!(claim.await.unwrap().unwrap().len(), 1);
    }
    let events = observer.events.lock().unwrap();
    let acquired = events
        .iter()
        .filter_map(|event| match event {
            SqliteWriteEvent::Acquired { class, .. } => Some(*class),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(acquired.len(), 2);
    assert!(acquired.contains(&SqliteWriteClass::Interactive));
    assert!(acquired.contains(&SqliteWriteClass::Maintenance));
    assert!(matches!(
        events.last(),
        Some(SqliteWriteEvent::Released { queue, .. })
            if queue.interactive == 0 && queue.maintenance == 0 && queue.critical == 0
    ));
    drop(events);
    let recovery_store = CrudStore::new(database.clone()).with_maintenance_access();
    let running = recovery_store
        .claim_due_thread_episodic_index_jobs(1_700_000_130, 1, 5)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let transaction = database.with_critical_writes().begin().await.unwrap();
    observer.events.lock().unwrap().clear();
    let recovery = tokio::spawn({
        let store = recovery_store.clone();
        let workspace_id = thread.workspace_id.clone();
        async move {
            store
                .requeue_running_thread_episodic_index_jobs_for_workspace(
                    &workspace_id,
                    1_700_000_131,
                    5,
                )
                .await
        }
    });
    observer.wait_for_enqueued(1).await;
    recovery.abort();
    assert!(recovery.await.unwrap_err().is_cancelled());
    assert!(observer.events.lock().unwrap().iter().any(|event| matches!(event, SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Maintenance, queue, .. } if queue.maintenance == 0)));
    transaction.rollback().await.unwrap();
    assert_eq!(
        recovery_store
            .find_thread_episodic_index_job(&running.id)
            .await
            .unwrap()
            .unwrap(),
        running
    );
    assert_eq!(
        recovery_store
            .requeue_running_thread_episodic_index_jobs_for_workspace(
                &thread.workspace_id,
                1_700_000_131,
                5
            )
            .await
            .unwrap(),
        1
    );
    let recovered = recovery_store
        .claim_due_thread_episodic_index_jobs(1_700_000_131, 1, 5)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(recovered.id, running.id);
    assert_eq!(recovered.attempt_count, running.attempt_count + 1);
    let events = observer.events.lock().unwrap();
    assert!(events.iter().all(|event| !matches!(event, SqliteWriteEvent::Acquired { class, .. } if *class == SqliteWriteClass::Interactive)));
    assert!(
        matches!(events.last(), Some(SqliteWriteEvent::Released { queue, .. }) if queue.maintenance == 0 && queue.critical == 0 && queue.interactive == 0)
    );
    drop(events);
    database.close().await.unwrap();
}

#[derive(Default)]
struct OptionalDeliveryDatabaseObserver {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadClass>>,
    events: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    enqueued: tokio::sync::Notify,
}

impl pioneer_sqlite::SqliteReadObserver for OptionalDeliveryDatabaseObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        if let pioneer_sqlite::SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}

impl pioneer_sqlite::SqliteWriteObserver for OptionalDeliveryDatabaseObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.events.lock().unwrap().push(event);
        if matches!(event, pioneer_sqlite::SqliteWriteEvent::Enqueued { .. }) {
            self.enqueued.notify_one();
        }
    }
}

impl OptionalDeliveryDatabaseObserver {
    async fn wait_for_enqueued(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let ready = self
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|event| {
                        matches!(event, pioneer_sqlite::SqliteWriteEvent::Enqueued { .. })
                    })
                    .count()
                    >= count;
                if ready {
                    break;
                }
                self.enqueued.notified().await;
            }
        })
        .await
        .expect("delivery claims should reach the writer queue");
    }
}
