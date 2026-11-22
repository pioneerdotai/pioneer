use super::*;

#[pioneer_postgres_test::postgres_test]
async fn episodic_requeue_and_canonical_snapshot_commits_are_serialized_in_both_orders() {
    use pioneer_postgres_pools::{PostgresWriteClass, PostgresWriteEvent};
    for snapshot_first in [true, false] {
        let (store, thread, turn, running, observer) = episodic_requeue_source_fixture().await;
        // Both contenders inherit Maintenance, so their explicit queue
        // barriers establish FIFO order without timing or priority changes.
        let maintenance = store.with_maintenance_access();
        let blocker = store
            .connection
            .with_critical_writes()
            .begin()
            .await
            .unwrap();
        observer.events.lock().unwrap().clear();
        observer.reads.lock().unwrap().clear();
        let snapshot = async {
            maintenance
                .materialize_item_snapshot_updated(
                    ItemUpdatedNotification {
                        workspace_id: thread.workspace_id.clone(),
                        thread_id: thread.id.clone(),
                        turn_id: turn.id.clone(),
                        item: TurnItem::UserMessage {
                            id: "requeue_source".to_owned(),
                            text: "B".to_owned(),
                            attachments: vec![],
                        },
                    },
                    1_700_000_011,
                )
                .await
        };
        let requeue = async {
            maintenance
                .requeue_thread_episodic_index_attempt(
                    &running.id,
                    running.attempt_count,
                    1_700_000_012,
                    Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR),
                )
                .await
        };
        tokio::pin!(snapshot, requeue);
        // Poll the first contender until its writer reservation is known.
        // Pending readers/CPU preparation complete outside that reservation.
        if snapshot_first {
            tokio::select! { biased;
                result = snapshot.as_mut() => panic!("snapshot bypassed occupied writer: {result:?}"),
                _ = observer.wait_for_enqueued(1) => {},
            }
            observer.reads.lock().unwrap().clear();
            tokio::select! { biased;
                result = requeue.as_mut() => panic!("requeue bypassed occupied writer: {result:?}"),
                _ = observer.wait_for_enqueued(2) => {},
            }
        } else {
            tokio::select! { biased;
                result = requeue.as_mut() => panic!("requeue bypassed occupied writer: {result:?}"),
                _ = observer.wait_for_enqueued(1) => {},
            }
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "the attempt guard must read under writer ownership, never the reader pool"
            );
            tokio::select! { biased;
                result = snapshot.as_mut() => panic!("snapshot bypassed occupied writer: {result:?}"),
                _ = observer.wait_for_enqueued(2) => {},
            }
        }
        if snapshot_first {
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "queued requeue must not read Running before the preceding source commit"
            );
        }
        blocker.rollback().await.unwrap();
        let (saved, requeued) = tokio::join!(snapshot, requeue);
        saved.unwrap();
        assert_eq!(
            requeued.unwrap(),
            if snapshot_first {
                ThreadEpisodicIndexAttemptOutcome::StaleAttempt
            } else {
                ThreadEpisodicIndexAttemptOutcome::Applied
            }
        );
        let acquired = observer
            .events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| {
                if let PostgresWriteEvent::Acquired { class, .. } = event {
                    Some(*class)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            acquired,
            vec![PostgresWriteClass::Maintenance; 2],
            "each contender must keep a single writer reservation through its commit"
        );
        let current = store
            .find_thread_episodic_index_job(&running.id)
            .await
            .unwrap()
            .unwrap();
        let mut expected = running.clone();
        if !snapshot_first {
            expected.next_run_at = unix_to_datetime(1_700_000_012);
        }
        expected.status = ThreadEpisodicIndexJobStatus::Canceled;
        expected.last_error = Some(THREAD_EPISODIC_SOURCE_VERSION_SUPERSEDED_ERROR.to_owned());
        expected.updated_at = unix_to_datetime(1_700_000_011);
        expected.completed_at = Some(expected.updated_at);
        assert_eq!(
            current, expected,
            "late requeue must preserve the entire canceled record"
        );
        let sources = store
            .list_thread_episodic_items_for_thread(&thread.workspace_id, &thread.id, 10)
            .await
            .unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(
            sources
                .iter()
                .find(|source| source.id == running.index_item_id)
                .unwrap()
                .status,
            ThreadEpisodicItemStatus::Superseded
        );
        let replacement = sources
            .iter()
            .find(|source| source.status == ThreadEpisodicItemStatus::PendingIndex)
            .unwrap();
        let job = store
            .find_thread_episodic_index_job_by_item(&replacement.id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(job.id, running.id);
        assert_eq!(job.status, ThreadEpisodicIndexJobStatus::Queued);
        assert_eq!(job.attempt_count, 0);
        assert_eq!(
            store
                .get_turn_item(&turn.id, "requeue_source")
                .await
                .unwrap()
                .unwrap(),
            TurnItem::UserMessage {
                id: "requeue_source".to_owned(),
                text: "B".to_owned(),
                attachments: vec![]
            }
        );
        assert_eq!(
            maintenance
                .requeue_thread_episodic_index_attempt(
                    &running.id,
                    running.attempt_count,
                    1_700_000_013,
                    Some(THREAD_EPISODIC_PROJECTION_CHANGED_ERROR),
                )
                .await
                .unwrap(),
            ThreadEpisodicIndexAttemptOutcome::StaleAttempt
        );
        assert_eq!(
            store
                .find_thread_episodic_index_job(&running.id)
                .await
                .unwrap()
                .unwrap(),
            current
        );
        store.database_connection().close().await.unwrap();
    }
}

#[tokio::test]
async fn episodic_direct_snapshot_is_atomic_and_idempotent_without_callback() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_source_save",
            "thr_source_save",
            "turn_source_save",
        )
        .await;
        let make_item = |text: &str| TurnItem::UserMessage {
            id: "source_save_item".to_owned(),
            text: text.to_owned(),
            attachments: vec![],
        };
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: make_item("A"),
                },
                1_700_000_010,
            )
            .await
            .unwrap();
        // No ingestor/callback is run. Snapshot save itself leaves durable work.
        let update = |text: &str| ItemUpdatedNotification {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            item: make_item(text),
        };
        store
            .materialize_item_snapshot_updated(update("B"), 1_700_000_011)
            .await
            .unwrap();
        let rows = store
            .list_thread_episodic_items_for_thread(&thread.workspace_id, &thread.id, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let job = store
            .find_thread_episodic_index_job_by_item(&rows[0].id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.status, ThreadEpisodicIndexJobStatus::Queued);
        store
            .materialize_item_snapshot_updated(update("B"), 1_700_000_099)
            .await
            .unwrap();
        assert_eq!(
            store
                .find_thread_episodic_index_job_by_item(&rows[0].id)
                .await
                .unwrap()
                .unwrap(),
            job
        );
        store
            .materialize_item_snapshot_updated(update("C"), 1_700_000_100)
            .await
            .unwrap();
        assert_eq!(
            store
                .find_thread_episodic_item(&rows[0].id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ThreadEpisodicItemStatus::Superseded
        );
        assert_eq!(
            store
                .find_thread_episodic_index_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ThreadEpisodicIndexJobStatus::Canceled
        );
        let current = store
            .list_thread_episodic_items_for_thread(&thread.workspace_id, &thread.id, 10)
            .await
            .unwrap()
            .into_iter()
            .find(|item| item.status == ThreadEpisodicItemStatus::PendingIndex)
            .unwrap();
        assert_eq!(
            current.text_hash,
            crate::thread_episodic_source::item_text_hash(
                &crate::thread_episodic_source::committed_item_ingestion_input_from_parts(
                    &thread.workspace_id,
                    &thread.id,
                    &turn.id,
                    make_item("C")
                )
                .unwrap(),
                "C"
            )
        );
        assert!(
            store
                .find_thread_episodic_index_job_by_item(&current.id)
                .await
                .unwrap()
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn episodic_direct_save_bookkeeping_failure_rolls_back_source_and_projection() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(fixture,
            "ws_source_rollback",
            "thr_source_rollback",
            "turn_source_rollback",
        )
        .await;
        let item = |text: &str| TurnItem::UserMessage {
            id: "rollback_source".to_owned(),
            text: text.to_owned(),
            attachments: vec![],
        };
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: item("before"),
                },
                1_700_000_010,
            )
            .await
            .unwrap();
        let before = store
            .get_turn_item(&turn.id, "rollback_source")
            .await
            .unwrap()
            .unwrap();
        // Fault injection belongs only to this fixture; production guards remain intact.
        store.connection.execute_unprepared("CREATE FUNCTION fail_episodic_job() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'bookkeeping failure'; END $$; CREATE TRIGGER fail_episodic_job BEFORE INSERT ON thread_episodic_index_jobs FOR EACH ROW EXECUTE FUNCTION fail_episodic_job()").await.unwrap();
        store
            .materialize_item_snapshot_updated(
                ItemUpdatedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: item("after"),
                },
                1_700_000_011,
            )
            .await
            .expect_err("source and job must be one atomic write set");
        assert_eq!(
            store
                .get_turn_item(&turn.id, "rollback_source")
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert!(
            store
                .list_thread_episodic_items_for_thread(&thread.workspace_id, &thread.id, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .list_thread_episodic_index_jobs_for_thread(&thread.workspace_id, &thread.id, 10)
                .await
                .unwrap()
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn episodic_direct_save_revalidates_group_context_and_rolls_back() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_context_guard",
            "thr_context_guard",
            "turn_context_guard",
        )
        .await;
        let item = |text: &str| TurnItem::AgentMessage {
            id: "context_source".to_owned(),
            text: text.to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: item("before"),
                },
                1_700_000_010,
            )
            .await
            .unwrap();
        let after = item("after");
        let prepared = store
            .prepare_episodic_source_save(&thread.workspace_id, &thread.id, &turn.id, &after)
            .await
            .unwrap();
        let now = unix_to_datetime(1_700_000_011);
        let projected = pioneer_crud_conversations::turn_item::prepare_turn_item_projection(
            &turn.id,
            &after,
            Some("completed"),
            now,
            now,
        )
        .unwrap();
        let transaction = store.connection.begin().await.unwrap();
        // A preceding write in the same transaction changes the causal group.
        pioneer_entity::task_run_turn::Entity::insert(pioneer_entity::task_run_turn::ActiveModel {
            id: Set("new_context_link".to_owned()),
            task_id: Set("context_task".to_owned()),
            run_id: Set("context_run".to_owned()),
            execution_id: Set(None),
            thread_id: Set(thread.id.clone()),
            turn_id: Set(turn.id.clone()),
            kind: Set("initial".to_owned()),
            round: Set(0),
            sequence: Set(0),
            status: Set("candidate_created".to_owned()),
            reviews_candidate_id: Set(None),
            requested_by_candidate_id: Set(None),
            requested_by_review_event_id: Set(None),
            created_at: Set(now),
            started_at: Set(None),
            completed_at: Set(None),
        })
        .exec(&transaction)
        .await
        .unwrap();
        pioneer_crud_conversations::turn_item::upsert_prepared_turn_item(&transaction, projected)
            .await
            .unwrap();
        let error = store
            .commit_episodic_source_save(&transaction, &prepared, now)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("context changed before commit"));
        transaction.rollback().await.unwrap();
        assert_eq!(
            store
                .get_turn_item(&turn.id, "context_source")
                .await
                .unwrap()
                .unwrap(),
            item("before")
        );
        assert!(
            store
                .get_task_run_turn_by_turn(&thread.id, &turn.id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .list_thread_episodic_index_jobs_for_thread(&thread.workspace_id, &thread.id, 10)
                .await
                .unwrap()
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn episodic_updated_delivery_and_task_projection_survive_missing_callback() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_source_delivery",
            "thr_source_delivery",
            "turn_source_delivery",
        )
        .await;
        let item = |text: &str| TurnItem::AgentMessage {
            id: "durable_source".to_owned(),
            text: text.to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        let completed = ItemCompletedNotification {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            item: item("first"),
        };
        store
            .materialize_task_delivery_item(
                ItemStartedNotification {
                    workspace_id: completed.workspace_id.clone(),
                    thread_id: completed.thread_id.clone(),
                    turn_id: completed.turn_id.clone(),
                    item: completed.item.clone(),
                },
                completed,
                1_700_000_010,
                None,
            )
            .await
            .unwrap();
        store
            .materialize_item_updated(
                ItemUpdatedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: item("changed"),
                },
                1_700_000_011,
            )
            .await
            .unwrap();
        let maintenance = store.with_maintenance_access();
        let first = maintenance
            .claim_due_turn_event_deliveries("thread_episodic", 1_700_000_100, 10)
            .await
            .unwrap();
        assert_eq!(first.len(), 1); // Causal predecessor blocks the update delivery.
        assert!(matches!(
            first[0].event.payload,
            CanonicalTurnEventPayload::ItemCompleted(_)
        ));
        let repeat = maintenance
            .claim_due_turn_event_deliveries("thread_episodic", 1_700_001_100, 10)
            .await
            .unwrap();
        assert_eq!(repeat.len(), 1);
        assert_eq!(repeat[0].id, first[0].id);
        assert_ne!(repeat[0].claim_token, first[0].claim_token);
        assert!(
            !maintenance
                .complete_turn_event_delivery(&first[0].id, &first[0].claim_token, 1_700_001_100)
                .await
                .unwrap()
        );
        assert!(
            maintenance
                .complete_turn_event_delivery(&repeat[0].id, &repeat[0].claim_token, 1_700_001_100)
                .await
                .unwrap()
        );
        let next = maintenance
            .claim_due_turn_event_deliveries("thread_episodic", 1_700_001_101, 10)
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        assert!(matches!(
            next[0].event.payload,
            CanonicalTurnEventPayload::ItemUpdated(_)
        ));
        assert_eq!(
            store
                .get_turn_item(&turn.id, "durable_source")
                .await
                .unwrap()
                .unwrap(),
            item("changed")
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn episodic_delivery_waits_for_projection_replay() {
    pioneer_postgres_test::with_database(async |fixture| {
        use pioneer_entity::{
            turn_event_projection_state as receipt, turn_event_projection_stream_state as stream,
        };
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_delivery_replay",
            "thr_delivery_replay",
            "turn_delivery_replay",
        )
        .await;
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: TurnItem::UserMessage {
                        id: "replay_source".to_owned(),
                        text: "durable source".to_owned(),
                        attachments: vec![],
                    },
                },
                1_700_000_010,
            )
            .await
            .unwrap();
        let event = pioneer_crud_execution::event_append::latest_event_for_turn(
            &store.connection,
            &turn.id,
        )
        .await
        .unwrap()
        .unwrap();
        receipt::Entity::update_many()
            .filter(receipt::Column::EventId.eq(event.id.clone()))
            .col_expr(receipt::Column::Status, Expr::value("pending"))
            .exec(&store.connection)
            .await
            .unwrap();
        stream::Entity::update_many()
            .filter(stream::Column::TurnId.eq(turn.id.clone()))
            .col_expr(
                stream::Column::ProjectedThroughSequence,
                Expr::value(event.sequence - 1),
            )
            .exec(&store.connection)
            .await
            .unwrap();
        assert!(
            store
                .claim_due_turn_event_deliveries("thread_episodic", 1_700_000_100, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .replay_due_turn_event_projections(1_700_000_100, 10)
                .await
                .unwrap()
                .projected,
            1
        );
        let delivered = store
            .claim_due_turn_event_deliveries("thread_episodic", 1_700_000_101, 10)
            .await
            .unwrap();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].event.id, event.id);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn episodic_canonical_payload_repair_commits_job_and_fences_stale_payload() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_source_repair",
            "thr_source_repair",
            "turn_source_repair",
        )
        .await;
        let item = |text: &str| TurnItem::UserMessage {
            id: "repair_source".to_owned(),
            text: text.to_owned(),
            attachments: vec![],
        };
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: item("before"),
                },
                1_700_000_010,
            )
            .await
            .unwrap();
        let row = pioneer_crud_conversations::turn_reads::find_turn_item(
            &store.connection,
            &turn.id,
            "repair_source",
        )
        .await
        .unwrap()
        .unwrap();
        let payload = serde_json::to_string(&item("repaired")).unwrap();
        let episodic = store
            .memory_store()
            .prepare_terminal_payload_episodic_save(&row, &payload)
            .await
            .unwrap()
            .unwrap();
        let transaction = store.connection.begin().await.unwrap();
        assert!(
            store
                .apply_one_terminal_turn_item_payload_repair(
                    &transaction,
                    &row,
                    &pioneer_crud_payloads::storage::PreparedPayload::new(&payload).unwrap(),
                    Some(&episodic)
                )
                .await
                .unwrap()
        );
        transaction.commit().await.unwrap();
        let jobs = store
            .list_thread_episodic_index_jobs_for_thread(&thread.workspace_id, &thread.id, 10)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, ThreadEpisodicIndexJobStatus::Queued);
        let stale_payload = serde_json::to_string(&item("stale repair")).unwrap();
        let stale = store
            .memory_store()
            .prepare_terminal_payload_episodic_save(&row, &stale_payload)
            .await
            .unwrap()
            .unwrap();
        let transaction = store.connection.begin().await.unwrap();
        assert!(
            !store
                .apply_one_terminal_turn_item_payload_repair(
                    &transaction,
                    &row,
                    &pioneer_crud_payloads::storage::PreparedPayload::new(&stale_payload).unwrap(),
                    Some(&stale)
                )
                .await
                .unwrap()
        );
        transaction.commit().await.unwrap();
        assert_eq!(
            store
                .get_turn_item(&turn.id, "repair_source")
                .await
                .unwrap()
                .unwrap(),
            item("repaired")
        );
        assert_eq!(
            store
                .list_thread_episodic_index_jobs_for_thread(&thread.workspace_id, &thread.id, 10)
                .await
                .unwrap(),
            jobs
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn episodic_started_update_does_not_complete_or_index_draft() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_source_draft",
            "thr_source_draft",
            "turn_source_draft",
        )
        .await;
        let item = TurnItem::AgentMessage {
            id: "draft_source".to_owned(),
            text: "unfinished final answer".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: item.clone(),
                },
                1_700_000_010,
            )
            .await
            .unwrap();
        let update = ItemUpdatedNotification {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            item: item.clone(),
        };
        store
            .materialize_item_updated(update.clone(), 1_700_000_011)
            .await
            .unwrap();
        store
            .materialize_item_snapshot_updated(update, 1_700_000_012)
            .await
            .unwrap();
        assert!(
            !store
                .get_thread_episodic_canonical_item(
                    &thread.workspace_id,
                    &thread.id,
                    &turn.id,
                    "draft_source"
                )
                .await
                .unwrap()
                .unwrap()
                .committed
        );
        assert!(
            store
                .list_thread_episodic_items_for_thread(&thread.workspace_id, &thread.id, 10)
                .await
                .unwrap()
                .is_empty()
        );
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item,
                },
                1_700_000_013,
            )
            .await
            .unwrap();
        assert!(
            store
                .get_thread_episodic_canonical_item(
                    &thread.workspace_id,
                    &thread.id,
                    &turn.id,
                    "draft_source"
                )
                .await
                .unwrap()
                .unwrap()
                .committed
        );
        Ok(())
    })
    .await
    .unwrap();
}

async fn episodic_requeue_source_fixture() -> (
    CrudStore,
    Thread,
    Turn,
    ThreadEpisodicIndexJobRecord,
    Arc<FixtureDatabaseObserver>,
) {
    use pioneer_postgres_pools::{PostgresDatabase, PostgresWriteExecutor};

    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let observer = Arc::new(FixtureDatabaseObserver::default());
    let database = PostgresDatabase::from_executor_with_read_observer(
        reader,
        PostgresWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    database.validate_reader().await.unwrap();
    let store = CrudStore::new(database);
    let now = unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_requeue_source".to_owned()),
        name: Set("Requeue source fixture".to_owned()),
        is_active: Set(true),
        is_current: Set(false),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(&store.connection)
    .await
    .unwrap();
    let (store, thread, turn) = start_test_turn(
        store,
        "ws_requeue_source",
        "thr_requeue_source",
        "turn_requeue_source",
    )
    .await;
    let item = TurnItem::UserMessage {
        id: "requeue_source".to_owned(),
        text: "A".to_owned(),
        attachments: vec![],
    };
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: turn.id.clone(),
                item: item.clone(),
            },
            1_700_000_010,
        )
        .await
        .unwrap();
    store
        .materialize_item_snapshot_updated(
            ItemUpdatedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: turn.id.clone(),
                item,
            },
            1_700_000_010,
        )
        .await
        .unwrap();
    let job = store
        .list_thread_episodic_index_jobs_for_thread(&thread.workspace_id, &thread.id, 10)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let running = store
        .with_maintenance_access()
        .claim_thread_episodic_index_job_if_due(&job.id, 1_700_000_010, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.attempt_count, 1);
    (store, thread, turn, running, observer)
}
