use super::*;

#[tokio::test]
async fn duplicate_interruption_during_projection_preserves_canonical_acceptance() {
    pioneer_postgres_test::with_database(async |fixture| {
    let workspace_id = "ws_interruption_projection_race";
    let thread_id = "thr_interruption_projection_race";
    let turn_id = "turn_interruption_projection_race";
    let store = postgres_store_with_workspace(fixture, workspace_id).await;
    let timestamp = 1_700_000_000;
    let thread = sample_thread(workspace_id, thread_id, timestamp);
    let turn = sample_turn(turn_id);
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    let db = store.database_connection();
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal,priority,revision,created_at,updated_at) VALUES('stop-task',$1,'workspace',$1,'system','running','Stop task','Goal',0,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [workspace_id.into()])).await.unwrap();
    db.execute_unprepared("INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,created_at,updated_at) VALUES('stop-run','stop-task','stop-run',1,1,'running','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('stop-binding','stop-task','stop-run',$1,$2,'initial',0,0,'in_progress',CURRENT_TIMESTAMP)", [thread_id.into(),turn_id.into()])).await.unwrap();
    let mut turn = store.get_turn(thread_id, turn_id).await.unwrap().unwrap().1;
    turn.status = TurnStatus::Interrupted;
    turn.error = Some("cancel with pending heartbeat writer".to_owned());
    let event = CanonicalTurnEventPayload::TurnFailed(TurnFailedNotification {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn: turn.clone(),
    });
    let context = crate::store::events::event_preparation::TurnEventProjectionContext {
        item_started_deadlines: None,
        enqueue_optional_deliveries: true,
        native_cancellation: None,
        unactivated_terminal: None,
        cancellation_actor_id: Some("P00000000000000000001".into()),
    };
    // Stop the first producer at the real append/projection boundary.
    // A CLI cancellation before runtime binding has no native receipt;
    // cleanup can concurrently replay this same terminal notification.
    let appended = store
        .append_claimed_turn_event_projection_once(
            event.clone(),
            unix_to_datetime(timestamp + 1),
            context.clone(),
            "interruption_projection_owner".to_owned(),
            &|| unix_to_datetime(timestamp + 61),
            None,
            None,
        )
        .await
        .unwrap()
        .expect("terminal event cannot be a superseded MCP observation");
    assert!(appended.was_inserted);
    let saved = pioneer_entity::task_run_cancellation_actor::Entity::find_by_id("stop-run")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.principal_id, "P00000000000000000001");
    assert!(
        pioneer_entity::task_run_read::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty(),
        "canonical interruption alone is not a terminal run receipt"
    );

    assert_eq!(appended.sequence, 2);
    assert!(
        store
            .native_cancellation_context(turn_id)
            .await
            .unwrap()
            .is_none()
    );

    let duplicate_error = store
        .materialize_native_agent_turn_event(event.clone(), timestamp + 2, None)
        .await
        .expect_err("projection remains pending behind the original producer");
    assert!(
        duplicate_error
            .downcast_ref::<crate::TurnEventProjectionAfterAppendError>()
            .expect("typed after-append outcome")
            .is_pending()
    );
    assert!(
        crate::turn_event_was_appended_before_error(&duplicate_error),
        "Gateway must preserve the terminal state and ACK the already committed event"
    );
    assert_eq!(
        store
            .get_turn(thread_id, turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress,
        "duplicate acceptance must not pretend the read model was projected"
    );
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
            .count(&store.connection)
            .await
            .unwrap(),
        2,
        "duplicate cancellation must not append a second terminal event"
    );

    let mut conflicting_turn = turn.clone();
    conflicting_turn.status = TurnStatus::Completed;
    conflicting_turn.error = None;
    let conflict_error = store
        .materialize_native_agent_turn_event(
            CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn: conflicting_turn,
            }),
            timestamp + 3,
            None,
        )
        .await
        .expect_err("a different terminal result must still fail before append");
    assert!(!crate::turn_event_was_appended_before_error(
        &conflict_error
    ));

    assert!(matches!(
        store
            .project_claimed_turn_event_once(
                appended,
                context,
                "interruption_projection_owner".to_owned(),
                unix_to_datetime(timestamp + 4),
            )
            .await
            .unwrap(),
        crate::store::events::event_projection::TurnEventProjectionOutcome::Projected
    ));
    assert_eq!(
        store.get_turn(thread_id, turn_id).await.unwrap().unwrap().1,
        turn,
        "original producer must retain its projection claim and finish normally"
    );
    store
        .materialize_native_agent_turn_event_owned_as(
            event,
            timestamp + 5,
            None,
            "synthetic-owner",
            Some("P00000000000000000002".into()),
        )
        .await
        .expect("replay cannot replace the accepted cancellation actor");
    let replayed_actor =
        pioneer_entity::task_run_cancellation_actor::Entity::find_by_id("stop-run")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        replayed_actor.principal_id, saved.principal_id,
        "replay cannot reattribute the accepted Stop"
    );
    // Model a crash after per-turn reconciliation and before terminal Run
    // commit. Discovery and the subsequent commit use only durable state.
    db.execute_unprepared(
        "UPDATE task_run_turn SET status='cancelled' WHERE id='stop-binding'",
    )
    .await
    .unwrap();
    let pending = store
        .list_unreconciled_terminal_task_child_turns(10)
        .await
        .unwrap();
    assert!(pending.iter().any(|pending| pending.turn_id == turn_id));
    store
        .append_task_cancellation_events(
            vec![pioneer_protocol::TaskEventPayload::RunCancelled {
                task_id: "stop-task".into(),
                run_id: "stop-run".into(),
                reason: Some("durable replay".into()),
                cancelled_at: timestamp + 6,
            }],
            timestamp + 6,
            None,
        )
        .await
        .unwrap();
    let receipts = pioneer_entity::task_run_read::Entity::find()
        .all(&db)
        .await
        .unwrap();
    assert!(
        receipts.is_empty(),
        "an independent TaskService system cancellation cannot inherit a pending Stop actor; only deferred terminal reconciliation uses that attribution"
    );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn materialization_remains_idempotent_after_projection_receipt_cleanup() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_receipt_cleanup").await;
        let timestamp = 1_700_000_000;
        let thread = sample_thread("ws_receipt_cleanup", "thread_receipt_cleanup", timestamp);
        let turn = sample_turn("turn_receipt_cleanup");
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .unwrap();
        let mut completed = turn.clone();
        completed.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn: completed,
                },
                timestamp + 1,
            )
            .await
            .unwrap();
        let now = chrono::Utc::now().fixed_offset();
        crate::upsert_projection_meta(
            &store.connection,
            crate::ProjectionMetaRecord {
                projection_key: "turn_event_projection_stream_state_backfill".into(),
                projection_version: 3,
                status: crate::PROJECTION_META_STATUS_COMPLETE.into(),
                source_thread_count: 0,
                source_turn_count: 0,
                source_turn_item_count: 0,
                source_turn_event_count: 0,
                last_error: None,
                backfill_started_at: Some(now),
                backfilled_at: Some(now),
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .unwrap();
        let cleanup = store.with_maintenance_access();
        assert_eq!(
            cleanup
                .cleanup_projection_receipts_quantum(None, None)
                .await
                .unwrap()
                .rows_deleted,
            2
        );
        let before = store
            .get_turn_event_projection_stream_state(&turn.id)
            .await
            .unwrap()
            .unwrap();
        let events_before = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .count(&store.connection)
            .await
            .unwrap();
        assert_eq!(
            events_before, 2,
            "TurnStarted and TurnCompleted remain durable"
        );
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("idempotent materialization must not require a compacted receipt");
        let after = store
            .get_turn_event_projection_stream_state(&turn.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after.projected_through_sequence,
            before.projected_through_sequence
        );
        assert_eq!(
            pioneer_entity::turn_event_projection_state::Entity::find()
                .filter(
                    pioneer_entity::turn_event_projection_state::Column::TurnId.eq(turn.id.clone())
                )
                .count(&store.connection)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pioneer_entity::turn_event::Entity::find()
                .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
                .count(&store.connection)
                .await
                .unwrap(),
            events_before
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn projection_watermark_is_authoritative_and_retains_receipts() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_projection_watermark_shadow").await;
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_projection_watermark_shadow";
        let thread_id = "thr_projection_watermark_shadow";
        let turn_id = "turn_projection_watermark_shadow";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let turn = sample_turn(turn_id);

        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn start should persist");

        let event = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
            .filter(pioneer_entity::turn_event::Column::Sequence.eq(1))
            .one(&store.connection)
            .await
            .expect("turn event lookup should succeed")
            .expect("turn/start event should exist");
        pioneer_entity::turn_event_projection_state::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::Status,
                sea_orm::sea_query::Expr::value(
                    pioneer_crud_conversations::event_projection::PROJECTION_STATUS_FAILED,
                ),
            )
            .filter(
                pioneer_entity::turn_event_projection_state::Column::EventId.eq(event.id.clone()),
            )
            .exec(&store.connection)
            .await
            .expect("projection receipt should become incomplete");

        let stream = store
            .get_turn_event_projection_stream_state(turn_id)
            .await
            .expect("projection stream should query")
            .expect("projection stream should exist");
        assert_eq!(stream.projected_through_sequence, 1);

        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("authoritative watermark should satisfy idempotency");

        let receipt = pioneer_entity::turn_event_projection_state::Entity::find_by_id(event.id)
            .one(&store.connection)
            .await
            .expect("projection receipt should query")
            .expect("projection receipt must remain present");
        assert_eq!(
            receipt.status,
            pioneer_crud_conversations::event_projection::PROJECTION_STATUS_FAILED
        );
        assert!(
            !pioneer_crud_conversations::event_projection::has_unprojected_event(
                &store.connection,
                turn_id,
            )
            .await
            .expect("watermark completion should query"),
            "a stale retained receipt must not make a fully projected Turn incomplete"
        );

        store
            .materialize_native_agent_turn_event(
                CanonicalTurnEventPayload::ItemStarted(ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: safe_web_fetch_item("call_watermark_authority"),
                }),
                timestamp + 1,
                None,
            )
            .await
            .expect("watermark should allow its immediate successor despite stale receipt state");

        let stream = store
            .get_turn_event_projection_stream_state(turn_id)
            .await
            .expect("projection stream should query")
            .expect("projection stream should exist");
        assert_eq!(stream.projected_through_sequence, 2);
        let receipts = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(turn_id))
            .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("projection receipts should query");
        assert_eq!(receipts.len(), 2, "phase two must not delete receipts");
        assert_eq!(
            receipts[0].status,
            pioneer_crud_conversations::event_projection::PROJECTION_STATUS_FAILED
        );
        assert_eq!(
            receipts[1].status,
            pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PROJECTED
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn projection_receipt_transition_rolls_back_when_watermark_cas_fails() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_projection_watermark_atomic").await;
        let timestamp = 1_700_000_000;
        let thread_id = "thr_projection_watermark_atomic";
        let turn_id = "turn_projection_watermark_atomic";
        let event_id = "evt_watermark_atomic";
        let claim_token = "claim_watermark_atomic";

        store
            .materialize_turn_start(
                &sample_thread("ws_projection_watermark_atomic", thread_id, timestamp),
                SandboxMode::FullAccess,
                &sample_turn(turn_id),
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn start should persist");
        let seed = store.connection.begin().await?;
        pioneer_crud_conversations::event_projection::insert_claimed(
            &seed,
            pioneer_crud_conversations::event_projection::NewTurnEventProjectionState {
                event_id: event_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                sequence: 2,
                projection_context_json: "{}".to_owned(),
                claim_token: claim_token.to_owned(),
                claim_expires_at: unix_to_datetime(timestamp + 60),
                created_at: unix_to_datetime(timestamp + 1),
            },
        )
        .await
        .expect("projection receipt fixture should insert");
        seed.commit().await?;
        set_turn_projection_watermark(&store, turn_id, 0).await;

        let transaction = store
            .connection
            .begin()
            .await
            .expect("projection transaction should begin");
        let error = crate::store::events::projection_receipts::mark_projected_claimed(
            &transaction,
            event_id,
            turn_id,
            2,
            claim_token,
            unix_to_datetime(timestamp + 2),
        )
        .await
        .expect_err("watermark gap must reject projection completion");
        assert!(
            format!("{error:#}").contains("did not advance atomically"),
            "unexpected watermark CAS error: {error:#}"
        );
        transaction
            .rollback()
            .await
            .expect("failed projection transaction should roll back");

        let receipt = pioneer_entity::turn_event_projection_state::Entity::find_by_id(event_id)
            .one(&store.connection)
            .await
            .expect("projection receipt should query")
            .expect("projection receipt must remain present");
        assert_eq!(
            receipt.status,
            pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PROJECTING,
            "receipt transition must roll back with the failed watermark CAS"
        );
        let stream = store
            .get_turn_event_projection_stream_state(turn_id)
            .await
            .expect("projection stream should query")
            .expect("projection stream should remain present");
        assert_eq!(stream.projected_through_sequence, 0);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn turn_event_projection_deferral_keeps_raw_event_and_replays_in_order() {
    pioneer_postgres_test::with_database(async |fixture| {
    let store = postgres_store_with_workspace(fixture, "ws_projection_replay").await;
    let timestamp = 1_700_000_000;
    let workspace_id = "ws_projection_replay";
    let thread_id = "thr_projection_replay";
    let turn_id = "turn_projection_replay";
    let item_id = "call_projection_replay";
    let thread = sample_thread(workspace_id, thread_id, timestamp);
    let turn = sample_turn(turn_id);

    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .expect("turn start should persist");

    let initial_stream = store
        .get_turn_event_projection_stream_state(turn_id)
        .await
        .expect("projection stream should query")
        .expect("projection stream should exist");
    assert_eq!(initial_stream.projected_through_sequence, 1);

    let start_event = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
        .filter(pioneer_entity::turn_event::Column::Sequence.eq(1))
        .one(&store.connection)
        .await
        .expect("turn event lookup should succeed")
        .expect("turn/start event should exist");

    pioneer_entity::turn_event_projection_state::Entity::update_many()
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::Status,
            sea_orm::sea_query::Expr::value(
                pioneer_crud_conversations::event_projection::PROJECTION_STATUS_FAILED,
            ),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(unix_to_datetime(timestamp + 20)),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(
                Option::<sea_orm::entity::prelude::DateTimeWithTimeZone>::None,
            ),
        )
        .col_expr(
            pioneer_entity::turn_event_projection_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(unix_to_datetime(timestamp + 2)),
        )
        .filter(pioneer_entity::turn_event_projection_state::Column::EventId.eq(start_event.id))
        .exec(&store.connection)
        .await
        .expect("projection state should be marked failed");
    set_turn_projection_watermark(&store, turn_id, 0).await;

    let deadlines = TurnItemAttemptDeadlines {
        lease_expires_at_unix: Some(timestamp + 121),
        idle_deadline_at_unix: Some(timestamp + 91),
        hard_deadline_at_unix: Some(timestamp + 301),
    };

    let deferred_error = store
        .materialize_native_agent_turn_event(
            CanonicalTurnEventPayload::ItemStarted(ItemStartedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: safe_web_fetch_item(item_id),
            }),
            timestamp + 1,
            Some(deadlines),
        )
        .await
        .expect_err("an unapplied authoritative projection must reject its commit ACK");
    assert!(
        crate::turn_event_was_appended_before_error(&deferred_error),
        "the rejected ACK must retain the raw event for ordered replay"
    );

    let events = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
        .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
        .all(&store.connection)
        .await
        .expect("turn event list should succeed");
    assert_eq!(
        events.len(),
        2,
        "failed projection must not roll back raw event"
    );
    let optional_deliveries = pioneer_entity::turn_event_delivery::Entity::find()
        .filter(pioneer_entity::turn_event_delivery::Column::EventId.eq(events[1].id.clone()))
        .all(&store.connection)
        .await
        .expect("optional delivery lookup should succeed");
    assert_eq!(
        optional_deliveries.len(),
        1,
        "native live delivery must commit atomically with the raw event before projection"
    );

    let item_state =
        pioneer_entity::turn_event_projection_state::Entity::find_by_id(events[1].id.clone())
            .one(&store.connection)
            .await
            .expect("projection state lookup should succeed")
            .expect("item projection state should exist");
    assert_eq!(
        item_state.status,
        pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PENDING
    );
    assert_eq!(item_state.attempt_count, 0);
    assert!(item_state.last_error.is_none());

    let item_before_replay = pioneer_entity::turn_item::Entity::find()
        .filter(pioneer_entity::turn_item::Column::TurnId.eq(turn_id))
        .filter(pioneer_entity::turn_item::Column::ItemId.eq(item_id))
        .one(&store.connection)
        .await
        .expect("turn item lookup before replay should succeed");
    assert!(
        item_before_replay.is_none(),
        "deferred event must wait for its predecessor before projection"
    );

    let deferred_replay = store
        .replay_due_turn_event_projections(timestamp + 10, 10)
        .await
        .expect("projection replay should succeed");
    assert_eq!(deferred_replay.claimed, 0);
    assert_eq!(deferred_replay.projected, 0);
    assert_eq!(deferred_replay.deferred, 0);
    assert_eq!(deferred_replay.failed, 0);
    assert_eq!(deferred_replay.exhausted, 0);

    let deferred_state =
        pioneer_entity::turn_event_projection_state::Entity::find_by_id(events[1].id.clone())
            .one(&store.connection)
            .await
            .expect("deferred projection state lookup should succeed")
            .expect("deferred projection state should exist");
    assert_eq!(
        deferred_state.status,
        pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PENDING
    );
    assert_eq!(deferred_state.attempt_count, 0);

    let replay_database = store.database_connection();
    let mut control = replay_database.work_signal("gateway.control").subscribe();
    control.begin_pass(&replay_database).await.unwrap();
    let replay = store
        .replay_due_turn_event_projections(timestamp + 30, 10)
        .await
        .expect("ordered projection replay should succeed");
    assert_eq!(replay.claimed, 2);
    assert_eq!(replay.projected, 2);
    assert_eq!(replay.deferred, 0);
    assert_eq!(replay.failed, 0);
    assert_eq!(replay.exhausted, 0);
    assert!(
        futures_util::FutureExt::now_or_never(control.wait(None)).is_some(),
        "the append wake may already be consumed before replay materializes control dependencies"
    );

    let final_replay = store
        .replay_due_turn_event_projections(timestamp + 40, 10)
        .await
        .expect("dependent projection replay should succeed");
    assert_eq!(final_replay.claimed, 0);
    assert_eq!(final_replay.projected, 0);
    assert_eq!(final_replay.deferred, 0);
    assert_eq!(final_replay.failed, 0);
    assert_eq!(final_replay.exhausted, 0);

    let states = pioneer_entity::turn_event_projection_state::Entity::find()
        .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(turn_id))
        .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
        .all(&store.connection)
        .await
        .expect("projection states should query");
    assert_eq!(states.len(), 2);
    assert!(states.iter().all(|state| {
        state.status
            == pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PROJECTED
    }));
    let final_stream = store
        .get_turn_event_projection_stream_state(turn_id)
        .await
        .expect("projection stream should query")
        .expect("projection stream should exist");
    assert_eq!(final_stream.projected_through_sequence, 2);

    let attempt = pioneer_entity::turn_item_attempt::Entity::find()
        .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id))
        .filter(pioneer_entity::turn_item_attempt::Column::ItemId.eq(item_id))
        .one(&store.connection)
        .await
        .expect("attempt lookup should succeed")
        .expect("attempt should be projected during replay");
    assert_eq!(
        attempt.lease_expires_at,
        deadlines.lease_expires_at_unix.map(unix_to_datetime)
    );
    assert_eq!(
        attempt.idle_deadline_at,
        deadlines.idle_deadline_at_unix.map(unix_to_datetime)
    );
    assert_eq!(
        attempt.hard_deadline_at,
        deadlines.hard_deadline_at_unix.map(unix_to_datetime)
    );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn poisoned_projection_stream_is_quarantined_and_operator_restore_resumes_in_order() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_projection_quarantine").await;
        let timestamp = 1_700_100_000;
        let workspace_id = "ws_projection_quarantine";
        let poisoned_thread_id = "thr_projection_poison";
        let poisoned_turn_id = "turn_projection_poison";
        let poisoned_item_id = "call_projection_poison";
        let healthy_thread_id = "thr_projection_healthy";
        let healthy_turn_id = "turn_projection_healthy";
        let healthy_item_id = "call_projection_healthy";

        for (thread_id, turn_id) in [
            (poisoned_thread_id, poisoned_turn_id),
            (healthy_thread_id, healthy_turn_id),
        ] {
            store
                .materialize_turn_start(
                    &sample_thread(workspace_id, thread_id, timestamp),
                    SandboxMode::FullAccess,
                    &sample_turn(turn_id),
                    &[],
                    pioneer_protocol::PersistedActorRef::System,
                )
                .await
                .expect("turn start should persist");
        }

        let poisoned_start = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(
                pioneer_entity::turn_event_projection_state::Column::TurnId.eq(poisoned_turn_id),
            )
            .filter(pioneer_entity::turn_event_projection_state::Column::Sequence.eq(1))
            .one(&store.connection)
            .await
            .expect("poisoned start projection lookup should succeed")
            .expect("poisoned start projection should exist");
        let repaired_projection_context = poisoned_start.projection_context_json.clone();
        let healthy_start = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(healthy_turn_id))
            .filter(pioneer_entity::turn_event_projection_state::Column::Sequence.eq(1))
            .one(&store.connection)
            .await
            .expect("healthy start projection lookup should succeed")
            .expect("healthy start projection should exist");

        for (event_id, corrupt_context) in [
            (poisoned_start.event_id.as_str(), true),
            (healthy_start.event_id.as_str(), false),
        ] {
            let mut update = pioneer_entity::turn_event_projection_state::Entity::update_many()
                .col_expr(
                    pioneer_entity::turn_event_projection_state::Column::Status,
                    sea_orm::sea_query::Expr::value(
                        pioneer_crud_conversations::event_projection::PROJECTION_STATUS_FAILED,
                    ),
                )
                .col_expr(
                    pioneer_entity::turn_event_projection_state::Column::NextRunAt,
                    sea_orm::sea_query::Expr::value(unix_to_datetime(timestamp + 20)),
                )
                .col_expr(
                    pioneer_entity::turn_event_projection_state::Column::ClaimToken,
                    sea_orm::sea_query::Expr::value(Option::<String>::None),
                )
                .col_expr(
                    pioneer_entity::turn_event_projection_state::Column::ClaimExpiresAt,
                    sea_orm::sea_query::Expr::value(
                        Option::<sea_orm::entity::prelude::DateTimeWithTimeZone>::None,
                    ),
                )
                .col_expr(
                    pioneer_entity::turn_event_projection_state::Column::UpdatedAt,
                    sea_orm::sea_query::Expr::value(unix_to_datetime(timestamp + 2)),
                )
                .filter(pioneer_entity::turn_event_projection_state::Column::EventId.eq(event_id));
            if corrupt_context {
                update = update.col_expr(
                    pioneer_entity::turn_event_projection_state::Column::ProjectionContextJson,
                    sea_orm::sea_query::Expr::value(r#"{"enqueue_optional_deliveries":"invalid"}"#),
                );
            }
            update
                .exec(&store.connection)
                .await
                .expect("projection head should become replayable");
        }
        set_turn_projection_watermark(&store, poisoned_turn_id, 0).await;
        set_turn_projection_watermark(&store, healthy_turn_id, 0).await;

        for (thread_id, turn_id, item_id) in [
            (poisoned_thread_id, poisoned_turn_id, poisoned_item_id),
            (healthy_thread_id, healthy_turn_id, healthy_item_id),
        ] {
            let error = store
                .materialize_native_agent_turn_event(
                    CanonicalTurnEventPayload::ItemStarted(ItemStartedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item: safe_web_fetch_item(item_id),
                    }),
                    timestamp + 1,
                    None,
                )
                .await
                .expect_err("successor should wait behind its unprojected causal head");
            assert!(crate::turn_event_was_appended_before_error(&error));
        }

        let poisoned_raw_before = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(poisoned_turn_id))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("poisoned raw stream should query");
        assert_eq!(poisoned_raw_before.len(), 2);
        let poisoned_raw_ids_before = poisoned_raw_before
            .iter()
            .map(|event| event.id.clone())
            .collect::<Vec<_>>();
        let poisoned_successor_id = poisoned_raw_before[1].id.clone();

        let replay = store
            .replay_due_turn_event_projections(timestamp + 30, 64)
            .await
            .expect("independent projection streams should replay");
        assert_eq!(replay.claimed, 3, "{replay:?}");
        assert_eq!(replay.projected, 2);
        assert_eq!(replay.failed, 0);
        assert_eq!(replay.exhausted, 1);
        assert_eq!(replay.quarantined, 1);
        assert_eq!(replay.quarantined_streams.len(), 1);
        assert_eq!(
            replay.quarantined_streams[0].blocking_event_id,
            poisoned_start.event_id
        );

        let quarantined = store
            .get_turn_event_projection_stream_state(poisoned_turn_id)
            .await
            .expect("quarantine state should query")
            .expect("poisoned stream state should exist");
        assert_eq!(quarantined.health, TurnProjectionStreamHealth::Quarantined);
        assert_eq!(
            quarantined.blocking_event_id.as_deref(),
            Some(poisoned_start.event_id.as_str())
        );
        assert!(quarantined.last_error.is_some());
        assert_eq!(quarantined.quarantined_at_unix, Some(timestamp + 30));

        let poisoned_states = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(
                pioneer_entity::turn_event_projection_state::Column::TurnId.eq(poisoned_turn_id),
            )
            .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("poisoned projection states should query");
        assert_eq!(poisoned_states.len(), 2);
        assert_eq!(
            poisoned_states[0].status,
            pioneer_crud_conversations::event_projection::PROJECTION_STATUS_EXHAUSTED
        );
        assert_eq!(
            poisoned_states[1].status,
            pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PENDING
        );
        assert_eq!(poisoned_states[1].attempt_count, 0);
        assert!(poisoned_states[1].last_error.is_none());

        let healthy_states = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(healthy_turn_id))
            .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("healthy projection states should query");
        assert!(healthy_states.iter().all(|state| {
            state.status
                == pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PROJECTED
        }));
        assert!(
            pioneer_entity::turn_item::Entity::find()
                .filter(pioneer_entity::turn_item::Column::TurnId.eq(healthy_turn_id))
                .filter(pioneer_entity::turn_item::Column::ItemId.eq(healthy_item_id))
                .one(&store.connection)
                .await
                .expect("healthy item should query")
                .is_some(),
            "a quarantined stream must not block healthy streams"
        );

        let repeated = store
            .replay_due_turn_event_projections(timestamp + 40, 64)
            .await
            .expect("quarantined stream should be excluded idempotently");
        assert_eq!(repeated.claimed, 0);
        assert_eq!(repeated.quarantined, 0);
        assert_eq!(
            pioneer_entity::turn_event_projection_stream_state::Entity::find()
                .filter(
                    pioneer_entity::turn_event_projection_stream_state::Column::TurnId
                        .eq(poisoned_turn_id),
                )
                .count(&store.connection)
                .await
                .expect("quarantine row count should query"),
            1
        );

        pioneer_entity::turn_event_projection_state::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_event_projection_state::Column::ProjectionContextJson,
                sea_orm::sea_query::Expr::value(repaired_projection_context),
            )
            .filter(
                pioneer_entity::turn_event_projection_state::Column::EventId
                    .eq(poisoned_start.event_id.clone()),
            )
            .exec(&store.connection)
            .await
            .expect("operator should repair blocker payload before restoring stream");

        assert_eq!(
            store
                .restore_turn_event_projection_stream(
                    poisoned_turn_id,
                    poisoned_successor_id.as_str(),
                    timestamp + 50,
                )
                .await
                .expect("stale repair command should be rejected without mutation"),
            RestoreTurnProjectionStreamOutcome::BlockingEventMismatch
        );
        assert_eq!(
            store
                .restore_turn_event_projection_stream(
                    poisoned_turn_id,
                    poisoned_start.event_id.as_str(),
                    timestamp + 50,
                )
                .await
                .expect("matching operator repair should restore stream"),
            RestoreTurnProjectionStreamOutcome::Restored
        );
        assert_eq!(
            store
                .restore_turn_event_projection_stream(
                    poisoned_turn_id,
                    poisoned_start.event_id.as_str(),
                    timestamp + 51,
                )
                .await
                .expect("operator repair retry should be idempotent"),
            RestoreTurnProjectionStreamOutcome::AlreadyHealthy
        );

        let restored = store
            .get_turn_event_projection_stream_state(poisoned_turn_id)
            .await
            .expect("restored stream should query")
            .expect("restored stream state should exist");
        assert_eq!(restored.health, TurnProjectionStreamHealth::Healthy);
        assert!(restored.blocking_event_id.is_none());
        assert!(restored.last_error.is_none());
        assert!(restored.quarantined_at_unix.is_none());
        assert_eq!(restored.restored_at_unix, Some(timestamp + 50));

        let resumed = store
            .replay_due_turn_event_projections(timestamp + 60, 64)
            .await
            .expect("restored stream should resume from its repaired causal head");
        assert_eq!(resumed.claimed, 2);
        assert_eq!(resumed.projected, 2);
        assert_eq!(resumed.quarantined, 0);

        let poisoned_states = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(
                pioneer_entity::turn_event_projection_state::Column::TurnId.eq(poisoned_turn_id),
            )
            .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("restored projection states should query");
        assert!(poisoned_states.iter().all(|state| {
            state.status
                == pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PROJECTED
        }));

        let poisoned_raw_after = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(poisoned_turn_id))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&store.connection)
            .await
            .expect("restored raw stream should query");
        assert_eq!(
            poisoned_raw_after
                .iter()
                .map(|event| event.id.clone())
                .collect::<Vec<_>>(),
            poisoned_raw_ids_before,
            "quarantine and repair must preserve the raw log and its order"
        );
        assert!(
            pioneer_entity::turn_item::Entity::find()
                .filter(pioneer_entity::turn_item::Column::TurnId.eq(poisoned_turn_id))
                .filter(pioneer_entity::turn_item::Column::ItemId.eq(poisoned_item_id))
                .one(&store.connection)
                .await
                .expect("restored item should query")
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn terminal_turn_projection_rejects_late_turn_started_without_regression() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_terminal_start_fence";
        let store = postgres_store_with_workspace(fixture, workspace_id).await;
        let thread_id = "thr_terminal_start_fence";
        let turn_id = "turn_terminal_start_fence";
        let thread = sample_thread(workspace_id, thread_id, timestamp);
        let original_input = vec![UserInput::Text {
            text: "original input".to_owned(),
            text_elements: Vec::new(),
        }];
        let turn = sample_turn(turn_id);
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                original_input.as_slice(),
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("initial start should persist");
        let mut completed = turn.clone();
        completed.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: completed,
                },
                timestamp + 1,
            )
            .await
            .expect("terminal state should persist");

        let conflicting_input = vec![UserInput::Text {
            text: "must never replace the original".to_owned(),
            text_elements: Vec::new(),
        }];
        let error = store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                conflicting_input.as_slice(),
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect_err("late start must be fenced by terminal monotonicity");
        assert!(format!("{error:#}").contains("identity reuse"));

        let (_, persisted) = store
            .get_turn(thread_id, turn_id)
            .await
            .expect("turn lookup should succeed")
            .expect("turn should exist");
        assert_eq!(persisted.status, TurnStatus::Completed);
        let persisted_input = store
            .get_turn_inputs(turn_id)
            .await
            .expect("turn input lookup should succeed");
        assert_eq!(persisted_input, original_input);
        Ok(())
    })
    .await
    .unwrap();
}
