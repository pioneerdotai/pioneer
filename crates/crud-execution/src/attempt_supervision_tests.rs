use super::*;
use crate::test_fixture::{fixture_event_scope, safe_web_fetch_item};
use pioneer_crud_conversations::turn_item::{
    prepare_turn_item_projection, upsert_prepared_turn_item,
};
use pioneer_protocol::{ToolCallStatus, TurnItem, generate_id};
use pioneer_sqlite::SqliteDatabase;

async fn start_item(
    db: &SqliteDatabase,
    turn_id: &str,
    item: TurnItem,
    started_at: i64,
) -> Result<()> {
    let at = unix_to_datetime(started_at);
    let projection =
        prepare_turn_item_projection(turn_id, &item, Some(TURN_ITEM_STATUS_IN_PROGRESS), at, at)?;
    let payload = projection.payload_json().to_owned();
    let tx = db.begin().await?;
    upsert_prepared_turn_item(&tx, projection).await?;
    turn_item_attempt::create_running_attempt(
        &tx,
        generate_id(21),
        turn_id,
        item.item_id(),
        item.item_type(),
        item.execution_class(),
        payload,
        turn_item_attempt::AttemptDeadlines {
            lease_expires_at: None,
            idle_deadline_at: None,
            hard_deadline_at: None,
        },
        at,
        Some(1),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn running_attempt_pages_continue_after_selected_attempt_disappears() {
    let timestamp = 1_700_000_000;
    let workspace_id = "ws_running_pages";
    let thread_id = "thr_running_pages";
    let turn_id = "turn_running_pages";
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture_event_scope(&fixture, workspace_id, thread_id, turn_id).await;
    let store = AttemptSupervision::new(&db);
    for item_id in ["page-a", "page-b", "page-c"] {
        start_item(&db, turn_id, safe_web_fetch_item(item_id), timestamp + 1)
            .await
            .unwrap();
    }
    let (first, cursor) = store
        .list_running_turn_item_attempt_page_by_type(TurnItemType::WebFetch, None, 2)
        .await
        .unwrap();
    assert_eq!(first.len(), 2);
    let cursor = cursor.expect("full page must retain continuation");
    // Removing a selected row must not shift an OFFSET and hide the tail.
    let removed = &first[0];
    store
        .configure_turn_item_attempt_deadlines(
            turn_id,
            &removed.item_id,
            timestamp + 1,
            Some(timestamp + 2),
            Some(timestamp + 2),
            Some(timestamp + 2),
        )
        .await
        .unwrap();
    let candidates = store
        .list_timeout_candidates(timestamp + 3, 8)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert!(
        store
            .transition_timeout_candidate(&candidates[0], timestamp + 3)
            .await
            .unwrap()
    );
    let (tail, next) = store
        .list_running_turn_item_attempt_page_by_type(TurnItemType::WebFetch, Some(&cursor), 2)
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert!(next.is_none());
    let mut seen = first
        .into_iter()
        .map(|item| item.item_id)
        .collect::<Vec<_>>();
    seen.extend(tail.into_iter().map(|item| item.item_id));
    seen.sort();
    assert_eq!(seen, ["page-a", "page-b", "page-c"]);
}

#[pioneer_postgres_test::postgres_test]
async fn timeout_pages_continue_past_unchanged_due_inputs_and_removed_candidates() {
    let at = 1_700_000_000;
    let turn_id = "turn_timeout_pages";
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture_event_scope(&fixture, "ws_timeout_pages", "thr_timeout_pages", turn_id).await;
    let store = AttemptSupervision::new(&db);
    for item in ["timeout-page-a", "timeout-page-b", "timeout-page-c"] {
        start_item(&db, turn_id, safe_web_fetch_item(item), at + 1)
            .await
            .unwrap();
        store
            .configure_turn_item_attempt_deadlines(
                turn_id,
                item,
                at + 1,
                Some(at + 2),
                Some(at + 2),
                Some(at + 2),
            )
            .await
            .unwrap();
    }
    let (first, cursor) = store
        .list_timeout_candidate_page(at + 3, 2, None)
        .await
        .unwrap();
    assert_eq!(first.len(), 2);
    let cursor = cursor.expect("a full raw page retains continuation without emitted events");
    // One row remains due (e.g. human wait); another disappears after the
    // point CAS. Neither can shift the frontier and hide the final input.
    assert!(
        store
            .transition_timeout_candidate(&first[0], at + 3)
            .await
            .unwrap()
    );
    // A later suffix becomes due while the original pass is in progress.
    // It belongs to the next pass, even if discovery uses a later clock.
    start_item(&db, turn_id, safe_web_fetch_item("timeout-page-d"), at + 4)
        .await
        .unwrap();
    store
        .configure_turn_item_attempt_deadlines(
            turn_id,
            "timeout-page-d",
            at + 4,
            Some(at + 5),
            Some(at + 5),
            Some(at + 5),
        )
        .await
        .unwrap();
    let (last, end) = store
        .list_timeout_candidate_page(at + 6, 2, Some(&cursor))
        .await
        .unwrap();
    assert_eq!(last.len(), 1);
    assert!(end.is_none());
    let mut seen = first
        .into_iter()
        .chain(last)
        .map(|row| row.item_id)
        .collect::<Vec<_>>();
    seen.sort();
    assert_eq!(seen, ["timeout-page-a", "timeout-page-b", "timeout-page-c"]);
    let (fresh, _) = store
        .list_timeout_candidate_page(at + 6, 8, None)
        .await
        .unwrap();
    assert!(fresh.iter().any(|row| row.item_id == "timeout-page-d"));
}

#[tokio::test]
async fn timeout_transition_rejects_candidate_stale_after_heartbeat() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_000_000;
        let turn_id = "turn_timeout_stale_candidate";
        let item_id = "item_timeout_stale_candidate";
        let db = fixture_event_scope(
            fixture,
            "ws_timeout_stale_candidate",
            "thr_timeout_stale_candidate",
            turn_id,
        )
        .await;
        let store = AttemptSupervision::new(&db);
        start_item(&db, turn_id, safe_web_fetch_item(item_id), timestamp + 1)
            .await
            .expect("item start should persist");
        store
            .configure_turn_item_attempt_deadlines(
                turn_id,
                item_id,
                timestamp + 1,
                Some(timestamp + 2),
                Some(timestamp + 2),
                Some(timestamp + 2),
            )
            .await
            .expect("expired deadlines should be configured");

        let candidates = store
            .list_timeout_candidates(timestamp + 3, 8)
            .await
            .expect("timeout candidate list should succeed");
        assert_eq!(candidates.len(), 1);

        assert!(
            store
                .heartbeat_turn_item_attempt(
                    turn_id,
                    item_id,
                    TurnItemType::WebFetch,
                    timestamp + 3,
                    Some(timestamp + 103),
                    Some(timestamp + 103),
                    Some(timestamp + 103),
                )
                .await
                .expect("heartbeat should move every deadline")
        );
        assert!(
            !store
                .transition_timeout_candidate(&candidates[0], timestamp + 3)
                .await
                .expect("stale timeout transition should be rejected"),
            "a timeout candidate must not survive a newer heartbeat frontier"
        );

        let row = pioneer_crud_conversations::turn_reads::find_turn_item(&db, turn_id, item_id)
            .await
            .expect("turn_item lookup should succeed")
            .expect("turn_item row should exist");
        assert_eq!(row.status.as_deref(), Some("in_progress"));
        assert_eq!(row.active_attempt_status.as_deref(), Some("running"));
        let payload: TurnItem = pioneer_crud_conversations::turn_payload::decode_item(&row)
            .expect("payload should decode");
        assert!(matches!(
            payload,
            TurnItem::WebFetch {
                status: ToolCallStatus::InProgress,
                ..
            }
        ));
        Ok(())
    })
    .await
    .unwrap();
}

async fn append_liveness_fixture_start(
    db: &SqliteDatabase,
    _workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    at: i64,
) -> Result<()> {
    use pioneer_crud_conversations::{
        events::{TurnEventPayload, TurnStartedEventPayload},
        protocol_projection::thread_from_db_model,
        thread_metadata::find_thread_by_id,
        turn_reads::get_turn_for_thread,
    };
    let row = find_thread_by_id(db, thread_id)
        .await?
        .expect("fixture thread");
    let thread = thread_from_db_model(row)?.expect("decodable thread");
    let (_, turn) = get_turn_for_thread(db, thread_id, turn_id)
        .await?
        .expect("fixture turn");
    let work_owner = turn.work_owner;
    crate::event_append::append_event(
        db,
        &TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread,
            sandbox_mode: pioneer_protocol::SandboxMode::FullAccess,
            turn,
            input: Vec::new(),
            actor: Some(pioneer_protocol::PersistedActorRef::System),
            reasoning_effort: None,
            work_owner,
        }),
        unix_to_datetime(at),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn turn_liveness_tracks_confirmed_external_activity_without_new_turn_event() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_turn_liveness_heartbeat";
        let thread_id = "thr_turn_liveness_heartbeat";
        let turn_id = "turn_turn_liveness_heartbeat";
        let item_id = "reasoning_heartbeat_liveness";
        let db = fixture_event_scope(fixture, workspace_id, thread_id, turn_id).await;
        append_liveness_fixture_start(&db, workspace_id, thread_id, turn_id, timestamp).await?;
        let store = AttemptSupervision::new(&db);

        let item = TurnItem::Reasoning {
            id: item_id.to_owned(),
            summary: Vec::new(),
            content: Vec::new(),
        };
        start_item(&db, turn_id, item.clone(), timestamp + 1).await?;
        crate::event_append::append_event(
            &db,
            &pioneer_crud_conversations::events::TurnEventPayload::ItemStarted(
                pioneer_protocol::ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item,
                },
            ),
            unix_to_datetime(timestamp + 1),
        )
        .await?;

        assert!(
            store
                .heartbeat_turn_item_attempt_with_activity_kind(
                    turn_id,
                    item_id,
                    TurnItemType::Reasoning,
                    timestamp + 5,
                    Some(timestamp + 905),
                    Some(timestamp + 905),
                    None,
                    "item/confirmed_external_activity",
                )
                .await
                .expect("heartbeat should persist")
        );

        let liveness = store
            .get_turn_liveness(turn_id)
            .await
            .expect("liveness query should succeed")
            .expect("liveness row should exist");
        assert_eq!(liveness.last_activity_sequence, 2);
        assert_eq!(
            liveness.last_activity_kind,
            "item/confirmed_external_activity"
        );
        assert_eq!(liveness.last_activity_item_id.as_deref(), Some(item_id));
        assert_eq!(
            liveness.last_activity_item_type.as_deref(),
            Some("reasoning")
        );
        assert_eq!(liveness.last_activity_at_unix, timestamp + 5);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn turn_liveness_tracks_authoritative_runtime_observation_without_item() {
    pioneer_postgres_test::with_database(async |fixture| {
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_turn_runtime_observation";
        let thread_id = "thr_turn_runtime_observation";
        let turn_id = "turn_runtime_observation";
        let db = fixture_event_scope(fixture, workspace_id, thread_id, turn_id).await;
        append_liveness_fixture_start(&db, workspace_id, thread_id, turn_id, timestamp).await?;
        let store = AttemptSupervision::new(&db);

        assert!(
            store
                .observe_turn_runtime_activity(
                    turn_id,
                    "runtime/observed_in_progress",
                    timestamp + 30,
                )
                .await
                .expect("runtime observation should persist")
        );
        let liveness = store
            .get_turn_liveness(turn_id)
            .await
            .expect("liveness query should succeed")
            .expect("liveness row should exist");
        assert_eq!(liveness.thread_id, thread_id);
        assert_eq!(liveness.last_activity_kind, "runtime/observed_in_progress");
        assert_eq!(liveness.last_activity_item_id, None);
        assert_eq!(liveness.last_activity_item_type, None);
        assert_eq!(liveness.last_activity_at_unix, timestamp + 30);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn turn_liveness_orders_activity_by_time_then_sequence() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let turn_id = "turn_liveness_ordering";
        let thread_id = "thr_liveness_ordering";
        let timestamp = 1_700_000_000;

        assert!(
            turn_liveness::observe_activity(
                &connection,
                turn_liveness::TurnLivenessObservation {
                    turn_id: turn_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    activity_sequence: 2,
                    activity_kind: "item/heartbeat".to_owned(),
                    item_id: Some("item_later_clock".to_owned()),
                    item_type: Some("reasoning".to_owned()),
                    observed_at: unix_to_datetime(timestamp + 10),
                },
            )
            .await
            .expect("initial liveness observation should persist")
        );

        assert!(
            !turn_liveness::observe_activity(
                &connection,
                turn_liveness::TurnLivenessObservation {
                    turn_id: turn_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    activity_sequence: 3,
                    activity_kind: "item/completed".to_owned(),
                    item_id: Some("item_older_clock".to_owned()),
                    item_type: Some("agent_message".to_owned()),
                    observed_at: unix_to_datetime(timestamp + 5),
                },
            )
            .await
            .expect("older liveness observation should be ignored")
        );

        let db = SqliteDatabase::from(connection);
        let store = AttemptSupervision::new(&db);
        let liveness = store
            .get_turn_liveness(turn_id)
            .await
            .expect("liveness query should succeed")
            .expect("liveness row should exist");
        assert_eq!(liveness.last_activity_sequence, 2);
        assert_eq!(liveness.last_activity_kind, "item/heartbeat");
        assert_eq!(liveness.last_activity_at_unix, timestamp + 10);

        assert!(
            turn_liveness::observe_activity(
                &db,
                turn_liveness::TurnLivenessObservation {
                    turn_id: turn_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    activity_sequence: 4,
                    activity_kind: "item/updated".to_owned(),
                    item_id: Some("item_same_clock".to_owned()),
                    item_type: Some("agent_message".to_owned()),
                    observed_at: unix_to_datetime(timestamp + 10),
                },
            )
            .await
            .expect("same-time higher sequence should update liveness")
        );

        let liveness = store
            .get_turn_liveness(turn_id)
            .await
            .expect("liveness query should succeed")
            .expect("liveness row should exist");
        assert_eq!(liveness.last_activity_sequence, 4);
        assert_eq!(liveness.last_activity_kind, "item/updated");
        assert_eq!(liveness.last_activity_at_unix, timestamp + 10);
        Ok(())
    })
    .await
    .unwrap();
}
