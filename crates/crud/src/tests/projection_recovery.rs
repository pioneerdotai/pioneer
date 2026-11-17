use super::*;
use futures_util::FutureExt;

#[tokio::test]
async fn keyed_replay_preserves_other_turns_and_the_captured_due_frontier() {
    let at = 1_700_000_000;
    let store = test_store_with_workspace("ws_progress")
        .await
        .with_maintenance_access();
    let mut turns = Vec::new();
    for index in 0..3 {
        let id = prepare_progress_finalization(&store, index, at).await;
        mark_first_turn_projection_failed(&store, &id, at + 5 + 20 * index as i64).await;
        turns.push(id);
    }

    // Even though wall time has advanced, this pass must not consume work
    // which became due after its captured frontier. Its clock must survive.
    let future = store
        .replay_turn_event_projection_frontier(at + 100, 1, Some(&turns[2..]), at + 10)
        .await
        .unwrap();
    assert_eq!(future.claimed, 0);
    assert_eq!(future.next_eligible_at_unix, Some(at + 45));

    // A full budget is not EOF. Repeated bounded calls can drain this captured
    // set without a new notification; another ready Turn stays untouched.
    for expected_remaining in [Some(at + 25), None] {
        let replay = store
            .replay_turn_event_projection_frontier(at + 100, 1, Some(&turns[..2]), at + 30)
            .await
            .unwrap();
        assert_eq!((replay.claimed, replay.projected), (1, 1));
        assert_eq!(replay.next_eligible_at_unix, expected_remaining);
    }
    let unrequested = store
        .get_turn_event_projection_stream_state(&turns[2])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unrequested.projected_through_sequence, 0);
    assert_eq!(
        store
            .replay_turn_event_projection_frontier(at + 100, 64, Some(&[]), at + 100)
            .await
            .unwrap()
            .claimed,
        0,
        "an empty keyed wake must never become global discovery"
    );

    // A fresh owner has no in-memory hint inventory. Full startup recovery
    // still finds the remaining obligation, and repeating it is idempotent.
    let restarted = CrudStore::new(store.database_connection()).with_maintenance_access();
    let recovered = restarted
        .replay_due_turn_event_projections(at + 100, 64)
        .await
        .unwrap();
    assert_eq!((recovered.claimed, recovered.projected), (1, 1));
    assert_eq!(recovered.next_eligible_at_unix, None);
    assert_eq!(
        restarted
            .replay_due_turn_event_projections(at + 100, 64)
            .await
            .unwrap()
            .claimed,
        0
    );
}

#[tokio::test]
async fn keyed_finalization_does_not_advance_an_independent_full_pass() {
    let at = 1_700_000_000;
    let store = test_store_with_workspace("ws_progress")
        .await
        .with_maintenance_access();
    let mut turns = Vec::new();
    for index in 0..3 {
        turns.push(prepare_progress_finalization(&store, index, at).await);
    }
    let first = store
        .reconcile_prepared_turn_finalizations(1, at + 10)
        .await
        .unwrap();
    assert!(!first.pass_complete);
    assert_eq!(first.committed[0].turn_completed.turn.id, turns[0]);
    let keyed = store
        .reconcile_prepared_turn_finalizations_for_turns(&turns[2..], at + 11)
        .await
        .unwrap();
    assert!(keyed.pass_complete && !keyed.pass_failed);
    assert_eq!(keyed.committed_count(), 1);
    assert_eq!(keyed.committed[0].turn_completed.turn.id, turns[2]);
    assert_eq!(
        store.list_prepared_turn_finalization_ids(10).await.unwrap(),
        vec![turns[1].clone()],
        "ready but unrequested work remains for its own wake or the full pass"
    );
    let next = store
        .reconcile_prepared_turn_finalizations(1, at + 12)
        .await
        .unwrap();
    assert_eq!(next.committed[0].turn_completed.turn.id, turns[1]);
    assert!(
        store
            .reconcile_prepared_turn_finalizations(1, at + 13)
            .await
            .unwrap()
            .pass_complete
    );
    let repeat = store
        .reconcile_prepared_turn_finalizations_for_turns(&turns, at + 14)
        .await
        .unwrap();
    assert_eq!(repeat.scanned, 0);
    assert!(repeat.pass_complete && !repeat.pass_failed);
}

#[tokio::test]
async fn event_wakes_are_keyed_delivery_claims_stay_quiet_and_acks_wake_delivery() {
    let (store, thread, turn) = test_store_with_started_turn(
        "ws_projection_wakes",
        "thr_projection_wakes",
        "turn_projection_wakes",
    )
    .await;
    let database = store.database_connection();
    let mut changes = database.work_signal("projection").subscribe();
    assert!(changes.begin_keyed_pass(&database).await.unwrap().is_none());

    append_optional_delivery_test_items(&store, &thread, &turn.id, 1).await;
    assert_eq!(
        changes.begin_keyed_pass(&database).await.unwrap(),
        Some(vec![format!("turn:{}", turn.id)])
    );
    let claims = store
        .with_maintenance_access()
        .claim_due_turn_event_deliveries(crate::NATIVE_TURN_EVENT_LIVE_CONSUMER, 1_700_000_200, 1)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        changes.begin_keyed_pass(&database).await.unwrap(),
        Some(vec![]),
        "claiming a lease creates no new delivery or projection work"
    );
    let settlements = [TurnEventDeliverySettlement {
        id: claims[0].id.clone(),
        claim_token: claims[0].claim_token.clone(),
        at_unix: 1_700_000_200,
        outcome: TurnEventDeliveryOutcome::Delivered,
    }];
    assert_eq!(
        store
            .settle_turn_event_deliveries(&settlements)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        changes.begin_keyed_pass(&database).await.unwrap(),
        Some(vec!["delivery".into()])
    );
    assert_eq!(
        store
            .settle_turn_event_deliveries(&settlements)
            .await
            .unwrap(),
        0
    );
    assert!(changes.wait(None).now_or_never().is_none());

    // Hints may never hide an uncertain late write, even if its intended
    // successful publication would have affected only delivery.
    drop(
        database
            .work_signal("projection")
            .changing()
            .with_keys(&["delivery".into()]),
    );
    assert!(changes.begin_keyed_pass(&database).await.unwrap().is_none());
}
