use super::*;

#[tokio::test]
async fn native_execution_window_transition_rejects_gaps_and_rolls_back_invalid_checkpoint() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_native_window",
            "thr_native_window",
            "turn_native_window",
        )
        .await;
        use futures_util::FutureExt;
        let database = store.database_connection();
        let mut consumers = ["gateway.control", "projection"]
            .map(|domain| database.work_signal(domain).subscribe());
        for consumer in &mut consumers {
            consumer.begin_pass(&database).await.unwrap();
        }
        let timestamp = unix_to_datetime(1_700_000_100);
        let started = TurnExecutionWindowStartedNotification {
            workspace_id: "ws_native_window".to_owned(),
            thread_id: "thr_native_window".to_owned(),
            turn_id: "turn_native_window".to_owned(),
            window_id: "runtime_window_1".to_owned(),
            window_index: 1,
            status: ExecutionWindowStatus::Running,
            started_at_unix_ms: 1_700_000_100_000,
        };
        store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Started {
                    notification: started.clone(),
                    window: NewTurnExecutionWindowRecord {
                        workspace_id: started.workspace_id.clone(),
                        thread_id: started.thread_id.clone(),
                        turn_id: started.turn_id.clone(),
                        window_index: 1,
                        status: ExecutionWindowStatus::Running,
                        exhaustion_reason: None,
                        agent_round_count: 0,
                        tool_call_count: 0,
                        provider_token_count: 0,
                        metadata_json: serde_json::json!({"runtimeWindowId": "runtime_window_1"}),
                        started_at: timestamp,
                    },
                    created_at: timestamp,
                    updated_at: timestamp,
                },
                1_700_000_100,
            )
            .await
            .expect("first execution window should commit atomically");
        for consumer in &mut consumers {
            assert!(
                consumer.changed().now_or_never().is_some(),
                "window commit wakes durable event consumers without caller dispatch"
            );
            consumer.begin_pass(&database).await.unwrap();
        }
        store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Started {
                    notification: started.clone(),
                    window: NewTurnExecutionWindowRecord {
                        workspace_id: started.workspace_id.clone(),
                        thread_id: started.thread_id.clone(),
                        turn_id: started.turn_id.clone(),
                        window_index: 1,
                        status: ExecutionWindowStatus::Running,
                        exhaustion_reason: None,
                        agent_round_count: 0,
                        tool_call_count: 0,
                        provider_token_count: 0,
                        metadata_json: serde_json::json!({"runtimeWindowId": "runtime_window_1"}),
                        started_at: timestamp,
                    },
                    created_at: timestamp,
                    updated_at: timestamp,
                },
                1_700_000_100,
            )
            .await
            .expect("exact start replay should be idempotent");
        for consumer in &mut consumers {
            assert!(
                consumer.changed().now_or_never().is_none(),
                "stable event replay has no new work"
            );
        }

        let uncontinued_started = TurnExecutionWindowStartedNotification {
            window_id: "runtime_window_2_without_continuation".to_owned(),
            window_index: 2,
            started_at_unix_ms: 1_700_000_101_000,
            ..started.clone()
        };
        let uncontinued = store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Started {
                    notification: uncontinued_started.clone(),
                    window: NewTurnExecutionWindowRecord {
                        workspace_id: uncontinued_started.workspace_id.clone(),
                        thread_id: uncontinued_started.thread_id.clone(),
                        turn_id: uncontinued_started.turn_id.clone(),
                        window_index: uncontinued_started.window_index,
                        status: ExecutionWindowStatus::Running,
                        exhaustion_reason: None,
                        agent_round_count: 0,
                        tool_call_count: 0,
                        provider_token_count: 0,
                        metadata_json: serde_json::json!({
                            "runtimeWindowId": uncontinued_started.window_id,
                        }),
                        started_at: timestamp + chrono::Duration::seconds(1),
                    },
                    created_at: timestamp + chrono::Duration::seconds(1),
                    updated_at: timestamp + chrono::Duration::seconds(1),
                },
                1_700_000_101,
            )
            .await;
        assert!(
            uncontinued.is_err(),
            "a later window must be created by a committed continuation"
        );
        assert_eq!(
            store
                .latest_turn_execution_window("turn_native_window")
                .await
                .expect("window should reload")
                .expect("first window should remain")
                .window_index,
            1
        );

        let exhausted = TurnExecutionWindowExhaustedNotification {
            workspace_id: started.workspace_id.clone(),
            thread_id: started.thread_id.clone(),
            turn_id: started.turn_id.clone(),
            window_id: started.window_id.clone(),
            window_index: 1,
            status: ExecutionWindowStatus::Exhausted,
            exhaustion_reason: ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
            limit: 4,
            observed: 4,
            agent_round_count: 2,
            tool_call_count: 4,
            provider_token_count: Some(100),
            started_at_unix_ms: started.started_at_unix_ms,
            exhausted_at_unix_ms: 1_700_000_110_000,
            reason: "max_tool_calls".to_owned(),
        };
        let stats = TurnExecutionWindowStatsRecord {
            agent_round_count: 2,
            tool_call_count: 4,
            provider_token_count: 100,
            metadata_json: serde_json::json!({"runtimeWindowId": "runtime_window_1"}),
            completed_at: timestamp + chrono::Duration::seconds(10),
            updated_at: timestamp + chrono::Duration::seconds(10),
        };
        store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Exhausted {
                    notification: exhausted,
                    stats,
                },
                1_700_000_110,
            )
            .await
            .expect("exhaustion should commit");
        let window = store
            .latest_turn_execution_window("turn_native_window")
            .await
            .expect("window should load")
            .expect("window should exist");
        assert_eq!(window.status, ExecutionWindowStatus::Exhausted);

        let oversized_payload = serde_json::json!({
            "payload": "x".repeat(TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES + 1)
        });
        let checkpoint_notification = TurnExecutionWindowCheckpointedNotification {
            workspace_id: started.workspace_id.clone(),
            thread_id: started.thread_id.clone(),
            turn_id: started.turn_id.clone(),
            window_id: started.window_id.clone(),
            window_index: 1,
            status: ExecutionWindowStatus::Checkpointed,
            checkpoint_id: "native_checkpoint_1".to_owned(),
            checkpoint_kind: "window_exhausted".to_owned(),
            payload_bytes: u64::MAX,
            created_at_unix_ms: 1_700_000_111_000,
        };
        let oversized = store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Checkpointed {
                    notification: checkpoint_notification.clone(),
                    checkpoint: NewTurnExecutionCheckpointRecord {
                        id: Some("native_checkpoint_1".to_owned()),
                        window_id: String::new(),
                        workspace_id: started.workspace_id.clone(),
                        thread_id: started.thread_id.clone(),
                        turn_id: started.turn_id.clone(),
                        checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                        payload_json: oversized_payload,
                        created_at: timestamp + chrono::Duration::seconds(11),
                    },
                },
                1_700_000_111,
            )
            .await;
        assert!(oversized.is_err());
        assert_eq!(
            store
                .latest_turn_execution_window("turn_native_window")
                .await
                .expect("window should reload")
                .expect("window should remain")
                .status,
            ExecutionWindowStatus::Exhausted,
            "failed checkpoint transaction must not advance authoritative status"
        );

        let payload = serde_json::json!({"window": 1, "state": "durable"});
        let mut valid_checkpoint_notification = checkpoint_notification;
        valid_checkpoint_notification.payload_bytes = u64::try_from(
            serde_json::to_vec(&payload)
                .expect("checkpoint payload should encode")
                .len(),
        )
        .expect("checkpoint payload length should fit u64");
        store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Checkpointed {
                    notification: valid_checkpoint_notification.clone(),
                    checkpoint: NewTurnExecutionCheckpointRecord {
                        id: Some("native_checkpoint_1".to_owned()),
                        window_id: String::new(),
                        workspace_id: started.workspace_id.clone(),
                        thread_id: started.thread_id.clone(),
                        turn_id: started.turn_id.clone(),
                        checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                        payload_json: payload,
                        created_at: timestamp + chrono::Duration::seconds(11),
                    },
                },
                1_700_000_111,
            )
            .await
            .expect("valid checkpoint should commit");

        let same_identity_conflicting_payload =
            serde_json::json!({"window": 1, "state": "same-id-conflict"});
        let same_identity_conflicting_notification = TurnExecutionWindowCheckpointedNotification {
            payload_bytes: u64::try_from(
                serde_json::to_vec(&same_identity_conflicting_payload)
                    .expect("same-identity conflicting payload should encode")
                    .len(),
            )
            .expect("checkpoint payload length should fit u64"),
            ..valid_checkpoint_notification.clone()
        };
        let same_identity_conflict = store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Checkpointed {
                    notification: same_identity_conflicting_notification,
                    checkpoint: NewTurnExecutionCheckpointRecord {
                        id: Some("native_checkpoint_1".to_owned()),
                        window_id: String::new(),
                        workspace_id: started.workspace_id.clone(),
                        thread_id: started.thread_id.clone(),
                        turn_id: started.turn_id.clone(),
                        checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                        payload_json: same_identity_conflicting_payload,
                        created_at: timestamp + chrono::Duration::seconds(11),
                    },
                },
                1_700_000_111,
            )
            .await;
        assert!(
            same_identity_conflict.is_err(),
            "an idempotency-key replay must match the exact committed payload"
        );

        let conflicting_payload = serde_json::json!({"window": 1, "state": "conflict"});
        let conflicting_checkpoint_notification = TurnExecutionWindowCheckpointedNotification {
            checkpoint_id: "native_checkpoint_2".to_owned(),
            payload_bytes: u64::try_from(
                serde_json::to_vec(&conflicting_payload)
                    .expect("conflicting checkpoint payload should encode")
                    .len(),
            )
            .expect("checkpoint payload length should fit u64"),
            created_at_unix_ms: 1_700_000_111_500,
            ..valid_checkpoint_notification
        };
        let conflicting_checkpoint = store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Checkpointed {
                    notification: conflicting_checkpoint_notification,
                    checkpoint: NewTurnExecutionCheckpointRecord {
                        id: Some("native_checkpoint_2".to_owned()),
                        window_id: String::new(),
                        workspace_id: started.workspace_id.clone(),
                        thread_id: started.thread_id.clone(),
                        turn_id: started.turn_id.clone(),
                        checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                        payload_json: conflicting_payload,
                        created_at: timestamp + chrono::Duration::milliseconds(11_500),
                    },
                },
                1_700_000_111,
            )
            .await;
        assert!(
            conflicting_checkpoint.is_err(),
            "a checkpointed window must reject a second checkpoint identity"
        );
        let checkpointed_window = store
            .latest_turn_execution_window("turn_native_window")
            .await
            .expect("window should reload")
            .expect("window should exist");
        assert_eq!(
            store
                .list_turn_execution_checkpoints_for_window(checkpointed_window.id.as_str())
                .await
                .expect("checkpoints should list")
                .len(),
            1
        );
        let continued = TurnExecutionWindowContinuedNotification {
            workspace_id: started.workspace_id,
            thread_id: started.thread_id,
            turn_id: started.turn_id,
            window_id: "runtime_window_2".to_owned(),
            window_index: 2,
            status: ExecutionWindowStatus::Continued,
            previous_window_id: started.window_id,
            previous_window_index: 1,
            checkpoint_id: "native_checkpoint_1".to_owned(),
            continued_at_unix_ms: 1_700_000_112_000,
        };
        store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Continued {
                    notification: continued.clone(),
                    updated_at: timestamp + chrono::Duration::seconds(12),
                },
                1_700_000_112,
            )
            .await
            .expect("continuation with exact checkpoint should commit");
        store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Continued {
                    notification: continued.clone(),
                    updated_at: timestamp + chrono::Duration::seconds(12),
                },
                1_700_000_112,
            )
            .await
            .expect("exact continuation replay should be idempotent");
        let next_window = store
            .latest_turn_execution_window("turn_native_window")
            .await
            .expect("window should load")
            .expect("window should exist");
        assert_eq!(next_window.window_index, 2);
        assert_eq!(next_window.status, ExecutionWindowStatus::Running);
        assert_eq!(
            next_window
                .metadata_json
                .get("createdByContinuationCheckpointId")
                .and_then(serde_json::Value::as_str),
            Some("native_checkpoint_1")
        );

        let second_started = TurnExecutionWindowStartedNotification {
            workspace_id: continued.workspace_id.clone(),
            thread_id: continued.thread_id.clone(),
            turn_id: continued.turn_id.clone(),
            window_id: continued.window_id.clone(),
            window_index: continued.window_index,
            status: ExecutionWindowStatus::Running,
            started_at_unix_ms: continued.continued_at_unix_ms,
        };
        let start = NativeExecutionWindowTransition::Started {
            notification: second_started.clone(),
            window: NewTurnExecutionWindowRecord {
                workspace_id: second_started.workspace_id.clone(),
                thread_id: second_started.thread_id.clone(),
                turn_id: second_started.turn_id.clone(),
                window_index: second_started.window_index,
                status: ExecutionWindowStatus::Running,
                exhaustion_reason: None,
                agent_round_count: 0,
                tool_call_count: 0,
                provider_token_count: 0,
                metadata_json: serde_json::json!({"runtimeWindowId": second_started.window_id}),
                started_at: timestamp + chrono::Duration::seconds(12),
            },
            created_at: timestamp + chrono::Duration::seconds(12),
            updated_at: timestamp + chrono::Duration::seconds(12),
        };
        // Pause a real durable append before the projection worker applies it.
        let predecessor = store
            .append_claimed_turn_event_projection_once(
                crate::events::TurnEventPayload::ItemStarted(ItemStartedNotification {
                    workspace_id: second_started.workspace_id.clone(),
                    thread_id: second_started.thread_id.clone(),
                    turn_id: second_started.turn_id.clone(),
                    item: TurnItem::Reasoning {
                        id: "resumed_reasoning".to_owned(),
                        content: Vec::new(),
                        summary: Vec::new(),
                    },
                }),
                unix_to_datetime(1_700_000_113),
                crate::store::events::event_preparation::TurnEventProjectionContext {
                    item_started_deadlines: None,
                    enqueue_optional_deliveries: true,
                    cancellation_actor_id: None,
                    native_cancellation: None,
                    unactivated_terminal: None,
                },
                "resumed_reasoning_projection".to_owned(),
                &|| unix_to_datetime(1_700_000_114),
                None,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let pending = store
            .materialize_native_execution_window_transition(start.clone(), 1_700_000_113)
            .await
            .expect_err("the continued window start must wait for the earlier event");
        assert!(pending.is::<crate::ExecutionWindowStartProjectionPending>());
        assert_eq!(
            pioneer_crud_conversations::turn_event::max_sequence(&database, &second_started.turn_id)
                .await
                .unwrap(),
            predecessor.sequence,
        );
        let replay = store
            .replay_due_turn_event_projections(1_700_000_115, 1)
            .await
            .unwrap();
        assert_eq!(replay.projected, 1);
        for now in [1_700_000_115, 1_700_000_116] {
            store
                .materialize_native_execution_window_transition(start.clone(), now)
                .await
                .expect("the same window start should commit after its predecessor");
        }
        assert_eq!(
            pioneer_crud_conversations::turn_event::max_sequence(&database, &second_started.turn_id)
                .await
                .unwrap(),
            predecessor.sequence + 1,
        );

        let future_started = TurnExecutionWindowStartedNotification {
            window_id: "runtime_window_4".to_owned(),
            window_index: 4,
            started_at_unix_ms: 1_700_000_114_000,
            ..second_started
        };
        let gap = store
            .materialize_native_execution_window_transition(
                NativeExecutionWindowTransition::Started {
                    notification: future_started.clone(),
                    window: NewTurnExecutionWindowRecord {
                        workspace_id: future_started.workspace_id.clone(),
                        thread_id: future_started.thread_id.clone(),
                        turn_id: future_started.turn_id.clone(),
                        window_index: future_started.window_index,
                        status: ExecutionWindowStatus::Running,
                        exhaustion_reason: None,
                        agent_round_count: 0,
                        tool_call_count: 0,
                        provider_token_count: 0,
                        metadata_json: serde_json::json!({
                            "runtimeWindowId": future_started.window_id,
                        }),
                        started_at: timestamp + chrono::Duration::seconds(14),
                    },
                    created_at: timestamp + chrono::Duration::seconds(14),
                    updated_at: timestamp + chrono::Duration::seconds(14),
                },
                1_700_000_114,
            )
            .await;
        assert!(
            gap.is_err(),
            "future window gaps must be negatively acknowledged"
        );
        assert_eq!(
            store
                .latest_turn_execution_window("turn_native_window")
                .await
                .expect("window should reload")
                .expect("latest window should remain")
                .window_index,
            2
        );
        Ok(())
    })
    .await
    .unwrap();
}
