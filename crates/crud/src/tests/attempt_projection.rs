use super::*;

#[tokio::test]
async fn item_snapshot_update_does_not_append_turn_event_but_surfaces_in_turn_items() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_diff_snapshot";
        let thread_id = "thr_diff_snapshot";
        let turn_id = "turn_diff_snapshot";
        let timestamp = 1_700_000_000;
        let store = postgres_store_with_workspace(fixture, workspace_id).await;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            work_owner: Default::default(),
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
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

        let first_diff = TurnItem::SystemEvent {
            id: "agent_diff_native_turn".to_owned(),
            level: SystemEventLevel::Info,
            message: "Diff updated".to_owned(),
            code: Some("agent_diff_updated".to_owned()),
            details: Some(serde_json::json!({"payload": "first diff"})),
        };
        store
            .materialize_item_snapshot_updated(
                ItemUpdatedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: first_diff,
                },
                timestamp + 1,
            )
            .await
            .expect("first snapshot should persist");

        let second_diff = TurnItem::SystemEvent {
            id: "agent_diff_native_turn".to_owned(),
            level: SystemEventLevel::Info,
            message: "Diff updated".to_owned(),
            code: Some("agent_diff_updated".to_owned()),
            details: Some(serde_json::json!({"payload": "second diff"})),
        };
        store
            .materialize_item_snapshot_updated(
                ItemUpdatedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: second_diff,
                },
                timestamp + 2,
            )
            .await
            .expect("second snapshot should persist");

        let raw_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .all(&store.connection)
            .await
            .expect("raw events should query");
        assert_eq!(
            raw_events.len(),
            1,
            "snapshot updates must not append extra turn_event rows"
        );

        let response = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items should load")
            .expect("turn should exist");
        assert_eq!(response.events.len(), 1);
        let TurnItemEventPayload::ItemUpdated { item, .. } = &response.events[0].payload else {
            panic!("snapshot should surface as item/updated");
        };
        let TurnItem::SystemEvent { details, .. } = item else {
            panic!("snapshot should be a system event");
        };
        assert_eq!(
            details.as_ref().and_then(|details| details.get("payload")),
            Some(&serde_json::json!("second diff"))
        );
        assert_eq!(response.last_sequence, raw_events[0].sequence);

        let liveness = store
            .get_turn_liveness(turn_id)
            .await
            .expect("liveness query should succeed")
            .expect("liveness row should exist");
        assert_eq!(liveness.last_activity_sequence, 1);
        assert_eq!(liveness.last_activity_kind, "item/snapshot_updated");
        assert_eq!(
            liveness.last_activity_item_id.as_deref(),
            Some("agent_diff_native_turn")
        );
        assert_eq!(
            liveness.last_activity_item_type.as_deref(),
            Some("system_event")
        );
        assert_eq!(liveness.last_activity_at_unix, timestamp + 2);

        let current_diff = store
            .get_turn_item(turn_id, "agent_diff_native_turn")
            .await
            .expect("current diff should load")
            .expect("current diff should exist");
        let committed = store
            .materialize_agent_diff_final_snapshot_if_changed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: current_diff.clone(),
                },
                timestamp + 3,
            )
            .await
            .expect("final diff snapshot should persist");
        assert!(committed, "first final snapshot should append raw event");
        let skipped = store
            .materialize_agent_diff_final_snapshot_if_changed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: current_diff,
                },
                timestamp + 4,
            )
            .await
            .expect("duplicate final diff snapshot should compare payload");
        assert!(!skipped, "unchanged final snapshot should be skipped");

        let raw_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .all(&store.connection)
            .await
            .expect("raw events should query after final snapshot");
        assert_eq!(
            raw_events.len(),
            2,
            "turn/start plus one final diff snapshot should remain"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tool_recovery_policy_snapshot_round_trips_through_items_and_history() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_tool_policy";
        seed_observation_workspace(&store, workspace_id).await;
        let thread_id = "thr_tool_policy";
        let turn_id = "turn_tool_policy";
        let item_id = "item_tool_policy";
        let recovery_policy = sample_tool_recovery_policy();

        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            work_owner: Default::default(),
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

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

        let started_item = TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({"url": "https://example.com"}),
            status: ToolCallStatus::InProgress,
            recovery_policy: Some(recovery_policy.clone()),
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com"
                })),
            },
            recovery: None,
            url: Some("https://example.com".to_owned()),
            final_url: None,
            status_code: None,
            content_type: None,
            extract_mode: None,
            resolved_mode: None,
            bytes_received: None,
            elapsed_ms: None,
            truncated: None,
            title: None,
            word_count: None,
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        };
        let mut completed_item = started_item.clone();
        if let TurnItem::WebFetch {
            status, success, ..
        } = &mut completed_item
        {
            *status = ToolCallStatus::Completed;
            *success = Some(true);
        }

        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: started_item,
                },
                timestamp + 1,
            )
            .await
            .expect("item started should persist");
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: completed_item,
                },
                timestamp + 2,
            )
            .await
            .expect("item completed should persist");

        let stored_item = store
            .get_turn_item(turn_id, item_id)
            .await
            .expect("item lookup should succeed")
            .expect("item should exist");
        assert_eq!(stored_item.recovery_policy(), Some(&recovery_policy));
        if let TurnItem::WebFetch { output_policy, .. } = stored_item {
            assert_eq!(
                output_policy,
                ToolOutputPolicySnapshot::for_tool_name("web_fetch")
            );
        } else {
            panic!("expected web_fetch item");
        }

        let turn_items = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items query should succeed")
            .expect("turn items should exist");
        let item_events_have_snapshot = turn_items.events.iter().filter(|event| {
            matches!(
                &event.payload,
                TurnItemEventPayload::ItemStarted { item, .. }
                    | TurnItemEventPayload::ItemCompleted { item, .. }
                        if item.recovery_policy() == Some(&recovery_policy)
            )
        });
        assert_eq!(item_events_have_snapshot.count(), 2);

        let history = store
            .get_thread_history(thread_id, Some(16))
            .await
            .expect("thread history query should succeed")
            .expect("thread history should exist");
        let history_events_have_snapshot = history.events.iter().filter(|event| {
            matches!(
                &event.payload,
                ThreadHistoryEventPayload::ItemStarted { item, .. }
                    | ThreadHistoryEventPayload::ItemCompleted { item, .. }
                        if item.recovery_policy() == Some(&recovery_policy)
            )
        });
        assert_eq!(history_events_have_snapshot.count(), 2);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn item_started_can_atomically_persist_attempt_deadlines() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_attempt_deadlines").await;
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_attempt_deadlines";
        let thread_id = "thr_attempt_deadlines";
        let turn_id = "turn_attempt_deadlines";
        let item_id = "call_attempt_deadlines";
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

        let item = TurnItem::DynamicToolCall {
            id: item_id.to_owned(),
            tool_name: "task_create".to_owned(),
            arguments: serde_json::json!({ "title": "Daily weather" }),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            execution_class: pioneer_protocol::TurnItemExecutionClass::Standard,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("task_create"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            success: None,
            outcome: None,
            observation: None,
        };
        let deadlines = TurnItemAttemptDeadlines {
            lease_expires_at_unix: Some(timestamp + 121),
            idle_deadline_at_unix: Some(timestamp + 91),
            hard_deadline_at_unix: Some(timestamp + 301),
        };

        store
            .materialize_item_started_with_attempt_deadlines(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item,
                },
                timestamp + 1,
                deadlines,
            )
            .await
            .expect("item started should persist with deadlines");

        let attempt = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id))
            .filter(pioneer_entity::turn_item_attempt::Column::ItemId.eq(item_id))
            .one(&store.connection)
            .await
            .expect("attempt lookup should succeed")
            .expect("attempt should exist");

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

        let missing = store
            .list_running_attempts_missing_deadlines(10)
            .await
            .expect("deadline repair candidates should query");
        assert!(
            missing.is_empty(),
            "attempt with all deadlines should not require repair"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn completed_attempt_payload_is_cleared_without_affecting_item_reads() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_attempt_payload_cleanup").await;
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_attempt_payload_cleanup";
        let thread_id = "thr_attempt_payload_cleanup";
        let turn_id = "turn_attempt_payload_cleanup";
        let item_id = "call_attempt_payload_cleanup";
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

        let started_item = TurnItem::CommandExecution {
            id: item_id.to_owned(),
            tool_name: "shell".to_owned(),
            arguments: serde_json::json!({ "cmd": "printf heavy" }),
            status: ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("shell"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            command: vec!["printf".to_owned(), "heavy".to_owned()],
            cwd: Some("/tmp".to_owned()),
            success: None,
            outcome: None,
            observation: None,
        };
        let mut completed_item = started_item.clone();
        if let TurnItem::CommandExecution {
            status, success, ..
        } = &mut completed_item
        {
            *status = ToolCallStatus::Completed;
            *success = Some(true);
        }

        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: started_item,
                },
                timestamp + 1,
            )
            .await
            .expect("item start should persist");

        let running_attempt = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id))
            .filter(pioneer_entity::turn_item_attempt::Column::ItemId.eq(item_id))
            .one(&store.connection)
            .await
            .expect("running attempt should query")
            .expect("running attempt should exist");
        assert_ne!(running_attempt.payload.as_str(), "{}");

        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: completed_item,
                },
                timestamp + 2,
            )
            .await
            .expect("item completed should persist");

        let completed_attempt = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id))
            .filter(pioneer_entity::turn_item_attempt::Column::ItemId.eq(item_id))
            .one(&store.connection)
            .await
            .expect("completed attempt should query")
            .expect("completed attempt should exist");
        assert_eq!(completed_attempt.status, ATTEMPT_STATUS_COMPLETED);
        assert_eq!(completed_attempt.payload.as_str(), "{}");

        let stored_item = store
            .get_turn_item(turn_id, item_id)
            .await
            .expect("turn item should query")
            .expect("turn item should exist");
        assert!(matches!(
            stored_item,
            TurnItem::CommandExecution {
                status: ToolCallStatus::Completed,
                success: Some(true),
                ..
            }
        ));

        let turn_items = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn/items should query")
            .expect("turn/items should exist");
        assert_eq!(
            turn_items
                .events
                .iter()
                .filter(|event| matches!(
                    event.payload,
                    TurnItemEventPayload::ItemStarted { .. }
                        | TurnItemEventPayload::ItemCompleted { .. }
                ))
                .count(),
            2,
            "turn/items must still read full durable item events"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn timeout_transition_terminalizes_and_runtime_rehydration_restores_source_attempt() {
    let timestamp = 1_700_000_000;
    let workspace_id = "ws_timeout_terminalize";
    let store = test_store_with_workspace(workspace_id).await;
    let thread_id = "thr_timeout_terminalize";
    let turn_id = "turn_timeout_terminalize";
    let item_id = "item_timeout_terminalize";
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
    store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: safe_web_fetch_item(item_id),
            },
            timestamp + 1,
        )
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
        .expect("deadlines should be configured");

    let candidates = store
        .list_timeout_candidates(timestamp + 3, 8)
        .await
        .expect("timeout candidate list should succeed");
    assert_eq!(candidates.len(), 1);
    assert!(
        store
            .transition_timeout_candidate(&candidates[0], timestamp + 3)
            .await
            .expect("timeout transition should succeed")
    );

    let row =
        pioneer_crud_conversations::turn_reads::find_turn_item(&store.connection, turn_id, item_id)
            .await
            .expect("turn_item lookup should succeed")
            .expect("turn_item row should exist");
    assert_eq!(row.status.as_deref(), Some("timed_out"));
    assert_eq!(row.active_attempt_status.as_deref(), Some("timed_out"));

    let payload: TurnItem = read_fixture_item(&store.connection, row)
        .await
        .expect("payload should decode");
    let TurnItem::WebFetch { status, .. } = payload else {
        panic!("expected web_fetch payload");
    };
    assert_eq!(status, ToolCallStatus::Failed);

    assert!(
        store
            .reactivate_timed_out_attempt(
                candidates[0].attempt_id.as_str(),
                timestamp + 4,
                TurnItemAttemptDeadlines {
                    lease_expires_at_unix: Some(timestamp + 104),
                    idle_deadline_at_unix: Some(timestamp + 104),
                    hard_deadline_at_unix: Some(timestamp + 104),
                },
            )
            .await
            .expect("authoritative active runtime should reactivate its source attempt")
    );
    let restored =
        pioneer_crud_conversations::turn_reads::find_turn_item(&store.connection, turn_id, item_id)
            .await
            .expect("restored turn_item lookup should succeed")
            .expect("restored turn_item row should exist");
    assert_eq!(restored.status.as_deref(), Some("in_progress"));
    assert_eq!(restored.active_attempt_status.as_deref(), Some("running"));
    let restored_payload: TurnItem = read_fixture_item(&store.connection, restored)
        .await
        .expect("restored payload should decode");
    assert!(matches!(
        restored_payload,
        TurnItem::WebFetch {
            status: ToolCallStatus::InProgress,
            ..
        }
    ));
}

#[tokio::test]
async fn turn_liveness_tracks_latest_activity_and_attempt_start_sequence() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_turn_liveness";
        seed_observation_workspace(&store, workspace_id).await;
        let thread_id = "thr_turn_liveness";
        let turn_id = "turn_turn_liveness";
        let stale_item_id = "reasoning_stale_liveness";
        let later_item_id = "agent_message_liveness";
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
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: stale_item_id.to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
                timestamp + 1,
            )
            .await
            .expect("stale item start should persist");
        store
            .configure_turn_item_attempt_deadlines(
                turn_id,
                stale_item_id,
                timestamp + 1,
                Some(timestamp + 2),
                Some(timestamp + 2),
                Some(timestamp + 2),
            )
            .await
            .expect("deadlines should be configured");

        let later_item = TurnItem::AgentMessage {
            id: later_item_id.to_owned(),
            text: "still working".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: later_item.clone(),
                },
                timestamp + 10,
            )
            .await
            .expect("later item start should persist");
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: later_item,
                },
                timestamp + 11,
            )
            .await
            .expect("later item completion should persist");

        let liveness = store
            .get_turn_liveness(turn_id)
            .await
            .expect("liveness query should succeed")
            .expect("liveness row should exist");
        assert_eq!(liveness.turn_id, turn_id);
        assert_eq!(liveness.thread_id, thread_id);
        assert_eq!(liveness.last_activity_sequence, 4);
        assert_eq!(liveness.last_activity_kind, "item/completed");
        assert_eq!(
            liveness.last_activity_item_id.as_deref(),
            Some(later_item_id)
        );
        assert_eq!(
            liveness.last_activity_item_type.as_deref(),
            Some("agent_message")
        );
        assert_eq!(liveness.last_activity_at_unix, timestamp + 11);

        let candidates = store
            .list_timeout_candidates(timestamp + 12, 8)
            .await
            .expect("timeout candidate list should succeed");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].item_id, stale_item_id);
        assert_eq!(candidates[0].started_event_sequence, Some(2));
        assert_eq!(candidates[0].last_heartbeat_at_unix, Some(timestamp + 1));
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn terminal_turn_projection_closes_running_attempts() {
    let timestamp = 1_700_000_000;
    let workspace_id = "ws_turn_terminal_cleanup";
    let store = test_store_with_workspace(workspace_id).await;
    let thread_id = "thr_turn_terminal_cleanup";
    let turn_id = "turn_turn_terminal_cleanup";
    let item_id = "item_turn_terminal_cleanup";
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
    store
        .materialize_item_started(
            ItemStartedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: safe_web_fetch_item(item_id),
            },
            timestamp + 1,
        )
        .await
        .expect("item start should persist");

    // A terminal projection must remain correct for a large valid Turn.
    // Seed more stale running timeline rows than the former application
    // loop bound while their authoritative items are already completed.
    // Preparation stays outside the writer transaction; the transaction
    // itself performs only the fixture's PostgreSQL statements.
    const STALE_PROJECTION_COUNT: usize = 129;
    let fixture_at = unix_to_datetime(timestamp + 1);
    let mut stale_rows = Vec::with_capacity(STALE_PROJECTION_COUNT);
    for index in 0..STALE_PROJECTION_COUNT {
        let stale_item_id = format!("item_turn_terminal_stale_{index:03}");
        let mut item = safe_web_fetch_item(stale_item_id.as_str());
        let TurnItem::WebFetch { status, .. } = &mut item else {
            unreachable!("safe web-fetch fixture changed variant");
        };
        *status = ToolCallStatus::Completed;
        let item_projection = pioneer_crud_conversations::turn_item::prepare_turn_item_projection(
            turn_id,
            &item,
            Some("completed"),
            fixture_at,
            fixture_at,
        )
        .expect("stale fixture item should prepare");
        let timeline_projection = TurnWorkItemProjectionRecord {
            work_item_id: work_item_projection_id(turn_id, stale_item_id.as_str()),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: stale_item_id,
            source_event_id: None,
            source_sequence: 0,
            order_key: format!("stale:{index:020}"),
            item_type: "web_fetch".to_owned(),
            visibility: WORK_VISIBILITY_VISIBLE.to_owned(),
            classification: "web_fetch".to_owned(),
            status: WORK_ITEM_STATUS_RUNNING.to_owned(),
            started_at: Some(fixture_at),
            completed_at: None,
            metadata_json: "{}".to_owned(),
            created_at: fixture_at,
            updated_at: fixture_at,
        };
        stale_rows.push((item_projection, timeline_projection));
    }
    let transaction = store
        .connection
        .begin()
        .await
        .expect("stale projection fixture transaction should begin");
    for (item_projection, timeline_projection) in stale_rows {
        pioneer_crud_conversations::turn_item::upsert_prepared_turn_item(
            &transaction,
            item_projection,
        )
        .await
        .expect("stale fixture item should persist");
        pioneer_crud_conversations::thread_timeline_projection::upsert_turn_work_item_projection(
            &transaction,
            timeline_projection,
        )
        .await
        .expect("stale fixture timeline row should persist");
    }
    transaction
        .commit()
        .await
        .expect("stale projection fixture transaction should commit");

    store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn: Turn {
                    work_owner: Default::default(),
                    id: turn_id.to_owned(),
                    status: TurnStatus::Completed,
                    turn_kind: Default::default(),
                    origin: Default::default(),
                    mode: Default::default(),
                    author: None,
                    reply_to_turn_id: None,
                    mentions: Vec::new(),
                    message_revision: 0,
                    message_deleted: false,
                    error: None,
                    prompt_manifest: None,
                    permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(
                    ),
                },
            },
            timestamp + 2,
        )
        .await
        .expect("turn completion should persist");

    let running_attempts = pioneer_entity::turn_item_attempt::Entity::find()
        .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id.to_owned()))
        .filter(pioneer_entity::turn_item_attempt::Column::Status.eq("running"))
        .all(&store.connection)
        .await
        .expect("running attempt query should succeed");
    assert!(running_attempts.is_empty());

    let row =
        pioneer_crud_conversations::turn_reads::find_turn_item(&store.connection, turn_id, item_id)
            .await
            .expect("turn_item lookup should succeed")
            .expect("turn_item row should exist");
    assert_eq!(row.status.as_deref(), Some("failed"));
    assert_eq!(row.active_attempt_status.as_deref(), Some("interrupted"));

    let payload: TurnItem = read_fixture_item(&store.connection, row)
        .await
        .expect("payload should decode");
    let TurnItem::WebFetch { status, .. } = payload else {
        panic!("expected web_fetch payload");
    };
    assert_eq!(status, ToolCallStatus::Failed);

    let running_projection_count = pioneer_entity::turn_work_item_projection::Entity::find()
        .filter(pioneer_entity::turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            pioneer_entity::turn_work_item_projection::Column::Status.eq(WORK_ITEM_STATUS_RUNNING),
        )
        .count(&store.connection)
        .await
        .expect("running timeline projection count should succeed");
    assert_eq!(running_projection_count, 0);
    let completed_projection_count = pioneer_entity::turn_work_item_projection::Entity::find()
        .filter(pioneer_entity::turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            pioneer_entity::turn_work_item_projection::Column::Status
                .eq(WORK_ITEM_STATUS_COMPLETED),
        )
        .count(&store.connection)
        .await
        .expect("completed timeline projection count should succeed");
    assert_eq!(
        completed_projection_count,
        u64::try_from(STALE_PROJECTION_COUNT).unwrap()
    );
}

fn sample_tool_recovery_policy() -> ToolRecoveryPolicySnapshot {
    ToolRecoveryPolicySnapshot {
        retry_class: ToolRecoveryRetryClass::Network,
        idempotency_mode: ToolRecoveryIdempotencyMode::Safe,
        max_attempts: 5,
        can_resume: true,
        resolved_action: RecoveryAction::RetryWithBackoff,
        base_backoff_secs: 3,
        max_wall_clock_secs: 240,
        no_progress_limit: 3,
    }
}
