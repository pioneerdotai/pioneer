use super::*;

#[tokio::test]
async fn cli_runtime_timeline_state_tracks_native_start_not_work_item_gaps() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_cli_timeline_state";
        let thread_id = "thread_cli_timeline_state";
        let turn_id = "turn_cli_timeline_state";
        let (store, _, _) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;
        let queued_at = unix_to_datetime(1_700_000_010);
        let running_at = unix_to_datetime(1_700_000_020);

        let initial = NewCliRuntimeTurnBinding {
            turn_id: turn_id.to_owned(),
            thread_id: thread_id.to_owned(),
            continuation_thread_id: thread_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_thread_id: "codex-thread-timeline-state".to_owned(),
            native_turn_id: None,
            request_id: None,
            status: "starting".to_owned(),
            model: Some("gpt-5.6".to_owned()),
            cwd: Some("/tmp/project".to_owned()),
            sandbox_json: None,
            approval_policy: Some("on-request".to_owned()),
            input_mapping_json: "{}".to_owned(),
            created_at: queued_at,
            updated_at: queued_at,
        };
        let database = store.database_connection();
        let mut portable = database.work_signal("maintenance.portable").subscribe();
        portable.begin_pass(&database).await.unwrap();
        let (_, attempt) = store
            .prepare_cli_runtime_initial_turn_attempt(
                initial.clone(),
                "attempt-cli-timeline-state".to_owned(),
                1,
            )
            .await
            .expect("queued CLI runtime attempt should prepare");
        assert!(futures_util::FutureExt::now_or_never(portable.wait(None)).is_some());
        portable.begin_pass(&database).await.unwrap();
        store
            .prepare_cli_runtime_initial_turn_attempt(
                initial,
                "attempt-cli-timeline-state".into(),
                1,
            )
            .await
            .unwrap();
        assert!(
            futures_util::FutureExt::now_or_never(portable.wait(None)).is_none(),
            "initial attempt replay must stay quiet"
        );
        assert_eq!(
            store
                .get_turn_work_projection(turn_id)
                .await
                .expect("queued work projection should load")
                .expect("queued work projection should exist")
                .state,
            "starting"
        );

        store
            .activate_cli_runtime_turn_attempt(
                turn_id,
                attempt.id.as_str(),
                "native-turn-timeline-state",
                None,
                running_at,
            )
            .await
            .expect("CLI runtime attempt should activate");
        assert_eq!(
            store
                .get_turn_work_projection(turn_id)
                .await
                .expect("running work projection should load")
                .expect("running work projection should exist")
                .state,
            "running",
            "a started CLI turn must stay running even when no work item is active"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_api_timeline_state_tracks_turn_lifecycle_not_work_item_gaps() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_native_timeline_state";
        let thread_id = "thread_native_timeline_state";
        let turn_id = "turn_native_timeline_state";
        let item_id = "item_native_timeline_state";
        let (store, _, _) =
            postgres_store_with_started_turn(fixture, workspace_id, thread_id, turn_id).await;

        let state = || async {
            store
                .get_turn_work_projection(turn_id)
                .await
                .expect("native work projection should load")
                .expect("native work projection should exist")
                .state
        };
        assert_eq!(
            state().await,
            "running",
            "a confirmed native Turn start is already running before its first work item"
        );

        let running_item = safe_web_fetch_item(item_id);
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: running_item.clone(),
                },
                1_700_000_010,
            )
            .await
            .expect("native work item should start");
        assert_eq!(state().await, "running");
        let started_projection = store
            .get_turn_work_item_projection(work_item_projection_id(turn_id, item_id).as_str())
            .await
            .expect("started item projection should load")
            .expect("started item projection should exist");

        let mut completed_item = running_item;
        if let TurnItem::WebFetch {
            status, success, ..
        } = &mut completed_item
        {
            *status = ToolCallStatus::Completed;
            *success = Some(true);
        }
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: completed_item,
                },
                1_700_000_020,
            )
            .await
            .expect("native work item should complete");
        let completed_projection = store
            .get_turn_work_item_projection(work_item_projection_id(turn_id, item_id).as_str())
            .await
            .expect("completed item projection should load")
            .expect("completed item projection should exist");
        assert_eq!(completed_projection.order_key, started_projection.order_key);
        assert!(
            completed_projection.source_sequence > started_projection.source_sequence,
            "projection revision must advance while stable ordering is preserved"
        );
        assert_ne!(
            completed_projection.source_event_id, started_projection.source_event_id,
            "projection revision must identify the completion event"
        );
        assert_eq!(
            state().await,
            "running",
            "a native Turn must remain running between completed and future work items"
        );
        Ok(())
    })
    .await
    .unwrap();
}
