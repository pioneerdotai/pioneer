use super::*;

#[tokio::test]
async fn recovery_lifecycle_events_round_trip_through_turn_history() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let workspace_id = "ws_000000000000000099";
        let thread_id = "thr_000000000000000099";
        let turn_id = "turn_000000000000000099";
        let item_id = "reasoning_recovery";
        let recovery_job_id = "recovery_job_99";

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

        seed_observation_workspace(&store, &thread.workspace_id).await;
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
            .materialize_item_timeout_detected(
                ItemTimeoutDetectedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    attempt_number: 1,
                    reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                    recovery_job_id: Some(recovery_job_id.to_owned()),
                },
                timestamp + 1,
            )
            .await
            .expect("timeout detected should persist");
        store
            .materialize_item_recovery_opened(
                ItemRecoveryOpenedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    trigger: RecoveryTrigger::Timeout,
                    action: RecoveryAction::RetryAttempt,
                    attempt_number: 1,
                },
                timestamp + 2,
            )
            .await
            .expect("recovery opened should persist");
        store
            .materialize_item_recovery_attached(
                ItemRecoveryAttachedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_second_failure".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    recovery_item_id: item_id.to_owned(),
                    recovery_item_type: TurnItemType::Reasoning,
                    trigger: RecoveryTrigger::ProviderError,
                    action: RecoveryAction::RetryAttempt,
                    existing_status: RecoveryJobStatus::Pending,
                    next_attempt_number: 1,
                },
                timestamp + 3,
            )
            .await
            .expect("recovery attached should persist");
        store
            .materialize_item_retry_scheduled(
                ItemRetryScheduledNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    attempt_number: 2,
                    next_run_at_unix: timestamp + 30,
                    reason: Some("retry later".to_owned()),
                },
                timestamp + 4,
            )
            .await
            .expect("retry scheduled should persist");
        store
            .materialize_item_retry_attempt_started(
                ItemRetryAttemptStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    attempt_number: 2,
                },
                timestamp + 5,
            )
            .await
            .expect("retry attempt started should persist");
        store
            .materialize_item_recovery_succeeded(
                ItemRecoverySucceededNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    attempt_number: 2,
                },
                timestamp + 6,
            )
            .await
            .expect("recovery succeeded should persist");
        store
            .materialize_item_recovery_exhausted(
                ItemRecoveryExhaustedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::Reasoning,
                    recovery_job_id: recovery_job_id.to_owned(),
                    attempt_number: 3,
                    status: RecoveryJobStatus::Exhausted,
                    error_message: "attempts exhausted".to_owned(),
                },
                timestamp + 7,
            )
            .await
            .expect("recovery exhausted should persist");

        let turn_items = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items should load")
            .expect("turn items should exist");
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemTimeoutDetected { recovery_job_id: Some(job_id), .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRecoveryOpened { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRecoveryAttached { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRetryScheduled { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRetryAttemptStarted { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRecoverySucceeded { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(turn_items.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::ItemRecoveryExhausted { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));

        let history = store
            .get_thread_history(thread_id, Some(32))
            .await
            .expect("thread history should load")
            .expect("thread history should exist");
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRecoveryOpened { recovery_job_id: job_id, .. }
                if job_id == recovery_job_id
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRecoveryAttached { recovery_job_id: job_id, recovery_item_id, .. }
                if job_id == recovery_job_id && recovery_item_id == item_id
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRetryScheduled { recovery_job_id: job_id, next_run_at_unix, .. }
                if job_id == recovery_job_id && *next_run_at_unix == timestamp + 30
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRetryAttemptStarted { recovery_job_id: job_id, attempt_number, .. }
                if job_id == recovery_job_id && *attempt_number == 2
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRecoverySucceeded { recovery_job_id: job_id, attempt_number, .. }
                if job_id == recovery_job_id && *attempt_number == 2
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemRecoveryExhausted { recovery_job_id: job_id, status, error_message, .. }
                if job_id == recovery_job_id
                    && *status == RecoveryJobStatus::Exhausted
                    && error_message == "attempts exhausted"
        )));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tool_retry_lifecycle_events_round_trip_without_recovery_jobs() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_100_000;
        let workspace_id = "ws_000000000000000199";
        seed_observation_workspace(&store, workspace_id).await;
        let thread_id = "thr_000000000000000199";
        let turn_id = "turn_000000000000000199";
        let item_id = "item_tool_retry";
        let episode_id = "tool_retry_turn_199_1";

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
        let budgets = vec![ToolRetryBudgetUsage {
            kind: ToolRetryBudgetKind::Episode,
            used: 1,
            limit: 2,
        }];

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
            .materialize_item_tool_retry_scheduled(
                ItemToolRetryScheduledNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::WebFetch,
                    tool_retry_episode_id: episode_id.to_owned(),
                    tool_name: "web_fetch".to_owned(),
                    attempt_number: 1,
                    error_class: ToolRetryErrorClass::Timeout,
                    retry_hint: "retry with a smaller request".to_owned(),
                    budgets: budgets.clone(),
                    failure_signature_fingerprint: "sig_timeout".to_owned(),
                    reason: "recoverable_tool_output".to_owned(),
                },
                timestamp + 1,
            )
            .await
            .expect("tool retry scheduled should persist");
        store
            .materialize_item_tool_retry_resolved(
                ItemToolRetryResolvedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::WebFetch,
                    tool_retry_episode_id: episode_id.to_owned(),
                    tool_name: "web_fetch".to_owned(),
                    attempt_number: 2,
                    resolution: ToolRetryResolution::Succeeded,
                    budgets: budgets.clone(),
                    reason: "successful_tool_output".to_owned(),
                },
                timestamp + 2,
            )
            .await
            .expect("tool retry resolved should persist");
        store
            .materialize_item_tool_retry_exhausted(
                ItemToolRetryExhaustedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::WebFetch,
                    tool_retry_episode_id: episode_id.to_owned(),
                    tool_name: "web_fetch".to_owned(),
                    attempt_number: 3,
                    error_class: ToolRetryErrorClass::Timeout,
                    exhaustion_kind: ToolRetryExhaustionKind::FailureSignature,
                    budgets: budgets.clone(),
                    failure_signature_fingerprint: "sig_timeout".to_owned(),
                    reason: "same_failure_signature".to_owned(),
                },
                timestamp + 3,
            )
            .await
            .expect("tool retry exhausted should persist");
        store
            .materialize_turn_tool_loop_budget_exceeded(
                TurnToolLoopBudgetExceededNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
                    limit: 32,
                    observed: 33,
                    action: ToolLoopBudgetAction::ContinueInNextWindow,
                    reason: "agent_rounds_exceeded".to_owned(),
                },
                timestamp + 4,
            )
            .await
            .expect("tool loop budget event should persist");

        let turn_items = store
            .get_turn_item_events(thread_id, turn_id)
            .await
            .expect("turn items should load")
            .expect("turn items should exist");
        let payloads = turn_items
            .events
            .iter()
            .map(|event| &event.payload)
            .collect::<Vec<_>>();
        assert!(matches!(
            payloads[0],
            TurnItemEventPayload::ItemToolRetryScheduled {
                tool_retry_episode_id,
                error_class: ToolRetryErrorClass::Timeout,
                ..
            } if tool_retry_episode_id == episode_id
        ));
        assert!(matches!(
            payloads[1],
            TurnItemEventPayload::ItemToolRetryResolved {
                resolution: ToolRetryResolution::Succeeded,
                ..
            }
        ));
        assert!(matches!(
            payloads[2],
            TurnItemEventPayload::ItemToolRetryExhausted {
                exhaustion_kind: ToolRetryExhaustionKind::FailureSignature,
                ..
            }
        ));
        assert!(matches!(
            payloads[3],
            TurnItemEventPayload::TurnToolLoopBudgetExceeded {
                action: ToolLoopBudgetAction::ContinueInNextWindow,
                ..
            }
        ));

        let history = store
            .get_thread_history(thread_id, Some(16))
            .await
            .expect("thread history should load")
            .expect("thread history should exist");
        assert!(
            history
                .events
                .windows(2)
                .all(|pair| pair[0].sequence < pair[1].sequence)
        );
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemToolRetryScheduled {
                tool_retry_episode_id,
                budgets,
                ..
            } if tool_retry_episode_id == episode_id
                && budgets.first().is_some_and(|budget| budget.kind == ToolRetryBudgetKind::Episode)
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemToolRetryResolved {
                resolution: ToolRetryResolution::Succeeded,
                ..
            }
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::ItemToolRetryExhausted {
                failure_signature_fingerprint,
                ..
            } if failure_signature_fingerprint == "sig_timeout"
        )));
        assert!(history.events.iter().any(|event| matches!(
            &event.payload,
            ThreadHistoryEventPayload::TurnToolLoopBudgetExceeded {
                limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
                observed: 33,
                ..
            }
        )));

        let recovery_jobs = pioneer_entity::recovery_job::Entity::find()
            .all(&connection)
            .await
            .expect("recovery job query should succeed");
        assert!(
            recovery_jobs.is_empty(),
            "tool retry lifecycle must not create recovery jobs"
        );
        Ok(())
    })
    .await
    .unwrap();
}
