//! Regression oracles use real native admission, actor, canonical commit and
//! LIVE delivery. The provider only supplies a controllable model completion.
use super::*;
use crate::message::turn_handlers::foreground_start::ForegroundAcceptance;

const THREAD: &str = "foreground-start-thread";
const TURN: &str = "foreground-start-turn";
const PUBLICATION: &str = "__foreground_start_before_publication__";

struct Fixture {
    processor: Arc<MessageProcessor>,
    store: Arc<CrudStore>,
    workspace: String,
    connection: ConnectionId,
    rx: mpsc::Receiver<Message>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicU64>,
}

impl Fixture {
    async fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(256);
        let sessions = Arc::new(SessionManager::new());
        let connection = register_authenticated_test_connection(&sessions, tx).await;
        let (workspaces, store, workspace) = setup_workspace_manager().await;
        ensure_test_superuser_execution_authority(&store).await;
        let secrets = test_gateway_secrets();
        secrets
            .set_workspace_provider_api_key(&workspace, "openai", "fixture-key")
            .unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let provider = Arc::new(RecoveryProvider {
            barrier: CancellationBarrierProvider {
                entered: entered.clone(),
                release: release.clone(),
            },
            calls: calls.clone(),
            cancelled: Arc::new(AtomicU64::new(0)),
        });
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            Arc::new(pioneer_provider::ProviderRegistry::with_provider(
                "openai", provider,
            )),
            sessions,
            workspaces,
            store.clone(),
            secrets,
            test_summary_config(),
            test_tool_loop_config(),
        ));
        let mut thread = open(
            &processor,
            connection,
            &mut rx,
            &ClientCore::new(),
            &workspace,
            THREAD,
        )
        .await;
        thread.name = Some("Startup regression".into());
        store
            .upsert_thread_model(
                &thread,
                pioneer_protocol::PersistedActorRef::Principal(
                    authenticated_test_superuser().principal_id.clone(),
                ),
            )
            .await
            .unwrap();
        processor
            .thread_manager
            .system_thread_restore_persisted(thread, None)
            .await
            .unwrap();
        while rx.try_recv().is_ok() {}
        Self {
            processor,
            store,
            workspace,
            connection,
            rx,
            entered,
            release,
            calls,
        }
    }

    fn params(&self, turn: &str) -> TurnStartParams {
        serde_json::from_value(json!({
            "thread_id":THREAD, "turn_id":turn,
            "model":"test-model", "model_provider":"openai", "mode":ThreadMode::Agent,
            "execution_backend":{"type":"apiProvider","provider":"openai"},
            "permission_profile":pioneer_protocol::TurnPermissionProfileSelection::full_access(),
            "input":[{"type":"text","text":"startup ordering"}]
        }))
        .unwrap()
    }

    async fn start(
        &self,
        params: TurnStartParams,
    ) -> tokio::task::JoinHandle<
        Result<ForegroundAcceptance, crate::message::turn_handlers::TurnStartFailure>,
    > {
        let admission = test_execution_authorization_admission(
            &self.processor,
            self.connection,
            &self.workspace,
            &params.thread_id,
        )
        .await;
        let processor = self.processor.clone();
        let connection = self.connection;
        tokio::spawn(async move {
            processor
                .accept_foreground_api_provider_start(
                    connection,
                    pioneer_protocol::PersistedActorRef::Principal(
                        authenticated_test_superuser().principal_id.clone(),
                    ),
                    None,
                    params,
                    None,
                    admission,
                )
                .await
        })
    }

    async fn reached(&self) {
        timeout(
            Duration::from_secs(10),
            self.processor
                .wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
    }

    async fn accepted(
        &self,
        task: tokio::task::JoinHandle<
            Result<ForegroundAcceptance, crate::message::turn_handlers::TurnStartFailure>,
        >,
    ) -> ForegroundAcceptance {
        timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .expect("native startup must be accepted")
    }

    async fn terminal(&self, turn: &str) {
        timeout(Duration::from_secs(10), async {
            while !self
                .store
                .native_turn_has_accepted_terminal(turn)
                .await
                .unwrap()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    fn no_dependent_events(&mut self) {
        while let Ok(Message::Text(text)) = self.rx.try_recv() {
            let value: JsonValue = serde_json::from_str(&text).unwrap();
            assert!(
                !matches!(
                    value["method"].as_str(),
                    Some(
                        events::TURN_STARTED
                            | events::TURN_COMPLETED
                            | events::TURN_FAILED
                            | events::TURN_BLOCKED
                            | events::ITEM_STARTED
                            | events::ITEM_COMPLETED
                            | events::ITEM_UPDATED
                            | events::ITEM_COMMAND_EXECUTION_OUTPUT_DELTA
                            | events::ITEM_AGENT_MESSAGE_DELTA
                    )
                ),
                "dependent event escaped the start owner: {value}"
            );
        }
    }

    async fn close(&self) {
        self.processor.foreground_starts.shutdown().await;
        self.processor.agent_manager.remove_thread(THREAD).await;
    }
}

#[tokio::test]
async fn stalled_start_fanout_allows_rpc_ack_handoff_and_durable_fast_terminal_in_order() {
    let mut f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier(PUBLICATION);
    let request_id = generate_test_request_id("foreground", "early-ack");
    let request =
        json!({"jsonrpc":"2.0", "id":request_id, "method":"turn/start", "params": f.params(TURN)})
            .to_string();
    timeout(
        Duration::from_secs(10),
        f.processor
            .process_request_for_connection(f.connection, &request),
    )
    .await
    .unwrap();
    let response = recv_response_by_id(&mut f.rx, &request_id).await;
    assert_eq!(response.result["turn"]["id"], TURN);
    f.reached().await;
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        f.store
            .get_turn_execution(TURN)
            .await
            .unwrap()
            .unwrap()
            .status,
        TurnExecutionStatus::Running
    );
    f.release.notify_one();
    f.terminal(TURN).await; // listener commit/producer ACK did not wait for fanout
    let pending_heads = f
        .store
        .claim_due_turn_event_deliveries(
            pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER,
            now_timestamp_secs() + 600,
            64,
        )
        .await
        .unwrap();
    for delivery in pending_heads {
        let id = delivery.id.clone();
        let attempts = delivery.attempt_count;
        f.processor
            .process_claimed_native_turn_event_delivery(delivery)
            .await
            .unwrap();
        let deferred = pioneer_entity::turn_event_delivery::Entity::find_by_id(id)
            .one(&f.store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            deferred.attempt_count, attempts,
            "a startup dependency must not exhaust live delivery"
        );
    }
    f.no_dependent_events();
    f.processor.release_completed_history_preparation_barrier();
    let start = recv_notification_by_method(&mut f.rx, events::TURN_STARTED).await;
    assert_eq!(start.params.unwrap()["turn"]["id"], TURN);
    // Drive the existing durable retry owner with a due timestamp; no sorting mock.
    timeout(Duration::from_secs(10), async {
        loop {
            let deliveries = f
                .store
                .claim_due_turn_event_deliveries(
                    pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER,
                    now_timestamp_secs() + 600,
                    64,
                )
                .await
                .unwrap();
            for delivery in deliveries {
                f.processor
                    .process_claimed_native_turn_event_delivery(delivery)
                    .await
                    .unwrap();
            }
            if completion_live_delivery(&f.store, TURN).await.status == "delivered" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.close().await;
}

#[tokio::test]
async fn request_drop_before_preparation_does_not_cancel_the_owned_durable_start() {
    let f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier("__foreground_start_before_prepare__");
    let request = f.start(f.params(TURN)).await;
    f.reached().await;
    request.abort();
    let _ = request.await;
    f.processor.release_completed_history_preparation_barrier();
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    assert!(
        f.store
            .get_turn_runtime_snapshot(TURN)
            .await
            .unwrap()
            .is_some()
    );
    f.release.notify_one();
    f.terminal(TURN).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.close().await;
}

#[tokio::test]
async fn dropping_accepted_response_releases_publication_but_not_execution() {
    let mut f = Fixture::new().await;
    let acceptance = f.accepted(f.start(f.params(TURN)).await).await;
    assert_eq!(acceptance.response.turn.id, TURN);
    f.no_dependent_events(); // ACK ownership guard has not been released yet.
    drop(acceptance); // same ownership path as failed ACK enqueue / handler teardown
    let _ = recv_notification_by_method(&mut f.rx, events::TURN_STARTED).await;
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    f.release.notify_one();
    f.terminal(TURN).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.close().await;
}

#[tokio::test]
async fn duplicate_request_joins_publication_and_retains_canonical_ids_without_another_provider() {
    let f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier(PUBLICATION);
    let accepted = f.accepted(f.start(f.params(TURN)).await).await;
    let canonical = accepted.response.turn.id.clone();
    drop(accepted);
    f.reached().await;
    let replay = f.start(f.params(TURN)).await;
    assert!(!replay.is_finished());
    f.processor.release_completed_history_preparation_barrier();
    let replay = f.accepted(replay).await;
    assert_eq!(replay.response.turn.id, canonical);
    drop(replay);
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    f.release.notify_one();
    f.terminal(TURN).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.close().await;
}

#[tokio::test]
async fn startup_count_and_byte_saturation_reject_before_durable_admission() {
    let f = Fixture::new().await;
    let mut obligations = Vec::new();
    for i in 0..32 {
        obligations.push(
            f.processor
                .foreground_starts
                .admit(THREAD, &format!("occupied-{i}"))
                .unwrap(),
        );
    }
    let rejected = timeout(Duration::from_secs(10), f.start(f.params(TURN)).await)
        .await
        .unwrap()
        .unwrap();
    assert!(rejected.is_err());
    assert!(f.store.get_turn(THREAD, TURN).await.unwrap().is_none());
    drop(obligations);
    let mut params = f.params(TURN);
    params.input = vec![pioneer_protocol::UserInput::Text {
        text: "x".repeat(8 * 1024 * 1024),
        text_elements: Vec::new(),
    }];
    assert!(
        timeout(Duration::from_secs(10), f.start(params).await)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(f.store.get_turn(THREAD, TURN).await.unwrap().is_none());
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    f.close().await;
}

#[tokio::test]
async fn actor_retirement_after_preparation_reports_failure_with_durable_recovery() {
    let f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier("__foreground_start_before_dispatch__");
    let task = f.start(f.params(TURN)).await;
    f.reached().await;
    f.processor.agent_manager.remove_thread(THREAD).await;
    f.processor.release_completed_history_preparation_barrier();
    assert!(
        timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    f.processor
        .recovery_coordinator
        .run_supervision_audits(now_timestamp_secs(), 64)
        .await
        .unwrap();
    assert!(
        f.store
            .find_unresolved_recovery_job_for_turn(TURN)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    f.close().await;
}

#[tokio::test]
async fn fast_terminal_before_running_cas_is_accepted_without_recovery_or_second_start() {
    let f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier("__foreground_start_after_actor_acceptance__");
    let task = f.start(f.params(TURN)).await;
    f.reached().await;
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    f.release.notify_one();
    f.terminal(TURN).await;
    f.processor.release_completed_history_preparation_barrier();
    drop(f.accepted(task).await);
    assert!(
        f.store
            .find_unresolved_recovery_job_for_turn(TURN)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.close().await;
}

#[tokio::test]
async fn cancel_before_activation_keeps_canonical_terminal_without_calling_provider() {
    let f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier("__foreground_start_before_dispatch__");
    let task = f.start(f.params(TURN)).await;
    f.reached().await;
    assert!(
        f.processor
            .mark_turn_interrupted(THREAD.into(), TURN.into(), "fixture Stop".into())
            .await
    );
    f.processor.release_completed_history_preparation_barrier();
    drop(f.accepted(task).await);
    assert_eq!(
        f.store
            .get_turn(THREAD, TURN)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Interrupted
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    f.close().await;
}

#[tokio::test]
async fn mandatory_turn_snapshot_and_user_lifecycle_failures_never_accept_or_run_provider() {
    for (table, predicate) in [
        ("turn", "1"),
        ("turn_runtime_snapshot", "1"),
        ("turn_event", "NEW.event_type = 'item/started'"),
    ] {
        let f = Fixture::new().await;
        let db = f.store.database_connection();
        db.execute_unprepared(&format!("CREATE TRIGGER reject_mandatory BEFORE INSERT ON {table} WHEN {predicate} BEGIN SELECT RAISE(ABORT, 'fixture mandatory failure'); END")).await.unwrap();
        let result = timeout(Duration::from_secs(10), f.start(f.params(TURN)).await)
            .await
            .unwrap()
            .unwrap();
        assert!(
            result.is_err(),
            "mandatory {table} failure was acknowledged"
        );
        assert_eq!(f.calls.load(Ordering::SeqCst), 0);
        db.execute_unprepared("DROP TRIGGER reject_mandatory")
            .await
            .unwrap();
        f.close().await;
    }
}

#[tokio::test]
async fn ack_enqueue_failure_after_handoff_does_not_cancel_provider() {
    let f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier("__foreground_start_after_actor_acceptance__");
    let processor = f.processor.clone();
    let connection = f.connection;
    let request =
        json!({"jsonrpc":"2.0", "id":generate_test_request_id("foreground", "enqueue-failed"),
        "method":"turn/start", "params":f.params(TURN)})
        .to_string();
    let task = tokio::spawn(async move {
        processor
            .process_request_for_connection(connection, &request)
            .await
    });
    f.reached().await;
    // Capture/authentication already happened. Remove only the response socket.
    f.processor
        .session_manager
        .unregister_connection(f.connection)
        .await;
    f.processor.release_completed_history_preparation_barrier();
    timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    f.release.notify_one();
    f.terminal(TURN).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.close().await;
}

#[tokio::test]
async fn replacement_ownership_cannot_be_rewritten_by_initial_running_cas() {
    let f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier("__foreground_start_after_actor_acceptance__");
    let task = f.start(f.params(TURN)).await;
    f.reached().await;
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    // Activation was legitimately committed before replacement. The subsequent
    // failed running CAS must stop this old run without terminalizing its heir.
    assert!(
        f.store
            .native_cancellation_context(TURN)
            .await
            .unwrap()
            .is_some()
    );
    let original = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
    let claim_at = original.lease_until.timestamp() + 1;
    let replacement = f
        .store
        .claim_expired_turn_execution(&original, &original.owner_id, claim_at, claim_at + 60)
        .await
        .unwrap()
        .expect("actual ownership claim");
    f.processor.release_completed_history_preparation_barrier();
    assert!(
        timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let current = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
    assert_eq!(current.owner_id, replacement.owner_id);
    assert_eq!(current.owner_generation, replacement.owner_generation);
    assert_eq!(current.status, replacement.status);
    assert!(
        !f.store
            .native_turn_has_accepted_terminal(TURN)
            .await
            .unwrap()
    );
    assert_eq!(
        f.store
            .get_turn(THREAD, TURN)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.close().await;
}

#[tokio::test]
async fn direct_progress_and_blocked_share_the_start_dependency_without_waiting_for_it() {
    let mut f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier(PUBLICATION);
    drop(f.accepted(f.start(f.params(TURN)).await).await);
    f.reached().await;
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    let delta = pioneer_protocol::ItemDeltaNotification {
        workspace_id: f.workspace.clone(),
        thread_id: THREAD.into(),
        turn_id: TURN.into(),
        item_id: "progress-input".into(),
        delta: "prefix".into(),
        stream: Some(pioneer_protocol::ItemDeltaStream::AgentMessage),
        payload: None,
        markdown: None,
        markdown_version: None,
    };
    timeout(
        Duration::from_secs(10),
        f.processor
            .handle_progress_agent_event(pioneer_protocol::AgentProgressEvent::ItemDelta {
                notification: delta.clone(),
            }),
    )
    .await
    .unwrap();
    let mut output = delta;
    output.stream = Some(pioneer_protocol::ItemDeltaStream::Stdout);
    output.delta = "persisted-during-startup".into();
    assert!(
        timeout(
            Duration::from_secs(10),
            f.processor.handle_durable_agent_event(
                pioneer_protocol::AgentDurableEvent::ToolOutputRecorded {
                    id: "foreground-start-output".into(),
                    notification: output,
                }
            )
        )
        .await
        .unwrap()
    );
    let rows = f
        .store
        .tool_output_page(&f.workspace, THREAD, TURN, "progress-input", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.text, "persisted-during-startup");
    assert!(
        timeout(
            Duration::from_secs(10),
            f.processor
                .mark_turn_blocked(THREAD.into(), TURN.into(), "fixture blocked".into())
        )
        .await
        .unwrap()
    );
    f.no_dependent_events();
    assert!(
        f.store
            .native_turn_has_accepted_terminal(TURN)
            .await
            .unwrap()
    );
    f.processor.release_completed_history_preparation_barrier();
    let _ = recv_notification_by_method(&mut f.rx, events::TURN_STARTED).await;
    f.close().await;
}

#[tokio::test]
async fn snapshot_observation_and_shutdown_do_not_allow_a_late_initial_start() {
    let mut f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier(PUBLICATION);
    drop(f.accepted(f.start(f.params(TURN)).await).await);
    f.reached().await;
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    f.release.notify_one();
    f.terminal(TURN).await;
    wait_for_thread_manager_turn_status(
        &f.processor.thread_manager,
        THREAD,
        TURN,
        TurnStatus::Completed,
    )
    .await;
    let processor = f.processor.clone();
    let connection = f.connection;
    let id = generate_test_request_id("foreground", "terminal-snapshot");
    let request = json!({"jsonrpc":"2.0", "id":id, "method":"turn/get",
        "params":{"thread_id":THREAD,"turn_id":TURN}})
    .to_string();
    let snapshot = tokio::spawn(async move {
        processor
            .process_request_for_connection(connection, &request)
            .await;
    });
    // This signal is emitted by the actual RPC at the Pending dependency.
    timeout(
        Duration::from_secs(10),
        f.processor.foreground_starts.snapshot_waiting.notified(),
    )
    .await
    .unwrap();
    while let Ok(Message::Text(text)) = f.rx.try_recv() {
        let value: JsonValue = serde_json::from_str(&text).unwrap();
        assert_ne!(value["id"], json!(id), "terminal snapshot escaped Pending");
        assert_ne!(value["method"], events::TURN_STARTED);
    }
    timeout(
        Duration::from_secs(10),
        f.processor.foreground_starts.shutdown(),
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(10), snapshot)
        .await
        .unwrap()
        .unwrap();
    let mut saw_response = false;
    // shutdown joined the publisher; request completion proves response enqueue.
    // Inspect every receiver entry, including a forbidden late TURN_STARTED.
    while let Ok(Message::Text(text)) = f.rx.try_recv() {
        let value: JsonValue = serde_json::from_str(&text).unwrap();
        assert_ne!(
            value["method"],
            events::TURN_STARTED,
            "retired publisher sent a delayed start"
        );
        if value["id"] == json!(id) {
            assert_eq!(value["result"]["turn"]["status"], "Completed");
            saw_response = true;
        }
    }
    assert!(saw_response);
    assert!(!f.processor.foreground_starts.pending(THREAD, TURN));
    assert!(
        f.store
            .get_turn_runtime_snapshot(TURN)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        f.processor
            .foreground_starts
            .admit(THREAD, "after-shutdown")
            .is_err()
    );
    f.close().await;
}

#[tokio::test]
async fn a_pending_turn_does_not_occupy_delivery_capacity_for_another_thread() {
    let mut f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier(PUBLICATION);
    drop(f.accepted(f.start(f.params(TURN)).await).await);
    f.reached().await;
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    f.release.notify_one();
    f.terminal(TURN).await;
    let other_thread = "foreground-independent-thread";
    let other_turn = "foreground-independent-turn";
    let mut thread = open(
        &f.processor,
        f.connection,
        &mut f.rx,
        &ClientCore::new(),
        &f.workspace,
        other_thread,
    )
    .await;
    thread.name = Some("Independent".into());
    f.store
        .upsert_thread_model(
            &thread,
            pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            ),
        )
        .await
        .unwrap();
    f.processor
        .thread_manager
        .system_thread_restore_persisted(thread, None)
        .await
        .unwrap();
    let mut params = f.params(other_turn);
    params.thread_id = other_thread.into();
    drop(f.accepted(f.start(params).await).await);
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    f.release.notify_one();
    f.terminal(other_turn).await;
    timeout(Duration::from_secs(10), async {
        loop {
            let deliveries = f
                .store
                .claim_due_turn_event_deliveries(
                    pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER,
                    now_timestamp_secs() + 600,
                    64,
                )
                .await
                .unwrap();
            for delivery in deliveries {
                f.processor
                    .process_claimed_native_turn_event_delivery(delivery)
                    .await
                    .unwrap();
            }
            if completion_live_delivery(&f.store, other_turn).await.status == "delivered" {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        completion_live_delivery(&f.store, other_turn).await.status,
        "delivered"
    );
    assert!(f.processor.foreground_starts.pending(THREAD, TURN));
    f.processor.release_completed_history_preparation_barrier();
    f.processor.agent_manager.remove_thread(other_thread).await;
    f.close().await;
}

#[tokio::test]
async fn fresh_processor_replays_the_committed_turn_without_starting_provider_again() {
    let f = Fixture::new().await;
    drop(f.accepted(f.start(f.params(TURN)).await).await);
    timeout(Duration::from_secs(10), f.entered.notified())
        .await
        .unwrap();
    f.release.notify_one();
    f.terminal(TURN).await;
    wait_for_thread_manager_turn_status(
        &f.processor.thread_manager,
        THREAD,
        TURN,
        TurnStatus::Completed,
    )
    .await;
    let original = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
    f.close().await;
    let (tx, _rx) = mpsc::channel(256);
    let sessions = Arc::new(SessionManager::new());
    let connection = register_authenticated_test_connection(&sessions, tx).await;
    let restored = MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        f.processor.provider_registry.clone(),
        sessions,
        f.processor.workspace_manager.clone(),
        f.store.clone(),
        f.processor.gateway_secrets.clone(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    let thread = f.store.get_thread_model(THREAD).await.unwrap().unwrap();
    restored
        .thread_manager
        .system_thread_restore_persisted(thread, None)
        .await
        .unwrap();
    let admission =
        test_execution_authorization_admission(&restored, connection, &f.workspace, THREAD).await;
    let replay = timeout(
        Duration::from_secs(10),
        restored.accept_foreground_api_provider_start(
            connection,
            pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            ),
            None,
            f.params(TURN),
            None,
            admission,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(replay.response.turn.id, TURN);
    assert_eq!(replay.response.turn.status, TurnStatus::Completed);
    assert_eq!(
        f.store
            .get_turn_execution(TURN)
            .await
            .unwrap()
            .unwrap()
            .owner_generation,
        original.owner_generation
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    drop(replay);
    restored.shutdown_resilience_workers().await;
}

#[tokio::test]
async fn old_failure_settlement_cannot_change_a_claimed_replacement() {
    for mandatory_ready in [false, true] {
        let f = Fixture::new().await;
        let task = if mandatory_ready {
            f.processor
                .arm_completed_history_preparation_barrier("__foreground_start_before_dispatch__");
            let task = f.start(f.params(TURN)).await;
            f.reached().await;
            f.processor.agent_manager.remove_thread(THREAD).await;
            f.processor.arm_completed_history_preparation_barrier(
                "__foreground_failure_before_settlement__",
            );
            f.processor.release_completed_history_preparation_barrier();
            task
        } else {
            f.store.database_connection().execute_unprepared(
                "CREATE TRIGGER reject_lifecycle BEFORE INSERT ON turn_event WHEN NEW.event_type = 'item/started' BEGIN SELECT RAISE(ABORT, 'fixture lifecycle failure'); END"
            ).await.unwrap();
            f.processor.arm_completed_history_preparation_barrier(
                "__foreground_failure_before_settlement__",
            );
            f.start(f.params(TURN)).await
        };
        f.reached().await; // initial source check passed; writer has not decided
        let original = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
        let at = original.lease_until.timestamp() + 1;
        let replacement = f
            .store
            .claim_expired_turn_execution(&original, &original.owner_id, at, at + 60)
            .await
            .unwrap()
            .unwrap();
        let before = f.store.get_turn(THREAD, TURN).await.unwrap().unwrap();
        let memory = f.processor.thread_manager.turn_get(THREAD, TURN).await;
        // Install a successor actor as well: stale settlement cannot retire it.
        f.processor
            .agent_manager
            .ensure_thread(THREAD, &f.workspace)
            .await
            .unwrap();
        let actor = f.processor.agent_manager.thread_generation(THREAD).await;
        if !mandatory_ready {
            f.store
                .database_connection()
                .execute_unprepared("DROP TRIGGER reject_lifecycle")
                .await
                .unwrap();
        }
        f.processor.release_completed_history_preparation_barrier();
        assert!(
            timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        let current = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
        assert_eq!(current.owner_generation, replacement.owner_generation);
        assert_eq!(current.owner_id, replacement.owner_id);
        assert_eq!(current.status, replacement.status);
        assert_eq!(
            f.store.get_turn(THREAD, TURN).await.unwrap().unwrap(),
            before
        );
        assert_eq!(
            f.processor.thread_manager.turn_get(THREAD, TURN).await,
            memory
        );
        assert_eq!(
            f.processor.agent_manager.thread_generation(THREAD).await,
            actor
        );
        assert!(
            f.store
                .find_unresolved_recovery_job_for_turn(TURN)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(f.calls.load(Ordering::SeqCst), 0);
        f.close().await;
    }
}

#[tokio::test]
async fn replacement_before_initial_activation_prevents_stale_provider_work() {
    let f = Fixture::new().await;
    f.processor.arm_completed_history_preparation_barrier(
        "__native_initial_control_before_registration__",
    );
    let task = f.start(f.params(TURN)).await;
    f.reached().await;
    let original = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
    let at = original.lease_until.timestamp() + 1;
    let replacement = f
        .store
        .claim_expired_turn_execution(&original, &original.owner_id, at, at + 60)
        .await
        .unwrap()
        .unwrap();
    f.processor.release_completed_history_preparation_barrier();
    assert!(
        timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert!(
        f.store
            .native_cancellation_context(TURN)
            .await
            .unwrap()
            .is_none()
    );
    let current = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
    assert_eq!(current.owner_generation, replacement.owner_generation);
    assert_eq!(current.status, replacement.status);
    assert!(
        !f.store
            .native_turn_has_accepted_terminal(TURN)
            .await
            .unwrap()
    );
    assert!(
        f.store
            .find_unresolved_recovery_job_for_turn(TURN)
            .await
            .unwrap()
            .is_none()
    );
    f.close().await;
}

#[tokio::test]
async fn stop_after_terminal_precheck_still_prevents_initial_activation() {
    for phase in [
        "__foreground_start_after_terminal_precheck__",
        "__native_initial_control_before_registration__",
    ] {
        let f = Fixture::new().await;
        f.processor.arm_completed_history_preparation_barrier(phase);
        let task = f.start(f.params(TURN)).await;
        f.reached().await;
        if phase == "__native_initial_control_before_registration__" {
            let processor = f.processor.clone();
            let stop = tokio::spawn(async move {
                processor
                    .mark_turn_interrupted(
                        THREAD.into(),
                        TURN.into(),
                        "Stop before registration".into(),
                    )
                    .await
            });
            // Applied is recorded by the actual actor after its synchronous
            // cancellation token fence, before waiting for registration.
            timeout(Duration::from_secs(10), async {
                loop {
                    if matches!(
                        f.processor
                            .agent_manager
                            .control_operation_status(
                                THREAD,
                                &pioneer_agent::AgentControlOperationId::CancelTurn {
                                    turn_id: TURN.into()
                                },
                            )
                            .await
                            .unwrap(),
                        Some(pioneer_agent::AgentControlOperationStatus::Applied { .. })
                    ) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            f.processor.release_completed_history_preparation_barrier();
            assert!(
                timeout(Duration::from_secs(10), stop)
                    .await
                    .unwrap()
                    .unwrap()
            );
        } else {
            assert!(
                timeout(
                    Duration::from_secs(10),
                    f.processor.mark_turn_interrupted(
                        THREAD.into(),
                        TURN.into(),
                        "Stop after precheck".into(),
                    )
                )
                .await
                .unwrap()
            );
            f.processor.release_completed_history_preparation_barrier();
        }
        drop(f.accepted(task).await);
        assert_eq!(f.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            f.store
                .get_turn(THREAD, TURN)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Interrupted
        );
        f.close().await;
    }
}

#[tokio::test]
async fn published_start_releases_causality_but_retains_capacity_through_unread_extras() {
    let mut f = Fixture::new().await;
    for n in 0..32 {
        let turn = format!("foreground-extra-{n:04}");
        f.processor
            .arm_completed_history_preparation_barrier("__foreground_start_unread_extras__");
        drop(f.accepted(f.start(f.params(&turn)).await).await);
        f.reached().await; // actual unread helper, after start enqueue
        assert!(!f.processor.foreground_starts.pending(THREAD, &turn));
        timeout(Duration::from_secs(10), f.entered.notified())
            .await
            .unwrap();
        f.release.notify_one();
        f.terminal(&turn).await;
        // Existing LIVE delivery, including terminal, proceeds while extras wait.
        timeout(Duration::from_secs(10), async {
            loop {
                let claims = f
                    .store
                    .claim_due_turn_event_deliveries(
                        pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER,
                        now_timestamp_secs() + 600,
                        64,
                    )
                    .await
                    .unwrap();
                for claim in claims {
                    f.processor
                        .process_claimed_native_turn_event_delivery(claim)
                        .await
                        .unwrap();
                }
                if completion_live_delivery(&f.store, &turn).await.status == "delivered" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        wait_for_thread_manager_turn_status(
            &f.processor.thread_manager,
            THREAD,
            &turn,
            TurnStatus::Completed,
        )
        .await;
        timeout(Duration::from_secs(10), async {
            while f
                .processor
                .agent_manager
                .turn_owner_generation(THREAD, &turn)
                .await
                .is_some()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        while f.rx.try_recv().is_ok() {}
    }
    let rejected = timeout(
        Duration::from_secs(10),
        f.start(f.params("foreground-extra-full")).await,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        rejected.is_err(),
        "completed preparation must not release a retained extras task's capacity"
    );
    assert!(
        f.store
            .get_turn(THREAD, "foreground-extra-full")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 32);
    // shutdown cancels every actual held extras future and drains its receipts.
    f.close().await;
}

#[tokio::test]
async fn stdout_progress_uses_the_actual_target_and_waits_for_start_publication() {
    let mut f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier(PUBLICATION);
    drop(f.accepted(f.start(f.params(TURN)).await).await);
    f.reached().await;
    let emit = |marker: &str| pioneer_protocol::AgentProgressEvent::ToolOutputDelta {
        workspace_id: f.workspace.clone(),
        thread_id: THREAD.into(),
        turn_id: TURN.into(),
        item_id: "stdout-progress-item".into(),
        stream: pioneer_protocol::ItemDeltaStream::Stdout,
        delta: marker.into(),
        payload: None,
    };
    timeout(
        Duration::from_secs(10),
        f.processor
            .handle_progress_agent_event(emit("held-direct-stdout")),
    )
    .await
    .unwrap();
    f.no_dependent_events();
    f.processor.release_completed_history_preparation_barrier();
    let _ = recv_notification_by_method(&mut f.rx, events::TURN_STARTED).await;
    // Same actual owner and target after release: this proves the first call
    // was blocked by startup, rather than rejected by a broken fixture.
    f.processor
        .handle_progress_agent_event(pioneer_protocol::AgentProgressEvent::ToolOutputDelta {
            workspace_id: f.workspace.clone(),
            thread_id: THREAD.into(),
            turn_id: TURN.into(),
            item_id: "stdout-progress-item".into(),
            stream: pioneer_protocol::ItemDeltaStream::Stdout,
            delta: "released-direct-stdout".into(),
            payload: None,
        })
        .await;
    let event =
        recv_notification_by_method(&mut f.rx, events::ITEM_COMMAND_EXECUTION_OUTPUT_DELTA).await;
    let n: pioneer_protocol::ItemDeltaNotification =
        serde_json::from_value(event.params.unwrap()).unwrap();
    assert_eq!(
        (
            n.thread_id.as_str(),
            n.turn_id.as_str(),
            n.item_id.as_str(),
            n.delta.as_str()
        ),
        (
            THREAD,
            TURN,
            "stdout-progress-item",
            "released-direct-stdout"
        )
    );
    f.release.notify_one();
    f.terminal(TURN).await;
    f.close().await;
}

#[tokio::test]
async fn prepared_capture_budget_failure_never_accepts_or_activates_provider() {
    let f = Fixture::new().await;
    let mut thread = f.store.get_thread_model(THREAD).await.unwrap().unwrap();
    // Request is small; the retained server-side materialization exceeds the
    // startup capture budget. This exercises the second budget boundary.
    thread.name = Some("x".repeat(8 * 1024 * 1024 + 1));
    f.store
        .upsert_thread_model(
            &thread,
            pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            ),
        )
        .await
        .unwrap();
    f.processor
        .thread_manager
        .system_thread_restore_persisted(thread, None)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(10), f.start(f.params(TURN)).await)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(
        f.store
            .get_turn_runtime_snapshot(TURN)
            .await
            .unwrap()
            .is_some(),
        "preparation must have reached the capture boundary"
    );
    assert_eq!(
        f.store
            .get_turn(THREAD, TURN)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Blocked
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    f.close().await;
}

#[tokio::test]
async fn unfinished_owned_startup_is_discovered_by_a_fresh_process_recovery_owner() {
    let f = Fixture::new().await;
    f.processor
        .arm_completed_history_preparation_barrier("__foreground_start_before_dispatch__");
    let request = f.start(f.params(TURN)).await;
    f.reached().await;
    // All mandatory durable inputs exist, but no actor accepted the payload.
    assert!(
        f.store
            .get_turn_runtime_snapshot(TURN)
            .await
            .unwrap()
            .is_some()
    );
    f.processor.agent_manager.remove_thread(THREAD).await;
    // Drain shutdown through the actual owner. Its incomplete handoff retains
    // an exact durable Recovering receipt even though this process exits.
    timeout(
        Duration::from_secs(10),
        f.processor.foreground_starts.shutdown(),
    )
    .await
    .unwrap();
    assert!(
        timeout(Duration::from_secs(10), request)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let old = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
    assert_eq!(old.status, TurnExecutionStatus::Recovering);
    assert!(
        !f.store
            .native_turn_has_accepted_terminal(TURN)
            .await
            .unwrap()
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    let restored = MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        f.processor.provider_registry.clone(),
        Arc::new(SessionManager::new()),
        f.processor.workspace_manager.clone(),
        f.store.clone(),
        f.processor.gateway_secrets.clone(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    let at = old.lease_until.timestamp() + 1;
    restored
        .recovery_coordinator
        .run_supervision_audits(at, 64)
        .await
        .unwrap();
    let successor = f.store.get_turn_execution(TURN).await.unwrap().unwrap();
    assert_eq!(
        successor.owner_id,
        restored.turn_execution_owner_id.as_ref()
    );
    assert!(successor.owner_generation > old.owner_generation);
    let recovery = f
        .store
        .find_unresolved_recovery_job_for_turn(TURN)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recovery.action,
        pioneer_protocol::RecoveryAction::RestartTurn
    );
    // Repeated discovery retains one canonical recovery job and never launches
    // provider directly; existing due/actor reconciliation owns the attempt.
    restored
        .recovery_coordinator
        .run_supervision_audits(at, 64)
        .await
        .unwrap();
    assert_eq!(
        f.store
            .find_unresolved_recovery_job_for_turn(TURN)
            .await
            .unwrap()
            .unwrap()
            .id,
        recovery.id
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    restored.shutdown_resilience_workers().await;
    f.close().await;
}
