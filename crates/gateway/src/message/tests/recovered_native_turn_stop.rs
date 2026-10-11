use super::*;
use pioneer_client::{core::ClientCore, state::snapshot::ActiveThreadSnapshot};
use pioneer_crud::{TurnExecutionStatus, TurnExecutorKind};
use pioneer_protocol::{
    GatewayNotification, NativeTerminalEffectPreparation, ThreadUpdatedNotification,
};
use pioneer_sqlite::SqliteWriteClass;
use sea_orm::ActiveModelTrait;
use std::sync::atomic::AtomicU64;

#[derive(Clone, Copy)]
enum Scenario {
    Subscribed,
    OpenAfter,
    CancelBeforeAck,
    DurableTerminalBeforeAck,
    CancelRollbackBeforeAck,
    CancelCommitDuringPendingAck,
    AcceptedCompletionDuringCancel,
    ProjectedCompletionDuringCancel,
    RestoreFailure,
}

fn run(scenario: Scenario) {
    run_standard_stack_message_test("recovered native turn control", async move {
        for kind in [TurnExecutorKind::NativeAgent, TurnExecutorKind::ApiProvider] {
            let delivery_orders: &[bool] =
                if matches!(scenario, Scenario::ProjectedCompletionDuringCancel) {
                    &[false, true]
                } else {
                    &[false]
                };
            for &delivery_before_rollback in delivery_orders {
                exercise(kind, scenario, delivery_before_rollback).await;
            }
        }
    });
}

#[test]
fn recovered_native_turn_stop_subscribed_client_and_repeated_preparation() {
    run(Scenario::Subscribed);
}
#[test]
fn recovered_native_turn_stop_open_after_recovery_and_desktop_restart() {
    run(Scenario::OpenAfter);
}
#[test]
fn recovered_native_turn_stop_cancellation_wins_before_pre_start_ack() {
    run(Scenario::CancelBeforeAck);
}
#[test]
fn recovered_native_turn_stop_durable_completion_wins_before_pre_start_ack() {
    run(Scenario::DurableTerminalBeforeAck);
}
#[test]
fn recovered_native_turn_stop_provisional_cancel_rolls_back_without_losing_execution() {
    run(Scenario::CancelRollbackBeforeAck);
}
#[test]
fn recovered_native_turn_stop_provisional_cancel_commit_prevents_provider_work() {
    run(Scenario::CancelCommitDuringPendingAck);
}
#[test]
fn recovered_native_turn_stop_accepted_completion_does_not_publish_provisional_interruption() {
    run(Scenario::AcceptedCompletionDuringCancel);
}
#[test]
fn recovered_native_turn_stop_projected_completion_does_not_publish_conflicting_interruption() {
    run(Scenario::ProjectedCompletionDuringCancel);
}
#[test]
fn recovered_native_turn_stop_restore_failure_rejects_pre_start_ack() {
    run(Scenario::RestoreFailure);
}

struct RecoveryProvider {
    barrier: CancellationBarrierProvider,
    calls: Arc<AtomicU64>,
}
#[async_trait::async_trait]
impl Provider for RecoveryProvider {
    fn name(&self) -> &str {
        "openai"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        self.barrier.capabilities()
    }
    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.barrier.chat(request).await
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![Ok(StreamChunk::delta(response.text))]).boxed())
    }
}

async fn open(
    processor: &MessageProcessor,
    connection: ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    client: &ClientCore,
    workspace: &str,
    thread: &str,
) -> Thread {
    let id = generate_test_request_id("nativestop", "open");
    processor
        .process_request_for_connection(
            connection,
            &json!({
                "jsonrpc":"2.0", "id":id, "method":"thread/start",
                "params":{"workspace_id":workspace,"thread_id":thread}
            })
            .to_string(),
        )
        .await;
    let (response, notification) =
        recv_response_and_notification_by_id_method(rx, &id, events::THREAD_STARTED).await;
    let response: ThreadStartResponse = serde_json::from_value(response.result).unwrap();
    client.upsert_thread(response.thread.clone());
    client.apply_thread_notification(GatewayNotification::from_jsonrpc(notification).unwrap());
    response.thread
}
fn active(client: &ClientCore, thread: &str) -> ActiveThreadSnapshot {
    let coordinator = client.thread_coordinator_snapshot(thread).unwrap();
    ActiveThreadSnapshot::from_coordinator(Some(thread), Some(&coordinator), false)
}
async fn update(rx: &mut mpsc::Receiver<Message>, client: &ClientCore) -> Thread {
    let notification = recv_notification_by_method(rx, events::THREAD_UPDATED).await;
    let GatewayNotification::ThreadUpdated(update) =
        GatewayNotification::from_jsonrpc(notification).unwrap()
    else {
        panic!("ThreadUpdated required");
    };
    let thread = update.thread.clone();
    client.apply_thread_notification(GatewayNotification::ThreadUpdated(update));
    thread
}

fn assert_no_interruption_fanout(rx: &mut mpsc::Receiver<Message>, turn_id: &str) {
    while let Ok(message) = rx.try_recv() {
        let Message::Text(text) = message else {
            continue;
        };
        let value: JsonValue = serde_json::from_str(text.as_str()).unwrap();
        match value.get("method").and_then(JsonValue::as_str) {
            Some(events::THREAD_UPDATED) => {
                let update: ThreadUpdatedNotification =
                    serde_json::from_value(value["params"].clone()).unwrap();
                assert!(
                    update
                        .thread
                        .turns
                        .iter()
                        .all(|turn| turn.id != turn_id || turn.status != TurnStatus::Interrupted),
                    "unconfirmed Interrupted must not become client snapshot"
                );
            }
            Some(events::TURN_FAILED) => {
                let failed: TurnFailedNotification =
                    serde_json::from_value(value["params"].clone()).unwrap();
                assert!(failed.turn.id != turn_id || failed.turn.status != TurnStatus::Interrupted);
            }
            _ => {}
        }
    }
}

fn apply_completion_notification(client: &ClientCore, value: JsonValue, completed: &Turn) -> bool {
    let notification: JsonRpcNotification = serde_json::from_value(value).unwrap();
    let notification = GatewayNotification::from_jsonrpc(notification).unwrap();
    let mut saw_snapshot = false;
    match &notification {
        GatewayNotification::ThreadUpdated(update) => {
            if let Some(turn) = update
                .thread
                .turns
                .iter()
                .find(|turn| turn.id == completed.id)
            {
                assert_ne!(
                    turn.status,
                    TurnStatus::Interrupted,
                    "unconfirmed interruption fanout"
                );
                if turn.status == TurnStatus::Completed {
                    assert_eq!(
                        turn, completed,
                        "terminal snapshot must retain canonical metadata"
                    );
                    saw_snapshot = true;
                }
            }
        }
        GatewayNotification::TurnCompleted(notification)
            if notification.turn.id == completed.id =>
        {
            assert_eq!(&notification.turn, completed);
        }
        GatewayNotification::TurnFailed(notification) if notification.turn.id == completed.id => {
            assert_ne!(notification.turn.status, TurnStatus::Interrupted);
        }
        _ => {}
    }
    client.apply_thread_notification(notification);
    saw_snapshot
}

async fn completion_live_delivery(
    store: &CrudStore,
    turn_id: &str,
) -> pioneer_entity::turn_event_delivery::Model {
    let db = store.with_maintenance_access().database_connection();
    let stream = pioneer_entity::turn_event_projection_stream_state::Entity::find_by_id(turn_id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    pioneer_entity::turn_event_delivery::Entity::find()
        .filter(
            pioneer_entity::turn_event_delivery::Column::EventId
                .eq(stream.accepted_terminal_event_id.unwrap()),
        )
        .filter(
            pioneer_entity::turn_event_delivery::Column::Consumer
                .eq(pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER),
        )
        .one(&db)
        .await
        .unwrap()
        .unwrap()
}

async fn cancel(
    processor: &MessageProcessor,
    connection: ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    client: &ClientCore,
    thread: &str,
    turn: &str,
) {
    let snapshot = active(client, thread);
    assert!(snapshot.can_request_turn_cancel(true));
    let plan = pioneer_client::turns::cancel::plan_turn_cancel_request(
        thread,
        snapshot.in_flight_turn_id.unwrap(),
        false,
        None,
    )
    .unwrap();
    assert_eq!(plan.params.thread_id, thread);
    assert_eq!(plan.params.turn_id, turn);
    let id = generate_test_request_id("nativestop", "cancel");
    processor
        .process_request_for_connection(
            connection,
            &json!({
                "jsonrpc":"2.0", "id":id, "method":"turn/cancel", "params":plan.params
            })
            .to_string(),
        )
        .await;
    let (response, notification) =
        recv_response_and_notification_by_id_method(rx, &id, events::TURN_FAILED).await;
    let response: TurnCancelResponse = serde_json::from_value(response.result).unwrap();
    assert_eq!(response.thread_id, thread);
    assert_eq!(response.turn.id, turn);
    assert_eq!(response.turn.status, TurnStatus::Interrupted);
    client.apply_thread_notification(GatewayNotification::from_jsonrpc(notification).unwrap());
    assert_eq!(active(client, thread).in_flight_turn_id, None);
    assert_eq!(
        processor
            .crud_store
            .get_turn(thread, turn)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Interrupted
    );
    assert_eq!(
        processor
            .crud_store
            .get_turn_execution(turn)
            .await
            .unwrap()
            .unwrap()
            .status,
        TurnExecutionStatus::Interrupted
    );
    assert!(
        processor
            .crud_store
            .native_cancellation_receipt_owned(turn, processor.turn_execution_owner_id.as_ref())
            .await
            .unwrap()
            .is_some()
    );
}

async fn exercise(kind: TurnExecutorKind, scenario: Scenario, delivery_before_rollback: bool) {
    let thread = "thread_recovered_native_control";
    let turn = "turn_recovered_native_control";
    let old_owner = "gateway-before-restart";
    let (tx, mut rx) = mpsc::channel(256);
    let sessions = Arc::new(SessionManager::new());
    let connection = register_authenticated_test_connection(sessions.as_ref(), tx).await;
    let (workspaces, store, workspace) = setup_workspace_manager().await;
    let entered = Arc::new(Notify::new());
    let calls = Arc::new(AtomicU64::new(0));
    let provider = Arc::new(RecoveryProvider {
        barrier: CancellationBarrierProvider {
            entered: entered.clone(),
            release: Arc::new(Notify::new()),
        },
        calls: calls.clone(),
    });
    let mut processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(pioneer_provider::ProviderRegistry::with_provider(
            "openai", provider,
        )),
        sessions,
        workspaces,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    if matches!(
        scenario,
        Scenario::AcceptedCompletionDuringCancel | Scenario::ProjectedCompletionDuringCancel
    ) {
        // Own the existing optional-delivery worker in this fixture. Drive its
        // production claim/process path at controlled points of the cancellation
        // barrier, instead of racing the coalesced background kick.
        processor
            .native_turn_event_delivery_kick_running
            .store(true, Ordering::SeqCst);
    }
    // Seed only the exact durable state left by the old Gateway. Neither its
    // ThreadManager nor its actor is present in this process.
    ensure_test_superuser_execution_turn(&processor, &workspace, thread, turn).await;
    let saved = store.get_turn(thread, turn).await.unwrap().unwrap().1;
    let now = chrono::Utc::now().fixed_offset();
    let runtime = crate::turn_runtime_snapshot::new_turn_runtime_snapshot(
        thread,
        &workspace,
        turn,
        saved.mode,
        &pioneer_agent::AgentTurnHookRuntimeContext::default(),
        "test-model",
        "openai",
        None,
        &HashMap::new(),
        &[UserInput::Text {
            text: "continue original work".into(),
            text_elements: vec![],
        }],
        &[],
        &[],
        &HashMap::new(),
        &[],
        &[],
    )
    .unwrap();
    store.upsert_turn_runtime_snapshot(runtime).await.unwrap();
    let security = pioneer_protocol::TurnExecutionSecuritySnapshot::workspace_write(
        saved.permission_profile.clone(),
        "/tmp/project",
        vec![
            pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                pioneer_protocol::TurnFilesystemAccess::Write,
                "/tmp/project",
            ),
        ],
        now.timestamp_millis(),
    );
    assert!(
        store
            .set_turn_execution_security_snapshot(turn, &security)
            .await
            .unwrap()
    );
    let db = store.with_maintenance_access().database_connection();
    pioneer_entity::turn_execution::ActiveModel {
        turn_id: Set(turn.into()),
        thread_id: Set(thread.into()),
        workspace_id: Set(workspace.clone()),
        executor_kind: Set(kind.as_str().into()),
        executor_key: Set(Some("openai".into())),
        status: Set(TurnExecutionStatus::Running.as_str().into()),
        owner_id: Set(old_owner.into()),
        owner_generation: Set(1),
        lease_until: Set(now - chrono::Duration::seconds(1)),
        heartbeat_at: Set(now - chrono::Duration::seconds(2)),
        started_at: Set(Some(now - chrono::Duration::seconds(2))),
        completed_at: Set(None),
        created_at: Set(now - chrono::Duration::seconds(2)),
        updated_at: Set(now - chrono::Duration::seconds(2)),
    }
    .insert(&db)
    .await
    .unwrap();
    let original_context = NativeTerminalEffectPreparation {
        batch_id: format!("{turn}:original-cancellation"),
        workspace_id: workspace.clone(),
        thread_id: thread.into(),
        turn_id: turn.into(),
        runtime_generation: 1,
        effects: vec![],
    };
    store
        .persist_native_cancellation_context(
            original_context.clone(),
            old_owner,
            now.timestamp(),
            true,
        )
        .await
        .unwrap();
    processor.thread_manager = Arc::new(ThreadManager::new("test-model", "openai"));
    let processor = Arc::new(processor);
    assert!(!processor.agent_manager.has_thread(thread).await);
    assert!(processor.thread_manager.thread_get(thread).await.is_none());
    let weak = Arc::downgrade(&processor);
    processor
        .recovery_coordinator
        .set_listener_starter(Arc::new(move |thread| {
            let weak = weak.clone();
            Box::pin(async move {
                weak.upgrade()
                    .unwrap()
                    .with_database_class(SqliteWriteClass::Critical)
                    .ensure_agent_listener_task(&thread)
                    .await
                    .map_err(|e| format!("{e:#}"))
            })
        }))
        .await;
    let coordinator = processor
        .recovery_coordinator
        .with_crud_store(Arc::new(store.with_maintenance_access()));
    let client = ClientCore::new();
    if !matches!(scenario, Scenario::OpenAfter | Scenario::RestoreFailure) {
        assert!(
            open(&processor, connection, &mut rx, &client, &workspace, thread)
                .await
                .turns
                .is_empty()
        );
        assert_eq!(active(&client, thread).in_flight_turn_id, None);
    }
    let mut others = store.get_thread_model(thread).await.unwrap().unwrap();
    let mut marker = saved.clone();
    marker.id = "other-message".into();
    marker.mode = ThreadMode::Message;
    marker.status = TurnStatus::Completed;
    others.turns = vec![marker.clone()];
    if matches!(scenario, Scenario::RestoreFailure) {
        others.workspace_id = "conflicting-workspace".into();
    }
    processor
        .thread_manager
        .system_thread_restore_persisted(others, None)
        .await
        .unwrap();
    let subscribers = processor
        .thread_manager
        .subscribed_connections(thread)
        .await;
    if !matches!(scenario, Scenario::RestoreFailure) {
        processor
            .arm_completed_history_preparation_barrier("__native_recovery_before_control_ack__");
    }
    // Orphan discovery and admission happen in the same production sweep.
    let start_at = now.timestamp();
    let events = coordinator.run_ready_jobs(start_at, 64).await.unwrap();
    if !matches!(scenario, Scenario::RestoreFailure) {
        assert!(events.iter().any(|event| matches!(event, crate::resilience::RecoveryCoordinatorEvent::RetryAttemptStarted { turn_id, .. } if turn_id == turn)));
    }
    for event in events {
        processor.handle_recovery_event(event, start_at).await;
    }
    if matches!(scenario, Scenario::RestoreFailure) {
        wait_for_cancelled_native_actor_cleanup(&processor.agent_manager, thread).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            processor
                .thread_manager
                .turn_get(thread, turn)
                .await
                .is_none()
        );
        assert!(
            processor
                .commit_durable_agent_event(AgentDurableEvent::NativeCancellationContextPrepared {
                    preparation: original_context.clone(),
                    initial_turn: false,
                })
                .await
                .is_err()
        );
        let jobs = store
            .find_recovery_jobs_by_turn_and_status(
                turn,
                pioneer_protocol::RecoveryJobStatus::Pending,
            )
            .await
            .unwrap();
        assert_eq!(
            jobs.len(),
            1,
            "refused actor must be requeued without waiting for watchdog"
        );
        let job = &jobs[0];
        assert_eq!(job.run_count, 1);
        assert_eq!(job.active_attempt_id, None);
        let source = pioneer_protocol::RecoveryAttemptContext {
            job_id: job.id.clone(),
            attempt_id: job.last_failure_attempt_id.clone().unwrap(),
        };
        assert!(
            store
                .find_recovery_jobs_by_turn_and_status(
                    turn,
                    pioneer_protocol::RecoveryJobStatus::Active
                )
                .await
                .unwrap()
                .is_empty()
        );
        let execution = store.get_turn_execution(turn).await.unwrap().unwrap();
        assert_eq!(execution.status, TurnExecutionStatus::Recovering);
        assert_eq!(execution.executor_kind, kind);
        assert_eq!(execution.thread_id, thread);
        assert_eq!(execution.workspace_id, workspace);
        assert_eq!(
            store
                .get_turn(thread, turn)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::InProgress
        );
        for _ in 0..2 {
            assert!(
                coordinator
                    .record_native_recovery_start_failure(
                        &execution,
                        &source,
                        "duplicate refusal".into(),
                        now_timestamp_secs()
                    )
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                store
                    .get_recovery_job(&job.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .run_count,
                1
            );
            assert_eq!(
                store.get_turn_execution(turn).await.unwrap().unwrap(),
                execution
            );
        }
        // A late admission update cannot undo the committed refusal.
        assert!(
            !store
                .mark_native_recovery_start_running_owned(
                    &execution,
                    &source,
                    now_timestamp_secs(),
                    now_timestamp_secs() + 45
                )
                .await
                .unwrap()
        );
        // Nor may the old refusal change a replacement attempt.
        let claimed = store
            .claim_due_recovery_jobs(job.next_run_at_unix, 45, 1)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, job.id);
        assert!(matches!(
            store
                .mark_claimed_recovery_job_active(
                    &job.id,
                    claimed[0].claim_token.as_deref().unwrap(),
                    "replacement-attempt",
                    job.next_run_at_unix
                )
                .await
                .unwrap(),
            pioneer_crud::ClaimedRecoveryActivation::Activated
        ));
        assert!(
            coordinator
                .record_native_recovery_start_failure(
                    &execution,
                    &source,
                    "late refusal".into(),
                    job.next_run_at_unix
                )
                .await
                .unwrap()
                .is_empty()
        );
        let replacement = store.get_recovery_job(&job.id).await.unwrap().unwrap();
        assert_eq!(
            replacement.status,
            pioneer_protocol::RecoveryJobStatus::Active
        );
        assert_eq!(
            replacement.active_attempt_id.as_deref(),
            Some("replacement-attempt")
        );
        assert_eq!(replacement.run_count, 1);
        assert_eq!(
            store.get_turn_execution(turn).await.unwrap().unwrap(),
            execution
        );
        // Its own refusal still consumes the unchanged policy's finite budget.
        let exhausted = coordinator
            .record_native_recovery_start_failure(
                &execution,
                &pioneer_protocol::RecoveryAttemptContext {
                    job_id: job.id.clone(),
                    attempt_id: "replacement-attempt".into(),
                },
                "replacement refused".into(),
                job.next_run_at_unix,
            )
            .await
            .unwrap();
        assert!(matches!(
            exhausted.as_slice(),
            [crate::resilience::RecoveryCoordinatorEvent::RecoveryExhausted(_)]
        ));
        assert_eq!(
            store
                .get_recovery_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            pioneer_protocol::RecoveryJobStatus::Exhausted
        );
        let exhausted_job = store.get_recovery_job(&job.id).await.unwrap().unwrap();
        assert_eq!(exhausted_job.run_count, 2);
        assert_eq!(exhausted_job.active_attempt_id, None);
        assert_eq!(
            exhausted_job.last_failure_attempt_id.as_deref(),
            Some("replacement-attempt")
        );
        let terminalization = store
            .with_maintenance_access()
            .claim_due_recovery_terminalizations(job.next_run_at_unix, 45, 1)
            .await
            .unwrap();
        assert_eq!(terminalization.len(), 1);
        assert_eq!(terminalization[0].recovery_job_id, job.id);
        assert_eq!(terminalization[0].turn_id, turn);
        assert_eq!(
            terminalization[0].recovery_status,
            pioneer_protocol::RecoveryJobStatus::Exhausted
        );
        while let Ok(Message::Text(payload)) = rx.try_recv() {
            if let Ok(notification) = serde_json::from_str::<JsonRpcNotification>(&payload) {
                assert_ne!(notification.method, events::THREAD_UPDATED);
            }
        }
        return;
    }
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "provider work must wait for control restoration ACK"
    );
    assert_eq!(
        processor
            .thread_manager
            .turn_get(thread, turn)
            .await
            .unwrap()
            .1,
        saved
    );
    assert_eq!(
        processor
            .thread_manager
            .subscribed_connections(thread)
            .await,
        subscribers
    );
    let loaded = if matches!(scenario, Scenario::OpenAfter) {
        open(&processor, connection, &mut rx, &client, &workspace, thread).await
    } else {
        update(&mut rx, &client).await
    };
    assert!(loaded.turns.contains(&saved));
    assert!(loaded.turns.contains(&marker));
    assert_eq!(loaded.turns.len(), 2);
    assert_eq!(
        active(&client, thread).in_flight_turn_id.as_deref(),
        Some(turn)
    );
    assert!(active(&client, thread).can_request_turn_cancel(true));

    if matches!(
        scenario,
        Scenario::AcceptedCompletionDuringCancel | Scenario::ProjectedCompletionDuringCancel
    ) {
        let projected_before_ack = matches!(scenario, Scenario::ProjectedCompletionDuringCancel);
        let delivery_processor = processor.for_background_reconciliation();
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *processor
            .native_cancellation_materialization_barrier
            .lock()
            .await = Some((reached.clone(), release.clone()));
        let id = generate_test_request_id("nativestop", "canonical-wins-cancel");
        let request = json!({"jsonrpc":"2.0","id":id,"method":"turn/cancel",
            "params":{"thread_id":thread,"turn_id":turn}})
        .to_string();
        let mut completed = saved.clone();
        completed.status = TurnStatus::Completed;
        let cancellation = processor.process_request_for_connection(connection, &request);
        let race = async {
            timeout(Duration::from_secs(10), reached.notified())
                .await
                .unwrap();
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(thread, turn)
                    .await
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Interrupted
            );
            assert_eq!(
                store.get_turn(thread, turn).await.unwrap().unwrap().1,
                saved
            );
            let mut preparation = original_context.clone();
            preparation.batch_id = format!("{turn}:canonical-completion");
            assert!(
                processor
                    .handle_durable_agent_event(AgentDurableEvent::NativeTerminalEffectsPrepared {
                        preparation
                    })
                    .await
            );
            store
                .with_critical_writes()
                .accept_native_turn_event_without_projection_for_test(
                    pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(
                        TurnCompletedNotification {
                            workspace_id: workspace.clone(),
                            thread_id: thread.into(),
                            turn: completed.clone(),
                        },
                    ),
                    now_timestamp_secs(),
                    processor.turn_execution_owner_id.as_ref(),
                )
                .await
                .unwrap();
            assert!(store.native_turn_has_accepted_terminal(turn).await.unwrap());
            assert_eq!(
                store.get_turn(thread, turn).await.unwrap().unwrap().1,
                saved,
                "canonical acceptance has not yet projected Completed"
            );
            if projected_before_ack {
                let replay = store
                    .with_maintenance_access()
                    .replay_due_turn_event_projections(now_timestamp_secs(), 64)
                    .await
                    .unwrap();
                assert_eq!(replay.projected, 1);
                assert_eq!(replay.failed, 0);
                assert_eq!(
                    store.get_turn(thread, turn).await.unwrap().unwrap().1,
                    completed
                );
            }
            processor.release_completed_history_preparation_barrier();
            wait_for_cancelled_native_actor_cleanup(&processor.agent_manager, thread).await;
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_no_interruption_fanout(&mut rx, turn);
            for _ in 0..2 {
                assert!(
                    !processor
                        .handle_durable_agent_event(
                            AgentDurableEvent::NativeCancellationContextPrepared {
                                preparation: original_context.clone(),
                                initial_turn: false,
                            }
                        )
                        .await
                );
                assert_eq!(
                    processor
                        .thread_manager
                        .turn_get(thread, turn)
                        .await
                        .unwrap()
                        .1
                        .status,
                    TurnStatus::Interrupted,
                    "failed synchronization must preserve provisional state for rollback"
                );
                assert_no_interruption_fanout(&mut rx, turn);
            }
            if delivery_before_rollback {
                assert!(projected_before_ack);
                delivery_processor
                    .process_due_native_turn_event_deliveries(now_timestamp_secs(), 64)
                    .await
                    .unwrap();
                let delivery = completion_live_delivery(&store, turn).await;
                assert_eq!(
                    delivery.status, "failed",
                    "conflict must retain the live delivery obligation"
                );
                assert_eq!(delivery.attempt_count, 1);
                assert!(delivery.delivered_at.is_none());
                assert_eq!(
                    processor
                        .thread_manager
                        .turn_get(thread, turn)
                        .await
                        .unwrap()
                        .1
                        .status,
                    TurnStatus::Interrupted,
                    "delivery cannot overwrite unresolved cancellation"
                );
                assert_no_interruption_fanout(&mut rx, turn);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            // The real cancellation commit conflicts with accepted Completed.
            // Its existing rollback, not a fixture reset, releases local state.
            release.notify_one();
        };
        timeout(Duration::from_secs(10), async {
            tokio::join!(cancellation, race);
        })
        .await
        .unwrap();
        let mut saw_error = false;
        let mut saw_snapshot = false;
        while !saw_error || (projected_before_ack && !saw_snapshot) {
            let payload = recv_text_timeout(&mut rx, Duration::from_secs(10)).await;
            let value: JsonValue = serde_json::from_str(&payload).unwrap();
            if value.get("id").and_then(JsonValue::as_str) == Some(&id) {
                assert!(
                    value.get("error").is_some(),
                    "losing cancellation must report failure"
                );
                saw_error = true;
            } else {
                saw_snapshot |= apply_completion_notification(&client, value, &completed);
            }
        }
        assert_eq!(
            processor
                .thread_manager
                .turn_get(thread, turn)
                .await
                .unwrap()
                .1,
            if projected_before_ack {
                completed.clone()
            } else {
                saved.clone()
            }
        );
        assert!(!store.native_cancellation_was_accepted(turn).await.unwrap());
        assert_no_interruption_fanout(&mut rx, turn);
        if !projected_before_ack {
            // Live delivery can precede aggregate projection. Keep its real
            // obligation until it can synchronize the rolled-back control state.
            delivery_processor
                .process_due_native_turn_event_deliveries(now_timestamp_secs(), 64)
                .await
                .unwrap();
            let delivery = completion_live_delivery(&store, turn).await;
            assert_eq!(delivery.status, "failed");
            assert!(delivery.delivered_at.is_none());
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(thread, turn)
                    .await
                    .unwrap()
                    .1,
                saved
            );
            assert_no_interruption_fanout(&mut rx, turn);
            assert!(!saw_snapshot);
            let replay = store
                .with_maintenance_access()
                .replay_due_turn_event_projections(now_timestamp_secs(), 64)
                .await
                .unwrap();
            assert_eq!(replay.projected, 1);
            assert_eq!(replay.failed, 0);
        }
        assert_eq!(
            store.get_turn(thread, turn).await.unwrap().unwrap().1,
            completed
        );
        // Drive the existing outbox using its recorded deadline. This also
        // retries a delivery which encountered provisional Interrupted earlier.
        let delivery = completion_live_delivery(&store, turn).await;
        let deliveries = delivery_processor
            .crud_store
            .claim_due_turn_event_deliveries(
                pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER,
                std::cmp::max(delivery.next_run_at.timestamp(), now_timestamp_secs()),
                64,
            )
            .await
            .unwrap();
        let mut completion_delivery = None;
        for delivery in deliveries {
            if matches!(&delivery.event.payload, pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(n)
                if n.turn.id == turn)
            {
                completion_delivery = Some(delivery.clone());
            }
            delivery_processor
                .process_claimed_native_turn_event_delivery(delivery)
                .await;
        }
        let completion_delivery =
            completion_delivery.expect("canonical completion must be claimable");
        let mut saw_completed = false;
        while !saw_completed || !saw_snapshot {
            let payload = recv_text_timeout(&mut rx, Duration::from_secs(10)).await;
            let value: JsonValue = serde_json::from_str(&payload).unwrap();
            saw_completed |=
                value.get("method").and_then(JsonValue::as_str) == Some(events::TURN_COMPLETED);
            saw_snapshot |= apply_completion_notification(&client, value, &completed);
        }
        assert_eq!(active(&client, thread).in_flight_turn_id, None);
        assert_eq!(
            processor
                .thread_manager
                .turn_get(thread, turn)
                .await
                .unwrap()
                .1,
            completed
        );
        let merged = processor.thread_manager.thread_get(thread).await.unwrap();
        assert!(merged.turns.contains(&marker));
        assert_eq!(merged.turns.len(), 2);
        assert_eq!(merged.status, pioneer_protocol::ThreadStatus::Idle);
        let delivered = completion_live_delivery(&store, turn).await;
        assert_eq!(delivered.status, "delivered");
        // Redeliver the same real claim through the production consumer: this
        // repeats canonical delivery without another append or pre-start call.
        delivery_processor
            .process_claimed_native_turn_event_delivery(completion_delivery)
            .await;
        let repeated = serde_json::to_value(
            recv_notification_by_method(&mut rx, events::TURN_COMPLETED).await,
        )
        .unwrap();
        assert!(!apply_completion_notification(
            &client, repeated, &completed
        ));
        assert_eq!(active(&client, thread).in_flight_turn_id, None);
        assert_eq!(
            processor
                .thread_manager
                .turn_get(thread, turn)
                .await
                .unwrap()
                .1,
            completed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_no_interruption_fanout(&mut rx, turn);
        assert_eq!(
            store
                .native_cancellation_context(turn)
                .await
                .unwrap()
                .unwrap()
                .preparation,
            original_context
        );
        assert_eq!(
            processor
                .thread_manager
                .subscribed_connections(thread)
                .await,
            subscribers
        );
        assert_eq!(
            store
                .get_turn_execution(turn)
                .await
                .unwrap()
                .unwrap()
                .status,
            TurnExecutionStatus::Completed
        );
        let desktop = ClientCore::new();
        let reopened = open(
            &processor, connection, &mut rx, &desktop, &workspace, thread,
        )
        .await;
        assert!(reopened.turns.contains(&completed));
        assert!(reopened.turns.contains(&marker));
        assert_eq!(active(&desktop, thread).in_flight_turn_id, None);

        // Controlled delivery and redelivery are complete. Restore automatic
        // kicks so the next Turn's real cancellation delivers TurnFailed.
        processor
            .native_turn_event_delivery_kick_running
            .store(false, Ordering::SeqCst);

        // Exercise ordinary authenticated admission, including its foreground
        // running-Turn gate, instead of a concurrent child or Message start.
        let next_turn = "turn_after_recovered_completion";
        let next_id = generate_test_request_id("nativestop", "next-turn");
        processor.process_request_for_connection(connection, &json!({
            "jsonrpc":"2.0","id":next_id,"method":"turn/start",
            "params": {
                "thread_id":thread,"turn_id":next_turn,
                "mode":"Chat","model":"test-model","model_provider":"openai",
                "execution_backend":{"type":"apiProvider","provider":"openai"},
                "permission_profile":pioneer_protocol::TurnPermissionProfileSelection::full_access(),
                "input":[{"type":"text","text":"new work after recovery"}]
            }
        }).to_string()).await;
        let (response, notification) =
            recv_response_and_notification_by_id_method(&mut rx, &next_id, events::TURN_STARTED)
                .await;
        let response: TurnStartResponse = serde_json::from_value(response.result).unwrap();
        assert_eq!(response.turn.id, next_turn);
        assert_eq!(response.turn.status, TurnStatus::InProgress);
        client.apply_thread_notification(GatewayNotification::from_jsonrpc(notification).unwrap());
        assert_eq!(
            active(&client, thread).in_flight_turn_id.as_deref(),
            Some(next_turn)
        );
        timeout(Duration::from_secs(10), entered.notified())
            .await
            .unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "only the new Turn may start provider work"
        );
        assert_eq!(
            processor
                .agent_manager
                .active_turn_id(thread)
                .await
                .as_deref(),
            Some(next_turn)
        );
        assert_eq!(
            processor
                .thread_manager
                .turn_get(thread, turn)
                .await
                .unwrap()
                .1,
            completed
        );
        assert_eq!(
            store.get_turn(thread, turn).await.unwrap().unwrap().1,
            completed
        );
        cancel(&processor, connection, &mut rx, &client, thread, next_turn).await;
        wait_for_cancelled_native_actor_cleanup(&processor.agent_manager, thread).await;
        assert_eq!(
            processor
                .thread_manager
                .turn_get(thread, turn)
                .await
                .unwrap()
                .1,
            completed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        return;
    }

    if matches!(
        scenario,
        Scenario::CancelRollbackBeforeAck | Scenario::CancelCommitDuringPendingAck
    ) {
        let rollback = matches!(scenario, Scenario::CancelRollbackBeforeAck);
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *processor
            .native_cancellation_materialization_barrier
            .lock()
            .await = Some((reached.clone(), release.clone()));
        if rollback {
            *processor
                .native_cancellation_materialization_failure
                .lock()
                .await = Some(sea_orm::DbErr::Custom(
                "injected cancellation commit failure".into(),
            ));
        }
        let jobs = store
            .find_recovery_jobs_by_turn_and_status(
                turn,
                pioneer_protocol::RecoveryJobStatus::Active,
            )
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        let job = jobs[0].clone();
        let source = pioneer_protocol::RecoveryAttemptContext {
            job_id: job.id.clone(),
            attempt_id: job.active_attempt_id.clone().unwrap(),
        };
        let execution = store.get_turn_execution(turn).await.unwrap().unwrap();
        let id = generate_test_request_id("nativestop", "racing-cancel");
        let request = json!({"jsonrpc":"2.0", "id":id, "method":"turn/cancel",
            "params":{"thread_id":thread,"turn_id":turn}})
        .to_string();
        let cancellation = processor.process_request_for_connection(connection, &request);
        let race = async {
            timeout(Duration::from_secs(10), reached.notified())
                .await
                .unwrap();
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(thread, turn)
                    .await
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Interrupted
            );
            assert_eq!(
                store
                    .get_turn(thread, turn)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::InProgress
            );
            assert!(!store.native_turn_has_accepted_terminal(turn).await.unwrap());
            assert!(!store.native_cancellation_was_accepted(turn).await.unwrap());
            processor.arm_completed_history_preparation_barrier(
                "__native_recovery_transition_pending__",
            );
            processor.release_completed_history_preparation_barrier();
            timeout(
                Duration::from_secs(10),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "provisional terminal cannot admit provider work"
            );
            assert!(
                processor
                    .agent_manager
                    .active_turn_id(thread)
                    .await
                    .is_some(),
                "uncertain terminal must retain recovery actor"
            );
            assert_eq!(
                store
                    .get_recovery_job(&job.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_protocol::RecoveryJobStatus::Active
            );
            // The same running seed never overwrites provisional local state.
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(thread, turn)
                    .await
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Interrupted
            );
            release.notify_one();
            processor.release_completed_history_preparation_barrier();
        };
        timeout(Duration::from_secs(10), async {
            tokio::join!(cancellation, race);
        })
        .await
        .unwrap();
        if rollback {
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(thread, turn)
                    .await
                    .unwrap()
                    .1
                    .status,
                TurnStatus::InProgress
            );
            assert_eq!(
                store
                    .get_turn(thread, turn)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::InProgress
            );
            assert!(!store.native_cancellation_was_accepted(turn).await.unwrap());
            timeout(Duration::from_secs(10), entered.notified())
                .await
                .unwrap();
            let mut saw_error = false;
            let mut saw_running = false;
            while !saw_error || !saw_running {
                let payload = recv_text_timeout(&mut rx, Duration::from_secs(10)).await;
                let value: JsonValue = serde_json::from_str(&payload).unwrap();
                if value.get("id").and_then(JsonValue::as_str) == Some(&id) {
                    assert!(
                        value.get("error").is_some(),
                        "failed durable cancellation must report failure"
                    );
                    saw_error = true;
                } else if value.get("method").and_then(JsonValue::as_str)
                    == Some(events::THREAD_UPDATED)
                {
                    let notification: JsonRpcNotification = serde_json::from_value(value).unwrap();
                    client.apply_thread_notification(
                        GatewayNotification::from_jsonrpc(notification).unwrap(),
                    );
                    saw_running = true;
                }
            }
            assert_eq!(
                active(&client, thread).in_flight_turn_id.as_deref(),
                Some(turn)
            );
            assert!(active(&client, thread).can_request_turn_cancel(true));
            let current = store.get_recovery_job(&job.id).await.unwrap().unwrap();
            assert_eq!(current.status, pioneer_protocol::RecoveryJobStatus::Active);
            assert_eq!(current.active_attempt_id, job.active_attempt_id);
            assert_eq!(current.run_count, 0);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            cancel(&processor, connection, &mut rx, &client, thread, turn).await;
        } else {
            let (response, notification) =
                recv_response_and_notification_by_id_method(&mut rx, &id, events::TURN_FAILED)
                    .await;
            let response: TurnCancelResponse = serde_json::from_value(response.result).unwrap();
            assert_eq!(response.turn.id, turn);
            assert_eq!(response.turn.status, TurnStatus::Interrupted);
            client.apply_thread_notification(
                GatewayNotification::from_jsonrpc(notification).unwrap(),
            );
            assert_eq!(active(&client, thread).in_flight_turn_id, None);
            assert_eq!(
                store
                    .get_turn(thread, turn)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Interrupted
            );
            assert_eq!(
                store
                    .get_turn_execution(turn)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TurnExecutionStatus::Interrupted
            );
            assert!(store.native_cancellation_was_accepted(turn).await.unwrap());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
        wait_for_cancelled_native_actor_cleanup(&processor.agent_manager, thread).await;
        // A delayed or repeated refusal cannot turn committed cancellation into retry.
        for _ in 0..2 {
            assert!(
                coordinator
                    .record_native_recovery_start_failure(
                        &execution,
                        &source,
                        "late pre-start refusal".into(),
                        now_timestamp_secs()
                    )
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        assert!(
            store
                .find_recovery_jobs_by_turn_and_status(
                    turn,
                    pioneer_protocol::RecoveryJobStatus::Active
                )
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .find_recovery_jobs_by_turn_and_status(
                    turn,
                    pioneer_protocol::RecoveryJobStatus::Pending
                )
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            processor
                .thread_manager
                .subscribed_connections(thread)
                .await,
            subscribers
        );
        assert!(
            processor
                .thread_manager
                .thread_get(thread)
                .await
                .unwrap()
                .turns
                .contains(&marker)
        );
        return;
    }

    if matches!(scenario, Scenario::CancelBeforeAck) {
        cancel(&processor, connection, &mut rx, &client, thread, turn).await;
    } else if matches!(scenario, Scenario::DurableTerminalBeforeAck) {
        let mut completion = original_context.clone();
        completion.batch_id = format!("{turn}:completion");
        assert!(
            processor
                .handle_durable_agent_event(AgentDurableEvent::NativeTerminalEffectsPrepared {
                    preparation: completion
                })
                .await
        );
        assert!(
            processor
                .handle_durable_agent_event(AgentDurableEvent::TurnCompleted {
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    recovery: None
                })
                .await
        );
    }
    processor.release_completed_history_preparation_barrier();
    if matches!(
        scenario,
        Scenario::CancelBeforeAck | Scenario::DurableTerminalBeforeAck
    ) {
        wait_for_cancelled_native_actor_cleanup(&processor.agent_manager, thread).await;
        // Wait for the resumed handler to deliver its final, fenced snapshot.
        let terminal = update(&mut rx, &client).await;
        assert_eq!(
            terminal.turns.iter().find(|t| t.id == turn).unwrap().status,
            if matches!(scenario, Scenario::CancelBeforeAck) {
                TurnStatus::Interrupted
            } else {
                TurnStatus::Completed
            }
        );
        assert_eq!(active(&client, thread).in_flight_turn_id, None);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        return;
    }
    timeout(Duration::from_secs(10), entered.notified())
        .await
        .unwrap();
    let generation = processor
        .agent_manager
        .turn_owner_generation(thread, turn)
        .await
        .unwrap();
    let context_before = store
        .native_cancellation_context(turn)
        .await
        .unwrap()
        .unwrap()
        .preparation;
    assert_eq!(context_before, original_context);
    for _ in 0..2 {
        assert!(
            processor
                .handle_durable_agent_event(AgentDurableEvent::NativeCancellationContextPrepared {
                    preparation: original_context.clone(),
                    initial_turn: false,
                })
                .await
        );
        update(&mut rx, &client).await;
        coordinator.run_ready_jobs(start_at, 64).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            processor
                .agent_manager
                .turn_owner_generation(thread, turn)
                .await,
            Some(generation)
        );
        assert_eq!(
            processor
                .thread_manager
                .thread_get(thread)
                .await
                .unwrap()
                .turns
                .len(),
            2
        );
    }
    let desktop = ClientCore::new();
    open(
        &processor, connection, &mut rx, &desktop, &workspace, thread,
    )
    .await;
    assert_eq!(
        active(&desktop, thread).in_flight_turn_id.as_deref(),
        Some(turn)
    );
    cancel(&processor, connection, &mut rx, &client, thread, turn).await;
    wait_for_cancelled_native_actor_cleanup(&processor.agent_manager, thread).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .native_cancellation_context(turn)
            .await
            .unwrap()
            .unwrap()
            .preparation,
        original_context
    );
    assert!(
        store
            .persist_native_cancellation_context(
                original_context,
                processor.turn_execution_owner_id.as_ref(),
                now_timestamp_secs(),
                false
            )
            .await
            .is_err()
    );
}
