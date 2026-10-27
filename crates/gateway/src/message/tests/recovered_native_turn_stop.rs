use super::*;
use pioneer_client::{core::ClientCore, state::snapshot::ActiveThreadSnapshot};
use pioneer_crud::{TurnExecutionStatus, TurnExecutorKind};
use pioneer_protocol::{
    GatewayNotification, NativeTerminalEffectPreparation, ThreadUpdatedNotification,
    TurnStartParams,
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
    cancelled: Arc<AtomicU64>,
}

struct PendingProviderCall {
    cancelled: Arc<AtomicU64>,
    returned: bool,
}

impl Drop for PendingProviderCall {
    fn drop(&mut self) {
        if !self.returned {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
        }
    }
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
        let mut pending = PendingProviderCall {
            cancelled: self.cancelled.clone(),
            returned: false,
        };
        let response = self.barrier.chat(request).await;
        pending.returned = true;
        response
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

async fn deliver_completion_live(
    processor: &MessageProcessor,
    store: &CrudStore,
    turn_id: &str,
) -> pioneer_crud::ClaimedTurnEventDeliveryRecord {
    let completion = completion_live_delivery(store, turn_id).await;
    let now = std::cmp::max(completion.next_run_at.timestamp(), now_timestamp_secs());
    // Each claim admits only the eligible causal head for a Turn. Deliver its
    // predecessors through the real consumer before claiming the completion.
    for _ in 0..64 {
        let deliveries = processor
            .crud_store
            .claim_due_turn_event_deliveries(pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER, now, 64)
            .await
            .unwrap();
        assert!(
            !deliveries.is_empty(),
            "completion delivery must have an eligible causal head"
        );
        let mut completion_claim = None;
        for delivery in deliveries {
            if delivery.id == completion.id {
                completion_claim = Some(delivery.clone());
            }
            processor
                .process_claimed_native_turn_event_delivery(delivery)
                .await;
        }
        if let Some(claim) = completion_claim {
            return claim;
        }
    }
    panic!("completion must be reached within the fixture's bounded delivery batch");
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
        cancelled: Arc::new(AtomicU64::new(0)),
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
                let _ = deliver_completion_live(&delivery_processor, &store, turn).await;
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
        let mut saw_response = false;
        let mut saw_snapshot = false;
        while !saw_response || (projected_before_ack && !saw_snapshot) {
            let payload = recv_text_timeout(&mut rx, Duration::from_secs(10)).await;
            let value: JsonValue = serde_json::from_str(&payload).unwrap();
            if value.get("id").and_then(JsonValue::as_str) == Some(&id) {
                if projected_before_ack {
                    // The rollback boundary has synchronized accepted Completed.
                    // turn/cancel idempotently returns that existing outcome.
                    let response: TurnCancelResponse =
                        serde_json::from_value(value["result"].clone()).unwrap();
                    assert_eq!(response.workspace_id, workspace);
                    assert_eq!(response.thread_id, thread);
                    assert_eq!(response.turn, completed);
                } else {
                    assert!(
                        value.get("error").is_some(),
                        "unprojected completion leaves cancellation rejected"
                    );
                }
                saw_response = true;
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
            let _ = deliver_completion_live(&delivery_processor, &store, turn).await;
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
        let completion_delivery = deliver_completion_live(&delivery_processor, &store, turn).await;
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

#[derive(Clone, Copy)]
enum InitialStopPhase {
    ActorBeforeRegistration,
    RegistrationWinsEarlyCommit,
    ActorAppearsBeforeRegistration,
    RegistrationAfterControlAckWindow,
    CanonicalStorageUnavailable,
    RegistrationStorageUnavailable,
    RegistrationWriteStorageUnavailable,
    DirectChildBeforeRegistration,
    NativeTaskBeforeRegistration,
    GraphFenceStorageUnavailable,
    AcceptedProjectionPending,
    ProjectionStorageUnavailable,
    BeforeActorProjectionStorageUnavailable,
    GraphFenceProjectionUnavailable,
}

// These phase tests use one executor thread: observing the control registry's
// Applied process control observes its synchronous out-of-band cancellation.
// Registration remains behind the barrier; no sleep or provider race is used.
#[tokio::test]
async fn public_native_stop_finishes_actor_admission_before_context_registration() {
    exercise_initial_native_stop(InitialStopPhase::ActorBeforeRegistration).await;
}

#[tokio::test]
async fn public_native_stop_switches_to_registered_context_when_early_writer_loses_phase() {
    exercise_initial_native_stop(InitialStopPhase::RegistrationWinsEarlyCommit).await;
}

#[tokio::test]
async fn public_native_stop_follows_actor_appearing_before_registration() {
    exercise_initial_native_stop(InitialStopPhase::ActorAppearsBeforeRegistration).await;
}

#[tokio::test]
async fn public_native_stop_retains_completion_after_the_process_ack_window() {
    exercise_initial_native_stop(InitialStopPhase::RegistrationAfterControlAckWindow).await;
}

#[tokio::test]
async fn public_initial_stop_retains_completion_across_a_real_terminal_storage_failure() {
    exercise_initial_native_stop(InitialStopPhase::CanonicalStorageUnavailable).await;
}

#[tokio::test]
async fn public_initial_stop_retains_completion_across_registration_storage_failure() {
    exercise_initial_native_stop(InitialStopPhase::RegistrationStorageUnavailable).await;
}

#[tokio::test]
async fn public_initial_stop_retains_completion_across_registration_write_failure() {
    exercise_initial_native_stop(InitialStopPhase::RegistrationWriteStorageUnavailable).await;
}

#[tokio::test]
async fn public_root_stop_finishes_a_direct_threads_child_after_delayed_registration() {
    exercise_initial_native_stop(InitialStopPhase::DirectChildBeforeRegistration).await;
}

#[tokio::test]
async fn native_task_cancel_ack_keeps_the_actor_owning_delayed_durable_publication() {
    exercise_initial_native_stop(InitialStopPhase::NativeTaskBeforeRegistration).await;
}

#[tokio::test]
async fn public_root_stop_rediscovers_unfinished_children_after_a_graph_fence() {
    exercise_initial_native_stop(InitialStopPhase::GraphFenceStorageUnavailable).await;
}

#[tokio::test]
async fn public_initial_stop_waits_for_accepted_cancellation_projection() {
    exercise_initial_native_stop(InitialStopPhase::AcceptedProjectionPending).await;
}

#[tokio::test]
async fn public_initial_stop_retains_actor_after_a_real_projection_failure() {
    exercise_initial_native_stop(InitialStopPhase::ProjectionStorageUnavailable).await;
}

#[tokio::test]
async fn public_early_stop_and_repeat_wait_for_real_projection_without_an_actor() {
    exercise_initial_native_stop(InitialStopPhase::BeforeActorProjectionStorageUnavailable).await;
}

#[tokio::test]
async fn public_root_stop_retries_a_direct_child_after_real_projection_failure() {
    exercise_initial_native_stop(InitialStopPhase::GraphFenceProjectionUnavailable).await;
}

async fn exercise_initial_native_stop(phase: InitialStopPhase) {
    let thread = "thread_initial_native_stop";
    let turn = "turn_initial_native_stop";
    let (tx, mut rx) = mpsc::channel(256);
    let sessions = Arc::new(SessionManager::new());
    let connection = register_authenticated_test_connection(sessions.as_ref(), tx).await;
    let (workspaces, store, workspace) = setup_workspace_manager().await;
    ensure_test_superuser_execution_authority(store.as_ref()).await;
    let calls = Arc::new(AtomicU64::new(0));
    let provider_cancelled = Arc::new(AtomicU64::new(0));
    let provider = Arc::new(RecoveryProvider {
        barrier: CancellationBarrierProvider {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        },
        calls: calls.clone(),
        cancelled: provider_cancelled.clone(),
    });
    let processor = Arc::new(MessageProcessor::new(
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
    ));
    let client = ClientCore::new();
    let opened = open(&processor, connection, &mut rx, &client, &workspace, thread).await;
    store
        .upsert_thread_model(
            &opened,
            pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            ),
        )
        .await
        .unwrap();
    processor
        .thread_manager
        .system_thread_restore_persisted(opened, None)
        .await
        .unwrap();
    // Use ordinary admission/preparation, retaining the prepared runtime at its
    // actual pre-dispatch boundary rather than inventing a cancellation context.
    let admission =
        test_execution_authorization_admission(&processor, connection, &workspace, thread).await;
    let params: TurnStartParams = serde_json::from_value(json!({
        "thread_id":thread, "turn_id":turn,
        "model":"test-model", "model_provider":"openai", "mode":ThreadMode::Agent,
        "execution_backend":{"type":"apiProvider","provider":"openai"},
        "permission_profile":pioneer_protocol::TurnPermissionProfileSelection::full_access(),
        "input":[{"type":"text","text":"stop before the first model call"}]
    }))
    .unwrap();
    let prepared = processor
        .prepare_api_provider_turn_start(
            connection,
            pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            ),
            None,
            params,
            None,
            crate::authorization::ExecutionAdmissionEntryPoint::InteractiveTurn,
            admission,
        )
        .await
        .unwrap();
    let crate::message::turn_handlers::ApiProviderTurnAdmission::New(prepared) = prepared else {
        panic!("first admission must prepare a new runtime");
    };
    let owner = store.get_turn_execution(turn).await.unwrap().unwrap();
    let db = store.database_connection();
    let response = pioneer_crud::load_agent_turn_response(&db, turn)
        .await
        .unwrap()
        .unwrap();
    let resource = pioneer_crud::load_agent_execution_resource_state(&db, &response.execution_id)
        .await
        .unwrap()
        .unwrap();
    let permit_id = resource
        .permit_id
        .clone()
        .expect("ordinary admission must acquire its permit");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let cancel_id = generate_test_request_id("initialstop", "cancel");
    let request = json!({"jsonrpc":"2.0","id":cancel_id,"method":"turn/cancel",
        "params":{"thread_id":thread,"turn_id":turn}})
    .to_string();
    match phase {
        InitialStopPhase::BeforeActorProjectionStorageUnavailable => {
            drop(prepared);
            // Preparation creates an idle actor before dispatch. Remove that
            // idle actor to exercise the durable Stop path without a runtime.
            assert!(
                processor
                    .agent_manager
                    .active_turn_id(thread)
                    .await
                    .is_none()
            );
            processor.agent_manager.remove_thread(thread).await;
            assert!(!processor.agent_manager.has_thread(thread).await);
            db.execute_unprepared("CREATE TRIGGER reject_early_projection BEFORE UPDATE OF status ON turn WHEN NEW.status = 'interrupted' BEGIN SELECT RAISE(ABORT, 'fixture projection unavailable'); END").await.unwrap();
            for suffix in ["first-pending", "repeat-pending"] {
                let id = generate_test_request_id("initialstop", suffix);
                let mut pending: JsonValue = serde_json::from_str(&request).unwrap();
                pending["id"] = json!(id);
                processor
                    .process_request_for_connection(connection, &pending.to_string())
                    .await;
                recv_error_by_id(&mut rx, &id).await;
                assert!(store.native_turn_has_accepted_terminal(turn).await.unwrap());
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
                assert!(!processor.agent_manager.has_thread(thread).await);
                assert_eq!(calls.load(Ordering::SeqCst), 0);
            }
            db.execute_unprepared("DROP TRIGGER reject_early_projection")
                .await
                .unwrap();
            let replay = store
                .with_maintenance_access()
                .replay_due_turn_event_projections(now_timestamp_secs() + 120, 64)
                .await
                .unwrap();
            assert!(replay.projected > 0);
            processor
                .process_request_for_connection(connection, &request)
                .await;
        }
        InitialStopPhase::NativeTaskBeforeRegistration => {
            processor.bind_task_bridge().await;
            processor.arm_completed_history_preparation_barrier(
                "__native_initial_control_before_registration__",
            );
            let created = create_task_for_test(
                &processor,
                test_task_create_params(&workspace, thread, turn, "cancel a real native Task", 3),
            )
            .await
            .unwrap();
            let run = created.run.unwrap();
            let child = wait_for_child_lineage_for_run(store.clone(), &run.id).await;
            timeout(
                Duration::from_secs(10),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            let child_owner = store
                .get_turn_execution(&child.child_turn_id)
                .await
                .unwrap()
                .unwrap();
            assert!(child_owner.started_at.is_some());
            assert!(
                store
                    .native_cancellation_context(&child.child_turn_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            // This invokes TaskAgentExecutor::cancel_run and its ordinary
            // AgentManager CancelTurn, not the Gateway initial Stop receipt.
            timeout(
                Duration::from_secs(1),
                processor.task_runtime.service().cancel_task(
                    pioneer_tasks::TaskMutationContext {
                        actor_id: Some(authenticated_test_superuser().principal_id.to_string()),
                        ..Default::default()
                    },
                    pioneer_protocol::TaskCancelParams {
                        task_id: created.task.id.clone(),
                        reason: Some("one Task cancellation".into()),
                        scope: pioneer_protocol::TaskCancelScope::FullSubtree,
                    },
                ),
            )
            .await
            .unwrap()
            .unwrap();
            timeout(Duration::from_secs(10), async {
                loop {
                    if matches!(
                        processor
                            .agent_manager
                            .control_operation_status(
                                &child.child_thread_id,
                                &pioneer_agent::AgentControlOperationId::CancelTurn {
                                    turn_id: child.child_turn_id.clone()
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
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(3)).await;
            tokio::task::yield_now().await;
            assert!(
                processor
                    .agent_manager
                    .has_thread(&child.child_thread_id)
                    .await
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            tokio::time::resume();
            processor.release_completed_history_preparation_barrier();
            timeout(Duration::from_secs(10), async {
                loop {
                    if store
                        .get_turn(&child.child_thread_id, &child.child_turn_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .1
                        .status
                        == TurnStatus::Interrupted
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let stopped = store
                .get_turn_execution(&child.child_turn_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (stopped.owner_id, stopped.owner_generation),
                (child_owner.owner_id, child_owner.owner_generation)
            );
            assert_eq!(stopped.status, TurnExecutionStatus::Interrupted);
            assert_eq!(
                store.get_task_run(&run.id).await.unwrap().unwrap().status,
                pioneer_protocol::TaskRunStatus::Cancelled
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            wait_for_cancelled_native_actor_cleanup(
                &processor.agent_manager,
                &child.child_thread_id,
            )
            .await;
            drop(prepared);
            processor
                .process_request_for_connection(connection, &request)
                .await;
        }
        InitialStopPhase::DirectChildBeforeRegistration
        | InitialStopPhase::GraphFenceStorageUnavailable
        | InitialStopPhase::GraphFenceProjectionUnavailable => {
            let tools = processor
                .materialize_agent_action_tools(pioneer_agent::TurnToolContext {
                    workspace_id: workspace.clone(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                })
                .await
                .unwrap();
            let mut children = Vec::new();
            let mut child_owners = std::collections::BTreeMap::new();
            let mut child_permits = std::collections::BTreeMap::new();
            for index in 0..if matches!(
                phase,
                InitialStopPhase::GraphFenceStorageUnavailable
                    | InitialStopPhase::GraphFenceProjectionUnavailable
            ) {
                2
            } else {
                1
            } {
                let binding = processor.agent_action_binding(turn).await.unwrap();
                let creation = binding
                    .options
                    .thread_creation_options
                    .iter()
                    .find(|o| o.label == "Current collaboration")
                    .unwrap();
                let created = super::thread_launch_choices::call(
                    &tools,
                    "thread_create",
                    &format!("initial-stop-child-create-{index}"),
                    json!({"optionId":creation.id}),
                )
                .await;
                let target = created["targetOptions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|option| {
                        !binding
                            .options
                            .target_options
                            .iter()
                            .any(|old| Some(old.id.as_str()) == option["id"].as_str())
                    })
                    .unwrap()["id"]
                    .clone();
                let start_id = format!("initial-stop-child-start-{index}");
                super::thread_launch_choices::call(&tools,"threads_turn_start",&start_id,json!({
                "targetOptionId":target,"input":[{"type":"text","text":"Run in the root work graph"}],
                "launch":{"useDefaultReasoning":true}
            })).await;
                let row = pioneer_crud::load_agent_action_outbox(
                    &db,
                    crate::authorization::action_id_for_call(&start_id).as_str(),
                )
                .await
                .unwrap()
                .unwrap();
                let payload: JsonValue = serde_json::from_str(&row.payload_json).unwrap();
                let child_turn = payload["dispatch"]["turn_id"].as_str().unwrap();
                child_owners.insert(
                    child_turn.to_owned(),
                    store.get_turn_execution(child_turn).await.unwrap().unwrap(),
                );
                children.push(payload);
            }
            if matches!(phase, InitialStopPhase::GraphFenceProjectionUnavailable) {
                // Both descendants must be inside real provider work when the
                // public root Stop accepts the first interruption. Never release
                // the provider: dropping its future is the cancellation proof.
                crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                    .await
                    .unwrap();
                timeout(Duration::from_secs(10), async {
                    while calls.load(Ordering::SeqCst) < children.len() as u64 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                for payload in &children {
                    let child_thread = payload["dispatch"]["thread_id"].as_str().unwrap();
                    let child_turn = payload["dispatch"]["turn_id"].as_str().unwrap();
                    assert_eq!(
                        processor
                            .agent_manager
                            .active_turn_id(child_thread)
                            .await
                            .as_deref(),
                        Some(child_turn)
                    );
                    assert!(
                        store
                            .native_cancellation_context(child_turn)
                            .await
                            .unwrap()
                            .is_some()
                    );
                    let resource = pioneer_crud::load_agent_execution_resource_state(
                        &db,
                        payload["spawned_execution_id"].as_str().unwrap(),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                    assert_eq!(resource.status, "running");
                    child_permits.insert(child_turn.to_owned(), resource.permit_id.unwrap());
                }
                assert_eq!(provider_cancelled.load(Ordering::SeqCst), 0);
            }
            if matches!(
                phase,
                InitialStopPhase::GraphFenceStorageUnavailable
                    | InitialStopPhase::GraphFenceProjectionUnavailable
            ) {
                let read = Arc::new(Notify::new());
                let continue_commit = Arc::new(Notify::new());
                *processor
                    .native_cancellation_materialization_barrier
                    .lock()
                    .await = Some((read.clone(), continue_commit.clone()));
                let mut first_request: JsonValue = serde_json::from_str(&request).unwrap();
                let failed_id = generate_test_request_id("initialstop", "graph-storage");
                first_request["id"] = json!(failed_id);
                let first_stop = tokio::spawn({
                    let processor = processor.clone();
                    async move {
                        processor
                            .process_request_for_connection(connection, &first_request.to_string())
                            .await
                    }
                });
                timeout(Duration::from_secs(10), read.notified())
                    .await
                    .unwrap();
                let mut first_turn = None;
                for payload in &children {
                    let child_thread = payload["dispatch"]["thread_id"].as_str().unwrap();
                    let child_turn = payload["dispatch"]["turn_id"].as_str().unwrap();
                    if processor
                        .thread_manager
                        .turn_get(child_thread, child_turn)
                        .await
                        .unwrap()
                        .1
                        .status
                        == TurnStatus::Interrupted
                    {
                        assert!(first_turn.replace(child_turn.to_owned()).is_none());
                    }
                }
                let first_turn = first_turn
                    .expect("the first child holds provisional interruption before its writer");
                // The graph fence has committed; fail the actual first child's
                // append or projection, irrespective of traversal order.
                let trigger = if matches!(phase, InitialStopPhase::GraphFenceProjectionUnavailable)
                {
                    format!(
                        "CREATE TRIGGER fail_graph_child BEFORE UPDATE OF status ON turn WHEN NEW.id='{first_turn}' AND NEW.status='interrupted' BEGIN SELECT RAISE(ABORT,'fixture projection unavailable'); END"
                    )
                } else {
                    format!(
                        "CREATE TRIGGER fail_graph_child BEFORE INSERT ON turn_event WHEN NEW.turn_id='{first_turn}' BEGIN SELECT RAISE(ABORT,'fixture storage unavailable'); END"
                    )
                };
                db.execute_unprepared(&trigger).await.unwrap();
                continue_commit.notify_one();
                timeout(Duration::from_secs(10), first_stop)
                    .await
                    .unwrap()
                    .unwrap();
                recv_error_by_id(&mut rx, &failed_id).await;
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
                for payload in &children {
                    let child_thread = payload["dispatch"]["thread_id"].as_str().unwrap();
                    let child_turn = payload["dispatch"]["turn_id"].as_str().unwrap();
                    let execution = payload["spawned_execution_id"].as_str().unwrap();
                    assert_eq!(
                        pioneer_crud::load_agent_execution(&db, execution)
                            .await
                            .unwrap()
                            .unwrap()
                            .status,
                        "cancelled"
                    );
                    assert_eq!(
                        store
                            .get_turn(child_thread, child_turn)
                            .await
                            .unwrap()
                            .unwrap()
                            .1
                            .status,
                        if child_turn == first_turn {
                            TurnStatus::InProgress
                        } else {
                            TurnStatus::Interrupted
                        }
                    );
                }
                if matches!(phase, InitialStopPhase::GraphFenceProjectionUnavailable) {
                    assert!(
                        store
                            .native_turn_has_accepted_terminal(&first_turn)
                            .await
                            .unwrap()
                    );
                    timeout(Duration::from_secs(10), async {
                        while provider_cancelled.load(Ordering::SeqCst) != children.len() as u64 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    for payload in &children {
                        wait_for_cancelled_native_actor_cleanup(
                            &processor.agent_manager,
                            payload["dispatch"]["thread_id"].as_str().unwrap(),
                        )
                        .await;
                    }
                    let id = generate_test_request_id("initialstop", "graph-repeat-pending");
                    let mut pending: JsonValue = serde_json::from_str(&request).unwrap();
                    pending["id"] = json!(id);
                    processor
                        .process_request_for_connection(connection, &pending.to_string())
                        .await;
                    recv_error_by_id(&mut rx, &id).await;
                }
                db.execute_unprepared("DROP TRIGGER fail_graph_child")
                    .await
                    .unwrap();
                if matches!(phase, InitialStopPhase::GraphFenceProjectionUnavailable) {
                    store
                        .with_maintenance_access()
                        .replay_due_turn_event_projections(now_timestamp_secs() + 120, 64)
                        .await
                        .unwrap();
                }
                processor
                    .process_request_for_connection(connection, &request)
                    .await;
                for payload in &children {
                    let child_thread = payload["dispatch"]["thread_id"].as_str().unwrap();
                    let child_turn = payload["dispatch"]["turn_id"].as_str().unwrap();
                    assert_eq!(
                        store
                            .get_turn(child_thread, child_turn)
                            .await
                            .unwrap()
                            .unwrap()
                            .1
                            .status,
                        TurnStatus::Interrupted
                    );
                    let stopped = store.get_turn_execution(child_turn).await.unwrap().unwrap();
                    let original = &child_owners[child_turn];
                    assert_eq!(stopped.owner_id, original.owner_id);
                    assert_eq!(stopped.owner_generation, original.owner_generation);
                    assert_eq!(stopped.status, TurnExecutionStatus::Interrupted);
                    assert_eq!(
                        pioneer_crud::load_agent_execution_resource_state(
                            &db,
                            payload["spawned_execution_id"].as_str().unwrap()
                        )
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                        "cancelled"
                    );
                    if matches!(phase, InitialStopPhase::GraphFenceProjectionUnavailable) {
                        wait_for_cancelled_native_actor_cleanup(
                            &processor.agent_manager,
                            child_thread,
                        )
                        .await;
                        assert_eq!(
                            pioneer_entity::agent_running_permit::Entity::find_by_id(
                                child_permits[child_turn].clone(),
                            )
                            .one(&db)
                            .await
                            .unwrap()
                            .unwrap()
                            .status,
                            "released"
                        );
                        assert_eq!(
                            processor
                                .thread_manager
                                .turn_get(child_thread, child_turn)
                                .await
                                .unwrap()
                                .1,
                            store
                                .get_turn(child_thread, child_turn)
                                .await
                                .unwrap()
                                .unwrap()
                                .1
                        );
                    }
                }
                if matches!(phase, InitialStopPhase::GraphFenceProjectionUnavailable) {
                    assert_eq!(
                        provider_cancelled.load(Ordering::SeqCst),
                        children.len() as u64
                    );
                }
                drop(prepared);
            } else {
                let payload = &children[0];
                let child_thread = payload["dispatch"]["thread_id"].as_str().unwrap();
                let child_turn = payload["dispatch"]["turn_id"].as_str().unwrap();
                let child_execution = payload["spawned_execution_id"].as_str().unwrap();
                let child = pioneer_crud::load_agent_execution(&db, child_execution)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    child.parent_execution_id.as_deref(),
                    Some(response.execution_id.as_str())
                );
                assert!(child.parent_task_id.is_none());
                let child_owner = store.get_turn_execution(child_turn).await.unwrap().unwrap();
                processor.arm_completed_history_preparation_barrier(
                    "__native_initial_control_before_registration__",
                );
                crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                    .await
                    .unwrap();
                timeout(
                    Duration::from_secs(10),
                    processor.wait_for_completed_history_preparation_barrier(),
                )
                .await
                .unwrap();
                assert!(
                    store
                        .get_turn_execution(child_turn)
                        .await
                        .unwrap()
                        .unwrap()
                        .started_at
                        .is_some()
                );
                let child_permit =
                    pioneer_crud::load_agent_execution_resource_state(&db, child_execution)
                        .await
                        .unwrap()
                        .unwrap()
                        .permit_id
                        .expect("admitted child must own a permit");
                let stop = tokio::spawn({
                    let processor = processor.clone();
                    async move {
                        processor
                            .process_request_for_connection(connection, &request)
                            .await
                    }
                });
                timeout(Duration::from_secs(10), async {
                    loop {
                        if matches!(
                            processor
                                .agent_manager
                                .control_operation_status(
                                    child_thread,
                                    &pioneer_agent::AgentControlOperationId::CancelTurn {
                                        turn_id: child_turn.into()
                                    }
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
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(3)).await;
                tokio::task::yield_now().await;
                assert!(processor.agent_manager.has_thread(child_thread).await);
                assert!(!stop.is_finished());
                tokio::time::resume();
                processor.release_completed_history_preparation_barrier();
                timeout(Duration::from_secs(10), stop)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    store
                        .get_turn(child_thread, child_turn)
                        .await
                        .unwrap()
                        .unwrap()
                        .1
                        .status,
                    TurnStatus::Interrupted
                );
                let stopped = store.get_turn_execution(child_turn).await.unwrap().unwrap();
                assert_eq!(stopped.owner_id, child_owner.owner_id);
                assert_eq!(stopped.owner_generation, child_owner.owner_generation);
                assert_eq!(stopped.status, TurnExecutionStatus::Interrupted);
                let child_resource =
                    pioneer_crud::load_agent_execution_resource_state(&db, child_execution)
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(child_resource.status, "cancelled");
                assert_eq!(
                    pioneer_crud::load_agent_execution(&db, child_execution)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    "cancelled"
                );
                assert_eq!(
                    pioneer_entity::agent_running_permit::Entity::find_by_id(child_permit)
                        .one(&db)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    "released"
                );
                wait_for_cancelled_native_actor_cleanup(&processor.agent_manager, child_thread)
                    .await;
                drop(prepared); // the parent never activated
            }
        }
        InitialStopPhase::ActorBeforeRegistration
        | InitialStopPhase::RegistrationAfterControlAckWindow
        | InitialStopPhase::CanonicalStorageUnavailable
        | InitialStopPhase::RegistrationStorageUnavailable
        | InitialStopPhase::RegistrationWriteStorageUnavailable
        | InitialStopPhase::AcceptedProjectionPending
        | InitialStopPhase::ProjectionStorageUnavailable => {
            processor.arm_completed_history_preparation_barrier(
                "__native_initial_control_before_registration__",
            );
            processor
                .dispatch_prepared_api_provider_turn_start(prepared)
                .await;
            timeout(
                Duration::from_secs(10),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            assert!(
                store
                    .native_cancellation_context(turn)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                processor
                    .agent_manager
                    .turn_owner_waiting_for_cancellation_context(thread, turn)
                    .await
                    .is_some()
            );
            if matches!(phase, InitialStopPhase::AcceptedProjectionPending) {
                store
                    .accept_turn_event_before_projection_for_test(
                        pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(
                            pioneer_protocol::ItemCompletedNotification {
                                workspace_id: workspace.clone(),
                                thread_id: thread.into(),
                                turn_id: turn.into(),
                                item: pioneer_protocol::TurnItem::AgentMessage {
                                    id: "pending-stop-predecessor".into(),
                                    text: "prior durable item".into(),
                                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                                    markdown: None,
                                    markdown_version: None,
                                },
                            },
                        ),
                        now_timestamp_secs(),
                    )
                    .await
                    .unwrap();
            }
            let stop = {
                let processor = processor.clone();
                tokio::spawn(async move {
                    processor
                        .process_request_for_connection(connection, &request)
                        .await
                })
            };
            timeout(Duration::from_secs(10), async {
                loop {
                    if matches!(
                        processor
                            .agent_manager
                            .control_operation_status(
                                thread,
                                &pioneer_agent::AgentControlOperationId::CancelTurn {
                                    turn_id: turn.into()
                                }
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
            assert!(
                !stop.is_finished(),
                "process admission must not acknowledge durable Stop"
            );
            if matches!(phase, InitialStopPhase::RegistrationAfterControlAckWindow) {
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(3)).await;
                tokio::task::yield_now().await;
                assert!(processor.agent_manager.has_thread(thread).await);
                assert!(!stop.is_finished());
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                tokio::time::resume();
            }
            if matches!(phase, InitialStopPhase::RegistrationWriteStorageUnavailable) {
                // The guard can read storage; only the real immutable context
                // INSERT fails after the Stop has been handed to the actor.
                db.execute_unprepared(&format!("CREATE TRIGGER fail_initial_registration BEFORE INSERT ON native_cancellation_context WHEN NEW.turn_id='{turn}' BEGIN SELECT RAISE(ABORT,'fixture registration storage unavailable'); END"))
                    .await.unwrap();
                processor.arm_completed_history_preparation_barrier(
                    "__initial_stop_registration_failed__",
                );
                processor.release_completed_history_preparation_barrier();
                timeout(
                    Duration::from_secs(10),
                    processor.wait_for_completed_history_preparation_barrier(),
                )
                .await
                .unwrap();
                assert!(processor.agent_manager.has_thread(thread).await);
                assert!(!stop.is_finished());
                assert!(
                    store
                        .native_cancellation_context(turn)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(!store.native_turn_has_accepted_terminal(turn).await.unwrap());
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                db.execute_unprepared("DROP TRIGGER fail_initial_registration")
                    .await
                    .unwrap();
            }
            if matches!(phase, InitialStopPhase::RegistrationStorageUnavailable) {
                processor.arm_completed_history_preparation_barrier(
                    "__native_initial_control_after_registration__",
                );
                processor.release_completed_history_preparation_barrier();
                timeout(
                    Duration::from_secs(10),
                    processor.wait_for_completed_history_preparation_barrier(),
                )
                .await
                .unwrap();
                assert!(
                    store
                        .native_cancellation_context(turn)
                        .await
                        .unwrap()
                        .is_some()
                );
                // The next publication reads this table in the lease guard,
                // before prepare_native_turn_control. Exercise that real Query
                // failure, rather than waiting for the later registration hook.
                db.execute_unprepared("ALTER TABLE native_cancellation_context RENAME TO unavailable_cancellation_context").await.unwrap();
                processor.arm_completed_history_preparation_barrier(
                    "__initial_stop_registration_guard_failed__",
                );
                processor.release_completed_history_preparation_barrier();
                timeout(
                    Duration::from_secs(10),
                    processor.wait_for_completed_history_preparation_barrier(),
                )
                .await
                .unwrap();
                assert!(processor.agent_manager.has_thread(thread).await);
                assert!(!stop.is_finished());
                assert!(!store.native_turn_has_accepted_terminal(turn).await.unwrap());
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                db.execute_unprepared("ALTER TABLE unavailable_cancellation_context RENAME TO native_cancellation_context").await.unwrap();
            }
            if matches!(phase, InitialStopPhase::CanonicalStorageUnavailable) {
                // Registration can commit, but the actual canonical append fails.
                // The control receipt must retain its owner through that failure.
                db.execute_unprepared(&format!("CREATE TRIGGER fail_initial_stop BEFORE INSERT ON turn_event WHEN NEW.turn_id='{turn}' BEGIN SELECT RAISE(ABORT,'fixture terminal storage unavailable'); END"))
                    .await.unwrap();
                processor.arm_completed_history_preparation_barrier(
                    "__initial_stop_terminal_commit_failed__",
                );
                processor.release_completed_history_preparation_barrier();
                timeout(
                    Duration::from_secs(10),
                    processor.wait_for_completed_history_preparation_barrier(),
                )
                .await
                .unwrap();
                assert!(
                    store
                        .native_cancellation_context(turn)
                        .await
                        .unwrap()
                        .is_some()
                );
                assert!(!store.native_turn_has_accepted_terminal(turn).await.unwrap());
                assert!(!stop.is_finished());
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                db.execute_unprepared("DROP TRIGGER fail_initial_stop")
                    .await
                    .unwrap();
            }
            if matches!(
                phase,
                InitialStopPhase::AcceptedProjectionPending
                    | InitialStopPhase::ProjectionStorageUnavailable
            ) {
                if matches!(phase, InitialStopPhase::ProjectionStorageUnavailable) {
                    db.execute_unprepared("CREATE TRIGGER reject_owned_projection BEFORE UPDATE OF status ON turn WHEN NEW.status = 'interrupted' BEGIN SELECT RAISE(ABORT, 'fixture projection unavailable'); END").await.unwrap();
                }
                processor.arm_completed_history_preparation_barrier(
                    "__initial_stop_terminal_commit_failed__",
                );
                processor.release_completed_history_preparation_barrier();
                timeout(
                    Duration::from_secs(10),
                    processor.wait_for_completed_history_preparation_barrier(),
                )
                .await
                .unwrap();
                assert!(store.native_turn_has_accepted_terminal(turn).await.unwrap());
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
                assert!(!stop.is_finished());
                assert!(processor.agent_manager.has_thread(thread).await);
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                if matches!(phase, InitialStopPhase::ProjectionStorageUnavailable) {
                    // Let the first real projector failure reach classification.
                    // A permanent refusal loses the actor before this next retry.
                    processor.arm_completed_history_preparation_barrier(
                        "__initial_stop_terminal_completion_pending__",
                    );
                    processor.release_completed_history_preparation_barrier();
                    timeout(
                        Duration::from_secs(10),
                        processor.wait_for_completed_history_preparation_barrier(),
                    )
                    .await
                    .unwrap();
                    assert!(!stop.is_finished());
                    assert!(processor.agent_manager.has_thread(thread).await);
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
                    db.execute_unprepared("DROP TRIGGER reject_owned_projection")
                        .await
                        .unwrap();
                }
                let replay = store
                    .with_maintenance_access()
                    .replay_due_turn_event_projections(now_timestamp_secs() + 120, 64)
                    .await
                    .unwrap();
                assert!(replay.projected > 0);
            }
            processor.release_completed_history_preparation_barrier();
            timeout(Duration::from_secs(10), stop)
                .await
                .unwrap()
                .unwrap();
        }
        InitialStopPhase::RegistrationWinsEarlyCommit
        | InitialStopPhase::ActorAppearsBeforeRegistration => {
            let read = Arc::new(Notify::new());
            let continue_commit = Arc::new(Notify::new());
            *processor
                .native_cancellation_materialization_barrier
                .lock()
                .await = Some((read.clone(), continue_commit.clone()));
            let stop = {
                let processor = processor.clone();
                tokio::spawn(async move {
                    processor
                        .process_request_for_connection(connection, &request)
                        .await
                })
            };
            timeout(Duration::from_secs(10), read.notified())
                .await
                .unwrap();
            assert!(
                store
                    .native_cancellation_context(turn)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                processor
                    .agent_manager
                    .turn_owner_generation(thread, turn)
                    .await
                    .is_none()
            );
            let actor_appears = matches!(phase, InitialStopPhase::ActorAppearsBeforeRegistration);
            processor.arm_completed_history_preparation_barrier(if actor_appears {
                "__native_initial_control_before_registration__"
            } else {
                "__native_initial_control_after_registration__"
            });
            processor
                .dispatch_prepared_api_provider_turn_start(prepared)
                .await;
            timeout(
                Duration::from_secs(10),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            assert_eq!(
                store
                    .native_cancellation_context(turn)
                    .await
                    .unwrap()
                    .is_some(),
                !actor_appears
            );
            assert!(
                store
                    .get_turn_execution(turn)
                    .await
                    .unwrap()
                    .unwrap()
                    .started_at
                    .is_some()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            continue_commit.notify_one();
            if actor_appears {
                timeout(Duration::from_secs(10), async {
                    loop {
                        if matches!(
                            processor
                                .agent_manager
                                .control_operation_status(
                                    thread,
                                    &pioneer_agent::AgentControlOperationId::CancelTurn {
                                        turn_id: turn.into()
                                    }
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
                assert!(!stop.is_finished());
                processor.release_completed_history_preparation_barrier();
            }
            timeout(Duration::from_secs(10), stop)
                .await
                .unwrap()
                .unwrap();
            // The same Stop committed with the real context before this ACK.
            assert!(store.native_turn_has_accepted_terminal(turn).await.unwrap());
            processor.release_completed_history_preparation_barrier();
        }
    }
    let (reply, _) =
        recv_response_and_notification_by_id_method(&mut rx, &cancel_id, events::TURN_FAILED).await;
    let reply: TurnCancelResponse = serde_json::from_value(reply.result).unwrap();
    assert_eq!(reply.turn.status, TurnStatus::Interrupted);
    let terminal = store.get_turn(thread, turn).await.unwrap().unwrap().1;
    assert_eq!(terminal, reply.turn);
    assert_eq!(
        processor
            .thread_manager
            .turn_get(thread, turn)
            .await
            .unwrap()
            .1,
        terminal
    );
    let stopped = store.get_turn_execution(turn).await.unwrap().unwrap();
    assert_eq!(stopped.owner_id, owner.owner_id);
    assert_eq!(stopped.owner_generation, owner.owner_generation);
    assert_eq!(stopped.status, TurnExecutionStatus::Interrupted);
    if !matches!(
        phase,
        InitialStopPhase::DirectChildBeforeRegistration
            | InitialStopPhase::GraphFenceStorageUnavailable
            | InitialStopPhase::NativeTaskBeforeRegistration
            | InitialStopPhase::BeforeActorProjectionStorageUnavailable
            | InitialStopPhase::GraphFenceProjectionUnavailable
    ) {
        assert!(
            store
                .native_cancellation_receipt_owned(turn, processor.turn_execution_owner_id.as_ref())
                .await
                .unwrap()
                .is_some()
        );
    }
    let execution = pioneer_crud::load_agent_execution(&db, &response.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.status, "cancelled");
    let resource = pioneer_crud::load_agent_execution_resource_state(&db, &response.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resource.status, "cancelled");
    let permit = pioneer_entity::agent_running_permit::Entity::find_by_id(permit_id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(permit.status, "released");
    wait_for_cancelled_native_actor_cleanup(&processor.agent_manager, thread).await;
    let expected_calls = if matches!(phase, InitialStopPhase::GraphFenceProjectionUnavailable) {
        2
    } else {
        0
    };
    assert_eq!(
        calls.load(Ordering::SeqCst),
        expected_calls,
        "confirmed Stop must fence delayed or repeated provider work"
    );
    let repeat_id = generate_test_request_id("initialstop", "repeat");
    processor
        .process_request_for_connection(
            connection,
            &json!({"jsonrpc":"2.0","id":repeat_id,"method":"turn/cancel",
        "params":{"thread_id":thread,"turn_id":turn}})
            .to_string(),
        )
        .await;
    let repeat = recv_response_by_id(&mut rx, &repeat_id).await;
    let repeat: TurnCancelResponse = serde_json::from_value(repeat.result).unwrap();
    assert_eq!(repeat.turn, terminal);
    assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
}
