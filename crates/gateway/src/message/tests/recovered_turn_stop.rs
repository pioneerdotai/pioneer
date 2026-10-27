use super::*;
use crate::message::turn_handlers::CliRuntimeRecoveryStartFailure;
use pioneer_client::{core::ClientCore, state::snapshot::ActiveThreadSnapshot};
use pioneer_protocol::{GatewayNotification, ThreadUpdatedNotification};

#[derive(Clone, Copy)]
pub(super) enum Scenario {
    OpenAfterRecovery,
    SubscribedBeforeRecovery,
    CancelDuringRecovery,
    RunningAttemptAfterRuntimeLoss,
    ExecutionChild,
    TerminalBeforeRepeat,
    RestoreFailure,
    ObservationActiveBeforeOpen,
    ObservationActiveSubscribed,
    ObservationActiveRestoreFailure,
}

fn run(scenario: Scenario) {
    // Reuse the persisted binding, authorization, attempt and recovery-job
    // fixture. The CLI session is a recording fake; no process is launched.
    let thread = std::thread::Builder::new()
        .name("recovered-turn-stop".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let _runtime_observer = pioneer_observability::runtime::register(
                runtime.handle(),
                pioneer_observability::runtime::RuntimeRole::GatewayPatch,
            );
            _runtime_observer.block_on(
                &runtime,
                message_future(run_interrupted_cli_runtime_turn_recovery_scenario(
                    matches!(
                        scenario,
                        Scenario::ObservationActiveBeforeOpen
                            | Scenario::ObservationActiveSubscribed
                            | Scenario::ObservationActiveRestoreFailure
                    ),
                    false,
                    matches!(scenario, Scenario::ExecutionChild),
                    false,
                    Some(scenario),
                )),
            );
        })
        .unwrap();
    thread.join().unwrap();
}

#[test]
fn recovered_turn_stop_open_after_recovery_cancels_original_turn() {
    run(Scenario::OpenAfterRecovery);
}

#[test]
fn recovered_turn_stop_subscribed_client_recovers_and_cancels_original_turn() {
    run(Scenario::SubscribedBeforeRecovery);
}

#[test]
fn recovered_turn_stop_cancel_during_recovery_uses_existing_transition_gate() {
    run(Scenario::CancelDuringRecovery);
}

#[test]
fn recovered_turn_stop_running_attempt_early_exit_restores_lost_server_and_client_state() {
    run(Scenario::RunningAttemptAfterRuntimeLoss);
}

#[test]
fn recovered_turn_stop_restores_execution_child_instead_of_continuation_parent() {
    run(Scenario::ExecutionChild);
}

#[test]
fn recovered_turn_stop_repeat_publishes_terminal_runtime_instead_of_saved_running_turn() {
    run(Scenario::TerminalBeforeRepeat);
}

#[test]
fn recovered_turn_stop_restore_failure_does_not_start_native_attempt_or_publish_running() {
    run(Scenario::RestoreFailure);
}

#[test]
fn recovered_turn_stop_active_observation_restores_subscribed_client_without_native_start() {
    run(Scenario::ObservationActiveSubscribed);
}

#[test]
fn recovered_turn_stop_active_observation_restores_client_opened_after_recovery_without_native_start()
 {
    run(Scenario::ObservationActiveBeforeOpen);
}

#[test]
fn recovered_turn_stop_active_observation_restore_failure_does_not_succeed_recovery() {
    run(Scenario::ObservationActiveRestoreFailure);
}

async fn open(
    processor: &MessageProcessor,
    connection_id: ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    client: &ClientCore,
    thread_id: &str,
    workspace_id: &str,
) -> Thread {
    let request_id = generate_test_request_id("recoveredstop", "open");
    processor
        .process_request_for_connection(
            connection_id,
            &json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "thread/start",
                "params": { "thread_id": thread_id, "workspace_id": workspace_id }
            })
            .to_string(),
        )
        .await;
    let (response, started) =
        recv_response_and_notification_by_id_method(rx, &request_id, events::THREAD_STARTED).await;
    let response: ThreadStartResponse = serde_json::from_value(response.result).unwrap();
    client.upsert_thread(response.thread.clone());
    client.apply_thread_notification(GatewayNotification::from_jsonrpc(started).unwrap());
    response.thread
}

fn active(client: &ClientCore, thread_id: &str) -> ActiveThreadSnapshot {
    let coordinator = client.thread_coordinator_snapshot(thread_id).unwrap();
    ActiveThreadSnapshot::from_coordinator(Some(thread_id), Some(&coordinator), false)
}

async fn receive_update(rx: &mut mpsc::Receiver<Message>, client: &ClientCore) -> Thread {
    let notification = recv_notification_by_method(rx, events::THREAD_UPDATED).await;
    let GatewayNotification::ThreadUpdated(update) =
        GatewayNotification::from_jsonrpc(notification).unwrap()
    else {
        panic!("recovery must deliver the existing ThreadUpdated protocol");
    };
    let thread = update.thread.clone();
    client.apply_thread_notification(GatewayNotification::ThreadUpdated(update));
    thread
}

pub(super) async fn exercise(
    mut processor: MessageProcessor,
    connection_id: ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    cli_session: Arc<RecordingCliRuntimeSession>,
    event: crate::resilience::RecoveryCoordinatorEvent,
    scenario: Scenario,
) {
    if matches!(
        scenario,
        Scenario::ObservationActiveBeforeOpen
            | Scenario::ObservationActiveSubscribed
            | Scenario::ObservationActiveRestoreFailure
    ) {
        exercise_active_observation(processor, connection_id, rx, cli_session, event, scenario)
            .await;
        return;
    }
    let crate::resilience::RecoveryCoordinatorEvent::CliRuntimeRetryAttemptRequested(request) =
        event
    else {
        panic!("fixture must exercise the real CLI recovery entry point");
    };
    let request = *request;
    let binding = &request.binding;
    let thread_id = binding.thread_id.clone();
    let turn_id = binding.turn_id.clone();
    let workspace_id = binding.workspace_id.clone();
    let saved_turn = processor
        .crud_store
        .get_turn(&thread_id, &turn_id)
        .await
        .unwrap()
        .unwrap()
        .1;

    // Lose just the Gateway's ThreadManager state, as on restart. Durable
    // recovery and native attempts remain those of the original Pioneer Turn.
    processor.thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let mut manager = processor.thread_manager.clone();
    assert!(manager.turn_get(&thread_id, &turn_id).await.is_none());
    while rx.try_recv().is_ok() {}
    let client = ClientCore::new();

    if matches!(scenario, Scenario::RestoreFailure) {
        let mut conflict = processor
            .crud_store
            .get_thread_model(&thread_id)
            .await
            .unwrap()
            .unwrap();
        conflict.workspace_id = "different-workspace".into();
        conflict.turns.clear();
        manager
            .system_thread_restore_persisted(conflict, None)
            .await
            .unwrap();
        assert!(matches!(
            processor
                .start_cli_runtime_recovery_attempt(request.clone())
                .await,
            Err(CliRuntimeRecoveryStartFailure::Unavailable { .. })
        ));
        assert!(manager.turn_get(&thread_id, &turn_id).await.is_none());
        assert!(cli_session.turn_starts.lock().await.is_empty());
        assert!(
            processor
                .crud_store
                .get_cli_runtime_turn_attempt_by_recovery_attempt(&request.recovery_attempt_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            rx.try_recv().is_err(),
            "failed restoration must not publish a running snapshot"
        );
        return;
    }

    if matches!(scenario, Scenario::ExecutionChild) {
        assert_ne!(thread_id, binding.continuation_thread_id);
        assert!(matches!(
            processor
                .start_cli_runtime_recovery_attempt(request.clone())
                .await,
            Ok(true)
        ));
        assert_eq!(
            manager.turn_get(&thread_id, &turn_id).await.unwrap().1,
            saved_turn
        );
        assert!(
            manager
                .thread_get(&binding.continuation_thread_id)
                .await
                .is_none()
        );
        assert!(matches!(
            processor
                .start_cli_runtime_recovery_attempt(request.clone())
                .await,
            Ok(false)
        ));
        assert_eq!(
            manager.thread_get(&thread_id).await.unwrap().turns,
            vec![saved_turn]
        );
        assert_eq!(cli_session.turn_starts.lock().await.len(), 1);
        return;
    }

    if !matches!(scenario, Scenario::OpenAfterRecovery) {
        let initial = open(
            &processor,
            connection_id,
            rx,
            &client,
            &thread_id,
            &workspace_id,
        )
        .await;
        assert!(initial.turns.is_empty());
        assert_eq!(active(&client, &thread_id).in_flight_turn_id, None);
    }
    let subscribers = manager.subscribed_connections(&thread_id).await;
    let pending_recovery = if matches!(scenario, Scenario::CancelDuringRecovery) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *cli_session.turn_start_pause.lock().await = Some((entered.clone(), release.clone()));
        let recovery_processor = processor.clone();
        let recovery_event =
            crate::resilience::RecoveryCoordinatorEvent::CliRuntimeRetryAttemptRequested(Box::new(
                request.clone(),
            ));
        let task = tokio::spawn(message_future(async move {
            recovery_processor
                .handle_recovery_event(recovery_event, chrono::Utc::now().timestamp())
                .await
        }));
        timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        Some((task, release))
    } else {
        processor
            .handle_recovery_event(
                crate::resilience::RecoveryCoordinatorEvent::CliRuntimeRetryAttemptRequested(
                    Box::new(request.clone()),
                ),
                chrono::Utc::now().timestamp(),
            )
            .await;
        None
    };
    let (_, loaded) = manager.turn_get(&thread_id, &turn_id).await.unwrap();
    assert_eq!(
        loaded, saved_turn,
        "recovery must restore the exact saved Turn"
    );

    if matches!(scenario, Scenario::OpenAfterRecovery) {
        let opened = open(
            &processor,
            connection_id,
            rx,
            &client,
            &thread_id,
            &workspace_id,
        )
        .await;
        assert!(opened.turns.iter().any(|turn| turn == &saved_turn));
        // Restarting only the desktop keeps the Gateway's loaded Turn. The
        // same open path must restore control without another recovery call.
        let restarted_desktop = ClientCore::new();
        open(
            &processor,
            connection_id,
            rx,
            &restarted_desktop,
            &thread_id,
            &workspace_id,
        )
        .await;
        assert_eq!(
            active(&restarted_desktop, &thread_id)
                .in_flight_turn_id
                .as_deref(),
            Some(turn_id.as_str())
        );
        assert!(active(&restarted_desktop, &thread_id).can_request_turn_cancel(true));
    } else {
        let updated = receive_update(rx, &client).await;
        assert_eq!(updated.id, thread_id);
        assert_eq!(updated.workspace_id, workspace_id);
        assert!(updated.turns.iter().any(|turn| turn == &saved_turn));
        assert_eq!(
            manager.subscribed_connections(&thread_id).await,
            subscribers
        );
    }
    assert_eq!(
        active(&client, &thread_id).in_flight_turn_id.as_deref(),
        Some(turn_id.as_str())
    );
    assert!(active(&client, &thread_id).can_request_turn_cancel(true));
    assert!(!active(&client, &thread_id).can_request_turn_cancel(false));

    if matches!(scenario, Scenario::TerminalBeforeRepeat) {
        // A saved running seed is stale relative to a terminal runtime Turn.
        // Deliberately leave the durable seed running to exercise the merge
        // fence and publication source, independently of timestamp ordering.
        let running_seed = manager.thread_get(&thread_id).await.unwrap();
        manager
            .turn_finish(&thread_id, &turn_id, TurnStatus::Completed, None)
            .await
            .unwrap();
        manager
            .system_thread_restore_persisted(running_seed, None)
            .await
            .unwrap();
        assert!(matches!(
            processor.start_cli_runtime_recovery_attempt(request).await,
            Err(CliRuntimeRecoveryStartFailure::InvalidBinding { .. })
        ));
        let terminal = receive_update(rx, &client).await;
        assert_eq!(terminal.turns[0].status, TurnStatus::Completed);
        assert_eq!(active(&client, &thread_id).in_flight_turn_id, None);
        assert_eq!(cli_session.turn_starts.lock().await.len(), 1);
        return;
    }

    if matches!(scenario, Scenario::RunningAttemptAfterRuntimeLoss) {
        processor.thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
        manager = processor.thread_manager.clone();
        let reopened_client = ClientCore::new();
        open(
            &processor,
            connection_id,
            rx,
            &reopened_client,
            &thread_id,
            &workspace_id,
        )
        .await;
        assert!(manager.turn_get(&thread_id, &turn_id).await.is_none());
        assert_eq!(active(&reopened_client, &thread_id).in_flight_turn_id, None);
        assert!(matches!(
            processor
                .start_cli_runtime_recovery_attempt(request.clone())
                .await,
            Ok(false)
        ));
        let updated = receive_update(rx, &reopened_client).await;
        assert_eq!(
            manager.turn_get(&thread_id, &turn_id).await.unwrap().1,
            saved_turn
        );
        assert_eq!(
            active(&reopened_client, &thread_id)
                .in_flight_turn_id
                .as_deref(),
            Some(turn_id.as_str())
        );
        client.apply_thread_notification(GatewayNotification::ThreadUpdated(
            ThreadUpdatedNotification {
                thread: updated,
                placement: None,
            },
        ));
    }

    if pending_recovery.is_none() {
        // Newer Message and detached TaskRun markers are valid runtime rows, but
        // cannot own the foreground Conversation or become the Stop target.
        let mut markers = manager.thread_get(&thread_id).await.unwrap();
        let mut message = saved_turn.clone();
        message.id = "message_after_recovery".into();
        message.mode = ThreadMode::Message;
        message.status = TurnStatus::Completed;
        let mut task_marker = saved_turn.clone();
        task_marker.id = "task_marker_after_recovery".into();
        task_marker.turn_kind = TurnKind::TaskRun;
        task_marker.origin = TurnOrigin::DetachedTask;
        let mut later_conversation = saved_turn.clone();
        later_conversation.id = "completed_conversation_after_recovery".into();
        later_conversation.status = TurnStatus::Completed;
        markers.turns = vec![
            message.clone(),
            task_marker.clone(),
            later_conversation.clone(),
        ];
        manager
            .system_thread_restore_persisted(markers, None)
            .await
            .unwrap();
        let subscribers = manager.subscribed_connections(&thread_id).await;
        // The running-attempt early exit must still republish, even if the client
        // lost the first notification. Make its Conversation idle before replay.
        let reconnecting_client = ClientCore::new();
        let mut idle = manager.thread_get(&thread_id).await.unwrap();
        idle.turns.clear();
        idle.status = ThreadStatus::Idle;
        reconnecting_client.upsert_thread(idle);
        for _ in 0..2 {
            assert!(matches!(
                processor
                    .start_cli_runtime_recovery_attempt(request.clone())
                    .await,
                Ok(false)
            ));
            let updated = receive_update(rx, &reconnecting_client).await;
            client.apply_thread_notification(GatewayNotification::ThreadUpdated(
                ThreadUpdatedNotification {
                    thread: updated.clone(),
                    placement: None,
                },
            ));
            assert_eq!(
                updated.turns,
                vec![
                    saved_turn.clone(),
                    message.clone(),
                    task_marker.clone(),
                    later_conversation.clone()
                ]
            );
            assert_eq!(
                manager.subscribed_connections(&thread_id).await,
                subscribers
            );
            assert_eq!(
                active(&reconnecting_client, &thread_id)
                    .in_flight_turn_id
                    .as_deref(),
                Some(turn_id.as_str())
            );
        }
        assert_eq!(cli_session.turn_starts.lock().await.len(), 1);
    }
    let snapshot = active(&client, &thread_id);
    let cancel = pioneer_client::turns::cancel::plan_turn_cancel_request(
        &thread_id,
        snapshot.in_flight_turn_id.unwrap(),
        false,
        Some("stop recovered work".into()),
    )
    .unwrap();
    assert_eq!(cancel.params.thread_id, binding.thread_id);
    assert_eq!(cancel.params.turn_id, binding.turn_id);
    let request_id = generate_test_request_id("recoveredstop", "cancel");
    let cancel_rpc = json!({
        "jsonrpc": "2.0", "id": request_id, "method": "turn/cancel", "params": cancel.params,
    })
    .to_string();
    if let Some((recovery, release)) = pending_recovery {
        let cancel_processor = processor.clone();
        let cancellation = tokio::spawn(message_future(async move {
            cancel_processor
                .process_request_for_connection(connection_id, &cancel_rpc)
                .await;
        }));
        // Start real cancellation while recovery holds the session gate. Wait
        // for its intent before releasing the fake native response, so no
        // wall-clock sleep determines the race ordering.
        timeout(Duration::from_secs(2), async {
            loop {
                if processor
                    .user_turn_cancel_intents
                    .lock()
                    .await
                    .contains_key(&(thread_id.clone(), turn_id.clone()))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!cancellation.is_finished());
        release.notify_one();
        assert!(recovery.await.unwrap());
        cancellation.await.unwrap();
    } else {
        processor
            .process_request_for_connection(connection_id, &cancel_rpc)
            .await;
    }
    let (response, failed) =
        recv_response_and_notification_by_id_method(rx, &request_id, events::TURN_FAILED).await;
    let response: TurnCancelResponse = serde_json::from_value(response.result).unwrap();
    assert_eq!(response.thread_id, thread_id);
    assert_eq!(response.turn.id, turn_id);
    assert_eq!(response.turn.status, TurnStatus::Interrupted);
    client.apply_thread_notification(GatewayNotification::from_jsonrpc(failed).unwrap());
    assert_eq!(active(&client, &thread_id).in_flight_turn_id, None);
    assert_eq!(
        processor
            .crud_store
            .get_turn(&thread_id, &turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Interrupted
    );
    assert_eq!(
        manager
            .turn_get(&thread_id, &turn_id)
            .await
            .unwrap()
            .1
            .status,
        TurnStatus::Interrupted
    );
    assert!(
        cli_session
            .interrupts
            .lock()
            .await
            .iter()
            .any(|(thread, turn)| {
                thread.as_deref() == Some(binding.native_thread_id.as_str())
                    && turn.as_deref() == Some("native_turn_default")
            })
    );
    assert!(matches!(
        processor.start_cli_runtime_recovery_attempt(request).await,
        Err(CliRuntimeRecoveryStartFailure::InvalidBinding { .. })
    ));
    while let Ok(Message::Text(payload)) = rx.try_recv() {
        if let Ok(notification) = serde_json::from_str::<JsonRpcNotification>(&payload) {
            if let Some(notification) = GatewayNotification::from_jsonrpc(notification) {
                client.apply_thread_notification(notification);
            }
        }
    }
    assert_eq!(
        manager
            .turn_get(&thread_id, &turn_id)
            .await
            .unwrap()
            .1
            .status,
        TurnStatus::Interrupted
    );
    assert_eq!(active(&client, &thread_id).in_flight_turn_id, None);
    assert_eq!(cli_session.turn_starts.lock().await.len(), 1);
}

async fn exercise_active_observation(
    mut processor: MessageProcessor,
    connection_id: ConnectionId,
    rx: &mut mpsc::Receiver<Message>,
    cli_session: Arc<RecordingCliRuntimeSession>,
    event: crate::resilience::RecoveryCoordinatorEvent,
    scenario: Scenario,
) {
    let crate::resilience::RecoveryCoordinatorEvent::CliRuntimeTerminalReconciliationRequested(
        request,
    ) = &event
    else {
        panic!("fixture must dispatch the production observation-gap handler");
    };
    let binding = &request.binding;
    let thread_id = &binding.thread_id;
    let turn_id = &binding.turn_id;
    let workspace_id = &binding.workspace_id;
    let saved_turn = processor
        .crud_store
        .get_turn(thread_id, turn_id)
        .await
        .unwrap()
        .unwrap()
        .1;
    let saved_attempt = processor
        .crud_store
        .latest_cli_runtime_turn_attempt(turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved_attempt.status, CliRuntimeTurnAttemptStatus::Running);
    *cli_session.turn_observation.lock().await = Some(CLIAgentRuntimeTurnObservation {
        status: CLIAgentRuntimeObservedTurnStatus::InProgress,
        message: None,
        reconciliation_events: Vec::new(),
    });
    processor.thread_manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
    let manager = processor.thread_manager.clone();
    while rx.try_recv().is_ok() {}
    let client = ClientCore::new();

    if matches!(scenario, Scenario::ObservationActiveRestoreFailure) {
        let mut conflict = processor
            .crud_store
            .get_thread_model(thread_id)
            .await
            .unwrap()
            .unwrap();
        conflict.workspace_id = "different-workspace".into();
        conflict.turns.clear();
        manager
            .system_thread_restore_persisted(conflict, None)
            .await
            .unwrap();
        processor
            .handle_recovery_event(event.clone(), chrono::Utc::now().timestamp())
            .await;
        let job = processor
            .crud_store
            .get_recovery_job(&request.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            job.status,
            RecoveryJobStatus::Pending,
            "failed runtime restoration must defer, not succeed, Active reconciliation"
        );
        assert!(manager.turn_get(thread_id, turn_id).await.is_none());
        assert!(cli_session.turn_starts.lock().await.is_empty());
        while let Ok(Message::Text(payload)) = rx.try_recv() {
            if let Ok(notification) = serde_json::from_str::<JsonRpcNotification>(&payload) {
                assert_ne!(
                    notification.method,
                    events::THREAD_UPDATED,
                    "failed restoration must not publish a running snapshot"
                );
            }
        }
        return;
    }

    let subscribed = matches!(scenario, Scenario::ObservationActiveSubscribed);
    if subscribed {
        let initial = open(
            &processor,
            connection_id,
            rx,
            &client,
            thread_id,
            workspace_id,
        )
        .await;
        assert!(initial.turns.is_empty());
        assert_eq!(active(&client, thread_id).in_flight_turn_id, None);
    }
    let subscribers = manager.subscribed_connections(thread_id).await;
    processor
        .handle_recovery_event(event.clone(), chrono::Utc::now().timestamp())
        .await;
    assert_eq!(
        manager.turn_get(thread_id, turn_id).await.unwrap().1,
        saved_turn
    );
    assert_eq!(
        manager.thread_get(thread_id).await.unwrap().turns,
        vec![saved_turn.clone()]
    );
    assert_eq!(manager.subscribed_connections(thread_id).await, subscribers);
    let job = processor
        .crud_store
        .get_recovery_job(&request.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.status, RecoveryJobStatus::Succeeded);
    let snapshot = if subscribed {
        receive_update(rx, &client).await
    } else {
        open(
            &processor,
            connection_id,
            rx,
            &client,
            thread_id,
            workspace_id,
        )
        .await
    };
    assert_eq!(snapshot.turns, vec![saved_turn.clone()]);
    assert_eq!(
        active(&client, thread_id).in_flight_turn_id.as_deref(),
        Some(turn_id.as_str())
    );
    assert!(active(&client, thread_id).can_request_turn_cancel(true));
    assert!(!active(&client, thread_id).can_request_turn_cancel(false));
    let subscribers = manager.subscribed_connections(thread_id).await;
    processor
        .handle_recovery_event(event.clone(), chrono::Utc::now().timestamp())
        .await;
    assert_eq!(manager.subscribed_connections(thread_id).await, subscribers);
    assert_eq!(
        manager.thread_get(thread_id).await.unwrap().turns,
        vec![saved_turn.clone()]
    );
    let current_attempt = processor
        .crud_store
        .latest_cli_runtime_turn_attempt(turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current_attempt.id, saved_attempt.id);
    assert_eq!(current_attempt.native_turn_id, saved_attempt.native_turn_id);
    assert_eq!(current_attempt.status, CliRuntimeTurnAttemptStatus::Running);
    assert!(
        cli_session.turn_starts.lock().await.is_empty(),
        "Active reconciliation must keep the existing native execution"
    );

    let cancel = pioneer_client::turns::cancel::plan_turn_cancel_request(
        thread_id,
        active(&client, thread_id).in_flight_turn_id.unwrap(),
        false,
        None,
    )
    .unwrap();
    assert_eq!(cancel.params.thread_id, binding.thread_id);
    assert_eq!(cancel.params.turn_id, binding.turn_id);
    let request_id = generate_test_request_id("recoveredstop", "activecancel");
    processor.process_request_for_connection(connection_id, &json!({
        "jsonrpc": "2.0", "id": request_id, "method": "turn/cancel", "params": cancel.params,
    }).to_string()).await;
    let (response, failed) =
        recv_response_and_notification_by_id_method(rx, &request_id, events::TURN_FAILED).await;
    let response: TurnCancelResponse = serde_json::from_value(response.result).unwrap();
    assert_eq!(response.thread_id, binding.thread_id);
    assert_eq!(response.turn.id, binding.turn_id);
    assert_eq!(response.turn.status, TurnStatus::Interrupted);
    client.apply_thread_notification(GatewayNotification::from_jsonrpc(failed).unwrap());
    assert_eq!(active(&client, thread_id).in_flight_turn_id, None);
    assert_eq!(
        manager.turn_get(thread_id, turn_id).await.unwrap().1.status,
        TurnStatus::Interrupted
    );
    assert_eq!(
        processor
            .crud_store
            .get_turn(thread_id, turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Interrupted
    );
    assert!(
        cli_session
            .interrupts
            .lock()
            .await
            .iter()
            .any(|(thread, turn)| {
                thread.as_deref() == Some(binding.native_thread_id.as_str())
                    && turn.as_deref() == saved_attempt.native_turn_id.as_deref()
            }),
        "cancel must interrupt the original native attempt of the bound Pioneer Turn"
    );
    // A replayed successful recovery event cannot revive the cancelled Turn.
    processor
        .handle_recovery_event(event.clone(), chrono::Utc::now().timestamp())
        .await;
    while let Ok(Message::Text(payload)) = rx.try_recv() {
        if let Ok(notification) = serde_json::from_str::<JsonRpcNotification>(&payload) {
            if let Some(notification) = GatewayNotification::from_jsonrpc(notification) {
                client.apply_thread_notification(notification);
            }
        }
    }
    assert_eq!(active(&client, thread_id).in_flight_turn_id, None);
    assert_eq!(
        manager.turn_get(thread_id, turn_id).await.unwrap().1.status,
        TurnStatus::Interrupted
    );
    assert!(cli_session.turn_starts.lock().await.is_empty());
}
