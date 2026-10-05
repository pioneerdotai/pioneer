//! Regression sources for real manager admission and actor publication seams.
use super::*;
use crate::{NativeTask, NativeTurnStopOwner, StopError, StoredControlOperationState};

fn pending_manager() -> Arc<AgentManager> {
    let mut manager = AgentManager::new(
        Arc::new(ProviderRegistry::with_provider(
            "pending",
            Arc::new(PendingProvider),
        )),
        test_tool_loop_config(),
    );
    manager.control_plane_config.acknowledgement_timeout = Duration::from_millis(250);
    Arc::new(manager)
}

async fn start_at_actor_seam(
    manager: &AgentManager,
    thread: &str,
    turn: &str,
) -> Result<(), AgentStartError> {
    // Unlike the convenience test helper, this calls the public business start
    // without first reading dependencies: the actor's snapshot is the pause seam.
    manager.start_turn_with_resolved_artifacts_environment_reasoning_permission_profile_and_security_snapshot(
        thread, turn, ThreadMode::Chat, "test-model", "pending", HashMap::new(),
        SkillCatalogSnapshot { version: 1, generated_at_unix: 0, skills: Vec::new() },
        vec![UserInput::Text { text: "admission".into(), text_elements: Vec::new() }],
        Vec::new(), Vec::new(), HashMap::new(), Vec::new(), None,
        pioneer_protocol::default_turn_permission_profile_snapshot(), test_full_access_security_snapshot(),
    ).await
}

async fn enqueued_registry(
    manager: &AgentManager,
    thread: &str,
    turn: &str,
) -> Arc<ControlOperationRegistry> {
    accepted_registry(
        manager,
        thread,
        AgentControlOperationId::StartTurn {
            turn_id: turn.into(),
        },
    )
    .await
}

async fn accepted_registry(
    manager: &AgentManager,
    thread: &str,
    id: AgentControlOperationId,
) -> Arc<ControlOperationRegistry> {
    let registry = manager
        .state
        .read()
        .await
        .threads
        .get(thread)
        .unwrap()
        .control_outcomes
        .clone();
    timeout(Duration::from_secs(1), async {
        loop {
            let enqueued = registry.lock_state().entries.get(&id).is_some_and(|entry| {
                matches!(&entry.state, StoredControlOperationState::Enqueued { .. })
            });
            if enqueued {
                break;
            }
            yield_now().await;
        }
    })
    .await
    .unwrap();
    registry
}

async fn published_owner(manager: &AgentManager, thread: &str, turn: &str) -> NativeTurnStopOwner {
    timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(owners) = manager
                .capture_native_stop_owners(&[thread.into()], 8)
                .await
            {
                if let Some(owner) = owners
                    .into_iter()
                    .find(|owner| owner.turn_id() == turn && !owner.is_quiescent())
                {
                    return owner;
                }
            }
            yield_now().await;
        }
    })
    .await
    .unwrap()
}

async fn cleared(manager: &AgentManager, thread: &str) {
    timeout(Duration::from_secs(3), async {
        loop {
            if manager.active_turn_id(thread).await.is_none()
                && manager
                    .capture_native_stop_owners(&[thread.into()], 8)
                    .await
                    .is_ok()
            {
                break;
            }
            yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn accepted_start_with_cancelled_expired_ack_waiter_is_unknown_until_actual_publication() {
    let manager = pending_manager();
    manager
        .ensure_thread("admission", "workspace")
        .await
        .unwrap();
    let dependencies = manager.runtime_dependencies.state.write().await;
    let native = manager.clone();
    let waiter =
        tokio::spawn(async move { start_at_actor_seam(&native, "admission", "turn").await });
    let registry = enqueued_registry(&manager, "admission", "turn").await;
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["admission".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    assert!(matches!(
        manager
            .cancel_turn_and_wait(
                "admission",
                "turn",
                "no graph fallback",
                tokio::time::Instant::now() + Duration::from_secs(1)
            )
            .await,
        Err(StopError::UnknownOwner)
    ));
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    // The caller no longer exists, so it cannot fence on its ACK timeout.
    // Let the real accepted registry deadline expire; it is still not proof.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["admission".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    assert!(
        matches!(registry.lock_state().entries.get(&AgentControlOperationId::StartTurn { turn_id: "turn".into() }).unwrap().state,
        StoredControlOperationState::Enqueued { deadline } if std::time::Instant::now() >= deadline)
    );
    drop(dependencies);
    let owner = published_owner(&manager, "admission", "turn").await;
    manager
        .cancel_captured_turn_and_wait(
            &owner,
            "retry",
            tokio::time::Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    cleared(&manager, "admission").await;
    let historical = manager
        .capture_native_stop_owners(&["admission".into()], 8)
        .await
        .unwrap();
    assert!(historical.iter().all(NativeTurnStopOwner::is_quiescent));
    manager.remove_thread("admission").await;
}

#[tokio::test]
async fn applied_recovery_ack_is_not_replacement_publication_or_quiescence() {
    let manager = pending_manager();
    manager
        .ensure_thread("recovery", "workspace")
        .await
        .unwrap();
    start_at_actor_seam(&manager, "recovery", "turn")
        .await
        .unwrap();
    let old = published_owner(&manager, "recovery", "turn").await;
    let (release, released) = tokio::sync::oneshot::channel();
    // The real actor owns old shutdown; its restart ACK intentionally precedes
    // this retained native join and replacement publication.
    old.control
        .completion
        .retain_tool(NativeTask::new(tokio::spawn(async move {
            let _ = released.await;
        })));
    manager
        .start_recovery_attempt(
            "recovery",
            RecoveryAttemptRequest {
                recovery_job_id: "job".into(),
                recovery_attempt_id: "attempt".into(),
                turn_id: "turn".into(),
                item_id: "reasoning".into(),
                item_type: TurnItemType::Reasoning,
                force_non_stream: false,
                disable_tool_calling: false,
                disable_image_input: false,
                refresh_provider_auth: false,
                compact_history: false,
                context_recovery_deadline_ms: None,
                continue_generation: false,
                model_override: None,
                retained_provider_history: Vec::new(),
                execution_checkpoint_context: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        manager
            .control_operation_status(
                "recovery",
                &AgentControlOperationId::StartRecoveryAttempt {
                    recovery_attempt_id: "attempt".into()
                }
            )
            .await
            .unwrap(),
        Some(AgentControlOperationStatus::Applied { .. })
    ));
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["recovery".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    release.send(()).unwrap();
    let replacement = timeout(Duration::from_secs(3), async {
        loop {
            let owner = published_owner(&manager, "recovery", "turn").await;
            if owner.run_id() != old.run_id() {
                return owner;
            }
            yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(replacement != old);
    manager
        .cancel_captured_turn_and_wait(
            &replacement,
            "stop replacement",
            tokio::time::Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    manager.remove_thread("recovery").await;
}

#[tokio::test]
async fn drained_finish_is_unknown_until_real_actor_terminal_clear() {
    let manager = test_manager();
    manager.ensure_thread("finish", "workspace").await.unwrap();
    let mut durable = manager.take_durable_receiver("finish").await.unwrap();
    manager
        .start_test_turn_with_default_profile(
            "finish",
            "turn",
            ThreadMode::Chat,
            "test-model",
            "echo",
            HashMap::new(),
            vec![UserInput::Text {
                text: "finish".into(),
                text_elements: Vec::new(),
            }],
            Vec::new(),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            let event = durable.recv().await.unwrap();
            if matches!(event, AgentDurableEvent::TurnCompleted { .. }) {
                break;
            }
            durable.acknowledge_last(Ok(()));
        }
    })
    .await
    .unwrap();
    // Root/tasks are actually joined, but the real actor is paused at its
    // terminal durable ACK before clearing active control.
    assert!(
        manager
            .capture_turn_stop_owner("finish", "turn")
            .await
            .unwrap()
            .is_quiescent()
    );
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["finish".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    durable.acknowledge_last(Ok(()));
    cleared(&manager, "finish").await;
    assert!(
        manager
            .capture_native_stop_owners(&["finish".into()], 8)
            .await
            .unwrap()
            .iter()
            .all(NativeTurnStopOwner::is_quiescent)
    );
    drop(durable);
    manager.remove_thread("finish").await;
}

#[tokio::test]
async fn continuation_of_drained_run_is_unknown_until_next_actual_run_is_published() {
    let provider = Arc::new(LoopBudgetProvider::new(
        LoopBudgetProviderMode::ToolWhileAvailableThenFinal,
        1,
    ));
    let mut config = test_tool_loop_config();
    set_execution_window_budget(&mut config, 1, 16);
    let manager = loop_budget_manager(provider, config);
    manager
        .ensure_thread("continuation", "ws_loop_budget")
        .await
        .unwrap();
    let mut durable = manager.take_durable_receiver("continuation").await.unwrap();
    let _events = start_loop_budget_turn(&manager, "continuation", "turn").await;
    timeout(Duration::from_secs(3), async {
        loop {
            let event = durable.recv().await.unwrap();
            if matches!(
                event,
                AgentDurableEvent::TurnExecutionWindowContinued { .. }
            ) {
                break;
            }
            durable.acknowledge_last(Ok(()));
        }
    })
    .await
    .unwrap();
    let old = manager
        .capture_turn_stop_owner("continuation", "turn")
        .await
        .unwrap();
    assert!(old.is_quiescent());
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["continuation".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    durable.acknowledge_last(Ok(()));
    let replacement = published_owner(&manager, "continuation", "turn").await;
    assert!(replacement != old);
    // The exact old captured handle cannot cancel the newly published window.
    manager
        .cancel_captured_turn_and_wait(
            &old,
            "old run",
            tokio::time::Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert!(!replacement.control.turn_cancellation_token.is_cancelled());
    manager
        .cancel_captured_turn_and_wait(
            &replacement,
            "new run",
            tokio::time::Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    drop(durable);
    manager.remove_thread("continuation").await;
}

#[tokio::test]
async fn unrelated_pending_start_is_not_cancelled_by_an_exact_other_thread_snapshot() {
    let manager = pending_manager();
    manager
        .ensure_thread("selected", "workspace")
        .await
        .unwrap();
    manager
        .ensure_thread("unrelated", "workspace")
        .await
        .unwrap();
    let dependencies = manager.runtime_dependencies.state.write().await;
    let native = manager.clone();
    let waiter =
        tokio::spawn(
            async move { start_at_actor_seam(&native, "unrelated", "foreign-turn").await },
        );
    enqueued_registry(&manager, "unrelated", "foreign-turn").await;
    assert!(
        manager
            .capture_native_stop_owners(&["selected".into()], 8)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["unrelated".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    drop(dependencies);
    waiter.await.unwrap().unwrap();
    let owner = published_owner(&manager, "unrelated", "foreign-turn").await;
    assert!(!owner.control.turn_cancellation_token.is_cancelled());
    manager
        .cancel_captured_turn_and_wait(
            &owner,
            "test cleanup",
            tokio::time::Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    manager.remove_thread("unrelated").await;
    manager.remove_thread("selected").await;
}

#[tokio::test]
async fn restored_checkpoint_start_is_unknown_before_actor_publication_despite_cancelled_waiter() {
    let manager = pending_manager();
    manager
        .ensure_thread("restored", "workspace")
        .await
        .unwrap();
    let mut durable = manager.take_durable_receiver("restored").await.unwrap();
    let payload = serde_json::from_value(serde_json::json!({
        "schema_version": pioneer_protocol::EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
        "workspace_id": "workspace", "thread_id": "restored", "turn_id": "turn",
        "original_request": {"input_count": 1, "text_truncated": false, "attachment_count": 0},
        "window": {"window_index": 1, "agent_round_count": 1, "tool_call_count": 0},
        "provider_budget": {"agent_round_count": 1, "tool_call_count": 0, "provider_usage_available": false},
        "tools": {"requested_count": 0, "executed_count": 0, "unexecuted_count": 0,
            "total_count": 0, "succeeded_count": 0, "failed_count": 0, "in_progress_count": 0,
            "detail_limit": 1, "details_truncated": false}
    })).unwrap();
    let checkpoint = ExecutionCheckpointContext {
        window_id: "turn:window:1".into(),
        window_index: 1,
        checkpoint_id: "checkpoint".into(),
        checkpoint_kind: "window_exhausted".into(),
        payload,
        usage: crate::ExecutionWindowUsageSnapshot::default(),
    };
    let dependencies = manager.runtime_dependencies.state.write().await;
    let native = manager.clone();
    let waiter = tokio::spawn(async move {
        native
            .start_restored_recovery_turn(
                "restored",
                "workspace",
                crate::RestoredRecoveryTurnRequest {
                    turn_id: "turn".into(),
                    execution_window_index: 2,
                    mode: ThreadMode::Chat,
                    hook_runtime_context: AgentTurnHookRuntimeContext::default(),
                    model: "test-model".into(),
                    provider_name: "pending".into(),
                    reasoning: None,
                    workspace_skill_policies: HashMap::new(),
                    skill_catalog: SkillCatalogSnapshot {
                        version: 1,
                        generated_at_unix: 0,
                        skills: Vec::new(),
                    },
                    agent_skill_overlay: Vec::new(),
                    input: vec![UserInput::Text {
                        text: "restore".into(),
                        text_elements: Vec::new(),
                    }],
                    capabilities: Vec::new(),
                    resolved_artifacts: Vec::new(),
                    runtime_environment: HashMap::new(),
                    history: Vec::new(),
                    permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(
                    ),
                    execution_security_snapshot: Some(test_full_access_security_snapshot()),
                },
                RecoveryAttemptRequest {
                    recovery_job_id: "restore-job".into(),
                    recovery_attempt_id: "restore-attempt".into(),
                    turn_id: "turn".into(),
                    item_id: "reasoning".into(),
                    item_type: TurnItemType::Reasoning,
                    force_non_stream: false,
                    disable_tool_calling: false,
                    disable_image_input: false,
                    refresh_provider_auth: false,
                    compact_history: false,
                    context_recovery_deadline_ms: None,
                    continue_generation: false,
                    model_override: None,
                    retained_provider_history: Vec::new(),
                    execution_checkpoint_context: Some(checkpoint),
                },
            )
            .await
    });
    accepted_registry(
        &manager,
        "restored",
        AgentControlOperationId::StartRestoredRecoveryTurn {
            recovery_attempt_id: "restore-attempt".into(),
        },
    )
    .await;
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["restored".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    drop(dependencies);
    let event = timeout(Duration::from_secs(1), durable.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        event,
        AgentDurableEvent::TurnExecutionWindowContinued { .. }
    ));
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["restored".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    durable.acknowledge_last(Ok(()));
    let owner = published_owner(&manager, "restored", "turn").await;
    manager
        .cancel_captured_turn_and_wait(
            &owner,
            "restored retry",
            tokio::time::Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    drop(durable);
    manager.remove_thread("restored").await;
}

#[tokio::test]
async fn actual_typed_start_rejection_closes_admission_without_inventing_a_run() {
    let manager = Arc::new(AgentManager::new(
        Arc::new(ProviderRegistry::new(|_| String::new())),
        test_tool_loop_config(),
    ));
    manager
        .ensure_thread("rejected", "workspace")
        .await
        .unwrap();
    let dependencies = manager.runtime_dependencies.state.write().await;
    let native = manager.clone();
    // "pending" is deliberately not an injected/supported provider here.
    let waiter =
        tokio::spawn(async move { start_at_actor_seam(&native, "rejected", "turn").await });
    enqueued_registry(&manager, "rejected", "turn").await;
    assert!(matches!(
        manager
            .capture_native_stop_owners(&["rejected".into()], 8)
            .await,
        Err(StopError::UnknownOwner)
    ));
    drop(dependencies);
    assert!(waiter.await.unwrap().is_err());
    timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(owners) = manager
                .capture_native_stop_owners(&["rejected".into()], 8)
                .await
            {
                assert!(owners.is_empty());
                break;
            }
            yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        manager
            .control_operation_status(
                "rejected",
                &AgentControlOperationId::StartTurn {
                    turn_id: "turn".into()
                }
            )
            .await
            .unwrap(),
        Some(AgentControlOperationStatus::Rejected { .. })
    ));
    manager.remove_thread("rejected").await;
}
