use super::*;
use pioneer_entity::turn_cli_runtime_binding as binding;
use sea_orm::ActiveModelTrait;
use std::time::Duration;

async fn terminal_guard_fixture() -> (CrudStore, crate::CliRuntimeTurnTerminalGuard) {
    terminal_guard_fixture_for_runtime("codex").await
}

async fn terminal_guard_fixture_for_runtime(
    runtime_kind: &str,
) -> (CrudStore, crate::CliRuntimeTurnTerminalGuard) {
    let (store, thread, turn) =
        test_store_with_started_turn("ws_cli_guard", "thr_cli_guard", "turn_cli_guard").await;
    let timestamp = unix_to_datetime(1_700_000_000);
    store
        .prepare_cli_runtime_initial_turn_attempt(
            crate::NewCliRuntimeTurnBinding {
                turn_id: turn.id.clone(),
                thread_id: thread.id.clone(),
                continuation_thread_id: thread.id.clone(),
                workspace_id: thread.workspace_id.clone(),
                runtime_id: runtime_kind.into(),
                runtime_kind: runtime_kind.into(),
                native_thread_id: "native_thread".into(),
                native_turn_id: None,
                request_id: None,
                status: "starting".into(),
                model: None,
                cwd: None,
                sandbox_json: None,
                approval_policy: None,
                input_mapping_json: "{}".into(),
                created_at: timestamp,
                updated_at: timestamp,
            },
            "attempt_guard_1".into(),
            1,
        )
        .await
        .unwrap();
    store
        .update_turn_status(
            &thread.id,
            &turn.id,
            TurnStatus::Blocked,
            Some("blocked"),
            1_700_000_000,
        )
        .await
        .unwrap();
    let binding = store
        .get_cli_runtime_turn_binding(&turn.id)
        .await
        .unwrap()
        .unwrap();
    let guard = store
        .cli_runtime_turn_terminal_guard(&binding)
        .await
        .unwrap()
        .unwrap();
    (store, guard)
}

async fn commit_guard(
    store: &CrudStore,
    guard: &crate::CliRuntimeTurnTerminalGuard,
) -> anyhow::Result<Option<crate::CliRuntimeTurnBindingRecord>> {
    store
        .terminalize_cli_runtime_turn_binding_guarded(
            &guard.binding.turn_id,
            "blocked",
            CliRuntimeTurnAttemptStatus::Interrupted,
            Some("old decision".into()),
            unix_to_datetime(1_700_000_000),
            Some(guard),
        )
        .await
}

async fn commit_control_guard(
    store: &CrudStore,
    guard: &crate::CliRuntimeTurnTerminalGuard,
) -> anyhow::Result<Option<crate::CliRuntimeTurnBindingRecord>> {
    store
        .terminalize_cli_runtime_turn_control_binding_guarded(
            &guard.binding.turn_id,
            "blocked",
            CliRuntimeTurnAttemptStatus::Interrupted,
            Some("old decision".into()),
            unix_to_datetime(1_700_000_000),
            Some(guard),
        )
        .await
}

#[pioneer_postgres_test::postgres_test]
async fn cli_terminal_guard_rejects_starting_activation_terminal_and_canonical_resume() {
    for control_only in [false, true] {
        for change in ["running", "terminal", "resume"] {
            let (store, logical_guard) = terminal_guard_fixture().await;
            let guard = if control_only {
                store
                    .cli_runtime_turn_terminal_control_guard(&logical_guard.binding)
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                logical_guard
            };
            match change {
                "running" => {
                    store
                        .activate_cli_runtime_turn_attempt(
                            &guard.binding.turn_id,
                            &guard.attempt.as_ref().unwrap().id,
                            "native_running",
                            None,
                            guard.binding.updated_at,
                        )
                        .await
                        .unwrap();
                }
                "terminal" => {
                    store
                        .terminalize_cli_runtime_turn_binding(
                            &guard.binding.turn_id,
                            "completed",
                            CliRuntimeTurnAttemptStatus::Completed,
                            None,
                            guard.binding.updated_at,
                        )
                        .await
                        .unwrap();
                }
                "resume" => {
                    store
                        .update_turn_status(
                            &guard.binding.thread_id,
                            &guard.binding.turn_id,
                            TurnStatus::InProgress,
                            None,
                            1_700_000_000,
                        )
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let current = store
                .get_cli_runtime_turn_binding(&guard.binding.turn_id)
                .await
                .unwrap();
            let attempt = store
                .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
                .await
                .unwrap();
            let committed = if control_only {
                commit_control_guard(&store, &guard).await.unwrap()
            } else {
                commit_guard(&store, &guard).await.unwrap()
            };
            assert!(committed.is_none());
            assert_eq!(
                store
                    .get_cli_runtime_turn_binding(&guard.binding.turn_id)
                    .await
                    .unwrap(),
                current
            );
            assert_eq!(
                store
                    .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
                    .await
                    .unwrap(),
                attempt
            );
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn cli_control_terminal_guard_fences_mapping_replacement_at_same_timestamp() {
    let (store, logical_guard) = terminal_guard_fixture().await;
    let selected = store
        .cli_runtime_turn_terminal_control_guard(&logical_guard.binding)
        .await
        .unwrap()
        .unwrap();
    let replacement = r#"{"sent":"replacement context"}"#;
    let mut row = binding::ActiveModel {
        turn_id: sea_orm::ActiveValue::Unchanged(selected.binding.turn_id.clone()),
        ..Default::default()
    };
    pioneer_crud_execution::input_mapping::InputMapping::prepare(replacement, false)
        .unwrap()
        .set(&mut row)
        .unwrap();
    row.update(&store.database_connection()).await.unwrap();
    let current = store
        .cli_runtime_turn_terminal_control_guard_by_id(&selected.binding.turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.binding.updated_at, selected.binding.updated_at);
    assert_ne!(current, selected);
    assert!(
        store
            .cli_runtime_turn_terminal_control_guard(&selected.binding)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        commit_control_guard(&store, &selected)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&selected.binding.turn_id)
            .await
            .unwrap(),
        selected.attempt
    );
    let committed = commit_control_guard(&store, &current)
        .await
        .unwrap()
        .unwrap();
    assert!(committed.input_mapping_json.is_empty());
    assert!(committed.input_mapping_frame.is_none());
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&selected.binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .input_mapping_json,
        replacement
    );
    assert_eq!(committed.status, "blocked");
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&selected.binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CliRuntimeTurnAttemptStatus::Interrupted
    );
}

#[pioneer_postgres_test::postgres_test]
async fn cli_terminal_guard_rejects_same_timestamp_status_aba_with_new_recovery_attempt() {
    let (store, guard) = terminal_guard_fixture().await;
    let (_, next) = store
        .prepare_cli_runtime_recovery_turn_attempt(
            &guard.binding.turn_id,
            "attempt_guard_2".into(),
            "recovery_job_2".into(),
            "recovery_attempt_2".into(),
            2,
            "resume".into(),
            guard.binding.updated_at,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&guard.binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        guard.binding
    );
    assert!(commit_guard(&store, &guard).await.unwrap().is_none());
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        next
    );
    assert_eq!(next.status, CliRuntimeTurnAttemptStatus::Starting);
}

#[pioneer_postgres_test::postgres_test]
async fn cli_terminal_guard_repairs_active_projection_atomically_and_rolls_back_on_projection_error()
 {
    let (store, guard) = terminal_guard_fixture().await;
    // The same project fixture uses a failing projection write to prove that
    // binding and attempt mutations do not survive an error later in the txn.
    let before = pioneer_entity::turn_work_projection::Entity::find_by_id(&guard.binding.turn_id)
        .one(&store.database_connection())
        .await
        .unwrap();
    assert!(
        before.is_some(),
        "rollback fixture requires an existing projection"
    );
    store.database_connection().execute_unprepared(
        "CREATE FUNCTION reject_cli_guard_projection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'guard projection failure'; END $$; CREATE TRIGGER reject_cli_guard_projection BEFORE UPDATE ON turn_work_projection FOR EACH ROW EXECUTE FUNCTION reject_cli_guard_projection()",
    ).await.unwrap();
    assert!(commit_guard(&store, &guard).await.is_err());
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&guard.binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        guard.binding
    );
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
            .await
            .unwrap(),
        guard.attempt
    );
    assert_eq!(
        pioneer_entity::turn_work_projection::Entity::find_by_id(&guard.binding.turn_id)
            .one(&store.database_connection())
            .await
            .unwrap(),
        before
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_cli_guard_projection ON turn_work_projection; DROP FUNCTION reject_cli_guard_projection()")
        .await
        .unwrap();
    let result = commit_guard(&store, &guard).await.unwrap().unwrap();
    assert_eq!(result.status, "blocked");
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&guard.binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CliRuntimeTurnAttemptStatus::Interrupted
    );
    assert!(commit_guard(&store, &guard).await.unwrap().is_none());
}

#[pioneer_postgres_test::postgres_test]
async fn cli_terminal_guard_rejects_reacquired_execution_owner_at_equal_timestamp() {
    let (store, guard) = terminal_guard_fixture().await;
    let db = store.database_connection();
    let owner = pioneer_crud_execution::turn_execution::insert_immutable(
        &db,
        crate::NewTurnExecution {
            turn_id: guard.binding.turn_id.clone(),
            thread_id: guard.binding.thread_id.clone(),
            workspace_id: guard.binding.workspace_id.clone(),
            executor_kind: crate::TurnExecutorKind::CliRuntime,
            executor_key: Some("codex".into()),
            status: crate::TurnExecutionStatus::Starting,
            owner_id: "owner".into(),
            lease_until: guard.binding.updated_at + chrono::Duration::seconds(60),
            created_at: guard.binding.created_at,
        },
    )
    .await
    .unwrap();
    store
        .update_turn_status(
            &guard.binding.thread_id,
            &guard.binding.turn_id,
            TurnStatus::Blocked,
            Some("blocked"),
            1_700_000_000,
        )
        .await
        .unwrap();
    let guard = store
        .cli_runtime_turn_terminal_guard(&guard.binding)
        .await
        .unwrap()
        .unwrap();
    assert!(
        pioneer_crud_execution::turn_execution::reacquire_blocked(
            &db,
            &owner.turn_id,
            &owner.owner_id,
            guard.binding.updated_at,
            guard.binding.updated_at,
        )
        .await
        .unwrap()
    );
    // The canonical status and binding are unchanged; the existing ownership
    // generation alone fences this otherwise identical terminal decision.
    assert!(commit_guard(&store, &guard).await.unwrap().is_none());
    let new_owner = store
        .get_turn_execution(&owner.turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new_owner.owner_id, owner.owner_id);
    assert_eq!(new_owner.owner_generation, owner.owner_generation + 1);
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&owner.turn_id)
            .await
            .unwrap(),
        guard.attempt
    );
}

#[pioneer_postgres_test::postgres_test]
async fn cli_terminal_guard_accepts_completed_attempt_after_its_own_starting_running_transitions() {
    let (store, guard) = terminal_guard_fixture().await;
    store
        .update_turn_status(
            &guard.binding.thread_id,
            &guard.binding.turn_id,
            TurnStatus::InProgress,
            None,
            1_700_000_000,
        )
        .await
        .unwrap();
    let (running, attempt) = store
        .activate_cli_runtime_turn_attempt(
            &guard.binding.turn_id,
            &guard.attempt.as_ref().unwrap().id,
            "valid-native",
            None,
            guard.binding.updated_at,
        )
        .await
        .unwrap();
    assert!(
        store
            .mark_cli_runtime_turn_attempt_terminal(
                &attempt.id,
                CliRuntimeTurnAttemptStatus::Completed,
                None,
                guard.binding.updated_at,
            )
            .await
            .unwrap()
    );
    store
        .update_turn_status(
            &running.thread_id,
            &running.turn_id,
            TurnStatus::Completed,
            None,
            1_700_000_000,
        )
        .await
        .unwrap();
    let completed_guard = store
        .cli_runtime_turn_terminal_guard(&running)
        .await
        .unwrap()
        .unwrap();
    let binding = store
        .terminalize_cli_runtime_turn_binding_guarded(
            &running.turn_id,
            "completed",
            CliRuntimeTurnAttemptStatus::Completed,
            None,
            guard.binding.updated_at,
            Some(&completed_guard),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.status, "completed");
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CliRuntimeTurnAttemptStatus::Completed
    );
}

async fn blocked_source_fixture() -> (
    CrudStore,
    crate::CliRuntimeTurnTerminalGuard,
    pioneer_protocol::CliRuntimeBlockedTurnGuard,
) {
    let (store, old) = terminal_guard_fixture().await;
    let timestamp = old.binding.created_at;
    store
        .update_turn_status(
            &old.binding.thread_id,
            &old.binding.turn_id,
            TurnStatus::InProgress,
            None,
            timestamp.timestamp(),
        )
        .await
        .unwrap();
    store
        .activate_cli_runtime_turn_attempt(
            &old.binding.turn_id,
            &old.attempt.unwrap().id,
            "blocked-source-A",
            None,
            timestamp,
        )
        .await
        .unwrap();
    store
        .register_cli_runtime_execution_segment(
            &old.binding.turn_id,
            &old.binding.native_thread_id,
            "blocked-source-A",
            timestamp,
        )
        .await
        .unwrap();
    let binding = store
        .get_cli_runtime_turn_binding(&old.binding.turn_id)
        .await
        .unwrap()
        .unwrap();
    let snapshot = store
        .cli_runtime_turn_terminal_guard(&binding)
        .await
        .unwrap()
        .unwrap();
    let guard = store
        .cli_runtime_blocked_turn_guard(&binding, "blocked-source-A")
        .await
        .unwrap()
        .unwrap();
    (store, snapshot, guard)
}

async fn commit_blocked_source(
    store: &CrudStore,
    guard: &pioneer_protocol::CliRuntimeBlockedTurnGuard,
) -> anyhow::Result<bool> {
    let (_, turn) = store
        .get_turn(&guard.thread_id, &guard.turn_id)
        .await?
        .unwrap();
    store
        .materialize_cli_runtime_blocked_turn_guarded(
            pioneer_protocol::TurnBlockedNotification {
                workspace_id: guard.workspace_id.clone(),
                thread_id: guard.thread_id.clone(),
                turn: Turn {
                    status: TurnStatus::Blocked,
                    error: Some("old blocked source".into()),
                    ..turn
                },
                resume: None,
            },
            guard,
            "owner",
            1_700_000_000,
        )
        .await
}

#[pioneer_postgres_test::postgres_test]
async fn cli_blocked_atomic_source_guard_rejects_new_attempt_and_changed_recovery_authority() {
    let (store, snapshot, old_guard) = blocked_source_fixture().await;
    let timestamp = snapshot.binding.created_at;
    let job = store
        .enqueue_recovery_job(
            snapshot.binding.turn_id.clone(),
            "blocked-source".into(),
            TurnItemType::SystemEvent,
            None,
            RecoveryTrigger::Timeout,
            RecoveryAction::OpenNextExecutionWindow,
            Some("recovery".into()),
            None,
            None,
            None,
            0,
            2,
            serde_json::json!({}),
            serde_json::json!({}),
            timestamp.timestamp(),
        )
        .await
        .unwrap();
    let claim = store
        .claim_due_recovery_jobs(timestamp.timestamp(), 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claim.id, job.id);
    assert!(matches!(
        store
            .mark_claimed_recovery_job_active(
                &job.id,
                claim.claim_token.as_deref().unwrap(),
                "blocked-recovery-owner",
                timestamp.timestamp()
            )
            .await
            .unwrap(),
        crate::ClaimedRecoveryActivation::Activated
    ));
    let (_, next) = store
        .prepare_cli_runtime_recovery_turn_attempt(
            &snapshot.binding.turn_id,
            "blocked-attempt-B".into(),
            job.id.clone(),
            "blocked-recovery-owner".into(),
            2,
            "next execution".into(),
            timestamp,
            None,
        )
        .await
        .unwrap();
    let (binding, next) = store
        .activate_cli_runtime_turn_attempt(
            &snapshot.binding.turn_id,
            &next.id,
            "blocked-source-B",
            None,
            timestamp,
        )
        .await
        .unwrap();
    let (_, _, segment) = store
        .register_cli_runtime_execution_segment(
            &snapshot.binding.turn_id,
            &snapshot.binding.native_thread_id,
            "blocked-source-B",
            timestamp,
        )
        .await
        .unwrap();
    assert!(!commit_blocked_source(&store, &old_guard).await.unwrap());
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        next
    );
    let fresh = store
        .cli_runtime_blocked_turn_guard(&binding, "blocked-source-B")
        .await
        .unwrap()
        .unwrap();
    // Same binding/attempt/segment values and timestamps; only the recovery
    // authority changed between capture and writer validation.
    store
        .mark_recovery_job_terminal(
            &job.id,
            RecoveryJobStatus::Blocked,
            Some("authority ended".into()),
            timestamp.timestamp(),
        )
        .await
        .unwrap();
    assert!(!commit_blocked_source(&store, &fresh).await.unwrap());
    let current = store
        .resolve_cli_runtime_native_turn_owner("codex", "blocked-source-B")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.binding, binding);
    assert_eq!(current.attempt, next);
    assert_eq!(current.segment.unwrap(), segment);
    assert_eq!(
        store
            .get_turn(&binding.thread_id, &binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
}

#[pioneer_postgres_test::postgres_test]
async fn cli_blocked_atomic_source_guard_rejects_execution_generation_aba_at_equal_timestamp() {
    let (store, snapshot, _) = blocked_source_fixture().await;
    let binding = &snapshot.binding;
    let timestamp = binding.created_at;
    let owner = pioneer_crud_execution::turn_execution::insert_immutable(
        &store.database_connection(),
        crate::NewTurnExecution {
            turn_id: binding.turn_id.clone(),
            thread_id: binding.thread_id.clone(),
            workspace_id: binding.workspace_id.clone(),
            executor_kind: crate::TurnExecutorKind::CliRuntime,
            executor_key: Some("codex".into()),
            status: crate::TurnExecutionStatus::Starting,
            owner_id: "owner".into(),
            lease_until: timestamp + chrono::Duration::seconds(60),
            created_at: timestamp,
        },
    )
    .await
    .unwrap();
    let guard = store
        .cli_runtime_blocked_turn_guard(binding, "blocked-source-A")
        .await
        .unwrap()
        .unwrap();
    pioneer_crud_execution::turn_execution::mark_terminal(
        &store.database_connection(),
        &binding.turn_id,
        crate::TurnExecutionStatus::Blocked,
        timestamp,
    )
    .await
    .unwrap();
    assert!(
        pioneer_crud_execution::turn_execution::reacquire_blocked(
            &store.database_connection(),
            &binding.turn_id,
            &owner.owner_id,
            timestamp,
            timestamp
        )
        .await
        .unwrap()
    );
    assert!(!commit_blocked_source(&store, &guard).await.unwrap());
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&binding.turn_id)
            .await
            .unwrap(),
        snapshot.attempt
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding(&binding.turn_id)
            .await
            .unwrap()
            .unwrap(),
        *binding
    );
    assert_eq!(
        store
            .get_turn(&binding.thread_id, &binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    assert_eq!(
        store
            .get_turn_execution(&binding.turn_id)
            .await
            .unwrap()
            .unwrap()
            .owner_generation,
        owner.owner_generation + 1
    );
}

async fn native_delivery_fixture() -> (
    CrudStore,
    pioneer_protocol::CliRuntimeBlockedTurnGuard,
    crate::NewCliRuntimeNativeEvent,
) {
    native_delivery_fixture_for_runtime("codex").await
}

async fn native_delivery_fixture_for_runtime(
    runtime_kind: &str,
) -> (
    CrudStore,
    pioneer_protocol::CliRuntimeBlockedTurnGuard,
    crate::NewCliRuntimeNativeEvent,
) {
    let (store, guard) = terminal_guard_fixture_for_runtime(runtime_kind).await;
    store
        .update_turn_status(
            &guard.binding.thread_id,
            &guard.binding.turn_id,
            TurnStatus::InProgress,
            None,
            1_700_000_000,
        )
        .await
        .unwrap();
    let (binding, _) = store
        .activate_cli_runtime_turn_attempt(
            &guard.binding.turn_id,
            &guard.attempt.unwrap().id,
            "native-delivery-A",
            None,
            guard.binding.updated_at,
        )
        .await
        .unwrap();
    let source = store
        .cli_runtime_terminal_event_source(&binding, "native-delivery-A")
        .await
        .unwrap()
        .unwrap();
    let event = crate::NewCliRuntimeNativeEvent {
        id: "native-terminal-delivery-fixture".into(),
        runtime_id: source.runtime_id.clone(),
        runtime_kind: source.runtime_kind.clone(),
        turn_id: Some(source.turn_id.clone()),
        thread_id: Some(source.thread_id.clone()),
        workspace_id: Some(source.workspace_id.clone()),
        native_thread_id: Some(source.native_thread_id.clone()),
        native_turn_id: Some("native-delivery-A".into()),
        native_method: "gateway/terminal_delivery".into(),
        payload_redacted_json: "{\"outcome\":\"failed\"}".into(),
        sequence: 1,
        created_at: binding.updated_at,
    };
    (store.with_maintenance_access(), source, event)
}

#[pioneer_postgres_test::postgres_test]
async fn cli_task_continuation_rolls_back_and_replays_across_scopes_without_a_new_window() {
    for (runtime_kind, executor_kind, already_completed) in [
        ("codex", crate::TurnExecutorKind::CliRuntime, false),
        ("codex", crate::TurnExecutorKind::CliRuntime, true),
        ("acp", crate::TurnExecutorKind::AcpRuntime, false),
        ("acp", crate::TurnExecutorKind::AcpRuntime, true),
    ] {
        let (store, _, mut terminal) = native_delivery_fixture_for_runtime(runtime_kind).await;
        let turn_id = terminal.turn_id.clone().unwrap();
        pioneer_crud_execution::turn_execution::insert_immutable(
            &store.database_connection(),
            crate::NewTurnExecution {
                turn_id: turn_id.clone(),
                thread_id: terminal.thread_id.clone().unwrap(),
                workspace_id: terminal.workspace_id.clone().unwrap(),
                executor_kind,
                executor_key: Some(terminal.runtime_id.clone()),
                status: crate::TurnExecutionStatus::Running,
                owner_id: "task-owner".into(),
                lease_until: chrono::Utc::now().fixed_offset() + chrono::Duration::hours(1),
                created_at: terminal.created_at,
            },
        )
        .await
        .unwrap();
        let mut before = store
            .cli_runtime_turn_terminal_guard_by_id(&turn_id)
            .await
            .unwrap()
            .unwrap();
        let mut source = store
            .cli_runtime_terminal_event_source(&before.binding, "native-delivery-A")
            .await
            .unwrap()
            .unwrap();
        terminal.id = source.terminal_delivery_id();
        assert!(
            store
                .persist_cli_runtime_terminal_event(terminal.clone(), &source, "task-owner")
                .await
                .unwrap()
        );
        source.terminal_delivery_id = Some(terminal.id.clone());
        let mut input = terminal.clone();
        input.id = "task-attempt:task-continuation".into();
        input.native_method = "gateway/task_continuation".into();
        input.payload_redacted_json = "{\"input\":[]}".into();
        let mut ack = terminal.clone();
        ack.id.push_str(":ack");
        ack.native_method = "gateway/terminal_ack".into();
        ack.payload_redacted_json = "{}".into();
        if already_completed {
            store
                .mark_cli_runtime_turn_attempt_terminal(
                    &source.attempt_id,
                    CliRuntimeTurnAttemptStatus::Completed,
                    None,
                    terminal.created_at,
                )
                .await
                .unwrap();
            store
                .append_cli_runtime_native_event_if_absent(ack.clone())
                .await
                .unwrap();
            source = store
                .cli_runtime_completed_task_continuation_source(&before.binding)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(source.attempt_status, "completed");
            before = store
                .cli_runtime_turn_terminal_guard_by_id(&turn_id)
                .await
                .unwrap()
                .unwrap();
        }
        store.database_connection().execute_unprepared(
        "CREATE FUNCTION reject_task_continuation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected task continuation failure'; END $$; CREATE TRIGGER reject_task_continuation BEFORE INSERT ON turn_cli_runtime_attempt FOR EACH ROW WHEN (NEW.id = 'task-attempt') EXECUTE FUNCTION reject_task_continuation()"
    ).await.unwrap();
        assert!(
            store
                .prepare_cli_runtime_task_continuation(
                    source.clone(),
                    "task-owner".into(),
                    "task-attempt".into(),
                    input.clone(),
                    ack.clone()
                )
                .await
                .is_err()
        );
        assert_eq!(
            store
                .get_cli_runtime_native_event(&ack.id)
                .await
                .unwrap()
                .is_some(),
            already_completed
        );
        assert!(
            store
                .get_cli_runtime_native_event(&input.id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_cli_runtime_turn_attempt("task-attempt")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .cli_runtime_turn_terminal_guard_by_id(&turn_id)
                .await
                .unwrap()
                .unwrap(),
            before
        );
        store
            .database_connection()
            .execute_unprepared("DROP TRIGGER reject_task_continuation ON turn_cli_runtime_attempt; DROP FUNCTION reject_task_continuation()")
            .await
            .unwrap();
        let interactive = store.with_interactive_writes();
        let maintenance = store.with_maintenance_access();
        let gate = store.database_connection().begin().await.unwrap();
        pioneer_crud_execution::execution_reads::lock_cli_turn_binding(&gate, &source.turn_id)
            .await
            .unwrap();
        let blocker_pid: i32 = gate
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT pg_backend_pid() AS pid",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "pid")
            .unwrap();
        let launch = |store: CrudStore| {
            let source = source.clone();
            let input = input.clone();
            let ack = ack.clone();
            tokio::spawn(async move {
                store
                    .prepare_cli_runtime_task_continuation(
                        source,
                        "task-owner".into(),
                        "task-attempt".into(),
                        input,
                        ack,
                    )
                    .await
            })
        };
        let first = launch(interactive);
        let replay = launch(maintenance);
        let observer = store.database_connection();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let waiting: bool = observer.query_one_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                     WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid))) AS waiting",
                    [blocker_pid.into()],
                )).await.unwrap().unwrap().try_get("", "waiting").unwrap();
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the competing continuation must wait on the held binding");
        gate.rollback().await.unwrap();
        let first = first.await.unwrap().unwrap();
        assert_eq!(first, replay.await.unwrap().unwrap());
        assert_eq!(
            first.1.execution_window_index,
            before.attempt.as_ref().unwrap().execution_window_index
        );
        assert_eq!(first.1.attempt_index, 2);
        assert_eq!(first.1.status, CliRuntimeTurnAttemptStatus::Starting);
        assert!(first.0.native_turn_id.is_none());
        assert_eq!(
            store
                .get_turn(&source.thread_id, &turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::InProgress
        );
        assert_eq!(
            store
                .get_cli_runtime_turn_attempt(&source.attempt_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            CliRuntimeTurnAttemptStatus::Completed
        );
        assert!(
            store
                .get_cli_runtime_native_event(&ack.id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .get_cli_runtime_native_event(&input.id)
                .await
                .unwrap()
                .is_some()
        );
        let reopened = CrudStore::new(store.database_connection()).with_maintenance_access();
        assert_eq!(
            reopened
                .prepare_cli_runtime_task_continuation(
                    source.clone(),
                    "task-owner".into(),
                    "task-attempt".into(),
                    input.clone(),
                    ack.clone()
                )
                .await
                .unwrap(),
            first
        );
        assert!(
            reopened
                .prepare_cli_runtime_task_continuation(
                    source,
                    "stale-owner".into(),
                    "task-attempt".into(),
                    input,
                    ack
                )
                .await
                .is_err(),
            "replay still requires the active owner"
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn cli_initial_cancellation_requires_matching_runtime_owner_and_commits_terminal() {
    for (runtime_kind, executor_kind) in [
        ("codex", crate::TurnExecutorKind::CliRuntime),
        ("acp", crate::TurnExecutorKind::AcpRuntime),
    ] {
        let (store, _, terminal) = native_delivery_fixture_for_runtime(runtime_kind).await;
        let turn_id = terminal.turn_id.as_ref().unwrap();
        pioneer_crud_execution::turn_execution::insert_immutable(
            &store.database_connection(),
            crate::NewTurnExecution {
                turn_id: turn_id.clone(),
                thread_id: terminal.thread_id.clone().unwrap(),
                workspace_id: terminal.workspace_id.clone().unwrap(),
                executor_kind,
                executor_key: Some(terminal.runtime_id.clone()),
                status: crate::TurnExecutionStatus::Running,
                owner_id: "cancellation-owner".into(),
                lease_until: chrono::Utc::now().fixed_offset() + chrono::Duration::hours(1),
                created_at: terminal.created_at,
            },
        )
        .await
        .unwrap();
        let owner = store.get_turn_execution(turn_id).await.unwrap().unwrap();
        let source = store
            .cli_runtime_turn_terminal_guard_by_id(turn_id)
            .await
            .unwrap()
            .unwrap();
        let (_, mut turn) = store
            .get_turn(&owner.thread_id, turn_id)
            .await
            .unwrap()
            .unwrap();
        turn.status = TurnStatus::Interrupted;
        turn.error = Some("cancelled".into());
        let notification = pioneer_protocol::TurnFailedNotification {
            workspace_id: owner.workspace_id.clone(),
            thread_id: owner.thread_id.clone(),
            turn,
        };
        let mut wrong_kind = owner.clone();
        wrong_kind.executor_kind = match executor_kind {
            crate::TurnExecutorKind::CliRuntime => crate::TurnExecutorKind::AcpRuntime,
            _ => crate::TurnExecutorKind::CliRuntime,
        };
        assert!(
            store
                .materialize_cli_runtime_cancellation_owned_as(
                    notification.clone(),
                    &wrong_kind,
                    &source,
                    None,
                    terminal.created_at.timestamp(),
                )
                .await
                .is_err()
        );
        assert_eq!(
            store
                .get_turn(&owner.thread_id, turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::InProgress
        );
        store
            .materialize_cli_runtime_cancellation_owned_as(
                notification,
                &owner,
                &source,
                None,
                terminal.created_at.timestamp(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .get_turn(&owner.thread_id, turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Interrupted
        );
        assert_eq!(
            store
                .get_turn_execution(turn_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            crate::TurnExecutionStatus::Interrupted
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn cli_stalled_task_blocked_commits_accepted_outcome_ack_atomically() {
    let (store, mut source, mut terminal) = native_delivery_fixture().await;
    terminal.id = source.terminal_delivery_id();
    assert!(
        store
            .persist_cli_runtime_terminal_event(terminal.clone(), &source, "owner")
            .await
            .unwrap()
    );
    assert!(
        !commit_blocked_source(&store, &source).await.unwrap(),
        "observational Blocked must not consume an accepted outcome"
    );
    source.terminal_delivery_id = Some(terminal.id.clone());
    let ack_id = format!("{}:ack", terminal.id);
    store.database_connection().execute_unprepared(
        "CREATE FUNCTION reject_task_blocked_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected task blocked ACK failure'; END $$; CREATE TRIGGER reject_task_blocked_ack BEFORE INSERT ON cli_runtime_native_event FOR EACH ROW WHEN (NEW.native_method = 'gateway/terminal_ack') EXECUTE FUNCTION reject_task_blocked_ack()"
    ).await.unwrap();
    assert!(commit_blocked_source(&store, &source).await.is_err());
    assert!(
        store
            .get_cli_runtime_native_event(&ack_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_turn(&source.thread_id, &source.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    assert_eq!(
        store
            .latest_cli_runtime_turn_attempt(&source.turn_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CliRuntimeTurnAttemptStatus::Running
    );
    store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_task_blocked_ack ON cli_runtime_native_event; DROP FUNCTION reject_task_blocked_ack()")
        .await
        .unwrap();
    assert!(commit_blocked_source(&store, &source).await.unwrap());
    assert_eq!(
        store
            .get_turn(&source.thread_id, &source.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Blocked
    );
    assert_eq!(
        store
            .get_cli_runtime_native_event(&ack_id)
            .await
            .unwrap()
            .unwrap()
            .native_method,
        "gateway/terminal_ack"
    );
    assert!(
        !store
            .has_pending_cli_runtime_terminal_event(&source.turn_id)
            .await
            .unwrap()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn addressed_timeout_pages_keep_turn_scope_and_revisit_changes_behind_cursor() {
    let at = 1_700_000_000;
    let thread = sample_thread("ws_addressed_control", "thread_addressed_control", at);
    let store = test_store_with_workspace(&thread.workspace_id).await;
    for turn_id in ["api_addressed", "other_addressed"] {
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &sample_turn(turn_id),
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .unwrap();
    }
    for (turn_id, item, started) in [
        ("other_addressed", "foreign-first", at),
        ("api_addressed", "a1", at + 1),
        ("api_addressed", "a2", at + 2),
        ("api_addressed", "a3", at + 3),
    ] {
        store
            .materialize_item_started_with_attempt_deadlines(
                ItemStartedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn_id.into(),
                    item: safe_web_fetch_item(item),
                },
                started,
                crate::TurnItemAttemptDeadlines {
                    lease_expires_at_unix: Some(at + 4),
                    idle_deadline_at_unix: Some(at + 4),
                    hard_deadline_at_unix: Some(at + 4),
                },
            )
            .await
            .unwrap();
    }
    let database = store.database_connection();
    let mut changes = database.work_signal("gateway.control").subscribe();
    changes.begin_keyed_pass(&database).await.unwrap();
    store
        .observe_turn_runtime_activity("api_addressed", "provider/delta", at + 5)
        .await
        .unwrap();
    assert_eq!(
        changes.begin_keyed_pass(&database).await.unwrap(),
        Some(vec![
            crate::GatewayControlWake::Activity("api_addressed".into()).key()
        ])
    );
    let (first, cursor) = store
        .list_timeout_candidate_page_for_turn("api_addressed", at + 10, 2, None)
        .await
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|row| row.item_id.as_str())
            .collect::<Vec<_>>(),
        ["a1", "a2"]
    );
    assert!(first.iter().all(|row| row.turn_id == "api_addressed"));
    let cursor = cursor.unwrap();
    // A real committed event arrives behind the current page frontier.
    store
        .materialize_item_started_with_attempt_deadlines(
            ItemStartedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: "api_addressed".into(),
                item: safe_web_fetch_item("behind"),
            },
            at - 1,
            crate::TurnItemAttemptDeadlines {
                lease_expires_at_unix: Some(at + 4),
                idle_deadline_at_unix: Some(at + 4),
                hard_deadline_at_unix: Some(at + 4),
            },
        )
        .await
        .unwrap();
    let (tail, next) = store
        .list_timeout_candidate_page_for_turn("api_addressed", at + 10, 2, Some(&cursor))
        .await
        .unwrap();
    assert_eq!(
        tail.iter()
            .map(|row| row.item_id.as_str())
            .collect::<Vec<_>>(),
        ["a3"]
    );
    assert!(next.is_none());
    assert_eq!(
        changes.begin_keyed_pass(&database).await.unwrap(),
        Some(vec![
            crate::GatewayControlWake::Activity("api_addressed".into()).key()
        ])
    );
    let (restarted, _) = store
        .list_timeout_candidate_page_for_turn("api_addressed", at + 10, 2, None)
        .await
        .unwrap();
    assert_eq!(restarted[0].item_id, "behind");
    let upper = store
        .running_turn_item_attempt_upper_id_by_type(TurnItemType::WebFetch)
        .await
        .unwrap()
        .unwrap();
    let (commands, _) = store
        .list_running_turn_item_attempt_page_by_type_bounded_for_turn(
            "api_addressed",
            TurnItemType::WebFetch,
            None,
            &upper,
            2,
        )
        .await
        .unwrap();
    assert_eq!(commands.len(), 2);
    assert!(commands.iter().all(|row| row.turn_id == "api_addressed"));
}
