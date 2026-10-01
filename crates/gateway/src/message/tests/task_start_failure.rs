use super::*;
use pioneer_protocol::{RequestId, TaskErrorClass, TaskEventsParams};
use pioneer_sqlite::is_anyhow_sqlite_transient_access;
use pioneer_tasks::{TaskStartCause, TaskStartFailure, TaskStartReporting, TaskStartStage};

const CANARY: &str = "/private/PIONEER9_PATH SELECT PIONEER9_SQL FROM history PIONEER9_HISTORY run_PIONEER9_ID bearer_PIONEER9_SECRET";

/// All Tokio tasks run on this thread, so both Gateway's fresh preparation task
/// and scheduler dispatch use the local subscriber and isolated Sentry Hub.
/// No global subscriber, endpoint, sleeps, or timing-based coordination.
fn capture_task_start<R: Send + 'static>(
    future: impl std::future::Future<Output = R> + Send + 'static,
) -> (R, Vec<sentry::protocol::Event<'static>>) {
    std::thread::Builder::new()
        .name("task-start-contract".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            crate::public_error::test_support::capture_events(|| runtime.block_on(future))
        })
        .unwrap()
        .join()
        .unwrap()
}

fn assert_safe_events(events: &[sentry::protocol::Event<'static>]) {
    let encoded = serde_json::to_string(events).unwrap();
    for canary in [
        "PIONEER9_PATH",
        "PIONEER9_SQL",
        "PIONEER9_HISTORY",
        "PIONEER9_ID",
        "PIONEER9_SECRET",
    ] {
        assert!(
            !encoded.contains(canary),
            "diagnostic or breadcrumb leaked {canary}"
        );
    }
}

#[test]
fn task_start_reporting_is_typed_idempotent_and_preserves_new_failures() {
    use crate::public_error::test_support::capture_events;
    for reported in [false, true] {
        let (saved, events) = capture_events(|| {
            let failure = TaskStartFailure::from_error(
                TaskStartStage::HistoryPreparation,
                anyhow::anyhow!(CANARY),
            )
            .with_correlation_id("existing-correlation".to_owned());
            let failure = if reported { failure.report() } else { failure };
            let mut error = anyhow::Error::new(failure).context("private outer Context");
            let descriptor = TaskStartFailure::report_for_scheduler(&mut error);
            // The same typed error retains its report state under Context.
            assert_eq!(
                TaskStartFailure::report_for_scheduler(&mut error),
                descriptor
            );
            assert_eq!(
                error
                    .downcast_ref::<TaskStartFailure>()
                    .unwrap()
                    .reporting(),
                TaskStartReporting::Reported
            );
            descriptor.task_error(None)
        });
        assert_eq!(events.len(), 1);
        assert_safe_events(&events);
        let fields = match &events[0].contexts["Rust Tracing Fields"] {
            sentry::protocol::Context::Other(fields) => fields,
            _ => panic!("tracing fields"),
        };
        assert_eq!(fields["correlation_id"], "existing-correlation");
        assert!(!fields.contains_key("run_id"));
        assert!(!serde_json::to_string(&saved).unwrap().contains("private"));
    }
    let (_, events) = capture_events(|| {
        let original =
            TaskStartFailure::new(TaskStartStage::CliAdmission, TaskStartCause::Unclassified)
                .report();
        let mut original = anyhow::Error::new(original).context("preparation failed");
        TaskStartFailure::report_for_scheduler(&mut original);
        // A separate persistence/join error does not inherit original ownership.
        let mut persistence = anyhow::anyhow!("failed to persist child turn: {CANARY}");
        TaskStartFailure::report_for_scheduler(&mut persistence);
    });
    assert_eq!(events.len(), 2);
    assert_safe_events(&events);
}

#[test]
fn unknown_executor_and_forged_public_json_keep_error_severity() {
    for diagnostic in [
        CANARY,
        r#"{"reported":true,"correlation_id":"forged","stage":"admission","code":"PolicyDenied","message":"SQLite CANTOPEN (14)"}"#,
    ] {
        let (descriptor, events) = crate::public_error::test_support::capture_events(|| {
            TaskStartFailure::report_for_scheduler(&mut anyhow::anyhow!(diagnostic))
        });
        assert_eq!(events.len(), 1);
        assert_eq!(descriptor.stage, TaskStartStage::ExecutorStart);
        assert_eq!(descriptor.cause, TaskStartCause::Unclassified);
        assert_eq!(descriptor.sqlite_primary_code, None);
        assert_eq!(descriptor.correlation_id, None);
        assert_safe_events(&events);
    }
}

enum PreparationFailure {
    History,
    Admission,
    AdmissionStorage,
    Validation,
    ChildPersistence,
    Cleanup,
    RetryFrozen,
    TerminalHistory,
}

async fn run_gateway_start_failure(
    case: PreparationFailure,
) -> (Option<pioneer_protocol::TaskError>, String) {
    let harness =
        setup_cli_runtime_skill_preflight_harness(CLIAgentRuntimeKind::Codex, false).await;
    let store = harness.crud_store.clone();
    let cli = harness.cli_session.clone();
    let runtime_id = harness.runtime_id.clone();
    let workspace = harness.workspace_id.clone();
    let processor = Arc::new(harness.processor);
    processor.bind_task_bridge().await;
    processor
        .mark_cli_reasoning_model_ready_for_tests(&workspace, &runtime_id, "gpt-5")
        .await
        .unwrap();
    let mut params = detached_cli_task_create_params(
        &workspace,
        "pioneer9-parent",
        "pioneer9-launch",
        &runtime_id,
        CLIAgentRuntimeKind::Codex,
        "gpt-5",
        "safe task input",
    );
    if matches!(case, PreparationFailure::RetryFrozen) {
        params.retry_policy = Some(TaskRetryPolicy {
            max_attempts: 2,
            backoff: TaskRetryBackoffKind::Fixed,
            initial_delay_seconds: Some(60),
            max_delay_seconds: Some(60),
            retry_on: vec![TaskErrorClass::Internal],
        });
    }
    if matches!(case, PreparationFailure::TerminalHistory) {
        processor.arm_completed_history_preparation_barrier("__task_cli_before_history__");
        *processor.task_history_preparation_failure.lock().unwrap() = Some(anyhow::anyhow!(CANARY));
    }
    ensure_task_create_parent_turn_for_test(&processor, &params)
        .await
        .unwrap();
    if matches!(
        case,
        PreparationFailure::History | PreparationFailure::AdmissionStorage
    ) {
        let temp = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite:{}?mode=rw",
            temp.path().join("missing/db.sqlite").display()
        );
        let error = sea_orm::Database::connect(url).await.unwrap_err();
        let error = anyhow::Error::new(error).context(CANARY);
        if matches!(case, PreparationFailure::History) {
            *processor.task_history_preparation_failure.lock().unwrap() = Some(error);
        } else {
            *processor.task_cli_readiness_failure.lock().unwrap() = Some(error);
        }
    } else if !matches!(case, PreparationFailure::TerminalHistory) {
        let failure = if matches!(case, PreparationFailure::Validation) {
            super::super::turn_handlers::TurnStartFailure::protocol_invalid_input(CANARY)
        } else {
            super::super::turn_handlers::TurnStartFailure::internal(CANARY)
        };
        *processor.task_cli_admission_failure.lock().unwrap() = Some(failure);
    }
    if matches!(case, PreparationFailure::ChildPersistence) {
        // Real writer failure in the existing scoped database, after admission
        // has already reported the original error. Domain invariants stay active.
        use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
        store
            .database_connection()
            .execute_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "CREATE TRIGGER pioneer9_child_failure BEFORE INSERT ON task_event \
             WHEN NEW.event_type = 'task/run/turn/failed' \
             BEGIN SELECT RAISE(ABORT, 'pioneer9 child failure persistence'); END;"
                    .to_owned(),
            ))
            .await
            .unwrap();
    }
    if matches!(case, PreparationFailure::Cleanup) {
        use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
        store
            .database_connection()
            .execute_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "CREATE TRIGGER pioneer9_cleanup_failure BEFORE INSERT ON turn_event \
             WHEN NEW.event_type = 'turn/blocked' \
             BEGIN SELECT RAISE(ABORT, 'pioneer9 cleanup persistence'); END;"
                    .to_owned(),
            ))
            .await
            .unwrap();
    }
    let created = create_task_for_test(&processor, params).await.unwrap();
    let run_id = created.run.as_ref().unwrap().id.clone();
    if matches!(case, PreparationFailure::TerminalHistory) {
        processor
            .wait_for_completed_history_preparation_barrier()
            .await;
        cancel_task_for_test(
            &processor,
            pioneer_protocol::TaskCancelParams {
                task_id: created.task.id.clone(),
                reason: Some("cancel before preparation".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::TaskOnly,
            },
        )
        .await
        .unwrap();
        processor.release_completed_history_preparation_barrier();
        processor
            .task_cli_terminal_preparation_finished
            .notified()
            .await;
        let response = store.get_task(&created.task.id).await.unwrap().unwrap();
        assert_eq!(response.runs[0].status, TaskRunStatus::Cancelled);
        assert_eq!(response.task.status, TaskStatus::Cancelled);
        assert!(cli.turn_starts.lock().await.is_empty());
        let child = response.task_run_turns.first().unwrap();
        let (_, turn) = store
            .get_turn(&child.thread_id, &child.turn_id)
            .await
            .unwrap()
            .unwrap();
        assert!(turn.status != TurnStatus::InProgress);
        // Terminal dispatch never replaces cancellation with start failure.
        return (response.runs[0].error.clone(), run_id);
    }
    let mut saved = None;
    for _ in 0..512 {
        let response = store.get_task(&created.task.id).await.unwrap().unwrap();
        let terminal_committed = matches!(case, PreparationFailure::RetryFrozen)
            || store
                .get_task_occurrence_contract_by_run(&run_id)
                .await
                .unwrap()
                .is_some_and(|occurrence| {
                    occurrence.status == pioneer_protocol::TaskOccurrenceStatus::Failed
                });
        if response.runs[0].status == pioneer_protocol::TaskRunStatus::Failed && terminal_committed
        {
            saved = Some(response);
            break;
        }
        tokio::task::yield_now().await;
    }
    let saved = saved.expect("scheduler must persist the failed start without activation");
    assert_eq!(
        saved.runs.len(),
        if matches!(case, PreparationFailure::RetryFrozen) {
            2
        } else {
            1
        },
        "only configured retry_on can create another run"
    );
    assert_eq!(saved.runs[0].attempt_number, 1);
    let error = saved.runs[0].error.clone().unwrap();
    if !matches!(case, PreparationFailure::RetryFrozen) {
        assert_eq!(saved.task.error.as_ref(), Some(&error));
    }
    assert!(
        cli.turn_starts.lock().await.is_empty(),
        "preparation must not activate CLI"
    );
    let events = processor
        .task_runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: created.task.id,
            after_sequence: None,
            limit: Some(100),
        })
        .await
        .unwrap();
    let child_error = events.events.iter().find_map(|event| match &event.payload {
        TaskEventPayload::TaskRunTurnFailed { error, .. } => error.as_ref(),
        _ => None,
    });
    if matches!(case, PreparationFailure::ChildPersistence) {
        assert!(child_error.is_none());
        assert_eq!(error.code, "task_executor_start_unclassified_failed");
    } else {
        assert_eq!(
            child_error,
            Some(&error),
            "one descriptor for child, run and Task"
        );
        let child = saved.task_run_turns.first().unwrap();
        let (_, turn) = store
            .get_turn(&child.thread_id, &child.turn_id)
            .await
            .unwrap()
            .unwrap();
        if matches!(case, PreparationFailure::Cleanup) {
            assert_eq!(
                turn.status,
                TurnStatus::InProgress,
                "cleanup failure was not hidden"
            );
        } else {
            assert!(
                turn.status != TurnStatus::InProgress,
                "existing close-admitted cleanup must run"
            );
        }
        assert_eq!(child.status, TaskRunTurnStatus::Failed);
    }
    if matches!(case, PreparationFailure::RetryFrozen) {
        let accepted = store
            .get_task_run_conversation_snapshot(&run_id)
            .await
            .unwrap()
            .unwrap();
        let retry = &saved.runs[1];
        assert_eq!(retry.attempt_number, 2);
        assert_eq!(retry.retry_of_run_id.as_deref(), Some(run_id.as_str()));
        assert_eq!(
            retry.ready_at,
            Some(saved.runs[0].completed_at.unwrap() + 60)
        );
        // A fresh capture would consume this failpoint. Reuse must bypass it.
        *processor.task_history_preparation_failure.lock().unwrap() = Some(anyhow::anyhow!(CANARY));
        *processor.task_cli_admission_failure.lock().unwrap() =
            Some(super::super::turn_handlers::TurnStartFailure::internal(
                "second attempt admission failure",
            ));
        processor
            .task_runtime
            .process_due_once(retry.ready_at.unwrap())
            .await
            .unwrap();
        let mut retried = None;
        for _ in 0..512 {
            let response = store.get_task(&saved.task.id).await.unwrap().unwrap();
            let terminal_committed = store
                .get_task_occurrence_contract_by_run(&retry.id)
                .await
                .unwrap()
                .is_some_and(|occurrence| {
                    occurrence.status == pioneer_protocol::TaskOccurrenceStatus::Failed
                });
            if response.runs[1].status == TaskRunStatus::Failed && terminal_committed {
                retried = Some(response);
                break;
            }
            tokio::task::yield_now().await;
        }
        let retried = retried.expect("configured retry completes");
        assert_eq!(retried.runs.len(), 2, "retry budget is unchanged");
        assert!(
            processor
                .task_history_preparation_failure
                .lock()
                .unwrap()
                .is_some()
        );
        let reused = store
            .get_task_run_conversation_snapshot(&retry.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(accepted.history_json, reused.history_json);
        assert_eq!(
            retried.runs[1].error.as_ref().unwrap().code,
            "task_cli_admission_unclassified_failed"
        );
        assert!(cli.turn_starts.lock().await.is_empty());
    }
    let encoded = serde_json::to_string(&error).unwrap();
    for marker in [
        "PIONEER9_",
        "public_error",
        "failed to freeze",
        "failed to accept",
    ] {
        assert!(!encoded.contains(marker));
    }
    (Some(error), run_id)
}

#[test]
fn gateway_completion_executor_scheduler_admission_reports_once_and_keeps_correlation() {
    let ((error, run_id), events) =
        capture_task_start(run_gateway_start_failure(PreparationFailure::Admission));
    assert_eq!(
        events.len(),
        1,
        "Gateway owns admission reporting; scheduler must not duplicate it"
    );
    assert_safe_events(&events);
    let error = error.unwrap();
    assert_eq!(error.code, "task_cli_admission_unclassified_failed");
    assert_eq!(error.class, TaskErrorClass::Internal);
    let TaskValue::Object(details) = error.details.as_ref().unwrap() else {
        panic!("details")
    };
    let TaskValue::String(correlation) = &details["correlation_id"] else {
        panic!("correlation")
    };
    assert!(uuid::Uuid::parse_str(correlation).is_ok());
    let encoded = serde_json::to_string(&events).unwrap();
    assert!(encoded.contains(correlation));
    let fields = match &events[0].contexts["Rust Tracing Fields"] {
        sentry::protocol::Context::Other(fields) => fields,
        _ => panic!("fields"),
    };
    assert!(!fields.contains_key("run_id"));
    assert!(!serde_json::to_string(fields).unwrap().contains(&run_id));
}

#[test]
fn gateway_history_cantopen_keeps_codes_context_and_one_safe_event() {
    for (case, stage) in [
        (PreparationFailure::History, "history_preparation"),
        (PreparationFailure::AdmissionStorage, "cli_admission"),
    ] {
        let ((error, _), events) = capture_task_start(run_gateway_start_failure(case));
        assert_eq!(events.len(), 1);
        assert_safe_events(&events);
        let error = error.unwrap();
        assert_eq!(error.class, TaskErrorClass::Internal);
        let TaskValue::Object(details) = error.details.unwrap() else {
            panic!("details")
        };
        assert_eq!(details["stage"], TaskValue::from(stage));
        assert_eq!(details["cause"], TaskValue::from("storage"));
        assert_eq!(details["sqlite_primary_code"], TaskValue::Integer(14));
        assert_eq!(details["sqlite_extended_code"], TaskValue::Integer(14));
        if stage != "history_preparation" {
            let TaskValue::String(correlation) = &details["correlation_id"] else {
                panic!("completion correlation must be retained")
            };
            assert!(
                serde_json::to_string(&events)
                    .unwrap()
                    .contains(correlation)
            );
        }
    }
}

#[test]
fn gateway_explicit_validation_is_a_refusal_without_policy_guessing() {
    let ((error, _), events) =
        capture_task_start(run_gateway_start_failure(PreparationFailure::Validation));
    assert!(events.is_empty());
    let error = error.unwrap();
    assert_eq!(error.class, TaskErrorClass::Validation);
    assert_eq!(error.code, "task_cli_admission_validation_failed");
}

#[test]
fn gateway_reported_admission_does_not_hide_child_persistence_error() {
    let ((_, _), events) = capture_task_start(run_gateway_start_failure(
        PreparationFailure::ChildPersistence,
    ));
    assert_eq!(
        events.len(),
        2,
        "original admission and new persistence failure each get ERROR"
    );
    assert_safe_events(&events);
}

#[test]
fn real_internal_completion_keeps_type_ownership_and_durable_agent_contract() {
    use super::super::turn_handlers::{TurnStartFailure, TurnStartSuccessResponse};
    let ((task_saved, durable_public), events) = capture_task_start(async {
        let harness =
            setup_cli_runtime_skill_preflight_harness(CLIAgentRuntimeKind::Codex, false).await;
        let security =
            pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp", 1);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let response = TurnStartSuccessResponse::Task {
            permission_profile: security.permission_profile.clone(),
            execution_security_snapshot: security.clone(),
            continuation_thread_id: "test-continuation".to_owned(),
            context_thread_id: "test-context".to_owned(),
            task_run_id: "test-run".to_owned(),
            execution_id: "test-execution".to_owned(),
            agent_author: None,
            agent_turn_response: pioneer_crud::AgentTurnResponseInput {
                turn_id: "test-turn".to_owned(),
                execution_id: "test-execution".to_owned(),
                presentation_snapshot_id: "test-presentation".to_owned(),
                now: pioneer_crud::utc_now(),
            },
            admitted_outcome: None,
            completion: Arc::new(std::sync::Mutex::new(Some(sender))),
        };
        let request_id = RequestId::new("R".repeat(21)).unwrap();
        harness
            .processor
            .send_turn_start_failure_for_test(
                harness.connection_id,
                request_id.clone(),
                &response,
                "test-thread",
                "test-turn",
                TurnStartFailure::internal(CANARY),
            )
            .await;
        let error = receiver
            .await
            .unwrap()
            .err()
            .unwrap()
            .context("completion context");
        let failure = error
            .downcast_ref::<TaskStartFailure>()
            .expect("typed completion, not JSON text");
        assert_eq!(failure.reporting(), TaskStartReporting::Reported);
        assert_eq!(failure.descriptor().stage, TaskStartStage::CliAdmission);
        assert_eq!(failure.descriptor().cause, TaskStartCause::Unclassified);
        assert!(
            uuid::Uuid::parse_str(failure.descriptor().correlation_id.as_deref().unwrap()).is_ok()
        );
        let public = failure
            .public_error()
            .expect("safe public projection is retained");
        assert_eq!(public.code, pioneer_protocol::PublicErrorCode::Internal);
        assert_eq!(public.stage, pioneer_protocol::PublicErrorStage::Admission);
        assert_eq!(
            failure.descriptor().correlation_id.as_deref(),
            Some(public.correlation_id.as_str())
        );
        assert!(!serde_json::to_string(public).unwrap().contains("PIONEER9_"));
        let task_saved = failure.descriptor().task_error(None);
        // Sender was consumed even on failure; no second completion is possible.
        harness
            .processor
            .send_turn_start_failure_for_test(
                harness.connection_id,
                request_id.clone(),
                &response,
                "test-thread",
                "test-turn",
                TurnStartFailure::protocol_invalid_input("already completed"),
            )
            .await;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let author = pioneer_protocol::TurnAuthorSnapshot {
            actor: pioneer_protocol::PersistedActorRef::System,
            display_name: "System".to_owned(),
            nickname: "system".to_owned(),
            avatar_revision: None,
            agent: None,
        };
        let durable = TurnStartSuccessResponse::DurableAgent {
            permission_profile: security.permission_profile.clone(),
            execution_security_snapshot: security,
            continuation_thread_id: "test-continuation".to_owned(),
            context_thread_id: "test-context".to_owned(),
            agent_author: author,
            completion: Arc::new(std::sync::Mutex::new(Some(sender))),
        };
        // Expected failure avoids a raw diagnostic in the unchanged DurableAgent reporter.
        harness
            .processor
            .send_turn_start_failure_for_test(
                harness.connection_id,
                request_id,
                &durable,
                "test-thread",
                "test-turn",
                TurnStartFailure::protocol_invalid_input("invalid request"),
            )
            .await;
        let error = receiver.await.unwrap().err().unwrap();
        assert!(error.downcast_ref::<TaskStartFailure>().is_none());
        let public: pioneer_protocol::PublicError =
            serde_json::from_str(&error.to_string()).unwrap();
        assert_eq!(public.code, pioneer_protocol::PublicErrorCode::InvalidInput);
        assert_eq!(public.stage, pioneer_protocol::PublicErrorStage::Admission);
        (task_saved, public)
    });
    assert_eq!(events.len(), 1);
    assert_safe_events(&events);
    assert!(
        !serde_json::to_string(&task_saved)
            .unwrap()
            .contains("PIONEER9_")
    );
    assert!(!durable_public.retryable);
}

#[test]
fn external_rpc_and_voice_admission_keep_their_safe_public_responses() {
    use super::super::turn_handlers::{TurnStartFailure, TurnStartSuccessResponse};
    let (_, events) = capture_task_start(async {
        let mut harness =
            setup_cli_runtime_skill_preflight_harness(CLIAgentRuntimeKind::Codex, false).await;
        let request_id = RequestId::new("R".repeat(21)).unwrap();
        harness
            .processor
            .send_turn_start_failure_for_test(
                harness.connection_id,
                request_id.clone(),
                &TurnStartSuccessResponse::TurnStart,
                "test-thread",
                "test-turn",
                TurnStartFailure::protocol_invalid_input(CANARY),
            )
            .await;
        let message = harness.rx.try_recv().expect("RPC failure response");
        let response: pioneer_protocol::JsonRpcErrorResponse =
            serde_json::from_str(message.to_text().unwrap()).unwrap();
        assert_eq!(response.id, Some(request_id.clone()));
        assert_eq!(response.error.code, -32600);
        let public: pioneer_protocol::PublicError =
            serde_json::from_value(response.error.data.unwrap()["public_error"].clone()).unwrap();
        assert_eq!(response.error.message, public.message);
        assert_eq!(public.code, pioneer_protocol::PublicErrorCode::InvalidInput);
        assert!(!public.retryable);
        harness
            .processor
            .send_turn_start_failure_for_test(
                harness.connection_id,
                request_id,
                &TurnStartSuccessResponse::VoiceSessionFinalizeAccepted {
                    session_id: "test-voice".to_owned(),
                },
                "test-thread",
                "test-turn",
                TurnStartFailure::protocol_invalid_input(CANARY),
            )
            .await;
        let message = harness.rx.try_recv().expect("voice failure notification");
        let encoded = message.to_text().unwrap();
        let notification: serde_json::Value = serde_json::from_str(encoded).unwrap();
        assert_eq!(notification["params"]["outcome"], "failed");
        assert_eq!(notification["params"]["error"]["kind"], "unknown");
        assert_eq!(notification["params"]["error"]["message"], public.message);
        assert!(!encoded.contains("PIONEER9_"));
    });
    assert!(events.is_empty());
}

#[test]
fn gateway_configured_retry_keeps_backoff_budget_and_frozen_history() {
    let (_, events) =
        capture_task_start(run_gateway_start_failure(PreparationFailure::RetryFrozen));
    assert_eq!(events.len(), 2, "each configured attempt has one owner");
    assert_safe_events(&events);
}

#[test]
fn gateway_terminal_run_during_history_preparation_keeps_cancellation() {
    let (_, events) = capture_task_start(run_gateway_start_failure(
        PreparationFailure::TerminalHistory,
    ));
    assert!(
        events.is_empty(),
        "superseded preparation must not fail the cancelled run"
    );
}

#[test]
fn gateway_reported_start_failure_keeps_independent_cleanup_breadcrumbs() {
    let (_, events) = capture_task_start(async {
        run_gateway_start_failure(PreparationFailure::Cleanup).await;
        // The cleanup WARN occurs after the original event. Capture it without
        // changing production severity by emitting a test-only checkpoint.
        tracing::error!("task cleanup breadcrumb checkpoint");
    });
    assert_eq!(
        events.len(),
        2,
        "one original failure plus the test checkpoint"
    );
    assert_eq!(
        events[0].message.as_deref(),
        Some("Task preparation or launch failed")
    );
    let checkpoint = &events[1];
    assert_eq!(
        checkpoint.message.as_deref(),
        Some("task cleanup breadcrumb checkpoint")
    );
    assert!(
        serde_json::to_string(&checkpoint.breadcrumbs)
            .unwrap()
            .contains("task_turn_admission_close_failed")
    );
    assert_safe_events(&events);
}

#[test]
fn separate_completion_channel_and_task_join_failures_keep_error_reporting() {
    let (_, events) = capture_task_start(async {
        let original =
            TaskStartFailure::new(TaskStartStage::CliAdmission, TaskStartCause::Unclassified)
                .report();
        assert_eq!(original.reporting(), TaskStartReporting::Reported);
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        drop(sender);
        let mut channel = anyhow::Error::new(receiver.await.unwrap_err()).context(CANARY);
        assert!(channel.downcast_ref::<TaskStartFailure>().is_none());
        TaskStartFailure::report_for_scheduler(&mut channel);
        let task = tokio::spawn(std::future::pending::<()>());
        task.abort();
        let mut join = anyhow::Error::new(task.await.unwrap_err()).context(CANARY);
        assert!(join.downcast_ref::<TaskStartFailure>().is_none());
        TaskStartFailure::report_for_scheduler(&mut join);
    });
    assert_eq!(
        events.len(),
        3,
        "original failure, closed channel, cancelled join"
    );
    assert_safe_events(&events);
}

async fn cantopen_with_private_context() -> anyhow::Error {
    let temp = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite:{}?mode=rw",
        temp.path().join("missing/db.sqlite").display()
    );
    anyhow::Error::new(sea_orm::Database::connect(url).await.unwrap_err()).context(CANARY)
}

async fn review_cli_fixture() -> (CliRuntimeSkillPreflightHarness, Arc<MessageProcessor>) {
    let mut harness =
        setup_cli_runtime_skill_preflight_harness(CLIAgentRuntimeKind::Codex, false).await;
    // Configure the existing runtime before binding its executor. No admission,
    // authorization or work-graph assertions are bypassed by this fixture.
    harness.processor.task_runtime = Arc::new(pioneer_tasks::TaskRuntime::new_with_config(
        harness.crud_store.clone(),
        review_enabled_task_runtime_config(),
    ));
    let processor = Arc::new(harness.processor.clone());
    processor.bind_task_bridge().await;
    processor
        .mark_cli_reasoning_model_ready_for_tests(
            &harness.workspace_id,
            &harness.runtime_id,
            "gpt-5",
        )
        .await
        .unwrap();
    (harness, processor)
}

async fn create_review_cli_child(
    harness: &CliRuntimeSkillPreflightHarness,
    processor: &Arc<MessageProcessor>,
    label: &str,
    review: bool,
    reviewer: bool,
) -> (pioneer_protocol::TaskCreateResponse, TestChildRuntimeAnchor) {
    let mut params = detached_cli_task_create_params(
        &harness.workspace_id,
        &format!("parent-{label}"),
        &format!("launch-{label}"),
        &harness.runtime_id,
        CLIAgentRuntimeKind::Codex,
        "gpt-5",
        "safe task input",
    );
    if review {
        params.agent_spec.as_mut().unwrap().review_policy = Some(TaskAgentReviewPolicy {
            mode: TaskAgentReviewMode::UserApproval,
            max_revision_rounds: 2,
            require_explicit_acceptance: true,
            reviewers: if reviewer {
                vec![pioneer_protocol::TaskResultReviewerSpec {
                    reviewer_kind: TaskResultReviewerKind::ReviewAgent,
                    agent_nickname: Some(format!("proposal-51-{}", harness.runtime_id)),
                    agent_role: None,
                    required: true,
                    weight: None,
                }]
            } else {
                Vec::new()
            },
            resolution_strategy: TaskResultReviewResolutionStrategy::UserFinal,
        });
    }
    ensure_task_create_parent_turn_for_test(processor, &params)
        .await
        .unwrap();
    let activations = harness.cli_session.turn_starts.lock().await.len();
    let response = create_task_for_test(processor, params).await.unwrap();
    let run = response.run.as_ref().unwrap();
    let mut lineage = None;
    for _ in 0..512 {
        if let Some(child) = harness
            .crud_store
            .get_latest_task_run_turn(&run.id)
            .await
            .unwrap()
            && let Some(scope) = harness
                .crud_store
                .get_task_thread_lineage(&child.thread_id)
                .await
                .unwrap()
        {
            lineage = Some(TestChildRuntimeAnchor {
                child_thread_id: scope.child_thread_id,
                child_turn_id: child.turn_id,
                parent_thread_id: scope.parent_thread_id,
                created_by_thread_id: scope.created_by_thread_id,
                created_by_turn_id: scope.created_by_turn_id,
            });
            if harness.cli_session.turn_starts.lock().await.len() == activations + 1
                && harness
                    .crud_store
                    .get_task_run(&run.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status
                    == TaskRunStatus::Running
            {
                break;
            }
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        harness.cli_session.turn_starts.lock().await.len(),
        activations + 1
    );
    let lineage = lineage.expect("initial child lineage");
    assert_eq!(
        harness
            .crud_store
            .get_task_run(&run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskRunStatus::Running
    );
    harness.crud_store.materialize_item_completed(ItemCompletedNotification {
        workspace_id: harness.workspace_id.clone(),
        thread_id: lineage.child_thread_id.clone(),
        turn_id: lineage.child_turn_id.clone(),
        item: TurnItem::AgentMessage {
            id: format!("result-{label}"),
            text: r#"<task_result>{"summary":"safe candidate","data":{"ok":true}}</task_result>"#.to_owned(),
            phase: Default::default(), markdown: None, markdown_version: None,
        },
    }, now_timestamp_secs()).await.unwrap();
    (response, lineage)
}

#[test]
fn revision_real_completion_preserves_public_error_and_one_report() {
    for (expected, late) in [(false, false), (false, true), (true, false)] {
        let (response, events) = capture_task_start(async move {
            use super::super::turn_handlers::TurnStartFailure;
            let (mut harness, processor) = review_cli_fixture().await;
            let (task, child) =
                create_review_cli_child(&harness, &processor, "revision-contract", true, false)
                    .await;
            assert!(
                processor
                    .complete_turn(child.child_thread_id, child.child_turn_id, None)
                    .await
            );
            let run = task.run.unwrap();
            let candidates = harness
                .crud_store
                .list_task_result_candidates(&run.id)
                .await
                .unwrap();
            assert_eq!(candidates.len(), 1);
            let attempts = processor
                .task_cli_preparation_attempts
                .load(Ordering::SeqCst);
            let activations = harness.cli_session.turn_starts.lock().await.len();
            if expected {
                *processor.task_cli_admission_failure.lock().unwrap() =
                    Some(TurnStartFailure::protocol_invalid_input(CANARY));
            } else if late {
                *processor
                    .task_cli_history_revalidation_failure
                    .lock()
                    .unwrap() = Some(cantopen_with_private_context().await);
            } else {
                *processor.task_cli_admission_failure.lock().unwrap() =
                    Some(TurnStartFailure::internal_typed(
                        TaskStartStage::CliAdmission,
                        cantopen_with_private_context().await,
                    ));
            }
            let request_id = generate_test_request_id("pioneer9", "revision");
            let request = json!({ "jsonrpc":"2.0", "id":request_id, "method":pioneer_protocol::constants::methods::TASK_REVISE,
                "params": { "taskId":task.task.id, "runId":run.id, "candidateId":candidates[0].id,
                    "feedback":"Revise safely.", "additionalInstructions":[] } });
            message_future(
                processor
                    .process_request_for_connection(harness.connection_id, &request.to_string()),
            )
            .await;
            let response = recv_error_by_id(&mut harness.rx, &request_id).await;
            assert_eq!(response.id.as_ref().unwrap().as_str(), request_id);
            assert_eq!(
                processor
                    .task_cli_preparation_attempts
                    .load(Ordering::SeqCst),
                attempts + 1
            );
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                activations
            );
            let public: pioneer_protocol::PublicError = serde_json::from_value(
                response.error.data.as_ref().unwrap()["public_error"].clone(),
            )
            .unwrap();
            assert_eq!(public.stage, pioneer_protocol::PublicErrorStage::Admission);
            assert_eq!(
                public.code,
                if expected {
                    pioneer_protocol::PublicErrorCode::InvalidInput
                } else {
                    pioneer_protocol::PublicErrorCode::Internal
                }
            );
            if late {
                assert_persisted_late_cli_failure(
                    &processor,
                    &task.task.id,
                    &run.id,
                    TaskRunTurnKind::Revision,
                    Some(&public.correlation_id),
                )
                .await;
                assert_eq!(
                    harness
                        .crud_store
                        .get_task(&task.task.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .runs
                        .len(),
                    1
                );
            }
            // Capture expected-refusal breadcrumbs without counting this test
            // checkpoint as an original failure.
            if expected {
                tracing::error!("revision contract breadcrumb checkpoint");
            }
            response
        });
        assert_eq!(events.len(), 1);
        assert_safe_events(&events);
        let encoded = serde_json::to_string(&response).unwrap();
        assert!(!encoded.contains("PIONEER9_"));
        let public = &response.error.data.unwrap()["public_error"];
        let correlation = public["correlation_id"]
            .as_str()
            .or_else(|| public["correlationId"].as_str())
            .unwrap();
        assert!(
            serde_json::to_string(&events)
                .unwrap()
                .contains(correlation)
        );
        if !expected {
            let encoded = serde_json::to_string(&events).unwrap();
            assert!(encoded.contains("storage"));
            let sentry::protocol::Context::Other(fields) =
                &events[0].contexts["Rust Tracing Fields"]
            else {
                panic!("fields")
            };
            assert_eq!(fields["sqlite_primary_code"], 14);
            assert_eq!(fields["sqlite_extended_code"], 14);
            assert_eq!(fields["correlation_id"], correlation);
            assert_eq!(
                fields["stage"],
                if late {
                    "cli_preparation"
                } else {
                    "cli_admission"
                }
            );
        } else {
            assert_eq!(
                events[0].message.as_deref(),
                Some("revision contract breadcrumb checkpoint")
            );
        }
    }
}

#[test]
fn reviewer_real_completion_live_keeps_pending_and_safe_breadcrumbs() {
    let (_, events) = capture_task_start(async {
        use super::super::turn_handlers::TurnStartFailure;
        let (harness, processor) = review_cli_fixture().await;
        let (task, child) =
            create_review_cli_child(&harness, &processor, "live-reviewer", true, true).await;
        let attempts = processor
            .task_cli_preparation_attempts
            .load(Ordering::SeqCst);
        let activations = harness.cli_session.turn_starts.lock().await.len();
        *processor.task_cli_admission_failure.lock().unwrap() =
            Some(TurnStartFailure::internal_typed(
                TaskStartStage::CliAdmission,
                cantopen_with_private_context().await,
            ));
        assert!(
            processor
                .complete_turn(
                    child.child_thread_id.clone(),
                    child.child_turn_id.clone(),
                    None
                )
                .await
        );
        let run_id = &task.run.unwrap().id;
        let candidates = harness
            .crud_store
            .list_task_result_candidates(run_id)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].status,
            TaskResultCandidateStatus::PendingReview
        );
        assert_eq!(
            harness
                .crud_store
                .get_task_run(run_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskRunStatus::WaitingReview
        );
        assert_eq!(
            processor
                .task_cli_preparation_attempts
                .load(Ordering::SeqCst),
            attempts + 1
        );
        assert_eq!(
            harness.cli_session.turn_starts.lock().await.len(),
            activations
        );
        assert!(
            harness
                .crud_store
                .get_turn_runtime_snapshot(&child.child_turn_id)
                .await
                .unwrap()
                .is_some(),
            "pending reconciliation retains its runtime input"
        );
        tracing::error!("reviewer live breadcrumb checkpoint");
    });
    assert_eq!(events.len(), 2, "original failure plus test checkpoint");
    assert_safe_events(&events);
    let checkpoint = &events[1];
    assert!(
        checkpoint
            .breadcrumbs
            .iter()
            .any(|crumb| crumb.message.as_deref()
                == Some("completed child task preparation is pending durable retry"))
    );
}

async fn persist_completed_child(
    harness: &CliRuntimeSkillPreflightHarness,
    child: &TestChildRuntimeAnchor,
    timestamp: i64,
) {
    let (_, mut turn) = harness
        .crud_store
        .get_turn(&child.child_thread_id, &child.child_turn_id)
        .await
        .unwrap()
        .unwrap();
    turn.status = TurnStatus::Completed;
    harness
        .crud_store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: harness.workspace_id.clone(),
                thread_id: child.child_thread_id.clone(),
                turn,
            },
            timestamp,
        )
        .await
        .unwrap();
}

#[test]
fn reviewer_background_batch_preserves_ownership_progress_and_no_new_immediate_retry() {
    for mixed in [false, true] {
        let (safe_batch, events) = capture_task_start(async move {
            use super::super::{tasks, turn_handlers::TurnStartFailure};
            let (harness, processor) = review_cli_fixture().await;
            let (poison, poison_child) =
                create_review_cli_child(&harness, &processor, "batch-poison", true, true).await;
            let (healthy, healthy_child) =
                create_review_cli_child(&harness, &processor, "batch-healthy", false, false).await;
            let independent = if mixed {
                Some(
                    create_review_cli_child(
                        &harness,
                        &processor,
                        "batch-independent",
                        false,
                        false,
                    )
                    .await,
                )
            } else {
                None
            };
            let now = now_timestamp_secs();
            persist_completed_child(&harness, &poison_child, now).await;
            persist_completed_child(&harness, &healthy_child, now + 1).await;
            if let Some((_, child)) = &independent {
                persist_completed_child(&harness, child, now + 2).await;
                processor
                    .task_output_capture_failures
                    .lock()
                    .unwrap()
                    .insert(
                        child.child_turn_id.clone(),
                        anyhow::anyhow!("independent output persistence failed"),
                    );
            }
            *processor.task_cli_admission_failure.lock().unwrap() =
                Some(TurnStartFailure::internal_typed(
                    TaskStartStage::CliAdmission,
                    cantopen_with_private_context().await,
                ));
            let attempts = processor
                .task_cli_preparation_attempts
                .load(Ordering::SeqCst);
            let activations = harness.cli_session.turn_starts.lock().await.len();
            let background = processor.for_background_reconciliation();
            // Same scoped batch/retry and outer reporting boundary used by the
            // lifecycle worker; no infinite worker or timer is started.
            let error = crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::TaskReconcile,
                background.reconcile_terminal_task_child_turns_with_retry(64),
            )
            .await
            .unwrap_err();
            assert!(
                error
                    .downcast_ref::<tasks::TaskChildReconciliationFailures>()
                    .is_some()
            );
            assert!(!tasks::retry_task_child_reconciliation_error(&error));
            assert!(!format!("{error:#}").contains("PIONEER9_"));
            assert!(!format!("{error:?}").contains("PIONEER9_"));
            assert_eq!(
                error.chain().count(),
                1,
                "typed source stays outside aggregate"
            );
            assert_eq!(
                processor
                    .task_cli_preparation_attempts
                    .load(Ordering::SeqCst),
                attempts + 1
            );
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                activations
            );
            assert_eq!(
                harness
                    .crud_store
                    .get_task_run(&healthy.run.unwrap().id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunStatus::Succeeded,
                "healthy row after poison makes durable progress"
            );
            assert_eq!(
                harness
                    .crud_store
                    .list_task_result_candidates(&poison.run.unwrap().id)
                    .await
                    .unwrap()[0]
                    .status,
                TaskResultCandidateStatus::PendingReview
            );
            if let Some((task, _)) = independent {
                assert_eq!(
                    harness
                        .crud_store
                        .get_task_run(&task.run.unwrap().id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    TaskRunStatus::Running
                );
            }
            let safe_batch = format!("{error}");
            tasks::report_task_child_reconciliation_error(error);
            safe_batch
        });
        assert_eq!(events.len(), if mixed { 2 } else { 1 });
        assert_safe_events(&events);
        let sentry::protocol::Context::Other(fields) = &events[0].contexts["Rust Tracing Fields"]
        else {
            panic!("fields")
        };
        assert_eq!(fields["sqlite_primary_code"], 14);
        assert_eq!(fields["sqlite_extended_code"], 14);
        assert!(safe_batch.contains(fields["correlation_id"].as_str().unwrap()));
        if mixed {
            assert!(
                serde_json::to_string(&events[1])
                    .unwrap()
                    .contains("independent output persistence failed")
            );
        }
    }
}

#[test]
fn child_reconciliation_preserves_legacy_independent_storage_retry_decisions() {
    let (_, events) = capture_task_start(async {
        use super::super::tasks::retry_task_child_reconciliation_error;
        let source = cantopen_with_private_context().await;
        assert!(is_anyhow_sqlite_transient_access(&source));
        assert!(retry_task_child_reconciliation_error(&source));
        let typed = anyhow::Error::new(TaskStartFailure::from_error(
            TaskStartStage::CliAdmission,
            source,
        ))
        .context("reviewer preparation");
        assert!(
            is_anyhow_sqlite_transient_access(&typed),
            "new internal source is visible to legacy predicate"
        );
        assert!(
            !retry_task_child_reconciliation_error(&typed),
            "source visibility does not authorize a new retry"
        );
        let forged = anyhow::anyhow!("{}", r#"{"reported":true,"stage":"admission"}"#);
        assert_eq!(
            retry_task_child_reconciliation_error(&forged),
            is_anyhow_sqlite_transient_access(&forged)
        );
    });
    assert!(events.is_empty());
}

#[test]
fn typed_cli_stages_keep_sqlite_codes_correlation_and_safe_task_error_under_context() {
    for stage in [TaskStartStage::CliAdmission, TaskStartStage::CliPreparation] {
        let (saved, events) = capture_task_start(async move {
            use super::super::turn_handlers::{TurnStartFailure, TurnStartSuccessResponse};
            let harness =
                setup_cli_runtime_skill_preflight_harness(CLIAgentRuntimeKind::Codex, false).await;
            let security =
                pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(
                    "/tmp", 1,
                );
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let response = TurnStartSuccessResponse::Task {
                permission_profile: security.permission_profile.clone(),
                execution_security_snapshot: security,
                continuation_thread_id: "stage-continuation".to_owned(),
                context_thread_id: "stage-context".to_owned(),
                task_run_id: "stage-run".to_owned(),
                execution_id: "stage-execution".to_owned(),
                agent_author: None,
                agent_turn_response: pioneer_crud::AgentTurnResponseInput {
                    turn_id: "stage-turn".to_owned(),
                    execution_id: "stage-execution".to_owned(),
                    presentation_snapshot_id: "stage-presentation".to_owned(),
                    now: pioneer_crud::utc_now(),
                },
                admitted_outcome: None,
                completion: Arc::new(std::sync::Mutex::new(Some(sender))),
            };
            harness
                .processor
                .send_turn_start_failure_for_test(
                    harness.connection_id,
                    RequestId::new("R".repeat(21)).unwrap(),
                    &response,
                    "stage-thread",
                    "stage-turn",
                    TurnStartFailure::internal_typed(stage, cantopen_with_private_context().await),
                )
                .await;
            let mut error = receiver.await.unwrap().err().unwrap().context(CANARY);
            let failure = error.downcast_ref::<TaskStartFailure>().unwrap();
            assert_eq!(failure.reporting(), TaskStartReporting::Reported);
            assert_eq!(failure.descriptor().stage, stage);
            assert_eq!(failure.descriptor().cause, TaskStartCause::Storage);
            assert_eq!(failure.descriptor().sqlite_primary_code, Some(14));
            assert_eq!(failure.descriptor().sqlite_extended_code, Some(14));
            assert_eq!(
                failure.public_error().unwrap().stage,
                pioneer_protocol::PublicErrorStage::Admission
            );
            let saved = TaskStartFailure::report_for_scheduler(&mut error).task_error(None);
            assert_eq!(saved.class, TaskErrorClass::Internal);
            assert!(!serde_json::to_string(&saved).unwrap().contains("PIONEER9_"));
            saved
        });
        assert_eq!(events.len(), 1);
        assert_safe_events(&events);
        let TaskValue::Object(details) = saved.details.unwrap() else {
            panic!("details")
        };
        assert_eq!(details["stage"], TaskValue::from(stage.label()));
        let TaskValue::String(correlation) = &details["correlation_id"] else {
            panic!("correlation")
        };
        assert!(
            serde_json::to_string(&events)
                .unwrap()
                .contains(correlation)
        );
    }
}

/// Inspect durable state and canonical payload at the production persistence
/// boundary; no inference from RPC or from a prebuilt descriptor is sufficient.
async fn assert_persisted_late_cli_failure(
    processor: &MessageProcessor,
    task_id: &str,
    run_id: &str,
    kind: TaskRunTurnKind,
    correlation: Option<&str>,
) -> pioneer_protocol::TaskError {
    use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
    let saved = processor
        .crud_store
        .get_task(task_id)
        .await
        .unwrap()
        .unwrap();
    let run = saved.runs.iter().find(|run| run.id == run_id).unwrap();
    assert_eq!(run.status, TaskRunStatus::Blocked);
    assert_eq!(saved.task.status, TaskStatus::Blocked);
    let error = run.error.clone().unwrap();
    assert_eq!(saved.task.error.as_ref(), Some(&error));
    assert_eq!(error.code, "task_cli_preparation_storage_failed");
    assert_eq!(error.class, TaskErrorClass::Internal);
    assert_eq!(error.message, "Task preparation or launch failed.");
    let TaskValue::Object(details) = error.details.as_ref().unwrap() else {
        panic!("details")
    };
    assert_eq!(details["stage"], TaskValue::from("cli_preparation"));
    assert_eq!(details["cause"], TaskValue::from("storage"));
    assert_eq!(details["sqlite_primary_code"], TaskValue::Integer(14));
    assert_eq!(details["sqlite_extended_code"], TaskValue::Integer(14));
    let TaskValue::String(saved_correlation) = &details["correlation_id"] else {
        panic!("correlation")
    };
    assert!(uuid::Uuid::parse_str(saved_correlation).is_ok());
    if let Some(correlation) = correlation {
        assert_eq!(saved_correlation, correlation);
    }
    assert!(!serde_json::to_string(&error).unwrap().contains("PIONEER9_"));
    let child = saved
        .task_run_turns
        .iter()
        .find(|turn| turn.kind == kind && turn.status == TaskRunTurnStatus::Blocked)
        .unwrap();
    let (_, turn) = processor
        .crud_store
        .get_turn(&child.thread_id, &child.turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(turn.status, TurnStatus::Blocked);
    assert_eq!(turn.error.as_deref(), Some("task_cli_preparation_failed"));
    let events = processor
        .crud_store
        .get_task_events(task_id, None)
        .await
        .unwrap()
        .events;
    let blocked = events
        .iter()
        .filter_map(|event| match &event.payload {
            TaskEventPayload::TaskRunTurnBlocked {
                task_run_turn,
                error,
            } if task_run_turn.id == child.id => Some(error),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(blocked.len(), 1, "one original child terminal transition");
    assert_eq!(blocked[0].as_ref(), Some(&error));
    // Read only through the fixture's existing scoped database handle. Inspect
    // every canonical terminal payload, including a possible idempotent close.
    let payloads = processor
        .crud_store
        .database_connection()
        .query_all_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT payload FROM turn_event WHERE turn_id = ? AND event_type = 'turn/blocked'",
            [child.turn_id.clone().into()],
        ))
        .await
        .unwrap();
    assert!(!payloads.is_empty());
    for row in payloads {
        let payload: String = row.try_get("", "payload").unwrap();
        assert!(payload.contains("task_cli_preparation_failed"));
        assert!(!payload.contains("PIONEER9_"));
        assert!(!payload.contains("failed to revalidate"));
    }
    error
}

#[test]
fn initial_late_cli_preparation_blocks_once_with_safe_storage_error_without_retry_or_activation() {
    let (error, events) = capture_task_start(async {
        let (harness, processor) = review_cli_fixture().await;
        let mut params = detached_cli_task_create_params(
            &harness.workspace_id,
            "late-initial-parent",
            "late-initial-launch",
            &harness.runtime_id,
            CLIAgentRuntimeKind::Codex,
            "gpt-5",
            "safe initial input",
        );
        params.retry_policy = Some(TaskRetryPolicy {
            max_attempts: 2,
            backoff: TaskRetryBackoffKind::Fixed,
            initial_delay_seconds: Some(60),
            max_delay_seconds: Some(60),
            retry_on: vec![TaskErrorClass::Internal],
        });
        ensure_task_create_parent_turn_for_test(&processor, &params)
            .await
            .unwrap();
        *processor
            .task_cli_history_revalidation_failure
            .lock()
            .unwrap() = Some(cantopen_with_private_context().await);
        let created = create_task_for_test(&processor, params).await.unwrap();
        // The executor's existing terminal notification is the completion
        // barrier: inspect durable state only after it has observed Blocked.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            processor.task_cli_terminal_preparation_finished.notified(),
        )
        .await
        .expect("initial late CLI preparation did not reach the executor terminal completion barrier within 5 seconds");
        let run_id = &created.run.unwrap().id;
        let error = assert_persisted_late_cli_failure(
            &processor,
            &created.task.id,
            run_id,
            TaskRunTurnKind::Initial,
            None,
        )
        .await;
        assert_eq!(
            processor
                .task_cli_preparation_attempts
                .load(Ordering::SeqCst),
            1
        );
        assert!(harness.cli_session.turn_starts.lock().await.is_empty());
        assert_eq!(
            harness
                .crud_store
                .get_task(&created.task.id)
                .await
                .unwrap()
                .unwrap()
                .runs
                .len(),
            1,
            "Blocked does not acquire a new retry path even for retry_on Internal"
        );
        error
    });
    assert_eq!(events.len(), 1);
    assert_safe_events(&events);
    assert!(
        serde_json::to_string(&events).unwrap().contains(
            match error.details.unwrap() {
                TaskValue::Object(details) => match &details["correlation_id"] {
                    TaskValue::String(value) => value.clone(),
                    _ => panic!("correlation"),
                },
                _ => panic!("details"),
            }
            .as_str()
        )
    );
}

#[test]
fn reviewer_late_cli_preparation_preserves_blocked_transition_and_safe_storage_error() {
    let (error, events) = capture_task_start(async {
        let (harness, processor) = review_cli_fixture().await;
        let (task, child) =
            create_review_cli_child(&harness, &processor, "late-reviewer", true, true).await;
        let attempts = processor
            .task_cli_preparation_attempts
            .load(Ordering::SeqCst);
        let activations = harness.cli_session.turn_starts.lock().await.len();
        *processor
            .task_cli_history_revalidation_failure
            .lock()
            .unwrap() = Some(cantopen_with_private_context().await);
        assert!(
            processor
                .complete_turn(child.child_thread_id, child.child_turn_id, None)
                .await
        );
        let run_id = &task.run.unwrap().id;
        let error = assert_persisted_late_cli_failure(
            &processor,
            &task.task.id,
            run_id,
            TaskRunTurnKind::Review,
            None,
        )
        .await;
        assert_eq!(
            processor
                .task_cli_preparation_attempts
                .load(Ordering::SeqCst),
            attempts + 1
        );
        assert_eq!(
            harness.cli_session.turn_starts.lock().await.len(),
            activations
        );
        assert_eq!(
            harness
                .crud_store
                .get_task(&task.task.id)
                .await
                .unwrap()
                .unwrap()
                .runs
                .len(),
            1
        );
        let background = processor.for_background_reconciliation();
        assert_eq!(
            background
                .reconcile_terminal_task_child_turns_with_retry(64)
                .await
                .unwrap(),
            0,
            "terminal Blocked is not retried by recovery batch"
        );
        error
    });
    assert_eq!(events.len(), 1);
    assert_safe_events(&events);
    assert_eq!(error.code, "task_cli_preparation_storage_failed");
    let TaskValue::Object(details) = error.details.unwrap() else {
        panic!("details")
    };
    let TaskValue::String(correlation) = &details["correlation_id"] else {
        panic!("correlation")
    };
    assert!(
        serde_json::to_string(&events)
            .unwrap()
            .contains(correlation)
    );
}
