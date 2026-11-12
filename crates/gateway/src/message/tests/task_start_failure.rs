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
            let _runtime_observer = pioneer_observability::runtime::register(
                runtime.handle(),
                pioneer_observability::runtime::RuntimeRole::GatewayPatch,
            );
            crate::public_error::test_support::capture_events(|| {
                _runtime_observer.block_on(&runtime, future)
            })
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
            let original = anyhow::Error::new(failure).context("private outer Context");
            let original_descriptor = original
                .downcast_ref::<TaskStartFailure>()
                .unwrap()
                .descriptor()
                .clone();
            let wrapped = TaskStartFailure::from_error(TaskStartStage::CliAdmission, original);
            assert_eq!(wrapped.descriptor(), &original_descriptor);
            let mut error = anyhow::Error::new(wrapped).context("another Context");
            let borrowed =
                TaskStartFailure::from_borrowed_error(TaskStartStage::ExecutorStart, &error);
            let descriptor = TaskStartFailure::report_for_scheduler(&mut error);
            assert_eq!(borrowed.report().descriptor(), &descriptor);
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
            let mut error = anyhow::anyhow!(diagnostic);
            let first = TaskStartFailure::report_for_scheduler(&mut error);
            assert_eq!(TaskStartFailure::report_for_scheduler(&mut error), first);
            first
        });
        assert_eq!(events.len(), 1);
        assert_eq!(descriptor.stage, TaskStartStage::ExecutorStart);
        assert_eq!(descriptor.cause, TaskStartCause::Unclassified);
        assert_eq!(descriptor.sqlite_primary_code, None);
        assert!(descriptor.correlation_id.is_some());
        assert_ne!(descriptor.correlation_id.as_deref(), Some("forged"));
        assert_safe_events(&events);
    }
}

async fn install_task_start_cleanup_failure(store: &CrudStore) {
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

enum PreparationFailure {
    History,
    Admission,
    AdmissionStorage,
    Validation,
    ChildPersistence,
    Cleanup,
    RetryFrozen,
    RetryBackoff,
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
    if matches!(
        case,
        PreparationFailure::RetryFrozen | PreparationFailure::RetryBackoff
    ) {
        params.retry_policy = Some(TaskRetryPolicy {
            max_attempts: 2,
            backoff: if matches!(case, PreparationFailure::RetryBackoff) {
                TaskRetryBackoffKind::Fixed
            } else {
                TaskRetryBackoffKind::None
            },
            initial_delay_seconds: matches!(case, PreparationFailure::RetryBackoff).then_some(60),
            max_delay_seconds: matches!(case, PreparationFailure::RetryBackoff).then_some(60),
            retry_on: vec![TaskErrorClass::Internal],
        });
    }
    if matches!(case, PreparationFailure::RetryFrozen) {
        // Non-composer CLI Tasks restore their accepted history before graph
        // materialization. A failure here exercises the existing automatic
        // retry policy without making an admitted actor fail its cleanup.
        params.metadata.as_mut().unwrap().composer_work = None;
        params.launch = Some(
            exact_cli_task_launch_for_test(
                &processor,
                &workspace,
                &format!("cli_runtime:{runtime_id}"),
                "gpt-5",
                &runtime_id,
            )
            .await
            .unwrap(),
        );
        *processor.task_history_preparation_failure.lock().unwrap() =
            Some(cantopen_with_private_context().await);
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
    } else if !matches!(
        case,
        PreparationFailure::TerminalHistory | PreparationFailure::RetryFrozen
    ) {
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
             WHEN NEW.event_type IN ('task/run/turn/failed', 'task/run/turn/blocked') \
             BEGIN SELECT RAISE(ABORT, 'pioneer9 child failure persistence'); END;"
                    .to_owned(),
            ))
            .await
            .unwrap();
    }
    if matches!(
        case,
        PreparationFailure::Cleanup | PreparationFailure::RetryBackoff
    ) {
        install_task_start_cleanup_failure(&store).await;
    }
    let mut wakes = processor
        .task_runtime
        .event_bus()
        .subscribe(Default::default());
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
    let expected_status = if matches!(
        case,
        PreparationFailure::Cleanup
            | PreparationFailure::RetryFrozen
            | PreparationFailure::RetryBackoff
            | PreparationFailure::ChildPersistence
    ) {
        TaskRunStatus::Failed
    } else {
        TaskRunStatus::Blocked
    };
    let saved = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let response = store.get_task(&created.task.id).await.unwrap().unwrap();
            if response.runs[0].status == expected_status {
                return response;
            }
            assert_ne!(
                wakes.recv().await,
                pioneer_tasks::TaskEventWakeDelivery::Closed
            );
        }
    })
    .await
    .expect("preparation must commit its terminal Task state within 30 seconds");
    // Only the CLI terminal branch sends this notification. An error returned
    // before that branch is completed and reported by scheduler fail_run.
    if saved.runs[0].status == TaskRunStatus::Blocked {
        tokio::time::timeout(
            Duration::from_secs(30),
            processor.task_cli_terminal_preparation_finished.notified(),
        )
        .await
        .expect("blocked preparation must finish reporting and cleanup within 30 seconds");
    }
    assert_eq!(
        saved.runs.len(),
        if matches!(
            case,
            PreparationFailure::RetryFrozen | PreparationFailure::RetryBackoff
        ) {
            2
        } else {
            1
        },
        "only configured retry_on can create another run"
    );
    assert_eq!(saved.runs[0].attempt_number, 1);
    let error = saved.runs[0].error.clone().unwrap();
    if !matches!(
        case,
        PreparationFailure::RetryFrozen | PreparationFailure::RetryBackoff
    ) {
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
        TaskEventPayload::TaskRunTurnFailed { error, .. }
        | TaskEventPayload::TaskRunTurnBlocked { error, .. } => error.as_ref(),
        _ => None,
    });
    if matches!(case, PreparationFailure::RetryFrozen) {
        assert!(child_error.is_none());
        assert!(
            saved.task_run_turns.is_empty(),
            "history fails before a child is admitted"
        );
        assert_eq!(error.code, "task_history_preparation_storage_failed");
        assert_eq!(error.class, TaskErrorClass::Internal);
        assert_eq!(
            processor
                .task_cli_preparation_attempts
                .load(Ordering::SeqCst),
            0
        );
    } else if matches!(case, PreparationFailure::ChildPersistence) {
        assert!(child_error.is_none());
        // The trigger raises a real SQLite error after admission was reported.
        assert_eq!(error.code, "task_executor_start_storage_failed");
        assert_eq!(error.class, TaskErrorClass::Internal);
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
        if matches!(
            case,
            PreparationFailure::Cleanup
                | PreparationFailure::RetryFrozen
                | PreparationFailure::RetryBackoff
        ) {
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
        assert_eq!(
            child.status,
            if saved.runs[0].status == TaskRunStatus::Blocked {
                TaskRunTurnStatus::Blocked
            } else {
                TaskRunTurnStatus::Failed
            }
        );
    }
    if matches!(
        case,
        PreparationFailure::RetryFrozen | PreparationFailure::RetryBackoff
    ) {
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
            Some(
                saved.runs[0].completed_at.unwrap()
                    + if matches!(case, PreparationFailure::RetryBackoff) {
                        60
                    } else {
                        0
                    }
            )
        );
        if matches!(case, PreparationFailure::RetryBackoff) {
            processor
                .task_runtime
                .process_due_once(saved.runs[0].completed_at.unwrap())
                .await
                .unwrap();
            let waiting = store.get_task(&saved.task.id).await.unwrap().unwrap();
            assert_eq!(waiting.runs.len(), 2);
            assert_eq!(waiting.runs[1].status, TaskRunStatus::Queued);
            assert_eq!(
                processor
                    .task_cli_preparation_attempts
                    .load(Ordering::SeqCst),
                1
            );
            assert!(cli.turn_starts.lock().await.is_empty());
            return (Some(error), run_id);
        }
        install_task_start_cleanup_failure(&store).await;
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
        let retried = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let response = store.get_task(&saved.task.id).await.unwrap().unwrap();
                if response.runs[1].status == TaskRunStatus::Failed {
                    return response;
                }
                assert_ne!(
                    wakes.recv().await,
                    pioneer_tasks::TaskEventWakeDelivery::Closed
                );
            }
        })
        .await
        .expect("configured retry must commit its failed state within 30 seconds");
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
    assert!(!correlation.trim().is_empty());
    let fields = match &events[0].contexts["Rust Tracing Fields"] {
        sentry::protocol::Context::Other(fields) => fields,
        _ => panic!("fields"),
    };
    assert_eq!(fields["correlation_id"], json!(correlation));
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
    let ((task_saved, durable_saved), events) = capture_task_start(async {
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
            !failure
                .descriptor()
                .correlation_id
                .as_deref()
                .unwrap()
                .is_empty()
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
            cli_runtime_kind: None,
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
            admitted_outcome: None,
            completion: Arc::new(std::sync::Mutex::new(Some(sender))),
        };
        // The committed outbox owns typed failures and their eventual reporting.
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
        let failure = error
            .downcast_ref::<TaskStartFailure>()
            .expect("the committed outbox must retain the typed admission cause");
        assert_eq!(failure.descriptor().stage, TaskStartStage::CliAdmission);
        assert_eq!(failure.descriptor().cause, TaskStartCause::Validation);
        assert_eq!(failure.reporting(), TaskStartReporting::Unreported);
        assert!(failure.public_error().is_none());
        (task_saved, failure.descriptor().task_error(None))
    });
    assert_eq!(events.len(), 1);
    assert_safe_events(&events);
    assert!(
        !serde_json::to_string(&task_saved)
            .unwrap()
            .contains("PIONEER9_")
    );
    assert_eq!(durable_saved.class, TaskErrorClass::Validation);
    assert_eq!(durable_saved.code, "task_cli_admission_validation_failed");
    assert!(
        !serde_json::to_string(&durable_saved)
            .unwrap()
            .contains("invalid request")
    );
}

#[test]
fn external_rpc_and_voice_admission_keep_their_safe_public_responses() {
    use super::super::turn_handlers::{TurnStartFailure, TurnStartSuccessResponse};
    let (_, events) = capture_task_start(async {
        let mut harness =
            setup_cli_runtime_skill_preflight_harness(CLIAgentRuntimeKind::Codex, false).await;
        let params = detached_cli_task_create_params(
            &harness.workspace_id,
            "pioneer9-voice-parent",
            "pioneer9-voice-turn",
            &harness.runtime_id,
            CLIAgentRuntimeKind::Codex,
            "gpt-5",
            "safe voice input",
        );
        let processor = Arc::new(harness.processor.clone());
        ensure_task_create_parent_turn_for_test(&processor, &params)
            .await
            .unwrap();
        subscribe_test_connection_to_materialized_thread(
            &harness.processor,
            harness.connection_id,
            &harness.workspace_id,
            "pioneer9-voice-parent",
        )
        .await;
        while harness.rx.try_recv().is_ok() {}
        let request_id = RequestId::new("R".repeat(21)).unwrap();
        harness
            .processor
            .send_turn_start_failure_for_test(
                harness.connection_id,
                request_id.clone(),
                &TurnStartSuccessResponse::TurnStart,
                "pioneer9-voice-parent",
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
                "pioneer9-voice-parent",
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
    let (_, events) =
        capture_task_start(run_gateway_start_failure(PreparationFailure::RetryBackoff));
    assert_eq!(
        events.len(),
        1,
        "a retry before its backoff deadline must not start"
    );
    assert_safe_events(&events);
}

#[test]
fn gateway_terminal_run_during_history_preparation_keeps_cancellation() {
    let (_, events) = capture_task_start(run_gateway_start_failure(
        PreparationFailure::TerminalHistory,
    ));
    assert!(
        !events.iter().any(|event| {
            matches!(event.contexts.get("Rust Tracing Fields"),
                    Some(sentry::protocol::Context::Other(fields))
                    if fields.get("stage") == Some(&serde_json::json!("history_preparation")))
        }),
        "superseded preparation must not report a Task start failure"
    );
    assert_safe_events(&events);
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
    create_cli_child_with_attachment(
        harness,
        processor,
        label,
        review,
        reviewer,
        TaskAttachmentMode::Detached,
    )
    .await
}

async fn create_cli_child_with_attachment(
    harness: &CliRuntimeSkillPreflightHarness,
    processor: &Arc<MessageProcessor>,
    label: &str,
    review: bool,
    reviewer: bool,
    attachment: TaskAttachmentMode,
) -> (pioneer_protocol::TaskCreateResponse, TestChildRuntimeAnchor) {
    create_cli_child_with_attachment_and_write_policy(
        harness, processor, label, review, reviewer, attachment, false,
    )
    .await
}

async fn create_cli_child_with_attachment_and_write_policy(
    harness: &CliRuntimeSkillPreflightHarness,
    processor: &Arc<MessageProcessor>,
    label: &str,
    review: bool,
    reviewer: bool,
    attachment: TaskAttachmentMode,
    scoped_write: bool,
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
    if scoped_write {
        params.agent_spec.as_mut().unwrap().tool_policy =
            Some(pioneer_protocol::TaskAgentToolPolicy {
                allowed_tools: Vec::new(),
                denied_tools: Vec::new(),
                write_mode: pioneer_protocol::TaskAgentWriteMode::ScopedWrite,
                allowed_paths: vec!["src".to_owned()],
                network_access: false,
            });
        params.concurrency_policy = Some(pioneer_protocol::TaskConcurrencyPolicy {
            key: None,
            max_parallel_runs: 1,
            on_conflict: pioneer_protocol::TaskConcurrencyConflictPolicy::Reject,
        });
    }
    if attachment == TaskAttachmentMode::Attached {
        // Composer launches are detached. An attached CLI Task selects its
        // exact native launch independently of composer metadata.
        params.metadata = None;
        params.launch = Some(
            exact_cli_task_launch_for_test(
                processor,
                &harness.workspace_id,
                &format!("cli_runtime:{}", harness.runtime_id),
                "gpt-5",
                &harness.runtime_id,
            )
            .await
            .unwrap(),
        );
        params.lifecycle_policy = Some(TaskLifecyclePolicy {
            attachment,
            on_parent_cancel: TaskParentTerminalAction::Cancel,
            on_parent_failure: TaskParentTerminalAction::Cancel,
            completion: TaskCompletionBehavior::CompleteOnTerminalRun,
        });
    }
    if review {
        params.agent_spec.as_mut().unwrap().review_policy = Some(TaskAgentReviewPolicy {
            mode: TaskAgentReviewMode::UserApproval,
            max_revision_rounds: 2,
            require_explicit_acceptance: true,
            disable_timeout_acceptance: false,
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
    harness
        .cli_session
        .set_next_native_turn_id(format!("native-{label}"))
        .await;
    let response = create_task_for_test(processor, params).await.unwrap();
    let run = response.run.as_ref().unwrap();
    let lineage = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
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
                && harness.cli_session.turn_starts.lock().await.len() == activations + 1
                && harness
                    .crud_store
                    .get_task_run(&run.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status
                    == TaskRunStatus::Running
                && harness
                    .crud_store
                    .get_cli_runtime_turn_binding(&child.turn_id)
                    .await
                    .unwrap()
                    .is_some_and(|binding| {
                        binding.native_turn_id.as_deref()
                            == Some(format!("native-{label}").as_str())
                    })
            {
                return TestChildRuntimeAnchor {
                    child_thread_id: scope.child_thread_id,
                    child_turn_id: child.turn_id,
                    parent_thread_id: scope.parent_thread_id,
                    created_by_thread_id: scope.created_by_thread_id,
                    created_by_turn_id: scope.created_by_turn_id,
                };
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial CLI child must persist its exact native activation ACK within 30 seconds");
    assert_eq!(
        harness.cli_session.turn_starts.lock().await.len(),
        activations + 1
    );
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
                let typed = TaskStartFailure::from_error(
                    TaskStartStage::CliAdmission,
                    cantopen_with_private_context().await,
                )
                .with_correlation_id("revision-original-correlation".to_owned());
                *processor.task_cli_admission_failure.lock().unwrap() =
                    Some(TurnStartFailure::internal_typed(
                        TaskStartStage::ExecutorStart,
                        anyhow::Error::new(typed).context("private nested admission Context"),
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
            let saved_run = harness
                .crud_store
                .get_task_run(&run.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(saved_run.status, TaskRunStatus::Blocked);
            let revision = harness
                .crud_store
                .list_task_run_turns(&run.id)
                .await
                .unwrap()
                .into_iter()
                .find(|turn| turn.kind == TaskRunTurnKind::Revision)
                .unwrap();
            assert_eq!(revision.status, TaskRunTurnStatus::Blocked);
            let saved_error = saved_run.error.unwrap();
            let TaskValue::Object(details) = saved_error.details.unwrap() else {
                panic!("safe details");
            };
            assert_eq!(
                details.get("stage"),
                Some(&TaskValue::from(if late {
                    "cli_preparation"
                } else {
                    "cli_admission"
                }))
            );
            assert_eq!(
                details.get("cause"),
                Some(&TaskValue::from(if expected {
                    "validation"
                } else {
                    "storage"
                }))
            );
            assert_eq!(
                details.get("correlation_id"),
                Some(&TaskValue::from(public.correlation_id.clone()))
            );
            if !expected {
                assert_eq!(
                    details.get("sqlite_primary_code"),
                    Some(&TaskValue::Integer(14))
                );
                assert_eq!(
                    details.get("sqlite_extended_code"),
                    Some(&TaskValue::Integer(14))
                );
            }
            if !expected && !late {
                assert_eq!(public.correlation_id, "revision-original-correlation");
            }
            assert_eq!(
                details.get("taskRunTurnId"),
                Some(&TaskValue::from(revision.id.clone()))
            );
            assert_eq!(
                details.get("requestedByReviewEventId"),
                revision
                    .requested_by_review_event_id
                    .clone()
                    .map(TaskValue::from)
                    .as_ref()
            );
            if !late {
                assert!(
                    harness
                        .crud_store
                        .get_turn(&revision.thread_id, &revision.turn_id)
                        .await
                        .unwrap()
                        .is_none(),
                    "preflight failure must not invent an actual Turn"
                );
            }
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
        let run_id = task.run.as_ref().unwrap().id.clone();
        let frozen_before = harness
            .crud_store
            .get_task_run_conversation_snapshot(&run_id)
            .await
            .unwrap()
            .expect("CLI preparation freezes its conversation before activation");
        let runtime_before = harness
            .crud_store
            .get_turn_runtime_snapshot(&child.child_turn_id)
            .await
            .unwrap();
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
        let frozen_after = harness
            .crud_store
            .get_task_run_conversation_snapshot(run_id)
            .await
            .unwrap()
            .expect("pending reconciliation retains its frozen input");
        assert_eq!(frozen_after, frozen_before);
        let runtime_after = harness
            .crud_store
            .get_turn_runtime_snapshot(&child.child_turn_id)
            .await
            .unwrap();
        assert_eq!(
            runtime_after, runtime_before,
            "pending reconciliation must preserve any provider runtime snapshot that existed"
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
    // A provider completion includes its transcript boundary and terminal CLI
    // cleanup. A plain Turn projection leaves the native attempt/session live.
    let binding = harness
        .crud_store
        .get_cli_runtime_turn_binding(&child.child_turn_id)
        .await
        .unwrap()
        .expect("activated CLI child binding");
    harness
        .cli_session
        .record_provider_assistant_message(
            &binding.native_thread_id,
            r#"<task_result>{"summary":"safe candidate","data":{"ok":true}}</task_result>"#,
        )
        .await;
    harness
        .cli_session
        .record_provider_boundary(
            &binding.native_thread_id,
            binding
                .native_turn_id
                .as_deref()
                .expect("activated native turn"),
        )
        .await;
    let finished = harness
        .processor
        .thread_manager
        .turn_finish(
            &child.child_thread_id,
            &child.child_turn_id,
            TurnStatus::Completed,
            None,
        )
        .await
        .unwrap();
    harness
        .crud_store
        .materialize_native_agent_turn_event_owned(
            pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                workspace_id: finished.workspace_id,
                thread_id: finished.thread_id,
                turn: finished.turn,
            }),
            timestamp,
            None,
            harness.processor.turn_execution_owner_id.as_ref(),
        )
        .await
        .unwrap();
    harness
        .processor
        .cleanup_completed_cli_task_fixture(&child.child_turn_id)
        .await;
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
                create_review_cli_child(&harness, &processor, "batch-healthy", true, false).await;
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
                pioneer_observability::DatabaseOperationSite::TaskChildReconcile,
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
                TaskRunStatus::WaitingReview,
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
            let healthy_turn = harness
                .crud_store
                .get_task_run_turn_by_turn(
                    &healthy_child.child_thread_id,
                    &healthy_child.child_turn_id,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(healthy_turn.status, TaskRunTurnStatus::CandidateCreated);
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
    assert_eq!(
        run.status,
        TaskRunStatus::Blocked,
        "children={:?}, run error={:?}",
        saved.task_run_turns,
        run.error
    );
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
    assert_eq!(saved_correlation.len(), 21);
    assert!(
        saved_correlation
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric())
    );
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
        persist_completed_child(&harness, &child, now_timestamp_secs()).await;
        let error = processor
            .task_agent_executor
            .reconcile_child_turn_completed(
                &child.child_thread_id,
                &child.child_turn_id,
                super::super::TaskChildReconciliationOrigin::Live,
            )
            .await
            .expect_err("late reviewer preparation remains a failed completion");
        let failure = error
            .downcast_ref::<TaskStartFailure>()
            .expect("late reviewer completion must preserve its typed failure");
        assert_eq!(failure.descriptor().stage, TaskStartStage::CliPreparation);
        assert_eq!(failure.descriptor().cause, TaskStartCause::Storage);
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

#[test]
fn terminal_child_pending_parent_receipt_retains_capsule_until_replayed_cleanup() {
    capture_task_start(async {
        let (harness, processor) = review_cli_fixture().await;
        let (task, child) =
            create_review_cli_child(&harness, &processor, "receipt-cleanup", false, false).await;
        let store = &harness.crud_store;
        let run_id = &task.run.as_ref().unwrap().id;
        let parent_thread = child
            .created_by_thread_id
            .as_deref()
            .unwrap_or(&child.parent_thread_id);
        let parent_turn = child.created_by_turn_id.as_deref().unwrap();
        let (workspace_id, mut parent) = store
            .get_turn(parent_thread, parent_turn)
            .await
            .unwrap()
            .unwrap();
        parent.status = TurnStatus::Completed;
        let now = now_timestamp_secs();
        // Real canonical acceptance, held before projection while child
        // reconciliation commits the aggregate and encounters the same event.
        store
            .accept_turn_event_before_projection_for_test(
                pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(TurnCompletedNotification {
                    workspace_id,
                    thread_id: parent_thread.to_owned(),
                    turn: parent,
                }),
                now,
            )
            .await
            .unwrap();
        store
            .insert_turn_llm_context(pioneer_crud::NewTurnLlmContextEntry {
                turn_id: child.child_turn_id.clone(),
                item_id: None,
                attempt_id: None,
                sequence: 999,
                source: "receipt_cleanup_fixture".into(),
                tool_name: None,
                payload: "{}".into(),
                output_policy_snapshot: "{}".into(),
                created_at: chrono::Utc::now().fixed_offset(),
                expires_at: Some(chrono::Utc::now().fixed_offset()),
            })
            .await
            .unwrap();
        persist_completed_child(&harness, &child, now).await;
        retain_cleanup_fixture_snapshot(store, &child.child_turn_id).await;
        let activations = harness.cli_session.turn_starts.lock().await.len();
        let background = processor.for_background_reconciliation();
        let pending = background
            .reconcile_terminal_task_child_turns(64)
            .await
            .unwrap_err();
        assert!(pending.to_string().contains("pending_projection=1"));
        assert_eq!(
            store.get_task_run(run_id).await.unwrap().unwrap().status,
            TaskRunStatus::Succeeded
        );
        let candidate = store
            .get_accepted_task_result_candidate(run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .terminal_task_child_cleanup_ready(&child.child_thread_id, &child.child_turn_id)
                .await
                .unwrap(),
            Some(false)
        );
        assert!(
            !processor
                .task_agent_executor
                .reconcile_child_turn_completed(
                    &child.child_thread_id,
                    &child.child_turn_id,
                    super::super::TaskChildReconciliationOrigin::Live,
                )
                .await
                .unwrap(),
            "terminal run alone is not a cleanup ACK"
        );
        // Bootstrap/TTL and a fresh worker scope simulate loss of local state.
        store
            .delete_turn_runtime_snapshots_for_closed_turns()
            .await
            .unwrap();
        store
            .delete_turn_llm_context_for_terminal_turns()
            .await
            .unwrap();
        store.delete_expired_turn_llm_context().await.unwrap();
        assert!(
            store
                .get_turn_runtime_snapshot(&child.child_turn_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .list_turn_llm_context(&child.child_turn_id)
                .await
                .unwrap()
                .iter()
                .any(|c| c.source == "receipt_cleanup_fixture")
        );
        assert_eq!(
            processor
                .for_background_reconciliation()
                .reconcile_terminal_task_child_turns(64)
                .await
                .unwrap(),
            0
        );
        // A quarantined projection remains visible and cannot authorize ACK.
        use sea_orm::{ConnectionTrait, DbBackend, Statement};
        store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event_projection_stream_state SET status = 'quarantined' WHERE turn_id = ?",
            [parent_turn.into()],
        )).await.unwrap();
        assert_eq!(
            store
                .get_turn_event_projection_stream_state(parent_turn)
                .await
                .unwrap()
                .unwrap()
                .health,
            pioneer_crud::TurnProjectionStreamHealth::Quarantined
        );
        assert_eq!(
            background
                .reconcile_terminal_task_child_turns(64)
                .await
                .unwrap(),
            0
        );
        assert!(
            store
                .get_turn_runtime_snapshot(&child.child_turn_id)
                .await
                .unwrap()
                .is_some()
        );
        store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event_projection_stream_state SET status = 'healthy' WHERE turn_id = ?",
            [parent_turn.into()],
        )).await.unwrap();
        let replay = store
            .with_maintenance_access()
            .replay_due_turn_event_projections(now + 120, 64)
            .await
            .unwrap();
        assert!(replay.projected > 0);
        assert_eq!(
            processor
                .for_background_reconciliation()
                .reconcile_terminal_task_child_turns(64)
                .await
                .unwrap(),
            1
        );
        assert!(
            store
                .get_turn_runtime_snapshot(&child.child_turn_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !store
                .list_turn_llm_context(&child.child_turn_id)
                .await
                .unwrap()
                .iter()
                .any(|c| c.source == "receipt_cleanup_fixture")
        );
        assert_eq!(
            processor
                .for_background_reconciliation()
                .reconcile_terminal_task_child_turns(64)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            harness.cli_session.turn_starts.lock().await.len(),
            activations
        );
        assert_eq!(
            store
                .get_accepted_task_result_candidate(run_id)
                .await
                .unwrap()
                .unwrap(),
            candidate,
            "no repeated extraction or new result"
        );
    });
}

async fn retain_cleanup_fixture_snapshot(store: &pioneer_crud::CrudStore, turn_id: &str) {
    // Native CLI activation persists a binding, not a provider runtime
    // snapshot. Seed a valid snapshot explicitly to exercise capsule cleanup
    // for both storage forms without changing the native activation path.
    let binding = store
        .get_cli_runtime_turn_binding(turn_id)
        .await
        .unwrap()
        .expect("cleanup fixture must have an activated CLI binding");
    let now = chrono::Utc::now().fixed_offset();
    store
        .upsert_turn_runtime_snapshot(pioneer_crud::NewTurnRuntimeSnapshot {
            turn_id: turn_id.to_owned(),
            thread_id: binding.thread_id,
            workspace_id: binding.workspace_id,
            mode_json: r#""Agent""#.to_owned(),
            model: "gpt-5".to_owned(),
            provider_name: format!("cli_runtime:{}", binding.runtime_id),
            reasoning_effort: None,
            agent_skill_versions_json: None,
            hook_runtime_context_json: "{}".to_owned(),
            workspace_skill_policies_json: "[]".to_owned(),
            input_json: "[]".to_owned(),
            capabilities_json: "[]".to_owned(),
            resolved_artifacts_json: "[]".to_owned(),
            runtime_environment_json: "{}".to_owned(),
            history_json: "[]".to_owned(),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
}

async fn retain_cleanup_fixture_context(store: &pioneer_crud::CrudStore, turn_id: &str) {
    retain_cleanup_fixture_snapshot(store, turn_id).await;
    for (sequence, source) in [
        (998, "cleanup_obligation_fixture"),
        (999, "assistant_round"),
    ] {
        store
            .insert_turn_llm_context(pioneer_crud::NewTurnLlmContextEntry {
                turn_id: turn_id.to_owned(),
                item_id: None,
                attempt_id: None,
                sequence,
                source: source.to_owned(),
                tool_name: None,
                payload: "{}".to_owned(),
                output_policy_snapshot: "{}".to_owned(),
                created_at: chrono::Utc::now().fixed_offset(),
                expires_at: Some(chrono::Utc::now().fixed_offset()),
            })
            .await
            .unwrap();
    }
}

async fn change_cleanup_fixture(
    store: &pioneer_crud::CrudStore,
    sql: &str,
    values: Vec<sea_orm::Value>,
) {
    use sea_orm::{ConnectionTrait, DbBackend, Statement};
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            values,
        ))
        .await
        .unwrap();
}

async fn assert_cleanup_fixture_retained(
    store: &pioneer_crud::CrudStore,
    child: &TestChildRuntimeAnchor,
) {
    assert!(
        store
            .get_turn_runtime_snapshot(&child.child_turn_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .list_turn_llm_context(&child.child_turn_id)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.source == "cleanup_obligation_fixture")
    );
}

async fn assert_terminal_cleanup_fixture_denied(
    processor: &Arc<MessageProcessor>,
    child: &TestChildRuntimeAnchor,
) {
    let store = &processor.crud_store;
    assert_eq!(
        store
            .terminal_task_child_cleanup_ready(&child.child_thread_id, &child.child_turn_id)
            .await
            .unwrap(),
        Some(false)
    );
    assert!(
        !store
            .list_unreconciled_terminal_task_child_turns(64)
            .await
            .unwrap()
            .iter()
            .any(|turn| turn.turn_id == child.child_turn_id)
    );
    assert!(
        !matches!(
            processor
                .task_agent_executor
                .reconcile_child_turn_completed(
                    &child.child_thread_id,
                    &child.child_turn_id,
                    super::super::TaskChildReconciliationOrigin::DurableBackground,
                )
                .await,
            Ok(true)
        ),
        "missing/invalid required lineage must not authorize an ACK"
    );
    assert_eq!(
        store
            .cleanup_terminal_task_child_capsule(&child.child_thread_id, &child.child_turn_id)
            .await
            .unwrap(),
        Some(false),
        "writer must reject a stale discovery or ACK"
    );
    assert_cleanup_fixture_retained(store, child).await;
}

#[test]
fn immediate_attached_terminal_child_cleanup_survives_lost_local_continuation() {
    for (detach_after_launch, context_only) in [(false, false), (true, false), (false, true)] {
        capture_task_start(async move {
            let (harness, processor) = review_cli_fixture().await;
            let (task, child) = create_cli_child_with_attachment(
                &harness,
                &processor,
                "attached-cleanup",
                false,
                false,
                TaskAttachmentMode::Attached,
            )
            .await;
            let store = &harness.crud_store;
            let run_id = &task.run.as_ref().unwrap().id;
            let parent_thread = child.created_by_thread_id.as_deref().unwrap();
            let parent_turn = child.created_by_turn_id.as_deref().unwrap();
            let parent_before = store
                .get_turn(parent_thread, parent_turn)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(parent_before.1.turn_kind, TurnKind::Conversation);
            assert_eq!(parent_before.1.status, TurnStatus::InProgress);
            assert!(
                store
                    .get_turn(parent_thread, run_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            if detach_after_launch {
                detach_task_for_test(
                    &processor,
                    pioneer_protocol::TaskDetachParams {
                        task_id: task.task.id.clone(),
                    },
                )
                .await
                .unwrap();
            }
            persist_completed_child(&harness, &child, now_timestamp_secs()).await;
            // Complete the real aggregate/extraction, but omit the caller's
            // capsule deletion, as on cancellation/process loss at this boundary.
            assert!(
                processor
                    .task_agent_executor
                    .reconcile_child_turn_completed(
                        &child.child_thread_id,
                        &child.child_turn_id,
                        super::super::TaskChildReconciliationOrigin::Live,
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                store.get_task_run(run_id).await.unwrap().unwrap().status,
                TaskRunStatus::Succeeded
            );
            let candidate = store
                .get_accepted_task_result_candidate(run_id)
                .await
                .unwrap()
                .unwrap();
            let activations = harness.cli_session.turn_starts.lock().await.len();
            processor
                .task_output_capture_failures
                .lock()
                .unwrap()
                .insert(
                    child.child_turn_id.clone(),
                    anyhow::anyhow!("terminal cleanup must not re-enter output capture/extraction"),
                );
            // Add the retained/non-retained rows after extraction so the model
            // history canary does not fabricate an input to the extractor.
            retain_cleanup_fixture_context(store, &child.child_turn_id).await;
            store
                .delete_turn_llm_context_for_terminal_turns()
                .await
                .unwrap();
            store.delete_expired_turn_llm_context().await.unwrap();
            store
                .delete_turn_runtime_snapshots_for_closed_turns()
                .await
                .unwrap();
            assert_cleanup_fixture_retained(store, &child).await;
            if context_only {
                store
                    .delete_turn_runtime_snapshot(&child.child_turn_id)
                    .await
                    .unwrap();
            }
            let background = processor.for_background_reconciliation();
            assert_eq!(
                background
                    .reconcile_terminal_task_child_turns(64)
                    .await
                    .unwrap(),
                1
            );
            assert!(
                store
                    .get_turn_runtime_snapshot(&child.child_turn_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            let contexts = store
                .list_turn_llm_context(&child.child_turn_id)
                .await
                .unwrap();
            assert!(
                !contexts
                    .iter()
                    .any(|entry| entry.source == "cleanup_obligation_fixture")
            );
            assert!(
                contexts
                    .iter()
                    .any(|entry| entry.source == "assistant_round")
            );
            assert_eq!(
                processor
                    .for_background_reconciliation()
                    .reconcile_terminal_task_child_turns(64)
                    .await
                    .unwrap(),
                0,
                "retained model history must not keep selecting the cleaned child"
            );
            assert_eq!(
                store
                    .get_turn(parent_thread, parent_turn)
                    .await
                    .unwrap()
                    .unwrap(),
                parent_before
            );
            assert!(
                store
                    .get_turn(parent_thread, run_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                activations
            );
            assert!(
                processor
                    .task_output_capture_failures
                    .lock()
                    .unwrap()
                    .contains_key(&child.child_turn_id)
            );
            assert_eq!(
                store
                    .get_accepted_task_result_candidate(run_id)
                    .await
                    .unwrap()
                    .unwrap(),
                candidate
            );
        });
    }
}

#[test]
fn terminal_child_cleanup_revalidates_required_lineage_and_resumable_fence() {
    for attachment in [TaskAttachmentMode::Attached, TaskAttachmentMode::Detached] {
        capture_task_start(async move {
            let (harness, processor) = review_cli_fixture().await;
            let (task, child) = create_cli_child_with_attachment(
                &harness,
                &processor,
                "cleanup-lineage",
                false,
                false,
                attachment,
            )
            .await;
            let store = &harness.crud_store;
            persist_completed_child(&harness, &child, now_timestamp_secs()).await;
            processor
                .task_agent_executor
                .reconcile_child_turn_completed(
                    &child.child_thread_id,
                    &child.child_turn_id,
                    super::super::TaskChildReconciliationOrigin::Live,
                )
                .await
                .unwrap();
            retain_cleanup_fixture_context(store, &child.child_turn_id).await;
            let lineage = store
                .get_task_thread_lineage(&child.child_thread_id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                store
                    .list_unreconciled_terminal_task_child_turns(64)
                    .await
                    .unwrap()
                    .iter()
                    .any(|turn| turn.turn_id == child.child_turn_id)
            );
            // Discovery and ACK can become stale before writer cleanup.
            assert_eq!(
                store
                    .terminal_task_child_cleanup_ready(&child.child_thread_id, &child.child_turn_id)
                    .await
                    .unwrap(),
                Some(true)
            );
            for missing_parent in [None, Some("unrelated-missing-parent")] {
                change_cleanup_fixture(
                    store,
                    "UPDATE thread_lineage SET created_by_turn_id = ? WHERE child_thread_id = ?",
                    vec![
                        missing_parent.map(str::to_owned).into(),
                        child.child_thread_id.clone().into(),
                    ],
                )
                .await;
                assert_terminal_cleanup_fixture_denied(&processor, &child).await;
            }
            change_cleanup_fixture(store,
                "UPDATE thread_lineage SET created_by_turn_id = ?, root_thread_id = child_thread_id WHERE child_thread_id = ?",
                vec![lineage.created_by_turn_id.clone().into(), child.child_thread_id.clone().into()],
            ).await;
            assert_terminal_cleanup_fixture_denied(&processor, &child).await;
            change_cleanup_fixture(
                store,
                "UPDATE thread_lineage SET root_thread_id = ? WHERE child_thread_id = ?",
                vec![
                    lineage.root_thread_id.into(),
                    child.child_thread_id.clone().into(),
                ],
            )
            .await;
            if attachment == TaskAttachmentMode::Attached {
                let scope = store
                    .get_task_run_conversation_snapshot(&task.run.as_ref().unwrap().id)
                    .await
                    .unwrap()
                    .unwrap();
                for source in [None, Some("unrelated-accepted-source")] {
                    change_cleanup_fixture(
                        store,
                        "UPDATE task_run_conversation_snapshot SET source_turn_id = ? WHERE run_id = ?",
                        vec![source.map(str::to_owned).into(), task.run.as_ref().unwrap().id.clone().into()],
                    )
                    .await;
                    assert_terminal_cleanup_fixture_denied(&processor, &child).await;
                }
                change_cleanup_fixture(
                    store,
                    "UPDATE task_run_conversation_snapshot SET source_turn_id = ? WHERE run_id = ?",
                    vec![
                        scope.source_turn_id.into(),
                        task.run.as_ref().unwrap().id.clone().into(),
                    ],
                )
                .await;
                let occurrence = store
                    .get_task_occurrence_contract_by_run(&task.run.as_ref().unwrap().id)
                    .await
                    .unwrap()
                    .unwrap();
                change_cleanup_fixture(
                    store,
                    "UPDATE task_occurrence_contract SET occurrence_key = 'scheduled:unrelated' WHERE run_id = ?",
                    vec![task.run.as_ref().unwrap().id.clone().into()],
                )
                .await;
                assert_terminal_cleanup_fixture_denied(&processor, &child).await;
                change_cleanup_fixture(
                    store,
                    "UPDATE task_occurrence_contract SET occurrence_key = ? WHERE run_id = ?",
                    vec![
                        occurrence.occurrence_key.into(),
                        task.run.as_ref().unwrap().id.clone().into(),
                    ],
                )
                .await;
            }
            change_cleanup_fixture(
                store,
                "UPDATE task_run_turn SET status = 'blocked' WHERE turn_id = ?",
                vec![child.child_turn_id.clone().into()],
            )
            .await;
            assert_terminal_cleanup_fixture_denied(&processor, &child).await;
            // Even after a terminal aggregate a resumable child keeps its capsule.
        });
    }
}

#[test]
fn detached_child_cannot_replace_required_occurrence_with_conversation_lineage() {
    capture_task_start(async {
        let (harness, processor) = review_cli_fixture().await;
        let (task, child) =
            create_review_cli_child(&harness, &processor, "required-occurrence", false, false)
                .await;
        let store = &harness.crud_store;
        persist_completed_child(&harness, &child, now_timestamp_secs()).await;
        processor
            .task_agent_executor
            .reconcile_child_turn_completed(
                &child.child_thread_id,
                &child.child_turn_id,
                super::super::TaskChildReconciliationOrigin::Live,
            )
            .await
            .unwrap();
        retain_cleanup_fixture_context(store, &child.child_turn_id).await;
        change_cleanup_fixture(store,
            "UPDATE thread_lineage SET created_by_turn_id = ?, created_by_thread_id = ? WHERE child_thread_id = ?",
            vec![task.task.created_by_turn_id.clone().into(), task.task.created_by_thread_id.clone().into(), child.child_thread_id.clone().into()],
        ).await;
        assert_terminal_cleanup_fixture_denied(&processor, &child).await;
        change_cleanup_fixture(
            store,
            "DELETE FROM thread_lineage WHERE child_thread_id = ?",
            vec![child.child_thread_id.clone().into()],
        )
        .await;
        assert_terminal_cleanup_fixture_denied(&processor, &child).await;
    });
}

#[test]
fn blocked_attached_child_keeps_capsule_for_explicit_resume() {
    capture_task_start(async {
        let (harness, processor) = review_cli_fixture().await;
        let (task, child) = create_cli_child_with_attachment(
            &harness,
            &processor,
            "blocked-attached-cleanup",
            false,
            false,
            TaskAttachmentMode::Attached,
        )
        .await;
        let store = &harness.crud_store;
        retain_cleanup_fixture_context(store, &child.child_turn_id).await;
        // Activation ACK can precede release of the launch transition. Wait
        // for it, then capture the native source under the same gate that the
        // guarded Blocked commit and terminal cleanup will hold.
        let transition = tokio::time::timeout(
            Duration::from_secs(30),
            processor.cli_runtime_turn_resume_transition(&child.child_turn_id),
        )
        .await
        .expect("initial CLI launch must release its transition within 30 seconds")
        .unwrap()
        .expect("activated CLI child must have a transition gate");
        let binding = store
            .get_cli_runtime_turn_binding(&child.child_turn_id)
            .await
            .unwrap()
            .expect("activated CLI child binding");
        let guard = store
            .cli_runtime_blocked_turn_guard(&binding, binding.native_turn_id.as_deref().unwrap())
            .await
            .unwrap()
            .expect("Blocked fixture must target the active native attempt");
        assert!(
            processor
                .mark_turn_blocked_with_recovery(
                    child.child_thread_id.clone(),
                    child.child_turn_id.clone(),
                    "explicit resume required".to_owned(),
                    None,
                    Some(guard),
                    Some(Arc::new(transition)),
                )
                .await
        );
        assert_eq!(
            store
                .get_turn(&child.child_thread_id, &child.child_turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Blocked
        );
        assert_eq!(
            store
                .get_task_run(&task.run.as_ref().unwrap().id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskRunStatus::Blocked
        );
        assert!(
            !processor
                .task_agent_executor
                .reconcile_child_turn_completed(
                    &child.child_thread_id,
                    &child.child_turn_id,
                    super::super::TaskChildReconciliationOrigin::DurableBackground,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            processor
                .for_background_reconciliation()
                .reconcile_terminal_task_child_turns(64)
                .await
                .unwrap(),
            0
        );
        store
            .delete_turn_runtime_snapshots_for_closed_turns()
            .await
            .unwrap();
        store
            .delete_turn_llm_context_for_terminal_turns()
            .await
            .unwrap();
        assert_cleanup_fixture_retained(store, &child).await;
    });
}

#[test]
fn revision_early_authority_and_lock_admission_complete_the_reserved_revision() {
    for case in ["lock_reject", "authority_missing", "authority_persistence"] {
        let (_, events) = capture_task_start(async move {
            use sea_orm::{
                ActiveModelTrait,
                ActiveValue::{Set, Unchanged},
                ConnectionTrait, EntityTrait,
            };
            let (harness, processor) = review_cli_fixture().await;
            let (task, child) = create_cli_child_with_attachment_and_write_policy(
                &harness,
                &processor,
                "early-revision",
                true,
                false,
                TaskAttachmentMode::Detached,
                case == "lock_reject",
            )
            .await;
            assert!(
                processor
                    .complete_turn(child.child_thread_id, child.child_turn_id, None)
                    .await
            );
            let run = task.run.unwrap();
            let candidate = harness
                .crud_store
                .list_task_result_candidates(&run.id)
                .await
                .unwrap()
                .remove(0);
            let revised = processor
                .task_runtime
                .service()
                .revise_task_result_candidate(
                    pioneer_tasks::TaskMutationContext::user(
                        authenticated_test_superuser().principal_id.to_string(),
                    ),
                    pioneer_protocol::TaskReviseParams {
                        task_id: task.task.id.clone(),
                        run_id: run.id.clone(),
                        candidate_id: candidate.id,
                        feedback: "Revise safely".to_owned(),
                        additional_instructions: Vec::new(),
                    },
                )
                .await
                .unwrap();
            let db = harness.crud_store.database_connection();
            if case == "lock_reject" {
                let locks = harness
                    .crud_store
                    .list_task_write_locks_by_run(&run.id)
                    .await
                    .unwrap();
                assert!(!locks.is_empty());
                for lock in locks {
                    assert_eq!(lock.status, pioneer_protocol::TaskWriteLockStatus::Acquired);
                    pioneer_entity::task_write_lock::ActiveModel {
                        id: Unchanged(lock.id),
                        expires_at: Set(Some(
                            chrono::DateTime::from_timestamp(now_timestamp_secs() - 60, 0)
                                .unwrap()
                                .fixed_offset(),
                        )),
                        ..Default::default()
                    }
                    .update(&db)
                    .await
                    .unwrap();
                }
                // A separate real Task takes the same scope after expiration.
                let (blocker, _) = create_cli_child_with_attachment_and_write_policy(
                    &harness,
                    &processor,
                    "revision-lock-blocker",
                    false,
                    false,
                    TaskAttachmentMode::Detached,
                    true,
                )
                .await;
                assert!(
                    harness
                        .crud_store
                        .list_task_write_locks_by_run(&blocker.run.unwrap().id)
                        .await
                        .unwrap()
                        .iter()
                        .any(|lock| lock.status == pioneer_protocol::TaskWriteLockStatus::Acquired)
                );
                db.execute_unprepared("CREATE TRIGGER reject_revision_admission BEFORE UPDATE ON task_run_turn WHEN NEW.kind='revision' AND NEW.status='failed' BEGIN SELECT RAISE(ABORT, 'injected revision failure'); END").await.unwrap();
            } else {
                assert_eq!(
                    pioneer_entity::task_execution_admission::Entity::delete_by_id(
                        task.task.id.clone()
                    )
                    .exec(&db)
                    .await
                    .unwrap()
                    .rows_affected,
                    1
                );
                if case == "authority_persistence" {
                    db.execute_unprepared("CREATE TRIGGER reject_revision_admission BEFORE UPDATE ON task_occurrence_contract WHEN NEW.status='failed' BEGIN SELECT RAISE(ABORT, 'independent persistence failure'); END").await.unwrap();
                }
            }
            let activations = harness.cli_session.turn_starts.lock().await.len();
            let attempts = processor
                .task_cli_preparation_attempts
                .load(Ordering::SeqCst);
            let outcome = dispatch_revision_turn_for_test(&processor, revised.clone()).await;
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                activations
            );
            assert_eq!(
                processor
                    .task_cli_preparation_attempts
                    .load(Ordering::SeqCst),
                attempts
            );
            assert!(
                harness
                    .crud_store
                    .get_turn(&revised.child_thread_id, &revised.child_turn_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            if case != "authority_missing" {
                let error = outcome.unwrap_err();
                let diagnostic =
                    TaskStartFailure::from_borrowed_error(TaskStartStage::ExecutorStart, &error);
                assert_eq!(diagnostic.descriptor().cause, TaskStartCause::Storage);
                assert_eq!(diagnostic.descriptor().sqlite_primary_code, Some(19));
                // The abort must leave neither a run-only terminal commit nor
                // a revision-only terminal commit, even in early admission.
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
                assert_eq!(
                    harness
                        .crud_store
                        .get_task_run_turn(&revised.task_run_turn.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    TaskRunTurnStatus::InProgress
                );
                let history = harness
                    .crud_store
                    .get_task_events(&task.task.id, None)
                    .await
                    .unwrap()
                    .events;
                assert!(!history.iter().any(|event| matches!(&event.payload,
                    TaskEventPayload::RunFailed { run_id, .. } | TaskEventPayload::RunBlocked { run_id, .. } if run_id == &run.id)));
                db.execute_unprepared("DROP TRIGGER reject_revision_admission")
                    .await
                    .unwrap();
                if case == "authority_persistence" {
                    // A real terminal persistence failure reaches the caller;
                    // it is not swallowed as the already-reported start cause.
                    assert!(format!("{error:#}").contains("independent persistence failure"));
                    return;
                }
                dispatch_revision_turn_for_test(&processor, revised.clone())
                    .await
                    .unwrap();
            } else {
                let mut error = outcome.unwrap_err();
                assert!(format!("{error:#}").contains("has no durable execution admission"));
                let failure = error.downcast_ref::<TaskStartFailure>().unwrap();
                assert_eq!(failure.descriptor().stage, TaskStartStage::ExecutorStart);
                assert_eq!(failure.descriptor().cause, TaskStartCause::Unclassified);
                let saved = harness
                    .crud_store
                    .get_task_run(&run.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .error
                    .unwrap();
                assert_eq!(
                    failure.descriptor().task_error(Some(run.id.clone())).code,
                    saved.code
                );
                let TaskValue::Object(details) = saved.details.unwrap() else {
                    panic!("safe descriptor");
                };
                assert_eq!(
                    details.get("correlation_id"),
                    Some(&TaskValue::from(
                        failure.descriptor().correlation_id.clone().unwrap()
                    ))
                );
                let original = TaskStartFailure::report_for_scheduler(&mut error);
                assert_eq!(TaskStartFailure::report_for_scheduler(&mut error), original);
            }
            let stored = harness
                .crud_store
                .get_task_run(&run.id)
                .await
                .unwrap()
                .unwrap();
            let revision = harness
                .crud_store
                .get_task_run_turn(&revised.task_run_turn.id)
                .await
                .unwrap()
                .unwrap();
            if case == "lock_reject" {
                assert_eq!(stored.status, TaskRunStatus::Failed);
                assert_eq!(revision.status, TaskRunTurnStatus::Failed);
                let error = stored.error.unwrap();
                assert_eq!(error.code, "write_lock_conflict");
                assert_eq!(error.class, TaskErrorClass::Policy);
                assert_eq!(error.message, "write_lock_conflict");
                assert_eq!(error.failed_run_id.as_deref(), Some(run.id.as_str()));
                let history = harness
                    .crud_store
                    .get_task_events(&task.task.id, None)
                    .await
                    .unwrap()
                    .events;
                let turn_event = history.iter().find(|event| matches!(&event.payload, TaskEventPayload::TaskRunTurnFailed { task_run_turn, .. } if task_run_turn.id == revision.id)).unwrap();
                let run_event = history.iter().find(|event| matches!(&event.payload, TaskEventPayload::RunFailed { run_id, .. } if run_id == &run.id)).unwrap();
                assert!(turn_event.sequence < run_event.sequence);
            } else {
                assert_eq!(stored.status, TaskRunStatus::Blocked);
                assert_eq!(revision.status, TaskRunTurnStatus::Blocked);
            }
            assert!(
                harness
                    .crud_store
                    .get_turn(&revised.child_thread_id, &revised.child_turn_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                activations
            );
            assert_eq!(
                processor
                    .task_cli_preparation_attempts
                    .load(Ordering::SeqCst),
                attempts
            );
        });
        assert_eq!(events.len(), usize::from(case != "lock_reject"), "{case}");
        assert_safe_events(&events);
    }
}

#[test]
fn revision_internal_completion_failure_is_not_reclassified_or_completed_twice() {
    for diagnostic_read in [false, true] {
        let (_, events) = capture_task_start(async move {
            use sea_orm::{ConnectionTrait, EntityTrait};

            let (harness, processor) = review_cli_fixture().await;
            let (task, child) = create_review_cli_child(
                &harness,
                &processor,
                "internal-revision-completion",
                true,
                false,
            )
            .await;
            let (_, original_turn) = harness
                .crud_store
                .get_turn(&child.child_thread_id, &child.child_turn_id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                processor
                    .complete_turn(child.child_thread_id, child.child_turn_id, None)
                    .await
            );
            let run = task.run.unwrap();
            let candidate = harness
                .crud_store
                .list_task_result_candidates(&run.id)
                .await
                .unwrap()
                .remove(0);
            let revised = processor
                .task_runtime
                .service()
                .revise_task_result_candidate(
                    pioneer_tasks::TaskMutationContext::user(
                        authenticated_test_superuser().principal_id.to_string(),
                    ),
                    pioneer_protocol::TaskReviseParams {
                        task_id: task.task.id.clone(),
                        run_id: run.id.clone(),
                        candidate_id: candidate.id,
                        feedback: "Revise safely".to_owned(),
                        additional_instructions: Vec::new(),
                    },
                )
                .await
                .unwrap();
            let db = harness.crud_store.database_connection();
            let blocked_turn = if diagnostic_read {
                // Persist a real revision Turn already Blocked, without provider
                // activation or Task reconciliation. Dispatch must recover this
                // outcome while its reserved TaskRunTurn is still InProgress.
                let thread = harness
                    .crud_store
                    .get_thread_model(&revised.child_thread_id)
                    .await
                    .unwrap()
                    .unwrap();
                let mut turn = original_turn;
                turn.id = revised.child_turn_id.clone();
                turn.status = TurnStatus::InProgress;
                turn.error = None;
                let actor = turn.author.as_ref().unwrap().actor.clone();
                harness
                    .crud_store
                    .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[], actor)
                    .await
                    .unwrap();
                turn.status = TurnStatus::Blocked;
                turn.error = Some("revision awaits explicit resume".to_owned());
                harness
                    .crud_store
                    .materialize_turn_blocked(
                        pioneer_protocol::TurnBlockedNotification {
                            workspace_id: harness.workspace_id.clone(),
                            thread_id: revised.child_thread_id.clone(),
                            turn,
                            resume: None,
                        },
                        now_timestamp_secs(),
                    )
                    .await
                    .unwrap();
                let stored = harness
                    .crud_store
                    .get_turn(&revised.child_thread_id, &revised.child_turn_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(stored.1.status, TurnStatus::Blocked);
                assert!(
                    harness
                        .crud_store
                        .get_blocked_turn_recovery_diagnostic(&revised.child_turn_id)
                        .await
                        .unwrap()
                        .is_none()
                );
                Some(stored)
            } else {
                // FK OFF matches Gateway runtime. Keep the reserved revision,
                // lineage and admission; reach the inner child_thread_missing.
                assert_eq!(
                    pioneer_entity::thread::Entity::delete_by_id(revised.child_thread_id.clone())
                        .exec(&db)
                        .await
                        .unwrap()
                        .rows_affected,
                    1
                );
                None
            };
            let history_before = harness
                .crud_store
                .get_task_events(&task.task.id, None)
                .await
                .unwrap()
                .events;
            let activations = harness.cli_session.turn_starts.lock().await.len();
            let preparations = processor
                .task_cli_preparation_attempts
                .load(Ordering::SeqCst);
            if diagnostic_read {
                // Local query fault in the existing full migrated DB: this table
                // is used by diagnostic lookup, not get_turn/Task terminal writes.
                // Renaming and restoring it through the normal DB handle avoids a
                // production injection hook or a separate simplified schema.
                db.execute_unprepared("ALTER TABLE recovery_terminalization_outbox RENAME TO withheld_revision_diagnostic_outbox").await.unwrap();
                assert!(
                    harness
                        .crud_store
                        .get_blocked_turn_recovery_diagnostic(&revised.child_turn_id)
                        .await
                        .is_err()
                );
                assert_eq!(
                    harness
                        .crud_store
                        .get_turn(&revised.child_thread_id, &revised.child_turn_id)
                        .await
                        .unwrap(),
                    blocked_turn
                );
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
            } else {
                // Reject only the known payload; a second Storage fallback could
                // commit, so a permanent writer fault cannot mask the old bug.
                db.execute_unprepared("CREATE TRIGGER reject_original_revision_completion BEFORE INSERT ON task_event WHEN json_extract(NEW.payload_json, '$.payload.error.code')='child_thread_missing' BEGIN SELECT RAISE(ABORT, 'independent inner completion failure'); END").await.unwrap();
            }
            let error = dispatch_revision_turn_for_test(&processor, revised.clone())
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains(if diagnostic_read {
                "recovery_terminalization_outbox"
            } else {
                "independent inner completion failure"
            }));
            assert!(
                error
                    .chain()
                    .any(|source| source.downcast_ref::<sea_orm::DbErr>().is_some())
            );
            assert!(error.downcast_ref::<TaskStartFailure>().is_none());
            let diagnostic =
                TaskStartFailure::from_borrowed_error(TaskStartStage::ExecutorStart, &error);
            assert_eq!(diagnostic.descriptor().cause, TaskStartCause::Storage);
            assert_eq!(
                diagnostic.descriptor().sqlite_primary_code,
                Some(if diagnostic_read { 1 } else { 19 })
            );
            assert!(diagnostic.descriptor().sqlite_extended_code.is_some());
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
            assert_eq!(
                harness
                    .crud_store
                    .get_task_run_turn(&revised.task_run_turn.id)
                    .await
                    .unwrap()
                    .unwrap(),
                revised.task_run_turn
            );
            assert_eq!(
                harness
                    .crud_store
                    .get_task_events(&task.task.id, None)
                    .await
                    .unwrap()
                    .events,
                history_before
            );
            assert_eq!(
                harness
                    .crud_store
                    .get_turn(&revised.child_thread_id, &revised.child_turn_id)
                    .await
                    .unwrap(),
                blocked_turn
            );
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                activations
            );
            assert_eq!(
                processor
                    .task_cli_preparation_attempts
                    .load(Ordering::SeqCst),
                preparations
            );

            // Explicit test redispatch after removing the independent fault; this
            // is not an automatic completion retry or a new runtime retry policy.
            if diagnostic_read {
                db.execute_unprepared("ALTER TABLE withheld_revision_diagnostic_outbox RENAME TO recovery_terminalization_outbox").await.unwrap();
                assert!(
                    harness
                        .crud_store
                        .get_blocked_turn_recovery_diagnostic(&revised.child_turn_id)
                        .await
                        .unwrap()
                        .is_none()
                );
            } else {
                db.execute_unprepared("DROP TRIGGER reject_original_revision_completion")
                    .await
                    .unwrap();
            }
            dispatch_revision_turn_for_test(&processor, revised.clone())
                .await
                .unwrap();
            let stored = harness
                .crud_store
                .get_task_run(&run.id)
                .await
                .unwrap()
                .unwrap();
            let revision = harness
                .crud_store
                .get_task_run_turn(&revised.task_run_turn.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.status, TaskRunStatus::Blocked);
            assert_eq!(revision.status, TaskRunTurnStatus::Blocked);
            let error = stored.error.unwrap();
            assert_eq!(
                error.code,
                if diagnostic_read {
                    "child_turn_blocked"
                } else {
                    "child_thread_missing"
                }
            );
            assert_eq!(
                error.class,
                if diagnostic_read {
                    TaskErrorClass::Policy
                } else {
                    TaskErrorClass::Internal
                }
            );
            assert_eq!(
                error.message,
                if diagnostic_read {
                    "child_turn_blocked"
                } else {
                    "child_thread_missing"
                }
            );
            assert!(error.recovery_diagnostic.is_none());
            assert_eq!(error.failed_run_id.as_deref(), Some(run.id.as_str()));
            let TaskValue::Object(details) = error.details.as_ref().unwrap() else {
                panic!("revision details");
            };
            assert_eq!(
                details.get("taskRunTurnId"),
                Some(&TaskValue::from(revision.id.clone()))
            );
            assert_eq!(
                details.get("threadId"),
                Some(&TaskValue::from(revised.child_thread_id.clone()))
            );
            assert_eq!(
                details.get("turnId"),
                Some(&TaskValue::from(revised.child_turn_id.clone()))
            );
            assert!(!details.contains_key("cause"));
            assert!(!details.contains_key("correlation_id"));
            let history = harness
                .crud_store
                .get_task_events(&task.task.id, None)
                .await
                .unwrap()
                .events;
            let blocked_turns = history
                .iter()
                .filter_map(|event| match &event.payload {
                    TaskEventPayload::TaskRunTurnBlocked {
                        task_run_turn,
                        error,
                    } if task_run_turn.id == revision.id => Some((event.sequence, error)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let blocked_runs = history
                .iter()
                .filter_map(|event| match &event.payload {
                    TaskEventPayload::RunBlocked { run_id, error, .. } if run_id == &run.id => {
                        Some((event.sequence, error))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(blocked_turns.len(), 1);
            assert_eq!(blocked_runs.len(), 1);
            assert_eq!(blocked_turns[0].1.as_ref(), Some(&error));
            assert_eq!(blocked_runs[0].1.as_ref(), Some(&error));
            assert!(blocked_turns[0].0 < blocked_runs[0].0);
            assert_eq!(
                harness
                    .crud_store
                    .get_turn(&revised.child_thread_id, &revised.child_turn_id)
                    .await
                    .unwrap(),
                blocked_turn
            );
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                activations
            );
            assert_eq!(
                processor
                    .task_cli_preparation_attempts
                    .load(Ordering::SeqCst),
                preparations
            );
        });
        // Persistence is returned to the caller, never reported as a new launch
        // failure; explicit redispatch preserves the known business outcome too.
        assert!(events.is_empty(), "diagnostic_read={diagnostic_read}");
    }
}

#[test]
fn task_child_consumer_rotates_past_more_than_a_page_of_undeferable_claim_failures() {
    run_standard_stack_message_test("child poison claim rotation", async {
        use pioneer_entity::task_child_reconcile_pending as point;
        use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};
        let (workspaces, store, _) = setup_workspace_manager().await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            workspaces,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        let tx = store.database_connection().begin().await.unwrap();
        for n in 0..40 {
            {
                let id: String = (&format!("poison_{n:03}")).to_string();
                point::Entity::insert(point::ActiveModel {
                    turn_id: sea_orm::Set(id.clone()),
                    thread_id: sea_orm::Set("missing-thread".into()),
                    revision: sea_orm::Set(format!("fixture-{id}")),
                    next_attempt_at: sea_orm::Set(Some(1)),
                    attempt_count: sea_orm::Set(0),
                    claim_token: sea_orm::Set(None),
                })
                .exec(&tx)
                .await
                .unwrap();
            }
        }
        {
            let id: String = ("z_healthy_tail").to_string();
            point::Entity::insert(point::ActiveModel {
                turn_id: sea_orm::Set(id.clone()),
                thread_id: sea_orm::Set("missing-thread".into()),
                revision: sea_orm::Set(format!("fixture-{id}")),
                next_attempt_at: sea_orm::Set(Some(1)),
                attempt_count: sea_orm::Set(0),
                claim_token: sea_orm::Set(None),
            })
            .exec(&tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        // Fail BOTH claim and independent deferral, so only the real consumer's
        // retained cursor can expose the healthy tail beyond its 16-row page.
        store.database_connection().execute_unprepared("CREATE TRIGGER test_child_claim_poison BEFORE UPDATE ON task_child_reconcile_pending WHEN OLD.turn_id GLOB 'poison_*' BEGIN SELECT RAISE(FAIL,'point write refused'); END").await.unwrap();
        for _ in 0..3 {
            assert!(
                processor
                    .for_background_reconciliation()
                    .reconcile_terminal_task_child_turns(16)
                    .await
                    .is_err()
            );
        }
        assert!(
            point::Entity::find_by_id("z_healthy_tail")
                .one(&store.database_connection())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            point::Entity::find_by_id("poison_000")
                .one(&store.database_connection())
                .await
                .unwrap()
                .unwrap()
                .claim_token
                .is_none()
        );
    });
}

#[test]
fn task_child_consumer_ack_and_deferral_failure_does_not_lease_or_suppress_healthy_tail() {
    run_standard_stack_message_test("child poison ACK isolation", async {
        use pioneer_entity::task_child_reconcile_pending as point;
        use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};
        let (workspaces, store, _) = setup_workspace_manager().await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            workspaces,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        let tx = store.database_connection().begin().await.unwrap();
        for id in ["a_ack_poison", "z_after_ack"] {
            {
                let id: String = (id).to_string();
                point::Entity::insert(point::ActiveModel {
                    turn_id: sea_orm::Set(id.clone()),
                    thread_id: sea_orm::Set("missing-thread".into()),
                    revision: sea_orm::Set(format!("fixture-{id}")),
                    next_attempt_at: sea_orm::Set(Some(1)),
                    attempt_count: sea_orm::Set(0),
                    claim_token: sea_orm::Set(None),
                })
                .exec(&tx)
                .await
                .unwrap();
            }
        }
        tx.commit().await.unwrap();
        store.database_connection().execute_unprepared("CREATE TRIGGER test_child_ack_poison BEFORE DELETE ON task_child_reconcile_pending WHEN OLD.turn_id='a_ack_poison' BEGIN SELECT RAISE(FAIL,'ACK refused'); END").await.unwrap();
        store.database_connection().execute_unprepared("CREATE TRIGGER test_child_defer_poison BEFORE UPDATE ON task_child_reconcile_pending WHEN OLD.turn_id='a_ack_poison' AND OLD.claim_token IS NOT NULL AND NEW.claim_token IS NULL BEGIN SELECT RAISE(FAIL,'deferral refused'); END").await.unwrap();
        assert!(
            processor
                .reconcile_terminal_task_child_turns(16)
                .await
                .is_err()
        );
        assert!(
            point::Entity::find_by_id("z_after_ack")
                .one(&store.database_connection())
                .await
                .unwrap()
                .is_none(),
            "ACK failure must not leave a later healthy row claimed for 60 seconds"
        );
        let poisoned = point::Entity::find_by_id("a_ack_poison")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert!(poisoned.claim_token.is_some());
        assert!(poisoned.next_attempt_at.unwrap() > chrono::Utc::now().timestamp());
    });
}

#[test]
fn task_child_scope_rotation_survives_undeferable_heads_and_wraps() {
    run_standard_stack_message_test("child scope poison rotation", async {
        use pioneer_entity::{
            task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
        };
        use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};
        let (workspaces, store, _) = setup_workspace_manager().await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            workspaces,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        // A real Run fanout locator with a persisted link. Its stale/missing
        // Turn is deliberately ACKed by the production point consumer.
        store
            .upsert_task_run_turn(pioneer_protocol::TaskRunTurn {
                id: "scope-fanout-link".into(),
                task_id: "scope-task".into(),
                run_id: "healthy-run".into(),
                execution_id: None,
                thread_id: "missing-thread".into(),
                turn_id: "scope_healthy_child".into(),
                kind: TaskRunTurnKind::Initial,
                round: 0,
                sequence: 0,
                status: TaskRunTurnStatus::CandidateCreated,
                reviews_candidate_id: None,
                requested_by_candidate_id: None,
                requested_by_review_event_id: None,
                created_at: 1,
                started_at: None,
                completed_at: Some(1),
            })
            .await
            .unwrap();
        point::Entity::delete_many()
            .exec(&store.database_connection())
            .await
            .unwrap();
        scope::Entity::delete_many()
            .filter(scope::Column::ScopeKind.ne("historical-seed"))
            .exec(&store.database_connection())
            .await
            .unwrap();
        while store
            .expand_child_scope(chrono::Utc::now().timestamp(), true)
            .await
            .unwrap()
        {}
        assert!(store.child_work_queues_ready().await.unwrap());
        point::Entity::delete_many()
            .exec(&store.database_connection())
            .await
            .unwrap();
        for n in 0..41 {
            let (kind, key, due) = if n == 40 {
                ("run", "healthy-run".to_owned(), 2)
            } else {
                ("task", format!("poison_{n:03}"), 1)
            };
            scope::Entity::insert(scope::ActiveModel {
                scope_kind: Set(kind.into()),
                scope_key: Set(key.clone()),
                incarnation_token: Set(format!("inc-{key}")),
                requested_revision: Set(format!("rev-{key}")),
                pass_revision: Set(None),
                cursor: Set(None),
                upper_watermark: Set(None),
                next_attempt_at: Set(Some(due)),
                attempt_count: Set(0),
                claim_token: Set(None),
            })
            .exec_without_returning(&store.database_connection())
            .await
            .unwrap();
        }
        // Observe actual point ACK, so merely failing to publish a point cannot
        // satisfy the assertion. Faults remain row-specific on scope UPDATE.
        store
            .database_connection()
            .execute_unprepared("CREATE TABLE test_scope_acks (turn_id TEXT PRIMARY KEY)")
            .await
            .unwrap();
        store.database_connection().execute_unprepared("CREATE TRIGGER test_scope_ack AFTER DELETE ON task_child_reconcile_pending WHEN OLD.turn_id='scope_healthy_child' BEGIN INSERT INTO test_scope_acks VALUES (OLD.turn_id); END").await.unwrap();
        store.database_connection().execute_unprepared("CREATE TRIGGER test_scope_refusal BEFORE UPDATE ON task_child_reconcile_scope WHEN OLD.scope_key GLOB 'poison_*' BEGIN SELECT RAISE(FAIL,'scope write refused'); END").await.unwrap();
        for _ in 0..40 {
            let error = processor
                .for_background_reconciliation()
                .reconcile_terminal_task_child_turns(16)
                .await
                .unwrap_err();
            assert!(format!("{error:?}").contains("deferral also failed"));
        }
        processor
            .for_background_reconciliation()
            .reconcile_terminal_task_child_turns(16)
            .await
            .unwrap();
        let ack = store
            .database_connection()
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT turn_id FROM test_scope_acks".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ack.try_get::<String>("", "turn_id").unwrap(),
            "scope_healthy_child"
        );
        // The next quantum wraps to the still-due prefix; failure remains
        // visible and no durable poison obligation was dropped/quarantined.
        assert!(
            processor
                .for_background_reconciliation()
                .reconcile_terminal_task_child_turns(16)
                .await
                .is_err()
        );
        store
            .database_connection()
            .execute_unprepared("DROP TRIGGER test_scope_refusal")
            .await
            .unwrap();
        for _ in 0..41 {
            processor
                .for_background_reconciliation()
                .reconcile_terminal_task_child_turns(16)
                .await
                .unwrap();
        }
        assert!(
            scope::Entity::find()
                .filter(scope::Column::ScopeKind.ne("historical-seed"))
                .all(&store.database_connection())
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn child_due_passes_revisit_behind_cursor_work_despite_continuous_suffix_growth() {
    run_standard_stack_message_test("finite child due passes", async {
        use pioneer_entity::{
            task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
        };
        use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};
        let (workspaces, store, _) = setup_workspace_manager().await;
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            test_provider(),
            Arc::new(SessionManager::new()),
            workspaces,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        store
            .upsert_task_run_turn(pioneer_protocol::TaskRunTurn {
                id: "finite-link".into(),
                task_id: "finite-task".into(),
                run_id: "finite-run".into(),
                execution_id: None,
                thread_id: "missing-thread".into(),
                turn_id: "finite-fanout".into(),
                kind: TaskRunTurnKind::Initial,
                round: 0,
                sequence: 0,
                status: TaskRunTurnStatus::CandidateCreated,
                reviews_candidate_id: None,
                requested_by_candidate_id: None,
                requested_by_review_event_id: None,
                created_at: 1,
                started_at: None,
                completed_at: Some(1),
            })
            .await
            .unwrap();
        while store
            .expand_child_scope(chrono::Utc::now().timestamp(), true)
            .await
            .unwrap()
        {}
        assert!(store.child_work_queues_ready().await.unwrap());
        let db = store.database_connection();
        point::Entity::delete_many().exec(&db).await.unwrap();
        scope::Entity::delete_many()
            .filter(scope::Column::ScopeKind.ne("historical-seed"))
            .exec(&db)
            .await
            .unwrap();
        async fn add_point(db: &impl ConnectionTrait, id: &str, due: i64) {
            point::Entity::insert(point::ActiveModel {
                turn_id: Set(id.into()),
                thread_id: Set("missing-thread".into()),
                revision: Set(format!("rev-{id}")),
                next_attempt_at: Set(Some(due)),
                attempt_count: Set(0),
                claim_token: Set(None),
            })
            .exec_without_returning(db)
            .await
            .unwrap();
        }
        async fn add_scope(db: &impl ConnectionTrait, kind: &str, key: &str, due: i64) {
            scope::Entity::insert(scope::ActiveModel {
                scope_kind: Set(kind.into()),
                scope_key: Set(key.into()),
                incarnation_token: Set(format!("inc-{key}")),
                requested_revision: Set(format!("rev-{key}")),
                pass_revision: Set(None),
                cursor: Set(None),
                upper_watermark: Set(None),
                next_attempt_at: Set(Some(due)),
                attempt_count: Set(0),
                claim_token: Set(None),
            })
            .exec_without_returning(db)
            .await
            .unwrap();
        }
        for n in 0..32 {
            add_point(&db, &format!("poison_point_{n:03}"), 1).await;
        }
        for n in 0..2 {
            add_scope(&db, "task", &format!("poison_scope_{n:03}"), 1).await;
        }
        db.execute_unprepared("CREATE TABLE test_finite_acks (turn_id TEXT PRIMARY KEY)")
            .await
            .unwrap();
        db.execute_unprepared("CREATE TRIGGER test_finite_ack AFTER DELETE ON task_child_reconcile_pending WHEN OLD.turn_id='behind-point' BEGIN INSERT INTO test_finite_acks VALUES (OLD.turn_id); END").await.unwrap();
        db.execute_unprepared("CREATE TRIGGER test_finite_point_refusal BEFORE UPDATE ON task_child_reconcile_pending WHEN OLD.turn_id GLOB 'poison_*' BEGIN SELECT RAISE(FAIL,'point refused'); END").await.unwrap();
        db.execute_unprepared("CREATE TRIGGER test_finite_scope_refusal BEFORE UPDATE ON task_child_reconcile_scope WHEN OLD.scope_key GLOB 'poison_*' BEGIN SELECT RAISE(FAIL,'scope refused'); END").await.unwrap();
        assert!(
            processor
                .for_background_reconciliation()
                .reconcile_terminal_task_child_turns(16)
                .await
                .is_err()
        );
        // Both locators arrive behind their retained raw cursors, with no hint,
        // rebuild or compatibility scan to rescue them after durable seed EOF.
        add_point(&db, "behind-point", 0).await;
        add_scope(&db, "run", "finite-run", 0).await;
        for quantum in 0..2 {
            // Supply more than a full new suffix before EVERY quantum. A cursor
            // that waits for a short suffix would never revisit either locator.
            for n in 0..32 {
                add_point(&db, &format!("zz_suffix_{quantum}_{n:03}"), 2 + quantum).await;
            }
            add_scope(&db, "task", &format!("zz_suffix_{quantum}"), 2 + quantum).await;
            assert!(
                processor
                    .for_background_reconciliation()
                    .reconcile_terminal_task_child_turns(16)
                    .await
                    .is_err()
            );
        }
        let ack = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Sqlite,
                "SELECT turn_id FROM test_finite_acks".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ack.try_get::<String>("", "turn_id").unwrap(),
            "behind-point"
        );
        assert!(
            scope::Entity::find_by_id(("run".to_owned(), "finite-run".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            point::Entity::find_by_id("finite-fanout")
                .one(&db)
                .await
                .unwrap()
                .is_some(),
            "real run fanout must publish a point, not merely advance a cursor"
        );
        assert!(
            point::Entity::find_by_id("poison_point_000")
                .one(&db)
                .await
                .unwrap()
                .is_some(),
            "failed work remains durable for subsequent passes"
        );
    });
}

#[test]
fn gateway_partial_task_startup_keeps_independent_registrations_and_unknown_failure_stops() {
    run_standard_stack_message_test("partial task startup integration", async {
        use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
        for partial in [true, false] {
            let (workspaces, store, workspace) = setup_workspace_manager().await;
            let processor = Arc::new(MessageProcessor::new(
                Arc::new(ThreadManager::new("test-model", "openai")),
                test_provider(),
                Arc::new(SessionManager::new()),
                workspaces,
                store.clone(),
                test_gateway_secrets(),
                test_summary_config(),
                test_tool_loop_config(),
            ));
            let mut params =
                test_task_create_params(&workspace, "unused", "unused", "startup fixture", 1);
            params.owner_kind = TaskOwnerKind::Workspace;
            params.owner_id = Some(workspace);
            params.created_by_thread_id = None;
            params.created_by_turn_id = None;
            params.executor_kind = TaskExecutorKind::System;
            params.agent_spec = None;
            params.lifecycle_policy = None;
            params.trigger.spec = TaskTriggerSpec::Manual {
                allowed_actor: None,
            };
            let task = processor
                .task_runtime
                .service()
                .create_task(pioneer_tasks::TaskCreateContext::default(), params)
                .await
                .unwrap();
            let db = store.database_connection();
            if partial {
                pioneer_entity::task_trigger::Entity::update_many()
                    .filter(pioneer_entity::task_trigger::Column::Id.eq(task.trigger.id))
                    .col_expr(
                        pioneer_entity::task_trigger::Column::SpecJson,
                        sea_orm::sea_query::Expr::value("invalid JSON"),
                    )
                    .exec(&db)
                    .await
                    .unwrap();
                let error = processor
                    .task_runtime
                    .start()
                    .await
                    .unwrap_err()
                    .context("caller context");
                assert!(
                    error
                        .downcast_ref::<pioneer_tasks::PartialTaskStartup>()
                        .is_some()
                );
                processor.initialize_resilience_workers().await.unwrap();
                assert!(
                    processor
                        .task_runtime
                        .start()
                        .await
                        .unwrap_err()
                        .downcast_ref::<pioneer_tasks::PartialTaskStartup>()
                        .is_some(),
                    "persistent poison was not repaired or hidden by independent Gateway startup"
                );
                assert!(processor.task_event_listener_worker.lock().await.is_some());
                assert!(processor.hook_recovery_worker.lock().await.is_some());
            } else {
                db.execute_unprepared("CREATE TRIGGER test_startup_storage_refusal BEFORE INSERT ON task_event BEGIN SELECT RAISE(FAIL,'startup storage unavailable'); END").await.unwrap();
                let error = processor.initialize_resilience_workers().await.unwrap_err();
                assert!(
                    format!("{error:#}").contains("failed to start task runtime"),
                    "negative control must reach the runtime startup boundary"
                );
                assert!(
                    error
                        .downcast_ref::<pioneer_tasks::PartialTaskStartup>()
                        .is_none()
                );
                assert!(processor.task_event_listener_worker.lock().await.is_none());
                assert!(processor.hook_recovery_worker.lock().await.is_none());
            }
            processor.shutdown_resilience_workers().await;
        }
    });
}

#[tokio::test]
async fn capsule_delegation_survives_nested_task_binding_recovery_and_revocation() {
    use super::thread_launch_choices::{call, invoke_with_mutation};
    use crate::message::task_agent_executor::materialize_task_agent_action_binding_for_execution;
    use pioneer_agent::{TaskToolProvider, TurnToolContext};

    // Existing recording CLI session acknowledges activation without starting
    // any external process/model. Task admission, lineage, grants and handlers
    // remain the production paths.
    let (harness, processor) = review_cli_fixture().await;
    let (creator_task, creator) =
        create_review_cli_child(&harness, &processor, "capsule-creator", false, false).await;
    let creator_context = TurnToolContext {
        workspace_id: harness.workspace_id.clone(),
        thread_id: creator.child_thread_id.clone(),
        turn_id: creator.child_turn_id.clone(),
    };
    let creator_binding = processor
        .agent_action_binding(&creator.child_turn_id)
        .await
        .unwrap();
    let creator_facts = creator_binding.adapter.lock().await.persistence_facts();
    let creation = creator_binding
        .options
        .thread_creation_options
        .iter()
        .find(|option| option.label == "New delegated root")
        .unwrap();
    let creator_tools = processor
        .materialize_agent_action_tools(creator_context.clone())
        .await
        .unwrap();
    let created = call(
        &creator_tools,
        "thread_create",
        "nested-capsule-destination",
        json!({"optionId":creation.id}),
    )
    .await;
    let destination = created["threadResult"]["threadId"]
        .as_str()
        .unwrap()
        .to_owned();

    // A/B/C have identical saved names. A later execution must select A by
    // threadId, without knowing route order or retaining the creator's option.
    let creator_targets: Vec<pioneer_protocol::AgentToolTargetOption> =
        serde_json::from_value(created["targetOptions"].clone()).unwrap();
    let creator_target = creator_targets
        .iter()
        .find(|o| o.thread_id == destination)
        .unwrap();
    let mut destinations = vec![destination.clone()];
    for suffix in ["b", "c"] {
        let other = call(
            &creator_tools,
            "thread_create",
            &format!("same-name-{suffix}"),
            json!({"optionId":creation.id}),
        )
        .await;
        destinations.push(
            other["threadResult"]["threadId"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    for id in &destinations {
        use sea_orm::{ActiveModelTrait, EntityTrait, Set};
        let model = pioneer_entity::thread::Entity::find_by_id(id.clone())
            .one(&processor.crud_store.database_connection())
            .await
            .unwrap()
            .unwrap();
        let mut row: pioneer_entity::thread::ActiveModel = model.into();
        row.name = Set(Some("Same destination".to_owned()));
        row.update(&processor.crud_store.database_connection())
            .await
            .unwrap();
    }

    // Create the second-level Task through the actual agent task handler, not
    // by assigning home roots, copying route rows, or fabricating admissions.
    harness
        .cli_session
        .set_next_native_turn_id("native-capsule-descendant")
        .await;
    let task_tools = crate::task_tools::GatewayTaskToolProvider::new(Arc::downgrade(&processor))
        .materialize_task_tools(pioneer_agent::TaskTurnContext {
            workspace_id: harness.workspace_id.clone(),
            thread_id: creator.child_thread_id.clone(),
            turn_id: creator.child_turn_id.clone(),
        })
        .await
        .unwrap();
    let task_tools = pioneer_agent::TurnToolMaterialization {
        bundles: task_tools.bundles,
        ..Default::default()
    };
    let nested = call(
        &task_tools,
        "task_create",
        "nested-capsule-task",
        json!({"title":"Nested capsule recovery", "goal":"Wait for controlled recovery"}),
    )
    .await;
    let task_id = nested["taskId"].as_str().unwrap();
    let run_id = wait_for_task_run_id(harness.crud_store.clone(), task_id).await;
    let descendant = wait_for_child_lineage_for_run(harness.crud_store.clone(), &run_id).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let activated = harness
                .crud_store
                .get_cli_runtime_turn_binding(&descendant.child_turn_id)
                .await
                .unwrap()
                .is_some_and(|binding| {
                    binding.native_turn_id.as_deref() == Some("native-capsule-descendant")
                });
            let running = harness
                .crud_store
                .get_task_run(&run_id)
                .await
                .unwrap()
                .unwrap()
                .status
                == TaskRunStatus::Running;
            if activated
                && running
                && processor
                    .agent_action_binding(&descendant.child_turn_id)
                    .await
                    .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let response = harness.crud_store.get_task(task_id).await.unwrap().unwrap();
    let admission = harness
        .crud_store
        .get_task_execution_admission(task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.root_thread_id, creator_facts.home_root_thread_id);
    assert!(response.agent_specs[0].depth > 1);
    let lineage = harness
        .crud_store
        .get_task_thread_lineage(&descendant.child_thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lineage.root_thread_id, admission.root_thread_id);
    assert_eq!(lineage.parent_thread_id, creator.child_thread_id);
    let execution = harness
        .crud_store
        .load_execution_for_run(&run_id)
        .await
        .unwrap()
        .unwrap();
    let db = harness.crud_store.database_connection();
    let route = pioneer_crud::list_agent_delegation_routes_for_source(
        &db,
        creator_facts.execution_id.as_str(),
        creator_facts.identity.id.as_str(),
    )
    .await
    .unwrap()
    .into_iter()
    .find(|route| route.destination_thread_id == destination)
    .unwrap();
    assert_eq!(route.route_kind, "capsule_bound");
    harness
        .crud_store
        .finalize_agent_execution(
            creator_facts.execution_id.as_str(),
            "completed",
            pioneer_crud::utc_now(),
        )
        .await
        .unwrap();
    let completed_creator =
        pioneer_crud::load_agent_execution(&db, creator_facts.execution_id.as_str())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(completed_creator.status, "completed");
    assert!(completed_creator.finished_at.is_some());
    processor
        .unregister_agent_action_binding(&creator.child_turn_id)
        .await;
    drop(creator_binding);
    drop(creator_tools);
    drop(task_tools);

    let context = TurnToolContext {
        workspace_id: harness.workspace_id.clone(),
        thread_id: descendant.child_thread_id.clone(),
        turn_id: descendant.child_turn_id.clone(),
    };
    let mut latest_tools = None;
    let mut latest_target = None;
    for launch in 0..2 {
        // Two cold launches of the admitted execution exercise the persisted
        // branch of task restoration. No bind_persisted_route is used here.
        drop(latest_tools.take());
        processor
            .unregister_agent_action_binding(&context.turn_id)
            .await;
        let (adapter, options, capabilities) = materialize_task_agent_action_binding_for_execution(
            &processor,
            &response.task,
            &response.agent_specs[0],
            &execution.id,
            &run_id,
            &admission.root_thread_id,
            processor.current_authorization_revision().await.unwrap(),
            false,
        )
        .await
        .unwrap();
        let recovered = adapter.persistence_facts();
        assert_ne!(recovered.execution_id, creator_facts.execution_id);
        assert_eq!(recovered.home_root_thread_id, admission.root_thread_id);
        let binding = crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
            adapter,
            options,
            capabilities,
        );
        let binding = processor
            .prepare_agent_action_binding(context.turn_id.clone(), binding)
            .await
            .unwrap();
        processor
            .register_agent_action_binding(context.turn_id.clone(), binding)
            .await;
        let tools = processor
            .materialize_agent_action_tools(context.clone())
            .await
            .unwrap();
        let catalog = call(
            &tools,
            "threads_start_options",
            &format!("capsule-options-{launch}"),
            json!({}),
        )
        .await;
        let targets: Vec<pioneer_protocol::AgentToolTargetOption> =
            serde_json::from_value(catalog["targetOptions"].clone()).unwrap();
        assert!(
            targets
                .iter()
                .any(|o| o.label == "Current thread" && o.thread_id == context.thread_id)
        );
        assert!(
            targets
                .iter()
                .any(|o| o.label == "Current collaboration"
                    && o.thread_id == admission.root_thread_id)
        );
        let target = targets
            .iter()
            .find(|option| {
                option.thread_id == destination
                    && option
                        .allowed_actions
                        .contains(&pioneer_protocol::AgentRouteAction::SendMessage)
            })
            .unwrap();
        assert!(
            target
                .allowed_actions
                .contains(&pioneer_protocol::AgentRouteAction::StartAgent)
        );
        assert_ne!(target.id, creator_target.id);
        assert_eq!(target.title.as_deref(), Some("Same destination"));
        for id in &destinations {
            assert!(
                targets
                    .iter()
                    .any(|o| &o.thread_id == id && o.title.as_deref() == Some("Same destination"))
            );
        }
        if launch == 0 {
            let stale = invoke_with_mutation(&tools, "thread_message_send", "old-execution-target",
                json!({"targetOptionId":creator_target.id,"input":[{"type":"text","text":"stale"}]}), None).await;
            assert!(matches!(
                stale,
                Err(pioneer_tools::ToolError::InvalidArguments(_))
            ));
        }
        let call_id = format!("capsule-restored-send-{launch}");
        let result = call(&tools, "thread_message_send", &call_id,
            json!({"targetOptionId":target.id, "input":[{"type":"text","text":format!("Recovered launch {launch}")}]})).await;
        assert_eq!(result["threadResult"]["threadId"], destination);
        let refreshed: Vec<pioneer_protocol::AgentToolTargetOption> =
            serde_json::from_value(result["targetOptions"].clone()).unwrap();
        assert!(
            refreshed
                .iter()
                .any(|o| o.thread_id == destination && o.id == target.id)
        );
        let outbox = pioneer_crud::load_agent_action_outbox(
            &db,
            crate::authorization::action_id_for_call(&call_id).as_str(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(outbox.owner_execution_id, recovered.execution_id.as_str());
        let receipt = pioneer_crud::load_agent_action_receipt(
            &db,
            crate::authorization::action_id_for_call(&call_id).as_str(),
        )
        .await
        .unwrap();
        let receipt = receipt.expect("canonical commit must retain its receipt");
        assert_eq!(
            receipt.actor_id.as_deref(),
            Some(recovered.execution_id.as_str())
        );
        assert_eq!(
            receipt.source_scope_id.as_deref(),
            Some(admission.root_thread_id.as_str())
        );
        assert_eq!(
            receipt.destination_scope_id.as_deref(),
            Some(destination.as_str())
        );
        let route_receipt: serde_json::Value =
            serde_json::from_str(receipt.route_receipt_json.as_deref().unwrap()).unwrap();
        assert_eq!(route_receipt["routeId"], route.id);
        latest_target = Some(target.id.clone());
        latest_tools = Some(tools);
    }
    let started = tokio::time::timeout(
        Duration::from_secs(5),
        call(
            latest_tools.as_ref().unwrap(),
            "threads_turn_start",
            "addressed-start-a",
            json!({"targetOptionId":latest_target.as_ref().unwrap(),
        "input":[{"type":"text","text":"Start in A"}]}),
        ),
    )
    .await
    .expect(
        "committed detached start must return refreshed targets without holding its admission lock",
    );
    assert_eq!(started["threadResult"]["threadId"], destination);
    assert!(
        started["targetOptions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["threadId"] == destination)
    );
    let principal = serde_json::to_string(&pioneer_protocol::PersistedActorRef::Principal(
        authenticated_test_superuser().principal_id.clone(),
    ))
    .unwrap();
    assert!(
        harness
            .crud_store
            .revoke_agent_delegation_route(
                &route.id,
                route.route_generation,
                processor.current_authorization_revision().await.unwrap() as i64,
                &principal,
                "capsule-review-revocation",
                pioneer_crud::utc_now()
            )
            .await
            .unwrap()
    );
    let stale_target = latest_target.unwrap();
    let denied = invoke_with_mutation(
        latest_tools.as_ref().unwrap(),
        "thread_message_send",
        "capsule-revoked-send",
        json!({"targetOptionId":stale_target, "input":[{"type":"text","text":"must not commit"}]}),
        None,
    )
    .await;
    assert!(
        matches!(&denied, Err(pioneer_tools::ToolError::InvalidArguments(message))
        if message == pioneer_protocol::AgentPublicOutcome::AgentActionNotAllowed.as_str()),
        "revoked route must fail opaque target resolution: {denied:?}"
    );
    assert!(
        pioneer_crud::load_agent_action_outbox(
            &db,
            crate::authorization::action_id_for_call("capsule-revoked-send").as_str()
        )
        .await
        .unwrap()
        .is_none()
    );
    let refreshed = processor
        .agent_action_binding(&context.turn_id)
        .await
        .unwrap();
    assert!(
        refreshed
            .options
            .target_options
            .iter()
            .all(|option| option.id != stale_target),
        "the handler must reload revocation before resolving the saved opaque target"
    );
    assert!(
        pioneer_crud::load_agent_action_receipt(
            &db,
            crate::authorization::action_id_for_call("capsule-revoked-send").as_str()
        )
        .await
        .unwrap()
        .is_none()
    );
    // The original Task and nested Task are kept live only by recording sessions;
    // no provider output or fabricated task completion is used by the oracle.
    assert_ne!(creator_task.task.id, task_id);
}
