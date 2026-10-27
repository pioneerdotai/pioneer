use super::*;
use pioneer_crud::TaskDeliveryTransitionOutcome;
use pioneer_protocol::{TaskFinishParams, TaskGetResponse};

const FIRE_AT: i64 = 4_000_000_000;

async fn read(runtime: &TaskRuntime, task_id: &str) -> TaskGetResponse {
    runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: task_id.to_owned(),
        })
        .await
        .unwrap()
}

async fn finish(runtime: &TaskRuntime, task_id: &str) -> pioneer_protocol::TaskFinishResponse {
    runtime
        .service()
        .finish_task(
            TaskMutationContext::user(TEST_PRINCIPAL_ID),
            TaskFinishParams {
                task_id: task_id.to_owned(),
                reason: Some("User asked to stop after the admitted work".to_owned()),
            },
        )
        .await
        .unwrap()
}

fn interval_params() -> TaskCreateParams {
    let mut params = create_params(TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(FIRE_AT),
        catch_up_policy: Some(TaskTriggerCatchUpPolicy::run_all_missed(4)),
    });
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_finish_owner".to_owned());
    params.created_by_thread_id = params.owner_id.clone();
    params.delivery_policy = Some(TaskDeliveryPolicy {
        mode: TaskDeliveryMode::Thread,
        thread_target: Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread),
        thread_id: params.owner_id.clone(),
        webhook_url: None,
        include_result: true,
        format: pioneer_protocol::TaskDeliveryFormat::FullResult,
    });
    params
}

async fn admit(runtime: &TaskRuntime, run: &TaskRun) -> TaskExecutionHandle {
    let store = runtime.service().store();
    store
        .claim_task_run_for_dispatch(&run.id, run.created_at)
        .await
        .unwrap()
        .expect("fixture run must cross the durable dispatch boundary");
    store
        .claim_task_run_execution_for_dispatch(
            &run.id,
            run.executor_kind,
            "finish-test",
            run.created_at,
            run.created_at + 60,
        )
        .await
        .unwrap()
        .expect("fixture execution must acquire its durable fence");
    let handle = TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    handle.mark_started(run.created_at).await.unwrap();
    handle
}

async fn deliveries(runtime: &TaskRuntime, task_id: &str) -> Vec<pioneer_protocol::TaskDelivery> {
    runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            task_id: Some(task_id.to_owned()),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(20),
        })
        .await
        .unwrap()
        .deliveries
}

fn result(run: &TaskRun) -> TaskResult {
    TaskResult {
        summary: Some(format!("Real result for run {}", run.run_number)),
        data: Some(TaskValue::Integer(run.run_number)),
        artifacts: Vec::new(),
        completed_by_run_id: Some(run.id.clone()),
    }
}

#[tokio::test]
async fn finish_drains_all_admitted_runs_fences_queue_and_preserves_delivery_after_restart() {
    let original = runtime().await;
    let mut params = interval_params();
    params.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 3,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let created = original
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap();
    original.process_due_once(FIRE_AT + 25).await.unwrap();
    let mut before = read(&original, &created.task.id).await;
    before.runs.sort_by_key(|run| run.run_number);
    assert_eq!(before.runs.len(), 3);
    let first = admit(&original, &before.runs[0]).await;
    let _second = admit(&original, &before.runs[1]).await;
    let queued = &before.runs[2];

    // Stop at the real durable acceptance boundary, before the service's
    // bounded queue disposal, to model Gateway restart in that exact window.
    let (already_finished, _) = original
        .service()
        .store()
        .request_task_finish(
            &created.task.id,
            Some("User asked to stop after the admitted work".to_owned()),
            FIRE_AT + 26,
            None,
        )
        .await
        .unwrap();
    assert!(!already_finished);
    let accepted = read(&original, &created.task.id).await;
    assert_eq!(accepted.task.status, TaskStatus::Running);
    let stop = accepted.task.finish_request().unwrap().clone();
    assert_eq!(
        stop.reason.as_deref(),
        Some("User asked to stop after the admitted work")
    );
    let draining = read(&original, &created.task.id).await;
    assert!(
        draining
            .triggers
            .iter()
            .all(|trigger| trigger.status == TaskTriggerStatus::Cancelled)
    );
    assert_eq!(
        draining
            .runs
            .iter()
            .filter(|run| run.status == TaskRunStatus::Running)
            .count(),
        2
    );
    assert_eq!(
        draining
            .runs
            .iter()
            .find(|run| run.id == queued.id)
            .unwrap()
            .status,
        TaskRunStatus::Queued,
        "acceptance must fence dispatch even before queue disposal has run"
    );
    assert!(
        original
            .service()
            .store()
            .claim_task_run_for_dispatch(&queued.id, FIRE_AT + 30)
            .await
            .unwrap()
            .is_none(),
        "a dispatcher holding the old queued run cannot admit it after finish"
    );

    let restarted = TaskRuntime::new(original.service().store());
    assert_eq!(restarted.process_due_once(FIRE_AT + 100).await.unwrap(), 0);
    let recovered = read(&restarted, &created.task.id).await;
    assert_ne!(
        recovered
            .runs
            .iter()
            .find(|run| run.id == queued.id)
            .unwrap()
            .status,
        TaskRunStatus::Queued,
        "restart must finish disposing the fenced queue"
    );
    assert_eq!(
        read(&restarted, &created.task.id)
            .await
            .task
            .finish_request(),
        Some(&stop)
    );
    assert!(!finish(&restarted, &created.task.id).await.already_finished);
    // Both terminal decisions can be prepared against the two-active snapshot.
    // The later commit must notice the sibling already finished and close the task.
    let second_terminal = original
        .service()
        .store()
        .prepare_task_terminal_transition(TaskEventPayload::RunCompleted {
            task_id: created.task.id.clone(),
            run_id: before.runs[1].id.clone(),
            result: Some(result(&before.runs[1])),
            completed_at: FIRE_AT + 102,
        })
        .await
        .unwrap();
    first
        .complete_run(Some(result(&before.runs[0])), FIRE_AT + 101)
        .await
        .unwrap();
    assert_eq!(
        read(&restarted, &created.task.id).await.task.status,
        TaskStatus::Running,
        "one parallel result must not terminate the other admitted run"
    );
    original
        .service()
        .store()
        .commit_task_terminal_transition(second_terminal)
        .await
        .unwrap();
    let done = read(&restarted, &created.task.id).await;
    assert_eq!(done.task.status, TaskStatus::Completed);
    assert_eq!(done.runs.len(), 3);
    for run in &before.runs[..2] {
        let saved = done.runs.iter().find(|saved| saved.id == run.id).unwrap();
        assert_eq!(saved.status, TaskRunStatus::Succeeded);
        assert_eq!(saved.result, Some(result(run)));
    }
    let pending = deliveries(&restarted, &created.task.id).await;
    assert_eq!(pending.len(), 2);
    assert!(
        pending
            .iter()
            .all(|delivery| delivery.status == TaskDeliveryStatus::Pending)
    );
    assert!(finish(&restarted, &created.task.id).await.already_finished);
    assert_eq!(
        deliveries(&restarted, &created.task.id).await,
        pending,
        "repeating finish cannot enqueue or republish a result"
    );

    let delivery = &pending[0];
    let TaskDeliveryTransitionOutcome::Applied((delivery, attempt)) = restarted
        .background_control_service()
        .start_delivery(&delivery.id, FIRE_AT + 103)
        .await
        .unwrap()
    else {
        panic!("finish must preserve the delivery's authority")
    };
    restarted
        .background_control_service()
        .fail_delivery(
            delivery,
            attempt,
            "transport_failure".to_owned(),
            None,
            None,
            FIRE_AT + 104,
        )
        .await
        .unwrap();
    let after_delivery_error = read(&restarted, &created.task.id).await;
    assert_eq!(after_delivery_error.task.status, TaskStatus::Completed);
    assert_eq!(after_delivery_error.task.finish_request(), Some(&stop));
    assert_eq!(restarted.process_due_once(FIRE_AT + 1000).await.unwrap(), 0);
    assert_eq!(read(&restarted, &created.task.id).await.runs.len(), 3);
    assert!(
        deliveries(&restarted, &created.task.id)
            .await
            .iter()
            .any(|delivery| delivery.last_error.is_some())
    );
}

#[tokio::test]
async fn finish_idle_creates_no_result_and_cannot_be_undone_by_resume_update_or_reschedule() {
    let original = runtime().await;
    let params = interval_params();
    let created = original
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap();
    // A model cannot inject or clear the server-owned stop decision through
    // general metadata writes, including before any genuine finish request.
    let forged_metadata = pioneer_protocol::TaskMetadata {
        finish_request: Some(pioneer_protocol::TaskFinishRequest {
            requested_at: FIRE_AT,
            reason: Some("forged marker".to_owned()),
            first_run_number: 0,
        }),
        ..Default::default()
    };
    let mut forged_create = interval_params();
    forged_create.metadata = Some(forged_metadata.clone());
    let create_error = original
        .service()
        .create_task(task_create_context_for(&forged_create), forged_create)
        .await
        .expect_err("task_create cannot manufacture a finish decision");
    assert!(format!("{create_error:#}").contains("server-owned"));
    let update_error = original
        .service()
        .update_task(
            TaskMutationContext::default(),
            TaskUpdateParams {
                task_id: created.task.id.clone(),
                metadata: Some(forged_metadata),
                ..Default::default()
            },
        )
        .await
        .expect_err("task_update cannot manufacture a finish decision");
    assert!(format!("{update_error:#}").contains("server-owned"));
    let unchanged = read(&original, &created.task.id).await;
    assert!(unchanged.task.finish_request().is_none());
    assert_eq!(unchanged.task.revision, created.task.revision);

    // Hold the exact materialization input a scheduler can calculate before
    // finish, then submit it only after the user stop has committed.
    let mut stale_run = idempotency_test_run(TaskRunStatus::Queued, 1);
    stale_run.id = pioneer_protocol::generate_id(21);
    stale_run.task_id = created.task.id.clone();
    stale_run.trigger_id = Some(created.trigger.id.clone());
    stale_run.ready_at = Some(FIRE_AT);
    stale_run.created_at = FIRE_AT;
    stale_run.updated_at = FIRE_AT;
    let actor = original
        .service()
        .store()
        .get_task_actor_contract(&created.task.id)
        .await
        .unwrap()
        .unwrap();
    let occurrence = crate::actor_contract::build_occurrence_contract(
        &created.task,
        &actor,
        &stale_run.id,
        Some(&created.trigger.id),
        format!("{}:1", created.trigger.id),
        1,
        FIRE_AT,
    )
    .unwrap();
    let stale_events = vec![
        TaskEventPayload::TaskQueued {
            task_id: created.task.id.clone(),
            run_id: Some(stale_run.id.clone()),
        },
        TaskEventPayload::RunCreated {
            run: stale_run.clone(),
            agent_spec: None,
        },
    ];
    let accepted = finish(&original, &created.task.id).await;
    assert!(!accepted.already_finished);
    assert_eq!(accepted.task.status, TaskStatus::Completed);
    assert!(
        original
            .service()
            .store()
            .append_due_trigger_task_events(
                &created.trigger,
                FIRE_AT,
                stale_events,
                vec![occurrence],
                vec![(stale_run.id.clone(), TaskExecutorKind::System)],
            )
            .await
            .unwrap()
            .is_empty(),
        "a pre-finish scheduler snapshot cannot create work after acceptance"
    );
    assert!(
        original
            .service()
            .store()
            .load_execution_for_run(&stale_run.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(accepted.task.result.is_none());
    let marker = accepted.task.finish_request().unwrap().clone();
    let restarted = TaskRuntime::new(original.service().store());
    let repeated = finish(&restarted, &created.task.id).await;
    assert!(repeated.already_finished);
    assert_eq!(repeated.task.finish_request(), Some(&marker));
    assert!(
        restarted
            .service()
            .resume_task(
                TaskMutationContext::default(),
                TaskResumeParams {
                    task_id: created.task.id.clone(),
                    reason: None,
                }
            )
            .await
            .is_err()
    );
    assert!(
        restarted
            .service()
            .update_task(
                TaskMutationContext::default(),
                TaskUpdateParams {
                    task_id: created.task.id.clone(),
                    title: Some("Do not revive".to_owned()),
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    assert!(
        restarted
            .service()
            .reschedule_task(
                TaskMutationContext::default(),
                TaskRescheduleParams {
                    task_id: created.task.id.clone(),
                    trigger: TaskTriggerInput {
                        spec: TaskTriggerSpec::Immediate
                    },
                }
            )
            .await
            .is_err()
    );
    assert_eq!(restarted.process_due_once(FIRE_AT + 100).await.unwrap(), 0);
    let saved = read(&restarted, &created.task.id).await;
    assert_eq!(saved.task.status, TaskStatus::Completed);
    assert_eq!(saved.task.finish_request(), Some(&marker));
    assert!(saved.runs.is_empty());
    assert!(saved.task.result.is_none());
    assert!(deliveries(&restarted, &created.task.id).await.is_empty());
}

#[tokio::test]
async fn finish_failure_retains_error_suppresses_retry_and_does_not_relabel_terminal_failure() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    params.retry_policy = Some(TaskRetryPolicy {
        max_attempts: 5,
        backoff: TaskRetryBackoffKind::Fixed,
        initial_delay_seconds: Some(1),
        max_delay_seconds: None,
        retry_on: vec![TaskErrorClass::Internal],
    });
    let mut retry_params = interval_params();
    retry_params.trigger.spec = TaskTriggerSpec::Immediate;
    retry_params.retry_policy = params.retry_policy.clone();
    let created = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap();
    let run = created.run.unwrap();
    let _handle = admit(&runtime, &run).await;
    let error = TaskError {
        recovery_diagnostic: None,
        code: "actual_failure".to_owned(),
        message: "The admitted work failed".to_owned(),
        class: TaskErrorClass::Internal,
        details: None,
        failed_run_id: Some(run.id.clone()),
    };
    // Failure preparation can decide to retry before finish wins the writer.
    // Its old decision must be revalidated rather than committing a new run.
    let prepared = runtime
        .service()
        .store()
        .prepare_task_terminal_transition(TaskEventPayload::RunFailed {
            task_id: created.task.id.clone(),
            run_id: run.id.clone(),
            error: Some(error.clone()),
            completed_at: run.created_at + 1,
        })
        .await
        .unwrap();
    finish(&runtime, &created.task.id).await;
    runtime
        .service()
        .store()
        .commit_task_terminal_transition(prepared)
        .await
        .unwrap();
    let saved = read(&runtime, &created.task.id).await;
    assert_eq!(saved.task.status, TaskStatus::Failed);
    assert_eq!(saved.task.error, Some(error.clone()));
    assert_eq!(saved.runs.len(), 1);
    assert_eq!(saved.runs[0].status, TaskRunStatus::Failed);
    assert_eq!(saved.runs[0].error, Some(error.clone()));
    assert!(saved.task.result.is_none());
    let repeated = finish(&runtime, &created.task.id).await;
    assert!(repeated.already_finished);
    assert_eq!(repeated.task.status, TaskStatus::Failed);
    let restarted = TaskRuntime::new(runtime.service().store());
    restarted
        .process_due_once(run.created_at + 100)
        .await
        .unwrap();
    assert_eq!(read(&restarted, &created.task.id).await.runs.len(), 1);

    // Older schedulers could rebind the shared occurrence to a queued retry
    // before acquiring dispatch. Exercise both that durable legacy shape and
    // the normal queue shape where the retry has no occurrence yet.
    for legacy_rebound in [false, true] {
        let pending = runtime
            .service()
            .create_task(task_create_context_for(&retry_params), retry_params.clone())
            .await
            .unwrap();
        let source = pending.run.unwrap();
        let handle = admit(&runtime, &source).await;
        let source_error = TaskError {
            failed_run_id: Some(source.id.clone()),
            ..error.clone()
        };
        handle
            .fail_run(Some(source_error.clone()), source.created_at + 1)
            .await
            .unwrap();
        let queued = read(&runtime, &pending.task.id).await;
        let retry = queued
            .runs
            .iter()
            .find(|run| run.attempt_number == 2)
            .unwrap();
        assert_eq!(retry.status, TaskRunStatus::Queued);
        let store = runtime.service().store();
        assert!(
            store
                .get_task_occurrence_contract_by_run(&retry.id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            deliveries(&runtime, &pending.task.id).await.is_empty(),
            "normal retry scheduling defers delivery of the failed attempt"
        );
        if legacy_rebound {
            let previous = store
                .get_task_occurrence_contract_by_run(&source.id)
                .await
                .unwrap()
                .unwrap();
            let mut rebound = previous
                .retry(u32::try_from(retry.attempt_number).unwrap())
                .unwrap();
            rebound.run_id = retry.id.clone();
            store
                .upsert_task_occurrence_contract(&rebound, source.created_at + 2)
                .await
                .unwrap();
            assert!(
                store
                    .get_task_occurrence_contract_by_run(&source.id)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let finished = finish(&runtime, &pending.task.id).await;
        assert_eq!(finished.task.status, TaskStatus::Failed);
        assert_eq!(finished.task.error, Some(source_error.clone()));
        let disposed = read(&runtime, &pending.task.id).await;
        assert_eq!(disposed.runs.len(), 2);
        assert_eq!(
            disposed
                .runs
                .iter()
                .find(|run| run.id == retry.id)
                .unwrap()
                .status,
            TaskRunStatus::Cancelled
        );
        let execution = store
            .load_execution_for_run(&source.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(execution.status, TaskRunExecutionStatus::Failed);
        assert_eq!(execution.error, Some(source_error.clone()));
        let occurrence_run_id = if legacy_rebound {
            &retry.id
        } else {
            &source.id
        };
        let occurrence = store
            .get_task_occurrence_contract_by_run(occurrence_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(occurrence.status, TaskOccurrenceStatus::Failed);
        if legacy_rebound {
            assert!(
                store
                    .get_task_occurrence_contract_by_run(&source.id)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let error_deliveries = deliveries(&runtime, &pending.task.id).await;
        assert_eq!(error_deliveries.len(), 1);
        assert_eq!(error_deliveries[0].run_id, source.id);
        assert_eq!(error_deliveries[0].status, TaskDeliveryStatus::Pending);
        assert_eq!(error_deliveries[0].mode, TaskDeliveryMode::Thread);
        assert_eq!(
            error_deliveries[0].target_thread_id.as_deref(),
            Some("thr_finish_owner")
        );
        assert_eq!(
            error_deliveries[0].error_snapshot,
            Some(source_error.clone())
        );
        // Replay the original terminal callback after cleanup: its frozen
        // result and delivery identity must remain a single durable outcome.
        handle
            .fail_run(Some(source_error), source.created_at + 1)
            .await
            .unwrap();
        assert_eq!(
            deliveries(&runtime, &pending.task.id).await,
            error_deliveries
        );
        let events = runtime
            .service()
            .get_task_events(TaskEventsParams {
                task_id: pending.task.id.clone(),
                after_sequence: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(events.events.iter().filter(|event| matches!(
            &event.payload, TaskEventPayload::RunFailed { run_id, .. } if run_id == &source.id,
        )).count(), 1);
        runtime
            .process_due_once(source.created_at + 100)
            .await
            .unwrap();
        assert_eq!(read(&runtime, &pending.task.id).await.runs.len(), 2);
    }
}

#[tokio::test]
async fn executing_task_can_finish_self_and_external_task_but_not_ancestor_and_cancel_stays_available()
 {
    let runtime = runtime().await;
    let mut root_params = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: FIRE_AT,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    root_params.created_by_thread_id = Some("thr_finish_ancestor".to_owned());
    let root = runtime
        .service()
        .create_task(task_create_context_for(&root_params), root_params)
        .await
        .unwrap();
    let mut child_params = create_params(TaskTriggerSpec::Immediate);
    child_params.created_by_thread_id = Some("thr_finish_ancestor".to_owned());
    child_params.parent_task_id = Some(root.task.id.clone());
    let child = runtime
        .service()
        .create_task(task_create_context_for(&child_params), child_params)
        .await
        .unwrap();
    let run = child.run.unwrap();
    let _handle = admit(&runtime, &run).await;
    let (thread_id, turn_id) =
        record_active_task_execution_turn(&runtime, &child.task.id, &run.id).await;
    let context = TaskMutationContext::parent_agent(thread_id, turn_id);
    let denied = runtime
        .service()
        .finish_task(
            context.clone(),
            TaskFinishParams {
                task_id: root.task.id.clone(),
                reason: None,
            },
        )
        .await
        .expect_err("finish must preserve ancestor protection");
    assert!(format!("{denied:#}").contains("current_execution_ancestor"));
    assert!(
        read(&runtime, &root.task.id)
            .await
            .task
            .finish_request()
            .is_none()
    );
    let external = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: FIRE_AT,
                timezone: None,
                catch_up_policy: None,
            }),
        )
        .await
        .unwrap();
    let stopped_external = runtime
        .service()
        .finish_task(
            context.clone(),
            TaskFinishParams {
                task_id: external.task.id.clone(),
                reason: Some("User requested external task stop".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(stopped_external.task.status, TaskStatus::Completed);
    let stopped_self = runtime
        .service()
        .finish_task(
            context.clone(),
            TaskFinishParams {
                task_id: child.task.id.clone(),
                reason: Some("Explicit stop condition reached".to_owned()),
            },
        )
        .await
        .unwrap();
    assert!(!stopped_self.already_finished);
    assert_eq!(stopped_self.task.status, TaskStatus::Running);
    let cancel_error = runtime
        .service()
        .cancel_task(
            context,
            TaskCancelParams {
                task_id: child.task.id.clone(),
                reason: None,
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect_err("self finish must not weaken cancel's independent guard");
    assert!(format!("{cancel_error:#}").contains("cannot_cancel_current_execution_task"));
    runtime
        .service()
        .cancel_task(
            TaskMutationContext::user(TEST_PRINCIPAL_ID),
            TaskCancelParams {
                task_id: child.task.id.clone(),
                reason: Some("Interrupt the remaining work".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::TaskOnly,
            },
        )
        .await
        .unwrap();
    let saved = read(&runtime, &child.task.id).await;
    assert_eq!(saved.task.status, TaskStatus::Cancelled);
    assert_eq!(saved.runs[0].status, TaskRunStatus::Cancelled);
    assert!(saved.task.finish_request().is_some());
}

#[tokio::test]
async fn finish_preserves_same_run_review_revision_and_acceptance() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context = parent_accept_context_for_candidate(&runtime, &task_id, &candidate_id).await;
    let before = read(&runtime, &task_id).await;
    let accepted = finish(&runtime, &task_id).await;
    assert_eq!(accepted.task.status, TaskStatus::WaitingReview);
    assert!(accepted.task.result.is_none());
    let revised = runtime
        .service()
        .revise_task_result_candidate(
            context.clone(),
            revise_params(task_id.clone(), run_id.clone(), candidate_id),
        )
        .await
        .unwrap();
    assert_eq!(revised.run.id, run_id);
    assert_eq!(revised.task_run_turn.kind, TaskRunTurnKind::Revision);
    let at = revised.task_run_turn.created_at + 1;
    let mut turn = revised.task_run_turn.clone();
    turn.status = TaskRunTurnStatus::CandidateCreated;
    turn.completed_at = Some(at);
    let mut candidate = before.result_candidates[0].clone();
    candidate.id = "finish_revised_candidate".to_owned();
    candidate.task_run_turn_id = turn.id.clone();
    candidate.turn_id = turn.turn_id.clone();
    candidate.round = turn.round;
    candidate.created_at = at;
    candidate.updated_at = at;
    candidate.result.as_mut().unwrap().summary = Some("Reviewed real result".to_owned());
    let handle = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        task_id.clone(),
        run_id.clone(),
    );
    handle
        .record_pending_review_result_candidate(turn, candidate.clone(), at)
        .await
        .unwrap();
    let waiting = read(&runtime, &task_id).await;
    assert_eq!(waiting.task.status, TaskStatus::WaitingReview);
    assert!(waiting.task.finish_request().is_some());
    runtime
        .service()
        .accept_task_result_candidate(
            context,
            accept_params(task_id.clone(), run_id.clone(), candidate.id),
        )
        .await
        .unwrap();
    let done = read(&runtime, &task_id).await;
    assert_eq!(done.task.status, TaskStatus::Completed);
    assert_eq!(done.runs.len(), 1);
    assert_eq!(done.runs[0].status, TaskRunStatus::Succeeded);
    assert_eq!(done.runs[0].result, candidate.result);
    assert!(done.task.finish_request().is_some());
}

#[tokio::test]
async fn ordinary_recurring_success_keeps_schedule_until_explicit_finish() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let params = interval_params();
    let created = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap();
    for at in [FIRE_AT, FIRE_AT + 10] {
        runtime.process_due_once(at).await.unwrap();
        let saved = read(&runtime, &created.task.id).await;
        assert!(saved.task.finish_request().is_none());
        assert_eq!(saved.triggers[0].status, TaskTriggerStatus::Active);
        assert!(
            saved
                .runs
                .iter()
                .all(|run| run.status == TaskRunStatus::Succeeded)
        );
    }
    let before_finish = read(&runtime, &created.task.id).await;
    assert_eq!(before_finish.runs.len(), 2);
    let last_result = before_finish
        .runs
        .iter()
        .max_by_key(|run| run.run_number)
        .unwrap()
        .result
        .clone();
    let stopped = finish(&runtime, &created.task.id).await;
    assert_eq!(stopped.task.status, TaskStatus::Completed);
    assert_eq!(stopped.task.result, last_result);
    assert_eq!(
        read(&runtime, &created.task.id).await.runs.len(),
        2,
        "finishing between occurrences must retain the real last result without a synthetic run"
    );
}

#[tokio::test]
async fn finish_keeps_failed_retry_source_before_active_sibling_in_final_outcome() {
    let runtime = runtime().await;
    let mut params = interval_params();
    params.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 3,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    params.retry_policy = Some(TaskRetryPolicy {
        max_attempts: 3,
        backoff: TaskRetryBackoffKind::Fixed,
        initial_delay_seconds: Some(1),
        max_delay_seconds: None,
        retry_on: vec![TaskErrorClass::Internal],
    });
    let created = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap();
    runtime.process_due_once(FIRE_AT + 25).await.unwrap();
    let mut initial = read(&runtime, &created.task.id).await;
    initial.runs.sort_by_key(|run| run.run_number);
    assert_eq!(initial.runs.len(), 3);
    let failing = admit(&runtime, &initial.runs[0]).await;
    let succeeding = admit(&runtime, &initial.runs[1]).await;
    let source_error = TaskError {
        recovery_diagnostic: None,
        code: "parallel_failure".to_owned(),
        message: "First sibling failed before finish".to_owned(),
        class: TaskErrorClass::Internal,
        details: None,
        failed_run_id: Some(initial.runs[0].id.clone()),
    };
    failing
        .fail_run(Some(source_error.clone()), FIRE_AT + 26)
        .await
        .unwrap();
    let with_retry = read(&runtime, &created.task.id).await;
    let retry = with_retry
        .runs
        .iter()
        .find(|run| run.retry_of_run_id.as_deref() == Some(initial.runs[0].id.as_str()))
        .unwrap();
    assert_eq!(retry.status, TaskRunStatus::Queued);
    let accepted = finish(&runtime, &created.task.id).await;
    assert!(!accepted.task.status.is_terminal());
    let draining = read(&runtime, &created.task.id).await;
    assert_eq!(
        draining
            .runs
            .iter()
            .find(|run| run.id == initial.runs[1].id)
            .unwrap()
            .status,
        TaskRunStatus::Running
    );
    assert_eq!(
        draining
            .runs
            .iter()
            .find(|run| run.id == retry.id)
            .unwrap()
            .status,
        TaskRunStatus::Cancelled
    );
    assert!(
        runtime
            .service()
            .store()
            .claim_task_run_for_dispatch(&retry.id, FIRE_AT + 27)
            .await
            .unwrap()
            .is_none()
    );
    let real_result = result(&initial.runs[1]);
    succeeding
        .complete_run(Some(real_result.clone()), FIRE_AT + 28)
        .await
        .unwrap();
    let done = read(&runtime, &created.task.id).await;
    assert_eq!(
        done.task.status,
        TaskStatus::Failed,
        "a later sibling's success cannot erase the earlier source whose retry was stopped"
    );
    assert_eq!(done.task.error, Some(source_error.clone()));
    let succeeded = done
        .runs
        .iter()
        .find(|run| run.id == initial.runs[1].id)
        .unwrap();
    assert_eq!(succeeded.status, TaskRunStatus::Succeeded);
    assert_eq!(succeeded.result, Some(real_result.clone()));
    assert_eq!(
        done.runs
            .iter()
            .find(|run| run.id == initial.runs[0].id)
            .unwrap()
            .status,
        TaskRunStatus::Failed
    );
    let deliveries = deliveries(&runtime, &created.task.id).await;
    assert_eq!(deliveries.len(), 2);
    assert!(
        deliveries
            .iter()
            .any(|delivery| delivery.run_id == initial.runs[0].id
                && delivery.error_snapshot == Some(source_error.clone()))
    );
    assert!(
        deliveries
            .iter()
            .any(|delivery| delivery.run_id == initial.runs[1].id
                && delivery.result_snapshot == Some(real_result.clone()))
    );
    runtime.process_due_once(FIRE_AT + 100).await.unwrap();
    assert_eq!(
        read(&runtime, &created.task.id).await.runs.len(),
        with_retry.runs.len()
    );
}

#[tokio::test]
async fn finish_recovery_disposes_future_retries_before_ready_at_in_bounded_passes() {
    // The larger case leaves more work than one disposal transaction can close.
    for run_count in [1_u32, 17] {
        let original = runtime().await;
        let mut params = interval_params();
        params.trigger.spec = TaskTriggerSpec::Interval {
            interval_seconds: 10,
            interval_anchor_at: Some(FIRE_AT),
            catch_up_policy: Some(TaskTriggerCatchUpPolicy::run_all_missed(run_count)),
        };
        params.concurrency_policy = Some(TaskConcurrencyPolicy {
            key: None,
            max_parallel_runs: run_count,
            on_conflict: TaskConcurrencyConflictPolicy::Queue,
        });
        params.retry_policy = Some(TaskRetryPolicy {
            max_attempts: 2,
            backoff: TaskRetryBackoffKind::Fixed,
            initial_delay_seconds: Some(86_400),
            max_delay_seconds: None,
            retry_on: vec![TaskErrorClass::Internal],
        });
        let created = original
            .service()
            .create_task(task_create_context_for(&params), params)
            .await
            .unwrap();
        original
            .process_due_once(FIRE_AT + i64::from(run_count - 1) * 10)
            .await
            .unwrap();
        let sources = read(&original, &created.task.id).await.runs;
        assert_eq!(sources.len(), run_count as usize);
        let mut handles = Vec::new();
        for source in &sources {
            handles.push(admit(&original, source).await);
        }
        let mut errors = Vec::new();
        for (index, (source, handle)) in sources.iter().zip(&handles).enumerate() {
            let error = TaskError {
                recovery_diagnostic: None,
                code: "future_retry_failure".to_owned(),
                message: "Real work failed before the Gateway stopped".to_owned(),
                class: TaskErrorClass::Internal,
                details: None,
                failed_run_id: Some(source.id.clone()),
            };
            handle
                .fail_run(Some(error.clone()), FIRE_AT + 1_000 + index as i64)
                .await
                .unwrap();
            errors.push(error);
        }
        let before_stop = read(&original, &created.task.id).await;
        let retries = before_stop
            .runs
            .iter()
            .filter(|run| run.retry_of_run_id.is_some())
            .cloned()
            .collect::<Vec<_>>();
        let recovered_at = FIRE_AT + 2_000;
        assert_eq!(retries.len(), sources.len());
        assert!(retries.iter().all(|run| {
            run.status == TaskRunStatus::Queued
                && run
                    .ready_at
                    .is_some_and(|ready_at| ready_at > recovered_at + 80_000)
        }));
        assert!(deliveries(&original, &created.task.id).await.is_empty());

        // Persist only acceptance, deliberately skipping finish_task and its
        // cleanup. Recovery must discover the queue through a normal pass.
        let store = original.service().store();
        store
            .request_task_finish(
                &created.task.id,
                Some("User asked to stop after the admitted work".to_owned()),
                FIRE_AT + 1_100,
                None,
            )
            .await
            .unwrap();
        let accepted = read(&original, &created.task.id).await;
        assert_eq!(accepted.task.status, TaskStatus::Failed);
        assert_eq!(accepted.task.error.as_ref(), errors.last());
        assert!(accepted.task.finish_request().is_some());
        assert!(
            accepted
                .runs
                .iter()
                .filter(|run| run.retry_of_run_id.is_some())
                .all(|run| run.status == TaskRunStatus::Queued)
        );
        let stopped_task = accepted.task;
        drop(handles);
        drop(original);

        let restarted = TaskRuntime::new(store.clone());
        restarted
            .register_executor(Arc::new(CompletingSystemExecutor))
            .await;
        assert_eq!(restarted.process_due_once(recovered_at).await.unwrap(), 0);
        let first_pass = read(&restarted, &created.task.id).await;
        let remaining = first_pass
            .runs
            .iter()
            .filter(|run| run.status == TaskRunStatus::Queued)
            .count();
        assert!(
            remaining < retries.len(),
            "recovery must make progress before retry ready_at"
        );
        if run_count == 1 {
            assert_eq!(remaining, 0);
        } else {
            assert!(
                remaining > 0,
                "a large queue must retain a bounded disposal quantum"
            );
        }
        // A remaining batch must be rediscovered without advancing to ready_at.
        for _ in 0..2 {
            assert_eq!(restarted.process_due_once(recovered_at).await.unwrap(), 0);
        }
        let done = read(&restarted, &created.task.id).await;
        assert_eq!(done.task, stopped_task);
        assert_eq!(done.runs.len(), sources.len() + retries.len());
        for retry in &retries {
            let saved = done.runs.iter().find(|run| run.id == retry.id).unwrap();
            assert_eq!(saved.status, TaskRunStatus::Cancelled);
            assert!(saved.started_at.is_none());
            assert!(
                store
                    .load_execution_for_run(&retry.id)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let queued_deliveries = deliveries(&restarted, &created.task.id).await;
        assert_eq!(queued_deliveries.len(), sources.len());
        for (source, error) in sources.iter().zip(&errors) {
            let saved = done.runs.iter().find(|run| run.id == source.id).unwrap();
            assert_eq!(saved.status, TaskRunStatus::Failed);
            assert_eq!(saved.error.as_ref(), Some(error));
            let execution = store
                .load_execution_for_run(&source.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(execution.status, TaskRunExecutionStatus::Failed);
            assert_eq!(execution.error.as_ref(), Some(error));
            let occurrence = store
                .get_task_occurrence_contract_by_run(&source.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(occurrence.status, TaskOccurrenceStatus::Failed);
            let delivery = queued_deliveries
                .iter()
                .find(|delivery| delivery.run_id == source.id)
                .unwrap();
            assert_eq!(delivery.status, TaskDeliveryStatus::Pending);
            assert_eq!(delivery.mode, TaskDeliveryMode::Thread);
            assert_eq!(
                delivery.target_thread_id.as_deref(),
                Some("thr_finish_owner")
            );
            assert_eq!(delivery.error_snapshot.as_ref(), Some(error));
        }
        assert_eq!(
            restarted.process_due_once(recovered_at + 1).await.unwrap(),
            0
        );
        assert_eq!(read(&restarted, &created.task.id).await, done);
        assert_eq!(
            deliveries(&restarted, &created.task.id).await,
            queued_deliveries
        );
    }
}
